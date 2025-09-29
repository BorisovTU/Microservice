package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.config.BigDecimalDeserializer;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.config.BigDecimalSerializer;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.constant.BeanConstants;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionRequest;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.property.CustomKafkaProperties;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.InstructionProcessorService;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.InstructionViewService;

import java.math.BigDecimal;
import java.util.UUID;

@EnableKafka
@EnableKafkaStreams
@Component
@Slf4j
@RequiredArgsConstructor
public class InternalInstructionListener {

    private final InstructionProcessorService instructionService;
    private final InstructionViewService instructionViewService;
    private final CustomKafkaProperties kafkaProperties;

    @KafkaListener(
            groupId = "${kafka.internal-instruction-view.consumer.group-id}",
            topics = "${kafka.internal-instruction-view.topic}",
            containerFactory = BeanConstants.INTERNAL_INSTRUCTION_CONSUMER_FACTORY
    )
    @Transactional
    public void processInstructionView(CorporateActionInstructionRequest instructionRequest) {
        instructionViewService.postView(instructionRequest);
    }

    @KafkaListener(
            groupId = "${kafka.internal-instruction.consumer.group-id}",
            topics = "${kafka.internal-instruction.topic}",
            containerFactory = BeanConstants.INTERNAL_INSTRUCTION_VIEW_CONSUMER_FACTORY
    )
    @Transactional
    public void processInstruction(CorporateActionInstructionRequest instructionRequest) {
        instructionService.processInstruction(instructionRequest);
    }

    @Autowired
    public void processBalanceValidation(StreamsBuilder streamsBuilder) {
        log.info("Processing Balance Validation with GlobalKTable started");

        // Создаем Serde для BigDecimal
        Serde<BigDecimal> bigDecimalSerde = Serdes.serdeFrom(
                new BigDecimalSerializer(),
                new BigDecimalDeserializer()
        );

        // 1. Агрегируем ВСЕ записи из топика в KTable
        KTable<String, BigDecimal> clientBalancesTable = streamsBuilder
                .stream(kafkaProperties.getInternalInstruction().getTopic(),
                        Consumed.with(Serdes.String(), new JsonSerde<>(CorporateActionInstructionRequest.class)))
                .map((key, instruction) -> {
                    String clientId = instruction.getBnfclOwnrDtls().getOwnerSecurityID();
                    BigDecimal balance = instruction.getBal();
                    log.info("PROCESSING RECORD - clientId: {}, balance: {}, instructionId: {}",
                            clientId, balance, instruction.getInstrNmb());
                    return KeyValue.pair(clientId, balance);
                }, Named.as("balance-mapper"))
                .groupByKey(Grouped.with(Serdes.String(), bigDecimalSerde))
                .aggregate(
                        () -> {
                            log.info("INITIALIZING AGGREGATE for new client");
                            return BigDecimal.ZERO;
                        },
                        (clientId, newBalance, aggregate) -> {
                            BigDecimal result = aggregate.add(newBalance);
                            log.info("AGGREGATE UPDATE - clientId: {}, newBalance: {}, previousTotal: {}, newTotal: {}",
                                    clientId, newBalance, aggregate, result);
                            return result;
                        },
                        Materialized.<String, BigDecimal, KeyValueStore<Bytes, byte[]>>as(kafkaProperties.getClientBalancesStore())
                                .withKeySerde(Serdes.String())
                                .withValueSerde(bigDecimalSerde)
                );

        // 2. Преобразуем KTable в поток обновлений и записываем в топик для GlobalKTable
        String globalTableTopic = kafkaProperties.getStreamTopicClientBalancesGlobal();
        clientBalancesTable
                .toStream()
                .to(globalTableTopic, Produced.with(Serdes.String(), bigDecimalSerde));

        // 3. Создаем GlobalKTable из топика с агрегированными балансами
        GlobalKTable<String, BigDecimal> clientBalancesGlobal = streamsBuilder
                .globalTable(globalTableTopic,
                        Consumed.with(Serdes.String(), bigDecimalSerde),
                        Materialized.<String, BigDecimal, KeyValueStore<Bytes, byte[]>>as("client-balances-global-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(bigDecimalSerde));

        // 4. Обрабатываем входящие инструкции для проверки баланса
        KStream<String, CorporateActionInstructionRequest> validationStream = streamsBuilder
                .stream(kafkaProperties.getInternalInstructionBalance().getTopic(),
                        Consumed.with(Serdes.String(), new JsonSerde<>(CorporateActionInstructionRequest.class)))
                .map((key, instruction) -> {
                    String clientId = instruction.getBnfclOwnrDtls().getOwnerSecurityID();
                    log.info("VALIDATION INPUT - clientId: {}, instructionId: {}",
                            clientId, instruction.getInstrNmb());
                    return KeyValue.pair(clientId, instruction);
                }, Named.as("validation-mapper"));

        // 5. Объединяем с GlobalKTable и проверяем лимит
        KStream<UUID, CorporateActionInstructionRequest> validatedInstructions = validationStream
                .leftJoin(clientBalancesGlobal,
                        (clientId, instruction) -> clientId,
                        (instruction, totalBalance) -> {
                            // Если баланс не найден, значит клиент новый или нет операций
                            if (totalBalance == null) {
                                totalBalance = BigDecimal.ZERO;
                                log.info("NO PREVIOUS BALANCE FOUND for client: {}",
                                        instruction.getBnfclOwnrDtls().getOwnerSecurityID());
                            }

                            BigDecimal clientLimit = instructionService.getInternalInstructionLimit(
                                    Long.parseLong(instruction.getBnfclOwnrDtls().getOwnerSecurityID())
                            );
                            BigDecimal newTotal = totalBalance.add(instruction.getBal());

                            log.info("VALIDATION - clientId: {}, currentBalance: {}, newInstruction: {}, newTotal: {}, limit: {}",
                                    instruction.getBnfclOwnrDtls().getOwnerSecurityID(), totalBalance,
                                    instruction.getBal(), newTotal, clientLimit);

                            return new ValidationResult(instruction, newTotal, clientLimit);
                        })
                .filter((clientId, validationResult) -> {
                    boolean isValid = validationResult.getNewTotal().compareTo(validationResult.getClientLimit()) <= 0;
                    if (isValid) {
                        log.info("VALIDATION PASSED - clientId: {}, newTotal: {}, limit: {}",
                                clientId, validationResult.getNewTotal(), validationResult.getClientLimit());
                    } else {
                        log.warn("VALIDATION FAILED - clientId: {}, newTotal: {}, limit: {}",
                                clientId, validationResult.getNewTotal(), validationResult.getClientLimit());
                    }
                    return isValid;
                }, Named.as("balance-validator"))
                .map((clientId, validationResult) -> {
                    UUID originalKey = UUID.fromString(validationResult.getInstruction().getInstrNmb());
                    log.info("SENDING VALIDATED INSTRUCTION - instructionId: {}, clientId: {}",
                            originalKey, clientId);
                    return KeyValue.pair(originalKey, validationResult.getInstruction());
                }, Named.as("result-mapper"));

        // 6. Отправляем validated инструкции в выходной топик
        validatedInstructions.to(kafkaProperties.getInternalInstruction().getTopic(),
                Produced.with(Serdes.UUID(), new JsonSerde<>(CorporateActionInstructionRequest.class)));

        log.info("Processing Balance Validation with GlobalKTable finished");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class ValidationResult {
        private CorporateActionInstructionRequest instruction;
        private BigDecimal newTotal;
        private BigDecimal clientLimit;
    }
}
