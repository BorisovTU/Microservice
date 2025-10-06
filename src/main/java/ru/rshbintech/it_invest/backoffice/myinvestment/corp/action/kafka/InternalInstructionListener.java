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
        instructionViewService.postSuccessView(instructionRequest);
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

    @KafkaListener(
            groupId = "${kafka.internal-instruction.consumer.group-id}",
            topics = "${kafka.internal-instruction.topic-dlq}",
            containerFactory = BeanConstants.INTERNAL_INSTRUCTION_VIEW_CONSUMER_FACTORY
    )
    @Transactional
    public void processInstructionBadView(CorporateActionInstructionRequest instructionRequest) {
        instructionViewService.postBadView(instructionRequest);

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
                }, Named.as("balance-mapper-new"))
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
                        Materialized.<String, BigDecimal, KeyValueStore<Bytes, byte[]>>as("client-balances-global-store-new")
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
        KStream<String, ValidationResult> validationResults = validationStream
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
                        });

        // 6. Разделяем поток на валидные и невалидные инструкции используя branch()
        @SuppressWarnings("unchecked")
        KStream<String, ValidationResult>[] branches = validationResults
                .branch(
                        (clientId, validationResult) -> {
                            // Первая ветка - валидные инструкции
                            boolean isValid = validationResult.getNewTotal().compareTo(validationResult.getClientLimit()) <= 0;
                            if (isValid) {
                                log.info("VALIDATION PASSED - clientId: {}, newTotal: {}, limit: {}",
                                        clientId, validationResult.getNewTotal(), validationResult.getClientLimit());
                            }
                            return isValid;
                        },
                        (clientId, validationResult) -> {
                            // Вторая ветка - невалидные инструкции
                            boolean isValid = validationResult.getNewTotal().compareTo(validationResult.getClientLimit()) <= 0;
                            if (!isValid) {
                                log.warn("VALIDATION FAILED - clientId: {}, newTotal: {}, limit: {}",
                                        clientId, validationResult.getNewTotal(), validationResult.getClientLimit());
                            }
                            return !isValid;
                        }
                );

        // 7. Обрабатываем валидные инструкции
        KStream<UUID, CorporateActionInstructionRequest> validatedInstructions = branches[0]
                .map((clientId, validationResult) -> {
                    UUID originalKey = UUID.fromString(validationResult.getInstruction().getInstrNmb());
                    log.info("SENDING VALIDATED INSTRUCTION - instructionId: {}, clientId: {}",
                            originalKey, clientId);
                    return KeyValue.pair(originalKey, validationResult.getInstruction());
                }, Named.as("valid-result-mapper-new"));

        // 8. Обрабатываем невалидные инструкции
        KStream<UUID, CorporateActionInstructionRequest> rejectedInstructions = branches[1]
                .map((clientId, validationResult) -> {
                    UUID originalKey = UUID.fromString(validationResult.getInstruction().getInstrNmb());
                    log.warn("SENDING REJECTED INSTRUCTION TO DLQ - instructionId: {}, clientId: {}, newTotal: {}, limit: {}",
                            originalKey, clientId, validationResult.getNewTotal(), validationResult.getClientLimit());
                    return KeyValue.pair(originalKey, validationResult.getInstruction());
                }, Named.as("rejected-result-mapper-new"));

        log.info("validated instructions: {}", validatedInstructions);
        // 9. Отправляем validated инструкции в выходной топик
        validatedInstructions.to(kafkaProperties.getInternalInstruction().getTopic(),
                Produced.with(Serdes.UUID(), new JsonSerde<>(CorporateActionInstructionRequest.class)));
        log.info("rejected instructions: {}", rejectedInstructions);
        // 10. Отправляем rejected инструкции в DLQ топик
        rejectedInstructions.to(kafkaProperties.getInternalInstruction().getTopicDlq(),
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
