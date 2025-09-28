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
        log.info("Processing Balance Validation started");

        // Создаем Serde для BigDecimal
        Serde<BigDecimal> bigDecimalSerde = Serdes.serdeFrom(
                new BigDecimalSerializer(),
                new BigDecimalDeserializer()
        );

        // 1. Сначала маппим в пары (clientId, balance)
        KStream<String, BigDecimal> instructionBalances = streamsBuilder
                .stream(kafkaProperties.getInternalInstruction().getTopic(),
                        Consumed.with(Serdes.String(), new JsonSerde<>(CorporateActionInstructionRequest.class)))
                .map((key, instruction) -> {
                    String clientId = instruction.getBnfclOwnrDtls().getOwnerSecurityID();
                    BigDecimal balance = instruction.getBal();
                    log.info("MAPPING - clientId: {}, balance: {}", clientId, balance);
                    return KeyValue.pair(clientId, balance);
                });

        // 2. Группируем по ключу и агрегируем суммы
        KTable<String, BigDecimal> clientBalances = instructionBalances
                .groupByKey(Grouped.with(Serdes.String(), bigDecimalSerde))
                .aggregate(
                        () -> BigDecimal.ZERO,
                        (clientId, newBalance, aggregate) -> {
                            BigDecimal result = aggregate.add(newBalance);
                            log.info("AGGREGATE - clientId: {}, newBalance: {}, previousTotal: {}, newTotal: {}",
                                    clientId, newBalance, aggregate, result);
                            return result;
                        },
                        Materialized.<String, BigDecimal, KeyValueStore<Bytes, byte[]>>as("client-balances-aggregate")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(bigDecimalSerde)
                );

        // 3. Обрабатываем входящие инструкции для проверки баланса
        KStream<String, CorporateActionInstructionRequest> validationStream = streamsBuilder
                .stream(kafkaProperties.getInternalInstructionBalance().getTopic(),
                        Consumed.with(Serdes.String(), new JsonSerde<>(CorporateActionInstructionRequest.class)))
                .map((key, instruction) -> {
                    // Используем clientId как ключ для join
                    String clientId = instruction.getBnfclOwnrDtls().getOwnerSecurityID();
                    return KeyValue.pair(clientId, instruction);
                });

        // 4. Объединяем с агрегированными балансами и проверяем лимит
        KStream<String, CorporateActionInstructionRequest> validatedInstructions = validationStream
                .leftJoin(clientBalances,
                        (instruction, totalBalance) -> {
                            if (totalBalance == null) {
                                totalBalance = BigDecimal.ZERO;
                            }

                            BigDecimal clientLimit = instructionService.getInternalInstructionLimit(
                                    Long.parseLong(instruction.getBnfclOwnrDtls().getOwnerSecurityID())
                            );
                            BigDecimal newTotal = totalBalance.add(instruction.getBal());

                            log.info("VALIDATION - clientId: {}, currentBalance: {}, newInstruction: {}, newTotal: {}, limit: {}",
                                    instruction.getBnfclOwnrDtls().getOwnerSecurityID(), totalBalance,
                                    instruction.getBal(), newTotal, clientLimit);

                            return new ValidationResult(instruction, newTotal, clientLimit);
                        },
                        Joined.with(
                                Serdes.String(), // Key
                                new JsonSerde<>(CorporateActionInstructionRequest.class), // Left value
                                bigDecimalSerde // Right value
                        ))
                .filter((clientId, validationResult) -> {
                    boolean isValid = validationResult.getNewTotal().compareTo(validationResult.getClientLimit()) <= 0;
                    log.info("FILTER - clientId: {}, newTotal: {}, limit: {}, valid: {}",
                            clientId, validationResult.getNewTotal(), validationResult.getClientLimit(), isValid);
                    return isValid;
                })
                .map((clientId, validationResult) -> {
                    // Возвращаем оригинальный ключ или создаем новый
                    String originalKey = validationResult.getInstruction().getInstrNmb(); // или другой уникальный идентификатор
                    return KeyValue.pair(originalKey, validationResult.getInstruction());
                });

        // 5. Отправляем validated инструкции в выходной топик
        validatedInstructions.to(kafkaProperties.getInternalInstruction().getTopic(),
                Produced.with(Serdes.String(), new JsonSerde<>(CorporateActionInstructionRequest.class)));

        log.info("Processing Balance Validation finished");
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
