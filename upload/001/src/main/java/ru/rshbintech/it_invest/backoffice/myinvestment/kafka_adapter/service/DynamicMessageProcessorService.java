package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.dao.DynamicMultiDatabaseMessageDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.DatabaseStats;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.KafkaMessage;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.OutgoingMessageResult;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
@Service
@RequiredArgsConstructor
public class DynamicMessageProcessorService {

    private final DynamicMultiDatabaseMessageDao messageDao;
    private final CloudStreamProducerService producerService;
    private final DynamicDatabaseRoutingService routingService;

    @Bean
    public Consumer<org.springframework.messaging.Message<String>> messageProcessor() {
        return message -> {
            try {
                String topic = (String) message.getHeaders().get("kafka_receivedTopic");
                String key = (String) message.getHeaders().get("kafka_receivedMessageKey");
                String payload = message.getPayload();

                KafkaMessage kafkaMessage = KafkaMessage.builder()
                        .topic(topic)
                        .messageId(key != null ? key : java.util.UUID.randomUUID().toString())
                        .timestamp(java.time.LocalDateTime.now())
                        .headers("{}")
                        .payload(payload)
                        .build();

                var result = messageDao.saveMessageToDatabase(kafkaMessage);

                if (!result.isSuccess()) {
                    log.error("Failed to save message to database: {}", result.getErrorDescription());
                } else {
                    log.debug("Successfully routed message to database for topic {}: {}", topic, kafkaMessage.getMessageId());
                }

            } catch (Exception e) {
                log.error("Error processing incoming message: {}", e.getMessage(), e);
            }
        };
    }

    @Scheduled(fixedDelayString = "#{@dynamicDatabaseRoutingService.getPollingOrder().stream()" +
            ".map(db -> @dynamicDatabaseRoutingService.getDatabaseConfig(db).getPollingInterval())" +
            ".min(Long::compare).orElse(5000)}")
    public void pollAllDatabasesForOutgoingMessages() {
        List<String> pollingOrder = routingService.getPollingOrder();

        for (String databaseName : pollingOrder) {
            try {
                if (!routingService.isDatabaseAvailable(databaseName)) {
                    log.warn("Database {} is unavailable, skipping", databaseName);
                    continue;
                }

                OutgoingMessageResult result = messageDao.readMessageFromDatabase(databaseName);

                if (result.isNoMessages()) {
                    continue;
                }

                if (!result.isSuccess()) {
                    log.warn("Error reading from database {}: code {}, description {}",
                            databaseName, result.getErrorCode(), result.getErrorDescription());
                    continue;
                }

                if (result.hasMessage()) {
                    KafkaMessage message = result.getMessage();
                    try {
                        if (producerService.supportsTopic(message.getTopic())) {
                            producerService.sendMessage(message);
                            log.debug("Successfully sent message from database {}: {}",
                                    databaseName, message.getMessageId());
                        } else {
                            log.warn("Topic {} not supported for message from database {}",
                                    message.getTopic(), databaseName);
                            messageDao.saveErrorToDatabase(databaseName, message.getTopic(),
                                    message.getMessageId(), 400, "Topic not supported");
                        }
                    } catch (Exception e) {
                        log.error("Error sending message from database {}: {}", databaseName, e.getMessage());
                        messageDao.saveErrorToDatabase(databaseName, message.getTopic(),
                                message.getMessageId(), 500, e.getMessage());
                    }
                }

            } catch (Exception e) {
                log.error("Error polling database {}: {}", databaseName, e.getMessage());
            }
        }
    }

    public void processSingleDatabase(String databaseName) {
        try {
            OutgoingMessageResult result = messageDao.readMessageFromDatabase(databaseName);

            if (result.hasMessage() && result.isSuccess()) {
                KafkaMessage message = result.getMessage();
                producerService.sendMessage(message);
                log.info("Manually processed message from database {}: {}", databaseName, message.getMessageId());
            }
        } catch (Exception e) {
            log.error("Error processing database {} manually: {}", databaseName, e.getMessage());
        }
    }

    public DatabaseStats getDatabaseStats() {
        List<DatabaseStats.DatabaseInfo> infos = new ArrayList<>();

        routingService.getAllDatabaseConfigs().forEach(config -> {
            boolean available = routingService.isDatabaseAvailable(config.getName());
            infos.add(new DatabaseStats.DatabaseInfo(
                    config.getName(),
                    config.getType(),
                    available,
                    config.isPollingEnabled(),
                    config.getPollingInterval()
            ));
        });

        return new DatabaseStats(infos);
    }
}
