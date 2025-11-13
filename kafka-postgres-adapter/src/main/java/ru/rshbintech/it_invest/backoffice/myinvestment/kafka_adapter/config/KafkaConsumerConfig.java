package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.property.KafkaProperties;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service.KafkaMessageListenerService;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Конфигурация Kafka Consumer'ов.
 * Создает и управляет Kafka listeners для всех топиков из конфигурации.
 * Потокобезопасная реализация с использованием CopyOnWriteArrayList и Lock.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final KafkaMessageListenerService kafkaMessageListenerService;
    private final KafkaProperties kafkaProperties;
    private final KafkaConfig.ConsumerFactoryFactory consumerFactoryFactory;

    /**
     * Потокобезопасный список контейнеров.
     */
    private final List<ConcurrentMessageListenerContainer<String, String>> containers = new CopyOnWriteArrayList<>();

    /**
     * Блокировка для синхронизации операций с контейнерами.
     * Защищает от race condition при одновременном создании и остановке контейнеров.
     */
    private final Lock containersLock = new ReentrantLock();

    /**
     * Создает и запускает Kafka listeners при старте приложения.
     * Вызывается автоматически после полной инициализации контекста Spring.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void createAndStartListeners() {
        containersLock.lock();
        try {
            if (!containers.isEmpty()) {
                log.warn("Kafka listeners already created, skipping initialization");
                return;
            }

            log.info("Creating Kafka listeners for {} topics", kafkaProperties.getConsumers().size());

            kafkaProperties.getConsumers().forEach(config -> {
                try {
                    ConcurrentMessageListenerContainer<String, String> container = createListenerContainer(config);
                    container.start();
                    containers.add(container);
                    log.info("Started Kafka listener for topic: {} with servers: {} and group: {}",
                            config.getTopic(), config.getBootstrapServers(), config.getGroupId());
                } catch (Exception e) {
                    log.error("Failed to create Kafka listener for topic {}: {}", config.getTopic(), e.getMessage(), e);
                }
            });

            log.info("All Kafka listeners started successfully. Total: {}", containers.size());
        } finally {
            containersLock.unlock();
        }
    }

    /**
     * Создает контейнер для прослушивания конкретного топика.
     *
     * @param config конфигурация потребителя для топика
     * @return настроенный контейнер для прослушивания
     */
    private ConcurrentMessageListenerContainer<String, String> createListenerContainer(
            KafkaProperties.ConsumerConfig config) {

        ConsumerFactory<String, String> consumerFactory =
                consumerFactoryFactory.createConsumerFactory(config.getBootstrapServers(), config.getGroupId());

        ContainerProperties containerProps = new ContainerProperties(config.getTopic());
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        containerProps.setMessageListener((MessageListener<String, String>) record -> {
            kafkaMessageListenerService.processMessage(
                    record.topic(),
                    record.key(),
                    record.value(),
                    record.partition(),
                    record.offset()
            );
        });

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);
        container.setConcurrency(1);

        return container;
    }

    /**
     * Останавливает все Kafka listeners при завершении работы приложения.
     * Метод является идемпотентным и потокобезопасным.
     */
    public void stopAllContainers() {
        containersLock.lock();
        try {
            if (containers.isEmpty()) {
                log.debug("No Kafka listeners to stop");
                return;
            }

            log.info("Stopping {} Kafka listeners...", containers.size());

            int stoppedCount = 0;
            int errorCount = 0;

            for (ConcurrentMessageListenerContainer<String, String> container : containers) {
                try {
                    if (container.isRunning()) {
                        container.stop();
                        stoppedCount++;
                        log.debug("Stopped Kafka listener for topic: {}", container.getContainerProperties().getTopics());
                    }
                } catch (Exception e) {
                    errorCount++;
                    log.error("Error stopping container: {}", e.getMessage(), e);
                }
            }

            containers.clear();
            log.info("Kafka listeners stopped: {}, errors: {}", stoppedCount, errorCount);

        } finally {
            containersLock.unlock();
        }
    }

}
