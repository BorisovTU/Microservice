package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Управление жизненным циклом Kafka компонентов.
 * Обеспечивает корректный запуск и остановку Kafka прослушивателей.
 * Потокобезопасная реализация с использованием AtomicBoolean и Lock.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaLifecycleConfig implements SmartLifecycle {

    private final KafkaConsumerConfig kafkaConsumerConfig;

    /**
     * Атомарный флаг состояния для обеспечения потокобезопасности.
     * Гарантирует видимость изменений между потоками без блокировок для чтения.
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Блокировка для синхронизации операций запуска и остановки.
     * Защищает от одновременного вызова start/stop из разных потоков.
     */
    private final Lock lifecycleLock = new ReentrantLock();

    /**
     * Запускает Kafka инфраструктуру.
     * Вызывается Spring при запуске контекста.
     */
    @Override
    public void start() {
        lifecycleLock.lock();
        try {
            if (running.compareAndSet(false, true)) {
                log.info("Starting Kafka infrastructure...");
                // Listeners уже запущены через @EventListener(ApplicationReadyEvent.class)
                // Здесь можно добавить дополнительную логику инициализации если нужно
                log.info("Kafka infrastructure started successfully");
            } else {
                log.debug("Kafka infrastructure is already running");
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * Останавливает Kafka инфраструктуру.
     * Вызывается Spring при остановке контекста.
     */
    @Override
    public void stop() {
        lifecycleLock.lock();
        try {
            if (running.compareAndSet(true, false)) {
                log.info("Stopping Kafka infrastructure...");
                kafkaConsumerConfig.stopAllContainers();
                log.info("Kafka infrastructure stopped successfully");
            } else {
                log.debug("Kafka infrastructure is already stopped");
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * Останавливает Kafka инфраструктуру с ожиданием завершения.
     *
     * @param callback callback для уведомления о завершении остановки
     */
    @Override
    public void stop(Runnable callback) {
        lifecycleLock.lock();
        try {
            stop();
            callback.run();
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * Проверяет, запущена ли Kafka инфраструктура.
     *
     * @return true если инфраструктура запущена, иначе false
     */
    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Определяет порядок запуска и остановки компонентов.
     * Компоненты с большим значением phase запускаются позже и останавливаются раньше.
     *
     * @return фаза жизненного цикла (запускается позже, останавливается раньше)
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    /**
     * Определяет, должен ли этот компонент запускаться автоматически.
     *
     * @return true для автоматического запуска
     */
    @Override
    public boolean isAutoStartup() {
        return true;
    }
}
