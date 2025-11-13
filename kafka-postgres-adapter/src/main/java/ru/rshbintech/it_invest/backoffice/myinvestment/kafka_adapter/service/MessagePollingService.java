package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.property.AppProperties;

/**
 * Сервис периодического опроса базы данных для обработки исходящих сообщений.
 * Выполняется по расписанию и инициирует обработку сообщений для отправки в Kafka.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessagePollingService {

    private final MessageProcessorService messageProcessorService;
    private final AppProperties appProperties;

    /**
     * Периодически опрашивает базу данных и обрабатывает исходящие сообщения.
     * Интервал опроса настраивается через свойство app.wait-msg-millis
     */
    @Scheduled(fixedRate = 5000L)
    public void pollAndProcessMessages() {
        try {
            log.debug("Polling for outgoing messages with interval: {} ms", appProperties.getWaitMsgMillis());
            messageProcessorService.processOutgoingMessage();
        } catch (Exception e) {
            log.error("Error in message polling: {}", e.getMessage(), e);
        }
    }
}
