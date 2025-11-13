package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.dao.MessageDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.exception.MessageProcessingException;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.KafkaMessage;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.OutgoingMessageResult;

/**
 * Основной сервис обработки сообщений.
 * Координирует процессы сохранения входящих сообщений и отправки исходящих сообщений.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProcessorService {

    private final MessageDao messageDao;
    private final KafkaProducerService kafkaProducerService;

    /**
     * Обрабатывает входящее сообщение из Kafka и сохраняет его в базу данных.
     *
     * @param message входящее сообщение для обработки и сохранения
     * @throws MessageProcessingException если произошла ошибка при обработке сообщения
     */
    @Transactional
    public void processIncomingMessage(KafkaMessage message) {
        try {
            var result = messageDao.callLoadMsg(message);
            if (!result.isSuccess()) {
                throw new MessageProcessingException("Stored procedure error: " + result.getErrorDescription());
            }
            log.debug("Successfully processed incoming message: {}", message.getMessageId());
        } catch (Exception e) {
            log.error("Error processing incoming message {}: {}", message.getMessageId(), e.getMessage(), e);
            throw new MessageProcessingException("Failed to process incoming message", e);
        }
    }

    /**
     * Обрабатывает исходящие сообщения, читая их из базы данных и отправляя в Kafka.
     * Выполняется периодически по расписанию.
     *
     * @throws MessageProcessingException если произошла ошибка при обработке исходящих сообщений
     */
    @Transactional
    public void processOutgoingMessage() {
        try {
            OutgoingMessageResult result = messageDao.callReadMsg();

            if (result.isNoMessages()) {
                log.debug("No messages available in queue");
                return;
            }

            if (!result.isSuccess()) {
                log.warn("Error reading message: code {}, description {}",
                        result.getErrorCode(), result.getErrorDescription());
                return;
            }

            if (result.hasMessage() && kafkaProducerService.supportsTopic(result.getMessage().getTopic())) {
                kafkaProducerService.sendMessage(result.getMessage());
                log.debug("Successfully sent outgoing message: {}", result.getMessage().getMessageId());
            }
        } catch (Exception e) {
            log.error("Error processing outgoing message: {}", e.getMessage(),e);
            throw new MessageProcessingException("Failed to process outgoing message", e);
        }
    }
}
