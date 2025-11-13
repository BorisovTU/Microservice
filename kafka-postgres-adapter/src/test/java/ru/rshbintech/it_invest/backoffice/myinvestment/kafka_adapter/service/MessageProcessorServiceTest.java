package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.dao.MessageDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.KafkaMessage;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.OutgoingMessageResult;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.StoredProcedureResult;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.exception.MessageProcessingException;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Юнит-тесты для {@link MessageProcessorService}
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Тестирование сервиса обработки сообщений")
class MessageProcessorServiceTest {

    @Mock
    private MessageDao messageDao;

    @Mock
    private KafkaProducerService kafkaProducerService;

    private MessageProcessorService messageProcessorService;

    private KafkaMessage testMessage;

    @BeforeEach
    void setUp() {
        messageProcessorService = new MessageProcessorService(messageDao, kafkaProducerService);

        testMessage = KafkaMessage.builder()
                .topic("test-topic")
                .messageId("test-message-id")
                .timestamp(LocalDateTime.now())
                .headers("{}")
                .payload("test-payload")
                .build();
    }

    @Test
    @DisplayName("Успешная обработка входящего сообщения")
    void processIncomingMessage_Success() {
        // Given
        StoredProcedureResult successResult = new StoredProcedureResult(0, "Success");
        when(messageDao.callLoadMsg(any(KafkaMessage.class))).thenReturn(successResult);

        // When
        assertDoesNotThrow(() -> messageProcessorService.processIncomingMessage(testMessage));

        // Then
        verify(messageDao, times(1)).callLoadMsg(testMessage);
    }

    @Test
    @DisplayName("Обработка входящего сообщения с ошибкой хранимой процедуры")
    void processIncomingMessage_StoredProcedureError() {
        // Given
        StoredProcedureResult errorResult = new StoredProcedureResult(1, "Database error");
        when(messageDao.callLoadMsg(any(KafkaMessage.class))).thenReturn(errorResult);

        // When & Then
        MessageProcessingException exception = assertThrows(
                MessageProcessingException.class,
                () -> messageProcessorService.processIncomingMessage(testMessage)
        );

        verify(messageDao, times(1)).callLoadMsg(testMessage);
    }

    @Test
    @DisplayName("Обработка входящего сообщения с исключением DAO")
    void processIncomingMessage_DaoException() {
        // Given
        when(messageDao.callLoadMsg(any(KafkaMessage.class)))
                .thenThrow(new RuntimeException("Database connection failed"));

        // When & Then
        MessageProcessingException exception = assertThrows(
                MessageProcessingException.class,
                () -> messageProcessorService.processIncomingMessage(testMessage)
        );

        assertTrue(exception.getMessage().contains("Failed to process incoming message"));
        verify(messageDao, times(1)).callLoadMsg(testMessage);
    }

    @Test
    @DisplayName("Успешная обработка исходящего сообщения")
    void processOutgoingMessage_Success() {
        // Given
        OutgoingMessageResult successResult = new OutgoingMessageResult(testMessage, 0, "Success");
        when(messageDao.callReadMsg()).thenReturn(successResult);
        when(kafkaProducerService.supportsTopic("test-topic")).thenReturn(true);

        // When
        assertDoesNotThrow(() -> messageProcessorService.processOutgoingMessage());

        // Then
        verify(messageDao, times(1)).callReadMsg();
        verify(kafkaProducerService, times(1)).sendMessage(testMessage);
    }

    @Test
    @DisplayName("Обработка исходящего сообщения при отсутствии сообщений")
    void processOutgoingMessage_NoMessages() {
        // Given
        OutgoingMessageResult noMessagesResult = new OutgoingMessageResult(null, 25228, "No messages");
        when(messageDao.callReadMsg()).thenReturn(noMessagesResult);

        // When
        assertDoesNotThrow(() -> messageProcessorService.processOutgoingMessage());

        // Then
        verify(messageDao, times(1)).callReadMsg();
        verify(kafkaProducerService, never()).sendMessage(any());
    }

    @Test
    @DisplayName("Обработка исходящего сообщения с неподдерживаемым топиком")
    void processOutgoingMessage_UnsupportedTopic() {
        // Given
        OutgoingMessageResult successResult = new OutgoingMessageResult(testMessage, 0, "Success");
        when(messageDao.callReadMsg()).thenReturn(successResult);
        when(kafkaProducerService.supportsTopic("test-topic")).thenReturn(false);

        // When
        assertDoesNotThrow(() -> messageProcessorService.processOutgoingMessage());

        // Then
        verify(messageDao, times(1)).callReadMsg();
        verify(kafkaProducerService, never()).sendMessage(any());
    }

    @Test
    @DisplayName("Обработка исходящего сообщения с ошибкой чтения")
    void processOutgoingMessage_ReadError() {
        // Given
        OutgoingMessageResult errorResult = new OutgoingMessageResult(null, 500, "Internal error");
        when(messageDao.callReadMsg()).thenReturn(errorResult);

        // When
        assertDoesNotThrow(() -> messageProcessorService.processOutgoingMessage());

        // Then
        verify(messageDao, times(1)).callReadMsg();
        verify(kafkaProducerService, never()).sendMessage(any());
    }
}
