package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Юнит-тесты для {@link KafkaMessageListenerService}
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Тестирование сервиса слушателя Kafka сообщений")
class KafkaMessageListenerServiceTest {

    @Mock
    private MessageProcessorService messageProcessorService;

    private KafkaMessageListenerService kafkaMessageListenerService;

    @BeforeEach
    void setUp() {
        kafkaMessageListenerService = new KafkaMessageListenerService(messageProcessorService);
    }

    @Test
    @DisplayName("Успешная обработка сообщения")
    void processMessage_Success() {
        // Given
        String topic = "test-topic";
        String key = "test-key";
        String value = "test-payload";
        int partition = 0;
        long offset = 123L;

        // When
        kafkaMessageListenerService.processMessage(topic, key, value, partition, offset);

        // Then
        verify(messageProcessorService, times(1)).processIncomingMessage(any());
    }

    @Test
    @DisplayName("Обработка сообщения с null ключом")
    void processMessage_NullKey() {
        // Given
        String topic = "test-topic";
        String key = null;
        String value = "test-payload";
        int partition = 0;
        long offset = 123L;

        // When
        kafkaMessageListenerService.processMessage(topic, key, value, partition, offset);

        // Then
        verify(messageProcessorService, times(1)).processIncomingMessage(any());
    }

    @Test
    @DisplayName("Обработка сообщения с исключением при обработке")
    void processMessage_ProcessingException() {
        // Given
        String topic = "test-topic";
        String key = "test-key";
        String value = "test-payload";
        int partition = 0;
        long offset = 123L;

        doThrow(new RuntimeException("Processing failed"))
                .when(messageProcessorService).processIncomingMessage(any());

        // When
        kafkaMessageListenerService.processMessage(topic, key, value, partition, offset);

        // Then
        // Exception should be caught and logged, no exception should be thrown
        verify(messageProcessorService, times(1)).processIncomingMessage(any());
    }
}
