package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.property.AppProperties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Юнит-тесты для {@link MessagePollingService}
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Тестирование сервиса опроса сообщений")
class MessagePollingServiceTest {

    @Mock
    private MessageProcessorService messageProcessorService;

    @Mock
    private AppProperties appProperties;

    private MessagePollingService messagePollingService;

    @BeforeEach
    void setUp() {
        messagePollingService = new MessagePollingService(messageProcessorService, appProperties);
    }

    @Test
    @DisplayName("Успешный опрос и обработка сообщений")
    void pollAndProcessMessages_Success() {
        // Given
        when(appProperties.getWaitMsgMillis()).thenReturn(500L);

        // When
        messagePollingService.pollAndProcessMessages();

        // Then
        verify(messageProcessorService, times(1)).processOutgoingMessage();
        verify(appProperties, times(1)).getWaitMsgMillis();
    }

    @Test
    @DisplayName("Опрос и обработка сообщений с исключением")
    void pollAndProcessMessages_Exception() {
        // Given
        when(appProperties.getWaitMsgMillis()).thenReturn(500L);
        doThrow(new RuntimeException("Processing error"))
                .when(messageProcessorService).processOutgoingMessage();

        // When
        messagePollingService.pollAndProcessMessages();

        // Then
        verify(messageProcessorService, times(1)).processOutgoingMessage();
        verify(appProperties, times(1)).getWaitMsgMillis();
    }

    @Test
    @DisplayName("Опрос с кастомным интервалом")
    void pollAndProcessMessages_CustomInterval() {
        // Given
        when(appProperties.getWaitMsgMillis()).thenReturn(1000L);

        // When
        messagePollingService.pollAndProcessMessages();

        // Then
        verify(messageProcessorService, times(1)).processOutgoingMessage();
        verify(appProperties, times(1)).getWaitMsgMillis();
    }
}
