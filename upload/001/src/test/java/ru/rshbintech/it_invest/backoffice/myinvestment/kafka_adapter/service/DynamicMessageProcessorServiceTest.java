package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.dao.DynamicMultiDatabaseMessageDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.KafkaMessage;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.OutgoingMessageResult;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.StoredProcedureResult;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Тесты сервиса обработки сообщений")
class DynamicMessageProcessorServiceTest {

    @Mock
    private DynamicMultiDatabaseMessageDao messageDao;

    @Mock
    private CloudStreamProducerService producerService;

    @Mock
    private DynamicDatabaseRoutingService routingService;

    @InjectMocks
    private DynamicMessageProcessorService messageProcessorService;

    private Message<String> testMessage;
    private KafkaMessage testKafkaMessage;

    @BeforeEach
    void setUp() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("kafka_receivedTopic", "test.topic");
        headers.put("kafka_receivedMessageKey", "test-key-123");

        testMessage = new Message<>() {
            @Override
            public String getPayload() {
                return "{\"test\": \"data\"}";
            }

            @Override
            public MessageHeaders getHeaders() {
                return new MessageHeaders(headers);
            }
        };

        testKafkaMessage = KafkaMessage.builder()
                .topic("test.topic")
                .messageId("test-key-123")
                .timestamp(LocalDateTime.now())
                .headers("{}")
                .payload("{\"test\": \"data\"}")
                .build();
    }

    @Test
    @DisplayName("КОГДА обрабатывается валидное входящее сообщение, ТО оно должно быть успешно сохранено в БД")
    void messageProcessor_ValidMessage_ShouldSaveToDatabase() {
        // ДАНО: Настроены моки для успешного сохранения в БД
        StoredProcedureResult successResult = new StoredProcedureResult(0, "OK");
        when(messageDao.saveMessageToDatabase(any(KafkaMessage.class))).thenReturn(successResult);

        Consumer<Message<String>> processor = messageProcessorService.messageProcessor();

        // КОГДА: Обрабатывается тестовое сообщение
        processor.accept(testMessage);

        // ТО: Должен быть вызван метод сохранения в БД
        verify(messageDao).saveMessageToDatabase(any(KafkaMessage.class));
    }

    @Test
    @DisplayName("КОГДА обрабатывается сообщение без ключа, ТО должен сгенерироваться UUID в качестве messageId")
    void messageProcessor_MessageWithoutKey_ShouldGenerateMessageId() {
        // ДАНО: Сообщение без ключа Kafka
        Map<String, Object> headers = new HashMap<>();
        headers.put("kafka_receivedTopic", "test.topic");
        // Отсутствует kafka_receivedMessageKey

        Message<String> messageWithoutKey = new Message<>() {
            @Override
            public String getPayload() {
                return "{\"test\": \"data\"}";
            }

            @Override
            public MessageHeaders getHeaders() {
                return new MessageHeaders(headers);
            }
        };

        StoredProcedureResult successResult = new StoredProcedureResult(0, "OK");
        when(messageDao.saveMessageToDatabase(any(KafkaMessage.class))).thenReturn(successResult);

        Consumer<Message<String>> processor = messageProcessorService.messageProcessor();

        // КОГДА: Обрабатывается сообщение без ключа
        processor.accept(messageWithoutKey);

        // ТО: Должен быть сгенерирован новый messageId (не равный тестовому)
        verify(messageDao).saveMessageToDatabase(argThat(kafkaMessage ->
                kafkaMessage.getMessageId() != null &&
                        !kafkaMessage.getMessageId().equals("test-key-123")
        ));
    }

    @Test
    @DisplayName("КОГДА сохранение в БД завершается ошибкой, ТО ошибка должна быть залогирована без исключения")
    void messageProcessor_DatabaseSaveFails_ShouldLogError() {
        // ДАНО: Настроены моки для возврата ошибки БД
        StoredProcedureResult errorResult = new StoredProcedureResult(-1, "Database error");
        when(messageDao.saveMessageToDatabase(any(KafkaMessage.class))).thenReturn(errorResult);

        Consumer<Message<String>> processor = messageProcessorService.messageProcessor();

        // КОГДА & ТО: Обработка не должна бросать исключение
        assertDoesNotThrow(() -> processor.accept(testMessage));

        verify(messageDao).saveMessageToDatabase(any(KafkaMessage.class));
    }

    @Test
    @DisplayName("КОГДА опрашиваются БД и есть валидное исходящее сообщение, ТО оно должно быть отправлено в Kafka")
    void pollAllDatabasesForOutgoingMessages_ValidMessage_ShouldSendToKafka() {
        // ДАНО: Настроены моки для возврата валидного сообщения
        when(routingService.getPollingOrder()).thenReturn(java.util.Arrays.asList("primary"));
        when(routingService.isDatabaseAvailable("primary")).thenReturn(true);

        OutgoingMessageResult successResult = new OutgoingMessageResult(
                testKafkaMessage, 0, "OK"
        );
        when(messageDao.readMessageFromDatabase("primary")).thenReturn(successResult);
        when(producerService.supportsTopic("test.topic")).thenReturn(true);

        // КОГДА: Вызывается метод опроса БД
        messageProcessorService.pollAllDatabasesForOutgoingMessages();

        // ТО: Сообщение должно быть отправлено в Kafka
        verify(producerService).sendMessage(testKafkaMessage);
        verify(messageDao, never()).saveErrorToDatabase(anyString(), anyString(), anyString(), anyInt(), anyString());
    }

    @Test
    @DisplayName("КОГДА опрашиваются БД и топик не поддерживается, ТО ошибка должна быть сохранена в БД")
    void pollAllDatabasesForOutgoingMessages_UnsupportedTopic_ShouldSaveError() {
        // ДАНО: Топик сообщения не поддерживается
        when(routingService.getPollingOrder()).thenReturn(java.util.Arrays.asList("primary"));
        when(routingService.isDatabaseAvailable("primary")).thenReturn(true);

        OutgoingMessageResult successResult = new OutgoingMessageResult(
                testKafkaMessage, 0, "OK"
        );
        when(messageDao.readMessageFromDatabase("primary")).thenReturn(successResult);
        when(producerService.supportsTopic("test.topic")).thenReturn(false);

        // КОГДА: Вызывается метод опроса БД
        messageProcessorService.pollAllDatabasesForOutgoingMessages();

        // ТО: Ошибка должна быть сохранена в БД, сообщение не отправлено
        verify(producerService, never()).sendMessage(any());
        verify(messageDao).saveErrorToDatabase("primary", "test.topic", "test-key-123", 400, "Topic not supported");
    }

    @Test
    @DisplayName("КОГДА в БД нет сообщений для отправки, ТО обработка должна быть пропущена")
    void pollAllDatabasesForOutgoingMessages_NoMessages_ShouldSkip() {
        // ДАНО: В БД нет сообщений
        when(routingService.getPollingOrder()).thenReturn(java.util.Arrays.asList("primary"));
        when(routingService.isDatabaseAvailable("primary")).thenReturn(true);

        OutgoingMessageResult noMessagesResult = new OutgoingMessageResult(
                null, 25228, "No messages available in queue"
        );
        when(messageDao.readMessageFromDatabase("primary")).thenReturn(noMessagesResult);

        // КОГДА: Вызывается метод опроса БД
        messageProcessorService.pollAllDatabasesForOutgoingMessages();

        // ТО: Не должно быть отправки сообщений или сохранения ошибок
        verify(producerService, never()).sendMessage(any());
        verify(messageDao, never()).saveErrorToDatabase(anyString(), anyString(), anyString(), anyInt(), anyString());
    }

    @Test
    @DisplayName("КОГДА БД недоступна, ТО опрос этой БД должен быть пропущен")
    void pollAllDatabasesForOutgoingMessages_DatabaseUnavailable_ShouldSkip() {
        // ДАНО: БД недоступна
        when(routingService.getPollingOrder()).thenReturn(java.util.Arrays.asList("primary"));
        when(routingService.isDatabaseAvailable("primary")).thenReturn(false);

        // КОГДА: Вызывается метод опроса БД
        messageProcessorService.pollAllDatabasesForOutgoingMessages();

        // ТО: Не должно быть попыток чтения из недоступной БД
        verify(messageDao, never()).readMessageFromDatabase("primary");
        verify(producerService, never()).sendMessage(any());
    }

    @Test
    @DisplayName("КОГДА при обработке отдельной БД возникает ошибка, ТО ошибка должна быть залогирована")
    void processSingleDatabase_WithError_ShouldLogError() {
        // ДАНО: Настроены моки для возврата ошибки
        when(messageDao.readMessageFromDatabase("primary")).thenThrow(new RuntimeException("Database error"));

        // КОГДА & ТО: Обработка не должна бросать исключение
        assertDoesNotThrow(() -> messageProcessorService.processSingleDatabase("primary"));
    }
}
