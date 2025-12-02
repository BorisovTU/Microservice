package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.dao;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.CallableStatementCreator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.CallableStatementCallback;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.KafkaMessage;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.OutgoingMessageResult;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.StoredProcedureResult;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service.DynamicDatabaseRoutingService;

import java.sql.*;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Тесты DAO для работы с динамическими БД")
class DynamicMultiDatabaseMessageDaoTest {

    @Mock
    private DynamicDatabaseRoutingService routingService;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @InjectMocks
    private DynamicMultiDatabaseMessageDao messageDao;

    @Captor
    private ArgumentCaptor<CallableStatementCreator> creatorCaptor;

    @Captor
    private ArgumentCaptor<CallableStatementCallback<StoredProcedureResult>> callbackCaptor;

    private KafkaMessage testKafkaMessage;
    private static final String TEST_DATABASE = "primary";
    private static final String TEST_SCHEMA = "test_schema";

    @BeforeEach
    void setUp() {
        testKafkaMessage = KafkaMessage.builder()
                .topic("test.topic")
                .messageId("test-message-123")
                .timestamp(LocalDateTime.now())
                .headers("{}")
                .payload("{\"test\": \"data\"}")
                .build();
    }

    @Test
    @DisplayName("КОГДА сообщение сохраняется в БД успешно, ТО должен вернуться успешный результат")
    void saveMessageToDatabase_Success_ShouldReturnSuccessResult() throws SQLException {
        // ДАНО: Настроены моки для успешного сохранения сообщения
        when(routingService.getTargetDatabaseForTopic(anyString())).thenReturn(TEST_DATABASE);
        when(routingService.getDatabaseConfig(TEST_DATABASE)).thenReturn(mock(ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.DatabaseConfig.class));
        when(routingService.getDatabaseConfig(TEST_DATABASE).getJdbcTemplate()).thenReturn(jdbcTemplate);
        when(routingService.getSchemaForDatabase(TEST_DATABASE)).thenReturn(TEST_SCHEMA);

        CallableStatement callableStatement = mock(CallableStatement.class);
        when(callableStatement.getInt(5)).thenReturn(0);
        when(callableStatement.getString(6)).thenReturn("OK");

        // КОГДА: Вызывается метод сохранения сообщения
        when(jdbcTemplate.execute(any(CallableStatementCreator.class), any(CallableStatementCallback.class)))
                .thenAnswer(invocation -> {
                    CallableStatementCallback<StoredProcedureResult> callback = invocation.getArgument(1);
                    return callback.doInCallableStatement(callableStatement);
                });

        StoredProcedureResult result = messageDao.saveMessageToDatabase(testKafkaMessage);

        // ТО: Результат должен быть успешным и содержать корректные данные
        assertNotNull(result);
        assertTrue(result.isSuccess());
        assertEquals(0, result.getErrorCode());
        assertEquals("OK", result.getErrorDescription());

        verify(routingService).getTargetDatabaseForTopic("test.topic");
        verify(jdbcTemplate).execute(any(CallableStatementCreator.class), any(CallableStatementCallback.class));
    }

    @Test
    @DisplayName("КОГДА при сохранении сообщения возникает ошибка БД, ТО должен вернуться результат с ошибкой")
    void saveMessageToDatabase_DatabaseError_ShouldReturnErrorResult() throws SQLException {
        // ДАНО: Настроены моки для возврата ошибки БД
        when(routingService.getTargetDatabaseForTopic(anyString())).thenReturn(TEST_DATABASE);
        when(routingService.getDatabaseConfig(TEST_DATABASE)).thenReturn(mock(ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.DatabaseConfig.class));
        when(routingService.getDatabaseConfig(TEST_DATABASE).getJdbcTemplate()).thenReturn(jdbcTemplate);
        when(routingService.getSchemaForDatabase(TEST_DATABASE)).thenReturn(TEST_SCHEMA);

        CallableStatement callableStatement = mock(CallableStatement.class);
        when(callableStatement.getInt(5)).thenReturn(-1);
        when(callableStatement.getString(6)).thenReturn("Database error");

        // КОГДА: Вызывается метод сохранения сообщения
        when(jdbcTemplate.execute(any(CallableStatementCreator.class), any(CallableStatementCallback.class)))
                .thenAnswer(invocation -> {
                    CallableStatementCallback<StoredProcedureResult> callback = invocation.getArgument(1);
                    return callback.doInCallableStatement(callableStatement);
                });

        StoredProcedureResult result = messageDao.saveMessageToDatabase(testKafkaMessage);

        // ТО: Результат должен содержать информацию об ошибке
        assertNotNull(result);
        assertFalse(result.isSuccess());
        assertEquals(-1, result.getErrorCode());
        assertEquals("Database error", result.getErrorDescription());
    }

    @Test
    @DisplayName("КОГДА в БД есть сообщение для отправки, ТО должен вернуться результат с сообщением")
    void readMessageFromDatabase_WithValidMessage_ShouldReturnMessage() throws SQLException {
        // ДАНО: Настроены моки для возврата валидного сообщения
        when(routingService.getDatabaseConfig(TEST_DATABASE)).thenReturn(mock(ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.DatabaseConfig.class));
        when(routingService.getDatabaseConfig(TEST_DATABASE).getJdbcTemplate()).thenReturn(jdbcTemplate);
        when(routingService.getSchemaForDatabase(TEST_DATABASE)).thenReturn(TEST_SCHEMA);

        CallableStatement callableStatement = mock(CallableStatement.class);
        when(callableStatement.getString(2)).thenReturn("MAKS");
        when(callableStatement.getString(3)).thenReturn("test.topic");
        when(callableStatement.getString(4)).thenReturn("test-message-123");
        when(callableStatement.getString(5)).thenReturn("{}");
        when(callableStatement.getString(6)).thenReturn("{\"data\": \"test\"}");
        when(callableStatement.getInt(7)).thenReturn(0);
        when(callableStatement.getString(8)).thenReturn("OK");

        // КОГДА: Вызывается метод чтения сообщения из БД
        when(jdbcTemplate.execute(any(CallableStatementCreator.class), any(CallableStatementCallback.class)))
                .thenAnswer(invocation -> {
                    CallableStatementCallback<OutgoingMessageResult> callback = invocation.getArgument(1);
                    return callback.doInCallableStatement(callableStatement);
                });

        OutgoingMessageResult result = messageDao.readMessageFromDatabase(TEST_DATABASE);

        // ТО: Результат должен содержать корректное сообщение
        assertNotNull(result);
        assertTrue(result.isSuccess());
        assertTrue(result.hasMessage());
        assertEquals("test.topic", result.getMessage().getTopic());
        assertEquals("test-message-123", result.getMessage().getMessageId());
        assertEquals("MAKS", result.getMessage().getCluster());
    }

    @Test
    @DisplayName("КОГДА сохраняется ошибка в БД, ТО должен выполниться SQL update без исключений")
    void saveErrorToDatabase_ValidParameters_ShouldExecuteUpdate() {
        // ДАНО: Настроены моки для работы с БД
        when(routingService.getDatabaseConfig(TEST_DATABASE)).thenReturn(mock(ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.DatabaseConfig.class));
        when(routingService.getDatabaseConfig(TEST_DATABASE).getJdbcTemplate()).thenReturn(jdbcTemplate);
        when(routingService.getSchemaForDatabase(TEST_DATABASE)).thenReturn(TEST_SCHEMA);

        // КОГДА: Вызывается метод сохранения ошибки
        messageDao.saveErrorToDatabase(TEST_DATABASE, "test.topic", "test-message-123", 500, "Test error");

        // ТО: Должен выполниться SQL запрос с корректными параметрами
        verify(jdbcTemplate).update(anyString(), eq("test.topic"), eq("test-message-123"), eq(500), eq("Test error"));
    }

    @Test
    @DisplayName("КОГДА для топика не найдена целевая БД, ТО должно быть выброшено исключение")
    void saveMessageToDatabase_DatabaseNotFound_ShouldThrowException() {
        // ДАНО: Маршрутизация не может найти БД для топика
        when(routingService.getTargetDatabaseForTopic(anyString())).thenReturn(null);

        // КОГДА & ТО: При вызове метода должно быть выброшено исключение
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> messageDao.saveMessageToDatabase(testKafkaMessage));

        assertEquals("No database mapping found for topic: test.topic", exception.getMessage());
    }
}
