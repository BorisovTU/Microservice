package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dao.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrLoadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrReadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrReadMsgError;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrGetValuesMetricsCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrLoadMsgCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrReadMsgErrorCall;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JdbcQmngrDaoTest {

  public static final QmngrReadMsgError QMNGR_READ_MSG_ERROR = QmngrReadMsgError.UNKNOWN_ERROR;
  @Mock
  private DataSource dataSource;

  @Mock
  private Connection connection;

  @Mock
  private CallableStatement callableStatement;

  @InjectMocks
  private JdbcQmngrDao jdbcQmngrDao;

  @BeforeEach
  void setUp() throws SQLException {
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareCall(anyString())).thenReturn(callableStatement);
  }

  @Test
  void callLoadMsg_ShouldSetAllParametersAndHandleClob() throws SQLException {
    // Arrange
    QmngrLoadMsgCall call = new QmngrLoadMsgCall(buildQmngrLoadMsgDto());
    Clob headersClob = mock(Clob.class);
    Clob messageClob = mock(Clob.class);

    when(connection.createClob()).thenReturn(headersClob, messageClob);

    // Act
    jdbcQmngrDao.callLoadMsg(call);

    // Assert
    verify(callableStatement).setString(1, "test-topic");
    verify(callableStatement).setString(2, "msg-123");
    verify(callableStatement).setTimestamp(3, Timestamp.valueOf(call.getEsbDt()));
    verify(headersClob).setString(1, "test-headers");
    verify(messageClob).setString(1, "test-message");
    verify(callableStatement).setClob(4, headersClob);
    verify(callableStatement).setClob(5, messageClob);
    verify(callableStatement).registerOutParameter(6, Types.INTEGER);
    verify(callableStatement).registerOutParameter(7, Types.VARCHAR);
  }

  @Test
  void callReadMsgError_ShouldSetErrorParameters() throws SQLException {
    // Arrange
    QmngrReadMsgErrorCall call = new QmngrReadMsgErrorCall(
        buildQmngrReadMsgDto(),
        QMNGR_READ_MSG_ERROR
    );

    // Act
    jdbcQmngrDao.callReadMsgError(call);

    // Assert
    verify(callableStatement).setString(1, "error-topic");
    verify(callableStatement).setString(2, "err-456");
    verify(callableStatement).setInt(3, QMNGR_READ_MSG_ERROR.getCode());
    verify(callableStatement).setString(4, QMNGR_READ_MSG_ERROR.getDescription());
  }

  @Test
  void callGetValuesMetrics_ShouldHandleOutParameter() throws SQLException {
    // Arrange
    QmngrGetValuesMetricsCall call = new QmngrGetValuesMetricsCall();
    when(callableStatement.getString(1)).thenReturn("{\"metrics\": 100}");

    // Act
    jdbcQmngrDao.callGetValuesMetrics(call);

    // Assert
    verify(callableStatement).registerOutParameter(1, Types.VARCHAR);
    assertEquals("{\"metrics\": 100}", call.getOutJson());
  }

  @Test
  void callLoadMsg_ShouldHandleSQLException() throws SQLException {
    // Arrange
    when(connection.prepareCall(anyString())).thenThrow(new SQLException("DB error"));
    QmngrLoadMsgCall call = new QmngrLoadMsgCall(buildQmngrLoadMsgDto());

    // Act & Assert
    RuntimeException ex = assertThrows(RuntimeException.class,
        () -> jdbcQmngrDao.callLoadMsg(call));
    assertTrue(ex.getMessage().contains("qmanager_load_msg"));
  }

  // Вспомогательные тестовые DTO
  private QmngrLoadMsgDto buildQmngrLoadMsgDto() {
    return QmngrLoadMsgDto.builder()
        .topic("test-topic")
        .msgId("msg-123")
        .esbDt(LocalDateTime.now())
        .headers("test-headers")
        .message("test-message")
        .build();
  }

  private QmngrReadMsgDto buildQmngrReadMsgDto() {
    return QmngrReadMsgDto.builder()
        .topic("error-topic")
        .msgId("err-456")
        .build();
  }
}