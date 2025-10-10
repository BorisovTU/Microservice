package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dao.impl;

import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dao.QmngrDao;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrGetValuesMetricsCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrLoadMsgCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrReadMsgErrorCall;

@Component
public class JdbcQmngrDao implements QmngrDao {
  private final DataSource dataSource;
  @Value("${app.oracle.schema}")
  private String schemaName;

  public JdbcQmngrDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void callLoadMsg(QmngrLoadMsgCall loadMsgCall) {
    final String sql = String.format("{call %s.it_integration.qmanager_load_msg(?, ?, ?, ?, ?, ?, ?)}", schemaName);
    try (Connection conn = dataSource.getConnection();
         CallableStatement cs = conn.prepareCall(sql)) {

      // Создание Clob из строки для headers
      Clob headersClob = conn.createClob();
      headersClob.setString(1, loadMsgCall.getHeaders());

      // Создание Clob из строки для message (если нужно)
      Clob messageClob = conn.createClob();
      messageClob.setString(1, loadMsgCall.getMessage());

      // Установка параметров
      cs.setString(1, loadMsgCall.getTopic());
      cs.setString(2, loadMsgCall.getMsgId());
      cs.setTimestamp(3, Timestamp.valueOf(loadMsgCall.getEsbDt()));
      cs.setClob(4, headersClob);
      cs.setClob(5, messageClob);

      // Регистрация OUT-параметров
      cs.registerOutParameter(6, Types.INTEGER);
      cs.registerOutParameter(7, Types.VARCHAR);

      cs.execute();

      // Заполнение OUT-параметров
      loadMsgCall.setErrorCode(cs.getInt(6));
      loadMsgCall.setErrorDesc(cs.getString(7));

    } catch (SQLException e) {
      throw new RuntimeException("Error calling qmanager_load_msg", e);
    }
  }

  @Override
  public void callReadMsgError(QmngrReadMsgErrorCall readMsgErrorCall) {
    final String sql = String.format("{call %s.it_integration.qmanager_read_msg_error(?, ?, ?, ?)}", schemaName);
    try (Connection conn = dataSource.getConnection();
         CallableStatement cs = conn.prepareCall(sql)) {

      cs.setString(1, readMsgErrorCall.getTopic());
      cs.setString(2, readMsgErrorCall.getMsgId());
      cs.setInt(3, readMsgErrorCall.getErrorCode());
      cs.setString(4, readMsgErrorCall.getErrorDesc());

      cs.execute();

    } catch (SQLException e) {
      throw new RuntimeException("Error calling qmanager_read_msg_error", e);
    }
  }

  @Override
  public void callGetValuesMetrics(QmngrGetValuesMetricsCall getValuesMetricsCall) {
    final String call = String.format("{call %s.rsb_broker_monitoring.getValuesMetrics(?)}", schemaName);
    try (Connection conn = dataSource.getConnection();
         CallableStatement cs = conn.prepareCall(call)) {

      cs.registerOutParameter(1, Types.VARCHAR);

      cs.execute();

      getValuesMetricsCall.setOutJson(cs.getString(1));

    } catch (SQLException e) {
      throw new RuntimeException("Error calling getValuesMetrics", e);
    }
  }
}