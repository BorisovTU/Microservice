package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.dao;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.CallableStatementCreator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.KafkaMessage;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.OutgoingMessageResult;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.StoredProcedureResult;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service.DynamicDatabaseRoutingService;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.time.LocalDateTime;

@Slf4j
@Repository
@RequiredArgsConstructor
public class DynamicMultiDatabaseMessageDao {

    private final DynamicDatabaseRoutingService databaseRoutingService;

    public StoredProcedureResult saveMessageToDatabase(KafkaMessage message) {
        String databaseName = databaseRoutingService.getTargetDatabaseForTopic(message.getTopic());
        if (databaseName == null) {
            throw new IllegalArgumentException("No database mapping found for topic: " + message.getTopic());
        }

        JdbcTemplate jdbcTemplate = databaseRoutingService.getDatabaseConfig(databaseName).getJdbcTemplate();
        String schema = databaseRoutingService.getSchemaForDatabase(databaseName);

        String sql = "{call " + schema + ".qmanager_load_msg(?, ?, ?, ?, ?, ?)}";

        CallableStatementCreator creator = (Connection connection) -> {
            CallableStatement cs = connection.prepareCall(sql);
            cs.setString(1, message.getTopic());
            cs.setString(2, message.getMessageId());
            cs.setTimestamp(3, java.sql.Timestamp.valueOf(message.getTimestamp()));
            cs.setString(4, message.getPayload());
            cs.registerOutParameter(5, Types.INTEGER);
            cs.registerOutParameter(6, Types.VARCHAR);
            return cs;
        };

        log.debug("Saving message to database {}: topic={}, messageId={}",
                databaseName, message.getTopic(), message.getMessageId());

        CallableStatementCallback<StoredProcedureResult> callback = (CallableStatement cs) -> {
            cs.execute();
            Integer errorCode = cs.getInt(5);
            String errorDesc = cs.getString(6);
            return new StoredProcedureResult(errorCode, errorDesc);
        };

        return jdbcTemplate.execute(creator, callback);
    }

    public OutgoingMessageResult readMessageFromDatabase(String databaseName) {
        JdbcTemplate jdbcTemplate = databaseRoutingService.getDatabaseConfig(databaseName).getJdbcTemplate();
        String schema = databaseRoutingService.getSchemaForDatabase(databaseName);
        final int WAIT_MSG_SECONDS = 10;

        String sql = "{call " + schema + ".qmanager_read_msg(?, ?, ?, ?, ?, ?, ?, ?)}";

        CallableStatementCreator creator = (Connection connection) -> {
            CallableStatement cs = connection.prepareCall(sql);
            cs.setInt(1, WAIT_MSG_SECONDS);
            cs.registerOutParameter(2, Types.VARCHAR);
            cs.registerOutParameter(3, Types.VARCHAR);
            cs.registerOutParameter(4, Types.VARCHAR);
            cs.registerOutParameter(5, Types.VARCHAR);
            cs.registerOutParameter(6, Types.VARCHAR);
            cs.registerOutParameter(7, Types.INTEGER);
            cs.registerOutParameter(8, Types.VARCHAR);
            return cs;
        };

        CallableStatementCallback<OutgoingMessageResult> callback = (CallableStatement cs) -> {
            cs.execute();
            String cluster = cs.getString(2);
            String topic = cs.getString(3);
            String msgId = cs.getString(4);
            String headers = cs.getString(5);
            String message = cs.getString(6);
            Integer errorCode = cs.getInt(7);
            String errorDesc = cs.getString(8);

            KafkaMessage kafkaMessage = null;
            if (errorCode != null && errorCode == 0 && msgId != null) {
                kafkaMessage = KafkaMessage.builder()
                        .cluster(cluster)
                        .topic(topic)
                        .messageId(msgId)
                        .timestamp(LocalDateTime.now())
                        .headers(headers)
                        .payload(message)
                        .sourceDatabase(databaseName)
                        .build();
            }

            return new OutgoingMessageResult(kafkaMessage, errorCode, errorDesc);
        };

        return jdbcTemplate.execute(creator, callback);
    }

    public void saveErrorToDatabase(String databaseName, String topic, String messageId,
                                    int errorCode, String errorDescription) {
        JdbcTemplate jdbcTemplate = databaseRoutingService.getDatabaseConfig(databaseName).getJdbcTemplate();
        String schema = databaseRoutingService.getSchemaForDatabase(databaseName);

        String sql = "{call " + schema + ".qmanager_read_msg_error(?, ?, ?, ?)}";

        try {
            jdbcTemplate.update(sql, topic, messageId, errorCode, errorDescription);
            log.debug("Saved error to database {}: topic={}, messageId={}",
                    databaseName, topic, messageId);
        } catch (Exception e) {
            log.error("Failed to save error to database {}: {}", databaseName, e.getMessage());
        }
    }
}
