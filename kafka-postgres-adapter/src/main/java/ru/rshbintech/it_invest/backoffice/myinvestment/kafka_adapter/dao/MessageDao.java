package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.dao;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.CallableStatementCreator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.KafkaMessage;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.OutgoingMessageResult;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.StoredProcedureResult;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.time.LocalDateTime;

/**
 * Data Access Object для работы с хранимыми процедурами базы данных.
 * Выполняет вызовы хранимых процедур для обработки сообщений.
 */
@Slf4j
@Repository
public class MessageDao {

    public static final String CALL = "{call ";
    @Autowired
    @Qualifier("second")
    private JdbcTemplate secondJdbcTemplate;

    @Value("${app.postgres.schema}")
    private String schemaName;

    /**
     * Вызывает хранимую процедуру для сохранения входящего сообщения в базу данных.
     *
     * @param message сообщение для сохранения
     * @return результат выполнения хранимой процедуры
     */
    public StoredProcedureResult callLoadMsg(KafkaMessage message) {
        String sql = CALL + schemaName + ".qmanager_load_msg(?, ?, ?, ?, ?, ?)}";

        CallableStatementCreator creator = (Connection connection) -> {
            CallableStatement cs = connection.prepareCall(sql);
            cs.setString(1, message.getTopic());
            cs.setString(2, message.getMessageId());
            cs.setTimestamp(3, java.sql.Timestamp.valueOf(message.getTimestamp()));
//            cs.setString(4, message.getHeaders());
            cs.setString(4, message.getPayload());
            cs.registerOutParameter(5, Types.INTEGER);
            cs.registerOutParameter(6, Types.VARCHAR);
            return cs;
        };

        log.info("parameters: {}, {}, {}, {}", message.getTopic(), message.getMessageId(), java.sql.Timestamp.valueOf(message.getTimestamp()), message.getPayload());
        CallableStatementCallback<StoredProcedureResult> callback = (CallableStatement cs) -> {
            cs.execute();
            Integer errorCode = cs.getInt(5);
            String errorDesc = cs.getString(6);
            return new StoredProcedureResult(errorCode, errorDesc);
        };

        return secondJdbcTemplate.execute(creator, callback);
    }

    /**
     * Вызывает хранимую процедуру для чтения следующего исходящего сообщения из базы данных.
     *
     * @return результат чтения сообщения, содержащий сообщение и метаданные операции
     */
    public OutgoingMessageResult callReadMsg() {
        String sql = CALL + schemaName + ".qmanager_read_msg(?, ?, ?, ?, ?, ?, ?, ?)}";
        final int WAIT_MSG_SECONDS = 10;

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
                        .build();
            }

            return new OutgoingMessageResult(kafkaMessage, errorCode, errorDesc);
        };

        return secondJdbcTemplate.execute(creator, callback);
    }

    /**
     * Вызывает хранимую процедуру для сохранения информации об ошибке обработки сообщения.
     *
     * @param topic            топик, в котором произошла ошибка
     * @param messageId        идентификатор сообщения, при обработке которого произошла ошибка
     * @param errorCode        код ошибки
     * @param errorDescription описание ошибки
     */
    public void callReadMsgError(String topic, String messageId, int errorCode, String errorDescription) {
        String sql = CALL + schemaName + ".qmanager_read_msg_error(?, ?, ?, ?)}";
        secondJdbcTemplate.update(sql, topic, messageId, errorCode, errorDescription);
    }
}
