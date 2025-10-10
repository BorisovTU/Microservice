package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.qmngr;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.QmngrReadMsgProcessState.CONTINUE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.QmngrReadMsgProcessState.STOP_NORMALLY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.QmngrReadMsgProcessState.STOP_ON_TECHNICAL_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrReadMsgError.UNKNOWN_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrReadMsgError.VALIDATION_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcErrorCode.NO_MESSAGES;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcErrorCode.SUCCESS;

import io.micrometer.core.annotation.Timed;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.util.Objects;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrReadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.mdc.MdcAdapter;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.ValidationResult;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.QmngrReadMsgProcessState;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.exception.QmngrReadMsgTechnicalException;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrReadMsgCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.QmngrReadMsgToMqSendingService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.QmngrReadMsgAuditService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.validation.GenericValidator;

/**
 * Сервис обработки сообщений из SOFR QManager с последующей их отправкой в MQ.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QmngrReadMsgService {
  @Value("${app.oracle.schema}")
  private String schemaName;
  private static final int WAIT_MSG_SECONDS = 10;
  private final DataSource dataSource;
  private final MdcAdapter mdcAdapter;
  private final GenericValidator validator;
  private final QmngrReadMsgAuditService readMsgAuditService;
  private final QmngrReadMsgErrorService readMsgErrorService;
  private final QmngrReadMsgToMqSendingService readMsgToMqSendingService;

  /**
   * Метод выполняет загрузку сообщения из SOFR QManager и отправляет его в MQ.
   * -------------------------
   * Важно понимать, что коммит транзакции означает, что текущее обрабатываемое сообщение (если оно было вычитано),
   * больше не может быть вычитано.
   * Разработка СОФР уверяет, что одно и то же сообщение не может быть вычитано на разных инстансах приложения,
   * так как используется select for update блокировка.
   * -------------------------
   * При обсуждении говорили о том что алгоритм:
   * 1. Сложен для понимания.
   * 2. Подвержен ошибкам.
   * 3. Не оптимален (на вычитку каждого сообщения открывается транзакция, что является явным оверхедом).
   * Но ввиду ограниченности времени другой реализации со стороны разработки СОФР пока не будет.
   * Алгоритм нуждается в переработке в будущем.
   * -------------------------
   * Алгоритм работы:
   * 1. Вызывается хранимая процедура qmanager_read_msg для загрузки сообщения.
   * 2. Если вызов qmanager_read_msg вернул errorCode = null, то это нештатная ситуация (так быть не должно), вызывается
   * процедура qmanager_read_msg_error для записи ошибки QmngrReadMsgError.UNKNOWN_ERROR_CODE в очередь Oracle (в
   * разрезе msgId). Выполняется коммит транзакции. Цикл продолжает работу.
   * 3. Если вызов qmanager_read_msg вернул errorCode = 0, выполняется валидация параметров полученного сообщения.
   * В случае, если валидация завершилась с ошибками, вызывается процедура qmanager_read_msg_error для записи ошибки
   * QmngrReadMsgError.VALIDATION_ERROR в очередь Oracle (в разрезе msgId). Выполняется коммит транзакции.
   * Цикл продолжает работу.
   * 4. Если вызов qmanager_read_msg вернул errorCode = 25228 - это значит, что сообщений в очереди нет.
   * Выполняется коммит транзакции. Цикл завершает работу.
   * 5. Если вызов qmanager_read_msg вернул errorCode отличный от 0 или от 25228 выполняется коммит транзакции. Цикл
   * продолжает работу.
   * 6. Если валидация в п.4. прошла успешно, выполняется отправка сообщения в MQ.
   * 7. Если в процессе вызова процедуры qmanager_read_msg или в процессе отправки сообщения в MQ возникает техническая
   * ошибка, выполняется откат транзакции. Цикл завершает работу.
   * -------------------------
   * ВАЖНО!
   * Стоит понимать, что данный алгоритм может привести к дубликатам в MQ брокере. Например, есть следующие сценарии:
   * 1. Сообщение успешно отправлено в MQ, выполняется успешный коммит транзакции. Здесь проблем не будет. Процесс
   * отправки сообщения в MQ успешно завершен.
   * 2. Сообщение не отправлено в MQ, выполняется откат транзакции. Здесь тоже проблем не будет. Так как сообщение
   * не было отправлено в MQ, то и транзакция будет откатываться. Это значит, что при следующем вызове сообщение
   * будет вычитано из очереди Oracle заново и заново будет обрабатываться.
   * 3. Сообщение успешно отправлено в MQ, но внезапно упал коммит транзакции. Вот тут как раз и может возникать
   * проблема. В этом случае сообщение уже будет в MQ брокере, но так как коммит не прошел, сообщение будет
   * вычитано заново из очереди Oracle и обработано, вследствие чего в MQ появляется дубликат.
   * -------------------------
   * Если рассмотреть проблему со стороны Kafka, то в Kafka есть транзакции, но они относятся к самой Kafka.
   * Протокол XA в Kafka не реализован, а это значит, что сделать распределенную транзакцию с использованием других
   * ресурсов, например, БД, не получится. Можно попробовать синхронизировать транзакции посредством подхода
   * Best effort one-phase commit, но это не дает абсолютно никаких гарантий, так как это все еще разные транзакции
   * под управлением разных транзакционных менеджеров (да, этот подход рабочий и позволяет достаточно хорошо
   * синхронизировать транзакционные менеджеры, но он ничего не гарантирует). Помимо прочего, стоит понимать, что в
   * Kafka в случае коммита или отката транзакции, сообщение все равно попадает в топик, но оно имеет атрибут
   * committed = false. Если из этого топика будет читать не транзакционный потребитель, то он сможет получить
   * это сообщение. Поэтому в данном случае на потребителе должен быть настроен isolation.level = read_committed, что
   * в рамках данной задачи несколько избыточно. По факту вышеописанная проблема будет решена реализацией идемпотентного
   * потребителя, чего по итогу и следует придерживаться.
   */
  @Scheduled(fixedRate = 500)
  public void process() {
    try {
      processSingle();
    } catch (QmngrReadMsgTechnicalException e) {
      log.error("Technical error during scheduled QManager read: {}", e.getMessage(), e);
    }
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = QmngrReadMsgTechnicalException.class)
  @Timed(value = "rshbintech.sofr.qmngr.mq.read.msg.time", description = "QManager read msg time", histogram = true)
  public QmngrReadMsgProcessState processSingle() throws QmngrReadMsgTechnicalException {
    final QmngrReadMsgCall readMsgCall = new QmngrReadMsgCall();
    QmngrReadMsgProcessState processState = CONTINUE;
    final String sql = String.format(
            "{call %s.it_integration.qmanager_read_msg(?, ?, ?, ?, ?, ?, ?)}",
            schemaName
    );
    try (Connection connection = dataSource.getConnection();
         CallableStatement callableStatement = connection.prepareCall(sql)) {
      callableStatement.setInt(1, WAIT_MSG_SECONDS);
      callableStatement.registerOutParameter(2, Types.VARCHAR); // Kafka Topic
      callableStatement.registerOutParameter(3, Types.VARCHAR); // Msg ID
      callableStatement.registerOutParameter(4, Types.CLOB);    // Headers
      callableStatement.registerOutParameter(5, Types.CLOB);    // Message Body
      callableStatement.registerOutParameter(6, Types.INTEGER); // Error Code
      callableStatement.registerOutParameter(7, Types.VARCHAR); // Error Description
      callableStatement.execute();
      readMsgCall.putOutParam(QmngrStoredProcParam.TOPIC, callableStatement.getString(2));
      readMsgCall.putOutParam(QmngrStoredProcParam.MSG_ID, callableStatement.getString(3));
      readMsgCall.putOutParam(QmngrStoredProcParam.HEADERS, callableStatement.getString(4));
      readMsgCall.putOutParam(QmngrStoredProcParam.MESSAGE, callableStatement.getClob(5));
      readMsgCall.putOutParam(QmngrStoredProcParam.ERROR_CODE, callableStatement.getInt(6));
      readMsgCall.putOutParam(QmngrStoredProcParam.ERROR_DESC, callableStatement.getString(7));
    } catch (Exception e) {
      readMsgAuditService.auditReadMsgCallError(readMsgCall, ExceptionUtils.getStackTrace(e));
      return QmngrReadMsgProcessState.STOP_ON_TECHNICAL_ERROR;
    }
    try {
      final Integer errorCode = readMsgCall.getErrorCode();
      if (errorCode == null) {
        readMsgAuditService.auditReadMsgCallUnknownStateCompletion(readMsgCall);
        readMsgErrorService.process(
                createReadMsgFrom(readMsgCall),
                UNKNOWN_ERROR,
                UNKNOWN_ERROR.getDescription()
        );
      } else if (Objects.equals(errorCode, SUCCESS.getCode())) {
        processState = processQmngrReadMsg(createReadMsgFrom(readMsgCall), processState);
      } else if (Objects.equals(errorCode, NO_MESSAGES.getCode())) {
        readMsgAuditService.logReadMsgCallNoMessagesStateCompletion(readMsgCall);
        processState = STOP_NORMALLY;
      } else {
        readMsgAuditService.auditReadMsgCallErrorCompletion(readMsgCall);
      }
    } catch (Exception e) {
      readMsgAuditService.auditReadMsgCallError(readMsgCall, ExceptionUtils.getStackTrace(e));
      processState = STOP_ON_TECHNICAL_ERROR;
    }
    return processState;
  }

  private QmngrReadMsgDto createReadMsgFrom(@NonNull QmngrReadMsgCall readMsgCall) {
    return QmngrReadMsgDto.builder()
            .topic(readMsgCall.getTopic())
            .msgId(readMsgCall.getMsgId())
            .headers(readMsgCall.getHeaders())
            .message(readMsgCall.getMessage())
            .build();
  }

  private QmngrReadMsgProcessState processQmngrReadMsg(
          @NonNull QmngrReadMsgDto readMsg,
          @NonNull QmngrReadMsgProcessState processState
  ) {
    try {
      mdcAdapter.putCorrelationId(readMsg.getMsgId());
      final ValidationResult validationResult = validator.validate(readMsg);
      if (validationResult.isValid()) {
        readMsgAuditService.auditReadMsgReceived(readMsg.getMsgId());

        Mono<SendResult<String, String>> mono = readMsgToMqSendingService.sendAsync(readMsg);
        mono.subscribe(result -> {
          log.debug("Successfully sent message to Kafka: {}", result.getRecordMetadata());
          readMsgAuditService.auditReadMsgToMqSendingSuccess(readMsg);
        }, throwable -> {
          log.error("Error sending message to Kafka: {}", throwable.getMessage(), throwable);
          readMsgAuditService.auditReadMsgToMqSendingError(readMsg, ExceptionUtils.getStackTrace(throwable));
        });
      } else {
        readMsgAuditService.auditReadMsgValidationError(
                readMsg.getMsgId(),
                readMsg.getTopic(),
                validationResult.getErrorMsg()
        );
        readMsgErrorService.process(readMsg, VALIDATION_ERROR, validationResult.getErrorMsg());
      }
    } catch (Exception e) {
      readMsgAuditService.auditReadMsgProcessingError(readMsg, ExceptionUtils.getStackTrace(e));
      return STOP_ON_TECHNICAL_ERROR;
    } finally {
      mdcAdapter.clear();
    }
    return processState;
  }
}