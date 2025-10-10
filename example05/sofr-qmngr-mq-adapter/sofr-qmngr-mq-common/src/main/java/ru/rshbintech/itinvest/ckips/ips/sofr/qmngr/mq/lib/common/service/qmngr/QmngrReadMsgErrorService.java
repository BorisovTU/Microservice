package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.qmngr;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.RequestStatus.StatusValue.ERROR;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Clob;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.sql.rowset.serial.SerialClob;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dao.QmngrDao;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrReadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.RequestStatus;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrReadMsgError;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrReadMsgErrorCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.QmngrReadMsgToMqSendingService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.QmngrReadMsgErrorAuditService;

/**
 * Сервис для установки ошибочного статуса обработки сообщения в SOFR QManager.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QmngrReadMsgErrorService {

  private final QmngrDao qmngrDao;
  private final QmngrReadMsgErrorAuditService readMsgErrorAuditService;
  private final JsonFactory jsonFactory;
  private final ObjectMapper objectMapper;
  private final QmngrReadMsgToMqSendingService readMsgToMqSendingService;
  @Value("#{environment.REQUEST_STATUS_TOPIC}")
  private String requestStatusTopic;

  /**
   * Метод установки ошибочного статуса обработки сообщения в SOFR QManager.
   *
   * @param readMsg      контейнер с информацией о сообщении, загруженном из SOFR QManager, для отправки в MQ
   * @param readMsgError внутренняя ошибка адаптера при обработке результата вызова хранимой процедуры
   *                     qmanager_read_msg
   * @param errorMsg     описание ошибки
   */
  public void process(@NonNull QmngrReadMsgDto readMsg,
                      @NonNull QmngrReadMsgError readMsgError,
                      String errorMsg) {
    final QmngrReadMsgErrorCall readMsgErrorCall = new QmngrReadMsgErrorCall(readMsg, readMsgError);
    try {
      announceErrorStatus(readMsg, errorMsg);
      readMsgErrorAuditService.logReadMsgErrorCall(readMsgErrorCall);
      qmngrDao.callReadMsgError(readMsgErrorCall);
    } catch (Exception e) {
      readMsgErrorAuditService.auditReadMsgErrorCallError(readMsgErrorCall, ExceptionUtils.getStackTrace(e));
      return;
    }
    readMsgErrorAuditService.auditReadMsgErrorCallSuccessCompletion(readMsgErrorCall);
  }

  @SneakyThrows
  private void announceErrorStatus(QmngrReadMsgDto readMsgDto, String errorMsg) {
    getReqGuid(readMsgDto.getMessage())
        .map(reqId -> {
          var messageBody = new RequestStatus(ERROR, errorMsg, reqId);
          return requestStatusToDto(messageBody, reqId);
        })
        .ifPresent(this::sendWithWrapper);
  }

  @SneakyThrows
  private void sendWithWrapper(QmngrReadMsgDto dto) {
    Mono<SendResult<String, String>> resultMono = readMsgToMqSendingService.sendAsync(dto);
  }

  @SneakyThrows
  private QmngrReadMsgDto requestStatusToDto(RequestStatus requestStatus, String reqId) {
    String messageJson = objectMapper.writeValueAsString(requestStatus);
    var clob = new SerialClob(messageJson.toCharArray());
    return QmngrReadMsgDto.builder()
        .msgId(UUID.randomUUID()
            .toString())
        .topic(requestStatusTopic)
        .headers(objectMapper.writeValueAsString(Map.of("RequestId", reqId)))
        .message(clob)
        .build();
  }

  /**
   * поиск в теле сообщения guid от запроса
   *
   * @param messageBody тело сообщения из QManager СОФР
   * @return id запроса, который обрабатывает СОФР этим сообщением
   */
  //TODO код, который здесь не нужен!!! Срочно сделать рефакторинг СОФР, чтобы он возвращал это поле в ответе
  @SneakyThrows
  private Optional<String> getReqGuid(Clob messageBody) {
    if (null == messageBody) {
      return Optional.empty();
    }
    try (var jp = jsonFactory.createParser(messageBody.getCharacterStream())) {
      if (jp.nextToken() != JsonToken.START_OBJECT) {
        return Optional.empty();
      }
      while (jp.nextToken() != null) {
        var currentName = jp.currentName();
        jp.nextToken();
        var value = jp.getValueAsString();
        if ("GUIDReq".equals(currentName)) {
          return Optional.ofNullable(value);
        }
      }
    } catch (JsonParseException jpe) {
      return Optional.empty();
    }
    return Optional.empty();
  }

}
