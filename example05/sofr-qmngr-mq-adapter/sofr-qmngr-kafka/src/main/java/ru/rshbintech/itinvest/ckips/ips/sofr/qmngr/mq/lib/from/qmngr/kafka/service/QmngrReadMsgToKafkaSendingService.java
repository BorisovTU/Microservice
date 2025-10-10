package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.qmngr.kafka.service;

import com.google.common.io.CharStreams;
import java.sql.Clob;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrReadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.exception.QmngrReadMsgSendToMqException;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.QmngrReadMsgToMqSendingService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.qmngr.kafka.handler.JsonStringHeadersHandler;

/**
 * Сервис отправки сообщения из SOFR QManager в Kafka.
 */
@Service
@RequiredArgsConstructor
public class QmngrReadMsgToKafkaSendingService implements QmngrReadMsgToMqSendingService {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final JsonStringHeadersHandler jsonStringHeadersHandler;
  private final PlatformDynamicKafkaTemplateService platformDynamicKafkaProducerService;

  /**
   * ВАЖНО! В данной реализации не рассматривается сценарий, когда топика может и не быть в Kafka (то есть считаем
   * всегда, что он есть). Это связано с: <a href="https://issues.apache.org/jira/browse/KAFKA-3450">KAFKA-3450</a>.
   * На данном этапе Kafka не умеет завершать работу Producer в случае возникновения такой ошибки.
   * Producer продолжает свою работу, пытаясь получить метаданные отсутствующего топика Kafka, при этом
   * он добавляет в лог огромное количество однотипных предупреждающих сообщений. Отключить это никак нельзя.
   * Повышение уровня логирования не решит проблему, а только усугубит, т.к. попытки получить метаданные от Kafka
   * все равно будут выполняться, к тому же добавится проблема того, что некоторые важные логи перестанут писаться.
   * Проблему можно решить, если перед вызовом send, например, получить информацию о топике посредством
   * KafkaAdminClient, однако это решение требует обсуждения с командой IPS, возможно, так делать запрещено (либо
   * API admin клиента Kafka закрыто).
   */
  @Override
  public Mono<SendResult<String, String>> sendAsync(
          @NonNull QmngrReadMsgDto readMsg) throws QmngrReadMsgSendToMqException {
    try {
      ProducerRecord<String, String> producerRecord = createKafkaRecordFrom(readMsg);
      KafkaTemplate<String, String> dynamicTemplate = platformDynamicKafkaProducerService
              .getKafkaTemplateByTopic(readMsg.getTopic());
      KafkaTemplate<String, String> template = Objects.requireNonNullElse(dynamicTemplate, kafkaTemplate);

      return Mono.fromFuture(template.send(producerRecord))
              .onErrorMap(throwable -> new QmngrReadMsgSendToMqException("Error sending Kafka record", throwable));
    } catch (Exception e) {
      throw new QmngrReadMsgSendToMqException("Error preparing Kafka record", e);
    }
  }

  private ProducerRecord<String, String> createKafkaRecordFrom(QmngrReadMsgDto readMsg)
            throws Exception {
    var kafkaHeaders = jsonStringHeadersHandler.toKafkaHeaders(readMsg.getHeaders());

    return new ProducerRecord<>(
                readMsg.getTopic(),
                null,
                null,
                readMsg.getMsgId(),
                messageToString(readMsg.getMessage()),
                kafkaHeaders);
  }

  private String messageToString(Clob message) throws Exception {
    if (null == message) {
      return "";
    }
    return CharStreams.toString(message.getCharacterStream());
  }
}
