package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service;

import org.springframework.kafka.support.SendResult;
import org.springframework.lang.NonNull;
import reactor.core.publisher.Mono;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrReadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.exception.QmngrReadMsgSendToMqException;

/**
 * Общий интерфейс для реализации отправителей сообщений из SOFR QManager в MQ.
 */
public interface QmngrReadMsgToMqSendingService {

  /**
   * Метод производит отправку сообщения из SOFR QManager в MQ.
   *
   * @param readMsg - контейнер с информацией о сообщении из SOFR QManager для отправки в MQ
   * @throws QmngrReadMsgSendToMqException ошибка отправки сообщения из SOFR QManager в Kafka (все ошибки, которые
   *                                       могут возникнуть в процессе отправки должны быть транслированы в это
   *                                       исключение, т.к. оно пишется в лог и отправляется в аудит в вызывающем коде)
   */
  Mono<SendResult<String, String>> sendAsync(@NonNull QmngrReadMsgDto readMsg) throws QmngrReadMsgSendToMqException;

}
