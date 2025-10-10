package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.exception;

/**
 * Исключение для перехвата ошибки отправки сообщения из SOFR QManager в MQ.
 */
public class QmngrReadMsgSendToMqException extends Exception {

  public QmngrReadMsgSendToMqException(String message) {
    super(message);
  }

  public QmngrReadMsgSendToMqException(String message, Throwable cause) {
    super(message, cause);
  }

  public QmngrReadMsgSendToMqException(Throwable cause) {
    super(cause);
  }
}
