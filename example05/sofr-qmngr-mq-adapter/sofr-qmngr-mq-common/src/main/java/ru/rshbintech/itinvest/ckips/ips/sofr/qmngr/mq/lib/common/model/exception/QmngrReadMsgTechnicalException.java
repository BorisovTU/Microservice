package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.exception;

/**
 * Исключение-маркер для отката транзакции при возникновении технической ошибки в процессе загрузки сообщения из
 * SOFR QManager и отправки его в MQ.
 */
public class QmngrReadMsgTechnicalException extends Exception {
}
