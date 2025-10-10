package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums;

/**
 * Перечисление со статусами обработчика процесса загрузки сообщения из SOFR QManager и отправки его в MQ.
 */
public enum QmngrReadMsgProcessState {

  //Цикл обработки сообщений SOFR QManager продолжает работу
  CONTINUE,
  //Цикл обработки сообщений SOFR QManager завершает работу по причине технической ошибки
  STOP_ON_TECHNICAL_ERROR,
  //Цикл обработки сообщений SOFR QManager завершает работу штатным образом (по бизнес-условию)
  STOP_NORMALLY

}
