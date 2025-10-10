package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc.READ_MSG_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.ERROR_CODE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.ERROR_DESC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.TOPIC;

import lombok.ToString;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrReadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrReadMsgError;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc;

/**
 * Контейнер с параметрами для вызова хранимой процедуры qmanager_read_msg_error.
 */
@ToString
public class QmngrReadMsgErrorCall extends AbstractQmngrStoredProcCall {

  @NonNull
  @Override
  public QmngrStoredProc getQmngrStoredProc() {
    return READ_MSG_ERROR;
  }

  /**
   * Конструктор для создания контейнера с параметрами вызова хранимой процедуры qmanager_read_msg_error.
   *
   * @param readMsg      контейнер с информацией о сообщении, загруженном из SOFR QManager, для отправки в MQ
   * @param readMsgError внутренняя ошибка адаптера при обработке результата вызова хранимой процедуры qmanager_read_msg
   */
  public QmngrReadMsgErrorCall(@NonNull final QmngrReadMsgDto readMsg,
                               @NonNull final QmngrReadMsgError readMsgError) {
    putInParam(TOPIC, readMsg.getTopic());
    putInParam(MSG_ID, readMsg.getMsgId());
    putInParam(ERROR_CODE, readMsgError.getCode());
    putInParam(ERROR_DESC, readMsgError.getDescription());
  }

  @Nullable
  public String getMsgId() {
    return getInParam(MSG_ID);
  }

  @Nullable
  public Integer getErrorCode() {
    return getInParam(ERROR_CODE);
  }

  @Nullable
  public String getErrorDesc() {
    return getInParam(ERROR_DESC);
  }

  public String getTopic() {
    return getInParam(TOPIC);
  }
}
