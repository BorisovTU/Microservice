package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc.LOAD_MSG;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.ERROR_CODE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.ERROR_DESC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.ESB_DT;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.MESSAGE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.TOPIC;

import java.time.LocalDateTime;
import lombok.ToString;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrLoadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc;

/**
 * Контейнер с параметрами для вызова хранимой процедуры qmanager_load_msg и с результатами ее вызова.
 */
@ToString
public class QmngrLoadMsgCall extends AbstractQmngrStoredProcCall {

  @NonNull
  @Override
  public QmngrStoredProc getQmngrStoredProc() {
    return LOAD_MSG;
  }

  /**
   * Конструктор для создания контейнера с параметрами вызова хранимой процедуры qmanager_load_msg и с результатами
   * ее вызова на основании контейнера с информацией о сообщении, которое должно быть сохранено в SOFR QManager.
   *
   * @param loadMsg контейнер с информацией о сообщении, которое должно быть сохранено в SOFR QManager.
   */
  public QmngrLoadMsgCall(@NonNull QmngrLoadMsgDto loadMsg) {
    putInParam(TOPIC, loadMsg.getTopic());
    putInParam(MSG_ID, loadMsg.getMsgId());
    putInParam(ESB_DT, loadMsg.getEsbDt());
    putInParam(HEADERS, loadMsg.getHeaders());
    putInParam(MESSAGE, loadMsg.getMessage());
  }

  @Nullable
  public String getMsgId() {
    return getInParam(MSG_ID);
  }

  @Nullable
  public Integer getErrorCode() {
    return getOutParam(ERROR_CODE);
  }

  @Nullable
  public String getErrorDesc() {
    return getOutParam(ERROR_DESC);
  }

  public void setErrorCode(Integer code) {
    putOutParam(ERROR_CODE, code);
  }

  public void setErrorDesc(String desc) {
    putOutParam(ERROR_DESC, desc);
  }

  public String getTopic() {
    return getInParam(TOPIC);
  }

  public LocalDateTime getEsbDt() {
    return getInParam(ESB_DT);
  }

  public String getHeaders() {
    return getInParam(HEADERS);
  }

  public String getMessage() {
    return getInParam(MESSAGE);
  }
}
