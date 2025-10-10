package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc.READ_MSG;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.ERROR_CODE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.ERROR_DESC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.MESSAGE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.TOPIC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.WAIT_MSG;

import java.sql.Blob;
import java.sql.Clob;
import lombok.ToString;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc;

/**
 * Контейнер с параметрами для вызова хранимой процедуры qmanager_read_msg и с результатами ее вызова.
 */
@ToString
public class QmngrReadMsgCall extends AbstractQmngrStoredProcCall {

  private static final int WAIT_MSG_SECONDS = 0;

  @NonNull
  @Override
  public QmngrStoredProc getQmngrStoredProc() {
    return READ_MSG;
  }

  /**
   * Конструктор для создания контейнера с параметрами вызова хранимой процедуры qmanager_read_msg и с результатами
   * ее вызова.
   */
  public QmngrReadMsgCall() {
    putInParam(WAIT_MSG, WAIT_MSG_SECONDS);
  }

  @Nullable
  public String getTopic() {
    return getOutParam(TOPIC);
  }

  @Nullable
  public String getMsgId() {
    return getOutParam(MSG_ID);
  }

  @Nullable
  public String getHeaders() {
    return getOutParam(HEADERS);
  }

  @Nullable
  public Clob getMessage() {
    return getOutParam(MESSAGE);
  }

  @Nullable
  public Integer getErrorCode() {
    return getOutParam(ERROR_CODE);
  }

  @Nullable
  public String getErrorDesc() {
    return getOutParam(ERROR_DESC);
  }

}
