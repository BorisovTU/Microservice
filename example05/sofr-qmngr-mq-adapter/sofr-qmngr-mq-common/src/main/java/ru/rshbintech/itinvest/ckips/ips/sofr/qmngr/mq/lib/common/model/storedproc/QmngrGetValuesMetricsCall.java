package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc.GET_VALUES_METRICS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.OUT_JSON;

import lombok.ToString;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc;

/**
 * Контейнер с результатом вызова хранимой процедуры getValuesMetrics.
 */
@ToString
public class QmngrGetValuesMetricsCall extends AbstractQmngrStoredProcCall {

  @NonNull
  @Override
  public QmngrStoredProc getQmngrStoredProc() {
    return GET_VALUES_METRICS;
  }

  @Nullable
  public String getOutJson() {
    return getOutParam(OUT_JSON);
  }

  public void setOutJson(String payload) {
    putOutParam(OUT_JSON, payload);
  }
}
