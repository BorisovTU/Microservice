package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam;

/**
 * Абстрактный контейнер с параметрами для вызова хранимой процедуры SOFR Qmanager.
 */
public abstract class AbstractQmngrStoredProcCall {

  @Getter
  private Map<String, Object> inParams;
  @SuppressWarnings("unused")
  private Map<String, Object> outParams;

  /**
   * Метод возвращает тип хранимой процедуры.
   *
   * @return тип хранимой процедуры
   */
  @NonNull
  public abstract QmngrStoredProc getQmngrStoredProc();

  protected void putInParam(@NonNull QmngrStoredProcParam param, @Nullable Object value) {
    if (this.inParams == null) {
      this.inParams = new HashMap<>();
    }
    this.inParams.put(param.getName(), value);
  }

  public void putOutParam(@NonNull QmngrStoredProcParam param, @Nullable Object value) {
    if (this.outParams == null) {
      this.outParams = new HashMap<>();
    }
    this.outParams.put(param.getName(), value);
  }

  @Nullable
  protected <T> T getInParam(@NonNull QmngrStoredProcParam param) {
    return getParam(param, this.inParams);
  }

  @Nullable
  protected <T> T getOutParam(@NonNull QmngrStoredProcParam param) {
    return getParam(param, this.outParams);
  }

  @Nullable
  private <T> T getParam(@NonNull QmngrStoredProcParam param, @Nullable Map<String, Object> params) {
    //noinspection unchecked
    return (T) Optional.ofNullable(params)
        .map(p -> p.get(param.getName()))
        .orElse(null);
  }

}
