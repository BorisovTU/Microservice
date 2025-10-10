package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrGetValuesMetricsCall;

/**
 * Сервис аудита процесса получения метрик из СОФР.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QmngrGetValuesMetricsAuditService {

  /**
   * Логирование вызова хранимой процедуры getValuesMetrics.
   *
   * @param getValuesMetricsCall контейнер с параметрами вызова хранимой процедуры getValuesMetrics
   */
  public void logGetValuesMetricsCall(@NonNull QmngrGetValuesMetricsCall getValuesMetricsCall) {
    log.info("Calling stored procedure = [{}]", getValuesMetricsCall.getQmngrStoredProc());
  }

  /**
   * Логирование ошибки вызова хранимой процедуры getValuesMetrics.
   *
   * @param getValuesMetricsCall контейнер с параметрами вызова хранимой процедуры getValuesMetrics
   * @param errorCause причина ошибки
   */
  public void auditGetValuesMetricsCallError(@NonNull QmngrGetValuesMetricsCall getValuesMetricsCall,
                                         @NonNull String errorCause) {
    final QmngrStoredProc storedProc = getValuesMetricsCall.getQmngrStoredProc();
    log.error(
        "Error while call stored procedure = [{}]. Cause: {}.",
        storedProc,
        errorCause
    );
  }

  /**
   * Логирование успешного результата вызова хранимой процедуры getValuesMetrics.
   *
   * @param getValuesMetricsCall контейнер с параметрами вызова хранимой процедуры getValuesMetrics
   */
  public void auditGetValuesMetricsCallSuccessCompletion(@NonNull QmngrGetValuesMetricsCall getValuesMetricsCall) {
    final QmngrStoredProc storedProc = getValuesMetricsCall.getQmngrStoredProc();
    log.info("Stored procedure = [{}] completed successfully", storedProc);
  }

}
