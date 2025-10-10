package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.qmngr;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.stereotype.Service;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dao.QmngrDao;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrGetValuesMetricsCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.QmngrGetValuesMetricsAuditService;

/**
 * Сервис для получения бизнес метрик из СОФР.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QmngrGetValuesMetricsService {

  private final QmngrDao qmngrDao;
  private final QmngrGetValuesMetricsAuditService getValuesMetricsAuditService;

  /**
   * Метод для получения бизнес метрик из СОФР.
   *
   * @return метрики в виде строки
   */
  public String getValuesMetrics() {
    final QmngrGetValuesMetricsCall getValuesMetricsCall = new QmngrGetValuesMetricsCall();
    try {
      getValuesMetricsAuditService.logGetValuesMetricsCall(getValuesMetricsCall);
      qmngrDao.callGetValuesMetrics(getValuesMetricsCall);
      getValuesMetricsAuditService.auditGetValuesMetricsCallSuccessCompletion(getValuesMetricsCall);
      return getValuesMetricsCall.getOutJson();
    } catch (Exception e) {
      getValuesMetricsAuditService.auditGetValuesMetricsCallError(getValuesMetricsCall,
              ExceptionUtils.getStackTrace(e));
      return null;
    }

  }

}
