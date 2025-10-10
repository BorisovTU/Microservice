package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit;

import java.util.Map;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.itinvest.ckips.ips.auditserviceclient.AuditService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.AuditProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.audit.AuditEvent;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.MqType;

/**
 * Сервис для отправки событий аудита в топик Kafka.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@EnableConfigurationProperties(AuditProperties.class)
public class AuditEventSenderService {

  private final AuditService auditService;
  private final AuditProperties auditProperties;

  /**
   * Отправка события аудита в топик аудита.
   *
   * @param message             сообщение
   * @param event               событие аудита
   * @param auditParamsSupplier функциональный интерфейс-поставщик параметров аудита для ленивого создания объекта
   */
  public void sendAuditEvent(@NonNull String message,
                             @NonNull AuditEvent event,
                             @NonNull Supplier<Map<String, String>> auditParamsSupplier) {
    if (!auditProperties.isEnabled()) {
      return;
    }
    try {
      auditService.write(message, event.getMessage(), event.getSeverity(), auditParamsSupplier.get());
    } catch (Exception e) {
      log.error("Error while sending message to audit. Cause: {}.", ExceptionUtils.getStackTrace(e));
    }
  }

}
