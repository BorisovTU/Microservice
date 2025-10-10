package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.mdc;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.MdcField.CORRELATION_ID;

import java.util.Objects;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * Адаптер для работы с диагностическим контекстом MDC.
 */
@Component
public class MdcAdapter {

  /**
   * Метод выполняет вставку correlationId в диагностический контекст MDC. Если переданный correlationId пуст,
   * генерирует новый на основе UUID.
   *
   * @param correlationId - correlationId для вставки в диагностический контекст MDC
   */
  public void putCorrelationId(@Nullable String correlationId) {
    MDC.put(
        CORRELATION_ID.getName(),
        StringUtils.isNotBlank(correlationId) ? correlationId : Objects.toString(UUID.randomUUID())
    );
  }

  public void clear() {
    MDC.clear();
  }

}
