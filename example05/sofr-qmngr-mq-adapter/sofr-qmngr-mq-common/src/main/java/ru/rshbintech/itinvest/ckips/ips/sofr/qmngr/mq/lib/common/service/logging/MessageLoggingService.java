package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.logging;

import static org.apache.commons.lang3.StringUtils.EMPTY;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring.MonitoringParamHeadersLoggingProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring.MonitoringParamMessageLoggingProperties;

/**
 * Сервис логирования тела сообщения.
 */
@Service
@RequiredArgsConstructor
@EnableConfigurationProperties({
    MonitoringParamHeadersLoggingProperties.class,
    MonitoringParamMessageLoggingProperties.class
})
public class MessageLoggingService {

  private final MonitoringParamHeadersLoggingProperties monitoringParamHeadersLoggingProperties;
  private final MonitoringParamMessageLoggingProperties monitoringParamMessageLoggingProperties;

  /**
   * Метод производит получение текста лога с телом и заголовками сообщения.
   *
   * @param messageHeaders заголовки сообщения для лога
   * @param messageBody    тело сообщения для лога
   * @return текст лога с телом сообщения
   */
  public String getLogMessageText(@Nullable String messageHeaders, @Nullable String messageBody) {
    final String logMessageText;
    if (StringUtils.isNotEmpty(messageBody) && monitoringParamMessageLoggingProperties.isEnabled()) {
      logMessageText = String.format(
          " with body = [%s]%s",
          trimMessageBodyIfNeed(messageBody),
          StringUtils.isNotEmpty(messageHeaders) && monitoringParamHeadersLoggingProperties.isEnabled()
              ? String.format(" and headers = [%s]", trimMessageHeadersIfNeed(messageHeaders))
              : EMPTY
      );
    } else if (StringUtils.isNotEmpty(messageHeaders) && monitoringParamHeadersLoggingProperties.isEnabled()) {
      logMessageText = String.format(" with headers = [%s]", trimMessageHeadersIfNeed(messageHeaders));
    } else {
      logMessageText = EMPTY;
    }
    return logMessageText;
  }

  @Nullable
  private String trimMessageHeadersIfNeed(@Nullable String messageHeaders) {
    return StringUtils.abbreviate(messageHeaders, monitoringParamHeadersLoggingProperties.getMaxLength());
  }

  @Nullable
  private String trimMessageBodyIfNeed(@Nullable String messageBody) {
    return StringUtils.abbreviate(messageBody, monitoringParamMessageLoggingProperties.getMaxLength());
  }

}
