package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit;

import static lombok.AccessLevel.PRIVATE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.MESSAGE;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring.MonitoringParamHeadersAuditProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring.MonitoringParamMessageAuditProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.audit.AuditEventParam;

/**
 * Фабрика построителя параметров аудита.
 */
@Service
@RequiredArgsConstructor
@EnableConfigurationProperties({
    MonitoringParamMessageAuditProperties.class,
    MonitoringParamHeadersAuditProperties.class
})
public class AuditEventParamsBuilderFactory {

  private final MonitoringParamHeadersAuditProperties monitoringParamHeadersAuditProperties;
  private final MonitoringParamMessageAuditProperties monitoringParamMessageAuditProperties;

  public AuditEventParamsBuilder createAuditEventParamsBuilder() {
    return new AuditEventParamsBuilder();
  }

  /**
   * Построитель параметров аудита.
   */
  @NoArgsConstructor(access = PRIVATE)
  public class AuditEventParamsBuilder {

    private final Map<String, String> auditEventParams = new HashMap<>();

    /**
     * Метод добавляет значение (если оно не null) в параметры аудита по ключу.
     *
     * @param paramKey   ключ параметра аудита
     * @param paramValue значение параметра аудита
     * @return построитель параметров аудита
     */
    public AuditEventParamsBuilder withParam(@NonNull AuditEventParam paramKey, @Nullable Object paramValue) {
      if (paramValue != null) {
        this.auditEventParams.put(paramKey.getName(), Objects.toString(paramValue));
      }
      return this;
    }

    /**
     * Метод добавляет заголовки сообщения (если они не null) в параметры аудита по ключу = headers.
     *
     * @param headersParamValue заголовки сообщения
     * @return построитель параметров аудита
     */
    public AuditEventParamsBuilder withHeadersParam(@Nullable String headersParamValue) {
      return withAbbreviatedParam(
          HEADERS,
          headersParamValue,
          monitoringParamHeadersAuditProperties.isEnabled(),
          monitoringParamHeadersAuditProperties.getMaxLength()
      );
    }

    /**
     * Метод добавляет заголовки сообщения (если они не null) в параметры аудита по ключу = headers.
     *
     * @param headersParamName  название параметра с заголовками сообщения
     * @param headersParamValue заголовки сообщения
     * @return построитель параметров аудита
     */
    public AuditEventParamsBuilder withHeadersParam(@NonNull AuditEventParam headersParamName,
                                                    @Nullable String headersParamValue) {
      return withAbbreviatedParam(
          headersParamName,
          headersParamValue,
          monitoringParamHeadersAuditProperties.isEnabled(),
          monitoringParamHeadersAuditProperties.getMaxLength()
      );
    }

    /**
     * Метод добавляет тело сообщения (если оно не null) в параметры аудита по ключу = message.
     *
     * @param messageParamValue тело сообщения
     * @return построитель параметров аудита
     */
    public AuditEventParamsBuilder withMessageParam(@Nullable String messageParamValue) {
      return withAbbreviatedParam(
          MESSAGE,
          messageParamValue,
          monitoringParamMessageAuditProperties.isEnabled(),
          monitoringParamMessageAuditProperties.getMaxLength()
      );
    }

    /**
     * Метод добавляет тело сообщения (если оно не null) в параметры аудита.
     *
     * @param messageParamName  название параметра с телом сообщения
     * @param messageParamValue тело сообщения
     * @return построитель параметров аудита
     */
    public AuditEventParamsBuilder withMessageParam(@NonNull AuditEventParam messageParamName,
                                                    @Nullable String messageParamValue) {
      return withAbbreviatedParam(
          messageParamName,
          messageParamValue,
          monitoringParamMessageAuditProperties.isEnabled(),
          monitoringParamMessageAuditProperties.getMaxLength()
      );
    }

    private AuditEventParamsBuilder withAbbreviatedParam(@NonNull AuditEventParam paramName,
                                                         @Nullable String paramValue,
                                                         boolean isAuditEnabled,
                                                         int maxLength) {
      if (isAuditEnabled && paramValue != null) {
        this.auditEventParams.put(paramName.getName(), StringUtils.abbreviate(paramValue, maxLength));
      }
      return this;
    }

    public Map<String, String> build() {
      return this.auditEventParams;
    }

  }

}
