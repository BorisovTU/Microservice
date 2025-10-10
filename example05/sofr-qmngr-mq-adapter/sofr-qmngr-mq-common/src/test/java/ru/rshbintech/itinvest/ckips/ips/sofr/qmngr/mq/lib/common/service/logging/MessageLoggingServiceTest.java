package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.logging;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.constants.QmngrConstants.DEFAULT_MONITORING_MESSAGE_MAX_LENGTH;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_MESSAGE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_STRING_HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_TRIM_TO_LENGTH;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring.MonitoringParamHeadersLoggingProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring.MonitoringParamMessageLoggingProperties;

@ExtendWith(MockitoExtension.class)
class MessageLoggingServiceTest {

  private static final String WITH_BODY_LOG_TEXT = " with body = [%s]";
  private static final String WITH_HEADERS_LOG_TEXT = " with headers = [%s]";
  private static final String WITH_BODY_AND_HEADERS_LOG_TEXT = " with body = [%s] and headers = [%s]";

  private final MessageLoggingService messageLoggingService;

  private final MonitoringParamHeadersLoggingProperties monitoringParamHeadersAuditProperties =
      Mockito.mock(MonitoringParamHeadersLoggingProperties.class);
  private final MonitoringParamMessageLoggingProperties monitoringParamMessageLoggingProperties =
      Mockito.mock(MonitoringParamMessageLoggingProperties.class);

  public MessageLoggingServiceTest() {
    this.messageLoggingService = new MessageLoggingService(
        monitoringParamHeadersAuditProperties,
        monitoringParamMessageLoggingProperties
    );
  }

  @Test
  void testGetLogMessageTextWithEnabledAndNotEmptyHeadersAndBody() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);
    Mockito.when(monitoringParamMessageLoggingProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageLoggingProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);
    final String logMessageText = messageLoggingService.getLogMessageText(TEST_VAL_STRING_HEADERS, TEST_VAL_MESSAGE);
    Assertions.assertEquals(
        String.format(WITH_BODY_AND_HEADERS_LOG_TEXT, TEST_VAL_MESSAGE, TEST_VAL_STRING_HEADERS),
        logMessageText
    );
  }

  @Test
  void testGetLogMessageTextWithEnabledHeadersAndBodyAndEmptyHeaders() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);
    Mockito.when(monitoringParamMessageLoggingProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageLoggingProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);
    final String logMessageText = messageLoggingService.getLogMessageText(null, TEST_VAL_MESSAGE);
    Assertions.assertEquals(
        String.format(WITH_BODY_LOG_TEXT, TEST_VAL_MESSAGE),
        logMessageText
    );
  }

  @Test
  void testGetLogMessageTextWithEnabledHeadersAndBodyAndEmptyBody() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);
    Mockito.when(monitoringParamMessageLoggingProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageLoggingProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);
    final String logMessageText = messageLoggingService.getLogMessageText(TEST_VAL_STRING_HEADERS, null);
    Assertions.assertEquals(
        String.format(WITH_HEADERS_LOG_TEXT, TEST_VAL_STRING_HEADERS),
        logMessageText
    );
  }

  @Test
  void testGetLogMessageTextWithEnabledAndEmptyHeadersAndBody() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);
    Mockito.when(monitoringParamMessageLoggingProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageLoggingProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);
    final String logMessageText = messageLoggingService.getLogMessageText(null, null);
    Assertions.assertEquals(
        String.format(EMPTY),
        logMessageText
    );
  }

  @Test
  void testGetLogMessageTextWithEnabledAndNotEmptyHeadersAndBodyWithTrim() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength())
        .thenReturn(TEST_VAL_TRIM_TO_LENGTH);
    Mockito.when(monitoringParamMessageLoggingProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageLoggingProperties.getMaxLength())
        .thenReturn(TEST_VAL_TRIM_TO_LENGTH);
    final String logMessageText = messageLoggingService.getLogMessageText(TEST_VAL_STRING_HEADERS, TEST_VAL_MESSAGE);
    Assertions.assertEquals(
        String.format(
            WITH_BODY_AND_HEADERS_LOG_TEXT,
            StringUtils.abbreviate(TEST_VAL_MESSAGE, TEST_VAL_TRIM_TO_LENGTH),
            StringUtils.abbreviate(TEST_VAL_STRING_HEADERS, TEST_VAL_TRIM_TO_LENGTH)
        ),
        logMessageText
    );
  }

}
