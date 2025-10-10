package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.sofr.metrics.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.qmngr.QmngrGetValuesMetricsService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.sofr.metrics.model.Metric;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.sofr.metrics.model.MetricsWrapper;

import java.util.Collections;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MetricsServiceTest {

    @Mock
    private QmngrGetValuesMetricsService getValuesMetricsService;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private MeterRegistry meterRegistry;

    @InjectMocks
    private MetricsService metricsService;

    /**
     * Тестирует метод process, когда метрики возвращаются и успешно преобразуются.
     *
     * @throws Exception если ObjectMapper выбрасывает исключение при десериализации
     */
    @Test
    void testProcess_withValidMetrics() throws Exception {

        String jsonMetrics = "{ \"metrics\": [{ \"name\": \"metric1\", \"value\": 10, \"timestamp\": \"2023-09-01\", \"description\": \"Test metric\" }] }";
        Metric testMetric = new Metric("metric1", 10, "2023-09-01", "Test metric");

        MetricsWrapper metricsWrapper = new MetricsWrapper();
        metricsWrapper.setMetrics(Collections.singletonList(testMetric));

        when(getValuesMetricsService.getValuesMetrics()).thenReturn(jsonMetrics);
        when(objectMapper.readValue(jsonMetrics, MetricsWrapper.class)).thenReturn(metricsWrapper);

        metricsService.process();

        verify(getValuesMetricsService, times(1)).getValuesMetrics();
        verify(objectMapper, times(1)).readValue(jsonMetrics, MetricsWrapper.class);
    }

    /**
     * Тестирует метод process, когда метрики не возвращаются (null)
     */
    @Test
    void testProcess_withNullMetrics() {

        when(getValuesMetricsService.getValuesMetrics()).thenReturn(null);

        metricsService.process();

        verify(meterRegistry, never()).gauge(anyString(), any());
        verify(getValuesMetricsService, times(1)).getValuesMetrics();
    }

}