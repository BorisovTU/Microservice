package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.sofr.metrics.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.qmngr.QmngrGetValuesMetricsService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.sofr.metrics.model.Metric;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.sofr.metrics.model.MetricsWrapper;


/**
 * Сервис для обновления метрик из SOFR в Micrometer.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class MetricsService {

  private final QmngrGetValuesMetricsService getValuesMetricsService;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;

  private final Map<String, AtomicInteger> dynamicMetrics = new ConcurrentHashMap<>();

  /**
   * Вызывается по расписанию. Получает метрики из SOFR и обновляет метрики на api /metrics
   */
  @Scheduled(cron = "${app.scheduling.cron}", zone = "${app.scheduling.zone}")
  public void process() {
    String metrics = getValuesMetricsService.getValuesMetrics();
    if (metrics == null || metrics.isEmpty()) {
      log.info("The stored procedure getValuesMetrics() returned a null or empty value.");
    } else {
      updateMetrics(metrics);
    }
  }

  /**
   * Получает метрики из строки и обновляет их в Micrometer.
   *
   * @param metricsStr Строка с JSON

   */
  private void updateMetrics(String metricsStr) {
    try {
      MetricsWrapper metricsWrapper = objectMapper.readValue(metricsStr, MetricsWrapper.class);

      if (metricsWrapper.getMetrics().isEmpty()) {
        log.info("Received metrics list is empty.");
        return;
      }

      // Удаляем метрики, которых больше нет в JSON
      dynamicMetrics.keySet().removeIf(existingMetricName ->
              metricsWrapper.getMetrics().stream()
                      .noneMatch(newMetric -> newMetric.getName() != null
                              && sanitizeMetricName(newMetric.getName()).equals(existingMetricName)));

      // Регистрируем или обновляем метрики
      for (Metric metric : metricsWrapper.getMetrics()) {
        if (metric.getName() == null || metric.getName().isBlank()) {
          log.warn("Skipping metric with empty name: {}", metric);
          continue;
        }

        String safeName = sanitizeMetricName(metric.getName());
        int safeValue = metric.getValue() != null ? metric.getValue() : 0;

        AtomicInteger metricValue = dynamicMetrics.computeIfAbsent(safeName, name -> {
          AtomicInteger newValue = new AtomicInteger(safeValue);
          Gauge.builder(name, newValue, AtomicInteger::get)
                  .description(metric.getDescription() != null ? metric.getDescription() : "N/A")
                  .register(meterRegistry);
          return newValue;
        });

        metricValue.set(safeValue);

        if (metric.getValue() == null) {
          log.warn("Metric '{}' received with null value, replaced with 0. Full metric: {}", safeName, metric);
        }
      }

    } catch (Exception e) {
      log.error("Error while parsing metrics. Cause: {}. Input: {}", e.getMessage(), metricsStr, e);
    }
  }

  /**
   * Делает имя метрики безопасным для Micrometer/Prometheus.
   */
  private String sanitizeMetricName(String name) {
    return name.replaceAll("[^a-zA-Z0-9_.-]", "_");
  }
}
