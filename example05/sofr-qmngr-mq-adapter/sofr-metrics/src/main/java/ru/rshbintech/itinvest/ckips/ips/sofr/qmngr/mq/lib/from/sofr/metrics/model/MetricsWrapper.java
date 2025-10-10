package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.sofr.metrics.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Контейнер со списком метрик.
 */
@Data
@NoArgsConstructor
public class MetricsWrapper {
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<Metric> metrics = new ArrayList<>();
}
