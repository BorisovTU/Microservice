package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.sofr.metrics.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Контейнер с параметрами метрики.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Metric {
  private String name;
  private Integer value;
  private String timestamp;
  private String description;
}
