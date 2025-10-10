package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration;

import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Конфигурация метрик. Без этой конфигурации аннотации micrometer работать не будут.
 */
@Configuration
@SuppressWarnings("unused")
public class MetricsConfiguration {

  @Bean
  public TimedAspect timedAspect(MeterRegistry meterRegistry) {
    return new TimedAspect(meterRegistry);
  }

}
