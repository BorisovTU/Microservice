package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.integrations.config;

import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka2jdbc")
public record Kafka2JdbcProperties(
    Map<String, Map<String, String>> kafkaClusters,
    List<Kafka2JdbcRouteProps> routes
) {
  public record Kafka2JdbcRouteProps(
      String clusterRef,
      String inputTopics,
      String filter,
      String outProcedureTemplate) {
  }
}



