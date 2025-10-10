package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.integrations.config;

import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.serde.ToStringKafkaHeaderDeserializer;
import org.apache.camel.spring.boot.CamelContextConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.integrations.camel.routes.DlqRouter;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.integrations.camel.routes.Kafka2JdbcRoute;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.integrations.exceptions.KafkaToJdbcException;


@Configuration
public class ConfigLoader {
  private final Kafka2JdbcProperties kafka2JdbcProperties;

  public ConfigLoader(Kafka2JdbcProperties kafka2JdbcProperties) {
    this.kafka2JdbcProperties = kafka2JdbcProperties;
  }

  @Bean
  public ToStringKafkaHeaderDeserializer toStringSerializer() {
    return new ToStringKafkaHeaderDeserializer();
  }

  @Bean
  CamelContextConfiguration contextConfiguration() {
    return new CamelContextConfiguration() {

      @Override
      public void beforeApplicationStart(CamelContext camelContext) {
        camelContext.getGlobalOptions()
            .put(Exchange.LOG_EIP_NAME, "ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.integrations.camel.${routeId}");

        kafka2JdbcProperties.routes()
            .forEach(routeProps ->
                createRouteAndAddToContext(routeProps, camelContext)
            );
      }

      @Override
      public void afterApplicationStart(CamelContext camelContext) {
        //no context after app start just now
      }
    };
  }

  @Bean
  public DlqRouter dlqRouter(
      @Value("${app.kafka2jdbc.dead-letter-queue.name}") final String dlqName,
      @Value("${app.kafka2jdbc.dead-letter-queue.clusterRef}") final String clusterRef
  ) {
    return new DlqRouter(dlqName, camelClusterConfigBy(clusterRef));
  }

  private String camelClusterConfigBy(String clusterName) {
    return Optional.ofNullable(kafka2JdbcProperties.kafkaClusters().get(clusterName))
        .orElseThrow(() -> new RuntimeException("Kafka cluster " + clusterName + " not exists in config"))
        .entrySet().stream()
        .map(entry -> entry.getKey() + "=" + entry.getValue())
        .collect(Collectors.joining("&"));
  }

  private void createRouteAndAddToContext(Kafka2JdbcProperties.Kafka2JdbcRouteProps routeProps,
                                          CamelContext camelContext) {
    try {
      camelContext.addRoutes(
          new Kafka2JdbcRoute(
              camelClusterConfigBy(routeProps.clusterRef()),
              routeProps.inputTopics(),
              routeProps.filter(),
              routeProps.outProcedureTemplate()
          )
      );
    } catch (Exception e) {
      throw new KafkaToJdbcException(e);
    }
  }
}
