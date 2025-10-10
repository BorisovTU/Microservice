package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.integrations.camel.routes;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.integrations.camel.processors.OnErrorLogger;


public class Kafka2JdbcRoute extends RouteBuilder {
  private static final Logger log = LoggerFactory.getLogger(Kafka2JdbcRoute.class);
  private final String inputTopic;
  private final String brokerList;
  private final String outProcedureTemplate;
  private final String filter;

  public Kafka2JdbcRoute(String brokerList,
                         String inputTopic,
                         String filter,
                         String outProcedureTemplate
  ) {
    this.brokerList = brokerList;
    this.inputTopic = inputTopic;
    this.filter = filter;
    this.outProcedureTemplate = outProcedureTemplate;
  }

  @Override
  public void configure() throws Exception {
    errorHandler(deadLetterChannel("direct:dlq")
        .useOriginalMessage().onExceptionOccurred(new OnErrorLogger()));

    onCompletion().process(this::manualKafkaCommit);

    from(("kafka:%s?" +
        "allowManualCommit=true" +
        "&pollOnError=RECONNECT" +
        "&headerDeserializer=#toStringSerializer%s").formatted(inputTopic,
        brokerList.isEmpty() ? "" : "&" + brokerList))
        .log(LoggingLevel.INFO,
            "Message received from Kafka. Topic: ${headers[kafka.TOPIC]}, Key: ${headers[kafka.KEY]}")
        .filter(x -> this.filter == null || simple(filter).matches(x))
        .setVariable("ProcessingTimestamp", simple("$simple{date:now:yyyy-MM-dd HH:mm:ss}"))
        .process(this.kafkaHeadersToJsonString)
        .to("sql-stored:%s".formatted(outProcedureTemplate))
        .choice()
          .when(simple("${body[result]} != null"))
            .throwException(RuntimeException.class,
                "Message key ${headers[kafka.KEY]}. Procedure call error: ${body[result]}"
            )
          .otherwise().end();
  }

  private final Processor kafkaHeadersToJsonString =
      (Exchange e) -> e.setVariable(
          "HeadersJson",
          new ObjectMapper().writeValueAsString(
              recordHeadrsTopMap((RecordHeaders) e.getIn().getHeader("kafka.HEADERS")))
      );

  private Map<String, String> recordHeadrsTopMap(RecordHeaders headers) {
    return Arrays.stream(
        headers.toArray()).collect(Collectors.toMap(Header::key, h -> new String(h.value()))
    );
  }

  private void manualKafkaCommit(Exchange exchange) {
    var messageId = exchange.getIn().getHeader("kafka.KEY");
    KafkaManualCommit manual = exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT,
        KafkaManualCommit.class);
    if (null != manual) {
      log.debug("Message with ID: {} trying to commit", messageId);
      manual.commit();
      log.info("Message with ID {} successfully commited", messageId);
    }
  }
}
