package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.integrations.camel.routes;

import org.apache.camel.builder.RouteBuilder;

public class DlqRouter extends RouteBuilder {

  private final String dlqName;
  private final String brokerList;

  public DlqRouter(final String dlqName, String brokerList) {
    this.dlqName = dlqName;
    this.brokerList = brokerList;
  }

  @Override
  public void configure() throws Exception {
    errorHandler(deadLetterChannel("log:dead?level=ERROR&showAll=true&multiline=true"));

    from("direct:dlq")
        .log("Message will be send to dlq. ID: ${headers[kafka.KEY]}")
        .setHeader("originalTopic", simple("${headers[kafka.TOPIC]}"))
        .to("kafka:%s?%s".formatted(dlqName, brokerList));
  }
}
