package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.integrations.camel.processors;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnErrorLogger implements Processor {
  private static final Logger log = LoggerFactory.getLogger(OnErrorLogger.class);

  @Override
  public void process(Exchange exchange) throws Exception {
    Exception cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
    String msg = "Something went wrong due to " + cause.getMessage();
    String lastEndpointUri = exchange.getProperty(Exchange.TO_ENDPOINT, String.class);

    log.error(
        "Message processing failure on step: {}. Error: {}",
        URLDecoder.decode(lastEndpointUri, StandardCharsets.UTF_8),
        msg,
        cause
    );

  }
}
