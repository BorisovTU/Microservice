package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.integrations.exceptions;

public class KafkaToJdbcException extends RuntimeException {
  public KafkaToJdbcException(Exception e) {
    super(e);
  }
}
