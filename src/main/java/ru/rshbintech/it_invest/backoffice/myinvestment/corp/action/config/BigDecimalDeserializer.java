package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.config;

import org.apache.kafka.common.serialization.Deserializer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

public class BigDecimalDeserializer implements Deserializer<BigDecimal> {
    @Override
    public BigDecimal deserialize(String topic, byte[] data) {
        if (data == null) return null;
        return new BigDecimal(new String(data, StandardCharsets.UTF_8));
    }
}
