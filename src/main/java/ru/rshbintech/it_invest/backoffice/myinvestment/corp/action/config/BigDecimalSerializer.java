package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.config;

import org.apache.kafka.common.serialization.Serializer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

public class BigDecimalSerializer implements Serializer<BigDecimal> {
    @Override
    public byte[] serialize(String topic, BigDecimal data) {
        if (data == null) return null;
        return data.toString().getBytes(StandardCharsets.UTF_8);
    }
}
