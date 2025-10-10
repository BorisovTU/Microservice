package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils;

import static org.apache.kafka.common.record.TimestampType.CREATE_TIME;

import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

@UtilityClass
public class TestUtils {

  public ConsumerRecord<String, String> createConsumerRecord(@NonNull String topic,
                                                             long timestamp,
                                                             @Nullable String key,
                                                             @NonNull String value,
                                                             @NonNull Headers headers) {
    return new ConsumerRecord<>(
        topic,
        0,
        0,
        timestamp,
        CREATE_TIME,
        Optional.ofNullable(key).map(String::length).orElse(0),
        value.length(),
        key,
        value,
        headers,
        Optional.empty()
    );
  }

}
