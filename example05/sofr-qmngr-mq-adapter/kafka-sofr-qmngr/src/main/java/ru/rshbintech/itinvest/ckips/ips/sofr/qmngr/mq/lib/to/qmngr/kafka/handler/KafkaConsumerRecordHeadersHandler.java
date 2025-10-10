package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.handler;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * Обработчик заголовков Kafka.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerRecordHeadersHandler {

  private final ObjectMapper objectMapper;

  /**
   * Метод производит получение заголовков из Kafka ConsumerRecord в виде строки JSON.
   *
   * @param headers - заголовки ConsumerRecord Kafka
   * @return заголовки сообщения Kafka в виде строки JSON
   */
  @Nullable
  public String toJsonString(@NonNull Headers headers) {
    final Map<String, String> headersMap = Arrays.stream(headers.toArray())
        .collect(Collectors.toMap(Header::key, header -> new String(header.value(), UTF_8)));
    if (CollectionUtils.isEmpty(headersMap)) {
      return null;
    }
    try {
      return objectMapper.writeValueAsString(headersMap);
    } catch (Exception e) {
      log.error("Error while write headers as json. Cause: {}.", e.getMessage(), e);
      return null;
    }
  }

}
