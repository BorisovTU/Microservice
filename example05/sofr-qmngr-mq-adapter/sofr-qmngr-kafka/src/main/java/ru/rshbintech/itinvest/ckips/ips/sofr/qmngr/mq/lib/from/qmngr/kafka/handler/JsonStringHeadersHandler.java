package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.qmngr.kafka.handler;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.qmngr.kafka.handler.type.HashMapTypeReference;

/**
 * Обработчик заголовков Kafka.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JsonStringHeadersHandler {

  private final ObjectMapper objectMapper;

  /**
   * Метод производит получение заголовков для Kafka из строки с JSON.
   *
   * @param headersString - строка с заголовками в виде JSON
   * @return коллекция заголовков Kafka
   */
  @NonNull
  public List<Header> toKafkaHeaders(@Nullable String headersString) {
    if (StringUtils.isEmpty(headersString)) {
      return Collections.emptyList();
    }
    try {
      return objectMapper.readValue(headersString, new HashMapTypeReference())
              .entrySet()
              .stream()
              .map(entry -> makeHeader(entry.getKey(), entry.getValue()))
              .toList();
    } catch (Exception e) {
      log.error("Error while read headers as list. Cause: {}.", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @NonNull
  private Header makeHeader(@NonNull String headerKey, @Nullable Object headerValue) {
    return new RecordHeader(
            headerKey,
            Optional.ofNullable(headerValue)
                    .map(value -> this.mapToString(value)
                            .getBytes(UTF_8))
                    .orElse(null)
    );
  }

  private String mapToString(Object object) {
    try {
      if (object instanceof String) {
        return (String) object;
      } else if (object instanceof Number) {
        return object.toString();
      } else if (object instanceof Boolean) {
        return object.toString();
      } else {
        return objectMapper.writeValueAsString(object);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
