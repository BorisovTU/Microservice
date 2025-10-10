package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.converter;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.constants.QmngrConstants.MSG_ID_MAX_LENGTH;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrLoadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.configuration.properties.KafkaConsumersProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.configuration.properties.KafkaConsumersProperties.QmngrLoadMsgConsumerProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.handler.KafkaConsumerRecordHeadersHandler;

/**
 * Конвертер ConsumerRecord в контейнер с информацией о сообщении из Kafka для сохранения в SOFR QManager.
 */
@Service
@RequiredArgsConstructor
@SuppressWarnings("unused")
public class ConsumerRecordToQmngrLoadMsgDtoConverter
    implements Converter<ConsumerRecord<String, String>, QmngrLoadMsgDto> {

  private final KafkaConsumersProperties kafkaConsumersProperties;
  private final KafkaConsumerRecordHeadersHandler kafkaConsumerRecordHeadersHandler;

  private Map<String, QmngrLoadMsgConsumerProperties> loadMsgTopicToConsumerProperties;

  @PostConstruct
  @SuppressWarnings("unused")
  private void init() {
    this.loadMsgTopicToConsumerProperties = kafkaConsumersProperties.getLoadMsg().stream()
        .collect(Collectors.toMap(QmngrLoadMsgConsumerProperties::getTopic, Function.identity()));
  }

  @NonNull
  @Override
  public QmngrLoadMsgDto convert(@NonNull ConsumerRecord<String, String> consumerRecord) {
    return QmngrLoadMsgDto.builder()
        .topic(consumerRecord.topic())
        .msgId(extractMsgId(consumerRecord))
        .esbDt(extractEsbDt(consumerRecord))
        .headers(kafkaConsumerRecordHeadersHandler.toJsonString(consumerRecord.headers()))
        .message(consumerRecord.value())
        .build();
  }

  @Nullable
  private String extractMsgId(@NonNull ConsumerRecord<String, String> consumerRecord) {
    final QmngrLoadMsgConsumerProperties loadMsgConsumerProperties = Optional.ofNullable(consumerRecord.topic())
        .map(topic -> this.loadMsgTopicToConsumerProperties.get(topic))
        .orElse(null);
    final String msgId;
    if (loadMsgConsumerProperties == null) {
      msgId = null;
    } else if (loadMsgConsumerProperties.isUseKeyAsMsgId()) {
      msgId = consumerRecord.key();
    } else if (StringUtils.isNotEmpty(loadMsgConsumerProperties.getMsgIdHeaderName())) {
      msgId = Optional.ofNullable(consumerRecord.headers())
          .map(headers -> headers.lastHeader(loadMsgConsumerProperties.getMsgIdHeaderName()))
          .map(header -> new String(header.value()))
          .orElse(null);
    } else {
      msgId = null;
    }
    return StringUtils.abbreviate(msgId, StringUtils.EMPTY, MSG_ID_MAX_LENGTH);
  }

  @Nullable
  private LocalDateTime extractEsbDt(@NonNull ConsumerRecord<String, String> consumerRecord) {
    final long timestamp = consumerRecord.timestamp();
    return timestamp != 0 ? Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime() : null;
  }

}
