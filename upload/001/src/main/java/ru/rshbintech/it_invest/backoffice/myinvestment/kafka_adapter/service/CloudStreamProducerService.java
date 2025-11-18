package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.exception.MessageSendException;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.KafkaMessage;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.RequestStatus;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.property.KafkaTopicBindingsProperties;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class CloudStreamProducerService {

    private final StreamBridge streamBridge;
    private final ObjectMapper objectMapper;
    private final KafkaTopicBindingsProperties kafkaTopicBindingsProperties;

    public void sendMessage(KafkaMessage message) {
        try {
            String bindingName = getBindingNameForTopic(message.getTopic());

            org.springframework.messaging.Message<String> streamMessage = MessageBuilder
                    .withPayload(message.getPayload())
                    .setHeader("kafka_messageKey", message.getMessageId())
                    .build();

            boolean sent = streamBridge.send(bindingName, streamMessage);

            if (sent) {
                log.debug("Successfully sent message to topic {} via binding {}: {}",
                        message.getTopic(), bindingName, message.getMessageId());
            } else {
                throw new MessageSendException("Failed to send message to binding: " + bindingName);
            }

        } catch (MessageSendException e) {
            throw e;
        } catch (Exception e) {
            log.error("Error sending message to Kafka topic {}: {}", message.getTopic(), e.getMessage(), e);
            throw new MessageSendException("Failed to send message", e);
        }
    }

    public void sendRequestStatus(RequestStatus requestStatus, String topic) {
        try {
            String bindingName = getBindingNameForTopic(topic);

            String payload = objectMapper.writeValueAsString(requestStatus);

            org.springframework.messaging.Message<String> streamMessage = MessageBuilder
                    .withPayload(payload)
                    .setHeader("kafka_messageKey", requestStatus.requestId())
                    .build();

            boolean sent = streamBridge.send(bindingName, streamMessage);

            if (sent) {
                log.debug("Successfully sent request status to topic {} via binding {}: {}",
                        topic, bindingName, requestStatus.requestId());
            } else {
                throw new MessageSendException("Failed to send request status to binding: " + bindingName);
            }

        } catch (Exception e) {
            log.error("Error sending request status to Kafka topic {}: {}", topic, e.getMessage(), e);
            throw new MessageSendException("Failed to send request status", e);
        }
    }

    public boolean supportsTopic(String topic) {
        try {
            return getBindingNameForTopic(topic) != null;
        } catch (MessageSendException e) {
            return false;
        }
    }

    private String getBindingNameForTopic(String topic) {
        Map<String, String> topicBindings = kafkaTopicBindingsProperties.getTopicBindings();
        if (topicBindings == null) {
            throw new MessageSendException("Topic bindings configuration is missing");
        }

        String bindingName = topicBindings.get(topic);
        if (bindingName == null) {
            throw new MessageSendException("No binding configuration for topic: " + topic);
        }

        return bindingName;
    }
}
