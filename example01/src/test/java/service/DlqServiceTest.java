package service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.service.DlqService;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

public class DlqServiceTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private KafkaConfig kafkaConfig;

    private DlqService dlqService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        kafkaConfig = new KafkaConfig();
        KafkaConfig.Topic topic = new KafkaConfig.Topic();
        topic.setDlq("fx.dlq.test");
        kafkaConfig.setTopic(topic);

        dlqService = new DlqService(kafkaTemplate, kafkaConfig);
    }

    @Test
    void sendToDlq_sendsMessageWithHeaders() {
        String sourceTopic = "fx.orders.client";
        String key = "key1";
        String value = "{\"json\":\"value\"}";
        Exception exception = new RuntimeException("Test error");

        dlqService.sendToDlq(sourceTopic, key, value, exception);

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());

        ProducerRecord<String, String> record = captor.getValue();

        assertEquals("fx.dlq.test", record.topic());

        assertEquals(key, record.key());
        assertEquals(value, record.value());

        Headers headers = record.headers();
        assertEquals(sourceTopic, new String(headers.lastHeader("sourceTopic").value(), StandardCharsets.UTF_8));
        String reasonHeader = new String(headers.lastHeader("deadLetterReason").value(), StandardCharsets.UTF_8);
        assertEquals("Test error. Сервис: fx-clients-trades-sofr", reasonHeader);
        assertEquals("1", new String(headers.lastHeader("attemptCount").value(), StandardCharsets.UTF_8));
    }
}
