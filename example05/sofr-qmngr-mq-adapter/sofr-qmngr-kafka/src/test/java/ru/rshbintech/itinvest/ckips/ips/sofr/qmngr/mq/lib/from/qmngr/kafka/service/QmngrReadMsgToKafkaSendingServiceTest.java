package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.qmngr.kafka.service;

import com.google.common.io.CharStreams;
import java.sql.Clob;
import java.util.concurrent.CompletableFuture;

import javax.sql.rowset.serial.SerialClob;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import org.springframework.kafka.support.SendResult;
import reactor.core.Exceptions;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrReadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.exception.QmngrReadMsgSendToMqException;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.qmngr.kafka.handler.JsonStringHeadersHandler;

@ExtendWith(MockitoExtension.class)
class QmngrReadMsgToKafkaSendingServiceTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private JsonStringHeadersHandler jsonStringHeadersHandler;

    @Mock
    private PlatformDynamicKafkaTemplateService platformDynamicKafkaTemplateService;

    @InjectMocks
    private QmngrReadMsgToKafkaSendingService service;

    private QmngrReadMsgDto msgDto;

    @BeforeEach
    void setUp() throws Exception {
        msgDto = QmngrReadMsgDto.builder()
                .topic("test-topic")
                .msgId("123")
                .message(new SerialClob("test message".toCharArray()))
                .headers("{}")
                .build();
    }

    /**
     * Тест успешной отправки сообщения через стандартный KafkaTemplate.
     * Проверяется, что метод send() был вызван 1 раз с корректным ProducerRecord.
     *
     * @throws Exception
     */
    @Test
    void testSendSuccessfully() throws Exception {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                msgDto.getTopic(), null, null, msgDto.getMsgId(), clobToString(msgDto.getMessage()), null);

        when(jsonStringHeadersHandler.toKafkaHeaders(any())).thenReturn(null);
        when(platformDynamicKafkaTemplateService.getKafkaTemplateByTopic(anyString())).thenReturn(null);

        RecordMetadata fakeRecordMetadata = new RecordMetadata(
                new TopicPartition("test-topic", 0), 0, 0, 0, 0, 0
        );

        SendResult<String, String> fakeSendResult = new SendResult<>(producerRecord, fakeRecordMetadata);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(fakeSendResult));

        service.sendAsync(msgDto).block();

        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    private String clobToString(Clob clob) {
        try (var reader = clob.getCharacterStream()) {
            return CharStreams.toString(reader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Тест отправки сообщения через динамический KafkaTemplate, возвращаемый PlatformDynamicKafkaTemplateService.
     * Проверяется, что используется именно динамический KafkaTemplate для отправки сообщения.
     *
     * @throws Exception
     */
    @Test
    void testSendWithPlatformKafkaTemplate() throws Exception {
        KafkaTemplate<String, String> platformKafkaTemplate = mock(KafkaTemplate.class);

        when(jsonStringHeadersHandler.toKafkaHeaders(any())).thenReturn(null);
        when(platformDynamicKafkaTemplateService.getKafkaTemplateByTopic(anyString())).thenReturn(platformKafkaTemplate);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                msgDto.getTopic(), null, null, msgDto.getMsgId(), clobToString(msgDto.getMessage()), null
        );
        RecordMetadata fakeRecordMetadata = new RecordMetadata(
                new TopicPartition("test-topic", 0), 0, 0, 0, 0, 0
        );
        SendResult<String, String> fakeSendResult = new SendResult<>(producerRecord, fakeRecordMetadata);

        when(platformKafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(fakeSendResult));

        service.sendAsync(msgDto).block();

        verify(platformKafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    /**
     * Тест обработки исключения при возникновении ошибки Kafka при отправке сообщения.
     * Проверяется, что выбрасывается исключение QmngrReadMsgSendToMqException.
     */
    @Test
    void testSendThrowsException() {

        when(jsonStringHeadersHandler.toKafkaHeaders(any())).thenReturn(null);
        when(platformDynamicKafkaTemplateService.getKafkaTemplateByTopic(anyString())).thenReturn(null);

        doThrow(RuntimeException.class).when(kafkaTemplate).send(any(ProducerRecord.class));

        assertThrows(QmngrReadMsgSendToMqException.class, () -> service.sendAsync(msgDto).block());
    }

    /**
     * Тест обработки прерывания (InterruptedException) при отправке сообщения.
     * Проверяется, что выбрасывается исключение QmngrReadMsgSendToMqException в случае InterruptedException
     *
     * @throws Exception
     */
    @Test
    void testSendAsyncInterruptedException(){

        when(jsonStringHeadersHandler.toKafkaHeaders(any())).thenReturn(null);
        when(platformDynamicKafkaTemplateService.getKafkaTemplateByTopic(anyString())).thenReturn(null);

        CompletableFuture<SendResult<String, String>> future = CompletableFuture.<SendResult<String, String>>failedFuture(new InterruptedException());
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        assertThrows(QmngrReadMsgSendToMqException.class, () -> {
            try {
                service.sendAsync(msgDto).block();
            } catch (Throwable ex) {
                throw Exceptions.unwrap(ex);
            }
        });
    }
}