package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.converter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.constants.QmngrConstants.MSG_ID_MAX_LENGTH;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_EMPTY_HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_ESB_DT;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_ESB_DT_TIMESTAMP;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_HEADERS_WITH_MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_HEADER_NAME_MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_MESSAGE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_STRING_HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_TOPIC_NAME_1;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_TOPIC_NAME_2;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_TOPIC_NAME_3;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.ReflectionUtils;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrLoadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.configuration.properties.KafkaConsumersProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.configuration.properties.KafkaConsumersProperties.QmngrLoadMsgConsumerProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.handler.KafkaConsumerRecordHeadersHandler;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestUtils;

@ExtendWith(MockitoExtension.class)
class ConsumerRecordToQmngrLoadMsgDtoConverterTest {

  final ConsumerRecordToQmngrLoadMsgDtoConverter consumerRecordToQmngrLoadMsgDtoConverter;

  public ConsumerRecordToQmngrLoadMsgDtoConverterTest() {
    this.consumerRecordToQmngrLoadMsgDtoConverter = new ConsumerRecordToQmngrLoadMsgDtoConverter(
        createKafkaConsumersProperties(),
        new KafkaConsumerRecordHeadersHandler(new ObjectMapper())
    );
    callInitMethod();
  }

  @Test
  void testConvertAndFillTopic() {
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_1, 0, null, EMPTY, TEST_VAL_EMPTY_HEADERS);
    final QmngrLoadMsgDto loadMsgDto = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME_1, loadMsgDto.getTopic());
  }

  @Test
  void testConvertAndFillMsgIdByConsumerRecordKey() {
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_1, 0, TEST_VAL_MSG_ID, EMPTY, TEST_VAL_EMPTY_HEADERS);
    final QmngrLoadMsgDto loadMsgDto = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    Assertions.assertEquals(TEST_VAL_MSG_ID, loadMsgDto.getMsgId());
  }

  @Test
  void testConvertAndFillMsgIdByEmptyConsumerRecordKey() {
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_1, 0, null, EMPTY, TEST_VAL_EMPTY_HEADERS);
    final QmngrLoadMsgDto loadMsgDto = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    Assertions.assertNull(loadMsgDto.getMsgId());
  }

  @Test
  void testConvertAndFillMsgIdByConsumerRecordKeyAndCutTo128Symbols() {
    final String extra = RandomStringUtils.randomAlphanumeric(MSG_ID_MAX_LENGTH);
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_1, 0, TEST_VAL_MSG_ID + extra, EMPTY,
            TEST_VAL_EMPTY_HEADERS);
    final QmngrLoadMsgDto loadMsgDto = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    Assertions.assertEquals(TEST_VAL_MSG_ID, loadMsgDto.getMsgId());
  }

  @Test
  void testConvertAndFillMsgIdByConsumerRecordHeaders() {
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_2, 0, null, EMPTY, TEST_VAL_HEADERS_WITH_MSG_ID);
    final QmngrLoadMsgDto loadMsgDto = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    Assertions.assertEquals(TEST_VAL_MSG_ID, loadMsgDto.getMsgId());
  }

  @Test
  void testConvertAndFillMsgIdByConsumerRecordHeadersAndCutTo128Symbols() {
    final String extra = RandomStringUtils.randomAlphanumeric(MSG_ID_MAX_LENGTH);
    RecordHeaders headers = new RecordHeaders();
    headers.add(new RecordHeader(TEST_VAL_HEADER_NAME_MSG_ID, StringUtils.getBytes(TEST_VAL_MSG_ID + extra, UTF_8)));
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_2, 0, null, EMPTY, headers);
    final QmngrLoadMsgDto loadMsgDto = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    Assertions.assertEquals(TEST_VAL_MSG_ID, loadMsgDto.getMsgId());
  }

  @Test
  void testConvertAndFillMsgIdByConsumerRecordHeadersWithAbsentMsgIdHeader() {
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_2, 0, null, EMPTY, TEST_VAL_EMPTY_HEADERS);
    final QmngrLoadMsgDto loadMsgDto = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    Assertions.assertNull(loadMsgDto.getMsgId());
  }

  @Test
  void testConvertAndFillMsgIdByEmptySettingsForTopic() {
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_3, 0, null, EMPTY, TEST_VAL_EMPTY_HEADERS);
    final QmngrLoadMsgDto loadMsgDto = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    Assertions.assertNull(loadMsgDto.getMsgId());
  }

  @Test
  void testConvertAndFillEsbDt() {
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_1, TEST_VAL_ESB_DT_TIMESTAMP, null, EMPTY,
            TEST_VAL_EMPTY_HEADERS);
    final QmngrLoadMsgDto loadMsgDto = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    Assertions.assertEquals(TEST_VAL_ESB_DT, loadMsgDto.getEsbDt());
  }

  @Test
  void testConvertAndFillEsbDtByZeroTimestamp() {
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_1, 0, null, EMPTY, TEST_VAL_EMPTY_HEADERS);
    final QmngrLoadMsgDto loadMsgDto = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    Assertions.assertNull(loadMsgDto.getEsbDt());
  }

  @Test
  void testConvertAndFillHeadersByEmptyHeaders() {
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_1, 0, null, EMPTY, TEST_VAL_EMPTY_HEADERS);
    final QmngrLoadMsgDto loadMsgDto = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    Assertions.assertNull(loadMsgDto.getHeaders());
  }

  @Test
  void testConvertAndFillHeaders() {
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_1, 0, null, EMPTY, TEST_VAL_HEADERS_WITH_MSG_ID);
    final QmngrLoadMsgDto loadMsgDto = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    Assertions.assertEquals(TEST_VAL_STRING_HEADERS, loadMsgDto.getHeaders());
  }

  @Test
  void testConvertAndFillMessage() {
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_1, 0, null, TEST_VAL_MESSAGE, TEST_VAL_EMPTY_HEADERS);
    final QmngrLoadMsgDto loadMsgDto = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    Assertions.assertEquals(TEST_VAL_MESSAGE, loadMsgDto.getMessage());
  }

  private KafkaConsumersProperties createKafkaConsumersProperties() {
    List<QmngrLoadMsgConsumerProperties> loadMsgProperties = new ArrayList<>();
    loadMsgProperties.add(createLoadMsgConsumerProperties(TEST_VAL_TOPIC_NAME_1, true, null));
    loadMsgProperties.add(createLoadMsgConsumerProperties(TEST_VAL_TOPIC_NAME_2, false, TEST_VAL_HEADER_NAME_MSG_ID));
    KafkaConsumersProperties kafkaConsumersProperties = new KafkaConsumersProperties();
    kafkaConsumersProperties.setLoadMsg(loadMsgProperties);
    return kafkaConsumersProperties;
  }

  private QmngrLoadMsgConsumerProperties createLoadMsgConsumerProperties(@NonNull String topic,
                                                                         boolean useKeyAsMsgId,
                                                                         @Nullable String msgIdHeaderName) {
    QmngrLoadMsgConsumerProperties loadMsgConsumerProperties = new QmngrLoadMsgConsumerProperties();
    loadMsgConsumerProperties.setTopic(topic);
    loadMsgConsumerProperties.setUseKeyAsMsgId(useKeyAsMsgId);
    loadMsgConsumerProperties.setMsgIdHeaderName(msgIdHeaderName);
    return loadMsgConsumerProperties;
  }

  private void callInitMethod() {
    final Method initMethod = ReflectionUtils.findMethod(ConsumerRecordToQmngrLoadMsgDtoConverter.class, "init");
    if (initMethod != null) {
      initMethod.setAccessible(true);
      ReflectionUtils.invokeMethod(initMethod, this.consumerRecordToQmngrLoadMsgDtoConverter);
    }
  }

}
