package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.listener;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_EMPTY_HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_MESSAGE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_TOPIC_NAME_1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrLoadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.MqType;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.AuditEventParamsBuilderFactory;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.AuditEventSenderService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.logging.MessageLoggingService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.qmngr.QmngrLoadMsgService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.converter.ConsumerRecordToQmngrLoadMsgDtoConverter;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.handler.KafkaConsumerRecordHeadersHandler;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestUtils;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
class QmngrLoadMsgListenerTest {

  private final QmngrLoadMsgService loadMsgService = Mockito.mock(QmngrLoadMsgService.class);
  private final MessageLoggingService messageLoggingService = Mockito.mock(MessageLoggingService.class);
  private final ConsumerRecordToQmngrLoadMsgDtoConverter consumerRecordToQmngrLoadMsgDtoConverter =
      Mockito.mock(ConsumerRecordToQmngrLoadMsgDtoConverter.class);

  private final QmngrLoadMsgListener loadMsgListener;

  QmngrLoadMsgListenerTest() {
    loadMsgListener = new QmngrLoadMsgListener(
        loadMsgService,
        messageLoggingService,
        Mockito.mock(AuditEventSenderService.class),
        Mockito.mock(AuditEventParamsBuilderFactory.class),
        Mockito.mock(KafkaConsumerRecordHeadersHandler.class),
        consumerRecordToQmngrLoadMsgDtoConverter
    );
  }

  @Test
  void testConsumerRecordProcessingSuccess() {
    Mockito.when(consumerRecordToQmngrLoadMsgDtoConverter.convert(ArgumentMatchers.any(ConsumerRecord.class)))
        .thenReturn(QmngrLoadMsgDto.builder().build());

    loadMsgListener.onMessage(
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_1, 0, TEST_VAL_MSG_ID, TEST_VAL_MESSAGE,
            TEST_VAL_EMPTY_HEADERS)
    );

    Mockito.verify(loadMsgService, Mockito.times(1))
        .process(ArgumentMatchers.any(QmngrLoadMsgDto.class), ArgumentMatchers.any(MqType.class));
  }

  @Test
  void testConsumerRecordProcessingError() {
    Mockito.doThrow(new RuntimeException("Consumer record to load msg conversion error"))
        .when(consumerRecordToQmngrLoadMsgDtoConverter)
        .convert(ArgumentMatchers.any(ConsumerRecord.class));

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    loadMsgListener.onMessage(
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_1, 0, TEST_VAL_MSG_ID, TEST_VAL_MESSAGE,
            TEST_VAL_EMPTY_HEADERS)
    );

    Mockito.verify(loadMsgService, Mockito.times(0))
        .process(ArgumentMatchers.any(QmngrLoadMsgDto.class), ArgumentMatchers.any(MqType.class));
  }

}
