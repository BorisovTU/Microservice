package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.MILLIS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.constants.QmngrConstants.MSG_ID_MAX_LENGTH;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

@UtilityClass
public class TestConstants {

  public static final String TEST_VAL_TOPIC_NAME_1 = "topic1";
  public static final String TEST_VAL_TOPIC_NAME_2 = "topic2";
  public static final String TEST_VAL_TOPIC_NAME_3 = "topic3";

  public static final String TEST_VAL_MSG_ID = RandomStringUtils.randomAlphanumeric(MSG_ID_MAX_LENGTH);

  public static final LocalDateTime TEST_VAL_ESB_DT = LocalDateTime.now(ZoneId.systemDefault()).truncatedTo(MILLIS);
  public static final long TEST_VAL_ESB_DT_TIMESTAMP = Timestamp.valueOf(TEST_VAL_ESB_DT).getTime();

  public static final String TEST_VAL_HEADER_NAME_MSG_ID = "msgId";
  public static final RecordHeaders TEST_VAL_EMPTY_HEADERS = new RecordHeaders();
  public static final RecordHeaders TEST_VAL_HEADERS_WITH_MSG_ID =
      new RecordHeaders(
          new Header[] {new RecordHeader(TEST_VAL_HEADER_NAME_MSG_ID, StringUtils.getBytes(TEST_VAL_MSG_ID, UTF_8))}
      );
  public static final String TEST_VAL_STRING_HEADERS =
      "{\"" + TEST_VAL_HEADER_NAME_MSG_ID + "\":\"" + TEST_VAL_MSG_ID + "\"}";

  public static final String TEST_VAL_MESSAGE = "<testMessage><testMessageBlock>" +
      "<testMessageField>testMessageFieldValue</testMessageField></testMessageBlock></testMessage>";

}
