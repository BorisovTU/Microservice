package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.constants.QmngrConstants.MSG_ID_MAX_LENGTH;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrReadMsgError.UNKNOWN_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrReadMsgError.VALIDATION_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcErrorCode.SUCCESS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.ERROR_CODE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.ERROR_DESC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.MESSAGE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.TOPIC;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import javax.sql.rowset.serial.SerialClob;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.RandomStringUtils;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrLoadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrReadMsgDto;

@UtilityClass
public class TestConstants {

  public static final String TEST_VAL_TOPIC_NAME = "topic.name";
  public static final String TEST_VAL_MSG_ID = RandomStringUtils.randomAlphanumeric(MSG_ID_MAX_LENGTH);
  public static final LocalDateTime TEST_VAL_ESB_DT = LocalDateTime.now(ZoneId.systemDefault()).truncatedTo(MILLIS);
  public static final String TEST_VAL_STRING_HEADERS = "{\"msgId\":\"" + TEST_VAL_MSG_ID + "\"}";
  public static final String TEST_VAL_MESSAGE = "<testMessage><testMessageBlock>" +
      "<testMessageField>testMessageFieldValue</testMessageField></testMessageBlock></testMessage>";
  public static final int TEST_VAL_ERROR_CODE = 12345;
  public static final String TEST_VAL_ERROR_DESC = "Ошибка 12345";
  public static final int TEST_VAL_TRIM_TO_LENGTH = 100;

  public static final QmngrLoadMsgDto TEST_VAL_LOAD_MSG = QmngrLoadMsgDto.builder()
      .topic(TEST_VAL_TOPIC_NAME)
      .msgId(TEST_VAL_MSG_ID)
      .esbDt(TEST_VAL_ESB_DT)
      .headers(TEST_VAL_STRING_HEADERS)
      .message(TEST_VAL_MESSAGE)
      .build();

  public static final Map<String, Object> TEST_VAL_LOAD_MSG_PROC_CALL_OUT_PARAMS_ERROR = Map.of(
      ERROR_CODE.getName(), TEST_VAL_ERROR_CODE,
      ERROR_DESC.getName(), TEST_VAL_ERROR_DESC
  );

  public static final Map<String, Object> TEST_VAL_LOAD_MSG_PROC_CALL_OUT_PARAMS_SUCCESS = Map.of(
      ERROR_CODE.getName(), SUCCESS.getCode(),
      ERROR_DESC.getName(), EMPTY
  );

  public static final QmngrReadMsgDto TEST_VAL_READ_MSG;

  static {
    try {
      TEST_VAL_READ_MSG = QmngrReadMsgDto.builder()
          .topic(TEST_VAL_TOPIC_NAME)
          .msgId(TEST_VAL_MSG_ID)
          .headers(TEST_VAL_STRING_HEADERS)
          .message(new SerialClob(TEST_VAL_MESSAGE.toCharArray()))
          .build();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static final Map<String, Object> TEST_VAL_READ_MSG_PROC_CALL_OUT_PARAMS_ERROR = Map.of(
      TOPIC.getName(), TEST_VAL_TOPIC_NAME,
      MSG_ID.getName(), TEST_VAL_MSG_ID,
      HEADERS.getName(), TEST_VAL_STRING_HEADERS,
      MESSAGE.getName(), TEST_VAL_MESSAGE,
      ERROR_CODE.getName(), TEST_VAL_ERROR_CODE,
      ERROR_DESC.getName(), TEST_VAL_ERROR_DESC
  );

  public static final Map<String, Object> TEST_VAL_READ_MSG_PROC_CALL_OUT_PARAMS_SUCCESS;

  static {
    try {
      TEST_VAL_READ_MSG_PROC_CALL_OUT_PARAMS_SUCCESS = Map.of(
          TOPIC.getName(), TEST_VAL_TOPIC_NAME,
          MSG_ID.getName(), TEST_VAL_MSG_ID,
          HEADERS.getName(), TEST_VAL_STRING_HEADERS,
          MESSAGE.getName(),  new SerialClob(TEST_VAL_MESSAGE.toCharArray()),
          ERROR_CODE.getName(), SUCCESS.getCode(),
          ERROR_DESC.getName(), EMPTY
      );
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static final Map<String, Object> TEST_VAL_PROC_ERROR_CALL_IN_PARAMS_WITH_UNKNOWN_ERROR = Map.of(
      TOPIC.getName(), TEST_VAL_TOPIC_NAME,
      MSG_ID.getName(), TEST_VAL_MSG_ID,
      ERROR_CODE.getName(), UNKNOWN_ERROR.getCode(),
      ERROR_DESC.getName(), UNKNOWN_ERROR.getDescription()
  );

  public static final Map<String, Object> TEST_VAL_PROC_ERROR_CALL_IN_PARAMS_WITH_VALIDATION_ERROR = Map.of(
      TOPIC.getName(), TEST_VAL_TOPIC_NAME,
      MSG_ID.getName(), TEST_VAL_MSG_ID,
      ERROR_CODE.getName(), VALIDATION_ERROR.getCode(),
      ERROR_DESC.getName(), VALIDATION_ERROR.getDescription()
  );

}
