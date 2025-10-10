package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils;

import java.lang.reflect.Field;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.springframework.lang.NonNull;
import org.springframework.util.ReflectionUtils;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.AbstractQmngrStoredProcCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrLoadMsgCall;

@UtilityClass
public class TestUtils {

  public void setQmngrStoredProcCallOutParams(@NonNull AbstractQmngrStoredProcCall call,
                                              @NonNull Map<String, Object> outParams) {
    final Field field = ReflectionUtils.findField(QmngrLoadMsgCall.class, "outParams");
    if (field != null) {
      field.setAccessible(true);
      ReflectionUtils.setField(field, call, outParams);
    }
  }

}
