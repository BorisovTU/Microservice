package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.AbstractQmngrStoredProcCall;

@RequiredArgsConstructor
public class QmngrProcOutParamsAnswer implements Answer<Void> {

  final Map<String, Object> outParamsMap;

  @Override
  public Void answer(InvocationOnMock invocation) {
    final AbstractQmngrStoredProcCall qmngrProcCall = invocation.getArgument(0);
    TestUtils.setQmngrStoredProcCallOutParams(qmngrProcCall, outParamsMap);
    return null;
  }

}
