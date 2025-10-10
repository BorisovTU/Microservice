package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dao;

import org.springframework.lang.NonNull;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrGetValuesMetricsCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrLoadMsgCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrReadMsgErrorCall;

/**
 * DAO для вызова хранимых процедур SOFR QManager.
 */
public interface QmngrDao {

  void callLoadMsg(@NonNull QmngrLoadMsgCall loadMsgCall);

  void callReadMsgError(@NonNull QmngrReadMsgErrorCall readMsgErrorCall);

  void callGetValuesMetrics(@NonNull QmngrGetValuesMetricsCall getValuesMetricsCall);

}
