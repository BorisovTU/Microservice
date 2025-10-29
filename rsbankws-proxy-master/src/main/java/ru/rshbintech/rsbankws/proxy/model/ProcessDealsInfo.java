package ru.rshbintech.rsbankws.proxy.model;

import lombok.Getter;
import lombok.Setter;
import ru.rshbintech.rsbankws.proxy.model.enums.ActionType;
import ru.rshbintech.rsbankws.proxy.model.enums.SeqType;

@Getter
@Setter
public class ProcessDealsInfo {

    private String reqId;
    private ActionType actionType;
    private String dealId;
    private String senderId;
    private String externalId;
    private String dealType;
    private String dealKind;
    private SeqType seqType;

}
