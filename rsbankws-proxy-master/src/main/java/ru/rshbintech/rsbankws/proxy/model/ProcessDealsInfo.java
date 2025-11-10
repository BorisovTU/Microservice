package ru.rshbintech.rsbankws.proxy.model;

import lombok.Getter;
import lombok.Setter;
import ru.rshbintech.rsbankws.proxy.model.enums.ActionType;
import ru.rshbintech.rsbankws.proxy.model.enums.SeqType;

@Getter
@Setter
public class ProcessDealsInfo {
    private String reqId;
    private String senderId;
    private String externalId;
    private String dealId;
    private String dealType;
    private String dealKind;
    private ActionType actionType;
    private SeqType seqType;
}
