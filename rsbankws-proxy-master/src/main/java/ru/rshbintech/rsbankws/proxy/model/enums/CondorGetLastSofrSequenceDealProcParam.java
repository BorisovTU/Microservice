package ru.rshbintech.rsbankws.proxy.model.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum CondorGetLastSofrSequenceDealProcParam {

    REQ_TYPE("ReqType"),
    SEQ_TYPE("SeqType"),
    DEAL_CODE("DealCode"),
    REQUEST_ID("RequestId"),
    SEQ_ID("SeqId"),
    ERROR_MESSAGE("ErrorMessage");

    private final String name;

}