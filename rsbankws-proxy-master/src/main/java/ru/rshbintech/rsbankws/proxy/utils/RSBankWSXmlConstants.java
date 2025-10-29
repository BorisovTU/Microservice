package ru.rshbintech.rsbankws.proxy.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class RSBankWSXmlConstants {

    public static final String MSG_ERROR_TAG_OR_VALUE_NOT_EXISTS = "Не были переданы входные данные: %s";

    //SOAP Envelope
    public static final String X_PATH_SOAP_ENVELOPE_XML_RPC_CALL = "//Envelope//Body//XMLRPCCall";
    public static final String X_PATH_SOAP_ENVELOPE_TO_METHOD_CALL_XML = "//Envelope//Body//XMLRPCCall//arg0";
    public static final String X_PATH_SOAP_ENVELOPE_TIMEOUT = "//Envelope//Body//XMLRPCCall//arg1";
    public static final String X_PATH_SOAP_ENVELOPE_PRIORITY = "//Envelope//Body//XMLRPCCall//arg2";
    //Method Call
    public static final String X_PATH_METHOD_CALL_REQ_ID_ATTRIBUTE = "//methodCall//@reqId";
    public static final String X_PATH_METHOD_CALL_METHOD_NAME = "//methodCall//methodName";
    public static final String X_PATH_METHOD_CALL_TO_PROCESS_DEALS_XML = "//params//param//value//string";
    //Process Deals
    public static final String X_PATH_PROCESS_DEALS_IDENTITY_DEAL =
            "//IFX//ProcessDeals_req//DealList//DealParm//IdentityDeal";
    public static final String X_PATH_PROCESS_DEALS_ACTION_TYPE = X_PATH_PROCESS_DEALS_IDENTITY_DEAL + "//ActionType";
    public static final String X_PATH_PROCESS_DEALS_DEAL_KIND = X_PATH_PROCESS_DEALS_IDENTITY_DEAL + "//DealKind";
    public static final String X_PATH_PROCESS_DEALS_EXTERNAL_ID = X_PATH_PROCESS_DEALS_IDENTITY_DEAL + "//ExternalID";
    public static final String X_PATH_PROCESS_DEALS_DEAL_ID = X_PATH_PROCESS_DEALS_IDENTITY_DEAL + "//DealID";
    public static final String X_PATH_PROCESS_DEALS_DEAL_DATA = "//IFX//ProcessDeals_req//DealList//DealParm//DealData";
    public static final String X_PATH_PROCESS_DEALS_REQ_ID = "//IFX//ProcessDeals_req//ReqID";
    public static final String X_PATH_PROCESS_DEALS_SENDER_ID = "//IFX//ProcessDeals_req//SenderId";

}
