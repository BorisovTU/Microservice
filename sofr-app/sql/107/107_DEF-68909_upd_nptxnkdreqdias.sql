/*Изменения в DCDRECORDS_DBT*/
DECLARE
BEGIN
  UPDATE DNPTXNKDREQDIAS_DBT
     SET T_GUID                 = NVL(T_GUID                , CHR(1)),
         T_GUIDRESP             = NVL(T_GUIDRESP            , CHR(1)),
         T_REQUESTDATE          = NVL(T_REQUESTDATE         , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_REQUESTTIME          = NVL(T_REQUESTTIME         , TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS')),
         T_PAYMENTID            = NVL(T_PAYMENTID           , CHR(1)),
         T_PAYMENTACTION        = NVL(T_PAYMENTACTION       , 0),
         T_FIXINGDATE           = NVL(T_FIXINGDATE          , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_CLIENTID             = NVL(T_CLIENTID            , CHR(1)),
         T_AGREEMENTNUMBER      = NVL(T_AGREEMENTNUMBER     , CHR(1)),
         T_ISIIS                = NVL(T_ISIIS               , CHR(0)),
         T_MARKETPLACE          = NVL(T_MARKETPLACE         , CHR(1)),
         T_ISINREGNUMBER        = NVL(T_ISINREGNUMBER       , CHR(1)),
         T_COUPONNUMBER         = NVL(T_COUPONNUMBER        , CHR(1)),
         T_COUPONSTARTDATE      = NVL(T_COUPONSTARTDATE     , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_COUPONENDDATE        = NVL(T_COUPONENDDATE       , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_CREATEDATE           = NVL(T_CREATEDATE          , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_CREATETIME           = NVL(T_CREATETIME          , TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS')),
         T_CHANGEDATE           = NVL(T_CHANGEDATE          , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_CHANGETIME           = NVL(T_CHANGETIME          , TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS')),
         T_ERRORCODE            = NVL(T_ERRORCODE           , 0),
         T_ERROR                = NVL(T_ERROR               , CHR(1)),
         T_SUM                  = NVL(T_SUM                 , 0),
         T_PARTYID              = NVL(T_PARTYID             , -1),
         T_CONTRACTID           = NVL(T_CONTRACTID          , 0),
         T_FIID                 = NVL(T_FIID                , -1),
         T_RECEIVEDCOUPONAMOUNT = NVL(T_RECEIVEDCOUPONAMOUNT, 0);
END;
/

