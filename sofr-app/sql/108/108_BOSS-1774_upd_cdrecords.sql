/*Изменения в DCDRECORDS_DBT*/
DECLARE
BEGIN
  UPDATE DCDRECORDS_DBT
     SET T_REQUESTDATE = TRUNC(T_REQUESTTIME)
   WHERE T_REQUESTDATE = TO_DATE('01.01.0001','DD.MM.YYYY') OR T_REQUESTDATE IS NULL;  
END;
/

DECLARE
BEGIN
  UPDATE DCDRECORDS_DBT
     SET T_GUID                   = NVL(T_GUID                  , CHR(1)),
         T_REQUESTTIME            = NVL(T_REQUESTTIME           , TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS')),
         T_SHORTNAME              = NVL(T_SHORTNAME             , CHR(1)),
         T_FULLNAME               = NVL(T_FULLNAME              , CHR(1)),
         T_AGREEMENTNUMBER        = NVL(T_AGREEMENTNUMBER       , CHR(1)),
         T_ISIIS                  = NVL(T_ISIIS                 , CHR(0)),
         T_AGREEMENTOPENDATE      = NVL(T_AGREEMENTOPENDATE     , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_AGREEMENTCLOSEDATE     = NVL(T_AGREEMENTCLOSEDATE    , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_CORPORATEACTIONTYPE    = NVL(T_CORPORATEACTIONTYPE   , CHR(1)),
         T_PAYMENTTYPE            = NVL(T_PAYMENTTYPE           , CHR(1)),
         T_RECORDPAYMENTID        = NVL(T_RECORDPAYMENTID       , 0),
         T_RECORDPAYMENTQTYID     = NVL(T_RECORDPAYMENTQTYID    , 0),
         T_OPERATIONSTATUS        = NVL(T_OPERATIONSTATUS       , CHR(1)),
         T_PAYMENTDATE            = NVL(T_PAYMENTDATE           , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_CLIENTID_OBJECTID      = NVL(T_CLIENTID_OBJECTID     , CHR(1)),
         T_CLIENTID_SYSTEMID      = NVL(T_CLIENTID_SYSTEMID     , CHR(1)),
         T_CLIENTID_SYSTEMNODEID  = NVL(T_CLIENTID_SYSTEMNODEID , CHR(1)),
         T_FINANCIALNAME          = NVL(T_FINANCIALNAME         , CHR(1)),
         T_ISINREGISTRATIONNUMBER = NVL(T_ISINREGISTRATIONNUMBER, CHR(1)),
         T_RECORDSNOBID           = NVL(T_RECORDSNOBID          , CHR(1)),
         T_TAXRATE                = NVL(T_TAXRATE               , 0),
         T_TAXBASE                = NVL(T_TAXBASE               , 0),
         T_ISSUERCURRENCY         = NVL(T_ISSUERCURRENCY        , CHR(1)),
         T_ISSUERSUM              = NVL(T_ISSUERSUM             , 0),
         T_CLIENTSUM              = NVL(T_CLIENTSUM             , 0),
         T_CLIENTCURRENCY         = NVL(T_CLIENTCURRENCY        , CHR(1)),
         T_KBK                    = NVL(T_KBK                   , CHR(1)),
         T_RETURNTAX              = NVL(T_RETURNTAX             , 0),
         T_INDIVIDUALTAX          = NVL(T_INDIVIDUALTAX         , 0),
         T_TAXREDUCTIONSUM        = NVL(T_TAXREDUCTIONSUM       , 0),
         T_SUMD1                  = NVL(T_SUMD1                 , 0),
         T_SUMD2                  = NVL(T_SUMD2                 , 0),
         T_OFFSETTAX              = NVL(T_OFFSETTAX             , 0),
         T_ACCOUNTNUMBER          = NVL(T_ACCOUNTNUMBER         , CHR(1)),
         T_OPERATIONDATE          = NVL(T_OPERATIONDATE         , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_ISGETTAX               = NVL(T_ISGETTAX              , CHR(0)),
         T_COUPONNUMBER           = NVL(T_COUPONNUMBER          , 0),
         T_COUPONSTARTDATE        = NVL(T_COUPONSTARTDATE       , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_COUPONENDDATE          = NVL(T_COUPONENDDATE         , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_PROCRESULT             = NVL(T_PROCRESULT            , CHR(1)),
         T_PAYRECEIVEDDATE        = NVL(T_PAYRECEIVEDDATE       , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_FIXINGDATE             = NVL(T_FIXINGDATE            , TO_DATE('01.01.0001','DD.MM.YYYY')),
         T_QUANTITY               = NVL(T_QUANTITY              , 0);
END;
/

