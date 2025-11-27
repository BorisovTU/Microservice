CREATE OR REPLACE PACKAGE BODY rsb_brkrep_rshb
IS 
-- Author  : Nikonorov Evgeny
-- Created : 28.05.2021
-- Purpose : Пакет для подготовки данных для отчета брокера РСХБ

  g_BrokerComissFIID NUMBER := -1;
  g_PrevMarketID NUMBER := -1;
  g_PrevCodeKind NUMBER := 1;
   
-- DAN пользовательская функция возвращает для субъекта псевдоним вида 4. Дополнительное наименование, если его нет то код вида 1
FUNCTION uGetPatyNameForBrkRep(pPartyID IN NUMBER)
    return varchar2
AS
    ptname varchar2(100);
BEGIN
  begin 
    select t_name into ptname from dpartyname_dbt where t_partyid = pPartyID and T_NAMETYPEID = 4 and rownum = 1;
   exception when no_data_found then
    ptname := RSI_RSBPARTY.GetPartyCode(pPartyID, 1 /*PTCK_CONTR*/);
  end; 
   return ptname;
END;  

  --получить сумму НКД в сделке на корзину
FUNCTION GetBasketNKDOnDate (DealID IN NUMBER, pDate IN DATE)
    return NUMBER
AS
    NKD NUMBER;
BEGIN
    NKD:=0;
    FOR One IN
    (
    SELECT SUM( tEns.T_NKD * tEns.T_PRINCIPAL ) AS t_NKD
    FROM ddl_tick_ens_dbt tEns
    WHERE tEns.t_DealID = DealID AND tEns.t_Date <= pDate
    )
    LOOP
        NKD := One.t_NKD;
        EXIT;
    END LOOP;
    RETURN NKD;
END;

--получить кол-во ценных бумаг на дату
FUNCTION GetRQAmountSecuritiesOnDate(pRqDocKind IN NUMBER, pRqDocID IN NUMBER, pDealPart IN NUMBER, pDate IN DATE)
    return NUMBER
AS
  Amount NUMBER;
BEGIN
  Amount := 0;

  FOR one_rq IN
  (
    SELECT SUM(RSI_DLRQ.GetRQAmountOnDate (rq.t_ID, pDate)) as t_Amount
      FROM ddlrq_dbt rq
     WHERE rq.t_DocKind  = pRqDocKind
       AND rq.t_DocID    = pRqDocID--id сделки
       AND rq.t_SubKind  = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS--1 --цб
       AND rq.t_DealPart = pDealPart
       AND (rq.t_Type = RSI_DLRQ.DLRQ_TYPE_DELIVERY --поставка
         OR rq.t_Type = (CASE pDealPart WHEN 1 THEN  RSI_DLRQ.DLRQ_TYPE_COMPDELIVERY ELSE RSI_DLRQ.DLRQ_TYPE_DELIVERY END))
       --если dealpart=2, то только тип 8 (поставка)
       --если dealpart=1, то поставка + комп поставка 8+9
  )
  LOOP

    Amount := one_rq.t_amount;
    EXIT;
  END LOOP;

  RETURN Amount;
END;


--получить кол-во денежных средств на часть сделки на дату
FUNCTION GetRQAmountCashOnDate(pRqDocKind IN NUMBER,pRqDocID IN NUMBER, pDealPart IN NUMBER, pDate IN DATE)
   RETURN NUMBER
AS
  Amount   NUMBER;
BEGIN
  Amount := 0;
  FOR one_rq IN
  (
    SELECT SUM(RSI_DLRQ.GetRQAmountOnDate(rq.t_ID, pDate)) as t_Amount
      FROM ddlrq_dbt rq
     WHERE rq.t_DocKind  = pRqDocKind
       AND rq.t_DocID    = pRqDocID--id сделки
       AND rq.t_SubKind  = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY--0 --дс
       AND rq.t_DealPart = pDealPart
       AND (rq.t_Type = RSI_DLRQ.DLRQ_TYPE_PAYMENT
         OR rq.t_Type = (CASE pDealPart WHEN 1 THEN RSI_DLRQ.DLRQ_TYPE_PAYMENT ELSE RSI_DLRQ.DLRQ_TYPE_INCREPO END))--оплата + проценты по репо
  )
  LOOP
    Amount := one_rq.t_amount;
    EXIT;
  END LOOP;

  RETURN Amount;
END;

--Получить идентификатор тарифного плана по договору обслуживания на дату
FUNCTION GetSfPlanID(p_ContrID IN NUMBER, p_Date IN DATE) RETURN NUMBER
AS

 v_PlanID NUMBER;
BEGIN

  SELECT pl.t_SfPlanID INTO v_PlanID
    FROM dsfcontrplan_dbt pl
   WHERE pl.t_SfContrID = p_ContrID
     AND pl.t_Begin <= p_Date
     AND (pl.t_End >= p_Date or pl.t_End = TO_DATE('01.01.0001','DD.MM.YYYY'));

  RETURN v_PlanID;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;
END GetSfPlanID;

--Получить плановую дату исполнения части сделки (максимальная из плановых и фактических по ТО)
FUNCTION GetPlanExecDate(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN DATE
AS

 v_PlanExecDate DATE;
BEGIN
  SELECT NVL(MAX(DECODE(pm.t_FactDate, TO_DATE('01.01.0001','DD.MM.YYYY'), pm.t_PlanDate, pm.t_FactDate)), TO_DATE('01.01.0001','DD.MM.YYYY')) INTO v_PlanExecDate
    FROM DDLRQ_DBT pm
   WHERE pm.t_DocKind  = p_DocKind
     AND pm.t_DocID    = p_DocID
     AND pm.t_Type     IN (RSI_DLRQ.DLRQ_TYPE_DELIVERY, RSI_DLRQ.DLRQ_TYPE_PAYMENT)
     AND pm.t_DealPart = p_Part;

  RETURN v_PlanExecDate;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN TO_DATE('01.01.0001','DD.MM.YYYY');

END GetPlanExecDate;

--Получить фактическую дату исполнения части сделки (максимальная из фактических по ТО)
FUNCTION GetExecDate(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN DATE
AS

 v_ExecDate DATE;
BEGIN
  SELECT NVL(MAX(pm.t_FactDate), TO_DATE('01.01.0001','DD.MM.YYYY')) INTO v_ExecDate
    FROM DDLRQ_DBT pm
   WHERE pm.t_DocKind  = p_DocKind
     AND pm.t_DocID    = p_DocID
     AND pm.t_Type     IN (RSI_DLRQ.DLRQ_TYPE_DELIVERY, RSI_DLRQ.DLRQ_TYPE_PAYMENT)
     AND pm.t_DealPart = p_Part
     AND pm.t_FactDate > TO_DATE('01.01.0001','DD.MM.YYYY')
     AND NOT Exists(SELECT 1
                      FROM ddlrq_dbt pm1
                     WHERE pm1.t_DocKind = pm.t_DocKind
                       AND pm1.t_DocID = pm.t_DocID
                       AND pm1.t_DealPart = pm.t_DealPart
                       AND pm1.t_Type IN (RSI_DLRQ.DLRQ_TYPE_DELIVERY, RSI_DLRQ.DLRQ_TYPE_PAYMENT)
                       AND pm1.t_FactDate = TO_DATE('01.01.0001','DD.MM.YYYY')
                   );

  RETURN v_ExecDate;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN TO_DATE('01.01.0001','DD.MM.YYYY');

END GetExecDate;

--Получить цену сделки
FUNCTION GetPrice(p_DealID IN NUMBER, p_Part IN NUMBER) RETURN FLOAT
AS

 v_Price FLOAT;
BEGIN

  SELECT (CASE WHEN leg.t_RelativePrice = 'X' THEN RSI_RSB_FIInstr.ConvSum( (leg.t_Price * RSI_RSB_FIInstr.FI_GetNominalOnDate(leg.t_PFI, tk.t_DealDate) / 100.0), fin.t_FaceValueFI, leg.t_CFI, tk.t_DealDate)
               ELSE leg.t_Price END
         )
    INTO v_Price
    FROM ddl_tick_dbt tk, ddl_leg_dbt leg, dfininstr_dbt fin
   WHERE tk.t_DealID = p_DealID
     AND leg.t_DealID = tk.t_DealID
     AND leg.t_LegKind = DECODE(p_Part, 1, 0, 2)
     AND leg.t_LegID = 0
     AND fin.t_FIID = leg.t_PFI;

  RETURN v_Price;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0.0;

END GetPrice;

--Получить цену поручения
FUNCTION GetReqPrice(p_DealID IN NUMBER) RETURN FLOAT
AS

 v_Price FLOAT;
BEGIN

  SELECT (CASE WHEN req.t_PriceType = 2 THEN RSI_RSB_FIInstr.ConvSum( (req.t_Price * RSI_RSB_FIInstr.FI_GetNominalOnDate(req.t_FIID, req.t_Date) / 100.0), fin.t_FaceValueFI, req.t_PriceFIID, req.t_Date)
               ELSE req.t_Price END
         )
    INTO v_Price
    FROM ddl_tick_dbt tk, dspground_dbt ground, dspgrdoc_dbt dealdoc, dspgrdoc_dbt reqdoc, ddl_req_dbt req, dfininstr_dbt fin
   WHERE tk.t_DealID = p_DealID
     AND dealdoc.t_sourcedocid = tk.t_DealID
     AND dealdoc.t_sourcedockind = tk.t_BOfficeKind
     AND ground.t_spgroundid = dealdoc.t_spgroundid
     AND ground.t_spgroundid = reqdoc.t_spgroundid
     AND dealdoc.t_sourcedocid != reqdoc.t_sourcedocid
     AND dealdoc.t_sourcedockind != reqdoc.t_sourcedockind
     AND reqdoc.t_sourcedocid = req.t_id
     AND reqdoc.t_sourcedockind = req.t_kind
     AND req.t_client = tk.t_ClientID 
     AND fin.t_FIID = req.t_FIID
     AND rownum = 1;

  RETURN v_Price;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN NULL;
END GetReqPrice;

--Получить сумму обязательств
FUNCTION GetCommitSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER, p_SubKind IN NUMBER, p_CFI IN NUMBER, p_Date IN DATE) RETURN NUMBER
AS

 v_Sum NUMBER;
BEGIN

  SELECT NVL(SUM(DECODE(p_SubKind, RSI_DLRQ.DLRQ_SUBKIND_CURRENCY,
                        RSI_RSB_FIInstr.ConvSum(rq.t_Amount, rq.t_FIID, p_CFI, p_Date),
                        rq.t_Amount)
                ), 0) INTO v_Sum
    FROM ddlrq_dbt rq
   WHERE rq.t_DocKind  = p_DocKind
     AND rq.t_DocID    = p_DocID
     AND rq.t_DealPart = p_Part
     AND rq.t_SubKind  = p_SubKind
     AND rq.t_Kind     = RSI_DLRQ.DLRQ_KIND_COMMIT;

  RETURN v_Sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;

END GetCommitSum;


--Получить сумму комиссии брокера
FUNCTION GetBrokerComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER
AS

 v_Sum  NUMBER;
 v_FIID NUMBER;
BEGIN

  g_BrokerComissFIID := -1;

  IF p_Part = 1 THEN
    SELECT q1.CommSum, q1.FIID_COMM INTO v_Sum, v_FIID
      FROM (SELECT NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
              FROM (SELECT dlc.t_Sum AS CommSum, dlc.t_FeeType, dlc.t_ComNumber
                      FROM ddlcomis_dbt dlc
                     WHERE dlc.t_DocKind = p_DocKind
                       AND dlc.t_DocID = p_DocID
                       AND dlc.t_IsBack = CHR(0)
                    UNION
                    SELECT basobj.t_CommSum AS CommSum, defcom.t_FeeType, defcom.t_CommNumber
                      FROM dsfbasobj_dbt basobj, dsfdefcom_dbt defcom
                     WHERE basobj.t_BaseObjectType = p_DocKind
                       AND basobj.t_BaseObjectID = p_DocID
                       AND defcom.t_ID = basobj.t_DefCommID
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_FeeType = q.t_Feetype
               AND cm.t_Number  = q.t_ComNumber
               AND cm.t_ReceiverID = RsbSessionData.OurBank
                 AND LOWER(cm.t_Code) not IN (LOWER('БрокерКомпенсац_RUR'), LOWER('БрокерКомпенсац_USD'), LOWER('БрокерКомпенсац_EUR'), LOWER('БрокерКомпенсац_CHF'),
                                          LOWER('БрокерКомпенсацВ_RUR'), LOWER('БрокерКомпенсацВ_USD'), LOWER('БрокерКомпенсацВ_EUR'), LOWER('БрокерКомпенсацВ_CHF')
                                         )
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  ELSE

    SELECT q1.CommSum, q1.FIID_COMM INTO v_Sum, v_FIID
      FROM (SELECT NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
              FROM (SELECT dlc.t_Sum AS CommSum, dlc.t_FeeType, dlc.t_ComNumber
                      FROM ddlcomis_dbt dlc
                     WHERE dlc.t_DocKind = p_DocKind
                       AND dlc.t_DocID = p_DocID
                       AND dlc.t_IsBack = 'X'
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_FeeType = q.t_Feetype
               AND cm.t_Number  = q.t_ComNumber
               AND cm.t_ReceiverID = RsbSessionData.OurBank
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  END IF;

  g_BrokerComissFIID := v_FIID;

  RETURN v_Sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;

END GetBrokerComissSum;

--Получить валюту комиссии брокера (обязательно после вызова GetBrokerComissSum)
FUNCTION GetBrokerComissFIID(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER
AS
BEGIN
  RETURN g_BrokerComissFIID;
END GetBrokerComissFIID;

--Получить сумму комиссий торговой площадке
FUNCTION GetMarketComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER, p_MarketID IN NUMBER, p_DealDate IN DATE) RETURN NUMBER
AS

 v_Sum  NUMBER := 0;
 v_cnt  NUMBER := 0;
BEGIN

  IF p_DocKind = RSB_SECUR.DL_SECURITYDOC AND RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(p_DocID, 34, '0'), 53, p_DealDate) = 2 THEN --категория "Первичное размещение" = Да
    v_Sum := 0;
  ELSE

    SELECT COUNT(1) INTO v_cnt
                      FROM ddlcomis_dbt dlc
                     WHERE dlc.t_DocKind = p_DocKind
                       AND dlc.t_DocID = p_DocID
       AND dlc.t_IsBankExpenses = 'X'
       AND ROWNUM = 1;

    --Если на сделке клиента в любой биржевой  комиссии  установлен признак "Отнесение на расходы банка" (т.е. плательщик = банк), то
    --сумма Компенсационной комиссии "БрокерКомпенсац"
    IF v_cnt > 0 THEN
    SELECT q1.CommSum INTO v_Sum
      FROM (SELECT NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
              FROM (SELECT dlc.t_Sum AS CommSum, dlc.t_FeeType, dlc.t_ComNumber
                      FROM ddlcomis_dbt dlc
                     WHERE dlc.t_DocKind = p_DocKind
                       AND dlc.t_DocID = p_DocID
                         AND dlc.t_IsBack = (CASE WHEN p_Part = 1 THEN CHR(0) ELSE 'X' END)
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_FeeType = q.t_Feetype
               AND cm.t_Number  = q.t_ComNumber
                 AND LOWER(cm.t_Code) IN (LOWER('БрокерКомпенсац_RUR'), LOWER('БрокерКомпенсац_USD'), LOWER('БрокерКомпенсац_EUR'), LOWER('БрокерКомпенсац_CHF'),
                                          LOWER('БрокерКомпенсацВ_RUR'), LOWER('БрокерКомпенсацВ_USD'), LOWER('БрокерКомпенсацВ_EUR'), LOWER('БрокерКомпенсацВ_CHF')
                                         )
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;
    ELSE
  IF p_Part = 1 THEN
    SELECT q1.CommSum INTO v_Sum
      FROM (SELECT NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
              FROM (SELECT dlc.t_Sum AS CommSum, dlc.t_FeeType, dlc.t_ComNumber
                      FROM ddlcomis_dbt dlc
                     WHERE dlc.t_DocKind = p_DocKind
                       AND dlc.t_DocID = p_DocID
                       AND dlc.t_IsBack = CHR(0)
                    UNION
                    SELECT basobj.t_CommSum AS CommSum, defcom.t_FeeType, defcom.t_CommNumber
                      FROM dsfbasobj_dbt basobj, dsfdefcom_dbt defcom
                     WHERE basobj.t_BaseObjectType = p_DocKind
                       AND basobj.t_BaseObjectID = p_DocID
                       AND defcom.t_ID = basobj.t_DefCommID
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_FeeType = q.t_Feetype
               AND cm.t_Number  = q.t_ComNumber
                   AND cm.t_ReceiverID = p_MarketID
                   AND LOWER(cm.t_Code) IN (LOWER('МскБиржНов'),
                                            LOWER('МскБиржКлирНов'),
                                            LOWER('МскБиржИТСНов'),
                                            LOWER('МскБиржВ'),
                                            LOWER('МскБиржКлирВ'),
                                            LOWER('МскБиржИТСВ'),
                                            LOWER('СПбБирж'),
                                            LOWER('СПбБиржКлир'),
                                            LOWER('СПбБиржИТС')
                                           )
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  ELSE

    SELECT q1.CommSum INTO v_Sum
      FROM (SELECT NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
              FROM (SELECT dlc.t_Sum AS CommSum, dlc.t_FeeType, dlc.t_ComNumber
                      FROM ddlcomis_dbt dlc
                     WHERE dlc.t_DocKind = p_DocKind
                       AND dlc.t_DocID = p_DocID
                       AND dlc.t_IsBack = 'X'
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_FeeType = q.t_Feetype
               AND cm.t_Number  = q.t_ComNumber
                   AND cm.t_ReceiverID = p_MarketID
                   AND LOWER(cm.t_Code) IN (LOWER('МскБиржНов'),
                                            LOWER('МскБиржКлирНов'),
                                            LOWER('МскБиржИТСНов'),
                                            LOWER('МскБиржВ'),
                                            LOWER('МскБиржКлирВ'),
                                            LOWER('МскБиржИТСВ'),
                                            LOWER('СПбБирж'),
                                            LOWER('СПбБиржКлир'),
                                            LOWER('СПбБиржИТС')
                                           )
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  END IF;
    END IF;
  END IF;

  RETURN v_Sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;

END GetMarketComissSum;

--Получить сумму комиссий торговой площадке по срочному рынку
FUNCTION DV_GetMarketComissSum(p_DealID IN NUMBER, p_MarketID IN NUMBER, p_DealDate IN DATE) RETURN NUMBER
AS

 v_Sum  NUMBER := 0;
 v_cnt  NUMBER := 0;
BEGIN

  SELECT COUNT(1) INTO v_cnt
    FROM ddvdlcom_dbt dvdlcom
   WHERE dvdlcom.t_DealID = p_DealID
     AND dvdlcom.t_isBankExpenses = 'X'
     AND ROWNUM = 1;
        
  --Если на сделке клиента в любой биржевой  комиссии  установлен признак "Отнесение на расходы банка" (т.е. плательщик = банк), то
  --сумма Компенсационной комиссии "БрокерКомпенсацС"
  IF v_cnt > 0 THEN
    SELECT q1.CommSum INTO v_Sum
      FROM (SELECT NVL(SUM(RSI_RSB_FIInstr.ConvSum(q.CommSum, cm.t_FIID_COMM, RSI_RSB_FIInstr.NATCUR, p_DealDate)),0) AS CommSum
              FROM (SELECT dvdlcom.t_Sum AS CommSum, dvdlcom.t_ComissID
                      FROM ddvdlcom_dbt dvdlcom
                     WHERE dvdlcom.t_DealID = p_DealID
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_ComissID  = q.t_ComissID
               AND LOWER(cm.t_Code) IN (LOWER('БрокерКомпенсацС'))) q1;
  ELSE --Сумма биржевых комиссий
    SELECT NVL(SUM(RSI_RSB_FIInstr.ConvSum(dvdlcom.t_Sum, cm.t_FIID_COMM, RSI_RSB_FIInstr.NATCUR, p_DealDate)),0) INTO v_Sum
      FROM ddvdlcom_dbt dvdlcom, dsfcomiss_dbt cm 
     WHERE dvdlcom.t_DealID   = p_DealID 
       AND dvdlcom.t_ComissID = cm.t_ComissID 
       AND cm.t_ReceiverID    = p_MarketID;
  END IF;

  RETURN v_Sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;

END DV_GetMarketComissSum;


--Установить запись о том, что данный договор обслуживания обрабатывался (для последующей печати по нему, даже если не было сделок)
PROCEDURE SetUsingContr( p_ClientID IN NUMBER,
                         p_ContrID  IN NUMBER
                       )
IS

  v_brkrep DBRKREPDEAL_TMP%ROWTYPE;

BEGIN

  v_brkrep.t_ClientID  := p_ClientID;
  v_brkrep.t_ContrID   := p_ContrID;
  v_brkrep.t_Part      := 0;
  v_brkrep.t_PlanID    := 0;
  v_brkrep.t_IsItog    := CHR(0);
  v_brkrep.t_Direction := 0;
  v_brkrep.t_A05_D     := TO_DATE('01.01.0001','DD.MM.YYYY');
  v_brkrep.t_A05_T     := TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS');
  v_brkrep.t_A06       := TO_DATE('01.01.0001','DD.MM.YYYY');
  v_brkrep.t_A07       := CHR(1);
  v_brkrep.t_A08       := CHR(1);
  v_brkrep.t_A09       := CHR(1);
  v_brkrep.t_A10       := CHR(1);
  v_brkrep.t_A11       := CHR(1);
  v_brkrep.t_A12       := CHR(1);
  v_brkrep.t_A13       := 0;
  v_brkrep.t_A13_i     := 0;
  v_brkrep.t_A14       := CHR(1);
  v_brkrep.t_A14_C     := -1;
  v_brkrep.t_A15       := 0;
  v_brkrep.t_A16       := 0;
  v_brkrep.t_A17       := 0;
  v_brkrep.t_A18       := 0;
  v_brkrep.t_A19       := CHR(1);
  v_brkrep.t_A19_C     := -1;
  v_brkrep.t_A20       := 0;
  v_brkrep.t_A21       := 0;
  v_brkrep.t_A22       := 0;
  v_brkrep.t_A23       := CHR(1);
  v_brkrep.t_A24       := CHR(1);
  v_brkrep.t_A25       := CHR(1);
  v_brkrep.t_A26       := 0;
  v_brkrep.t_A27       := 0;
  v_brkrep.t_A28       := 0;
  v_brkrep.t_A29       := 0;
  v_brkrep.t_A30       := CHR(1);
  v_brkrep.t_A31       := TO_DATE('01.01.0001','DD.MM.YYYY');
  v_brkrep.t_A32_1     := 0;
  v_brkrep.t_A32_2     := -1;
  v_brkrep.t_A33_1     := 0;
  v_brkrep.t_A33_2     := -1;
  v_brkrep.t_A33_3     := 0;
  v_brkrep.t_A34       := 0;
  v_brkrep.t_A35       := 0;
  v_brkrep.t_FIID      := -1;
  v_brkrep.t_A95       := 0;
  v_brkrep.t_A95_M     := CHR(1);
  v_brkrep.t_MarketID  := -1;

  INSERT INTO dbrkrepdeal_tmp VALUES v_brkrep;

END SetUsingContr;


--Получить вид кода субъекта на бирже для конкретной биржи
FUNCTION GetPtCodeKindForMarket(p_MarketID IN NUMBER) RETURN NUMBER
AS
  v_MMVB_Code VARCHAR2(35);
  v_SPB_Code  VARCHAR2(35);

  v_Market_Code VARCHAR2(35);

  v_CodeKind NUMBER := 1;
BEGIN

  IF g_PrevMarketID = p_MarketID THEN
    v_CodeKind := g_PrevCodeKind;
  ELSE
    v_MMVB_Code := trim(rsb_common.GetRegStrValue('SECUR\MICEX_CODE', 0));
    v_SPB_Code  := trim(rsb_common.GetRegStrValue('SECUR\SPBEX_CODE', 0));

    v_Market_Code := RSI_RSBPARTY.GetPartyCode(p_MarketID, 1 /*PTCK_CONTR*/);

    IF v_MMVB_Code = v_Market_Code THEN
      v_CodeKind := 8; /*PTCK_MICEX*/
    ELSIF v_SPB_Code = v_Market_Code THEN
      v_CodeKind := 76;
    END IF;

    g_PrevMarketID := p_MarketID;
    g_PrevCodeKind := v_CodeKind;
  END IF;

  RETURN v_CodeKind;
END;

--Формирование данных по сделкам для раздела 2 отчета
PROCEDURE CreateDealData( p_DlContrID     IN NUMBER,
                          p_Part          IN NUMBER,
                          p_BegDate       IN DATE,
                          p_EndDate       IN DATE,
                          p_ByExchange    IN NUMBER,
                          p_ByOutExchange IN NUMBER
                        )
IS

  TYPE brkrep_t IS TABLE OF DBRKREPDEAL_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;
BEGIN

  WITH q_sf AS (SELECT sf.t_ID, sf.t_PartyID, sf.t_ServKind, sf.t_ServKindSub
                  FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf
                 WHERE mp.t_DlContrID = p_DlContrID
                   AND sf.t_ID = mp.t_SfContrID
               ),
       q as (
             --сделки без корзины
             SELECT tk.t_DealID, tk.t_BOfficeKind, tk.t_DealDate, tk.t_DealTime, tk.t_ClientID, tk.t_ClientContrID,
                    tk.t_DealCode, tk.t_DealCodeTS, tk.t_MarketID, tk.t_Flag1, tk.t_PartyID, tk.t_dealtype,
                    leg.t_LegKind, leg.t_IncomeRate, leg.t_CFI,
                    GetRQAmountCashOnDate(rq.t_DocKind, rq.t_DocID, rq.t_DealPart , p_EndDate) as t_TotalCost,
                    leg.t_NKD,
                    GetRQAmountSecuritiesOnDate(rq.t_DocKind, rq.t_DocID, rq.t_DealPart, p_EndDate) as t_Amount,
                    rq.t_DealPart, rq.t_State,
                    avr.t_LSIN, avr.t_ISIN, fin.t_FaceValueFI, fin.t_Issuer,
                    RSB_SECUR.IsRepo(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsRepo,
                    RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsBuy,
                    RSB_SECUR.IsSale(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsSale,
                    RSB_SECUR.IsOTC(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsOTC,
                    case when RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(tk.t_DealID, 34, '0'), 116, tk.t_DealDate) = 1 then 'X' else CHR(0) end as IsMarginCall,
                    GetExecDate(tk.t_BOfficeKind, tk.t_DealID, rq.t_DealPart) as FactExecDate,
                    fin.t_FIID,
                    q_sf.t_PartyID as ClientID, q_sf.t_ID as SfContrID, q_sf.t_ServKind, q_sf.t_ServKindSub,
                    (CASE WHEN leg.t_RejectDate <> TO_DATE('01.01.0001','DD.MM.YYYY') AND leg.t_RejectDate <= p_EndDate THEN 1 ELSE 0 END) as IsRejectDeal
               FROM ddl_tick_dbt tk, ddlrq_dbt rq, ddl_leg_dbt leg, q_sf, dfininstr_dbt fin, davoiriss_dbt avr
              WHERE tk.t_BOfficeKind   = 101 /*DL_SECURITYDOC*/
                AND tk.t_ClientID      = q_sf.t_PartyID
                AND tk.t_ClientContrID = q_sf.t_ID
                AND rq.t_DocKind       = tk.t_BOfficeKind
                AND rq.t_DocID         = tk.t_DealID
                AND rq.t_Type          = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                AND leg.t_DealID       = tk.t_DealID
                AND leg.t_LegKind      = DECODE(rq.t_DealPart, 1, 0 /*LEG_KIND_DL_TICK*/, 2 /*LEG_KIND_DL_TICK_BACK*/)
                AND leg.t_LegID        = 0
                AND fin.t_FIID         = tk.t_PFI
                AND avr.t_FIID         = fin.t_FIID
                AND tk.t_Flag1 = (CASE WHEN p_ByExchange <> 0 AND p_ByOutExchange = 0 THEN 'X'
                                       WHEN p_ByExchange = 0 AND p_ByOutExchange <> 0 THEN CHR(0)
                                       ELSE tk.t_Flag1 END )
                AND Rsb_Secur.IsBasket( Rsb_Secur.get_OperationGroup( Rsb_Secur.get_OperSysTypes( tk.t_DealType, tk.t_BOfficeKind ) ) ) = 0

             -- плюс сделки с корзиной
             UNION ALL
             SELECT tk.t_DealID, tk.t_BOfficeKind, tk.t_DealDate, tk.t_DealTime, tk.t_ClientID, tk.t_ClientContrID,
                    tk.t_DealCode, tk.t_DealCodeTS, tk.t_MarketID, tk.t_Flag1, tk.t_PartyID, tk.t_dealtype,
                    leg.t_LegKind, leg.t_IncomeRate, leg.t_CFI,
                    GetRQAmountCashOnDate(tk.t_BOfficeKind, tk.t_DealID,  decode (leg.t_LegKind,0,1,2),p_EndDate) as t_TotalCost,
                    GetBasketNKDOnDate(tk.t_DealID, p_EndDate) as t_NKD,
                    GetRQAmountSecuritiesOnDate(tk.t_BOfficeKind, tk.t_DealID, decode (leg.t_LegKind,0,1,2), p_EndDate) as t_Amount,
                    DECODE(leg.t_LegKind, 0 /*LEG_KIND_DL_TICK*/, 1, 2),
                    (CASE WHEN EXISTS(SELECT 1
                                        FROM ddlrq_dbt rq
                                       WHERE rq.t_DocKind = tk.t_BOfficeKind
                                         AND rq.t_DocID = tk.t_DealID
                                         AND rq.t_DealPart = DECODE(leg.t_LegKind, 0 /*LEG_KIND_DL_TICK*/, 1, 2)
                                         AND rq.t_State <> RSI_DLRQ.DLRQ_STATE_EXEC
                                     ) THEN RSI_DLRQ.DLRQ_STATE_PLAN
                          ELSE RSI_DLRQ.DLRQ_STATE_EXEC END
                    ),
                    avr.t_LSIN, avr.t_ISIN, fin.t_FaceValueFI, fin.t_Issuer,
                    RSB_SECUR.IsRepo(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsRepo,
                    RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsBuy,
                    RSB_SECUR.IsSale(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsSale,
                    RSB_SECUR.IsOTC(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsOTC,
                    case when RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(tk.t_DealID, 34, '0'), 116, tk.t_DealDate) = 1 then 'X' else CHR(0) end as IsMarginCall,
                    GetExecDate(tk.t_BOfficeKind, tk.t_DealID, DECODE(leg.t_LegKind, 0 /*LEG_KIND_DL_TICK*/, 1, 2)) as FactExecDate,
                    fin.t_FIID,
                    q_sf.t_PartyID as ClientID, q_sf.t_ID as SfContrID, q_sf.t_ServKind, q_sf.t_ServKindSub,
                    (CASE WHEN leg.t_RejectDate <> TO_DATE('01.01.0001','DD.MM.YYYY') AND leg.t_RejectDate <= p_EndDate THEN 1 ELSE 0 END) as IsRejectDeal
               FROM ddl_tick_dbt tk, ddl_leg_dbt leg, q_sf, dfininstr_dbt fin, davoiriss_dbt avr
              WHERE tk.t_BOfficeKind   = 101 /*DL_SECURITYDOC*/
                AND tk.t_ClientID      = q_sf.t_PartyID
                AND tk.t_ClientContrID = q_sf.t_ID
                AND leg.t_DealID       = tk.t_DealID
                AND leg.t_LegID        = 0
                AND leg.t_LegKind IN (0,2)
                AND fin.t_FIID         = tk.t_PFI
                AND avr.t_FIID         = fin.t_FIID
                AND tk.t_Flag1 = (CASE WHEN p_ByExchange <> 0 AND p_ByOutExchange = 0 THEN 'X'
                                       WHEN p_ByExchange = 0 AND p_ByOutExchange <> 0 THEN CHR(0)
                                       ELSE tk.t_Flag1 END )
                AND Rsb_Secur.IsBasket( Rsb_Secur.get_OperationGroup( Rsb_Secur.get_OperSysTypes( tk.t_DealType, tk.t_BOfficeKind ) ) ) = 1
             )
  SELECT    q.ClientID,
            q.SfContrID,
            p_Part,
            GetSfPlanID(q.SfContrID, q.t_DealDate),
            CHR(0),
            (CASE WHEN q.IsRepo = 1 AND q.IsBuy = 1 THEN DECODE(q.t_DealPart, 1, 1, 2)
                  WHEN q.IsRepo = 1 AND q.IsSale = 1 THEN DECODE(q.t_DealPart, 1, 2, 1)
                  ELSE DECODE(q.IsBuy, 1, 1, 2) END
            ),
  /*A05_D*/ q.t_DealDate,
  /*A05_T*/ q.t_DealTime,
  /*A06*/   GetPlanExecDate(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*A07*/   q.t_DealCode,
  /*A08*/   DECODE(q.t_Flag1, 'X', q.t_DealCodeTS, CHR(1)),
-- /*A09*/   RSI_RSBPARTY.GetPartyCode(q.t_MarketID, 1 /*PTCK_CONTR*/),
  /*A09*/   uGetPatyNameForBrkRep(q.t_MarketID)||'('||(CASE WHEN q.t_ServKind = 1 THEN 'Фондовый рынок' WHEN q.t_ServKind = 21 THEN 'Валютный рынок' WHEN q.t_ServKind = 15 THEN 'Срочный рынок' ELSE CHR(1) END)||')' , --DAN
  /*A10*/   (CASE WHEN q.IsRepo = 1 THEN DECODE(q.IsBuy, 1, 'ОРЕПО', 'ПРЕПО') || ' ' || TO_CHAR(q.t_DealPart)
                  ELSE DECODE(q.IsBuy, 1, 'Покупка', 'Продажа') END
            ),
  /*A11*/   NVL((SELECT pt.t_ShortName
                   FROM dparty_dbt pt
                  WHERE pt.t_PartyID = q.t_Issuer), CHR(1)),
  /*A12*/   (CASE WHEN q.t_LSIN <> CHR(1) THEN q.t_LSIN ELSE q.t_ISIN END),
  /*A13*/   GetPrice(q.t_DealID, q.t_DealPart),
  /*A13_i*/ (CASE WHEN q.IsRepo = 1 THEN q.t_IncomeRate
                  ELSE 0 END
            ),
  /*A14*/   NVL((SELECT f.t_CCY FROM dfininstr_dbt f WHERE f.t_FIID = q.t_CFI), CHR(1)),
  /*A14_C*/ q.t_CFI,
  /*A15*/   q.t_Amount,
  /*A16*/   q.t_TotalCost,
  /*A17*/   RSI_RSB_FIInstr.ConvSum(q.t_TotalCost, q.t_CFI, RSI_RSB_FIInstr.NATCUR, CASE WHEN q.t_State = RSI_DLRQ.DLRQ_STATE_EXEC THEN GetPlanExecDate(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) ELSE p_EndDate END),
  /*A18*/   GetBrokerComissSum(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*A19*/   NVL((SELECT f.t_CCY FROM dfininstr_dbt f WHERE f.t_FIID = GetBrokerComissFIID(q.t_BOfficeKind, q.t_DealID, q.t_DealPart)), CHR(1)),
  /*A19_C*/ GetBrokerComissFIID(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*A20*/   GetMarketComissSum(q.t_BOfficeKind, q.t_DealID, q.t_DealPart, q.t_MarketID, q.t_DealDate),
  /*A21*/   0,
  /*A22*/   0,
  /*A23*/   (CASE WHEN q.t_Flag1 = 'X' AND q.IsOTC = 0 THEN 'биржевая' ELSE 'внебиржевая' END),
  /*A24*/   NVL((SELECT pt.t_ShortName
                   FROM dparty_dbt pt
                  WHERE pt.t_PartyID = q.t_PartyID), CHR(1)),
  /*A25*/   RSI_RSBPARTY.GetPartyCode(q.t_PartyID, (case when q.t_Flag1 = CHR(0) or q.IsOTC = 1 then 6 /*PTCK_SWIFT*/ else GetPtCodeKindForMarket(q.t_MarketID) end)),
  /*A26*/  case Rsb_Secur.IsBasket( Rsb_Secur.get_OperationGroup( Rsb_Secur.get_OperSysTypes( q.t_DealType, q.t_BOfficeKind ) ) )  WHEN 1 THEN 0
                else q.t_NKD end,
  /*A27*/   RSI_RSB_FIInstr.ConvSum(q.t_NKD, q.t_FaceValueFI, RSI_RSB_FIInstr.NATCUR, q.t_DealDate),
  /*A28*/  case q.t_LegKind when 2 then ( case IsSale when 1 then q.t_TotalCost else 0 end) else (case IsBuy when 1 then q.t_TotalCost else 0 end) end,
  /*A29*/  case q.t_LegKind when 2 then ( case IsBuy when 1 then q.t_Amount else 0 end) else (case IsSale when 1 then q.t_Amount else 0 end) end,
  /*A30*/   CHR(1),
  /*A31*/   TO_DATE('01.01.0001','DD.MM.YYYY'),
  /*A32_1*/ 0,
  /*A32_2*/ -1,
  /*A33_1*/ 0,
  /*A33_2*/ -1,
  /*A33_3*/ 0,
  /*A34*/   0,
  /*A35*/   0,
  /*FIID*/  q.t_FIID,
  /*A95*/   case when q.IsMarginCall <> 'X' then GetReqPrice(q.t_DealID) else 0 end,
  /*A95_M*/ case when q.IsMarginCall = 'X' then 'Принудительное закрытие позиций' else CHR(1) end,
  /*ServKind*/ q.t_ServKind,
  /*ServKindSub*/ q.t_ServKindSub,
  /*MarketID*/q.t_MarketID
  BULK COLLECT INTO v_brkrep
  FROM q
  WHERE 1 = (CASE WHEN p_Part = 1 AND q.IsRepo = 0 AND q.t_DealDate >= p_BegDate AND q.t_DealDate <= p_EndDate THEN 1
                  WHEN p_Part = 2 AND q.IsRepo = 1 AND q.t_DealDate >= p_BegDate AND q.t_DealDate <= p_EndDate THEN 1
                  WHEN p_Part = 3 AND q.IsRepo = 0 AND q.t_DealDate < p_BegDate AND q.FactExecDate BETWEEN p_BegDate AND p_EndDate AND q.IsRejectDeal = 0 THEN 1
                  WHEN p_Part = 4 AND q.IsRepo = 1 AND q.t_DealDate < p_BegDate AND q.FactExecDate BETWEEN p_BegDate AND p_EndDate AND q.IsRejectDeal = 0 THEN 1
                  WHEN p_Part = 5 AND q.IsRepo = 0 AND q.t_DealDate >= p_BegDate AND q.t_DealDate <= p_EndDate AND q.IsRejectDeal <> 0 THEN 1
                  WHEN p_Part = 6 AND q.IsRepo = 1 AND q.t_DealDate >= p_BegDate AND q.t_DealDate <= p_EndDate AND q.IsRejectDeal <> 0 THEN 1
                  ELSE 0 END );

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepdeal_tmp
             VALUES v_brkrep (indx);
  END IF;

  --Создать итоговые строки
  --IF p_Part IN (1,2) THEN
    FOR one_curr IN (SELECT cur.t_FIID, cur.t_CCY
                       FROM dfininstr_dbt cur
                      WHERE cur.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY
                        AND EXISTS(SELECT 1
                                     FROM dbrkrepdeal_tmp rep
                                    WHERE rep.t_Part     = p_Part
                                      AND rep.t_IsItog   = CHR(0)
                                      AND (   (rep.t_A14 = cur.t_CCY AND rep.t_A14 != CHR(1) )
                                           OR (rep.t_A19 = cur.t_CCY AND rep.t_A19 != CHR(1) )
                                           OR (cur.t_FIID = RSI_RSB_FIInstr.NATCUR AND (rep.t_A20 <> 0 OR rep.t_A21 <> 0 OR rep.t_A22 <> 0 OR rep.t_A27 <> 0))
                                          )
                                  )
                    )
    LOOP

        SELECT    -1,
                  -1,
                  p_Part,
                  q.t_PlanID,
                  'X',
                  q.t_Direction,
        /*A05_D*/ TO_DATE('01.01.0001','DD.MM.YYYY'),
        /*A05_T*/ TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS'),
        /*A06*/   TO_DATE('01.01.0001','DD.MM.YYYY'),
        /*A07*/   CHR(1),
        /*A08*/   CHR(1),
        /*A09*/   CHR(1),
        /*A10*/   CHR(1),
        /*A11*/   CHR(1),
        /*A12*/   CHR(1),
        /*A13*/   0,
        /*A13_i*/ 0,
        /*A14*/   CHR(1),
        /*A14_C*/ -1,
        /*A15*/   0,
        /*A16*/   q.t_A16,
        /*A17*/   q.t_A17,
        /*A18*/   q.t_A18,
        /*A19*/   CHR(1),
        /*A19_C*/ -1,
        /*A20*/   q.t_A20,
        /*A21*/   q.t_A21,
        /*A22*/   q.t_A22,
        /*A23*/   CHR(1),
        /*A24*/   CHR(1),
        /*A25*/   CHR(1),
        /*A26*/   0,
        /*A27*/   q.t_A27,
        /*A28*/   0,
        /*A29*/   0,
        /*A30*/   one_curr.t_CCY,
        /*A31*/   TO_DATE('01.01.0001','DD.MM.YYYY'),
        /*A32_1*/ 0,
        /*A32_2*/ -1,
        /*A33_1*/ 0,
        /*A33_2*/ -1,
        /*A33_3*/ 0,
        /*A34*/   0,
        /*A35*/   0,
        /*FIID*/  -1,
        /*A95*/   0,
        /*A95_M*/ CHR(1),
        /*ServKind*/ q.t_ServKind,
        /*ServKindSub*/ q.t_ServKindSub,
        /*MarketID*/q.t_MarketID
        BULK COLLECT INTO v_brkrep
        FROM (SELECT NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A16 ELSE 0 END)), 0) AS t_A16,
                     NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A17 ELSE 0 END)), 0) AS t_A17,
                     NVL(SUM((CASE WHEN t_A19_C = one_curr.t_FIID THEN t_A18 ELSE 0 END)), 0) AS t_A18,
                     NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A20 ELSE 0 END)), 0) AS t_A20,
                     NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A21 ELSE 0 END)), 0) AS t_A21,
                     NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A22 ELSE 0 END)), 0) AS t_A22,
                     NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A27 ELSE 0 END)), 0) AS t_A27,
                     t_PlanID, t_Direction, t_MarketID, t_ServKind, t_ServKindSub
                FROM dbrkrepdeal_tmp
               WHERE t_Part = p_Part
                 AND t_IsItog = CHR(0)
                 AND (   t_A14 = one_curr.t_CCY
                      OR t_A19 = one_curr.t_CCY
                      OR (one_curr.t_FIID = RSI_RSB_FIInstr.NATCUR AND (t_A20 <> 0 OR t_A21 <> 0 OR t_A22 <> 0 OR t_A27 <> 0))
                     )
              GROUP BY t_MarketID, t_ServKind, t_ServKindSub, t_PlanID, t_Direction
             ) q;

      IF v_brkrep.COUNT > 0 THEN
         FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
            INSERT INTO dbrkrepdeal_tmp
                 VALUES v_brkrep (indx);
      END IF;

    END LOOP;
  --END IF;

END CreateDealData;

--Получить сумму по ТО с учетом знака
FUNCTION GetCurrentRQAmount(p_DocKInd IN NUMBER, p_DocID IN NUMBER, p_RqType IN NUMBER, p_EndDate IN DATE, p_FactDate IN DATE, p_IsBuy IN NUMBER, p_IsSale IN NUMBER, p_ToFIID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER
AS
  v_Amount NUMBER;
BEGIN

  SELECT sum((CASE WHEN p_ToFIID > -1
               THEN RSI_RSB_FIInstr.ConvSum(rq.t_Amount, rq.t_FIID, p_ToFIID, CASE WHEN rq.t_State = RSI_DLRQ.DLRQ_STATE_EXEC THEN rq.t_FactDate ELSE p_EndDate END)
               ELSE rq.t_Amount END)
          * (CASE WHEN (rq.t_Type = RSI_DLRQ.DLRQ_TYPE_COMPPAYM AND p_IsBuy = 1 AND rq.t_Kind = RSI_DLRQ.DLRQ_KIND_REQUEST) OR
                       (rq.t_Type = RSI_DLRQ.DLRQ_TYPE_COMPPAYM AND p_IsSale = 1 AND rq.t_Kind = RSI_DLRQ.DLRQ_KIND_COMMIT) OR
                       (rq.t_Type = RSI_DLRQ.DLRQ_TYPE_COMPDELIVERY AND p_IsBuy = 1 AND rq.t_Kind = RSI_DLRQ.DLRQ_KIND_COMMIT) OR
                       (rq.t_Type = RSI_DLRQ.DLRQ_TYPE_COMPDELIVERY AND p_IsSale = 1 AND rq.t_Kind = RSI_DLRQ.DLRQ_KIND_REQUEST)
                   THEN -1
                   ELSE 1 END))
    INTO v_Amount
    FROM ddlrq_dbt rq
   WHERE rq.t_DocKind = p_DocKind
     AND rq.t_DocID = p_DocID
     AND rq.t_Type = p_RqType
     AND rq.t_FactDate = p_FactDate
     AND rq.t_DealPart = p_Part;

  RETURN v_Amount;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;

END GetCurrentRQAmount;


--Получить валюту ТО
FUNCTION GetCurrentRQFIID(p_DocKInd IN NUMBER, p_DocID IN NUMBER, p_RqType IN NUMBER, p_FactDate IN DATE, p_Part IN NUMBER) RETURN NUMBER
AS
  v_FIID NUMBER;
BEGIN

  SELECT rq.t_FIID INTO v_FIID
    FROM ddlrq_dbt rq
   WHERE rq.t_DocKind  = p_DocKind
     AND rq.t_DocID    = p_DocID
     AND rq.t_Type     = p_RqType
     AND rq.t_FactDate = p_FactDate
     AND rq.t_DealPart = p_Part
     AND ROWNUM = 1;

  RETURN v_FIID;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN -1;

END GetCurrentRQFIID;


--Получить идентификатор пула счета
FUNCTION GetAccPoolID(p_ContrID       IN NUMBER,
                      p_Account       IN VARCHAR2,
                      p_Code_Currency IN NUMBER,
                      p_Chapter       IN NUMBER
                     ) RETURN NUMBER
AS
  v_PoolID NUMBER := -1;
BEGIN

  WITH cat AS (SELECT * FROM dmccateg_dbt WHERE t_LevelType = 1 AND t_Code = 'ЦБ Клиента, ВУ'),
        sf AS (SELECT * FROM dsfcontr_dbt WHERE t_ID = p_ContrID)
  SELECT (CASE WHEN cat.t_Class1 = 1859 THEN tpl.t_Value1
               WHEN cat.t_Class2 = 1859 THEN tpl.t_Value2
               WHEN cat.t_Class3 = 1859 THEN tpl.t_Value3
               WHEN cat.t_Class4 = 1859 THEN tpl.t_Value4
               WHEN cat.t_Class5 = 1859 THEN tpl.t_Value5
               WHEN cat.t_Class6 = 1859 THEN tpl.t_Value6
               WHEN cat.t_Class7 = 1859 THEN tpl.t_Value7
               WHEN cat.t_Class8 = 1859 THEN tpl.t_Value8
               ELSE 0 END) INTO v_PoolID
   FROM cat, sf, dmcaccdoc_dbt accd, dmctempl_dbt tpl
  WHERE accd.t_CatID = cat.t_ID
    AND accd.t_Owner = sf.t_PartyID
    AND accd.t_ClientContrID = sf.t_ID
    AND accd.t_Chapter = p_Chapter
    AND accd.t_Currency = p_Code_Currency
    AND accd.t_Account = p_Account
    AND tpl.t_CatID = accd.t_CatID
    AND tpl.t_Number = accd.t_TemplNum
    AND ROWNUM = 1;

  RETURN v_PoolID;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN -1;
END GetAccPoolID;


--Получить сумму проводок по операциям передачи в пул/возврата из пула по конкретному счету
FUNCTION GetPoolAccTrnSum(p_AccountID IN NUMBER, p_Debet IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE) RETURN NUMBER
AS
  v_Sum NUMBER := 0;
BEGIN

  IF p_Debet > 0 THEN
    SELECT NVL(SUM(acctrn.t_Sum_Payer), 0) INTO v_Sum
      FROM dacctrn_dbt acctrn
     WHERE acctrn.t_AccountID_Payer = p_AccountID
       AND acctrn.t_Date_Carry >= p_BegDate
       AND acctrn.t_Date_Carry <= p_EndDate
       AND acctrn.t_State = 1
       AND EXISTS(SELECT 1
                    FROM ddlgrdoc_dbt grdoc, ddlgrdeal_dbt grdeal
                   WHERE grdoc.t_DocKind = 1
                     AND grdoc.t_DocID = acctrn.t_AccTrnID
                     AND grdeal.t_ID = grdoc.t_GrDealID
                     AND grdeal.t_DocKind = 4619);
  ELSE
    SELECT NVL(SUM(acctrn.t_Sum_Receiver), 0) INTO v_Sum
      FROM dacctrn_dbt acctrn
     WHERE acctrn.t_AccountID_Receiver = p_AccountID
       AND acctrn.t_Date_Carry >= p_BegDate
       AND acctrn.t_Date_Carry <= p_EndDate
       AND acctrn.t_State = 1
       AND EXISTS(SELECT 1
                    FROM ddlgrdoc_dbt grdoc, ddlgrdeal_dbt grdeal
                   WHERE grdoc.t_DocKind = 1
                     AND grdoc.t_DocID = acctrn.t_AccTrnID
                     AND grdeal.t_ID = grdoc.t_GrDealID
                     AND grdeal.t_DocKind = 4619);

  END IF;

  RETURN v_Sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;
END GetPoolAccTrnSum;

--Формирование данных по лотам и позициям для раздела АКТИВЫ отчета (только фактические данные)
PROCEDURE CreateActiveData( p_DlContrID     IN NUMBER,
                            p_BegDate       IN DATE,
                            p_EndDate       IN DATE,
                            p_IsEDP         IN NUMBER
                          )
IS

  TYPE brkrep_t IS TABLE OF DBRKREPACTIVE_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;
  v_srbrkrep brkrep_t;

  TYPE dbrkrepinacc_t IS TABLE OF DBRKREPINACC_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_dbrkrepinacc dbrkrepinacc_t;

  v_CourceType NUMBER;
BEGIN

  v_CourceType := Rsb_Common.GetRegIntValue( 'SECUR\ВИД КУРСА РЫНОЧНАЯ ЦЕНА', 0 );

  /*Данные по составу портфеля Фондового рынка*/
  --Здесь только отбор фактических данных
  --После подготовки плановых данных в макросе будет выполняться коррекировка CorrectActiveData
  WITH q_sf AS (SELECT sf.t_ID, sf.t_PartyID, sf.t_ServKind, sf.t_ServKindSub, mp.t_MarketID
                  FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf
                 WHERE mp.t_DlContrID = p_DlContrID
                   AND sf.t_ID = mp.t_SfContrID
               ),
       q_lot AS (SELECT DISTINCT l.t_FIID, l.t_Department, q_sf.t_MarketID, q_sf.t_ServKind, q_sf.t_ServKindSub, q_sf.t_PartyID, q_sf.t_ID as SfContrID
                   FROM q_sf, dpmwrtcl_dbt l
                  WHERE l.t_contract = q_sf.t_ID
                    AND l.t_party = q_sf.t_PartyID
                    AND l.t_enddate >= p_BegDate-1
                    AND l.t_begdate <= p_EndDate
                    AND l.t_amount <> 0
                ),
       q1 AS (SELECT s.*
                FROM (SELECT q_lot.T_FIID,
                            (select sum(lot.t_amount) qnty 
                               from dpmwrtcl_dbt lot 
                              where lot.t_contract = q_lot.SfContrID
                                and lot.T_FIID =  q_lot.t_FIID
                                and lot.t_party = q_lot.t_PartyID
                                and lot.t_enddate >= p_BegDate-1
                                and lot.t_begdate <= p_BegDate-1
                                and lot.t_department = q_lot.t_department) as InRest,
                            (select sum(lot.t_amount) qnty
                               from dpmwrtcl_dbt lot  
                              where lot.t_contract = q_lot.SfContrID
                                and lot.T_FIID =  q_lot.t_FIID
                                and lot.t_party = q_lot.t_PartyID
                                and lot.t_enddate >= p_EndDate
                                and lot.t_begdate <= p_EndDate
                                and lot.t_department = q_lot.t_department) as OutRest,
                                q_lot.t_MarketID, q_lot.t_ServKind, q_lot.t_ServKindSub, q_lot.t_PartyID as ClientID
                           FROM q_lot
                     ) s
               WHERE (s.InRest != 0 OR s.OutRest != 0)
             )
  SELECT q.ClientID,
         0,
         CHR(0),
         -1,
         q.t_FIID,
 /*A53*/ CHR(1),
 /*A54*/ CHR(1),
 /*A55*/ q.InRest,
 /*A55_1*/ 0,
 /*A56*/ 0,
 /*A56_1*/ 0,
 /*A56_2*/ 0,
 /*A57*/ 0,
 /*A57_1*/ 0,
 /*A57_2*/ 0,
 /*A58*/ q.OutRest,
 /*A58_1*/ 0,
 /*A59*/ 0,
 /*A59_1*/-1,
 /*A60*/ 0,
 /*A61*/ 0,
 /*A62*/ 0,
 /*A63*/ 0,
 /*ServKind*/    q.t_ServKind,
 /*ServKindSub*/ q.t_ServKindSub,
 /*MarketID*/    q.t_MarketID
  BULK COLLECT INTO v_dbrkrepinacc
    FROM (SELECT q1.ClientID, (CASE WHEN p_IsEDP <> 0 AND q1.t_ServKindSub <> 9 /*ЕДП Кроме внебирж. рынка*/ THEN 0 ELSE q1.t_MarketID END) t_MarketID, (CASE WHEN p_IsEDP <> 0 AND q1.t_ServKindSub <> 9 THEN 0 ELSE q1.t_ServKind END) t_ServKind, (CASE WHEN p_IsEDP <> 0 AND q1.t_ServKindSub <> 9 THEN 0 ELSE q1.t_ServKindSub END) t_ServKindSub, q1.t_FIID,
                 NVL(SUM(q1.InRest), 0)  as InRest,
                 NVL(SUM(q1.OutRest), 0) as OutRest
            FROM q1
          GROUP BY q1.ClientID, (CASE WHEN p_IsEDP <> 0 AND q1.t_ServKindSub <> 9 /*ЕДП Кроме внебирж. рынка*/ THEN 0 ELSE q1.t_MarketID END), (CASE WHEN p_IsEDP <> 0 AND q1.t_ServKindSub <> 9 THEN 0 ELSE q1.t_ServKind END), (CASE WHEN p_IsEDP <> 0 AND q1.t_ServKindSub <> 9 THEN 0 ELSE q1.t_ServKindSub END), q1.t_FIID) q, dfininstr_dbt fin, davoiriss_dbt av
   WHERE fin.t_FIID = q.t_FIID
     AND av.t_FIID = fin.t_FIID;

  IF v_dbrkrepinacc.COUNT > 0 THEN
    FORALL indx IN v_dbrkrepinacc.FIRST .. v_dbrkrepinacc.LAST
    INSERT INTO dbrkrepinacc_tmp
    VALUES v_dbrkrepinacc(indx);
  END IF;

  /*Активы по ФИССиКО*/
  WITH q_sf AS (SELECT sf.t_ID, sf.t_PartyID, sf.t_ServKind, sf.t_ServKindSub, mp.t_MarketID
                  FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf
                 WHERE mp.t_DlContrID = p_DlContrID
                   AND sf.t_ID = mp.t_SfContrID
               ),
          q AS (SELECT s.*
               FROM (SELECT outturn.t_FIID as t_FIID,
                            (NVL(inturn.t_longposition, 0) - NVL(inturn.t_shortposition, 0)) as InRest,
                            (outturn.t_longposition - outturn.t_shortposition) as OutRest,
                            (NVL(inturn.t_longpositioncost, 0) - NVL(inturn.t_shortpositioncost, 0)) as InCostRest,
                            (outturn.t_longpositioncost - outturn.t_shortpositioncost) as OutCostRest,
                            q_sf.t_MarketID, q_sf.t_ServKind, q_sf.t_ServKindSub, q_sf.t_PartyID as ClientID
                          FROM q_sf 
                         INNER JOIN ddvfiturn_dbt outturn 
                            ON outturn.t_Client = q_sf.t_PartyID
                           AND outturn.t_ClientContr = q_sf.t_ID
                           AND outturn.t_Date = (SELECT max(t_Date)
                                                   FROM ddvfiturn_dbt
                                                  WHERE t_Client = q_sf.t_PartyID
                                                    AND t_ClientContr = q_sf.t_ID
                                                    AND t_FIID = outturn.t_FIID
                                                    AND t_Date <= p_EndDate)
                          LEFT JOIN ddvfiturn_dbt inturn
                            ON inturn.t_Client = q_sf.t_PartyID
                           AND inturn.t_ClientContr = q_sf.t_ID
                           AND inturn.t_FIID = outturn.t_FIID
                           AND inturn.t_Date = (SELECT max(t_Date)
                                                  FROM ddvfiturn_dbt
                                                 WHERE t_Client = q_sf.t_PartyID
                                                   AND t_ClientContr = q_sf.t_ID
                                                   AND t_FIID = inturn.t_FIID
                                                   AND t_Date < p_BegDate) /*PNV 537157 было <=, но тогда это не входящие остатки, а исходящие на дату p_BegDate*/
                     ) s
              WHERE (s.InRest != 0 OR s.OutRest != 0 )
             )
  SELECT      q.t_ServKind,
              q.t_ServKindSub,
              q.t_FIID,
  /*A8*/      NVL(SUM(q.InRest), 0),
  /*A9*/      NVL(SUM(q.OutRest), 0),
  /*A10*/     NVL(SUM(q.OutRest), 0),
  /*A11*/     NVL(SUM(q.InCostRest), 0),
  /*A12*/     NVL(SUM(q.OutCostRest), 0),
  /*A13*/     NVL(SUM(q.OutCostRest), 0),
  /*Course1*/ 0,
  /*Course2*/ 0,
  /*MarketID*/q.t_MarketID,
  /*A11_1*/   NVL(SUM(q.InCostRest), 0),
  /*A12_1*/   NVL(SUM(q.OutCostRest), 0),
  /*A13_1*/   NVL(SUM(q.OutCostRest), 0)
  BULK COLLECT INTO v_srbrkrep
    FROM q
  GROUP BY q.ClientID, q.t_MarketID, q.t_ServKind, q.t_ServKindSub, q.t_FIID;

  IF v_srbrkrep.COUNT > 0 THEN
     FORALL indx IN v_srbrkrep.FIRST .. v_srbrkrep.LAST
        INSERT INTO dbrkrepactive_tmp
             VALUES v_srbrkrep (indx);
  END IF;

END CreateActiveData;

FUNCTION GetActiveRateId(p_FIID IN NUMBER, p_Date IN DATE) RETURN NUMBER deterministic result_cache
IS
  v_CourseTypeMP  NUMBER;
  v_CourseTypeAVR NUMBER;
  
  v_RateId        NUMBER := -1;
BEGIN
  v_CourseTypeMP := Rsb_Common.GetRegIntValue( 'SECUR\ВИД КУРСА РЫНОЧНАЯ ЦЕНА', 0 );
  v_RateId := RSB_SPREPFUN.GetRateIdByMPWithMaxTradeVolume(p_Date, p_FIID, v_CourseTypeMP, 90);
  
  IF v_RateId = -1 THEN
   v_CourseTypeAVR := Rsb_Common.GetRegIntValue( 'SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0 );
   v_RateId := RSB_SPREPFUN.GetRateIdByMPWithMaxTradeVolume(p_Date, p_FIID, v_CourseTypeAVR, 90);
  END IF;

  RETURN v_RateID;
EXCEPTION
  WHEN OTHERS THEN RETURN -1;
END GetActiveRateId;

FUNCTION GetActiveBalanceCost( p_FIID        IN NUMBER,
                               p_Date        IN DATE,
                               p_DlContrId   IN NUMBER,
                               p_ServKind    IN NUMBER,
                               p_ServKindSub IN NUMBER,
                               p_MarketId    IN NUMBER
                             ) RETURN NUMBER
IS
  v_lot           DPMWRTSUM_DBT%ROWTYPE;
  v_balanceCost   NUMBER := 0.0;
--  v_amount        NUMBER := 0;
BEGIN
  FOR contr IN (SELECT sf.t_Id, sf.t_PartyId 
                  FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf
                 WHERE sf.t_Id = mp.t_SfContrId
                   AND mp.t_DlContrId = p_DlContrId
                   AND mp.t_MarketId = p_MarketId
                   AND sf.t_ServKind = p_ServKind
                   AND sf.t_ServKindSub = p_ServKindSub)
  LOOP
    RSB_PMWRTOFF.WRTRestOnDate(-1, p_FIID, contr.t_PartyId, contr.t_Id, -1, p_Date, -1, 1, v_lot);
    v_balanceCost := v_balanceCost + v_lot.t_BalanceCost;
--    v_amount := v_amount + v_lot.t_amount;
  END LOOP;
    
  RETURN v_balanceCost;
END GetActiveBalanceCost;

--Корректировка данных по внутреннему учёту для состава портфеля и активов отчета с учетом плановых движений
PROCEDURE CorrectActiveData( p_DlContrID     IN NUMBER,
                             p_BegDate       IN DATE,
                             p_EndDate       IN DATE
                           )
IS

  TYPE brkrep_t IS TABLE OF DBRKREPACTIVE_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;
  v_srbrkrep brkrep_t;

  TYPE dbrkrepinacc_t IS TABLE OF DBRKREPINACC_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_dbrkrepinacc dbrkrepinacc_t;

BEGIN

  SELECT    q.t_ClientID,
            0,
            CHR(0),
            -1,
            q.t_FIID,
  /*A53*/  (CASE WHEN NVL(av.t_LSIN,CHR(1)) = CHR(1) THEN NVL(av.t_ISIN,CHR(1))
                 WHEN NVL(av.t_ISIN,CHR(1)) = CHR(1) THEN NVL(av.t_LSIN,CHR(1))
                 WHEN av.t_ISIN = av.t_LSIN THEN av.t_ISIN
                 ELSE av.t_LSIN||'/'||av.t_ISIN END),
  /*A54*/   NVL(fin.T_Name,CHR(1)),
  /*A55*/   q.t_A55,
  /*A55_1*/ 0,
  /*A56*/   q.t_A56,
  /*A56_1*/ q.t_A56_1,
  /*A56_2*/ 0,
  /*A57*/   q.t_A57,
  /*A57_1*/ q.t_A57_1,
  /*A57_2*/ 0,
  /*A58*/   q.t_A58,
  /*A58*/   q.t_A58_1,
  /*A59*/   RSB_SPREPFUN.GetCourse(q.t_RateId, p_EndDate),
  /*A59_1*/ RSB_SPREPFUN.GetCourseFI(q.t_RateId),
  /*A60*/   0,
  /*A61*/   0,
  /*A62*/   RSI_RSB_FIInstr.FI_CalcNKD(fin.t_FIID, p_EndDate, q.t_A58, 0),
  /*A63*/   0,
  /*ServKind*/    q.t_ServKind,
  /*ServKindSub*/ q.t_ServKindSub,
  /*MarketID*/    q.t_MarketID
  BULK COLLECT INTO v_dbrkrepinacc
  FROM (SELECT NVL(SUM(t_A55), 0)   AS t_A55,
               NVL(SUM(t_A56), 0)   AS t_A56,
               NVL(SUM(t_A56_1), 0) AS t_A56_1,
               NVL(SUM(t_A57), 0)   AS t_A57,
               NVL(SUM(t_A57_1), 0) AS t_A57_1,
               NVL(SUM(t_A58), 0)   AS t_A58,
               NVL(SUM(t_A58+t_A56_1-t_A57_1), 0) AS t_A58_1,
               GetActiveRateId(t_FIID, p_EndDate) t_RateId,
               t_ClientID, t_MarketID, t_ServKind, t_ServKindSub,
               t_FIID
          FROM dbrkrepinacc_tmp
         WHERE t_IsItog = CHR(0)
        GROUP BY t_ClientID, t_MarketID, t_ServKind, t_ServKindSub, t_FIID
       ) q, dfininstr_dbt fin, davoiriss_dbt av
  WHERE fin.t_FIID = q.t_FIID
    AND av.t_FIID = fin.t_FIID;

  DELETE FROM dbrkrepinacc_tmp;

  IF v_dbrkrepinacc.COUNT > 0 THEN
     FORALL indx IN v_dbrkrepinacc.FIRST .. v_dbrkrepinacc.LAST
        INSERT INTO dbrkrepinacc_tmp
             VALUES v_dbrkrepinacc(indx);
  END IF;

  UPDATE dbrkrepinacc_tmp r
     SET (t_A59,
          t_A59_1,
          t_A60, 
          t_A61, 
          t_A62, 
          t_A63 
         ) = (SELECT q.t_A59,
                     q.t_A59_1,
                     q.t_A60,
                     RSI_RSB_FIInstr.ConvSum(q.t_A60, q.t_A59_1, RSI_RSB_FIInstr.NATCUR, p_EndDate),
                     RSI_RSB_FIInstr.ConvSum(q.t_A62, q.t_fv, q.t_A59_1, p_EndDate),
                     RSI_RSB_FIInstr.ConvSum(q.t_A62, q.t_fv, RSI_RSB_FIInstr.NATCUR, p_EndDate)
                FROM ( SELECT CASE WHEN q1.t_A59_1 != -1 THEN q1.t_A59
                                   ELSE CASE WHEN q1.t_A58 > 0 THEN (q1.t_bc - q1.t_A62) / q1.t_A58 ELSE 0 END
                              END t_A59,
                              CASE WHEN q1.t_A59_1 != -1 THEN q1.t_A59_1
                                   ELSE q1.t_fv
                              END t_A59_1,
                              CASE WHEN q1.t_A59_1 != -1 THEN ABS(q1.t_A58*q1.t_A59)
                                   ELSE q1.t_bc
                              END t_A60,
                              q1.t_A62,
                              q1.t_fv 
                         FROM ( SELECT r.t_A58,
                                       r.t_A59,
                                       r.t_A59_1,
                                       r.t_A62,
                                       (SELECT GetActiveBalanceCost(r.t_FIID, p_EndDate, p_DlContrId, r.t_ServKind, r.t_ServKindSub, r.t_MarketID)
                                          FROM dual
                                         WHERE r.t_A59_1 = -1 AND r.t_A58 > 0
                                       ) t_bc,
                                       (SELECT f.t_FaceValueFI FROM dfininstr_dbt f WHERE f.t_FIID = r.t_FIID) t_fv
                                  FROM dual
                              ) q1
                     ) q
             );

  --Создать итоговые строки
  SELECT    q.t_ClientID,
            0,
            'X',
            -1,
            -1,
  /*A53*/   CHR(1),
  /*A54*/   CHR(1),
  /*A55*/   q.t_A55,
  /*A55_1*/ 0,
  /*A56*/   q.t_A56,
  /*A56_1*/ q.t_A56_1,
  /*A56_2*/ 0,
  /*A57*/   q.t_A57,
  /*A57_1*/ q.t_A57_1,
  /*A57_2*/ 0,
  /*A58*/   q.t_A58,
  /*A58_1*/ q.t_A58_1,
  /*A59*/   0,
  /*A59_1*/ -1,
  /*A60*/   0,
  /*A61*/   q.t_A61,
  /*A62*/   0,
  /*A63*/   q.t_A63,
  /*ServKind*/    q.t_ServKind,
  /*ServKindSub*/ q.t_ServKindSub,
  /*MarketID*/    q.t_MarketID
  BULK COLLECT INTO v_dbrkrepinacc
  FROM (SELECT NVL(SUM(t_A55), 0)   AS t_A55,
               NVL(SUM(t_A56), 0)   AS t_A56,
               NVL(SUM(t_A56_1), 0) AS t_A56_1,
               NVL(SUM(t_A57), 0)   AS t_A57,
               NVL(SUM(t_A57_1), 0) AS t_A57_1,
               NVL(SUM(t_A58), 0)   AS t_A58,
               NVL(SUM(t_A58_1), 0) AS t_A58_1,
               NVL(SUM(t_A61), 0)   AS t_A61,
               NVL(SUM(t_A63), 0)   AS t_A63,
               t_ClientID, t_MarketID, t_ServKind, t_ServKindSub
          FROM dbrkrepinacc_tmp
         WHERE t_IsItog = CHR(0)
        GROUP BY t_ClientID, t_MarketID, t_ServKind, t_ServKindSub
       ) q;

  IF v_dbrkrepinacc.COUNT > 0 THEN
     FORALL indx IN v_dbrkrepinacc.FIRST .. v_dbrkrepinacc.LAST
        INSERT INTO dbrkrepinacc_tmp
             VALUES v_dbrkrepinacc(indx);
  END IF;


  /*Данные по активам Фондового рынка - формируем по данным состава портфеля*/
  SELECT      q.t_ServKind,
              q.t_ServKindSub,
              q.t_FIID,
  /*A8*/      q.t_A8,
  /*A9*/      q.t_A9,
  /*A10*/     q.t_A10,
  /*A11*/     q.t_A11_1 * q.t_rate_1 + q.t_A11_1_NKD * q.t_rateNKD1_1,
  /*A12*/     q.t_A12_1 * q.t_rate_2 + q.t_A12_1_NKD * q.t_rateNKD1_2,
  /*A13*/     q.t_A13_1 * q.t_rate_2 + q.t_A13_1_NKD * q.t_rateNKD1_2,
  /*Course1*/ q.t_Course1,
  /*Course2*/ q.t_Course2,
  /*MarketID*/q.t_MarketID,
  /*A11_1*/   q.t_A11_1 + q.t_A11_1_NKD * q.t_rateNKD2_1,
  /*A12_1*/   q.t_A12_1 + q.t_A12_1_NKD * q.t_rateNKD2_2,
  /*A13_1*/   q.t_A13_1 + q.t_A13_1_NKD * q.t_rateNKD2_2
  BULK COLLECT INTO v_brkrep
    FROM ( SELECT q1.*,
                  CASE WHEN q1.t_UseBalanceCost1 = 1 THEN q1.t_BalanceCost1
                       ELSE q1.t_A8 *q1.t_Course1
                  END as t_A11_1,
                  NVL((SELECT RSI_RSB_FIInstr.FI_CalcNKD(q1.t_FIID, p_BegDate-1, q1.t_A8, 0)
                         FROM dual
                        WHERE q1.t_UseBalanceCost1 = 0
                      ), 0
                  ) as t_A11_1_NKD,
                  
                  CASE WHEN q1.t_UseBalanceCost2 = 1 THEN q1.t_BalanceCost2 
                       ELSE q1.t_A9 *q1.t_Course2
                  END as t_A12_1,
                  NVL((SELECT RSI_RSB_FIInstr.FI_CalcNKD(q1.t_FIID, p_EndDate,   q1.t_A9, 0)
                         FROM dual
                        WHERE q1.t_UseBalanceCost2 = 0
                      ), 0
                  ) as t_A12_1_NKD,
                  
                  CASE WHEN q1.t_UseBalanceCost2 = 1 THEN q1.t_BalanceCost2
                       ELSE q1.t_A10*q1.t_Course2
                  END as t_A13_1,
                  NVL((SELECT RSI_RSB_FIInstr.FI_CalcNKD(q1.t_FIID, p_EndDate,   q1.t_A10, 0)
                         FROM dual
                        WHERE q1.t_UseBalanceCost2 = 0
                      ), 0
                  ) as t_A13_1_NKD,
                  
                  NVL(RSI_RSB_FIInstr.ConvSum(1, q1.t_CourseFI1,   RSI_RSB_FIInstr.NATCUR, p_BegDate-1), 0) t_rate_1,
                  NVL(RSI_RSB_FIInstr.ConvSum(1, q1.t_CourseFI2,   RSI_RSB_FIInstr.NATCUR, p_EndDate),   0) t_rate_2,
                  NVL(RSI_RSB_FIInstr.ConvSum(1, q1.t_FaceValueFI, RSI_RSB_FIInstr.NATCUR, p_BegDate-1), 0) t_rateNKD1_1,
                  NVL(RSI_RSB_FIInstr.ConvSum(1, q1.t_FaceValueFI, RSI_RSB_FIInstr.NATCUR, p_EndDate),   0) t_rateNKD1_2,
                  NVL(RSI_RSB_FIInstr.ConvSum(1, q1.t_FaceValueFI, q1.t_CourseFI1,         p_BegDate-1), 0) t_rateNKD2_1,
                  NVL(RSI_RSB_FIInstr.ConvSum(1, q1.t_FaceValueFI, q1.t_CourseFI2,         p_EndDate),   0) t_rateNKD2_2
             FROM (SELECT q2.t_MarketID, q2.t_ServKind, q2.t_ServKindSub, q2.t_FIID, q2.t_A8, q2.t_A9, q2.t_A10, q2.t_FaceValueFI,
                          RSB_SPREPFUN.GetCourse(q2.t_RateId1, p_BegDate-1) t_Course1,
                          CASE WHEN q2.t_RateId1 != -1 THEN RSB_SPREPFUN.GetCourseFI(q2.t_RateId1)
                               ELSE q2.t_FaceValueFI
                          END t_CourseFI1,
                          CASE WHEN q2.t_RateId1 = -1 THEN 1 ELSE 0 END t_UseBalanceCost1,
                          NVL((SELECT GetActiveBalanceCost(q2.t_FIID, p_BegDate-1, p_DlContrId, q2.t_ServKind, q2.t_ServKindSub, q2.t_MarketID) 
                                 FROM dual
                                WHERE q2.t_RateId1 = -1 AND q2.t_A8 > 0
                              ), 0
                          ) t_BalanceCost1,
                          
                          RSB_SPREPFUN.GetCourse(q2.t_RateId2, p_EndDate) t_Course2,
                          CASE WHEN q2.t_RateId2 != -1 THEN RSB_SPREPFUN.GetCourseFI(q2.t_RateId2)
                               ELSE q2.t_FaceValueFI
                          END t_CourseFI2,
                          CASE WHEN q2.t_RateId2 = -1 THEN 1 ELSE 0 END t_UseBalanceCost2,
                          NVL((SELECT GetActiveBalanceCost(q2.t_FIID, p_EndDate, p_DlContrId, q2.t_ServKind, q2.t_ServKindSub, q2.t_MarketID) 
                                 FROM dual
                                WHERE q2.t_RateId2 = -1 AND (q2.t_A9 > 0 OR q2.t_A10 > 0)
                              ), 0 
                          ) t_BalanceCost2
             FROM (SELECT ia.t_MarketID, ia.t_ServKind, ia.t_ServKindSub, ia.t_FIID,
                          ia.t_A55 as t_A8,
                          ia.t_A58 as t_A9,
                          ia.t_A58_1 as t_A10,
                          fin.t_FaceValueFI,
                                  --NVL(RSB_Secur.SC_ConvSumTypeRep(1, fin.t_FIID, fin.t_FaceValueFI, fin.t_FaceValueFI, v_CourceType, p_BegDate-1), 0) as Course1,
                                  --NVL(RSB_Secur.SC_ConvSumTypeRep(1, fin.t_FIID, fin.t_FaceValueFI, fin.t_FaceValueFI, v_CourceType, p_EndDate), 0) as Course2,
                                  GetActiveRateId(fin.t_FIID, p_BegDate-1) t_RateId1,
                                  GetActiveRateId(fin.t_FIID, p_EndDate) t_RateId2
                     FROM dbrkrepinacc_tmp ia, dfininstr_dbt fin
                    WHERE fin.t_FIID = ia.t_FIID
                      AND ia.t_IsItog = CHR(0)
                          ) q2
                  ) q1
         ) q;


  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepactive_tmp
             VALUES v_brkrep (indx);
  END IF;

END CorrectActiveData;


FUNCTION GetPlanRestAcc(p_DlContrID IN NUMBER, p_Account IN VARCHAR2, p_Chapter IN NUMBER, p_Code_Currency IN NUMBER, p_EndDate IN DATE) RETURN NUMBER DETERMINISTIC
AS
  v_ExistNett NUMBER := 0;
  v_PlanRest NUMBER := 0;

  v_ComisSum      NUMBER := 0;
  v_PaidComisSum  NUMBER := 0;
  v_LiabilitySum  NUMBER := 0;
BEGIN
  --Проверим есть ли впринципе операции неттинга по договорам клиента
  with q as (select sf.t_ID, sf.t_PartyID
               from ddlcontrmp_dbt mp, dsfcontr_dbt sf
              where mp.t_DlContrID = p_DlContrID
                and sf.t_ID = mp.t_SfContrID
            )
  select Count(1) INTO v_ExistNett
    from q, ddl_nett_dbt nt
   where nt.t_DocKind = RSB_SECUR.DL_NTGSEC
     and nt.t_ClientID = q.t_PartyID
     and nt.t_ClientContrID = q.t_ID
     and ROWNUM = 1;

  if v_ExistNett = 0 then --Если неттинга нет, то работаем только по таблице ddl_tick_dbt - так значительно быстрее
    with q as (select sf.t_ID, sf.t_PartyID
                 from ddlcontrmp_dbt mp, dsfcontr_dbt sf
                where mp.t_DlContrID = p_DlContrID
                  and sf.t_ID = mp.t_SfContrID
              ),
        tk as (select tk.*, q.t_PartyID as SfPartyID
                 from q, ddl_tick_dbt tk
                where tk.t_BOfficeKind IN (101, 117, 127)
                  and tk.t_ClientID = q.t_PartyID
                  and tk.t_ClientContrID = q.t_ID
               UNION
               select tk.*, q.t_PartyID as SfPartyID
                 from q, ddl_tick_dbt tk
                where tk.t_BOfficeKind IN (101, 117, 127)
                  and tk.t_IsPartyClient = 'X'
                  and tk.t_PartyID = q.t_PartyID
                  and tk.t_PartyContrID = q.t_ID
              )
    select NVL(Sum(rq.t_Amount*(case when (rq.t_Kind = RSI_DLRQ.DLRQ_KIND_COMMIT and tk.t_ClientID = tk.SfPartyID) or
                                          (rq.t_Kind = RSI_DLRQ.DLRQ_KIND_REQUEST and tk.t_PartyID = tk.SfPartyID)
                                          then -1
                                     else 1 end)), 0) INTO v_PlanRest
      from tk, ddlrq_dbt rq
     where (tk.t_DealDate <= p_EndDate
            or (tk.t_BOfficeKind = RSB_SECUR.DL_SECURITYDOC
                and tk.t_RequestID > 0
                and Exists(select 1
                             from ddl_tick_dbt tk2
                            where tk2.t_DealID = tk.t_RequestID
                              and tk2.t_DealDate <= p_EndDate)
               )
           )
       and rq.t_DocKind = tk.t_BOfficeKind
       and rq.t_DocID = tk.t_DealID
       and rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY
       and Exists(select 1
                    from ddlrqacc_dbt rqacc
                   where rqacc.t_DocKind = rq.t_DocKind
                     and rqacc.t_DocID = rq.t_DocID
                     and rqacc.t_SubKind = rq.t_SubKind
                     and rqacc.t_FIID = rq.t_FIID -- DAN добавил связку по FIID i-sup 538632
                     and rqacc.t_Party = tk.SfPartyID
                     and rqacc.t_Type IN (rq.t_Type, -1)
                     and rqacc.t_Account = p_Account
                     and rqacc.t_Chapter = p_Chapter
                     and rqacc.t_FIID = p_Code_Currency
                 )
       and ( (tk.t_DealStatus = 0 /*DL_PREPARING*/ and tk.t_Prognos = chr(88))
             or
             (Exists(select 1
                       from ddlgrdeal_dbt gr
                      where gr.t_DocKind = rq.t_DocKind
                        and gr.t_DocID   = rq.t_DocID
                        and (   (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_COMISS) and (    (tk.t_ClientID = tk.SfPartyID and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYCOM)) 
                                                                                or (tk.t_IsPartyClient = 'X' and tk.t_PartyID = tk.SfPartyID and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYCOMCONTR))
                                                                              ) 
                                                                          and decode((select /*+ index(com DDLCOMIS_DBT_IDX0)*/ count(1)       
                                                                                        from ddlcomis_dbt com                               
                                                                                       where com.t_ID = rq.t_SourceObjID        
                                                                                         and com.t_IsBankExpenses = 'X'              
                                                                                         and rq.t_SourceObjKind = RSB_SECUR.DL_SECURITYCOM 
                                                                                         and rownum < 2),0,0,1) = 0    
                                )
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_AVANCE,RSI_DLRQ.DLRQ_TYPE_DEPOSIT)  and rq.t_DealPart = 1                                        and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYAVANCE))
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMENT)                            and rq.t_DealPart = 1                                        and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYMENT))
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_AVANCE)                             and rq.t_DealPart = 2                                        and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYAVANCE2))
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMENT,RSI_DLRQ.DLRQ_TYPE_INCREPO) and rq.t_DealPart = 2                                        and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYMENT2,RSI_DLGR.DLGR_TEMPL_PAYPC))
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_COMPPAYM)                                                                                        and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_COMPPAYMENT))
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMREPOCOUP)                                                                                    and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_COUP))
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMREPOPART)                                                                                    and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PARTREP))
                            )
                        and Exists(select 1
                                     from ddlgracc_dbt gracc
                                    where gracc.t_GrDealID = gr.t_ID
                                      and gracc.t_AccNum = RSI_DLGR.DLGR_ACCKIND_ACCOUNTING
                                      and ( gracc.t_State = RSI_DLGR.DLGRACC_STATE_PLAN
                                          or (gracc.t_State = RSI_DLGR.DLGRACC_STATE_FACTEXEC and gracc.t_FactDate > p_EndDate )
                                          )
                                  )
                    )
             )
           ) ;
  else --Если неттинг есть, то, увы, выборка по view

    with q as (select sf.t_ID, sf.t_PartyID
                 from ddlcontrmp_dbt mp, dsfcontr_dbt sf
                where mp.t_DlContrID = p_DlContrID
                  and sf.t_ID = mp.t_SfContrID
              )
    select NVL(Sum(rq.t_Amount*(case when (rq.t_Kind = RSI_DLRQ.DLRQ_KIND_COMMIT and td.t_ClientID = q.t_PartyID) or
                                          (rq.t_Kind = RSI_DLRQ.DLRQ_KIND_REQUEST and td.t_PartyID = q.t_PartyID)
                                          then -1
                                     else 1 end)), 0) INTO v_PlanRest
      from q, ddlrq_dbt rq, dv_sctotaldeals td
     where ((   td.t_ClientID = q.t_PartyID
            and td.t_ClientContrID = q.t_ID
            ) or
            (    td.t_IsPartyClient = 'X'
             and td.t_PartyID = q.t_PartyID
             and td.t_PartyContrID = q.t_ID
            )
           )
       and (td.t_DealDate <= p_EndDate
            or (td.t_DocKind = RSB_SECUR.DL_SECURITYDOC
                and Exists(select 1
                             from ddl_tick_dbt tk, ddl_tick_dbt tk1
                            where tk.t_DealID = td.t_DocID
                              and tk.t_RequestID > 0
                              and tk1.t_DealID = tk.t_RequestID
                              and tk1.t_DealDate <= p_EndDate)
               )
           )
       and rq.t_DocKind = td.t_DocKind
       and rq.t_DocID = td.t_DocID
       and rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY
       and Exists(select 1
                    from ddlrqacc_dbt rqacc
                   where rqacc.t_DocKind = rq.t_DocKind
                     and rqacc.t_DocID = rq.t_DocID
                     and rqacc.t_SubKind = rq.t_SubKind
                     and rqacc.t_Party = q.t_PartyID
                     and rqacc.t_Type IN (rq.t_Type, -1)
                     and rqacc.t_Account = p_Account
                     and rqacc.t_Chapter = p_Chapter
                     and rqacc.t_FIID = p_Code_Currency
                 )
       and ( (td.t_Status = 0 /*DL_PREPARING*/ and td.t_Prognos = chr(88))
             or
             (Exists(select 1
                       from ddlgrdeal_dbt gr
                      where gr.t_DocKind = rq.t_DocKind
                        and gr.t_DocID   = rq.t_DocID
                        and (   (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_COMISS) and (    (td.t_ClientID = q.t_PartyID and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYCOM)) 
                                                                                or (td.t_IsPartyClient = 'X' and td.t_PartyID = q.t_PartyID and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYCOMCONTR))
                                                                              ) 
                                                                          and decode((select /*+ index(com DDLCOMIS_DBT_IDX0)*/ count(1)       
                                                                                        from ddlcomis_dbt com                               
                                                                                       where com.t_ID = rq.t_SourceObjID        
                                                                                         and com.t_IsBankExpenses = 'X'              
                                                                                         and rq.t_SourceObjKind = RSB_SECUR.DL_SECURITYCOM 
                                                                                         and rownum < 2),0,0,1) = 0    
                                )
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_AVANCE,RSI_DLRQ.DLRQ_TYPE_DEPOSIT)  and rq.t_DealPart = 1                                       and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYAVANCE))
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMENT)                            and rq.t_DealPart = 1                                       and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYMENT))
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_AVANCE)                             and rq.t_DealPart = 2                                       and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYAVANCE2))
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMENT,RSI_DLRQ.DLRQ_TYPE_INCREPO) and rq.t_DealPart = 2                                       and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYMENT2,RSI_DLGR.DLGR_TEMPL_PAYPC))
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_COMPPAYM)                                                                                       and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_COMPPAYMENT))
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMREPOCOUP)                                                                                   and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_COUP))
                             or (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMREPOPART)                                                                                   and gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PARTREP))
                            )
                        and Exists(select 1
                                     from ddlgracc_dbt gracc
                                    where gracc.t_GrDealID = gr.t_ID
                                      and gracc.t_AccNum = RSI_DLGR.DLGR_ACCKIND_ACCOUNTING
                                      and ( gracc.t_State = RSI_DLGR.DLGRACC_STATE_PLAN
                                          or (gracc.t_State = RSI_DLGR.DLGRACC_STATE_FACTEXEC and gracc.t_FactDate > p_EndDate )
                                          )
                                  )
                    )
             )
           ) ;
  end if;

  v_PlanRest := v_PlanRest + rsb_account.restac(p_Account, p_Code_Currency, p_EndDate, p_Chapter, null);


  --По валютному рынку
  with q as (select sf.t_ID, sf.t_PartyID
               from ddlcontrmp_dbt mp, dsfcontr_dbt sf
              where mp.t_DlContrID = p_DlContrID
                and sf.t_ID = mp.t_SfContrID
            )
  select NVL(SUM( (SELECT nvl(SUM(comis.t_Sum),0)
                     FROM ddlcomis_dbt comis, dsfcomiss_dbt sfc
                    WHERE comis.t_DocKind = ndeal.t_DocKind
                      AND comis.t_DocID = ndeal.t_ID
                      AND comis.t_Date <= p_EndDate
                      AND sfc.t_FIID_Comm = p_Code_Currency --Добавила проверку валюты, чтобы быть уверенной, что комиссия упадёт на нужный счёт
                      AND comis.t_FeeType = sfc.t_FeeType
                      AND comis.t_ComNumber = sfc.t_Number
                      AND comis.t_IsBankExpenses <> 'X'
                  )
                ), 0),
         /*Сначала суммируем комиссии по скроллингу, а затем вычитаем платежи, т.к. РСХБ переделали комиссии таким образом,
           что в скроллинге фактическая дата отмечается на шаге оплаты комиссий, а платежи и проводки формируются при оплате Т/О.
           В начале года была ситуация, что комиссии проставлены 31 декабря, а платежи шли 11 января. */
         NVL(SUM((SELECT nvl(SUM(paym.t_Amount),0)
                    FROM dpmpaym_dbt paym
                   WHERE paym.t_DocKind = ndeal.t_DocKind
                     AND paym.t_DocumentID = ndeal.t_ID
                     AND paym.t_Purpose IN (RSB_PAYMENT.PM_PURP_COMMARKET, 72 /*PM_PURP_COMMBANK*/, RSB_PAYMENT.PM_PURP_COMBROKER)
                     AND paym.t_ValueDate <= p_EndDate
                     AND paym.t_PaymStatus <> PM_COMMON.PM_REJECTED
                     AND ((paym.t_PayerAccount = p_Account and paym.t_FIID = p_Code_Currency) OR
                          (paym.t_ReceiverAccount = p_Account and paym.t_PayFIID = p_Code_Currency))
                 )
                ), 0),
         NVL(SUM((SELECT nvl(SUM(case when paym.t_PayerAccount = p_Account then -paym.t_Amount else paym.t_Amount end),0) as LiabilitySum
                    FROM dpmpaym_dbt paym
                   WHERE paym.t_DocKind = ndeal.t_DocKind
                     AND paym.t_DocumentID = ndeal.t_ID
                     AND paym.t_Purpose NOT IN (RSB_PAYMENT.PM_PURP_COMMARKET, 72 /*PM_PURP_COMMBANK*/, RSB_PAYMENT.PM_PURP_COMBROKER)
                     AND paym.t_ValueDate > p_EndDate
                     AND paym.t_PaymStatus <> PM_COMMON.PM_REJECTED
                     AND ((paym.t_PayerAccount = p_Account and paym.t_FIID = p_Code_Currency) OR
                          (paym.t_ReceiverAccount = p_Account and paym.t_PayFIID = p_Code_Currency))
                 )
                ),0)
    into v_ComisSum, v_PaidComisSum, v_LiabilitySum
    from q, ddvndeal_dbt ndeal
   where ndeal.t_Client = q.t_PartyID
     and ndeal.t_ClientContr = q.t_ID
     and ndeal.t_Date <= p_EndDate
     and ndeal.t_MarketKind = RSB_SECUR.DV_MARKETKIND_CURRENCY
     and ndeal.t_State in(1,2)   --Потом сюда нужно добавить обсчёт прогнозных сделок
     and exists (select 1
                   from dpmpaym_dbt paym
                  where paym.t_DocKind = ndeal.t_DocKind
                    and paym.t_DocumentID = ndeal.t_ID
                    and paym.t_ValueDate > p_EndDate
                    and paym.t_PaymStatus <> PM_COMMON.PM_REJECTED)
     and exists (select 1
                   from dpmpaym_dbt paym
                  where paym.t_DocKind = ndeal.t_DocKind
                    and paym.t_DocumentID = ndeal.t_ID
                    and ((paym.t_PayerAccount = p_Account and paym.t_FIID = p_Code_Currency) or
                         (paym.t_ReceiverAccount = p_Account and paym.t_PayFIID= p_Code_Currency)));

  v_PlanRest := v_PlanRest + v_LiabilitySum - (v_ComisSum - v_PaidComisSum);

  RETURN v_PlanRest;
END;

PROCEDURE LoadAccInTmp( p_DlContrID     IN NUMBER,
                        p_BegDate       IN DATE,
                        p_EndDate       IN DATE,
                        p_IsEDP         IN NUMBER,
                        p_ByOutExchange IN NUMBER,
                        p_NeedPlanRest  IN NUMBER
                      )
IS
  TYPE brkacc_t IS TABLE OF DBRKREPACC_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkacc brkacc_t;
BEGIN

  WITH cat AS (SELECT t_ID FROM dmccateg_dbt WHERE t_LevelType = 1 AND t_Code IN ('ДС клиента, ц/б','Брокерский счет ДБО')),
        sf AS (SELECT sf.t_ID, sf.t_ServKind, sf.t_ServKindSub, sf.t_PartyID, mp.t_MarketID
                 FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf
                WHERE mp.t_DlContrID = p_DlContrID
                  AND sf.t_ID = mp.t_SfContrID
                  AND sf.t_ServKindSub <> 9 /*Кроме внебирж*/)
  SELECT q.t_AccountID,
         q.ServKind,
         q.ServKindSub,
         q.MarketID,
         rsb_account.restac(q.t_Account, q.t_Code_Currency, p_BegDate-1, q.t_Chapter, null),
         rsb_account.restac(q.t_Account, q.t_Code_Currency, p_EndDate, q.t_Chapter, null),
         0,
         (CASE WHEN p_NeedPlanRest <> 0 THEN GetPlanRestAcc(p_DlContrID, q.t_Account, q.t_Chapter, q.t_Code_Currency, p_EndDate) ELSE 0 END)
  BULK COLLECT INTO v_brkacc
    FROM (SELECT DISTINCT acc.t_AccountID, acc.t_Account, acc.t_Chapter, acc.t_Code_Currency,
                 (CASE WHEN p_IsEDP = 0 THEN sf.t_ServKind ELSE 0 END) as ServKind,
                 (CASE WHEN p_IsEDP = 0 THEN sf.t_ServKindSub ELSE 0 END) as ServKindSub,
                 (CASE WHEN p_IsEDP = 0 THEN sf.t_MarketID    ELSE -1 END) as MarketID
            FROM sf, cat, dmcaccdoc_dbt mc, daccount_dbt acc
           WHERE mc.t_CatID = cat.t_ID
             AND mc.t_Owner = sf.t_PartyID
             AND mc.t_ClientContrID = sf.t_ID
             AND acc.t_Account = mc.t_Account
             AND acc.t_Chapter = mc.t_Chapter
             AND acc.t_Code_Currency = mc.t_Currency
             AND acc.t_Open_Date <= p_EndDate
             AND (acc.t_Close_Date = TO_DATE('01.01.0001','DD.MM.YYYY') or acc.t_Close_Date >= p_BegDate)
         ) q;

  IF v_brkacc.COUNT > 0 THEN
     FORALL indx IN v_brkacc.FIRST .. v_brkacc.LAST
        INSERT INTO dbrkrepacc_tmp
             VALUES v_brkacc (indx);
  END IF;

  IF p_ByOutExchange <> 0 THEN
    WITH cat AS (SELECT t_ID FROM dmccateg_dbt WHERE t_LevelType = 1 AND t_Code IN ('ДС клиента, ц/б','Брокерский счет ДБО')),
          sf AS (SELECT /*+ materialize */ sf.t_ID, sf.t_ServKind, sf.t_ServKindSub, sf.t_PartyID
                   FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf
                  WHERE mp.t_DlContrID = p_DlContrID
                    AND sf.t_ID = mp.t_SfContrID
                    AND sf.t_ServKind = 1
                    AND sf.t_ServKindSub = 9 /*Только внебирж*/)
    SELECT q.t_AccountID,
           q.ServKind,
           q.ServKindSub,
           q.MarketID,
           rsb_account.restac(q.t_Account, q.t_Code_Currency, p_BegDate-1, q.t_Chapter, null),
           rsb_account.restac(q.t_Account, q.t_Code_Currency, p_EndDate, q.t_Chapter, null),
           0,
           (CASE WHEN p_NeedPlanRest <> 0 THEN GetPlanRestAcc(p_DlContrID, q.t_Account, q.t_Chapter, q.t_Code_Currency, p_EndDate) ELSE 0 END)
    BULK COLLECT INTO v_brkacc
      FROM (           SELECT DISTINCT acc.t_AccountID, acc.t_Account, acc.t_Chapter, acc.t_Code_Currency,
                    sf.t_ServKind as ServKind,
                    sf.t_ServKindSub as ServKindSub,
                    -1 as MarketID
               FROM sf, cat, dmcaccdoc_dbt mc, daccount_dbt acc
              WHERE mc.t_CatID = cat.t_ID
                AND mc.t_Owner = sf.t_PartyID
                AND mc.t_ClientContrID = sf.t_ID
                AND acc.t_Account = mc.t_Account
                AND acc.t_Chapter = mc.t_Chapter
                AND acc.t_Code_Currency = mc.t_Currency
                AND acc.t_Open_Date <= p_EndDate
                AND (acc.t_Close_Date = TO_DATE('01.01.0001','DD.MM.YYYY') or acc.t_Close_Date >= p_BegDate)
           ) q;

    IF v_brkacc.COUNT > 0 THEN
       FORALL indx IN v_brkacc.FIRST .. v_brkacc.LAST
          INSERT INTO dbrkrepacc_tmp
               VALUES v_brkacc (indx);
    END IF;
  END IF;

END;

--Формирование данных по движению д/с
PROCEDURE CreateCasheMoveData( p_DlContrID     IN NUMBER,
                               p_BegDate       IN DATE,
                               p_EndDate       IN DATE,
                               p_IsEDP         IN NUMBER,
                               p_ByOutExchange IN NUMBER
                             )

IS
  TYPE brkrep_t IS TABLE OF DBRKREPCASHE_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;

BEGIN

  WITH acc_data AS (SELECT t_AccountID, t_ServKind, t_ServKindSub, t_MarketID FROM dbrkrepacc_tmp)
  SELECT
  /*T_CURRENCY   */ q.Currency,
  /*T_DATE       */ q.t_Date_Carry,
  /*T_SERVKIND   */ q.t_ServKind,
  /*T_SERVKINDSUB*/ q.t_ServKindSub,
  /*T_ISITOG     */ CHR(0),
  /*T_ACCTRNID   */ q.t_AccTrnID,
  /*T_OPERNAME   */ q.t_Ground,
  /*T_MARKETID   */ q.t_MarketID,
  /*T_MARKETNAME */ (CASE WHEN q.t_MarketID = -1 OR p_IsEDP <> 0 THEN (CASE WHEN q.t_ServKindSub = 9 THEN 'Внебиржевой рынок' ELSE CHR(1) END)
                          ELSE NVL((select uGetPatyNameForBrkRep(pt.t_PartyID)/*pt.t_ShortName*/||'('||(CASE WHEN q.t_ServKind = 1 THEN 'Фондовый рынок' WHEN q.t_ServKind = 21 THEN 'Валютный рынок' WHEN q.t_ServKind = 15 THEN 'Срочный рынок' ELSE CHR(1) END)||')'
                                      from dparty_dbt pt where pt.t_PartyID = q.t_MarketID), CHR(1))
                     END),
  /*T_INSUM      */ q.InSum,
  /*T_OUTSUM     */ q.OutSum,
  /*T_OUTNDS     */ 0,
  /*T_REST       */ 0,
  /*T_ACCOUNTID  */ q.t_AccountID
  BULK COLLECT INTO v_brkrep
  FROM(  SELECT acctrn.t_FIID_Payer as Currency,
                acctrn.t_Date_Carry,
                acc_data.t_ServKind,
                acc_data.t_ServKindSub,
                acctrn.t_AccTrnID,
                0 as InSum,
                acctrn.t_Sum_Payer as OutSum,
                acc_data.t_AccountID,
                acctrn.t_Ground,
                acc_data.t_MarketID
           FROM acc_data, dacctrn_dbt acctrn
          WHERE acctrn.t_State = 1
            AND acctrn.t_Chapter = 1
            AND acctrn.t_Date_Carry >= p_BegDate
            AND acctrn.t_Date_Carry <= p_EndDate
            AND acctrn.t_AccountID_Payer = acc_data.t_AccountID
            AND acctrn.t_Sum_Payer <> 0
         UNION ALL
         SELECT acctrn.t_FIID_Receiver as Currency,
                acctrn.t_Date_Carry,
                acc_data.t_ServKind,
                acc_data.t_ServKindSub,
                acctrn.t_AccTrnID,
                acctrn.t_Sum_Receiver as InSum,
                0 as OutSum,
                acc_data.t_AccountID,
                acctrn.t_Ground,
                acc_data.t_MarketID
           FROM acc_data, dacctrn_dbt acctrn
          WHERE acctrn.t_State = 1
            AND acctrn.t_Chapter = 1
            AND acctrn.t_Date_Carry >= p_BegDate
            AND acctrn.t_Date_Carry <= p_EndDate
            AND acctrn.t_AccountID_Receiver = acc_data.t_AccountID
            AND acctrn.t_Sum_Receiver <> 0
      ) q;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepcashe_tmp
             VALUES v_brkrep (indx);
  END IF;

  --Определить принадлежность проводок операциям

  --Проводки по строкам графика сделок БОЦБ
  UPDATE dbrkrepcashe_tmp cs
     SET (t_MarketID, t_MarketName) =
         (SELECT tk.t_MarketID,
                 /*pt.t_ShortName*/uGetPatyNameForBrkRep(pt.t_PartyID)||'('||'Фондовый рынок'||')'
            FROM ddlgrdoc_dbt grd, ddlgrdeal_dbt grdeal, ddl_tick_dbt tk, dparty_dbt pt
           WHERE grd.t_DocKind = 1 --Проводка
             AND grd.t_DocID = cs.t_AccTrnID
             AND grdeal.t_ID = grd.t_GrDealID
             AND grdeal.t_DocKind IN (RSB_SECUR.DL_SECURITYDOC, RSB_SECUR.DL_RETIREMENT)
             AND tk.t_DealID = grdeal.t_DocID
             AND pt.t_PartyID = tk.t_MarketID
             and rownum < 2
         )
  WHERE (cs.t_MarketName = CHR(1) OR cs.t_MarketName IS NULL )
     AND EXISTS(SELECT 1
                  FROM ddlgrdoc_dbt grd
                 WHERE grd.t_DocKind = 1 --Проводка
                   AND grd.t_DocID = cs.t_AccTrnID);

  --Проводки шагов операций
  UPDATE dbrkrepcashe_tmp cs
     SET (t_MarketID, t_MarketName) =
         (SELECT mp.t_MarketID,
                 /*pt.t_ShortName*/uGetPatyNameForBrkRep(pt.t_PartyID)||'('||(CASE WHEN sf.t_ServKind = 1 THEN 'Фондовый рынок' WHEN sf.t_ServKind = 21 THEN 'Валютный рынок' WHEN sf.t_ServKind = 15 THEN 'Срочный рынок' ELSE CHR(1) END)||')'
            FROM doprdocs_dbt odoc, doproper_dbt opr, dsfcontr_dbt sf, ddlcontrmp_dbt mp, dparty_dbt pt
           WHERE odoc.t_AccTrnID = cs.t_AccTrnID
             AND opr.t_ID_Operation = odoc.t_ID_Operation
             AND sf.t_ID = (CASE WHEN opr.t_DocKind IN (RSB_SECUR.DL_WRTMONEY, RSB_SECUR.DL_HOLDNDFL) THEN (SELECT np.t_Contract FROM dnptxop_dbt np WHERE np.t_ID = TO_NUMBER(opr.t_DocumentID))
                                 WHEN EXISTS(SELECT 1 FROM ddvndeal_dbt dv WHERE dv.t_DocKInd = opr.t_DocKind AND dv.t_ID = TO_NUMBER(opr.t_DocumentID)) THEN (SELECT dv.t_ClientContr FROM ddvndeal_dbt dv WHERE dv.t_DocKind = opr.t_DocKind AND dv.t_ID = TO_NUMBER(opr.t_DocumentID))
                                 ELSE 0 END)
             AND mp.t_SfContrID = sf.t_ID
             AND pt.t_PartyID = mp.t_MarketID
             and rownum < 2
         )
  WHERE (cs.t_MarketName = CHR(1) OR cs.t_MarketName IS NULL )
     AND EXISTS(SELECT 1 FROM doprdocs_dbt odoc WHERE odoc.t_AccTrnID = cs.t_AccTrnID);

  --Проводки в операции расчётов на срочном рынке
  UPDATE dbrkrepcashe_tmp cs
     SET (t_MarketID, t_MarketName) =
         (SELECT oper.t_Party,
                 /*pt.t_ShortName*/uGetPatyNameForBrkRep(pt.t_PartyID)||'('||'Срочный рынок'||')'
            FROM doprdocs_dbt odoc, doproper_dbt opr, ddvoper_dbt oper, dparty_dbt pt
           WHERE odoc.t_AccTrnID = cs.t_AccTrnID
             AND opr.t_ID_Operation = odoc.t_ID_Operation
             AND opr.t_DocKind = RSB_SECUR.DL_DVOPER
             AND oper.t_ID = ltrim(opr.t_DocumentID)
             AND pt.t_PartyID = oper.t_Party
             and rownum < 2
         )
   WHERE (cs.t_MarketName = CHR(1) OR cs.t_MarketName IS NULL )
     AND EXISTS(SELECT 1 FROM doprdocs_dbt odoc WHERE odoc.t_AccTrnID = cs.t_AccTrnID);

  --Если не ЕДП, то добавить строки для СВОД как копию созданных, но без обслуживания
  --Если ЕДП, то они итак уже сводные
  IF p_IsEDP = 0 THEN
    SELECT
    /*T_CURRENCY   */ t_Currency,
    /*T_DATE       */ t_Date,
    /*T_SERVKIND   */ 0,
    /*T_SERVKINDSUB*/ 0,
    /*T_ISITOG     */ t_IsItog,
    /*T_ACCTRNID   */ t_AccTrnID,
    /*T_OPERNAME   */ t_OperName,
    /*T_MARKETID   */ t_MarketID,
    /*T_MARKETNAME */ t_MarketName,
    /*T_INSUM      */ t_InSum,
    /*T_OUTSUM     */ t_OutSum,
    /*T_OUTNDS     */ t_OutNDS,
    /*T_REST       */ t_Rest,
    /*T_ACCOUNTID  */ t_AccountID
    BULK COLLECT INTO v_brkrep
      FROM dbrkrepcashe_tmp
     WHERE t_ServKindSub <> 9;

    IF v_brkrep.COUNT > 0 THEN
       FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
          INSERT INTO dbrkrepcashe_tmp
               VALUES v_brkrep (indx);
    END IF;
  END IF;


  --Сформировать итоговые строки
  SELECT
  /*T_CURRENCY   */ t_Currency,
  /*T_DATE       */ t_Date,
  /*T_SERVKIND   */ t_ServKind,
  /*T_SERVKINDSUB*/ t_ServKindSub,
  /*T_ISITOG     */ 'X',
  /*T_ACCTRNID   */ 0,
  /*T_OPERNAME   */ CHR(1),
  /*T_MARKETID   */ t_MarketID,
  /*T_MARKETNAME */ CHR(1),
  /*T_INSUM      */ SUM(t_InSum),
  /*T_OUTSUM     */ SUM(t_OutSum),
  /*T_OUTNDS     */ SUM(t_OutNDS),
  /*T_REST       */ SUM(t_InSum)-SUM(t_OutSum)-SUM(t_OutNDS),
  /*T_ACCOUNTID  */ 0
  BULK COLLECT INTO v_brkrep
    FROM dbrkrepcashe_tmp
   WHERE t_IsItog = CHR(0)
   GROUP BY t_Currency, t_Date, t_MarketID, t_ServKind, t_ServKindSub;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepcashe_tmp
             VALUES v_brkrep (indx);
  END IF;

END CreateCasheMoveData;

PROCEDURE GetErrMailTempl(p_TemplID IN NUMBER, p_Subject OUT CLOB, p_Body OUT CLOB, p_err OUT NUMBER)
IS
BEGIN
  SELECT t_Subject, t_Body INTO p_Subject, p_Body FROM dbrkrep_errmailtempl_dbt WHERE t_ID = p_TemplID;
  p_err := 0;
EXCEPTION
  WHEN OTHERS
    THEN p_err := 1;
END;

--Отбор договоро со сделками, заключенными в периоде отчета (помещаются в DSCSRVREPCNTRCONC_TMP)
PROCEDURE GetCntrWithConcDeals(p_BegDate IN DATE, p_EndDate IN DATE, p_ClientID IN NUMBER, p_ByExchange IN NUMBER, p_ByOutExchange IN NUMBER, p_ExcludeUK IN NUMBER)
IS
  v_sql VARCHAR2(10000);
BEGIN
  DELETE FROM DSCSRVREPCNTRCONC_TMP;
  
  v_sql := ' 
    INSERT INTO DSCSRVREPCNTRCONC_TMP (T_PARTYID,
                                       T_DLCONTRID)
    SELECT DISTINCT p.t_ClientID, mp.t_DlContrID 
      FROM ((SELECT tk.t_ClientID, tk.t_ClientContrID
               FROM ddl_tick_dbt tk
              WHERE tk.t_BOfficeKind = 101 /*DL_SECURITYDOC*/
                AND tk.t_DealStatus in (10, 20)
                AND tk.t_DealDate between :p_BegDate and :p_EndDate ';

  IF p_ByExchange <> 0 AND p_ByOutExchange = 0 THEN
    v_sql := v_sql||' AND tk.t_Flag1 = ''X'' ';
  ELSIF p_ByExchange = 0 AND p_ByOutExchange <> 0 THEN 
    v_sql := v_sql||' AND tk.t_Flag1 = CHR(0) '; 
  END IF;

  v_sql := v_sql||' ) ';

  IF p_ByExchange <> 0 THEN
    v_sql := v_sql||' 
            UNION 
            (SELECT dv.t_client, dv.t_clientcontr                                                                                                         
               FROM ddvdeal_dbt dv                                                                                             
              WHERE dv.t_state in (1, 2)                                                                                                                                                                                                                                                     
                and dv.t_type IN (''B'',''S'',''E'') 
                and dv.t_date between :p_BegDate and :p_EndDate
            )
            UNION 
            (SELECT dvn.t_client, dvn.t_clientcontr
               FROM ddvndeal_dbt dvn, ddvnfi_dbt dvnfi, dfininstr_dbt fin
              WHERE dvn.t_dvkind in (6, 7) 
                and dvn.t_state in (1, 2) 
                and dvnfi.t_dealid = dvn.t_id 
                and dvnfi.t_fiid = fin.t_fiid 
                and fin.t_fi_kind = 1 
                and dvn.t_sector = ''X''
                and dvn.t_date between :p_BegDate and :p_EndDate
            ) ';
  END IF; 

  v_sql := v_sql||' 
           ) p, ddlcontrmp_dbt mp
     WHERE p.t_ClientContrID = mp.t_SfContrID
       AND p.t_ClientID > 0 ';

  IF p_ClientID > 0 THEN
    v_sql := v_sql||' AND p.t_ClientID = :p_ClientID ';
  END IF;

  IF p_ExcludeUK <> 0 THEN
    v_sql := v_sql||' AND p.t_ClientID != 114800 ';
  END IF;

  IF p_ByExchange = 0 THEN
    IF p_ClientID > 0 THEN
      execute immediate v_sql
        using p_BegDate, p_EndDate, p_ClientID;
    ELSE
      execute immediate v_sql
        using p_BegDate, p_EndDate;
    END IF;
  ELSE
    IF p_ClientID > 0 THEN
      execute immediate v_sql
        using p_BegDate, p_EndDate, p_BegDate, p_EndDate, p_BegDate, p_EndDate, p_ClientID;
    ELSE
      execute immediate v_sql
        using p_BegDate, p_EndDate, p_BegDate, p_EndDate, p_BegDate, p_EndDate;
    END IF;
  END IF;
END;

END rsb_brkrep_rshb;
/