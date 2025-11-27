CREATE OR REPLACE PACKAGE BODY rsb_brkrep
IS
-- Author  : Nikonorov Evgeny
-- Created : 07.06.2017
-- Purpose : Пакет для подготовки данных для отчета брокера

  g_BrokerComissFIID NUMBER := -1;
  g_PrevMarketID NUMBER := -1;
  g_PrevCodeKind NUMBER := 1;

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
     AND req.t_client = tk.t_clientid 
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
FUNCTION GetMarketComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER, p_MarketID IN NUMBER) RETURN NUMBER
AS

 v_Sum  NUMBER;
BEGIN

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
               AND LOWER(cm.t_Code) NOT IN (LOWER('МскБиржКлирНов'),LOWER('МскБиржИТСНов'),LOWER('СПбБиржКлир'),LOWER('СПбБиржИТС'))
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
               AND LOWER(cm.t_Code) NOT IN (LOWER('МскБиржКлирНов'),LOWER('МскБиржИТСНов'),LOWER('СПбБиржКлир'),LOWER('СПбБиржИТС'))
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  END IF;

  RETURN v_Sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;

END GetMarketComissSum;

--Получить сумму комиссий клирингово центра
FUNCTION GetCliringComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER
AS

 v_Sum  NUMBER;
BEGIN

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
               AND LOWER(cm.t_Code) IN (LOWER('МскБиржКлирНов'), LOWER('СПбБиржКлир'))
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
               AND LOWER(cm.t_Code) IN (LOWER('МскБиржКлирНов'), LOWER('СПбБиржКлир'))
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  END IF;

  RETURN v_Sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;

END GetCliringComissSum;

--Получить сумму комиссий за ИТС
FUNCTION GetITSComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER
AS

 v_Sum  NUMBER;
BEGIN

  IF p_Part = 1 THEN
    SELECT q1.CommSum INTO v_Sum
      FROM (SELECT NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
              FROM (SELECT dlc.t_Sum /*уже включает в себя НДС*/ AS CommSum, dlc.t_FeeType, dlc.t_ComNumber 
                      FROM ddlcomis_dbt dlc 
                     WHERE dlc.t_DocKind = p_DocKind                                                                                                                           
                       AND dlc.t_DocID = p_DocID 
                       AND dlc.t_IsBack = CHR(0)
                    UNION 
                    SELECT basobj.t_CommSum /*уже включает в себя НДС*/ AS CommSum, defcom.t_FeeType, defcom.t_CommNumber 
                      FROM dsfbasobj_dbt basobj, dsfdefcom_dbt defcom 
                     WHERE basobj.t_BaseObjectType = p_DocKind
                       AND basobj.t_BaseObjectID = p_DocID
                       AND defcom.t_ID = basobj.t_DefCommID
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_FeeType = q.t_Feetype                                                                                                                 
               AND cm.t_Number  = q.t_ComNumber 
               AND LOWER(cm.t_Code) IN (LOWER('МскБиржИТСНов'), LOWER('СПбБиржИТС'))
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  ELSE

    SELECT q1.CommSum INTO v_Sum
      FROM (SELECT NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
              FROM (SELECT dlc.t_Sum /*уже включает в себя НДС*/ AS CommSum, dlc.t_FeeType, dlc.t_ComNumber 
                      FROM ddlcomis_dbt dlc 
                     WHERE dlc.t_DocKind = p_DocKind                                                                                                                           
                       AND dlc.t_DocID = p_DocID 
                       AND dlc.t_IsBack = 'X'
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_FeeType = q.t_Feetype                                                                                                                 
               AND cm.t_Number  = q.t_ComNumber 
               AND LOWER(cm.t_Code) IN (LOWER('МскБиржИТСНов'), LOWER('СПбБиржИТС'))
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  END IF;

  RETURN v_Sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;

END GetITSComissSum;

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
  v_brkrep.t_A95_M     := CHR(0);
  
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
    g_PrevCodeKind := v_CodeKInd;
  END IF;

  RETURN v_CodeKind;
END;

--Формирование данных по сделкам для разделов 1-3 отчета
PROCEDURE CreateDealData( p_ClientID      IN NUMBER,
                          p_ContrID       IN NUMBER,
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

  WITH q as (
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
                    fin.t_FIID
               FROM ddl_tick_dbt tk, ddlrq_dbt rq, ddl_leg_dbt leg, dfininstr_dbt fin, davoiriss_dbt avr
              WHERE tk.t_BOfficeKind   = 101 /*DL_SECURITYDOC*/
                AND tk.t_ClientID      = p_ClientID
                AND tk.t_ClientContrID = p_ContrID
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
                    fin.t_FIID
               FROM ddl_tick_dbt tk, ddl_leg_dbt leg, dfininstr_dbt fin, davoiriss_dbt avr
              WHERE tk.t_BOfficeKind   = 101 /*DL_SECURITYDOC*/
                AND tk.t_ClientID      = p_ClientID
                AND tk.t_ClientContrID = p_ContrID
                AND leg.t_DealID       = tk.t_DealID
                AND fin.t_FIID         = tk.t_PFI
                AND avr.t_FIID         = fin.t_FIID
                AND tk.t_Flag1 = (CASE WHEN p_ByExchange <> 0 AND p_ByOutExchange = 0 THEN 'X'
                                       WHEN p_ByExchange = 0 AND p_ByOutExchange <> 0 THEN CHR(0)
                                       ELSE tk.t_Flag1 END )
                AND Rsb_Secur.IsBasket( Rsb_Secur.get_OperationGroup( Rsb_Secur.get_OperSysTypes( tk.t_DealType, tk.t_BOfficeKind ) ) ) = 1
             )
  SELECT    p_ClientID,
            p_ContrID,
            p_Part,
            GetSfPlanID(p_ContrID, q.t_DealDate),
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
  /*A09*/   RSI_RSBPARTY.GetPartyCode(q.t_MarketID, 1 /*PTCK_CONTR*/),
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
  /*A20*/   GetMarketComissSum(q.t_BOfficeKind, q.t_DealID, q.t_DealPart, q.t_MarketID),
  /*A21*/   GetCliringComissSum(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*A22*/   GetITSComissSum(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
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
  /*A95_M*/ q.IsMarginCall,
  /*ServKind*/    0,
  /*ServKindSub*/ 0,
  /*MarketID*/-1
  BULK COLLECT INTO v_brkrep
  FROM q
  WHERE 1 = (CASE WHEN p_Part = 1 AND q.t_DealDate >= p_BegDate AND q.t_DealDate <= p_EndDate THEN 1
                  WHEN p_Part = 2 AND q.t_DealDate < p_BegDate AND (q.FactExecDate > p_EndDate OR q.FactExecDate = TO_DATE('01.01.0001','DD.MM.YYYY')) THEN 1
                  WHEN p_Part = 3 AND q.IsRepo = 0 AND q.FactExecDate BETWEEN p_BegDate AND p_EndDate THEN 1
                  WHEN p_Part = 4 AND q.IsRepo = 1 AND q.FactExecDate BETWEEN p_BegDate AND p_EndDate THEN 1
                  ELSE 0 END );

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepdeal_tmp
             VALUES v_brkrep (indx);
  END IF;

  --Создать итоговые строки
  FOR one_curr IN (SELECT cur.t_FIID, cur.t_CCY
                     FROM dfininstr_dbt cur
                    WHERE cur.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY
                      AND EXISTS(SELECT 1
                                   FROM dbrkrepdeal_tmp rep
                                  WHERE rep.t_ClientID = p_ClientID
                                    AND rep.t_ContrID  = p_ContrID
                                    AND rep.t_Part     = p_Part
                                    AND rep.t_IsItog   = CHR(0)
                                    AND (   rep.t_A14 = cur.t_CCY
                                         OR rep.t_A19 = cur.t_CCY
                                         OR (cur.t_FIID = RSI_RSB_FIInstr.NATCUR AND (rep.t_A20 <> 0 OR rep.t_A21 <> 0 OR rep.t_A22 <> 0 OR rep.t_A27 <> 0))
                                        )
                                )
                  )
  LOOP

      SELECT    p_ClientID,
                p_ContrID,
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
      /*A95_M*/ CHR(0),
      /*ServKind*/    0,
      /*ServKindSub*/ 0,
      /*MarketID*/-1
      BULK COLLECT INTO v_brkrep
      FROM (SELECT NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A16 ELSE 0 END)), 0) AS t_A16, 
                   NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A17 ELSE 0 END)), 0) AS t_A17, 
                   NVL(SUM((CASE WHEN t_A19_C = one_curr.t_FIID THEN t_A18 ELSE 0 END)), 0) AS t_A18, 
                   NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A20 ELSE 0 END)), 0) AS t_A20, 
                   NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A21 ELSE 0 END)), 0) AS t_A21, 
                   NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A22 ELSE 0 END)), 0) AS t_A22, 
                   NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A27 ELSE 0 END)), 0) AS t_A27, 
                   t_PlanID, t_Direction 
              FROM dbrkrepdeal_tmp 
             WHERE t_ClientID = p_ClientID 
               AND t_ContrID = p_ContrID 
               AND t_Part = p_Part 
               AND t_IsItog = CHR(0) 
               AND (   t_A14 = one_curr.t_CCY
                    OR t_A19 = one_curr.t_CCY
                    OR (one_curr.t_FIID = RSI_RSB_FIInstr.NATCUR AND (t_A20 <> 0 OR t_A21 <> 0 OR t_A22 <> 0 OR t_A27 <> 0))
                   )
            GROUP BY t_PlanID, t_Direction
           ) q;
    
    IF v_brkrep.COUNT > 0 THEN
       FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
          INSERT INTO dbrkrepdeal_tmp
               VALUES v_brkrep (indx);
    END IF;

  END LOOP;

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

--Формирование данных по компенсационным выплатам, компенсационным поставкам, купонным выплатам для раздела 4
PROCEDURE CreateCompData( p_ClientID      IN NUMBER,
                          p_ContrID       IN NUMBER,
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


  SELECT    q.t_ClientID,
            q.t_ClientContrID,
            p_Part,
            GetSfPlanID(q.t_ClientContrID, q.t_DealDate),
            CHR(0),
            (CASE WHEN q.IsRepo = 1 AND q.IsBuy = 1 THEN DECODE(q.t_DealPart, 1, 1, 2)
                  WHEN q.IsRepo = 1 AND q.IsSale = 1 THEN DECODE(q.t_DealPart, 1, 2, 1)
                  ELSE DECODE(q.IsBuy, 1, 1, 2) END
            ),
  /*A05_D*/ q.t_DealDate,
  /*A05_T*/ q.t_DealTime,
  /*A06*/   GetPlanExecDate(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*A07*/   q.t_DealCode,
  /*A08*/   CHR(1),
  /*A09*/   RSI_RSBPARTY.GetPartyCode(q.t_MarketID, 1 /*PTCK_CONTR*/),
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
  /*A14*/   CHR(1),
  /*A14_C*/ q.t_CFI,
  /*A15*/   q.t_Amount,
  /*A16*/   q.t_TotalCost,
  /*A17*/   RSI_RSB_FIInstr.ConvSum(q.t_TotalCost, q.t_CFI, RSI_RSB_FIInstr.NATCUR, CASE WHEN q.t_State = RSI_DLRQ.DLRQ_STATE_EXEC THEN GetPlanExecDate(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) ELSE p_EndDate END),
  /*A18*/   0,
  /*A19*/   CHR(1),
  /*A19_C*/ -1,
  /*A20*/   0,
  /*A21*/   0,
  /*A22*/   0,
  /*A23*/   CHR(1),
  /*A24*/   NVL((SELECT pt.t_ShortName 
                   FROM dparty_dbt pt 
                  WHERE pt.t_PartyID = q.t_PartyID), CHR(1)),
  /*A25*/   CHR(1),
  /*A26*/   q.t_NKD,
  /*A27*/   0,
  /*A28*/   case q.t_LegKind when 2 then ( case IsSale  when 1 then q.t_TotalCost else 0 end) else (case IsBuy when 1 then q.t_TotalCost else 0 end)  end,
  /*A29*/   case q.t_LegKind when 2 then ( case IsBuy when 1 then q.t_Amount   else 0 end) else (case IsSale  when 1 then q.t_Amount    else 0 end)  end,  
  /*A30*/   CHR(1),
  /*A31*/   q.t_FactDate,
  /*A32_1*/ q.CoupSum,
  /*A32_2*/ q.CoupSumFIID,
  /*A33_1*/ q.CompPaySum,
  /*A33_2*/ q.CompPaySumFIID,
  /*A33_3*/ q.CompPaySumNatcur,
  /*A34*/   NVL(RSI_RSB_FIInstr.ConvSum(q.CoupSum, q.CoupSumFIID, RSI_RSB_FIInstr.NATCUR, q.t_FactDate), 0) + NVL(q.CompPaySumNatcur, 0),
  /*A35*/   GetCurrentRQAmount(q.t_BOfficeKind, q.t_DealID, RSI_DLRQ.DLRQ_TYPE_COMPDELIVERY, p_EndDate, q.t_FactDate, q.IsBuy, q.IsSale, -1, q.t_DealPart),
  /*FIID*/  q.t_FIID,
  /*A95*/   0,
  /*A95_M*/ CHR(0),
  /*ServKind*/    0,
  /*ServKindSub*/ 0,
  /*MarketID*/-1 
  BULK COLLECT INTO v_brkrep
  FROM (WITH d AS (SELECT DISTINCT tk.t_DealID, rq.t_FactDate, rq.t_DealPart, rq.t_State,
                          RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsBuy, 
                          RSB_SECUR.IsSale(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsSale 
                     FROM ddl_tick_dbt tk, ddlrq_dbt rq 
                    WHERE tk.t_BOfficeKind = 101 /*DL_SECURITYDOC*/
                      AND tk.t_ClientID = p_ClientID
                      AND tk.t_ClientContrID = p_ContrID
                      AND rq.t_DocKind = tk.t_BOfficeKind 
                      AND rq.t_DocID = tk.t_DealID 
                      AND rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_COMPPAYM,RSI_DLRQ.DLRQ_TYPE_PAYMREPOCOUP,RSI_DLRQ.DLRQ_TYPE_COMPDELIVERY)
                      AND rq.t_FactDate >= p_BegDate
                      AND rq.t_FactDate <= p_EndDate
                      AND RSB_SECUR.IsRepo(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) = 1 
                  ) 
        --репо без корзины
        SELECT tk.t_DealID, tk.t_BOfficeKind, tk.t_DealDate, tk.t_DealTime, tk.t_ClientID, tk.t_ClientContrID,
               tk.t_DealCode, tk.t_DealCodeTS, tk.t_MarketID, tk.t_Flag1, tk.t_PartyID,
               leg.t_LegKind, leg.t_IncomeRate, leg.t_CFI, 
               GetRQAmountCashOnDate(rq.t_DocKind, rq.t_DocID, rq.t_DealPart , p_EndDate) as t_TotalCost,
               leg.t_NKD,
               GetRQAmountSecuritiesOnDate(rq.t_DocKind, rq.t_DocID, rq.t_DealPart, p_EndDate) as t_Amount,
               rq.t_DealPart, 
               avr.t_LSIN, avr.t_ISIN, fin.t_FaceValueFI, fin.t_Issuer,
               d.t_FactDate,
               d.t_State,
               1 as IsRepo,
               d.IsBuy, 
               d.IsSale,
               GetCurrentRQAmount(tk.t_BOfficeKind, tk.t_DealID, RSI_DLRQ.DLRQ_TYPE_COMPPAYM, p_EndDate, d.t_FactDate, d.IsBuy, d.IsSale, -1, rq.t_DealPart) as CompPaySum,
               GetCurrentRQFIID(tk.t_BOfficeKind, tk.t_DealID, RSI_DLRQ.DLRQ_TYPE_COMPPAYM, d.t_FactDate, rq.t_DealPart) as CompPaySumFIID,
               GetCurrentRQAmount(tk.t_BOfficeKind, tk.t_DealID, RSI_DLRQ.DLRQ_TYPE_COMPPAYM, p_EndDate, d.t_FactDate, d.IsBuy, d.IsSale, RSI_RSB_FIInstr.NATCUR, rq.t_DealPart) as CompPaySumNatCur,
               GetCurrentRQAmount(tk.t_BOfficeKind, tk.t_DealID, RSI_DLRQ.DLRQ_TYPE_PAYMREPOCOUP, p_EndDate, d.t_FactDate, d.IsBuy, d.IsSale, -1, rq.t_DealPart) as CoupSum,
               GetCurrentRQFIID(tk.t_BOfficeKind, tk.t_DealID, RSI_DLRQ.DLRQ_TYPE_PAYMREPOCOUP, d.t_FactDate, rq.t_DealPart) as CoupSumFIID,
               fin.t_FIID
          FROM d, ddl_tick_dbt tk, ddlrq_dbt rq, ddl_leg_dbt leg, dfininstr_dbt fin, davoiriss_dbt avr 
         WHERE tk.t_DealID = d.t_DealID  
           AND rq.t_DocKind = tk.t_BOfficeKind 
           AND rq.t_DocID = tk.t_DealID 
           AND rq.t_DealPart = d.t_DealPart 
           AND rq.t_Type  = RSI_DLRQ.DLRQ_TYPE_DELIVERY
           AND leg.t_DealID = tk.t_DealID
           AND leg.t_LegKind = DECODE(rq.t_DealPart, 1, 0 /*LEG_KIND_DL_TICK*/, 2 /*LEG_KIND_DL_TICK_BACK*/)
           AND leg.t_LegID = 0 
           AND fin.t_FIID = tk.t_PFI 
           AND avr.t_FIID = fin.t_FIID 
           AND tk.t_Flag1 = (CASE WHEN p_ByExchange <> 0 AND p_ByOutExchange = 0 THEN 'X'
                                  WHEN p_ByExchange = 0 AND p_ByOutExchange <> 0 THEN CHR(0)
                                  ELSE tk.t_Flag1 END )
           AND Rsb_Secur.IsBasket( Rsb_Secur.get_OperationGroup( Rsb_Secur.get_OperSysTypes( tk.t_DealType, tk.t_BOfficeKind ) ) ) = 0
           
           UNION ALL
           --репо с корзиной
           SELECT tk.t_DealID, tk.t_BOfficeKind, tk.t_DealDate, tk.t_DealTime, tk.t_ClientID, tk.t_ClientContrID,
               tk.t_DealCode, tk.t_DealCodeTS, tk.t_MarketID, tk.t_Flag1, tk.t_PartyID,
               leg.t_LegKind, leg.t_IncomeRate, leg.t_CFI, 
               GetRQAmountCashOnDate( tk.t_BofficeKind, tk.t_DealID, d.t_DealPart , p_EndDate ) as t_TotalCost,
               leg.t_NKD,
               GetRQAmountSecuritiesOnDate( tk.t_BofficeKind, tk.t_DealID, d.t_DealPart, p_EndDate ) as t_Amount,
               decode (leg.t_LegKind,0,1,2), 
               avr.t_LSIN, avr.t_ISIN, fin.t_FaceValueFI, fin.t_Issuer,
               d.t_FactDate,
               d.t_State,
               1 as IsRepo,
               d.IsBuy, 
               d.IsSale,
               GetCurrentRQAmount(tk.t_BOfficeKind, tk.t_DealID, RSI_DLRQ.DLRQ_TYPE_COMPPAYM, p_EndDate, d.t_FactDate, d.IsBuy, d.IsSale, -1, d.t_DealPart) as CompPaySum,
               GetCurrentRQFIID(tk.t_BOfficeKind, tk.t_DealID, RSI_DLRQ.DLRQ_TYPE_COMPPAYM, d.t_FactDate, d.t_DealPart) as CompPaySumFIID,
               GetCurrentRQAmount(tk.t_BOfficeKind, tk.t_DealID, RSI_DLRQ.DLRQ_TYPE_COMPPAYM, p_EndDate, d.t_FactDate, d.IsBuy, d.IsSale, RSI_RSB_FIInstr.NATCUR, d.t_DealPart) as CompPaySumNatcur,
               GetCurrentRQAmount(tk.t_BOfficeKind, tk.t_DealID, RSI_DLRQ.DLRQ_TYPE_PAYMREPOCOUP, p_EndDate, d.t_FactDate, d.IsBuy, d.IsSale, DECODE(tk.t_Flag1, 'X', leg.t_CFI, fin.t_FaceValueFI), d.t_DealPart) as CoupSum,
               GetCurrentRQFIID(tk.t_BOfficeKind, tk.t_DealID, RSI_DLRQ.DLRQ_TYPE_PAYMREPOCOUP, d.t_FactDate, d.t_DealPart) as CoupSumFIID,
               fin.t_FIID
          FROM d, ddl_tick_dbt tk, ddl_leg_dbt leg, dfininstr_dbt fin, davoiriss_dbt avr 
         WHERE tk.t_DealID = d.t_DealID  
           AND leg.t_DealID = tk.t_DealID
           AND leg.t_LegKind = DECODE(d.t_DealPart, 1, 0 /*LEG_KIND_DL_TICK*/, 2 /*LEG_KIND_DL_TICK_BACK*/)
           AND leg.t_LegID = 0 
           AND fin.t_FIID = tk.t_PFI 
           AND avr.t_FIID = fin.t_FIID 
           AND tk.t_Flag1 = (CASE WHEN p_ByExchange <> 0 AND p_ByOutExchange = 0 THEN 'X'
                                  WHEN p_ByExchange = 0 AND p_ByOutExchange <> 0 THEN CHR(0)
                                  ELSE tk.t_Flag1 END )
           AND Rsb_Secur.IsBasket( Rsb_Secur.get_OperationGroup( Rsb_Secur.get_OperSysTypes( tk.t_DealType, tk.t_BOfficeKind ) ) ) = 1
       ) q;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepdeal_tmp
             VALUES v_brkrep (indx);
  END IF;

END CreateCompData;

--Формирование данных по погашениям купонов, частичным погашениям, погашениям выпусков для раздела 5
PROCEDURE CreateRetireData( p_ClientID      IN NUMBER,
                            p_ContrID       IN NUMBER,
                            p_Part          IN NUMBER,
                            p_BegDate       IN DATE,
                            p_EndDate       IN DATE
                          ) 
IS

  TYPE brkrep_t IS TABLE OF DBRKREPDEAL_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;
  v_brkrec DBRKREPDEAL_TMP%ROWTYPE;
  v_brkrec2 DBRKREPDEAL_TMP%ROWTYPE;
BEGIN

  FOR cData IN ( SELECT tk.t_DealDate, tk.t_DealID, tk.t_BofficeKind,
                        RSB_SECUR.IsRet_Issue(RSB_SECUR.get_OperationGroup(Rsb_Secur.get_OperSysTypes(tk.t_DealType, tk.t_BOfficeKind))) t_IsRet_Issue,
                        RSB_SECUR.IsRet_Coupon(RSB_SECUR.get_OperationGroup(Rsb_Secur.get_OperSysTypes(tk.t_DealType, tk.t_BOfficeKind))) t_IsRet_Coupon,
                        RSB_SECUR.IsRet_Partly(RSB_SECUR.get_OperationGroup(Rsb_Secur.get_OperSysTypes(tk.t_DealType, tk.t_BOfficeKind))) t_IsRet_Partly,
                        NVL((SELECT t_ShortName FROM dparty_dbt WHERE t_PartyID = FI.t_Issuer), CHR(1)) t_Issuer,
                        FI.t_FIID,
                        (CASE WHEN AVR.t_LSIN IS NOT NULL AND AVR.t_LSIN NOT IN (CHR(0), CHR(1)) THEN AVR.t_LSIN ELSE NVL(AVR.t_ISIN, CHR(1)) END) t_LSIN_ISIN,
                        leg.t_Principal,
                        FI.t_FaceValueFI,
                        NVL((SELECT f.t_CCY FROM dfininstr_dbt f WHERE f.t_FIID = FI.t_FaceValueFI), CHR(1)) t_CCY,
                        leg.t_Cost,
                        leg.t_NKD,
                        tk.t_Number_Coupon,
                        tk.t_Number_Partly
                   FROM DFININSTR_DBT FI, DAVOIRISS_DBT AVR,
                        DDL_LEG_DBT leg, DDL_TICK_DBT tk
                  WHERE tk.t_BofficeKind = RSB_SECUR.DL_RETIREMENT
                    AND tk.t_ClientID = p_ClientID
                    AND tk.t_ClientContrID = p_ContrID
                    AND tk.t_DealStatus >= 10--DL_READIED
                    AND leg.t_DealID = tk.t_DealID
                    AND leg.t_LegID = 0
                    AND leg.t_LegKind = 0
                    AND FI.t_FIID = tk.t_PFI
                    AND AVR.t_FIID = FI.t_FIID
                    AND EXISTS ( SELECT 1
                                   FROM DDLRQ_DBT
                                  WHERE t_DocKind = tk.t_BofficeKind
                                    AND t_DocID = tk.t_DealID
                                    AND t_DealPart = 1
                                    AND t_Type = RSI_DLRQ.DLRQ_TYPE_PAYMENT
                                    AND t_State = RSI_DLRQ.DLRQ_STATE_EXEC
                                    AND t_FactDate BETWEEN p_BegDate AND p_EndDate
                               )
               )
  LOOP
    v_brkrec := NULL;

    v_brkrec.t_ClientID      := p_ClientID;
    v_brkrec.t_ContrID       := p_ContrID;
    v_brkrec.t_Part          := p_Part;
    v_brkrec.t_PlanID        := 0;
    v_brkrec.t_IsItog        := CHR(0);    
    v_brkrec.t_A05_D         := cData.t_DealDate;
    IF cData.t_IsRet_Issue = 1 THEN
      v_brkrec.t_A07         := 'Погашение выпуска';
      v_brkrec.t_A16         := cData.t_Cost;
      v_brkrec.t_A24         := 'Выплата номинала при погашении выпуска';
      v_brkrec.t_Direction   := 3;
    ELSIF cData.t_IsRet_Coupon = 1 THEN
      v_brkrec.t_A07         := 'Погашение купона';
      v_brkrec.t_A16         := cData.t_NKD;
      v_brkrec.t_A24         := 'Выплата купонного дохода';
      v_brkrec.t_Direction   := 1;
    ELSIF cData.t_IsRet_Partly = 1 THEN
      v_brkrec.t_A07         := 'Частичное погашение';
      v_brkrec.t_A16         := cData.t_Cost;
      v_brkrec.t_A24         := 'Выплата номинала при частичном погашении';
      v_brkrec.t_Direction   := 2;
    END IF;
    v_brkrec.t_A11           := cData.t_Issuer;
    v_brkrec.t_FIID          := cData.t_FIID;
    v_brkrec.t_A12           := cData.t_LSIN_ISIN;
    v_brkrec.t_A15           := cData.t_Principal;
    v_brkrec.t_A14           := cData.t_CCY;
    v_brkrec.t_A17           := RSI_RSB_FIInstr.ConvSum(v_brkrec.t_A16, cData.t_FaceValueFI, RSI_RSB_FIInstr.NATCUR, GetPlanExecDate(cData.t_BOfficeKind, cData.t_DealID, 1));

    IF (cData.t_IsRet_Issue = 1 OR cData.t_IsRet_Partly = 1) AND cData.t_Number_Coupon IS NOT NULL AND cData.t_Number_Coupon NOT IN (CHR(0), CHR(1)) THEN
      v_brkrec2 := NULL;

      v_brkrec2.t_ClientID   := p_ClientID;
      v_brkrec2.t_ContrID    := p_ContrID;
      v_brkrec2.t_Part       := p_Part;
      v_brkrec2.t_PlanID     := 0;
      v_brkrec2.t_IsItog     := CHR(0);
      v_brkrec2.t_A05_D      := cData.t_DealDate;
      v_brkrec2.t_A07        := 'Погашение купона';
      v_brkrec2.t_A16        := cData.t_NKD;
      v_brkrec2.t_A24        := 'Выплата купонного дохода';
      v_brkrec2.t_A17        := RSI_RSB_FIInstr.ConvSum(v_brkrec2.t_A16, cData.t_FaceValueFI, RSI_RSB_FIInstr.NATCUR, GetPlanExecDate(cData.t_BOfficeKind, cData.t_DealID, 1));
      v_brkrec2.t_A11        := cData.t_Issuer;
      v_brkrec2.t_FIID       := cData.t_FIID;
      v_brkrec2.t_A12        := cData.t_LSIN_ISIN;
      v_brkrec2.t_A15        := cData.t_Principal;
      v_brkrec2.t_A14        := cData.t_CCY;
      v_brkrec2.t_Direction   := 1;

      v_brkrep( v_brkrep.Count+1 ) := v_brkrec2;
    END IF;

    v_brkrep( v_brkrep.Count+1 ) := v_brkrec;
  END LOOP;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepdeal_tmp VALUES v_brkrep (indx);
  END IF;

END CreateRetireData;

--Получить идентификатор пула сччета
FUNCTION GetAccPoolID(p_ContrID       IN NUMBER,
                      p_Account       IN VARCHAR2,
                      p_Code_Currency IN NUMBER,
                      p_Chapter       IN NUMBER
                     ) RETURN NUMBER
AS
  v_PoolID NUMBER := -1;
BEGIN

  SELECT (CASE WHEN cat.t_Class1 = 1859 THEN tpl.t_Value1 
               WHEN cat.t_Class2 = 1859 THEN tpl.t_Value2 
               WHEN cat.t_Class3 = 1859 THEN tpl.t_Value3 
               WHEN cat.t_Class4 = 1859 THEN tpl.t_Value4 
               WHEN cat.t_Class5 = 1859 THEN tpl.t_Value5 
               WHEN cat.t_Class6 = 1859 THEN tpl.t_Value6 
               WHEN cat.t_Class7 = 1859 THEN tpl.t_Value7 
               WHEN cat.t_Class8 = 1859 THEN tpl.t_Value8 
               ELSE 0 END) INTO v_PoolID
   FROM dmcaccdoc_dbt accd, dmccateg_dbt cat, dmctempl_dbt tpl 
  WHERE accd.t_Chapter = p_Chapter 
    AND accd.t_Currency = p_Code_Currency 
    AND accd.t_Account = p_Account 
    AND accd.t_ClientContrID = p_ContrID 
    AND cat.t_Number = accd.t_CatNum
    AND cat.t_LevelType = 1
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

--Формирование данных по внутреннему учёту для раздела 7 отчета (только фактические данные)
PROCEDURE CreateInAccData( p_ClientID      IN NUMBER,
                           p_ContrID       IN NUMBER,
                           p_BegDate       IN DATE,
                           p_EndDate       IN DATE
                         ) 
IS

  TYPE brkrep_t IS TABLE OF DBRKREPINACC_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;

BEGIN

  WITH q AS (SELECT s.*
               FROM (SELECT acc.t_Code_Currency as t_FIID, 
                            -1*rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_BegDate-1, acc.t_Chapter, null) as InRest, 
                            -1*rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_EndDate, acc.t_Chapter, null) as OutRest, 
                            (CASE WHEN GetAccPoolID(p_ContrID, acc.t_Account, acc.t_Code_Currency, acc.t_Chapter) <= 0 
                                       THEN rsb_account.kreditac(acc.t_Account,acc.t_Chapter,acc.t_Code_Currency,p_BegDate,p_EndDate, null) 
                                            - GetPoolAccTrnSum(acc.t_AccountID, 0, p_BegDate, p_EndDate)
                                       ELSE 0 END) as WrtOffSum, 
                            (CASE WHEN GetAccPoolID(p_ContrID, acc.t_Account, acc.t_Code_Currency, acc.t_Chapter) <= 0
                                        THEN rsb_account.debetac(acc.t_Account,acc.t_Chapter,acc.t_Code_Currency,p_BegDate,p_EndDate, null) 
                                             - GetPoolAccTrnSum(acc.t_AccountID, 1, p_BegDate, p_EndDate)
                                        ELSE 0 END)as EnrolSum,
                            (SELECT accd.t_Place 
                               FROM dmcaccdoc_dbt accd 
                              WHERE accd.t_Chapter = acc.t_Chapter 
                                AND accd.t_Currency = acc.t_Code_Currency 
                                AND accd.t_Account = acc.t_Account 
                                AND accd.t_ClientContrID = p_ContrID 
                                AND ROWNUM = 1) as t_PlaceID
                       FROM daccount_dbt acc 
                      WHERE acc.t_Chapter = 22 
                        AND acc.t_Client = p_ClientID 
                        AND EXISTS(SELECT 1
                                     FROM dmcaccdoc_dbt accd, dmccateg_dbt cat
                                    WHERE accd.t_Chapter = acc.t_Chapter
                                      AND accd.t_Currency = acc.t_Code_Currency
                                      AND accd.t_Account = acc.t_Account
                                      AND accd.t_ClientContrID = p_ContrID
                                      AND cat.t_Number = accd.t_CatNum
                                      AND cat.t_LevelType = 1
                                      AND cat.t_Code = 'ЦБ Клиента, ВУ'
                                  )
                     ) s
              WHERE (s.InRest != 0 OR s.OutRest != 0 OR s.EnrolSum != 0 OR s.WrtOffSum != 0)
             )
  SELECT    p_ClientID,
            p_ContrID,
            CHR(0),
            q.t_PlaceID,
            q.t_FIID,
  /*A53*/   CHR(1),
  /*A54*/   CHR(1),
  /*A55*/   NVL(SUM(q.InRest), 0),
  /*A55_1*/ 0,
  /*A56*/   NVL(SUM(q.EnrolSum), 0),
  /*A56_1*/ 0,
  /*A56_2*/ 0,
  /*A57*/   NVL(SUM(q.WrtOffSum), 0),
  /*A57_1*/ 0,
  /*A57_2*/ 0,
  /*A58*/   NVL(SUM(q.OutRest), 0),
  /*A58_1*/ 0,
  /*A59*/   0,
  /*A59_1*/ -1,
  /*A60*/   0,
  /*A61*/   0,
  /*A62*/   0,
  /*A63*/   0,
  /*ServKind*/    0,
  /*ServKindSub*/ 0,
  /*MarketID*/-1
  BULK COLLECT INTO v_brkrep
    FROM q
  GROUP BY q.t_PlaceID, q.t_FIID;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepinacc_tmp
             VALUES v_brkrep (indx);
  END IF;

END CreateInAccData;

FUNCTION GetActiveRateId(p_FIID IN NUMBER, p_Date IN DATE) RETURN NUMBER
IS
  v_CourseTypeMP  NUMBER;
  v_CourseTypeAVR NUMBER;
  
  v_RateId        NUMBER := -1;
BEGIN
  v_CourseTypeMP := Rsb_Common.GetRegIntValue( 'SECUR\ВИД КУРСА РЫНОЧНАЯ ЦЕНА', 0 );
  v_CourseTypeAVR := Rsb_Common.GetRegIntValue( 'SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0 );

  v_RateId := RSB_SPREPFUN.GetRateIdByMPWithMaxTradeVolume(p_Date, p_FIID, v_CourseTypeMP, 90);
  
  IF v_RateId = -1 THEN
    v_RateId := RSB_SPREPFUN.GetRateIdByMPWithMaxTradeVolume(p_Date, p_FIID, v_CourseTypeAVR, 90);
  END IF;

  RETURN v_RateID;
EXCEPTION
  WHEN OTHERS THEN RETURN -1;
END GetActiveRateId;

FUNCTION GetActiveBalanceCost( p_FIID        IN NUMBER,
                               p_Date        IN DATE,
                               p_ClientId    IN NUMBER,
                               p_ContrId     IN NUMBER
                             ) RETURN NUMBER
IS
  v_lot DPMWRTSUM_DBT%ROWTYPE;
BEGIN
  RSB_PMWRTOFF.WRTRestOnDate(-1, p_FIID, p_ClientId, p_ContrId, -1, p_Date, -1, 1, v_lot);
  RETURN v_lot.t_BalanceCost;
END GetActiveBalanceCost;

--Корректировка данных по внутреннему учёту для раздела 7 отчета с учетом плановых движений
PROCEDURE CorrectInAccData( p_ClientID      IN NUMBER,
                            p_ContrID       IN NUMBER,
                            p_EndDate       IN DATE
                          ) 
IS

  TYPE brkrep_t IS TABLE OF DBRKREPINACC_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;
BEGIN

  SELECT    p_ClientID,
            p_ContrID,
            CHR(0),
            q.t_PlaceID,
            q.t_FIID,
  /*A53*/   (CASE WHEN avr.t_LSIN <> CHR(1) THEN avr.t_LSIN ELSE avr.t_ISIN END),
  /*A54*/   NVL((SELECT t_ShortName FROM dparty_dbt WHERE t_PartyID = t_Issuer), CHR(1)),
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
  /*A59*/   RSB_SPREPFUN.GetCourse(q.t_RateId, p_EndDate),--NVL(RSB_Secur.SC_ConvSumTypeRep(1, fin.t_FIID, fin.t_FaceValueFI, fin.t_FaceValueFI, v_CourceType, p_EndDate), 0),
  /*A59_1*/ RSB_SPREPFUN.GetCourseFI(q.t_RateId),
  /*A60*/   0,
  /*A61*/   0,
  /*A62*/   RSI_RSB_FIInstr.FI_CalcNKD(fin.t_FIID, p_EndDate, q.t_A58, 0),
  /*A63*/   0,
  /*ServKind*/    0,
  /*ServKindSub*/ 0,
  /*MarketID*/-1 
  BULK COLLECT INTO v_brkrep
  FROM (SELECT NVL(SUM(t_A55), 0)   AS t_A55, 
               NVL(SUM(t_A56), 0)   AS t_A56,
               NVL(SUM(t_A56_1), 0) AS t_A56_1,
               NVL(SUM(t_A57), 0)   AS t_A57, 
               NVL(SUM(t_A57_1), 0) AS t_A57_1,
               NVL(SUM(t_A58), 0)   AS t_A58,
               NVL(SUM(t_A58+t_A56_1-t_A57_1), 0) AS t_A58_1,
               GetActiveRateId( t_FIID, p_EndDate) t_RateId,
               t_PlaceID,
               t_FIID,
               NVL((SELECT mrkt.t_Market
                      FROM ddldepset_dbt dep, ddlmarket_dbt mrkt
                     WHERE dep.t_Depositary = t_PlaceId AND mrkt.t_DepSetId = dep.t_DepSetId AND ROWNUM = 1 ),
                   -1) AS t_MarketId
          FROM dbrkrepinacc_tmp 
         WHERE t_ClientID = p_ClientID 
           AND t_ContrID = p_ContrID
           AND t_IsItog = CHR(0)
        GROUP BY t_PlaceID, t_FIID
       ) q, dfininstr_dbt fin, davoiriss_dbt avr
  WHERE fin.t_FIID = q.t_FIID
    AND avr.t_FIID = fin.t_FIID;


  DELETE FROM dbrkrepinacc_tmp 
        WHERE t_ClientID = p_ClientID 
          AND t_ContrID = p_ContrID;


  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepinacc_tmp
             VALUES v_brkrep (indx);
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
                                   ELSE CASE WHEN q1.t_A58 > 0 THEN (q1.t_bc-q1.t_A62)/q1.t_A58 ELSE 0 END
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
                                       (SELECT GetActiveBalanceCost(r.t_FIID, p_EndDate, p_ClientId, p_ContrId)
                                          FROM dual
                                         WHERE r.t_A59_1 = -1 AND r.t_A58 > 0
                                       ) t_bc,
                                       (SELECT f.t_FaceValueFI FROM dfininstr_dbt f WHERE f.t_FIID = r.t_FIID) t_fv
                                  FROM dual
                              ) q1
                     ) q
             );

  --Создать итоговые строки
  SELECT    p_ClientID,
            p_ContrID,
            'X',
            q.t_PlaceID,
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
  /*ServKind*/    0,
  /*ServKindSub*/ 0,
  /*MarketID*/-1
  BULK COLLECT INTO v_brkrep
  FROM (SELECT NVL(SUM(t_A55), 0)   AS t_A55, 
               NVL(SUM(t_A56), 0)   AS t_A56, 
               NVL(SUM(t_A56_1), 0) AS t_A56_1,
               NVL(SUM(t_A57), 0)   AS t_A57, 
               NVL(SUM(t_A57_1), 0) AS t_A57_1,
               NVL(SUM(t_A58), 0)   AS t_A58, 
               NVL(SUM(t_A58_1), 0) AS t_A58_1,
               NVL(SUM(t_A61), 0)   AS t_A61, 
               NVL(SUM(t_A63), 0)   AS t_A63, 
               t_PlaceID
          FROM dbrkrepinacc_tmp 
         WHERE t_ClientID = p_ClientID 
           AND t_ContrID = p_ContrID
           AND t_IsItog = CHR(0)
        GROUP BY t_PlaceID
       ) q;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepinacc_tmp
             VALUES v_brkrep (indx);
  END IF;     


END CorrectInAccData;

--Формирование данных по бумагам внесенным в пул
PROCEDURE CreatePoolData( p_ClientID      IN NUMBER,
                          p_ContrID       IN NUMBER,
                          p_BegDate       IN DATE,
                          p_EndDate       IN DATE
                        ) 
IS

  TYPE brkrep_t IS TABLE OF DBRKREPPOOL_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;

BEGIN

  WITH q AS (SELECT s.*,
                    GetActiveRateId(s.t_FIID, p_EndDate) t_RateId,
                    NVL((SELECT mrkt.t_Market
                           FROM ddldepset_dbt dep, ddlmarket_dbt mrkt
                          WHERE dep.t_Depositary = s.t_PlaceId AND mrkt.t_DepSetId = dep.t_DepSetId AND ROWNUM = 1 ),
                        -1) AS t_MarketId
               FROM (SELECT acc.t_Code_Currency as t_FIID, 
                            -1*rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_BegDate-1, acc.t_Chapter, null) as InRest, 
                            -1*rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_EndDate, acc.t_Chapter, null) as OutRest, 
                            rsb_account.kreditac(acc.t_Account,acc.t_Chapter,acc.t_Code_Currency,p_BegDate,p_EndDate, null) as WrtOffSum, 
                            rsb_account.debetac(acc.t_Account,acc.t_Chapter,acc.t_Code_Currency,p_BegDate,p_EndDate, null) as EnrolSum,
                            (SELECT accd.t_Place
                               FROM dmcaccdoc_dbt accd 
                              WHERE accd.t_Chapter = acc.t_Chapter 
                                AND accd.t_Currency = acc.t_Code_Currency 
                                AND accd.t_Account = acc.t_Account 
                                AND accd.t_ClientContrID = p_ContrID 
                                AND ROWNUM = 1) as t_PlaceID,
                            GetAccPoolID(p_ContrID, acc.t_Account, acc.t_Code_Currency, acc.t_Chapter) as t_PoolID
                       FROM daccount_dbt acc 
                      WHERE acc.t_Chapter = 22 
                        AND acc.t_Client = p_ClientID 
                        AND EXISTS(SELECT 1
                                     FROM dmcaccdoc_dbt accd, dmccateg_dbt cat
                                    WHERE accd.t_Chapter = acc.t_Chapter
                                      AND accd.t_Currency = acc.t_Code_Currency
                                      AND accd.t_Account = acc.t_Account
                                      AND accd.t_ClientContrID = p_ContrID
                                      AND cat.t_Number = accd.t_CatNum
                                      AND cat.t_LevelType = 1
                                      AND cat.t_Code = 'ЦБ Клиента, ВУ'
                                      AND GetAccPoolID(p_ContrID, acc.t_Account, acc.t_Code_Currency, acc.t_Chapter) > 0
                                  )
                     ) s
              WHERE (s.InRest != 0 OR s.OutRest != 0 OR s.EnrolSum != 0 OR s.WrtOffSum != 0)
             )
  SELECT    p_ClientID,
            p_ContrID,
            CHR(0),
            q.t_PlaceID,
            q.t_PoolID,
            q.t_FIID,
  /*A73*/   NVL((SELECT (CASE WHEN avr.t_LSIN <> CHR(1) THEN avr.t_LSIN ELSE avr.t_ISIN END) FROM davoiriss_dbt avr WHERE avr.t_FIID = q.t_FIID), CHR(1)),
  /*A74*/   (SELECT fin.t_FI_Code FROM dfininstr_dbt fin WHERE fin.t_FIID = q.t_FIID),
  /*A75*/   NVL(SUM(q.InRest), 0),
  /*A76*/   NVL(SUM(q.EnrolSum), 0),
  /*A77*/   NVL(SUM(q.WrtOffSum), 0),
  /*A78*/   NVL(SUM(q.OutRest), 0),
  /*A79*/   RSB_SPREPFUN.GetCourse(q.t_RateId, p_EndDate), --NVL((SELECT RSB_Secur.SC_ConvSumTypeRep(1, fin.t_FIID, fin.t_FaceValueFI, fin.t_FaceValueFI, v_CourceType, p_EndDate, q.t_MarketId) FROM dfininstr_dbt fin WHERE fin.t_FIID = q.t_FIID), 0),
  /*A79_1*/ RSB_SPREPFUN.GetCourseFI(q.t_RateId),
  /*A80*/   0,
  /*A81*/   0,
  /*A82*/   RSI_RSB_FIInstr.FI_CalcNKD(q.t_FIID, p_EndDate, NVL(SUM(q.OutRest), 0), 0),
  /*A83*/   0,
  /*A84*/   0,
  /*A85*/   0,
  /*A86*/   NVL((SELECT iss.t_ShortName FROM dparty_dbt iss WHERE iss.t_PartyID = RSI_RSB_FIInstr.FI_GetIssuerOnDate(q.t_FIID, p_EndDate)), CHR(1))
  BULK COLLECT INTO v_brkrep
    FROM q
  GROUP BY q.t_PlaceID, q.t_PoolID, q.t_FIID, q.t_RateId, q.t_MarketId;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkreppool_tmp
             VALUES v_brkrep (indx);
  END IF;

  UPDATE dbrkreppool_tmp r
   SET (t_A79,
        t_A79_1,
        t_A80,
        t_A81,
        t_A82, 
        t_A83, 
        t_A84,
        t_A85 
       ) = (SELECT q.t_A79,
                   q.t_A79_1,
                   q.t_A80,
                   q.t_A81,
                   q.t_A82,
                   q.t_A83,
                   q.t_A80 + q.t_A82,
                   q.t_A81 + q.t_A83
              FROM (SELECT q1.t_A79,
                           q1.t_A79_1,
                           q1.t_A80,
                           RSI_RSB_FIInstr.ConvSum(q1.t_A80, q1.t_A79_1, RSI_RSB_FIInstr.NATCUR, p_EndDate) t_A81,
                           RSI_RSB_FIInstr.ConvSum(q1.t_A82, q1.t_fv, q1.t_A79_1, p_EndDate) t_A82,
                           RSI_RSB_FIInstr.ConvSum(q1.t_A82, q1.t_fv, RSI_RSB_FIInstr.NATCUR, p_EndDate) t_A83
                      FROM ( SELECT CASE WHEN q2.t_A79_1 != -1 THEN q2.t_A79
                                         ELSE CASE WHEN q2.t_A78 > 0 THEN (q2.t_bc-q2.t_A82)/q2.t_A78  ELSE 0 END
                                    END t_A79,
                                    CASE WHEN q2.t_A79_1 != -1 THEN q2.t_A79_1
                                         ELSE q2.t_fv
                                    END t_A79_1,
                                    CASE WHEN q2.t_A79_1 != -1 THEN ABS(q2.t_A78*q2.t_A79)
                                         ELSE q2.t_bc
                                    END t_A80,
                                    q2.t_A82,
                                    q2.t_fv
                               FROM ( SELECT r.t_A78,
                                             r.t_A79,
                                             r.t_A79_1,
                                             r.t_A82,
                                             (SELECT GetActiveBalanceCost(r.t_FIID, p_EndDate, p_ClientId, p_ContrId)
                                                FROM dual
                                               WHERE r.t_A79_1 = -1 AND r.t_A78 > 0
                                             ) t_bc,
                                             (SELECT f.t_FaceValueFI FROM dfininstr_dbt f WHERE f.t_FIID = r.t_FIID) t_fv
                                        FROM dual
                                    ) q2
                           ) q1
                   ) q
           );
/*
  UPDATE dbrkreppool_tmp r
     SET r.t_A80 = ABS(r.t_A78*r.t_A79),
         r.t_A82 = RSI_RSB_FIInstr.FI_CalcNKD(r.t_FIID, p_EndDate, r.t_A78, 0)
   WHERE r.t_ClientID = p_ClientID 
     AND r.t_ContrID = p_ContrID; 

  UPDATE dbrkreppool_tmp r
     SET r.t_A81 = RSI_RSB_FIInstr.ConvSum(r.t_A80, (SELECT f.t_FaceValueFI FROM dfininstr_dbt f WHERE f.t_FIID = r.t_FIID), RSI_RSB_FIInstr.NATCUR, p_EndDate),
         r.t_A83 = RSI_RSB_FIInstr.ConvSum(r.t_A82, (SELECT f.t_FaceValueFI FROM dfininstr_dbt f WHERE f.t_FIID = r.t_FIID), RSI_RSB_FIInstr.NATCUR, p_EndDate)
   WHERE r.t_ClientID = p_ClientID 
     AND r.t_ContrID = p_ContrID;

  UPDATE dbrkreppool_tmp r
     SET r.t_A84 = r.t_A80+r.t_A82,
         r.t_A85 = r.t_A81+r.t_A83
   WHERE r.t_ClientID = p_ClientID 
     AND r.t_ContrID = p_ContrID;
*/

  --Создать итоговые строки
  SELECT    p_ClientID,
            p_ContrID,
            'X',
            q.t_PlaceID,
            q.t_PoolID,
            -1,
  /*A73*/   CHR(1),
  /*A74*/   CHR(1),
  /*A75*/   q.t_A75,
  /*A76*/   q.t_A76,
  /*A77*/   q.t_A77,
  /*A78*/   q.t_A78,
  /*A79*/   0,
  /*A79_1*/ -1,
  /*A80*/   0,
  /*A81*/   q.t_A81,
  /*A82*/   0,
  /*A83*/   q.t_A83,
  /*A84*/   0,
  /*A85*/   q.t_A85,
  /*A86*/   CHR(1)
  BULK COLLECT INTO v_brkrep
  FROM (SELECT NVL(SUM(t_A75), 0)   AS t_A75, 
               NVL(SUM(t_A76), 0)   AS t_A76, 
               NVL(SUM(t_A77), 0)   AS t_A77, 
               NVL(SUM(t_A78), 0)   AS t_A78, 
               NVL(SUM(t_A81), 0)   AS t_A81, 
               NVL(SUM(t_A83), 0)   AS t_A83, 
               NVL(SUM(t_A85), 0)   AS t_A85,
               t_PlaceID, t_PoolID
          FROM dbrkreppool_tmp 
         WHERE t_ClientID = p_ClientID 
           AND t_ContrID = p_ContrID
           AND t_IsItog = CHR(0)
        GROUP BY t_PlaceID, t_PoolID
       ) q;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkreppool_tmp
             VALUES v_brkrep (indx);
  END IF;     


END CreatePoolData;

PROCEDURE GetErrMailTempl(p_TemplID IN NUMBER, p_Subject OUT CLOB, p_Body OUT CLOB, p_err OUT NUMBER)
IS
BEGIN
  SELECT t_Subject, t_Body INTO p_Subject, p_Body FROM dbrkrep_errmailtempl_dbt WHERE t_ID = p_TemplID;
  p_err := 0;
EXCEPTION
  WHEN OTHERS
    THEN p_err := 1;
END;


END rsb_brkrep;
/
