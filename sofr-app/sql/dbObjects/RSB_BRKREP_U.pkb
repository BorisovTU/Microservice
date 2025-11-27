CREATE OR REPLACE PACKAGE BODY rsb_brkrep_u
IS
-- Author  : Nikonorov Evgeny
-- Created : 07.06.2017
-- Purpose : Пакет для подготовки данных для отчета брокера

  g_BrokerComissFIID NUMBER := -1;
  g_PrevMarketID NUMBER := -1;
  g_PrevCodeKind NUMBER := 1;
  
--ak
  g_MarketComissNDS number := 0;
  g_CliringComissNDS number := 0;
  g_ITSComissNDS number := 0;
  g_MarketCliringITSComissFIID number := -1;
--~al  

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
    SELECT SUM(GetRQAmountOnDate (rq.t_ID, pDate)) as t_Amount
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
   --SELECT SUM(GetRQAmountOnDate(rq.t_ID, pDate)) as t_Amount
    SELECT ABS(SUM( CASE WHEN t_kind = 1 THEN -GetRQAmountOnDate(rq.t_ID, pDate)
                                     ELSE   GetRQAmountOnDate(rq.t_ID, pDate)
                         END
                ))as t_Amount 
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

--получить кол-во ТО на дату
FUNCTION GetRQAmountOnDate (pRqID IN NUMBER, pDate IN DATE)
   RETURN NUMBER
AS
   Amount   NUMBER;
BEGIN
   Amount := 0;
   --селект гарантирует только одну запись в выборке
   FOR one_rq
      IN (SELECT t1.t_amount
            FROM v_rqhist t1
           WHERE t1.t_RQID = pRqID
             AND t1.t_Instance = (SELECT MAX (t0.t_Instance)
                                    FROM v_rqhist t0
                                   WHERE t0.T_CHANGEDATE <= pDate
                                     AND t0.T_RQID = t1.T_RQID
                                 )
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
FUNCTION GetPlanExecDate(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN DATE deterministic
AS

 v_PlanExecDate DATE;
BEGIN
  SELECT NVL(MAX(DECODE(pm.t_FactDate, TO_DATE('01.01.0001','DD.MM.YYYY'), pm.t_PlanDate, pm.t_FactDate)), TO_DATE('01.01.0001','DD.MM.YYYY')) INTO v_PlanExecDate
    FROM DDLRQ_DBT pm
   WHERE pm.t_DocKind  = p_DocKind
     AND pm.t_DocID    = p_DocID
     AND pm.t_Type     IN (RSI_DLRQ.DLRQ_TYPE_DELIVERY, RSI_DLRQ.DLRQ_TYPE_PAYMENT)
     AND pm.t_DealPart = p_Part;
  
  /* Golovkin 20.06.2019 в случае отложенной поставки */
  if v_PlanExecDate = TO_DATE ('31.12.9999', 'dd.mm.yyyy') then
    SELECT NVL (MAX (DECODE (pm.t_FactDate,TO_DATE ('01.01.0001', 'DD.MM.YYYY'), pm.t_PlanDate,pm.t_FactDate)),TO_DATE ('01.01.0001', 'DD.MM.YYYY')) INTO v_PlanExecDate
      FROM V_RQHIST pm, ddlrq_dbt rq 
     WHERE     rq.t_DocKind = p_DocKind
           AND rq.t_DocID = p_DocID
           AND rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_DELIVERY, RSI_DLRQ.DLRQ_TYPE_PAYMENT)
           AND rq.t_DealPart = 1
           AND pm.t_rqid = rq.t_id
           AND pm.t_instance =
                  (SELECT MAX (h1.t_instance)
                     FROM V_RQHIST h1
                    WHERE h1.t_rqid = pm.t_rqid
                          AND H1.T_PLANDATE < TO_DATE ('31.12.9999', 'dd.mm.yyyy'));
  end if;

  RETURN v_PlanExecDate;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN TO_DATE('01.01.0001','DD.MM.YYYY');

END GetPlanExecDate;

-- Табличная функция  возвращает Все сделки за период (производная от GetPlanExecDate)
FUNCTION SelectDealExecDate(p_BegDate date,p_EndDate date) RETURN tt_DDL_TICK pipelined
AS
 v_res DDL_TICK_dbt%rowtype ;
BEGIN
  for cur in (select /*+  index(dl_tick DDL_TICK_DBT_IDX4) */
    DL_TICK.* --DL_TICK.T_DEALID
     from DDL_TICK_DBT DL_TICK
         where DL_TICK.T_DEALSTATUS > 0
           and DL_TICK.T_BOFFICEKIND in (101, 117, 127)
      and DL_TICK.T_DEALDATE between p_BegDate and p_EndDate
      union
    select /*+ ordered index(DL_TICK DDL_TICK_DBT_IDX0 )*/ DL_TICK.*
     from DDLRQ_DBT pm
       join DDL_TICK_DBT DL_TICK on DL_TICK.T_DEALID = pm.t_DocID and  pm.t_DocKind = DL_TICK.T_BOFFICEKIND
     where  pm.t_DocKind in (101, 117, 127) 
        AND pm.t_Type  IN (RSI_DLRQ.DLRQ_TYPE_DELIVERY, RSI_DLRQ.DLRQ_TYPE_PAYMENT)
        AND pm.t_DealPart in (1,2)
        and pm.t_FactDate >= p_BegDate and pm.t_FactDate != date'9999-12-31'
        and DL_TICK.T_DEALSTATUS > 0
        and DL_TICK.T_DEALDATE < p_BegDate
      union
    select /*+ ordered index(DL_TICK DDL_TICK_DBT_IDX0 )*/ DL_TICK.*
     from DDLRQ_DBT pm
       join DDL_TICK_DBT DL_TICK on DL_TICK.T_DEALID = pm.t_DocID and  pm.t_DocKind = DL_TICK.T_BOFFICEKIND
     where  pm.t_DocKind in (101, 117, 127) 
        AND pm.t_Type  IN (RSI_DLRQ.DLRQ_TYPE_DELIVERY, RSI_DLRQ.DLRQ_TYPE_PAYMENT)
        AND pm.t_DealPart in (1,2)
        and pm.t_FactDate = date'0001-01-01' and pm.t_PlanDate >= p_BegDate and pm.t_PlanDate != date'9999-12-31'
        and DL_TICK.T_DEALSTATUS > 0
        and DL_TICK.T_DEALDATE < p_BegDate
      union
    select /*+ ordered  index(DL_TICK DDL_TICK_DBT_IDX0 ) */ DL_TICK.*
     from DDLRQ_DBT rq
       join DDL_TICK_DBT DL_TICK on DL_TICK.T_DEALID = rq.t_DocID and  rq.t_DocKind = DL_TICK.T_BOFFICEKIND
       join V_RQHIST pm on  pm.t_rqid = rq.t_id
     where  rq.t_DocKind in (101, 117, 127) 
        AND rq.t_Type  IN (RSI_DLRQ.DLRQ_TYPE_DELIVERY, RSI_DLRQ.DLRQ_TYPE_PAYMENT)
        AND rq.t_DealPart = 1
        and ((rq.t_FactDate = date'0001-01-01' and rq.t_PlanDate = date'9999-12-31')
             or rq.t_FactDate = date'9999-12-31' )
        and DL_TICK.T_DEALSTATUS > 0
        and DL_TICK.T_DEALDATE < p_BegDate
        AND decode(pm.t_instance,
                  (SELECT MAX (h1.t_instance)
                     FROM V_RQHIST h1
                    WHERE h1.t_rqid = pm.t_rqid
                          AND H1.T_PLANDATE < date'9999-12-31'),1,0) = 1
        and DECODE (pm.t_FactDate,date'0001-01-01', pm.t_PlanDate,pm.t_FactDate) >= p_BegDate
       )
  loop
     v_res:= cur;
     pipe row(v_res) ;
  end loop;
END SelectDealExecDate;



--Получить фактическую дату исполнения части сделки (максимальная из фактических по ТО)
FUNCTION GetExecDate(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN DATE deterministic
AS
 pragma udf ;
 v_ExecDate DATE;
BEGIN
  SELECT NVL(RSI_RSBCALENDAR.GETDATEAFTERWORKDAY(MAX(pm.t_FactDate),0), TO_DATE('01.01.0001','DD.MM.YYYY')) INTO v_ExecDate
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

--Получить фактическую дату исполнения части сделки (максимальная из фактических по ТО) без использования календарей
FUNCTION GetExecDateWOCalendar(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN DATE deterministic
AS
 pragma udf ;
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

END GetExecDateWOCalendar;

--Получить фактическую дату исполнения оплаты
FUNCTION GetExecDatePaym(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN DATE deterministic
AS
 pragma udf ;
 v_ExecDate DATE;
BEGIN
  SELECT NVL(RSI_RSBCALENDAR.GETDATEAFTERWORKDAY(MAX(pm.t_FactDate),0), TO_DATE('01.01.0001','DD.MM.YYYY')) INTO v_ExecDate
    FROM DDLRQ_DBT pm
   WHERE pm.t_DocKind  = p_DocKind
     AND pm.t_DocID    = p_DocID
     AND pm.t_Type     IN ( RSI_DLRQ.DLRQ_TYPE_PAYMENT)
     AND pm.t_DealPart = p_Part
     AND pm.t_FactDate > TO_DATE('01.01.0001','DD.MM.YYYY')
     AND NOT Exists(SELECT 1
                      FROM ddlrq_dbt pm1
                     WHERE pm1.t_DocKind = pm.t_DocKind
                       AND pm1.t_DocID = pm.t_DocID
                       AND pm1.t_DealPart = pm.t_DealPart
                       AND pm1.t_Type IN ( RSI_DLRQ.DLRQ_TYPE_PAYMENT)
                       AND pm1.t_FactDate = TO_DATE('01.01.0001','DD.MM.YYYY')
                   );

  RETURN v_ExecDate;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN TO_DATE('01.01.0001','DD.MM.YYYY');

END GetExecDatePaym;
--Получить цену сделки
FUNCTION GetPrice(p_DealID IN NUMBER, p_Part IN NUMBER) RETURN FLOAT
AS

 v_Price FLOAT;
BEGIN

-- ak - возвращаем как есть
--  SELECT (CASE WHEN leg.t_RelativePrice = 'X' THEN RSI_RSB_FIInstr.ConvSum( (leg.t_Price * RSI_RSB_FIInstr.FI_GetNominalOnDate(leg.t_PFI, tk.t_DealDate) / 100.0), fin.t_FaceValueFI, leg.t_CFI, tk.t_DealDate)
--               ELSE leg.t_Price END
--         )
  SELECT leg.t_Price
--~ak
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
/*
  SELECT (CASE WHEN req.t_PriceType = 2 THEN RSI_RSB_FIInstr.ConvSum( (req.t_Price * RSI_RSB_FIInstr.FI_GetNominalOnDate(req.t_FIID, req.t_Date) / 100.0), fin.t_FaceValueFI, req.t_PriceFIID, req.t_Date)
               ELSE req.t_Price END
         )
*/    
  SELECT req.t_Price  
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
     AND req.T_CLIENT = tk.T_CLIENTID --PNV 535689
     AND fin.t_FIID = req.t_FIID; 

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
FUNCTION GetBrokerComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER deterministic
AS
 pragma udf;
 v_Sum  NUMBER;
 v_FIID NUMBER;
BEGIN

  g_BrokerComissFIID := -1;

  IF p_Part = 1 THEN
    SELECT q1.CommSum, q1.FIID_COMM INTO v_Sum, v_FIID
      FROM (SELECT /*+ leading(q)*/ NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
              FROM (SELECT sum(dlc.t_Sum) AS CommSum, dlc.t_FeeType, dlc.t_ComNumber
                      FROM ddlcomis_dbt dlc
                     WHERE dlc.t_DocKind = p_DocKind
                       AND dlc.t_DocID = p_DocID
                       AND dlc.t_IsBack = CHR(0)
                     group by dlc.t_FeeType, dlc.t_ComNumber
                    UNION
                    SELECT  sum(basobj.t_CommSum) AS CommSum, defcom.t_FeeType, defcom.t_CommNumber
                      FROM dsfbasobj_dbt basobj, dsfdefcom_dbt defcom
                     WHERE basobj.t_BaseObjectType = p_DocKind
                       AND basobj.t_BaseObjectID = p_DocID
                       AND defcom.t_ID = basobj.t_DefCommID
                     group by defcom.t_FeeType, defcom.t_CommNumber
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
FUNCTION GetMarketComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER, p_MarketID IN NUMBER) RETURN NUMBER deterministic
AS
 pragma udf;
 v_Sum  NUMBER;
--ak
 v_nds number;
 v_fiid number;
--~ak 
BEGIN
  
--ak
  g_MarketComissNDS := 0;
  g_MarketCliringITSComissFIID := -1;
--~ak  

  IF p_Part = 1 THEN
    SELECT q1.CommSum 
--ak
           , q1.nds, q1.fiid_comm
--~ak    
    INTO v_Sum
--ak
         , v_nds, v_fiid
--~ak
      FROM (SELECT NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
--ak
                   , nvl(sum(q.nds), 0) as nds
--~ak      
              FROM (SELECT dlc.t_Sum AS CommSum, dlc.t_FeeType, dlc.t_ComNumber
--ak
                           , dlc.t_nds as nds
--~ak              
                      FROM ddlcomis_dbt dlc
                     WHERE dlc.t_DocKind = p_DocKind
                       AND dlc.t_DocID = p_DocID
                       AND dlc.t_IsBack = CHR(0)
                    UNION
                    SELECT basobj.t_CommSum AS CommSum, defcom.t_FeeType, defcom.t_CommNumber
--ak
                           , basobj.t_ndssum as nds
--~ak
                      FROM dsfbasobj_dbt basobj, dsfdefcom_dbt defcom
                     WHERE basobj.t_BaseObjectType = p_DocKind
                       AND basobj.t_BaseObjectID = p_DocID
                       AND defcom.t_ID = basobj.t_DefCommID
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_FeeType = q.t_Feetype
               AND cm.t_Number  = q.t_ComNumber
               AND cm.t_ReceiverID = p_MarketID
               AND cm.t_Code NOT IN ('МскБиржКлирНов','МскБиржИТСНов')
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  ELSE

    SELECT q1.CommSum 
--ak
           , q1.nds, q1.fiid_comm
--~ak    
    INTO v_Sum
--ak
         , v_nds, v_fiid
--~ak    
      FROM (SELECT NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
--ak
                   , nvl(sum(q.nds), 0) as nds
--~ak      
              FROM (SELECT dlc.t_Sum AS CommSum, dlc.t_FeeType, dlc.t_ComNumber
--ak
                           , dlc.t_nds as nds
--~ak
                      FROM ddlcomis_dbt dlc
                     WHERE dlc.t_DocKind = p_DocKind
                       AND dlc.t_DocID = p_DocID
                       AND dlc.t_IsBack = 'X'
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_FeeType = q.t_Feetype
               AND cm.t_Number  = q.t_ComNumber
               AND cm.t_ReceiverID = p_MarketID
               AND cm.t_Code NOT IN ('МскБиржКлирНов','МскБиржИТСНов')
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  END IF;

--ak
  g_MarketComissNDS := v_nds;
  if(v_fiid >= 0)then
    g_MarketCliringITSComissFIID := v_fiid;
  end if;
--~ak  

  RETURN v_Sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;

END GetMarketComissSum;

--ak
--Получить НДС комиссии торговой площадке (обязательно после вызова GetMarketComissSum)
FUNCTION GetMarketComissNDS(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER
AS
BEGIN
  RETURN g_MarketComissNDS;
END GetMarketComissNDS;
--~ak

--Получить сумму комиссий клирингово центра
FUNCTION GetCliringComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER deterministic
AS
 pragma udf;
 v_Sum  NUMBER;
-- ak
 v_nds number;
 v_fiid number;
--~ak 
BEGIN

--ak
  g_CliringComissNDS := 0;
--~ak  

  IF p_Part = 1 THEN
    SELECT q1.CommSum 
--ak
           , q1.nds, q1.fiid_comm    
--~ak
    INTO v_Sum
--ak
         , v_nds, v_fiid
--~ak
      FROM (SELECT NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
--ak
                   , nvl(sum(q.nds), 0) as nds
--~ak      
              FROM (SELECT dlc.t_Sum AS CommSum, dlc.t_FeeType, dlc.t_ComNumber
--ak
                           , dlc.t_nds as nds
--~ak
                      FROM ddlcomis_dbt dlc
                     WHERE dlc.t_DocKind = p_DocKind
                       AND dlc.t_DocID = p_DocID
                       AND dlc.t_IsBack = CHR(0)
                    UNION
                    SELECT basobj.t_CommSum AS CommSum, defcom.t_FeeType, defcom.t_CommNumber
--ak
                           , basobj.t_ndssum as nds
--~ak
                      FROM dsfbasobj_dbt basobj, dsfdefcom_dbt defcom
                     WHERE basobj.t_BaseObjectType = p_DocKind
                       AND basobj.t_BaseObjectID = p_DocID
                       AND defcom.t_ID = basobj.t_DefCommID
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_FeeType = q.t_Feetype
               AND cm.t_Number  = q.t_ComNumber
               AND cm.t_Code = 'МскБиржКлирНов'
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  ELSE

    SELECT q1.CommSum 
--ak
           , q1.nds, q1.fiid_comm
--~ak
    INTO v_Sum
--ak
         , v_nds, v_fiid
--~ak    
      FROM (SELECT NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
--ak
                   , nvl(sum(q.nds), 0) as nds
--~ak      
              FROM (SELECT dlc.t_Sum AS CommSum, dlc.t_FeeType, dlc.t_ComNumber
--ak
                           , dlc.t_nds as nds
--~ak
                      FROM ddlcomis_dbt dlc
                     WHERE dlc.t_DocKind = p_DocKind
                       AND dlc.t_DocID = p_DocID
                       AND dlc.t_IsBack = 'X'
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_FeeType = q.t_Feetype
               AND cm.t_Number  = q.t_ComNumber
               AND cm.t_Code = 'МскБиржКлирНов'
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  END IF;

--ak
  g_CliringComissNDS := v_nds;
  if(v_fiid >= 0)then
    g_MarketCliringITSComissFIID := v_fiid;
  end if;
--~ak  

  RETURN v_Sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;

END GetCliringComissSum;

--ak
--Получить НДС комиссии клирингово центра (обязательно после вызова GetCliringComissSum)
FUNCTION GetCliringComissNDS(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER
AS
BEGIN
  RETURN g_CliringComissNDS;
END GetCliringComissNDS;
--~ak

--Получить сумму комиссий за ИТС
FUNCTION GetITSComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER deterministic
AS
 pragma udf;
 v_Sum  NUMBER;
--ak
 v_nds number;
 v_fiid number;
--~ak
BEGIN

--ak
  g_ITSComissNDS := 0;
--~ak  

  IF p_Part = 1 THEN
    SELECT q1.CommSum 
--ak
           , q1.nds, q1.fiid_comm
--~ak    
    INTO v_Sum
--ak
         , v_nds, v_fiid
--~ak
      FROM (SELECT NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
--ak
                   , nvl(sum(q.nds), 0) as nds
--~ak
              FROM (SELECT dlc.t_Sum /*уже включает в себя НДС*/ AS CommSum, dlc.t_FeeType, dlc.t_ComNumber
--ak
                           , dlc.t_nds as nds
--~ak
                      FROM ddlcomis_dbt dlc
                     WHERE dlc.t_DocKind = p_DocKind
                       AND dlc.t_DocID = p_DocID
                       AND dlc.t_IsBack = CHR(0)
                    UNION
                    SELECT basobj.t_CommSum /*уже включает в себя НДС*/ AS CommSum, defcom.t_FeeType, defcom.t_CommNumber
--ak
                           , basobj.t_ndssum as nds
--~ak
                      FROM dsfbasobj_dbt basobj, dsfdefcom_dbt defcom
                     WHERE basobj.t_BaseObjectType = p_DocKind
                       AND basobj.t_BaseObjectID = p_DocID
                       AND defcom.t_ID = basobj.t_DefCommID
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_FeeType = q.t_Feetype
               AND cm.t_Number  = q.t_ComNumber
               AND cm.t_Code = 'МскБиржИТСНов'
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  ELSE

    SELECT q1.CommSum 
--ak
           , q1.nds, q1.fiid_comm
--~ak    
    INTO v_Sum
--ak
         , v_nds, v_fiid
--~ak    
      FROM (SELECT NVL(SUM(q.CommSum),0) AS CommSum, NVL(cm.t_FIID_COMM, -1) as FIID_COMM
--ak
                   , nvl(sum(q.nds), 0) as nds
--~ak      
              FROM (SELECT dlc.t_Sum /*уже включает в себя НДС*/ AS CommSum, dlc.t_FeeType, dlc.t_ComNumber
--ak
                           , dlc.t_nds as nds
--~ak
                      FROM ddlcomis_dbt dlc
                     WHERE dlc.t_DocKind = p_DocKind
                       AND dlc.t_DocID = p_DocID
                       AND dlc.t_IsBack = 'X'
                   ) q, dsfcomiss_dbt cm
             WHERE cm.t_FeeType = q.t_Feetype
               AND cm.t_Number  = q.t_ComNumber
               AND cm.t_Code = 'МскБиржИТСНов'
            GROUP BY cm.t_FIID_COMM) q1
     WHERE ROWNUM = 1;

  END IF;

--ak
  g_ITSComissNDS := v_nds;
  if(v_fiid >= 0)then
    g_MarketCliringITSComissFIID := v_fiid;
  end if;
--~ak  

  RETURN v_Sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;

END GetITSComissSum;

--ak
--Получить НДС комиссии за ИТС(обязательно после вызова GetITSComissSum)
FUNCTION GetITSComissNDS(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER
AS
BEGIN
  RETURN g_ITSComissNDS;
END GetITSComissNDS;

--Получить валюту комиссий (обязательно после вызова функций расчета комиссий)
FUNCTION GetMarketCliringITSComissFIID(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER
AS
BEGIN
  RETURN g_MarketCliringITSComissFIID;
END GetMarketCliringITSComissFIID;
--~ak

--Установить запись о том, что данный договор обслуживания обрабатывался (для последующей печати по нему, даже если не было сделок)
PROCEDURE SetUsingContr( p_ClientID IN NUMBER,
                         p_ContrID  IN NUMBER,
                         p_BegDate  IN DATE,
                         p_EndDate  IN DATE
                       )
IS

  v_brkrep DBRKREPDEAL_u_TMP%ROWTYPE;

BEGIN

  for c in(select t_id t_sfcontrid
           from dsfcontr_dbt sfcontr 
           where sfcontr.t_ServKind = rsi_npto.PTSK_STOCKDL/*1 фондовый дилинг*/ and
                 sfcontr.t_partyid = p_ClientID and
                 (sfcontr.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr.t_DateClose >= p_BegDate) and
                 (p_ContrID = 0 or sfcontr.t_id = p_ContrID))
  loop
    v_brkrep.t_ClientID  := p_ClientID;
    v_brkrep.t_ContrID   := c.t_sfcontrid;
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
    v_brkrep.t_A30       := -1;
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

    INSERT INTO dbrkrepdeal_u_tmp VALUES v_brkrep;
    
  end loop;

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

  TYPE brkrep_t IS TABLE OF DBRKREPDEAL_u_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;
BEGIN

  SELECT    p_ClientID,
            q.t_sfcontrid,
--ak
--            p_Part,
--            when q.IsRepo = 1 then 4
--                 when q.FactExecDate between p_BegDate and p_EndDate then 3
--                 when q.t_DealDate < p_BegDate then 2
--                 when q.t_DealDate >= p_BegDate and q.t_DealDate <= p_EndDate then 1
--            else 0 end,
            case
                 when q.t_state = 7 then 8/*CHVA отмененные сделки 523492*/ 
                 when q.t_DealDate >= p_BegDate and q.t_DealDate <= p_EndDate then 1
                 when q.t_DealDate < p_BegDate and q.FactExecDate between p_BegDate and p_EndDate then 3
                 when q.t_DealDate < p_BegDate then 2
            else 0 end,
--~ak
            case when q.IsRepo = 1 then -1 else GetSfPlanID(q.t_sfcontrid, q.t_DealDate) end,
            CHR(0),
            (CASE WHEN q.IsRepo = 1 AND q.IsBuy = 1 THEN DECODE(q.t_DealPart, 1, 1, 2)
                  WHEN q.IsRepo = 1 AND q.IsSale = 1 THEN DECODE(q.t_DealPart, 1, 2, 1)
                  ELSE DECODE(q.IsBuy, 1, 1, 2) END
            ),
  /*A05_D*/ q.t_DealDate,
  /*A05_T*/ q.t_DealTime,
  /*A06*/   GetPlanExecDate(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*A07*/   decode(q.t_DealCodeTS, chr(1), q.t_DealCode, q.t_DealCodeTS),
  /*A08*/   DECODE(q.t_Flag1, 'X', q.t_DealCodeTS, CHR(1)),
  /*A09*/   RSI_RSBPARTY.GetPartyCode(q.t_MarketID, 1 /*PTCK_CONTR*/),
/*ak
  --A10--   (CASE WHEN q.IsRepo = 1 THEN DECODE(q.IsBuy, 1, 'ОРЕПО', 'ПРЕПО') || ' ' || TO_CHAR(q.t_DealPart)
                  ELSE DECODE(q.IsBuy, 1, 'Покупка', 'Продажа') END
            ),*/
  /*A10*/   (CASE WHEN q.IsRepo = 1 and q.t_DealPart = 1 THEN DECODE(q.IsBuy, 1, 'Покупка РЕПО ч.', 'Продажа РЕПО ч.') || ' ' || TO_CHAR(q.t_DealPart)
                  WHEN q.IsRepo = 1 and q.t_DealPart = 2 THEN DECODE(q.IsSale, 1, 'Покупка РЕПО ч.', 'Продажа РЕПО ч.') || ' ' || TO_CHAR(q.t_DealPart)
                  ELSE DECODE(q.IsBuy, 1, 'Покупка', 'Продажа') END
            ),
/*~ak*/            
  /*A11*/   NVL((SELECT pt.t_ShortName
                   FROM dparty_dbt pt
                  WHERE pt.t_PartyID = q.t_Issuer), CHR(1))||
            nvl((select ' '||avrkinds.t_name
                 from davrkinds_dbt avrkinds
                 where avrkinds.t_fi_kind = q.t_fi_kind and
                       avrkinds.t_avoirkind = q.t_avoirkind), chr(1)),
  --A12--   (CASE WHEN q.t_LSIN <> CHR(1) THEN q.t_LSIN ELSE q.t_ISIN END),
  /*A12*/   (CASE WHEN q.t_LSIN = CHR(1) THEN q.t_ISIN 
                  WHEN q.t_ISIN = CHR(1) THEN q.t_LSIN
                  WHEN q.t_LSIN = q.t_ISIN THEN q.t_LSIN
                  ELSE q.t_LSIN||'/'||q.t_ISIN END),
/*ak
  --A13--   (CASE WHEN q.IsRepo = 1 AND q.t_DealPart = 2 THEN 0
                  ELSE GetPrice(q.t_DealID, q.t_DealPart) END
            ),
  --A13_i-- (CASE WHEN q.IsRepo = 1 AND q.t_DealPart = 2 THEN q.t_IncomeRate
                  ELSE 0 END
            ),*/
  /*A13*/   GetPrice(q.t_DealID, q.t_DealPart),
  /*A13_i*/ (CASE WHEN q.IsRepo = 1 THEN q.t_IncomeRate
                  ELSE 0 END
            ),
/*~ak*/
  /*A14*/   NVL((SELECT f.t_CCY FROM dfininstr_dbt f WHERE f.t_FIID = q.t_CFI), CHR(1)),
  /*A14_C*/ q.t_CFI,
  /*A15*/   q.t_Amount,
  --A16   q.t_TotalCost,
  --A17   RSI_RSB_FIInstr.ConvSum(q.t_TotalCost, q.t_CFI, RSI_RSB_FIInstr.NATCUR, CASE WHEN q.t_State = RSI_DLRQ.DLRQ_STATE_EXEC THEN GetPlanExecDate(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) ELSE p_EndDate END),
  --/*A16*/   q.t_TotalCost - q.t_NKD,
  /*A16*/   q.t_TotalCost - RSI_RSB_FIInstr.ConvSum(q.t_NKD,q.t_nkdfiid,q.t_CFI,CASE WHEN q.t_State = RSI_DLRQ.DLRQ_STATE_EXEC THEN GetPlanExecDate(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) ELSE p_EndDate END,1), -- Golovkin 26.04.2019 пересчет нкд в валюту сделки
  /*A17*/   RSI_RSB_FIInstr.ConvSum(q.t_TotalCost, q.t_CFI, RSI_RSB_FIInstr.NATCUR, CASE WHEN GetExecDatePaym(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) <> TO_DATE('01.01.0001','DD.MM.YYYY') THEN GetExecDatePaym(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) ELSE q.t_dealdate END, 1) -
                 RSI_RSB_FIInstr.ConvSum(q.t_NKD, q.t_nkdfiid, RSI_RSB_FIInstr.NATCUR, CASE WHEN GetExecDatePaym(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) <> TO_DATE('01.01.0001','DD.MM.YYYY')  THEN GetExecDatePaym(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) ELSE q.t_dealdate END, 1), -- Golovkin 21.06.2019 пересчет нкд из валюты нкд в рубли
  /*A18*/   GetBrokerComissSum(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*A19*/   NVL((SELECT f.t_CCY FROM dfininstr_dbt f WHERE f.t_FIID = GetBrokerComissFIID(q.t_BOfficeKind, q.t_DealID, q.t_DealPart)), CHR(1)),
  /*A19_C*/ GetBrokerComissFIID(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*A20*/   GetMarketComissSum(q.t_BOfficeKind, q.t_DealID, q.t_DealPart, q.t_MarketID),
  /*A21*/   GetCliringComissSum(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*A22*/   GetITSComissSum(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  --A23--   DECODE(q.t_Flag1, 'X', 'биржевая', 'внебиржевая'),
            decode((select trim(upper(nvl(max(t_codelist),chr(1)))) 
                                  from dobjattr_dbt objattr, dobjatcor_dbt objatcor 
                                  where objattr.t_objecttype = 101 and 
                                        objattr.t_groupid = 105 and /*(Без-)адресная сделка*/ 
                                        objatcor.t_objecttype = objattr.t_objecttype and 
                                        objatcor.t_groupid = objattr.t_groupid and 
                                        objatcor.t_attrid = objattr.t_attrid and 
                                        objatcor.t_object = lpad(q.t_DealID, 34, '0') and 
                                        objatcor.t_validfromdate <= q.t_dealdate and 
                                        objatcor.t_validtodate > q.t_dealdate), 'NLP', 'Безадресная',
                                                                                'AC', 'Адресная',
                                                                                DECODE(q.t_Flag1, 'X', 'Биржевая', 'Адресная')),
  /*A24*/   NVL((SELECT pt.t_ShortName
                   FROM dparty_dbt pt
                  WHERE pt.t_PartyID = q.t_PartyID), CHR(1)),
  /*A25*/   RSI_RSBPARTY.GetPartyCode(q.t_PartyID, DECODE(q.t_Flag1, CHR(0), 6 /*PTCK_SWIFT*/, GetPtCodeKindForMarket(q.t_MarketID))),
  /*A26*/ RSI_RSB_FIInstr.ConvSum(
               case Rsb_Secur.IsBasket( Rsb_Secur.get_OperationGroup( Rsb_Secur.get_OperSysTypes( q.t_DealType, q.t_BOfficeKind ) ) )  WHEN 1 THEN 0
               else q.t_NKD end,
                q.t_nkdfiid,q.t_cfi,CASE WHEN q.t_State = RSI_DLRQ.DLRQ_STATE_EXEC THEN GetPlanExecDate(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) ELSE p_EndDate END,1), -- Golovkin 26.04.2019 пересчет нкд в валюту сделки
  /*A27*/   RSI_RSB_FIInstr.ConvSum(q.t_NKD, q.t_FaceValueFI, RSI_RSB_FIInstr.NATCUR, CASE WHEN q.t_State = RSI_DLRQ.DLRQ_STATE_EXEC THEN GetPlanExecDate(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) ELSE p_EndDate END, 1),
/*ak
  --A28--  case q.t_LegKind when 2 then ( case IsSale when 1 then q.t_TotalCost else 0 end) else (case IsBuy when 1 then q.t_TotalCost else 0 end) end,
  --A29--  case q.t_LegKind when 2 then ( case IsBuy when 1 then q.t_Amount else 0 end) else (case IsSale when 1 then q.t_Amount else 0 end) end,*/
  /*A28*/  q.t_TotalCost,
  /*A29*/  q.t_Amount,
/*~ak*/
  /*A30*/   -1,
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
  /*A95_M*/ q.IsMarginCall
--ak
  /*t_marketcomissnds*/ , GetMarketComissNDS(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*t_cliringcomissnds*/ GetCliringComissNDS(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*t_tscomissnds*/ GetITSComissNDS(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*t_mcicomissfiid*/ GetMarketCliringITSComissFIID(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*t_mcicomissfiname*/ NVL((SELECT f.t_CCY FROM dfininstr_dbt f WHERE f.t_FIID = GetMarketCliringITSComissFIID(q.t_BOfficeKind, q.t_DealID, q.t_DealPart)), CHR(1))
--~ak  
  BULK COLLECT INTO v_brkrep
  FROM (
             --сделки без корзины
             SELECT /*+ ordered*/ sfcontr.t_id t_sfcontrid,
                    tk.t_DealID, tk.t_BOfficeKind, tk.t_DealDate, tk.t_DealTime, tk.t_ClientID, tk.t_ClientContrID,
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
					case when RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(tk.t_DealID, 34, '0'), 116, tk.t_DealDate) = 1 then 'X' else CHR(0) end as IsMarginCall,
                    GetExecDateWOCalendar(tk.t_BOfficeKind, tk.t_DealID, rq.t_DealPart) as FactExecDate,
                    fin.t_FIID,
                    fin.t_fi_kind,
                    fin.t_avoirkind,
                    leg.t_nkdfiid -- Golovkin 26.04.2019 валюта НКД                    
               FROM dsfcontr_dbt sfcontr, ddl_tick_dbt tk, ddlrq_dbt rq, ddl_leg_dbt leg, dfininstr_dbt fin, davoiriss_dbt avr
              WHERE sfcontr.t_ServKind = rsi_npto.PTSK_STOCKDL/*1 фондовый дилинг*/
                and sfcontr.t_partyid = p_ClientID
                and (sfcontr.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr.t_DateClose >= p_BegDate)
                and (p_ContrID = 0 or sfcontr.t_id = p_ContrID)
                and tk.t_BOfficeKind   = 101 /*DL_SECURITYDOC*/
                AND tk.t_ClientID      = p_ClientID
                AND tk.t_ClientContrID = sfcontr.t_id
                AND rq.t_DocKind       = tk.t_BOfficeKind
                AND rq.t_DocID         = tk.t_DealID
                AND rq.t_Type          = RSI_DLRQ.DLRQ_TYPE_DELIVERY
--            --   AND RSI_DLRQ.RSI_GetRQStateOnDate(RQ.T_ID, p_BegDate) != RSI_DLRQ.DLRQ_STATE_REJECT -- Golovkin 28.11.2019 ID : 500392 /*chva 523492 пока закомментил , сделки должны отбираться для раздела отмененных сделок*/
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
             SELECT /*+ ordered*/ sfcontr.t_id t_sfcontrid,
                    tk.t_DealID, tk.t_BOfficeKind, tk.t_DealDate, tk.t_DealTime, tk.t_ClientID, tk.t_ClientContrID,
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
					case when RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(tk.t_DealID, 34, '0'), 116, tk.t_DealDate) = 1 then 'X' else CHR(0) end as IsMarginCall,
                    GetExecDateWOCalendar(tk.t_BOfficeKind, tk.t_DealID, DECODE(leg.t_LegKind, 0 /*LEG_KIND_DL_TICK*/, 1, 2)) as FactExecDate,
                    fin.t_FIID,
                    fin.t_fi_kind,
                    fin.t_avoirkind,
                    leg.t_nkdfiid -- Golovkin 26.04.2019 валюта НКД
               FROM dsfcontr_dbt sfcontr, ddl_tick_dbt tk, ddl_leg_dbt leg, dfininstr_dbt fin, davoiriss_dbt avr
              WHERE sfcontr.t_ServKind = rsi_npto.PTSK_STOCKDL/*1 фондовый дилинг*/
                and sfcontr.t_partyid = p_ClientID
                and (sfcontr.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr.t_DateClose >= p_BegDate)
                and (p_ContrID = 0 or sfcontr.t_id = p_ContrID)
                and tk.t_BOfficeKind   = 101 /*DL_SECURITYDOC*/
                AND tk.t_ClientID      = p_ClientID
                AND tk.t_ClientContrID = sfcontr.t_id
                AND leg.t_DealID       = tk.t_DealID
                AND fin.t_FIID         = tk.t_PFI
                AND avr.t_FIID         = fin.t_FIID
                AND tk.t_Flag1 = (CASE WHEN p_ByExchange <> 0 AND p_ByOutExchange = 0 THEN 'X'
                                       WHEN p_ByExchange = 0 AND p_ByOutExchange <> 0 THEN CHR(0)
                                       ELSE tk.t_Flag1 END )
                AND Rsb_Secur.IsBasket( Rsb_Secur.get_OperationGroup( Rsb_Secur.get_OperSysTypes( tk.t_DealType, tk.t_BOfficeKind ) ) ) = 1
             ) q
-- ak
-- 1 - обычные сделки, заключенные в период и не исполненные
-- 2 - обычные сделки, заключенные ранее и не исполненные
-- 3 - исполненные сделки
-- 4 - сделки репо
--v2
-- 1 - сделки, заключенные в период
-- 2 - сделки, заключенные ранее и не исполненные
-- 3 - сделки, заключенные ранее и исполненные
--  WHERE 1 = (CASE WHEN p_Part = 1 AND q.t_DealDate >= p_BegDate AND q.t_DealDate <= p_EndDate THEN 1
--                  WHEN p_Part = 2 AND q.t_DealDate < p_BegDate AND (q.FactExecDate > p_EndDate OR q.FactExecDate = TO_DATE('01.01.0001','DD.MM.YYYY')) THEN 1
--                  WHEN p_Part = 3 AND q.IsRepo = 0 AND q.FactExecDate BETWEEN p_BegDate AND p_EndDate THEN 1
--                  WHEN p_Part = 4 AND q.IsRepo = 1 AND q.FactExecDate BETWEEN p_BegDate AND p_EndDate THEN 1
--                  ELSE 0 END );
--  WHERE (q.IsRepo = 0 and (q.t_DealDate <= p_EndDate and (q.FactExecDate >= p_BegDate OR q.FactExecDate = TO_DATE('01.01.0001','DD.MM.YYYY')))) or
--        (q.IsRepo = 1 and (q.t_DealDate <= p_EndDate and (q.FactExecDate >= p_BegDate OR q.FactExecDate = TO_DATE('01.01.0001','DD.MM.YYYY'))));
  WHERE q.t_DealDate <= p_EndDate and (q.FactExecDate >= p_BegDate OR q.FactExecDate = TO_DATE('01.01.0001','DD.MM.YYYY'));
        -- Не исполненные с планируемой датой меньше даты отчета уберем
        --and (q.FactExecDate != TO_DATE('01.01.0001','DD.MM.YYYY') or GetPlanExecDate(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) >= p_BegDate);
--~ak

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepdeal_u_tmp
             VALUES v_brkrep (indx);
  END IF;

  --Создать итоговые строки
  FOR one_curr IN (SELECT cur.t_FIID, cur.t_CCY
                     FROM dfininstr_dbt cur
                    WHERE cur.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY
                      AND EXISTS(SELECT 1
                                   FROM dbrkrepdeal_u_tmp rep
                                  WHERE rep.t_ClientID = p_ClientID
                                    --AND rep.t_ContrID  = p_ContrID
                                    --AND rep.t_Part     = p_Part
                                    AND rep.t_IsItog   = CHR(0)
                                    AND (   rep.t_A14 = cur.t_CCY
                                         OR rep.t_A19 = cur.t_CCY
                                         or rep.t_mcicomissfiname = cur.t_CCY
                                         OR cur.t_FIID = RSI_RSB_FIInstr.NATCUR)
                                    AND 
                                        (rep.t_A15 <> 0 OR rep.t_A20 <> 0 OR 
                                         rep.t_A21 <> 0 OR rep.t_A22 <> 0 OR 
                                         rep.t_A27 <> 0 OR rep.t_A28 <> 0 OR 
                                         rep.t_A29 <> 0 or rep.t_A26 <> 0 or
                                         rep.t_marketcomissnds != 0 or
                                         rep.t_cliringcomissnds != 0 or 
                                         rep.t_tscomissnds != 0
                                        )
                                )
                  )
  LOOP
      SELECT    p_ClientID,
                q.t_sfcontrid,
                q.t_Part,
                q.t_PlanID,
                'X',
                q.t_Direction,
      /*A05_D*/ TO_DATE('01.01.0001','DD.MM.YYYY'),
      /*A05_T*/ TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS'),
      /*A06*/   TO_DATE('01.01.0001','DD.MM.YYYY'),
      /*A07*/   CHR(1),
      /*A08*/   CHR(1),
      /*A09*/   CHR(1),
      /*A10*/   q.t_A10,
      /*A11*/   CHR(1),
      /*A12*/   CHR(1),
      /*A13*/   0,
      /*A13_i*/ 0,
      /*A14*/   CHR(1),
      /*A14_C*/ -1,
--ak
--      /*A15*/   0,
      /*A15*/   q.t_A15,
--~ak
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
      /*A26*/   q.t_A26,
      /*A27*/   q.t_A27,
--ak
--      /*A28*/   0,
--      /*A29*/   0,
      /*A28*/   q.t_A28,
      /*A29*/   q.t_A29,
--~ak
      /*A30*/   one_curr.t_fiid,
      /*A31*/   TO_DATE('01.01.0001','DD.MM.YYYY'),
      /*A32_1*/ 0,
      /*A32_2*/ -1,
      /*A33_1*/ 0,
      /*A33_2*/ -1,
      /*A33_3*/ 0,
      /*A34*/   0,
      /*A35*/   0,
      --FIID--  -1,
      /*FIID*/  t_fiid,
      /*A95*/   0,
      /*A95_M*/ CHR(0),
--ak
  /*t_marketcomissnds*/ q.t_marketcomissnds,
  /*t_cliringcomissnds*/ q.t_cliringcomissnds,
  /*t_tscomissnds*/ q.t_tscomissnds,
  /*t_mcicomissfiid*/ -1,
  /*t_mcicomissfiname*/ chr(1)
--~ak      
      BULK COLLECT INTO v_brkrep
      FROM (SELECT 
                   t_A10,
--ak
                   NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A15 ELSE 0 END)), 0) AS t_A15,
--~ak                   
                   NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A16 ELSE 0 END)), 0) AS t_A16,
                   NVL(SUM((CASE WHEN RSI_RSB_FIInstr.NATCUR = one_curr.t_FIID THEN t_A17 ELSE 0 END)), 0) AS t_A17,
                   NVL(SUM((CASE WHEN t_A19_C = one_curr.t_FIID THEN t_A18 ELSE 0 END)), 0) AS t_A18,
                   NVL(SUM((CASE WHEN t_mcicomissfiid = one_curr.t_FIID THEN t_A20 ELSE 0 END)), 0) AS t_A20,
                   NVL(SUM((CASE WHEN t_mcicomissfiid = one_curr.t_FIID THEN t_A21 ELSE 0 END)), 0) AS t_A21,
                   NVL(SUM((CASE WHEN t_mcicomissfiid = one_curr.t_FIID THEN t_A22 ELSE 0 END)), 0) AS t_A22,
                   NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A26 ELSE 0 END)), 0) AS t_A26,
                   NVL(SUM((CASE WHEN RSI_RSB_FIInstr.NATCUR = one_curr.t_FIID THEN t_A27 ELSE 0 END)), 0) AS t_A27,
--ak
                   NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A28 ELSE 0 END)), 0) AS t_A28,
                   NVL(SUM((CASE WHEN t_A14_C = one_curr.t_FIID THEN t_A29 ELSE 0 END)), 0) AS t_A29,
                   NVL(SUM((CASE WHEN t_mcicomissfiid = one_curr.t_FIID THEN t_marketcomissnds ELSE 0 END)), 0) AS t_marketcomissnds,
                   NVL(SUM((CASE WHEN t_mcicomissfiid = one_curr.t_FIID THEN t_cliringcomissnds ELSE 0 END)), 0) AS t_cliringcomissnds,
                   NVL(SUM((CASE WHEN t_mcicomissfiid = one_curr.t_FIID THEN t_tscomissnds ELSE 0 END)), 0) AS t_tscomissnds,
--~ak                   
                   sfcontr.t_id t_sfcontrid, t_part, t_PlanID, t_Direction, dbrkrepdeal_u_tmp.t_fiid
              FROM dbrkrepdeal_u_tmp
              join dsfcontr_dbt sfcontr on sfcontr.t_ServKind = rsi_npto.PTSK_STOCKDL/*1 фондовый дилинг*/ and
                                           sfcontr.t_partyid = p_ClientID and
                                           (sfcontr.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr.t_DateClose >= p_BegDate) and
                                           (p_ContrID = 0 or sfcontr.t_id = p_ContrID)
             WHERE t_ClientID = p_ClientID
               AND t_ContrID = sfcontr.t_id
               --AND t_Part = p_Part
               --AND t_Part != 4/*кроме РЕПО*/
               AND t_planid >= 0/*кроме РЕПО*/
               AND t_IsItog = CHR(0)
               AND (  (t_A14 = one_curr.t_CCY
                    OR t_A19 = one_curr.t_CCY
                    or t_mcicomissfiname = one_curr.t_CCY
                    OR one_curr.t_FIID = RSI_RSB_FIInstr.NATCUR
                   ) 
               AND (t_A15 <> 0 OR t_A20 <> 0 OR 
                    t_A21 <> 0 OR t_A22 <> 0 OR 
                    t_A27 <> 0 OR t_A28 <> 0 OR 
                    t_A29 <> 0 or t_A26 != 0 or
                    t_marketcomissnds != 0 or
                    t_cliringcomissnds != 0 or 
                    t_tscomissnds != 0)
                   )
            GROUP BY t_A10, sfcontr.t_id, t_part, t_PlanID, t_Direction, dbrkrepdeal_u_tmp.t_fiid
           ) q;

    IF v_brkrep.COUNT > 0 THEN
       FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
          INSERT INTO dbrkrepdeal_u_tmp
               VALUES v_brkrep (indx);
    END IF;

/*ak - Добавляем итоги по недостающим валютам
   for c in (select t_contrid, t_Part, t_PlanID, t_Direction 
             from dbrkrepdeal_u_tmp
             where t_clientid = p_ClientID and
                   t_IsItog = chr(88))
   loop
     merge into dbrkrepdeal_u_tmp b
       using (select t_fiid, t_ccy 
              from dfininstr_dbt 
              where t_fi_code in ('810','840','978')) f
       on (b.t_ClientID = p_ClientID and 
           b.t_contrid = c.t_contrid and
           b.t_PlanID = c.t_PlanID and
           b.t_IsItog = CHR(88) and 
           b.t_a30 = f.t_fiid)
     when not matched then
       insert values(
                p_ClientID,
                c.t_contrid,
                c.t_Part,
                c.t_PlanID,
                chr(88),
                c.t_Direction,
      TO_DATE('01.01.0001','DD.MM.YYYY'),
      TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS'),
      TO_DATE('01.01.0001','DD.MM.YYYY'),
      CHR(1),
      CHR(1),
      CHR(1),
      CHR(1),
      CHR(1),
      CHR(1),
      0,
      0,
      CHR(1),
      -1,
      0,
      0,
      0,
      0,
      CHR(1),
      -1,
      0,
      0,
      0,
      CHR(1),
      CHR(1),
      CHR(1),
      0,
      0,
      0,
      0,
      f.t_fiid,
      TO_DATE('01.01.0001','DD.MM.YYYY'),
      0,
      -1,
      0,
      -1,
      0,
      0,
      0,
        -1,
       0,
       0,
       0,
       -1,
       chr(1));
    end loop;
~ak*/
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

  RETURN nvl(v_Amount,0);

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

  TYPE brkrep_t IS TABLE OF DBRKREPDEAL_u_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
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
  /*A07*/   decode(q.t_DealCodets, chr(1), q.t_DealCode, q.t_DealCodets),
  /*A08*/   CHR(1),
  /*A09*/   RSI_RSBPARTY.GetPartyCode(q.t_MarketID, 1 /*PTCK_CONTR*/),
/*ak - Продано/Куплено/Покупка РЕПО ч.1/Продажа РЕПО ч.2/Продажа РЕПО ч.1/Покупка РЕПО ч.2  
  --A10   (CASE WHEN q.IsRepo = 1 THEN DECODE(q.IsBuy, 1, 'ОРЕПО', 'ПРЕПО') || ' ' || TO_CHAR(q.t_DealPart)
                  ELSE DECODE(q.IsBuy, 1, 'Покупка', 'Продажа') END
            ),*/
  /*A10*/   (CASE WHEN q.IsRepo = 1 and q.t_DealPart = 1 THEN DECODE(q.IsBuy, 1, 'Покупка РЕПО ч.', 'Продажа РЕПО ч.') || ' ' || TO_CHAR(q.t_DealPart)
                  WHEN q.IsRepo = 1 and q.t_DealPart = 2 THEN DECODE(q.IsSale, 1, 'Покупка РЕПО ч.', 'Продажа РЕПО ч.') || ' ' || TO_CHAR(q.t_DealPart)
                  ELSE DECODE(q.IsBuy, 1, 'Куплено', 'Продано') END
            ),
/*~ak*/
  /*A11*/   NVL((SELECT pt.t_ShortName
                   FROM dparty_dbt pt
                  WHERE pt.t_PartyID = q.t_Issuer), CHR(1))||
            nvl((select ' '||avrkinds.t_name
                 from davrkinds_dbt avrkinds
                 where avrkinds.t_fi_kind = q.t_fi_kind and
                       avrkinds.t_avoirkind = q.t_avoirkind), chr(1)),
  --A12--   (CASE WHEN q.t_LSIN <> CHR(1) THEN q.t_LSIN ELSE q.t_ISIN END),
  /*A12*/   (CASE WHEN q.t_LSIN = CHR(1) THEN q.t_ISIN 
                  WHEN q.t_ISIN = CHR(1) THEN q.t_LSIN
                  WHEN q.t_LSIN = q.t_ISIN THEN q.t_LSIN
                  ELSE q.t_LSIN||'/'||q.t_ISIN END),
/*ak
  --A13--   (CASE WHEN q.IsRepo = 1 AND q.t_DealPart = 2 THEN 0
                  ELSE GetPrice(q.t_DealID, q.t_DealPart) END
            ),
  --A13_i-- (CASE WHEN q.IsRepo = 1 AND q.t_DealPart = 2 THEN q.t_IncomeRate
                  ELSE 0 END
            ),*/
  /*A13*/   GetPrice(q.t_DealID, q.t_DealPart),
  /*A13_i*/ (CASE WHEN q.IsRepo = 1 THEN q.t_IncomeRate
                  ELSE 0 END
            ),
/*~ak*/
  /*A14*/   CHR(1),
  /*A14_C*/ q.t_CFI,
  /*A15*/   q.t_Amount,
  --A16--   q.t_TotalCost,
  --A17--   RSI_RSB_FIInstr.ConvSum(q.t_TotalCost, q.t_CFI, RSI_RSB_FIInstr.NATCUR, CASE WHEN q.t_State = RSI_DLRQ.DLRQ_STATE_EXEC THEN GetPlanExecDate(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) ELSE p_EndDate END),
  /*A16*/   q.t_TotalCost - q.t_NKD,
  /*A17*/   RSI_RSB_FIInstr.ConvSum(q.t_TotalCost - q.t_NKD, q.t_CFI, RSI_RSB_FIInstr.NATCUR, CASE WHEN q.t_State = RSI_DLRQ.DLRQ_STATE_EXEC THEN GetPlanExecDate(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) ELSE p_EndDate END),
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
  /*A30*/   -1,
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
  /*A95_M*/ CHR(0)
--ak
  /*t_marketcomissnds*/ ,0,
  /*t_cliringcomissnds*/ 0,
  /*t_tscomissnds*/ 0,
  /*t_mcicomissfiid*/ -1,
  /*t_mcicomissfiname*/ chr(1)
--~ak
  BULK COLLECT INTO v_brkrep
  
  FROM (--репо без корзины
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
               fin.t_FIID,
               fin.t_fi_kind,
               fin.t_avoirkind
          FROM (SELECT DISTINCT tk.t_DealID, rq.t_FactDate, rq.t_DealPart, rq.t_State,
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
                  ) d, 
               ddl_tick_dbt tk, ddlrq_dbt rq, ddl_leg_dbt leg, dfininstr_dbt fin, davoiriss_dbt avr
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
               fin.t_FIID,
               fin.t_fi_kind,
               fin.t_avoirkind
          FROM (SELECT DISTINCT tk.t_DealID, rq.t_FactDate, rq.t_DealPart, rq.t_State,
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
                  ) d, 
               ddl_tick_dbt tk, ddl_leg_dbt leg, dfininstr_dbt fin, davoiriss_dbt avr
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
        INSERT INTO dbrkrepdeal_u_tmp
             VALUES v_brkrep (indx);
  END IF;

END CreateCompData;

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

--Формирование данных по внутреннему учёту для раздела 6 отчета (только фактические данные)
PROCEDURE CreateInAccData( p_ClientID      IN NUMBER,
                           p_ContrID       IN NUMBER,
                           p_BegDate       IN DATE,
                           p_EndDate       IN DATE
                         )
IS

  TYPE brkrep_t IS TABLE OF DBRKREPINACC_u_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;

BEGIN

  SELECT    p_ClientID,
            q.t_sfcontrid,
            CHR(0),
            q.t_PlaceID,
            q.t_FIID,
  /*A53*/   case when nvl(avoiriss.t_lsin,chr(1)) = chr(1) then nvl(avoiriss.t_isin,chr(1))
                 when nvl(avoiriss.t_isin,chr(1)) = chr(1) then nvl(avoiriss.t_lsin,chr(1))
                 when avoiriss.t_isin = avoiriss.t_lsin then avoiriss.t_isin
                 else avoiriss.t_lsin||'/'||avoiriss.t_isin end,
  /*A54*/   nvl(fininstr.t_name,chr(1)),
  /*A55*/   q.InRest,
  /*A56*/   q.EnrolSum,
  /*A56_1*/ 0,
  /*A57*/   q.WrtOffSum,
  /*A57_1*/ 0,
  /*A58*/   q.OutRest,
  /*A58_1*/ 0,
  /*A59*/   0,
  /*A59_1*/ -1,
  /*A60*/   0,
  /*A61*/   0,
  /*A62*/   0,
  /*A63*/   0
  BULK COLLECT INTO v_brkrep
    FROM (SELECT 
                    t_sfcontrid,
                    t_PlaceID,
                    t_FIID,
          /*A55*/   NVL(SUM(InRest), 0) InRest, 
          /*A56*/   NVL(SUM(EnrolSum), 0) EnrolSum,
          /*A57*/   NVL(SUM(WrtOffSum), 0) WrtOffSum,
          /*A58*/   NVL(SUM(OutRest), 0) OutRest
               FROM (SELECT acc.t_sfcontrid,
                            acc.t_Code_Currency as t_FIID,
                            -1*rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_BegDate-1, acc.t_Chapter, null) as InRest,
                            -1*rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_EndDate, acc.t_Chapter, null) as OutRest,
                            (CASE WHEN GetAccPoolID(acc.t_sfcontrid, acc.t_Account, acc.t_Code_Currency, acc.t_Chapter) <= 0
                                       THEN rsb_account.kreditac(acc.t_Account,acc.t_Chapter,acc.t_Code_Currency,p_BegDate,p_EndDate, null)
                                            - GetPoolAccTrnSum(acc.t_AccountID, 0, p_BegDate, p_EndDate)
                                       ELSE 0 END) as WrtOffSum,
                            (CASE WHEN GetAccPoolID(acc.t_sfcontrid, acc.t_Account, acc.t_Code_Currency, acc.t_Chapter) <= 0
                                        THEN rsb_account.debetac(acc.t_Account,acc.t_Chapter,acc.t_Code_Currency,p_BegDate,p_EndDate, null)
                                             - GetPoolAccTrnSum(acc.t_AccountID, 1, p_BegDate, p_EndDate)
                                        ELSE 0 END)as EnrolSum,
                            (SELECT accd.t_Place
                               FROM dmcaccdoc_dbt accd
                              WHERE accd.t_Chapter = acc.t_Chapter
                                AND accd.t_Currency = acc.t_Code_Currency
                                AND accd.t_Account = acc.t_Account
                                AND accd.t_ClientContrID = acc.t_sfcontrid
                                AND ROWNUM = 1) as t_PlaceID
                       FROM ( select /*+ leading(sfcontr,cat,accd,acc) index(accd DMCACCDOC_DBT_USR5) */
                                     distinct acc.t_AccountID,acc.t_Account, acc.t_Code_Currency , acc.t_Chapter , sfcontr.t_id t_sfcontrid
                                 from daccount_dbt acc
                                 join dsfcontr_dbt sfcontr on sfcontr.t_ServKind = rsi_npto.PTSK_STOCKDL/*1 фондовый дилинг*/ and
                                                              sfcontr.t_partyid = p_ClientID and
                                                              (sfcontr.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr.t_DateClose >= p_BegDate) and
                                                              (p_ContrID = 0 or sfcontr.t_id = p_ContrID)
                                join dmccateg_dbt cat
                                   on cat.t_LevelType = 1
                                             AND cat.t_Code = 'ЦБ Клиента, ВУ'
                                join dmcaccdoc_dbt accd
                                    on accd.t_Chapter = acc.t_Chapter
                                              AND accd.t_Currency = acc.t_Code_Currency
                                              AND accd.t_Account = acc.t_Account
                                              AND accd.t_ClientContrID = sfcontr.t_id
                                              AND cat.t_Number = accd.t_CatNum
                                WHERE acc.t_Chapter = 22
                                  --AND acc.t_Client = p_ClientID
                                  /*AND EXISTS(SELECT \*+ index(accd DMCACCDOC_DBT_USR4) *\  1
                                               FROM dmcaccdoc_dbt accd, dmccateg_dbt cat
                                              WHERE accd.t_Chapter = acc.t_Chapter
                                                AND accd.t_Currency = acc.t_Code_Currency
                                                AND accd.t_Account = acc.t_Account
                                                AND accd.t_ClientContrID = sfcontr.t_id
                                                AND cat.t_Number = accd.t_CatNum
                                                AND cat.t_LevelType = 1
                                                AND cat.t_Code = 'ЦБ Клиента, ВУ'
                                            )*/
                              ) acc
                     ) s
              --WHERE (s.InRest != 0 OR s.OutRest != 0 OR s.EnrolSum != 0 OR s.WrtOffSum != 0)
              GROUP BY t_sfcontrid, t_placeid, t_FIID
             ) q
  --GROUP BY q.t_sfcontrid, q.t_PlaceID, q.t_FIID;
  left join davoiriss_dbt avoiriss on avoiriss.t_fiid = q.t_fiid
  left join dfininstr_dbt fininstr on fininstr.t_fiid = q.t_fiid
  where q.InRest != 0 OR q.OutRest != 0 OR q.EnrolSum != 0 OR q.WrtOffSum != 0;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepinacc_u_tmp
             VALUES v_brkrep (indx);
  END IF;

END CreateInAccData;

-- Получение котировки
FUNCTION GetRate( SumB     IN NUMBER
                 ,pFromFI IN NUMBER
                 ,pToFI    IN NUMBER
                 ,pType    IN NUMBER
                 ,pbdate   IN DATE
                )
  RETURN NUMBER
IS
  v_Rate     NUMBER;
  v_RateID   NUMBER;
  v_RateDate DATE;
BEGIN
  v_Rate := RSI_RSB_FIInstr.FI_GetRate( pFromFI, pToFI, pType, pbdate, pbdate-to_date('01.01.0001','DD.MM.YYYY'), 0, v_RateID, v_RateDate );
  if( v_Rate <= 0 ) then
     v_Rate := RSI_RSB_FIInstr.FI_GetRate( pFromFI, -1, pType, pbdate, pbdate-to_date('01.01.0001','DD.MM.YYYY'), 0, v_RateID, v_RateDate );
  end if;
  if(v_RateID > 0)then
    begin
      select t_rate/power(10,t_point)/t_scale 
      into v_Rate
      from dratedef_dbt
      where t_rateid = v_RateID and
            t_isinverse != chr(88) and
            t_sincedate = v_RateDate;
    exception when others then
      begin
        select t_rate/power(10,t_point)/t_scale 
        into v_Rate
        from dratehist_dbt
        where t_rateid = v_RateID and
              t_isinverse != chr(88) and
              t_sincedate = v_RateDate;
      exception when others then
        null;
      end;
    end;
  end if;

  return v_Rate*SumB;
EXCEPTION
  when OTHERS then return 0.0;
END;

--Golovkin стоимоть бумаг в валюте котировки
FUNCTION GetAvrCost( SumB     IN NUMBER
                 ,pFromFI IN NUMBER
                 ,pType    IN NUMBER
                 ,pbdate   IN DATE
                )
  RETURN NUMBER
IS
  v_Rate     NUMBER;
  v_RateID   NUMBER;
  v_RateDate DATE;
  v_isRelative dratedef_dbt.t_isrelative%type;
  v_fininstr dfininstr_dbt%rowtype;
  v_rate_fiid number;
  v_RateMrkt NUMBER;--CHVA
  v_rate_mrkt_fiid number;--CHVA
  v_RateID_Mrkt   NUMBER;--CHVA
  v_RateDate_Mrkt DATE;--CHVA
  
BEGIN
--CHVA Ищем курс "Рыночная цена за дату" и определяем в какой валюте этот курс
  v_RateMrkt := RSI_RSB_FIInstr.FI_GetRate( pFromFI, -1, 1, pbdate, pbdate-to_date('01.01.0001','DD.MM.YYYY'), 0, v_RateID_Mrkt, v_RateDate_Mrkt );
  if(v_RateID_Mrkt > 0) then
   begin
    select T_FIID
    into v_rate_mrkt_fiid
    from dratedef_dbt
    where  t_rateid = v_RateID_Mrkt and
            t_isinverse != chr(88) and
            t_sincedate = v_RateDate_Mrkt;
    exception when others then
      begin
        select ratedef.T_FIID 
        into v_rate_mrkt_fiid
        from dratehist_dbt ratehist, dratedef_dbt ratedef
        where ratehist.t_rateid = v_RateID_Mrkt and
              ratehist.t_isinverse != chr(88) and
              ratehist.t_sincedate = v_RateDate_Mrkt and
              ratehist.t_rateid = ratedef.t_rateid;
      exception when others then
       v_rate_mrkt_fiid := -1;
      end;
     end;
  end if;
  
  
  v_Rate := RSI_RSB_FIInstr.FI_GetRate( pFromFI, v_rate_mrkt_fiid /*-1*/, pType, pbdate, pbdate-to_date('01.01.0001','DD.MM.YYYY'), 0, v_RateID, v_RateDate );-- CHVA ищем курс Средневзвешенная цена в этой валюте

  if(v_RateID > 0)then
    begin
      select t_rate/power(10,t_point)/t_scale,
               T_ISRELATIVE, T_FIID 
      into v_Rate, v_isRelative, v_rate_fiid
      from dratedef_dbt
      where t_rateid = v_RateID and
            t_isinverse != chr(88) and
            t_sincedate = v_RateDate;
    exception when others then
      begin
        select ratehist.t_rate/power(10,ratehist.t_point)/ratehist.t_scale,
                 ratedef.T_ISRELATIVE, ratedef.T_FIID 
        into v_Rate, v_isRelative, v_rate_fiid
        from dratehist_dbt ratehist, dratedef_dbt ratedef
        where ratehist.t_rateid = v_RateID and
              ratehist.t_isinverse != chr(88) and
              ratehist.t_sincedate = v_RateDate and
              ratehist.t_rateid = ratedef.t_rateid;
      exception when others then
        null;
      end;
    end;
  end if;

  SELECT * INTO v_fininstr
  FROM DFININSTR_DBT 
  WHERE T_FIID = pFromFI;

  -- Если котировка в %, то считаем от номинала
  if v_isRelative = chr(88) then
      v_rate := v_rate/100*RSI_RSB_Fiinstr.Fi_GetNominalOnDate(v_fininstr.t_fiid,pbdate); --v_fininstr.t_facevalue;

      if v_rate_fiid != v_fininstr.t_facevaluefi then
          v_rate := RSI_RSB_FIInstr.ConvSum(v_rate,  v_fininstr.t_facevaluefi, v_rate_fiid, pbdate);
      end if;      
  end if;

  return v_Rate*SumB;
EXCEPTION
  when OTHERS then return 0.0;
END;

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

--Корректировка данных по внутреннему учёту для раздела 6 отчета с учетом плановых движений
PROCEDURE CorrectInAccData( p_ClientID      IN NUMBER,
                            p_ContrID       IN NUMBER,
                            p_BegDate       IN DATE,
                            p_EndDate       IN DATE
                          )
IS

  TYPE brkrep_t IS TABLE OF DBRKREPINACC_u_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;

  v_CourceType NUMBER;
BEGIN

--  v_CourceType := Rsb_Common.GetRegIntValue( 'SECUR\ВИД КУРСА РЫНОЧНАЯ ЦЕНА', 0 );
  v_CourceType := Rsb_Common.GetRegIntValue( 'SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0 );

  
  select  p_ClientID,
          sfcontr.t_id,
          CHR(0),
          1/*placeid*/,
          q.t_FIID,
/*A53*/   chr(1),
/*A54*/   chr(1),
/*A55*/   0,
/*A56*/   0,
/*A56_1*/ case when t_direction = 1 then t_a29 else 0 end a56_1,
/*A57*/   0,
/*A57_1*/ case when t_direction = 2 then t_a29 else 0 end a57_1,
/*A58*/   0,
/*A58*/   0,
/*A59*/   0,
/*A59_1*/ -1,
/*A60*/   0,
/*A61*/   0,
/*A62*/   0,
/*A63*/   0
  BULK COLLECT INTO v_brkrep
  from dbrkrepdeal_u_tmp q
  join dsfcontr_dbt sfcontr on sfcontr.t_ServKind = rsi_npto.PTSK_STOCKDL/*1 фондовый дилинг*/ and
                               sfcontr.t_partyid = p_ClientID and
                               (sfcontr.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr.t_DateClose >= p_BegDate) and
                               (p_ContrID = 0 or sfcontr.t_id = p_ContrID)
  where q.t_ClientID = p_ClientID and
        q.t_ContrID = sfcontr.t_id and
        q.t_IsItog = CHR(0) and
        ((q.t_part in (1,2,4) and q.t_a06 > p_EndDate));

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepinacc_u_tmp
             VALUES v_brkrep (indx);

  END IF;

  SELECT    p_ClientID,
            q.t_sfcontrid,
            CHR(0),
            q.t_PlaceID,
            q.t_FIID,
  /*A53*/   case when nvl(avr.t_lsin,chr(1)) = chr(1) then nvl(avr.t_isin,chr(1))
                 when nvl(avr.t_isin,chr(1)) = chr(1) then nvl(avr.t_lsin,chr(1))
                 when avr.t_isin = avr.t_lsin then avr.t_isin
                 else avr.t_lsin||'/'||avr.t_isin end,
  /*A54*/   nvl(fin.t_name,chr(1)),
  /*A55*/   q.t_A55,
  /*A56*/   q.t_A56,
  /*A56_1*/ q.t_A56_1,
  /*A57*/   q.t_A57,
  /*A57_1*/ q.t_A57_1,
  /*A58*/   q.t_A58,
  /*A58*/   q.t_A58_1,
  /*A59*/   RSB_SPREPFUN.GetCourse(q.t_RateId, p_EndDate),--NVL(RSB_Secur.SC_ConvSumTypeRep(1, fin.t_FIID, fin.t_FaceValueFI, fin.t_FaceValueFI, v_CourceType, p_EndDate), 0),
  /*A59_1*/ RSB_SPREPFUN.GetCourseFI(q.t_RateId),
--ak  
  /*A60*/   0,--NVL(GetRate(1, fin.t_FIID, fin.t_FaceValueFI, v_CourceType, p_EndDate), 0),
--~ak
  /*A61*/   0,
  /*A62*/   RSI_RSB_FIInstr.FI_CalcNKD(fin.t_FIID, p_EndDate, q.t_A58, 0),
  /*A63*/   0
  BULK COLLECT INTO v_brkrep
  FROM (SELECT sfcontr.t_id t_sfcontrid, 
               NVL(SUM(t_A55), 0)   AS t_A55,
               NVL(SUM(t_A56), 0)   AS t_A56,
               NVL(SUM(t_A56_1), 0) AS t_A56_1,
               NVL(SUM(t_A57), 0)   AS t_A57,
               NVL(SUM(t_A57_1), 0) AS t_A57_1,
               NVL(SUM(t_A58), 0)   AS t_A58,
               NVL(SUM(t_A58+t_A56_1-t_A57_1), 0) AS t_A58_1,
               GetActiveRateId(t.t_FIID, p_EndDate) t_RateId,
               t_PlaceID,
               t.t_FIID
          FROM dbrkrepinacc_u_tmp t
          join dsfcontr_dbt sfcontr on sfcontr.t_ServKind = rsi_npto.PTSK_STOCKDL/*1 фондовый дилинг*/ and
                                       sfcontr.t_partyid = p_ClientID and
                                       (sfcontr.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr.t_DateClose >= p_BegDate) and
                                       (p_ContrID = 0 or sfcontr.t_id = p_ContrID)
         WHERE t_ClientID = p_ClientID
           AND t_ContrID = sfcontr.t_id
           AND t_IsItog = CHR(0)
        GROUP BY sfcontr.t_id, t_PlaceID, t.t_FIID
       ) q, dfininstr_dbt fin, davoiriss_dbt avr
  WHERE fin.t_FIID = q.t_FIID
    AND avr.t_FIID = fin.t_FIID;

  DELETE FROM dbrkrepinacc_u_tmp
        WHERE t_ClientID = p_ClientID;


  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepinacc_u_tmp
             VALUES v_brkrep (indx);
  END IF;
 --insert into usertable  (usrfiid) select t_fiid from dbrkrepinacc_u_tmp where t_clientid = 136093 and t_fiid = 1609;  
/*  
  UPDATE dbrkrepinacc_u_tmp r
     SET --r.t_A60 = ABS(t_A58*t_A59),
         r.t_A60 =  NVL(GetAvrCost(t_A58, r.t_FIID, v_CourceType, p_EndDate), 0), -- Golovkin стоимость в валюте котировки
         r.t_A61 = RSI_RSB_FIInstr.ConvSum(ABS(r.t_A58*r.t_A59), (SELECT f.t_FaceValueFI FROM dfininstr_dbt f WHERE f.t_FIID = r.t_FIID), RSI_RSB_FIInstr.NATCUR, p_EndDate),
         r.t_A63 = RSI_RSB_FIInstr.ConvSum(r.t_A62, (SELECT f.t_FaceValueFI FROM dfininstr_dbt f WHERE f.t_FIID = r.t_FIID), RSI_RSB_FIInstr.NATCUR, p_EndDate),
--ak
         r.t_A59 = t_A60
--~ak
   WHERE r.t_ClientID = p_ClientID;
*/

  UPDATE dbrkrepinacc_u_tmp r
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
                                   ELSE CASE WHEN q1.t_A58 > 0 AND q1.t_bc > 0 THEN (q1.t_bc-q1.t_A62)/q1.t_A58 ELSE 0 END
                              END t_A59,
                              CASE WHEN q1.t_A59_1 != -1 THEN q1.t_A59_1
                                   ELSE q1.t_fv
                              END t_A59_1,
                              CASE WHEN q1.t_A59_1 != -1 THEN ABS(q1.t_A58*q1.t_A59)
                                   ELSE CASE WHEN q1.t_bc > 0 THEN (q1.t_bc-q1.t_A62) ELSE 0 END
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
            t_contrid,
            'X',
            q.t_PlaceID,
            -1,
  /*A53*/   CHR(1),
  /*A54*/   CHR(1),
  /*A55*/   q.t_A55,
  /*A56*/   q.t_A56,
  /*A56_1*/ q.t_A56_1,
  /*A57*/   q.t_A57,
  /*A57_1*/ q.t_A57_1,
  /*A58*/   q.t_A58,
  /*A58_1*/ q.t_A58_1,
  /*A59*/   0,
  /*A59_1*/ -1,
  /*A60*/   q.t_A60,
  /*A61*/   q.t_A61,
  /*A62*/   q.t_A62,
  /*A63*/   q.t_A63
  BULK COLLECT INTO v_brkrep
  FROM (SELECT t_contrid,
               NVL(SUM(t_A55), 0)   AS t_A55,
               NVL(SUM(t_A56), 0)   AS t_A56,
               NVL(SUM(t_A56_1), 0) AS t_A56_1,
               NVL(SUM(t_A57), 0)   AS t_A57,
               NVL(SUM(t_A57_1), 0) AS t_A57_1,
               NVL(SUM(t_A58), 0)   AS t_A58,
               NVL(SUM(t_A58_1), 0) AS t_A58_1,
               NVL(SUM(t_A60), 0)   AS t_A60,
               NVL(SUM(t_A61), 0)   AS t_A61,
               NVL(SUM(t_A62), 0)   AS t_A62,
               NVL(SUM(t_A63), 0)   AS t_A63,
               t_PlaceID
          FROM dbrkrepinacc_u_tmp
         WHERE t_ClientID = p_ClientID
           AND t_IsItog = CHR(0)
        GROUP BY t_contrid, t_PlaceID
       ) q;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepinacc_u_tmp
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

  TYPE brkrep_t IS TABLE OF DBRKREPPOOL_u_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;

  v_CourceType NUMBER;

BEGIN

--  v_CourceType := Rsb_Common.GetRegIntValue( 'SECUR\ВИД КУРСА РЫНОЧНАЯ ЦЕНА', 0 );
  v_CourceType := Rsb_Common.GetRegIntValue( 'SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0 );

  SELECT    p_ClientID,
            q.t_sfcontrid,
            CHR(0),
            q.t_PlaceID,
            q.t_PoolID,
            q.t_FIID,
  --A73--   NVL((SELECT (CASE WHEN avr.t_LSIN <> CHR(1) THEN avr.t_LSIN ELSE avr.t_ISIN END) FROM davoiriss_dbt avr WHERE avr.t_FIID = q.t_FIID), CHR(1)),
  /*A73*/   NVL((SELECT (CASE WHEN avr.t_LSIN = CHR(1) THEN avr.t_ISIN 
                              WHEN avr.t_ISIN = CHR(1) THEN avr.t_LSIN
                              WHEN avr.t_ISIN = avr.t_LSIN THEN avr.t_ISIN
                         ELSE avr.t_LSIN||'/'||avr.t_ISIN END) FROM davoiriss_dbt avr WHERE avr.t_FIID = q.t_FIID), CHR(1)),
  /*A74*/   (SELECT fin.t_FI_Code FROM dfininstr_dbt fin WHERE fin.t_FIID = q.t_FIID),
  /*A75*/   q.InRest,
  /*A76*/   q.EnrolSum,
  /*A77*/   q.WrtOffSum,
  /*A78*/   q.OutRest,
  /*A79*/   RSB_SPREPFUN.GetCourse(q.t_RateId, p_EndDate), --NVL((SELECT RSB_Secur.SC_ConvSumTypeRep(1, fin.t_FIID, fin.t_FaceValueFI, fin.t_FaceValueFI, v_CourceType, p_EndDate) FROM dfininstr_dbt fin WHERE fin.t_FIID = q.t_FIID), 0),
  /*A79_1*/ RSB_SPREPFUN.GetCourseFI(q.t_RateId),
  /*A80*/   0,
  /*A81*/   0,
  /*A82*/   0,
  /*A83*/   0,
  /*A84*/   0,
  /*A85*/   0,
  /*A86*/   NVL((SELECT iss.t_ShortName FROM dparty_dbt iss WHERE iss.t_PartyID = RSI_RSB_FIInstr.FI_GetIssuerOnDate(q.t_FIID, p_EndDate)), CHR(1))
  BULK COLLECT INTO v_brkrep
    FROM (SELECT
                    t_sfcontrid,
                    t_PlaceID,
                    t_PoolID,
                    t_FIID,
          /*A55*/   NVL(SUM(InRest), 0) InRest, 
          /*A56*/   NVL(SUM(EnrolSum), 0) EnrolSum,
          /*A57*/   NVL(SUM(WrtOffSum), 0) WrtOffSum,
          /*A58*/   NVL(SUM(OutRest), 0) OutRest,
                    GetActiveRateId(s.t_FIID, p_EndDate) t_RateId
               FROM (SELECT acc.t_sfcontrid,
                            acc.t_Code_Currency as t_FIID,
                            -1*rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_BegDate-1, acc.t_Chapter, null) as InRest,
                            -1*rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_EndDate, acc.t_Chapter, null) as OutRest,
                            rsb_account.kreditac(acc.t_Account,acc.t_Chapter,acc.t_Code_Currency,p_BegDate,p_EndDate, null) as WrtOffSum,
                            rsb_account.debetac(acc.t_Account,acc.t_Chapter,acc.t_Code_Currency,p_BegDate,p_EndDate, null) as EnrolSum,
                            (SELECT accd.t_Place
                               FROM dmcaccdoc_dbt accd
                              WHERE accd.t_Chapter = acc.t_Chapter
                                AND accd.t_Currency = acc.t_Code_Currency
                                AND accd.t_Account = acc.t_Account
                                AND accd.t_ClientContrID = acc.t_sfcontrid
                                AND ROWNUM = 1) as t_PlaceID,
                            GetAccPoolID(acc.t_sfcontrid, acc.t_Account, acc.t_Code_Currency, acc.t_Chapter) as t_PoolID
                      FROM (select /*+ leading(sfcontr,cat,accd, tpl,acc) index(accd DMCACCDOC_DBT_USR5) */ 
                                distinct acc.t_Account, acc.t_Code_Currency, acc.t_Chapter , sfcontr.t_id t_sfcontrid
                             from  daccount_dbt acc
                             join dsfcontr_dbt sfcontr on sfcontr.t_ServKind = rsi_npto.PTSK_STOCKDL/*1 фондовый дилинг*/ and
                                                          sfcontr.t_partyid = p_ClientID and
                                                          (sfcontr.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr.t_DateClose >= p_BegDate) and
                                                          (p_ContrID = 0 or sfcontr.t_id = p_ContrID)
                              join dmccateg_dbt cat 
                                on cat.t_LevelType = 1
                                  and cat.t_Code = 'ЦБ Клиента, ВУ' 
                              join dmcaccdoc_dbt accd
                                on accd.t_Chapter = acc.t_Chapter
                                and accd.t_Currency = acc.t_Code_Currency
                                and accd.t_Account = acc.t_Account
                                and accd.t_ClientContrID = sfcontr.t_id
                                and accd.t_CatNum = cat.t_Number
                              join dmctempl_dbt tpl
                                on tpl.t_CatID = accd.t_CatID
                               and tpl.t_Number = accd.t_TemplNum
                               and CASE WHEN cat.t_Class1 = 1859 THEN tpl.t_Value1
                                        WHEN cat.t_Class2 = 1859 THEN tpl.t_Value2
                                        WHEN cat.t_Class3 = 1859 THEN tpl.t_Value3
                                        WHEN cat.t_Class4 = 1859 THEN tpl.t_Value4
                                        WHEN cat.t_Class5 = 1859 THEN tpl.t_Value5
                                        WHEN cat.t_Class6 = 1859 THEN tpl.t_Value6
                                        WHEN cat.t_Class7 = 1859 THEN tpl.t_Value7
                                        WHEN cat.t_Class8 = 1859 THEN tpl.t_Value8
                                        ELSE 0 END > 0       
                            WHERE acc.t_Chapter = 22
                              AND acc.t_Client = p_ClientID
                              /*AND EXISTS(SELECT 1
                                           FROM dmcaccdoc_dbt accd, dmccateg_dbt cat, dmctempl_dbt tpl
                                          WHERE accd.t_Chapter = acc.t_Chapter
                                            AND accd.t_Currency = acc.t_Code_Currency
                                            AND accd.t_Account = acc.t_Account
                                            AND accd.t_ClientContrID = sfcontr.t_id
                                            AND cat.t_Number = accd.t_CatNum
                                            AND cat.t_LevelType = 1
                                            AND cat.t_Code = 'ЦБ Клиента, ВУ'
                                            --AND GetAccPoolID(sfcontr.t_id, acc.t_Account, acc.t_Code_Currency, acc.t_Chapter) > 0
                                            AND tpl.t_CatID = accd.t_CatID
                                            AND tpl.t_Number = accd.t_TemplNum
                                            AND ROWNUM = 1 and
                                            (CASE WHEN cat.t_Class1 = 1859 THEN tpl.t_Value1
                                                       WHEN cat.t_Class2 = 1859 THEN tpl.t_Value2
                                                       WHEN cat.t_Class3 = 1859 THEN tpl.t_Value3
                                                       WHEN cat.t_Class4 = 1859 THEN tpl.t_Value4
                                                       WHEN cat.t_Class5 = 1859 THEN tpl.t_Value5
                                                       WHEN cat.t_Class6 = 1859 THEN tpl.t_Value6
                                                       WHEN cat.t_Class7 = 1859 THEN tpl.t_Value7
                                                       WHEN cat.t_Class8 = 1859 THEN tpl.t_Value8
                                                       ELSE 0 END) > 0
                                            ) */
                            ) acc
                     ) s
              --WHERE (s.InRest != 0 OR s.OutRest != 0 OR s.EnrolSum != 0 OR s.WrtOffSum != 0)
              GROUP BY s.t_sfcontrid, s.t_PlaceID, s.t_PoolID, s.t_FIID
             ) q
  --GROUP BY q.t_sfcontrid, q.t_PlaceID, q.t_PoolID, q.t_FIID;
  WHERE (q.InRest != 0 OR q.OutRest != 0 OR q.EnrolSum != 0 OR q.WrtOffSum != 0);

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkreppool_u_tmp
             VALUES v_brkrep (indx);
  END IF;

  UPDATE dbrkreppool_u_tmp r
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
                                         ELSE CASE WHEN q2.t_A78 > 0 AND q2.t_bc > 0 THEN (q2.t_bc-q2.t_A82)/q2.t_A78  ELSE 0 END
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
  UPDATE dbrkreppool_u_tmp r
     SET r.t_A80 = ABS(r.t_A78*r.t_A79),
         r.t_A82 = RSI_RSB_FIInstr.FI_CalcNKD(r.t_FIID, p_EndDate, r.t_A78, 0)
   WHERE r.t_ClientID = p_ClientID;

  UPDATE dbrkreppool_u_tmp r
     SET r.t_A81 = RSI_RSB_FIInstr.ConvSum(r.t_A80, (SELECT f.t_FaceValueFI FROM dfininstr_dbt f WHERE f.t_FIID = r.t_FIID), RSI_RSB_FIInstr.NATCUR, p_EndDate),
         r.t_A83 = RSI_RSB_FIInstr.ConvSum(r.t_A82, (SELECT f.t_FaceValueFI FROM dfininstr_dbt f WHERE f.t_FIID = r.t_FIID), RSI_RSB_FIInstr.NATCUR, p_EndDate)
   WHERE r.t_ClientID = p_ClientID;

  UPDATE dbrkreppool_u_tmp r
     SET r.t_A84 = r.t_A80+r.t_A82,
         r.t_A85 = r.t_A81+r.t_A83
   WHERE r.t_ClientID = p_ClientID;
*/

  --Создать итоговые строки
  SELECT    p_ClientID,
            t_ContrID,
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
  /*A79_1*/-1,
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
               t_ContrID, t_PlaceID, t_PoolID
          FROM dbrkreppool_u_tmp
         WHERE t_ClientID = p_ClientID
           AND t_IsItog = CHR(0)
        GROUP BY t_ContrID, t_PlaceID, t_PoolID
       ) q;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkreppool_u_tmp
             VALUES v_brkrep (indx);
  END IF;


END CreatePoolData;


END rsb_brkrep_u;
/
