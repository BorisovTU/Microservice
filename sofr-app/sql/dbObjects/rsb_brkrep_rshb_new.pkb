CREATE OR REPLACE PACKAGE BODY RSB_BRKREP_RSHB_NEW
IS 
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
     AND leg.t_LegKind = DECODE(p_Part, BROKERREP_PART_DEALINPERIOD, 0, 2)
     AND leg.t_LegID = 0
     AND fin.t_FIID = leg.t_PFI;

  RETURN v_Price;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN NULL;
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
FUNCTION GetBrokerComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER, p_CFI IN NUMBER DEFAULT -1) RETURN NUMBER
AS
 v_Sum  NUMBER;
 v_FIID NUMBER;
BEGIN

  g_BrokerComissFIID := -1;

  IF p_Part = BROKERREP_PART_DEALINPERIOD THEN
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
      g_BrokerComissFIID := p_CFI; --В случае отсутствия валюты комиссии брокера дублируем валюту сделки
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
  IF p_Part = BROKERREP_PART_DEALINPERIOD THEN
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

--Формирование данных по сделкам фондового рынка
PROCEDURE CreateDealData( p_DlContrID     IN NUMBER,
                          p_BegDate       IN DATE,
                          p_EndDate       IN DATE,
                          p_ByExchange    IN NUMBER,
                          p_ByOutExchange IN NUMBER,
                          p_IsEDP         IN NUMBER
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
                    GetRQAmountCashOnDate(rq.t_DocKind, rq.t_DocID, rq.t_DealPart, p_EndDate) as t_TotalCost,
                    leg.t_NKD,
                    GetRQAmountSecuritiesOnDate(rq.t_DocKind, rq.t_DocID, rq.t_DealPart, p_EndDate) as t_Amount,
                    rq.t_DealPart, rq.t_State,
                    avr.t_LSIN, avr.t_ISIN, fin.t_FaceValueFI, fin.t_Issuer,
                    RSB_SECUR.IsRepo(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsRepo,
                    RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsBuy,
                    RSB_SECUR.IsSale(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsSale,
                    RSB_SECUR.IsOTC(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) as IsOTC,
                    case when RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(tk.t_DealID, 34, '0'), 116, tk.t_DealDate) = 1 then 1 else 0 end as IsMarginCall,
                    GetExecDate(tk.t_BOfficeKind, tk.t_DealID, rq.t_DealPart) as FactExecDate,
                    fin.t_FIID,
                    q_sf.t_PartyID as ClientID, q_sf.t_ID as SfContrID, q_sf.t_ServKind, q_sf.t_ServKindSub,
                    (CASE WHEN leg.t_RejectDate <> TO_DATE('01.01.0001','DD.MM.YYYY') AND leg.t_RejectDate <= p_EndDate THEN 1 ELSE 0 END) as IsRejectDeal
               FROM ddl_tick_dbt tk, ddlrq_dbt rq, ddl_leg_dbt leg, q_sf, dfininstr_dbt fin, davoiriss_dbt avr
              WHERE tk.t_BOfficeKind   = 101 /*DL_SECURITYDOC*/
                AND tk.t_ClientID      = q_sf.t_PartyID
                AND tk.t_ClientContrID = q_sf.t_ID
                AND tk.t_DealDate     <= p_EndDate
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
                    GetRQAmountCashOnDate(tk.t_BOfficeKind, tk.t_DealID, decode(leg.t_LegKind,0,1,2),p_EndDate) as t_TotalCost,
                    GetBasketNKDOnDate(tk.t_DealID, p_EndDate) as t_NKD,
                    GetRQAmountSecuritiesOnDate(tk.t_BOfficeKind, tk.t_DealID, decode(leg.t_LegKind,0,1,2), p_EndDate) as t_Amount,
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
                    case when RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(tk.t_DealID, 34, '0'), 116, tk.t_DealDate) = 1 then 1 else 0 end as IsMarginCall,
                    GetExecDate(tk.t_BOfficeKind, tk.t_DealID, DECODE(leg.t_LegKind, 0 /*LEG_KIND_DL_TICK*/, 1, 2)) as FactExecDate,
                    fin.t_FIID,
                    q_sf.t_PartyID as ClientID, q_sf.t_ID as SfContrID, q_sf.t_ServKind, q_sf.t_ServKindSub,
                    (CASE WHEN leg.t_RejectDate <> TO_DATE('01.01.0001','DD.MM.YYYY') AND leg.t_RejectDate <= p_EndDate THEN 1 ELSE 0 END) as IsRejectDeal
               FROM ddl_tick_dbt tk, ddl_leg_dbt leg, q_sf, dfininstr_dbt fin, davoiriss_dbt avr
              WHERE tk.t_BOfficeKind   = 101 /*DL_SECURITYDOC*/
                AND tk.t_ClientID      = q_sf.t_PartyID
                AND tk.t_ClientContrID = q_sf.t_ID
                AND tk.t_DealDate     <= p_EndDate
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
            case when q.IsRepo = 0 AND q.t_DealDate BETWEEN p_BegDate AND p_EndDate AND q.IsRejectDeal = 0 then BROKERREP_PART_DEALINPERIOD 
                 when q.IsRepo = 1 AND q.t_DealDate BETWEEN p_BegDate AND p_EndDate AND q.IsRejectDeal = 0 then BROKERREP_PART_REPOINPERIOD 
                 when q.IsRepo = 0 AND q.t_DealDate < p_BegDate AND (q.FactExecDate = TO_DATE('01.01.0001','DD.MM.YYYY') OR q.FactExecDate >= p_BegDate) AND q.IsRejectDeal = 0 then BROKERREP_PART_EXECDEAL 
                 when q.IsRepo = 1 AND q.t_DealDate < p_BegDate AND (q.FactExecDate = TO_DATE('01.01.0001','DD.MM.YYYY') OR q.FactExecDate >= p_BegDate) AND q.IsRejectDeal = 0 then BROKERREP_PART_EXECREPO
                 when q.IsRepo = 0 AND q.t_DealDate BETWEEN p_BegDate AND p_EndDate AND q.IsRejectDeal <> 0 then BROKERREP_PART_CANCELDEAL 
                 when q.IsRepo = 1 AND q.t_DealDate BETWEEN p_BegDate AND p_EndDate AND q.IsRejectDeal <> 0 then BROKERREP_PART_CANCELREPO
                 else 0
            end as Part,
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
  /*A08*/   DECODE(q.t_Flag1, 'X', q.t_DealCodeTS, q.t_DealCode),
  /*A09*/   CASE WHEN q.t_ServKindSub = 9 THEN 'Внебиржевой рынок' ELSE uGetPatyNameForBrkRep(q.t_MarketID) END,
  /*A10*/   (CASE WHEN q.IsRepo = 1 THEN DECODE(q.IsBuy, 1, 'Обратное РЕПО', 'Прямое РЕПО') || ' ' || TO_CHAR(q.t_DealPart) || ' часть'
                  ELSE DECODE(q.IsBuy, 1, 'Покупка', 'Продажа') END
            ),
  /*A11*/   NVL((SELECT pt.t_ShortName
                   FROM dparty_dbt pt
                  WHERE pt.t_PartyID = q.t_Issuer), CHR(1)),
  /*A12*/   (CASE WHEN q.t_ISIN <> CHR(1) THEN q.t_ISIN ELSE q.t_LSIN END),
  /*A13*/   GetPrice(q.t_DealID, q.t_DealPart),
  /*A13_i*/ (CASE WHEN q.IsRepo = 1 THEN q.t_IncomeRate
                  ELSE NULL END
            ),
  /*A14*/   NVL((SELECT f.t_CCY FROM dfininstr_dbt f WHERE f.t_FIID = q.t_CFI), CHR(1)),
  /*A14_C*/ q.t_CFI,
  /*A15*/   q.t_Amount,
  /*A16*/   q.t_TotalCost,
  /*A17*/   RSI_RSB_FIInstr.ConvSum(q.t_TotalCost, q.t_CFI, RSI_RSB_FIInstr.NATCUR, CASE WHEN q.t_State = RSI_DLRQ.DLRQ_STATE_EXEC THEN GetPlanExecDate(q.t_BOfficeKind, q.t_DealID, q.t_DealPart) ELSE p_EndDate END),
  /*A18*/   GetBrokerComissSum(q.t_BOfficeKind, q.t_DealID, q.t_DealPart, q.t_CFI),
  /*A19*/   NVL((SELECT f.t_CCY FROM dfininstr_dbt f WHERE f.t_FIID = GetBrokerComissFIID(q.t_BOfficeKind, q.t_DealID, q.t_DealPart)), CHR(1)),
  /*A19_C*/ GetBrokerComissFIID(q.t_BOfficeKind, q.t_DealID, q.t_DealPart),
  /*A20*/   GetMarketComissSum(q.t_BOfficeKind, q.t_DealID, q.t_DealPart, q.t_MarketID, q.t_DealDate),
  /*A21*/   0,
  /*A22*/   0,
  /*A23*/   (CASE WHEN q.t_Flag1 = 'X' AND q.IsOTC = 0 THEN 'биржевая' ELSE 'внебиржевая' END),
  /*A24*/   NVL((SELECT pt.t_ShortName
                   FROM dparty_dbt pt
                  WHERE pt.t_PartyID = q.t_PartyID), CHR(1)),
  /*A25*/   NVL((SELECT avrkind.t_Name
                   FROM dfininstr_dbt fin, davrkinds_dbt avrkind 
                  WHERE fin.t_fiid = q.t_FIID  
                    AND avrkind.t_AvoirKind = fin.t_AvoirKind  
                    AND avrkind.t_FI_KIND   = fin.t_FI_Kind), CHR(1)) as AvrkName, /*будем писать сюда вид ЦБ вместо кода контрагента*/
  /*A26*/   case Rsb_Secur.IsBasket( Rsb_Secur.get_OperationGroup( Rsb_Secur.get_OperSysTypes( q.t_DealType, q.t_BOfficeKind ) ) )  WHEN 1 THEN null
                else q.t_NKD end,
  /*A27*/   RSI_RSB_FIInstr.ConvSum(q.t_NKD, q.t_FaceValueFI, RSI_RSB_FIInstr.NATCUR, q.t_DealDate),
  /*A28*/   case q.t_LegKind when 2 then ( case IsSale when 1 then q.t_TotalCost else 0 end) else (case IsBuy when 1 then q.t_TotalCost else 0 end) end,
  /*A29*/   case q.t_LegKind when 2 then ( case IsBuy when 1 then q.t_Amount else 0 end) else (case IsSale when 1 then q.t_Amount else 0 end) end,
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
  /*A95*/   case when q.IsMarginCall = 0 and (q.IsRepo = 0 or q.t_DealPart <> 2) then GetReqPrice(q.t_DealID) else null end, --Не заполняем по второй части РЕПО, т.к. в заявке второй цены нет
  /*A95_M*/ case when q.IsMarginCall = 1 then 'Принудительное закрытие позиций' else CHR(1) end,
  /*ServKind*/ CASE WHEN p_IsEDP <> 0 AND q.t_ServKindSub <> 9 /*ЕДП Кроме внебирж. рынка*/ THEN 0 ELSE q.t_ServKind END,
  /*ServKindSub*/ CASE WHEN p_IsEDP <> 0 AND q.t_ServKindSub <> 9 THEN 0 ELSE q.t_ServKindSub END,
  /*MarketID*/CASE WHEN p_IsEDP <> 0 AND q.t_ServKindSub <> 9 THEN 0 ELSE q.t_MarketID END
  BULK COLLECT INTO v_brkrep
  FROM q
 WHERE q.t_DealDate BETWEEN p_BegDate AND p_EndDate
    OR q.FactExecDate = TO_DATE('01.01.0001','DD.MM.YYYY') OR q.FactExecDate >= p_BegDate;

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
                                    WHERE rep.t_Part > 0
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
                  q.t_Part,
                  0,
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
                     t_Direction, t_Part, t_MarketID, t_ServKind, t_ServKindSub
                FROM dbrkrepdeal_tmp
               WHERE t_Part > 0
                 AND t_IsItog = CHR(0)
                 AND (   t_A14 = one_curr.t_CCY
                      OR t_A19 = one_curr.t_CCY
                      OR (one_curr.t_FIID = RSI_RSB_FIInstr.NATCUR AND (t_A20 <> 0 OR t_A21 <> 0 OR t_A22 <> 0 OR t_A27 <> 0))
                     )
              GROUP BY t_MarketID, t_ServKind, t_ServKindSub, t_Direction, t_Part
             ) q;

      IF v_brkrep.COUNT > 0 THEN
         FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
            INSERT INTO dbrkrepdeal_tmp
                 VALUES v_brkrep (indx);
      END IF;

    END LOOP;
END CreateDealData;

/**
 @brief Получить наименование инструмента по сделке валютного рынка
 @param[in]  p_DealDate   дата заключения  
 @param[in]  p_ExDealDate дата заполнения сделки 
 @param[in]  p_IsSwap     признак свопа 
 @param[in]  p_DealPart   часть сделки 
 @param[in]  p_BaseCCY    ISO-код валюты базового актива 
 @param[in]  p_ContrCCY   ISO-код валюты контрактива 
 @return строковое наименование инструмента по сделке
*/                                                                  
FUNCTION GetInstrName(p_DealDate IN DATE, p_ExDealDate IN DATE, p_IsSwap IN NUMBER, p_DealPart IN NUMBER, p_BaseCCY IN VARCHAR2, p_ContrCCY IN VARCHAR2) RETURN VARCHAR2
AS
  v_Instrument VARCHAR2(30);
BEGIN
  v_Instrument := p_BaseCCY || p_ContrCCY;
  IF p_IsSwap = 1 THEN
    v_Instrument := v_Instrument || '_Спец. SWAP';
    IF p_DealPart = 1 THEN
      v_Instrument := v_Instrument || ' часть 1';
    ELSE
      v_Instrument := v_Instrument || ' часть 2';
    END IF;
  ELSE
    IF p_DealDate = p_ExDealDate THEN
      v_Instrument := v_Instrument || '_TOD';
    ELSE
      v_Instrument := v_Instrument || '_TOM';
    END IF;
  END IF;

  RETURN v_Instrument; 
END GetInstrName;

/**
 @brief Получить сумму комиссии брокера по сделке валютного рынка
 @param[in]  p_DealID  идентификатор сделки 
 @param[in]  p_DocKind вид документа 
 @param[in]  p_ToFIID  целевая валюта комиссии 
 @return сумма комиссии в заданной валюте
*/                                           
FUNCTION GetBrokerComissSumCurMarket(p_DealID IN NUMBER, p_DocKind IN NUMBER, p_ToFIID IN NUMBER) RETURN NUMBER
AS
  v_Sum NUMBER := 0;
BEGIN
  select SUM(RSI_RSB_FIInstr.ConvSum(dlc.t_sum, sfc.t_fiid_comm, p_ToFIID, dlc.t_date, 0))
    into v_Sum 
    from ddlcomis_dbt dlc, dsfcomiss_dbt sfc 
   where dlc.t_docid      = p_DealID
     and dlc.t_dockind    = p_DocKind
     and dlc.t_feetype    = sfc.t_feetype 
     and dlc.t_comnumber  = sfc.t_number  
     and sfc.t_ReceiverID = RsbSessionData.OurBank
     and LOWER(sfc.t_Code) not IN (LOWER('БрокерКомпенсацВ_RUR'), LOWER('БрокерКомпенсацВ_USD'), LOWER('БрокерКомпенсацВ_EUR'), LOWER('БрокерКомпенсацВ_CHF'));
  
  RETURN v_Sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;
END GetBrokerComissSumCurMarket;

/**
 @brief Формирование данных по сделкам валютного рынка
 @param[in]  p_DlContrID  идентификатор ДБО 
 @param[in]  p_BegDate    начало периода 
 @param[in]  p_EndDate    конец периода 
 @param[in]  p_IsEDP      признак счета ЕДП 
*/                      
PROCEDURE CreateCurDealData(p_DlContrID IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE, p_IsEDP IN NUMBER)
IS
  TYPE brkrep_t IS TABLE OF DBRKREPCURDEAL_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;
BEGIN
  WITH q_sf AS (SELECT sf.t_ID, sf.t_PartyID, sf.t_ServKind, sf.t_ServKindSub
                FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf
               WHERE mp.t_DlContrID = p_DlContrID
                 AND sf.t_ID = mp.t_SfContrID
             ),
     q as (SELECT dvn.t_id, dvn.t_dockind, dvn.t_dvkind, 
                  case when dvn.t_dvkind = 7 then 0 else (case when dvnfi.t_type = 0 then 1 else 2 end) end as DealPart, 
                  case when dvn.t_dvkind = 7 then 0 else 1 end as IsSwap, 
                  rsb_brkrep_rshb.GetSfPlanID(q_sf.t_ID, dvn.t_date) as PlanID, 
                  dvn.t_date, dvn.t_time, dvnfi.t_execdate, dvn.t_contractor,
                  dvn.t_code, dvn.t_sector, dvn.t_extcode, dvn.t_MarketID, dvnfi.t_type,
                  fin.t_fiid as BaseFiid, fin.t_ccy as BaseCurr, 
                  price.t_fiid as ContrFiid, price.t_ccy as ContrCurr,
                  case when dvn.t_type = 1 then 'Покупка'
                       when dvn.t_type = 2 then 'Продажа'
                       when dvn.t_type = 5 and dvnfi.t_type = 2 then 'Продажа' 
                       when dvn.t_type = 6 and dvnfi.t_type = 0 then 'Продажа' 
                       when dvn.t_type = 5 and dvnfi.t_type = 0 then 'Покупка' 
                       when dvn.t_type = 6 and dvnfi.t_type = 2 then 'Покупка' 
                       else ''
                   end as DealKind, 
                  case when RSB_SECUR.GetMainObjAttr(RSB_SECUR.GetDvnObjType(dvn.t_dockind), LPAD(dvn.t_id, 34, '0'), 116, dvn.t_Date) = 1 then 1 else 0 end as IsMarginCall,
                  q_sf.t_PartyID as ClientID, q_sf.t_ID as SfContrID, q_sf.t_ServKind, q_sf.t_ServKindSub,
                  dvnfi.t_amount, dvnfi.t_price, dvnfi.t_cost, 
                  case when exists (select 1 
                                      from doproper_dbt op, doprstep_dbt st 
                                     where op.t_kind_operation = dvn.t_kind
                                       and op.t_documentid = lpad(dvn.t_id, 34, '0') 
                                       and op.t_id_operation = st.t_id_operation 
                                       and st.t_isexecute != 'X' 
                                       and ((dvn.t_dvkind = 7 and st.t_symbol = 'Б') or
                                            (dvn.t_dvkind <> 7 and dvnfi.t_type = 0 and st.t_symbol = 'о') or
                                            (dvn.t_dvkind <> 7 and dvnfi.t_type <> 0 and st.t_symbol = 'О'))
                                   ) then 1
                       else 0 
                   end as IsNotExecLiability
             FROM ddvndeal_dbt dvn, ddvnfi_dbt dvnfi, q_sf, dfininstr_dbt fin, dfininstr_dbt price
            WHERE dvn.t_client = q_sf.t_PartyID
              and dvn.t_clientcontr = q_sf.t_ID
              and dvn.t_dvkind in (6, 7) 
              and dvn.t_state in (1, 2) 
              and dvnfi.t_dealid = dvn.t_id 
              and dvnfi.t_fiid = fin.t_fiid 
              and fin.t_fi_kind = 1 
              and dvnfi.t_pricefiid = price.t_fiid
              and dvn.t_sector = 'X'
              and dvn.t_date <= p_EndDate 
              and not exists (select 1 
                                from dpmpaym_dbt paym 
                               where paym.t_documentid = dvn.t_id 
                                 and paym.t_dockind = dvn.t_dockind 
                                 and paym.t_valuedate < p_BegDate 
                                 and paym.t_purpose in (Rsb_Payment.BAi, Rsb_Payment.CAi, Rsb_Payment.BRi, Rsb_Payment.CRi)
                                 and paym.t_paymstatus in (Rsb_Payment.PM_FINISHED, Rsb_Payment.PM_CLOSED_W_M_MOVEMENT) 
                              )
          )
  SELECT q.t_id as DealID,
         q.ClientID,
         q.SfContrID,
         case when q.t_date between p_BegDate and p_EndDate then BROKERREP_PART_DEALINPERIOD 
              when q.t_date < p_BegDate then BROKERREP_PART_EXECDEAL
              else 0
          end as Part,
         q.PlanID,
         CHR(0) as IsItog,
         to_char(q.t_date, 'dd.mm.yyyy') || ' ' || to_char(q.t_time, 'hh24:mi:ss') as ConcDate, /*A05: Дата и время заключения сделки*/
         q.t_execdate, /*A06: Дата исполнения сделки*/
         case when q.t_sector = 'X' then q.t_extcode else q.t_code end as CodeTS, /*A08: Номер в ТС*/
         case when q.t_sector = 'X' then uGetPatyNameForBrkRep(q.t_MarketID) else CHR(1) end as MarketPlace, /*A09: Торговая площадка*/
         GetInstrName(q.t_date, q.t_execdate, q.IsSwap, q.DealPart, q.BaseCurr, q.ContrCurr) as Instrument, /*A10: Инструмент*/
         q.DealKind, /*A11: Вид сделки*/
         q.t_Amount as Volume, /*A12: Объем сделки*/
         case when q.IsMarginCall = 1 or q.DealPart = 2 then null --Не заполняем по второй части свопов, т.к. в заявке второй цены нет
              else nvl((select req.t_price 
                          from dspground_dbt ground, dspgrdoc_dbt dealdoc, dspgrdoc_dbt reqdoc, ddl_req_dbt req 
                         where dealdoc.t_sourcedocid = q.t_id 
                           and dealdoc.t_sourcedockind = q.t_dockind                             
                           and ground.t_spgroundid = dealdoc.t_spgroundid 
                           and ground.t_spgroundid = reqdoc.t_spgroundid 
                           and dealdoc.t_sourcedocid != reqdoc.t_sourcedocid 
                           and dealdoc.t_sourcedockind != reqdoc.t_sourcedockind 
                           and reqdoc.t_sourcedocid = req.t_id 
                           and req.t_sourcekind = dealdoc.t_sourcedockind 
                           and req.t_client = q.ClientID 
                           and reqdoc.t_sourcedockind = req.t_kind    
                           and rownum = 1), null)
          end as ReqPrice, /*A43: Цена заявки*/
         q.t_Price, /*A13: Цена сделки*/
         q.BaseCurr, /*A14: Валюта сделки*/
         q.ContrCurr, /*A15: Валюта расчетов*/
         q.t_Amount as BaseSum, /*A16: Сумма в валюте сделки*/
         q.t_Cost as ContrSum, /*A17: Сумма в валюте расчетов*/
         case when q.BaseFiid = PM_COMMON.NATCUR then q.t_Amount 
              else RSI_RSB_FIInstr.ConvSum(q.t_Amount, q.BaseFiid, PM_COMMON.NATCUR, q.t_Date, 0) 
          end as ContrSumRub, /*A18: Сумма расчетов в рублях (по курсу ЦБ на дату заключения)*/ 
         GetBrokerComissSumCurMarket(q.t_id, q.t_dockind, q.ContrFiid) as BrokerComissContr, /*Комиссия брокера в валюте расчетов*/ 
         GetBrokerComissSumCurMarket(q.t_id, q.t_dockind, PM_COMMON.NATCUR) as BrokerComissRub, /*Комиссия брокера в рублях*/ 
         GetMarketComissSum(q.t_dockind, q.t_id, case when q.t_dvkind = 7 then 1 else (case when q.t_type = 0 then 1 else 2 end) end, q.t_MarketID, q.t_Date) as MarketComiss, /*A21: Комиссия, компенсирующая биржевые сборы, в рублях*/
         case when q.t_sector = 'X' then 'Безадресная' else 'Адресная' end as DealType, /*A24: Тип сделки*/
         case when q.t_contractor > 0 then nvl((select t_shortname from dparty_dbt pt where pt.t_partyid = q.t_contractor), CHR(1)) else CHR(1) end as Contractor, /*A25: Контрагент*/
         case when q.IsNotExecLiability = 1 and q.DealKind = 'Покупка' then q.t_Cost else 0 end as LiabilityBase, /*Обязательства в валюте сделки*/
         case when q.IsNotExecLiability = 1 and q.DealKind <> 'Покупка' then q.t_Amount else 0 end as LiabilityContr, /*Обязательства в валюте расчетов*/
         case when q.IsMarginCall = 1 then 'Принудительное закрытие позиций' else CHR(1) end as Note, /*Примечание*/
         CASE WHEN p_IsEDP <> 0 AND q.t_ServKindSub <> 9 /*ЕДП Кроме внебирж. рынка*/ THEN 0 ELSE q.t_ServKind END,
         CASE WHEN p_IsEDP <> 0 AND q.t_ServKindSub <> 9 THEN 0 ELSE q.t_ServKindSub END,
         CASE WHEN p_IsEDP <> 0 AND q.t_ServKindSub <> 9 THEN 0 ELSE q.t_MarketID END
    BULK COLLECT INTO v_brkrep
    FROM q;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepcurdeal_tmp
             VALUES v_brkrep (indx);
  END IF;
  
  --Создать итоговые строки
  SELECT 0,
         -1,
         -1,
         t_Part,
         0,
         'X',
         'Итого в RUB', /*A05: Дата и время заключения сделки*/
         TO_DATE('01.01.0001','DD.MM.YYYY'), /*A06: Дата исполнения сделки*/
         CHR(1), /*A08: Номер в ТС*/
         CHR(1), /*A09: Торговая площадка*/
         CHR(1), /*A10: Инструмент*/
         CHR(1), /*A11: Вид сделки*/
         0, /*A12: Объем сделки*/
         0, /*A43: Цена заявки*/
         0, /*A13: Цена сделки*/
         CHR(1), /*A14: Валюта сделки*/
         CHR(1), /*A15: Валюта расчетов*/
         0, /*A16: Сумма в валюте сделки*/
         0, /*A17: Сумма в валюте расчетов*/
         SUM(t_ContrSumRub), /*A18: Сумма расчетов в рублях (по курсу ЦБ на дату заключения)*/ 
         0, /*Комиссия брокера в валюте расчетов*/ 
         SUM(t_BrokerComissRub), /*Комиссия брокера в рублях*/ 
         SUM(t_MarketComiss), /*A21: Комиссия, компенсирующая биржевые сборы, в рублях*/
         CHR(1), /*A24: Тип сделки*/
         CHR(1), /*A25: Контрагент*/
         0, /*Обязательства в валюте сделки*/
         0, /*Обязательства в валюте расчетов*/
         CHR(1), /*Примечание*/
         t_ServKind,
         t_ServKindSub,
         t_MarketID
    BULK COLLECT INTO v_brkrep
    FROM dbrkrepcurdeal_tmp
   WHERE t_IsItog = CHR(0)
   GROUP BY t_MarketID, t_ServKind, t_ServKindSub, t_Part;

    IF v_brkrep.COUNT > 0 THEN
       FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
          INSERT INTO dbrkrepcurdeal_tmp
               VALUES v_brkrep (indx);
    END IF;

END CreateCurDealData;

/**
 @brief Получить сумму комиссии брокера по сделке срочного рынка
 @param[in]  p_DealID  идентификатор сделки 
 @param[in]  p_Date    дата курса 
 @param[in]  p_ToFIID  целевая валюта комиссии 
 @return сумма комиссии в заданной валюте
*/                      
FUNCTION GetBrokerComissSumDvMarket(p_DealID IN NUMBER, p_Date IN DATE, p_ToFIID IN NUMBER) RETURN NUMBER
AS
  v_Sum NUMBER := 0;
BEGIN
  select SUM(RSI_RSB_FIInstr.ConvSum(dlc.t_sum, sfc.t_fiid_comm, p_ToFIID, p_Date, 0))
    into v_Sum 
    from ddvdlcom_dbt dlc, dsfcomiss_dbt sfc 
   where dlc.t_DealID     = p_DealID
     and dlc.t_ComissID   = sfc.t_ComissID  
     and sfc.t_ReceiverID = RsbSessionData.OurBank
     and LOWER(sfc.t_Code) not IN (LOWER('БрокерКомпенсацС'));
  
  RETURN v_Sum;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;
END GetBrokerComissSumDvMarket;

/**
 @brief Формирование данных по сделкам срочного рынка
 @param[in]  p_DlContrID  идентификатор ДБО 
 @param[in]  p_BegDate    начало периода 
 @param[in]  p_EndDate    конец периода 
 @param[in]  p_IsEDP      признак счета ЕДП 
*/                             
PROCEDURE CreateDvDealData(p_DlContrID IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE, p_IsEDP IN NUMBER)
IS
  TYPE brkrep_t IS TABLE OF DBRKREPDVDEAL_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;
BEGIN
  WITH q_sf AS (SELECT sf.t_ID, sf.t_PartyID, sf.t_ServKind, sf.t_ServKindSub
                FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf
               WHERE mp.t_DlContrID = p_DlContrID
                 AND sf.t_ID = mp.t_SfContrID
             ),
     q as (SELECT dv.t_id, dv.t_extcode, dv.t_Date, dv.t_Time, dv.t_type, dv.t_date_clr, 
                  RSB_BRKREP_RSHB_NEW.GetSfPlanID(q_sf.t_ID, dv.t_date) as PlanID,                                                                                         
                  (SELECT AvrKinds.t_name FROM davrkinds_dbt AvrKinds WHERE AvrKinds.t_fi_kind = fin.t_fi_kind and AvrKinds.t_avoirkind = fin.t_avoirkind) as FIKind, 
                  deriv.t_OptionType, fin.t_Name as FIName, fin.t_Issuer as MarketID, fin.t_ParentFI, fin.t_AvoirKind,
                  q_sf.t_PartyID as ClientID, q_sf.t_ID as SfContrID, q_sf.t_ServKind, q_sf.t_ServKindSub,
                  nvl(dv.t_price, 0) as Price, nvl(dv.t_bonus, 0) as Bonus, dv.t_Amount, 
                  case when fin.t_avoirkind = 1 then nvl(dv.t_positioncost_rur, 0) else nvl(dv.t_positionbonus, 0) end as DealSum,
                  nvl((SELECT turn.t_Margin 
                         FROM ddvdlturn_dbt turn 
                        WHERE turn.t_DealID = dv.t_id
                          AND turn.t_Date = (SELECT max(t_Date) FROM ddvdlturn_dbt WHERE t_DealID = dv.t_id AND t_Date <= p_EndDate) 
                      ), 0) as Margin, 
                  case when RSB_SECUR.GetMainObjAttr(Rsb_Secur.OBJTYPE_OPER_DV, LPAD(dv.t_id, 34, '0'), 116, dv.t_Date) = 1 then 1 else 0 end as IsMarginCall,
                  case when dv.t_type = 'E' then (SELECT d.t_name FROM doprkoper_dbt d WHERE d.t_kind_operation = dv.t_kind) end as OperationExec				  
           FROM ddvdeal_dbt dv, dfininstr_dbt fin, dfideriv_dbt deriv, q_sf                                                                                              
            WHERE dv.t_client      = q_sf.t_PartyID                                                                                                                             
              and dv.t_clientcontr = q_sf.t_ID                                                                                                                             
              and dv.t_state in (1, 2)                                                                                                                             
              and fin.t_fiid = dv.t_fiid                                                                                                                           
              and deriv.t_fiid = fin.t_fiid                                                                                                                        
              and dv.t_type IN ('B','S','E') 
              and dv.t_date <= p_EndDate
              and dv.t_date_clr >= p_BegDate
          )
  SELECT q.t_id as DealID,
         q.ClientID,
         q.SfContrID,
         case when q.t_date between p_BegDate and p_EndDate then BROKERREP_PART_DEALINPERIOD 
              when q.t_date < p_BegDate then BROKERREP_PART_EXECDEAL 
              else 0
          end as Part,
         q.PlanID,
         CHR(0) as IsItog,
         q.t_extcode as Code, /*C01: № сделки*/
         to_char(q.t_date, 'dd.mm.yyyy') || ' ' || to_char(q.t_time, 'hh24:mi:ss') as ConcDate,	/*C02: Дата и время заключения сделки*/
         q.FIKind || decode(q.t_OptionType, 0, '', 1, ' - Put', ' - Call') as FIKind, /*C03: Вид контракта*/ 
         q.FIName, /*C04: Контракт*/
         case when q.t_type = 'B' then 'Покупка' else 'Продажа' end as DealKind, /*C05: Вид сделки*/                                                                             
         uGetPatyNameForBrkRep(q.MarketID) as MarketPlace, /*C06: Место заключения сделки*/
         q.t_date_clr as ClrDate, /*C07: Дата расчётов по сделке*/                                                                                                                        
         case when q.IsMarginCall = 1 then null 
              else nvl((select req.t_price 
                          from dspground_dbt ground, dspgrdoc_dbt dealdoc, dspgrdoc_dbt reqdoc, ddl_req_dbt req 
                         where dealdoc.t_sourcedocid = q.t_id 
                           and dealdoc.t_sourcedockind = 192                             
                           and ground.t_spgroundid = dealdoc.t_spgroundid 
                           and ground.t_spgroundid = reqdoc.t_spgroundid 
                           and dealdoc.t_sourcedocid != reqdoc.t_sourcedocid 
                           and dealdoc.t_sourcedockind != reqdoc.t_sourcedockind 
                           and reqdoc.t_sourcedocid = req.t_id 
                           and reqdoc.t_sourcedockind = req.t_kind 
                           and req.t_client = q.ClientID    
                           and rownum = 1), null)    
          end as ReqPrice, /*C08: Цена заявки*/
         case when q.t_AvoirKind = 1 then q.Price else q.Bonus end as PriceFuturesOrBonus, /*C09: Цена одного фьючерсного контракта (размер премии по опциону)*/                                                       
         case when q.t_AvoirKind = 2 then q.Price else null end as PriceOption, /*C10: Цена исполнения по опциону*/                                                                        
         q.t_Amount, /*C11: Кол-во*/
         case when q.t_ParentFI = PM_COMMON.NATCUR then q.DealSum 
              else RSI_RSB_FIInstr.ConvSum(q.DealSum, q.t_ParentFI, PM_COMMON.NATCUR, q.t_date_clr, 0) 
          end as DealSumRub, /*C12: Сумма сделки, руб.*/                                                                                                                              
         case when q.t_ParentFI = PM_COMMON.NATCUR then q.Margin 
              else RSI_RSB_FIInstr.ConvSum(q.Margin, q.t_ParentFI, PM_COMMON.NATCUR, q.t_date_clr, 0) 
          end as MarginRub, /*C13: Вариационная маржа, руб.*/                                                                                                                              
         DV_GetMarketComissSum(q.t_id, q.MarketID, q.t_date_clr) as MarketComiss, /*C14: Комиссия, компенсирующая биржевые сборы, руб.*/
         GetBrokerComissSumDvMarket(q.t_id, q.t_date_clr, PM_COMMON.NATCUR) as BrokerComissRub, /*C15: Комиссия брокера, руб*/       
         case when q.IsMarginCall = 1 then 'Принудительное закрытие позиций' 
		      when q.t_type = 'E' then q.OperationExec
		      else CHR(1) end as Note, /*Примечание*/
         CASE WHEN p_IsEDP <> 0 AND q.t_ServKindSub <> 9 /*ЕДП Кроме внебирж. рынка*/ THEN 0 ELSE q.t_ServKind END,
         CASE WHEN p_IsEDP <> 0 AND q.t_ServKindSub <> 9 THEN 0 ELSE q.t_ServKindSub END,
         CASE WHEN p_IsEDP <> 0 AND q.t_ServKindSub <> 9 THEN 0 ELSE q.MarketID END
    BULK COLLECT INTO v_brkrep
    FROM q;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepdvdeal_tmp
             VALUES v_brkrep (indx);
  END IF;
  
  --Создать итоговые строки
  SELECT 0,
         -1,
         -1,
         t_Part,
         0,
         'X',
         CHR(1), /*C01: № сделки*/
         'Итого в RUB', /*C02: Дата и время заключения сделки*/
         CHR(1), /*C03: Вид контракта*/ 
         CHR(1), /*C04: Контракт*/
         CHR(1), /*C05: Вид сделки*/                                                                             
         CHR(1), /*C06: Место заключения сделки*/
         TO_DATE('01.01.0001','DD.MM.YYYY'), /*C07: Дата расчётов по сделке*/                                                                                                                        
         0, /*C08: Цена заявки*/
         0, /*C09: Цена одного фьючерсного контракта (размер премии по опциону)*/                                                       
         0, /*C10: Цена исполнения по опциону*/                                                                        
         0, /*C11: Кол-во*/
         SUM(t_DealSumRub), /*C12: Сумма сделки, руб.*/                                                                                                                              
         SUM(t_MarginRub), /*C13: Вариационная маржа, руб.*/                                                                                                                              
         SUM(t_MarketComiss), /*C14: Комиссия, компенсирующая биржевые сборы, руб.*/
         SUM(t_BrokerComissRub), /*C15: Комиссия брокера, руб*/  
         CHR(1), /*Примечание*/
         t_ServKind,
         t_ServKindSub,
         t_MarketID
    BULK COLLECT INTO v_brkrep
    FROM dbrkrepdvdeal_tmp
   WHERE t_IsItog = CHR(0)
   GROUP BY t_MarketID, t_ServKind, t_ServKindSub, t_Part;

    IF v_brkrep.COUNT > 0 THEN
       FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
          INSERT INTO dbrkrepdvdeal_tmp
               VALUES v_brkrep (indx);
    END IF;

END CreateDvDealData;

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

--Получение плановых остатков по всем ц/б клиента на начало и конец периода
PROCEDURE GetPlanRestData( p_DlContrID     IN NUMBER,
                           p_BegDate       IN DATE,  
                           p_EndDate       IN DATE,
                           p_IsEDP         IN NUMBER 
                         )
IS
  TYPE dbrkrepinacc_t IS TABLE OF DBRKREPINACC_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_dbrkrepinacc dbrkrepinacc_t;
BEGIN
  --Получение плановых остатков по всем ц/б клиента на конец периода
  SELECT   q.t_PartyID,
           0,
           CHR(0),
           -1,
           q.t_FIID,
 /*A53*/   CHR(1),
 /*A54*/   CHR(1),
 /*A55*/   0,
 /*A55_1*/ 0,
 /*A56*/   0,
 /*A56_1*/ SUM(q.t_RequestAmount),
 /*A56_2*/ 0,
 /*A57*/   0,
 /*A57_1*/ SUM(q.t_CommitAmount),
 /*A57_2*/ 0,
 /*A58*/   0,
 /*A58_1*/ 0,
 /*A59*/   0,
 /*A59_1*/ -1,
 /*A60*/   0,
 /*A61*/   0,
 /*A62*/   0,
 /*A63*/   0,
 /*ServKind*/    q.t_ServKind,
 /*ServKindSub*/ q.t_ServKindSub,
 /*MarketID*/    q.t_MarketID
  BULK COLLECT INTO v_dbrkrepinacc
    FROM (SELECT sf.t_PartyID, rq.t_FIID, 
                 CASE WHEN (rq.t_Kind = RSI_DLRQ.DLRQ_KIND_REQUEST AND tk.t_ClientID = sf.t_PartyID) OR (rq.t_Kind = RSI_DLRQ.DLRQ_KIND_COMMIT AND tk.t_PartyID = sf.t_PartyID) THEN rq.t_Amount ELSE 0 END t_RequestAmount,  
                 CASE WHEN (rq.t_Kind = RSI_DLRQ.DLRQ_KIND_COMMIT AND tk.t_ClientID = sf.t_PartyID) OR (rq.t_Kind = RSI_DLRQ.DLRQ_KIND_REQUEST AND tk.t_PartyID = sf.t_PartyID) THEN rq.t_Amount ELSE 0 END t_CommitAmount,
                 (CASE WHEN p_IsEDP <> 0 AND sf.t_ServKindSub <> 9 /*ЕДП Кроме внебирж. рынка*/ THEN 0 ELSE mp.t_MarketID END) t_MarketID, 
                 (CASE WHEN p_IsEDP <> 0 AND sf.t_ServKindSub <> 9 THEN 0 ELSE sf.t_ServKind END) t_ServKind, 
                 (CASE WHEN p_IsEDP <> 0 AND sf.t_ServKindSub <> 9 THEN 0 ELSE sf.t_ServKindSub END) t_ServKindSub  
            FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf, ddl_tick_dbt tk, ddlrq_dbt rq
           WHERE (  (tk.t_ClientID = sf.t_PartyID AND tk.t_ClientContrID = sf.t_ID) 
                 OR (tk.t_IsPartyClient = 'X' AND tk.t_PartyID = sf.t_PartyID AND tk.t_PartyContrID = sf.t_ID)
                 )
             AND tk.t_DealDate <= p_EndDate
             AND mp.t_DlContrID = p_DlContrID 
             AND sf.t_ID = mp.t_SfContrID 
             AND sf.t_ServKind = 1/*PTSK_STOCKDL*/
             AND tk.t_BofficeKind = RSB_SECUR.OBJTYPE_SECDEAL
             AND rq.t_DocKind = tk.t_BOfficeKind
             AND rq.t_DocID = tk.t_DealID
             AND rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
             AND rq.t_State NOT IN (RSI_DLRQ.DLRQ_STATE_UNKNOWN, RSI_DLRQ.DLRQ_STATE_REJECT)
             AND ((rq.t_Kind IN (RSI_DLRQ.DLRQ_KIND_REQUEST, RSI_DLRQ.DLRQ_KIND_COMMIT) AND tk.t_ClientID = sf.t_PartyID) OR (rq.t_Kind IN (RSI_DLRQ.DLRQ_KIND_COMMIT, RSI_DLRQ.DLRQ_KIND_REQUEST) AND tk.t_PartyID = sf.t_PartyID))
             AND (  rq.t_FactDate = TO_DATE('01.01.0001','DD.MM.YYYY')
                 OR NOT EXISTS (SELECT /*+ index(lot DPMWRTSUM_DBT_IDX1)*/ 1
                                     FROM dpmwrtsum_dbt lot
                                    WHERE     lot.t_DocKind in (29, 135)
                                          AND lot.t_DocID = rq.t_ID
                                          AND lot.t_Party = sf.t_PartyID
                                          AND lot.t_State = 1
                                          AND lot.t_Contract = sf.t_ID)
                 OR rq.t_FactDate > p_EndDate
                 )
         ) q
   GROUP BY q.t_PartyID, q.t_FIID, q.t_ServKind, q.t_ServKindSub, q.t_MarketID;

  IF v_dbrkrepinacc.COUNT > 0 THEN
    FORALL indx IN v_dbrkrepinacc.FIRST .. v_dbrkrepinacc.LAST
    INSERT INTO dbrkrepinacc_tmp
    VALUES v_dbrkrepinacc(indx);
  END IF;

  --Получение плановых остатков по всем ц/б клиента на начало периода
  SELECT   q.t_PartyID,
           0,
           CHR(0),
           -1,
           q.t_FIID,
 /*A53*/   CHR(1),
 /*A54*/   CHR(1),
 /*A55*/   0,
 /*A55_1*/ 0,
 /*A56*/   0,
 /*A56_1*/ 0,
 /*A56_2*/ SUM(q.t_RequestAmount),
 /*A57*/   0,
 /*A57_1*/ 0,
 /*A57_2*/ SUM(q.t_CommitAmount),
 /*A58*/   0,
 /*A58_1*/ 0,
 /*A59*/   0,
 /*A59_1*/ -1,
 /*A60*/   0,
 /*A61*/   0,
 /*A62*/   0,
 /*A63*/   0,
 /*ServKind*/    q.t_ServKind,
 /*ServKindSub*/ q.t_ServKindSub,
 /*MarketID*/    q.t_MarketID
  BULK COLLECT INTO v_dbrkrepinacc
    FROM (SELECT sf.t_PartyID, rq.t_FIID, 
                 CASE WHEN (rq.t_Kind = RSI_DLRQ.DLRQ_KIND_REQUEST AND tk.t_ClientID = sf.t_PartyID) OR (rq.t_Kind = RSI_DLRQ.DLRQ_KIND_COMMIT AND tk.t_PartyID = sf.t_PartyID) THEN rq.t_Amount ELSE 0 END t_RequestAmount,  
                 CASE WHEN (rq.t_Kind = RSI_DLRQ.DLRQ_KIND_COMMIT AND tk.t_ClientID = sf.t_PartyID) OR (rq.t_Kind = RSI_DLRQ.DLRQ_KIND_REQUEST AND tk.t_PartyID = sf.t_PartyID) THEN rq.t_Amount ELSE 0 END t_CommitAmount,
                 (CASE WHEN p_IsEDP <> 0 AND sf.t_ServKindSub <> 9 /*ЕДП Кроме внебирж. рынка*/ THEN 0 ELSE mp.t_MarketID END) t_MarketID, 
                 (CASE WHEN p_IsEDP <> 0 AND sf.t_ServKindSub <> 9 THEN 0 ELSE sf.t_ServKind END) t_ServKind, 
                 (CASE WHEN p_IsEDP <> 0 AND sf.t_ServKindSub <> 9 THEN 0 ELSE sf.t_ServKindSub END) t_ServKindSub  
            FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf, ddl_tick_dbt tk, ddlrq_dbt rq
           WHERE (  (tk.t_ClientID = sf.t_PartyID AND tk.t_ClientContrID = sf.t_ID) 
                 OR (tk.t_IsPartyClient = 'X' AND tk.t_PartyID = sf.t_PartyID AND tk.t_PartyContrID = sf.t_ID)
                 )
             AND tk.t_DealDate <= p_BegDate-1
             AND mp.t_DlContrID = p_DlContrID 
             AND sf.t_ID = mp.t_SfContrID 
             AND sf.t_ServKind = 1/*PTSK_STOCKDL*/
             AND tk.t_BofficeKind = RSB_SECUR.OBJTYPE_SECDEAL
             AND rq.t_DocKind = tk.t_BOfficeKind
             AND rq.t_DocID = tk.t_DealID
             AND rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
             AND rq.t_State NOT IN (RSI_DLRQ.DLRQ_STATE_UNKNOWN, RSI_DLRQ.DLRQ_STATE_REJECT)
             AND ((rq.t_Kind IN (RSI_DLRQ.DLRQ_KIND_REQUEST, RSI_DLRQ.DLRQ_KIND_COMMIT) AND tk.t_ClientID = sf.t_PartyID) OR (rq.t_Kind IN (RSI_DLRQ.DLRQ_KIND_COMMIT, RSI_DLRQ.DLRQ_KIND_REQUEST) AND tk.t_PartyID = sf.t_PartyID))
             AND (  rq.t_FactDate = TO_DATE('01.01.0001','DD.MM.YYYY')
                 OR NOT EXISTS (SELECT /*+ index(lot DPMWRTSUM_DBT_IDX1)*/ 1
                                     FROM dpmwrtsum_dbt lot
                                    WHERE     lot.t_DocKind in (29, 135)
                                          AND lot.t_DocID = rq.t_ID
                                          AND lot.t_Party = sf.t_PartyID
                                          AND lot.t_State = 1
                                          AND lot.t_Contract = sf.t_ID)
                 OR rq.t_FactDate > p_BegDate-1
                 )
         ) q
   GROUP BY q.t_PartyID, q.t_FIID, q.t_ServKind, q.t_ServKindSub, q.t_MarketID;

  IF v_dbrkrepinacc.COUNT > 0 THEN
    FORALL indx IN v_dbrkrepinacc.FIRST .. v_dbrkrepinacc.LAST
    INSERT INTO dbrkrepinacc_tmp
    VALUES v_dbrkrepinacc(indx);
  END IF;
  
END GetPlanRestData;

FUNCTION GetActiveRateId(p_FIID IN NUMBER, p_Date IN DATE, p_IsEDP IN NUMBER) RETURN NUMBER deterministic result_cache
IS
  v_CourseTypeMP  NUMBER;
  v_CourseTypeAVR NUMBER;
  p_NDays         NUMBER := -1;
  v_RateId        NUMBER := -1;
BEGIN
  IF p_IsEDP = 0 THEN --Для ЮЛ всегда ограничение в 90 дней
    p_NDays := 90;
  ELSE
    p_NDays := Rsb_Common.GetRegIntValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ГЛУБИНА_ПОИСКА_КОТИРОВОК_ФЛ_ОТЧ', 90);
  END IF;

  v_CourseTypeMP := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА РЫНОЧНАЯ ЦЕНА', 0);
  v_RateId := RSB_SPREPFUN.GetRateIdByMPWithMaxTradeVolume(p_Date, p_FIID, v_CourseTypeMP, p_NDays);
  
  IF v_RateId = -1 THEN
   v_CourseTypeAVR := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0);
   v_RateId := RSB_SPREPFUN.GetRateIdByMPWithMaxTradeVolume(p_Date, p_FIID, v_CourseTypeAVR, p_NDays);
  END IF;

  RETURN v_RateID;
EXCEPTION
  WHEN OTHERS THEN RETURN -1;
END GetActiveRateId;

--Корректировка данных по внутреннему учёту для состава портфеля и активов отчета с учетом плановых движений
PROCEDURE CorrectActiveData( p_BegDate  IN DATE,
                             p_EndDate  IN DATE,
                             p_IsEDP    IN NUMBER
                           )
IS
  TYPE brkrep_t IS TABLE OF DBRKREPACTIVEAVOIR_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
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
  /*A55_1*/ q.t_A55_1,
  /*A56*/   q.t_A56,
  /*A56_1*/ q.t_A56_1,
  /*A56_2*/ q.t_A56_2,
  /*A57*/   q.t_A57,
  /*A57_1*/ q.t_A57_1,
  /*A57_2*/ q.t_A57_2,
  /*A58*/   q.t_A58,
  /*A58_1*/ q.t_A58_1,
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
               NVL(SUM(t_A55+t_A56_2-t_A57_2), 0) AS t_A55_1,
               NVL(SUM(t_A56), 0)   AS t_A56,
               NVL(SUM(t_A56_1), 0) AS t_A56_1,
               NVL(SUM(t_A56_2), 0) AS t_A56_2,
               NVL(SUM(t_A57), 0)   AS t_A57,
               NVL(SUM(t_A57_1), 0) AS t_A57_1,
               NVL(SUM(t_A57_2), 0) AS t_A57_2,
               NVL(SUM(t_A58), 0)   AS t_A58,
               NVL(SUM(t_A58+t_A56_1-t_A57_1), 0) AS t_A58_1,
               GetActiveRateId(t_FIID, p_EndDate, p_IsEDP) t_RateId,
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
                                   ELSE 0 
                              END t_A59,
                              CASE WHEN q1.t_A59_1 != -1 THEN q1.t_A59_1 
                                   ELSE q1.t_fv 
                              END t_A59_1,
                              CASE WHEN q1.t_A59_1 != -1 THEN ABS(q1.t_A58*q1.t_A59) 
                                   ELSE 0 
                              END t_A60,
                              q1.t_A62,
                              q1.t_fv 
                         FROM ( SELECT r.t_A58,
                                       r.t_A59,
                                       r.t_A59_1,
                                       r.t_A62,
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
  /*A55*/   0,
  /*A55_1*/ 0,
  /*A56*/   0,
  /*A56_1*/ 0,
  /*A56_2*/ 0,
  /*A57*/   0,
  /*A57_1*/ 0,
  /*A57_2*/ 0,
  /*A58*/   0,
  /*A58_1*/ 0,
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
  FROM (SELECT NVL(SUM(t_A61), 0)   AS t_A61,
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
  SELECT      
  /*T_NAME           */ q.t_Name,
  /*T_ISINLSIN       */ CASE WHEN q.t_ISIN = chr(1) THEN q.t_LSIN ELSE q.t_ISIN END,
  /*T_FACEVALUEFICCY */ NVL((SELECT t_CCY FROM dfininstr_dbt WHERE t_FIID = q.t_FaceValueFI), CHR(1)),
  /*T_INREST         */ q.t_InRest,
  /*T_INCOURSE       */ q.t_Course1,
  /*T_INCOURSEFICCY  */ NVL((SELECT t_CCY FROM dfininstr_dbt WHERE t_FIID = q.t_CourseFI1), CHR(1)),
  /*T_INNKD          */ CASE WHEN q.t_AvrKind = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN q.t_InRest_NKD ELSE NULL END,
  /*T_INCOSTREST     */ q.t_InRest_1 * q.t_rate_1 + q.t_InRest_NKD * q.t_rateNKD1_1,
  /*T_BUY            */ q.t_Buy,
  /*T_SALE           */ q.t_Sale,
  /*T_OUTREST        */ q.t_OutRest,
  /*T_OUTCOURSE      */ q.t_Course2,
  /*T_OUTCOURSEFICCY */ NVL((SELECT t_CCY FROM dfininstr_dbt WHERE t_FIID = q.t_CourseFI2), CHR(1)),
  /*T_OUTNKD         */ CASE WHEN q.t_AvrKind = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN q.t_OutRest_NKD ELSE NULL END,
  /*T_OUTCOSTREST    */ q.t_OutRest_1 * q.t_rate_2 + q.t_OutRest_NKD * q.t_rateNKD1_2,
  /*T_INPLANREST     */ q.t_InPlanRest,
  /*T_INPLANCOSTREST */ q.t_InPlanRest_1 * q.t_rate_1 + q.t_InPlanRest_NKD * q.t_rateNKD1_1,
  /*T_OUTPLANREST    */ q.t_OutPlanRest,
  /*T_OUTPLANCOSTREST*/ q.t_OutPlanRest_1 * q.t_rate_2 + q.t_OutPlanRest_NKD * q.t_rateNKD1_2,
  /*T_ISITOG         */ CHR(0),
  /*T_SERVKIND       */ q.t_ServKind,
  /*T_SERVKINDSUB    */ q.t_ServKindSub
  BULK COLLECT INTO v_brkrep
    FROM ( SELECT q1.*,
                  q1.t_InRest * q1.t_Course1 as t_InRest_1,
                  NVL(RSI_RSB_FIInstr.FI_CalcNKD(q1.t_FIID, p_BegDate-1, q1.t_InRest, 0), 0) as t_InRest_NKD,
                  q1.t_OutRest * q1.t_Course2 as t_OutRest_1,
                  NVL(RSI_RSB_FIInstr.FI_CalcNKD(q1.t_FIID, p_EndDate, q1.t_OutRest, 0), 0) as t_OutRest_NKD,
                  q1.t_OutPlanRest * q1.t_Course2 as t_OutPlanRest_1,
                  NVL(RSI_RSB_FIInstr.FI_CalcNKD(q1.t_FIID, p_EndDate, q1.t_OutPlanRest, 0), 0) as t_OutPlanRest_NKD,  
                  q1.t_InPlanRest * q1.t_Course1 as t_InPlanRest_1,
                  NVL(RSI_RSB_FIInstr.FI_CalcNKD(q1.t_FIID, p_BegDate-1, q1.t_InPlanRest, 0), 0) as t_InPlanRest_NKD,                 
                  NVL(RSI_RSB_FIInstr.ConvSum(1, (CASE WHEN q1.t_RateId1 != -1 THEN q1.t_CourseFI1 ELSE q1.t_FaceValueFI END), RSI_RSB_FIInstr.NATCUR, p_BegDate-1), 0) t_rate_1,
                  NVL(RSI_RSB_FIInstr.ConvSum(1, (CASE WHEN q1.t_RateId2 != -1 THEN q1.t_CourseFI2 ELSE q1.t_FaceValueFI END), RSI_RSB_FIInstr.NATCUR, p_EndDate),   0) t_rate_2,
                  NVL(RSI_RSB_FIInstr.ConvSum(1, q1.t_FaceValueFI, RSI_RSB_FIInstr.NATCUR, p_BegDate-1), 0) t_rateNKD1_1,
                  NVL(RSI_RSB_FIInstr.ConvSum(1, q1.t_FaceValueFI, RSI_RSB_FIInstr.NATCUR, p_EndDate),   0) t_rateNKD1_2
             FROM (SELECT q2.t_ServKind, q2.t_ServKindSub, 
                          q2.t_FIID, q2.t_RateId1, q2.t_RateId2, q2.t_Buy, q2.t_Sale,
                          q2.t_InRest, q2.t_OutRest, q2.t_OutPlanRest, q2.t_InPlanRest,
                          q2.t_FaceValueFI, q2.t_Name, q2.t_ISIN, q2.t_LSIN, q2.t_AvrKind,
                          RSB_SPREPFUN.GetCourse(q2.t_RateId1, p_BegDate-1) t_Course1,
                          RSB_SPREPFUN.GetCourseFI(q2.t_RateId1) t_CourseFI1,
                          RSB_SPREPFUN.GetCourse(q2.t_RateId2, p_EndDate) t_Course2,
                          RSB_SPREPFUN.GetCourseFI(q2.t_RateId2) t_CourseFI2
                     FROM (SELECT CASE WHEN ia.t_ServKindSub <> 9 THEN 0 ELSE ia.t_ServKind END t_ServKind, 
                                  CASE WHEN ia.t_ServKindSub <> 9 THEN 0 ELSE ia.t_ServKindSub END t_ServKindSub,  
                                  ia.t_FIID,
                                  SUM(ia.t_A55) as t_InRest,
                                  SUM(ia.t_A58) as t_OutRest,
                                  SUM(ia.t_A58_1) as t_OutPlanRest,
                                  SUM(ia.t_A55_1) as t_InPlanRest,
                                  SUM(ia.t_A56) as t_Buy,
                                  SUM(ia.t_A57) as t_Sale,
                                  fin.t_FaceValueFI,
                                  GetActiveRateId(ia.t_FIID, p_BegDate-1, p_IsEDP) t_RateId1,
                                  GetActiveRateId(ia.t_FIID, p_EndDate, p_IsEDP) t_RateId2,
                                  fin.t_Name, avr.t_ISIN, avr.t_LSIN,
                                  RSB_FIInstr.FI_AvrKindsGetRoot(fin.t_FI_Kind, fin.t_AvoirKind) t_AvrKind
                             FROM dbrkrepinacc_tmp ia, dfininstr_dbt fin, davoiriss_dbt avr
                            WHERE fin.t_FIID = ia.t_FIID
                              AND avr.t_FIID = fin.t_FIID
                              AND ia.t_IsItog = CHR(0)
                            GROUP BY CASE WHEN ia.t_ServKindSub <> 9 THEN 0 ELSE ia.t_ServKind END, 
                                     CASE WHEN ia.t_ServKindSub <> 9 THEN 0 ELSE ia.t_ServKindSub END, 
                                     ia.t_FIID, fin.t_FaceValueFI, fin.t_Name, avr.t_ISIN, avr.t_LSIN, fin.t_FI_Kind, fin.t_AvoirKind --Группируем вместе ММВБ и СПБ
                          ) q2
                  ) q1
         ) q;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepactiveavoir_tmp
             VALUES v_brkrep (indx);
  END IF;
  
  --Создать итоговые строки
  SELECT      
  /*T_NAME           */ 'Итого в RUB',
  /*T_ISINLSIN       */ CHR(1),
  /*T_FACEVALUEFICCY */ CHR(1),
  /*T_INREST         */ 0,
  /*T_INCOURSE       */ 0,
  /*T_INCOURSEFICCY  */ CHR(1),
  /*T_INNKD          */ 0,
  /*T_INCOSTREST     */ SUM(t_InCostRest),
  /*T_BUY            */ 0,
  /*T_SALE           */ 0,
  /*T_OUTREST        */ 0,
  /*T_OUTCOURSE      */ 0,
  /*T_OUTCOURSEFICCY */ CHR(1),
  /*T_OUTNKD         */ 0,
  /*T_OUTCOSTREST    */ SUM(t_OutCostRest),
  /*T_INPLANREST     */ 0,
  /*T_INPLANCOSTREST */ SUM(t_InPlanCostRest),
  /*T_OUTPLANREST    */ 0,
  /*T_OUTPLANCOSTREST*/ SUM(t_OutPlanCostRest),
  /*T_ISITOG         */ 'X',
  /*T_SERVKIND       */ t_ServKind,
  /*T_SERVKINDSUB    */ t_ServKindSub
    BULK COLLECT INTO v_brkrep
    FROM dbrkrepactiveavoir_tmp
   WHERE t_IsItog = CHR(0)
   GROUP BY t_ServKind, t_ServKindSub;
       
  IF v_brkrep.COUNT > 0 THEN
    FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
       INSERT INTO dbrkrepactiveavoir_tmp
            VALUES v_brkrep (indx);
  END IF;

END CorrectActiveData;

--Формирование данных по лотам для раздела Оценка позиции по ЦБ и НЕФИ (только фактические данные)
PROCEDURE CreateActiveData( p_DlContrID     IN NUMBER,
                            p_BegDate       IN DATE,
                            p_EndDate       IN DATE,
                            p_IsEDP         IN NUMBER
                          )
IS
  TYPE dbrkrepinacc_t IS TABLE OF DBRKREPINACC_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_dbrkrepinacc dbrkrepinacc_t;
  
  v_CourceType NUMBER;
BEGIN
  v_CourceType := Rsb_Common.GetRegIntValue( 'SECUR\ВИД КУРСА РЫНОЧНАЯ ЦЕНА', 0);

  /*Данные по составу портфеля Фондового рынка*/
  --Здесь только отбор фактических данных
  --После подготовки плановых данных в макросе будет выполняться коррекировка CorrectActiveData
  WITH q_sf AS (SELECT sf.t_ID, sf.t_PartyID, sf.t_ServKind, sf.t_ServKindSub, mp.t_MarketID
                  FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf
                 WHERE mp.t_DlContrID = p_DlContrID
                   AND sf.t_ID = mp.t_SfContrID
               ),
       cat AS (SELECT T_ID FROM dmccateg_dbt WHERE t_LevelType = 1 AND t_Code = 'ЦБ Клиента, ВУ'),
       mcacc AS (SELECT DISTINCT accd.t_Account, accd.t_Chapter, accd.t_Currency, q_sf.t_MarketID, q_sf.t_ServKind, q_sf.t_ServKindSub, q_sf.t_PartyID, q_sf.t_ID as SfContrID
                   FROM cat, q_sf, dmcaccdoc_dbt accd
                  WHERE accd.t_CatID = cat.T_ID
                    AND accd.t_Owner = q_sf.t_PartyID
                    AND accd.t_ClientContrID = q_sf.t_ID
                ),
          q1 AS (SELECT s.*
               FROM (SELECT DISTINCT acc.t_Code_Currency t_FIID,
                                     acc.t_AccountID,
                                     -1*rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_BegDate-1, acc.t_Chapter, null) InRest, 
                                     -1*rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_EndDate, acc.t_Chapter, null) OutRest, 
                                     rsb_account.kreditac(acc.t_Account, acc.t_Chapter, acc.t_Code_Currency, p_BegDate, p_EndDate, null) WrtOffSum,
                                     rsb_account.debetac(acc.t_Account, acc.t_Chapter, acc.t_Code_Currency, p_BegDate, p_EndDate, null) EnrolSum,
                                     (CASE WHEN p_IsEDP <> 0 AND mcacc.t_ServKindSub <> 9 /*ЕДП Кроме внебирж. рынка*/ THEN 0 ELSE mcacc.t_MarketID END) t_MarketID, 
                                     (CASE WHEN p_IsEDP <> 0 AND mcacc.t_ServKindSub <> 9 THEN 0 ELSE mcacc.t_ServKind END) t_ServKind, 
                                     (CASE WHEN p_IsEDP <> 0 AND mcacc.t_ServKindSub <> 9 THEN 0 ELSE mcacc.t_ServKindSub END) t_ServKindSub, 
                                     mcacc.t_PartyID ClientID
                       FROM mcacc, daccount_dbt acc
                      WHERE acc.t_Chapter = mcacc.t_Chapter
                        AND acc.t_Account = mcacc.t_Account
                        AND acc.t_Code_Currency = mcacc.t_Currency
                     ) s
              WHERE (s.InRest != 0 OR s.OutRest != 0 OR s.EnrolSum != 0 OR s.WrtOffSum != 0)
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
 /*A56*/ q.EnrolSum,
 /*A56_1*/ 0,
 /*A56_2*/ 0,
 /*A57*/ q.WrtOffSum,
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
    FROM (SELECT q1.ClientID, q1.t_MarketID, q1.t_ServKind, q1.t_ServKindSub, q1.t_FIID,
                 NVL(SUM(q1.InRest), 0)    as InRest,
                 NVL(SUM(q1.OutRest), 0)   as OutRest,
                 NVL(SUM(q1.EnrolSum), 0)  as EnrolSum,
                 NVL(SUM(q1.WrtOffSum), 0) as WrtOffSum
            FROM q1
          GROUP BY q1.ClientID, q1.t_MarketID, q1.t_ServKind, q1.t_ServKindSub, q1.t_FIID) q, dfininstr_dbt fin, davoiriss_dbt av
   WHERE fin.t_FIID = q.t_FIID
     AND av.t_FIID = fin.t_FIID;

  IF v_dbrkrepinacc.COUNT > 0 THEN
    FORALL indx IN v_dbrkrepinacc.FIRST .. v_dbrkrepinacc.LAST
    INSERT INTO dbrkrepinacc_tmp
    VALUES v_dbrkrepinacc(indx);
  END IF;

  GetPlanRestData(p_DlContrID, p_BegDate, p_EndDate, p_IsEDP); --Подготовка плановых данных
  CorrectActiveData(p_BegDate, p_EndDate, p_IsEDP); --Корректировка оценки позиций по ЦБ
  
END CreateActiveData;

--Формирование данных по ФИССиКО для раздела Оценка позиции по ПФИ
PROCEDURE CreateActiveDerivData( p_DlContrID     IN NUMBER,
                                 p_BegDate       IN DATE,
                                 p_EndDate       IN DATE,
                                 p_IsEDP         IN NUMBER
                               )
IS
  TYPE brkrep_t IS TABLE OF DBRKREPACTIVEDERIV_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;
BEGIN
  WITH q_sf AS (SELECT sf.t_ID, sf.t_PartyID, sf.t_ServKind, sf.t_ServKindSub, mp.t_MarketID
                  FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf
                 WHERE mp.t_DlContrID = p_DlContrID
                   AND sf.t_ID = mp.t_SfContrID
               ),
         q1 AS (SELECT s.*
                  FROM (SELECT outturn.t_FIID as t_FIID,
                               (NVL(inturn.t_longposition, 0) - NVL(inturn.t_shortposition, 0)) as InRest,
                               (outturn.t_longposition - outturn.t_shortposition) as OutRest,
                               (NVL(inturn.t_longpositioncost, 0) - NVL(inturn.t_shortpositioncost, 0)) as InCostRest,
                               (outturn.t_longpositioncost - outturn.t_shortpositioncost) as OutCostRest,
                               NVL(inturn.t_guaranty, 0) as InGuaranty, 
                               outturn.t_guaranty as OutGuaranty,
                               q_sf.t_MarketID, q_sf.t_ServKind, q_sf.t_ServKindSub, q_sf.t_PartyID as ClientID,
                               NVL((SELECT SUM(turn.t_Buy + turn.t_ShortExecution) 
                                      FROM ddvfiturn_dbt turn 
                                     WHERE t_Client = q_sf.t_PartyID
                                       AND t_ClientContr = q_sf.t_ID
                                       AND t_FIID = outturn.t_FIID
                                       AND t_Date BETWEEN p_BegDate AND p_EndDate), 0) as Buy,
                               NVL((SELECT SUM(turn.t_Sale + turn.t_LongExecution) 
                                      FROM ddvfiturn_dbt turn 
                                     WHERE t_Client = q_sf.t_PartyID
                                       AND t_ClientContr = q_sf.t_ID
                                       AND t_FIID = outturn.t_FIID
                                       AND t_Date BETWEEN p_BegDate AND p_EndDate), 0) as Sale
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
                 WHERE (s.InRest != 0 OR s.OutRest != 0 OR s.Buy != 0 OR s.Sale != 0)
               )
  SELECT      
  /*T_FIKIND         */ NVL((SELECT knd.T_NAME FROM davrkinds_dbt knd WHERE knd.T_FI_KIND = fin.T_FI_KIND and knd.T_AVOIRKIND = fin.T_AVOIRKIND),'') || decode(deriv.t_OptionType, 0, '', 1, ' - Put', ' - Call'),     
  /*T_FINAME         */ fin.t_Name,       
  /*T_INREST         */ q.InRest,       
  /*T_INCOSTREST     */ q.InCostRest,   
  /*T_INGUARANTY     */ q.InGuaranty,   
  /*T_BUY            */ q.Buy,     
  /*T_SALE           */ q.Sale,          
  /*T_OUTREST        */ q.OutRest,      
  /*T_OUTCOSTREST    */ q.OutCostRest,  
  /*T_OUTGUARANTY    */ q.OutGuaranty,  
  /*T_INPLANREST     */ q.InRest,   
  /*T_INPLANCOSTREST */ q.InCostRest,
  /*T_OUTPLANREST    */ q.OutRest as PlanRest,   
  /*T_OUTPLANCOSTREST*/ q.OutCostRest as PlanCostRest,
  /*T_ISITOG         */ CHR(0),
  /*T_SERVKIND       */ q.t_ServKind,
  /*T_SERVKINDSUB    */ q.t_ServKindSub
  BULK COLLECT INTO v_brkrep
    FROM (SELECT q1.ClientID, 
                 (CASE WHEN q1.t_ServKindSub <> 9 THEN 0 ELSE q1.t_ServKind END) t_ServKind, 
                 (CASE WHEN q1.t_ServKindSub <> 9 THEN 0 ELSE q1.t_ServKindSub END) t_ServKindSub, 
                 q1.t_FIID,
                 NVL(SUM(q1.InRest), 0) as InRest,       
                 NVL(SUM(q1.InCostRest), 0) as InCostRest,   
                 NVL(SUM(q1.InGuaranty), 0) as InGuaranty,            
                 NVL(SUM(q1.OutRest), 0) as OutRest,      
                 NVL(SUM(q1.OutCostRest), 0) as OutCostRest,  
                 NVL(SUM(q1.OutGuaranty), 0) as OutGuaranty,
                 NVL(SUM(q1.Buy), 0) as Buy,  
                 NVL(SUM(q1.Sale), 0) as Sale
            FROM q1
          GROUP BY q1.ClientID, q1.t_FIID,
                  (CASE WHEN q1.t_ServKindSub <> 9 THEN 0 ELSE q1.t_ServKind END), 
                  (CASE WHEN q1.t_ServKindSub <> 9 THEN 0 ELSE q1.t_ServKindSub END)) q, dfininstr_dbt fin, dfideriv_dbt deriv
   WHERE fin.t_FIID = q.t_FIID
     AND deriv.t_FIID = fin.t_FIID;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepactivederiv_tmp
             VALUES v_brkrep (indx);
  END IF;
  
  --Создать итоговые строки
  SELECT 
  /*T_FIKIND         */ 'Итого в RUB',     
  /*T_FINAME         */ CHR(1),       
  /*T_INREST         */ 0,       
  /*T_INCOSTREST     */ SUM(t_InCostRest),   
  /*T_INGUARANTY     */ SUM(t_InGuaranty),   
  /*T_BUY            */ 0,     
  /*T_SALE           */ 0,          
  /*T_OUTREST        */ 0,      
  /*T_OUTCOSTREST    */ SUM(t_OutCostRest),  
  /*T_OUTGUARANTY    */ SUM(t_OutGuaranty),  
  /*T_INPLANREST     */ 0,   
  /*T_INPLANCOSTREST */ SUM(t_InPlanCostRest),
  /*T_OUTPLANREST    */ 0,   
  /*T_OUTPLANCOSTREST*/ SUM(t_OutPlanCostRest),
  /*T_ISITOG         */ 'X',
  /*T_SERVKIND       */ t_ServKind,
  /*T_SERVKINDSUB    */ t_ServKindSub
    BULK COLLECT INTO v_brkrep
    FROM dbrkrepactivederiv_tmp
   WHERE t_IsItog = CHR(0)
   GROUP BY t_ServKind, t_ServKindSub;
    
  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepactivederiv_tmp
             VALUES v_brkrep (indx);
  END IF;

END CreateActiveDerivData;

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
         (CASE WHEN p_NeedPlanRest <> 0 THEN GetPlanRestAcc(p_DlContrID, q.t_Account, q.t_Chapter, q.t_Code_Currency, p_BegDate-1) ELSE 0 END),
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
           (CASE WHEN p_NeedPlanRest <> 0 THEN GetPlanRestAcc(p_DlContrID, q.t_Account, q.t_Chapter, q.t_Code_Currency, p_BegDate-1) ELSE 0 END),
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
PROCEDURE CreateCasheMoveData( p_BegDate  IN DATE,
                               p_EndDate  IN DATE,
                               p_IsEDP    IN NUMBER
                             )
IS

BEGIN

  INSERT INTO DBRKREPCASHEMOVING_TMP
  WITH acc_data AS (SELECT t_AccountID, t_ServKind, t_ServKindSub, t_MarketID FROM dbrkrepacc_tmp)
  SELECT
 /*T_CURRENCY   */ q.Currency,
  /*T_DATE       */ q.t_Date_Carry,
  /*T_SERVKIND   */ q.t_ServKind,
  /*T_SERVKINDSUB*/ q.t_ServKindSub,
  /*T_ISITOG     */ CHR(0),
  /*T_ACCTRNID   */ q.t_AccTrnID,
  /*T_OPERNAME   */ q.t_Ground,
  /*T_MARKETID   */ CASE WHEN p_IsEDP <> 0 AND q.t_ServKindSub <> 9 THEN 0 ELSE q.t_MarketID END,
  /*T_MARKETNAME */ (CASE WHEN p_IsEDP <> 0 THEN (CASE WHEN q.t_ServKindSub = 9 THEN 'Внебиржевой рынок' ELSE 'Единая денежная позиция (ЕДП)' END)
                          WHEN q.t_MarketID = -1 THEN (CASE WHEN q.t_ServKindSub = 9 THEN 'Внебиржевой рынок' ELSE CHR(1) END)
                          ELSE NVL((select uGetPatyNameForBrkRep(pt.t_PartyID)||' ('||(CASE WHEN q.t_ServKind = 1 THEN 'Фондовый рынок' WHEN q.t_ServKind = 21 THEN 'Валютный рынок' WHEN q.t_ServKind = 15 THEN 'Срочный рынок' ELSE CHR(1) END)||')'
                                      from dparty_dbt pt where pt.t_PartyID = q.t_MarketID), CHR(1))
                     END),
  /*T_INSUM      */ q.InSum,
  /*T_OUTSUM     */ q.OutSum,
  /*T_OUTNDS     */ 0,
  /*T_REST       */ 0,
  /*T_ACCOUNTID  */ q.t_AccountID,
  /*T_ACCOUNT_PAYER     */ t_account_payer,
  /*T_ACCOUNT_RECEIVER  */ t_account_receiver,
  /*T_GROUPTYPE         */ chr(1),
  /*T_ISGROUP           */ chr(1),
  /*T_PRINT_DATE        */ chr(1)
  FROM(  SELECT acctrn.t_FIID_Payer as Currency,
                acctrn.t_Date_Carry,
                acc_data.t_ServKind,
                acc_data.t_ServKindSub,
                acctrn.t_AccTrnID,
                0 as InSum,
                acctrn.t_Sum_Payer as OutSum,
                acc_data.t_AccountID,
                acctrn.t_Ground,
                acc_data.t_MarketID,
                acctrn.t_account_payer,
                acctrn.t_account_receiver
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
                acc_data.t_MarketID,
                acctrn.t_account_payer,
                acctrn.t_account_receiver
           FROM acc_data, dacctrn_dbt acctrn
          WHERE acctrn.t_State = 1
            AND acctrn.t_Chapter = 1
            AND acctrn.t_Date_Carry >= p_BegDate
            AND acctrn.t_Date_Carry <= p_EndDate
            AND acctrn.t_AccountID_Receiver = acc_data.t_AccountID
            AND acctrn.t_Sum_Receiver <> 0
      ) q;

  MERGE INTO DBRKREPCASHEMOVING_TMP w
  USING
  (WITH t as(
   SELECT t.t_acctrnid,
         nvl(d.t_grouptype,t.t_opername) t_grouptype,
         nvl(d.t_isgroup,chr(1)) t_isgroup,
         t.t_date,
         t.t_currency,
         t.t_marketid
   FROM DBRKREPCASHEMOVING_TMP t LEFT JOIN DBRKREPGROUPMOVING_DBT d
                                ON t.t_account_payer    LIKE d.t_deb_acc         || '%'
                               AND t.t_account_receiver LIKE d.t_cr_acc          || '%'
                               AND (upper(d.t_ground)   LIKE upper(t.t_opername) || '%'
                                 OR upper(t.t_opername) LIKE upper(d.t_ground)   || '%')
         ),
     f as (
         SELECT t.*,
                CASE WHEN t.t_isgroup = chr(88)
                    THEN MIN(t.t_date)  over (PARTITION BY t.t_currency, t.t_marketid, t.t_grouptype)
                END t_date_min,
                CASE WHEN t.t_isgroup = chr(88)
                    THEN MAX(t.t_date)  over (PARTITION BY t.t_currency, t.t_marketid, t.t_grouptype)
                END t_date_max,
                CASE WHEN t.t_isgroup <> chr(88)
                    THEN t.t_date
                END t_date_
         FROM t
         )
     SELECT DISTINCT
            f.t_acctrnid,
            f.t_grouptype,
            f.t_isgroup,
            CASE WHEN f.t_isgroup = chr(88) AND f.t_date_min = f.t_date_max
                   THEN to_char(f.t_date_max,'dd.mm.yyyy')
                 WHEN f.t_isgroup = chr(88) AND f.t_date_min <> f.t_date_max
                   THEN to_char(f.t_date_min,'dd.mm.yyyy') || '-' || to_char(f.t_date_max,'dd.mm.yyyy')
                 ELSE to_char(f.t_date_,'dd.mm.yyyy')
            END AS t_printDate
     FROM f
   )r
   ON (r.t_acctrnid = w.t_acctrnid)
    WHEN MATCHED THEN
      UPDATE SET w.t_grouptype  = r.t_grouptype,
                 w.t_isgroup    = r.t_isgroup,
                 w.t_printDate = r.t_printDate;


  IF p_IsEDP = 0 THEN
    --Определить принадлежность проводок операциям

    --Проводки по строкам графика сделок БОЦБ
    UPDATE DBRKREPCASHEMOVING_TMP cs
       SET (t_MarketID, t_MarketName) =
           (SELECT tk.t_MarketID,
                   uGetPatyNameForBrkRep(pt.t_PartyID)||' ('||'Фондовый рынок'||')'
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
    UPDATE DBRKREPCASHEMOVING_TMP cs
       SET (t_MarketID, t_MarketName) =
           (SELECT mp.t_MarketID,
                   uGetPatyNameForBrkRep(pt.t_PartyID)||' ('||(CASE WHEN sf.t_ServKind = 1 THEN 'Фондовый рынок' WHEN sf.t_ServKind = 21 THEN 'Валютный рынок' WHEN sf.t_ServKind = 15 THEN 'Срочный рынок' ELSE CHR(1) END)||')'
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
    UPDATE DBRKREPCASHEMOVING_TMP cs
       SET (t_MarketID, t_MarketName) =
           (SELECT oper.t_Party,
                   uGetPatyNameForBrkRep(pt.t_PartyID)||' ('||'Срочный рынок'||')'
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
    INSERT INTO DBRKREPCASHEMOVING_TMP
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
    /*T_ACCOUNTID  */ t_AccountID,
    /*T_ACCOUNT_PAYER     */ t_Account_Payer,
    /*T_ACCOUNT_RECEIVER  */ t_Account_Receiver,
    /*T_GROUPTYPE         */ t_GroupType,
    /*T_ISGROUP           */ t_IsGroup,
    /*T_PRINT_DATE        */ t_PrintDate
      FROM DBRKREPCASHEMOVING_TMP
     WHERE t_ServKindSub <> 9;

  END IF;

  INSERT INTO DBRKREPCASHEMOVING_TMP
   --Сформировать итоговые строки
  SELECT
  /*T_CURRENCY   */ t_Currency,
  /*T_DATE       */ null,
  /*T_SERVKIND   */ t_ServKind,
  /*T_SERVKINDSUB*/ t_ServKindSub,
  /*T_ISITOG     */ 'X',
  /*T_ACCTRNID   */ 0,
  /*T_OPERNAME   */ CHR(1),
  /*T_MARKETID   */ 0,
  /*T_MARKETNAME */ CHR(1),
  /*T_INSUM      */ SUM(t_InSum),
  /*T_OUTSUM     */ SUM(t_OutSum),
  /*T_OUTNDS     */ SUM(t_OutNDS),
  /*T_REST       */ SUM(t_InSum)-SUM(t_OutSum)-SUM(t_OutNDS),
  /*T_ACCOUNTID  */ 0,
  /*T_ACCOUNT_PAYER     */ 0,
  /*T_ACCOUNT_RECEIVER  */ 0,
  /*T_GROUPTYPE         */ CHR(1),
  /*T_ISGROUP           */ CHR(1),
  /*T_PRINT_DATE        */ CHR(1)
    FROM DBRKREPCASHEMOVING_TMP
   WHERE t_IsItog = CHR(0)
   GROUP BY t_ServKind, t_ServKindSub, t_Currency;

  INSERT INTO DBRKREPCASHEGROUPMOVING_TMP
  SELECT q.t_Currency,
         q.t_date,
         q.t_printDate,
         q.t_grouptype,
         q.t_MarketName,
         q.InSum,
         q.OutSum,
         q.Rest,
         q.t_ServKind,
         q.t_ServKindSub,
         q.t_IsItog
     FROM (SELECT t_Currency,
                    min(t_date) as t_date,
                    t_grouptype,
                    NVL(SUM(t_InSum ), 0) as InSum,
                    NVL(SUM(t_OutSum), 0) as OutSum,
                    NVL(SUM(t_Rest  ), 0) as Rest,
                    t_IsItog,
                    t_MarketName,
                    t_printDate,
                    t_ServKind,
                    t_ServKindSub
               FROM DBRKREPCASHEMOVING_TMP
              WHERE t_isgroup = chr(88)
             GROUP BY t_ServKind,
                      t_ServKindSub,
                      t_Currency,
                      t_MarketName,
                      t_grouptype,
                      t_printDate,
                      t_IsItog,
                      t_isgroup
             UNION ALL
              SELECT t_Currency,
                     t_date,
                     t_grouptype,
                     t_InSum as InSum,
                     t_OutSum as OutSum,
                     t_Rest as Rest,
                     t_IsItog,
                     t_MarketName,
                     t_printDate,
                     t_ServKind,
                     t_ServKindSub
               FROM DBRKREPCASHEMOVING_TMP
              WHERE t_isgroup <> chr(88)
                        ) q
            ORDER BY q.t_Currency ASC, q.t_date ASC, q.t_IsItog ASC, q.t_grouptype ASC, q.t_MarketName ASC;


END CreateCasheMoveData;

--Формирование данных по оценке денежной позиции
PROCEDURE CreateAccMoveData( p_BegDate  IN DATE,
                             p_EndDate  IN DATE,
                             p_IsEDP    IN NUMBER
                           )
IS
  TYPE brkrep_t IS TABLE OF DBRKREPACCITOG_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;
BEGIN
  SELECT
  /*T_CURRENCY   */ q.t_Code_Currency,
  /*T_CCY        */ q.Ccy,
  /*T_ISITOG     */ CHR(0),  
  /*T_SERVKIND   */ q.t_ServKind,
  /*T_SERVKINDSUB*/ q.t_ServKindSub,
  /*T_MARKETID   */ CASE WHEN p_IsEDP <> 0 AND q.t_ServKindSub <> 9 THEN 0 ELSE q.t_MarketID END,
  /*T_MARKETNAME */ (CASE WHEN p_IsEDP <> 0 THEN (CASE WHEN q.t_ServKindSub = 9 THEN 'Внебиржевой рынок' ELSE 'Единая денежная позиция (ЕДП)' END)
                          WHEN q.t_MarketID = -1 THEN (CASE WHEN q.t_ServKindSub = 9 THEN 'Внебиржевой рынок' ELSE CHR(1) END)
                          ELSE NVL((select uGetPatyNameForBrkRep(pt.t_PartyID)||' ('||(CASE WHEN q.t_ServKind = 1 THEN 'Фондовый рынок' WHEN q.t_ServKind = 21 THEN 'Валютный рынок' WHEN q.t_ServKind = 15 THEN 'Срочный рынок' ELSE CHR(1) END)||')'
                                      from dparty_dbt pt where pt.t_PartyID = q.t_MarketID), CHR(1))
                     END),
  /*T_INREST     */ q.SumInRest,
  /*T_OUTREST    */ q.SumOutRest,
  /*T_PLANREST   */ q.SumPlanRest
  BULK COLLECT INTO v_brkrep
  FROM( SELECT acc.t_Code_Currency, 
               (SELECT fin.t_Ccy from dfininstr_dbt fin where fin.t_FIID = acc.t_Code_Currency) Ccy, 
               SUM(repacc.t_InRest) SumInRest, 
               SUM(repacc.t_OutRest) SumOutRest, 
               SUM(repacc.t_OutPlanRest) SumPlanRest,
               repacc.t_ServKind, 
               repacc.t_ServKindSub, 
               repacc.t_MarketID
          FROM dbrkrepacc_tmp repacc, daccount_dbt acc 
         WHERE acc.t_AccountID = repacc.t_AccountID
         GROUP BY repacc.t_ServKind, repacc.t_ServKindSub, repacc.t_MarketID, acc.t_Code_Currency 
      ) q
  WHERE q.SumInRest <> 0 OR q.SumOutRest <> 0 OR q.SumPlanRest <> 0;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepaccitog_tmp
             VALUES v_brkrep (indx);
  END IF;

  IF p_IsEDP = 0 THEN
    --Сформировать итоговые строки
    SELECT
  /*T_CURRENCY   */ t_Currency,
  /*T_CCY        */ 'Итого в '||t_CCY,
  /*T_ISITOG     */ 'X',  
  /*T_SERVKIND   */ 0,
  /*T_SERVKINDSUB*/ 0,
  /*T_MARKETID   */ 0,
  /*T_MARKETNAME */ CHR(0),
  /*T_INREST     */ SUM(t_InRest),
  /*T_OUTREST    */ SUM(t_OutRest),
  /*T_PLANREST   */ SUM(t_PlanRest)
    BULK COLLECT INTO v_brkrep
      FROM dbrkrepaccitog_tmp
     WHERE t_IsItog = CHR(0)
       AND t_ServKindSub <> 9
     GROUP BY t_Currency, t_CCY;

    IF v_brkrep.COUNT > 0 THEN
       FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
          INSERT INTO dbrkrepaccitog_tmp
               VALUES v_brkrep (indx);
    END IF;
  END IF;

END CreateAccMoveData;

--Формирование данных по обязательствам перед банком на дату
PROCEDURE CreateDebtDataByDate(p_DlContrID IN NUMBER, p_OnDate IN DATE)
IS
  TYPE brkrep_t IS TABLE OF DBRKREPDEBT_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;
BEGIN
  --Обязательства по комиссиям
  WITH q AS (SELECT sf.t_ID, sf.t_PartyID
               FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf
              WHERE mp.t_DlContrID = p_DlContrID
                AND sf.t_ID = mp.t_SfContrID
            )
  SELECT
  /*T_TYPE      */ c.t_Type,
  /*T_TEXT      */ 'за '||TRIM(TO_CHAR(c.t_dateperiodend, 'month', 'nls_date_language = russian'))||' '||TO_CHAR(c.t_DatePeriodEnd, 'YYYY'),
  /*T_ORIGINDATE*/ c.t_DatePeriodEnd,
  /*T_PAYDATE   */ CASE WHEN c.t_Type = DEBT_INVEST_COM
                        THEN NVL((SELECT MAX(serv.t_EndDate)
                                    FROM ddlcontrserv_dbt serv
                                   WHERE serv.t_DlContrID = p_DlContrID 
                                     AND serv.t_BeginDate < c.t_DatePeriodEnd                                                                                                                                                                                                                                          
                                     AND serv.t_EndDate >= c.t_DatePeriodBegin
                                     AND serv.t_EndDate <= RSI_RSBCALENDAR.GetDateAfterWorkDay((ADD_MONTHS(TRUNC(c.t_DatePeriodEnd, 'q'), 3)-1), 10, 10003)
                                 ),
                                 RSI_RSBCALENDAR.GetDateAfterWorkDay((ADD_MONTHS(TRUNC(c.t_DatePeriodEnd, 'q'), 3)-1)/*Последний день текущего квартала*/, 10, 10003)
                                )
                        ELSE RSI_RSBCALENDAR.GetDateAfterWorkDay(TRUNC(LAST_DAY(c.t_DatePeriodEnd))/*Последний день текущего месяца*/, 10, 10003)
                    END,
  /*T_CURRENCY  */ c.t_FIID_Sum,
  /*T_CCY       */ NVL((SELECT fin.t_CCY FROM dfininstr_dbt fin WHERE fin.t_FIID = c.t_FIID_Sum), CHR(1)),
  /*T_SUM       */ c.t_Sum,
  /*T_ISTOTAL   */ CHR(0),
  /*T_ISITOG    */ CHR(0),
  /*T_ONDATE    */ p_OnDate
    BULK COLLECT INTO v_brkrep
    FROM (SELECT sfdef.t_DatePeriodBegin, sfdef.t_DatePeriodEnd, sfdef.t_FIID_Sum, sfdef.t_Sum, q.t_ID, 
                 CASE WHEN com.t_Code = 'ИнвестСоветник' THEN DEBT_INVEST_COM ELSE DEBT_FIX_COM END as t_Type
            FROM dsfdef_dbt sfdef, doproper_dbt opr, doprstep_dbt step, dsfcomiss_dbt com, q  
           WHERE sfdef.t_FeeType = 1 
             AND com.t_Code in ('БрокерФикс', 'ИнвестСоветник') 
             AND sfdef.t_CommNumber = com.t_Number 
             AND sfdef.t_Status = 40 
             AND sfdef.t_SfContrID = q.t_ID
             AND sfdef.t_DatePeriodEnd <= p_OnDate
             AND opr.t_Kind_Operation = 4603
             AND opr.t_DocumentID = LPAD(sfdef.t_ID, 34, 0)
             AND step.t_ID_Operation = opr.t_ID_Operation
             AND step.t_Number_Step = 50
             AND step.t_Fact_Date > p_OnDate
           UNION ALL
          SELECT sfdef.t_DatePeriodBegin, sfdef.t_DatePeriodEnd, sfdef.t_FIID_Sum, sfdef.t_Sum, q.t_ID, 
                 CASE WHEN com.t_Code = 'ИнвестСоветник' THEN DEBT_INVEST_COM ELSE DEBT_FIX_COM END as t_Type
            FROM dsfdef_dbt sfdef, dsfcomiss_dbt com, q   
           WHERE sfdef.t_FeeType = 1 
             AND com.t_Code IN ('БрокерФикс', 'ИнвестСоветник')  
             AND sfdef.t_CommNumber = com.t_Number 
             AND sfdef.t_Status = 10 
             AND sfdef.t_SfContrID = q.t_ID
             AND sfdef.t_DatePeriodEnd <= p_OnDate) c;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepdebt_tmp
             VALUES v_brkrep (indx);
  END IF;
  
  --Итоговые обязательства по комиссиям
  SELECT
  /*T_TYPE      */ t_Type,
  /*T_TEXT      */ CASE WHEN t_Type = DEBT_INVEST_COM THEN 'Комиссия брокера в рамках услуги инвест. консультирования, в т.ч.'
                        ELSE 'Минимальная брокерская комиссия, в т.ч.' END,
  /*T_ORIGINDATE*/ TO_DATE('01.01.0001','DD.MM.YYYY'),
  /*T_PAYDATE   */ TO_DATE('01.01.0001','DD.MM.YYYY'),
  /*T_CURRENCY  */ t_Currency,
  /*T_CCY       */ t_CCY,
  /*T_SUM       */ SUM(t_Sum),
  /*T_ISTOTAL   */ 'X',
  /*T_ISITOG    */ CHR(0),
  /*T_ONDATE    */ p_OnDate
    BULK COLLECT INTO v_brkrep
    FROM dbrkrepdebt_tmp
   WHERE t_IsTotal = CHR(0)
     AND t_OnDate = p_OnDate
   GROUP BY t_Type, t_Currency, t_CCY;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepdebt_tmp
             VALUES v_brkrep (indx);
  END IF;
  
  IF(rsb_common.GetRegBoolValue(p_KeyPath => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ОТЧЕТ БРОКЕРА\ИСКАТЬ_ПОСРОЧ_КОМИСС_030323')) THEN
  
    --Просроченная брокерская комиссия по сделкам
    SELECT
    /*T_TYPE      */ DEBT_DEAL_COM,
    /*T_TEXT      */ 'Просроченная брокерская комиссия по сделкам',
    /*T_ORIGINDATE*/ TO_DATE('01.01.0001','DD.MM.YYYY'),
    /*T_PAYDATE   */ TO_DATE('01.01.0001','DD.MM.YYYY'),
    /*T_CURRENCY  */ RSI_RSB_FIInstr.NATCUR,
    /*T_CCY       */ 'RUB',
    /*T_SUM       */ (SUM(t_reqSum) - SUM(t_payed_tr)),
    /*T_ISTOTAL   */ 'X',
    /*T_ISITOG    */ CHR(0),
    /*T_ONDATE    */ p_OnDate
       BULK COLLECT INTO v_brkrep
       FROM U_COMPAY_DBT d 
      WHERE d.t_calcid = (SELECT MAX(c_out.t_calcid) 
                            FROM U_COMPAY_DBT c_out
                           WHERE c_out.t_calcdate = (SELECT MAX(c_in.t_calcdate) 
                                                       FROM U_COMPAY_DBT  c_in 
                                                      WHERE c_in.t_calcdate <= p_OnDate)
                          )
        AND d.t_dlcontrid = p_DlContrID
        AND d.t_begdate <= p_OnDate
        AND d.t_sysdate is not null 
     HAVING (SUM(t_reqSum) - SUM(t_payed_tr)) <> 0;

     IF v_brkrep.COUNT > 0 THEN
       FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
          INSERT INTO dbrkrepdebt_tmp
               VALUES v_brkrep (indx);
    END IF;
  END IF;
  
  --Прочая задолженность 
  WITH q AS (SELECT t.t_ID, MAX(t.t_Instance) AS t_Instance
               FROM (SELECT rstr.t_ID, rstr.t_Instance
                       FROM ddl_debtreestr_dbt rstr
                      WHERE rstr.t_DlContrID = p_DlContrID
                        AND rstr.t_DebtDate <= p_OnDate  
                        AND rstr.t_ChangeDate <= p_OnDate
                      UNION 
                     SELECT rstr.t_ID, hist.t_Instance
                       FROM ddl_debtreestrhist_dbt hist INNER JOIN ddl_debtreestr_dbt rstr ON rstr.t_ID = hist.t_DebtID
                      WHERE rstr.t_DlContrID = p_DlContrID
                        AND hist.t_DebtDate <= p_OnDate   
                        AND hist.t_ChangeDate <= p_OnDate   
                    ) t
              GROUP BY t.t_ID)
  SELECT   
  /*T_TYPE      */ DEBT_EXPIRED,
  /*T_TEXT      */ 'Прочая просроченная задолженность',
  /*T_ORIGINDATE*/ TO_DATE('01.01.0001','DD.MM.YYYY'),
  /*T_PAYDATE   */ TO_DATE('01.01.0001','DD.MM.YYYY'),
  /*T_CURRENCY  */ c.t_DebtCurrency,
  /*T_CCY       */ NVL((SELECT fin.t_CCY FROM dfininstr_dbt fin WHERE fin.t_FIID = c.t_DebtCurrency), CHR(1)),
  /*T_SUM       */ SUM(c.t_DebtSum),
  /*T_ISTOTAL   */ 'X',
  /*T_ISITOG    */ CHR(0),
  /*T_ONDATE    */ p_OnDate
    BULK COLLECT INTO v_brkrep 
    FROM (SELECT rstr.t_DebtSum, rstr.t_DebtCurrency, rstr.t_State
            FROM q INNER JOIN ddl_debtreestr_dbt rstr ON rstr.t_ID = q.t_ID AND rstr.t_Instance = q.t_Instance
           UNION 
          SELECT hist.t_DebtSum, hist.t_DebtCurrency, hist.t_State
            FROM q INNER JOIN ddl_debtreestrhist_dbt hist ON hist.t_DebtID = q.t_ID AND hist.t_Instance = q.t_Instance
         ) c
   WHERE c.t_DebtSum <> 0
     AND c.t_State = Rsb_DebtSum.DEBTREESTR_STATE_ACTIVE
   GROUP BY c.t_DebtCurrency;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepdebt_tmp
             VALUES v_brkrep (indx);
  END IF;
  
  --Сформировать итоговую строку
  SELECT
  /*T_TYPE      */ 0,
  /*T_TEXT      */ 'Итого в RUB',
  /*T_ORIGINDATE*/ TO_DATE('01.01.0001','DD.MM.YYYY'),
  /*T_PAYDATE   */ TO_DATE('01.01.0001','DD.MM.YYYY'),
  /*T_CURRENCY  */ RSI_RSB_FIInstr.NATCUR,
  /*T_CCY       */ CHR(1),
  /*T_SUM       */ SUM(NVL(RSI_RSB_FIInstr.ConvSum(t_Sum, t_Currency, RSI_RSB_FIInstr.NATCUR, p_OnDate),0)),
  /*T_ISTOTAL   */ 'X',
  /*T_ISITOG    */ 'X',
  /*T_ONDATE    */ t_OnDate
    BULK COLLECT INTO v_brkrep
    FROM dbrkrepdebt_tmp
   WHERE t_IsTotal = 'X'
     AND t_OnDate = p_OnDate
     AND t_IsItog = CHR(0)
   GROUP BY t_OnDate;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepdebt_tmp
             VALUES v_brkrep (indx);
  END IF;
END CreateDebtDataByDate;

--Формирование данных по обязательствам перед банком
PROCEDURE CreateDebtData(p_DlContrID IN NUMBER, p_BeginDate IN DATE, p_EndDate IN DATE)
IS
BEGIN
  CreateDebtDataByDate(p_DlContrID, p_EndDate);
  CreateDebtDataByDate(p_DlContrID, p_BeginDate-1); --Для раздела 1.1 нужно узнать плановые обязательства на начало периода, но в 1.4 выводить их не будем
END CreateDebtData;

--Формирование сводной информации
PROCEDURE CreateSvodInfoData(p_BegDate IN DATE, p_EndDate IN DATE)
IS
  TYPE brkrep_t IS TABLE OF DBRKREPSVODINFO_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
  v_brkrep brkrep_t;
BEGIN 
  SELECT
  /*T_NAME       */ 'Оценка денежных средств, с учетом плановой позиции',
  /*T_INSUM      */ InPlanRest,
  /*T_OUTSUM     */ OutPlanRest,
  /*T_DIFFERENCE */ OutPlanRest - InPlanRest,
  /*T_SERVKIND   */ ServKind,
  /*T_SERVKINDSUB*/ ServKindSub,
  /*T_ISITOG     */ CHR(0)
    BULK COLLECT INTO v_brkrep
    FROM (SELECT SUM(InPlanRest) InPlanRest, SUM(OutPlanRest) OutPlanRest, ServKind, ServKindSub
            FROM (SELECT NVL(RSI_RSB_FIInstr.ConvSum(repacc.t_InPlanRest, acc.t_Code_Currency, RSI_RSB_FIInstr.NATCUR, p_BegDate-1),0) InPlanRest, 
                         NVL(RSI_RSB_FIInstr.ConvSum(repacc.t_OutPlanRest, acc.t_Code_Currency, RSI_RSB_FIInstr.NATCUR, p_EndDate),0) OutPlanRest,
                         CASE WHEN repacc.t_ServKindSub <> 9 THEN 0 ELSE repacc.t_ServKind END ServKind,
                         CASE WHEN repacc.t_ServKindSub <> 9 THEN 0 ELSE repacc.t_ServKindSub END ServKindSub
                    FROM dbrkrepacc_tmp repacc, daccount_dbt acc 
                   WHERE acc.t_AccountID = repacc.t_AccountID
                 )
           GROUP BY ServKind, ServKindSub
         )
   WHERE InPlanRest <> 0 OR OutPlanRest <> 0;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepsvodinfo_tmp
             VALUES v_brkrep (indx);
  END IF;
  
  SELECT
  /*T_NAME       */ 'Обязательства перед Банком',
  /*T_INSUM      */ InPlanRest,
  /*T_OUTSUM     */ OutPlanRest,
  /*T_DIFFERENCE */ OutPlanRest - InPlanRest,
  /*T_SERVKIND   */ ServKind,
  /*T_SERVKINDSUB*/ ServKindSub,
  /*T_ISITOG     */ CHR(0)
    BULK COLLECT INTO v_brkrep
    FROM (SELECT NVL((SELECT -t_Sum FROM dbrkrepdebt_tmp WHERE t_IsItog = 'X' AND t_OnDate = p_BegDate-1), 0) InPlanRest,
                 NVL((SELECT -t_Sum FROM dbrkrepdebt_tmp WHERE t_IsItog = 'X' AND t_OnDate = p_EndDate), 0) OutPlanRest,
                 0 ServKind, 0 ServKindSub --Обязательства только в биржевом портфеле
            FROM dual)
   WHERE InPlanRest <> 0 OR OutPlanRest <> 0;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepsvodinfo_tmp
             VALUES v_brkrep (indx);
  END IF;
  
  SELECT
  /*T_NAME       */ 'Оценка позиции по активам, с учетом плановой позиции',
  /*T_INSUM      */ InPlanRest,
  /*T_OUTSUM     */ OutPlanRest,
  /*T_DIFFERENCE */ OutPlanRest - InPlanRest,
  /*T_SERVKIND   */ t_ServKind,
  /*T_SERVKINDSUB*/ t_ServKindSub,
  /*T_ISITOG     */ CHR(0)
    BULK COLLECT INTO v_brkrep
    FROM (SELECT SUM(t_InPlanCostRest) InPlanRest, SUM(t_OutPlanCostRest) OutPlanRest, t_ServKind, t_ServKindSub 
            FROM (SELECT t_InPlanCostRest, t_OutPlanCostRest, t_ServKind, t_ServKindSub 
                    FROM dbrkrepactiveavoir_tmp 
                   WHERE t_IsItog = 'X'
                   UNION ALL
                  SELECT t_InPlanCostRest, t_OutPlanCostRest, t_ServKind, t_ServKindSub 
                    FROM dbrkrepactivederiv_tmp 
                   WHERE t_IsItog = 'X'
                 )
           GROUP BY t_ServKind, t_ServKindSub
         )         
   WHERE InPlanRest <> 0 OR OutPlanRest <> 0;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepsvodinfo_tmp
             VALUES v_brkrep (indx);
  END IF;
  
  --Сформировать итоговые строки
  SELECT
  /*T_NAME       */ 'Итого в RUB по курсу ЦБ',
  /*T_INSUM      */ SUM(t_InSum),
  /*T_OUTSUM     */ SUM(t_OutSum),
  /*T_DIFFERENCE */ SUM(t_Difference),
  /*T_SERVKIND   */ t_ServKind,
  /*T_SERVKINDSUB*/ t_ServKindSub,
  /*T_ISITOG     */ 'X'
    BULK COLLECT INTO v_brkrep
    FROM dbrkrepsvodinfo_tmp
   WHERE t_IsItog = CHR(0)
   GROUP BY t_ServKind, t_ServKindSub;

  IF v_brkrep.COUNT > 0 THEN
     FORALL indx IN v_brkrep.FIRST .. v_brkrep.LAST
        INSERT INTO dbrkrepsvodinfo_tmp
             VALUES v_brkrep (indx);
  END IF;
END CreateSvodInfoData;

--Формирование данных по курсам валют
PROCEDURE CreateCoursesData(p_BeginDate IN DATE, p_EndDate IN DATE)
IS
  v_brkrep DBRKREPCOURSES_TMP%ROWTYPE;
  v_RateType  NUMBER := 7;
  v_Rate      NUMBER;
  v_Scale     NUMBER;
  v_Point     NUMBER;
  v_IsInverse CHAR;
BEGIN 
  FOR one_curr IN (SELECT DISTINCT acc.t_Code_Currency, fin.t_CCY,                          
                          CASE WHEN repacc.t_ServKindSub <> 9 THEN 0 ELSE repacc.t_ServKind END ServKind,
                          CASE WHEN repacc.t_ServKindSub <> 9 THEN 0 ELSE repacc.t_ServKindSub END ServKindSub
                     FROM dbrkrepacc_tmp repacc, daccount_dbt acc, dfininstr_dbt fin                                                                  
                    WHERE acc.t_AccountID = repacc.t_AccountID                                                                     
                      AND acc.t_Code_Currency <> RSI_RSB_FIInstr.NATCUR                                                                        
                      AND fin.t_FIID = acc.t_Code_Currency
                  )
  LOOP
    v_brkrep.t_Currency    := one_curr.t_Code_Currency;
    v_brkrep.t_CCY         := one_curr.t_CCY;
    v_brkrep.t_ServKind    := one_curr.ServKind;
    v_brkrep.t_ServKindSub := one_curr.ServKindSub;

    v_brkrep.t_InRate      := NVL(RSI_RSB_FIInstr.ConvSum2(1, one_curr.t_Code_Currency, RSI_RSB_FIInstr.NATCUR, p_BeginDate, 0, v_RateType, v_Rate, v_Scale, v_Point, v_IsInverse), 0);
    v_brkrep.t_InPoint     := v_Point + (LENGTH(TO_CHAR(v_Scale)) - 1);
    IF v_brkrep.t_InPoint IS NULL OR v_brkrep.t_InPoint < 4 THEN
      v_brkrep.t_InPoint := 4;
    ELSIF v_brkrep.t_InPoint > 8 THEN
      v_brkrep.t_InPoint := 8;
    END IF;

    v_brkrep.t_OutRate     := NVL(RSI_RSB_FIInstr.ConvSum2(1, one_curr.t_Code_Currency, RSI_RSB_FIInstr.NATCUR, p_EndDate, 0, v_RateType, v_Rate, v_Scale, v_Point, v_IsInverse), 0);
    v_brkrep.t_OutPoint    := v_Point + (LENGTH(TO_CHAR(v_Scale)) - 1);
    IF v_brkrep.t_OutPoint IS NULL OR v_brkrep.t_OutPoint < 4 THEN
      v_brkrep.t_OutPoint := 4;
    ELSIF v_brkrep.t_OutPoint > 8 THEN
      v_brkrep.t_OutPoint := 8;
    END IF;

    INSERT INTO dbrkrepcourses_tmp VALUES v_brkrep;
  END LOOP;
END CreateCoursesData;

PROCEDURE GetErrMailTempl(p_TemplID IN NUMBER, p_Subject OUT CLOB, p_Body OUT CLOB, p_err OUT NUMBER)
IS
BEGIN
  SELECT t_Subject, t_Body INTO p_Subject, p_Body FROM dbrkrep_errmailtempl_dbt WHERE t_ID = p_TemplID;
  p_err := 0;
EXCEPTION
  WHEN OTHERS
    THEN p_err := 1;
END;

FUNCTION GetSignerName(p_SignerParty IN NUMBER)
  RETURN VARCHAR2
IS
  v_SignerName dperson_dbt.t_Name%TYPE;
BEGIN
  SELECT t_Name
    INTO v_SignerName
    FROM dperson_dbt
   WHERE t_PartyID = p_SignerParty;
  RETURN v_SignerName;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    BEGIN 
      SELECT t_Name
        INTO v_SignerName
        FROM dperson_dbt
       WHERE t_Oper = RsbSessionData.Oper;
      RETURN v_SignerName;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN '';
    END;
END GetSignerName;

FUNCTION IsDigitNumber(p_StrNumber IN VARCHAR2)
  RETURN NUMBER
IS
  v_stat NUMBER := 0;
  v_i NUMBER := 1;
  v_ch CHAR(1);
  v_DigitString VARCHAR2(11) := '0123456789''';
  v_flag_point NUMBER := 0;
BEGIN
  WHILE v_stat = 0 AND v_i <= length(p_StrNumber)
  LOOP
    v_ch := SUBSTR(p_StrNumber, v_i, 1);
    IF INSTR(v_DigitString, v_ch) = 0 THEN
      IF v_ch = '.' OR v_ch = ',' THEN
        v_flag_point := 1;
      ELSE
        v_stat := 1;
      END IF;
    ELSIF v_flag_point = 1 AND v_ch != '0' THEN
      v_stat := 1;
    END IF;
    v_i := v_i + 1;
  END LOOP;

  RETURN v_stat;
END IsDigitNumber;

FUNCTION IsHaveFractPart(p_MoneyValue IN NUMBER)
  RETURN NUMBER
IS
BEGIN
  IF IsDigitNumber(to_char(p_MoneyValue)) = 1 THEN
    /* Есть дробная часть. */
    RETURN 1;                         
  ELSE
    RETURN 0;
  END IF;
END IsHaveFractPart; 

FUNCTION AmountPrecision(p_Amount IN NUMBER, p_DecimalPlaces IN NUMBER default 6)
  RETURN NUMBER
IS
BEGIN
  IF IsHaveFractPart(ABS(p_Amount)) = 1 THEN
    RETURN p_DecimalPlaces;
  ELSE
    RETURN 0;
  END IF;
END AmountPrecision; 

END RSB_BRKREP_RSHB_NEW;
/