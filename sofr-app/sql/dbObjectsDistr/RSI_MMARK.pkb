CREATE OR REPLACE PACKAGE BODY RSI_MMark
IS
    -- Author  : Korolev Sergey
    -- Created : 16.12.2003
  
  -- ф-ия, приводящая ставку к одному "знаменателю"
    function FI_ConvertToPointlessRate(
        i_Price in ddl_leg_dbt.t_Price%TYPE, -- ставка
        i_Point in ddl_leg_dbt.t_Point%TYPE  -- точность
                                    ) 
    return ddl_leg_dbt.t_Price%TYPE
  IS    
    p int := 1;   
  Begin 
    p := power(10, i_Point);
    if 1 > p then p := 1; end if; -- аналог rs_max
    return i_Price / p;         -- p - denominator
  End FI_ConvertToPointlessRate; 

  -- ф-ия, вычисляющая ко-во дней между двумя датами
  function Period_ndays(
            i_Start    in ddl_leg_dbt.t_Start%TYPE, 
        i_Maturity in ddl_leg_dbt.t_Maturity%TYPE
                   )
  return int
  IS
  Begin
    return i_Maturity - i_Start;  
  End Period_ndays;

  -- Для RsVox объекта RmmGuarantee
  -- возвращает mtoFiid если на дату mdate
  -- существует курс для перевода mfromFiid
  -- в mtoFiid. Если курса в mtoFiid нет, 
  -- возвращает любую валюту для которой 
  -- есть курс mfromFiid. Если нет никакого курса
  -- возвращает -1 
  -- Карачаров А.В.
  FUNCTION P_MM_GUARANTEERATEFIID(
    mfromFiid IN NUMBER, -- Идентификатор ФИ
    mtoFiid   IN NUMBER, -- Идентификатор валюты для которой ищем курс
    mdate     IN DATE    -- Отчетная дата
    )
    RETURN NUMBER
  AS
    mrate    FLOAT(53);  -- курс
    mnewfiid NUMBER(10); -- валюта курса
    mtype    NUMBER(5);  -- переменная для хранения типа ФИ "Рыночная цена"
    mfikind  NUMBER(5);  -- переменная для хранения вида ФИ "Валюты" 
  BEGIN
    -- инициализация переменных
    mrate    := NULL;
    mnewfiid := NULL;
    mtype    := NULL;
    mfikind  := NULL;
    
    BEGIN
      SELECT t_type
      INTO mtype
      FROM dratetype_dbt
      WHERE t_definition = 'Рыночная цена'
        AND ROWNUM < 2
      ;
    EXCEPTION WHEN no_data_found THEN mtype := NULL;
    END;
    
    IF mtype IS NOT NULL THEN
    -- если есть тип ФИ "Рыночная цена"
    -- 1. определяем вид ФИ "Валюты"
      SELECT t_fi_kind
      INTO mfikind
      FROM dfikinds_dbt
      WHERE t_name = 'Валюты'
      ;
  
    -- 2. ищем есть ли курс на mdate
      BEGIN
        SELECT t_rate
        INTO mrate
      FROM  
      (
        SELECT t_rate, t_point 
        FROM dratedef_dbt
        WHERE t_type = mtype
          AND t_otherfi = mfromFiid
          AND t_fiid = mtoFiid
          AND t_sincedate <= mdate
        ORDER by t_sincedate DESC
      )
      WHERE ROWNUM < 2
      ;
      EXCEPTION WHEN no_data_found THEN mrate := NULL;
      END;
    
      IF mrate IS NULL THEN
      -- если курса на дату mdate нет, берем любой курс на дату
      BEGIN
        SELECT t_fiid
        INTO mnewfiid
        FROM
        (
          SELECT t_fiid
          FROM dratedef_dbt
          WHERE t_type = mtype
            AND t_otherfi = mfromfiid
            AND t_fiid IN (
                           SELECT t_fiid
                           FROM dfininstr_dbt
                           WHERE t_fi_kind = mfikind
                          )
            AND t_sincedate <= mdate
          ORDER by t_sincedate DESC          
        )
        WHERE ROWNUM < 2               
        ;
      EXCEPTION 
        WHEN no_data_found THEN  mnewFiid := -1;
      END;
      ELSE
      -- курс для mfromFiid в mtoFiid на дату mdate найден
      -- соответственно надо вернуть mtoFiid  
        mnewFiid := mtoFiid;
      END IF;  
    END IF;
    
    IF mnewFiid IS NULL THEN 
    -- если нет ни одного курса на mdate 
    -- возвращаем -1
      mnewFiid := -1;
    END IF;

    RETURN mnewfiid;

  END;

  -- Получить курс mfromFiid в валюте mtoFiid, 
  -- если такого курса на дату mdate нет, 
  -- вернуть -1.
  -- Карачаров А.В.
  FUNCTION P_MM_GUARANTEERATE(
    mfromFiid IN NUMBER, -- Идентификатор ФИ
    mtoFiid   IN NUMBER, -- Идентификатор валюты для которой ищем курс
    mdate     IN DATE   -- Отчетная дата
    )
    RETURN NUMBER
  AS
    mrate    FLOAT(53);  -- курс
    mtype    NUMBER(5);  -- переменная для хранения типа ФИ "Рыночная цена"
  BEGIN
    -- инициализация переменных
    mrate    := NULL;
    mtype    := NULL;
    
    BEGIN
      SELECT t_type
      INTO mtype
      FROM dratetype_dbt
      WHERE t_definition = 'Рыночная цена'
        AND ROWNUM < 2
      ;
    EXCEPTION WHEN no_data_found THEN mtype := NULL;
    END;
    
    IF mtype IS NOT NULL THEN
    BEGIN
      SELECT t_rate/POWER(10, t_point)
      INTO mrate
      FROM
      (
        SELECT t_rate, t_point
        FROM dratedef_dbt
        WHERE t_type = mtype
          AND t_otherfi = mfromfiid
          AND t_fiid = mtofiid
          AND t_sincedate <= mdate
        ORDER by t_sincedate DESC
      )
      WHERE ROWNUM < 2
      ;
    EXCEPTION WHEN no_data_found THEN mrate := -1;
    END;
    END IF;
 
    
    RETURN mrate;
  END;

  FUNCTION ExistSecWithUnsafeDepository(
    ContractID IN NUMBER,
    OperDate IN DATE
    )
    RETURN NUMBER
    IS
      cntSec NUMBER := 0;
    BEGIN
        select COUNT(1) into cntSec from 
        ddl_secur_dbt sec, 
        (
          select TO_NUMBER(ltrim(t_object, '0')) as t_object, MAX(t_validfromdate) from dobjatcor_dbt where
          t_objecttype = 3 and t_groupid = 52
          and t_general = 'X' 
          AND t_attrid = 1
          and OperDate BETWEEN T_VALIDFROMDATE AND T_VALIDTODATE
          group by  t_object
        ) ctg
        where 
        sec.t_contractkind = 126 and sec.t_contractid = ContractID
        and(sec.t_depositorydepo = ctg.t_object or sec.t_depositoryverify = ctg.t_object);

        if (cntSec = 0)
        then
            select COUNT(1) into cntSec from
            ddl_secur_dbt sec
            where
            sec.t_contractkind = 126 and sec.t_contractid = ContractID
            and sec.t_SecReserve = CNST.SET_CHAR;
        end if;

        return cntSec; 
    END;

   -- Заполнить таблицу платежей для автоквитовки
   FUNCTION FillKvitTmp(
     objid       IN NUMBER,
     objn        IN NUMBER,
     calcdate    IN DATE,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
   RETURN NUMBER
   IS
   BEGIN
      INSERT INTO DMM_KVIT_TMP (t_dealid, t_planpaymid, t_factpaymid, t_kvitamount, t_auto, t_valuedate)
      WITH
      plan_payms AS (
      SELECT pm.*
        FROM dpmpaym_dbt  pm,
             ddl_tick_dbt tick
       WHERE tick.t_bofficekind = 102
         AND tick.t_dealstatus = 10
         AND pm.t_dockind = tick.t_bofficekind
         AND pm.t_documentid = tick.t_DealID
         AND t_paymstatus = 1000
         AND t_futurepayeramount > 0
         AND t_receiverbankid != t_payerbankid
         AND t_receiver = RSBSESSIONDATA.OurBank()
      ),
      fact_payms AS (
      SELECT *
        FROM dpmpaym_dbt
       WHERE t_isfactpaym = 'X'
         AND t_paymstatus = 2995
         AND t_tobackoffice = 'J'
      ),
      payms_union AS (
      SELECT pp.t_documentid t_DealID,
             pp.t_paymentid t_PlanPaymID,
             fp.t_paymentid t_FactPaymID,
             pp.t_valuedate t_ValueDate,
             LEAST(pp.t_futurepayeramount, fp.t_futurepayeramount) t_KvitAmount,
             DECODE(fp.t_valuedate, pp.t_valuedate, 1, 0) t_IsEQ_ValueDate,
             DECODE(ROUND(fp.t_futurepayeramount, 2), ROUND(pp.t_futurepayeramount, 2), 1, 0) t_IsEQ_Amount,
             DECODE(fp.t_futurereceiveraccount, pp.t_futurereceiveraccount, 1, 0) t_IsEQ_Account
        FROM plan_payms pp,
             fact_payms fp
       WHERE fp.t_payer = pp.t_payer
         AND fp.t_fiid = pp.t_fiid
      ),
      matching_tbl AS(
      SELECT t.*,
             CASE
                WHEN t.t_IsEQ_Amount = 1 AND  t.t_IsEQ_ValueDate = 1 AND t.t_IsEQ_Account = 1
                   THEN COUNT(t.t_PlanPaymID) OVER (partition by t.t_FactPaymID, t.t_IsEQ_ValueDate, t.t_IsEQ_Amount, t.t_IsEQ_Account)
                WHEN t.t_IsEQ_Amount = 1 AND t.t_IsEQ_ValueDate = 1
                   THEN COUNT(t.t_PlanPaymID) OVER (partition by t.t_FactPaymID, t.t_IsEQ_ValueDate, t.t_IsEQ_Amount)
                ELSE 0
             END lvl_1,
             CASE
                WHEN t.t_IsEQ_Amount = 1 AND t.t_IsEQ_ValueDate = 1 AND t.t_IsEQ_Account = 1
                   THEN COUNT(t.t_FactPaymID) OVER (partition by t.t_PlanPaymID, t.t_IsEQ_ValueDate, t.t_IsEQ_Amount, t.t_IsEQ_Account)
                WHEN t.t_IsEQ_Amount = 1 AND t.t_IsEQ_ValueDate = 1
                   THEN COUNT(t.t_FactPaymID) OVER (partition by t.t_PlanPaymID, t.t_IsEQ_ValueDate, t.t_IsEQ_Amount)
                ELSE 0
             END lvl_2
        FROM payms_union t
      )
      SELECT t_DealID, t_PlanPaymID, t_FactPaymID, t_KvitAmount, DECODE(lvl_1 * lvl_2, 1, 'X', CHR(0)), t_ValueDate
        FROM matching_tbl
       WHERE lvl_1 * lvl_2 = 1;

      RETURN 0;
   END FillKvitTmp;

   FUNCTION GenGraphRec(
     objid       IN NUMBER,
     objn        IN NUMBER,
     GraphType   IN NUMBER,
     calcdate    IN DATE,
     ContextV    IN NUMBER DEFAULT 0,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
   RETURN NUMBER
   IS
      val       NUMBER := 0;
      IsStdProc CHAR   := CHR(0);
      ProcName  VARCHAR2(512);
   BEGIN
      IsStdProc := RSB_Common.GetRegFlagValue('MMARK\ГРАФИКИ_ПЛАТЕЖЕЙ\ЗАПИСИ_ГРАФИКА\ISSTDPROC'); 
      IF (IsStdProc <> CNST.SET_CHAR) THEN
        ProcName := RSB_Common.GetRegStrValue('MMARK\ГРАФИКИ_ПЛАТЕЖЕЙ\ЗАПИСИ_ГРАФИКА\USERPROC');
      ELSE
        ProcName := 'RSI_MMARK.StdGenGraphRec';
      END IF;
      
      EXECUTE IMMEDIATE 'BEGIN :retval := ' || ProcName || '(:objid, :objn, :GraphType, :calcdate, :ContextV, :ContextType); END;'
       USING OUT val,
             IN objid,
             IN objn,
             IN GRaphType,
             IN calcdate,
             IN ContextV,
             IN ContextType;
      RETURN val;
      
      EXCEPTION WHEN OTHERS THEN RETURN -1;
   END GenGraphRec;
   
   FUNCTION RecalcGraph(
     objid       IN NUMBER,
     objn        IN NUMBER,
     GraphType   IN NUMBER,
     calcdate    IN DATE,
     ContextV    IN NUMBER DEFAULT 0,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
   RETURN NUMBER
   IS
      stat       INTEGER := 0;
      TempSum    NUMBER := 0;
      GraphDate1 DATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
      Purpose    NUMBER := (CASE WHEN GraphType = 1 THEN 10 ELSE 11 END);
      LEG        DDL_LEG_DBT%ROWTYPE;
      
      CURSOR c_pmgraph IS
        SELECT * 
        FROM DMMPMGRAPH_DBT 
        WHERE t_DocumentID = objn AND t_GraphType = GraphType AND t_Date > calcdate
        ORDER BY t_Date ASC
        FOR UPDATE;
   BEGIN
      IF (ContextType != MMarkConst.CN_CONTEXT_GENGRAPHREC) THEN
        DELETE FROM DMMPMGRAPH_DBT WHERE t_DocumentID = objn AND t_GraphType = GraphType;
        INSERT INTO DMMPMGRAPH_DBT(t_DocumentID, t_GraphType, t_SumCalc, t_Date) 
           (SELECT objn, GraphType, t_BaseAmount, t_ValueDate FROM DPMPAYM_DBT 
              WHERE t_DocumentID = objn AND t_DocKind = objid AND t_Purpose = Purpose );
      END IF; 
      stat := GenGraphRec(objid, objn, GraphType, calcdate, ContextV, ContextType);
      
      SELECT * INTO LEG FROM DDL_LEG_DBT WHERE t_DealID = objn AND T_LEGKIND = 0 AND T_LEGID = 1;
      BEGIN
        SELECT NVL(MAX(t_ValueDate), LEG.t_Start) INTO GraphDate1 FROM dpmpaym_dbt
            WHERE t_DocKind = objid AND t_DocumentID = objn AND t_purpose = 11 AND t_ValueDate <= calcdate;
        EXCEPTION WHEN no_data_found THEN GraphDate1 := LEG.t_Start;
      END;
   
      FOR rec IN c_pmgraph
      LOOP
        TempSum := RSI_MMARK.GenGraphSum(objid, objn, GraphType, calcdate, GraphDate1, rec.t_Date, ContextV, ContextType);
        IF (TempSum = -1) THEN
            RETURN -1;
        END IF;
        
        UPDATE DMMPMGRAPH_DBT 
        SET t_SumCalc = TempSum
        WHERE t_GraphID = rec.t_GraphID;
        
        GraphDate1 := rec.t_Date;
        
      END LOOP;
      
      DELETE FROM DMMPMGRAPH_DBT WHERE t_DocumentID = objn AND t_GraphType = GraphType AND t_SumCalc = 0;
      RETURN 0;
   END RecalcGraph;
   
   FUNCTION GetWorkDate(CDate IN DATE, AdjType IN NUMBER)
   RETURN DATE
   IS
    WorkDate DATE := CDate;
   BEGIN
    IF (AdjType = 1) THEN
        WHILE (RSI_RsbCalendar.isworkday(WorkDate) = 0)
        LOOP
            WorkDate := WorkDate + 1;
        END LOOP;
    ELSIF (AdjType = 2) THEN
        WHILE (RSI_RsbCalendar.isworkday(WorkDate) = 0)
        LOOP
            WorkDate := WorkDate - 1;
        END LOOP;
    END IF;
    RETURN WorkDate;
   END GetWorkDate;
   
   FUNCTION StdGenGraphRec(
     objid       IN NUMBER,
     objn        IN NUMBER,
     GraphType   IN NUMBER,
     calcdate    IN DATE,
     ContextV    IN NUMBER DEFAULT 0,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
   RETURN NUMBER
   IS
      PeriodDur   ANYDATA;
      MonthNumber ANYDATA;
      stat        NUMBER := 0;
      DateP       DATE;
      GraphDate   DATE;
      GraphDateForIns DATE;
      MonthDay    NUMBER := 0;
      DateAdjType NUMBER := 0; --вид переноса дат платежей на рабочий день
      ConfNotExists NUMBER := 0;

      PeriodVal   VARCHAR2(256);
      PeriodMonth VARCHAR2(256);
      PeriodUnit  NUMBER;

      GRAPHCONF DMMGRAPHCONF_DBT%ROWTYPE;
      LEG       DDL_LEG_DBT%ROWTYPE;
      TICK      DDL_TICK_DBT%ROWTYPE;

      PMGRAPH   dmmpmgraph_dbt%ROWTYPE;
      
      l_exists    NUMBER(1);

   BEGIN
      BEGIN
        SELECT * INTO GRAPHCONF FROM DMMGRAPHCONF_DBT WHERE t_dealid = objn;
      EXCEPTION
        WHEN OTHERS THEN ConfNotExists := 1;
      END;   
         
      SELECT * INTO LEG FROM DDL_LEG_DBT WHERE t_DealID = objn AND T_LEGKIND = 0 AND T_LEGID = 1;
      SELECT * INTO TICK FROM DDL_TICK_DBT WHERE t_DealID = objn;

      IF (ContextType = MMarkConst.CN_CONTEXT_GENGRAPHREC) THEN
        DELETE FROM DMMPMGRAPH_DBT WHERE t_GraphType = GraphType AND t_DocumentID = objn;
      ELSE
        DELETE FROM DMMPMGRAPH_DBT WHERE t_GraphType = GraphType AND t_DocumentID = objn AND t_Date > calcdate;
      END IF;
      
      IF (ConfNotExists = 1) THEN
        DateAdjType := TICK.t_AdjDateType;
      ELSE
        DateAdjType := GRAPHCONF.t_Princ_AdjType; 
      END IF;

      SELECT 
         CASE WHEN
            EXISTS (SELECT 1 FROM DMMPMGRAPH_DBT WHERE t_DocumentID = objn AND t_GraphType = GraphType AND t_Date = LEG.t_Maturity)
         THEN 1
         ELSE 0
         END INTO l_exists
      FROM DUAL;
      
      IF (l_exists = 0 OR (GraphType = 1 AND GRAPHCONF.t_PrincInEndTerm = 'X') OR (GraphType = 2 AND GRAPHCONF.t_PercInEndTerm = 'X')) THEN
        INSERT INTO DMMPMGRAPH_DBT(t_DocumentID, t_GraphType, t_SumCalc, t_Date) VALUES(objn, GraphType, 0, LEG.t_Maturity);
      END IF;
      
      IF (ConfNotExists = 1) THEN
        RETURN 0;
      END IF;

      IF (GraphType = 1) THEN
         PeriodVal   := GRAPHCONF.t_Princ_Period;
         PeriodMonth := GRAPHCONF.t_Princ_Month;
         PeriodUnit  := GRAPHCONF.t_Princ_PeriodUnit;
      ELSE
         PeriodVal   := GRAPHCONF.t_Perc_Period;
         PeriodMonth := GRAPHCONF.t_Perc_Month;
         PeriodUnit  := GRAPHCONF.t_Perc_PeriodUnit;
      END IF;
      stat := RSI_LOANS_SPVARIABLE.CalcFormulaExt( PeriodDur,   PeriodVal,   1, objid, objn, calcdate, NULL, ContextType);
      IF (stat = 0) THEN
        stat := RSI_LOANS_SPVARIABLE.CalcFormulaExt( MonthNumber, PeriodMonth, 1, objid, objn, calcdate, NULL, ContextType);
      END IF;
      IF (stat != 0 OR PeriodDur.accessnumber <= 0 OR MonthNumber.accessnumber <= 0) 
        THEN
        return 1;
      END IF;
      
      DateP := LEG.t_Start;

      GraphDate := DateP;
      IF (PeriodUnit = 2 AND PeriodDur.accessnumber != 1) THEN
        GraphDate := ADD_MONTHS(GraphDate, PeriodDur.accessnumber);
      END IF;
      
      WHILE (GraphDate <= LEG.t_Maturity)
      LOOP
         IF (PeriodUnit = 2) THEN
            IF (MonthNumber.accessnumber > EXTRACT(DAY FROM LAST_DAY (GraphDate))) THEN
                MonthDay := EXTRACT(DAY FROM LAST_DAY (GraphDate));
            ELSE
                MonthDay := MonthNumber.accessnumber;
            END IF;
            GraphDate := TRUNC(GraphDate, 'MM') + (MonthDay-1);
         ELSIF (PeriodUnit = 1) THEN
            GraphDate := GraphDate + PeriodDur.accessnumber;
         END IF;
         
         GraphDateForIns := GetWorkDate(GraphDate, DateAdjType);
         
         IF (GraphDateForIns < LEG.t_Maturity AND GraphDateForIns > calcdate AND GraphDate > calcdate) THEN
            INSERT INTO DMMPMGRAPH_DBT(t_DocumentID, t_GraphType, t_SumCalc, t_Date) VALUES(objn, GraphType, 0, GraphDateForIns);
         END IF;
         
         IF (PeriodUnit = 2) THEN
            GraphDate := ADD_MONTHS(GraphDate, PeriodDur.accessnumber);
         END IF;
      END LOOP;

      RETURN stat;
   END StdGenGraphRec;
   
  FUNCTION GenGraphSum(
     objid       IN NUMBER,
     objn        IN NUMBER,
     GraphType   IN NUMBER,
     calcdate    IN DATE,
     GraphDate1  IN DATE,
     GraphDate2  IN DATE,
     ContextV    IN NUMBER DEFAULT 0,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
   RETURN NUMBER
   IS
      val       NUMBER := 0;
      IsStdProc CHAR   := CHR(0);
      ProcName  VARCHAR2(512);
   BEGIN
      IsStdProc := RSB_Common.GetRegFlagValue('MMARK\ГРАФИКИ_ПЛАТЕЖЕЙ\СУММА_ГРАФИКА\ISSTDPROC'); 
      IF (IsStdProc <> CNST.SET_CHAR) THEN
        ProcName := RSB_Common.GetRegStrValue('MMARK\ГРАФИКИ_ПЛАТЕЖЕЙ\СУММА_ГРАФИКА\USERPROC');
      ELSE
        ProcName := 'RSI_MMARK.StdGenGraphSum';
      END IF;
      
      EXECUTE IMMEDIATE 'BEGIN :retval := ' || ProcName || '(:objid, :objn, :GraphType, :calcdate, :GraphDate1, :GraphDate2, :ContextV, :ContextType); END;'
       USING OUT val,
             IN objid,
             IN objn,
             IN GRaphType,
             IN calcdate,
             IN GraphDate1,
             IN GraphDate2,
             IN ContextV,
             IN ContextType;
      RETURN val;
      
      EXCEPTION WHEN OTHERS THEN RETURN -1;
   END GenGraphSum;
   
   FUNCTION StdGenGraphSum(
     objid       IN NUMBER,
     objn        IN NUMBER,
     GraphType   IN NUMBER,
     calcdate    IN DATE,
     GraphDate1  IN DATE,
     GraphDate2  IN DATE,
     ContextV    IN NUMBER DEFAULT 0,
     ContextType IN NUMBER DEFAULT MMarkConst.CN_CONTEXT_NONE
   )
   RETURN NUMBER
   IS
      IsNewCalc     NUMBER := 1; --признак первого расчета графика
      GraphRecCount NUMBER := 0;
      LastGraphDate DATE;
      PastRecSum    NUMBER := 0;
      DealSum       NUMBER := 0; --сумма сделки
      LEG           DDL_LEG_DBT%ROWTYPE;
      TICK          DDL_TICK_DBT%ROWTYPE;
      Res           NUMBER(32,2) := 0;
      PlanPrinc     NUMBER := 0;
      ExecutedPmCnt NUMBER := 0;
      ExecutedPmAmount NUMBER := 0;
      ExecutedPrincPMCnt NUMBER := 0;

   BEGIN
      SELECT MAX(t_Date) INTO LastGraphDate FROM DMMPMGRAPH_DBT WHERE t_GraphType = GraphType AND t_DocumentID = objn;
      SELECT COUNT(1)    INTO GraphRecCount FROM DMMPMGRAPH_DBT WHERE t_GraphType = GraphType AND t_DocumentID = objn;
      SELECT * INTO LEG  FROM DDL_LEG_DBT WHERE T_DEALID = objn AND T_LEGKIND = 0 AND T_LEGID = 1;
      SELECT * INTO TICK FROM DDL_TICK_DBT WHERE T_DEALID = objn;

      IF (ContextType != MMarkConst.CN_CONTEXT_GENGRAPHREC AND GraphType = 1) THEN
        SELECT NVL(COUNT(1), 0), NVL(SUM(t_BaseAmount), 0) INTO ExecutedPmCnt, ExecutedPmAmount FROM DPMPAYM_DBT WHERE t_DocKind = objid AND t_DocumentID = objn
           AND t_purpose = 10 AND t_PaymStatus not in (0, 1000);
      END IF; 

      SELECT NVL(COUNT(1), 0) INTO ExecutedPrincPMCnt FROM DPMPAYM_DBT WHERE t_DocKind = objid AND t_DocumentID = objn
           AND t_purpose = 9 AND t_PaymStatus not in (0, 1000);

      IF (GraphType = 1) THEN --ОД
         IF (ContextV = MMarkConst.CN_CONTEXT_PREPARING_DEAL) THEN
            DealSum := LEG.t_Principal;
         ELSIF (ContextV = MMarkConst.CN_CONTEXT_READIED_DEAL) THEN
            IF (ExecutedPrincPMCnt > 0) THEN
                IF (MMARK_UTL.IsCLAIMSACQ (TICK.t_DealGroup) > 0) THEN
                    DealSum := MMarkCommon.GetAccRest(objid, objn, LastGraphDate, MMarkConst.tdr_nomcost, 3, LEG.t_pfi);
                ELSIF ( (TICK.t_DealType = 12335) or (TICK.t_DealType = 12310) ) THEN   /*SVE 536761*/
                    DealSum := MMarkCommon.GetAccRest(objid, objn, LastGraphDate, MMarkConst.tdr_mainrest_tf, 1, LEG.t_pfi);
                ELSE
                    DealSum := MMarkCommon.GetAccRest(objid, objn, LastGraphDate, MMarkConst.tdr_mainrest, 1, LEG.t_pfi);
                END IF;
            ELSE
                DealSum := LEG.t_Principal;
            END IF;
         END IF;
         IF (GraphDate2 != LastGraphDate) THEN
            res := DealSum/(GraphRecCount-ExecutedPmCnt);
         ELSE
            SELECT NVL(SUM(t_SumCalc), 0) INTO PastRecSum FROM DMMPMGRAPH_DBT
            WHERE t_DocumentID = objn AND t_GraphType = GraphType AND t_Date < GraphDate2;
            res := DealSum - PastRecSum + ExecutedPmAmount;
         END IF;
      ELSE --Проценты
          SELECT NVL(SUM(t_SumCalc), 0) INTO PastRecSum FROM DMMPMGRAPH_DBT
              WHERE t_DocumentID = objn AND t_GraphType = GraphType AND t_Date < GraphDate2;
                
          res := MmarkCommon.CalcPercent(objid, objn, LEG.t_Start, GraphDate2) - PastRecSum; 
      END IF;
      IF (res IS NULL OR res < 0) THEN
            res := 0;
      END IF;
      RETURN res;
   END StdGenGraphSum;

END RSI_MMark;
/
