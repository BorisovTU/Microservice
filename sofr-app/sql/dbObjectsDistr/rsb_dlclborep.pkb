CREATE OR REPLACE PACKAGE BODY RSB_DLCLBOREP
IS

  g_do               DDLCLBO_DBT%rowtype;
  g_BegDate          DATE;
  g_EndDate          DATE;
                              
  TYPE AvrValueData_t IS RECORD (
     t_SumRub          NUMBER
     , t_Sum         NUMBER
     , t_Course      NUMBER
     , t_Rate        NUMBER
     -- DEF-52640, для вкладки 'Расшифровка стоимости ЦБ' 
     , t_NKD         NUMBER            -- курс НКД
     , t_MarketPrice     NUMBER        -- курс вида 'рыночная цена'
     , t_CourseFI     NUMBER            -- валюта цены
     , t_CourseCB     NUMBER            -- курс ЦБ РФ для валюты номинала
  );

  TYPE AvrMPData_t IS RECORD (t_MarketPrice NUMBER,
                              t_Rate        NUMBER,
                              t_Message     VARCHAR2(255),
                              t_CourseFI    NUMBER);
                                 
  TYPE ArrearData_t IS TABLE OF ddlclboarrear_dbt%rowtype;
  g_ArrearData_ins ArrearData_t := ArrearData_t();

  
  FUNCTION GetFIName(pFIID IN NUMBER)
  RETURN dfininstr_dbt.t_Name%type
  DETERMINISTIC
  IS
    v_Name dfininstr_dbt.t_Name%type;
  BEGIN
    SELECT t_Name
      INTO v_Name
      FROM dfininstr_dbt
     WHERE t_FIID = pFIID;
     
    RETURN v_Name;
  EXCEPTION
    WHEN OTHERS
      THEN RETURN NULL;
  END GetFIName;
  
  PROCEDURE CollectMetalRestAccData
  IS
  BEGIN
    INSERT INTO ddlclbometalrestac_dbt (t_SessionID, 
                                        t_MainID,
                                        t_Rest)
    SELECT g_do.t_SessionID,
           g_do.t_MainID,
           mc1.t_Sum
      FROM (SELECT SUM (NVL(RSI_RSB_FIInstr.ConvSum (
                                ABS (restac ),
                                mc.t_Currency,
                                RSI_RSB_FIINSTR.NATCUR,
                                g_EndDate,
                                7),
                            0)) t_Sum
              FROM ( select r.t_Account, r.t_Currency,r.t_Chapter,
                       rsb_account.restac (r.t_Account, r.t_Currency,g_EndDate, r.t_Chapter,NULL) as restac 
                     from  ( SELECT distinct acc.t_Account, acc.t_Currency, ACC.T_CHAPTER
                        FROM dmccateg_dbt cat, dmcaccdoc_dbt acc
                       WHERE     cat.t_LevelType = 1
                             AND cat.t_Code IN ('ДМ клиента, ВУ')
                             AND acc.t_catid = cat.t_id --acc.t_CatNum = cat.t_Number
                             AND acc.t_Chapter IN (22)
                             AND ACC.T_CLIENTCONTRID = g_do.t_ClientContrID
                             AND ACC.T_OWNER = g_do.t_PartyId
                     -- GROUP BY acc.t_Account, acc.t_Currency, ACC.T_CHAPTER
                            ) r
                      ) mc
              where restac != 0 )  mc1
     WHERE mc1.t_Sum > 0;
  END CollectMetalRestAccData;

  PROCEDURE CollectCurRestAccData
  IS
    TYPE RestAccData_t IS TABLE OF ddlclborestac_dbt%rowtype;
    v_RestAccData RestAccData_t := RestAccData_t();
  BEGIN
    SELECT g_do.t_SessionID, g_do.t_MainID, t_AccountId, t_Code_Currency, t_rest
      BULK COLLECT INTO v_RestAccData
      FROM ( SELECT acc2.t_AccountId, acc2.t_Code_Currency, 
                    RSB_ACCOUNT.RESTAC(acc2.t_Account, acc2.t_Code_Currency, g_EndDate, acc2.t_Chapter, NULL) t_rest
               FROM (SELECT DISTINCT acc.t_Code_Currency, acc.t_Account, acc.t_Chapter, acc.t_AccountId
                       FROM (SELECT t_ID
                               FROM dmccateg_dbt
                              WHERE     t_LevelType = 1
                                    AND t_Code IN ('ДС клиента, ц/б',
                                                   'Брокерский счет ДБО')) cat,
                            dmcaccdoc_dbt mc, daccount_dbt acc
                      WHERE     mc.t_CatID = cat.t_ID
                            AND mc.t_ClientContrID = g_do.t_ClientContrID
                            AND acc.t_Account = mc.t_Account
                            AND acc.t_Chapter = mc.t_Chapter
                            AND acc.t_Code_Currency = mc.t_Currency
                            AND acc.t_Open_Date <= g_EndDate
                            AND (   acc.t_Close_Date = RSI_RSB_FIINSTR.ZERO_DATE
                                 OR acc.t_Close_Date >= g_BegDate)) acc2)
     WHERE t_rest <> 0;

    IF v_RestAccData IS NOT EMPTY THEN
      BEGIN
          FORALL i IN v_RestAccData.FIRST .. v_RestAccData.LAST SAVE EXCEPTIONS
            INSERT INTO ddlclborestac_dbt
                 VALUES v_RestAccData(i);
      EXCEPTION
        WHEN OTHERS THEN NULL;
      END;

      v_RestAccData.DELETE();
    END IF;
  END CollectCurRestAccData;
  
  FUNCTION GetLastRateOnDateByType (pVN IN NUMBER, pDate IN DATE, pRateType IN NUMBER, pMarketID IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
    v_FIID   NUMBER;
  BEGIN
    SELECT t_fiid
      INTO v_FIID
     FROM (SELECT t_rateid, t_fiid
               FROM (SELECT rate.t_rateid, rate.t_sincedate, rate.t_type, rate.t_fiid
                       FROM dratedef_dbt rate
                      WHERE rate.t_otherfi = pVN /*string(Fiid_from) */
                        AND (pMarketID IS NULL OR pMarketID <= 0 OR rate.t_Market_Place = pMarketID)
                        AND t_sincedate    = (SELECT MAX (t_sincedate)
                                                FROM dratedef_dbt
                                               WHERE     t_otherfi = rate.t_otherfi
                                                     AND t_type    = rate.t_type
                                                     AND (pMarketID IS NULL OR pMarketID <= 0 OR rate.t_Market_Place = pMarketID)
                                                     AND t_sincedate <= pDate /*RateDate*/
                                             )
                     UNION
                       SELECT r.t_rateid, h.t_sincedate, r.t_type, r.t_fiid
                         FROM dratehist_dbt h, dratedef_dbt r
                        WHERE     r.t_rateid    = h.t_rateid
                              AND r.t_otherfi   = pVN  /*string(Fiid_from) */
                              AND (pMarketID IS NULL OR pMarketID <= 0 OR r.t_Market_Place = pMarketID)
                              AND h.t_sincedate = ( SELECT MAX (h2.t_sincedate)
                                                      FROM dratehist_dbt h2, dratedef_dbt r2
                                                     WHERE r2.t_rateid  = h2.t_rateid
                                                       AND r2.t_otherfi = r.t_otherfi
                                                       AND r2.t_type    = r.t_type
                                                       AND (pMarketID IS NULL OR pMarketID <= 0 OR r.t_Market_Place = pMarketID)
                                                       AND h2.t_sincedate <= pDate /*RateDate*/
                                                  )
                    ) WHERE t_type IN (pRateType)
           ORDER BY t_sincedate DESC, t_type ASC
          )
    WHERE ROWNUM = 1;
    
    RETURN v_FIID;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      IF pMarketID IS NOT NULL AND pMarketID > 0 THEN
        RETURN GetLastRateOnDateByType (pVN, pDate, pRateType, NULL);
      ELSE
        RETURN NULL;
      END IF;
  END GetLastRateOnDateByType;
  
 FUNCTION IsDateAfterWorkDayM (
                                   p_Date IN DATE,
                                   p_SinceDate IN DATE,
                                   p_identProgram IN NUMBER,
                                   p_maxDaysCnt IN NUMBER,
                                   p_CalParamArr RSI_DlCalendars.calparamarr_t)
      RETURN NUMBER deterministic
  AS
      v_recCount        NUMBER (10) := 0;
      v_calKindId       NUMBER (10) := 0;
      v_objType         NUMBER (10) := 0;
      v_objTypeFromParm NUMBER (10);
      v_DayType         NUMBER (5);
      v_operName        DDLCALENOPRS_DBT.T_NAME%TYPE;
      l_sql             VARCHAR2 (32767);
      l_whereSql        VARCHAR2 (32767);
      v_CalParamArrEmpty     RSI_DlCalendars.calparamarr_t;
      v_CalParamArr     RSI_DlCalendars.calparamarr_t;
      v_Cur             SYS_REFCURSOR;
      TYPE CalendRow IS RECORD
    (
        T_CALKINDID     NUMBER,
        T_COUNT         NUMBER
    );

    TYPE CalendTable IS TABLE OF CalendRow;

    p_CalendTable CalendTable;
  --p_ParamName       DDLCALPARAMLNK_DBT.T_KNDCODE%TYPE;
  BEGIN
     v_CalParamArrEmpty.DELETE;
     v_CalParamArr := NVL(p_CalParamArr,v_CalParamArrEmpty);

      v_objTypeFromParm :=
          CASE
              WHEN v_CalParamArr.EXISTS ('ObjectType')
              THEN
                  TO_NUMBER (v_CalParamArr ('ObjectType'))
              ELSE
                  NULL
          END;
      v_operName :=
          CASE
              WHEN v_CalParamArr.EXISTS ('Object')
              THEN
                  v_CalParamArr ('Object')
              ELSE
                  NULL
          END;
      v_DayType :=
          CASE
              WHEN v_CalParamArr.EXISTS ('DayType')
              THEN
                  TO_NUMBER (v_CalParamArr ('DayType'))
              ELSE
                  0
          END;

      SELECT COUNT (*)
        INTO v_recCount
        FROM DDLCALENOPRS_DBT dlcalenoprs
       WHERE     dlcalenoprs.t_NAME = v_operName
             AND dlcalenoprs.t_IDENTPROGRAM = p_identProgram
             AND dlcalenoprs.t_OBJTYPE =
                 CASE
                     WHEN v_objTypeFromParm > 0 THEN v_objTypeFromParm
                     ELSE dlcalenoprs.t_OBJTYPE
                 END;

      IF (v_recCount = 1)
      THEN
          SELECT t_OBJTYPE
            INTO v_objType
            FROM DDLCALENOPRS_DBT dlcalenoprs
           WHERE     dlcalenoprs.t_NAME = v_operName
                 AND dlcalenoprs.t_IDENTPROGRAM = p_identProgram
                 AND dlcalenoprs.t_OBJTYPE =
                     CASE
                         WHEN v_objTypeFromParm > 0 THEN v_objTypeFromParm
                         ELSE dlcalenoprs.t_OBJTYPE
                     END;

          v_CalParamArr ('ObjectType') := TO_CHAR (v_objType);
      END IF;

      IF (    (GREATEST (NVL(v_objTypeFromParm,-1), v_objType) = RSI_DlCalendars.DL_CALLNK_MARKET)
          AND (v_DayType = 0))
      THEN
          v_CalParamArr ('DayType') := RSI_DlCalendars.DL_CALLNK_MRKTDAY_TRADE;
      END IF;

      l_sql := 'SELECT prm.*, 0 ';

      BEGIN
          FOR param_knd IN (SELECT * FROM DDLCALPARAMKND_DBT)
          LOOP
              IF (v_CalParamArr.EXISTS (param_knd.T_CODE))
              THEN
                  l_sql :=
                         l_sql
                      || ' + COALESCE (
                 (SELECT 1
                    FROM DDLCALPARAMLNK_DBT lnk
                   WHERE     lnk.T_CALPARAMID = prm.T_ID
                         AND lnk.T_KNDCODE = '''
                      || param_knd.T_CODE
                      || '''
                         AND lnk.T_VALUE = '''
                      || v_CalParamArr (param_knd.T_CODE)
                      || '''),
                 0)';
                  l_whereSql :=
                         l_whereSql
                      || ' AND NOT EXISTS
                   (SELECT 1
                      FROM DDLCALPARAMLNK_DBT lnk
                     WHERE     lnk.T_CALPARAMID = prm.T_ID
                           AND lnk.T_KNDCODE = '''
                      || param_knd.T_CODE
                      || '''
                           AND lnk.T_VALUE <> '''
                      || v_CalParamArr (param_knd.T_CODE)
                      || ''')';
              ELSE
                  l_sql :=
                         l_sql
                      || ' + COALESCE (
                 CASE
                     WHEN NOT EXISTS
                              (SELECT 1
                                 FROM DDLCALPARAMLNK_DBT lnk
                                WHERE     lnk.T_CALPARAMID = prm.T_ID
                                      AND lnk.T_KNDCODE = '''
                      || param_knd.T_CODE
                      || ''')
                     THEN
                         1
                     ELSE
                         NULL
                 END,
                 0)';
              END IF;
          END LOOP;

          l_sql :=
                 l_sql
              || ' AS t_Count
              FROM DDLCALPARAM_DBT prm
     WHERE     T_IDENTPROGRAM = '
              || TO_CHAR (p_identProgram)
              || l_whereSql
              || ' ORDER BY t_count DESC';

          l_sql :=
              'select T_CALKINDID, T_COUNT from (' || l_sql || ') q1';

          OPEN v_Cur for l_sql;

          FETCH v_Cur BULK COLLECT INTO p_CalendTable;

          CLOSE v_Cur;

          FOR i IN p_CalendTable.FIRST .. p_CalendTable.LAST
          LOOP
              IF (p_maxDaysCnt > 0) THEN
                IF (((CASE WHEN RSI_RSBCALENDAR.IsWorkDay(p_Date, p_CalendTable(i).T_CALKINDID)  = 1
                           THEN p_Date 
                           ELSE RSI_RSBCALENDAR.GetDateAfterWorkDay(p_Date, -1, p_CalendTable(i).T_CALKINDID) 
                      END) - p_SinceDate) <= p_maxDaysCnt) THEN
                   RETURN 0;
                END IF;
              ELSE
                IF ((CASE WHEN RSI_RSBCALENDAR.IsWorkDay(p_Date, p_CalendTable(i).T_CALKINDID)  = 1
                         THEN p_Date 
                         ELSE RSI_RSBCALENDAR.GetDateAfterWorkDay(p_Date, -1, p_CalendTable(i).T_CALKINDID) 
                         END) = p_SinceDate) THEN
                   RETURN 0;
                END IF;
              END IF;
          END LOOP;

         RETURN 1;

      EXCEPTION
          WHEN OTHERS
          THEN
              NULL;
      END;

      RETURN 1;
  END;
  
  FUNCTION GetRateU (pFromFI IN NUMBER, pDate IN DATE, pToFI IN NUMBER, pRateType IN NUMBER, pMarketID IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
    v_SinceDate DATE;
    v_Rate      NUMBER := 1.0;
    v_err       NUMBER;
    p_calparamarr RSI_DlCalendars.calparamarr_t;
  BEGIN
    v_Rate := RSB_SPREPFUN.GetRateOnDateCrossDbl(pDate, pFromFI, pToFI, 0, pRateType, v_err, v_SinceDate, pMarketID);

    IF v_Rate IS NULL OR v_Rate = 0.0 THEN
      RETURN NULL;
    END IF;

    p_calparamarr('Market') := 2;
    p_calparamarr('DayType') := RSI_DlCalendars.DL_CALLNK_MRKTDAY_TRADE;
    p_calparamarr('ObjectType') := RSI_DlCalendars.DL_CALLNK_MARKET;

    IF (pRateType != RATETYPE_NKD) THEN
      IF (IsDateAfterWorkDayM(pDate, v_SinceDate, 83, 90, p_calparamarr) = 1)
        THEN
          RETURN NULL;
      END IF;
    ELSE
      IF (IsDateAfterWorkDayM(pDate, v_SinceDate, 83, 0, p_calparamarr) = 1) --в случае НКД  дата ставки курса должна совпадать с последним днем отчетного периода
        THEN
          RETURN NULL;
      END IF;
    END IF;

    RETURN v_Rate;
  END GetRateU;
  
  PROCEDURE GetRate90 (pBO IN NUMBER, pFIID IN NUMBER, pDate IN DATE, pFIID_Nom IN NUMBER, pMP OUT NUMBER, pDealCode OUT VARCHAR, pCrossFIID OUT number)
  DETERMINISTIC
  IS
    v_fiid     NUMBER;
    v_MarketID NUMBER;
    v_Rate     NUMBER;

    CURSOR deal_cur IS select /*+ result_cache */ mp,  fiid, code, t_MarketId from
                     (SELECT leg.t_cost / leg.t_principal mp, leg.t_cfi as fiid, t.t_dealcode code, t.t_MarketId
                         FROM ddl_tick_dbt t, ddl_leg_dbt leg
                        WHERE t.t_bofficekind = pBO
                          AND t.t_dealstatus != 0 --DL_PREPARING
                          AND t.t_pfi = pFIID
                          AND pDate - t.t_dealdate <= 90
                          AND pDate - t.t_dealdate >= 0
                          AND leg.t_dealid = t.t_dealid
                          AND leg.t_legkind = 0
                        order by t.t_dealdate desc, t.t_dealtime desc, t.t_dealid)
                        where rownum < 2;
  BEGIN
    OPEN deal_cur;
    FETCH deal_cur INTO pMP, v_fiid, pDealCode, v_MarketID;
    
    IF deal_cur%FOUND THEN
      pCrossFIID := v_fiid;
      IF v_fiid != RSI_RSB_FIINSTR.NATCUR THEN
        IF v_fiid != pFIID_Nom THEN
          v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, v_fiid, pFIID_Nom, pDate, 0);
        ELSE
          v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, v_fiid, RSI_RSB_FIINSTR.NATCUR, pDate, 0);
        END IF;
          
        IF v_Rate IS NULL OR v_Rate = 0.0 THEN
          pMP := NULL;
          CLOSE deal_cur;
          RETURN;
        END IF;
      END IF;
        
      IF v_fiid != pFIID_Nom THEN
        pMP := v_Rate * pMP;
      END IF;
    END IF;
  
    CLOSE deal_cur;
  
  EXCEPTION
    WHEN OTHERS
      THEN RETURN;
  END GetRate90;
  
  FUNCTION GetAvoirMarketPrice (pVN          IN  NUMBER,
                                pFVFI        IN  NUMBER,
                                pDate        IN  DATE,
                                pMarketId    IN  NUMBER)
  RETURN AvrMPData_t
  RESULT_CACHE
  IS
    v_MarketPrice  NUMBER;
    v_FindCourseFI NUMBER;
    v_DealCode     ddl_tick_dbt.t_DealCode%type;
    v_Rate         NUMBER;
    v_result       AvrMPData_t;
  BEGIN
    v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, pFVFI, RSI_RSB_FIINSTR.NATCUR, pDate, 0);
    IF v_Rate IS NOT NULL AND v_Rate <> 0.0 THEN
      IF pVN != RSI_RSB_FIINSTR.NATCUR THEN
      
        --Рыночная цена
        v_FindCourseFi := GetLastRateOnDateByType (pVN, pDate, RATETYPE_MARKET_PRICE, pMarketID);
        --DBMS_OUTPUT.PUT_LINE('fvfi='||v_FindCourseFi);
        IF v_FindCourseFi IS NOT NULL THEN
          v_MarketPrice := GetRateU (pVN, pDate, v_FindCourseFI, RATETYPE_MARKET_PRICE, pMarketID);
          IF v_MarketPrice IS NOT NULL AND pFVFI != v_FindCourseFI THEN
            v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, v_FindCourseFI, RSI_RSB_FIINSTR.NATCUR, pDate, 0);
          END IF;
        END IF;
        --DBMS_OUTPUT.PUT_LINE('mp='||v_MarketPrice);
        
        --Котировка Bloomberg
        IF v_MarketPrice IS NULL OR v_MarketPrice = 0.0 THEN
          v_FindCourseFi := GetLastRateOnDateByType (pVN, pDate, RATETYPE_BLOOMBERG_PRICE, pMarketID);
          IF v_FindCourseFi IS NOT NULL THEN
            v_MarketPrice := GetRateU (pVN, pDate, v_FindCourseFI, RATETYPE_BLOOMBERG_PRICE, pMarketID);
            IF v_MarketPrice IS NOT NULL AND pFVFI != v_FindCourseFI THEN
              v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, v_FindCourseFI, RSI_RSB_FIINSTR.NATCUR, pDate, 0);
            END IF;
          END IF;
        END IF;
        --DBMS_OUTPUT.PUT_LINE('mp_bloomberg='||v_MarketPrice);
        
        --Последняя сделка за 90 дней
        IF v_MarketPrice IS NULL OR v_MarketPrice = 0.0 THEN
          IF RSB_SECUR.GetObjAttrNumber (RSB_SECUR.OBJTYPE_AVOIRISS,
                                         NOT_USE_DEALS_ATTR_GRP,
                                         RSB_SECUR.GetMainObjAttr (RSB_SECUR.OBJTYPE_AVOIRISS, LPAD (pVN, 10, '0'), NOT_USE_DEALS_ATTR_GRP, pDate) ) != '0' THEN
            GetRate90 (RSB_SECUR.DL_SECURITYDOC, pVN, pDate, pFVFI, v_MarketPrice, v_DealCode, v_FindCourseFi); 
            IF v_MarketPrice IS NOT NULL THEN
              v_result.t_Message := 'Котировку для ц/б ' || GetFIName(pVN) || ' равную ' || v_MarketPrice || ' определил по сделке ' || v_DealCode;
            END IF;
          END IF;
        END IF;
        --DBMS_OUTPUT.PUT_LINE('mp_deal='||v_MarketPrice);
        
        --Мотивированное суждение
        IF v_MarketPrice IS NULL OR v_MarketPrice = 0.0 THEN
          v_FindCourseFi := GetLastRateOnDateByType (pVN, pDate, RATETYPE_REASONED_PRICE, pMarketID);
          IF v_FindCourseFi IS NOT NULL THEN
            v_MarketPrice := GetRateU (pVN, pDate, v_FindCourseFI, RATETYPE_REASONED_PRICE, pMarketID);
            IF v_MarketPrice IS NOT NULL AND pFVFI != v_FindCourseFI THEN
              v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, v_FindCourseFI, RSI_RSB_FIINSTR.NATCUR, pDate, 0);
            END IF;
          END IF;
        END IF;
        --DBMS_OUTPUT.PUT_LINE('mp_rp='||v_MarketPrice);
        
        IF v_MarketPrice IS NULL OR v_MarketPrice = 0.0 THEN
          v_result.t_Message := 'Не определена рыночная стоимость ц/б "' || GetFIName(pVN) || '"';
        END IF;
      ELSE
        v_MarketPrice := 1.0;
      END IF;
    END IF;
    --DBMS_OUTPUT.PUT_LINE('market_price='||v_MarketPrice);
    v_result.t_MarketPrice := v_MarketPrice;
    v_result.t_Rate        := v_Rate;
    v_result.t_CourseFI    := v_FindCourseFi;
   
    RETURN v_result;
  END GetAvoirMarketPrice;
  
  FUNCTION GetAvoirValue (pVN          IN  NUMBER,
                          pFVFI        IN  NUMBER,
                          pCount       IN  NUMBER, 
                          pDate        IN  DATE,
                          pUseNKDRate  IN  BOOLEAN,
                          pMarketId    IN  NUMBER)
  RETURN AvrValueData_t
  IS
    v_MPData AvrMPData_t;
    v_result AvrValueData_t;
    v_NKD    NUMBER; 
    v_NKDRate NUMBER := 0;      
    v_err    NUMBER;
    v_SinceDate DATE;    
  BEGIN
    v_MPData := GetAvoirMarketPrice(pVN, pFVFI, pDate, pMarketId);
    
    IF v_MPData.t_Message IS NOT NULL THEN
      RSB_SPREPFUN.AddRepError(v_MPData.t_Message);
    END IF;
    
    IF    v_MPData.t_MarketPrice IS NOT NULL
      AND v_MPData.t_MarketPrice > 0.0 
    THEN
      IF (pUseNKDRate) THEN
        v_NKDRate := GetRateU (pVN, pDate, pFVFI, RATETYPE_NKD, pMarketId);
      END IF;
      IF v_NKDRate IS NULL THEN
         v_NKD := RSI_RSB_FIINSTR.CalcNKD (pVN, pDate, pCount, 0);
      ELSE
         v_NKD := v_NKDRate * pCount;
      END IF;
         
      v_result.t_Sum    := v_MPData.t_MarketPrice * pCount + v_NKD;
      v_result.t_SumRub := v_result.t_Sum * v_MPData.t_Rate;
      v_result.t_Course := v_MPData.t_MarketPrice + v_NKD / pCount;
      v_result.t_Rate   := v_MPData.t_Rate;

      -- DEF-52640, для листа 'Расшифровка стоимости ЦБ'
      v_result.t_MarketPrice    := RSB_SPREPFUN.GetRateOnDateCrossDbl(                  -- Курс вида 'рыночная цена'
         pDate, pVN, pFVFI, 0, 1, v_err, v_SinceDate, pMarketID
      );
      v_result.t_CourseFI := v_MPData.t_CourseFI;                                       -- Валюта цены
      v_result.t_NKD    := RSB_SPREPFUN.GetRateOnDateCrossDbl(                            -- Курс вида 'НКД на одну ц/б' 
         pDate, pVN, pFVFI, 0, RATETYPE_NKD, v_err, v_SinceDate, pMarketID
      );
      v_result.t_CourseCB := RSB_SPREPFUN.GetRateOnDateCrossDbl(                        -- Курс ЦБ для валюты номинала
         pDate, pFVFI, 0, 0, 7, v_err, v_SinceDate, pMarketID
      );
    END IF;
    
    return v_result;
  END GetAvoirValue;
  
  PROCEDURE CollectAvrCostData
  IS
    v_SumRub        NUMBER := 0;
    v_Sum           NUMBER := 0;
    v_Course        NUMBER := 0;
    v_NKD           NUMBER := 0;
    v_CourseFI      NUMBER := 0;
    v_CourseCb      NUMBER := 0;
    v_AvrValue      AvrValueData_t;
    v_UseNKDRate BOOLEAN := Rsb_Common.GetRegBoolValue('SECUR\РАСЧЕТ НКД ПО КУРСУ', 0);
  BEGIN
    FOR one_acc IN (
      SELECT DISTINCT mc.t_dockind, mc.t_docid, mc.t_currency, fin.t_fiid fiid, fin.t_name fi_name,
             ABS(RSB_ACCOUNT.restac(mc.t_account, mc.t_currency, g_EndDate, mc.t_Chapter, null)) t_Qnty, 
             fin.t_facevaluefi,
             NVL((SELECT mrkt.t_Market
                    FROM ddldepset_dbt dep, ddlmarket_dbt mrkt
                   WHERE dep.t_Depositary = mc.t_Place AND mrkt.t_DepSetId = dep.t_DepSetId AND ROWNUM = 1 ),
                 -1) AS t_MarketId
        FROM dmcaccdoc_dbt mc, dfininstr_dbt fin
       WHERE mc.t_owner = g_do.t_PartyId
         AND mc.t_clientcontrid = g_do.t_ClientContrID
         AND mc.t_Chapter = 22
         AND mc.t_CatId IN (SELECT cat.t_Id
                              FROM dmccateg_dbt cat
                             WHERE cat.t_LevelType = 1 AND cat.t_Number IN (560, 561, 562))
         --AND (mc.t_dockind = RSB_SECUR.DL_VSBANNER OR mc.t_iscommon = CHR (88))
         AND mc.t_iscommon = CHR (88)
         AND fin.t_fiid(+) = mc.t_fiid)
    LOOP
      IF one_acc.t_Qnty > 0 THEN
        IF one_acc.t_DocKind = RSB_SECUR.DL_VSBANNER THEN
          INSERT INTO ddlclboavoir_dbt (
            t_SessionID, t_MainId, t_FIID, t_Name, t_CountryCode, t_INNorTIN, t_AvoirType
            , t_IssueNumber, t_ISIN, t_BAID, t_CurrCode, t_BAKind, t_Quantity, t_Value
            , t_ClientContrID, t_PartyId, t_MarketID   -- DEF-52640
          )
          SELECT g_do.t_SessionID t_SessionID,
                 g_do.t_MainID t_MainID,
                 BNR.T_BCID * -1 as t_FIID,
                 issuer.t_ShortName,
                 NVL (
                    (CASE
                        WHEN EXISTS (SELECT 1 FROM dpartyown_dbt WHERE t_PartyId = issuer.t_PartyId AND t_PartyKind = PTK_INTERNATIONAL_ORG)
                        THEN
                           '998'
                        ELSE
                           (SELECT t_CodeNum3
                              FROM dcountry_dbt
                             WHERE     t_CodeLat3 <> CHR (1)
                                   AND t_CodeLat3 = issuer.t_NRCountry)
                     END),
                    '999')
                    t_Country,
                 COALESCE (RSB_SECUR.SC_GetObjCodeOnDate (3, 16, issuer.t_PartyId, g_EndDate),
                           RSB_SECUR.SC_GetObjCodeOnDate (3, 62, issuer.t_PartyId, g_EndDate),
                           RSB_SECUR.SC_GetObjCodeOnDate (3, 33, issuer.t_PartyId, g_EndDate),
                           RSB_SECUR.SC_GetObjCodeOnDate (3, 69, issuer.t_PartyId, g_EndDate)) -- LEI
                    t_INNorTIN,
                 RSB_SECUR.GetObjAttrName (12, 1, RSB_SECUR.GetMainObjAttr (12, LPAD (fin.t_FIID, 10, '0'), 1, g_EndDate))
                    t_avoirtype,
                 TRIM (BNR.T_BCSERIES) || ' ' || TRIM (BNR.T_BCNUMBER) AS t_IssueNumber,
                 '' AS t_ISIN,
                 '' AS t_BAID,
                 cur.t_FI_Code AS t_CurrCode,
                 3 AS t_BAKind,
                 one_acc.t_Qnty as t_Quantity,
                 RSI_RSB_FIInstr.ConvSum(RSB_BILL.GetVABnrCostPFI(BNR.T_BCID, g_EndDate), leg.t_pfi, RSI_RSB_FIINSTR.NATCUR, g_EndDate, 0) as t_Cost,
                 g_do.t_ClientContrID, g_do.t_PartyId, one_acc.t_MarketID
            FROM DVSBANNER_DBT bnr,
                 DPARTY_DBT issuer,
                 DDL_LEG_DBT leg,
                 dfininstr_dbt cur,
                 dfininstr_dbt fin
           WHERE     t_bcid = one_acc.t_DocId
                 AND issuer.T_PARTYID = BNR.T_ISSUER
                 AND leg.T_DEALID = BNR.T_BCID
                 AND LEG.T_LEGID = 0
                 AND LEG.T_LEGKIND = 1
                 AND cur.T_FIID = leg.t_pfi
                 AND FIN.T_FIID = BNR.T_FIID;

        ELSE
          v_AvrValue := GetAvoirValue (one_acc.t_Currency, one_acc.t_FaceValueFI, one_acc.t_Qnty, g_EndDate, v_UseNKDRate, one_acc.t_MarketID);
          IF v_AvrValue.t_SumRub IS NOT NULL THEN
             v_Sum := v_AvrValue.t_Sum;
             v_SumRub := v_AvrValue.t_SumRub;

             -- DEF-52640, для листа 'Расшифровка стоимости ЦБ' 
             v_Course := v_AvrValue.t_MarketPrice;        -- Курс вида 'рыночная цена ц/б'
             v_NKD := v_AvrValue.t_NKD;                -- курс вида 'НКД на одну ц/б'
             v_CourseFI := v_AvrValue.t_CourseFI;        -- валюта цены
             v_CourseCb := v_AvrValue.t_CourseCb;        -- курс ЦБ РФ для валюты номинала
          ELSE
             v_Sum        := 0;
             v_SumRub   := 0;
             v_Course     := 0;
             v_NKD         := 0;
             v_CourseFI  := 0;
             v_CourseCb := 0; 
          END IF;
        
          INSERT INTO ddlclboavoir_dbt (
            t_SessionID, t_MainId, t_FIID, t_Name, t_CountryCode, t_INNorTIN, t_AvoirType
            , t_IssueNumber, t_ISIN, t_BAID, t_CurrCode, t_BAKind, t_Quantity, t_Value

            -- DEF-52640, для листа 'Расшифровка стоимости ЦБ' 
            , t_ClientContrID, t_PartyId, t_MarketID
            , t_Price
            , t_Course        -- Курс вида 'рыночная цена ц/б'
            , t_NKD        -- курс вида 'НКД на одну ц/б'
            , t_CourseFI    -- валюта цены
            , t_CourseCB        -- курс ЦБ РФ для валюты номинала
          )
          SELECT g_do.t_SessionID t_SessionID,
                 g_do.t_MainID t_MainID,
                 fi.t_FIID,
                 issuer.t_ShortName,
                 NVL ( (CASE
                           WHEN EXISTS (SELECT 1 FROM dpartyown_dbt WHERE t_PartyId = issuer.t_PartyId AND t_PartyKind = PTK_INTERNATIONAL_ORG )
                           THEN '998'
                           ELSE
                              (SELECT t_CodeNum3
                                 FROM dcountry_dbt
                                WHERE t_CodeLat3 <> CHR (1) AND t_CodeLat3 = issuer.t_NRCountry)
                        END),
                      '999')
                    t_Country,
                 COALESCE (RSB_SECUR.SC_GetObjCodeOnDate (3, 16, issuer.t_PartyId, g_EndDate),
                           RSB_SECUR.SC_GetObjCodeOnDate (3, 62, issuer.t_PartyId, g_EndDate),
                           RSB_SECUR.SC_GetObjCodeOnDate (3, 33, issuer.t_PartyId, g_EndDate),
                           RSB_SECUR.SC_GetObjCodeOnDate (3, 69, issuer.t_PartyId, g_EndDate)) -- LEI
                    t_INNorTIN,
                 rsb_secur.GetObjAttrName (12, 1, rsb_secur.GetMainObjAttr (12, LPAD (fi.t_FIID, 10, '0'), 1, g_EndDate))
                    t_avoirtype,
                 DECODE (avr.t_LSIN, CHR (1), fi.t_FI_Code, avr.t_LSIN) t_IssueNumber,
                 DECODE (avr.t_ISIN, CHR (1), avr.t_LSIN, avr.t_ISIN) t_ISIN,
                 DECODE (ba.t_ISIN, CHR (1), ba.t_FI_Code, ba.t_ISIN) t_BAID,
                 (SELECT t_FI_Code
                    FROM dfininstr_dbt
                   WHERE t_FIID = NVL (ba.t_FaceValueFI, fi.t_FaceValueFI))
                    t_CurrCode,
                 ba.t_Kind t_BAKind,
                 one_acc.t_Qnty t_Quantity,
                 v_SumRub t_Value
                 -- DEF-52640, для листа 'Расшифровка стоимости ЦБ' 
                 , g_do.t_ClientContrID, g_do.t_PartyId, one_acc.t_MarketID
                 , v_Sum
                 , v_Course     -- Курс вида 'рыночная цена ц/б'
                 , v_NKD    -- курс вида 'НКД на одну ц/б'
                 , v_CourseFI   -- валюта цены
                 , v_CourseCb    -- курс ЦБ РФ для валюты номинала
            FROM dfininstr_dbt fi,
                 davoiriss_dbt avr,
                 dparty_dbt issuer,
                 (SELECT fi.t_FIID t_recid,
                         bafi.t_FI_Code,
                         bafi.T_FaceValueFI,
                         (CASE rsb_fiinstr.FI_AvrKindsGetRoot (RSI_RSB_FIINSTR.FIKIND_AVOIRISS, bafi.t_AvoirKind)
                             WHEN RSI_RSB_FIINSTR.AVOIRKIND_SHARE THEN 0
                             WHEN RSI_RSB_FIINSTR.AVOIRKIND_BOND THEN 1
                             WHEN RSI_RSB_FIINSTR.AVOIRKIND_INVESTMENT_SHARE THEN 2
                             ELSE 3
                         END)
                            t_Kind,
                         baavr.t_ISIN
                    FROM dfininstr_dbt fi, dfininstr_dbt bafi, davoiriss_dbt baavr
                   WHERE     --rsb_fiinstr.FI_AvrKindsGetRoot (RSI_RSB_FIINSTR.FIKIND_AVOIRISS, FI.T_AVOIRKIND) = RSI_RSB_FIINSTR.AVOIRKIND_DEPOS_RECEIPT
                         bafi.t_FIID = fi.t_ParentFI
                         AND baavr.t_FIID = bafi.t_FIID) ba
            WHERE    fi.t_FIID = one_acc.t_Currency
                 AND avr.t_FIID = fi.t_FIID
                 AND issuer.t_PartyId = fi.t_Issuer
                 AND ba.t_recid(+) = fi.t_FIID;
        END IF;
      END IF;
    END LOOP;   
  END CollectAvrCostData;
  
  FUNCTION GetAnyLastCourseOnDate (pRateDate IN DATE, pFIID IN NUMBER)
  RETURN NUMBER
  IS
    v_rateid NUMBER;
    v_type   NUMBER;
    v_FIID   NUMBER;
    v_Course NUMBER;
  BEGIN
    SELECT t_rateid, t_type, t_fiid
      INTO v_rateid, v_type, v_fiid
      FROM (SELECT t_rateid, t_type, t_fiid
              FROM (SELECT rate.t_rateid, rate.t_sincedate, rate.t_type, rate.t_fiid
                      FROM dratedef_dbt rate
                     WHERE rate.t_otherfi = pFIID
                       AND t_sincedate = (SELECT MAX(t_sincedate)
                                            FROM dratedef_dbt
                                           WHERE t_otherfi = rate.t_otherfi
                                             AND t_type = rate.t_type
                                             AND t_sincedate <= pRateDate
                                         )
                     UNION
                    SELECT r.t_rateid, h.t_sincedate, r.t_type, r.t_fiid
                      FROM dratehist_dbt h, dratedef_dbt r
                     WHERE r.t_rateid = h.t_rateid
                       AND r.t_otherfi = pFIID
                       AND h.t_sincedate = ( SELECT MAX(h2.t_sincedate)
                                               FROM dratehist_dbt h2, dratedef_dbt r2
                                              WHERE r2.t_rateid = h2.t_rateid
                                                AND r2.t_otherfi = r.t_otherfi
                                                AND r2.t_type = r.t_type
                                                AND h2.t_sincedate <= pRateDate
                                           )
                   )
            ORDER BY t_sincedate DESC
           )
    WHERE ROWNUM = 1;
    
    v_Course := RSI_RSB_FIINSTR.ConvSumType (1.0, pFIID, v_FIID, v_Type, pRateDate);
    IF v_Course IS NOT NULL AND v_FIID != RSI_RSB_FIINSTR.NATCUR THEN 
      v_Course := RSI_RSB_FIINSTR.ConvSum (v_Course, v_FIID, RSI_RSB_FIINSTR.NATCUR, pRateDate);
    END IF;
    
    IF v_Course IS NULL THEN
      RSB_SPREPFUN.AddRepError(RSB_SPREPFUN.GetCurrencyConvertErrorMsg(pFIID, v_FIID, pRateDate));
      RETURN NULL;
    END IF;
    
    RETURN v_Course;
  EXCEPTION
    WHEN NO_DATA_FOUND
      THEN RETURN NULL;
  END GetAnyLastCourseOnDate;
  
  PROCEDURE CollectPFIData
  IS
    c_MPType CONSTANT NUMBER := Rsb_Common.GetRegIntValue('ПРОИЗВОДНЫЕ ИНСТРУМЕНТЫ\ВИД КУРСА "РЫНОЧНАЯ ЦЕНА"', 0);
    
    TYPE PFIData_t IS TABLE OF ddlclbopfi_dbt%rowtype;
    PFIData_ins PFIData_t := PFIData_t();
    v_PFIData ddlclbopfi_dbt%rowtype;
    
    v_PriceRub        NUMBER;
    v_MaxLastPriceRub NUMBER;
    v_Sum             NUMBER;
    v_BAPriceRub      NUMBER;
    v_MarketPrice     NUMBER;
    v_Rate            NUMBER;
  BEGIN
    FOR one_rec IN ( SELECT ndeal.t_ID,
                            ndeal.t_DVKind,
                            ndeal.t_DocKind,
                            case when ndeal.t_Type in (RSB_DERIVATIVES.ALG_DV_BUY, RSB_DERIVATIVES.ALG_DV_SB, RSB_DERIVATIVES.ALG_DV_FLOAT_FIX) then 'B'
                                 when ndeal.t_Type in (RSB_DERIVATIVES.ALG_DV_SALE, RSB_DERIVATIVES.ALG_DV_BS, RSB_DERIVATIVES.ALG_DV_FIX_FLOAT) then 'S' else '' end t_Type,
                            case when ndeal.t_DVKind = RSB_DERIVATIVES.DV_OPTION then 'Опцион'
                                 when ndeal.t_DVKind = RSB_DERIVATIVES.DV_FORWARD AND ndeal.t_Sector = 'X' AND ndeal.t_MarketKind = 2 then 'Фьючерс'
                                 when ndeal.t_DVKind = RSB_DERIVATIVES.DV_FORWARD then 'Форвард'
                                 when ndeal.t_DVKind = RSB_DERIVATIVES.DV_CURSWAP then 'Валютный своп'
                                 when ndeal.t_DVKind = RSB_DERIVATIVES.DV_CURSWAP_FX then 'Своп'
                                 when ndeal.t_DVKind = RSB_DERIVATIVES.DV_PCTSWAP AND nfi2.t_FIID <> nfi.t_FIID then 'CIRS'
                                 when ndeal.t_DVKind = RSB_DERIVATIVES.DV_PCTSWAP then 'IRS' else 'Иное' end t_PFIType,
                            nfi.t_Amount,
                            case when (   (ndeal.t_DVKind = RSB_DERIVATIVES.DV_CURSWAP_FX and ndeal.t_MarketKind = DV_MARKETKIND_CURRENCY)
                                       or (ndeal.t_DVKind = RSB_DERIVATIVES.DV_CURSWAP and ndeal.t_Sector = 'X' and ndeal.t_MarketKind = DV_MARKETKIND_CURRENCY)) then 'S'
                                 else '' end t_ExtCode,
                            case when ndeal.t_Sector = 'X' and ndeal.t_MarketKind > 0 then NVL((SELECT t_ShortName FROM dparty_dbt WHERE t_PartyID = ndeal.t_Contractor),'') else '' end t_ExtName,
                            fin.t_Name t_BAName,
                            fin.t_FIID t_BA_FIID,
                            fin.t_FI_Kind,
                            fin.t_FaceValueFI,
                            nfi.t_Price,
                            nfi.t_PriceFIID,
                            0 t_MaxLastPrice,
                            0 t_PosAmount,
                            ndeal.t_Forvard
                       FROM ddvndeal_dbt ndeal LEFT OUTER JOIN ddvnfi_dbt nfi2 ON nfi2.t_DealID = ndeal.t_ID AND nfi2.t_Type = 2, ddvnfi_dbt nfi, dfininstr_dbt fin, doproper_dbt oper
                      WHERE ndeal.t_Client = g_do.t_PartyID
                        AND ndeal.t_ClientContr = g_do.t_ClientContrID
                        AND ndeal.t_Date <= g_EndDate
                        AND nfi.t_DealID = ndeal.t_ID
                        AND nfi.t_Type = case when ndeal.t_Forvard = 'X' then 1 else 0 end
                        AND fin.t_FIID = nfi.t_FIID
                        AND oper.t_DocKind = ndeal.t_DocKind
                        AND oper.t_DocumentID = lpad(ndeal.t_ID, 34, '0')
                        AND ndeal.t_State > 0
                        AND EXISTS (SELECT 1
                                      FROM dpmpaym_dbt paym
                                     WHERE paym.t_DocKind = ndeal.t_DocKind
                                       AND paym.t_DocumentID = ndeal.t_ID
                                       AND paym.t_ValueDate > g_EndDate
                                       AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
                        AND (    ndeal.t_DVKind IN (RSB_DERIVATIVES.DV_FORWARD, RSB_DERIVATIVES.DV_OPTION, RSB_DERIVATIVES.DV_PCTSWAP)
                             OR (    ndeal.t_DVKind IN (RSB_DERIVATIVES.DV_CURSWAP, RSB_DERIVATIVES.DV_CURSWAP_FX)
                                 AND NOT EXISTS (SELECT 1
                                                   FROM dpmpaym_dbt paym
                                                  WHERE paym.t_DocKind = ndeal.t_DocKind
                                                    AND paym.t_DocumentID = ndeal.t_ID
                                                    AND paym.t_Purpose IN (RSB_PAYMENT.BAi, RSB_PAYMENT.CAi)   -- Платежи по 1ч
                                                    AND paym.t_ValueDate > g_EndDate
                                                    AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
                                 AND EXISTS (SELECT 1
                                               FROM dpmpaym_dbt paym
                                              WHERE paym.t_DocKind = ndeal.t_DocKind
                                                AND paym.t_DocumentID = ndeal.t_ID
                                                AND paym.t_Purpose IN (RSB_PAYMENT.BRi, RSB_PAYMENT.CRi)   -- Платежи по 2ч
                                                AND paym.t_ValueDate > g_EndDate
                                                AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
                                )
                            )
                     UNION
                     SELECT deal.t_ID,
                            fin.t_AvoirKind t_DVKind,
                            RSB_DERIVATIVES.DL_DVDEAL t_DocKind,
                            deal.t_Type,
                            case when fin.t_AvoirKind = RSB_DERIVATIVES.DV_DERIVATIVE_FUTURES then 'Фьючерс' else 'Опцион' end t_PFIType,
                            deal.t_Amount,
                            NVL(rsb_secur.SC_GetObjCodeOnDate(CNST.OBJTYPE_FININSTR, 11, deal.t_ID, g_EndDate),'') t_ExtCode,
                            NVL((SELECT t_ShortName FROM dparty_dbt WHERE t_PartyID = fin.t_Issuer),'') t_ExtName,
                            base.t_Name t_BAName,
                            base.t_FIID t_BA_FIID,
                            base.t_FI_Kind,
                            base.t_FaceValueFI,
                            case when fin.t_AvoirKind = RSB_DERIVATIVES.DV_DERIVATIVE_FUTURES then deal.t_Price else deriv.t_Strike end t_Price,
                            case when fin.t_AvoirKind = RSB_DERIVATIVES.DV_DERIVATIVE_FUTURES AND base.t_FI_Kind = 3 THEN fin.t_ParentFI
                                  when fin.t_AvoirKind = RSB_DERIVATIVES.DV_DERIVATIVE_FUTURES then deriv.t_TickFIID
                                  else deriv.t_StrikeFIID
                            end t_PriceFIID,
                            FIRST_VALUE(deal.t_Price) OVER (PARTITION BY deal.t_FIID ORDER BY deal.t_FIID, deal.t_Date desc, deal.t_Time desc) t_MaxLastPrice,
                            abs(fiturn.t_LongPosition - fiturn.t_ShortPosition) t_PosAmount,
                            case when base.t_FIID <> fin.t_FaceValueFI then 'X' else chr(0) end t_Forvard
                       FROM ddvdeal_dbt deal, dfininstr_dbt fin, dfininstr_dbt base, dfideriv_dbt deriv, ddvfiturn_dbt fiturn
                      WHERE deal.t_Client = g_do.t_PartyID
                        AND deal.t_ClientContr = g_do.t_ClientContrID
                        AND deal.t_Date <= g_EndDate
                        AND deal.t_Type IN ('B','S')
                        AND deal.t_State = 1
                        AND fin.t_FIID = deal.t_FIID
                        AND deriv.t_FIID = deal.t_FIID
                        and base.t_FIID = case when NVL((SELECT fin2.t_FI_Kind FROM dfininstr_dbt fin2 WHERE fin2.t_FIID = fin.t_FaceValueFI), -1) = RSI_RSB_FIINSTR.FIKIND_DERIVATIVE
                                               then NVL((SELECT fin2.t_FaceValueFI FROM dfininstr_dbt fin2 WHERE fin2.t_FIID = fin.t_FaceValueFI), -1)
                                               else fin.t_FaceValueFI end
                        AND deriv.t_InCirculationDate <= g_EndDate
                        AND fiturn.t_FIID = deal.t_FIID
                        AND fiturn.t_Client = deal.t_Client
                        AND fiturn.t_Broker = deal.t_Broker
                        AND fiturn.t_ClientContr = deal.t_ClientContr
                        AND fiturn.t_BrokerContr = deal.t_BrokerContr
                        AND fiturn.t_Department = deal.t_Department
                        AND fiturn.t_GenagrID = deal.t_GenagrID
                        AND fiturn.t_Date = g_EndDate)
    LOOP
      IF    one_rec.t_DocKind = RSB_DERIVATIVES.DL_DVDEAL
        AND one_rec.t_DVKind = RSB_DERIVATIVES.DV_DERIVATIVE_FUTURES
      THEN
        IF one_rec.t_PriceFIID != RSI_RSB_FIINSTR.NATCUR THEN
          IF RSB_SPREPFUN.SmartConvertSum(v_PriceRub, one_rec.t_Price, g_EndDate, one_rec.t_PriceFIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
          THEN
            CONTINUE;
          END IF;
          
          IF RSB_SPREPFUN.SmartConvertSum(v_MaxLastPriceRub, one_rec.t_MaxLastPrice, g_EndDate, one_rec.t_PriceFIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
          THEN
            CONTINUE;
          END IF;
        ELSE
          v_PriceRub := one_rec.t_Price;
          v_MaxLastPriceRub := one_rec.t_MaxLastPrice;
        END IF;
        
        v_Sum := (v_MaxLastPriceRub - v_PriceRub) * one_rec.t_PosAmount;

      ELSIF one_rec.t_DocKind = RSB_DERIVATIVES.DL_DVNDEAL
        AND one_rec.t_DVKind = RSB_DERIVATIVES.DV_FORWARD
      THEN
        IF one_rec.t_PriceFIID != RSI_RSB_FIINSTR.NATCUR THEN
          IF RSB_SPREPFUN.SmartConvertSum(v_PriceRub, one_rec.t_Price, g_EndDate, one_rec.t_PriceFIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
          THEN
            CONTINUE;
          END IF;
        ELSE
          v_PriceRub := one_rec.t_Price;
        END IF;

        IF   one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY 
          OR one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_METAL
        THEN
          IF RSB_SPREPFUN.SmartConvertSum(v_BAPriceRub, 1.0, g_EndDate, one_rec.t_BA_FIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
          THEN
            CONTINUE;
          END IF;        
        ELSIF  one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS THEN
          v_MarketPrice := RSI_RSB_FIINSTR.ConvSumType (1.0, one_rec.t_BA_FIID, one_rec.t_FaceValueFI, c_MPType, g_EndDate);
          IF v_MarketPrice IS NOT NULL THEN
            IF one_rec.t_FaceValueFI != RSI_RSB_FIINSTR.NATCUR THEN
              IF RSB_SPREPFUN.SmartConvertSum(v_Rate, 1.0, g_EndDate, one_rec.t_FaceValueFI, RSI_RSB_FIINSTR.NATCUR, 1) != 0
              THEN
                CONTINUE;
              END IF;              
            ELSE
              v_Rate := 1;
            END IF;
            
            v_BAPriceRub := v_Rate * v_MarketPrice;
          ELSE
            v_BAPriceRub := 0;
            RSB_SPREPFUN.AddRepError('Не определена рыночная стоимость ц/б "' || one_rec.t_BAName || '"');
            CONTINUE;
          END IF;
        ELSE
          v_BAPriceRub := GetAnyLastCourseOnDate (g_EndDate, one_rec.t_BA_FIID);
          IF v_BAPriceRub IS NULL THEN
            CONTINUE;
          END IF;
        END IF;
        
        v_Sum := (v_BAPriceRub - v_PriceRub) * one_rec.t_Amount;
 
      ELSIF (   one_rec.t_DocKind = RSB_DERIVATIVES.DL_DVDEAL
            AND one_rec.t_DVKind = RSB_DERIVATIVES.DV_DERIVATIVE_OPTION)
         OR (   one_rec.t_DocKind = RSB_DERIVATIVES.DL_DVNDEAL
            AND one_rec.t_DVKind = RSB_DERIVATIVES.DV_OPTION)
      THEN
        IF one_rec.t_PriceFIID != RSI_RSB_FIINSTR.NATCUR THEN
          IF RSB_SPREPFUN.SmartConvertSum(v_PriceRub, one_rec.t_Price, g_EndDate, one_rec.t_PriceFIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
          THEN
            CONTINUE;
          END IF;
        ELSE
          v_PriceRub := one_rec.t_Price;
        END IF;

        IF   one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY 
          OR one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_METAL
        THEN
          IF RSB_SPREPFUN.SmartConvertSum(v_BAPriceRub, 1.0, g_EndDate, one_rec.t_BA_FIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
          THEN
            CONTINUE;
          END IF;
        ELSIF  one_rec.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS THEN
          v_MarketPrice := RSI_RSB_FIINSTR.ConvSumType (1.0, one_rec.t_BA_FIID, one_rec.t_FaceValueFI, c_MPType, g_EndDate);
          IF v_MarketPrice IS NOT NULL THEN
            IF one_rec.t_FaceValueFI != RSI_RSB_FIINSTR.NATCUR THEN
              IF RSB_SPREPFUN.SmartConvertSum(v_Rate, 1.0, g_EndDate, one_rec.t_FaceValueFI, RSI_RSB_FIINSTR.NATCUR, 1) != 0
              THEN
                CONTINUE;
              END IF;              
            ELSE
              v_Rate := 1;
            END IF;
            
            v_BAPriceRub := v_Rate * v_MarketPrice;
          ELSE
            v_BAPriceRub := 0;
            RSB_SPREPFUN.AddRepError('Не определена рыночная стоимость ц/б "' || one_rec.t_BAName || '"');
            CONTINUE;
          END IF;
        ELSE
          v_BAPriceRub := GetAnyLastCourseOnDate (g_EndDate, one_rec.t_BA_FIID);
          IF v_BAPriceRub IS NULL THEN
            CONTINUE;
          END IF;
        END IF;
        
        v_Sum := (v_BAPriceRub - v_PriceRub) * one_rec.t_Amount;

      ELSIF one_rec.t_DVKind = RSB_DERIVATIVES.DV_PCTSWAP THEN
        v_Sum := 0;
        
      ELSIF one_rec.t_DVKind = RSB_DERIVATIVES.DV_CURSWAP
         OR one_rec.t_DVKind = RSB_DERIVATIVES.DV_CURSWAP_FX
      THEN
        IF RSB_SPREPFUN.SmartConvertSum(v_Sum, one_rec.t_Amount, g_EndDate, one_rec.t_BA_FIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
        THEN
          v_Sum := 0;
          CONTINUE;
        END IF;
      END IF;
    
      v_PFIData.t_SessionID := g_do.t_SessionID;
      v_PFIData.t_MainID    := g_do.t_MainID;
      v_PFIData.t_DealID    := one_rec.t_ID;
      v_PFIData.t_DocKind   := one_rec.t_DocKind;
      v_PFIData.t_PFIType   := one_rec.t_PFIType;
      v_PFIData.t_GroupFIID := CASE
                                 WHEN  one_rec.t_DocKind = RSB_DERIVATIVES.DL_DVNDEAL
                                   AND one_rec.t_DVKind IN (RSB_DERIVATIVES.DV_OPTION,
                                                            RSB_DERIVATIVES.DV_CURSWAP,
                                                            RSB_DERIVATIVES.DV_CURSWAP_FX)
                                 THEN one_rec.t_BA_FIID
                                 ELSE 0
                               END; -- Требуется ли группировка по ФИ
      v_PFIData.t_ExtCode   := one_rec.t_ExtCode;
      v_PFIData.t_ExtName   := one_rec.t_ExtName;
      v_PFIData.t_DealSide  := one_rec.t_Type;
      v_PFIData.t_Quantity  := CASE
                                 WHEN one_rec.t_DocKind = RSB_DERIVATIVES.DL_DVDEAL
                                   OR one_rec.t_DVKind IN (RSB_DERIVATIVES.DV_CURSWAP, RSB_DERIVATIVES.DV_CURSWAP_FX)
                                 THEN one_rec.t_Amount
                                 ELSE 1
                               END;
      v_PFIData.t_Value     := v_Sum;
      
      PFIData_ins.Extend();
      PFIData_ins(PFIData_ins.LAST) := v_PFIData;
    END LOOP;
    
    IF PFIData_ins IS NOT EMPTY THEN
      FORALL i IN PFIData_ins.FIRST .. PFIData_ins.LAST
        INSERT INTO ddlclbopfi_dbt
             VALUES PFIData_ins(i);

      PFIData_ins.DELETE();
    END IF;
  END CollectPFIData;
  
  PROCEDURE GetSumRQByDeal (pDealID   IN  NUMBER,
                            pIsREPO   IN  NUMBER,
                            pIsSale   IN  NUMBER,
                            pSumDt    OUT NUMBER,
                            pSumKt    OUT NUMBER,
                            pMarketID IN  NUMBER)
  IS
    v_s         NUMBER;
    v_sum_pay1  NUMBER := 0; -- сумма ТО типа "оплата" + "аванс" по 1-й части
    v_sum_pay2  NUMBER := 0; -- сумма ТО типа "оплата" + "аванс" по 2-й части
    v_sum_del1  NUMBER := 0; -- сумма ТО типа "поставка" по 1-й части
    v_sum_del2  NUMBER := 0; -- сумма ТО типа "поставка" по 2-й части
    v_sum_com   NUMBER := 0; -- сумма ТО типа "комиссия"
    v_sum_Perc  NUMBER := 0; -- сумма ТО типа "проценты"
    v_Kind_Perc NUMBER := -1;-- вид ТО процентов
    v_UseNKDRate BOOLEAN := Rsb_Common.GetRegBoolValue('SECUR\РАСЧЕТ НКД ПО КУРСУ', 0);
  BEGIN
    FOR one_rq IN (SELECT rq.t_Type, rq.t_Kind, rq.t_DealPart, rq.t_Amount, rq.t_FIID, fin.t_FaceValueFI
                     FROM v_rqhistex rq, dfininstr_dbt fin
                    WHERE     fin.t_FIID = rq.t_FIID
                          AND rq.t_State NOT IN (RSI_DLRQ.DLRQ_STATE_EXEC, RSI_DLRQ.DLRQ_STATE_REJECT)
                          AND rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMENT, RSI_DLRQ.DLRQ_TYPE_DELIVERY, RSI_DLRQ.DLRQ_TYPE_INCREPO)
                          AND rq.t_DocId = pDealID
                          AND rq.t_DocKind = RSB_SECUR.DL_SECURITYDOC
                          and rq.t_instance = (select MAX(h1.t_instance) from v_rqhistex h1 where h1.t_rqid = rq.t_rqid and h1.t_changedate <= g_EndDate))
    LOOP
      IF   one_rq.t_Type = RSI_DLRQ.DLRQ_TYPE_PAYMENT
        OR one_rq.t_Type = RSI_DLRQ.DLRQ_TYPE_INCREPO
      THEN
        IF one_rq.t_FIID = RSI_RSB_FIINSTR.NATCUR THEN
          v_s := one_rq.t_Amount;
        ELSE
          v_s := RSI_RSB_FIINSTR.ConvSum (one_rq.t_Amount, one_rq.t_FIID, RSI_RSB_FIINSTR.NATCUR, g_EndDate);
        END IF;
        
        IF one_rq.t_Type = RSI_DLRQ.DLRQ_TYPE_PAYMENT THEN
          IF one_rq.t_DealPart = 1 THEN
            v_sum_pay1 := v_sum_pay1 + v_s;
          ELSE
            v_sum_pay2 := v_sum_pay2 + v_s;
          END IF;
        ELSE
          v_sum_Perc := v_sum_Perc + v_s; 
          v_Kind_Perc := one_rq.t_Kind;
        END IF;
      ELSE
        v_s := GetAvoirValue (one_rq.t_FIID,
                              one_rq.t_FaceValueFI,
                              one_rq.t_Amount,
                              g_EndDate,
                              v_UseNKDRate,
                              pMarketID).t_SumRub;
                              
        IF v_s IS NOT NULL THEN
          IF one_rq.t_DealPart = 1 THEN
            v_sum_del1 := v_sum_del1 + v_s;
          ELSE
            v_sum_del2 := v_sum_del2 + v_s;
          END IF;
        END IF;
      END IF;
    END LOOP;
    
    IF (v_sum_pay1 <> 0 OR v_sum_del1 <> 0) AND pIsREPO = 1 THEN
      pSumDt := 0;
      pSumKt := 0;
      RETURN;
    END IF;
    
    FOR one_com IN (SELECT rq.t_Type, rq.t_Kind, rq.t_DealPart, rq.t_Amount, rq.t_FIID
                      FROM ddlrq_dbt rq, ddlcomis_dbt dlcomis
                     WHERE     rq.t_DocId = pDealID
                           AND rq.t_DocKind = RSB_SECUR.DL_SECURITYDOC
                           AND rq.t_Type = RSI_DLRQ.DLRQ_TYPE_COMISS
                           AND rq.t_SourceObjKind = 4721
                           AND rq.t_SourceObjID   = dlcomis.t_ID
                           AND dlcomis.t_DocKind = RSB_SECUR.DL_SECURITYDOC
                           AND dlcomis.t_DocID   = pDealID
                           AND (dlcomis.t_FactPayDate = to_date('01010001', 'ddmmyyyy') or dlcomis.t_FactPayDate > g_EndDate ))
    LOOP
      IF one_com.t_FIID = RSI_RSB_FIINSTR.NATCUR THEN
        v_s := one_com.t_Amount;
      ELSE
        v_s := RSI_RSB_FIINSTR.ConvSum (one_com.t_Amount, one_com.t_FIID, RSI_RSB_FIINSTR.NATCUR, g_EndDate);
      END IF;
      
      v_sum_com := v_sum_com + v_s;
    END LOOP;
    
    IF pIsREPO = 1 AND pIsSale = 1 THEN
      pSumDt := v_sum_del2;
      pSumKt := v_sum_pay2 + v_sum_com;

      IF v_Kind_Perc = RSI_DLRQ.DLRQ_KIND_REQUEST THEN -- Требование
        pSumDt := pSumDt + v_sum_Perc;
      ELSIF v_Kind_Perc = RSI_DLRQ.DLRQ_KIND_COMMIT THEN
        pSumKt := pSumKt + v_sum_Perc;
      END IF;
      
    ELSIF pIsREPO = 1 THEN
      pSumDt := v_sum_pay2;
      pSumKt := v_sum_del2 + v_sum_com;

      IF v_Kind_Perc = RSI_DLRQ.DLRQ_KIND_REQUEST THEN -- Требование
        pSumDt := pSumDt + v_sum_Perc;
      ELSIF v_Kind_Perc = RSI_DLRQ.DLRQ_KIND_COMMIT THEN
        pSumKt := pSumKt + v_sum_Perc;
      END IF;
      
    ELSIF pIsSale = 1 THEN
      pSumDt := v_sum_pay1;
      pSumKt := v_sum_del1 + v_sum_com;
      
    ELSE
      pSumDt := v_sum_del1;
      pSumKt := v_sum_pay1 + v_sum_com;
    END IF;
  END GetSumRQByDeal;
  
  PROCEDURE CollectArrearSCData
  IS
    v_ArrerSCData ddlclboarrear_dbt%rowtype;
    v_SumDt       NUMBER := 0;
    v_SumKt       NUMBER := 0;
  BEGIN
    FOR one_rec IN (SELECT tick.t_DealId, tick.t_ClientId, tick.t_PartyId, tick.t_MarketId,
                  RSB_SECUR.IsBuy (Opr.oGrp) t_IsBuy,
                  RSB_SECUR.IsSale (Opr.oGrp) t_IsSale,
                  RSB_SECUR.IsREPO (Opr.oGrp) t_IsREPO,
                  RSB_SECUR.IsExchange (Opr.oGrp) t_IsExchange,
                  RSB_SECUR.IsBasket (Opr.oGrp) t_IsBasket
               FROM ddl_tick_dbt tick,
                    (SELECT t_Kind_Operation, t_DocKind, rsb_secur.get_OperationGroup(t_SysTypes) oGrp FROM doprkoper_dbt) Opr
              WHERE     tick.t_BofficeKind = RSB_SECUR.DL_SECURITYDOC
                    AND Opr.t_Kind_Operation = tick.t_DealType
                    AND Opr.t_DocKind = tick.t_BOfficeKind
                    AND (   RSB_SECUR.IsBuy (Opr.oGrp) = 1
                         OR RSB_SECUR.IsSale (Opr.oGrp) = 1
                         OR RSB_SECUR.IsREPO (Opr.oGrp) = 1)
                    AND tick.t_ClientId = g_do.t_PartyID
                    and tick.t_dealstatus != 0 --DL_PREPARING
                    AND tick.t_DealDate <= g_EndDate
                    AND (   tick.t_CloseDate = RSI_RSB_FIINSTR.ZERO_DATE
                         or tick.t_CloseDate > g_EndDate)
                    AND tick.t_ClientContrId = g_do.t_ClientContrID)
    LOOP
      GetSumRQByDeal (one_rec.t_DealId,
                      one_rec.t_IsREPO,
                      one_rec.t_IsSale,
                      v_SumDt,
                      v_SumKt,
                      one_rec.t_MarketId);
      IF v_SumDt = 0 AND v_SumKt = 0 THEN
        CONTINUE;
      END IF;
      
      v_ArrerSCData.t_SessionID        := g_do.t_SessionID;
      v_ArrerSCData.t_MainID           := g_do.t_MainID;
      v_ArrerSCData.t_ArrearType       := ARREAR_TYPE_NOT_REPO;
      v_ArrerSCData.t_DocKind          := RSB_SECUR.DL_SECURITYDOC;
      v_ArrerSCData.t_Side             := CHR(1);
      v_ArrerSCData.t_ContractorID     := one_rec.t_PartyID;
      v_ArrerSCData.t_WithCentralContr := CHR(1);
      
      IF one_rec.t_IsREPO = 1 THEN
        v_ArrerSCData.t_Side := CASE 
                                  WHEN one_rec.t_IsSale = 1 THEN 'Прямое'
                                  ELSE 'Обратное'
                                END;
        v_ArrerSCData.t_ArrearType := ARREAR_TYPE_REPO;
        v_ArrerSCData.t_WithCentralContr := CASE
                                              WHEN  one_rec.t_IsExchange = 1
                                                AND one_rec.t_IsBasket = 1
                                              THEN 'ДА'
                                              ELSE 'НЕТ'
                                            END;
      END IF;
      
      BEGIN
        v_ArrerSCData.t_ContractorName := CHR(1);
        v_ArrerSCData.t_ContractorINN := CHR(1);
        
        SELECT pt.t_ShortName,
               (CASE t_NotResident
                   WHEN CHR(88)
                       THEN NVL ( rsb_secur.SC_GetObjCodeOnDate (3, 62, pt.t_PartyId, g_EndDate ),
                                NVL (rsb_secur.SC_GetObjCodeOnDate (3, 33, pt.t_PartyId, g_EndDate ), ''))
                   ELSE rsb_secur.SC_GetObjCodeOnDate (3, 16, pt.t_PartyId, g_EndDate)
               END)
                 t_INNorTIN
         INTO v_ArrerSCData.t_ContractorName, v_ArrerSCData.t_ContractorINN
         FROM dparty_dbt pt
        WHERE pt.t_PartyId = one_rec.t_PartyID;
      EXCEPTION
        WHEN NO_DATA_FOUND
          THEN NULL;
      END;
      
      IF v_SumDt > 0 THEN
        v_ArrerSCData.t_IsDebit := CNST.SET_CHAR;
        v_ArrerSCData.t_Value   := v_SumDt;
      
        g_ArrearData_ins.Extend();
        g_ArrearData_ins(g_ArrearData_ins.LAST) := v_ArrerSCData;
      END IF;
      
      IF v_SumKt > 0 THEN
        v_ArrerSCData.t_IsDebit := CNST.UNSET_CHAR;
        v_ArrerSCData.t_Value   := v_SumKt;
      
        g_ArrearData_ins.Extend();
        g_ArrearData_ins(g_ArrearData_ins.LAST) := v_ArrerSCData;
      END IF;
    END LOOP;
  END CollectArrearSCData;
  
  FUNCTION GetComSumByDealVA (pDealID IN NUMBER, pCurDate IN DATE)
  RETURN NUMBER
  IS
    v_InSum     NUMBER := 0;
    v_CurSum    NUMBER := 0;
    v_CurSumNDS NUMBER := 0;
  BEGIN
    FOR one_com IN (SELECT oprsfcom.t_Sum, oprsfcom.t_SumNDS, oprsfcom.t_FIID_sum
                      FROM ddl_tick_dbt tick, doproper_dbt opr, doprsfcom_dbt oprsfcom, dsfcomiss_dbt com
                     WHERE tick.t_DealID = pDealID
                       AND opr.t_DocKind = tick.t_BOfficeKind
                       AND opr.t_DocumentID = LPAD (tick.t_DealID, 34, '0')
                       AND oprsfcom.t_id_operation = opr.t_id_operation
                       AND com.t_FeeType = oprsfcom.t_FeeType
                       AND com.t_Number = oprsfcom.t_CommNumber)
    LOOP
      v_CurSum := one_com.t_Sum;
      v_CurSumNDS := one_com.t_SumNDS;
      
      IF one_com.t_FIID_sum != RSI_RSB_FIINSTR.NATCUR THEN
        IF RSB_SPREPFUN.SmartConvertSum(v_CurSum, v_CurSum, pCurDate, one_com.t_FIID_sum, RSI_RSB_FIINSTR.NATCUR, 1) != 0
        THEN
          v_CurSum := 0;
        END IF;
        
        IF RSB_SPREPFUN.SmartConvertSum(v_CurSumNDS, v_CurSumNDS, pCurDate, one_com.t_FIID_sum, RSI_RSB_FIINSTR.NATCUR, 1) != 0
        THEN
          v_CurSumNDS := 0;
        END IF;        
      END IF;
      
      v_InSum := v_InSum + v_CurSum + v_CurSumNDS;
    END LOOP;
    
    RETURN v_InSum;
  END GetComSumByDealVA;
  
  PROCEDURE CollectArrearVAData
  IS
    v_ArrearVAData       ddlclboarrear_dbt%rowtype;
    v_prevContractor     ddlclboarrear_dbt.t_ContractorID%type := 0;
    v_prevContractorName ddlclboarrear_dbt.t_ContractorName%type;
    v_prevContractorINN  ddlclboarrear_dbt.t_ContractorINN%type;
    v_SumDt              NUMBER := 0;
    v_SumKt              NUMBER := 0;
    v_ComisSum           NUMBER := 0;
    v_CurSum             NUMBER := 0;
  BEGIN
    FOR one_rec IN (SELECT tick.t_dealid as t_Id, tick.T_BOFFICEKIND as t_DocKind, tick.T_PARTYID,
                           party.t_ShortName t_ContractorName,
                           case when party.t_NotResident = CHR(0) then NVL(rsb_secur.SC_GetObjCodeOnDate(3, 16, tick.T_PARTYID, g_EndDate),'')
                                else NVL(rsb_secur.SC_GetObjCodeOnDate(3, 62, tick.T_PARTYID, g_EndDate),
                                         NVL(rsb_secur.SC_GetObjCodeOnDate (3, 33, tick.t_PartyId, g_EndDate ), '')) end t_ContractorINN
                      FROM (SELECT tick.*,
                                   RSB_SECUR.get_OperationGroup (
                                      RSB_SECUR.get_OperSysTypes (tick.t_DealType,
                                                                  tick.t_BofficeKind))
                                      t_OprGrp
                              FROM DDL_TICK_DBT tick
                             WHERE     tick.T_BOFFICEKIND = RSB_BILL.DL_VEKSELACCOUNTED
                                   AND tick.T_CLIENTID = g_do.t_PartyID
                                   AND TICK.T_CLIENTCONTRID = g_do.t_ClientContrId
                                   AND (    tick.t_regdate <= g_EndDate
                                        AND (   tick.t_closedate = RSI_RSB_FIINSTR.ZERO_DATE
                                             OR tick.t_closedate > g_EndDate))) tick, dparty_dbt party
                    WHERE    (RSB_SECUR.IsSale (tick.t_OprGrp) = 1
                          OR RSB_SECUR.IsBuy (tick.t_OprGrp) = 1)
                          AND party.T_PARTYID = tick.T_PARTYID
                     ORDER BY tick.T_PARTYID,  tick.T_BOFFICEKIND, tick.t_dealid)
    LOOP
      v_ComisSum := GetComSumByDealVA (one_rec.t_ID, g_EndDate);
      
      IF v_prevContractor != 0 AND v_prevContractor != one_rec.t_PartyID THEN
        IF v_SumDt <> 0 OR v_SumKt <> 0 THEN
          v_ArrearVAData.t_SessionID      := g_do.t_SessionID;
          v_ArrearVAData.t_MainID         := g_do.t_MainID;
          v_ArrearVAData.t_ArrearType     := ARREAR_TYPE_NOT_REPO;
          v_ArrearVAData.t_DocKind        := one_rec.t_DocKind;
          v_ArrearVAData.t_ContractorID   := v_prevContractor;
          v_ArrearVAData.t_ContractorName := v_prevContractorName;
          v_ArrearVAData.t_ContractorINN  := v_prevContractorINN;
          
          v_ArrearVAData.t_IsDebit := CNST.SET_CHAR;
          v_ArrearVAData.t_Value   := v_SumDt;
          g_ArrearData_ins.Extend();
          g_ArrearData_ins(g_ArrearData_ins.LAST) := v_ArrearVAData;
          
          v_ArrearVAData.t_IsDebit := CNST.UNSET_CHAR;
          v_ArrearVAData.t_Value   := v_SumKt;
          g_ArrearData_ins.Extend();
          g_ArrearData_ins(g_ArrearData_ins.LAST) := v_ArrearVAData; 
        END IF;
      END IF;
      FOR one_pm IN (SELECT paym.T_PAYAMOUNT, paym.t_PayFIID,
                            case when paym.t_Payer = g_do.t_PartyID then 1 else 0 end t_IsObl
                          FROM dpmpaym_dbt paym
                         WHERE   paym.t_DocKind = one_rec.t_DocKind
                             AND paym.t_DocumentID = one_rec.t_ID
                             AND paym.t_Purpose = RSB_PAYMENT.CAi
                             AND paym.t_ValueDate > g_EndDate
                             AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
      LOOP
        v_CurSum := one_pm.T_PAYAMOUNT;
        IF one_pm.t_PayFIID != RSI_RSB_FIINSTR.NATCUR THEN
          IF RSB_SPREPFUN.SmartConvertSum(v_CurSum, v_CurSum, g_EndDate, one_pm.t_PayFIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
          THEN
            v_CurSum := 0;
          END IF;
        END IF;
        
        v_SumDt := v_SumDt + v_CurSum;
        v_SumKt := v_SumKt + (v_CurSum + v_ComisSum);
      END LOOP;
      
      v_prevContractor     := one_rec.t_PartyID;
      v_prevContractorName := one_rec.t_ContractorName;
      v_prevContractorINN  := one_rec.t_ContractorINN;
    END LOOP;
    
    IF v_prevContractor !=0 AND (v_SumDt <> 0 OR v_SumKt <> 0) THEN
      v_ArrearVAData.t_SessionID      := g_do.t_SessionID;
      v_ArrearVAData.t_MainID         := g_do.t_MainID;
      v_ArrearVAData.t_ArrearType     := ARREAR_TYPE_NOT_REPO;
      v_ArrearVAData.t_DocKind        := RSB_BILL.DL_VEKSELACCOUNTED;
      v_ArrearVAData.t_ContractorID   := v_prevContractor;
      v_ArrearVAData.t_ContractorName := v_prevContractorName;
      v_ArrearVAData.t_ContractorINN  := v_prevContractorINN;
          
      v_ArrearVAData.t_IsDebit        := CNST.SET_CHAR;
      v_ArrearVAData.t_Value          := v_SumDt;
      g_ArrearData_ins.Extend();
      g_ArrearData_ins(g_ArrearData_ins.LAST) := v_ArrearVAData;
          
      v_ArrearVAData.t_IsDebit        := CNST.UNSET_CHAR;
      v_ArrearVAData.t_Value          := v_SumKt;
      g_ArrearData_ins.Extend();
      g_ArrearData_ins(g_ArrearData_ins.LAST) := v_ArrearVAData; 
    END IF;    
  END CollectArrearVAData;
  
  PROCEDURE CollectArrearPFIData
  IS
    c_MPType CONSTANT NUMBER := Rsb_Common.GetRegIntValue('ПРОИЗВОДНЫЕ ИНСТРУМЕНТЫ\ВИД КУРСА "РЫНОЧНАЯ ЦЕНА"', 0);
  
    v_ArrearPFIData   ddlclboarrear_dbt%rowtype;
    v_SumDt           NUMBER := 0;
    v_SumKt           NUMBER := 0;
    v_PriceRub        NUMBER := 0;
    v_MarketPrice     NUMBER := 0;
    v_Rate            NUMBER := 0;
    v_ComisSum        NUMBER := 0;
    v_tmpComisSum     NUMBER := 0;
    v_PaidComisSum    NUMBER := 0;
    v_tmpPaidComisSum NUMBER := 0;
  BEGIN
    FOR one_rec IN (SELECT ndeal.t_ID,
                           ndeal.t_DocKind,
                           ndeal.t_DvKind,
                           ndeal.t_Contractor,
                           party.t_ShortName t_ContractorName,
                           case when party.t_NotResident = CHR(0) then NVL(rsb_secur.SC_GetObjCodeOnDate(3, 16, ndeal.t_Contractor, g_EndDate),'')
                                else NVL(rsb_secur.SC_GetObjCodeOnDate(3, 62, ndeal.t_Contractor, g_EndDate),
                                         NVL(rsb_secur.SC_GetObjCodeOnDate (3, 33, ndeal.t_Contractor, g_EndDate), '')) end t_ContractorINN
                      FROM ddvndeal_dbt ndeal, dparty_dbt party, doproper_dbt oper
                     WHERE ndeal.t_Client = g_do.t_PartyID
                       AND ndeal.t_ClientContr = g_do.t_ClientContrID
                       AND ndeal.t_Date <= g_EndDate
                       AND (    ndeal.t_DVKind IN (RSB_DERIVATIVES.DV_FORWARD,
                                                   RSB_DERIVATIVES.DV_OPTION,
                                                   RSB_DERIVATIVES.DV_PCTSWAP,
                                                   RSB_DERIVATIVES.DV_FORWARD_FX,
                                                   RSB_DERIVATIVES.DV_BANKNOTE_FX)
                            OR (    ndeal.t_DVKind IN (RSB_DERIVATIVES.DV_CURSWAP,RSB_DERIVATIVES.DV_CURSWAP_FX)
                                AND NOT EXISTS (SELECT 1
                                                  FROM dpmpaym_dbt paym
                                                 WHERE paym.t_DocKind = ndeal.t_DocKind
                                                   AND paym.t_DocumentID = ndeal.t_ID
                                                   AND paym.t_Purpose IN (RSB_PAYMENT.BAi,RSB_PAYMENT.CAi)   -- Платежи по 1ч
                                                   AND paym.t_ValueDate > g_EndDate
                                                   AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
                                AND EXISTS (SELECT 1
                                              FROM dpmpaym_dbt paym
                                             WHERE paym.t_DocKind = ndeal.t_DocKind
                                               AND paym.t_DocumentID = ndeal.t_ID
                                               AND paym.t_Purpose IN (RSB_PAYMENT.BRi,RSB_PAYMENT.CRi)   -- Платежи по 2ч
                                               AND paym.t_ValueDate > g_EndDate
                                               AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
                               )
                           )
                       AND oper.t_DocKind = ndeal.t_DocKind
                       AND oper.t_DocumentID = lpad(ndeal.t_ID, 34, '0')
                       AND ndeal.t_State > 0
                       AND EXISTS (SELECT 1
                                     FROM dpmpaym_dbt paym
                                    WHERE paym.t_DocKind = ndeal.t_DocKind
                                      AND paym.t_DocumentID = ndeal.t_ID
                                      AND paym.t_ValueDate > g_EndDate
                                      AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
                       AND party.t_PartyID = ndeal.t_Contractor)
    LOOP
      FOR one_com IN (SELECT comis.t_Sum, sfc.t_FIID_Comm
                        FROM ddlcomis_dbt comis, dsfcomiss_dbt sfc
                       WHERE comis.t_DocKind = one_rec.t_DocKind
                         AND comis.t_DocID = one_rec.t_ID
                         AND comis.t_Date <= g_EndDate
                         AND comis.t_FeeType = sfc.t_FeeType
                         AND comis.t_ComNumber = sfc.t_Number)
      LOOP
        v_tmpComisSum := RSI_RSB_FIINSTR.ConvSum(one_com.t_Sum, one_com.t_FIID_Comm, RSI_RSB_FIINSTR.NATCUR, g_EndDate);
        IF v_tmpComisSum IS NOT NULL THEN
          v_ComisSum := v_ComisSum + v_tmpComisSum;
        END IF;
      END LOOP;
      
      FOR one_pcom IN (SELECT paym.t_Amount, paym.t_PayFIID
                    FROM dpmpaym_dbt paym
                   WHERE paym.t_DocKind = one_rec.t_DocKind
                     AND paym.t_DocumentID = one_rec.t_ID
                     AND paym.t_Purpose IN (RSB_PAYMENT.PM_PURP_COMMARKET,
                                            PM_PURP_COMMBANK,
                                            RSB_PAYMENT.PM_PURP_COMBROKER)
                     AND paym.t_ValueDate <= g_EndDate
                     AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED)
      LOOP
        v_tmpComisSum := RSI_RSB_FIINSTR.ConvSum(one_pcom.t_Amount, one_pcom.t_PayFIID, RSI_RSB_FIINSTR.NATCUR, g_EndDate);
        IF v_tmpPaidComisSum IS NOT NULL THEN
          v_PaidComisSum := v_PaidComisSum + v_tmpPaidComisSum;
        END IF;
      END LOOP;
      
      FOR one_pm IN (SELECT paym.t_Amount, paym.t_PayFIID, fin.t_FaceValueFI, fin.t_FI_Kind, fin.t_Name,
                            case when paym.t_Payer = g_do.t_PartyID then 0 else 1 end t_IsReq
                       FROM dpmpaym_dbt paym, dfininstr_dbt fin
                      WHERE paym.t_DocKind = one_rec.t_DocKind
                        AND paym.t_DocumentID = one_rec.t_ID
                        AND paym.t_Purpose NOT IN (RSB_PAYMENT.PM_PURP_COMMARKET,
                                                   PM_PURP_COMMBANK,
                                                   RSB_PAYMENT.PM_PURP_COMBROKER)
                        AND paym.t_ValueDate > g_EndDate
                        AND paym.t_PaymStatus <> RSB_PAYMENT.PM_REJECTED
                        AND paym.t_PayFIID = fin.t_FIID)
      LOOP
        IF   one_pm.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY
          OR one_pm.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_METAL
        THEN
          IF one_pm.t_PayFIID != RSI_RSB_FIINSTR.NATCUR THEN
            v_PriceRub := RSI_RSB_FIINSTR.ConvSum(one_pm.t_Amount, one_pm.t_PayFIID,  RSI_RSB_FIINSTR.NATCUR, g_EndDate);
            IF v_PriceRub IS NULL THEN
              CONTINUE;
            END IF;
          ELSE
            v_PriceRub := one_pm.t_Amount;
          END IF;
          
        ELSIF one_pm.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS THEN
          v_MarketPrice := RSI_RSB_FIINSTR.ConvSumType (1.0, one_pm.t_PayFIID, one_pm.t_FaceValueFI, c_MPType, g_EndDate);
          IF v_MarketPrice IS NOT NULL THEN
            IF one_pm.t_FaceValueFI != RSI_RSB_FIINSTR.NATCUR THEN
              v_Rate := RSI_RSB_FIINSTR.ConvSum (1.0, one_pm.t_FaceValueFI, RSI_RSB_FIINSTR.NATCUR, g_EndDate);
              IF v_Rate IS NULL THEN
                CONTINUE;
              END IF;              
            ELSE
              v_Rate := 1;
            END IF;
            
            v_PriceRub := v_Rate * v_MarketPrice * one_pm.t_Amount;
          ELSE
            v_PriceRub := 0;
            RSB_SPREPFUN.AddRepError('Не определена рыночная стоимость ц/б "' || one_pm.t_Name || '"');
            CONTINUE;
          END IF;
          
        ELSE
          v_PriceRub := GetAnyLastCourseOnDate(g_EndDate, one_pm.t_PayFIID);
          IF v_PriceRub IS NULL THEN
            CONTINUE;
          END IF;
        END IF;
        
        IF one_pm.t_IsReq = 1 THEN
          v_SumDt := v_SumDt + v_PriceRub;
        ELSE
          v_SumKt := v_SumKt + v_PriceRub;
        END IF;
      END LOOP;
      
      v_SumKt := v_SumKt + (v_ComisSum - v_PaidComisSum);
      
      v_ArrearPFIData.t_SessionID      := g_do.t_SessionID;
      v_ArrearPFIData.t_MainID         := g_do.t_MainID;
      v_ArrearPFIData.t_ArrearType     := ARREAR_TYPE_NOT_REPO;
      v_ArrearPFIData.t_DocKind        := one_rec.t_DocKind;
      v_ArrearPFIData.t_ContractorID   := one_rec.t_Contractor;
      v_ArrearPFIData.t_ContractorName := one_rec.t_ContractorName;
      v_ArrearPFIData.t_ContractorINN  := one_rec.t_ContractorINN;
      
      IF v_SumDt > 0 THEN
        v_ArrearPFIData.t_IsDebit      := CNST.SET_CHAR;
        v_ArrearPFIData.t_Value        := v_SumDt;
        g_ArrearData_ins.Extend();
        g_ArrearData_ins(g_ArrearData_ins.LAST) := v_ArrearPFIData;
      END IF;
      
      IF v_SumKt > 0 THEN
        v_ArrearPFIData.t_IsDebit      := CNST.UNSET_CHAR;
        v_ArrearPFIData.t_Value        := v_SumKt;
        g_ArrearData_ins.Extend();
        g_ArrearData_ins(g_ArrearData_ins.LAST) := v_ArrearPFIData;
      END IF;
    END LOOP;
  END CollectArrearPFIData;
  
  PROCEDURE InsertArrearData
  IS
  BEGIN
    IF g_ArrearData_ins IS NOT EMPTY THEN
      FORALL i IN g_ArrearData_ins.FIRST .. g_ArrearData_ins.LAST
        INSERT INTO ddlclboarrear_dbt
             VALUES g_ArrearData_ins(i);
      
      g_ArrearData_ins.DELETE();
    END IF;
  END InsertArrearData;
  
  PROCEDURE CollectInOutCurData
  IS
  BEGIN
    INSERT INTO ddlclboinoutcur_dbt (T_SESSIONID,
                                     T_MAINID,
                                     T_CLIENTCODE,
                                     T_OPCODE,
                                     T_ISENROL,
                                     T_DATE,
                                     T_CURRCODE,
                                     T_SUM,
                                     T_SUMRUB)
    SELECT g_do.t_SessionID t_SessionID,
           g_do.t_MainID    t_MainID,
           g_do.t_Code      t_ClientCode,
           nptxop.t_Code    t_OpCode,
           CASE nptxop.t_subkind_operation
                WHEN DL_NPTXOP_WRTKIND_ENROL  THEN CNST.SET_CHAR
                WHEN DL_NPTXOP_WRTKIND_WRTOFF THEN CNST.UNSET_CHAR
           END
                t_IsEnrol,
           nptxop.t_OperDate,
           (SELECT t_FI_Code FROM dfininstr_dbt WHERE t_FIID = nptxop.t_Currency) t_CurrCode,
           nptxop.t_OutSum t_Sum,
           RSI_RSB_FIINSTR.ConvSum (nptxop.t_OutSum, nptxop.t_Currency, RSI_RSB_FIINSTR.NATCUR, nptxop.t_OperDate, 0) t_SumRub
      FROM dnptxop_dbt nptxop
     WHERE     NPTXOP.T_DOCKIND = RSB_SECUR.DL_WRTMONEY
           AND NPTXOP.T_OPERDATE BETWEEN g_BegDate AND g_EndDate
           AND nptxop.t_status = RSI_NPTXC.DL_TXOP_CLOSE
           AND nptxop.t_Client = g_do.t_PartyID
           AND nptxop.t_Contract = g_do.t_ClientContrID
           AND nptxop.t_subkind_operation in (DL_NPTXOP_WRTKIND_ENROL, DL_NPTXOP_WRTKIND_WRTOFF)
           AND NOT EXISTS (SELECT 1
                             FROM dnotetext_dbt notetext
                            WHERE     notetext.t_notekind = 103
                              AND notetext.t_objecttype = 131
                              AND notetext.t_documentid = LPAD (nptxop.t_id, 34, 0)
                              AND TRIM (LOWER ( REPLACE (RSB_STRUCT.GETSTRING (notetext.t_text), CHR (0)))) =
                                  LOWER ('Дубль')) -- DEF-33231
           AND NOT EXISTS (SELECT 1
                             FROM USR_ACC306ENROLL_DBT ua 
                            WHERE ua.t_NptxopID = nptxop.t_ID 
                              AND SUBSTR(ua.t_DebetAccount, 1, 5) = '47422'
                          );
  END CollectInOutCurData;
  
  PROCEDURE CollectInOutSecData
  IS
    TYPE InOutCurData_t IS TABLE OF ddlclboinoutsec_dbt%rowtype;
    InOutCurData_ins InOutCurData_t := InOutCurData_t();
    v_InOutCurData ddlclboinoutsec_dbt%rowtype;
    
    v_AvrValueData AvrValueData_t;
    v_UseNKDRate BOOLEAN := Rsb_Common.GetRegBoolValue('SECUR\РАСЧЕТ НКД ПО КУРСУ', 0);
  BEGIN
    FOR one_rec IN (SELECT leg.t_PFI,
                           leg.t_Principal,
                           tick.t_DealDate,
                           tick.t_DealCode t_Code,
                           tick.t_MarketId,
                           CASE
                              WHEN RSB_SECUR.IsAvrWrtIn (RSB_SECUR.Get_OperationGroup (oprk.t_SysTypes)) = 1 THEN CHR(88)
                              WHEN RSB_SECUR.IsAvrWrtOut (RSB_SECUR.Get_OperationGroup (oprk.t_SysTypes)) = 1 THEN CHR(0)
                           END
                              t_IsEnrol,
                           fi.t_Name,
                           CASE RSI_RSB_FIINSTR.FI_AvrKindsGetRoot( 2, fi.t_AvoirKind )
                              WHEN RSI_RSB_FIINSTR.AVOIRKIND_INVESTMENT_SHARE THEN (SELECT t_FormValueFIID FROM davrinvst_dbt WHERE t_FIID = fi.t_FIID)
                              ELSE fi.t_FaceValueFI
                           END
                              t_FVFI,
                           (SELECT t_NotResident
                              FROM dparty_dbt
                             WHERE t_PartyId = ( CASE RSI_RSB_FIInstr.FI_AvrKindsGetRoot (2, fi.t_AvoirKind)
                                                    WHEN RSI_RSB_FIINSTR.AVOIRKIND_DEPOSITORY_RECEIPT THEN (SELECT finParent.t_Issuer
                                                                                                              FROM dfininstr_dbt finParent
                                                                                                             WHERE finParent.t_FIID = fi.t_ParentFI)
                                                    ELSE fi.t_Issuer
                                               END ) )
                              t_IsIssuerNotResident,
                          rsb_secur.GetObjAttrName (12, 1, rsb_secur.GetMainObjAttr (12, LPAD (fi.t_FIID, 10, '0'), 1, g_EndDate ))
                             t_AvoirType
                      FROM ddl_tick_dbt tick, doprkoper_dbt oprk, doproper_dbt opr, ddl_leg_dbt leg, dfininstr_dbt fi
                     WHERE     tick.t_BOfficeKind = RSB_SECUR.DL_AVRWRT
                           AND tick.t_ClientId = g_do.t_PartyID
                           AND tick.t_DealDate BETWEEN g_BegDate AND g_EndDate
                           AND opr.t_DocumentId = LPAD(tick.t_DealId, 34, '0')
                           AND opr.t_DocKind = tick.T_BOfficeKind
                           AND opr.t_Completed = CHR(88)
                           AND tick.t_ClientContrId = g_do.t_ClientContrID
                           AND leg.t_DealId = tick.t_DealId
                           AND leg.t_LegKind = 0 --LEG_KIND_DL_TICK
                           AND leg.t_LegId = 0
                           AND oprk.t_DocKind = tick.t_BOfficeKind
                           AND oprk.t_Kind_Operation = tick.t_DealType
                           AND fi.t_FIID = leg.t_PFI
                           AND fi.t_FI_Kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS
                           AND RSB_SECUR.GetObjAttrNumber (RSB_SECUR.OBJTYPE_SECDEAL,
                                                           OUTER_WRT_ATTR_GRP ,
                                                           rsb_secur.GetMainObjAttr (RSB_SECUR.OBJTYPE_SECDEAL, LPAD (tick.t_DealId, 34, '0'), OUTER_WRT_ATTR_GRP, g_EndDate ) ) = '1')
    LOOP
      v_AvrValueData := GetAvoirValue(one_rec.t_PFI,
                                      one_rec.t_FVFI,
                                      one_rec.t_Principal,
                                      one_rec.t_DealDate,
                                      v_UseNKDRate,
                                      one_rec.t_MarketID);
      IF v_AvrValueData.t_Sum IS NOT NULL THEN
        v_InOutCurData.t_SessionID           := g_do.t_SessionID;
        v_InOutCurData.t_MainID              := g_do.t_MainID;
        v_InOutCurData.t_ClientCode          := g_do.t_Code;
        v_InOutCurData.t_OpCode              := one_rec.t_Code;
        v_InOutCurData.t_Date                := one_rec.t_DealDate;
        v_InOutCurData.t_IsEnrol             := one_rec.t_IsEnrol;
        v_InOutCurData.t_AvoirName           := one_rec.t_Name;
        v_InOutCurData.t_FaceValueFI         := RSB_SECUR.Get_FI_Code(one_rec.t_FVFI, RSB_SECUR.OBJTYPE_FININSTR, RSB_SECUR.CODE_FI_CODE);
        v_InOutCurData.t_IsIssuerNotResident := one_rec.t_IsIssuerNotResident;
        v_InOutCurData.t_ISIN                := RSB_SECUR.Get_FI_Code(one_rec.t_PFI,  RSB_SECUR.OBJTYPE_FININSTR, RSB_SECUR.CODE_ISIN);
        v_InOutCurData.t_AvoirType           := one_rec.t_AvoirType;
        v_InOutCurData.t_Quantity            := one_rec.t_Principal;
        v_InOutCurData.t_Course              := v_AvrValueData.t_Course;
        v_InOutCurData.t_Cost                := v_AvrValueData.t_Sum;
        v_InOutCurData.t_SumRub              := v_AvrValueData.t_SumRub;
        
        InOutCurData_ins.Extend();
        InOutCurData_ins(InOutCurData_ins.LAST) := v_InOutCurData;
      END IF;
    END LOOP;
    
    IF InOutCurData_ins IS NOT EMPTY THEN
      FORALL i IN InOutCurData_ins.FIRST .. InOutCurData_ins.LAST
        INSERT INTO ddlclboinoutsec_dbt
             VALUES InOutCurData_ins(i);
          
      InOutCurData_ins.Delete();
    END IF;
  END CollectInOutSecData;

  -- Обработка ДО
  PROCEDURE ProcessDO(pSessionID IN NUMBER,
                      pClientContrIdstart IN NUMBER,
                      pClientContrIdend  IN NUMBER,
                      pBegDate IN DATE,
                      pEndDate IN DATE,
                      pIsParallel IN NUMBER DEFAULT 1)
  IS
    v_ContrNumber dsfcontr_dbt.t_Number%type;
    v_StartDt date := sysdate;
    v_Cnt integer := 0;
  BEGIN
  -- it_log.log(p_msg => 'Start pSessionID='||pSessionID||' pClientContrIdstart='||pClientContrIdstart||'  pClientContrIdend= '||pClientContrIdend);

  RSB_SPREPFUN.g_SessionID := pSessionID;
  RSB_SPREPFUN.g_RepKind   := 0;

  g_BegDate := pBegDate;
  g_EndDate := pEndDate;

    for cur in (SELECT * FROM DDLCLBO_DBT
                     WHERE t_SessionID = pSessionID
                      AND t_ClientContrID  between pClientContrIdstart and pClientContrIdend )
      loop
        v_Cnt := v_Cnt+1;
      BEGIN
        g_do := cur ;
        /*SELECT * INTO g_do
          FROM DDLCLBO_DBT
        WHERE t_SessionID = pSessionID
          AND t_ClientContrID = pClientContrID;*/


        CollectMetalRestAccData();
        CollectCurRestAccData();

        CollectAvrCostData();

        IF ((g_do.t_ServKind = PTSK_DV) OR (g_do.t_ServKind = PTSK_CM)) THEN
          CollectPFIData();
        END IF;

        IF g_do.t_ServKind = PTSK_STOCKDL THEN
          CollectArrearSCData();
        END IF;

        IF g_do.t_ServKind = PTSK_VEKSACC THEN
          CollectArrearVAData();
        END IF;

        IF ((g_do.t_ServKind = PTSK_DV) OR (g_do.t_ServKind = PTSK_CM)) THEN
          CollectArrearPFIData();
        END IF;

        InsertArrearData();

        CollectInOutCurData();
        CollectInOutSecData();
      EXCEPTION
      WHEN OTHERS THEN
          rollback;
          it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR,p_msg => 'Ошибка при обработке договора ' ||SQLERRM,p_msg_clob => sys.dbms_utility.format_error_backtrace );
          SELECT t_Number INTO v_ContrNumber
             FROM dsfcontr_dbt
            WHERE t_Id = cur.t_clientcontrid; -- pClientContrId;
          RSB_SPREPFUN.AddRepError('Ошибка при обработке договора ' || v_ContrNumber || ' ' ||SQLERRM);
      END;
      commit;
    end loop;
    -- it_log.log(p_msg => 'FINISH '||v_Cnt||' дог. за '||round((sysdate - v_StartDt)*24*60*60)||' сек');
    EXCEPTION
    WHEN OTHERS THEN
        rollback;
        it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR,p_msg => 'Ошибка формирования данных ' ||SQLERRM,p_msg_clob => sys.dbms_utility.format_error_backtrace);
        RSB_SPREPFUN.AddRepError('Ошибка формирования данных ' ||SQLERRM);
  END ProcessDO;

  --Формирование данных
  PROCEDURE CreateAllData( pBegDate IN DATE,
                           pEndDate IN DATE,
                           pSessionID IN NUMBER,
                           pParallelLevel IN NUMBER,
                           pPartitionCount IN NUMBER,
                           pPartitionNum IN NUMBER )
  IS
    v_Cnt NUMBER;
    v_task_name VARCHAR2(30);
    v_sql_chunks CLOB;
    v_sql_process VARCHAR2(400);
    v_try NUMBER(5) := 0;
    v_status NUMBER;
  BEGIN
  
    SELECT COUNT(1) INTO v_Cnt
      FROM DDLCLBO_DBT
     WHERE t_SessionID = pSessionID;

    IF v_Cnt > 0 THEN
      IF(pParallelLevel > 0) THEN
        v_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;
        DBMS_PARALLEL_EXECUTE.create_task (task_name => v_task_name);

        v_sql_chunks := 'SELECT min(t_ClientContrID), max(t_ClientContrID) ' ||
                        'FROM (SELECT t_ClientContrID, ' ||
                        '             NTILE(' || TO_CHAR(pParallelLevel*100) || ') OVER ( ORDER BY t_ClientContrID ) t_Node ' ||
                        '      FROM ( SELECT t_ClientContrID, ' ||
                        '                    NTILE(' || TO_CHAR(pPartitionCount) || ') OVER ( PARTITION BY t_SessionID ORDER BY t_ClientContrID) t_PartNum ' ||
                        '             FROM DDLCLBO_DBT WHERE t_SessionID = ' || TO_CHAR(pSessionID) || ') ' ||
                        '      WHERE t_PartNum = ' || TO_CHAR(pPartitionNum)||
                        '      ) group by t_Node';

        DBMS_PARALLEL_EXECUTE.create_chunks_by_sql(task_name => v_task_name,
                                                         sql_stmt  => v_sql_chunks,
                                                         by_rowid  => FALSE);

        v_sql_process := 'CALL RSB_DLCLBOREP.ProcessDO('||TO_CHAR(pSessionID)||', :start_id, :end_id, '||
                                                       'TO_DATE('''||TO_CHAR(pBegDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY''), '||
                                                       'TO_DATE('''||TO_CHAR(pEndDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'')) ';

        DBMS_PARALLEL_EXECUTE.run_task(task_name => v_task_name,
                                            sql_stmt => v_sql_process,
                                            language_flag => DBMS_SQL.NATIVE,
                                            parallel_level => pParallelLevel);

        v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
        WHILE(v_try < 2 AND v_status != DBMS_PARALLEL_EXECUTE.FINISHED)
        LOOP
          v_try := v_try + 1;
          DBMS_PARALLEL_EXECUTE.resume_task(v_task_name);
          v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
        END LOOP;

        DBMS_PARALLEL_EXECUTE.drop_task(v_task_name);

      ELSE -- для отладки
       FOR one_do IN (SELECT t_ClientContrID FROM DDLCLBO_DBT WHERE t_SessionID = pSessionID)
        LOOP
          RSB_DLCLBOREP.ProcessDO(pSessionID,one_do.t_ClientContrID, one_do.t_ClientContrID, pBegDate, pEndDate);
        END LOOP;
      END IF;
    END IF;

  END CreateAllData;

END RSB_DLCLBOREP;
/