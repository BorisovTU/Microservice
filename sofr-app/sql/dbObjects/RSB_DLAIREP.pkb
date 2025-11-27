CREATE OR REPLACE PACKAGE BODY RSB_DLAIREP
IS
  TYPE AvrMPData_t IS RECORD (t_MarketPrice  NUMBER,
                              t_Rate         NUMBER,
                              t_FindCourseFi NUMBER,
                              t_Message      VARCHAR2(255));

  TYPE DecrData_t IS TABLE OF CLOB;                            
  g_DecrData DecrData_t;  
                              
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

  FUNCTION CheckPartyType(pPartyId IN NUMBER, pPartyType IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
    v_isPartyType NUMBER := 0; 
  BEGIN
    SELECT 1 INTO v_isPartyType
      FROM dpartyown_dbt
     WHERE t_PartyID = pPartyId
       AND t_PartyKind = pPartyType;
  
    RETURN v_isPartyType;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0;
  END CheckPartyType;
  
  FUNCTION HasOGRN(pPartyID IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
  BEGIN
    RETURN CASE 
             WHEN RSI_RSBPARTY.GetPartyCode(pPartyID, 27) = '0'
               THEN 0
             ELSE 1
           END;
  END HasOGRN;
  
  
  FUNCTION GetCountryCode(pPartyCode IN d707ds_dbt.t_NameObject%type)
  RETURN VARCHAR
  DETERMINISTIC
  IS
    v_code VARCHAR(5);
    
    CURSOR c IS SELECT t_codenum3
                  FROM dcountry_dbt 
                 WHERE t_CodeLat3 = pPartyCode;
  BEGIN
    OPEN c;
    FETCH c INTO v_code;
    
    IF c%NOTFOUND THEN
      v_code := '999';
    END IF;
    
    CLOSE c;
    
    RETURN v_code;
  END GetCountryCode;

  FUNCTION GetOkatoCode(pParty IN NUMBER, pOnDate DATE)
  RETURN VARCHAR
  DETERMINISTIC
  IS
    v_okatoCode VARCHAR(20);
  BEGIN
    v_okatoCode := RSB_SECUR.GetObjAttrNumber(RSB_SECUR.OBJTYPE_PARTY, 
                                              12, 
                                              RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_PARTY,
                                                                       LPAD(pParty, 10, '0'),
                                                                       12,
                                                                       pOnDate));
    
    IF v_okatoCode = CHR(1) THEN
      v_okatoCode := RSB_SECUR.GetObjAttrNumber(RSB_SECUR.OBJTYPE_PARTY, 
                                                12, 
                                                RSB_SECUR.GetMainObjAttrNoDate (RSB_SECUR.OBJTYPE_PARTY,
                                                                                LPAD(pParty, 10, '0'),
                                                                                12));      
    END IF;
    
    IF length(v_okatoCode) > 2 THEN
      v_okatoCode := substr(v_okatoCode, 1, 2);
    END IF;
    
    RETURN v_okatoCode;
  END GetOkatoCode;
  
  FUNCTION GetLastRateOnDateByType (pVN IN NUMBER, pDate IN DATE, pRateType IN NUMBER, pMarketID IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
    v_FIID   NUMBER;
  BEGIN
    SELECT t_fiid
      INTO v_FIID
     FROM (SELECT t_rateid, t_fiid
               FROM (SELECT rate.t_rateid, rate.t_sincedate, rate.t_type, rate.t_fiid, rate.T_ISRELATIVE, rate.t_OtherFI
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
                     UNION ALL
                       SELECT r.t_rateid, h.t_sincedate, r.t_type, r.t_fiid, r.T_ISRELATIVE, r.t_OtherFI
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
            ORDER BY t_sincedate DESC,
                     (CASE
                         WHEN T_ISRELATIVE != CHR (88) THEN 1
                         WHEN T_ISRELATIVE = CHR (88)
                              AND t_fiid = (SELECT fin.t_facevaluefi
                                              FROM dfininstr_dbt fin
                                             WHERE fin.t_fiid = t_otherfi) THEN 1
                         ELSE 0
                      END) DESC,
                     t_type ASC
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
  
  PROCEDURE GetRate90 (pBO IN NUMBER, pFIID IN NUMBER, pDate IN DATE, pFIID_Nom IN NUMBER, pMP OUT NUMBER, pDealCode OUT VARCHAR)
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
      IF v_fiid != RSI_RSB_FIINSTR.NATCUR THEN
        IF v_fiid != pFIID_Nom THEN
          v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, v_fiid, pFIID_Nom, pDate, 0);
        ELSE
          v_Rate := RSI_RSB_FIINSTR.CalcSumCross (1.0, v_fiid, RSI_RSB_FIINSTR.NATCUR, pDate, 0);
        END IF;
          
        IF v_Rate IS NULL OR v_Rate = 0.0 THEN
          pMP := NULL;
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
  IS
    v_MarketPrice  NUMBER;
    v_FindCourseFI NUMBER := 0;
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
       
        --Последняя сделка за 90 дней
        IF v_MarketPrice IS NULL OR v_MarketPrice = 0.0 THEN
          IF RSB_SECUR.GetObjAttrNumber (RSB_SECUR.OBJTYPE_AVOIRISS,
                                         NOT_USE_DEALS_ATTR_GRP,
                                         RSB_SECUR.GetMainObjAttr (RSB_SECUR.OBJTYPE_AVOIRISS, LPAD (pVN, 10, '0'), NOT_USE_DEALS_ATTR_GRP, pDate) ) != '0' THEN
            GetRate90 (RSB_SECUR.DL_SECURITYDOC, pVN, pDate, pFVFI, v_MarketPrice, v_DealCode); 
            IF v_MarketPrice IS NOT NULL THEN
              v_result.t_Message := 'Котировку для ц/б ' || GetFIName(pVN) || ' равную ' || v_MarketPrice || ' определил по сделке ' || v_DealCode;
            END IF;
          END IF;
        END IF;

        
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
        
        IF v_MarketPrice IS NULL OR v_MarketPrice = 0.0 THEN
          v_result.t_Message := 'Не определена рыночная стоимость ц/б "' || GetFIName(pVN) || '"';
        END IF;
      ELSE
        v_MarketPrice := 1.0;
      END IF;
    END IF;
    --DBMS_OUTPUT.PUT_LINE('market_price='||v_MarketPrice);
    v_result.t_MarketPrice  := v_MarketPrice;
    v_result.t_Rate         := v_Rate;
    v_result.t_FindCourseFI := v_FindCourseFi;
   
    RETURN v_result;
  END GetAvoirMarketPrice;

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
                     UNION ALL
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


  -- Обработка ДО
  PROCEDURE ProcessDO(pClientContrId IN NUMBER,
                      pSessionID IN NUMBER,
                      pBegDate IN DATE,
                      pEndDate IN DATE,
                      pIsParallel IN NUMBER DEFAULT 1)
  IS
    c_MPType CONSTANT NUMBER := Rsb_Common.GetRegIntValue('ПРОИЗВОДНЫЕ ИНСТРУМЕНТЫ\ВИД КУРСА "РЫНОЧНАЯ ЦЕНА"', 0);
  
    v_ds          D707DS_DBT%rowtype;
    v_EndWorkDate DATE;  
  
    TYPE deal_t   IS TABLE OF d707deal_dbt%rowtype;
    TYPE acc_t    IS TABLE OF d707acc_dbt%rowtype;
    TYPE decr_t   IS TABLE OF daidecr_dbt%rowtype;
    
    deal_ins deal_t := deal_t();
    acc_ins  acc_t  := acc_t();
    decr_ins decr_t := decr_t();
    
    v_deal d707deal_dbt%rowtype;
    v_acc  d707acc_dbt%rowtype;
    v_decr daidecr_dbt%rowtype;

    v_A01  VARCHAR(5);
    v_A5   NUMBER := 0;
    v_A6   NUMBER := 0;
    v_A7   NUMBER := 0;
    v_A8   NUMBER := 0;
    v_A9   NUMBER := 0;
    v_Itog NUMBER := 0;
    
    v_MP   AvrMPData_t;
    v_tmp_sum NUMBER := 0;
    v_MarketPrice NUMBER;
    v_NKD  NUMBER := 0;
    v_NKDRate NUMBER := 0;
    v_cr   NUMBER;
    v_cf_price NUMBER;
    v_mf_price NUMBER;
    v_pos_amount NUMBER;
    v_fw_price NUMBER;
    v_fw_ba_price NUMBER;
    v_rate NUMBER;
    v_op_price NUMBER;
    v_op_ba_price NUMBER;
    v_other_price NUMBER;
    v_pfi NUMBER;
    v_UseNKDRate BOOLEAN := Rsb_Common.GetRegBoolValue('SECUR\РАСЧЕТ НКД ПО КУРСУ', 0);
  BEGIN
    SELECT * INTO v_ds
      FROM D707DS_DBT
    WHERE t_SessionID = pSessionID
      AND t_ID = pClientContrID;

    RSB_SPREPFUN.g_SessionID := pSessionID;
    RSB_SPREPFUN.g_RepKind   := 0;

    IF RSI_RSBCALENDAR.IsWorkDay(pEndDate) = 1 THEN
      v_EndWorkDate := pEndDate;
    ELSE
      v_EndWorkDate := RSI_RSBCALENDAR.GetDateAfterWorkDay(pEndDate, -1);
    END IF;

    IF v_ds.t_NotResident = CNST.SET_CHAR THEN
      IF CHeckPartyType(v_ds.t_Party, PTK_INTERNATIONAL_ORG) = 1 THEN
        IF HasOGRN(v_ds.t_party) = 1 THEN
          v_A01 := '996';
        ELSE
          v_A01 := '998';
        END IF;
      ELSE
        v_A01 := GetCountryCode(v_ds.t_NameObject);
      END IF;
    ELSE
      v_A01 := GetOkatoCode(v_ds.t_party, pEndDate);
      IF v_A01 = CHR(1) THEN
        v_A01 := '00';
      END IF;
    END IF;
    
    
    IF v_A01 = CHR(1) THEN
     v_A01 := '999';
    END IF;

    IF v_ds.t_sfsk = PTSK_STOCKDL THEN
      FOR one_rec IN (
         select distinct mc.t_account, mc.t_currency vn, fin.t_fiid fiid, fin.t_name fi_name, rsb_account.restac(mc.t_account, mc.t_currency, pEndDate, mc.t_Chapter, null) count,  
                acc.t_Kind_Account ka, fin.t_facevaluefi fvfi, a.t_isin, av_k.t_name as avr_kind, 
                (select RSI_RSB_FIInstr.FI_GetNominalOnDate(fin.t_fiid, v_EndWorkDate) from dual) as nominal, 
                (select NVL(rsb_secur.GetObjAttrName(12, 1, NVL(rsb_secur.GetMainObjAttr(12, LPAD (fin.t_fiid , 10, '0'), 1, pEndDate), 0)),'') from dual) as avr_type, 
                NVL ( (SELECT mrkt.t_Market 
                         FROM ddlmarket_dbt mrkt, ddldepset_dbt dep 
                        WHERE dep.t_Depositary = mc.t_Place AND mrkt.t_DepSetId = dep.t_DepSetId AND ROWNUM = 1), 
                     -1) 
                   t_MarketId,
                 (case when RSB_FIInstr.FI_AvrKindsGetRoot( fin.t_FI_KIND, fin.t_AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_INVESTMENT_SHARE then 1
                        else 0 
                   end) IsInvestShare
                  from dmcaccdoc_dbt mc, dfininstr_dbt fin, daccount_dbt acc, davoiriss_dbt a, davrkinds_dbt av_k 
                 where  mc.t_owner = v_ds.t_Party and mc.t_clientcontrid = v_ds.t_ID 
                   and mc.t_Chapter = 22 
                   and mc.t_CatID IN (352, 364, 365) 
                   and mc.t_iscommon = CHR(88)
                   and fin.t_fiid = mc.t_fiid  
                   and fin.t_fiid = a.t_fiid  
                   and acc.t_account = mc.t_account  
                   and acc.t_chapter =  mc.t_Chapter 
                   and fin.t_avoirkind = av_k.t_avoirkind 
                   and av_k.t_fi_kind = fin.t_fi_kind
                   and acc.t_code_currency = mc.t_currency)
      LOOP
        IF one_rec.count = 0 THEN
           CONTINUE;
        END IF;
        
        v_MP := GetAvoirMarketPrice(one_rec.vn, one_rec.fvfi, pEndDate, one_rec.t_MarketId);
        
        IF v_MP.t_Message IS NOT NULL THEN
          RSB_SPREPFUN.AddRepError(v_MP.t_Message);
          --CONTINUE;
        END IF;
        
        IF    v_MP.t_MarketPrice IS NOT NULL
           AND v_MP.t_MarketPrice > 0.0 
        THEN
           IF (v_UseNKDRate) THEN
             v_NKDRate := GetRateU (one_rec.vn, pEndDate, one_rec.fvfi, RATETYPE_NKD, one_rec.t_MarketId);
           END IF;
           IF v_NKDRate IS NULL THEN
             v_NKD := RSI_RSB_FIINSTR.CalcNKD (one_rec.vn, pEndDate, ABS(one_rec.count), 0);
           ELSE
             v_NKD := v_NKDRate * ABS(one_rec.count);
           END IF;
         
           v_tmp_sum     := v_MP.t_MarketPrice * ABS(one_rec.count) + v_NKD;
           v_tmp_sum     := v_tmp_sum * v_MP.t_Rate;
           v_NKD         := v_NKD  * v_MP.t_Rate;
           v_MarketPrice := v_MP.t_MarketPrice;
           --v_result.t_Course := v_MP.t_MarketPrice + v_NKD / pCount;
           --v_result.t_Rate   := v_MP.t_Rate;
        ELSE
           v_tmp_sum     := 0;
           v_NKD         := 0;
           v_MarketPrice := 0;
        END IF;
        v_deal.t_SessionID   := pSessionID;
        v_deal.t_ContrID     := v_ds.t_ID;
        v_deal.t_Country     := v_A01;
        v_deal.t_AvrKind     := one_rec.avr_kind;
        v_deal.t_AvrType     := one_rec.avr_type;
        v_deal.t_FIName      := one_rec.fi_name;
        v_deal.t_ISIN        := one_rec.t_ISIN;
        
        IF (one_rec.IsInvestShare = 1) THEN
           v_deal.t_SecQnty := ABS(ROUND(one_rec.count, 5));
        ELSE
           v_deal.t_SecQnty := ABS(one_rec.count);
        END IF;
        
        v_deal.t_SecQnty     := ABS(one_rec.count);
        v_deal.t_Nominal     := one_rec.nominal;
        v_deal.t_FIID        := nvl(v_MP.t_FindCourseFi, one_rec.vn);
        v_deal.t_MarketPrice := v_MarketPrice;
        v_deal.t_Rate        := v_MP.t_Rate;
        v_deal.t_TotalCost   := v_tmp_sum;
        v_deal.t_NKD         := v_NKD;

        deal_ins.Extend();
        deal_ins(deal_ins.Last) := v_deal;
        
        v_A6 := v_A6 + v_tmp_sum;
      END LOOP;
      
      IF deal_ins IS NOT EMPTY THEN
        FORALL i IN deal_ins.First .. deal_ins.Last
          INSERT INTO d707deal_dbt
            VALUES deal_ins(i);
      END IF;
      
      FOR one_rec IN ( SELECT t_kind, t_amount, t_vn, t_IsSale, t_IsRepo, t_type 
                         FROM D707STOCK1_DBT
                        WHERE t_SessionID = pSessionID
                          AND t_party = v_ds.t_Party
                          AND t_id = v_ds.t_ID )
      LOOP
        IF    CASE
                WHEN one_rec.t_IsRepo = 1
                  THEN CASE WHEN one_rec.t_IsSale = 1 THEN 0 ELSE 1 END
                ELSE one_rec.t_IsSale
              END = one_rec.t_kind
          AND one_rec.t_type = RSI_DLRQ.DLRQ_TYPE_PAYMENT
        THEN
          CONTINUE;
        END IF;
        
        v_tmp_sum := 0.0;
        
        IF one_rec.t_vn != RSI_RSB_FIINSTR.NATCUR THEN
          v_cr := RSB_SPREPFUN.GetRateOnDateCrossDbl_Ex(pEndDate, one_rec.t_vn, RSI_RSB_FIINSTR.NATCUR, 1);
          v_tmp_sum := v_cr * one_rec.t_amount;
        ELSE
          v_tmp_sum := ABS(one_rec.t_amount); 
        END IF;
        
        v_A7 := v_A7 + CASE 
                         WHEN one_rec.t_kind = RSI_DLRQ.DLRQ_KIND_REQUEST
                           THEN v_tmp_sum
                         ELSE -v_tmp_sum
                       END;
      END LOOP;
    END IF;

    IF v_ds.t_sfsk = PTSK_VEKSACC THEN
      FOR one_rec IN (select (RSI_RSB_FIInstr.ConvSum(pm.t_baseamount, pm.t_basefiid, RSI_RSB_FIINSTR.NATCUR, pEndDate, 0 ) * (CASE WHEN pm.t_receiver = t.t_ClientId THEN 1 ELSE -1 END)) as t_rest
                        from ddl_tick_dbt t, dpmpaym_dbt pm
                       where t.t_clientid = v_ds.t_Party
                         and t.t_clientcontrid = v_ds.t_ID
                         and t.t_dealstatus = DL_READIED
                         and pm.t_dockind = t.t_bofficekind
                         and t.t_dealdate <= pEndDate
                         and (t.t_closedate > pEndDate or t.t_closedate = to_date('01010001', 'ddmmyyyy'))
                         and pm.t_documentid = t.t_dealid
                         and pm.t_valuedate > pEndDate
                         and pm.t_paymstatus <> RSB_PAYMENT.PM_REJECTED
                         and pm.T_PURPOSE in (RSB_PAYMENT.CAi, RSB_PAYMENT.PM_PURP_VSBARTERDIFF, RSB_PAYMENT.PM_PURP_PRINC_RET, RSB_PAYMENT.PM_PURP_PERCENT)
                         and t.t_bofficekind in (RSB_BILL.DL_VEKSELACCOUNTED, DL_VAREPAY, DL_VAPAWN, RSB_BILL.DL_VAENWR))
      LOOP
        v_A7 := v_A7 + one_rec.t_rest;
      END LOOP;
    END IF;
    
    IF v_ds.t_sfsk IN (PTSK_DV, PTSK_CM) THEN
      FOR one_rec IN ( select dvn.t_dvkind kind,
                              dvn.t_id id,
                              dvn.t_dockind dockind,
                              dvn.t_type type,
                              dvn.t_optiontype ot,
                              dvn.t_optionstyle os,
                              fin.t_name name,
                              fin.t_fiid base_fiid,
                              fin.t_fi_code fi_code,
                              fin.t_fi_kind fi_kind,
                              fin.t_facevaluefi vn,
                              dvnfi.t_amount amount,
                              -1 fiid,
                              dvnfi.t_price price,
                              dvnfi.t_pricefiid pricefiid,
                              0 t_MaxLastPrice
                         from ddvndeal_dbt dvn, ddvnfi_dbt dvnfi, dfininstr_dbt fin
                        where dvn.t_client = v_ds.t_Party
                          and dvn.t_ClientContr = v_ds.t_ID
                          and EXISTS (SELECT 1 FROM doproper_dbt  o WHERE o.t_kind_operation = dvn.t_kind  and o.t_documentid = dvn.t_id)
                          and dvn.t_date <= pEndDate
                          and dvn.t_state > 0
                          and exists (select 1
                                        from dpmpaym_dbt p
                                       where p.t_dockind = dvn.t_dockind
                                         and p.t_documentid = dvn.t_id
                                         and p.t_valuedate > pEndDate
                                         and p.t_paymstatus <> RSB_PAYMENT.PM_REJECTED)
                          and dvn.t_dvkind not in (RSB_DERIVATIVES.DV_PCTSWAP, RSB_DERIVATIVES.DV_BANKNOTE_FX)
                          and dvnfi.t_dealid = dvn.t_id
                          and dvnfi.t_type = case when dvn.T_Forvard = 'X' then RSB_DERIVATIVES.DV_NFITYPE_FORWARD else RSB_DERIVATIVES.DV_NFITYPE_BASEACTIV end
                          and fin.t_fiid = dvnfi.t_fiid )
      LOOP
        IF    one_rec.DocKind = RSB_DERIVATIVES.DL_DVDEAL
          AND one_rec.Kind = RSB_DERIVATIVES.DV_DERIVATIVE_FUTURES
        THEN
          IF one_rec.PriceFIID != RSI_RSB_FIINSTR.NATCUR THEN
            IF RSB_SPREPFUN.SmartConvertSum(v_cf_price, one_rec.Price, pEndDate, one_rec.PriceFIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
            THEN
              CONTINUE;
            END IF;
            
            IF RSB_SPREPFUN.SmartConvertSum(v_mf_price, one_rec.t_MaxLastPrice, pEndDate, one_rec.PriceFIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
            THEN
              CONTINUE;
            END IF;
          ELSE
            v_cf_price := one_rec.Price;
            v_mf_price := one_rec.t_MaxLastPrice;
          END IF;
          
          v_pos_amount := 0.0;
          
          BEGIN
            select abs(t.t_LongPosition - t.t_ShortPosition) amount
              into v_pos_amount 
              from ddvfiturn_dbt t, ddvdeal_dbt dv
             where   dv.t_id = one_rec.id
                 and dv.t_fiid = t.t_fiid
                 and dv.t_broker = t.t_broker
                 and dv.t_clientcontr = t.t_clientcontr
                 and dv.t_brokercontr = t.t_brokercontr
                 and dv.t_department = t.t_department
                 and dv.t_genagrid = t.t_genagrid
                 and t.t_date <= pEndDate;
          EXCEPTION
            WHEN NO_DATA_FOUND
              THEN NULL;
          END;
          
          v_tmp_sum := (v_mf_price - v_cf_price) * v_pos_amount;
          IF one_rec.type = 1 then
             v_tmp_sum := - v_tmp_sum;
          END IF;

          IF   one_rec.fi_kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY
            OR one_rec.fi_kind = RSI_RSB_FIINSTR.FIKIND_INDEX
          THEN
            v_A7 := v_A7 + v_tmp_sum;
          ELSIF one_rec.fi_kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS THEN
            v_A8 := v_A8 + v_tmp_sum;
          ELSE
            v_A9 := v_A9 + v_tmp_sum;
          END IF;
        ELSIF one_rec.DocKind = RSB_DERIVATIVES.DL_DVNDEAL
          AND one_rec.Kind = RSB_DERIVATIVES.DV_FORWARD
        THEN
          IF one_rec.PriceFIID != RSI_RSB_FIINSTR.NATCUR THEN
            IF RSB_SPREPFUN.SmartConvertSum(v_fw_price, one_rec.Price, pEndDate, one_rec.PriceFIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
            THEN
              CONTINUE;
            END IF;
          ELSE
            v_fw_price := one_rec.Price;
          END IF;

          IF   one_rec.FI_Kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY THEN
            IF RSB_SPREPFUN.SmartConvertSum(v_fw_ba_price, 1.0, pEndDate, one_rec.base_FIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
            THEN
              CONTINUE;
            END IF;        
          ELSIF  one_rec.FI_Kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS THEN
            v_MarketPrice := RSI_RSB_FIINSTR.ConvSumType (1.0, one_rec.base_FIID, one_rec.vn, c_MPType, pEndDate);
            IF v_MarketPrice IS NOT NULL THEN
              IF one_rec.vn != RSI_RSB_FIINSTR.NATCUR THEN
                IF RSB_SPREPFUN.SmartConvertSum(v_rate, 1.0, pEndDate, one_rec.vn, RSI_RSB_FIINSTR.NATCUR, 1) != 0
                THEN
                  CONTINUE;
                END IF;              
              ELSE
                v_rate := 1;
              END IF;
              
              v_fw_ba_price := v_rate * v_MarketPrice;
            ELSE
              v_fw_ba_price := 0;
              RSB_SPREPFUN.AddRepError('Не определена рыночная стоимость ц/б "' || one_rec.name || '"');
              CONTINUE;
            END IF;
          ELSE
            v_fw_ba_price := GetAnyLastCourseOnDate (pEndDate, one_rec.base_FIID);
            IF v_fw_ba_price IS NULL THEN
              CONTINUE;
            END IF;
          END IF;
          
          v_tmp_sum := (v_fw_ba_price - v_fw_price) * one_rec.Amount;
          IF one_rec.type = RSB_DERIVATIVES.ALG_DV_BUY THEN
            v_tmp_sum := - v_tmp_sum;
          END IF;

          IF   one_rec.fi_kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY
            OR one_rec.fi_kind = RSI_RSB_FIINSTR.FIKIND_INDEX
          THEN
            v_A7 := v_A7 + v_tmp_sum;
          ELSIF one_rec.fi_kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS THEN
            v_A8 := v_A8 + v_tmp_sum;
          ELSE
            v_A9 := v_A9 + v_tmp_sum;
          END IF;
        
        ELSIF (   one_rec.DocKind = RSB_DERIVATIVES.DL_DVDEAL
              AND one_rec.Kind = RSB_DERIVATIVES.DV_DERIVATIVE_OPTION)
           OR (   one_rec.DocKind = RSB_DERIVATIVES.DL_DVNDEAL
              AND one_rec.Kind = RSB_DERIVATIVES.DV_OPTION)
        THEN
          IF one_rec.PriceFIID != RSI_RSB_FIINSTR.NATCUR THEN
            IF RSB_SPREPFUN.SmartConvertSum(v_op_price, one_rec.Price, pEndDate, one_rec.PriceFIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
            THEN
              CONTINUE;
            END IF;
          ELSE
            v_op_price := one_rec.Price;
          END IF;

          IF   one_rec.FI_Kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY THEN
            IF RSB_SPREPFUN.SmartConvertSum(v_op_ba_price, 1.0, pEndDate, one_rec.base_FIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
            THEN
              CONTINUE;
            END IF;        
          ELSIF  one_rec.FI_Kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS THEN
            v_MarketPrice := RSI_RSB_FIINSTR.ConvSumType (1.0, one_rec.base_FIID, one_rec.vn, c_MPType, pEndDate);
            IF v_MarketPrice IS NOT NULL THEN
              IF one_rec.vn != RSI_RSB_FIINSTR.NATCUR THEN
                IF RSB_SPREPFUN.SmartConvertSum(v_rate, 1.0, pEndDate, one_rec.vn, RSI_RSB_FIINSTR.NATCUR, 1) != 0
                THEN
                  CONTINUE;
                END IF;              
              ELSE
                v_rate := 1;
              END IF;
              
              v_op_ba_price := v_rate * v_MarketPrice;
            ELSE
              v_op_ba_price := 0;
              RSB_SPREPFUN.AddRepError('Не определена рыночная стоимость ц/б "' || one_rec.name || '"');
              CONTINUE;
            END IF;
          ELSE
            v_op_ba_price := GetAnyLastCourseOnDate (pEndDate, one_rec.base_FIID);
            IF v_op_ba_price IS NULL THEN
              CONTINUE;
            END IF;
          END IF;
          
          v_tmp_sum := (v_op_ba_price - v_op_price) * one_rec.Amount;
          IF       one_rec.DocKind = RSB_DERIVATIVES.DL_DVDEAL
               AND one_rec.type = 1
            OR     one_rec.DocKind = RSB_DERIVATIVES.DL_DVDEAL
               AND one_rec.type = RSB_DERIVATIVES.ALG_DV_BUY
          THEN
            v_tmp_sum := - v_tmp_sum;
          END IF;

          IF   one_rec.fi_kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY
            OR one_rec.fi_kind = RSI_RSB_FIINSTR.FIKIND_INDEX
          THEN
            v_A7 := v_A7 + v_tmp_sum;
          ELSIF one_rec.fi_kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS THEN
            v_A8 := v_A8 + v_tmp_sum;
          ELSE
            v_A9 := v_A9 + v_tmp_sum;
          END IF;
        
        ELSE
          FOR one_rec2 IN (select p.t_amount amount, p.t_payfiid fiid, f.t_facevaluefi vn, f.t_fi_kind fi_kind, f.t_name,
                                  case when p.t_payer = v_ds.t_Party then 0 else 1 end IsReq
                             from dpmpaym_dbt p, dfininstr_dbt f
                            where p.t_dockind = one_rec.DocKind and p.t_documentid = one_rec.id and p.t_payfiid = f.t_fiid
                              and p.t_valuedate > pEndDate
                              and p.t_paymstatus <> RSB_PAYMENT.PM_REJECTED)
          LOOP
            IF   one_rec2.FI_Kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY THEN
              IF one_rec2.FIID != RSI_RSB_FIINSTR.NATCUR THEN
                IF RSB_SPREPFUN.SmartConvertSum(v_other_price, one_rec2.amount, pEndDate, one_rec2.FIID, RSI_RSB_FIINSTR.NATCUR, 1) != 0
                THEN
                  CONTINUE;
                END IF;
              ELSE
                v_other_price := one_rec2.amount;
              END IF;        
            ELSIF  one_rec2.FI_Kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS THEN
              v_MarketPrice := RSI_RSB_FIINSTR.ConvSumType (1.0, one_rec2.FIID, one_rec2.vn, c_MPType, pEndDate);
              IF v_MarketPrice IS NOT NULL THEN
                IF one_rec.vn != RSI_RSB_FIINSTR.NATCUR THEN
                  IF RSB_SPREPFUN.SmartConvertSum(v_rate, 1.0, pEndDate, one_rec2.vn, RSI_RSB_FIINSTR.NATCUR, 1) != 0
                  THEN
                    CONTINUE;
                  END IF;              
                ELSE
                  v_rate := 1;
                END IF;
                
                v_other_price := v_rate * v_MarketPrice * one_rec2.amount;
              ELSE
                v_other_price := 0;
                RSB_SPREPFUN.AddRepError('Не определена рыночная стоимость ц/б "' || one_rec2.t_name || '"');
                CONTINUE;
              END IF;
            ELSE
              v_other_price := GetAnyLastCourseOnDate (pEndDate, one_rec2.FIID);
              IF v_op_ba_price IS NULL THEN
                CONTINUE;
              END IF;
              v_other_price := v_other_price * one_rec2.amount;
            END IF;

            IF one_rec2.IsReq = 0 THEN
              v_tmp_sum := - v_other_price;
            ELSE
              v_tmp_sum := v_other_price;
            END IF; 

            IF   one_rec.fi_kind = RSI_RSB_FIINSTR.FIKIND_CURRENCY
              OR one_rec.fi_kind = RSI_RSB_FIINSTR.FIKIND_INDEX
            THEN
              v_A7 := v_A7 + v_tmp_sum;
            ELSIF one_rec.fi_kind = RSI_RSB_FIINSTR.FIKIND_AVOIRISS THEN
              v_A8 := v_A8 + v_tmp_sum;
            ELSE
              v_A9 := v_A9 + v_tmp_sum;
            END IF;            
          END LOOP;
        END IF;
      END LOOP;
    END IF;

    IF v_ds.t_sfsk = PTSK_STOCKDL THEN
      FOR one_rec IN (select stck2.t_kind, stck2.t_amount, stck2.t_fiid, stck2.t_vn, stck2.t_fi_name, stck2.t_IsSale, stck2.t_IsRepo, stck2.t_MarketId,
                             fin.t_faceValueFi fvfi, avr.t_isin as isin, TO_CHAR(avr.t_begplacementdate, 'dd.mm.yyyy') as bpldate, RSI_DLRQ.DLRQ_TYPE_DELIVERY t_Type
                        from D707STOCK2_DBT stck2, dfininstr_dbt fin, davoiriss_dbt avr
                       where stck2.t_SessionID = pSessionID
                         and stck2.t_party = v_ds.t_Party
                         and stck2.t_id = v_ds.t_ID
                         and fin.t_fiid = stck2.t_fiid
                         and avr.t_fiid = fin.t_fiid
                    UNION ALL
                    SELECT t_kind, t_amount, t_vn, -1, null, t_IsSale, t_IsRepo, -1, -1, null, null, t_type
                      FROM D707STOCK1_DBT
                     WHERE t_SessionID = pSessionID
                       AND t_party = v_ds.t_Party
                       AND t_id = v_ds.t_ID
                       AND t_type = RSI_DLRQ.DLRQ_TYPE_COMISS)
      LOOP
        IF one_rec.t_amount = 0 THEN
          CONTINUE;
        END IF;
        
        IF one_rec.t_Type = RSI_DLRQ.DLRQ_TYPE_DELIVERY THEN
          IF CASE
               WHEN one_rec.t_IsRepo = 1
                 THEN CASE WHEN one_rec.t_IsSale = 1 THEN 0 ELSE 1 END
               ELSE one_rec.t_IsSale
             END != one_rec.t_kind
          THEN
            CONTINUE;
          END IF;
          
          v_MP := GetAvoirMarketPrice(one_rec.t_fiid, one_rec.fvfi, pEndDate, one_rec.t_MarketId);
          
          IF v_MP.t_Message IS NOT NULL THEN
            RSB_SPREPFUN.AddRepError(v_MP.t_Message);
            CONTINUE;
          END IF;

          IF    v_MP.t_Rate IS NOT NULL
            AND v_MP.t_Rate <> 0.0
          THEN
            IF (v_UseNKDRate) THEN
             v_NKDRate := GetRateU (one_rec.t_fiid, pEndDate, one_rec.fvfi, RATETYPE_NKD, one_rec.t_MarketId);
            END IF;
            IF v_NKDRate IS NULL THEN
              v_NKD := RSI_RSB_FIINSTR.ConvSum( RSI_RSB_FIINSTR.CalcNKD (one_rec.t_fiid, pEndDate, one_rec.t_Amount, 0),
                                                one_rec.fvfi,
                                                v_MP.t_FindCourseFI,
                                                pEndDate);
            ELSE
              v_NKD := RSI_RSB_FIINSTR.ConvSum( v_NKDRate * one_rec.t_Amount,
                                                one_rec.fvfi,
                                                v_MP.t_FindCourseFI,
                                                pEndDate);
            END IF;
            v_tmp_sum := v_MP.t_Rate * v_MP.t_MarketPrice * one_rec.t_Amount + v_NKD;
          ELSE
            v_tmp_sum := 0.0;
            RSB_SPREPFUN.AddRepError('Не определен курс для ФИ ' || one_rec.fvfi);
          END IF;
          
        ELSIF one_rec.t_Type = RSI_DLRQ.DLRQ_TYPE_COMISS THEN
          IF one_rec.t_fiid != RSI_RSB_FIINSTR.NATCUR THEN
            v_cr := RSB_SPREPFUN.GetRateOnDateCrossDbl_Ex(pEndDate, one_rec.t_fiid, RSI_RSB_FIINSTR.NATCUR, 1);
            v_tmp_sum := v_cr * one_rec.t_amount;
          ELSE
            v_tmp_sum := ABS(one_rec.t_amount); 
          END IF;
        END IF;
        
        v_A8 := v_A8 + CASE
                         WHEN one_rec.t_Kind = RSI_DLRQ.DLRQ_KIND_REQUEST
                           THEN v_tmp_sum
                         ELSE - v_tmp_sum
                       END;
      END lOOP;
    END IF;
    
    IF v_ds.t_sfsk = PTSK_VEKSACC THEN
      FOR one_rec IN (select t.t_Name as t_FiidName, t.t_facevaluefi as t_pfi, pm.t_baseamount as t_baseamount, pm.t_basefiid as t_fiid, CASE WHEN pm.t_receiver = tick.t_ClientId THEN 1 ELSE -1 END as t_coef
                        from dfininstr_dbt t, ddl_tick_dbt tick, dpmpaym_dbt pm
                       where tick.t_clientid = v_ds.t_Party
                         and tick.t_clientcontrid = v_ds.t_ID
                         and tick.t_dealdate <= pEndDate
                         and pm.t_basefiid = t.t_FIID
                         and tick.t_dealstatus = DL_READIED
                         and (tick.t_closedate > pEndDate or tick.t_closedate = to_date('01010001', 'ddmmyyyy'))
                         and pm.t_dockind = tick.t_bofficekind
                         and pm.t_documentid = tick.t_dealid
                         and pm.t_valuedate > pEndDate
                         and pm.t_paymstatus <> RSB_PAYMENT.PM_REJECTED
                         and pm.T_PURPOSE in RSB_PAYMENT.BAi
                         and tick.t_bofficekind in (RSB_BILL.DL_VEKSELACCOUNTED, DL_VAREPAY, DL_VAPAWN, RSB_BILL.DL_VAENWR))
      LOOP
        IF one_rec.t_PFI = -1 THEN
          v_pfi := RSI_RSB_FIINSTR.NATCUR;
        ELSE
          v_pfi := one_rec.t_PFI;
        END IF;
        
        v_MarketPrice := RSI_RSB_FIINSTR.ConvSumType (1.0, one_rec.t_FIID, v_pfi, c_MPType, pEndDate);
        IF v_MarketPrice IS NOT NULL AND v_MarketPrice > 0.0 THEN
          IF v_pfi != RSI_RSB_FIINSTR.NATCUR THEN
            v_rate := RSB_SPREPFUN.GetRateOnDateCrossDbl_Ex(pEndDate, v_pfi, RSI_RSB_FIINSTR.NATCUR, 1);
            IF v_rate IS NULL OR v_rate = 0 THEN
              v_rate := 1;
            END IF;
          ELSE
            v_rate := 1;
          END IF;

          v_tmp_sum := v_rate * v_MarketPrice * one_rec.t_BaseAmount * one_rec.t_Coef;
        ELSE
          v_tmp_sum := 0.0;
          RSB_SPREPFUN.AddRepError('Не определена рыночная стоимость ц/б "' || one_rec.t_FIIDName || '"');
        END IF;
        
        v_A8 := v_A8 + v_tmp_sum;
      END LOOP;
    END IF;
/*    
    FOR one_rec IN ( WITH cat AS (SELECT t_ID
                                    FROM dmccateg_dbt
                                   WHERE     t_LevelType = 1
                                         AND t_Code IN ('ДС клиента, ц/б', 'Брокерский счет ДБО'))
                     SELECT mcacc.t_Code_Currency t_Cur, mcacc.t_AccountId, mcacc.t_Account,
                            rsb_account.restac (mcacc.t_Account,
                                                mcacc.t_Code_Currency,
                                                pEndDate,
                                                mcacc.t_Chapter,
                                                NULL) t_Sum
                       FROM (SELECT DISTINCT acc.t_Code_Currency, acc.t_Account, acc.t_Chapter,
                                             acc.t_AccountId
                               FROM cat, dmcaccdoc_dbt mc, daccount_dbt acc
                              WHERE     mc.t_CatID = cat.t_ID
                                    AND mc.t_owner = v_ds.t_Party AND mc.t_ClientContrID = v_ds.t_ID
                                    AND acc.t_Account = mc.t_Account
                                    AND acc.t_Chapter = mc.t_Chapter
                                    AND acc.t_Code_Currency = mc.t_Currency
                                    AND acc.t_Open_Date <= pEndDate
                                    AND (   acc.t_Close_Date = TO_DATE('01.01.0001', 'DD.MM.YYYY')
                                         OR acc.t_Close_Date >= pBegDate) ) mcacc)
    LOOP
      IF one_rec.t_cur != RSI_RSB_FIINSTR.NATCUR THEN
        v_cr := RSB_SPREPFUN.GetRateOnDateCrossDbl_Ex(pEndDate, one_rec.t_cur, RSI_RSB_FIINSTR.NATCUR, 1);
        v_tmp_sum := v_cr * one_rec.t_sum;
      ELSE
        v_tmp_sum := one_rec.t_sum;
      END IF;
    END LOOP;*/
    
    SELECT NVL( SUM ( t_rest * t_cr ), 0)
      INTO v_A5
      FROM D707ACC_DBT
     WHERE t_SessionID = pSessionID
       AND t_ContrID = v_ds.t_ID;
    
    v_Itog := v_A5 + v_A6 + v_A7 + v_A8 + v_A9;
    
    update daidecr_dbt set    T_COUNTRY = v_A01,
                              T_A5=v_A5,
                              T_A6=v_A6,
                              T_A7=v_A7,
                              T_A8=v_A8,
                              T_A9=v_A9,
                              T_ITOG= v_ITOG where T_SESSIONID = pSessionID and 
                           T_CONTRID = v_ds.t_ID ;
     
    if sql%rowcount = 0  then                      

    INSERT INTO daidecr_dbt ( T_SESSIONID,
                              T_CONTRID,
                              T_COUNTRY,
                              T_A5,
                              T_A6,
                              T_A7,
                              T_A8,
                              T_A9,
                              T_ITOG)
      VALUES (pSessionID, v_ds.t_ID, v_A01, v_A5, v_A6, v_A7, v_A8, v_A9, v_Itog);
     end if;
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        RSB_SPREPFUN.AddRepError('Ошибка при обработке договора ' || v_ds.t_sfnum || ' ' ||SQLERRM);
      END;
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
      FROM D707DS_DBT
     WHERE t_SessionID = pSessionID;

    IF v_Cnt > 0 THEN
      IF(pParallelLevel > 0) THEN
        v_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;
        DBMS_PARALLEL_EXECUTE.create_task (task_name => v_task_name);

        v_sql_chunks := 'SELECT t_ID, t_SessionID ' ||
                        '  FROM ( SELECT t_ID, t_SessionID, ' ||
                        '                NTILE(' || TO_CHAR(pPartitionCount) || ') OVER ( PARTITION BY t_SessionID ORDER BY t_ID) t_PartNum ' ||
                        '           FROM D707DS_DBT WHERE t_SessionID = ' || TO_CHAR(pSessionID) || ') ' ||
                        ' WHERE t_PartNum = ' || TO_CHAR(pPartitionNum);

        DBMS_PARALLEL_EXECUTE.create_chunks_by_sql(task_name => v_task_name,
                                                         sql_stmt  => v_sql_chunks,
                                                         by_rowid  => FALSE);

        v_sql_process := 'CALL RSB_DLAIREP.ProcessDO(:start_id, :end_id, '||
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
       FOR one_do IN (SELECT t_ID FROM D707DS_DBT WHERE t_SessionID = pSessionID)
        LOOP
          RSB_DLAIREP.ProcessDO(one_do.t_ID, pSessionID, pBegDate, pEndDate);
        END LOOP;
      END IF;
    END IF;

  END CreateAllData;
  
  FUNCTION CreateDecrTableDataCSV(pSessionID IN NUMBER)
  RETURN NUMBER
  IS
    v_result    CLOB;
    v_row       VARCHAR(32767);
    v_rownum    NUMBER := 0; 
    
    v_old_party VARCHAR(60);
    v_old_contr NUMBER := -1;
    v_i         NUMBER;
    v_ServKind  VARCHAR(20);
    v_tmpsum    NUMBER;
    
    FUNCTION q_scr (pStr VARCHAR) RETURN VARCHAR
    IS
      v_i NUMBER := 0;
    BEGIN
      v_i := INSTR (pStr, '"');

      RETURN CASE WHEN v_i > 0 THEN SUBSTR (pStr, 1, v_i) || '"' || q_scr (SUBSTR (pStr, v_i + 1)) ELSE pStr END;
    END;
    
    PROCEDURE PrintLineDecription(pClientCode        IN VARCHAR,
                                  pClient            IN VARCHAR,
                                  pClientFull        IN VARCHAR,
                                  pA01               IN VARCHAR,
                                  pResidenceStatus   IN CHAR,
                                  pRegisterType      IN CHAR,
                                  pActivity          IN CHAR,
                                  pQualifedInvestor  IN CHAR,
                                  pContractNumber    IN VARCHAR,
                                  pContractOpenDate  IN DATE,
                                  pContractCloseDate IN DATE,
                                  pIndustry          IN VARCHAR,
                                  pAccount           IN VARCHAR,
                                  pCrNom             IN NUMBER,
                                  pA05               IN NUMBER,
                                  pNumDEPO           IN NUMBER,
                                  pSecQnty           IN NUMBER,
                                  pMarketSecCost     IN NUMBER,
                                  pA06               IN NUMBER,
                                  pA07               IN NUMBER,
                                  pA08               IN NUMBER,
                                  pA09               IN NUMBER,
                                  pItog              IN NUMBER,
                                  pRiskName          IN NUMBER)
    IS
    BEGIN
      v_row := '"' || q_scr(pClientCode) || '";' ||
               '"' || q_scr(pClient)     || '";' ||
               '"' || q_scr(pClientFull) || '";' ||
               '"' || pA01 || '";' ||
               CASE pResidenceStatus 
                 WHEN 'X' THEN '"нерезидент"'
                 WHEN '0' THEN '"резидент"'
                 ELSE ''
               END || ';' ||
               CASE pRegisterType
                 WHEN 'X' THEN '"физ.лицо"'
                 WHEN '0' THEN '"юр.лицо"'
                 ELSE ''
               END || ';' ||
               CASE pActivity
                 WHEN 'X' THEN '"Да"'
                 WHEN '0' THEN '"Нет"'
                 ELSE ''
               END || ';' ||
               CASE pQualifedInvestor
                 WHEN 'X' THEN '"Да"'
                 WHEN '0' THEN '"Нет"'
                 ELSE ''
               END || ';' ||
               '"' || pContractNumber || '";' ||
               TO_CHAR(pContractOpenDate, 'DD.MM.YYYY') || ';' ||
               CASE WHEN pContractCloseDate > TO_DATE('01.01.2000', 'DD.MM.YYYY')
                      THEN TO_CHAR(pContractCloseDate, 'DD.MM.YYYY')
                    ELSE ''
               END || ';' || 
               '"' || pIndustry || '";' ||
               '"' || pAccount || '";' ||
               CASE WHEN pCrNom = 0
                      THEN ''
                    ELSE pCrNom
               END || ';' ||
               CASE WHEN pA05 IS NULL
                      THEN ''
                    ELSE TRUNC(pA05, 5)
               END || ';' ||
               ' ;' ||
               ' ;' ||
               pA06 || ';' ||
               pA06 || ';' ||
               CASE WHEN pItog >= 0
                      THEN pA07
                    ELSE ''
               END || ';' ||
               CASE WHEN pItog >= 0
                      THEN pA08
                    ELSE ''
               END || ';' ||
               CASE WHEN pItog >= 0
                      THEN pA09
                    ELSE ''
               END || ';' ||
               CASE WHEN pItog >= 0
                      THEN pItog
                    ELSE ''
               END || ';' ||
               CASE pRiskName
                 WHEN RISKLEVEL_NOTINSTALL THEN '"Без уровня"' 
                 WHEN RISKLEVEL_USUAL      THEN '"Стандартный"'
                 WHEN RISKLEVEL_ELEVATED   THEN '"Повышенный"'
                 WHEN RISKLEVEL_SPECIAL    THEN '"Особый"'
               END || ';' ||
               CASE WHEN pItog < 0
                      THEN pA07
                    ELSE ''
               END || ';' ||
               CASE WHEN pItog < 0
                      THEN pA08
                    ELSE ''
               END || ';' ||
               CASE WHEN pItog < 0
                      THEN pA09
                    ELSE ''
               END || ';' ||
               CASE WHEN pItog < 0
                      THEN pItog
                    ELSE ''
               END || ';' ||
               pItog || CHR(13)|| CHR(10);
               
      DBMS_LOB.WRITEAPPEND(v_result, LENGTH(v_row), v_row);
      v_rownum := v_rownum + 1;

      IF v_rownum >= MAX_ROWCOUNT THEN
        g_DecrData.Extend();
        g_DecrData(g_DecrData.Count) := v_result;
        
        v_rownum := 0;
        
        DBMS_LOB.CREATETEMPORARY(lob_loc => v_result,
                                     cache   => true,
                                     dur     => DBMS_LOB.LOB_READWRITE);        
      END IF;
    END PrintLineDecription;
    
  BEGIN
    RSB_SPREPFUN.g_SessionID := pSessionID;
    RSB_SPREPFUN.g_RepKind   := 0;
    
    g_DecrData := DecrData_t();
  
    DBMS_LOB.CREATETEMPORARY(lob_loc => v_result,
                                 cache   => true,
                                 dur     => DBMS_LOB.LOB_READWRITE);
    
    FOR one_rec IN (SELECT decr.t_ContrID, decr.t_Country, decr.t_A5, decr.t_A6, decr.t_A7, decr.t_A8, decr.t_A9, decr.t_Itog,
                           ds.t_sname t_Client, ds.t_name t_ClientFull, ds.t_ClientCode, ds.t_sfnum t_number, ds.t_NotResident, ds.t_FL, ds.t_SfSK, ds.t_subkind, ds.t_KI, ds.t_Active, ds.t_RiskLevel,
                           ds.t_sfbeg t_DateBegin, CASE WHEN ds.t_sfcls = to_date('01.01.0001','dd.mm.yyyy') THEN to_date('01.01.2000','dd.mm.yyyy') ELSE ds.t_sfcls END t_DateClose, 
                           acc.t_Account, acc.t_rest, acc.t_cr 
                      FROM DAIDecr_dbt decr, d707ds_dbt ds, d707acc_dbt acc 
                     WHERE decr.t_SessionID = pSessionID 
                       AND ds.t_SessionID = decr.t_SessionID 
                       AND ds.t_ID = decr.t_ContrID 
                       AND acc.t_SessionID(+) = decr.t_SessionID 
                       AND acc.t_ContrID(+) = decr.t_ContrID 
                       --AND NOT ( INSTR(ds.t_sfnum, '_v') > 0 AND ds.t_Active = '0' AND t_rest = 0) 
                     ORDER BY ds.T_CLIENTCODE, ds.t_sfnum, ds.t_id, ds.t_active desc)
    LOOP
      IF v_old_contr != one_rec.t_ContrID THEN
        IF v_old_party != one_rec.T_CLIENT THEN
          v_old_party := one_rec.T_CLIENT;
        END IF;
        
        IF one_rec.t_SfSK = PTSK_STOCKDL THEN
          v_ServKind := CASE WHEN one_rec.t_SubKind = 8 
                               THEN 'Биржевой рынок'
                             ELSE 'Внебиржевой рынок'
                        END;
        ELSIF one_rec.t_SfSK = PTSK_VEKSACC THEN
          v_ServKind := 'Учтенные векселя';  
        ELSE
          v_ServKind := CASE WHEN one_rec.t_SfSK = PTSK_DV
                               THEN 'Срочный рынок'
                             ELSE 'Валютный рынок'
                        END;
        END IF;
          
        IF one_rec.t_Account IS NULL THEN
          RSB_SPREPFUN.AddRepError('Счетов для ДО ' || one_rec.t_Number || ' не найдено');
            --выведем строку без счетов
          PrintLineDecription (one_rec.t_CLIENTCODE,
                               one_rec.t_CLIENT,
                               one_rec.t_CLIENTFULL,
                               one_rec.T_COUNTRY,
                               one_rec.T_NOTRESIDENT,
                               one_rec.T_FL,
                               one_rec.T_ACTIVE,
                               one_rec.T_KI,
                               one_rec.T_NUMBER,
                               one_rec.T_DATEBEGIN,
                               one_rec.T_DATECLOSE,
                               v_ServKind,
                               '',
                               NULL,
                               NULL,
                               '',
                               NULL,
                               NULL,
                               ROUND(one_rec.t_A6/ 1000, 2),
                               ROUND(one_rec.t_A7/ 1000, 2),
                               ROUND(one_rec.t_A8/ 1000, 2),
                               ROUND(one_rec.t_A9/ 1000, 2),
                               ROUND(one_rec.t_Itog/ 1000, 2),
                               one_rec.t_RiskLevel);
          CONTINUE;
        END IF;
          
        PrintLineDecription(one_rec.T_CLIENTCODE,
                            one_rec.T_CLIENT,
                            one_rec.T_CLIENTFULL,
                            one_rec.T_COUNTRY,
                            one_rec.T_NOTRESIDENT,
                            one_rec.T_FL,
                            one_rec.T_ACTIVE,
                            one_rec.T_KI,
                            one_rec.T_NUMBER,
                            one_rec.T_DATEBEGIN,
                            one_rec.T_DATECLOSE,
                            v_ServKind,
                            one_rec.t_Account,
                            one_rec.t_cr,
                            ROUND(one_rec.t_rest * one_rec.t_cr / 1000, 5),
                            '',
                            NULL,
                            NULL,
                            ROUND(one_rec.t_A6/ 1000, 2),
                            ROUND(one_rec.t_A7/ 1000, 2),
                            ROUND(one_rec.t_A8/ 1000, 2),
                            ROUND(one_rec.t_A9/ 1000, 2),
                            ROUND(one_rec.t_Itog/ 1000, 0),
                            one_rec.t_RiskLevel);
        
        v_old_contr := one_rec.t_ContrID;
        CONTINUE;
      END IF;
        
      PrintLineDecription(one_rec.T_CLIENTCODE,
                          one_rec.T_CLIENT,
                          one_rec.T_CLIENTFULL,
                          one_rec.T_COUNTRY,
                          one_rec.T_NOTRESIDENT,
                          one_rec.T_FL,
                          one_rec.T_ACTIVE,
                          one_rec.T_KI,
                          one_rec.T_NUMBER,
                          one_rec.T_DATEBEGIN,
                          one_rec.T_DATECLOSE,
                          '',
                          one_rec.t_Account,
                          one_rec.t_cr,
                          round(one_rec.t_rest * one_rec.t_cr/1000, 5),
                          '',
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          NULL,
                          one_rec.t_RiskLevel);
    END LOOP;
    
    g_DecrData.Extend();
    g_DecrData(g_DecrData.Count) := v_result;
    
    RETURN g_DecrData.Count;
  END CreateDecrTableDataCSV;
  
  FUNCTION GetDecrTableDataCSV(pNumber NUMBER)
  RETURN CLOB
  IS
  BEGIN
    IF    g_DecrData IS NOT NULL
      AND g_DecrData.Exists (pNumber) THEN
      RETURN g_DecrData(pNumber);
    ELSE
      RETURN NULL;
    END IF;
  END GetDecrTableDataCSV;
  
  PROCEDURE ClearDecrTableDataCSV
  IS
  BEGIN
    IF g_DecrData.Count > 0 THEN
      FOR i IN g_DecrData.FIRST .. g_DecrData.LAST
      LOOP
        DBMS_LOB.FREETEMPORARY(g_DecrData(i));
      END LOOP;
    END IF;
    
    g_DecrData := NULL;  
  END;

END RSB_DLAIREP;
/