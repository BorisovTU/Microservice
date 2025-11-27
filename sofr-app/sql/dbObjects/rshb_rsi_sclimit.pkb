CREATE OR REPLACE PACKAGE BODY RSHB_RSI_SCLIMIT
AS
  /**
   @file 		RSHB_RSI_SCLIMIT.pkb
   @brief 		Утилиты для расчета лимитов
     
   # changeLog
   |date       |author         |tasks                                                     |note                                                        
   |-----------|---------------|----------------------------------------------------------|-------------------------------------------------------------
   |2024.12.27 |Зыков М.В.     | BOSS-6238 BOSS-5028                                      | Доработать параллельный расчет лимитов при включенном обособлении ДС
   |2024.07.30 |Гераськина Т.В.| BOSS-4379                                                | Доступ к сложным ФИ для ЮЛ неквалифицированных инвесторов
   |2024.07.11 |Зыков М.В.     | BOSS-2461.3,BIQ-16667                                    | Перевод процедуры расчета лимитов на обработчик сервисов QManager  
   |2024.07.03 |Велигжанин А.В.| DEF-68258                                                | функция GetCodeSCZeroLimit() для определения t_seccode
   |           |               |                                                          | при расчете нулевых лимитов
   |2024.07.04 |Велигжанин А.В.| DEF-68441                                                | доработка в RSI_CreateSecurLimByKindCurZero ()
   |           |               |                                                          | если t_stockcode не заполнен, подставляем t_ekk
   |2024.06.28 |Велигжанин А.В.| DEF-63448                                                | доработка в RSI_CreateSecurLimByKindCurZero ()
   |           |               |                                                          | t_stockcode берем из ddl_clientinfo_dbt, а не из ddlobjcode_dbt
   |2024.03.22 |Зыков МВ.      | BIQ-16667                                                | Перевод процедуры расчета лимитов на обработчик сервисов QManager  
   |2024.03.04 |Велигжанин А.В.| 62480                                                    | GetLimitPrm(), процедура для получения данных
   |           |               |                                                          | из справочника расчета лимитов.                
    
  */

   UK_id NUMBER := 114800;

   vMICEX_CODE VARCHAR2(2000) := RSB_Common.GetRegStrValue('SECUR\MICEX_CODE');
   vSPBEX_CODE VARCHAR2(2000) := RSB_Common.GetRegStrValue('SECUR\SPBEX_CODE');
   vMICEX_ID number(10) ;
   vSPBEX_ID number(10) ;


   LastErrorMessage   VARCHAR2 (1024) := '';

   RegVal_CalcWaPrice   NUMBER
      := RSB_COMMON.GetRegIntValue ('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\CALC_WA_POSITION_PRICE');
   RegVal_EDPStartDate  VARCHAR2(20)
      := RSB_COMMON.GetRegStrValue ('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ДАТА ВКЛЮЧЕНИЯ ЕДП');
   RegVal_SpecRepo      NUMBER
      := RSB_COMMON.GetRegIntValue ('SECUR\НОМЕР КАТ-ИИ - ЯВЛ.ТЕХН.РЕПО');
   RegVal_RepoWithCCPTradingModes VARCHAR2(200)
      := ','||REPLACE(RSB_COMMON.GetRegStrValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\QUIK\РЕЖ_ТОРГОВ_РЕПО_С_ЦК'),' ','')||',';

   savearch           NUMBER (5) := 0; -- 1 - архивировать расчет 0 - не архивировать (для тестирования). UPD Функционал оставил, но всегда теперь должен стоять в ноль, т.к. архивирование вынесено в макрос !
   ExcludeErrClients  NUMBER(5):= 1; -- 1 - исключиать клиентов с необработанными графиками и ЗР 0 - не исключать

  type tr_findLimitPrm is record(
       Firm  VARCHAR2 (12)
      ,Tag   VARCHAR2 (5)
      ,TrdAcc  VARCHAR2 (20)
      ,CODESCZEROLIMIT VARCHAR2(12));


   type tt_findLimitPrm is table of tr_findLimitPrm index by varchar2(100);
   gt_findLimitPrm tt_findLimitPrm ;


   /**
    @brief    Функция для журналирования времени выполнения.
   */
   FUNCTION ElapsedTime ( p_time IN pls_integer ) return varchar2 
   IS
   BEGIN
     RETURN to_char((dbms_utility.get_time - p_time) / 100, 'fm9999999990D00');
   END ElapsedTime;

   PROCEDURE TimeStamp_ (Label_           IN VARCHAR2,
                         date_               DATE,
                         start_              TIMESTAMP,
                         end_                TIMESTAMP,
                         action_             NUMBER DEFAULT NULL,
                         excepsqlcode_       NUMBER DEFAULT NULL,
                         all_log_            boolean default false
                        )
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN

     if  all_log_ or substr(g_calc_DIRECT,1,1)    = GC_CALC_SID_DEFAULT then 
       INSERT INTO DCALCLIMITLOG_DBT (
                                     T_DATE,
                                     T_LABEL,
                                     T_START,
                                     T_END,
                                     T_ACTION,
                                     T_EXCEPSQLCODE,
                                     T_CALC_DIRECT
                                    )
           VALUES (
                     date_ ,
                     substr(g_log_add||label_,1,250) ,
                     NVL (start_, NVL (end_, SYSTIMESTAMP)),
                     NVL (end_, NVL (start_, SYSTIMESTAMP)),
                     action_ ,
                     abs(excepsqlcode_),
                     g_calc_DIRECT);

       COMMIT;
     end if;
   END;

   
   FUNCTION to_chardatesql(p_Date date) return varchar2 as
   BEGIN
     RETURN ' to_date('''||to_char(p_Date,'ddmmyyyy')||''',''ddmmyyyy'')';
   END;

  PROCEDURE LockRecordsFrom(p_TableName IN VARCHAR2,
                           p_where IN VARCHAR2 default null,
                           p_hint IN VARCHAR2 default null,
                           p_mess_error IN VARCHAR2 default null )
  AS
  --pragma autonomous_transaction ;
  row_locked EXCEPTION;
  PRAGMA EXCEPTION_INIT(row_locked, -54);
  res_locked EXCEPTION;
  PRAGMA EXCEPTION_INIT(res_locked, -30006);
  LockRecors_cursor sys_refcursor ;
  v_hint varchar2(100):= case when p_hint is not null then '/*+ '||p_hint||' */' end;
  v_TableName varchar2(100) := case when INSTR(trim(p_TableName), ' ') > 0 then substr(trim(p_TableName),1,INSTR(trim(p_TableName), ' ')-1) else trim(p_TableName) end;
  BEGIN
    if p_where is null then
      execute immediate 'LOCK TABLE '||p_TableName||' IN EXCLUSIVE MODE WAIT 10';
    else
     OPEN LockRecors_cursor FOR 'select '||v_hint||' * from '||p_TableName||' where '||p_where ||' for update wait 10';
     close LockRecors_cursor;
   end if;
  EXCEPTION
    WHEN row_locked or res_locked THEN
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR,p_msg => 'ERROR LockRecorsFrom'||chr(10)||'p_TableName='||p_TableName||chr(10)||'p_where='||p_where);
      raise_application_error(-20000,nvl(p_mess_error,'Таблица '||v_TableName||' заблокирована другим процессом в системе'));
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR,p_msg => 'ERROR LockRecorsFrom'||chr(10)||'p_TableName='||p_TableName||chr(10)||'p_where='||p_where);
  END;


  PROCEDURE Gather_Table_Stats(p_TableName IN VARCHAR2)
  AS
  BEGIN
        dbms_stats.gather_table_stats(ownname => SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
        tabname => p_TableName,
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        cascade=>TRUE, method_opt=>'FOR ALL COLUMNS SIZE AUTO');
  END;

  FUNCTION GetIssuerContryId(p_Fiid IN NUMBER) RETURN NUMBER DETERMINISTIC
  IS
     v_CountryId number(10):=0;
  BEGIN
        SELECT NVL (
             (SELECT (SELECT t_countryid
                        FROM DCOUNTRY_DBT ct
                       WHERE ct.t_codelat3 =
                                CASE
                                   WHEN TRIM (pt.t_nrcountry) IN
                                           (CHR (1), CHR (0))
                                   THEN
                                      '---'
                                   ELSE
                                      TRIM (pt.t_nrcountry)
                                END)
                FROM dparty_dbt pt
               WHERE pt.t_partyid = (SELECT fin.T_ISSUER
                                       FROM dfininstr_dbt fin
                                      WHERE fin.t_fiid = p_Fiid)),
             (SELECT t_countryid
                FROM DCOUNTRY_DBT ct
               WHERE ct.t_codelat3 = 'RUS'))
             AS t_countryid
     INTO v_CountryId
     FROM DUAL;
     RETURN v_CountryId;
  END;

  FUNCTION GetAvoirKind(p_Fiid IN NUMBER) RETURN NUMBER DETERMINISTIC
  IS
     v_AvoirKind number(10):=0;
  BEGIN
     SELECT fin.T_AVOIRKIND
       INTO v_AvoirKind
       FROM dfininstr_dbt fin
      WHERE fin.t_fiid = p_Fiid;
     RETURN v_AvoirKind;
  EXCEPTION
       WHEN NO_DATA_FOUND THEN RETURN -1;
  END;

   PROCEDURE CheckCashStockForDuplAndSetErr(p_CalcDate IN DATE)
   AS
      TYPE duplRec IS RECORD
      (
         T_MARKET            DDL_LIMITCASHSTOCK_DBT.T_MARKET%TYPE,
         T_INTERNALACCOUNT   DDL_LIMITCASHSTOCK_DBT.T_INTERNALACCOUNT%TYPE,
         T_LIMIT_KIND        DDL_LIMITCASHSTOCK_DBT.T_LIMIT_KIND%TYPE,
         T_DATE              DDL_LIMITCASHSTOCK_DBT.T_DATE%TYPE
      );

      TYPE duplRecArr IS TABLE OF duplRec;

      v_duplRecArr   duplRecArr;

      v_Cursor       SYS_REFCURSOR;
   BEGIN
      OPEN v_Cursor FOR 'SELECT t_market,
                            t_internalaccount,
                            t_limit_kind,
                            t_date
             FROM (  SELECT t_market,
                            t_internalaccount,
                            t_limit_kind,
                            t_date,
                            COUNT (*) AS t_count
                       FROM DDL_LIMITCASHSTOCK_DBT
                      WHERE T_DATE = :p_CalcDate and T_ISBLOCKED <> CHR (88)
                   GROUP BY t_market,
                            t_internalaccount,
                            t_limit_kind,
                            t_date) q1
            WHERE q1.t_count > 1' using p_CalcDate ;

      LOOP
         FETCH v_Cursor
         BULK COLLECT INTO v_duplRecArr
         LIMIT 1000;

         IF v_duplRecArr.COUNT > 0
         THEN
            FORALL indx IN v_duplRecArr.FIRST .. v_duplRecArr.LAST
               UPDATE DDL_LIMITCASHSTOCK_DBT
                  SET T_ISBLOCKED = CHR (88)
                WHERE t_market = v_duplRecArr (indx).t_market
                      AND t_internalaccount = v_duplRecArr (indx).t_internalaccount
                      AND t_date = p_CalcDate ;

            FORALL indx IN v_duplRecArr.FIRST .. v_duplRecArr.LAST
               UPDATE DDL_CLIENTINFO_DBT
                  SET T_HASERRORS = CHR (88)
                      ,t_errors_reason = RSHB_RSI_SCLIMIT.add_text(t_errors_reason,'Дублирование привязки счёта. Счёт привязан к другому субдоговору')
                WHERE t_calc_sid = g_calc_clientinfo and t_client IN
                         (SELECT t_client
                            FROM DDL_LIMITCASHSTOCK_DBT
                           WHERE t_market = v_duplRecArr (indx).t_market
                                 AND t_internalaccount =
                                        v_duplRecArr (indx).t_internalaccount
                                 AND t_limit_kind =
                                        v_duplRecArr (indx).t_limit_kind
                                 AND t_date = p_CalcDate );
         END IF;

         EXIT WHEN v_Cursor%NOTFOUND;
      END LOOP;

      CLOSE v_Cursor;
   END;


  FUNCTION SfcontrIsEDP(p_SfcontrID IN NUMBER) RETURN  NUMBER DETERMINISTIC
  IS
  BEGIN
     RETURN RSB_SECUR.GetGeneralMainObjAttr (659,LPAD (p_SfcontrID, 10, '0'),102, to_date('31122999','ddmmyyyy'));
  END;

  FUNCTION GetMicexID RETURN  NUMBER DETERMINISTIC
  IS
  BEGIN
   if vMICEX_ID is null then 
     BEGIN
        select t_objectid into vMICEX_ID from dobjcode_dbt where t_objecttype = 3 and t_codekind = 1 and t_state = 0 and  t_code = vMICEX_CODE;
     EXCEPTION
        WHEN no_data_found THEN NULL;
     END;
    end if;
    RETURN vMICEX_ID;
  END;

  FUNCTION GetSpbexID RETURN  NUMBER DETERMINISTIC
  IS
  BEGIN
    if vSPBEX_ID is null then
      BEGIN
        select t_objectid into vSPBEX_ID from dobjcode_dbt where t_objecttype = 3 and t_codekind = 1 and t_state = 0 and t_code = vSPBEX_CODE;
      EXCEPTION
        WHEN no_data_found THEN NULL;
      END;
    end if;
    RETURN vSPBEX_ID;
  END;
   
   function GetPartition_calc_sid(p_calc_sid varchar2) return varchar2 as
   begin
     if p_calc_sid = GC_CALC_SID_DEFAULT
     then
       return 'P99999999X';
     else
       return 'p' || p_calc_sid;
     end if;
   end;

   FUNCTION GetDateLimitByKind(  Kind IN INTEGER, FIID IN INTEGER, IsCur IN INTEGER ,IsNatCur IN INTEGER  )RETURN DATE
   IS
   v_date Date;

   BEGIN
    IF( IsNatCur = 1) THEN
      BEGIN

      Select
      (CASE WHEN  Kind = 0 THEN T_DATE0
       WHEN  Kind = 1 THEN T_DATE1
       WHEN  Kind = 2 THEN T_DATE2
       WHEN  Kind = 365 THEN T_DATE365
       END
       )
      INTO v_date
      FROM DDL_LIMITCHECKDATE_DBT
      WHERE t_IsNatCur = CHR(88)
      AND T_ISCUR = DECODE( IsCur, 1, CHR(88), CHR(1) );

      EXCEPTION
         WHEN NO_DATA_FOUND THEN RETURN TO_DATE('31.12.2999','DD.MM.YYYY');
      END;
    ELSE
      BEGIN
      Select
      (CASE WHEN  Kind = 0 THEN T_DATE0
       WHEN  Kind = 1 THEN T_DATE1
       WHEN  Kind = 2 THEN T_DATE2
       WHEN  Kind = 365 THEN T_DATE365
       END
       )
      INTO v_date
      FROM DDL_LIMITCHECKDATE_DBT
      WHERE t_CurID = FIID
      AND T_ISCUR = DECODE( IsCur, 1, CHR(88), CHR(1) );

    EXCEPTION
       WHEN NO_DATA_FOUND THEN RETURN TO_DATE('31.12.2999','DD.MM.YYYY');
      END;

    END IF;

    RETURN v_date;
   END;


   FUNCTION GetCalendarIDForLimit(p_MarketID IN NUMBER, p_CurrencyId IN NUMBER DEFAULT -1,
                                  p_MarketPlace IN NUMBER DEFAULT -1, p_SecFiidId IN NUMBER DEFAULT -1) RETURN NUMBER deterministic
   IS
      v_calparamarr RSI_DlCalendars.calparamarr_t;
   BEGIN
      v_calparamarr('Object') := 'Расчет лимитов QUIK';
      v_calparamarr('ObjectType') := 1;
      v_calparamarr('Market') := p_MarketID;
      if (p_CurrencyId >= 0) then
         v_calparamarr('Currency') := p_CurrencyId;
      end if;
      if (p_MarketPlace > 0) then
         v_calparamarr('MarketPlace') := p_MarketPlace;
      end if;
      if (p_SecFiidId >= 0) then
         v_calparamarr('EmitCountry') := GetIssuerContryId(p_SecFiidId);
         v_calparamarr('AvoirKind') := GetAvoirKind(p_SecFiidId);
      end if;
      return RSI_DlCalendars.DL_GetCalendByDynParam(83, v_calparamarr);
   END;

   /**
    @brief    DEF-65531, Функция определения сдвига для параметра расчета лимитов
    @param[in]    p_Kind    		Параметр расчета лимитов (T1, T2)
   */
   FUNCTION GetDateShift(p_Kind IN NUMBER) RETURN NUMBER deterministic
   IS
      v_DateShift NUMBER(10) := 0;
   BEGIN
      IF p_Kind = 1 THEN v_DateShift := 1;
      ELSIF p_Kind = 2 THEN v_DateShift := 2;
      ELSIF p_Kind = -1 THEN v_DateShift := -1;
      END IF;
      RETURN v_DateShift;
   END;

   /**
    @brief    DEF-65531, Функция возвращает дату, на которую производится расчет лимита для соответствующего параметра.
                         Определяется с помощью выбора даты из календаря по всем рынкам
    @param[in]    p_Kind    		Параметр расчета лимитов (T0, T1, T2, T365)
    @param[in]    p_Date    		Дата расчета лимитов
   */
   FUNCTION GetCheckDateByAllCalendars(p_Kind IN NUMBER, p_Date IN DATE) RETURN DATE deterministic
   IS
      v_CheckDate DATE;
      v_ClnDate DATE;
      v_DateShift NUMBER(10);
      v_DayType NUMBER := 1; -- торговые календари
      v_ClnID NUMBER;
   BEGIN
      ts_ := SYSTIMESTAMP;
      v_DateShift := GetDateShift(p_Kind);
      v_CheckDate := CASE WHEN v_DateShift >= 0 THEN TO_DATE ('31.12.9999', 'DD.MM.YYYY') ELSE TO_DATE ('01.01.0001', 'DD.MM.YYYY') END;
      FOR rec IN (select distinct t_marketid from ddl_limitprm_dbt) LOOP
         FOR recCal IN ( 
           -- календари фондового рынка
           SELECT r.t_calkindID FROM ddlcalparam_dbt r WHERE r.t_identprogram = 83 -- для ФР -- модуль Ценные бумаги
           AND r.t_id in (SELECT t_calparamid FROM ddlcalparamlnk_dbt l WHERE l.t_kndcode = 'Market' AND l.t_value = TO_CHAR(rec.t_MarketId))
           AND r.t_id in (SELECT t_calparamid FROM ddlcalparamlnk_dbt l WHERE l.t_kndcode = 'DayType' AND l.t_value = TO_CHAR(v_DayType))   
           AND r.t_id in (SELECT t_calparamid FROM ddlcalparamlnk_dbt l WHERE l.t_kndcode = 'MarketPlace' AND l.t_value = TO_CHAR(1))
           UNION 
           -- календари валютного рынка
           SELECT r.t_calkindID FROM ddlcalparam_dbt r WHERE r.t_identprogram = 158 -- модуль ФИССиКО
           AND r.t_id in (SELECT t_calparamid FROM ddlcalparamlnk_dbt l WHERE l.t_kndcode = 'Market' AND l.t_value = TO_CHAR(rec.t_MarketId))
           AND r.t_id in (SELECT t_calparamid FROM ddlcalparamlnk_dbt l WHERE l.t_kndcode = 'DayType' AND l.t_value = TO_CHAR(v_DayType))
           AND r.t_id in (SELECT t_calparamid FROM ddlcalparamlnk_dbt l WHERE l.t_kndcode = 'MarketPlace' AND l.t_value = TO_CHAR(2))
           UNION 
           -- календари срочного рынка
           SELECT r.t_calkindID FROM ddlcalparam_dbt r WHERE r.t_identprogram = 158 -- модуль ФИССиКО
           AND r.t_id in (SELECT t_calparamid FROM ddlcalparamlnk_dbt l WHERE l.t_kndcode = 'Market' AND l.t_value = TO_CHAR(rec.t_MarketId))
           AND r.t_id in (SELECT t_calparamid FROM ddlcalparamlnk_dbt l WHERE l.t_kndcode = 'DayType' AND l.t_value = TO_CHAR(v_DayType))
           AND r.t_id in (SELECT t_calparamid FROM ddlcalparamlnk_dbt l WHERE l.t_kndcode = 'MarketPlace' AND l.t_value = TO_CHAR(3))
         )
         LOOP
            v_ClnDate := RSI_RSBCALENDAR.GETDATEAFTERWORKDAY(p_Date, v_DateShift, recCal.t_calkindID);
            IF ( ((p_Kind >= 0) AND (v_ClnDate < v_CheckDate))  -- если при просмотре вперед увидели меньшую дату в календаре
              OR ((p_Kind < 0) AND (v_ClnDate > v_CheckDate))   -- или при просмотре назад увидели большую дату
            ) THEN
              v_CheckDate := v_ClnDate;                         -- запоминаем найденную дату
              v_ClnID := recCal.t_calkindID;                    -- и календарь
            END IF;
         END LOOP;
      END LOOP;

      TimeStamp_ ( 'GetCheckDateByAllCalendars(), Kind: '||p_Kind
          ||', CheckDate: '||v_CheckDate 
          ||', ClnID: '|| v_ClnID
          , p_Date, ts_, SYSTIMESTAMP
      );
      return v_CheckDate;
   END;

   /**
    @brief    DEF-65531, Функция возвращает дату, на которую производится расчет лимита для соответствующего параметра.
                         Определяется, с помощью выбора даты из календаря по полученному рынку
    @param[in]    p_Kind    		Параметр расчета лимитов (T0, T1, T2, T365)
    @param[in]    p_Date    		Дата расчета лимитов
    @param[in]    p_IsEDP    		Если 1, то ЕДП
    @param[in]    p_MarketPlace      	вид рынка (1 - Фондовый, 2 - Валютный, 3 - Срочный)
   */
   FUNCTION GetCheckDateByMarketPlace(p_Kind IN NUMBER, p_Date IN DATE, p_MarketPlace IN NUMBER DEFAULT -1) RETURN DATE deterministic
   IS
      v_CheckDate DATE;
      v_ClnDate DATE;
      v_DateShift NUMBER(10);
      v_ClnID NUMBER;
   BEGIN
      ts_ := SYSTIMESTAMP;
      v_DateShift := GetDateShift(p_Kind);
      v_CheckDate := CASE WHEN v_DateShift >= 0 THEN TO_DATE ('31.12.9999', 'DD.MM.YYYY') ELSE TO_DATE ('01.01.0001', 'DD.MM.YYYY') END;
      FOR rec IN (select distinct t_marketid from ddl_limitprm_dbt) LOOP
         FOR recCal IN ( 
           SELECT r.t_calkindID FROM ddlcalparam_dbt r
           WHERE r.t_identprogram = case when p_MarketPlace = 1 then 83 else 158 end -- для ФР -- модуль Ценные бумаги, иначе -- ФИССиКО
           AND r.t_id in (SELECT t_calparamid FROM ddlcalparamlnk_dbt l WHERE l.t_kndcode = 'Market' AND l.t_value = TO_CHAR(rec.t_MarketId))
           AND r.t_id in (SELECT t_calparamid FROM ddlcalparamlnk_dbt l WHERE l.t_kndcode = 'DayType' AND l.t_value = TO_CHAR(1))   -- торговые календари
           AND r.t_id in (SELECT t_calparamid FROM ddlcalparamlnk_dbt l WHERE l.t_kndcode = 'MarketPlace' AND l.t_value = TO_CHAR(p_MarketPlace))
         )
         LOOP
            v_ClnDate := RSI_RSBCALENDAR.GETDATEAFTERWORKDAY(p_Date, v_DateShift, recCal.t_calkindID);
            IF ( ((p_Kind >= 0) AND (v_ClnDate < v_CheckDate))  -- если при просмотре вперед увидели меньшую дату в календаре
              OR ((p_Kind < 0) AND (v_ClnDate > v_CheckDate))   -- или при просмотре назад увидели большую дату
            ) THEN
              v_CheckDate := v_ClnDate;                         -- запоминаем найденную дату
              v_ClnID := recCal.t_calkindID;                    -- и календарь
            END IF;
         END LOOP;
      END LOOP;

      TimeStamp_ ( 'GetCheckDateByMarketPlace(), Kind: '||p_Kind
          ||', MarketPlace: '|| p_MarketPlace
          ||', CheckDate: '||v_CheckDate
          ||', ClnID: '|| v_ClnID
          , p_Date, ts_, SYSTIMESTAMP
      );
      return v_CheckDate;
   END;

   /**
    @brief    DEF-65531, Процедура возвращает дату, на которую производится расчет лимита для соответствующего параметра.
    @param[in]    p_Kind    		Параметр расчета лимитов (0, 1, 2, 365)
    @param[in]    p_Date    		Дата расчета лимитов
    @param[in]    p_IsEDP    		Если 1, то ЕДП
    @param[in]    p_MarketPlace      	вид рынка (1 - Фондовый, 2 - Валютный, 3 - Срочный)
   */
   FUNCTION GetCheckDate(p_Kind IN NUMBER, p_Date IN DATE, p_IsEDP IN NUMBER, p_MarketPlace IN NUMBER DEFAULT -1) RETURN DATE deterministic
   IS
      v_CheckDate DATE;
   BEGIN
      IF (p_Kind = 0) THEN 
        -- для D0 сдвиг не делаем
        v_CheckDate := p_Date;
      ELSIF (p_Kind = 365) THEN
        -- для D365 берем дальнюю дату
        v_CheckDate := TO_DATE ('31.12.9999', 'DD.MM.YYYY');
      ELSIF(p_IsEDP = 1) THEN
        -- для ЕДП, дата определяется путем просмотра торговых календарей по всем рынкам
        v_CheckDate := GetCheckDateByAllCalendars(p_Kind, p_Date);
      ELSE
        -- для неЕДП, дата определяется путем просмотра торговых календарей по рынку, переданному параметром
        v_CheckDate := GetCheckDateByMarketPlace(p_Kind, p_Date, p_MarketPlace);
      END IF;
      RETURN v_CheckDate;
   END;

   FUNCTION GetCheckDateByParams(p_Kind IN NUMBER, p_Date IN DATE, p_MarketID IN NUMBER, p_IsEDP IN NUMBER, p_CurrencyId IN NUMBER DEFAULT -1,
                                 p_MarketPlace IN NUMBER DEFAULT -1, p_SecFiidId IN NUMBER DEFAULT -1) RETURN DATE deterministic
   IS
      v_Date DATE;
      v_DateShift NUMBER(10) := 0;
   BEGIN
      v_Date := p_Date;
      IF p_Kind > 2 THEN v_Date := TO_DATE ('31.12.9999', 'DD.MM.YYYY');
      END IF;

      IF p_Kind = 1 THEN v_DateShift := 1;
      ELSIF p_Kind = 2 THEN v_DateShift := 2;
      ELSIF p_Kind = -1 THEN v_DateShift := -1;
      END IF;

      IF p_IsEDP = 1 THEN
         IF v_DateShift <> 0 THEN
            v_Date := CASE WHEN v_DateShift >= 0 THEN TO_DATE ('31.12.9999', 'DD.MM.YYYY') ELSE TO_DATE ('01.01.0001', 'DD.MM.YYYY') END;
            FOR rec IN (select distinct t_marketid from ddl_limitprm_dbt) LOOP
               FOR recCal IN ( SELECT T_CALKINDID
                  FROM DDLCALPARAMLNK_DBT lnk, DDLCALPARAM_DBT prm
                 WHERE     lnk.T_CALPARAMID = prm.T_ID
                       AND lnk.T_KNDCODE = 'Market'
                       AND lnk.T_VALUE = TO_CHAR(rec.t_MarketId)
                       AND prm.T_IDENTPROGRAM = 83)
               LOOP
                  IF (p_Kind >= 0) THEN
                     v_Date := LEAST(RSI_RSBCALENDAR.GETDATEAFTERWORKDAY(p_Date,v_DateShift,recCal.T_CALKINDID), v_Date);
                  ELSE
                     v_Date := GREATEST(RSI_RSBCALENDAR.GETDATEAFTERWORKDAY(p_Date,v_DateShift,recCal.T_CALKINDID), v_Date);
                  END IF;
               END LOOP;
            END LOOP;
         END IF;
      ELSE
         if v_DateShift <> 0 THEN
            v_Date := RSI_RSBCALENDAR.GETDATEAFTERWORKDAY (p_Date, v_DateShift, GetCalendarIDForLimit(p_MarketID, p_CurrencyId, p_MarketPlace, p_SecFiidId));
         END IF;
      END IF;
      /*DBMS_OUTPUT.put_line
      ('p_Kind: '|| p_Kind || ' p_Date: '|| p_Date || ' p_MarketID: '|| p_MarketID || ' p_IsEDP: '|| p_IsEDP || ' p_CurrencyId: '|| p_CurrencyId || ' p_MarketPlace: '|| p_MarketPlace || ' p_SecFiidId: '|| p_SecFiidId || ' v_Date: ' || v_Date || '\n');*/
      return v_Date;
   END;

   PROCEDURE SaveArchSecur (p_CalcDate IN DATE)
   AS
   pragma autonomous_transaction ;
    chk_date integer ;
   BEGIN
      ts_ := SYSTIMESTAMP;
      select /*+ full(l) */ count(*) into chk_date  from ddl_limitsecurites_dbt l
                WHERE t_date = p_CalcDate and rownum < 2;
      if chk_date > 0  then
        --bpv переносим рассчитанные лимиты в архивные таблицы. Нужны для выпуска отчета КЛ_06 остатки клиентов с лимитами для ДРРК
        LockRecordsFrom('ddl_limitsecuritesarch_dbt','t_date = '||to_chardatesql(p_CalcDate) );
        DELETE FROM ddl_limitsecuritesarch_dbt
              WHERE t_date = p_CalcDate;
        commit;
        INSERT /*+ parallel(8) enable_parallel_dml */ INTO ddl_limitsecuritesarch_dbt a
           SELECT /*+  parallel(l 8) full(l) */
                  T_ID,
                  T_DATE,
                  T_TIME,
                  T_MARKET,
                  T_CLIENT,
                  T_SECURITY,
                  T_FIRM_ID,
                  T_SECCODE,
                  T_CLIENT_CODE,
                  T_OPEN_BALANCE,
                  T_OPEN_LIMIT,
                  T_CURRENT_LIMIT,
                  T_TRDACCID,
                  T_WA_POSITION_PRICE,
                  T_LIMIT_KIND,
                  T_QUANTITY,
                  T_PLAN_PLUS_DEAL,
                  T_PLAN_MINUS_DEAL,
                  T_ISBLOCKED,
                  T_MARKET_KIND,
                  T_MONEYCONSOLIDATED
             FROM ddl_limitsecurites_dbt l
            WHERE t_date = p_CalcDate;

        TimeStamp_ (
           'Перенос расчета по бумагам в архив ('||sql%rowcount||' зап.)',
           p_CalcDate,
           ts_,
           SYSTIMESTAMP);
    else
        TimeStamp_ (
           'НЕТ расчитанных лимитов по бумаге за дату для архива',
           p_CalcDate,
           ts_,
           SYSTIMESTAMP);
    end if;
     commit;
   END;

   PROCEDURE SaveArchMoney (p_CalcDate IN DATE)
   AS
   pragma autonomous_transaction;
    chk_date integer ;
   BEGIN
      ts_ := SYSTIMESTAMP;
      select /*+ full(l) */ count(*) into chk_date  from ddl_limitcashstock_dbt l
                WHERE t_date = p_CalcDate and rownum < 2;
      if chk_date > 0 then
        LockRecordsFrom('ddl_limitcashstockarch_dbt','t_date = '||to_chardatesql(p_CalcDate) );
        DELETE FROM ddl_limitcashstockarch_dbt
              WHERE t_date = p_CalcDate;
        commit;
        INSERT /*+ parallel(8) enable_parallel_dml */ INTO ddl_limitcashstockarch_dbt a
           SELECT /*+  parallel(l 8) full(l) */
                  T_ID,
                  T_DATE,
                  T_TIME,
                  T_MARKET,
                  T_CLIENT,
                  T_INTERNALACCOUNT,
                  T_FIRM_ID,
                  T_TAG,
                  T_CURRID,
                  T_CURR_CODE,
                  T_CLIENT_CODE,
                  T_OPEN_BALANCE,
                  T_OPEN_LIMIT,
                  T_CURRENT_LIMIT,
                  T_LEVERAGE,
                  T_LIMIT_KIND,
                  T_MONEY306,
                  T_DUE474,
                  T_PLAN_PLUS_DEAL,
                  T_PLAN_MINUS_DEAL,
                  T_COMPREVIOUS,
                  T_ISBLOCKED,
                  T_MARKET_KIND,
                  0,
                  0,
                  0
             FROM ddl_limitcashstock_dbt l
            WHERE t_date = p_CalcDate;

        TimeStamp_ (
           'Перенос расчета по деньгам в архив ('||sql%rowcount||' зап.)',
           p_CalcDate,
           ts_,
           SYSTIMESTAMP);
    else
        TimeStamp_ (
           'НЕТ расчитанных лимитов по деньгам за дату для архива',
           p_CalcDate,
           ts_,
           SYSTIMESTAMP);
    end if;
     commit;
   END;

     PROCEDURE SaveArchFuture (p_CalcDate IN DATE)
   AS
   pragma autonomous_transaction;
    chk_date integer ;
   BEGIN
      ts_ := SYSTIMESTAMP;
      select /*+ full(l) */ count(*) into chk_date  from DDL_LIMITFUTURMARK_DBT l
                WHERE t_date = p_CalcDate and rownum < 2;
      if chk_date > 0  then
        LockRecordsFrom('DDL_LIMITFUTURMARKARCH_DBT','t_date = '||to_chardatesql(p_CalcDate) );
        DELETE FROM DDL_LIMITFUTURMARKARCH_DBT
              WHERE t_date = p_CalcDate;
        commit;
        INSERT /*+ parallel(8) enable_parallel_dml */ INTO DDL_LIMITFUTURMARKARCH_DBT a
           SELECT /*+  parallel(l 8) full(l) */
                  T_ID,
                  T_DATE,
                  T_TIME,
                  T_CLIENT,
                  T_INTERNALACCOUNT,
                  T_CLASS_CODE,
                  T_ACCOUNT,
                  T_VOLUMEMN,
                  T_VOLUMEPL,
                  T_KFL,
                  T_KGO,
                  T_USE_KGO,
                  T_FIRM_ID,
                  T_SECCODE,
                  T_MONEY306,
                  T_DUE474,
                  T_SUMGO,
                  T_COMPREVIOUS,
                  T_ISBLOCKED,
                  T_MARKET_KIND,
                  T_MARKET
             FROM DDL_LIMITFUTURMARK_DBT l
            WHERE t_date = p_CalcDate;

        TimeStamp_ (
           'Перенос расчета по срочному рынку в архив ('||sql%rowcount||' зап.)',
           p_CalcDate,
           ts_,
           SYSTIMESTAMP);
    else
        TimeStamp_ (
           'НЕТ расчитанных лимитов по срочному рынку за дату для архива',
           p_CalcDate,
           ts_,
           SYSTIMESTAMP);
    end if;
     commit;
   END;

   PROCEDURE InitError
   AS
   BEGIN
      LastErrorMessage := '';
   END;

   PROCEDURE SetError (ErrNum IN INTEGER, ErrMes IN VARCHAR2 DEFAULT NULL)
   AS
   BEGIN
      IF (ErrMes IS NULL)
      THEN
         LastErrorMessage := '';
      ELSE
         LastErrorMessage := ErrMes;
      END IF;

      RAISE_APPLICATION_ERROR (ErrNum, '');
   END;

   PROCEDURE GetLastErrorMessage (ErrMes OUT VARCHAR2)
   AS
   BEGIN
      ErrMes := LastErrorMessage;
   END;

  function Get_whereContrTable (p_calc_sid in varchar2 
                                ,p_MarketID IN NUMBER
                                ,p_MarketCode IN VARCHAR2
                                ,p_CalcDate IN DATE
                                , p_ByStock IN NUMBER
                                , p_ByCurr IN NUMBER
                                , p_ByEDP IN NUMBER
                                , p_byDeriv IN NUMBER
                                , p_UseListClients IN NUMBER) return varchar2
    as
  l_sql_where_contr varchar2(2000):='';
  l_sql_where varchar2(2000):='';
  l_in_ServKind boolean := false;
  begin
    l_sql_where_contr:='';
    l_sql_where:='';
    IF (p_ByStock = 0) OR  (p_ByCurr = 0) OR (p_ByEDP = 0) OR (p_byDeriv = 0) THEN
       IF (p_ByStock = 1) THEN
           l_sql_where_contr := l_sql_where_contr || case when l_in_ServKind then ',' end || '1';
           l_in_ServKind := true;
       END IF;
       IF (p_ByCurr = 1) THEN
           l_sql_where_contr := l_sql_where_contr || case when l_in_ServKind then  ',' end ||'21';
           l_in_ServKind := true;
       END IF;
        IF (p_byDeriv = 1) THEN
           l_sql_where_contr := l_sql_where_contr || case when l_in_ServKind then  ',' end ||'15';
           l_in_ServKind := true;
        END IF;
       if l_in_ServKind then
           l_sql_where_contr := '( c.t_MarketID = '||p_MarketID||' and c.t_isEDP = chr(0) and c.t_ServKind in ( '||l_sql_where_contr ||'))' ;
       end if;
       IF (p_ByEDP = 1) THEN
           l_sql_where_contr := case when l_in_ServKind then ' ( ' end ||l_sql_where_contr || case when l_in_ServKind then ' or ' end || ' (c.t_MarketID = '||p_MarketID||' and c.t_IsEDP = CHR(88)) '||case when l_in_ServKind then ' ) ' end ;
           l_in_ServKind := true;
        END IF;
    END IF;
    IF  (p_UseListClients = 1) THEN
        l_sql_where := l_sql_where_contr|| case when l_in_ServKind then ' AND ' else 'c.t_MarketID = '||p_MarketID||' and ' end ||' c.t_dlcontrid in  (select t_dlcontrid from ddl_panelcontr_dbt where t_calc_sid = '''||g_calc_panelcontr||'''  and t_setflag = chr(88)) ';
    else
        l_sql_where := l_sql_where_contr|| case when not l_in_ServKind then ' c.t_MarketID = '||p_MarketID||' ' end ;
    END IF;
    l_sql_where :=' c.t_calc_sid = '''||g_calc_clientinfo||''' and '||l_sql_where;
     return l_sql_where ; 
  end;



   -- получить DL_LIMITADJUST из записи, переданной из системы в виде RAW

   PROCEDURE RSI_GetLimitAdjFromRAW (RecLimitAdj IN RAW, rLimitAdj IN OUT DDL_LIMITADJUST_DBT%ROWTYPE)
   AS
   BEGIN
      InitError ();
      rsb_struct.readStruct ('DDL_LIMITADJUST_DBT');

      rLimitAdj.t_ID := rsb_struct.getlong ('T_ID', RecLimitAdj);
      rLimitAdj.T_LIMITID := rsb_struct.getlong ('T_LIMITID', RecLimitAdj);
      -- rLimitAdj.T_LIMITKIND := rsb_struct.getInt ('T_LIMITKIND', RecLimitAdj);
      rLimitAdj.T_DATE := rsb_struct.getdate ('T_DATE', RecLimitAdj);
      rLimitAdj.T_TIME := rsb_struct.getdate ('T_TIME', RecLimitAdj);
      rLimitAdj.T_MARKET := rsb_struct.getInt ('T_MARKET', RecLimitAdj);
      rLimitAdj.T_CLIENT := rsb_struct.getlong ('T_CLIENT', RecLimitAdj);
      rLimitAdj.T_INTERNALACCOUNT := rsb_struct.getlong ('T_INTERNALACCOUNT', RecLimitAdj);
      rLimitAdj.T_LIMIT_TYPE := rsb_struct.getString ('T_LIMIT_TYPE', RecLimitAdj);
      rLimitAdj.T_FIRM_ID := rsb_struct.getString ('T_FIRM_ID', RecLimitAdj);
      rLimitAdj.T_CLIENT_CODE := rsb_struct.getString ('T_CLIENT_CODE', RecLimitAdj);
      rLimitAdj.T_OPEN_BALANCE := rsb_struct.getmoney ('T_OPEN_BALANCE', RecLimitAdj);
      rLimitAdj.T_OPEN_LIMIT := rsb_struct.getmoney ('T_OPEN_LIMIT', RecLimitAdj);
      rLimitAdj.T_CURRENT_LIMIT := rsb_struct.getmoney ('T_CURRENT_LIMIT', RecLimitAdj);
      rLimitAdj.T_LIMIT_OPERATION := rsb_struct.getString ('T_LIMIT_OPERATION', RecLimitAdj);
      rLimitAdj.T_TRDACCID := rsb_struct.getString ('T_TRDACCID', RecLimitAdj);
      rLimitAdj.T_SECCODE := rsb_struct.getString ('T_SECCODE', RecLimitAdj);
      rLimitAdj.T_TAG := rsb_struct.getString ('T_TAG', RecLimitAdj);
      rLimitAdj.T_CURRID := rsb_struct.getlong ('T_CURRID', RecLimitAdj);
      rLimitAdj.T_CURR_CODE := rsb_struct.getString ('T_CURR_CODE', RecLimitAdj);
      rLimitAdj.T_LIMIT_KIND := rsb_struct.getInt ('T_LIMIT_KIND', RecLimitAdj);
      rLimitAdj.T_LEVERAGE := rsb_struct.getmoney ('T_LEVERAGE', RecLimitAdj);
      rLimitAdj.T_ID_OPER := rsb_struct.getlong ('T_ID_OPER', RecLimitAdj);
      rLimitAdj.T_ID_STEP := rsb_struct.getInt ('T_ID_STEP', RecLimitAdj);
      rLimitAdj.T_ISBLOCKED := rsb_struct.getchar ('T_ISBLOCKED', RecLimitAdj);
      rLimitAdj.T_CURRENT_BALANCE := rsb_struct.getmoney ('T_CURRENT_BALANCE', RecLimitAdj);
   END;                                              -- RSI_GetLimitAdjFromRAW

   PROCEDURE RSI_InsDfltIntoWRTBC (
      p_LimitAdj IN OUT DDL_LIMITADJUST_DBT%ROWTYPE)
   IS
   BEGIN
      p_LimitAdj.t_ID := NVL (p_LimitAdj.t_ID, 0);
      p_LimitAdj.T_LIMITID := NVL (p_LimitAdj.T_LIMITID, 0);
      -- p_LimitAdj.T_LIMITKIND := NVL (p_LimitAdj.T_LIMITKIND, 0);
      p_LimitAdj.T_DATE := NVL (p_LimitAdj.T_DATE, UnknownDate);
      p_LimitAdj.T_TIME := NVL (p_LimitAdj.T_TIME, UnknownTime);
      p_LimitAdj.T_MARKET := NVL (p_LimitAdj.T_MARKET, -1);
      p_LimitAdj.T_CLIENT := NVL (p_LimitAdj.T_CLIENT, 0);
      p_LimitAdj.T_INTERNALACCOUNT := NVL (p_LimitAdj.T_INTERNALACCOUNT, 0);
      p_LimitAdj.T_LIMIT_TYPE := NVL (p_LimitAdj.T_LIMIT_TYPE, CHR (1));
      p_LimitAdj.T_FIRM_ID := NVL (p_LimitAdj.T_FIRM_ID, CHR (1));
      p_LimitAdj.T_CLIENT_CODE := NVL (p_LimitAdj.T_CLIENT_CODE, CHR (1));
      p_LimitAdj.T_OPEN_BALANCE := NVL (p_LimitAdj.T_OPEN_BALANCE, 0);
      p_LimitAdj.T_OPEN_LIMIT := NVL (p_LimitAdj.T_OPEN_LIMIT, 0);
      p_LimitAdj.T_CURRENT_LIMIT := NVL (p_LimitAdj.T_CURRENT_LIMIT, 0);
      p_LimitAdj.T_LIMIT_OPERATION :=
         NVL (p_LimitAdj.T_LIMIT_OPERATION, CHR (1));
      p_LimitAdj.T_TRDACCID := NVL (p_LimitAdj.T_TRDACCID, CHR (1));
      p_LimitAdj.T_SECCODE := NVL (p_LimitAdj.T_SECCODE, CHR (1));
      p_LimitAdj.T_TAG := NVL (p_LimitAdj.T_TAG, CHR (1));
      p_LimitAdj.T_CURRID := NVL (p_LimitAdj.T_CURRID, -1);
      p_LimitAdj.T_CURR_CODE := NVL (p_LimitAdj.T_CURR_CODE, CHR (1));
      p_LimitAdj.T_LIMIT_KIND := NVL (p_LimitAdj.T_LIMIT_KIND, 0);
      p_LimitAdj.T_LEVERAGE := NVL (p_LimitAdj.T_LEVERAGE, 0);
      p_LimitAdj.T_ID_OPER := NVL (p_LimitAdj.T_ID_OPER, 0);
      p_LimitAdj.T_ID_STEP := NVL (p_LimitAdj.T_ID_STEP, 0);
      p_LimitAdj.T_ISBLOCKED := NVL (p_LimitAdj.T_ISBLOCKED, CHR (0));
      p_LimitAdj.T_CURRENT_BALANCE := NVL (p_LimitAdj.T_CURRENT_BALANCE, 0);
   END;

   PROCEDURE RSI_CreateLimitAdJust (RecLimitAdj IN RAW, ID_Operation IN NUMBER, ID_Step IN NUMBER)
   AS
      rLimitAdj   DDL_LIMITADJUST_DBT%ROWTYPE;
   BEGIN
      InitError ();

      RSI_GetLimitAdjFromRAW (RecLimitAdj, rLimitAdj);
      rLimitAdj.t_ID := 0;
      rLimitAdj.T_ID_OPER := ID_Operation;
      rLimitAdj.T_ID_STEP := ID_Step;

      RSI_InsDfltIntoWRTBC (rLimitAdj);

      INSERT INTO DDL_LIMITADJUST_DBT
           VALUES rLimitAdj;
   END;                                               -- RSI_CreateLimitAdJust

   PROCEDURE RSI_CreateLimitAdJust (RecLimitAdj IN OUT DDL_LIMITADJUST_DBT%ROWTYPE, ID_Operation IN NUMBER, ID_Step IN NUMBER)
   AS
   BEGIN
      InitError ();

      RecLimitAdj.t_ID := 0;
      RecLimitAdj.T_ID_OPER := ID_Operation;
      RecLimitAdj.T_ID_STEP := ID_Step;

      RSI_InsDfltIntoWRTBC (RecLimitAdj);

      INSERT INTO DDL_LIMITADJUST_DBT
           VALUES RecLimitAdj;
   END;                                               -- RSI_CreateLimitAdJust


   PROCEDURE RSI_RestoreLimitAdJust (ID_Operation IN NUMBER, ID_Step IN NUMBER)
   AS
   BEGIN
      LockRecordsFrom('DDL_LIMITADJUST_DBT','T_ID_OPER = '||ID_Operation||' AND T_ID_STEP = '||ID_Step );
      DELETE FROM DDL_LIMITADJUST_DBT limad
            WHERE limad.T_ID_OPER = ID_Operation
                  AND limad.T_ID_STEP = ID_Step;
   END;                                              -- RSI_RestoreLimitAdJust

      FUNCTION RSI_GetLastDateCalc(p_MarketKind IN NUMBER, p_MarketID IN NUMBER) RETURN DATE
   IS
     v_sid_f constant ddl_limitcashstock_dbt.t_market_kind%type := 'фондовый';
     v_sid_edp constant ddl_limitcashstock_dbt.t_market_kind%type := 'ЕДП';
     v_sid_v constant ddl_limitcashstock_dbt.t_market_kind%type := 'валютный';
     v_MaxCalcDate_fc DATE;
     v_MaxCalcDate_edp DATE;
     v_MaxCalcDate_vc DATE;
     v_MaxCalcDate_fs DATE;
     v_MaxCalcDate DATE;
--    v_MarketCode VARCHAR2(35) := CHR(1);
--    vMICEX_CODE VARCHAR2(2000):=chr(1); -- Пакетная переменная
   BEGIN
--     vMICEX_CODE := RSB_Common.GetRegStrValue('SECUR\MICEX_CODE');
--     vSPBEX_CODE := RSB_Common.GetRegStrValue('SECUR\SPBEX_CODE');
--      v_MarketCode := nvl(p_Market,chr(1));
      IF p_MarketKind in (MARKET_KIND_STOCK,MARKET_KIND_EDP,MARKET_KIND_CURR) then
         SELECT /*+ parallel(4) RESULT_CACHE */
               NVL(MAX (case when t_market = p_MarketID 
                                and LOWER(t_market_kind) = v_sid_f
                            then t_Date end), TO_DATE('01.01.0001','DD.MM.YYYY'))
              ,NVL(MAX (case when t_market_kind = v_sid_edp
                            then t_Date end), TO_DATE('01.01.0001','DD.MM.YYYY'))
              ,NVL(MAX (case when t_market = p_MarketID 
                                and LOWER(t_market_kind) = v_sid_v 
                            then t_Date end), TO_DATE('01.01.0001','DD.MM.YYYY'))
            INTO v_MaxCalcDate_fc,v_MaxCalcDate_edp,v_MaxCalcDate_vc
            FROM ddl_limitcashstock_dbt ;
      END IF;

      IF p_MarketKind = MARKET_KIND_STOCK THEN
           
           SELECT /*+ RESULT_CACHE */ 
               NVL(MAX (t_Date), TO_DATE('01.01.0001','DD.MM.YYYY'))
             into v_MaxCalcDate_fs 
             FROM ddl_limitsecurites_dbt 
             WHERE t_market_kind = v_sid_f AND t_market = p_MarketID;

         v_MaxCalcDate := greatest(v_MaxCalcDate_fc,v_MaxCalcDate_fs) ;

       /*SELECT NVL(MAX (t_Date), TO_DATE('01.01.0001','DD.MM.YYYY')) INTO v_MaxCalcDate
          FROM (SELECT cash.t_Date
                  FROM ddl_limitcashstock_dbt cash
                 WHERE LOWER (cash.t_market_kind) = 'фондовый' AND cash.t_market = p_MarketID
                UNION ALL
                SELECT securites.t_Date
                  FROM ddl_limitsecurites_dbt securites
                 WHERE LOWER (securites.t_market_kind) = 'фондовый' AND securites.t_market = p_MarketID\* and securites.t_moneyconsolidated <> chr(88)*\);
      */
      ELSIF p_MarketKind = MARKET_KIND_EDP THEN
       v_MaxCalcDate := v_MaxCalcDate_edp ;
        /*SELECT NVL(MAX (t_Date), TO_DATE('01.01.0001','DD.MM.YYYY')) INTO v_MaxCalcDate
          FROM (SELECT cash.t_Date
                  FROM ddl_limitcashstock_dbt cash
                 WHERE  (cash.t_market_kind) = 'ЕДП'
               \* UNION ALL
                SELECT securites.t_Date
                  FROM ddl_limitsecurites_dbt securites
                 WHERE  securites.t_moneyconsolidated = chr(88)*\);
      */
      ELSIF p_MarketKind = MARKET_KIND_CURR THEN
        v_MaxCalcDate := v_MaxCalcDate_vc ;
          /*  SELECT NVL(MAX (t_Date), TO_DATE('01.01.0001','DD.MM.YYYY'))  INTO v_MaxCalcDate
              FROM (SELECT cash.t_Date
                      FROM ddl_limitcashstock_dbt cash
                     WHERE LOWER (cash.t_market_kind) = 'валютный' AND cash.t_market = p_MarketID );
                    \*UNION ALL
                    SELECT securites.t_Date
                      FROM ddl_limitsecurites_dbt securites
                     WHERE LOWER (securites.t_market_kind) = 'валютный' AND securites.t_market = v_MarketCode)*\
      */
       ELSIF p_MarketKind = MARKET_KIND_DERIV AND p_MarketID = GetMicexID() THEN
                SELECT /*+ RESULT_CACHE */ 
                  NVL(MAX (t_Date), TO_DATE('01.01.0001','DD.MM.YYYY')) INTO v_MaxCalcDate
                  FROM ddl_limitfuturmark_dbt
                  WHERE t_market = vMICEX_CODE;
       END IF;

      RETURN v_MaxCalcDate;
      EXCEPTION
       WHEN NO_DATA_FOUND THEN RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
   END; -- RSI_GetLastDateCalc

   FUNCTION RSI_CheckCalcExists(p_MarketKind IN NUMBER, p_MarketID IN NUMBER, p_CheckDate IN DATE) RETURN NUMBER
   IS
    v_CalcExists NUMBER := 0;
--    v_MarketCode VARCHAR2(35) := CHR(1);
    vMICEX_CODE VARCHAR2(2000):=chr(1);
   BEGIN
     vMICEX_CODE := RSB_Common.GetRegStrValue('SECUR\MICEX_CODE');
--     vSPBEX_CODE := RSB_Common.GetRegStrValue('SECUR\SPBEX_CODE');
--      v_MarketCode := nvl(p_Market,chr(1));
      IF p_MarketKind = MARKET_KIND_STOCK THEN
        SELECT 1 INTO v_CalcExists
          FROM dual WHERE exists (SELECT /*+ index(cash DDL_LIMITCASHSTOCK_DBT_IDX4)*/ 1
                  FROM ddl_limitcashstock_dbt cash
                 WHERE cash.t_market_kind = 'фондовый' AND cash.t_market = p_MarketID AND cash.t_Date = p_CheckDate
                UNION ALL
                SELECT 1
                  FROM ddl_limitsecurites_dbt securites
                 WHERE securites.t_market_kind = 'фондовый' AND securites.t_market = p_MarketID  AND securites.t_Date = p_CheckDate/* and securites.t_moneyconsolidated <> chr(88)*/);
      ELSIF p_MarketKind = MARKET_KIND_EDP THEN
         SELECT 1 INTO v_CalcExists
          FROM dual WHERE exists(SELECT /*+ index(cash DDL_LIMITCASHSTOCK_DBT_IDX4)*/ 1
                  FROM ddl_limitcashstock_dbt cash
                 WHERE  cash.t_market_kind = 'ЕДП'  AND cash.t_Date = p_CheckDate
               /* UNION ALL
                SELECT securites.t_Date
                  FROM ddl_limitsecurites_dbt securites
                 WHERE  securites.t_moneyconsolidated = chr(88)*/);
      ELSE
          IF p_MarketKind = MARKET_KIND_CURR THEN
            SELECT 1 INTO v_CalcExists
              FROM dual WHERE exists(SELECT /*+ index(cash DDL_LIMITCASHSTOCK_DBT_IDX4)*/ 1
                      FROM ddl_limitcashstock_dbt cash
                     WHERE cash.t_market_kind = 'валютный' AND cash.t_market = p_MarketID  AND cash.t_Date = p_CheckDate);
                    /*UNION ALL
                    SELECT securites.t_Date
                      FROM ddl_limitsecurites_dbt securites
                     WHERE LOWER (securites.t_market_kind) = 'валютный' AND securites.t_market = v_MarketCode)*/
          ELSE
            IF p_MarketKind = MARKET_KIND_DERIV AND p_MarketID = GetMicexID() THEN
                SELECT 1  INTO v_CalcExists
                  FROM dual WHERE exists (SELECT 1 FROM ddl_limitfuturmark_dbt
                  WHERE t_market = vMICEX_CODE  AND t_Date = p_CheckDate);
            END IF;
          END IF;
      END IF;

      RETURN v_CalcExists;
      EXCEPTION
       WHEN NO_DATA_FOUND THEN RETURN 0;
   END; -- RSI_GetLastDateCalc

   FUNCTION UseNotExecRQbyDeal (p_CalcDate IN DATE, p_DealID IN NUMBER)
      RETURN NUMBER
   IS
      v_sign   NUMBER := 0;
      v_Note   VARCHAR2 (2);
   BEGIN
      v_Note :=
         SUBSTR (rsb_struct.getString (rsi_rsb_kernel.
                                        GetNote (RSB_SECUR.OBJTYPE_SECDEAL,
                                                 LPAD (p_DealID, 34, '0'),
                                                 35 /*Код расчетов по сделке*/
                                                   ,
                                                 p_CalcDate
                                                )),
                 1,
                 2
                );

      IF v_Note IS NOT NULL
      THEN
         IF SUBSTR (v_Note, 1, 2) IN
               ('Y1', 'Y2', 'Y3', 'Y4', 'Y5', 'Y6', 'Y7', 'Y8', 'Y9')
         THEN
            v_sign := 1;
         END IF;
      END IF;


      RETURN v_sign;
   END;                                                  -- UseNotExecRQbyDeal

   function GetSumPlanPeriodCom(p_Client          IN NUMBER,
                             p_ClientContrID   IN NUMBER,
                             p_ServKindSub     IN NUMBER,
                             p_StartDate        IN DATE,
                             p_CheckDate       IN DATE,
                             p_FIID            IN NUMBER,
                             p_MarketID IN NUMBER
                            ) 
       return number deterministic
   is 
     pragma udf ;
     v_Sum            NUMBER := 0;  
   begin
     select nvl(sum(c.t_sum),0) into v_Sum from DDL_LIMITCOM_DBT c
     where c.t_calc_sid = g_calc_clientinfo and c.t_marketid = p_MarketID and c.t_sfcontrid = p_ClientContrID and c.t_fiid = p_FIID
          and c.t_plandate between p_StartDate and p_CheckDate ;
        
      return v_Sum ;
   end;


   FUNCTION GetSumPlanCashRQ (p_Client          IN NUMBER,
                              p_ClientContrID   IN NUMBER,
                              p_ServKindSub     IN NUMBER,
                              p_CalcDate        IN DATE,
                              p_CheckDate       IN DATE,
                              p_AccountID       IN NUMBER,
                              p_ToFI            IN NUMBER,
                              p_IsReq           IN NUMBER,
                              p_MarketID IN NUMBER
                             )
      RETURN NUMBER deterministic
   IS
      pragma udf;
      v_Sum            NUMBER := 0;
      v_Sum2           NUMBER := 0;
      v_ClientRqKind   NUMBER;
      v_ContrRqKind    NUMBER;
   BEGIN
      IF p_IsReq <> 0
      THEN
         v_ClientRqKind := RSI_DLRQ.DLRQ_KIND_REQUEST;
         v_ContrRqKind := RSI_DLRQ.DLRQ_KIND_COMMIT;
      ELSE
         v_ClientRqKind := RSI_DLRQ.DLRQ_KIND_COMMIT;
         v_ContrRqKind := RSI_DLRQ.DLRQ_KIND_REQUEST;
      END IF;

      if p_ServKindSub = 8 then

          SELECT /*+ ordered cardinality(tk 100) index(tk dlimit_dltick_idx1)
                    use_nl(rq) index (rq DDLRQ_DBT_IDX1)*/
           NVL (SUM (RSI_RSB_FIInstr.ConvSum (rq.t_Amount,
                                              rq.t_FIID,
                                              p_ToFI,
                                              p_CalcDate,
                                              1
                                              )),0)
            INTO v_Sum
            FROM dlimit_dltick_dbt tk, ddlrq_dbt rq                       --ddl_tick_dbt tk
           WHERE tk.t_calc_sid = g_calc_clientinfo and tk.t_ClientID = p_Client
                 AND tk.t_ClientContrID = p_ClientContrID
                 AND ( (p_CalcDate <> p_CheckDate) /*bpv т.о. на категории по сделке будум обращать внимание только по лимиту T0*/
                      OR (rq.t_PlanDate <> p_CalcDate) /*и только по исплнениям в Т0*/
                      OR ( (NVL (tk.t_tradechr, '0') != 'P') /*Режимы адресных торгов и РПС всегда начинаются с P - по ним исполнение в Т0 не учитываем, спецрепы здесь же*/
                            AND (tk.t_specrepo != CHR (88)) /*спецрепо*/
                            AND INSTR(RegVal_RepoWithCCPTradingModes, ','||NVL(tk.t_trademode, '0')||',') = 0)
                      /* 20/05/2019 Иногда Репо РПС в систему приходит как две сделки и переговорная сделка не с ЦК рассчитывается в квике, поэтому ее в исполнение в лимитах не учитываем*/
                      OR (tk.t_dealtype NOT IN (2122, 2127, 12122, 12127)
                          AND tk.t_trademode NOT IN ('PSOB', 'PSEQ'))
                      OR (SELECT /*+ index(c DDL_CLIENTINFO_DBT_IDX0) */ DISTINCT t_NotExcludeRepo
                            FROM DDL_CLIENTINFO_DBT c
                           WHERE t_calc_sid = g_calc_clientinfo and t_sfcontrid = tk.t_clientcontrid) = CHR (88))
                 AND tk.t_bofficekind <> RSB_SECUR.DL_RETIREMENT/*117*/ --bpv отражения клиентских погашений по счетам БУ в софр нет, поэтому их не берем
                 AND tk.t_DealDate <= p_CalcDate - 1 /*p_CheckDate нельзя учитывать еще не заключенные сделки*/
                 -- AND (p_ServKindSub = 9 /*Внебиржевой рынок*/ OR UseNotExecRQbyDeal(p_CalcDate, tk.t_DealID) != 0)
                 --AND p_ServKindSub = 8
                 AND tk.t_MarketID = p_MarketID
                 AND rq.t_DocKind = tk.t_BOfficeKind
                 AND rq.t_DocID = tk.t_DealID
                 AND rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY/*0*/
                 AND rq.t_PlanDate <= p_CheckDate
                 AND RQ.T_FIID = p_ToFI
                 AND ( (rq.t_Kind = v_ClientRqKind AND tk.t_ClientID = p_Client)
                      OR (rq.t_Kind = v_ContrRqKind AND tk.t_PartyID = p_Client))
                 AND ( RQ.T_SOURCEOBJKIND != RSB_SECUR.DL_SECURITYCOM
                      or decode((SELECT /*+ index(comm DDLCOMIS_DBT_IDX0)*/ count(1)         -- NOT EXISTS (SELECT /*+ index(comm DDLCOMIS_DBT_IDX0)*/ 1
                                  FROM ddlcomis_dbt comm                                 --             FROM ddlcomis_dbt comm
                                  WHERE COMM.T_ID = RQ.T_SOURCEOBJID                  --        WHERE     COMM.T_ID = RQ.T_SOURCEOBJID
                                       AND COMM.T_DOCKIND = TK.t_BOfficeKind             --             AND COMM.T_DOCKIND = TK.t_BOfficeKind
                                       AND COMM.T_ISBANKEXPENSES = CHR (88)              --           AND COMM.T_ISBANKEXPENSES = CHR (88)
                                      -- AND RQ.T_SOURCEOBJKIND = RSB_SECUR.DL_SECURITYCOM --                   AND RQ.T_SOURCEOBJKIND = RSB_SECUR.DL_SECURITYCOM)
                                       AND ROWNUM < 2),0,0,1) = 0)    
                 
                 AND (  rq.t_State != RSI_DLRQ.DLRQ_STATE_EXEC
                      OR ( rq.t_PlanDate <= p_CheckDate AND rq.t_State = 2 AND rq.t_Type = 6
                           AND (SELECT TT.T_DEALSTATUS  FROM ddl_tick_dbt tt WHERE tt.t_dealid = tk.t_dealid) = 0)
                      OR EXISTS (SELECT 1
                                  FROM ddlgrdeal_dbt gr
                                  WHERE  gr.t_DocKind = rq.t_DocKind
                                     AND gr.t_DocID = rq.t_DocID
                                     AND gr.t_PlanDate <= p_CheckDate
                                     AND ( (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_COMISS)
                                            AND tk.t_ClientID = p_Client
                                            AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYCOM/*9*/))
                                        OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_COMISS)
                                              AND tk.t_IsPartyClient = 'X'
                                              AND tk.t_PartyID = p_Client
                                              AND gr.t_TemplNum IN (RSI_DLGR. DLGR_TEMPL_PAYCOMCONTR/*10*/))
                                        OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_AVANCE, RSI_DLRQ.DLRQ_TYPE_DEPOSIT)
                                              AND rq.t_DealPart = 1
                                              AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYAVANCE/*13*/))
                                        OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMENT)
                                              AND rq.t_DealPart = 1
                                              AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYMENT/*15*/))
                                        OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_AVANCE)
                                              AND rq.t_DealPart = 2
                                              AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYAVANCE2))
                                        OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMENT, RSI_DLRQ.DLRQ_TYPE_INCREPO)
                                              AND rq.t_DealPart = 2
                                              AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYMENT2,RSI_DLGR.DLGR_TEMPL_PAYPC))
                                        OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_COMPPAYM)
                                              AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_COMPPAYMENT))
                                        OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMREPOCOUP)
                                              AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_COUP))
                                        OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMREPOPART)
                                              AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PARTREP)))
                                     AND EXISTS (SELECT 1
                                               FROM ddlgracc_dbt gracc
                                              WHERE gracc.t_GrDealID = gr.t_ID
                                                    AND gracc.t_AccNum = RSI_DLGR.DLGR_ACCKIND_ACCOUNTING
                                                    AND (gracc.t_State = RSI_DLGR.DLGRACC_STATE_PLAN
                                                         OR (gracc.t_State = RSI_DLGR.DLGRACC_STATE_FACTEXEC
                                                             AND gracc.t_FactDate >= p_CalcDate) /*bpv график проверяем на дату расчета*/
                                                         )
                                                 )
                                   ));
      end if;
      RETURN v_Sum + v_Sum2;
   END;                                                    -- GetSumPlanCashRQ



   FUNCTION GetSumPlanCashCM (p_Client          IN NUMBER,
                              p_ClientContrID   IN NUMBER,
                              p_CalcDate        IN DATE,
                              p_CheckDate       IN DATE,
                              p_Account         IN VARCHAR2,
                              p_FIID            IN NUMBER,
                              p_IsReq           IN NUMBER
                             )
      RETURN NUMBER deterministic
   IS
      pragma udf;
      v_Sum_plus    NUMBER := 0;    
      v_Sum_minus   NUMBER := 0;    
   begin
      select /*+ index(cm DLIMIT_CMTICK_IDX1)*/ sum(cm.t_plan_plus),sum(cm.t_plan_minus) into v_Sum_plus, v_Sum_minus 
       from dlimit_cmtick_dbt cm 
       where cm.t_calc_sid =  g_calc_clientinfo 
       and cm.t_clientcontrid = p_ClientContrID
       and cm.t_fiid = p_FIID
       and( (cm.t_sumtype = 0 and ( (cm.t_isfactpaym = chr(0) and cm.t_plan_date <= p_CheckDate)
                                 or (cm.t_plan_date >= p_CalcDate and cm.t_plan_date <= p_CheckDate)))
          or(cm.t_sumtype = 1 and cm.t_plan_date <= p_CheckDate)) ;    
     if p_IsReq <> 0 then 
       return nvl(v_Sum_plus,0) ;
     else
       return nvl(v_Sum_minus,0) ;
     end if;
   end;
      /*FUNCTION GetSumPlanCashPM (p_Client          IN NUMBER,
                              p_ClientContrID   IN NUMBER,
                              p_CalcDate        IN DATE,
                              p_CheckDate       IN DATE,
                              p_Account         IN VARCHAR2,
                              p_FIID            IN NUMBER,
                              p_IsReq           IN NUMBER
                             )
      RETURN NUMBER deterministic
   IS
      pragma udf;
      v_Sum    NUMBER := 0;
      v_Sum2   NUMBER := 0;
      v_Sum3   NUMBER := 0;
   BEGIN
     if p_IsReq <> 0 then 
      SELECT \*+ ordered *\ NVL (SUM (pm.t_Amount), 0)
        INTO v_Sum
        FROM dpmpaym_dbt pm,
             ddvndeal_dbt ndeal,
             ddvnfi_dbt nfi,
             dfininstr_dbt bafi
       WHERE     ndeal.t_Client = p_Client
             AND ndeal.t_ClientContr = p_ClientContrID
             AND ndeal.t_DocKind IN (4813, 199) -- Конверсионная сделка ФИСС и КО \*bpv и плюс СВОПы*\
             AND ndeal.t_Date < p_CalcDate
             AND ndeal.t_Sector = CHR (88)
             AND ndeal.t_MarketKind IN (2) -- валютный , возможно нужен еще 5 - все(единый пул обеспечения)
           --  AND ndeal.t_state > 0                            -- не отложенная
             AND nfi.t_dealID = ndeal.t_ID
             AND nfi.t_Type = 0
             AND bafi.t_FIID = nfi.t_FIID
             AND bafi.t_fi_kind = 1                                  -- валюта
             AND pm.t_DocKind = ndeal.t_DocKind
             AND pm.t_DocumentID = ndeal.t_ID
             \*AND (CASE
                     WHEN p_IsReq <> 0 THEN pm.t_ReceiverAccount
                     ELSE pm.t_PayerAccount
                  END) = p_Account     *\           -- p_IsReq == 1 - требование
             and pm.t_ReceiverAccount =  p_Account  
             AND pm.t_PayFIID = p_FIID
--             AND pm.t_valueDate <= p_CheckDate
--             AND pm.t_valueDate >= p_CalcDate -- AND pm.t_State = 1000 -- уточнить насчет статусов платежей
             AND ( (pm.t_isfactpaym = chr(0) and pm.t_valueDate <= p_CheckDate ) or
                   (pm.t_valueDate >= p_CalcDate  and pm.t_valueDate <= p_CheckDate));
      else
      SELECT \*+ ordered *\ NVL (SUM (pm.t_Amount), 0)
        INTO v_Sum
        FROM dpmpaym_dbt pm,
             ddvndeal_dbt ndeal,
             ddvnfi_dbt nfi,
             dfininstr_dbt bafi
       WHERE     ndeal.t_Client = p_Client
             AND ndeal.t_ClientContr = p_ClientContrID
             AND ndeal.t_DocKind IN (4813, 199) -- Конверсионная сделка ФИСС и КО \*bpv и плюс СВОПы*\
             AND ndeal.t_Date < p_CalcDate
             AND ndeal.t_Sector = CHR (88)
             AND ndeal.t_MarketKind IN (2) -- валютный , возможно нужен еще 5 - все(единый пул обеспечения)
           --  AND ndeal.t_state > 0                            -- не отложенная
             AND nfi.t_dealID = ndeal.t_ID
             AND nfi.t_Type = 0
             AND bafi.t_FIID = nfi.t_FIID
             AND bafi.t_fi_kind = 1                                  -- валюта
             AND pm.t_DocKind = ndeal.t_DocKind
             AND pm.t_DocumentID = ndeal.t_ID
             AND pm.t_PayerAccount = p_Account                
             AND pm.t_PayFIID = p_FIID
             AND ( (pm.t_isfactpaym = chr(0) and pm.t_valueDate <= p_CheckDate ) or
                 (pm.t_valueDate >= p_CalcDate  and pm.t_valueDate <= p_CheckDate));
      end if;
      \*в обязательства плюсуем еще не оплаченную комиссюю*\
      IF p_IsReq = 0
      THEN            
             -- Один вместо 2
             select NVL(sum(NVL(q1.t_sum, 0) - NVL((select sum(PM.T_AMOUNT)
                                        from dpmpaym_dbt pm
                                       where pm.t_DocKind = q1.t_DocKind
                                         and PM.T_FIID = q1.T_FIID_COMM
                                         and pm.t_DocumentID = q1.T_DOCID
                                         and pm.t_purpose = q1.ppurpose
                                         and PM.T_PAYER = p_Client)
                                     ,0))
                        ,0)
                into v_Sum2
                from (select comm.T_DOCKIND
                            ,comm.t_docid
                            ,sfc.T_FIID_COMM
                            ,preceiverid.ppurpose
                            ,sum(comm.t_sum) as t_sum
                        from ddlcomis_dbt comm
                            ,ddvndeal_dbt ndeal
                            ,ddvnfi_dbt nfi
                            ,dfininstr_dbt bafi
                            ,dsfcomiss_dbt sfc
                            ,(select OW.T_PARTYID
                                   ,40 ppurpose
                               from DPARTYOWN_DBT ow
                              where OW.T_PARTYKIND = 3
                             union --!!! distinct 
                             select d.t_PartyID
                                   ,72
                               from ddp_dep_dbt d) preceiverid
                       where ndeal.t_Client = p_Client
                         and ndeal.t_ClientContr = p_ClientContrID
                         and ndeal.t_DocKind in (4813, 199) -- Конверсионная сделка ФИСС и КО \*bpv и плюс СВОПы*\
                         and ndeal.t_Date < p_CalcDate
                         and ndeal.t_Sector = CHR(88)
                         and ndeal.t_MarketKind in (2)
                         and nfi.t_dealID = ndeal.t_ID
                         and nfi.t_Type = 0
                         and bafi.t_FIID = nfi.t_FIID
                         and bafi.t_fi_kind = 1 -- валюта
                         and COMM.T_DOCID = ndeal.t_id
                         and COMM.T_DOCKIND in (4813, 199)
                         and sfc.t_number = comm.t_comnumber
                         and sfc.T_FEETYPE = comm.T_FEETYPE
                         and preceiverid.t_partyid = sfc.t_receiverid
                         and comm.T_ISBANKEXPENSES <> CHR(88)
                         and nfi.t_paydate <= p_CheckDate
                         and (select sfc.t_fiid_comm
                                from dsfcomiss_dbt sfc
                               where sfc.t_number = comm.t_comnumber
                                 and sfc.t_servicekind <> 1) = p_FIID
                       group by comm.T_DOCKIND
                               ,comm.t_docid
                               ,sfc.T_FIID_COMM
                               ,preceiverid.ppurpose) q1 ;
                             

            \*SELECT NVL(SUM (
                      NVL (q1.t_sum, 0)
                      - NVL (
                           (SELECT SUM (PM.T_AMOUNT)
                              FROM dpmpaym_dbt pm
                             WHERE     pm.t_DocKind = q1.t_DocKind
                                   AND PM.T_FIID = q1.T_FIID_COMM
                                   AND pm.t_DocumentID = q1.T_DOCID
                                   AND pm.t_purpose IN (40)
                                   AND PM.T_PAYER = p_Client),
                           0)),0)
                      INTO v_Sum2
              FROM (  SELECT comm.T_DOCKIND,
                             comm.t_docid,
                             sfc.T_FIID_COMM,
                             SUM (comm.t_sum) AS t_sum
                        FROM ddlcomis_dbt comm,
                             ddvndeal_dbt ndeal,
                             ddvnfi_dbt nfi,
                             dfininstr_dbt bafi,
                             dsfcomiss_dbt sfc
                       WHERE     ndeal.t_Client = p_Client
                             AND ndeal.t_ClientContr = p_ClientContrID
                             AND ndeal.t_DocKind IN (4813, 199) -- Конверсионная сделка ФИСС и КО \*bpv и плюс СВОПы*\
                             AND ndeal.t_Date < p_CalcDate
                             AND ndeal.t_Sector = CHR (88)
                             AND ndeal.t_MarketKind IN (2)
                             AND nfi.t_dealID = ndeal.t_ID
                             AND nfi.t_Type = 0
                             AND bafi.t_FIID = nfi.t_FIID
                             AND bafi.t_fi_kind = 1 -- валюта
                             AND COMM.T_DOCID = ndeal.t_id
                             AND COMM.T_DOCKIND IN (4813, 199)
                             AND sfc.t_number = comm.t_comnumber
                             AND sfc.T_FEETYPE = comm.T_FEETYPE
                             AND EXISTS
                                    (SELECT 1
                                       FROM DPARTYOWN_DBT ow
                                      WHERE OW.T_PARTYID = sfc.t_receiverid
                                            AND OW.T_PARTYKIND = 3)
                             AND comm.T_ISBANKEXPENSES <> CHR (88)
                             AND nfi.t_paydate <= p_CheckDate
                             AND (SELECT sfc.t_fiid_comm
                                    FROM dsfcomiss_dbt sfc
                                   WHERE sfc.t_number = comm.t_comnumber
                                         AND sfc.t_servicekind <> 1) = p_FIID
                    GROUP BY comm.T_DOCKIND, comm.t_docid, sfc.T_FIID_COMM) q1;

            SELECT NVL(SUM (
                      NVL (q1.t_sum, 0)
                      - NVL (
                           (SELECT SUM (PM.T_AMOUNT)
                              FROM dpmpaym_dbt pm
                             WHERE     pm.t_DocKind = q1.t_DocKind
                                   AND PM.T_FIID = q1.T_FIID_COMM
                                   AND pm.t_DocumentID = q1.T_DOCID
                                   AND pm.t_purpose IN (72)
                                   AND PM.T_PAYER = p_Client),
                           0)),0)
                      INTO v_Sum3
              FROM (  SELECT comm.T_DOCKIND,
                             comm.t_docid,
                             sfc.T_FIID_COMM,
                             SUM (comm.t_sum) AS t_sum
                        FROM ddlcomis_dbt comm,
                             ddvndeal_dbt ndeal,
                             ddvnfi_dbt nfi,
                             dfininstr_dbt bafi,
                             dsfcomiss_dbt sfc
                       WHERE     ndeal.t_Client = p_Client
                             AND ndeal.t_ClientContr = p_ClientContrID
                             AND ndeal.t_DocKind IN (4813, 199) -- Конверсионная сделка ФИСС и КО \*bpv и плюс СВОПы*\
                             AND ndeal.t_Date < p_CalcDate
                             AND ndeal.t_Sector = CHR (88)
                             AND ndeal.t_MarketKind IN (2)
                             AND nfi.t_dealID = ndeal.t_ID
                             AND nfi.t_Type = 0
                             AND bafi.t_FIID = nfi.t_FIID
                             AND bafi.t_fi_kind = 1 -- валюта
                             AND COMM.T_DOCID = ndeal.t_id
                             AND COMM.T_DOCKIND IN (4813, 199)
                             AND sfc.t_number = comm.t_comnumber
                             AND sfc.T_FEETYPE = comm.T_FEETYPE
                             AND sfc.t_receiverid IN
                                    (SELECT d.t_PartyID
                                       FROM ddp_dep_dbt d)
                             AND comm.T_ISBANKEXPENSES <> CHR (88)
                             AND nfi.t_paydate <= p_CheckDate
                             AND (SELECT sfc.t_fiid_comm
                                    FROM dsfcomiss_dbt sfc
                                   WHERE sfc.t_number = comm.t_comnumber
                                         AND sfc.t_servicekind <> 1) = p_FIID
                    GROUP BY comm.T_DOCKIND, comm.t_docid, sfc.T_FIID_COMM) q1;*\
      END IF;

      RETURN v_Sum + v_Sum2 + v_Sum3;
   END;                                                    -- GetSumPlanCashPM
*/
   FUNCTION GetSumPlanAvrRQ (p_Client          IN NUMBER,
                             p_ClientContrID   IN NUMBER,
                             p_ServKindSub     IN NUMBER,
                             p_CalcDate        IN DATE,
                             p_CheckDate       IN DATE,
                             p_FIID            IN NUMBER,
                             p_IsReq           IN NUMBER,
                             p_MarketID IN NUMBER
                            )
      RETURN NUMBER deterministic
   IS
      pragma udf;
      v_SumPt          NUMBER := 0;
      v_SumCl          NUMBER := 0;
      v_ClientRqKind   NUMBER;
      v_ContrRqKind    NUMBER;
   BEGIN
      IF p_IsReq <> 0
      THEN
         v_ClientRqKind := RSI_DLRQ.DLRQ_KIND_REQUEST;
         v_ContrRqKind := RSI_DLRQ.DLRQ_KIND_COMMIT;
      ELSE
         v_ClientRqKind := RSI_DLRQ.DLRQ_KIND_COMMIT;
         v_ContrRqKind := RSI_DLRQ.DLRQ_KIND_REQUEST;
      END IF;


      WITH q
              AS (SELECT *
                    FROM dlimit_dltick_dbt tk
                   WHERE tk.t_calc_sid = g_calc_clientinfo and  tk.t_ClientID = p_Client
                         AND tk.t_ClientContrID = p_ClientContrID
                         AND tk.t_MarketID = p_MarketID )
      SELECT NVL (SUM (rq.t_Amount), 0)
        INTO v_SumCl
        FROM ddlrq_dbt rq, q
       WHERE rq.t_DocKind = q.t_BOfficeKind AND rq.t_DocID = q.t_DealID
             AND rq.t_Type IN
                    (RSI_DLRQ.DLRQ_TYPE_DELIVERY,
                     RSI_DLRQ.DLRQ_TYPE_COMPDELIVERY)
             AND rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
             AND rq.t_FIID = p_FIID
             AND rq.t_state <> -1 --bpv техничский статус чтобы отрубить старые неисполненные ТО
             AND rq.t_Kind = v_ClientRqKind
             AND rq.t_PlanDate <= p_CheckDate
                         AND ( (p_CalcDate <> p_CheckDate) /*bpv т.о. на категории по сделке будум обращать внимание только по лимиту T0*/
                              OR (p_CalcDate <> rq.t_plandate)
                              OR (NVL (q.t_tradechr, '0') != 'P' /*Режимы адресных торгов и РПС всегда начинаются с P - по ним исполнение в Т0 не учитываем, спецрепы здесь же*/
                                  AND (q.t_specrepo != CHR (88)) /*спецрепо*/
                                  AND INSTR(RegVal_RepoWithCCPTradingModes, ','||NVL(q.t_trademode, '0')||',') = 0)
                                  --OR tk.t_dealtype NOT IN (2122, 2127, 12122, 12127)
                                  /* 20/05/2019 Иногда Репо РПС в систему приходит как две сделки и переговорная сделка не с ЦК рассчитывается в квике, поэтому ее в исполнение в лимитах не учитываем*/
                              OR (q.t_dealtype NOT IN
                                         (2122, 2127, 12122, 12127)
                                  AND q.t_trademode NOT IN ('PSOB', 'PSEQ')
                              OR (SELECT /*+ index(c DDL_CLIENTINFO_DBT_IDX0) */ DISTINCT t_NotExcludeRepo
                                    FROM DDL_CLIENTINFO_DBT
                                       WHERE t_calc_sid = g_calc_clientinfo and t_sfcontrid = q.t_clientcontrid) =
                                        CHR (88)))
             AND (rq.t_FactDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY') --OR rq.t_FactDate >= p_CheckDate
                  OR rq.t_FactDate >= p_CalcDate
                  OR NOT EXISTS
                            (SELECT /*+ index(lot DPMWRTSUM_DBT_IDX1 )*/ 1
                               FROM dpmwrtsum_dbt lot
                              WHERE     lot.t_DocKind in ( 29, 135 )
                                    AND lot.t_DocID = rq.t_ID
                                    AND lot.t_Party = p_Client
                                    AND lot.t_State = 1
                                    AND lot.t_Contract = p_ClientContrID));

      RETURN v_SumPt + v_SumCl;
   END;                                                     -- GetSumPlanAvrRQ



   FUNCTION GetSumComPrevious (p_Client          IN NUMBER,
                               p_ClientContrID   IN NUMBER,
                               p_AccountID       IN NUMBER,
                               p_ToFI            IN NUMBER,
                               p_CalcDate        IN DATE,
                               p_IsDue474        IN NUMBER,
                               p_MarketID IN NUMBER
                              )
      RETURN NUMBER
   IS
      v_Sum   NUMBER := 0;
   BEGIN
      IF p_IsDue474 = 1
      THEN
         SELECT ABS (rsb_account.restac (acc.t_Account,
                                         acc.t_Currency,
                                         p_CalcDate - 1,
                                         acc.t_Chapter,
                                         NULL
                                        ))
           INTO v_Sum
           FROM dmcaccdoc_dbt acc
          WHERE     t_clientcontrid = p_clientcontrid
                AND t_currency = p_tofi
                AND t_iscommon = CHR (88)
                AND t_catnum = 5087                                  /*47423*/
                                   ;
      ELSE
         WITH tk
                 AS (SELECT tick.t_DealDate,
                            tick.t_BOfficeKind,
                            tick.t_DealID,
                            tick.t_dealtype
                       FROM ddl_tick_dbt tick
                      WHERE TICK.T_MARKETID = p_MarketID AND tick.t_DealDate =
                               RSI_RSBCALENDAR.GETDATEAFTERWORKDAY (p_CalcDate, -1, GetCalendarIDForLimit(p_MarketID, -1,-1, tick.t_pfi)) --Сделки, заключенные в предыдущий день
                            AND ( (p_IsDue474 <> 1)
                                 OR tick.t_dealtype NOT IN
                                       (12144, 12154, 12122, 12127)) --костыль. Нужно исключить сделки, по которым комиссия начисляется и списывается в один день ПЕРЕДЕЛАТЬ!
                            AND ( ( (NVL (
                                        (SELECT t_numinlist
                                           FROM DOBJATTR_DBT
                                          WHERE     t_objecttype = 101
                                                AND t_groupid = 106
                                                AND t_attrid = RSB_SECUR.
                                                                GetMainObjAttr (
                                                                  101,
                                                                  LPAD (tick.T_DEALID, 34, '0'),
                                                                  106,
                                                                  p_CalcDate)),
                                        '0') NOT LIKE
                                        ('P%') /*Режимы адресных торгов и РПС всегда начинаются с P - по ним исполнение в Т0 не учитываем, спецрепы здесь же*/
                                     AND INSTR(RegVal_RepoWithCCPTradingModes,
                                               ','||NVL((SELECT t_numinlist
                                                           FROM DOBJATTR_DBT
                                                          WHERE t_objecttype = 101
                                                            AND t_groupid = 106
                                                            AND t_attrid = RSB_SECUR.GetMainObjAttr(101, LPAD(tick.T_DEALID, 34, '0'), 106, p_CalcDate)),'0')||','
                                              ) = 0)
                                   /*('PSRP',
                                   'PSOB',
                                   'PTEQ',
                                   'PTOB',
                                   'PSRD',
                                   'PSGC')*/
                                   -- OR tick.t_dealtype NOT IN (2122, 2127, 12122, 12127)
                                   /* 20/05/2019 Иногда Репо РПС в систему приходит как две сделки и переговорная сделка не с ЦК рассчитывается в квике, поэтому ее в исполнение в лимитах не учитываем*/
                                   OR (tick.t_dealtype NOT IN
                                          (2122, 2127, 12122, 12127)
                                       AND NVL (
                                              (SELECT t_numinlist
                                                 FROM DOBJATTR_DBT
                                                WHERE t_objecttype = 101
                                                      AND t_groupid = 106
                                                      AND t_attrid =
                                                             RSB_SECUR.
                                                              GetMainObjAttr (
                                                                101,
                                                                LPAD (tick.T_DEALID, 34, '0'),
                                                                106,
                                                                p_CalcDate)),
                                              '0') NOT IN
                                              ('PSOB', 'PSEQ'))))
                            AND ( (tick.t_ClientID = p_Client
                                   AND tick.t_ClientContrID = p_ClientContrID)
                                 OR (tick.t_IsPartyClient = 'X'
                                     AND tick.t_PartyID = p_Client
                                     AND tick.t_PartyContrID =
                                            p_ClientContrID)))
         SELECT NVL (SUM (q.t_CommSum), 0)
           INTO v_Sum
           FROM (SELECT NVL (SUM (RSI_RSB_FIInstr.ConvSum (dlcm.t_Sum,
                                                           cm.t_FIID_Comm,
                                                           p_ToFI,
                                                           p_CalcDate,
                                                           1
                                                          )),
                             0
                            )
                           AS t_CommSum
                   FROM ddlcomis_dbt dlcm, dsfcomiss_dbt cm, tk
                  WHERE     dlcm.t_Contract = p_ClientContrID
                        AND dlcm.t_DocKind = tk.t_BOfficeKind
                        AND dlcm.t_DocID = tk.t_DealID
                        AND cm.t_FeeType = dlcm.t_FeeType
                        AND cm.t_Number = dlcm.t_ComNumber
                        AND cm.t_FIID_Comm = p_ToFI
                 -- AND cm.t_ReceiverID IN (SELECT d.t_PartyID from ddp_dep_dbt d) --bpv непонятно почему берется тоьлко та, где получатель банк. На остаток же влияют все комиссии
                 UNION
                 SELECT NVL (SUM (RSI_RSB_FIInstr.ConvSum (basobj.t_CommSum,
                                                           cm.t_FIID_Comm,
                                                           p_ToFI,
                                                           p_CalcDate,
                                                           1
                                                          )),
                             0
                            )
                           AS t_CommSum
                   FROM dsfbasobj_dbt basobj,
                        dsfdefcom_dbt defcom,
                        dsfcomiss_dbt cm,
                        tk
                  WHERE     basobj.t_BaseObjectType = tk.t_BOfficeKind
                        AND basobj.t_BaseObjectID = tk.t_DealID
                        AND defcom.t_ID = basobj.t_DefCommID
                        AND cm.t_FeeType = defcom.t_Feetype
                        AND cm.t_Number = defcom.t_CommNumber
                        AND cm.t_FIID_Comm = p_ToFI -- AND cm.t_ReceiverID IN (SELECT d.t_PartyID from ddp_dep_dbt d) --bpv непонятно почему берется тоьлко та, где получатель банк. На остаток же влияют все комиссии
                                                   ) q;
      END IF;

      RETURN v_Sum;
   END;                                                   -- GetSumComPrevious

   FUNCTION GetSumComPreviousForRevise (p_Client          IN NUMBER,
                                        p_ClientContrID   IN NUMBER,
                                        p_FI              IN NUMBER,
                                        p_CalcDate        IN DATE,
                                        p_MarketID IN NUMBER
                                       )
      RETURN NUMBER
   IS
      v_Sum   NUMBER(32,12) := 0;
      v_PrevDate DATE := RSI_RSBCALENDAR.GETDATEAFTERWORKDAY (p_CalcDate, -1, GetCalendarIDForLimit(p_MarketID, -1, p_FI)); --Сделки, заключенные в предыдущий день
   BEGIN
      WITH dl
              AS (SELECT tick.t_DealDate AS t_dealdate,
                         tick.t_BOfficeKind AS t_dockind,
                         tick.t_DealID AS t_dealid
                    FROM ddl_tick_dbt tick
                   WHERE tick.t_DealDate = v_PrevDate
                         AND ( (tick.t_ClientID = p_Client
                                AND tick.t_ClientContrID = p_ClientContrID)
                              OR (    tick.t_IsPartyClient = 'X'
                                  AND tick.t_PartyID = p_Client
                                  AND tick.t_PartyContrID = p_ClientContrID))
                  UNION
                  SELECT dv.T_DATE AS t_dealdate,
                         192 AS t_dockind,
                         dv.t_id AS t_dealid
                    FROM ddvdeal_dbt dv
                   WHERE     T_CLIENT = p_Client
                         AND T_CLIENTCONTR = p_ClientContrID
                         AND dv.T_DATE = v_PrevDate
                  UNION
                  SELECT dvn.T_DATE AS t_dealdate,
                         dvn.T_DOCKIND AS t_dockind,
                         dvn.t_id AS t_dealid
                    FROM ddvndeal_dbt dvn
                   WHERE     T_CLIENT = p_Client
                         AND T_CLIENTCONTR = p_ClientContrID
                         AND dvn.t_date = v_PrevDate)
      SELECT NVL (SUM (q.t_CommSum), 0)
        INTO v_Sum
        FROM (SELECT NVL (SUM (dlcm.t_Sum), 0) AS t_CommSum
                FROM ddlcomis_dbt dlcm, dsfcomiss_dbt cm, dl
               WHERE     dlcm.t_Contract = p_ClientContrID
                     AND cm.t_FeeType = dlcm.t_FeeType
                     AND cm.t_Number = dlcm.t_ComNumber
                     AND cm.t_FIID_Comm = p_FI
                     AND dlcm.t_Contract = p_ClientContrID
                     AND dlcm.t_DocKind = dl.t_dockind
                     AND dlcm.t_DocID = dl.t_DealID
              UNION
              SELECT NVL (SUM (basobj.t_CommSum), 0) AS t_CommSum
                FROM dsfbasobj_dbt basobj,
                     dsfdefcom_dbt defcom,
                     dsfcomiss_dbt cm,
                     dl
               WHERE     basobj.t_BaseObjectType = dl.t_dockind
                     AND basobj.t_BaseObjectID = dl.t_DealID
                     AND defcom.t_ID = basobj.t_DefCommID
                     AND cm.t_FeeType = defcom.t_Feetype
                     AND cm.t_Number = defcom.t_CommNumber
                     AND cm.t_FIID_Comm = p_FI
              UNION
              SELECT                                                   /*+ ORDERED*/
                    NVL (SUM (COM.T_SUM), 0) AS t_CommSum
                FROM ddvfi_com_dbt COM, dsfcomiss_dbt sfcom
               WHERE     COM.T_IsTrust != 'X'
                     AND COM.T_CLIENTCONTR = p_ClientContrID
                     AND COM.T_DATE = v_PrevDate
                     AND sfcom.t_ComissID = COM.t_ComissID
                     AND sfcom.t_FIID_COMM = p_FI
                     AND sfcom.t_ReceiverID IN (SELECT d.t_PartyID
                                                  FROM ddp_dep_dbt d)) q;

      RETURN v_Sum;
   END;

   FUNCTION GetSumGuarantyPrevious (p_Client          IN NUMBER,
                                    p_ClientContrID   IN NUMBER,
                                    p_Department      IN NUMBER,
                                    p_CalcDate        IN DATE,
                                    p_PrevWorkDate    IN DATE,
                                    p_ToFI            IN NUMBER
                                   )
      RETURN NUMBER
   IS
      v_Sum   NUMBER := 0;
   BEGIN
      SELECT NVL (SUM (RSI_RSB_FIInstr.ConvSum (TURN.t_Guaranty,
                                                fin.t_ParentFI,
                                                p_ToFI,
                                                p_CalcDate,
                                                1
                                               )),
                  0
                 )
        INTO v_Sum
        FROM ddvfiturn_dbt TURN, dfininstr_dbt fin
       WHERE     TURN.T_IsTrust != 'X'
             AND TURN.T_DEPARTMENT = p_Department
             AND TURN.T_CLIENTCONTR = p_ClientContrID
             AND TURN.T_DATE = p_PrevWorkDate
             AND fin.t_FIID = TURN.T_FIID;

      RETURN v_Sum;
   END;                                              -- GetSumGuarantyPrevious

   FUNCTION GetSumFutureComPrevious(p_Client IN NUMBER, p_ClientContrID IN NUMBER, p_Department IN NUMBER, p_CalcDate IN DATE, p_PrevWorkDate IN DATE, p_AccCode_Currency IN NUMBER, p_ToFI IN NUMBER) RETURN NUMBER
   IS
      v_Sum   NUMBER := 0;
   BEGIN

     SELECT /*+ ORDERED*/ NVL(SUM(RSI_RSB_FIInstr.ConvSum(COM.T_SUM, sfcom.t_FIID_COMM, p_ToFI, p_CalcDate, 1)),0) INTO v_Sum
        FROM ddvfi_com_dbt COM, dsfcomiss_dbt sfcom
       WHERE     COM.T_IsTrust != 'X'
             AND COM.T_DEPARTMENT = p_Department
             AND COM.T_CLIENTCONTR = p_ClientContrID
             AND COM.T_DATE = p_PrevWorkDate -- комиссиия всегда формируется за дату заключения сделки
             AND sfcom.t_ComissID = COM.t_ComissID
        AND sfcom.t_FIID_COMM   = p_AccCode_Currency
        AND sfcom.t_ReceiverID  IN (SELECT d.t_PartyID from ddp_dep_dbt d);

      RETURN v_Sum;

   END;                                             -- GetSumFutureComPrevious

   FUNCTION GetSumCorr(p_Client IN NUMBER, p_FirmID IN VARCHAR2, p_Department IN NUMBER, p_CalcDate IN DATE, p_PrevWorkDate IN DATE, p_ToFI IN NUMBER) RETURN NUMBER
   IS
     v_Sum          NUMBER := 0;
     v_SumMargIN    NUMBER := 0;
     v_SumMargOUT   NUMBER := 0;
     v_SumRecBonus  NUMBER := 0;
     v_SumPayBonus  NUMBER := 0;
     v_SumComm      NUMBER := 0;
   BEGIN

    SELECT NVL (SUM (TURN.T_MARGIN), 0)
      INTO v_SumMargIN
      FROM ddvfiturn_dbt TURN,
           dfininstr_dbt fin,
           (SELECT MP.T_SFCONTRID
              FROM ddlcontrmp_dbt mp, dsfcontr_dbt sfcontr
             WHERE     mp.t_FirmID = p_FirmID
                   AND SFCONTR.T_ID = MP.T_SFCONTRID
                   AND sfcontr.t_ServKind = 15                 --Срочные контракты
                   AND (   sfcontr.t_DateClose =
                              TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                        OR sfcontr.t_DateClose >= p_CalcDate)
                   AND sfcontr.t_ServKindSub = 8
                   AND SFCONTR.T_PARTYID = p_Client) contr
     WHERE     TURN.T_IsTrust != 'X'
           AND TURN.T_DEPARTMENT = p_Department
           AND TURN.T_CLIENTCONTR = contr.T_SFCONTRID
           AND TURN.T_DATE > p_PrevWorkDate
           AND TURN.T_DATE < p_CalcDate
           AND fin.t_FIID = TURN.T_FIID
           AND fin.t_ParentFI = p_ToFI
           AND TURN.T_MARGIN > 0;

    SELECT NVL (SUM (TURN.T_MARGIN), 0)
      INTO v_SumMargOUT
      FROM ddvfiturn_dbt TURN,
           dfininstr_dbt fin,
           (SELECT MP.T_SFCONTRID
              FROM ddlcontrmp_dbt mp, dsfcontr_dbt sfcontr
             WHERE     mp.t_FirmID = p_FirmID
                   AND SFCONTR.T_ID = MP.T_SFCONTRID
                   AND sfcontr.t_ServKind = 15                 --Срочные контракты
                   AND (   sfcontr.t_DateClose =
                              TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                        OR sfcontr.t_DateClose >= p_CalcDate)
                   AND sfcontr.t_ServKindSub = 8
                   AND SFCONTR.T_PARTYID = p_Client) contr
     WHERE     TURN.T_IsTrust != 'X'
           AND TURN.T_DEPARTMENT = p_Department
           AND TURN.T_CLIENTCONTR = contr.T_SFCONTRID
           AND TURN.T_DATE > p_PrevWorkDate
           AND TURN.T_DATE < p_CalcDate
           AND fin.t_FIID = TURN.T_FIID
           AND fin.t_ParentFI = p_ToFI
           AND TURN.T_MARGIN < 0;

    SELECT NVL (SUM (TURN.T_RECEIVEDBONUS), 0), NVL (SUM (TURN.T_PAIDBONUS), 0)
      INTO v_SumRecBonus, v_SumPayBonus
      FROM ddvfiturn_dbt TURN,
           dfininstr_dbt fin,
           (SELECT MP.T_SFCONTRID
              FROM ddlcontrmp_dbt mp, dsfcontr_dbt sfcontr
             WHERE     mp.t_FirmID = p_FirmID
                   AND SFCONTR.T_ID = MP.T_SFCONTRID
                   AND sfcontr.t_ServKind = 15                 --Срочные контракты
                   AND (   sfcontr.t_DateClose =
                              TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                        OR sfcontr.t_DateClose >= p_CalcDate)
                   AND sfcontr.t_ServKindSub = 8
                   AND SFCONTR.T_PARTYID = p_Client) contr
     WHERE     TURN.T_IsTrust != 'X'
           AND TURN.T_DEPARTMENT = p_Department
           AND TURN.T_CLIENTCONTR = contr.T_SFCONTRID
           AND TURN.T_DATE > p_PrevWorkDate
           AND TURN.T_DATE < p_CalcDate
           AND fin.t_FIID = TURN.T_FIID
           AND fin.t_ParentFI = p_ToFI;

    SELECT NVL (SUM (RSI_RSB_FIInstr.ConvSum (COM.T_SUM,
                                              sfcom.t_FIID_COMM,
                                              p_ToFI,
                                              p_CalcDate,
                                              1)),
                0)
      INTO v_SumComm
      FROM ddvfi_com_dbt COM,
           dsfcomiss_dbt sfcom,
           (SELECT MP.T_SFCONTRID
              FROM ddlcontrmp_dbt mp, dsfcontr_dbt sfcontr
             WHERE     mp.t_FirmID = p_FirmID
                   AND SFCONTR.T_ID = MP.T_SFCONTRID
                   AND sfcontr.t_ServKind = 15                 --Срочные контракты
                   AND (   sfcontr.t_DateClose =
                              TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                        OR sfcontr.t_DateClose >= p_CalcDate)
                   AND sfcontr.t_ServKindSub = 8
                   AND SFCONTR.T_PARTYID = p_Client) contr
     WHERE     COM.T_IsTrust != 'X'
           AND COM.T_DEPARTMENT = p_Department
           AND COM.T_CLIENTCONTR = contr.T_SFCONTRID
           AND COM.T_DATE > p_PrevWorkDate
           AND COM.T_DATE < p_CalcDate
           AND sfcom.t_ComissID = COM.t_ComissID
           AND sfcom.t_FIID_COMM = p_ToFI
           AND sfcom.t_ReceiverID IN (SELECT d.t_PartyID
                                        FROM ddp_dep_dbt d);

     v_Sum := v_SumMargIN + v_SumRecBonus - v_SumMargOUT + v_SumPayBonus - v_SumComm;

     RETURN v_Sum;

   END; -- GetSumCorr
 
   -- определение вида рынка для суб-договора
   function GetMarketKindbyServKind(p_ServKind number) return number deterministic as
   begin
     if p_ServKind = 1
     then
       return MARKET_KIND_STOCK; -- фондовый рынок
     elsif p_ServKind = 15
     then
       return MARKET_KIND_DERIV; -- срочный рынок
     elsif p_ServKind = 21
     then
       return MARKET_KIND_CURR; -- валютный рынок
     else
       return 0;
     end if;
   end;
   
   -- BOSS-771, процедура для считывания значений справочника 
   -- BOSS-6236 BOSS-5028 Доработать параллельный расчет лимитов при включенном обособлении ДС
   -- https://sdlc.go.rshbank.ru/confluence/pages/viewpage.action?pageId=369666715
   procedure findLimitPrm(p_MarketKind      in integer
                         ,p_Marketid        in integer
                         ,p_ImplKind        in integer
                         ,p_IsEdp           in integer
                         ,o_Firm            out varchar2 --tt_findLimitPrm.Firm%type
                         ,o_Tag             out varchar2 --ttt_findLimitPrm.tag%type
                         ,o_TrdAcc          out varchar2 --tt_findLimitPrm.trdacc%type
                         ,o_CodescZeroLimit out varchar2 --tt_findLimitPrm.CodescZeroLimit%type
                          ) as
     v_key varchar2(100);
   begin
     v_key := p_MarketKind || '#' || p_Marketid || '#' || p_ImplKind || '#' || p_IsEdp;
     if not gt_findLimitPrm.exists(v_key)
     then
       -- параметры по-умолчанию
       o_Firm            := 'MC0134700000';
       o_Tag             := 'EQTV';
       o_TrdAcc          := 'L01+00000F00';
       o_CodescZeroLimit := chr(1);
       for s in (select cast(r.T_FIRMCODE as varchar2(12)) as FIRM_ID
                       ,cast(r.T_POSCODE as varchar2(5)) as TAG
                       ,cast(r.T_DEPOACC as varchar2(20)) as TRDACC
                       ,cast(r.t_codesczerolimit as varchar2(12)) as CODESCZEROLIMIT
                   from DDL_LIMITPRM_DBT r
                  where r.t_marketkind = p_MarketKind
                    and r.t_marketid = p_Marketid
                    and r.t_ImplKind = p_ImplKind)
       loop
         o_Firm            := s.firm_id;
         o_Tag             := s.tag;
         o_TrdAcc          := s.trdacc;
         o_CodescZeroLimit := s.CODESCZEROLIMIT;
       end loop;
       --  Для клиентов ЕДП: EQTV
       if p_IsEdp = 1
          and (p_MarketKind != MARKET_KIND_STOCK or p_Marketid != GetMicexID)
       then
         for s in (select cast(r.T_POSCODE as varchar2(5)) as TAG
                     from DDL_LIMITPRM_DBT r
                    where r.t_marketkind = MARKET_KIND_STOCK
                      and r.t_marketid = GetMicexID
                      and r.t_ImplKind = 1)
         loop
           o_Tag := s.tag;
         end loop;
       end if;
       if p_MarketKind = MARKET_KIND_CURR
          and p_ImplKind != 1
          and not Rsb_Common.GetRegBoolValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ИСП_ОБОСОБЛ_ДС')
       then
         for s in (select cast(r.T_DEPOACC as varchar2(20)) as TRDACC
                     from DDL_LIMITPRM_DBT r
                    where r.t_marketkind = p_MarketKind
                      and r.t_marketid = p_Marketid
                      and r.t_ImplKind = 1)
         loop
           o_TrdAcc := s.trdacc;
         end loop;
       end if;
       gt_findLimitPrm(v_key).Firm := o_Firm;
       gt_findLimitPrm(v_key).tag := o_Tag;
       gt_findLimitPrm(v_key).trdacc := o_TrdAcc;
       gt_findLimitPrm(v_key).CodescZeroLimit := o_CodescZeroLimit;
     else
       o_Firm            := gt_findLimitPrm(v_key).Firm;
       o_Tag             := gt_findLimitPrm(v_key).tag;
       o_TrdAcc          := gt_findLimitPrm(v_key).trdacc;
       o_CodescZeroLimit := gt_findLimitPrm(v_key).CodescZeroLimit;
     end if;
   end;
   
   function GetFIRM_ID(p_MarketID   in number
                      ,p_MarketKind in number
                      ,p_ImplKind   in number) return varchar2 deterministic is
     v_Firm            varchar2(12); --tr_findLimitPrm.Firm%type ;
     v_Tag             varchar2(5); --tr_findLimitPrm.tag%type ;
     v_TrdAcc          varchar2(20); --tr_findLimitPrm.trdacc%type ;
     v_CodescZeroLimit varchar2(12); --tr_findLimitPrm.CodescZeroLimit%type ;
   begin
     findLimitPrm(p_MarketKind => p_MarketKind
                 ,p_Marketid => p_Marketid
                 ,p_ImplKind => p_ImplKind
                 ,p_IsEdp => 0
                 ,o_Firm => v_Firm
                 ,o_Tag => v_Tag
                 ,o_TrdAcc => v_TrdAcc
                 ,o_CodescZeroLimit => v_CodescZeroLimit);
     return v_Firm;
   exception
     when NO_DATA_FOUND then
       return chr(1);
   end; 
   
   function GetFIRM_IDbyServKind(p_MarketID in number
                                ,p_ServKind in number
                                ,p_ImplKind in number) return varchar2 deterministic as
   begin
     return GetFIRM_ID(p_MarketID => p_MarketID, p_MarketKind => GetMarketKindbyServKind(p_ServKind), p_ImplKind => p_ImplKind);
   end;
   
   function GetCodeSCZeroLimit(p_MarketKind in number
                              ,p_MarketID   in integer
                              ,p_ImplKind   in integer) return varchar2 deterministic as
     v_Firm            varchar2(12); --tr_findLimitPrm.Firm%type ;
     v_Tag             varchar2(5); --tr_findLimitPrm.tag%type ;
     v_TrdAcc          varchar2(20); --tr_findLimitPrm.trdacc%type ;
     v_CodescZeroLimit varchar2(12); --tr_findLimitPrm.CodescZeroLimit%type ;
   begin
     findLimitPrm(p_MarketKind => p_MarketKind
                 ,p_Marketid => p_Marketid
                 ,p_ImplKind => p_ImplKind
                 ,p_IsEdp => 0
                 ,o_Firm => v_Firm
                 ,o_Tag => v_Tag
                 ,o_TrdAcc => v_TrdAcc
                 ,o_CodescZeroLimit => v_CodescZeroLimit);
     return v_CodescZeroLimit;
   exception
     when NO_DATA_FOUND then
       return chr(1);
   end;
   
   function GetTAG(p_MarketID   in number
                  ,p_MarketKind in number
                  ,p_ImplKind   in number
                  ,p_IsEdp      in number) return varchar2 deterministic as
     v_Firm            varchar2(12); --tr_findLimitPrm.Firm%type ;
     v_Tag             varchar2(5); --tr_findLimitPrm.tag%type ;
     v_TrdAcc          varchar2(20); --tr_findLimitPrm.trdacc%type ;
     v_CodescZeroLimit varchar2(12); --tr_findLimitPrm.CodescZeroLimit%type ;
   begin
     findLimitPrm(p_MarketKind => p_MarketKind
                 ,p_Marketid => p_Marketid
                 ,p_ImplKind => p_ImplKind
                 ,p_IsEdp => p_IsEdp
                 ,o_Firm => v_Firm
                 ,o_Tag => v_Tag
                 ,o_TrdAcc => v_TrdAcc
                 ,o_CodescZeroLimit => v_CodescZeroLimit);
     return v_Tag;
   exception
     when NO_DATA_FOUND then
       return chr(1);
   end; 
   
   function GetTAGbyServKind(p_MarketID in number
                            ,p_ServKind in number
                            ,p_ImplKind in number
                            ,p_IsEdp    in number) return varchar2 deterministic as
   begin
     return GetTAG(p_MarketID => p_MarketID, p_MarketKind => GetMarketKindbyServKind(p_ServKind), p_ImplKind => p_ImplKind, p_IsEdp => p_IsEdp);
   end;
   
   function GetDepoAccPrm(p_MarketID   in number
                         ,p_MarketKind in number
                         ,p_ImplKind   in number) return varchar2 deterministic as
     v_Firm            varchar2(12); --tr_findLimitPrm.Firm%type ;
     v_Tag             varchar2(5); --tr_findLimitPrm.tag%type ;
     v_TrdAcc          varchar2(20); --tr_findLimitPrm.trdacc%type ;
     v_CodescZeroLimit varchar2(12); --tr_findLimitPrm.CodescZeroLimit%type ;
   begin
     findLimitPrm(p_MarketKind => p_MarketKind
                 ,p_Marketid => p_Marketid
                 ,p_ImplKind => p_ImplKind
                 ,p_IsEdp => 0
                 ,o_Firm => v_Firm
                 ,o_Tag => v_Tag
                 ,o_TrdAcc => v_TrdAcc
                 ,o_CodescZeroLimit => v_CodescZeroLimit);
     return v_TrdAcc;
   exception
     when NO_DATA_FOUND then
       return chr(1);
   end;
   
   function GetTRDACCID(p_sfcontrId  in number
                       ,p_CalcDate   in date
                       ,p_MarketID   in number
                       ,p_MarketKind in number
                       ,p_ImplKind   in number) return varchar2 deterministic as
     v_TrdAcc varchar2(20) := chr(1); --tr_findLimitPrm.trdacc%type ;
   begin
     case
       when p_MarketKind = MARKET_KIND_STOCK
            and p_MarketID = GetMicexID then
         v_TrdAcc := nvl(replace(rsb_struct.getString(rsi_rsb_kernel.GetNote(659, LPAD(p_SfcontrId, 10, '0'), 5, p_CalcDate)), chr(0)), chr(1));
       when p_MarketKind = MARKET_KIND_CURR
            and p_MarketID = GetMicexID then
         v_TrdAcc := nvl(replace(rsb_struct.getString(rsi_rsb_kernel.GetNote(659, LPAD(p_SfcontrId, 10, '0'), 8, p_CalcDate)), chr(0)), chr(1));
       when p_MarketKind = MARKET_KIND_STOCK
            and p_MarketID = GetSpbexID then
         v_TrdAcc := nvl(replace(rsb_struct.getString(rsi_rsb_kernel.GetNote(659, LPAD(p_SfcontrId, 10, '0'), 10, p_CalcDate)), chr(0)), chr(1));
       else
         null;
     end case;
     if v_TrdAcc != chr(1)
     then
       v_TrdAcc := nvl(replace(v_TrdAcc, chr(10)), chr(1));
     end if;
     if v_TrdAcc = chr(1)
     then
       v_TrdAcc := GetDepoAccPrm(p_MarketID, p_MarketKind, p_ImplKind);
     end if;
     return v_TrdAcc;
   exception
     when NO_DATA_FOUND then
       return chr(1);
   end;
   
   function GetTRDACCIDbyServKind(p_sfcontrId in number
                                 ,p_CalcDate  in date
                                 ,p_MarketID  in number
                                 ,p_ServKind  in number
                                 ,p_ImplKind  in number) return varchar2 deterministic as
   begin
     return GetTRDACCID(p_sfcontrId => p_sfcontrId
                       ,p_CalcDate => p_CalcDate
                       ,p_MarketID => p_MarketID
                       ,p_MarketKind => GetMarketKindbyServKind(p_ServKind)
                       ,p_ImplKind => p_ImplKind);
   end;

  
   PROCEDURE getFlagLimitPrm(p_MarketID IN NUMBER,p_MarketKind IN NUMBER, v_IsDepo IN OUT NUMBER, v_IsKind2 IN OUT NUMBER, v_DepoAcc IN OUT VARCHAR2, p_ImplKind IN NUMBER DEFAULT 1)
   AS
   BEGIN

     select
             DECODE(prm.T_ISDEPO, 'X', 1, 0),
             DECODE(prm.T_KINDLARGERTWO, 'X', 1, 0),
             prm.T_DEPOACC
       into v_IsDepo, v_IsKind2,v_DepoAcc
       from ddl_limitprm_dbt prm
      where PRM.T_MARKETID = p_MarketID
        and PRM.T_MARKETKIND = p_MarketKind
        and PRM.T_IMPLKIND = p_ImplKind;

   EXCEPTION
      WHEN NO_DATA_FOUND THEN
         v_IsDepo := 0;
         v_IsKind2 := 0;
      v_DepoAcc := chr(1);
   END;                                                             


/*   FUNCTION GetSumComPrevious(p_ClientContrID IN NUMBER, p_CalcDate IN DATE, p_Kind IN INTEGER, p_Currency IN NUMBER, p_MarketID IN NUMBER) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
   BEGIN
    SELECT NVL (SUM (DLCOM.T_SUM), 0)
      INTO v_Sum
      FROM ddlcomis_dbt dlcom, dsfcomiss_dbt sfcom, ddl_tick_dbt tick
     WHERE     DLCOM.T_CONTRACT = p_ClientContrID
           AND SFCOM.T_FEETYPE = dlcom.t_feetype
           AND SFCOM.T_NUMBER = DLCOM.T_COMNUMBER
           AND SFCOM.T_FIID_COMM = p_Currency
           AND TICK.T_BOFFICEKIND = DLCOM.T_DOCKIND
           AND tick.t_dealid = DLCOM.T_DOCID
           AND TICK.T_DEALSTATUS <> 0
           AND TICK.T_DEALDATE < p_CalcDate
           AND TICK.T_MARKETID = p_MarketID
           AND (   (    DLCOM.T_FACTPAYDATE = TO_DATE('01.01.0001', 'dd.mm.yyyy') AND
                        DLCOM.T_PLANPAYDATE <=
                           GetDateLimitByKind (p_Kind,
                                               SFCOM.T_FIID_COMM,
                                               0,
                                               0))
                OR (    DLCOM.T_FACTPAYDATE <=
                           GetDateLimitByKind (p_Kind,
                                               SFCOM.T_FIID_COMM,
                                               0,
                                               0)
                    AND DLCOM.T_FACTPAYDATE >= p_CalcDate));

     RETURN v_Sum;

   END; -- GetSumComPrevious

   FUNCTION GetSumComPrevious_1(p_ClientContrID IN NUMBER, p_CalcDate IN DATE, p_Currency IN NUMBER, p_MarketID IN NUMBER) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
   BEGIN
    SELECT NVL (SUM (DLCOM.T_SUM), 0)
      INTO v_Sum
      FROM ddlcomis_dbt dlcom, dsfcomiss_dbt sfcom, ddl_tick_dbt tick
     WHERE     DLCOM.T_CONTRACT = p_ClientContrID
           AND SFCOM.T_FEETYPE = dlcom.t_feetype
           AND SFCOM.T_NUMBER = DLCOM.T_COMNUMBER
           AND SFCOM.T_FIID_COMM = p_Currency
           AND TICK.T_BOFFICEKIND = DLCOM.T_DOCKIND
           AND tick.t_dealid = DLCOM.T_DOCID
           AND TICK.T_DEALSTATUS <> 0
           AND TICK.T_DEALDATE < p_CalcDate
           AND TICK.T_MARKETID = p_MarketID
           AND DLCOM.T_FACTPAYDATE = RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,-1) ;

     RETURN v_Sum;

   END; -- GetSumComPrevious_1
*/
   FUNCTION GetSumDebAndCredCash(p_Account IN VARCHAR2, p_CalcDate IN DATE, p_SubKind_Oper IN NUMBER) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
   BEGIN
    SELECT NVL (SUM (NPTXOP.T_OUTSUM), 0)
      INTO v_Sum
      FROM dnptxop_dbt nptxop
     WHERE     NPTXOP.T_STATUS = 2                                     -- Закрытая
           AND NPTXOP.T_DOCKIND = 4607 -- Операция Списания/Зачисления денежных средств
           AND NPTXOP.T_OPERDATE =
                  RSI_RsbCalendar.GetDateAfterWorkDay (p_CalcDate, -1)
           AND NPTXOP.T_ACCOUNT = p_Account
           AND NPTXOP.T_LIMITSTATUS <> 2
           AND NPTXOP.T_SUBKIND_OPERATION = p_SubKind_Oper;
     RETURN v_Sum;

   END; -- GetSumDebAndCredCash



   FUNCTION GetObjCodeOnDate (pFIID         IN NUMBER,
                              pObjectType   IN NUMBER,
                              pCodeKind     IN NUMBER,
                              pDate         IN DATE
                             )
      RETURN VARCHAR2
   IS
      vBankDate   DOBJCODE_DBT.T_BankDate%TYPE;
      vCode       DOBJCODE_DBT.T_CODE%TYPE;
   BEGIN
      BEGIN
         SELECT objcode.T_BankDate
           INTO vBankDate
           FROM (  SELECT t_BankDate
                     FROM DOBJCODE_DBT
                    WHERE     t_ObjectType = pObjectType
                          AND t_CodeKind = pCodeKind
                          AND t_ObjectID = pFIID
                          AND t_BankDate <= pDate
                 ORDER BY t_BankDate DESC) objcode
          WHERE ROWNUM = 1;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            vBankDate := TO_DATE ('01.01.0001', 'dd.mm.yyyy');
         WHEN OTHERS
         THEN
            vBankDate := TO_DATE ('01.01.0001', 'dd.mm.yyyy');
      END;

      --  IF vBankDate <> TO_DATE ('01.01.0001', 'dd.mm.yyyy')
      -- THEN
      BEGIN
         SELECT objcode.T_Code
           INTO vCode
           FROM (SELECT t_Code
                   FROM DOBJCODE_DBT
                  WHERE     t_ObjectType = pObjectType
                        AND t_CodeKind = pCodeKind
                        AND t_ObjectID = pFIID
                        AND t_BankDate = vBankDate
                        AND t_BankCloseDate = TO_DATE ('01.01.0001', 'dd.mm.yyyy')) objcode
          WHERE ROWNUM = 1;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            vCode := CHR (1);
         WHEN OTHERS
         THEN
            vCode := CHR (1);
      END;

      --END IF;

      RETURN vCode;
   END;

   FUNCTION GetCurrPrice (p_FIID IN NUMBER, p_MarketID IN NUMBER)
      RETURN NUMBER
   AS
      v_CurrPrice   NUMBER (10) := -1;
   BEGIN
      BEGIN
         SELECT t_fiid
           INTO v_CurrPrice
           FROM (  SELECT t_fiid
                     FROM dratedef_dbt t
                    WHERE     t_otherfi = p_FIID
                          AND t_type = 1
                          AND t_market_place = p_MarketID
                 ORDER BY t_market_place, t_type)
          WHERE ROWNUM = 1;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            v_CurrPrice := -1;
      END;

      IF v_CurrPrice = -1
      THEN
         BEGIN
            SELECT t_fiid
              INTO v_CurrPrice
              FROM (  SELECT t_fiid
                        FROM dratedef_dbt t
                       WHERE t_otherfi = p_FIID AND t_type = 23
                    ORDER BY t_market_place, t_type)
             WHERE ROWNUM = 1;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_CurrPrice := -1;
         END;
      END IF;

      RETURN v_CurrPrice;
   END;


   FUNCTION CalcWaPrice (p_CalcDate     IN DATE,
                         p_Client       IN NUMBER,
                         p_SfContrID    IN NUMBER,
                         p_FIID         IN NUMBER,
                         p_PriceFIID    IN NUMBER,
                         p_ClientCode   IN VARCHAR2,
                         p_SecCode      IN VARCHAR2,
                         p_IsDebug      IN NUMBER)
      RETURN NUMBER
   IS
      v_sum_prev        NUMBER := 0;
      v_sum             NUMBER := 0;
      v_sumid_t0        NUMBER := 0;
      v_legid_t0        NUMBER := 0;
      v_time_t0         DATE;
      v_t0              DATE;
      v_sum_t0          NUMBER := 0;
      v_dealcodets      VARCHAR2(50):= CHR(1);
      v_dealdate        DATE;
      v_first           NUMBER := 1;
      v_long            CHAR := CHR (0);
      v_short           CHAR := CHR (0);
      v_wa_price_prev   NUMBER := 0;
      v_position_prev   NUMBER := 0;
      v_GOdocid_prev    NUMBER := 0;
      v_wa_price        NUMBER := 0;
      Sum_tmp           NUMBER := 0;
      Amount_tmp        NUMBER := 0;
      v_numstep         NUMBER := 0;
      v_MarketID        NUMBER := -1;
      v_isBond          NUMBER := 0;
      v_FaceValueFI     NUMBER := -1;
      v_PriceFIID       NUMBER :=-1;

      CURSOR C_DEALS1 (
         p_CalcDate    IN DATE,
         p_Client      IN NUMBER,
         p_SfContrID   IN NUMBER,
         p_FIID        IN NUMBER,
         v_RegVal      IN NUMBER)
      IS
         SELECT tk.t_dealcode dealcode,
                tk.t_dealtype dealtype,
                lot.t_buy_sale,
                lot.t_amount,
                lot.t_sum,
                CASE WHEN tk.t_BOfficeKind = 127 AND tk.T_FLAG3 = chr(88) THEN (SELECT s.T_DATE FROM ddlsum_dbt s WHERE s.T_DOCKIND = tk.t_BOfficeKind AND s.T_DOCID = tk.t_dealid AND s.T_KIND = 1220) ELSE lot.t_changedate END t_changedate,
                t_sumid,
                0 t_legid,
                CASE WHEN tk.t_BOfficeKind = 127 AND tk.T_FLAG3 = chr(88) THEN (SELECT leg.T_SUPPLYTIME FROM ddl_leg_dbt leg WHERE leg.T_DEALID = tk.t_dealid AND leg.t_legkind = 0 AND leg.t_legid = 0) ELSE lot.t_time END t_time,
                t_currency,
                tk.t_dealcodets
           FROM v_scwrthistex lot, ddlrq_dbt rq, ddl_tick_dbt tk
          WHERE     lot.t_changedate < p_CalcDate
                AND lot.t_Party = p_Client
                AND lot.T_contract = p_SfContrID
                AND lot.t_fiid = p_FIID
                AND lot.t_Buy_Sale IN
                       (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY,
                        RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO,
                        RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE)
                AND lot.t_instance = 0
                AND lot.t_dockind = 29
                AND rq.t_ID = lot.t_DocID
                AND tk.t_BOfficeKind = rq.t_DocKind
                AND tk.t_DealID = rq.t_DocID
                AND 0 =
                       RSB_SECUR.
                        IsRepo (
                          rsb_secur.
                           get_OperationGroup (
                             rsb_secur.get_OperSysTypes (tk.t_DealType, tk.t_BofficeKind)))
                AND 0 = NVL (RSB_SECUR.
                              GetMainObjAttr (101,
                                              LPAD (tk.T_DEALID, 34, '0'),
                                              v_RegVal,
                                              p_CalcDate),
                             0)
         UNION ALL
         SELECT c.t_commcode dealcode,
                C.T_DOCKIND dealtype,
                lot.t_buy_sale,
                lot.t_amount,
                lot.t_sum,
                lot.t_changedate,
                t_sumid,
                0 t_legid,
                lot.t_time,
                lot.t_currency,
                chr(1) t_dealcodets
           FROM v_scwrthistex lot, ddl_comm_dbt c
          WHERE     lot.t_changedate < p_CalcDate
                AND lot.t_Party = p_Client
                AND lot.T_contract = p_SfContrID
                AND lot.t_fiid = p_FIID
                AND lot.t_Buy_Sale IN
                       (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY,
                        RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO,
                        RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE)
                AND lot.t_dockind = 135
                AND lot.t_instance = 0
                AND c.t_DocumentID = lot.t_DocID
                AND C.T_DOCKIND = lot.t_dockind
         UNION ALL
         SELECT tk.t_dealcode dealcode,
                tk.t_dealtype dealtype,
               CASE WHEN (RSB_SECUR.IsSale (rsb_secur.get_OperationGroup (rsb_secur.get_OperSysTypes (tk.t_DealType, tk.t_BofficeKind))) = 1 OR
                      RSB_SECUR.ISAVRWRTOUT (rsb_secur.get_OperationGroup (rsb_secur.get_OperSysTypes (tk.t_DealType, tk.t_BofficeKind))) = 1 )  THEN 1 ELSE 0 END
                t_buy_sale,
                leg.t_principal t_amount,
                leg.t_cost t_sum,
                leg.t_maturity t_changedate,
                0 t_sumid,
                leg.t_id t_legid,
                t_supplytime,
                t_cfi t_currency,
                tk.t_dealcodets
           FROM ddl_tick_dbt tk, ddl_leg_Dbt leg, ddlrq_dbt rq
          WHERE     leg.t_dealid = tk.t_dealid
                AND leg.t_legid = 0
                AND leg.t_legkind = 0
                AND TK.T_CLIENTCONTRID = p_sfcontrid
                AND TK.t_dealdate < p_CalcDate
                AND leg.t_pfi = p_fiid
                AND rq.t_docid = tk.t_dealid
                AND RQ.T_FIID = tk.t_pfi
                AND tk.t_bofficekind = 101
                AND (rq.t_FactDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                     OR (NOT EXISTS
                                (SELECT 1
                                   FROM dpmwrtsum_dbt lot
                                  WHERE lot.t_DocKind = 29
                                        AND lot.t_DocID = rq.t_ID)
                         AND TK.T_FLAG3 = CHR (0)))
                AND 1 <>
                       RSB_SECUR.
                        IsRepo (
                          rsb_secur.
                           get_OperationGroup (
                             rsb_secur.get_OperSysTypes (tk.t_DealType, tk.t_BofficeKind)))
                AND 1 <> NVL (RSB_SECUR.
                               GetMainObjAttr (101,
                                               LPAD (tk.T_DEALID, 34, '0'),
                                               v_RegVal,
                                               p_CalcDate),
                              0)
                AND 1 <> NVL (RSB_SECUR.
                               GetMainObjAttr (101,
                                               LPAD (tk.T_DEALID, 34, '0'),
                                               210,
                                               p_CalcDate),
                              0)              /*НДФЛ зачисление ВТБ исключаем*/
         ORDER BY t_changedate ASC, t_time, t_dealcodets, t_sumid;

      CURSOR C_DEALS2 (
         p_CalcDate    IN DATE,
         p_Client      IN NUMBER,
         p_SfContrID   IN NUMBER,
         p_FIID        IN NUMBER,
         v_RegVal      IN NUMBER,
         v_sumid_t0    IN NUMBER,
         v_dealcodets IN VARCHAR2)
      IS
         SELECT lot.t_buy_sale,
                lot.t_amount,
                lot.t_sum,
                lot.t_changedate2 t_changedate,
                lot.t_sumid,
                lot.t_docid,
                lot.t_dockind,
                lot.t_currency,
                lot.t_date,
                lot.t_parent,
                lot.t_source,
                lot.t_supplytime2 t_supplytime,
                NVL(tk.t_dealcodets, CHR(1)) dealcodets 
           FROM (SELECT l.*,
                        CASE
                           WHEN (l.t_DocKind = 29
                                 AND EXISTS
                                        (SELECT tk.*
                                           FROM ddlrq_dbt rq, ddl_tick_dbt tk
                                          WHERE     rq.t_ID = l.t_DocID
                                                AND tk.t_DealID = rq.t_docid
                                                AND tk.T_FLAG3 = CHR (88)
                                                AND tk.t_BOfficeKind = 127
                                                AND tk.T_BOFFICEKIND = rq.t_dockind))
                           THEN
                              (SELECT s.T_DATE
                                 FROM ddlrq_dbt rq, ddl_tick_dbt tk, ddlsum_dbt s
                                WHERE     rq.t_ID = l.t_DocID
                                      AND tk.T_BOFFICEKIND = rq.t_dockind
                                      AND rq.t_docid = tk.t_dealid
                                      AND s.T_DOCKIND = tk.t_BOfficeKind
                                      AND s.T_DOCID = tk.t_dealid
                                      AND s.T_KIND = 1220)
                           ELSE
                              l.t_changedate
                        END
                           t_changedate2,
                        CASE
                           WHEN (l.t_DocKind = 29
                                 AND EXISTS
                                        (SELECT tk.*
                                           FROM ddlrq_dbt rq, ddl_tick_dbt tk
                                          WHERE     rq.t_ID = l.t_DocID
                                                AND tk.t_DealID = rq.t_docid
                                                AND tk.T_FLAG3 = CHR (88)
                                                AND tk.t_BOfficeKind = 127
                                                AND tk.T_BOFFICEKIND = rq.t_dockind))
                           THEN
                              (SELECT leg.T_SUPPLYTIME
                                 FROM ddlrq_dbt rq, ddl_tick_dbt tk, ddl_leg_dbt leg
                                WHERE     rq.t_ID = l.t_DocID
                                      AND tk.T_BOFFICEKIND = rq.t_dockind
                                      AND rq.t_docid = tk.t_dealid
                                      AND leg.T_DEALID = tk.t_dealid
                                      AND leg.t_legkind = 0
                                      AND leg.t_legid = 0)
                           ELSE
                              l.t_time
                        END
                           t_supplytime2
                   FROM v_scwrthistex l) lot join ddlrq_dbt rq on  rq.t_ID = lot.t_DocID join ddl_tick_dbt tk on   tk.t_BOfficeKind = rq.t_DocKind
                         AND tk.t_DealID = rq.t_DocID
          WHERE    lot.t_changedate2 < p_CalcDate
                AND lot.t_Party = p_Client
                AND lot.T_contract = p_SfContrID
                AND lot.t_fiid = p_FIID
                AND ((TO_DATE(TO_CHAR( lot.t_changedate2,'DD.MM.YYYY')||' '||TO_CHAR(lot.t_supplytime2, 'HH24:MI:SS'), 'DD.MM.YYYY hh24:mi:ss') >
                      TO_DATE(TO_CHAR( v_t0,'DD.MM.YYYY')||' '||TO_CHAR(v_time_t0, 'HH24:MI:SS'), 'DD.MM.YYYY hh24:mi:ss'))
                      OR 
                     (TO_DATE(TO_CHAR( lot.t_changedate2,'DD.MM.YYYY')||' '||TO_CHAR(lot.t_supplytime2, 'HH24:MI:SS'), 'DD.MM.YYYY hh24:mi:ss') =
                      TO_DATE(TO_CHAR( v_t0,'DD.MM.YYYY')||' '||TO_CHAR(v_time_t0, 'HH24:MI:SS'), 'DD.MM.YYYY hh24:mi:ss')
                      AND (((CASE WHEN tk.t_dealcodets IS NOT NULL THEN tk.t_dealcodets ELSE CHR(1) END) > v_dealcodets) 
                             OR 
                           ((CASE WHEN tk.t_dealcodets IS NOT NULL THEN tk.t_dealcodets ELSE CHR(1) END) = v_dealcodets AND lot.t_sumid >= v_sumid_t0))
                      ))
                AND lot.t_instance = 0
                AND lot.t_dockind IN (29, 135)
                AND lot.t_portfolio = 0
                                       AND (   (    lot.t_DocKind = 29
                                  AND EXISTS
                                         (SELECT tk.*
                                            FROM ddlrq_dbt rq, ddl_tick_dbt tk
                                           WHERE     rq.t_ID = lot.t_DocID
                                                 AND tk.t_DealID = rq.t_docid
                                                 AND tk.T_BOFFICEKIND = rq.t_dockind
                                                 ))
                              OR (    lot.t_DocKind = 135
                                  AND EXISTS
                                         (SELECT tk.*
                                            FROM ddl_tick_dbt tk
                                           WHERE     tk.t_DealID = lot.t_dealid
                                                 AND tk.T_BOFFICEKIND in (101,127)
                                                 )))
                       AND NOT EXISTS                              -- убираем репо
                              (SELECT *
                                 FROM ddlrq_dbt rq, ddl_tick_dbt tk
                                WHERE     rq.t_ID = lot.t_DocID
                                      AND tk.t_BOfficeKind = rq.t_DocKind
                                      AND tk.t_DealID = rq.t_DocID
                                      AND 1 = RSB_SECUR.IsRepo(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind)))
                                      /*Берем только первую часть из орепо как тех.репo*/
                                      AND NOT (RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) = 1 AND rq.t_DealPart = 1
                                      AND 1 = (CASE WHEN v_RegVal != -1 THEN NVL(RSB_SECUR.GetMainObjAttr (101,LPAD (tk.T_DEALID,34,'0'),v_RegVal,p_CalcDate), 0) ELSE 
                                                    CASE WHEN lot.t_DocKind = 135 THEN 1 ELSE 0 END END)))
         UNION ALL
         SELECT
                CASE WHEN (RSB_SECUR.IsSale (rsb_secur.get_OperationGroup (rsb_secur.get_OperSysTypes (tk.t_DealType, tk.t_BofficeKind))) = 1 OR
                      RSB_SECUR.ISAVRWRTOUT (rsb_secur.get_OperationGroup (rsb_secur.get_OperSysTypes (tk.t_DealType, tk.t_BofficeKind))) = 1 )  THEN 1 ELSE 0 END
                   t_buy_sale,
                leg.t_principal t_amount,
                leg.t_cost t_sum,
                leg.t_maturity t_changedate,
                0 t_sumid,
                RQ.T_ID t_docid,
                rq.t_dockind,
                leg.t_cfi t_currency,
                rq.t_plandate,
                0 t_parent,
                0 t_source,
                leg.t_supplytime,
                tk.t_dealcodets dealcodets
           FROM ddl_tick_dbt tk, ddl_leg_Dbt leg, ddlrq_dbt rq
          WHERE     leg.t_dealid = tk.t_dealid
                AND leg.t_legid = 0
                AND leg.t_legkind = 0
                AND TK.T_CLIENTCONTRID = p_sfcontrid
                AND TK.t_dealdate < p_CalcDate
                AND leg.t_pfi = p_fiid
                AND rq.t_docid = tk.t_dealid
                AND RQ.T_FIID = tk.t_pfi
                AND tk.t_bofficekind = 101
                AND ( ( (leg.t_maturity = v_t0)
                       AND (LEG.T_SUPPLYTIME >= v_time_t0))
                     OR (leg.t_maturity > v_t0))
                     -- время поставки до клиринга может быть одинаковым и dealid не гарантирует корректного порядка. Поэтому порядок определяем по порядку биржевого кода
                     -- справедливо для позиций, которые появляются в результате еще не исполненных сделок
                     -- т.е. только в случае если Начальная сделка тоже не исполнена, т.е. по ней нет лота нет (v_sumid_t0 = 0)
                     AND CASE WHEN v_sumid_t0 = 0
                                     THEN v_dealcodets
                                      ELSE chr(0)
                             END <= tk.t_dealcodets
                --AND RQ.T_PLANDATE >= p_CAlcDate
                AND (rq.t_FactDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                     OR (NOT EXISTS
                                (SELECT 1
                                   FROM dpmwrtsum_dbt lot
                                  WHERE lot.t_DocKind = 29
                                        AND lot.t_DocID = rq.t_ID)
                         AND TK.T_FLAG3 = CHR (0)))
                AND 1 <>
                       RSB_SECUR.
                        IsRepo (
                          rsb_secur.
                           get_OperationGroup (
                             rsb_secur.get_OperSysTypes (tk.t_DealType, tk.t_BofficeKind)))
                AND 1 <> NVL (RSB_SECUR.
                               GetMainObjAttr (101,
                                               LPAD (tk.T_DEALID, 34, '0'),
                                               v_RegVal,
                                               tk.t_dealdate),
                              0)
                AND 1 <> NVL (RSB_SECUR.
                               GetMainObjAttr (101,
                                               LPAD (tk.T_DEALID, 34, '0'),
                                               210,
                                               tk.t_dealdate),
                              0)              /*НДФЛ зачисление ВТБ исключаем*/
         ORDER BY t_changedate ASC, t_supplytime, dealcodets, t_sumid;

      PROCEDURE PrintDebug (isdebug IN NUMBER, msg IN VARCHAR2)
      IS
      BEGIN
         IF (isdebug = 1)
         THEN
            DBMS_OUTPUT.put_line (msg);
         END IF;
      END;
   BEGIN

      IF (RSI_RSB_FIINSTR.FI_AVRKINDSGETROOTbyfiid (p_Fiid) =
             RSI_RSB_FIINSTR.AVOIRKIND_BOND)
      THEN
         v_isBond := 1;
      END IF;

      SELECT t_facevaluefi
        INTO v_FaceValueFI
        FROM dfininstr_dbt
       WHERE t_fiid = p_FIID;

      --1: ищем начало позиции
      PrintDebug (p_isDebug, '--1: ищем начало позиции');

      PrintDebug (p_isDebug,p_CalcDate||' '||
                           p_Client||' '||
                           p_SfContrID||' '||
                           p_FIID||' '||
                           RegVal_SpecRepo);

      FOR rec IN C_DEALS1 (p_CalcDate,
                           p_Client,
                           p_SfContrID,
                           p_FIID,
                           RegVal_SpecRepo)
      LOOP

         IF rec.t_Buy_Sale IN
               (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY,
                RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO)
         THEN
            v_sum := v_sum + rec.t_amount;
            PrintDebug (p_IsDebug, rec.dealcode || ' ' || rec.t_sumid || ' ' || rec.t_changedate || '   +' || rec.t_Amount || '  =' || v_sum || '   t_sum=' || rec.t_sum || '   t_currency=' || rec.t_currency);
         ELSE
            v_sum := v_sum - rec.t_amount;
            PrintDebug (p_IsDebug, rec.dealcode || ' ' || rec.t_sumid || ' ' || rec.t_changedate || '   -' || rec.t_Amount || '  =' || v_sum || '   t_sum=' || rec.t_sum || '   t_currency=' || rec.t_currency);
         END IF;

         IF v_first = 0
         THEN
            IF v_sum > 0 AND v_sum > v_sum_prev AND v_sum_prev <= 0
            THEN
               v_sumid_t0 := rec.t_sumid;
               v_legid_t0 := rec.t_legid;
               v_time_t0 := rec.t_time;
               v_t0 := rec.t_changedate;
               v_dealcodets := rec.t_dealcodets;
               v_sum_t0 := v_sum;
               v_long := CHR (88);
               v_short := CHR (0);
               PrintDebug (p_isDebug, 'LONG');
               PrintDebug (p_IsDebug, '+ v_sum_t0 := v_sum;' || '   ' || v_sum_t0);
               PrintDebug (p_IsDebug, '+ v_sum_prev := v_sum;' || '   ' || v_sum_prev);
               PrintDebug (p_IsDebug, '+ t_dealcodets := ' || '   ' || rec.t_dealcodets);
            ELSE
               IF v_sum < 0 AND v_sum < v_sum_prev AND v_sum_prev >= 0
               THEN
                  v_sumid_t0 := rec.t_sumid;
                  v_legid_t0 := rec.t_legid;
                  v_time_t0 := rec.t_time;
                  v_t0 := rec.t_changedate;
                  v_dealcodets := rec.t_dealcodets;
                  v_sum_t0 := v_sum;
                  v_long := CHR (0);
                  v_short := CHR (88);
                  PrintDebug (p_isDebug, 'SHORT');
                  PrintDebug (p_IsDebug, '- v_sum_t0 := v_sum;' || '   ' || v_sum_t0);
                  PrintDebug (p_IsDebug, '- v_sum_prev := v_sum;' || '   ' || v_sum_prev);
                  PrintDebug (p_IsDebug, '- t_dealcodets := ' || '   ' || rec.t_dealcodets);
               END IF;
            END IF;

            v_sum_prev := v_sum;
         END IF;

         IF v_first = 1
         THEN
            v_first := 0;

            IF v_sum > 0
            THEN
               v_long := CHR (88);
            ELSE
               v_short := CHR (88);
            END IF;

            v_sumid_t0 := rec.t_sumid;
            v_legid_t0 := rec.t_legid;
            v_time_t0 := rec.t_time;
            v_t0 := rec.t_changedate;
            v_dealcodets := rec.t_dealcodets;
            v_sum_t0 := v_sum;
            v_sum_prev := v_sum;
         END IF;
      END LOOP;

      v_first := 1;

      IF v_sum_t0 < 0
      THEN
         v_sum_t0 := v_sum_t0 * (-1);
      END IF;

      PrintDebug (p_isDebug, '"Нулевая" операция, v_dealcodets = ''' || CASE WHEN v_dealcodets = CHR(1) THEN '' ELSE v_dealcodets END || '''');
      PrintDebug (p_IsDebug, 'v_sumid_t0 := rec.t_sumid;' || '   ' || v_sumid_t0);
      PrintDebug (p_IsDebug, 'v_legid_t0 := rec.t_legid;' || '   ' || v_legid_t0);
      PrintDebug (p_IsDebug, 'v_time_t0 := rec.t_time;' || '   ' || v_time_t0);
      PrintDebug (p_IsDebug, 'v_t0 := rec.t_changedate;' || '   ' || v_t0);
      PrintDebug (p_IsDebug, 'v_sum_t0 := v_sum;' || '   ' || v_sum_t0);

      --2: определяем валюту для расчета цены
      PrintDebug (p_isDebug, '--2: определяем валюту для расчета цены');
      --вынесено в GetWAPositionPrice
      PrintDebug (p_isDebug, 'p_PriceFIID = ' || p_PriceFIID);
      v_PriceFIID := p_PriceFIID;
      --3: перебираем операции позиции и считаем цену
      PrintDebug (p_isDebug, '--3: перебираем операции позиции и считаем цену');

      FOR rec IN C_DEALS2 (p_CalcDate,
                           p_Client,
                           p_SfContrID,
                           p_FIID,
                           RegVal_SpecRepo,
                           v_sumid_t0,
                           v_dealcodets)
      LOOP

         v_numstep := v_numstep + 1;
         PrintDebug (p_IsDebug, ' -------- v_numstep    ' || v_numstep);

        IF (v_PriceFIID = -1) THEN
            IF rec.t_sumid <> 0  THEN
               BEGIN
                   SELECT l.t_cfi
                    INTO v_PriceFIID
                    FROM ddl_tick_dbt t, ddl_leg_dbt l, ddlrq_dbt r
                    WHERE l.t_dealid = t.t_dealid AND l.t_legid = 0 and l.t_legkind = 0
                    and t.t_bofficekind = r.t_dockind and t.t_dealid = r.t_docid
                    and r.t_id = CASE WHEN rec.t_dockind = 29 THEN rec.t_docid
                                       ELSE (select t_docid from dpmwrtsum_dbt where t_sumid = rec.t_source)
                                       END;
               EXCEPTION
                    WHEN no_data_found THEN
                       raise_application_error(-20001, 'Не удалось определить валюту цены приобретения');
               END;
            ELSE-- неисполненная операция
           v_PriceFIID := rec.t_Currency;
            END IF;
           PrintDebug (p_IsDebug, ' Валюта цены по сделке приобретения:    ' || v_PriceFIID);
        END IF;

         IF rec.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE
            AND v_short = 'X' and rec.t_sumid <> 0 /*если сделка неисполнена, то лота нет*/
         THEN

            BEGIN
               SELECT lot.t_Sum, LOT.T_AMOUNT
                 INTO Sum_tmp, amount_tmp
                 FROM v_scwrthistex lot
                WHERE lot.t_sumid = rec.t_parent AND lot.t_instance = 0;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  Sum_tmp := rec.t_Sum;
                  Amount_tmp := rec.t_amount;
            END;
         ELSE
            Sum_tmp := rec.t_Sum;
            Amount_tmp := rec.t_amount;
         END IF;

         IF (v_numstep = 1) THEN
            Sum_tmp := sum_tmp/amount_tmp*v_sum_t0;
            Amount_tmp := v_sum_t0;
         END IF;

         IF (rec.t_dockind = 29) THEN
            SELECT tk.t_dealdate INTO v_DealDate
                FROM ddlrq_dbt rq, ddl_tick_dbt tk
             WHERE     rq.t_ID = rec.t_DocID
                AND tk.t_DealID = rq.t_docid
                AND tk.T_BOFFICEKIND = rq.t_dockind;
         ELSE
            v_DealDate := rec.t_date;
         END IF;

         IF ( (v_long = 'X'
               AND rec.t_Buy_Sale IN
                      (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY,
                       RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO))
             OR (v_short = 'X'
                 AND rec.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE))
         THEN
            IF ( (v_isBond = 1)
                AND (RSI_RSB_FIINSTR.FI_GETNOMINALONDATE (p_Fiid, rec.t_date) =
                        0))
            THEN
               PrintDebug (p_IsDebug, 'RSI_RSB_FIINSTR.FI_GETNOMINALONDATE(p_Fiid,p_CalcDate) = 0');
               PrintDebug (p_IsDebug, p_ClientCode || '   ' || p_SecCode);
               PrintDebug (p_IsDebug, '---------------------------------------');
            END IF;



            IF rec.t_dockind = 135
            THEN
               IF v_GOdocid_prev <> rec.t_docid THEN
                   PrintDebug (p_IsDebug, 'Конвертация');
                   v_GOdocid_prev := rec.t_docid;

                   --если это первый лот, значит конвертация заменила выпуск, посчитаем цену на дату конвертации по предыдущему выпуску с коррекцией на коэфф конвертации
                   -- и сделаем это только один раз по одному из лотов операции конвертации
                   SELECT --c.t_fiid, l.t_changedate, s.t_numerator, s.t_denominator,
                         CalcWaPrice (rec.t_changedate,
                                                       l.t_party,
                                                       p_SfContrID,
                                                       c.t_fiid,
                                                       v_PriceFIID,
                                                       p_ClientCode,
                                                       p_SecCode,
                                                       p_IsDebug)
                          * s.t_denominator
                          / s.t_numerator
                             price
                     INTO v_wa_price
                     FROM ddl_comm_dbt c, dpmwrtsum_dbt l, dscdlfi_dbt s
                    WHERE     l.t_sumid = rec.t_sumid
                          AND l.t_dockind = 135
                          AND l.t_docid = c.t_documentid
                          AND c.t_dockind = l.t_dockind
                          AND s.t_dealid = c.t_documentid
                          AND s.t_dealkind = c.t_dockind;
              END IF;
            --НДФЛ зачисления миграции
            ELSIF ( (v_t0 = TO_DATE ('31122018', 'ddmmyyyy'))
                   AND (sum_tmp = 0))
            THEN
               BEGIN
                  PrintDebug (p_IsDebug, 'Зачисления миграции 31/12/2018');

                  SELECT ROUND (SUM (t_principal * price) / SUM (t_principal), 6)
                    INTO v_wa_price
                    FROM (SELECT l.t_Cost,
                                 l.t_principal, /*l.t_cost / l.t_principal / RSI_RSB_FIINSTR.FI_GETNOMINALONDATE (p_Fiid, t.t_dealdate) * 100*/
                                 CASE
                                    WHEN ( (v_isBond = 1)
                                          AND l.t_relativeprice = CHR (0))
                                    THEN
                                         RSI_RSB_FIInstr.
                                          ConvSum (l.t_price,
                                                   l.t_cfi,
                                                   v_Facevaluefi,
                                                   L.T_START,
                                                   1)
                                       / RSI_RSB_FIINSTR.FI_GETNOMINALONDATE (t.t_pfi, t.t_dealdate)
                                       * 100
                                    ELSE
                                       CASE
                                          WHEN (v_isBond = 1) THEN l.t_price
                                          ELSE RSI_RSB_FIInstr.
                                                ConvSum (l.t_price,
                                                         l.t_cfi,
                                                         v_PriceFIID,
                                                         L.T_START,
                                                         1)
                                       END
                                 END
                                    price
                            FROM v_scwrthistex v,
                                 ddlrq_dbt rq,
                                 ddl_tick_dbt t,
                                 ddl_tick_dbt t2,
                                 ddl_leg_dbt l
                           WHERE     t_sumid = v_sumid_t0
                                 AND v.t_instance = 0
                                 AND rq.t_id = v.t_docid
                                 AND rq.t_docid = t.t_dealid
                                 AND t.t_clientcontrid = t2.t_clientcontrid
                                 AND t.t_pfi = t2.t_pfi
                                 AND t2.t_flag3 = CHR (88)
                                 AND l.t_dealid = t2.t_dealid
                                 AND l.t_legid = 0
                                 AND l.t_legkind = 0);

                  IF (v_wa_price IS NULL)
                  THEN
                     PrintDebug (p_IsDebug, 'нет цены    ' || p_ClientCode || '   ' || p_seccode);
                     v_wa_price := 0;
                  END IF;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     PrintDebug (p_IsDebug, 'нет цены    ' || p_ClientCode || '   ' || p_seccode);
                     v_wa_price := 0;
               END;
            ELSIF (sum_tmp = 0)
            THEN
               --НДФЛ зачисление не миграция (ДЕПО-зачисление)
               PrintDebug (p_IsDebug, ' Зачисление не миграция (ДЕПО-зачисление)');

               BEGIN
                  SELECT CASE
                            WHEN  (v_isBond = 1)
                                  AND (l.t_relativeprice = CHR (0) OR s.t_sum <> 0) /*Облиг. в абс. выражении или цена задана в данных для НУ, там она всегда только в абс выражении. Приоритет у данных для НУ*/
                            THEN
                                 CASE WHEN s.t_sum <> 0 THEN
                                     RSI_RSB_FIInstr.ConvSum (s.t_sum,
                                                              S.T_CURRENCY,
                                                              v_Facevaluefi,
                                                              v_DealDate,
                                                              1)
                                   / RSI_RSB_FIINSTR.FI_GETNOMINALONDATE (t.t_pfi, t.t_dealdate)
                                   * 100
                                ELSE
                                     RSI_RSB_FIInstr.ConvSum (l.t_price,
                                                              l.t_cfi,
                                                              v_Facevaluefi,
                                                              v_DealDate,
                                                              1)
                                   / RSI_RSB_FIINSTR.FI_GETNOMINALONDATE (t.t_pfi, t.t_dealdate) *100
                                END
                            ELSE
                               CASE
                                  WHEN (v_isBond = 1) THEN l.t_price
                                  ELSE
                                      CASE WHEN s.t_sum <> 0 THEN
                                          RSI_RSB_FIInstr.ConvSum (s.t_sum,
                                                                S.T_CURRENCY,
                                                                v_PriceFIID,
                                                                v_DealDate,
                                                                1)
                                    ELSE
                                        RSI_RSB_FIInstr.ConvSum (l.t_price,
                                                                l.t_cfi,
                                                                v_PriceFIID,
                                                                v_DealDate,
                                                                1)
                                    END
                               END
                         END
                            price
                    INTO v_wa_price
                    FROM v_scwrthistex v,
                         ddlrq_dbt rq,
                         ddl_tick_dbt t,
                         ddl_leg_dbt l,
                         ddlsum_dbt s
                   WHERE     t_sumid = rec.t_sumid                 --v_sumid_t0
                         AND v.t_instance = 0
                         AND rq.t_id = v.t_docid
                         AND rq.t_docid = t.t_dealid
                         AND l.t_dealid = t.t_dealid
                         AND l.t_legid = 0
                         AND l.t_legkind = 0
                         AND s.t_docid = t.t_dealid
                         AND s.t_dockind = t.t_bofficekind
                         AND s.t_kind = 1220 /*PRICE*/;
               --                     DBMS_OUTPUT.put_line ('!!!' || v_wa_price);
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     PrintDebug (p_IsDebug, 'нет цены    ' || p_ClientCode || '   ' || p_seccode);
                     v_wa_price := 0;
               END;
            ----
            ELSE
               IF (rec.t_Buy_Sale = (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY))
               THEN
                  PrintDebug (p_IsDebug, 'Покупка ' || rec.dealcodets||'  '||rec.t_amount);
               ELSIF (rec.t_Buy_Sale = (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO))
               THEN
                  PrintDebug (p_IsDebug, 'Зачисление ' ||rec.dealcodets||'  '|| rec.t_amount);
               ELSIF (rec.t_Buy_Sale = (RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE))
               THEN
                  PrintDebug (p_IsDebug, 'Продажа ' || rec.dealcodets||'  '||rec.t_amount);
               END IF;

               IF (v_isBond = 1)
               THEN
                  v_wa_price :=
                       RSI_RSB_FIInstr.ConvSum (sum_tmp,
                                                rec.t_currency,
                                                v_FaceValueFI,
                                                v_DealDate,
                                                1)
                     / amount_tmp
                     / RSI_RSB_FIINSTR.FI_GETNOMINALONDATE (p_fiid, rec.t_date)
                     * 100;
               ELSE
                  v_wa_price :=
                     RSI_RSB_FIInstr.ConvSum (sum_tmp,
                                              rec.t_currency,
                                              v_PriceFIID,
                                              v_DealDate,
                                              1)
                     / amount_tmp;
               END IF;
            END IF;

            PrintDebug (p_IsDebug, ' цена сделки v_wa_price=' || v_wa_price || '    rec.t_amount=' || rec.t_amount);
            v_wa_price :=
               (v_wa_price_prev * v_position_prev + v_wa_price * amount_tmp)
               / (v_position_prev + amount_tmp);

            v_wa_price_prev := v_wa_price;
            v_position_prev := v_position_prev + amount_tmp;

            PrintDebug (p_IsDebug, 'позиция ->  price = ' || v_wa_price_prev || '   amount = ' || v_position_prev);
         ELSE
            IF ( (v_short = 'X'
                  AND rec.t_Buy_Sale IN
                         (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY,
                          RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO))
                OR (v_long = 'X'
                    AND rec.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE))
            THEN
               IF (rec.t_Buy_Sale = (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY))
               THEN
                  PrintDebug (p_IsDebug, 'Покупка ' || rec.dealcodets||'  ' || rec.t_amount);
               ELSIF (rec.t_Buy_Sale = (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO))
               THEN
                  PrintDebug (p_IsDebug, 'Зачисление ' || rec.dealcodets||'  ' || rec.t_amount);
               ELSIF (rec.t_Buy_Sale = (RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE))
               THEN
                  PrintDebug (p_IsDebug, 'Продажа ' || rec.dealcodets||'  ' || rec.t_amount);
               END IF;

               v_wa_price := v_wa_price_prev;

               v_position_prev := v_position_prev - amount_tmp;
               PrintDebug (p_IsDebug, 'позиция ->  price = ' || v_wa_price_prev || '   amount = ' || v_position_prev);
            END IF;
         END IF;

         v_first := 0;
      END LOOP;


      PrintDebug (p_IsDebug, '----Result:  price = ' || v_wa_price_prev || '   amount = ' || v_position_prev);
      RETURN ROUND (NVL (v_wa_price, 0), 6);
   EXCEPTION
      WHEN OTHERS
      THEN
         PrintDebug (p_IsDebug, 'p_clientcode=' || p_clientcode || '    p_fiid=' || p_fiid);
         PrintDebug (p_IsDebug, DBMS_UTILITY.Format_Error_Stack || ' ' || DBMS_UTILITY.Format_Error_Backtrace);

         TimeStamp_ (
               'p_clientcode='
            || p_clientcode
            || '    p_fiid='
            || p_fiid
            || ' '
            || DBMS_UTILITY.Format_Error_Stack
            || ' '
            || DBMS_UTILITY.Format_Error_Backtrace,
            p_CalcDate,
            SYSTIMESTAMP,
            SYSTIMESTAMP
           ,excepsqlcode_ => 100
           ,all_log_=> true);
         RETURN 0;
   END;                                                   -- CalcWaPrice


--p_CalcDate - дата расчета
--p_Client      - идентификатор клиента
--p_SfContrID   - идентификатор субдоговора
--p_FIID       - идентификатор бумаги
-- следующие испоьзуются только при  (RegVal_CalcWaPrice = 0 AND p_Force = 0)
--p_ClientCode -
--p_SecCode
--p_FirmID
--p_LimitKind
--p_TrdaccID
--
--p_Force - принудительный расчет, игнорирование настройки  RegVal_CalcWaPrice
--p_IsDebug    - вывод детализации в DBMS_output

   FUNCTION GetWAPositionPrice (p_CalcDate     IN DATE,
                                p_Client       IN NUMBER,
                                p_SfContrID    IN NUMBER,
                                p_FIID         IN NUMBER,
                                p_ClientCode   IN VARCHAR2,
                                p_SecCode      IN VARCHAR2,
                                p_FirmID       IN VARCHAR2,
                                p_LimitKind    IN NUMBER,
                                p_TrdaccID     IN VARCHAR2,
                                p_Force        IN NUMBER DEFAULT 0,
                                p_IsDebug      IN NUMBER DEFAULT 0)
      RETURN NUMBER
   IS
      v_wa_price    NUMBER := 0;
      v_CurrPrice   NUMBER := -1;
      v_MarketID    NUMBER := -1;
   BEGIN

      IF (RegVal_CalcWaPrice = 0 AND p_Force = 0)
      THEN
         BEGIN
            SELECT t_wa_position_price
              INTO v_wa_price
              FROM DDL_LIMITSECURITES
             WHERE     t_seccode = p_seccode
                   AND t_client_code = p_clientcode
                   AND t_limit_kind = p_LimitKind
                   AND t_TrdAccID = p_TrdAccID;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               TimeStamp_ (
                     'v_wa_price:no_data_found  seccode:'
                  || p_seccode
                  || '   clientcode:'
                  || p_ClientCode
                  || '   LimitKind:'
                  || p_LimitKind,
                  p_CalcDate,
                  SYSTIMESTAMP,
                  SYSTIMESTAMP
                 ,all_log_=> true);
               v_wa_price := 0;
         END;
      ELSE
         BEGIN
            SELECT t_marketid
              INTO v_MarketID
              FROM ddlcontrmp_dbt
             WHERE t_sfcontrid = p_sfcontrid;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_MarketID := -1;
         END;

         v_CurrPrice := GetCurrPrice (p_FIID, v_MarketID);
         v_wa_price :=
            CalcWaPrice (p_CalcDate,
                         p_Client,
                         p_SfContrID,
                         p_FIID,
                         v_CurrPrice,
                         p_ClientCode,
                         p_SecCode,
                         p_IsDebug);
      END IF;
      RETURN v_wa_price;
   END;



   PROCEDURE UpdSumPlanAvrRQ (p_CalcDate IN DATE, p_MarketID IN NUMBER, p_Kind IN NUMBER)
   AS
   BEGIN
      UPDATE DDL_LIMITSECURITES_INT_TMP
         SET T_PLAN_PLUS_DEAL =
                GetSumPlanAvrRQ (T_CLIENT,
                                 T_CONTRACT,
                                 T_SERVKINDSUB,
                                 T_DATE,
                                 GetCheckDate(p_Kind, p_CalcDate, CASE WHEN T_MONEYCONSOLIDATED = CHR(88) THEN 1 ELSE 0 END, RSI_DlCalendars.DL_CALLNK_MARKETPLACE_SEC),
                                 T_SECURITY,
                                 1,
                                 p_MarketID),
             T_PLAN_MINUS_DEAL =
                GetSumPlanAvrRQ (T_CLIENT,
                                 T_CONTRACT,
                                 T_SERVKINDSUB,
                                 T_DATE,
                                 GetCheckDate(p_Kind, p_CalcDate, CASE WHEN T_MONEYCONSOLIDATED = CHR(88) THEN 1 ELSE 0 END, RSI_DlCalendars.DL_CALLNK_MARKETPLACE_SEC),
                                 T_SECURITY,
                                 0,
                                 p_MarketID),
              T_OPEN_LIMIT =  case when T_DATE = GetCheckDate(p_Kind, p_CalcDate, CASE WHEN T_MONEYCONSOLIDATED = CHR(88) THEN 1 ELSE 0 END, RSI_DlCalendars.DL_CALLNK_MARKETPLACE_SEC)
               THEN GetSumPlanAvrRQ (T_CLIENT,
                                 T_CONTRACT,
                                 T_SERVKINDSUB,
                                 T_DATE,
                                 GetCheckDate(p_Kind, p_CalcDate, CASE WHEN T_MONEYCONSOLIDATED = CHR(88) THEN 1 ELSE 0 END, RSI_DlCalendars.DL_CALLNK_MARKETPLACE_SEC)-1,
                                 T_SECURITY,
                                 1,
                                 p_MarketID) -
                GetSumPlanAvrRQ (T_CLIENT,
                                 T_CONTRACT,
                                 T_SERVKINDSUB,
                                 T_DATE ,
                                 GetCheckDate(p_Kind, p_CalcDate, CASE WHEN T_MONEYCONSOLIDATED = CHR(88) THEN 1 ELSE 0 END, RSI_DlCalendars.DL_CALLNK_MARKETPLACE_SEC)-1,
                                 T_SECURITY,
                                 0,
                                 p_MarketID)
                                 ELSE 0 END
       WHERE EXISTS
                (SELECT 1
                   FROM DDL_FIID_DBT FIID
                  WHERE  t_calc_sid = g_calc_clientinfo and FIID.T_CLIENTID = DDL_LIMITSECURITES_INT_TMP.T_CLIENT
                        AND FIID.T_CLIENTCONTRID =
                               DDL_LIMITSECURITES_INT_TMP.T_CONTRACT
                        AND FIID.T_FIID =
                               DDL_LIMITSECURITES_INT_TMP.t_SECURITY);

      UPDATE DDL_LIMITSECURITES_INT_TMP
         SET T_OPEN_BALANCE =
                (T_QUANTITY + T_PLAN_PLUS_DEAL - T_PLAN_MINUS_DEAL);
   END;
 
   PROCEDURE InsertLIMITCASHSTOCKFromInt(p_CalcDate IN DATE)
   AS
   BEGIN
    TimeStamp_ ('Сохранение расчета лимитов по денежным средствам ',p_CalcDate,NULL,SYSTIMESTAMP);
    commit;
    merge /*+ index(lim DDL_LIMITCASHSTOCK_DBT_IDX2) */  into DDL_LIMITCASHSTOCK_DBT lim
    using (
      with int_tmp as
       (select  min(rowid) over(partition by T_MARKET, T_MARKET_KIND, T_CLIENT, T_INTERNALACCOUNT, T_CURRID, tmp.T_LIMIT_KIND) minrow_id
              ,rowid row_id
              ,tmp.*
          from (select T_ID as T_ID
                      ,T_DATE as T_DATE
                      ,T_TIME as T_TIME
                      ,case
                         when T_MARKET_KIND = 'ЕДП' then
                          to_char(rshb_rsi_sclimit.GetMicexID())
                         else
                          T_MARKET
                       end as T_MARKET
                      ,T_CLIENT as T_CLIENT
                      ,T_INTERNALACCOUNT as T_INTERNALACCOUNT
                      ,T_FIRM_ID as T_FIRM_ID
                      ,T_TAG as T_TAG
                      ,T_CURRID as T_CURRID
                      ,T_CURR_CODE as T_CURR_CODE
                      ,T_CLIENT_CODE as T_CLIENT_CODE
                      ,T_OPEN_BALANCE as T_OPEN_BALANCE
                      ,T_OPEN_LIMIT as T_OPEN_LIMIT
                      ,T_CURRENT_LIMIT as T_CURRENT_LIMIT
                      ,T_LEVERAGE as T_LEVERAGE
                      ,T_LIMIT_KIND as T_LIMIT_KIND
                      ,T_MONEY306 as T_MONEY306
                      ,T_DUE474 as T_DUE474
                      ,T_PLAN_PLUS_DEAL as T_PLAN_PLUS_DEAL
                      ,T_PLAN_MINUS_DEAL as T_PLAN_MINUS_DEAL
                      ,T_COMPREVIOUS as T_COMPREVIOUS
                      ,T_ISBLOCKED as T_ISBLOCKED
                      ,T_MARKET_KIND as T_MARKET_KIND
                      ,case
                         when T_MARKET_KIND = 'фондовый'
                              and T_MARKET != to_char(rshb_rsi_sclimit.GetMicexID()) then
                          'ЕДП'
                         else
                          T_MARKET_KIND
                       end as EDP_MARKET_KIND
                      ,T_CONTRID as T_CONTRID
                      ,T_SERVSUBKIND as T_SERVSUBKIND
                      ,T_ENDDATE as T_ENDDATE
                      ,T_COMPREVIOUS_1 as T_COMPREVIOUS_1
                      ,T_SP as T_SP
                      ,T_ZCH as T_ZCH
                  from DDL_LIMITCASHSTOCK_INT l
                  where T_CALC_SID = g_calc_clientinfo ) tmp)
      select f.T_ID
            ,f.T_DATE
            ,f.T_TIME
            ,f.T_MARKET
            ,f.T_CLIENT
            ,f.T_INTERNALACCOUNT
            ,f.T_FIRM_ID
            ,f.T_TAG
            ,f.T_CURRID
            ,f.T_CURR_CODE
            ,f.T_CLIENT_CODE
            ,f.T_OPEN_BALANCE
            ,f.T_OPEN_LIMIT
            ,f.T_CURRENT_LIMIT
            ,f.T_LEVERAGE
            ,f.T_LIMIT_KIND
            ,f.T_MONEY306
            ,f.T_DUE474
            ,f.T_PLAN_PLUS_DEAL
            ,f.T_PLAN_MINUS_DEAL
            ,f.T_COMPREVIOUS
            ,f.T_ISBLOCKED
            ,f.T_MARKET_KIND
            ,f.T_CONTRID
            ,f.T_SERVSUBKIND
            ,f.T_ENDDATE
            ,f.T_COMPREVIOUS_1
            ,f.T_SP
            ,f.T_ZCH
            ,nvl(sum(ledp.T_PLAN_PLUS_DEAL), 0) edp_PLAN_PLUS_DEAL
            ,nvl(sum(ledp.T_PLAN_MINUS_DEAL), 0) edp_PLAN_MINUS_DEAL
            ,nvl(sum(ledp.T_COMPREVIOUS), 0) edp_COMPREVIOUS
            ,nvl(sum(ledp.T_OPEN_LIMIT), 0) edp_OPEN_LIMIT
        from int_tmp f
        left join int_tmp ledp
          on f.T_MARKET_KIND = 'ЕДП'
         and ledp.EDP_MARKET_KIND = 'ЕДП'
         and (ledp.minrow_id != ledp.row_id or ledp.EDP_MARKET_KIND != ledp.T_MARKET_KIND)
         and ledp.T_CLIENT = f.T_CLIENT
         and ledp.T_DATE = f.T_DATE
         and ledp.T_INTERNALACCOUNT = f.T_INTERNALACCOUNT
         and ledp.T_CURRID = f.T_CURRID
         and ledp.T_LIMIT_KIND = f.T_LIMIT_KIND
        left join int_tmp nedp
          on f.T_MARKET_KIND != f.EDP_MARKET_KIND
         and nedp.T_MARKET_KIND = 'ЕДП'
         and nedp.minrow_id = nedp.row_id
         and nedp.T_CLIENT = f.T_CLIENT
         and nedp.T_DATE = f.T_DATE
         and nedp.T_INTERNALACCOUNT = f.T_INTERNALACCOUNT
         and nedp.T_CURRID = f.T_CURRID
         and nedp.T_LIMIT_KIND = f.T_LIMIT_KIND
       where f.minrow_id = f.row_id
         and nedp.EDP_MARKET_KIND is null
       group by f.T_ID
               ,f.T_DATE
               ,f.T_TIME
               ,f.T_MARKET
               ,f.T_CLIENT
               ,f.T_INTERNALACCOUNT
               ,f.T_FIRM_ID
               ,f.T_TAG
               ,f.T_CURRID
               ,f.T_CURR_CODE
               ,f.T_CLIENT_CODE
               ,f.T_OPEN_BALANCE
               ,f.T_OPEN_LIMIT
               ,f.T_CURRENT_LIMIT
               ,f.T_LEVERAGE
               ,f.T_LIMIT_KIND
               ,f.T_MONEY306
               ,f.T_DUE474
               ,f.T_PLAN_PLUS_DEAL
               ,f.T_PLAN_MINUS_DEAL
               ,f.T_COMPREVIOUS
               ,f.T_ISBLOCKED
               ,f.T_MARKET_KIND
               ,f.T_CONTRID
               ,f.T_SERVSUBKIND
               ,f.T_ENDDATE
               ,f.T_COMPREVIOUS_1
               ,f.T_SP
               ,f.T_ZCH) buf
          on (lim.T_CLIENT = buf.T_CLIENT and lim.T_DATE = buf.T_DATE and lim.T_INTERNALACCOUNT = buf.T_INTERNALACCOUNT and lim.T_CURRID = buf.T_CURRID and
             lim.T_LIMIT_KIND = buf.T_LIMIT_KIND and lim.T_MARKET = rshb_rsi_sclimit.GetMicexID() and lim.T_MARKET_KIND = 'ЕДП')
      --
       when matched then
        update
           set lim.T_PLAN_PLUS_DEAL  = lim.T_PLAN_PLUS_DEAL + buf.T_PLAN_PLUS_DEAL + buf.edp_PLAN_PLUS_DEAL
              ,lim.T_PLAN_MINUS_DEAL = lim.T_PLAN_MINUS_DEAL + buf.T_PLAN_MINUS_DEAL + buf.edp_PLAN_MINUS_DEAL
              ,lim.T_COMPREVIOUS     = lim.T_COMPREVIOUS + buf.T_COMPREVIOUS + buf.edp_COMPREVIOUS
              ,lim.T_OPEN_BALANCE    = lim.T_OPEN_BALANCE
                                         - buf.T_COMPREVIOUS - buf.edp_COMPREVIOUS  
                                         + buf.T_PLAN_PLUS_DEAL + buf.edp_PLAN_PLUS_DEAL 
                                         - buf.T_PLAN_MINUS_DEAL - buf.edp_PLAN_MINUS_DEAL
              ,lim.T_OPEN_LIMIT      = lim.T_OPEN_LIMIT + buf.T_OPEN_LIMIT + buf.edp_OPEN_LIMIT
      when not matched then
        insert
        values
          (buf.T_ID                                                                
          ,buf.T_DATE
          ,buf.T_TIME
          ,buf.T_MARKET
          ,buf.T_CLIENT
          ,buf.T_INTERNALACCOUNT
          ,buf.T_FIRM_ID
          ,buf.T_TAG
          ,buf.T_CURRID
          ,buf.T_CURR_CODE
          ,buf.T_CLIENT_CODE
          ,buf.T_OPEN_BALANCE - buf.edp_COMPREVIOUS + buf.edp_PLAN_PLUS_DEAL - buf.edp_PLAN_MINUS_DEAL
          ,buf.T_OPEN_LIMIT + buf.edp_OPEN_LIMIT
          ,buf.T_CURRENT_LIMIT
          ,buf.T_LEVERAGE
          ,buf.T_LIMIT_KIND
          ,buf.T_MONEY306
          ,buf.T_DUE474
          ,buf.T_PLAN_PLUS_DEAL + buf.edp_PLAN_PLUS_DEAL
          ,buf.T_PLAN_MINUS_DEAL + buf.edp_PLAN_MINUS_DEAL
          ,buf.T_COMPREVIOUS + buf.edp_COMPREVIOUS
          ,buf.T_ISBLOCKED
          ,buf.T_MARKET_KIND
          ,buf.T_COMPREVIOUS_1
          ,buf.T_SP
          ,buf.T_ZCH) ;
    COMMIT; -- чтобы не было ошибки ORA-12838
   END InsertLIMITCASHSTOCKFromInt;
 
   procedure DeleteWoOpenBalance(p_CalcDate date)
   as
   begin
    TimeStamp_ ('Удаление лимитов с нулевым open_balance ',p_CalcDate,NULL,SYSTIMESTAMP);
    delete /*+ parallel(lim 4) enable_parallel_dml */ ddl_limitcashstock_dbt lim
    where t_id in (with pre_calced_limits as (
                      select /*+ parallel(l 4) full(l) */ sum(case when l.t_open_balance != 0
                                         or l.t_open_limit != 0
                                         or l.t_money306 != 0
                                         or l.t_due474 != 0
                                         or l.t_plan_minus_deal != 0
                                         or l.t_plan_plus_deal != 0 then 1 else 0 end) 
                                                   over (partition by l.t_date
                                                                    ,l.t_market
                                                                    ,l.t_client
                                                                    ,l.t_client_code
                                                                    ,l.t_currid
                                                                    ,l.t_firm_id
                                                                    ,l.t_market_kind) cnt_not_null_lims,
                             l.t_id
                        from ddl_limitcashstock_dbt l
                        where t_curr_code != 'SUR'
                          and t_date = p_CalcDate
                          )
                      select l.t_id
                        from pre_calced_limits l
                       where l.cnt_not_null_lims = 0);
    COMMIT; -- чтобы не было ошибки ORA-12838
   end DeleteWoOpenBalance;

   PROCEDURE InsertLimitFromIntSecur
   AS
   BEGIN
      INSERT INTO DDL_LIMITSECURITES_DBT
         (SELECT 0,
                 int_tmp.T_DATE,
                 int_tmp.T_TIME,
                 int_tmp.T_MARKET,
                 int_tmp.T_CLIENT,
                 int_tmp.T_SECURITY,
                 int_tmp.T_FIRM_ID,
                 int_tmp.T_SECCODE,
                 int_tmp.T_CLIENT_CODE,
                 int_tmp.T_OPEN_BALANCE,
                 int_tmp.T_OPEN_LIMIT,
                 int_tmp.T_CURRENT_LIMIT,
                 int_tmp.T_TRDACCID,
                 int_tmp.T_WA_POSITION_PRICE,
                 int_tmp.T_LIMIT_KIND,
                 int_tmp.T_QUANTITY,
                 int_tmp.T_PLAN_PLUS_DEAL,
                 int_tmp.T_PLAN_MINUS_DEAL,
                 int_tmp.T_ISBLOCKED,
                 int_tmp.T_MARKET_KIND,
                 int_tmp.T_MONEYCONSOLIDATED
            FROM DDL_LIMITSECURITES_INT_TMP int_tmp);
       exception when others then
        TimeStamp_ ('!Error InsertLimitFromIntSecur  '||Dbms_Utility.Format_Error_Stack || ' ' || Dbms_Utility.Format_Error_Backtrace,
                  null,
                  NULL,
                  SYSTIMESTAMP,9999
                  ,excepsqlcode_ => 100
                  ,all_log_=> true
                 );
   END;

   FUNCTION GetObjAtCor( p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE,
                           p_Object     IN dobjatcor_dbt.t_Object%TYPE,
                           p_GroupID    IN dobjatcor_dbt.t_GroupID%TYPE,
                           p_Date       IN dobjatcor_dbt.t_ValidFromDate%TYPE )
     RETURN dobjattr_dbt.t_AttrID%TYPE
   IS
     p_AttrID dobjattr_dbt.t_AttrID%TYPE;
  BEGIN

    BEGIN
      SELECT AtCor.t_AttrID INTO p_AttrID
        FROM dobjatcor_dbt AtCor
       WHERE AtCor.t_ObjectType  = p_ObjectType
         AND AtCor.t_GroupID     = p_GroupID
         AND AtCor.t_Object      = p_Object
         AND AtCor.t_ValidToDate >= p_Date
         AND AtCor.t_ValidFromDate = (SELECT MAX(t.t_ValidFromDate)
                                        FROM dobjatcor_dbt t
                                       WHERE t.t_ObjectType     = p_ObjectType
                                         AND t.t_GroupID        = p_GroupID
                                         AND t.t_Object         = p_Object
                                         AND t.t_ValidFromDate <= p_Date
                                         AND t.t_ValidToDate    >= p_Date
                                     );
    EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
        p_AttrID := 0;
      WHEN OTHERS
      THEN
        p_AttrID := 0;
    END;

    RETURN p_AttrID;

  END GetObjAtCor;


  FUNCTION GetKindMarketCodeOrNote (pMarketID IN NUMBER, IsSecCode IN NUMBER, IsTradeaccID IN NUMBER)
     RETURN NUMBER DETERMINISTIC
  IS
     vCode NUMBER := 0;
  BEGIN

     IF IsSecCode = 1
     THEN
       IF pMarketId = GetMicexID()
       THEN
         vCode  := 11;
       END IF;

       IF pMarketId = GetSpbexID()
       THEN
         vCode  := 22;
       END IF;
     END IF;

     IF IsTradeaccID = 1
     THEN
       IF pMarketId = GetMicexID()
       THEN
         vCode  := 5;
       END IF;

       IF pMarketId = GetSpbexID()
       THEN
         vCode  := 10;
       END IF;
     END IF;


     RETURN vCode;
  END;


PROCEDURE RSI_CreateLimitsKindParallel (p_ExecStr IN VARCHAR2,p_parallel in number default 4)
   AS
      l_task_name   VARCHAR2 (30);
      l_try         NUMBER;
      l_status      NUMBER;

      l_stmt        CLOB;
   BEGIN
      l_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;
      DBMS_PARALLEL_EXECUTE.create_task (task_name => l_task_name);

      l_stmt := 'SELECT level, level FROM DUAL CONNECT BY LEVEL <= '||p_parallel;
      DBMS_PARALLEL_EXECUTE.create_chunks_by_sql (task_name => l_task_name, sql_stmt => l_stmt, by_rowid => FALSE);

      DBMS_PARALLEL_EXECUTE.run_task (task_name => l_task_name,
                                      sql_stmt => p_ExecStr,
                                      language_flag => DBMS_SQL.NATIVE,
                                      parallel_level => p_parallel
                                     );

      l_try := 0;
      l_status := DBMS_PARALLEL_EXECUTE.task_status (l_task_name);

      WHILE (l_try < 2 AND l_status != DBMS_PARALLEL_EXECUTE.FINISHED)
      LOOP
         l_try := l_try + 1;
         DBMS_PARALLEL_EXECUTE.resume_task (l_task_name);
         l_status := DBMS_PARALLEL_EXECUTE.task_status (l_task_name);
      END LOOP;

      DBMS_PARALLEL_EXECUTE.drop_task (l_task_name);
   END;                                        -- RSI_CreateLimitsKindParallel


   PROCEDURE RSI_CreateCashStockLimByKind (
      p_start_id        IN NUMBER,
      p_end_id          IN NUMBER,
      p_CalcDate        IN DATE,
      p_ByMarket        IN NUMBER,
      p_ByOutMarket     IN NUMBER,
      p_ByEdp           IN NUMBER,
      p_MarketCode      IN VARCHAR2,
      p_MarketID        IN NUMBER
      )
   AS
      v_CheckDate      DATE;
      v_Time           DATE;
     v_Kind         NUMBER;
     v_IsEDP char(1) :=  case when p_ByEDP = 1 then CHR(88) else CHR(0) end ;
     v_MicexID integer := GetMicexID();
   BEGIN
      ts_ := SYSTIMESTAMP;

      --EXECUTE IMMEDIATE 'truncate table DDL_LIMITCASHSTOCK_INT_TMP';

      v_Time := TO_DATE ('01-01-0001:' || TO_CHAR (SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');

      IF p_start_id = 1
      THEN
         v_Kind := 0;
      ELSIF p_start_id = 2
      THEN
         v_Kind := 1;
      ELSIF p_start_id = 3
      THEN
         v_Kind := 2;
      ELSE
         v_Kind := 365;
      END IF;

     -- DEF-65531, определяем дату в начале функции, 
     -- так как она зависит только от v_Kind, ЕДП и, если неЕДП, вида рынка (ФР, ВР, СР)
     v_CheckDate := GetCheckDate(v_Kind, p_CalcDate, p_ByEDP, 1); 

      insert first 
      when p_MarketID = v_MicexID  then 
       into DDL_LIMITCASHSTOCK_DBT values (
                t_id  ,                          
                t_date ,          
                t_time  ,         
                t_market ,        
                t_client  ,       
                t_internalaccount,
                t_firm_id ,       
                t_tag ,           
                t_currid,         
                t_curr_code,      
                t_client_code,    
                t_open_balance,   
                t_open_limit ,    
                t_current_limit,  
                t_leverage ,      
                t_limit_kind ,    
                t_money306 ,      
                t_due474 ,        
                t_plan_plus_deal ,
                t_plan_minus_deal,
                t_comprevious ,   
                t_isblocked ,     
                t_market_kind ,   
                t_comprevious_1 , 
                t_sp ,            
                t_zch  )     
      else  into DDL_LIMITCASHSTOCK_INT values (
                t_id  ,            
                t_date ,          
                t_time  ,         
                t_market ,        
                t_client  ,       
                t_internalaccount,
                t_firm_id ,       
                t_tag ,           
                t_currid,         
                t_curr_code,      
                t_client_code,    
                t_open_balance,   
                t_open_limit ,    
                t_current_limit,  
                t_leverage ,      
                t_limit_kind ,    
                t_money306 ,      
                t_due474 ,        
                t_plan_plus_deal ,
                t_plan_minus_deal,
                t_comprevious ,   
                t_isblocked ,     
                t_market_kind ,   
                t_contrid ,       
                t_servsubkind ,   
                t_enddate ,       
                t_comprevious_1 , 
                t_sp ,            
                t_zch ,           
                g_calc_clientinfo ) 
       with sf as ( select /*+ materialize */ * 
                   from ( select distinct t_AccountID,
                      t_time306,
                      t_Client,
                      t_SfcontrID,
                      t_Account,
                      t_Code_Currency,
                      t_Money306,
                      t_OtherReq ,
                      t_comprevious ,
                      sum(case when t_Money306 = 0 and t_Due474 = 0 and t_OtherReq = 0 and t_comprevious = 0   then 0 else 1 end ) over (partition by t_client,t_sfcontrid) isRest,
                      t_MarketID ,
                      t_Firm_ID ,
                      case when t_isedp = chr(88) then t_ekk else t_mpcode end t_mpcode,
                      t_ServKindSub,
                      t_CCY,
                      t_Leverage,
                      t_Due474,
                      t_IsBlocked ,
                      t_test_result,
                      t_tag 
                      from DDL_CLIENTINFO_DBT c
                       WHERE t_calc_sid = g_calc_clientinfo and t_ServKind = 1  
                            and ((ExcludeErrClients = 0 ) or (t_HasErrors = chr(0))) and v_IsEDP = t_IsEdp
                            and t_MarketID = p_MarketID) sf
                 where  t_Code_Currency = RSI_RSB_FIInstr.NATCUR  or isRest != 0  
                  or exists ( select /*+ index(pc DDL_LIMITCOM_DBT_IDX1) */ 1  from DDL_LIMITCOM_DBT pc
                                        where pc.t_calc_sid = g_calc_clientinfo and pc.t_marketid = p_MarketID
                                        and  pc.t_client = sf.t_Client and pc.t_sfcontrid = sf.t_SfcontrID )
                  or exists ( select /*+ index(tk DLIMIT_DLTICK_IDX1)*/ 1 from dlimit_dltick_dbt tk
                                        where tk.t_calc_sid = g_calc_clientinfo and tk.t_MarketID = p_MarketID
                                        and tk.t_ClientID = sf.t_Client and tk.t_ClientContrID = sf.t_SfcontrID))
       SELECT                                                            --T_ID                                                                
             0 t_id ,                                                        --T_DATE                                                                          
             p_CalcDate t_date,                                              --T_TIME                                                                         
             q.t_time306 t_time,                                                --T_MARKET                                                          
             q.Market t_market,                                              --T_CLIENT                                                        
             q.t_Client t_client,                                   --T_INTERNALACCOUNT                                                        
             q.t_AccountID t_internalaccount,                                                                                                  
             q.FIRM_ID t_firm_id, --'MC0134700000', --'MC0038600000', 20181218 - kva - изменено до ХФ                                          
             q.tag /*case when q.tag <> chr(1) then q.tag else v_TAG end*/ t_tag, --'EQTV',      --T_CURRID                                              
             q.t_Code_Currency t_currid,                                                                                                       
             DECODE (q.t_Code_Currency, RSI_RSB_FIInstr.NATCUR, 'SUR',q.t_CCY) t_curr_code,                                                    
             q.t_mpcode AS t_client_code,                                                                                                      
             q.t_Money306 - q.t_ComPrevious - q.t_comprevious_1 + q.Plan_Plus_Deal - q.Plan_Minus_Deal t_open_balance,                                                               
             q.Open_Limit t_open_limit,                                                                                                        
             q.Open_Limit t_current_limit,                                                                                                     
             test_result t_leverage /*BIQ 9185*/,                          --T_LEVERAGE                                                        
             v_Kind t_limit_kind,      --T_LIMIT_KIND                                                                                          
             q.t_Money306  t_money306,                     --T_DUE474                                                    
             q.t_Due474 t_due474,                                        --q.ComPrevious,                                                      
             q.Plan_Plus_Deal t_plan_plus_deal,                             --T_PLAN_MINUS_DEAL                                                
             q.Plan_Minus_Deal t_plan_minus_deal,                                                                                              
             q.t_ComPrevious t_comprevious,                                      --T_ISBLOCKED                                                   
             q.IsBlocked t_isblocked,                                                                                                          
             CASE WHEN p_ByEDP = 1 THEN 'ЕДП' ELSE 'фондовый' END t_market_kind,                                                               
             q.ContrID t_contrid,                                                                                                              
             q.ServKindSub t_servsubkind,                                                                                                      
             v_CheckDate t_enddate,  
             q.t_comprevious_1,                                                                                                                
             0 t_sp,                                                                                                                           
             0 t_zch                                                                                                                           
   --     BULK COLLECT INTO v_limcashstock
        FROM (SELECT  t_AccountID,
                      t_time306,
                      t_Client,
                      t_Account,
                      t_Code_Currency,
                      t_Money306 t_Money306,
                      t_MarketID as Market, 
                      t_Firm_ID  FIRM_ID,
                      t_mpcode,
                      t_ServKindSub,
                      t_CCY,
                      t_Leverage,
                      GetSumPlanCashRQ (t_Client,
                                        t_SfcontrID,
                                        t_ServKindSub,
                                        p_CalcDate,
                                        p_CalcDate-1,
                                        t_AccountID,
                                        t_Code_Currency,
                                        1, p_MarketID
                                       ) -
                      GetSumPlanCashRQ (t_Client,
                                        t_SfcontrID,
                                        t_ServKindSub,
                                        p_CalcDate,
                                        p_CalcDate-1,
                                        t_AccountID,
                                        t_Code_Currency,
                                        0, p_MarketID
                                       ) AS Open_Limit,
                      t_Due474,
                      GetSumPlanCashRQ (t_Client,
                                        t_SfcontrID,
                                        t_ServKindSub,
                                        p_CalcDate,
                                        v_CheckDate,
                                        t_AccountID,
                                        t_Code_Currency,
                                        1, p_MarketID
                                       )
                         AS Plan_Plus_Deal,
                      GetSumPlanCashRQ (t_Client,
                                        t_SfcontrID,
                                        t_ServKindSub,
                                        p_CalcDate,
                                        v_CheckDate,
                                        t_AccountID,
                                        t_Code_Currency,
                                        0, p_MarketID
                                       ) +
                      GetSumPlanPeriodCom(t_Client,
                                        t_SfcontrID,
                                        t_ServKindSub,
                                        p_CalcDate,
                                        v_CheckDate,
                                        t_Code_Currency,
                                        p_MarketID)                
                         AS Plan_Minus_Deal,
                      t_comprevious AS t_ComPrevious,
                      t_OtherReq as t_ComPrevious_1,
                      t_IsBlocked AS IsBlocked,
                      t_mpcode AS Client_code,
                      t_SfcontrID AS ContrID,
                      t_ServKindSub AS ServKindSub,
                      t_test_result as test_result,
                      t_tag as tag,
                      t_Firm_ID
              FROM sf
                 ) q;
    
     TimeStamp_ ('Расчет лимита Т' || v_Kind || ' MONEY',
                  p_CalcDate,
                  ts_,
                  SYSTIMESTAMP,
                900*p_ByEDP +  p_start_id * 10
                 );
   END;                                        -- RSI_CreateCashStockLimByKind




   -- BOSS-771, проверяет наличие категорий
   -- 'Предоставлять брокеру право использования активов в его интересах'
   -- И 'Перевод активов на новый номер ТКС произведен'
   -- на субдоговоре вид обслуживания = "Фондовый дилинг" И биржа = ММВБ
   -- Возвращается флаг:
   -- для flag=1 используется параметры справочника (Биржа" = ММВБ, "Рынок" = "валютный", "ТКС" = "Основной")
   -- для flag=2 используется параметры справочника (Биржа" = ММВБ, "Рынок" = "валютный", "ТКС" = "Для клиентов 2-го типа")
   function GetImplKind(p_dlcontrid IN number, p_CalcDate in DATE)
     return number deterministic
   is
     pragma udf;
     x_Flg6 number := 1;
     x_Flg7 number := 1;
     x_Flg number := 1; -- по умолчанию
   begin
     /*SELECT 
       case when c6.t_id is not null then 1 else 2 end AS flag_c6 
       , case when c7.t_id is not null then 1 else 2 end AS flag_c7 
     INTO 
       x_Flg6, x_Flg7
     FROM (
        SELECT sf.t_id FROM dsfcontr_dbt sf, DDLCONTRMP_DBT mp
        WHERE
          mp.t_dlcontrid = p_dlcontrid
          and sf.t_id = mp.t_sfcontrid
          and sf.t_servkind = 1 -- вид обслуживания = "Фондовый дилинг"
          and mp.t_marketid = 2 -- биржа = ММВБ
          and sf.T_DATEBEGIN <= p_CalcDate
          and (sf.T_DATECLOSE >= p_CalcDate or sf.T_DATECLOSE = TO_DATE('01.01.0001','DD.MM.YYYY'))
       ) a
       LEFT JOIN dobjatcor_dbt c6 ON (
          c6.t_objecttype = 659 and c6.t_object = LPAD(a.t_id, 10, '0') and c6.t_groupid = 6 and c6.t_attrid = 1 
          and p_CalcDate between c6.T_VALIDFROMDATE and c6.T_VALIDTODATE and c6.T_GENERAL = chr(88)
       )
       LEFT JOIN dobjatcor_dbt c7 ON (
          c7.t_objecttype = 659 and c7.t_object = LPAD(a.t_id, 10, '0') and c7.t_groupid = 7 and c7.t_attrid = 1 
          and p_CalcDate between c7.T_VALIDFROMDATE and c7.T_VALIDTODATE and c7.T_GENERAL = chr(88)
       )
     ;
     -- Если "Предоставлять брокеру право использования активов в его интересах" = "Нет" 
     -- И категория "Перевод активов на новый номер ТКС произведен" = "Да"
      if(x_Flg6 = 2 AND x_Flg7 = 1) then
       x_Flg := 2;
     end if;
    if(x_Flg6 = 2 AND x_Flg7 = 1) then
       x_Flg := 2;
     end if;
*/
     SELECT 
       case when (select c7.t_id from  dobjatcor_dbt c7 
         where c7.t_objecttype = 659 and c7.t_object = LPAD(a.t_id, 10, '0') and c7.t_groupid = 7 and c7.t_attrid = 1 
          and p_CalcDate between c7.T_VALIDFROMDATE and c7.T_VALIDTODATE and c7.T_GENERAL = chr(88)) is null then 1 else 2 end
     INTO 
        x_Flg
     FROM (
        SELECT distinct sf.t_id FROM dsfcontr_dbt sf, DDLCONTRMP_DBT mp
        WHERE
          mp.t_dlcontrid = p_dlcontrid
          and sf.t_id = mp.t_sfcontrid
          and sf.t_servkind = 1 -- вид обслуживания = "Фондовый дилинг"
          and mp.t_marketid = 2 -- биржа = ММВБ
          and sf.T_DATEBEGIN <= p_CalcDate
          and (sf.T_DATECLOSE >= p_CalcDate or sf.T_DATECLOSE = TO_DATE('01.01.0001','DD.MM.YYYY'))
       ) a
     ;
 
     return x_Flg;

   EXCEPTION
       WHEN others THEN  
         return 1;
   end;



   PROCEDURE RSI_CreateCashStockLimByKindCur (p_CalcDate IN DATE, p_Kind IN NUMBER, p_IsDepo IN NUMBER, p_ByEDP IN NUMBER)
   AS
      v_CheckDate      DATE;
      v_Time           DATE;
   BEGIN
      ts_ := SYSTIMESTAMP;

      TimeStamp_ (
            'RSI_CreateCashStockLimByKindCur( p_Kind='||p_Kind||', p_IsDepo='||p_IsDepo||', p_ByEDP='||p_ByEDP||' )'
            , p_CalcDate, ts_, SYSTIMESTAMP);

      v_Time := TO_DATE ('01-01-0001:' || TO_CHAR (SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');

      v_CheckDate := GetCheckDate(p_Kind, p_CalcDate, p_ByEDP, 2);

      insert first 
      when p_ByEDP = 1  then 
        into DDL_LIMITCASHSTOCK_INT values (
                t_id  ,            
                t_date ,          
                t_time  ,         
                t_market ,        
                t_client  ,       
                t_internalaccount,
                t_firm_id ,       
                t_tag ,           
                t_currid,         
                t_curr_code,      
                t_client_code,    
                t_open_balance,   
                t_open_limit ,    
                t_current_limit,  
                t_leverage ,      
                t_limit_kind ,    
                t_money306 ,      
                t_due474 ,        
                t_plan_plus_deal ,
                t_plan_minus_deal,
                t_comprevious ,   
                t_isblocked ,     
                t_market_kind ,   
                t_contrid ,       
                t_servsubkind ,   
                t_enddate ,       
                t_comprevious_1 , 
                t_sp ,            
                t_zch ,           
                g_calc_clientinfo )     
      else into DDL_LIMITCASHSTOCK_DBT values (
                t_id  ,            
                t_date ,          
                t_time  ,         
                t_market ,        
                t_client  ,       
                t_internalaccount,
                t_firm_id ,       
                t_tag ,           
                t_currid,         
                t_curr_code,      
                t_client_code,    
                t_open_balance,   
                t_open_limit ,    
                t_current_limit,  
                t_leverage ,      
                t_limit_kind ,    
                t_money306 ,      
                t_due474 ,        
                t_plan_plus_deal ,
                t_plan_minus_deal,
                t_comprevious ,   
                t_isblocked ,     
                t_market_kind ,   
                t_comprevious_1 , 
                t_sp ,            
                t_zch  ) 
      with sf as (select /*+ materialize*/ * 
                  from (select  distinct t_sfcontrid, t_dlcontrid, t_client, t_accountid, t_account, t_time306
                                   , t_Money306,t_OtherReq, sum(case when t_Money306 =0 and t_OtherReq = 0 then 0 else 1 end ) over (partition by t_client,t_sfcontrid) isMoney306 
                                   , t_code_currency, t_leverage, t_mpcode, t_trdaccid, t_servkind, t_servkindsub, t_isblocked, t_isedp
                                   , t_test_result, t_marketid, t_haserrors, t_ImplKind,t_tag, t_Firm_ID, t_ekk
                                  from ddl_clientinfo_dbt where t_calc_sid = g_calc_clientinfo and t_marketid = GetMicexID() and t_servkind = 21 
                                   and  ((ExcludeErrClients = 0 ) or (t_HasErrors = chr(0)))  and CASE WHEN p_ByEDP = 1 THEN CHR(88) ELSE CHR(0) END = t_IsEDP
                                   AND t_Code_Currency = CASE WHEN p_IsDepo <> 1 THEN t_Code_Currency
                                                              ELSE RSI_RSB_FIInstr.NATCUR
                                                           END ) sf
                 where t_code_currency = RSI_RSB_FIInstr.NATCUR or isMoney306 != 0  
                    or exists (select /*+ index(cm DLIMIT_CMTICK_IDX1)*/ 1 from dlimit_cmtick_dbt cm  -- таблица с требованиями и обязательствами  
                                        where cm.t_calc_sid = g_calc_clientinfo
                                        and  cm.t_clientid = sf.t_client and  sf.t_sfcontrid =  cm.t_clientcontrid ) )
      SELECT                                                          
            0 t_id,                                                                                                          
             p_CalcDate t_date,                                                                                              
             q.t_time306 t_time,                                                                                                  
             Market t_market,                                                                                                
             q.t_Client t_client,                                                                                            
             q.t_AccountID t_internalaccount                                                                                 
             , t_firm_id                                                                                                  
             , t_tag  ,                                                                                                        
             q.t_Code_Currency t_currid,                                                                                           
             DECODE (q.t_Code_Currency, RSI_RSB_FIInstr.NATCUR, 'SUR', q.t_CCY ) t_curr_code,                                      
             q.Client_code t_client_code,                                                                                          
             (q.Money306 - q.t_OtherReq + q.Plan_Plus_Deal - q.Plan_Minus_Deal) t_open_balance,                                                    
             q.Open_Limit t_open_limit,                                                                                            
             q.Open_Limit t_current_limit,                                                                                         
             q.Test_Result t_leverage,                                                                                             
             p_Kind t_limit_kind,                                                                                                  
             q.Money306 t_money306,                                                                                                
             q.ComPrevious t_due474,                                                                                               
             q.Plan_Plus_Deal t_plan_plus_deal,                                                                                    
             q.Plan_Minus_Deal t_plan_minus_deal,                                                                                  
             q.ComPrevious t_comprevious,                                                                                          
             q.t_IsBlocked t_isblocked,                                                                                            
             CASE WHEN p_ByEDP = 1 THEN 'ЕДП' ELSE 'валютный' END t_market_kind,                                                   
             t_SfcontrID t_contrid ,                                                                                      
             t_ServKindSub t_servsubkind ,                                                                            
             TO_DATE('01.01.0001','dd.mm.yyyy') t_enddate,                                                                     
             q.t_OtherReq t_comprevious_1,                                                                                                    
             0 t_sp,                                                                                                               
             0 t_zch
--        BULK COLLECT INTO v_limcashstock
        FROM ( SELECT  t_AccountID,
                             t_Client,
                             t_time306,
                             t_Account,
                             t_Code_Currency,
                             t_Money306  Money306,
                             t_OtherReq, 
--                             CASE WHEN p_ByEDP = 1 THEN 0 ELSE t_Money306 END Money306, -- остаток посчитается в лимитах по фонде
                             t_MarketID AS Market,
                             t_tag, -- case when t_isedp = chr(88) then v_TAG else 'RTOD' end as TAG,
                             t_ServKindSub,
                             (select t_CCY from dfininstr_dbt where t_fiid = t_code_currency) as t_CCY,
                             t_leverage t_LeverageCur,
                             case when p_CalcDate = v_CheckDate THEN
                             GetSumPlanCashCM (t_Client,
                                               t_SfcontrID,
                                               p_CalcDate,
                                               v_CheckDate-1,
                                               t_Account,
                                               t_Code_Currency,
                                               1
                                              )  -
                             GetSumPlanCashCM (t_Client,
                                               t_SfcontrID,
                                               p_CalcDate,
                                               v_CheckDate-1,
                                               t_Account,
                                               t_Code_Currency,
                                               0
                                              )
                             ELSE 0 END  AS Open_Limit,
--                             0 AS Due474,
                             GetSumPlanCashCM (t_Client,
                                               t_SfcontrID,
                                               p_CalcDate,
                                               v_CheckDate,
                                               t_Account,
                                               t_Code_Currency,
                                               1
                                              )
                                AS Plan_Plus_Deal,
                             GetSumPlanCashCM (t_Client,
                                               t_SfcontrID,
                                               p_CalcDate,
                                               v_CheckDate,
                                               t_Account,
                                               t_Code_Currency,
                                               0
                                              )
                                AS Plan_Minus_Deal,
                             0 AS ComPrevious,                       -- пока 0
                             t_IsBlocked,
                             CASE WHEN p_ByEDP = 1 THEN t_ekk else t_mpcode end AS Client_code ,
                             t_test_result as test_result,
                             t_Firm_ID
                             , t_ImplKind 
                             , t_isedp -- DEF-65032
                             ,t_SfcontrID
                FROM sf 
                     )  q;


      TimeStamp_ (
            'RSI_CreateCashStockLimByKindCur( v_limcashstock.COUNT='||sql%rowcount||' )'
            , p_CalcDate, ts_, SYSTIMESTAMP);

      TimeStamp_ (
            'Расчет лимита по валютному рынку Т'
         || p_Kind
         || ' MONEY',
         p_CalcDate,
         ts_,
         SYSTIMESTAMP );

   END;                                     -- RSI_CreateCashStockLimByKindCur

  PROCEDURE RSI_CreateFutureMarkLim (p_CalcDate IN DATE, p_UseListClients IN NUMBER)
   AS
      v_Time            DATE := TO_DATE ('01-01-0001:' || TO_CHAR (SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');
      v_PrevWorkDate    DATE;-- := RSI_RsbCalendar.GetDateAfterWorkDay (p_CalcDate, -1, CALENDAR_MB);
      v_RestDate        DATE;
      v_CalendId        NUMBER(10) := 0;

      TYPE limfuturemark_t IS TABLE OF DDL_LIMITFUTURMARK_DBT%ROWTYPE
                                 INDEX BY BINARY_INTEGER;

      v_limfuturemark   limfuturemark_t;
      v_sql varchar2(32000);
   BEGIN
      v_CalendId := GetCalendarIDForLimit(GetMicexID(), -1, RSI_DLCALENDARS.DL_CALLNK_MARKETPLACE_DV);
      v_PrevWorkDate := RSI_RsbCalendar.GetDateAfterWorkDay (p_CalcDate, -1, v_CalendId);

      IF RSI_DLCALENDARS.GetBalanceDateAfterWorkDayByCalendar (p_CalcDate, -1, v_CalendId) <
            RSI_RSBCALENDAR.GETDATEAFTERWORKDAY (p_CalcDate, -1, v_CalendId)
      THEN
         v_RestDate := RSI_DLCALENDARS.GetBalanceDateAfterWorkDayByCalendar (p_CalcDate, 1, v_CalendId);
      ELSE
         v_RestDate := p_CalcDate - 1;
      END IF;

      v_sql :='SELECT                                                            --T_ID
            0,                                                        --T_DATE
             :p_CalcDate,                                              --T_TIME
             q.T_TIME306,                                                --T_CLIENT
             q.t_Client,                                   --T_INTERNALACCOUNT
             q.t_AccountID,                                     --T_CLASS_CODE
             ''SPBFUT'',
             --T_ACCOUNT
             CAST ((q.TrdAccID) AS VARCHAR2 (25)),
             --T_VOLUMEMN
             /* TEG так как счета безлимитные, то просто выводим остаток
             CASE
             WHEN q.Money306 - q.t_OtherReq < 0 THEN -q.ComPrevious
             ELSE (q.Money306 - q.t_OtherReq - q.ComPrevious)
             END,*/
             (q.Money306 - q.t_OtherReq - q.DUE474),                             --T_VOLUMEPL
             0,                                                        --T_KFL
             0,
             --T_KGO
             CAST (
                (CASE
                    WHEN q.KGO IS NULL THEN 1
                    ELSE rsb_struct.getdouble (q.KGO)
                 END) AS NUMBER),                                  --T_USE_KGO
             ''N'',                     --''Да'', 20181219 - kva - изменения до ХФ
             --T_FIRM_ID
             --''SPBFUTF502'',                                        --неизвестно
             ''SPBFUT''||q.t_firmid t_firmid,  --dan BIQ-7571
             --T_SECCODE
             CHR (1),
             --T_MONEY306
             q.Money306  Money306 , -- CASE WHEN q.Money306 < 0 THEN 0 ELSE q.Money306 END Money306,/*CHVA 517610*/
             --T_DUE474
             q.due474,                              --q.ComPrevious, --T_SUMGO
             q.SUMGO,                                          --T_COMPREVIOUS
             q.ComPrevious,                                      --T_ISBLOCKED
             q.IsBlocked,
             ''срочный'',
             ''ММВБ''
        FROM (';
               v_sql:=v_sql||q'[
       SELECT c.t_AccountID,
              c.T_TIME306,
              c.t_Client,
              c.t_Account,
              c.t_Code_Currency,
              C.T_MONEY306 AS Money306,
              c.t_OtherReq,
              'CL' || c.t_mpcode AS TrdAccID,
              rsi_rsb_kernel.GetNote (:OBJTYPE_SFCONTR,
                                      LPAD (sfcontr.t_ID, 10, '0'),
                                      7      /*Коэффициент гарантийного обеспечения*/
                                       ,
                                      :p_CalcDate)
                 AS KGO,
              RSHB_RSI_SCLIMIT.
               GetSumFutureComPrevious (
                 c.t_Client,
                 c.T_SFCONTRID,
                 NVL (
                    (SELECT accdoc.t_DepartmentID
                       FROM dmcaccdoc_dbt accdoc
                      WHERE     accdoc.t_Chapter = c.t_Chapter
                            AND accdoc.t_Account = c.t_Account
                            AND accdoc.t_Currency = c.t_Code_Currency
                            AND accdoc.t_ClientContrID > 0
                            AND ROWNUM = 1),
                    1),
                 :p_CalcDate,
                 :v_PrevWorkDate,
                 c.t_code_currency,
                 c.t_code_currency)
                 AS ComPrevious,
              DECODE (
                 NVL (rsi_rsb_kernel.
                       GetNote (207,               --Договор брокерского обслуживания
                                LPAD (dlc.t_DlContrID, 34, '0'),
                                1,
                                :v_PrevWorkDate),
                      '0'),
                 '00', 0,
                 rsb_struct.getMoney (rsi_rsb_kernel.
                                       GetNote (
                                         207,      --Договор брокерского обслуживания
                                         LPAD (dlc.t_DlContrID, 34, '0'),
                                         1,
                                         :v_PrevWorkDate)))
                 AS SUMGO,
              c.t_IsBlocked AS IsBlocked,
              0 AS DUE474,
              REPLACE (mp.t_firmid, 'SPBFUT') t_firmid                 --dan BIQ-7571
         FROM ddlcontr_dbt dlc,
              DDL_CLIENTINFO_DBT c,
              ddlcontrmp_dbt mp,
              dsfcontr_dbt sfcontr
        WHERE  c.t_calc_sid = ']'||g_calc_clientinfo||q'[' and c.T_SERVKIND = 15
              AND DLC.T_DLCONTRID = c.T_DLCONTRID
              AND mp.t_DlContrID = c.T_DLCONTRID
              AND mp.t_SfContrID = c.T_SFCONTRID
              AND SFCONTR.T_ID = c.T_SFCONTRID
              AND ((c.t_HasErrors = chr(0)) or (:ExcludeClients = 0)) ]';

         if p_UseListClients <> 0 then
            v_sql:=v_sql||'
                 AND EXISTS
                        (SELECT 1
                           FROM DDL_PANELCONTR_DBT
                          WHERE  t_calc_sid = '''||g_calc_panelcontr||''' and T_SETFLAG = CHR (88)
                                AND T_CLIENTID = sfcontr.t_partyid
                                AND T_DLCONTRID = dlc.t_DlContrID)';
         end if;
         v_sql:=v_sql||' ) q ';

   execute immediate v_sql
           BULK COLLECT INTO v_limfuturemark
           using p_CalcDate, RSB_SECUR.OBJTYPE_SFCONTR, p_CalcDate, p_CalcDate, v_PrevWorkDate,v_PrevWorkDate,v_PrevWorkDate,ExcludeErrClients;

      IF v_limfuturemark.COUNT > 0
      THEN
         FORALL indx IN v_limfuturemark.FIRST .. v_limfuturemark.LAST
            INSERT INTO DDL_LIMITFUTURMARK_DBT
                 VALUES v_limfuturemark (indx);
      END IF;
   END;                                             -- RSI_CreateFutureMarkLim

  PROCEDURE RSI_CheckCashStockLimits ( p_CalcDate        IN DATE) as
  BEGIN
    LockRecordsFrom('ddl_limitcashstock_dbt');
   if  substr(g_calc_DIRECT,1,1)    = GC_CALC_SID_DEFAULT then 
      EXECUTE IMMEDIATE 'ALTER TABLE DDL_LIMITCASHSTOCK_INT TRUNCATE PARTITION P99999999X';
   end if; 
 END;
  
  PROCEDURE RSI_ClearCashStockLimits (
      p_CalcDate        IN DATE,
      p_ByMarket       IN NUMBER,
      p_ByOutMarket  IN NUMBER,
      p_ByEDP           IN NUMBER,
      p_MarketCode IN VARCHAR,
      p_MarketID IN NUMBER,
      p_UseListClients IN NUMBER DEFAULT 0)
   AS
   v_market_kind ddl_limitcashstock_dbt.t_market_kind% type := CASE WHEN p_ByEDP = 1 THEN 'ЕДП' ELSE 'фондовый' END ;
   BEGIN
     -- DELETE FROM ddl_limitcashstock_dbt WHERE t_client_code is null or t_client_code = chr(1);
 
      IF p_ByOutMarket != 0
      THEN
         DELETE FROM ddl_limitcashstock_dbt WHERE t_Market = 0;
      END IF;

      IF p_ByMarket != 0 or p_ByEDP != 0
      THEN
         --удаляем все по параметрам запуска. Если расчет идет по выбранным клиентам, то убираем только их.
 
         IF p_UseListClients = 0 THEN
            commit;
            DELETE /*+ full(l) enable_parallel_dml parallel(l 4) */ FROM ddl_limitcashstock_dbt l
               WHERE t_Market = p_MarketID  AND  (t_market_kind = v_market_kind or t_client_code is null or t_client_code = chr(1)) ;
            commit; 
         ELSE
            commit;
            DELETE /*+  enable_parallel_dml parallel(l 4) */ FROM ddl_limitcashstock_dbt l
            WHERE (t_Market = p_MarketID
                 AND t_market_kind = v_market_kind and t_date <> p_CalcDate);
            commit;
            DELETE FROM ddl_limitcashstock_dbt
             WHERE t_id in ( select /*+ ordered*/ t_id FROM  ddl_clientinfo_dbt c,ddl_limitcashstock_dbt t
               WHERE c.t_calc_sid = g_calc_clientinfo and t.t_Market = p_MarketID
                 AND t_market_kind = v_market_kind
                 and t.t_internalaccount  = c.t_accountid
                 and c.t_marketid = p_MarketID );
         END IF;
      ELSE

         DELETE /*+ full(l) */ FROM ddl_limitcashstock_dbt WHERE t_client_code is null or t_client_code = chr(1);
      END IF;

   END;

   PROCEDURE RSI_CreateCashStockLimits (
      p_CalcDate        IN DATE,
      p_ByMarket       IN NUMBER,
      p_ByOutMarket  IN NUMBER,
      p_ByEDP           IN NUMBER,
      p_MarketCode IN VARCHAR,
      p_MarketID IN NUMBER,
      p_UseListClients IN NUMBER DEFAULT 0)
   AS
   BEGIN
     -- Изменения процедуры согласовывать с пакеном IT_LIMIT
      TimeStamp_ ('Старт расчета лимитов по денежным средствам '||p_MarketCode,p_CalcDate,NULL,SYSTIMESTAMP);
     commit;
      ts_ := SYSTIMESTAMP;
    --  DBMS_OUTPUT.put_line ('run' || mainsessionid);
      RSI_CreateLimitsKindParallel (
         'BEGIN
            rshb_rsi_sclimit.g_log_add         := '''||g_log_add||''';
            rshb_rsi_sclimit.g_calc_DIRECT     := '''||g_calc_DIRECT||''';
            rshb_rsi_sclimit.g_calc_clientinfo := '''||g_calc_clientinfo||''';
            rshb_rsi_sclimit.g_calc_panelcontr := '''||g_calc_panelcontr||''';
         rshb_rsi_sclimit.RSI_CreateCashStockLimByKind(:start_id, :end_id, TO_DATE('''
         || TO_CHAR (p_CalcDate, 'DD.MM.YYYY')
         || ''',''DD.MM.YYYY''), '
         || TO_CHAR (p_ByMarket)
         || ', '
         || TO_CHAR (p_ByOutMarket)
         || ','
         || TO_CHAR (p_ByEDP)
         || ','''
         || TO_CHAR (p_MarketCode)
         || ''', '
         || TO_CHAR (p_MarketID)
         || ' ); END;');
      TimeStamp_ ('Завершен расчет лимитов по денежным средствам',p_CalcDate,NULL,SYSTIMESTAMP);
      ts_ := SYSTIMESTAMP;

      IF savearch = 1
      THEN
         SaveArchMoney (p_CalcDate);
      END IF;
   END;                                           -- RSI_CreateCashStockLimits

   PROCEDURE RSI_DeleteCashStockLimByKindCur (p_CalcDate IN DATE)
   AS
   BEGIN

   DELETE FROM DDL_LIMITCASHSTOCK_dbt
      WHERE t_market_kind = 'валютный'
            AND t_client_code IN
                   (SELECT t_client_code
                      FROM DDL_LIMITCASHSTOCK_dbt t
                     WHERE t_market_kind = 'валютный'
                           AND NOT EXISTS
                                      (SELECT 1
                                         FROM DDL_LIMITCASHSTOCK_dbt
                                        WHERE t_client_code = t.t_client_code
                                              AND t_market_kind =
                                                     t.t_Market_kind
                                              AND t_open_balance <> 0));
   END;

  PROCEDURE RSI_CreateCashStockLimByKindCur_job(p_start_id      IN NUMBER,
                                                p_end_id        IN NUMBER,
                                                p_CalcDate      IN DATE,
                                                p_IsDepo        IN NUMBER,
                                                p_ByEDP         IN NUMBER
                                                ) AS
    v_Kind number;
  BEGIN
     IF p_start_id = 1 THEN
      v_Kind := 0;
    ELSIF p_start_id = 2 THEN
      v_Kind := 1;
    ELSIF p_start_id = 3 THEN
      v_Kind := 2;
    ELSE
      v_Kind := 365;
    END IF;
    RSI_CreateCashStockLimByKindCur(p_CalcDate, v_Kind, p_IsDepo, p_ByEDP);
  END;
  
  /*RSI_CreateCashStockLimByKindCur (p_CalcDate, 0, p_IsDepo, p_ByEDP);
  RSI_CreateCashStockLimByKindCur (p_CalcDate, 1, p_IsDepo, p_ByEDP);
  
  IF ((p_IsKind2 = 1) OR (p_ByEDP = 1))--по ЕДП считаем 4 вида лимита, т.к. их надо оъединять с четырями видами лимитов фондового рынка
  THEN                                -- формировать лимиты для kind 2,365
     RSI_CreateCashStockLimByKindCur (p_CalcDate, 2, p_IsDepo, p_ByEDP);
     RSI_CreateCashStockLimByKindCur (p_CalcDate, 365, p_IsDepo, p_ByEDP);
  END IF;*/

  PROCEDURE RSI_CreateCashStockLimByKindCurParallel(p_parallel IN NUMBER,
                                                    p_CalcDate IN DATE,
                                                    p_IsDepo   IN NUMBER,
                                                    p_ByEDP    IN NUMBER) as
  begin
    commit;
    RSI_CreateLimitsKindParallel('BEGIN
                                  rshb_rsi_sclimit.g_log_add         := '''||g_log_add||''';
                                  rshb_rsi_sclimit.g_calc_DIRECT     := '''||g_calc_DIRECT||''';
                                  rshb_rsi_sclimit.g_calc_clientinfo := '''||g_calc_clientinfo||''';
                                  rshb_rsi_sclimit.g_calc_panelcontr := '''||g_calc_panelcontr||''';
                                 rshb_rsi_sclimit.RSI_CreateCashStockLimByKindCur_job(:start_id, :end_id, TO_DATE(''' ||
                                 TO_CHAR(p_CalcDate, 'DD.MM.YYYY') ||
                                 ''',''DD.MM.YYYY''), ' ||
                                 TO_CHAR(p_IsDepo) || ',' ||
                                 TO_CHAR(p_ByEDP) || /*',' ||
                                 TO_CHAR(USERENV('sessionid')) || ',' ||
                                 TO_CHAR(mainsessionid) ||*/ ' ); END;',
                                 p_parallel);
  end;

   PROCEDURE RSI_ClearCashStockLimitsCur (p_CalcDate IN DATE, p_ByEDP IN NUMBER, p_UseListClients IN NUMBER DEFAULT 0)
   AS
   BEGIN
      --DELETE FROM ddl_limitcashstock_dbt WHERE LOWER (t_Market_Kind) = 'валютный' and (t_client_code is null or t_client_code = chr(1));
 
      DELETE  FROM ddl_limitcashstock_dbt l
      WHERE t_client_code is null or t_client_code = chr(1)
            OR (t_Market = GetMicexID()
           AND t_market_kind = CASE WHEN p_ByEDP = 1 THEN 'ЕДП' ELSE 'валютный' END and t_date <> p_CalcDate);

      IF p_ByEDP = 0 THEN -- по ЕДП нельзя удалять,т.к. лимиты по ЕДП по валютке не формируютс, а только дополняются. Они уже были удалены и сформированы при расчете ЕДП по фондовому рынку

        if p_UseListClients = 0 then
          DELETE   FROM ddl_limitcashstock_dbt l
                 WHERE t_Market_Kind =  'валютный' ;
        else
          DELETE  FROM ddl_limitcashstock_dbt l
                 WHERE t_Market_Kind =  'валютный'
                      and (t_client_code is null or t_client_code = chr(1)
                         or  t_internalaccount  in (select t_accountid
                                                     from ddl_clientinfo_dbt
                                                     where t_calc_sid = g_calc_clientinfo and t_marketid = GetMicexID() and t_ServKind = 21 and t_isEDP = CHR(0)));
        end if;
      else
        DELETE  FROM ddl_limitcashstock_dbt l
            WHERE t_Market_Kind = 'валютный' and (t_client_code is null or t_client_code = chr(1));
      END IF;
   END;
   
   PROCEDURE RSI_CreateCashStockLimitsCur (p_CalcDate IN DATE, p_IsKind2 IN NUMBER, p_IsDepo IN NUMBER, p_ByEDP IN NUMBER, p_UseListClients IN NUMBER DEFAULT 0)
   AS
   BEGIN

    RSI_CreateCashStockLimByKindCurParallel(case when(p_IsKind2 = 1) OR
                                            (p_ByEDP = 1) then 4 else 2 end,
                                            p_CalcDate,
                                            p_IsDepo,
                                            p_ByEDP);

     /* RSI_CreateCashStockLimByKindCur (p_CalcDate, 0, p_IsDepo, p_ByEDP);
      RSI_CreateCashStockLimByKindCur (p_CalcDate, 1, p_IsDepo, p_ByEDP);

      IF ((p_IsKind2 = 1) OR (p_ByEDP = 1))--по ЕДП считаем 4 вида лимита, т.к. их надо оъединять с четырями видами лимитов фондового рынка
      THEN                                -- формировать лимиты для kind 2,365
         RSI_CreateCashStockLimByKindCur (p_CalcDate, 2, p_IsDepo, p_ByEDP);
         RSI_CreateCashStockLimByKindCur (p_CalcDate, 365, p_IsDepo, p_ByEDP);
      END IF;*/
   END;

   PROCEDURE RSI_DeleteCashStockLimitsCur (p_CalcDate IN DATE, p_IsKind2 IN NUMBER, p_IsDepo IN NUMBER, p_ByEDP IN NUMBER, p_UseListClients IN NUMBER DEFAULT 0)
   AS
   BEGIN
      IF (p_ByEDP = 0) THEN -- по ЕДП не несёт смысла, удалять ничего не требуется
         RSI_DeleteCashStockLimByKindCur (p_CalcDate);
      END IF;
   END;




   PROCEDURE RSI_CreateSecurLimByKind (p_start_id        IN NUMBER,
                                       p_end_id          IN NUMBER,
                                       p_CalcDate        IN DATE,
                                       p_ByMarket        IN NUMBER,
                                       p_ByOutMarket     IN NUMBER,
                                       p_DepoAcc         IN VARCHAR2,
                                       p_MarketCode         IN VARCHAR2,
                                       p_MarketID     IN NUMBER
                                      )
   AS
      v_Time           DATE;
       v_Kind           NUMBER;
     p_count NUMBER:= -1;
     v_KindMarketCode NUMBER := 0;
     TYPE limsecur_t IS TABLE OF DDL_LIMITSECURITES_DBT%ROWTYPE INDEX BY BINARY_INTEGER;
      v_limsecur       limsecur_t;

      TYPE limsecur_int IS TABLE OF DDL_LIMITSECURITES_INT_TMP%ROWTYPE
                              INDEX BY BINARY_INTEGER;

      v_limsecur_int   limsecur_int;
   BEGIN
      v_Time := TO_DATE ('01-01-0001:' || TO_CHAR (SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');

       EXECUTE IMMEDIATE 'truncate table DDL_LIMITSECURITES_INT_TMP';


      IF p_start_id = 1
      THEN
         v_Kind := 0;
      ELSIF p_start_id = 2
      THEN
         v_Kind := 1;
      ELSIF p_start_id = 3
      THEN
         v_Kind := 2;
      ELSE
         v_Kind := 365;
      END IF;

      ts_ := SYSTIMESTAMP;

     v_KindMarketCode := GetKindMarketCodeOrNote(p_MarketId, 1, 0);

      SELECT                                        /*+ STAR_TRANSFORMATION */
             --T_ID
             0,                                                       --T_DATE
             p_CalcDate,                                              --T_TIME
             v_Time,                                                --T_MARKET
             q.Market,                                              --T_CLIENT
             q.t_Party,                                           --T_SECURITY
             q.t_FIID,
             --T_FIRM_ID
             q.FIRM_ID, --'MC0134700000', --'MC0038600000', 20181218 - kva - изменено до ХФ
             --T_SECCODE
             CAST (
                (CASE WHEN q.SecCode IS NULL THEN CHR (1) ELSE q.SecCode END) AS VARCHAR2 (35)),
             --T_CLIENT_CODE
             CAST (
                (CASE
                    WHEN q.ClientCode IS NULL THEN CHR (1)
                    ELSE q.ClientCode
                 END) AS VARCHAR2 (35)),
             --T_OPEN_BALANCE
             (q.SumQuantity + q.Plan_Plus_Deal - q.Plan_Minus_Deal),
             --T_OPEN_LIMIT
             q.Open_Limit,                                   --T_CURRENT_LIMIT
             q.Open_Limit,
             --T_TRDACCID
             CAST (
                (CASE
                    WHEN q.TrdAccID = chr(1) THEN p_DepoAcc --'L01+00000F00'
                    ELSE q.TrdAccID
                 END) AS VARCHAR2 (25)),
             --T_WA_POSITION_PRICE
             (CASE
                 WHEN (q.SumQuantity + q.Plan_Plus_Deal - q.Plan_Minus_Deal) =
                         0
                 THEN
                    0
                 ELSE
                    q.wa_position_price
              END)
                wa_position_price,
             --T_LIMIT_KIND
             v_Kind,                                              --T_QUANTITY
             q.SumQuantity,                                 --T_PLAN_PLUS_DEAL
             q.Plan_Plus_Deal,                             --T_PLAN_MINUS_DEAL
             q.Plan_Minus_Deal,
             --T_ISBLOCKED
             q.IsBlocked,
             'фондовый',
             q.t_ServKindSub,
             q.t_Contract,
             q.MoneyConsolidated
        BULK COLLECT INTO v_limsecur_int
        FROM (SELECT l.t_Sum AS SumQuantity,
                     -- l.SumCostRub,
                     0 AS SumCostRub,
                     l.t_Party,
                     l.t_Contract,
                     l.t_FIID,
                     c.t_ServKindSub,
                     c.t_marketid AS Market,
                     c.t_Firm_ID
                     /*case when c.t_Firm_ID <> chr(1) then c.t_Firm_ID -- DEF-62316
                     when c.t_isedp = chr(88) or c.t_client = UK_id then v_FIRM_ID_MB else v_FIRM_ID end*/ FIRM_ID,
                     0 AS Open_Limit,
                     --TO_CHAR(RSI_RSB_FIINSTR.FI_GetObjCodeOnDate(l.t_FIID, 9/*Финансовый инструмент*/, 11/*Код на ММВБ*/, p_CalcDate)) as SecCode,
                     (SELECT MAX (t_code)
                        FROM dobjcode_dbt
                       WHERE     t_objecttype = 9
                             AND t_codekind = v_KindMarketCode
                             AND t_objectid = l.t_fiid
                             AND t_state = 0)
                        AS SecCode,
                     case when c.t_marketid = GetMicexID() then c.t_mpcode
                       when c.t_isedp = chr(88) and c.t_marketid <>  GetMicexID() and c.t_stockcode is not null then c.t_stockcode
                       when c.t_isedp = chr(88) and c.t_marketid <>  GetMicexID() and c.t_stockcode is null then c.t_ekk
                        else c.t_mpcode end AS ClientCode,
                     c.t_TrdAccId
                        AS TrdAccID,
                     0 AS Plan_Plus_Deal,  -- заполняется в       UpdSumPlanAvrRQ (p_CalcDate, v_CheckDate);
                     0 AS Plan_Minus_Deal, -- заполняется в       UpdSumPlanAvrRQ (p_CalcDate, v_CheckDate);
                     c.t_IsBlocked
                        AS IsBlocked,
                     (CASE WHEN p_MarketID > 0 AND v_Kind IN (2, 365) THEN 0 --                            GetWAPositionPrice (
                                                                           --                               v_CheckDate,
                                                                           --                               l.t_Party,
                                                                           --                               l.t_Contract,
                                                                           --                               l.t_FIID,
                                                                           --                               mp.t_mpcode,
                                                                           --                               GetObjCodeOnDate (l.t_FIID,
                                                                           --                                                 9   /*Финансовый инструмент*/
                                                                           --                                                  ,
                                                                           --                                                 11            /*Код на ММВБ*/
                                                                           --                                                   ,
                                                                           --                                                 p_CalcDate
                                                                           --                                                ),
                                                                           --                               v_FIRM_ID, v_Kind)
                      ELSE 0 END) AS wa_position_price,
                      c.t_isedp as MoneyConsolidated,
                      c.t_Firm_ID
                FROM (select distinct t_servkind, t_legalform, t_sfstate, t_depoclosedate, t_deponumber, t_TrdAccID, t_sfcontrid, t_client ,t_mpcode,t_isblocked,t_ServKindSub, t_marketid, t_isedp, t_dlcontrid, t_ekk, t_stockcode, t_Firm_ID
                          from DDL_CLIENTINFO_DBT   t
                          where t.t_calc_sid = g_calc_clientinfo and  ( ExcludeErrClients = 0  or t_HasErrors = chr(0)) and t.t_servkind = 1 and t.t_marketid = p_MarketID
                         ---group by t_servkind, t_legalform, t_sfstate, t_depoclosedate, t_deponumber, t_trdaccid, t_sfcontrid, t_client,t_mpcode,t_isblocked,t_ServKindSub, t_marketid, t_isedp, t_dlcontrid, t_stockcode, t_ekk
                      )  c,
                     D_LIMITLOTS_TMP l
               WHERE  l.t_calc_sid = g_calc_clientinfo and c.t_SfcontrID = l.t_Contract -- and c.t_servkind = 1
             --        and case when p_ByEDP = 1 then chr(88) else chr(0) end = t_IsEDP
              -- and c.t_marketid = p_MarketID
              ) q;

      IF v_limsecur_int.COUNT > 0
      THEN
         FORALL indx IN v_limsecur_int.FIRST .. v_limsecur_int.LAST
            INSERT INTO DDL_LIMITSECURITES_INT_TMP
                 VALUES v_limsecur_int (indx);
      END IF;

     UpdSumPlanAvrRQ (p_CalcDate, p_MarketID, v_Kind);
      InsertLimitFromIntSecur;
--      TimeStamp_ (
--            'Расчет лимита Т'
--         || v_Kind
--         || ' DEPO по бумагам в наличии',
--         p_CalcDate,
--         ts_,
--         SYSTIMESTAMP,
--         p_RootSessionID,
--         p_start_id * 10 + 40);

      ts_ := SYSTIMESTAMP;

      --Выпуски, по которым нет остатка, но имеются неисполненные ТО

      EXECUTE IMMEDIATE 'truncate table DDL_LIMITSECURITES_INT_TMP';

      WITH l
              AS (SELECT q.Party,
                         q.Contract,
                         q.FIID,
                         GetSumPlanAvrRQ (q.Party,
                                          q.Contract,
                                          sfcontr.t_ServKindSub,
                                          p_CalcDate,
                                          GetCheckDate(v_Kind, p_CalcDate, SfcontrIsEDP(q.Contract), RSI_DlCalendars.DL_CALLNK_MARKETPLACE_SEC),
                                          q.FIID,
                                          1,
                                         p_MarketID
                                         )
                            AS Plan_Plus_Deal,
                         GetSumPlanAvrRQ (q.Party,
                                          q.Contract,
                                          sfcontr.t_ServKindSub,
                                          p_CalcDate,
                                          GetCheckDate(v_Kind, p_CalcDate, SfcontrIsEDP(q.Contract), RSI_DlCalendars.DL_CALLNK_MARKETPLACE_SEC),
                                          q.FIID,
                                          0,
                                          p_MarketID
                                         )
                            AS Plan_Minus_Deal
                    FROM (SELECT DISTINCT tk.t_ClientID AS Party, tk.t_ClientContrID AS Contract, rq.t_FIID AS FIID
                            FROM    ddlrq_dbt rq
                                 INNER JOIN
                                    dlimit_dltick_dbt tk
                                 ON  tk.t_calc_sid = g_calc_clientinfo and    tk.t_DealID = rq.t_DocID
                                    inner join (select t_sfcontrid, t_notexcluderepo from ddl_clientinfo_dbt where t_calc_sid = g_calc_clientinfo and t_servkind = 1 group by t_sfcontrid, t_notexcluderepo) c on (tk.t_clientcontrid = c.t_sfcontrid)
                                    AND tk.t_ClientID > 0
                                    AND tk.t_ClientContrID > 0
                                    AND tk.t_MarketID = p_MarketID
                                    AND tk.t_DealDate < p_CalcDate --bpv не учитываем еще не заключенные сделки
                                    AND ( (v_Kind <> 0) /*bpv т.о. на категории по сделке будум обращать внимание только по лимиту T0*/
                               OR  (p_CalcDate <> rq.t_PlanDate)
                                                                 OR ( (NVL (tk.t_tradechr, '0') != 'P') /*Режимы адресных торгов и РПС всегда начинаются с P - по ним исполнение в Т0 не учитываем, спецрепы здесь же*/
                                                                                                       AND (tk.t_specrepo != CHR (88)) /*спецрепо*/
                                                                                                                                      )
                                             /* 20/05/2019 Иногда Репо РПС в систему приходит как две сделки и переговорная сделка не с ЦК рассчитывается в квике, поэтому ее в исполнение в лимитах не учитываем*/
                                             OR (tk.t_dealtype NOT IN
                                                    (2122, 2127, 12122, 12127)
                                       AND tk.t_trademode NOT IN
                                              ('PSOB', 'PSEQ'))
                                   OR c.t_NotExcludeRepo = CHR (88))
                           WHERE rq.t_DocKind IN
                                    (RSB_SECUR.DL_SECURITYDOC,
                                     RSB_SECUR.DL_RETIREMENT,
                                     RSB_SECUR.DL_AVRWRT)
                                 AND rq.t_SubKind =
                                        RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                                 AND (rq.t_FactDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                                      OR rq.t_FactDate >= p_CalcDate
                                      OR NOT EXISTS
                                                (SELECT 1
                                                   FROM dpmwrtsum_dbt lot
                                                  WHERE lot.t_DocKind in (29, 135)
                                                        AND lot.t_State = 1
                                                        AND lot.t_DocID =
                                                               rq.t_ID))) q,
                         dsfcontr_dbt sfcontr
                   WHERE sfcontr.t_ID = q.Contract
                         AND sfcontr.t_servkindsub = 8
                         AND (sfcontr.t_dateclose = TO_DATE ('01010001', 'ddmmyyyy')
                              OR sfcontr.t_dateclose >= p_CalcDate)
                         AND NOT EXISTS
                                    (SELECT 1
                                       FROM D_LIMITLOTS_TMP lot
                                      WHERE  lot.t_calc_sid = g_calc_clientinfo and  lot.t_Party = q.Party
                                            AND lot.t_Contract = q.Contract
                                            AND lot.t_FIID = q.FIID)
           )
      SELECT                                                            --T_ID
            0,                                                        --T_DATE
             p_CalcDate,                                              --T_TIME
             v_Time,                                                --T_MARKET
             q.Market,                                              --T_CLIENT
             q.Party,                                             --T_SECURITY
             q.FIID,                                               --T_FIRM_ID
             FIRM_ID, --'MC0134700000', --'MC0038600000', 20181218 - kva - изменено до ХФ
             --T_SECCODE
             CAST (
                (CASE WHEN q.SecCode IS NULL THEN CHR (1) ELSE q.SecCode END) AS VARCHAR2 (35)),
             --T_CLIENT_CODE
             CAST (
                (CASE
                    WHEN q.ClientCode IS NULL THEN CHR (1)
                    ELSE q.ClientCode
                 END) AS VARCHAR2 (35)),
             --T_OPEN_BALANCE
             (q.SumQuantity + q.Plan_Plus_Deal - q.Plan_Minus_Deal),
             --T_OPEN_LIMIT
             q.Open_Limit,                                   --T_CURRENT_LIMIT
             q.Open_Limit,
             --T_TRDACCID
             CAST (
                (CASE
                    WHEN q.TrdAccID = chr(1) THEN p_DepoAcc /*'L01+00000F00'*/
                    ELSE q.TrdAccID
                 END) AS VARCHAR2 (25)),                 --T_WA_POSITION_PRICE
             (CASE
                 WHEN (q.SumQuantity + q.Plan_Plus_Deal - q.Plan_Minus_Deal) =
                         0
                 THEN
                    0
                 ELSE
                    q.wa_position_price
              END)
                wa_position_price,                              --T_LIMIT_KIND
             v_Kind,                                              --T_QUANTITY
             q.SumQuantity,                                 --T_PLAN_PLUS_DEAL
             q.Plan_Plus_Deal,                             --T_PLAN_MINUS_DEAL
             q.Plan_Minus_Deal,                                  --T_ISBLOCKED
             q.IsBlocked,
             'фондовый',
             q.t_ServKindSub,
             q.Contract,
             q.MoneyConsolidated
        BULK COLLECT INTO v_limsecur_int
        FROM (SELECT 0 AS SumQuantity,
                     0 AS SumCostRub,
                     l.Party,
                     l.Contract,
                     c.t_servkindsub,
                     l.FIID,
                     c.t_Marketid AS Market,
                     c.t_Firm_ID
                    /* case when c.t_Firm_ID <> chr(1) then c.t_Firm_ID -- DEF-62316
                     when c.t_isedp = chr(88) or c.t_client = UK_id then v_FIRM_ID_MB else v_FIRM_ID end*/ FIRM_ID,
                     0 AS Open_Limit,
                     --TO_CHAR(RSI_RSB_FIINSTR.FI_GetObjCodeOnDate(l.FIID, 9/*Финансовый инструмент*/, 11/*Код на ММВБ*/, p_CalcDate)) as SecCode,
                     (SELECT MAX (t_code)
                        FROM dobjcode_dbt
                       WHERE     t_objecttype = 9
                             AND t_codekind = v_KindMarketCode
                             AND t_objectid = l.fiid
                             AND t_state = 0)
                        AS SecCode,
                     case when c.t_marketid = GetMicexID() then c.t_mpcode
                       when c.t_isedp = chr(88) and c.t_marketid <>  GetMicexID() and c.t_stockcode is not null then c.t_stockcode
                       when c.t_isedp = chr(88) and c.t_marketid <>  GetMicexID() and c.t_stockcode is null then c.t_ekk
                        else c.t_mpcode end AS ClientCode,
                     c.t_Trdaccid
                        AS TrdAccID,
                     l.Plan_Plus_Deal,
                     l.Plan_Minus_Deal,
                     c.t_IsBlocked
                        AS IsBlocked,
                     (CASE WHEN p_MarketID > 0 AND v_Kind IN (2, 365) THEN 0 --                            GetWAPositionPrice (
                                                                           --                               v_CheckDate,
                                                                           --                               l.Party,
                                                                           --                               l.Contract,
                                                                           --                               l.FIID,
                                                                           --                               mp.t_mpcode,
                                                                           --                               GetObjCodeOnDate (l.FIID,
                                                                           --                                                 9   /*Финансовый инструмент*/
                                                                           --                                                  ,
                                                                           --                                                 11            /*Код на ММВБ*/
                                                                           --                                                   ,
                                                                           --                                                 p_CalcDate
                                                                           --                                                ),
                                                                           --                               v_FIRM_ID,v_Kind)
                      ELSE 0 END) AS wa_position_price,
                      c.t_isedp as MoneyConsolidated
                FROM (select t_servkind, t_legalform, t_sfstate, t_depoclosedate, t_deponumber, t_TrdAccID, t_sfcontrid, t_client ,t_mpcode,t_isblocked,t_ServKindSub, t_marketid, t_isedp, t_dlcontrid, t_ekk, t_stockcode, t_Firm_ID
                        from DDL_CLIENTINFO_DBT   t
                       where t_calc_sid = g_calc_clientinfo AND t_marketid = p_MarketID and t_servkind = 1 and  ((ExcludeErrClients = 0 ) or (t_HasErrors = chr(0)))
                    group by t_servkind, t_legalform, t_sfstate, t_depoclosedate, t_deponumber, t_trdaccid, t_sfcontrid, t_client,t_mpcode,t_isblocked,t_ServKindSub, t_marketid, t_isedp, t_dlcontrid, t_ekk, t_stockcode, t_Firm_ID)  c,
                     l
               WHERE     c.t_SfcontrID = l.Contract  
                     --and case when p_ByEDP = 1 then chr(88) else chr(0) end = t_IsEDP
                     -- AND (l.Plan_Plus_Deal > 0 OR l.Plan_Minus_Deal > 0) /*bpv нулевые лимиты надо отображать*/
                     ) q;

      IF v_limsecur_int.COUNT > 0
      THEN
         FORALL indx IN v_limsecur_int.FIRST .. v_limsecur_int.LAST
            INSERT INTO DDL_LIMITSECURITES_INT_TMP
                 VALUES v_limsecur_int (indx);
      END IF;

      UpdSumPlanAvrRQ (p_CalcDate, p_MarketID, v_Kind);
      InsertLimitFromIntSecur;

    /* TimeStamp_ (
            'Расчет лимита Т'
         || v_Kind
         || ' DEPO по бумагам в поставке',
         p_CalcDate,
         ts_,
         SYSTIMESTAMP,
         p_start_id * 10 + 41); */

      /*********/
     ts_ := SYSTIMESTAMP;

      IF (v_Kind = 0)
      THEN
   --   ts_ := SYSTIMESTAMP;

      --Выпуски, по которым нет остатка, но имеются неисполненные ТО по спецрепо.
      WITH l
              AS (SELECT q.Party,
                         q.Contract,
                         q.FIID
                    FROM (SELECT DISTINCT tk.t_ClientID AS Party, tk.t_ClientContrID AS Contract, rq.t_FIID AS FIID
                            FROM    ddlrq_dbt rq
                                 INNER JOIN
                                    dlimit_dltick_dbt tk
                                 ON tk.t_calc_sid = g_calc_clientinfo and   tk.t_DealID = rq.t_DocID
                                 inner join (select t_sfcontrid, t_notexcluderepo from ddl_clientinfo_dbt where t_calc_sid = g_calc_clientinfo and t_servkind = 1 group by t_sfcontrid, t_notexcluderepo) c on (tk.t_clientcontrid = c.t_sfcontrid)
                                    AND tk.t_ClientID > 0
                                    AND tk.t_ClientContrID > 0
                                    AND tk.t_MarketID = p_MarketID
                                    AND tk.t_DealDate < p_CalcDate --bpv не учитываем еще не заключенные сделки
                           AND (( (NVL (tk.t_tradechr, '0') = 'P') /*Режимы адресных торгов и РПС всегда начинаются с P - по ним исполнение в Т0 не учитываем, спецрепы здесь же*/
                                                                                                    AND (tk.t_specrepo = CHR (88)) /*спецрепо*/
                                                                                                                                   )
                                           /* 20/05/2019 Иногда Репо РПС в систему приходит как две сделки и переговорная сделка не с ЦК рассчитывается в квике, поэтому ее в исполнение в лимитах не учитываем*/
                                           AND (tk.t_dealtype IN
                                                   (2122,
                                                    2127,
                                                    12122,
                                                    12127)
                                                OR tk.t_trademode  IN
                                                      ('PSOB', 'PSEQ'))
                                         OR t_NotExcludeRepo = CHR (0))
                           WHERE rq.t_DocKind IN
                                    (RSB_SECUR.DL_SECURITYDOC,
                                     RSB_SECUR.DL_RETIREMENT,
                                     RSB_SECUR.DL_AVRWRT)
                                 AND rq.t_SubKind =
                                        RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                                 AND (rq.t_FactDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                                      OR rq.t_FactDate >= p_CalcDate
                                      OR NOT EXISTS
                                                (SELECT 1
                                                   FROM dpmwrtsum_dbt lot
                                                  WHERE lot.t_DocKind in (29, 135)
                                                        AND lot.t_State = 1
                                                        AND lot.t_DocID =
                                                               rq.t_ID))) q,
                         dsfcontr_dbt sfcontr
                   WHERE sfcontr.t_ID = q.Contract
                         AND sfcontr.t_servkindsub = 8
                      AND (sfcontr.t_dateclose = TO_DATE ('01010001', 'ddmmyyyy')
                           OR sfcontr.t_dateclose >= p_CalcDate)
                         AND NOT EXISTS
                                    (SELECT 1
                                    FROM D_LIMITLOTS_TMP lot
                                      WHERE lot.t_calc_sid = g_calc_clientinfo and  lot.t_Party = q.Party
                                         AND lot.t_Contract = q.Contract
                                         AND lot.t_FIID = q.FIID)
        )
      SELECT                                                         --T_ID
            0,                                                     --T_DATE
             p_CalcDate,                                           --T_TIME
             v_Time,                                             --T_MARKET
             q.Market,                                           --T_CLIENT
             q.Party,                                          --T_SECURITY
             q.FIID,                                            --T_FIRM_ID
             FIRM_ID,--'MC0134700000', --'MC0038600000', 20181218 - kva - изменено до ХФ
             --T_SECCODE
             CAST (
                (CASE
                    WHEN q.SecCode IS NULL THEN CHR (1)
                    ELSE q.SecCode
                 END) AS VARCHAR2 (35)),
             --T_CLIENT_CODE
             CAST (
                (CASE
                    WHEN q.ClientCode IS NULL THEN CHR (1)
                    ELSE q.ClientCode
                 END) AS VARCHAR2 (35)),
             --T_OPEN_BALANCE
             0,
             --T_OPEN_LIMIT
             q.Open_Limit,                                --T_CURRENT_LIMIT
             q.Open_Limit,
             --T_TRDACCID
             CAST (
                (CASE
                    WHEN q.TrdAccID = chr(1) THEN p_depoAcc /*'L01+00000F00'*/
                    ELSE q.TrdAccID
                 END) AS VARCHAR2 (25)),              --T_WA_POSITION_PRICE
             0 wa_position_price,                            --T_LIMIT_KIND
             v_Kind,                                           --T_QUANTITY
             q.SumQuantity,                              --T_PLAN_PLUS_DEAL
             q.Plan_Plus_Deal,                          --T_PLAN_MINUS_DEAL
             q.Plan_Minus_Deal,                               --T_ISBLOCKED
             q.IsBlocked,
            'фондовый',
            q.MoneyConsolidated
        BULK COLLECT INTO v_limsecur
        FROM (SELECT
                    0 AS SumQuantity,                       /*CHVA 516066*/
                     0 AS SumCostRub,
                     l.Party,
                     l.Contract,
                     l.FIID,
                     c.t_marketid AS Market,
                     c.t_Firm_ID
                     /*case when c.t_Firm_ID <> chr(1) then c.t_Firm_ID -- DEF-62316
                     when c.t_isedp = chr(88) or c.t_client = UK_id then v_FIRM_ID_MB else v_FIRM_ID end*/ FIRM_ID,
                     0 AS Open_Limit,
                     --TO_CHAR(RSI_RSB_FIINSTR.FI_GetObjCodeOnDate(l.FIID, 9/*Финансовый инструмент*/, 11/*Код на ММВБ*/, p_CalcDate)) as SecCode,
                     (SELECT MAX (t_code)
                        FROM dobjcode_dbt
                       WHERE     t_objecttype = 9
                             AND t_codekind = v_KindMarketCode
                             AND t_objectid = l.fiid
                             AND t_state = 0)
                        AS SecCode,
                                          case when c.t_marketid = GetMicexID() then c.t_mpcode
                    when c.t_isedp = chr(88) and c.t_marketid <>  GetMicexID() and c.t_stockcode is not null then c.t_stockcode
                    when c.t_isedp = chr(88) and c.t_marketid <>  GetMicexID() and c.t_stockcode is null then c.t_ekk
                     else c.t_mpcode end AS ClientCode,
                     c.t_TrdAccID
                        AS TrdAccID,
                     0 as Plan_Plus_Deal,
                     0 as Plan_Minus_Deal,
                     c.t_IsBlocked
                        AS IsBlocked,
                     0 AS wa_position_price,
                     c.t_isedp as MoneyConsolidated
                FROM (select t_servkind, t_legalform, t_sfstate, t_depoclosedate, t_deponumber, t_TrdAccID, t_sfcontrid, t_client ,t_mpcode,t_isblocked,t_ServKindSub, t_marketid, t_isedp, t_dlcontrid, t_ekk, t_stockcode, t_Firm_ID
                       from DDL_CLIENTINFO_DBT   t
                      where t.t_calc_sid = g_calc_clientinfo  and t_servkind = 1  AND t_marketid = p_MarketID  and  ((ExcludeErrClients = 0 ) or (t_HasErrors = chr(0)))
                      group by t_servkind, t_legalform, t_sfstate, t_depoclosedate, t_deponumber, t_trdaccid, t_sfcontrid, t_client,t_mpcode,t_isblocked,t_ServKindSub, t_marketid, t_isedp, t_dlcontrid, t_ekk, t_stockcode, t_Firm_ID)  c,
                     l
               WHERE     c.t_SfcontrID = l.Contract  
                       --and case when p_ByEDP = 1 then chr(88) else chr(0) end = t_IsEDP                      
                     -- AND (l.Plan_Plus_Deal > 0 OR l.Plan_Minus_Deal > 0) /*bpv нулевые лимиты надо отображать*/
                     ) q
       WHERE NOT EXISTS
                    (SELECT 1
                       FROM DDL_LIMITSECURITES_DBT
                      WHERE     t_limit_kind = v_Kind
                            AND t_security = q.FIID
                            AND t_Market = q.Market
                            AND t_client_code = q.ClientCode);

--      TimeStamp_ (
--         'Расчет по бумагам в поставке. Лимит Т'
--         || v_Kind,
--         p_CalcDate,
--         ts_,
--         SYSTIMESTAMP,
--         p_RootSessionID);


      IF v_limsecur.COUNT > 0
      THEN
         FORALL indx IN v_limsecur.FIRST .. v_limsecur.LAST
            INSERT INTO DDL_LIMITSECURITES_DBT
                 VALUES v_limsecur (indx);
      END IF;
      END IF;

      /****************/

         TimeStamp_ ('Расчет лимита DEPO Т'|| v_Kind,p_CalcDate,ts_,SYSTIMESTAMP,p_start_id * 10 + 40);

   END;                                            -- RSI_CreateSecurLimByKind


   PROCEDURE RSI_CreateSecurLimGAZP (p_start_id      IN NUMBER,
                                     --p_end_id          IN NUMBER,
                                     p_CalcDate      IN DATE,
                                     p_ByMarket      IN NUMBER,
                                     p_ByOutMarket   IN NUMBER,
                             --        p_ByEDP   IN NUMBER,
                                     p_DepoAcc         IN VARCHAR2,
                                     p_MarketCode         IN VARCHAR2,
                                     p_MarketID     IN NUMBER
                                    )
   AS
      v_CheckDate   DATE;
      v_Time        DATE;

      v_Kind        NUMBER;
      v_ScZeroLimit VARCHAR2(12) := chr(1);
      v_FIID NUMBER(5) := -1;
      TYPE limsecur_t IS TABLE OF DDL_LIMITSECURITES_DBT%ROWTYPE
                            INDEX BY BINARY_INTEGER;

      v_limsecur    limsecur_t;
   BEGIN
      v_Time := TO_DATE ('01-01-0001:' || TO_CHAR (SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');

      IF p_start_id = 1
      THEN
         v_Kind := 0;
      ELSIF p_start_id = 2
      THEN
         v_Kind := 1;
      ELSIF p_start_id = 3
      THEN
         v_Kind := 2;
      ELSE
         v_Kind := 365;
      END IF;

      ts_ := SYSTIMESTAMP;

      v_ScZeroLimit := GetCodeSCZeroLimit(RSI_SCLIMIT.MARKET_KIND_STOCK,p_MarketID,1);
     BEGIN
       select t_objectid INTO v_FIID from dobjcode_dbt where t_codekind = GetKindMarketCodeOrNote(p_MarketId, 1, 0)
          AND t_objecttype = 9 and t_state = 0 and rownum = 1 and t_code = v_ScZeroLimit;
     EXCEPTION
       WHEN no_data_found THEN  v_FIID := -1;
     END;

   /*BPV оставшиеся договора, по которым никакого остатка нет, нужно устанавливать нулевой лимит по GAZP*/
   /*upd 160519
   по клиентам ФЛ, лимит по цб должен попадать в файл с лимитами только при наличии статуса на договоре "Обработка зачвершена"
   и отсутствием даты закрытия договора депо на договоре
   По клиентам юридическим лицам нулевой лимит формируется  только при наличии статуса на договоре "Обработка завершена" 
   (категория № 101 <Статус договора> на ДБО) И при наличии информации об открытом договоре депо 
   (заполнено примечание № 102 <Номер договора Депо> на ДБО). Если номер договора ДЕПО не указан, то проверяем указанный 
   в примечании счет соответствующего субдоговора фондового рынка  
   (Для фондового рынка МБ Примечание № 5 "Счет клиента на ММВБ Фондовый сектор",
    для фондового рынка СПБ Примечание № 10 <Счет клиента на СПБ Фондовый сектор> )
     и если счет начинается с символа "Y", то нулевой лимит формируем.   */

     INSERT INTO DDL_LIMITSECURITES_DBT
      SELECT                                                            --T_ID
            0,                                                        --T_DATE
             p_CalcDate,                                              --T_TIME
             v_Time,                                                --T_MARKET
             q.Market,                                              --T_CLIENT
             q.Party,                                             --T_SECURITY
             q.FIID,                                               --T_FIRM_ID
             q.FIRM_ID,--'MC0134700000', --'MC0038600000', 20181218 - kva - изменено до ХФ
             --T_SECCODE
             CAST (
                (CASE WHEN q.SecCode IS NULL THEN CHR (1) ELSE q.SecCode END) AS VARCHAR2 (35)),
             --T_CLIENT_CODE
             CAST (
                (CASE
                    WHEN q.ClientCode IS NULL THEN CHR (1)
                    ELSE q.ClientCode
                 END) AS VARCHAR2 (35)),
             --T_OPEN_BALANCE
             0,
             --T_OPEN_LIMIT
             0,                                   --T_CURRENT_LIMIT
             0,
             --T_TRDACCID
             CAST (
                (CASE
                    WHEN q.TrdAccID = chr(1)  THEN p_DepoAcc --'L01+00000F00'
                    ELSE q.TrdAccID
                 END) AS VARCHAR2 (25)),                 --T_WA_POSITION_PRICE
             0,                                                 --T_LIMIT_KIND
             v_Kind,                                              --T_QUANTITY
             q.SumQuantity,                                 --T_PLAN_PLUS_DEAL
             q.Plan_Plus_Deal,                             --T_PLAN_MINUS_DEAL
             q.Plan_Minus_Deal,                                  --T_ISBLOCKED
             q.IsBlocked,
             'фондовый',
             q.MoneyConsolidated
      --  BULK COLLECT INTO v_limsecur
        FROM (SELECT /*+ index(s UDDL_LIMITSECURITES_DBT_IDX2 )*/ 0 AS SumQuantity,
                     0 AS SumCostRub,
                     c.t_client Party,
                     c.t_sfcontrid Contract,
                     v_FIID AS FIID,                                    --l.FIID,
                     c.t_MarketID AS Market,
                     c.t_Firm_ID
                     /*case when c.t_Firm_ID <> chr(1) then c.t_Firm_ID -- DEF-62316
                     when c.t_isedp = chr(88) or c.t_client = UK_id then v_FIRM_ID_MB else v_FIRM_ID end*/ FIRM_ID,
                     0 AS Open_Limit,
                     v_ScZeroLimit AS SecCode,
                     case when c.t_marketid = GetMicexID() then c.t_mpcode
                       when c.t_isedp = chr(88) and c.t_marketid <>  GetMicexID() and c.t_stockcode is not null then c.t_stockcode
                       when c.t_isedp = chr(88) and c.t_marketid <>  GetMicexID() and c.t_stockcode is null then c.t_ekk
                        else c.t_mpcode end AS ClientCode,
                     c.t_TrdAccId AS TrdAccID,
                     0 AS Plan_Plus_Deal,                  --T_PLAN_MINUS_DEAL
                     0 AS Plan_Minus_Deal,                       --T_ISBLOCKED
                     c.t_IsBlocked
                        AS IsBlocked,
                        c.t_isedp as MoneyConsolidated
                FROM  (select /*+ index(t DDL_CLIENTINFO_DBT_IDX1) */ t_servkind, t_legalform, t_sfstate, t_depoclosedate, t_deponumber, t_TrdAccID, t_sfcontrid, t_client ,t_mpcode,t_isblocked,t_ServKindSub, t_marketid, t_isedp, t_dlcontrid, t_ekk, t_stockcode, t_Firm_ID
                         from DDL_CLIENTINFO_DBT   t
                        where t.t_calc_sid = g_calc_clientinfo and t.t_Servkind = 1  AND t.t_MarketID = p_MarketID and  ((ExcludeErrClients = 0 ) or (t_HasErrors = chr(0)))
                         and t.t_sfstate = 3  /* Статус Обработка завершена*/
                         AND (( t.t_legalform = 2
                                AND t.t_depoclosedate IS NULL) /*дата закрытия договора депо*/
                            or ( t.t_legalform = 1
                               and ( t.t_DepoNumber IS NOT NULL
                                     OR substr(t.t_TrdAccID,1,1) = 'Y')))
                          and t.t_isblocked != chr(88) /*RSB_SECUR.GetGeneralMainObjAttr (207,LPAD (t.t_DlContrID, 34, '0'),1,p_CalcDate) <> 1*/
                        group by t_servkind, t_legalform, t_sfstate, t_depoclosedate, t_deponumber, t_trdaccid, t_sfcontrid, t_client,t_mpcode,t_isblocked,t_ServKindSub, t_marketid, t_isedp, t_dlcontrid, t_ekk, t_stockcode, t_Firm_ID)  c
                 LEFT JOIN
                  DDL_LIMITSECURITES_DBT s
               ON (    s.T_LIMIT_KIND = v_Kind
                   AND s.T_CLIENT_CODE =
                   case when c.t_marketid = GetMicexID() then c.t_mpcode
                       when c.t_isedp = chr(88) and c.t_marketid <>  GetMicexID() and c.t_stockcode is not null then c.t_stockcode
                       when c.t_isedp = chr(88) and c.t_marketid <>  GetMicexID() and c.t_stockcode is null then c.t_ekk
                        else c.t_mpcode
                         end
                   AND s.T_MARKET = c.t_marketid
                   AND s.T_ISBLOCKED <> CHR(88)
                   AND s.T_SECCODE is not null
                   AND s.t_seccode <> CHR (0)
                   AND s.t_seccode <> CHR (1)
                   AND s.t_market_kind = 'фондовый')
               WHERE  --   c.t_Servkind = 1
                     --AND case when p_ByEDP = 1 then chr(88) else chr(0) end = c.t_IsEDP
                    -- AND c.t_MarketID = p_MarketID
                     /*AND ( (c.t_legalform = 1)
                          OR (c.t_sfstate = 3  \* Статус Обработка завершена*\
                              AND c.t_depoclosedate IS NULL)) \*дата закрытия договора депо*\
                     AND (   (c.t_legalform = 2)
                          OR c.t_DepoNumber IS NOT NULL
                          OR c.t_TrdAccID <> chr(1))*/
                      s.t_id is null --  отбираем записи, у котороых нет лимитов DEPO
                     ) q;

      /*IF v_limsecur.COUNT > 0
      THEN
         FORALL indx IN v_limsecur.FIRST .. v_limsecur.LAST
            INSERT INTO DDL_LIMITSECURITES_DBT
                 VALUES v_limsecur (indx);
      END IF;*/
      TimeStamp_ ('  Расчет нулевых лимитов по фондовому рынку '||p_MarketCode||'. Кол-во: ' ||v_limsecur.COUNT,
                  p_CalcDate,
                  NULL,
                  SYSTIMESTAMP
                 );

   END;                                              -- RSI_CreateSecurLimGAZP



   PROCEDURE RSI_CreateSecurLimByKindCur (p_CalcDate IN DATE, p_Kind IN NUMBER, p_DepoAcc IN VARCHAR2)
   AS
      v_CheckDate   DATE;
      v_Time        DATE;
      v_FIRM_ID     VARCHAR2 (12) := CHR (1);
      v_FIRM_ID_STOCK     VARCHAR2 (12) := CHR (1);
      TYPE limsecur_t IS TABLE OF DDL_LIMITSECURITES_DBT%ROWTYPE
                            INDEX BY BINARY_INTEGER;

      v_limsecur    limsecur_t;
   BEGIN
      v_Time := TO_DATE ('01-01-0001:' || TO_CHAR (SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');
      ts_ := SYSTIMESTAMP;

 
      --пока считаем, что всё на ММВБ и разделения на биржи нет
      v_FIRM_ID := GetFIRM_ID (GetMicexID(), MARKET_KIND_CURR,1);
      v_FIRM_ID_STOCK := GetFIRM_ID (GetMicexID(), MARKET_KIND_STOCK,1);

      SELECT                                                            --T_ID
            0,
             --T_DATE
             p_CalcDate,
             --T_TIME
             v_Time,
             --T_MARKET
             1                                                  /*MarketCode*/
              ,
             --T_CLIENT
             q.t_Client,
             --T_SECURITY
             -1,                                   -- не заполняется -- или 0?
             --T_FIRM_ID
             case when RegVal_EDPStartDate <> '00.00.0000' and to_date(RegVal_EDPStartDate,'DD.MM.YYYY')<=p_CalcDate then v_FIRM_ID_STOCK else v_FIRM_ID end,
             --T_SECCODE
             NVL (q.t_CCY, q.t_code_currency) t_ccy,
             --T_CLIENT_CODE
             CAST (
                (CASE
                    WHEN q.Client_code IS NULL THEN CHR (1)
                    ELSE q.Client_code
                 END) AS VARCHAR2 (35)),
             --T_OPEN_BALANCE
             (q.SumQuantity + q.Plan_Plus_Deal - q.Plan_Minus_Deal),
             --T_OPEN_LIMIT
             q.Open_Limit,
             --T_CURRENT_LIMIT
             q.Open_Limit,
             --T_TRDACCID
             CAST (
                (CASE
                    WHEN q.TrdAccID = chr(1) THEN p_DepoAcc
                    ELSE q.TrdAccID
                 END) AS VARCHAR2 (25)),
             --T_WA_POSITION_PRICE
             0,
             --T_LIMIT_KIND
             p_Kind,
             --T_QUANTITY
             q.SumQuantity,
             --T_PLAN_PLUS_DEAL
             q.Plan_Plus_Deal,
             --T_PLAN_MINUS_DEAL
             q.Plan_Minus_Deal,
             --T_ISBLOCKED
             q.IsBlocked,
             --T_MARKET_KIND
             'валютный',
             q.MoneyConsolidated
        BULK COLLECT INTO v_limsecur
        FROM (SELECT                                             /*+ ORDERED*/
                    DISTINCT
                     acc.t_AccountID,
                     acc.t_Client,
                     acc.t_Account,
                     acc.t_Code_Currency,
                     ABS (rsb_account.restac (acc.t_Account,
                                              acc.t_Code_Currency,
                                              p_CalcDate - 1,
                                              acc.t_Chapter,
                                              NULL
                                             ))  AS SumQuantity,     -- Money306
                     1 AS ByMarket,
                     sfcontr.t_ServKindSub,
                     (SELECT t_code
                        FROM dobjcode_dbt
                       WHERE     t_objecttype = 9
                             AND t_codekind = 105
                             AND t_state = 0
                             AND t_objectid = curr.t_fiid)
                        t_CCY,
                     dlc.t_LeverageCur,
                     0 AS Open_Limit,
                     0 AS Due474,
                     GetSumPlanCashCM (acc.t_Client,
                                       sfcontr.t_ID,
                                       p_CalcDate,
                                       GetCheckDate(p_Kind, p_CalcDate, SfcontrIsEDP(sfcontr.t_ID), RSI_DlCalendars.DL_CALLNK_MARKETPLACE_CUR),
                                       acc.t_Account,
                                       acc.t_Code_Currency,
                                       1
                                      )
                        AS Plan_Plus_Deal,
                     GetSumPlanCashCM (acc.t_Client,
                                       sfcontr.t_ID,
                                       p_CalcDate,
                                       GetCheckDate(p_Kind, p_CalcDate, SfcontrIsEDP(sfcontr.t_ID), RSI_DlCalendars.DL_CALLNK_MARKETPLACE_CUR),
                                       acc.t_Account,
                                       acc.t_Code_Currency,
                                       0
                                      )
                        AS Plan_Minus_Deal,
                     0 AS ComPrevious,                               -- пока 0
                     (CASE
                         WHEN RSB_SECUR.
                               GetGeneralMainObjAttr (
                                 207      /*Договор брокерского обслуживания*/
                                    ,
                                 LPAD (dlc.t_DlContrID, 34, '0'),
                                 1                      /*Признак блокировки*/
                                  ,
                                 p_CalcDate) = 1 THEN 'X'
                         ELSE CHR (0)
                      END)
                        AS IsBlocked,
                     mp.t_mpcode AS Client_code,
                     rsb_struct.getString (rsi_rsb_kernel.
                                            GetNote (
                                              RSB_SECUR.OBJTYPE_SFCONTR,
                                              LPAD (sfcontr.t_ID, 10, '0'),
                                              8 /*Счет клиента на ММВБ Валютный сектор*/
                                               ,
                                              p_CalcDate))
                        AS TrdAccID,
                        chr(0) as MoneyConsolidated
                FROM daccount_dbt acc,
                     dmcaccdoc_dbt accdoc,
                     dsfcontr_dbt sfcontr,
                     dfininstr_dbt curr,
                     ddlcontrmp_dbt mp,
                     ddlcontr_dbt dlc
               WHERE     acc.t_Chapter = 1
                     AND acc.t_Account LIKE '306%'
                     AND acc.t_Client NOT IN (SELECT d.t_PartyID
                                                FROM ddp_dep_dbt d)
                     AND acc.t_Open_Date < p_CalcDate
                     AND (acc.t_Close_Date = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                          OR acc.t_Close_Date >= p_CalcDate)             --!!!
                     AND accdoc.t_Chapter = acc.t_Chapter
                     AND accdoc.t_Account = acc.t_Account
                     AND accdoc.t_Currency = acc.t_Code_Currency
                     AND accdoc.t_ClientContrID > 0
                     AND sfcontr.t_ID = accdoc.t_ClientContrID
                     AND sfcontr.t_ServKind = 21                    --Валютный
                     AND (sfcontr.t_DateClose = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                          OR sfcontr.t_DateClose >= p_CalcDate)
                     AND curr.t_FIID = acc.t_Code_Currency
                     AND curr.t_FI_Kind = 1
                     AND mp.t_SfContrID = sfcontr.t_ID
                     AND dlc.t_DlContrID = mp.t_DlContrID
                     AND curr.t_FIID <> RSI_RSB_FIInstr.NATCUR) q;


      IF v_limsecur.COUNT > 0
      THEN
         FORALL indx IN v_limsecur.FIRST .. v_limsecur.LAST
            INSERT INTO DDL_LIMITSECURITES_DBT
                 VALUES v_limsecur (indx);
      END IF;

      /*TOM*/

      SELECT                                                            --T_ID
            0,
             --T_DATE
             p_CalcDate,
             --T_TIME
             v_Time,
             --T_MARKET
             1                                                  /*MarketCode*/
              ,
             --T_CLIENT
             q.t_Client,
             --T_SECURITY
             -1,                                   -- не заполняется -- или 0?
             --T_FIRM_ID
             v_FIRM_ID,
             --T_SECCODE
             NVL (q.t_CCY, q.t_code_currency) t_ccy,
             --T_CLIENT_CODE
             CAST (
                (CASE
                    WHEN q.Client_code IS NULL THEN CHR (1)
                    ELSE q.Client_code
                 END) AS VARCHAR2 (35)),
             --T_OPEN_BALANCE
             --(q.SumQuantity + q.Plan_Plus_Deal - q.Plan_Minus_Deal),
             0,
             --T_OPEN_LIMIT
             q.Open_Limit,
             --T_CURRENT_LIMIT
             q.Open_Limit,
             --T_TRDACCID
             CAST (
                (CASE
                    WHEN q.TrdAccID = chr(1) THEN p_DepoAcc
                    ELSE q.TrdAccID
                 END) AS VARCHAR2 (25)),
             --T_WA_POSITION_PRICE
             0,
             --T_LIMIT_KIND
             p_Kind,
             --T_QUANTITY
             q.SumQuantity,
             --T_PLAN_PLUS_DEAL
             q.Plan_Plus_Deal,
             --T_PLAN_MINUS_DEAL
             q.Plan_Minus_Deal,
             --T_ISBLOCKED
             q.IsBlocked,
             --T_MARKET_KIND
              'валютный',
              q.MoneyConsolidated
        BULK COLLECT INTO v_limsecur
        FROM (SELECT DISTINCT
                     acc.t_AccountID,
                     acc.t_Client,
                     acc.t_Account,
                     acc.t_Code_Currency,
                     ABS (rsb_account.restac (acc.t_Account,
                                              acc.t_Code_Currency,
                                              p_CalcDate - 1,
                                              acc.t_Chapter,
                                              NULL
                                             ))
                        AS SumQuantity,                            -- Money306
                     1 AS ByMarket,
                     sfcontr.t_ServKindSub,
                     (SELECT t_code
                        FROM dobjcode_dbt
                       WHERE     t_objecttype = 9
                             AND t_codekind = 106
                             AND t_state = 0
                             AND t_objectid = curr.t_fiid)
                        t_CCY,
                     dlc.t_LeverageCur,
                     0 AS Open_Limit,
                     0 AS Due474,
                     0 AS Plan_Plus_Deal,
                     0 AS Plan_Minus_Deal,
                     0 AS ComPrevious,                               -- пока 0
                     (CASE
                         WHEN RSB_SECUR.
                               GetGeneralMainObjAttr (
                                 207      /*Договор брокерского обслуживания*/
                                    ,
                                 LPAD (dlc.t_DlContrID, 34, '0'),
                                 1                      /*Признак блокировки*/
                                  ,
                                 p_CalcDate) = 1 THEN 'X'
                         ELSE CHR (0)
                      END)
                        AS IsBlocked,
                     mp.t_mpcode AS Client_code,
                     rsb_struct.getString (rsi_rsb_kernel.
                                            GetNote (
                                              RSB_SECUR.OBJTYPE_SFCONTR,
                                              LPAD (sfcontr.t_ID, 10, '0'),
                                              8 /*Счет клиента на ММВБ Валютный сектор*/
                                               ,
                                              p_CalcDate))
                        AS TrdAccID,
                        chr(0) as MoneyConsolidated
                FROM daccount_dbt acc,
                     dmcaccdoc_dbt accdoc,
                     dsfcontr_dbt sfcontr,
                     dfininstr_dbt curr,
                     ddlcontrmp_dbt mp,
                     ddlcontr_dbt dlc
               WHERE     acc.t_Chapter = 1
                     AND acc.t_Account LIKE '306%'
                     AND acc.t_Client NOT IN (SELECT d.t_PartyID
                                                FROM ddp_dep_dbt d)
                     AND acc.t_Open_Date < p_CalcDate
                     AND (acc.t_Close_Date = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                          OR acc.t_Close_Date >= p_CalcDate)             --!!!
                     AND accdoc.t_Chapter = acc.t_Chapter
                     AND accdoc.t_Account = acc.t_Account
                     AND accdoc.t_Currency = acc.t_Code_Currency
                     AND accdoc.t_ClientContrID > 0
                     AND sfcontr.t_ID = accdoc.t_ClientContrID
                     AND sfcontr.t_ServKind = 21                    --Валютный
                     AND (sfcontr.t_DateClose = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                          OR sfcontr.t_DateClose >= p_CalcDate)
                     AND curr.t_FIID = acc.t_Code_Currency
                     AND curr.t_FI_Kind = 1
                     AND mp.t_SfContrID = sfcontr.t_ID
                     AND dlc.t_DlContrID = mp.t_DlContrID
                     AND curr.t_FIID <> RSI_RSB_FIInstr.NATCUR) q;

      TimeStamp_ ('Расчет по валютному рынку',
                  p_CalcDate,
                  NULL,
                  SYSTIMESTAMP,
                  898
                 );

      IF v_limsecur.COUNT > 0
      THEN
         FORALL indx IN v_limsecur.FIRST .. v_limsecur.LAST
            INSERT INTO DDL_LIMITSECURITES_DBT
                 VALUES v_limsecur (indx);
      END IF;
   END;                                         -- RSI_CreateSecurLimByKindCur

 
   PROCEDURE RSI_CreateSecurLimByKindCurZero (p_CalcDate IN DATE, p_Kind IN NUMBER, p_DepoAcc IN VARCHAR2, p_MarketID IN INTEGER, p_MarketCode IN VARCHAR2, p_ByEDP IN NUMBER, p_UseListClients IN NUMBER DEFAULT 0)
   AS
      v_Time        DATE;
      v_FIRM_ID     VARCHAR2 (12) := CHR (1);
      v_FIRM_ID_STOCK     VARCHAR2 (12) := CHR (1);
      v_CodeSCZeroLimit VARCHAR2 (12) := CHR(1);

      TYPE limsecur_t IS TABLE OF DDL_LIMITSECURITES_DBT%ROWTYPE
                            INDEX BY BINARY_INTEGER;

      v_limsecur    limsecur_t;
   BEGIN
      v_Time := TO_DATE ('01-01-0001:' || TO_CHAR (SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');

      --пока считаем, что всё на ММВБ и разделения на биржи нет
      v_FIRM_ID := GetFIRM_ID (p_MarketID, MARKET_KIND_CURR,1);
      v_FIRM_ID_STOCK := GetFIRM_ID (GetMicexID(), MARKET_KIND_STOCK,1);

      v_CodeSCZeroLimit := GetCodeScZeroLimit(3, p_MarketID, 1); 		-- DEF-68258, определение параметра перенесено в функцию

      DELETE FROM DDL_LIMITSECURITES_DBT t
            WHERE t_market = p_marketID and t_market_kind = 'валютный' and (t_Client_code is null or t_client_code = chr(1)
                or ( t_moneyConsolidated = case when p_ByEdp = 1 then chr(88) else chr(0) end and t_date <> p_calcdate));
      
      IF p_UseListClients = 1 THEN
        DELETE FROM ddl_limitsecurites_Dbt
          WHERE T_ID IN ( SELECT s.t_id
                            FROM ddl_limitsecurites_Dbt s,
                                (select code
                                   from (select case when p_ByEDP = 0 then t_mpcode else t_stockcode end code
                                           from ddl_clientinfo_dbt
                                          where t_calc_sid = g_calc_clientinfo
                                            and t_marketid = p_MarketID
                                            and t_servkind = 21)
                                 group by code) c
                           WHERE s.t_client_code = c.code
                             AND s.t_Market = p_MarketID
                             AND s.t_market_kind = 'валютный'
                             and s.t_moneyConsolidated = case when p_ByEdp = 1 then chr(88) else chr(0) end );
      ELSE
        DELETE FROM ddl_limitsecurites_dbt
         WHERE t_Market = p_MarketID and t_market_kind = 'валютный' and t_moneyConsolidated = case when p_ByEdp = 1 then chr(88) else chr(0) end ;
      END IF;

      ts_ := SYSTIMESTAMP;

      SELECT 0,
             p_CalcDate, --T_DATE
             v_Time,     --T_TIME
             q.Market,   --T_MARKET
             q.t_Client, --T_CLIENT
             -1,         --T_SECURITY -- не заполняется -- или 0?
             case        --T_FIRM_ID
               when q.t_firm_id <> chr(1) then q.t_firm_id     -- DEF-62316, берем из ddl_clientinfo_dbt, если он там есть
               when RegVal_EDPStartDate <> '00.00.0000' and to_date(RegVal_EDPStartDate,'DD.MM.YYYY')<=p_CalcDate then  v_FIRM_ID_STOCK else v_FIRM_ID end
             ,coalesce ( v_CodeSCZeroLimit, q.t_CCY, to_char(q.t_code_currency) ) t_ccy     -- T_SECCODE -- DEF-68258, применяется параметр из ddl_limitprm_dbt
             ,CAST (
                (CASE
                    WHEN q.Client_code IS NULL THEN CHR (1)
                    ELSE CASE WHEN t_isEdp = chr(88) THEN q.Client_code_stock ELSE q.Client_code END
                 END) AS VARCHAR2 (35)), --T_CLIENT_CODE
             0, --T_OPEN_BALANCE --(q.SumQuantity + q.Plan_Plus_Deal - q.Plan_Minus_Deal),
             q.Open_Limit, --T_OPEN_LIMIT
             q.Open_Limit, --T_CURRENT_LIMIT
             CAST (
                (CASE
                    WHEN q.TrdAccID is null THEN p_DepoAcc
                    ELSE q.TrdAccID
                 END) AS VARCHAR2 (25)), --T_TRDACCID
             0,                 --T_WA_POSITION_PRICE
             p_Kind,            --T_LIMIT_KIND
             q.SumQuantity,     --T_QUANTITY
             q.Plan_Plus_Deal,  --T_PLAN_PLUS_DEAL
             q.Plan_Minus_Deal, --T_PLAN_MINUS_DEAL
             q.IsBlocked,       --T_ISBLOCKED
             'валютный',        --T_MARKET_KIND
             q.MoneyConsolidated
        BULK COLLECT INTO v_limsecur
        FROM (SELECT DISTINCT
                     c.t_AccountID,
                     c.t_Client,
                     c.t_Account,
                     c.t_Code_Currency,
                     0 /*ABS (rsb_account.restac (acc.t_Account,
                                              acc.t_Code_Currency,
                                              p_CalcDate - 1,
                                              acc.t_Chapter,
                                              NULL
                                             ))*/
                      AS SumQuantity,                              -- Money306
                     c.t_MarketID AS Market,
                     c.t_ServKindSub,
                     (SELECT t_code
                        FROM dobjcode_dbt
                       WHERE     t_objecttype = 9
                             AND t_codekind = 106
                             AND t_state = 0
                             AND t_objectid = c.t_Code_Currency)
                        t_CCY,
                     dlc.t_LeverageCur,
                     0 AS Open_Limit,
                     0 AS Due474,
                     0 AS Plan_Plus_Deal,
                     0 AS Plan_Minus_Deal,
                     0 AS ComPrevious,                               -- пока 0
                     c.t_IsBlocked
                        AS IsBlocked,
                     c.t_mpcode AS Client_code
                     ,coalesce(c.t_stockcode, c.t_ekk) AS Client_code_stock  -- DEF-63448, берем из ddl_clientinfo_dbt, а не из ddlobjcode_dbt
                                                                             -- DEF-68441, если stockcode не заполнен, подставляем t_ekk
                     ,case when c.t_trdaccid <> chr(1) then c.t_trdaccid -- DEF-62316, берем из ddl_clientinfo_dbt, если он там есть
                        else rsb_struct.getString (rsi_rsb_kernel.
                                                    GetNote (
                                                      RSB_SECUR.OBJTYPE_SFCONTR,
                                                      LPAD (c.t_sfcontrID, 10, '0'),
                                                      8 /*Счет клиента на ММВБ Валютный сектор*/
                                                       ,
                                                      p_CalcDate)) end AS TrdAccID,
                     c.t_isEdp,
                     C.t_isedp as moneyConsolidated,
                     c.t_firm_id AS t_firm_id -- DEF-62316
                FROM
                 (select 
                     t_firm_id, t_sfcontrid, t_dlcontrid, t_client, t_accountid, t_account, t_Money306, t_code_currency
                     , t_leverage, t_mpcode, t_trdaccid, t_servkind, t_servkindsub, t_isblocked, t_isedp, t_marketid
                     , t_stockcode -- DEF-63448, Краткий код на связанной фондовой секции биржи ММВБ по ДБО 
                     , t_ekk -- DEF-68441, используется, если t_stockcode не заполнен
                     from ddl_clientinfo_dbt  
                     where t_calc_sid = g_calc_clientinfo and  ((ExcludeErrClients = 0 ) or (t_HasErrors = chr(0))) 
                     and t_servkind = 21 and t_servkindsub = 8 and t_marketid = p_marketid 
                     group by 
                       t_firm_id, t_sfcontrid, t_dlcontrid, t_client, t_accountid, t_account, t_Money306, t_code_currency
                       , t_leverage, t_mpcode, t_trdaccid, t_servkind,t_servkindsub, t_isblocked, t_isedp, t_marketid
                       , t_stockcode, t_ekk
                 ) c,
                     ddlcontr_dbt dlc
               WHERE
                      CASE WHEN p_ByEDP = 1 THEN CHR(88) ELSE CHR(0) END = c.t_IsEDP
                     AND dlc.t_dlcontrid = c.t_dlcontrid
                     AND  c.t_Code_Currency = 7 ) q ;

      TimeStamp_ ('  Расчет нулевых лимитов по валютному рынку. Кол-во: ' ||v_limsecur.COUNT,
                  p_CalcDate,
                  NULL,
                  SYSTIMESTAMP);

      IF v_limsecur.COUNT > 0
      THEN
         FORALL indx IN v_limsecur.FIRST .. v_limsecur.LAST
            INSERT INTO DDL_LIMITSECURITES_DBT
                 VALUES v_limsecur (indx);
      END IF;
   END;                                     -- RSI_CreateSecurLimByKindCurZero

   PROCEDURE RSI_CheckSecurLimits(p_CalcDate        IN DATE) 
   AS
   BEGIN
     LockRecordsFrom('DDL_LIMITSECURITES_DBT');
     commit;
   END;

   PROCEDURE RSI_ClearSecurLimits (p_CalcDate        IN DATE,
                                    p_ByMarket        IN NUMBER,
                                    p_ByOutMarket     IN NUMBER,
           --                         p_ByEDP           IN NUMBER,
                                    p_DepoAcc         IN VARCHAR2,
                                    p_MarketCode      IN VARCHAR2,
                                    p_MarketID        IN NUMBER,
                                    p_UseListClients IN NUMBER DEFAULT 0
                                   )
   AS
   BEGIN
       TimeStamp_ ('Расчет лимитов DEPO фондовый рынок '||p_MarketCode,p_CalcDate,NULL,SYSTIMESTAMP,  105);

       --DELETE FROM ddl_limitsecurites_dbt WHERE t_market_kind = 'фондовый' and (t_client_code is null or t_client_code = chr(1));

      IF p_ByMarket != 0
      THEN
          IF p_UseListClients = 1 THEN

            DELETE FROM ddl_limitsecurites_dbt
               WHERE t_market_kind = 'фондовый'  and t_Market in ( p_MarketID,0) 
                     and  (t_date <> p_CalcDate OR t_client_code is null or t_client_code = chr(1));
                 --AND t_moneyconsolidated = CASE WHEN p_ByEDP = 1 THEN CHR(88) ELSE CHR(0) END

             DELETE FROM ddl_limitsecurites_Dbt WHERE T_ID IN ( SELECT s.t_id
               FROM ddl_limitsecurites_Dbt s,
                    (select code from (select case when t_marketid = GetMicexID() then t_mpcode else t_stockcode end code from ddl_clientinfo_dbt where t_calc_sid = g_calc_clientinfo and t_marketid = p_MarketID and t_servkind = 1) group by code) c
                WHERE     s.t_client_code = c.code
                AND s.t_Market = p_MarketID
               AND s.t_market_kind = 'фондовый');
          ELSE

            DELETE FROM ddl_limitsecurites_dbt
               WHERE t_Market = p_MarketID and t_market_kind = 'фондовый';
                 --AND t_moneyconsolidated = CASE WHEN p_ByEDP = 1 THEN CHR(88) ELSE CHR(0) END
--                 AND ( ( p_UseListClients = 0) or
--                            (t_client_code  in (select case when t_marketid = MarketID then t_mpcode else t_stockcode end from ddl_clientinfo_dbt where t_marketid = p_MarketID /*and t_isEDP = CASE WHEN p_ByEDP = 1 THEN chr(88) ELSE CHR(0) END */) ) ) ;
 --and t_client_code not in  (select c.t_mpcode from DDL_CLIENTINFO_DBT c where t_servkind = 1 and t_Market = p_MarketID and  case when p_ByEdp = 1 then chr(0) else chr(88) end = t_IsEDP);
          END IF;
      else

        DELETE FROM ddl_limitsecurites_dbt WHERE t_market_kind = 'фондовый' and (t_client_code is null or t_client_code = chr(1));
      END IF;
      /*IF p_ByOutMarket != 0  THEN
         DELETE FROM ddl_limitsecurites_dbt t
               WHERE t_Market = 0 AND  t_market_kind = 'фондовый';
      END IF;*/

   END;
   
   PROCEDURE RSI_CreateSecurLimits (p_CalcDate        IN DATE,
                                    p_ByMarket        IN NUMBER,
                                    p_ByOutMarket     IN NUMBER,
           --                         p_ByEDP           IN NUMBER,
                                    p_DepoAcc         IN VARCHAR2,
                                    p_MarketCode      IN VARCHAR2,
                                    p_MarketID        IN NUMBER,
                                    p_UseListClients IN NUMBER DEFAULT 0
                                   )
   AS
      v_Cursor           SYS_REFCURSOR;
      v_marketCode       DOBJCODE_DBT.T_CODE%TYPE;
      v_isinAndName      VARCHAR2(1000);

      TYPE secCodeRec IS RECORD
      (
         T_ISIN       DAVOIRISS_DBT.T_ISIN%TYPE,
         T_MARKET     DDL_LIMITSECURITES_DBT.T_MARKET%TYPE,
         T_CODE       VARCHAR2 (1000),
         T_SECURITY   DDL_LIMITSECURITES_DBT.T_SECURITY%TYPE,
         T_NAME       DFININSTR_DBT.T_NAME%TYPE,
         T_IS_ERROR   CHAR
      );
      TYPE secCodeArr IS TABLE OF secCodeRec;
      v_secCodeArr       secCodeArr;

   BEGIN
      -- Изменения процедуры согласовывать с пакеном IT_LIMIT


      --DELETE FROM ddl_limitsecurites_dbt WHERE t_market_kind = 'фондовый' and (t_client_code is null or t_client_code = chr(1));

      TimeStamp_ (
         'Старт расчета лимитов по ценным бумагам',
         p_CalcDate,
         NULL,
         SYSTIMESTAMP);
      ts_ := SYSTIMESTAMP;

      RSI_CreateLimitsKindParallel (
         'BEGIN 
            rshb_rsi_sclimit.g_log_add         := '''||g_log_add||''';
            rshb_rsi_sclimit.g_calc_DIRECT     := '''||g_calc_DIRECT||''';
            rshb_rsi_sclimit.g_calc_clientinfo := '''||g_calc_clientinfo||''';
            rshb_rsi_sclimit.g_calc_panelcontr := '''||g_calc_panelcontr||''';
         rshb_rsi_sclimit.RSI_CreateSecurLimByKind(:start_id, :end_id, TO_DATE('''
         || TO_CHAR (p_CalcDate, 'DD.MM.YYYY')
         || ''',''DD.MM.YYYY''), '
         || TO_CHAR (p_ByMarket)
         || ', '
         || TO_CHAR (p_ByOutMarket)
         || ', '''
         --|| TO_CHAR (p_ByEDP)
         --|| ', '
         || TO_CHAR (p_depoacc)
         || ''', '''
         || TO_CHAR (p_MarketCode)
         || ''', '
         || TO_CHAR (p_MarketID)
         ||/* ', '
         || TO_CHAR (USERENV ('sessionid'))
         || ', '
         || TO_CHAR (mainsessionid)
         || */' ); END;');

      Gather_Table_Stats('DDL_LIMITSECURITES_DBT');

      TimeStamp_ (
         'Завершен расчет лимитов по ценным бумагам',
         p_CalcDate,
         NULL,
         SYSTIMESTAMP );


      --         RSI_CreateSecurLimByKind(1,1,p_CalcDate,p_ByMarket,p_ByOutMarket,USERENV ('sessionid'),mainsessionid);
      --         RSI_CreateSecurLimByKind(2,2,p_CalcDate,p_ByMarket,p_ByOutMarket,USERENV ('sessionid'),mainsessionid);
      --         RSI_CreateSecurLimByKind(3,3,p_CalcDate,p_ByMarket,p_ByOutMarket,USERENV ('sessionid'),mainsessionid);
      --         RSI_CreateSecurLimByKind(365,365,p_CalcDate,p_ByMarket,p_ByOutMarket,USERENV ('sessionid'),mainsessionid);

  END;

/*
    что тут происходит:
    - блокировка заблокированных ЦБ и счетов
      установка T_ISBLOCKED = CHR (88)

    - удаляются строки по неподходящим договорам
      DELETE FROM DDL_LIMITSECURITES_DBT
    
    - ищутся актуальные бумаги с незаполненым seccode
      и определяется seccode через таблицу sofr_info_instruments - это некая справочная таблица, которая заполняется механизмом загрузки из рудаты
      
    - в цикле по одной бумаге:
      - если код не найден - пишется в лог
      - если код найден:
        - инсертится этот код в DOBJCODE_DBT
        - обновляется код в DDL_LIMITSECURITES_DBT по t_security, если в таблице он не заполнен
    
    - собирается основная информация по бумагам
      но уже с условием WHERE q1.t_MarketCode = q1.t_RuDataCode
    
    - в цикле по одной бумаге:
      - пишется в лог "неверный код в RU DATA" (почему???)
  
    - запускается сохранение лимитов в архив
  */
   PROCEDURE RSI_LOCKSecurLimits (p_CalcDate        IN DATE,
                                    p_ByMarket        IN NUMBER,
                                    p_ByOutMarket     IN NUMBER,
                                    p_DepoAcc         IN VARCHAR2,
                                    p_MarketCode      IN VARCHAR2,
                                    p_MarketID        IN NUMBER,
                                    p_UseListClients IN NUMBER DEFAULT 0
                                   )
   AS
      v_Cursor           SYS_REFCURSOR;
      v_marketCode       DOBJCODE_DBT.T_CODE%TYPE;
      v_isinAndName      VARCHAR2(1000);

      TYPE secCodeRec IS RECORD
      (
         T_ISIN       DAVOIRISS_DBT.T_ISIN%TYPE,
         T_MARKET     DDL_LIMITSECURITES_DBT.T_MARKET%TYPE,
         T_CODE       VARCHAR2 (1000),
         T_SECURITY   DDL_LIMITSECURITES_DBT.T_SECURITY%TYPE,
         T_NAME       DFININSTR_DBT.T_NAME%TYPE,
         T_IS_ERROR   CHAR
      );
      TYPE secCodeArr IS TABLE OF secCodeRec;
      v_secCodeArr       secCodeArr;
      v_select varchar2(32000);
   BEGIN


      TimeStamp_ (
         'Блокировка заблокированных ЦБ и счетов',
         p_CalcDate,
         NULL,
         SYSTIMESTAMP );


     --блокировка заблокированных ЦБ и счетов
       UPDATE DDL_LIMITSECURITES_DBT
        SET T_ISBLOCKED = CHR (88)
      WHERE t_id IN
               (select /*+ ordered */ l.t_id
                  from DOBJATCOR_DBT ATTR
                  join DDL_LIMITSECURITES_DBT l
                    on ( L.T_SECURITY = to_number(ATTR.T_OBJECT))
                 where ATTR.T_OBJECTTYPE = 12
                   and ATTR.T_GROUPID = 129
                   and p_CalcDate between ATTR.T_VALIDFROMDATE and ATTR.T_VALIDTODATE
                  AND l.t_market = p_MarketID
                   and l.t_date = p_CalcDate 
                union
                select /*+ ordered */ l.t_id
                  from (select to_number(substr(ATTR.T_OBJECT, 1, 2), 'FM0x') T_CHAPTER
                              ,to_number(substr(ATTR.T_OBJECT, 3, 7), 'FM0xxxxxx') T_CODE_CURRENCY
                              ,substr(ATTR.T_OBJECT, 10) T_ACCOUNT
                          from DOBJATCOR_DBT ATTR
                         where ATTR.T_OBJECTTYPE = 4
                           and ATTR.T_GROUPID = 102
                           and p_CalcDate between ATTR.T_VALIDFROMDATE and ATTR.T_VALIDTODATE) ATTR
                  join DACCOUNT_DBT A
                    on a.t_code_currency = ATTR.T_CODE_CURRENCY
                   and a.t_chapter = ATTR.t_chapter
                   and a.t_account = ATTR.t_account
                  join dmcaccdoc_dbt mc 
                    on  mc.t_account = a.t_account
                       AND mc.t_currency = a.t_code_currency
                       AND mc.t_chapter = a.t_chapter
                       AND mc.t_iscommon = CHR (88)
                  join ddlcontrmp_dbt mp
                    on  MC.T_CLIENTCONTRID = mp.t_sfcontrid
                  join DDL_LIMITSECURITES_DBT L
                    on L.T_SECURITY = A.T_CODE_CURRENCY
                      and L.T_CLIENT = A.T_CLIENT
                      AND mp.t_mpcode = l.t_client_code
                 where ATTR.T_CHAPTER = 22
                   and A.T_BALANCE = '61' 
                   AND mp.t_marketid = p_MarketID
                   AND l.t_market = p_MarketID
                   and l.t_date = p_CalcDate);

      TimeStamp_ (
         'Старт RSI_CreateSecurLimGAZP',
         p_CalcDate,
         NULL,
         SYSTIMESTAMP);

         RSI_CreateSecurLimGAZP (1,
                              p_CalcDate,
                              p_ByMarket,
                              p_ByOutMarket,
                              p_DepoAcc,
                              p_MarketCode,
                              p_MarketID
                             );

   --bpv по договорам, по которыместь дата закрытия договора депо и дата закрытия меньше либо равна дате арсчета, то лимиты по ценным бумагам по таким договорам не выгружаем.
   -- удалить лишние значительно быстрее, чем добавлять дополнительное условие в селекты

      DELETE FROM DDL_LIMITSECURITES_DBT
         WHERE t_market_kind = 'фондовый'
               AND t_market = p_MarketID
               AND t_client_code IN (SELECT DISTINCT T_MPCODE
                                       FROM DDL_CLIENTINFO_DBT t 
                                       where t.t_calc_sid = g_calc_clientinfo and  T.T_DEPOCLOSEDATE <=p_calcdate);

      v_select := q'[SELECT q1.t_isin,
                q1.t_market,
                case when q1.t_market = RSHB_RSI_SCLIMIT.GetMicexID () 
                       then rudata_read.get_seccode_mmvb(p_isin => q1.t_isin)
                     when q1.t_market = RSHB_RSI_SCLIMIT.GetSpbexID ()
                       then rudata_read.get_seccode_spb(p_isin => q1.t_isin)
                end as t_code,
                q1.T_SECURITY,
                q1.T_NAME,
                CASE
                   WHEN EXISTS
                           (SELECT 1
                              FROM DRATEDEF_DBT rate
                             WHERE     RATE.T_MARKET_PLACE = q1.t_market
                                   AND rate.t_type = 1
                                   AND RATE.t_otherfi = q1.T_SECURITY
                                   AND RATE.T_SINCEDATE <= RSHB_RSI_SCLIMIT.
                                                            GetCheckDateByParams (
                                                              -1,
                                                              q1.t_date,
                                                              q1.t_market,
                                                              0)) THEN CHR (88)
                   ELSE CHR (0)
                END
                   AS t_is_error
           FROM (SELECT DISTINCT
                        av.t_isin,
                        sec.T_MARKET,
                        sec.T_SECURITY,
                        fin.T_NAME,
                        sec.t_date
                   FROM DDL_LIMITSECURITES_DBT sec,
                        dfininstr_dbt fin,
                        davoiriss_dbt av
                  WHERE (   sec.t_seccode IS NULL
                         OR sec.t_seccode = CHR (0)
                         OR sec.t_seccode = CHR (1))
                         AND sec.t_date = :p_CalcDate
                         and sec.t_market = :p_MarketID
                          ]';
     if p_UseListClients != 0 then
        v_select := v_select|| ' and sec.t_client IN (select T_CLIENTID from DDL_PANELCONTR_DBT where t_calc_sid = '''||g_calc_panelcontr||''' and T_SETFLAG = chr(88))
                         ' ;
     end if;
      v_select := v_select|| ' AND fin.t_fiid = sec.T_SECURITY
                         AND av.t_fiid = fin.t_fiid
                         AND fin.T_FI_KIND = :p_FI_Kind) q1 ' ;
      
      OPEN v_Cursor FOR v_select
               USING p_CalcDate,
               p_MarketID,
               RSI_RSB_FIInstr.FIKIND_AVOIRISS;

      v_marketCode := objcode_read.get_code(p_object_type => RSB_SECUR.OBJTYPE_PARTY,
                                            p_code_kind   => cnst.PTCK_CONTR,
                                            p_object_id   => p_MarketID);

      LOOP
         FETCH v_Cursor
         BULK COLLECT INTO v_secCodeArr
         LIMIT 1000;

         FOR indx IN 1 .. v_secCodeArr.COUNT
         LOOP
            v_isinAndName := v_secCodeArr (indx).t_isin
                          || CASE WHEN TRIM(v_secCodeArr (indx).t_isin) is not null THEN ' ' ELSE '' END
                          || v_secCodeArr (indx).t_name;

            IF    v_secCodeArr (indx).t_code IS NULL
               OR v_secCodeArr (indx).t_code = CHR (0)
               OR v_secCodeArr (indx).t_code = CHR (1)
            THEN
               IF v_secCodeArr (indx).T_IS_ERROR = CHR(88) THEN
                  TimeStamp_ (
                        'Ошибка! По торгуемому выпуску '
                     || v_isinAndName
                     || ' в RU DATA не найден код выпуска на '
                     || v_marketCode,
                     p_CalcDate,
                     NULL,
                     SYSTIMESTAMP,
                     LOGACTION_ERR_NO_CODE_FOR_SEC
                     ,all_log_=> true);
               ELSE
                  TimeStamp_ (
                        'Предупреждение! По выпуску '
                     || v_isinAndName
                     || ' в RU DATA не найден код выпуска на '
                     || v_marketCode,
                     p_CalcDate,
                     NULL,
                     SYSTIMESTAMP,
                     LOGACTION_WARN_RUDATA_NOTFOUND
                     ,all_log_=> true);
               END IF;
            ELSE
               BEGIN
                  objcode_utils.insert_code(p_object_type => Rsb_Secur.OBJTYPE_FININSTR,
                                            p_code_kind   => RSHB_RSI_SCLIMIT.GetKindMarketCodeOrNote(p_MarketId, 1, 0),
                                            p_object_id   => v_secCodeArr (indx).t_security,
                                            p_code        => v_secCodeArr (indx).t_code,
                                            p_date        => RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate, -1));

                  UPDATE DDL_LIMITSECURITES_DBT
                     SET t_seccode = v_secCodeArr (indx).t_code
                   WHERE t_date = p_CalcDate
                     AND t_market = p_MarketID
                     AND (   p_UseListClients = 0
                          OR (T_CLIENT IN (select T_CLIENTID from DDL_PANELCONTR_DBT where t_calc_sid = g_calc_panelcontr and T_SETFLAG = chr(88))))
                     AND T_SECURITY = v_secCodeArr (indx).t_security
                     AND (   t_seccode IS NULL
                          OR t_seccode = CHR (0)
                          OR t_seccode = CHR (1));
                  TimeStamp_ (
                        'По выпуску '
                     || v_isinAndName
                     || ' на бирже ' || v_marketCode || ' добавлен код '
                     || v_secCodeArr (indx).t_code,
                     p_CalcDate,
                     NULL,
                     SYSTIMESTAMP,
                     LOGACTION_INFO_RUDATA_ADDED
                     ,all_log_=> true);
               EXCEPTION
                  WHEN DUP_VAL_ON_INDEX
                  THEN
                     TimeStamp_ (
                            'Ошибка добавления кода ' || v_secCodeArr (indx).t_code
                        || ' по выпуску ' || v_isinAndName
                        || ' на бирже ' || v_marketCode || '. Бумага с таким биржевым кодом уже существует в системе.',
                        p_CalcDate,
                        NULL,
                        SYSTIMESTAMP,
                        LOGACTION_ERR_RUDATA_DUPL
                        ,all_log_=> true);
                  WHEN OTHERS
                  THEN
                     TimeStamp_ (
                           '!Error InsertAvrObjCode  fiid='
                        || v_secCodeArr (indx).t_security
                        || ' '
                        || DBMS_UTILITY.Format_Error_Stack
                        || ' '
                        || DBMS_UTILITY.Format_Error_Backtrace,
                        p_CalcDate,
                        NULL,
                        SYSTIMESTAMP,
                        LOGACTION_ERR_RUDATA_OTHER
                        , excepsqlcode_ => 100
                        ,all_log_=> true);
               END;
            END IF;
         END LOOP;

         EXIT WHEN v_Cursor%NOTFOUND;
      END LOOP;

      CLOSE v_Cursor;

      v_select := q'[ SELECT DISTINCT q1.t_isin,
                         q1.t_market,
                         q1.t_MarketCode as t_code,
                         q1.T_SECURITY,
                         q1.t_name,
                         q1.t_is_error
           FROM (SELECT  av.t_isin,
                        LIMSEC.T_MARKET,
                        limsec.T_SECURITY,
                        fin.t_name,
                        CHR (88) AS t_is_error,
                        objcode_read.get_code_on_date(p_object_type => :p_ObjType,
                                                      p_code_kind   => RSHB_RSI_SCLIMIT.GetKindMarketCodeOrNote(limsec.T_MARKET, 1, 0),
                                                      p_object_id   => limsec.T_SECURITY,
                                                      p_date        => LIMSEC.T_DATE) AS t_MarketCode,
                        objcode_read.get_code_on_date(p_object_type => :p_ObjType,
                                                      p_code_kind   => 104,
                                                      p_object_id   => limsec.T_SECURITY,
                                                      p_date        => LIMSEC.T_DATE) AS t_RuDataCode
                   FROM ddl_limitsecurites_dbt limsec,
                        davoiriss_dbt av,
                        dfininstr_dbt fin
                  WHERE     t_market_kind = :p_mark_kind
                        AND av.T_FIID = limsec.T_SECURITY
                        AND limsec.t_date = :p_CalcDate
                        and limsec.t_market = :p_MarketID 
                        ]';
      if p_UseListClients != 0 then
        v_select := v_select|| ' and limsec.t_client IN (select T_CLIENTID from DDL_PANELCONTR_DBT where t_calc_sid = '''||g_calc_panelcontr||''' and T_SETFLAG = chr(88))
                         ' ;
      end if;
      v_select := v_select||' AND fin.t_fiid = av.T_FIID
                        AND fin.T_FI_KIND = :p_FI_Kind) q1
          WHERE q1.t_MarketCode = q1.t_RuDataCode ';

      OPEN v_Cursor FOR v_select 
         USING
               Rsb_Secur.OBJTYPE_FININSTR,
               Rsb_Secur.OBJTYPE_FININSTR,
               'фондовый',
               p_CalcDate,
               p_MarketID,
               RSI_RSB_FIInstr.FIKIND_AVOIRISS;

      LOOP
         FETCH v_Cursor
         BULK COLLECT INTO v_secCodeArr
         LIMIT 1000;

         FOR indx IN 1 .. v_secCodeArr.COUNT
         LOOP
            v_isinAndName := v_secCodeArr (indx).t_isin
                          || CASE WHEN TRIM(v_secCodeArr (indx).t_isin) is not null THEN ' ' ELSE '' END
                          || v_secCodeArr (indx).t_name;

            TimeStamp_ (
                  'Ошибка! По выпуску '
               || v_isinAndName
               || ' на '
               || v_marketCode
               || ' неверный код в RU DATA: '
               || v_secCodeArr (indx).t_code ,
               p_CalcDate,
               NULL,
               SYSTIMESTAMP,
               LOGACTION_ERR_RUDATA_INCORRECT
               ,all_log_=> true);
         END LOOP;

         EXIT WHEN v_Cursor%NOTFOUND;
      END LOOP;

      CLOSE v_Cursor;

      ts_ := SYSTIMESTAMP;

      IF savearch = 1
      THEN
         SaveArchSecur (p_CalcDate);
      END IF;
   END;

   -- Вставка в DDL_LIMITCOM_DBT задолженности по коммисси ИнвестСоветник
   PROCEDURE InsertLimitCom_InvestSov( p_MarketID IN NUMBER
                                 ,p_CalcDate IN DATE
                                , p_sql_where varchar2)
     as
       l_sql VARCHAR2(20000) := '';

   begin
     l_sql :=
 'insert into DDL_LIMITCOM_DBT 
   select t_MarketID,t_client,t_sfcontrid, t_commnumber,
        sum(t_sum),t_fiid_sum, PLANDATE, '''||g_calc_clientinfo||'''                          
   from (select /*+ leading(sfdef)  index(c DDL_CLIENTINFO_DBT_IDX0)*/ distinct sfdef.t_id, c.t_MarketID,c.t_client, c.t_sfcontrid, sfdef.t_commnumber,
            sfdef.t_sum, sfdef.t_fiid_sum,
            sfdef.t_PlanPayDate PLANDATE  
         from ddl_clientinfo_dbt c, dsfdef_dbt sfdef
         WHERE '||p_sql_where||'
             and sfdef.t_feetype = 1 and sfdef.t_status in ( 10,40)
             and sfdef.t_sfcontrid = c.t_sfcontrid
             and sfdef.t_commnumber = 1078 
             AND (sfdef.t_status = 10 or sfdef.T_DATEPAY >= :CalcDate )
             and sfdef.t_dateperiodend <=  trunc(:p_CalcDate,''q'') -1)
  group by t_MarketID,t_client,t_sfcontrid, t_commnumber,t_fiid_sum, PLANDATE ';
  
     execute immediate l_sql using   p_CalcDate, p_CalcDate ;
  
   end;

   -- Вставка в DDL_LIMITCOM_DBT задолженности по комиссии "БрокерФикс"
   PROCEDURE InsertLimitCom_BrokerFIX( p_MarketID IN NUMBER
                                 ,p_CalcDate IN DATE
                                , p_sql_where varchar2)
     as
       l_sql VARCHAR2(20000) := '';
      begin
  
     l_sql :=
 'insert into DDL_LIMITCOM_DBT 
  SELECT t_MarketID, t_client, t_sfcontrid, t_commnumber,
         SUM(t_sum), t_fiid_sum, PLANDATE, '''||g_calc_clientinfo||'''     
  FROM (SELECT /*+ leading(sfdef)  index(c DDL_CLIENTINFO_DBT_IDX0)*/ 
       DISTINCT sfdef.t_id, c.t_MarketID, c.t_client, c.t_sfcontrid, sfdef.t_commnumber,
               sfdef.t_sum, sfdef.t_fiid_sum, sfdef.t_dateperiodend PLANDATE
        FROM ddl_clientinfo_dbt c, dsfdef_dbt sfdef
        WHERE sfdef.t_commnumber = 1063  and  '||p_sql_where||'
          AND sfdef.t_feetype = 1 AND sfdef.t_status in ( 10,40)
          AND sfdef.t_sfcontrid = c.t_sfcontrid
          AND sfdef.t_dateperiodend <= :CalcDate
          AND (sfdef.t_status = 10 or sfdef.T_DATEPAY >= :CalcDate ))
  GROUP BY t_MarketID, t_client, t_sfcontrid, t_commnumber, t_fiid_sum, PLANDATE';
  
     execute immediate l_sql using   p_CalcDate,p_CalcDate;
  
   end;


   PROCEDURE RSI_ClearContrTable ( p_MarketID IN NUMBER
                                 ,p_MarketCode IN VARCHAR2
                                 ,p_CalcDate IN DATE
                                , p_ByStock IN NUMBER
                                , p_ByCurr IN NUMBER
                                , p_ByEDP IN NUMBER
                                , p_byDeriv IN NUMBER
                                , p_UseListClients IN NUMBER)
   AS
   l_sql VARCHAR2(20000) := '';

   begin
    l_sql:= Get_whereContrTable (g_calc_clientinfo
                                 , p_MarketID
                                 ,p_MarketCode
                                 ,p_CalcDate
                                , p_ByStock
                                , p_ByCurr
                                , p_ByEDP
                                , p_byDeriv
                                , 0) ;
       /*l_sql:=null;
         IF  p_ByStock <> 1 or p_ByCURR <> 1 or p_ByEDP <> 1 or p_byDeriv <> 1 THEN
            IF p_ByStock = 1 THEN
               l_sql:=case when l_sql is not null then l_sql||' or ' end ||'( t_ServKind = 1 and t_IsEDP = chr(0))';
            END IF;
            IF p_ByCURR = 1 THEN
               l_sql:=case when l_sql is not null then l_sql||' or ' end ||'( t_ServKind = 21 and t_IsEDP = chr(0))';
            END IF;
            IF p_byDeriv = 1 THEN
                l_sql:=case when l_sql is not null then l_sql||' or ' end ||'(  t_ServKind = 15 and t_IsEDP = chr(0))';
            END IF;
            IF p_ByEDP = 1 THEN
                l_sql:=case when l_sql is not null then l_sql||' or ' end ||'( t_IsEDP = chr(88))';
            END IF;
             l_sql:=' and ('||l_sql||')';
         END IF;*/

      LockRecordsFrom('DDL_CLIENTINFO_DBT c ',l_sql);

      execute immediate 'DELETE FROM DDL_CLIENTINFO_DBT c Where '||l_sql ;
   end;

  
  function Get_FillContrTableRestDate(p_MarketID number,p_CalcDate date) return date 
    as
    v_CalendId NUMBER(10) := 0;
    v_RestDate date ;
   begin  
      v_CalendId := GetCalendarIDForLimit(p_MarketID);

      /*если вчерашний день баланса и вчерашний рабочий биржи не совпадают, то значит, что все проводки прошлого дня ушли в следующий балансовый день*/
      IF RSI_DLCALENDARS.GetBalanceDateAfterWorkDayByCalendar (p_CalcDate, -1, v_CalendId) <
            RSI_RSBCALENDAR.GETDATEAFTERWORKDAY (p_CalcDate, -1, v_CalendId)
      THEN
         v_RestDate := RSI_DLCALENDARS.GetBalanceDateAfterWorkDayByCalendar (p_CalcDate, 1, v_CalendId);
      ELSE
         v_RestDate := p_CalcDate - 1;
      END IF;
     return v_RestDate;
   end;
   --общие данные для всех лимитов можно собрать однократно
   PROCEDURE RSI_FillContrTablenotDeriv ( p_MarketID IN NUMBER
                                 ,p_MarketCode IN VARCHAR2
                                 ,p_CalcDate IN DATE
                                , p_ByStock IN NUMBER
                                , p_ByCurr IN NUMBER
                                , p_ByEDP IN NUMBER
                                , p_byDeriv IN NUMBER
                                , p_UseListClients IN NUMBER)
   AS
      v_Restdate   DATE :=Get_FillContrTableRestDate(p_MarketID ,p_CalcDate );
      l_sql VARCHAR2(20000) := '';
      l_sql_where_contr varchar2(2000):='';
      l_sql_where varchar2(2000):='';
   BEGIN
      ts_ := SYSTIMESTAMP;
      TimeStamp_ ('Сбор данных по договорам '||p_MarketCode, p_CalcDate, NULL, SYSTIMESTAMP,1);

    l_sql_where:='';
    if p_ByStock != 1 or p_ByCurr != 1 or p_ByEDP != 1 then -- если все установлены то ничего не проверяем
       if p_ByEDP = 1 then
          l_sql_where:=' RSHB_RSI_SCLIMIT.SfcontrIsEDP(sfcontr.t_id)=1 ';
       end if;
       if  p_ByStock = 1 then
         l_sql_where:=l_sql_where|| case when l_sql_where is not null then '
           or ' end || ' (sfcontr.t_ServKind = 1 and  RSHB_RSI_SCLIMIT.SfcontrIsEDP(sfcontr.t_id) <> 1) ';
       end if;
        if p_ByCurr = 1 then
          l_sql_where:=l_sql_where|| case when l_sql_where is not null then '
            or ' end || ' (sfcontr.t_ServKind = 21 and  RSHB_RSI_SCLIMIT.SfcontrIsEDP(sfcontr.t_id) <> 1) ';
        end if;
       l_sql_where:=case when l_sql_where is not null then ' and ('||l_sql_where||')' end;
    end if;
    l_sql:= 'INSERT  INTO DDL_CLIENTINFO_DBT r (
            r.t_sfcontrid, r.t_sfstate, r.t_partyid, r.t_legalform, r.t_dlcontrid, r.t_accountid, r.t_client, r.t_account
            , r.t_code_currency, r.t_chapter, r.t_money306, r.t_due474, r.t_market, r.t_mpcode, r.t_servkind, r.t_servkindsub
            , r.t_ccy, r.t_leverage, r.t_open_limit, r.t_isblocked, r.t_notexcluderepo, r.t_trdaccid, r.t_deponumber
            , r.t_depoclosedate, r.t_isedp, r.t_haserrors, r.t_marketid, r.t_isqi, r.t_test_result, r.t_ekk, r.t_stockcode
            , r.t_tag, r.t_implkind, r.t_firm_id, r.t_calc_sid 
         )
         with sfcontr as ( select /*+ materialize */ 
                           sfcontr.*
                           , RSHB_RSI_SCLIMIT.GetTRDACCIDbyServKind(t_id ,:p_CalcDate ,t_marketid ,t_ServKind,t_implkind) t_trdaccid
                           , RSHB_RSI_SCLIMIT.GetTAGbyServKind(t_marketid ,t_ServKind, t_implkind ,SfcontrIsEDP ) t_tag
                           , RSHB_RSI_SCLIMIT.GetFIRM_IDbyServKind(t_marketid ,t_ServKind, t_implkind) t_firm_id	
                           from ( select /*+ ordered */ distinct
                              sfcontr.t_partyid,
                              p.t_legalform,
                              sfcontr.t_id,
                              sfcontr.t_ServKind,
                              sfcontr.t_ServKindSub,
                              mp.t_marketid,
                              mp.t_mpcode,
                              dlc.t_dlcontrid,
                              RSHB_RSI_SCLIMIT.SfcontrIsEDP(sfcontr.t_id) as SfcontrIsEDP,
                              RSHB_RSI_SCLIMIT.GetImplKind(dlc.t_dlcontrid,:p_CalcDate) t_implkind
                         from ';
          IF  (p_UseListClients = 1) THEN
              l_sql:=l_sql ||' DDL_PANELCONTR_DBT tt,
                             ' ;
          end if;
            l_sql:=l_sql ||'ddlcontrmp_dbt mp,
                            dsfcontr_dbt sfcontr,
                            ddlcontr_dbt dlc,
                            dparty_Dbt p
                        where sfcontr.t_ID > 0
                        AND t_ServKind <> 15 /*не срочный рынок*/
                        AND (sfcontr.t_DateClose = TO_DATE (''01.01.0001'', ''DD.MM.YYYY'')
                           OR sfcontr.t_DateClose >= :p_CalcDate)
                        AND sfcontr.t_ServKindSub = 8               /*Биржевой рынок - отчесет внебиржу и андеррайтинг*/
                        AND sfcontr.t_partyid = p.t_partyid
                        AND mp.t_SfContrID = sfcontr.t_ID
                        AND mp.t_Marketid = :p_MarketID
                        AND dlc.t_DlContrID = mp.t_DlContrID
                        '||l_sql_where ;
          IF  (p_UseListClients = 1) THEN
            l_sql:=l_sql ||' and tt.t_calc_sid = '''||g_calc_panelcontr||''' and tt.t_dlcontrid = mp.t_DlContrID and tt.t_setflag = chr(88)
                          ';
          end if;
          l_sql:=l_sql ||') sfcontr )
         SELECT /*+ ordered use_nl(accdoc) index(accdoc DMCACCDOC_DBT_IDXC)*/   DISTINCT sfcontr.t_id,
                              0,
                              sfcontr.t_partyid,
                              sfcontr.t_legalform,
                              sfcontr.t_dlcontrid,
                              acc.t_AccountID,
                              acc.t_Client,
                              acc.t_Account,
                              acc.t_Code_Currency,/* drestdate_dbt*/
                              acc.t_chapter,
                              0 /*(rsb_account.restac (acc.t_Account,acc.t_Code_Currency,v_restdate,acc.t_Chapter,NULL))*/ AS Money306,
                              0,
                              DECODE (sfcontr.t_ServKindSub, 8, 1, 0) AS Market,
                              sfcontr.t_mpcode,
                              sfcontr.t_ServKind,
                              sfcontr.t_ServKindSub,
                              curr.t_CCY,
                              0, /*dlc.t_Leverage,*/ /*BIQ 9185 - в поле LEVERAGE с 01/10/2021 код результатов тестирования по квалам, заполним ниже*/
                              0 AS Open_Limit,
                              chr(0),
                              chr(0),
                              /*case when sfcontr.t_servkind = 1 then replace(rsb_struct.getString (rsi_rsb_kernel.GetNote ( 659,LPAD (sfcontr.t_Id, 10, ''0''),5 ,p_CalcDate)),chr(0)) else chr(1) end ,*/
                              sfcontr.t_trdaccid ,
                              chr(1),
                              null
                              , CASE WHEN sfcontr.SfcontrIsEDP  = 1 THEN CHR(88) ELSE CHR (0) END   	-- t_IsEdp
                              , CHR(0)									-- t_HasError
                              , sfcontr.t_marketid         -- t_MarketID
                              , chr(0)									-- t_isqi
                              , -1									-- t_test_result
                              , chr(1)									-- t_ekk
                              , chr(1)									-- t_stockcode
                              , sfcontr.t_tag           -- t_tag
                              , sfcontr.t_implkind  -- (case when RSB_SECUR.GetGeneralMainObjAttr(659, LPAD (sfcontr.t_ID, 10, ''0''), 7 /*Перевод активов на новый номер ТКС произведен*/,:p_CalcDate) = 1 /*Да*/ then 2 else 1 end) -- t_implkind
                              , sfcontr.t_firm_id
                              , '''||g_calc_clientinfo||''' 
                 from  sfcontr
                   join dmcaccdoc_dbt accdoc
                      on sfcontr.t_ID = accdoc.t_ClientContrID
                       and sfcontr.t_partyid = accdoc.t_owner
                       AND accdoc.t_catid = 70 -- accdoc.t_catnum = 201 /*ДС Клиента*/
                   join dfininstr_dbt curr
                    on  curr.t_FIID = accdoc.t_Currency
                    and curr.t_FI_Kind = 1
                   join daccount_dbt  acc
                     on accdoc.t_Chapter = acc.t_Chapter
                       and accdoc.t_Account = acc.t_Account
                       and accdoc.t_Currency = acc.t_Code_Currency
                WHERE accdoc.t_Chapter = 1
                      AND accdoc.t_Account LIKE ''306%''
                      AND (accdoc.t_disablingDate = TO_DATE (''01.01.0001'', ''DD.MM.YYYY'')
                           OR accdoc.t_disablingDate >= :p_CalcDate)
                      AND acc.t_client NOT IN (SELECT d.t_PartyID
                                                FROM ddp_dep_dbt d)
                      AND acc.t_Open_Date <= :v_restdate /*CHVA 523496*/
                      AND (acc.t_Close_Date = TO_DATE (''01.01.0001'', ''DD.MM.YYYY'')
                           OR acc.t_Close_Date >= :p_CalcDate)';
 
     if ((p_ByStock <> 0) or (p_ByCurr <> 0) or (p_ByEDP <> 0)) THEN
        execute immediate l_sql using   p_CalcDate,p_CalcDate,p_CalcDate, p_MarketID,p_CalcDate,v_restdate,p_CalcDate;
     END IF;

     end;
  
  PROCEDURE RSI_FillContrTablebyDeriv ( p_MarketID IN NUMBER
                                 ,p_MarketCode IN VARCHAR2
                                 ,p_CalcDate IN DATE
                                , p_ByStock IN NUMBER
                                , p_ByCurr IN NUMBER
                                , p_ByEDP IN NUMBER
                                , p_byDeriv IN NUMBER
                                , p_UseListClients IN NUMBER)
   AS
      l_sql VARCHAR2(20000) := '';
      l_sql_where_contr varchar2(2000):='';
      l_sql_where varchar2(2000):='';
      v_Restdate   DATE := Get_FillContrTableRestDate(p_MarketID ,p_CalcDate );

   BEGIN
     if ( p_byDeriv <> 0 ) THEN
         TimeStamp_ ('Сбор данных по договорам(Deriv) '||p_MarketCode,
                  p_CalcDate,
                  NULL,
                  SYSTIMESTAMP,
                 -- NULL,
                  0
                 );


         l_sql := '
           INSERT  INTO DDL_CLIENTINFO_DBT r (
              r.t_sfcontrid, r.t_sfstate, r.t_partyid, r.t_legalform, r.t_dlcontrid, r.t_accountid, r.t_client, r.t_account
              , r.t_code_currency, r.t_chapter, r.t_money306, r.t_due474, r.t_market, r.t_mpcode, r.t_servkind, r.t_servkindsub
              , r.t_ccy, r.t_leverage, r.t_open_limit, r.t_isblocked, r.t_notexcluderepo, r.t_trdaccid, r.t_deponumber
              , r.t_depoclosedate, r.t_isedp, r.t_haserrors, r.t_marketid, r.t_isqi, r.t_test_result, r.t_ekk, r.t_stockcode
              , r.t_tag, r.t_implkind, r.t_firm_id, r.t_calc_sid 
           )

             with contr as  (select /*+ materialize ordered */ distinct sfcontr.t_id,
                  sfcontr.t_partyid,
                  p.t_legalform,
                  mp.t_dlcontrid,
                  mp.t_mpcode,
                  sfcontr.t_ServKind,
                  sfcontr.t_ServKindSub,
                  mp.t_marketid,
                  RSHB_RSI_SCLIMIT.GetImplKind(mp.t_dlcontrid,:p_CalcDate) t_implkind
                  from ';
           IF  (p_UseListClients = 1) THEN
             l_sql:=l_sql ||'DDL_PANELCONTR_DBT tt ,
                 ';
           END IF; 
           l_sql:=l_sql ||' ddlcontrmp_dbt mp,
                       dsfcontr_dbt   sfcontr,
                       ddlcontr_dbt   dlc,
                       dparty_Dbt     p
                 where mp.t_mpcode != CHR(1)
                   AND p.t_partyid = sfcontr.t_partyid
                   AND sfcontr.t_ServKind = 15
                   AND (sfcontr.t_DateClose = TO_DATE (''01.01.0001'', ''DD.MM.YYYY'')
                     OR sfcontr.t_DateClose >= :p_CalcDate)
                   AND sfcontr.t_ServKindSub = 8
                   AND mp.t_SfContrID = sfcontr.t_ID
                   AND mp.t_DlContrID = dlc.t_DlContrID
                   AND mp.t_mpcode != CHR(1)
                   AND mp.t_Marketid = :p_MarketID
                   AND RSHB_RSI_SCLIMIT.SfcontrIsEDP(sfcontr.t_id) <> 1  
                   ';
          IF  (p_UseListClients = 1) THEN
            l_sql:=l_sql ||
                  ' AND tt.t_calc_sid = '''||g_calc_panelcontr||''' AND tt.T_SETFLAG = CHR (88)
                    AND tt.T_CLIENTID = sfcontr.t_partyid
                    AND tt.T_DLCONTRID = mp.t_DlContrID
                   ';
           end if;
          l_sql:=l_sql ||')
            SELECT  /*+ ordered use_nl(accdoc) index(accdoc DMCACCDOC_DBT_IDXC)*/ DISTINCT '||
             q'[
                contr.t_id,
                0,
                contr.t_partyid,
                contr.t_legalform,
                contr.t_dlcontrid,
                acc.t_AccountID,
                acc.t_Client,
                acc.t_Account,
                acc.t_Code_Currency,
                acc.t_chapter,
                0 AS Money306,
                0,
                1,
                contr.t_mpcode,
                contr.t_ServKind,
                contr.t_ServKindSub,
                curr.t_CCY,
                0,
                0 AS Open_Limit,
                CHR (0),
                CHR (0),
                CHR (1),
                CHR (1),
                NULL,
                CHR (0),
                CHR (0),
                contr.t_marketid,
                CHR (0),
                -1,
                CHR (1),
                CHR (1),
                CHR (1),
                contr.t_ImplKind , --(case when RSB_SECUR.GetGeneralMainObjAttr(659, LPAD (contr.t_ID, 10, '0'), 7 /*Перевод активов на новый номер ТКС произведен*/,:p_CalcDate) = 1 /*Да*/ then 2 else 1 end),
                CHR(1),
                ']'||g_calc_clientinfo||q'[' 
           FROM contr,
               dmcaccdoc_dbt  accdoc,
               daccount_dbt   acc,
               dfininstr_dbt  curr
          WHERE     acc.t_Chapter = 1
                AND acc.t_Account LIKE '306%'
                AND acc.t_Client NOT IN (SELECT d.t_PartyID
                                           FROM ddp_dep_dbt d)
                AND acc.t_Open_Date < :p_CalcDate
                AND (acc.t_Close_Date = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                     OR acc.t_Close_Date >= :p_CalcDate)
                AND accdoc.t_Chapter = acc.t_Chapter
                AND accdoc.t_Account = acc.t_Account
                AND accdoc.t_Currency = acc.t_Code_Currency
                AND accdoc.t_ClientContrID > 0
                AND contr.t_ID = accdoc.t_ClientContrID
                AND contr.t_partyid = accdoc.t_owner
                AND accdoc.t_catid = 70
                AND curr.t_FIID = acc.t_Code_Currency
                AND acc.t_Open_Date <= :v_restdate
                AND curr.t_FI_Kind = 1
                ]';
         execute immediate l_sql using p_CalcDate, p_CalcDate, p_MarketID, p_CalcDate, p_CalcDate, v_restdate;
     END IF;
  end; 
  
  
  PROCEDURE RSI_FillContrTableAcc ( p_MarketID IN NUMBER
                                   ,p_MarketCode IN VARCHAR2
                                   ,p_CalcDate IN DATE
                                   , p_ByStock IN NUMBER
                                   , p_ByCurr IN NUMBER
                                   , p_ByEDP IN NUMBER
                                   , p_byDeriv IN NUMBER
                                   , p_UseListClients IN NUMBER)
   AS
      l_sql VARCHAR2(20000) := '';
      l_sql_where varchar2(2000):= Get_whereContrTable(g_calc_clientinfo
                                                       ,p_MarketID
                                                       ,p_MarketCode
                                                       ,p_CalcDate
                                                       , p_ByStock
                                                       , p_ByCurr
                                                       , p_ByEDP
                                                       , p_byDeriv
                                                        , p_UseListClients);
      l_merge_on varchar2(2000) := ' c.t_calc_sid = '''||g_calc_clientinfo||''' and c.t_MarketID = '||p_MarketID ;
      
      v_Restdate   DATE := Get_FillContrTableRestDate(p_MarketID ,p_CalcDate );
      
   begin 
     TimeStamp_ ('Заполнение остатков 306',p_CalcDate,NULL,SYSTIMESTAMP);
         l_sql :=
      '  MERGE /*+ index(c DDL_CLIENTINFO_DBT_IDX0) */ INTO DDL_CLIENTINFO_DBT c
        USING (SELECT c.t_accountid, t_code_currency, nvl(r.t_rest,0) t_rest, c.t_sfcontrid
                 FROM DDL_CLIENTINFO_DBT c
                 left join drestdate_dbt r on C.T_ACCOUNTID = R.T_ACCOUNTID
                         AND C.T_CODE_CURRENCY = R.T_RESTCURRENCY
                WHERE  '||l_sql_where|| '
                  AND (R.T_ACCOUNTID is null or r.t_restdate = (SELECT MAX (t_restdate)
                                FROM drestdate_dbt t
                               WHERE t.t_accountid = C.T_ACCOUNTID
                                     AND T.T_RESTCURRENCY = C.T_CODE_CURRENCY
                                     AND t_restdate < :p_CalcDate ))  

               group by c.t_accountid, t_code_currency, nvl(r.t_rest,0) , c.t_sfcontrid ) s
           ON ( '||l_merge_on||' and  c.t_accountid = s.t_accountid
               AND C.T_CODE_CURRENCY = s.t_code_currency and c.t_sfcontrid = s.t_sfcontrid
               ) WHEN MATCHED THEN UPDATE SET c.t_Money306 = s.t_rest , c.t_time306 = sysdate ';
   -- dbms_output.put_line(l_sql);
   execute immediate l_sql using   p_CalcDate;

  
      TimeStamp_ ('Заполнение остатка 47423 ',p_CalcDate,NULL,SYSTIMESTAMP);
         l_sql :=
      ' MERGE /*+ index(c DDL_CLIENTINFO_DBT_IDX0) */ INTO ddl_clientinfo_dbt c
             USING (  SELECT /*+ ordered cardinality(c,1000) '||case when p_UseListClients = 1 then 'index(c DDL_CLIENTINFO_DBT_IDX2)' end ||'  
                        use_nl(mc) index(mc DMCACCDOC_DBT_IDXC) 
                        use_nl(a) index(a DACCOUNT_DBT_IDX0) 
                        use_nl(r) */  SUM(ABS(t_rest)) due474,
                     mc.t_clientcontrid,
                     mc.t_currency,
                     mc.t_owner
                FROM ddl_clientinfo_dbt c,dmcaccdoc_dbt mc, daccount_dbt a, drestdate_dbt r
               WHERE '||l_sql_where||'
                     and c.t_servkind != 21 -- Не используется  
                     and c.t_sfcontrid = mc.t_clientcontrid
                     AND c.t_code_currency = mc.t_currency
                     AND c.t_client = mc.t_owner
                     and mc.t_catid = 818
                     AND mc.t_iscommon = CHR (88)
                     AND mc.t_chapter = a.t_chapter
                     AND mc.t_account = a.t_account
                     AND MC.T_CURRENCY = A.T_CODE_CURRENCY
                     AND a.T_ACCOUNTID = R.T_ACCOUNTID
                     AND a.T_CODE_CURRENCY = R.T_RESTCURRENCY
                     AND r.t_restdate =
                            (SELECT MAX (t_restdate)
                               FROM drestdate_dbt t
                              WHERE t.t_accountid = a.T_ACCOUNTID
                                    AND T.T_RESTCURRENCY =  a.T_CODE_CURRENCY
                                    AND t.t_restdate <= :v_restdate)
            GROUP BY mc.t_clientcontrid, mc.t_currency, mc.t_owner
            HAVING SUM(ABS(t_rest)) > 0 ) d
        ON ( '||l_merge_on||' and c.t_sfcontrid = d.t_clientcontrid
            AND c.t_code_currency = d.t_currency
            AND c.t_client = d.t_owner
             ) WHEN MATCHED
     THEN  UPDATE SET c.t_due474 = d.due474';
   execute immediate l_sql using  v_restdate ;

      TimeStamp_ ('Заполнение неоплаченых комиссий по прочим требованиям ',p_CalcDate,NULL,SYSTIMESTAMP);
         l_sql :=
      '  MERGE /*+ index(c DDL_CLIENTINFO_DBT_IDX0) */ INTO DDL_CLIENTINFO_DBT c
        USING (SELECT  c.t_sfcontrid, dr.T_DEBTCURRENCY, sum(T_DEBTSUM) DEBTSUM
                 FROM DDL_CLIENTINFO_DBT c
                 join DDL_DEBTREESTR_DBT dr on c.t_sfcontrid = dr.t_sfcontrid and C.T_CODE_CURRENCY = dr.T_DEBTCURRENCY
               WHERE  '||l_sql_where|| '
                  AND dr.T_DEBTDATE < :p_CalcDate and dr.t_state = 1 
               group by  c.t_sfcontrid, dr.T_DEBTCURRENCY  
                  )  s
           ON ( '||l_merge_on||' and  C.T_CODE_CURRENCY = s.T_DEBTCURRENCY and c.t_sfcontrid = s.t_sfcontrid
               ) WHEN MATCHED THEN UPDATE SET c.T_OTHERREQ = s.DEBTSUM  ';
    --dbms_output.put_line(l_sql);
   execute immediate l_sql using   p_CalcDate;

   END;
  
  PROCEDURE RSI_FillContrTableCOM ( p_MarketID IN NUMBER
                                 ,p_MarketCode IN VARCHAR2
                                 ,p_CalcDate IN DATE
                                , p_ByStock IN NUMBER
                                , p_ByCurr IN NUMBER
                                , p_ByEDP IN NUMBER
                                , p_byDeriv IN NUMBER
                                , p_UseListClients IN NUMBER)
   AS 
      l_sql_where varchar2(2000):= Get_whereContrTable( g_calc_clientinfo
                                                        ,p_MarketID
                                                        ,p_MarketCode
                                                        ,p_CalcDate
                                                        , p_ByStock
                                                        , p_ByCurr
                                                        , p_ByEDP
                                                        , p_byDeriv
                                                        , p_UseListClients);
   BEGIN  
   TimeStamp_ ('Расчет сумм неоплаченных комиссий ',p_CalcDate,NULL,SYSTIMESTAMP);
   
   LockRecordsFrom('DDL_LIMITCOM_DBT','t_calc_sid = '''||g_calc_clientinfo||''' and t_MarketID = '||p_MarketID);
   DELETE FROM DDL_LIMITCOM_DBT Where t_calc_sid = g_calc_clientinfo and t_MarketID = p_MarketID ;

   InsertLimitCom_InvestSov( p_MarketID ,p_CalcDate, l_sql_where);     
   InsertLimitCom_BrokerFIX( p_MarketID ,p_CalcDate, l_sql_where);     
   
   END;

   --Контроль общих данных для всех лимитов выполняется после  RSI_FillContrTable
   PROCEDURE  RSI_CheckContrTable  (p_CalcDate IN DATE, p_ByStock IN NUMBER, p_ByCurr IN NUMBER,
               p_ByEDP IN NUMBER ,p_byDeriv IN NUMBER,p_UseListClients IN NUMBER default 0)
   AS
      v_Restdate   DATE;
      v_RegVal_default_leverage   NUMBER(10);
      v_RegVal_default_leverage_entity   NUMBER(10); /*BOSS-4379 Доступ к сложным ФИ для ЮЛ неквалифицированных инвесторов */
      l_sql VARCHAR2(20000) := '';
      l_sql_where varchar2(2000):='';
   begin

     TimeStamp_ ('Заполнение параметров ДБО: статус, блокировка ',p_CalcDate,NULL,SYSTIMESTAMP);

     l_sql :=
      '     MERGE /*+ index(c DDL_CLIENTINFO_DBT_IDX2) */ INTO ddl_clientinfo_dbt c
      USING ( SELECT /*+ ordered '||case when p_UseListClients = 1 then ' cardinality(tt 100) ' end||'*/ RSB_SECUR.GetGeneralMainObjAttr (207,LPAD (c.t_DlContrID, 34, ''0''),101,:p_CalcDate) sfstate,
                      (CASE WHEN RSB_SECUR.GetGeneralMainObjAttr (207,LPAD (c.t_DlContrID, 34, ''0''),1,:p_CalcDate) = 1 THEN ''X'' ELSE CHR (0) END) AS IsBlocked,
                         (SELECT T_CODE
                       FROM DDLOBJCODE_DBT
                      WHERE T_CODEKIND = 1 AND T_OBJECTTYPE = 207
                            AND (T_BANKCLOSEDATE =
                                    TO_DATE (''01010001'', ''ddmmyyyy'')
                                 OR T_BANKCLOSEDATE >= :p_CalcDate)
                            AND T_OBJECTID = c.T_DLCONTRID) ekk,
                            (SELECT T_MPCODE
                       FROM DDLCONTRMP_DBT MP, DSFCONTR_DBT S
                      WHERE  MP.T_SFCONTRID = S.T_ID
                            AND S.T_SERVKIND = 1
                            AND S.T_SERVKINDSUB = 8
                            AND ( (S.T_DATECLOSE =
                                      TO_DATE (''01010001'', ''ddmmyyyy''))
                                 OR (S.T_DATECLOSE >= :p_CalcDate))
                             AND MP.t_marketid = :MarketID
                            AND MP.T_DLCONTRID = c.T_DLCONTRID
                            AND ROWNUM = 1) STOCKCODE,
                      c.t_dlcontrid
                  FROM ';
        if  (p_UseListClients = 1) then
              l_sql:=l_sql ||' DDL_PANELCONTR_DBT tt
             join ddl_clientinfo_dbt c
                on  tt.t_dlcontrid = c.t_DlContrID
                  ';
        else
          l_sql:=l_sql ||' ddl_clientinfo_dbt c
            ';
        end if;
        l_sql:=l_sql ||' WHERE c.t_calc_sid = '''||g_calc_clientinfo||''' 
            ';
        if  (p_UseListClients = 1) then
              l_sql:=l_sql ||' and tt.t_calc_sid = '''||g_calc_panelcontr||''' and tt.t_setflag = chr(88) 
            ';
        end if ;
        l_sql:=l_sql ||' GROUP BY c.t_dlcontrid ) d
          ON (c.t_calc_sid = '''||g_calc_clientinfo||''' and c.t_dlcontrid = d.t_dlcontrid )
      WHEN MATCHED
      THEN
         UPDATE SET c.t_sfstate = d.sfstate,
                    c.t_IsBlocked = d.IsBlocked,
                    c.t_ekk = d.ekk,
                    c.t_stockcode = d.stockcode';
   execute immediate l_sql using     p_CalcDate,p_CalcDate,p_CalcDate,p_CalcDate, GetMicexID();


    TimeStamp_ ('Заполнение параметров ДБО: учет Репо, депо договор ',p_CalcDate,NULL,SYSTIMESTAMP);

    l_sql :=
      '     MERGE /*+ index(c DDL_CLIENTINFO_DBT_IDX2) */ INTO ddl_clientinfo_dbt c
           USING ( SELECT /*+ ordered '||case when p_UseListClients = 1 then ' cardinality(tt 100) ' end||'*/ (CASE WHEN RSB_SECUR.GetGeneralMainObjAttr (207,LPAD (c.t_DlContrID, 34, ''0''),130,:p_CalcDate) = 1 THEN ''X'' ELSE CHR (0) END) AS NotExcludeRepo,
                           REPLACE (rsb_struct.getString (rsi_rsb_kernel.GetNote (207,LPAD (c.t_dlcontrid, 34, ''0''),102,:p_CalcDate)), CHR (0) ) AS deponumber,
                           CASE WHEN rsi_rsb_kernel.GetNote (207,LPAD (c.t_dlcontrid, 34, ''0''),110,:p_CalcDate) IS NULL THEN NULL
                              ELSE rsb_struct.getDate (rsi_rsb_kernel.GetNote (207,LPAD (c.t_dlcontrid, 34, ''0''),110,:p_CalcDate)) END depoclosedate,
                           c.t_dlcontrid
                       FROM ';
     if  (p_UseListClients = 1) then
             l_sql:=l_sql ||' DDL_PANELCONTR_DBT tt
             join ddl_clientinfo_dbt c
                on  tt.t_dlcontrid = c.t_DlContrID
                  ';
     else
          l_sql:=l_sql ||' ddl_clientinfo_dbt c
            ';
     end if;
     l_sql:=l_sql ||' WHERE c.t_calc_sid = '''||g_calc_clientinfo||''' and c.t_ServKind <> 15 /*не срочный рынок*/  
            ';
     if  (p_UseListClients = 1) then
              l_sql:=l_sql ||' and tt.t_calc_sid = '''||g_calc_panelcontr||''' and tt.t_setflag = chr(88) 
            ';
     end if ;
     l_sql:=l_sql ||' GROUP BY c.t_dlcontrid ) d
      ON ( c.t_calc_sid = '''||g_calc_clientinfo||''' and c.t_dlcontrid = d.t_dlcontrid )
      WHEN MATCHED THEN
         UPDATE SET c.t_NotExcludeRepo = d.NotExcludeRepo,
                    c.t_deponumber = d.deponumber,
                    c.t_depoclosedate = d.depoclosedate';

   execute immediate l_sql using     p_CalcDate,p_CalcDate,p_CalcDate,p_CalcDate;


    TimeStamp_ ('Заполнение задолженности по коммисиям по субдоговарам ',p_CalcDate,NULL,SYSTIMESTAMP);

    l_sql :=
      '  MERGE /*+ index(c DDL_CLIENTINFO_DBT_IDX0) */ INTO DDL_CLIENTINFO_DBT c
        USING ( select ';
     if  (p_UseListClients = 1) then
        l_sql:= l_sql||'/*+ leading(tt,c)  cardinality(tt 100) */
                  ';
     end if;
     l_sql:= l_sql|| ' l.t_client,l.t_sfcontrid,l.t_fiid, nvl(sum(l.t_sum),0) sum_com
                    from ';
     if  (p_UseListClients = 1) then
             l_sql:=l_sql ||' DDL_PANELCONTR_DBT tt
             join ddl_clientinfo_dbt c
                on  tt.t_dlcontrid = c.t_DlContrID
                  ';
     else
          l_sql:=l_sql ||' ddl_clientinfo_dbt c
            ';
     end if;
     l_sql:=l_sql ||',DDL_LIMITCOM_DBT l  WHERE c.t_calc_sid = '''||g_calc_clientinfo||'''   
            ';
     if  (p_UseListClients = 1) then
              l_sql:=l_sql ||' and tt.t_calc_sid = '''||g_calc_panelcontr||''' and tt.t_setflag = chr(88) 
            ';
     end if ;
     l_sql:=l_sql ||' and c.t_sfcontrid = l.t_sfcontrid
            AND c.t_code_currency = l.t_fiid   AND c.t_client = l.t_client  and l.t_plandate < :p_CalcDate
            and l.t_calc_sid = '''||g_calc_clientinfo||'''
                 group by l.t_client,l.t_sfcontrid,l.t_fiid)  s
           ON ( c.t_calc_sid = '''||g_calc_clientinfo||''' and  C.T_CODE_CURRENCY = s.t_fiid and c.t_sfcontrid = s.t_sfcontrid
               and c.t_client = s.t_client) WHEN MATCHED THEN UPDATE SET c.T_COMPREVIOUS = s.sum_com ';
    --dbms_output.put_line(l_sql);
   execute immediate l_sql using   p_CalcDate;


      v_RestDate := p_CalcDate - 1;

/*BIQ 9185 признак квал инвестора и результат тестирования*/
   v_RegVal_Default_Leverage
      := to_number(nvl( RSB_COMMON.GetRegStrValue ('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\QUIK\LEVERAGE_DEFAULT'),-1));
      
/*BOSS-4379 Доступ к сложным ФИ для ЮЛ неквалифицированных инвесторов */
   v_RegVal_Default_Leverage_Entity
      := to_number(nvl( RSB_COMMON.GetRegStrValue ('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\QUIK\LEVERAGE_DEFAULT_ENTITY'),-1));      

  TimeStamp_ ('Заполнение признака квалинвестора и результатов теста',p_CalcDate,NULL,SYSTIMESTAMP);
   l_sql :=
      '  MERGE /*+ index(c DDL_CLIENTINFO_DBT_IDX3) */ INTO ddl_clientinfo_dbt c USING (
     SELECT /*+ ordered '||case when p_UseListClients = 1 then ' cardinality(tt 100) ' end||'*/ c.t_partyid,
            DECODE (q.t_state, 1, CHR (88), chr(0)) isqi,
            CASE WHEN n.t_text IS NULL THEN -1 ELSE rsb_struct.getint (t_text) END
            test_result
       FROM ';
        if  (p_UseListClients = 1) then
              l_sql:=l_sql ||' DDL_PANELCONTR_DBT tt
             join ddl_clientinfo_dbt c
                on  tt.t_dlcontrid = c.t_DlContrID
             ';
        else
         l_sql:=l_sql ||' ddl_clientinfo_dbt c
            ';
         end if;
     l_sql:=l_sql ||' LEFT JOIN dnotetext_dbt n
               ON (    t_objecttype = 3
                   AND t_notekind = 108
                   AND t_Validtodate = to_date(''31129999'',''ddmmyyyy'')
                   AND t_documentid = LPAD (c.t_partyid, 10, ''0''))
            LEFT JOIN dscqinv_dbt q
               ON (q.t_partyid = c.t_partyid AND q.t_state = 1)
      WHERE c.t_calc_sid = '''||g_calc_clientinfo||''' and c.t_ServKind <> 15 /*не срочный рынок*/ 
            ';
        if  (p_UseListClients = 1) then
              l_sql:=l_sql ||' and tt.t_calc_sid = '''||g_calc_panelcontr||''' and tt.t_setflag = chr(88) 
            ';
        end if ;
      l_sql:=l_sql ||' AND (N.T_ID IS NOT NULL OR Q.T_PARTYID IS NOT NULL)
           GROUP BY c.t_partyid, q.t_state, n.t_text ) s
          ON (c.t_calc_sid = '''||g_calc_clientinfo||''' and c.t_partyid = s.t_partyid)
         WHEN MATCHED
         THEN
            UPDATE SET c.t_isqi = s.isqi,
            C.T_TEST_RESULT = case
                        when ((s.isqi = chr(88)) and (:v_RegVal_Default_Leverage is not null))
                             then :v_RegVal_Default_Leverage
                        else s.test_result end';

     if ((p_ByStock <> 0) or (p_ByCurr <> 0) or (p_ByEDP <> 0)) THEN
        execute immediate l_sql using   v_RegVal_Default_Leverage, v_RegVal_Default_Leverage;
     END IF;

/*BOSS-4379 Доступ к сложным ФИ для ЮЛ неквалифицированных инвесторов */
   if v_RegVal_default_leverage_entity <> -1 then 
       TimeStamp_ ('Заполнение результатов теста для ЮЛ не квал.инвесторов ',p_CalcDate,NULL,SYSTIMESTAMP);
       l_sql :=
          '  MERGE /*+ index(c DDL_CLIENTINFO_DBT_IDX3) */ INTO ddl_clientinfo_dbt c USING (
                 SELECT /*+ ordered '||case when p_UseListClients = 1 then ' cardinality(tt 100) ' end||'*/ c.t_partyid
                   FROM ';
        if  (p_UseListClients = 1) then
          l_sql:=l_sql ||' DDL_PANELCONTR_DBT tt
             join ddl_clientinfo_dbt c
                on  tt.t_dlcontrid = c.t_DlContrID
             where tt.t_calc_sid = '''||g_calc_panelcontr||''' and tt.t_setflag = chr(88) and 
               ';
        else
          l_sql:=l_sql ||' ddl_clientinfo_dbt c
             where ';
         end if;
       l_sql:=l_sql ||' exists  (select 1 from dparty_dbt p where p.t_partyid = c.t_partyid and p.t_legalform = 1 ) 
              and not exists (select 1 from dpartyown_dbt o where o.t_partyid = c.t_partyid and o.t_partykind = 65)
              and not exists (select 1 from dscqinv_dbt q where q.t_partyid = c.t_partyid AND q.t_state = 1)
              and not exists (select 1 from dnotetext_dbt n where n.t_objecttype = 3 AND t_notekind = 108 AND t_Validtodate = to_date(''31129999'',''ddmmyyyy'') AND t_documentid = LPAD (c.t_client, 10, ''0''))
              and c.t_calc_sid = '''||g_calc_clientinfo||''' and c.t_ServKind <> 15 /*не срочный рынок*/ 
          GROUP BY c.t_partyid ) s      
              ON (c.t_calc_sid = '''||g_calc_clientinfo||''' and c.t_partyid = s.t_partyid)
             WHEN MATCHED
             THEN
                UPDATE SET C.T_TEST_RESULT = :v_RegVal_Default_Leverage_Entity';

         if ((p_ByStock <> 0) or (p_ByCurr <> 0) or (p_ByEDP <> 0)) THEN
            execute immediate l_sql using   v_RegVal_Default_Leverage_Entity;
         END IF; 
     end if;   


           --помечаем клиентов, по которым есть необработанные записи репликации по фондовому рынку и ошибки в собу
         TimeStamp_ ('Помечаем клиентов с необработанными ЗР для исключения из расчета',p_CalcDate,NULL,SYSTIMESTAMP);
      IF (p_ByEDP = 1) or (p_ByStock = 1) THEN
         FOR i IN
         (SELECT DISTINCT gtrec.t_clientid, 0 t_clientcontrid
                           FROM dgtrecord_dbt gtrec
                          WHERE     gtrec.t_statusid != 3        /*не Обработан*/
                                AND gtrec.t_clientid != 0
                                 AND gtrec.t_sysdate < p_CalcDate + 1
                                 AND gtrec.t_sysdate >= p_CalcDate
                                AND GTREC.T_APPLICATIONID_FROM = 112 /*SRC-12*/
                                AND (  p_UseListClients = 0 or
                                 exists(select  1 from DDL_PANELCONTR_DBT tt
                                         join DDL_CLIENTINFO_DBT c on c.t_dlcontrid = tt.t_dlcontrid
                                   where tt.t_calc_sid = g_calc_panelcontr and c.t_calc_sid = g_calc_clientinfo and c.t_ServKind <> 15 and  c.t_partyid = gtrec.t_clientid and TT.T_SETFLAG = chr(88)))
                         /*UNION ALL
                         SELECT DISTINCT tick.t_clientid, TICK.T_CLIENTCONTRID
                           FROM ddlgracc_dbt gracc, ddlgrdeal_dbt grdeal, ddl_tick_dbt tick
                          WHERE     gracc.t_grdealid = grdeal.t_id
                                AND grdeal.t_docid = tick.t_dealid
                                AND tick.t_clientid != -1
                                and tick.t_MarketID = p_MarketID
                                AND gracc.t_accnum = 2                     --БУ
                                AND grdeal.t_plandate =
                                       RSI_RSBCALENDAR.GETDATEAFTERWORKDAY (p_CalcDate, -1, GetCalendarIDByMarket(p_MarketID))
                                AND gracc.t_state = 1                       --П
                                AND rsb_secur.
                                     IsOutExchange (
                                       rsb_secur.
                                        get_OperationGroup (
                                          rsb_secur.get_OperSysTypes (tick.t_DealType, tick.t_BofficeKind))) =
                                       0 */
                                       ) LOOP
               IF i.t_clientcontrid = 0 THEN
                   UPDATE DDL_CLIENTINFO_DBT SET T_HASERRORS = CHR(88)
                      ,t_errors_reason = RSHB_RSI_SCLIMIT.add_text(t_errors_reason,'Необработанные ЗР по клиенту (фондовый рынок)')
                    WHERE t_calc_sid = g_calc_clientinfo and t_servkind = 1  and t_partyid = i.t_clientid;
               ELSE
                  UPDATE DDL_CLIENTINFO_DBT SET T_HASERRORS = CHR(88)
                      ,t_errors_reason = RSHB_RSI_SCLIMIT.add_text(t_errors_reason,'Необработанные ЗР по договору (фондовый рынок)')
                    WHERE t_calc_sid = g_calc_clientinfo and t_servkind = 1  and t_sfcontrid = i.t_clientcontrid;
               END IF;
           END LOOP;
      ELSIF ((p_ByEDP = 1) OR (p_ByCurr = 1)) THEN
         FOR i IN  (SELECT DISTINCT gtrec.t_clientid
                            FROM dgtrecord_dbt gtrec
                           WHERE     gtrec.t_statusid != 3    /*не Обработан*/
                                 AND gtrec.t_clientid != 0
                                 AND gtrec.t_sysdate < p_CalcDate + 1
                                 AND gtrec.t_sysdate >= p_CalcDate
                                 AND GTREC.T_APPLICATIONID_FROM = 118 /*SRC-118*/
                                 AND exists(select 1 from DDL_CLIENTINFO_DBT c
                                   where c.t_calc_sid = g_calc_clientinfo and  c.t_partyid = gtrec.t_clientid and c.t_ServKind <> 15 )
                                                                    )
           LOOP
              UPDATE DDL_CLIENTINFO_DBT SET T_HASERRORS = CHR(88)
                  ,t_errors_reason = RSHB_RSI_SCLIMIT.add_text(t_errors_reason,'Необработанные ЗР по клиенту (валютный рынок)')
               WHERE t_calc_sid = g_calc_clientinfo and t_ServKind = 21
                  AND t_partyid = i.t_clientid;
           END LOOP ;
      END IF;

    if p_UseListClients = 0 then
      UPDATE DDL_CLIENTINFO_DBT SET T_HASERRORS = CHR(88)
                  ,t_errors_reason = RSHB_RSI_SCLIMIT.add_text(t_errors_reason,'Договор ФЛ без признака ЕДП')
        WHERE t_calc_sid = g_calc_clientinfo and  t_dlcontrid in (SELECT /*+ ordered full(c) use_nl(p) */ c.t_dlcontrid
                FROM ddl_clientinfo_dbt c, dparty_dbt p, dpersn_dbt cc
               WHERE  c.t_calc_sid = g_calc_clientinfo and    p.t_legalform = 2
                     AND t_isedp = CHR (0)
                     AND c.t_partyid = p.t_partyid
                     AND CC.T_PERSONID = p.t_partyid
                     AND CC.T_ISEMPLOYER = CHR (0)
            GROUP BY t_dlcontrid) ;
    else
      UPDATE DDL_CLIENTINFO_DBT SET T_HASERRORS = CHR(88)
                  ,t_errors_reason = RSHB_RSI_SCLIMIT.add_text(t_errors_reason,'Договор ФЛ без признака ЕДП')
         WHERE t_calc_sid = g_calc_clientinfo 
                 and  t_dlcontrid in (SELECT /*+ ordered  cardinality(d 100) */ c.t_dlcontrid
                                      FROM DDL_PANELCONTR_DBT d, ddl_clientinfo_dbt c, dparty_dbt p, dpersn_dbt cc
                                     WHERE  c.t_calc_sid = g_calc_clientinfo and    p.t_legalform = 2
                                           AND c.t_isedp = CHR (0)
                                           AND c.t_partyid = p.t_partyid
                                           AND CC.T_PERSONID = p.t_partyid
                                           AND CC.T_ISEMPLOYER = CHR (0)
                                           AND c.t_dlcontrid = d.t_dlcontrid
                                           and d.t_calc_sid = g_calc_panelcontr and d.t_setflag = chr(88)
                                  GROUP BY c.t_dlcontrid) ;

    end if;
     /* Исключать из расчета договоры, к которым привязаны счета другого клиента (Владелец счета отличается от Клиента по договору) и выводить информацию в протокол*/
    TimeStamp_ ('Исключаем из расчета договоры, к которым привязаны счета другого клиента ',p_CalcDate,NULL,SYSTIMESTAMP);

     l_sql_where:='';
    IF (p_ByStock = 0) OR  (p_ByCurr = 0) OR (p_ByEDP = 0) OR (p_byDeriv = 0) THEN
         l_sql_where := l_sql_where || ' AND ( 1=0  ';
       IF (p_ByStock = 1) THEN
           l_sql_where := l_sql_where || ' OR (c.t_ServKind = 1) ';
       END IF;
       IF (p_ByCurr = 1) THEN
           l_sql_where := l_sql_where || '  OR (c.t_ServKind = 21) ';
       END IF;
        IF (p_byDeriv = 1) THEN
           l_sql_where := l_sql_where || '  OR (c.t_ServKind = 15) ';
        END IF;
       IF (p_ByEDP = 1) THEN
           l_sql_where := l_sql_where || ' OR  (c.t_IsEDP = CHR(88)) ';
       END IF;
       l_sql_where := l_sql_where || '  ) ';
       IF (p_byEDP = 0) THEN
          l_sql_where := l_sql_where||' AND c.t_isEDP = chr(0) ';
       END IF;
    END IF;

     l_sql :=  ' UPDATE DDL_CLIENTINFO_DBT
               SET T_HASERRORS = CHR (88)
               ,t_errors_reason = RSHB_RSI_SCLIMIT.add_text(t_errors_reason,''К договору привязан счет другого клиента'')          
             WHERE  t_calc_sid = '''||g_calc_clientinfo||''' and   t_dlcontrid in
                        ';
     IF  (p_UseListClients = 1) THEN
              l_sql:=l_sql ||' (SELECT /*+ ordered cardinality(tt 100) use_nl(dlc) */ c.t_dlcontrid
                          FROM  DDL_PANELCONTR_DBT tt,
                             ';
     else
              l_sql:=l_sql ||' (SELECT /*+ leading(sfcontr,accdoc) use_nl(accdoc) use_nl(acc) */ c.t_dlcontrid
                          FROM ';
     end if;
              l_sql:=l_sql ||' DDL_CLIENTINFO_DBT c,
                              ddlcontr_dbt dlc,
                              ddlcontrmp_dbt mp,
                              dsfcontr_dbt sfcontr,
                              dmcaccdoc_dbt accdoc,
                              daccount_dbt acc
                            WHERE  c.t_calc_sid = '''||g_calc_clientinfo||''' and   dlc.t_DlContrID = c.t_dlcontrid
                            ';
     IF  (p_UseListClients = 1) THEN
              l_sql:=l_sql ||' and tt.t_calc_sid = '''||g_calc_panelcontr||''' and tt.t_dlcontrid = c.t_DlContrID and tt.t_setflag = chr(88)
                             ';
     end if;
              l_sql:=l_sql ||' AND mp.t_DlContrID = dlc.t_DlContrID
                                  AND sfcontr.t_ID = mp.t_SfContrID
                                  AND sfcontr.t_ServKindSub = 8
                                  AND (   sfcontr.t_DateClose =
                                             TO_DATE (''01.01.0001'', ''DD.MM.YYYY'')
                                       OR sfcontr.t_DateClose >=
                                             :p_CalcDate)
                                  AND accdoc.t_owner = sfcontr.t_partyid
                                  AND accdoc.t_ClientContrID = sfcontr.t_ID
                                  AND acc.t_Chapter = accdoc.t_Chapter
                                  AND acc.t_Account = accdoc.t_Account
                                  AND acc.t_Code_Currency = accdoc.t_Currency
                                  AND accdoc.t_catid = 70
                                  AND acc.t_Open_Date <=
                                         :v_restdate
                                  AND (   acc.t_Close_Date =
                                             TO_DATE (''01.01.0001'', ''DD.MM.YYYY'')
                                       OR acc.t_Close_Date >=
                                             :p_CalcDate)   --!!!
                                  AND (   accdoc.t_disablingDate =
                                             TO_DATE (''01.01.0001'', ''DD.MM.YYYY'')
                                       OR accdoc.t_disablingDate >=
                                             :p_CalcDate)
                                  AND acc.t_client != sfcontr.t_partyid '||l_sql_where||
           ' GROUP BY c.t_dlcontrid )';
            execute immediate l_sql using  p_CalcDate, v_restdate, p_CalcDate, p_CalcDate;

   end;

  function GetLotMaxChangeDate return date result_cache as
  MaxChangeDate date ;
  begin
    Select /*+ parallel(4) full(s)*/ NVL(max(t_changedate), TO_DATE ('01.01.0001', 'DD.MM.YYYY')) INTO MaxChangeDate
       from dpmwrtsum_dbt s
       where s.t_Party > 0;
    return MaxChangeDate ;
  end;


  PROCEDURE ClearPlanSumCur (p_CalcDate IN DATE,p_ByCurr IN NUMBER, p_ByEDP IN NUMBER, p_MarketID IN NUMBER, p_UseListClients IN NUMBER,p_action number default 101)
   AS
   l_sql  VARCHAR2(4000) := '';
  BEGIN
   TimeStamp_ ('Отбор требований и обязательств ',p_CalcDate,NULL,SYSTIMESTAMP,/* null,*/p_action);
   IF (p_ByCurr <> 0 or p_ByEDP = 1)  and  p_MarketID = GetMicexID()  THEN
         IF p_UseListClients = 1 THEN
           l_sql := l_sql || ' and t_dlcontrid in (select tt.t_dlcontrid from DDL_PANELCONTR_DBT tt where tt.t_calc_sid = '''||g_calc_panelcontr||''' and tt.t_setflag = chr(88) )';
         END IF;
        --EXECUTE IMMEDIATE 'TRUNCATE TABLE dlimit_dltick_dbt';
        LockRecordsFrom('dlimit_cmtick_dbt','t_calc_sid = '''||g_calc_clientinfo||''''||l_sql );
        EXECUTE IMMEDIATE 'delete dlimit_cmtick_dbt where t_calc_sid = '''||g_calc_clientinfo||''''||l_sql;
    end if ;
  END;

  procedure CollectPlanSumCur(p_CalcDate       in date
                      ,p_ByCurr        in number
                      ,p_ByEDP          in number
                      ,p_MarketID       in number
                      ,p_UseListClients in number) as
    l_sql         varchar2(32000) := '';
  begin
    ts_ := SYSTIMESTAMP;
     IF (p_ByCurr <> 0 or p_ByEDP = 1) AND p_MarketID = GetMicexID()
     then
        -- Производная от  GetSumPlanCashPM 
       l_sql:='insert into dlimit_cmtick_dbt partition ('||GetPartition_calc_sid(g_calc_clientinfo)||
       ') ( t_dealid,t_dockind,t_sumtype,t_clientid,t_dlcontrid,t_clientcontrid,t_plan_date,t_isfactpaym,t_fiid,t_plan_plus,t_plan_minus,t_calc_sid)
   with sfcontr as ( select /*+ materialize */ distinct t_sfcontrid, t_dlcontrid, t_client, t_accountid, t_account, t_time306, t_Money306
                 ,t_code_currency, t_leverage, t_mpcode, t_trdaccid, t_servkind, t_servkindsub, t_isblocked, t_isedp,t_test_result
                 ,t_marketid, t_haserrors, t_ImplKind,t_tag, t_Firm_ID, t_ekk
               from ddl_clientinfo_dbt c where t_calc_sid = ''' || g_calc_clientinfo || ''' and T_MARKETID = '||p_MarketID||' and t_servkind = 21 
                ';
         if p_UseListClients = 1
         then
           l_sql := l_sql || '  AND exists (select 1 from DDL_PANELCONTR_DBT tt where tt.t_calc_sid = ''' || g_calc_panelcontr ||
                 ''' and tt.t_setflag = chr(88) and tt.t_dlcontrid = c.t_dlcontrid)';
           end if;
         l_sql := l_sql ||')
  select  t_dealid, t_dockind, iskom t_sumtype , t_client t_clientid ,t_dlcontrid , t_clientcontr t_clientcontrid , t_valuedate t_plan_date 
      , t_isfactpaym, t_payfiid t_fiid  , sum(sum1) t_plan_plus,  sum(sum0) t_plan_minus,'''||g_calc_clientinfo||'''
  from ( SELECT /*+ leading(pm,nfi,bafi,ndeal) inex(ndeal DDVNDEAL_DBT_IDX0)*/ 
        nfi.t_dealID, ndeal.t_DocKind, 0 isKom, ndeal.t_Client,pm.t_dlcontrid ,ndeal.t_ClientContr, pm.t_valueDate, pm.t_isfactpaym, pm.t_PayFIID 
       ,decode(pm.isReg,1,pm.t_Amount,0) Sum1,  decode(pm.isReg,0,pm.t_Amount,0) Sum0
       FROM (select /*+ index(pm DPMPAYM_DBT_IDX10)*/ sf.t_client p_Client,sf.t_dlcontrid,sf.t_SfcontrID p_ClientContrID, 1 isReg, pm.* from sfcontr sf
          join  dpmpaym_dbt pm on ( sf.t_account = pm.t_ReceiverAccount and pm.t_PayFIID = sf.t_code_currency)
          where pm.t_isfactpaym = chr(0)  or  pm.t_valueDate >= :p_CalcDate  
          union all
         select /*+ index(pm DPMPAYM_DBT_IDXF)*/  sf.t_client p_Client,sf.t_dlcontrid, sf.t_SfcontrID  p_ClientContrID, 0 isReg, pm.* from sfcontr sf
          join dpmpaym_dbt pm on ( sf.t_account = pm.t_PayerAccount and pm.t_PayFIID = sf.t_code_currency)
          where pm.t_isfactpaym = chr(0)  or  pm.t_valueDate >= :p_CalcDate  
        ) pm,
        ddvndeal_dbt ndeal,
        ddvnfi_dbt nfi,
        dfininstr_dbt bafi
       WHERE     ndeal.t_Client = pm.p_Client 
        AND ndeal.t_ClientContr = pm.p_ClientContrID
        AND ndeal.t_DocKind IN (4813, 199) -- Конверсионная сделка ФИСС и КО /*bpv и плюс СВОПы*/
        AND ndeal.t_Date < :p_CalcDate
        AND ndeal.t_Sector = CHR (88)
        AND ndeal.t_MarketKind IN (2) -- валютный , возможно нужен еще 5 - все(единый пул обеспечения)
        AND nfi.t_dealID = ndeal.t_ID
        AND nfi.t_Type = 0
        AND bafi.t_FIID = nfi.t_FIID
        AND bafi.t_fi_kind = 1                                  -- валюта
        AND pm.t_DocKind = ndeal.t_DocKind
        AND pm.t_DocumentID = nfi.t_dealID
      union all
      select q1.t_dealID,q1.t_DocKind, 1 isKom, q1.t_Client ,q1.t_dlcontrid,q1.t_ClientContr,  q1.t_paydate, chr(0) t_isfactpaym, q1.t_PayFIID ,
         0 as Sum1, NVL(q1.t_sum, 0) - NVL((select sum(PM.T_AMOUNT) as sum0
                from dpmpaym_dbt pm
               where pm.t_DocKind = q1.t_DocKind
                 and PM.T_FIID = q1.T_FIID_COMM
                 and pm.t_DocumentID = q1.T_DOCID
                 and pm.t_purpose = q1.ppurpose
                 and PM.T_PAYER = q1.t_Client),0) as sum0 
       from (select /*+ leading(sf,ndeal) use_nl(ndeal) index(ndeal DDVNDEAL_DBT_IDX4)*/
               nfi.t_dealID 
              ,ndeal.t_DocKind
              ,comm.t_docid
              ,sfc.T_FIID_COMM
              ,preceiverid.ppurpose
              ,nfi.t_paydate
              ,sf.t_code_currency as t_PayFIID
              ,ndeal.t_Client
              ,sf.t_dlcontrid
              ,ndeal.t_ClientContr 
              ,sum(comm.t_sum) as t_sum
             from ddlcomis_dbt comm                           
              ,dsfcomiss_dbt sfc
              ,(select OW.T_PARTYID
                     ,40 ppurpose
                 from DPARTYOWN_DBT ow
                where OW.T_PARTYKIND = 3
               union --!!! distinct 
               select d.t_PartyID
                     ,72
                 from ddp_dep_dbt d) preceiverid
              ,ddvndeal_dbt ndeal
              ,sfcontr sf
              ,ddvnfi_dbt nfi
              ,dfininstr_dbt bafi
             where ndeal.t_Client = sf.t_client 
               and ndeal.t_ClientContr = sf.t_SfcontrID 
               and ndeal.t_DocKind in (4813, 199) -- Конверсионная сделка ФИСС и КО /*bpv и плюс СВОПы*/
               and ndeal.t_Date < :p_CalcDate
               and ndeal.t_Sector = CHR(88)
               and ndeal.t_MarketKind in (2)
               and nfi.t_dealID = ndeal.t_ID
               and nfi.t_Type = 0
               and bafi.t_FIID = nfi.t_FIID
               and bafi.t_fi_kind = 1 -- валюта
               and COMM.T_DOCID = ndeal.t_id
               and COMM.T_DOCKIND in (4813, 199)
               and sfc.t_number = comm.t_comnumber
               and sfc.T_FEETYPE = comm.T_FEETYPE
               and preceiverid.t_partyid = sfc.t_receiverid
               and comm.T_ISBANKEXPENSES <> CHR(88)
               and (select sfc2.t_fiid_comm
                      from dsfcomiss_dbt sfc2
                     where sfc2.t_number = sfc.t_number
                       and sfc2.t_servicekind <> 1) = sf.t_code_currency
                group by nfi.t_dealID, ndeal.t_DocKind, comm.t_docid, sfc.T_FIID_COMM, preceiverid.ppurpose , nfi.t_paydate
                       ,sf.t_code_currency, ndeal.t_Client, sf.t_dlcontrid, ndeal.t_ClientContr ) q1 
      
      ) 
   where sum1 != 0 or sum0 != 0
   group by t_dealID,t_DocKind, isKom, t_Client ,t_dlcontrid, t_ClientContr, t_valueDate, t_isfactpaym, t_PayFIID' ;
       execute immediate l_sql
            using  p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate;
   
    end if;
  end;
  


  PROCEDURE ClearTickTmp (p_CalcDate IN DATE,p_ByStock IN NUMBER, p_ByEDP IN NUMBER, p_MarketID IN NUMBER, p_UseListClients IN NUMBER,p_action number default 101)
   AS
   l_sql  VARCHAR2(4000) := '';
  BEGIN
   TimeStamp_ ('Отбор сделок ',p_CalcDate,NULL,SYSTIMESTAMP,/* null,*/p_action);
   if ((p_ByStock = 1) or (p_ByEDP = 1))
    then
        IF p_MarketID <> -1 THEN
           l_sql := l_sql || ' AND  t_MarketID = '||p_MarketID ;
        end if;
        IF p_UseListClients = 1 THEN
           l_sql := l_sql || '  AND t_dlcontrid in (select tt.t_dlcontrid from DDL_PANELCONTR_DBT tt where tt.t_calc_sid = '''||g_calc_panelcontr||''' and tt.t_setflag = chr(88) )';
         END IF;
        --EXECUTE IMMEDIATE 'TRUNCATE TABLE dlimit_dltick_dbt';
        LockRecordsFrom('dlimit_dltick_dbt','t_calc_sid = '''||g_calc_clientinfo||''''||l_sql );
        EXECUTE IMMEDIATE 'delete dlimit_dltick_dbt where t_calc_sid = '''||g_calc_clientinfo||''''||l_sql;
    end if ;
  END;

  procedure SetTickTmp(p_CalcDate       in date
                      ,p_ByStock        in number
                      ,p_ByEDP          in number
                      ,p_MarketID       in number
                      ,p_UseListClients in number) as
    l_sql_ins     varchar2(32000) := '';

    l_sql         varchar2(32000) := '';
    MaxChangeDate date;
  begin
    ts_ := SYSTIMESTAMP;
    if ((p_ByStock = 1) or (p_ByEDP = 1))
    then
      MaxChangeDate := GetLotMaxChangeDate;
      l_sql_ins := ' INSERT INTO dlimit_dltick_dbt (t_bofficekind,
                                              t_dealid,
                                              t_dealtype,
                                              t_clientcontrid,
                                              t_clientid,
                                              t_trademode,
                                              t_tradechr,
                                              t_dealdate,
                                              t_partyid,
                                              t_ispartyclient,
                                              t_specrepo,
                                              t_marketid,
                                              t_dlcontrid,
                                              t_calc_sid)
                        with contr as (select /*+ materialize*/
                                        distinct t_sfcontrid
                                                ,t_dlcontrid
                                                ,t_client
                                                ,t_servkind
                                                ,t_servkindsub
                                                ,t_isblocked
                                                ,t_isedp
                                                ,t_marketid
                                          from ddl_clientinfo_dbt c
                                         where c.t_calc_sid = ''' || g_calc_clientinfo || ''' and t_servkind = 1
                                             ';
      if p_UseListClients = 1
      then
        l_sql_ins := l_sql_ins || '  AND exists (select 1 from DDL_PANELCONTR_DBT tt where tt.t_calc_sid = ''' || g_calc_panelcontr ||
                 ''' and tt.t_setflag = chr(88) and tt.t_dlcontrid = c.t_dlcontrid)';
      end if;
      if p_MarketID <> -1
      then
        l_sql_ins := l_sql_ins || ' AND  c.t_MarketID = :p_MarketID ';
      end if;
      l_sql_ins := l_sql_ins || 'order by t_client )
              ';
      l_sql := ' SELECT /*+ %HINT% */ DISTINCT tk.t_BOfficeKind,
                                        tk.t_DealID,
                                        tk.t_dealtype,
                                        tk.t_ClientContrID,
                                        tk.t_ClientID,
                                        (SELECT t_numinlist
                                           FROM DOBJATTR_DBT
                                          WHERE     t_objecttype = 101 AND t_groupid = 106
                                                AND t_attrid = RSB_SECUR.GetMainObjAttr (
                                                                  101,LPAD (tk.T_DEALID, 34, ''0''),
                                                                  106,:p_CalcDate)) t_trademode,
                                         SUBSTR ((SELECT t_numinlist
                                           FROM DOBJATTR_DBT
                                          WHERE     t_objecttype = 101 AND t_groupid = 106
                                                AND t_attrid = RSB_SECUR.GetMainObjAttr (
                                                                  101, LPAD (tk.T_DEALID, 34, ''0''),
                                                                  106, :p_CalcDate)), 1, 1) t_tradechr,
                                        t_DEALdate,
                                        t_partyid,
                                        t_ispartyclient,
                                        NVL ( (SELECT CHR (88) FROM DOBJATTR_DBT
                                                WHERE     t_objecttype = 101 AND t_groupid = 103
                                                      AND t_attrid = RSB_SECUR.GetMainObjAttr (
                                                                        101,LPAD (tk.T_DEALID, 34, ''0''),
                                                                        103, :p_CalcDate)),
                                             CHR (0) ) t_specrepo,
                                         c.t_marketid,
                                         c.t_dlcontrid,
                                         ''' || g_calc_clientinfo || '''
                          FROM  contr c, ddl_tick_dbt tk %ADDTABLE%
                             WHERE tk.t_BOfficeKind = 101
                               AND tk.t_ClientID = c.t_client
                               AND RSB_SECUR.GetMainObjAttr (101,LPAD (tk.T_DEALID, 34, ''0''),210, :p_CalcDate) <> 1
                               AND tk.t_ClientContrID = c.t_sfcontrid
                               AND tk.t_DealDate < :p_CalcDate
                   AND tk.t_DealDate <> TO_DATE (''31.12.2018'', ''dd.mm.yyyy'') ';
      if MaxChangeDate < p_CalcDate
      then
        l_sql:= replace(l_sql,'%ADDTABLE%') ;
        l_sql:= replace(l_sql,'%HINT%','ordered ' || case
                 when p_UseListClients = 1 then
                  'cardinality(c 100) index(tk DDL_TICK_DBT_IDX6)'
                 else
                  'use_hash(tk) index(tk DDL_TICK_DBT_IDX7) '
                  end );
        l_sql := l_sql || ' and  TK.T_DEALSTATUS <> 20 ';
      else
        l_sql := replace(replace(l_sql,'%ADDTABLE%'),'%HINT%','ordered ' || case
                 when p_UseListClients = 1 then
                  'cardinality(c 100) index(tk DDL_TICK_DBT_IDX6)'
                 else
                  'use_hash(tk) index(tk DDL_TICK_DBT_IDX7) '
                 end ) || 'and  TK.T_DEALSTATUS <> 20 
         union all '||replace(replace(l_sql,'%ADDTABLE%',', DDLRQ_DBT q '),'%HINT%', case
                 when p_UseListClients = 1 then
                  ' ordered cardinality(c 100) index(tk DDL_TICK_DBT_IDX6) '
                 else
                  ' leading (q,tk,c ) cardinality(q 100) index(q DDLRQ_DBT_USR2) index(tk DDL_TICK_DBT_IDX0)'
                 end )|| ' and  TK.T_DEALSTATUS =  20 and 
                 TK.T_DEALID = q.T_DOCID and  q.T_DOCKIND = 101 AND q.T_PLANDATE > = :p_CalcDate ';
        
        /*l_sql := l_sql || ' and decode( TK.T_DEALSTATUS,20,(SELECT MAX (T_PLANDATE)
                                         FROM DDLRQ_DBT WHERE T_DOCID = TK.T_DEALID
                                              AND T_DOCKIND = TK.T_BOFFICEKIND),:p_CalcDate ) >= :p_CalcDate ';*/
      end if;
      l_sql := l_sql_ins||l_sql ;
      if p_MarketID <> -1
      then
        if MaxChangeDate < p_CalcDate
        then
          execute immediate l_sql
            using p_MarketID, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate;
        else
          execute immediate l_sql
            using p_MarketID, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate;
        end if;
      else
        if MaxChangeDate < p_CalcDate
        then
          execute immediate l_sql
            using  p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate;
        else
          execute immediate l_sql
            using  p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate, p_CalcDate;
        end if;
      end if;
    end if;
    /*  TimeStamp_ (
    ' Сбор данных по сделкам с ценными бумагами',
    p_CalcDate,
    ts_,
    SYSTIMESTAMP);*/
  end;

   PROCEDURE SetAccountTmp (p_CalcDate IN DATE, p_ByEDP IN NUMBER)
   AS
      p_CheckDate   DATE;

      TYPE limacc_t IS TABLE OF DDL_DIFFERENTID%ROWTYPE
                          INDEX BY BINARY_INTEGER;

      p_limacc      limacc_t;
   BEGIN
      --    DELETE FROM DDL_DIFFERENTID_TMP;
      LockRecordsFrom('DDL_DIFFERENTID');

      DELETE FROM DDL_DIFFERENTID;

      --execute immediate 'truncate DDL_differentid';
      p_CheckDate := TO_DATE ('31.12.9999', 'DD.MM.YYYY');

      SELECT /*+ parallel(c,10)*/  t_ClientID, tk.t_ClientContrID, RQ.T_FIID
        BULK COLLECT INTO p_limacc
        FROM ddlrq_dbt rq, dlimit_dltick_dbt tk, ddl_clientinfo_dbt c
       WHERE  c.t_calc_sid = g_calc_clientinfo and    tk.t_bofficekind <> RSB_SECUR.DL_RETIREMENT --bpv отражения клиентских погашений по счетам БУ в софр нет, поэтому их не берем
             AND tk.t_DealDate <= p_CalcDate - 1 /*p_CheckDate  нельзя учитывать еще не заключенные сделки*/
             AND tk.t_ClientContrID = c.t_SfContrID
             AND CASE WHEN p_ByEDP = 1 THEN CHR(88) ELSE CHR(0) END = c.t_IsEDP
             AND rq.t_DocKind = tk.t_BOfficeKind
             AND rq.t_DocID = tk.t_DealID
             AND rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY
             AND rq.t_PlanDate <= p_CheckDate
             AND rq.t_Kind IN
                    (RSI_DLRQ.DLRQ_KIND_REQUEST, RSI_DLRQ.DLRQ_KIND_COMMIT);


      IF p_limacc.COUNT > 0
      THEN
         FORALL indx IN p_limacc.FIRST .. p_limacc.LAST
            --         INSERT INTO DDL_DIFFERENTID_TMP
            INSERT INTO DDL_DIFFERENTID
                 VALUES p_limacc (indx);
      END IF;
   END;

   PROCEDURE ClearFIIDTmp( p_MarketID IN NUMBER, p_UseListClients IN NUMBER)
   AS
   l_sql  VARCHAR2(4000) := '';
   BEGIN

        IF p_MarketID <> -1 THEN
           l_sql := l_sql || ' AND  t_MarketID = '||p_MarketID ;
        end if;
        IF p_UseListClients = 1 THEN
           l_sql := l_sql || '  AND t_dlcontrid in (select tt.t_dlcontrid from DDL_PANELCONTR_DBT tt where tt.t_calc_sid = '''||g_calc_panelcontr||''' and tt.t_setflag = chr(88) )';
         END IF;
        LockRecordsFrom('DDL_FIID_DBT','t_calc_sid = '''||g_calc_clientinfo||''''||l_sql );
        EXECUTE IMMEDIATE 'delete DDL_FIID_DBT where t_calc_sid = '''||g_calc_clientinfo||''''||l_sql;
    END;
   
   PROCEDURE SetFIIDTmp (p_CalcDate IN DATE, p_ByEDP IN NUMBER,p_MarketID IN NUMBER, p_UseListClients IN NUMBER)
   AS
      p_CheckDate   DATE;

      TYPE limacc_t IS TABLE OF DDL_FIID_DBT%ROWTYPE
                          INDEX BY BINARY_INTEGER;

      p_limacc      limacc_t;
   BEGIN

      p_CheckDate := TO_DATE ('31.12.9999', 'DD.MM.YYYY');

      SELECT /*+ ordered cardinality(q 100)*/ DISTINCT q.t_ClientID, q.t_ClientContrID, rq.t_FIID,p_MarketID,c.t_dlcontrid, g_calc_clientinfo
        BULK COLLECT INTO p_limacc
        FROM dlimit_dltick_dbt q,  ddl_clientinfo_dbt c, ddlrq_dbt rq
       WHERE q.t_calc_sid = g_calc_clientinfo and  c.t_calc_sid = g_calc_clientinfo and rq.t_DocKind = q.t_BOfficeKind AND rq.t_DocID = q.t_DealID
             AND q.t_ClientContrID = c.t_SfContrID
             and q.t_marketid = decode(p_MarketID,-1,q.t_marketid,p_MarketID)
             --AND CASE WHEN p_ByEDP = 1 THEN CHR(88) ELSE CHR(0) END = c.t_IsEDP
             AND rq.t_Type IN
                    (RSI_DLRQ.DLRQ_TYPE_DELIVERY,
                     RSI_DLRQ.DLRQ_TYPE_COMPDELIVERY)
             AND rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
             AND rq.t_state <> -1 --bpv техничский статус чтобы отрубить старые неисполненные ТО
             AND rq.t_Kind IN
                    (RSI_DLRQ.DLRQ_KIND_REQUEST, RSI_DLRQ.DLRQ_KIND_COMMIT)
             AND rq.t_PlanDate <= p_CheckDate
             AND (rq.t_FactDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                  OR rq.t_FactDate >= p_CalcDate
                  OR NOT EXISTS
                            (SELECT /*+ index(lot DPMWRTSUM_DBT_IDX1 )*/ 1
                               FROM dpmwrtsum_dbt lot
                              WHERE     lot.t_DocKind in (29, 135)
                                    AND lot.t_DocID = rq.t_ID
                                    AND lot.t_State = 1
                                    AND lot.t_Party = q.t_ClientID
                                    AND lot.t_Contract = q.t_ClientContrID))
             AND (p_UseListClients = 0 OR  exists (select 1
                                                       from DDL_PANELCONTR_DBT d
                                                       where d.t_calc_sid = g_calc_panelcontr and c.t_dlcontrid = d.t_dlcontrid and d.t_setflag = chr(88))) ;

      IF p_limacc.COUNT > 0
      THEN
         FORALL indx IN p_limacc.FIRST .. p_limacc.LAST
            INSERT INTO DDL_FIID_DBT
                 VALUES p_limacc (indx);
      END IF;
   END;
  
   PROCEDURE ClearLotTmp( p_CalcDate IN DATE,p_MarketID IN NUMBER, p_UseListClients IN NUMBER)
   AS
   l_sql  VARCHAR2(4000) := '';
   BEGIN
        TimeStamp_ ('Отбор лотов',p_CalcDate,NULL,SYSTIMESTAMP,/* null,*/ 102);

        IF p_MarketID <> -1 THEN
           l_sql := l_sql || ' AND  t_MarketID = '||p_MarketID ;
        end if;
        IF p_UseListClients = 1 THEN
           l_sql := l_sql || '  AND t_dlcontrid in (select tt.t_dlcontrid from DDL_PANELCONTR_DBT tt where tt.t_calc_sid = '''||g_calc_panelcontr||''' and tt.t_setflag = chr(88) )';
         END IF;
        LockRecordsFrom('D_LIMITLOTS_TMP','t_calc_sid = '''||g_calc_clientinfo||''''||l_sql );
        EXECUTE IMMEDIATE 'delete D_LIMITLOTS_TMP where t_calc_sid = '''||g_calc_clientinfo||''''||l_sql;
   END ;
   
   PROCEDURE SetLotTmp (p_CalcDate IN DATE, p_ByEDP IN NUMBER,p_MarketID IN NUMBER, p_UseListClients IN NUMBER)
   AS
      p_CheckDate   DATE;
      MaxChangeDate DATE;
      v_sql varchar2(32000);
   BEGIN


    ts_ := SYSTIMESTAMP;

    MaxChangeDate := GetLotMaxChangeDate;

    v_sql := ' INSERT INTO D_LIMITLOTS_TMP
     with sfcont as (select /*+ materialize ordered ';

    IF p_UseListClients <> 0 THEN
      v_sql :=v_sql||'index(c DDL_CLIENTINFO_DBT_IDX2) ';
    end if;

    v_sql := v_sql||'*/ distinct c.t_sfcontrid,  c.t_dlcontrid, c.t_client
                        , c.t_servkind, c.t_servkindsub, c.t_isblocked, c.t_isedp, c.t_marketid
                     from ';
    IF p_UseListClients <> 0 THEN
       v_sql :=v_sql||' DDL_PANELCONTR_DBT tt
                       join ddl_clientinfo_dbt c
                          on tt.t_calc_sid = '''||g_calc_panelcontr||''' and tt.t_dlcontrid = c.t_dlcontrid
                             and TT.T_SETFLAG = chr(88)
                       ';
    else
       v_sql :=v_sql||'ddl_clientinfo_dbt c
                      ';
    end if;
    v_sql :=v_sql||' where c.t_calc_sid = '''||g_calc_clientinfo||'''';
    IF p_MarketID <> -1 THEN
       v_sql :=v_sql || ' AND  t_MarketID = '||p_MarketID ;
    end if;
    v_sql :=v_sql||' and c.t_Client > 0
                         and c.t_servkindsub = 8
                         and c.t_servkind = 1 )
           SELECT /*+ ordered cardinality(c 100) use_nl(lot) */ NVL (SUM (lot.t_Amount), 0) SumQuantity,
                lot.t_Party,
                lot.t_Contract,
                lot.t_FIID,
                c.t_marketid,
                c.t_dlcontrid,
                '''||g_calc_clientinfo||'''
              FROM sfcont c,'||case when MaxChangeDate >= p_CalcDate then 'v_scwrthistex'
                                    else 'dpmwrtsum_dbt' end ||' lot
             WHERE  lot.t_Amount > 0
                   AND lot.t_DocKind in (29, 135)
                   AND lot.t_DocID > 0
                   AND lot.t_state = 1
                   AND lot.t_contract =  c.t_SfContrID
                   AND lot.t_Party = c.t_Client
                   AND lot.t_portfolio = 0
                   AND lot.t_Buy_Sale IN (:PM_WRITEOFF_SUM_BUY ,:PM_WRITEOFF_SUM_BUY_BO)
              ';
     if MaxChangeDate >= p_CalcDate then
       v_sql :=v_sql||'    AND lot.t_ChangeDate < :p_CalcDate
                    AND decode(lot.t_Instance,(select max(bc.t_Instance) from v_scwrthistex bc
                                where bc.t_SumID = lot.t_SumID and bc.t_ChangeDate < :p_CalcDate),1,0) = 1
              ';
     end if;
     v_sql :=v_sql||'GROUP BY lot.t_Party, lot.t_Contract, lot.t_FIID,c.t_marketid,c.t_dlcontrid';
     --dbms_output.put_line( v_sql);
     IF MaxChangeDate >= p_CalcDate then
          execute immediate v_sql
           using RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY,RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO,p_CalcDate,p_CalcDate;
      else
          execute immediate v_sql
           using RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY,RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO;
      end if;
   END;

   PROCEDURE RSI_DeleteZeroSecurLimByCur (p_CalcDate IN DATE)
   AS
   BEGIN
       DELETE FROM DDL_LIMITSECURITES_DBT limDepo
            WHERE limDepo.t_date = p_CalcDate
                  /*общее условие, что стираем только среди нулевых лимитов не ЕДП, начало*/
                  AND limDepo.T_MARKET_KIND = 'валютный'
                  AND (limDepo.T_MoneyConsolidated <> CHR (88)
                       OR limDepo.T_MoneyConsolidated IS NULL)
                  AND limDepo.t_seccode =
                         (SELECT t_code
                            FROM dobjcode_dbt
                           WHERE     t_objecttype = 9
                                 AND t_codekind = 106
                                 AND t_state = 0
                                 AND t_objectid = 7)
                  AND limDepo.t_open_balance = 0
                  /*общее условие, что стираем только среди нулевых лимитов не ЕДП, конец*/
                  /*частное условие, что стираем только те, у которых не существует записей в DEPO с ненулевым остатоком, начало*/
                  AND NOT EXISTS
                             (SELECT 1
                                FROM  DDL_LIMITCASHSTOCK_DBT limMon
                               WHERE  limMon.t_date = p_CalcDate
                                     AND limMon.T_MARKET_KIND = 'валютный'
                                     AND limMon.t_open_balance <> 0
                                     AND limMon.T_CLIENT_CODE = limDepo.T_CLIENT_CODE);
                  /*частное условие, что стираем только те, у которых не существует записей в DEPO с ненулевым остатоком, конец*/
   END;

   PROCEDURE RSI_DeleteClientErr (p_CalcDate IN DATE)
   AS
   BEGIN
 
      DELETE FROM DDL_LIMITSECURITES_DBT
            WHERE T_MARKET_KIND = 'валютный'
                  AND t_Client IN
                         (SELECT DISTINCT gtrec.t_clientid
                            FROM dgtrecord_dbt gtrec
                           WHERE     gtrec.t_statusid != 3    /*не Обработан*/
                                 AND gtrec.t_clientid != 0
                                  AND gtrec.t_sysdate < p_CalcDate + 1
                                 AND gtrec.t_sysdate >= p_CalcDate
                                 AND GTREC.T_APPLICATIONID_FROM = 118 /*SRC-118*/
                                                                     );

 
      DELETE FROM DDL_LIMITCASHSTOCK_DBT
            WHERE T_MARKET_KIND = 'валютный'
                  AND t_Client IN
                         (SELECT DISTINCT gtrec.t_clientid
                            FROM dgtrecord_dbt gtrec
                           WHERE     gtrec.t_statusid != 3    /*не Обработан*/
                                 AND gtrec.t_clientid != 0
                                 AND gtrec.t_sysdate < p_CalcDate + 1
                                 AND gtrec.t_sysdate >= p_CalcDate
                                 AND GTREC.T_APPLICATIONID_FROM = 118 /*SRC-118*/
                                                                     );
   END;

   PROCEDURE RSI_CreateSecurLimitsCur (p_CalcDate IN DATE, p_IsKind2 IN NUMBER, p_DepoAcc IN VARCHAR2)
   AS
   BEGIN

      DELETE FROM ddl_limitsecurites_dbt
            WHERE t_Market_Kind = 'валютный'; -- для валютного только биржа

      RSI_CreateSecurLimByKindCur (p_CalcDate, 0, p_DepoAcc);
      RSI_CreateSecurLimByKindCur (p_CalcDate, 1, p_DepoAcc);

      IF (p_IsKind2 = 1)
      THEN                                -- формировать лимиты для kind 2,365
         RSI_CreateSecurLimByKindCur (p_CalcDate, 2, p_DepoAcc);
         RSI_CreateSecurLimByKindCur (p_CalcDate, 365, p_DepoAcc);
      END IF;

      /*bpv удалить лишние по нулевым клиентам*/

      RSI_DeleteZeroSecurLimByCur (p_CalcDate);
   END;                                               -- RSI_CreateSecurLimits

   PROCEDURE RSI_CheckFutureMarkLimits (p_CalcDate IN DATE)
   AS
   BEGIN
       LockRecordsFrom('DDL_LIMITFUTURMARK_DBT');
       commit;
   END ;
   
   PROCEDURE RSI_CreateFutureMarkLimits (p_CalcDate IN DATE, p_UseListClients IN NUMBER)
   AS
   BEGIN
      IF p_UseListClients = 0 THEN
         EXECUTE IMMEDIATE 'TRUNCATE TABLE DDL_LIMITFUTURMARK_DBT';
      ELSE

         DELETE FROM DDL_LIMITFUTURMARK_DBT
      WHERE t_date <> p_CalcDate OR t_internalaccount IN
               (SELECT c.t_accountid
                  FROM ddl_clientinfo_dbt c,
                       ddl_panelcontr_dbt p
                 WHERE  p.t_calc_sid = g_calc_panelcontr and c.t_calc_sid = g_calc_clientinfo and p.t_dlcontrid = c.t_dlcontrid
                       AND p.T_CLIENTID = c.T_PARTYID
                       AND P.T_SETFLAG = CHR (88)
                       AND c.T_SERVKIND = 15);
      END IF;

      RSI_CreateFutureMarkLim (p_CalcDate, p_UseListClients);
   END;                                               -- RSI_CreateSecurLimits

   PROCEDURE RSI_CreateLimits (p_MarketID     IN NUMBER,
                               p_MarketCode   IN VARCHAR2,
                               p_CalcDate     IN DATE,
                               p_ByStock      IN NUMBER,
                               p_ByCurr       IN NUMBER,
                               p_ByDeriv      IN NUMBER,
                               p_ByEDP        IN NUMBER default 0,
                               p_UseListClients IN NUMBER default 0
                              )
   AS
      v_job1           VARCHAR2 (20) := 'JOB_LIM_1';
      v_job2           VARCHAR2 (20) := 'JOB_LIM_2';
      v_state          VARCHAR2 (50);
      v_IsDepo         NUMBER := 0;
      v_IsKind2        NUMBER := 0;
      v_DepoAcc        VARCHAR2 (20) := CHR (1);

    BEGIN
   --- НЕ ВНОСИТЬ ИЗМЕНЕНИЯ В КОД ПРОЦЕДУРЫ БЕЗ КОРРЕКТИРОВКИ ПАКЕТА IT_LIMIT

      TimeStamp_ (
         'Запуск. Операционист  ' || RsbSessionData.Oper||'. '||'p_ByStock=' ||p_ByStock||'  '||'p_ByCurr=' ||p_ByCurr||'p_ByDeriv=' ||p_ByDeriv||'  '||'p_ByEDP=' ||p_ByEDP||'  '||'p_MarketID=' ||p_MarketID||'  '||'p_UseListClients=' ||p_UseListClients,
         p_CalcDate,
         NULL,
         SYSTIMESTAMP);


 
      IF ((p_ByStock <> 0) OR (p_ByCurr <> 0) OR (p_ByEDP <> 0) OR (p_ByDeriv <> 0))    THEN
         ts_ := SYSTIMESTAMP;
        IF (p_ByStock <> 0) THEN
            RSI_ClearContrTable( p_MarketID,p_MarketCode,p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients); 
            RSI_FillContrTablenotDeriv ( p_MarketID,p_MarketCode,p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients);
            RSI_FillContrTablebyDeriv ( p_MarketID,p_MarketCode,p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients);
            Gather_Table_Stats('DDL_CLIENTINFO_DBT');
            RSI_FillContrTableAcc     ( p_MarketID,p_MarketCode,p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients);
            RSI_FillContrTableCOM ( p_MarketID,p_MarketCode,p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients);
         ELSE
            RSI_ClearContrTable(p_MarketID,p_MarketCode,p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
            RSI_FillContrTablenotDeriv ( p_MarketID,p_MarketCode,p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
            RSI_FillContrTablebyDeriv (  p_MarketID,p_MarketCode,p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
            Gather_Table_Stats('DDL_CLIENTINFO_DBT');
            RSI_FillContrTableAcc     (  p_MarketID,p_MarketCode,p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
            RSI_FillContrTableCOM  (  p_MarketID,p_MarketCode,p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
         END IF;
         TimeStamp_ ('Контроль данных по договорам '||p_MarketCode, p_CalcDate, NULL, SYSTIMESTAMP);
         RSI_CheckContrTable(p_CalcDate , p_ByStock , p_ByCurr  , p_ByEDP, p_ByDeriv, p_UseListClients );
         TimeStamp_ ('RSI_CheckContrTable  Завершена  '||p_MarketID, p_CalcDate, NULL, SYSTIMESTAMP );
      END IF;

      IF ((p_ByCurr = 1 or p_ByEDP = 1) AND p_MarketID = GetMicexID()) or p_ByStock <> 0
      THEN                                      
        RSI_CheckSecurLimits(p_CalcDate) ;
      END IF;
      IF p_ByEDP = 1 or p_ByStock = 1 or (p_ByCurr <> 0 AND p_MarketID = GetMicexID())
      THEN                                      
        RSI_CheckCashStockLimits (p_CalcDate) ; 
      END IF;

   --- НЕ ВНОСИТЬ ИЗМЕНЕНИЯ В КОД ПРОЦЕДУРЫ БЕЗ КОРРЕКТИРОВКИ ПАКЕТА IT_LIMIT
      
     IF (p_ByStock <> 0)                           --OR p_ByOutMarketStock <> 0
      THEN
         BEGIN
            DBMS_SCHEDULER.drop_job (v_job1, TRUE);
            DBMS_SCHEDULER.drop_job (v_job2, TRUE);
         EXCEPTION
            WHEN OTHERS
            THEN
               NULL;
         END;

         --  IF p_ByStock <> 0 then

         ClearTickTmp (p_CalcDate,p_ByStock , 0, p_MarketID, p_UseListClients) ;
         SetTickTmp (p_CalcDate, p_ByStock, 0, p_MarketID, p_UseListClients); 
         TimeStamp_ ('SetTickTmp Завершена  ',p_CalcDate,NULL, SYSTIMESTAMP);

         --SetAccountTmp (p_CalcDate, 0);
         --TimeStamp_ ('SetAccountTmp Завершена  ',p_CalcDate,NULL,SYSTIMESTAMP);
         ClearLotTmp(p_CalcDate, p_MarketID, p_UseListClients) ;
         SetLotTmp (p_CalcDate, 0,p_MarketID, p_UseListClients); 
         TimeStamp_ ('SetLotTmp  Завершена  ',p_CalcDate,NULL,SYSTIMESTAMP);

         TimeStamp_ ('Отбор фин.инструментов в поставке',p_CalcDate,NULL,SYSTIMESTAMP,/* null,*/ 103);
         ClearFIIDTmp(p_MarketID, p_UseListClients) ;
         SetFIIDTmp (p_CalcDate, 0,p_MarketID, p_UseListClients); 
         TimeStamp_ ('SetFIIDTmp  Завершена  ',p_CalcDate,NULL, SYSTIMESTAMP);

         TimeStamp_ ('Расчет лимитов MONEY фондовый рынок '||p_MarketCode,p_CalcDate,NULL,SYSTIMESTAMP, /*null,*/ 104);
         RSI_ClearCashStockLimits  (p_CalcDate, p_ByStock, 0,0,p_MArketCode, p_MarketID, /*mainsessionid,*/ p_UseListClients);
         RSI_CreateCashStockLimits (p_CalcDate, p_ByStock, 0,0,p_MArketCode, p_MarketID, /*mainsessionid,*/ p_UseListClients);
     --    CheckCashStockForDuplAndSetErr(p_CalcDate);


         getFlagLimitPrm (p_MarketID,MARKET_KIND_STOCK,v_IsDepo,v_IsKind2,v_DepoAcc);

         RSI_ClearSecurLimits   (p_CalcDate, p_ByStock, 0,v_DepoAcc, p_MArketCode, p_MarketID,/* mainsessionid,*/ p_UseListClients);
         RSI_CreateSecurLimits  (p_CalcDate, p_ByStock, 0,v_DepoAcc, p_MArketCode, p_MarketID,/* mainsessionid,*/ p_UseListClients);
         RSI_LOCKSecurLimits (p_CalcDate, p_ByStock, 0,v_DepoAcc, p_MArketCode, p_MarketID, /*mainsessionid,*/ p_UseListClients) ;
      END IF;
    --- НЕ ВНОСИТЬ ИЗМЕНЕНИЯ В КОД ПРОЦЕДУРЫ БЕЗ КОРРЕКТИРОВКИ ПАКЕТА IT_LIMIT

      IF (p_ByCurr <> 0 or p_ByEDP = 1) AND p_MarketID = GetMicexID()
      THEN                                       
         ClearPlanSumCur (p_CalcDate,p_ByCurr , p_ByEDP, p_MarketID, p_UseListClients) ;
         CollectPlanSumCur (p_CalcDate, p_ByCurr, p_ByEDP, p_MarketID, p_UseListClients); 
         TimeStamp_ ('SetCMTick SetCMComm  Завершена  ',p_CalcDate,NULL, SYSTIMESTAMP);
      END IF;
      
      IF (p_ByCurr <> 0 AND p_MarketID = GetMicexID())
      THEN                                       -- для валютного только биржа
         TimeStamp_ ('Старт расчета по валютному рынку. Операционист  '|| RsbSessionData.Oper,p_CalcDate,NULL,SYSTIMESTAMP);

         v_IsDepo := 0;
         v_IsKind2 := 0;
         v_DepoAcc := CHR (1);

         getFlagLimitPrm (p_MarketID,
                          MARKET_KIND_CURR,
                          v_IsDepo,
                          v_IsKind2,
                          v_DepoAcc
                         );     -- для валютного определим признаки(настройки)

         TimeStamp_ ('Расчет лимитов валютный рынок '||p_MarketCode,p_CalcDate,NULL,SYSTIMESTAMP, /*null, */700);
         RSI_ClearCashStockLimitsCur (p_CalcDate,  0/*EDP*/, p_UseListClients);
         RSI_CreateCashStockLimitsCur (p_CalcDate, v_IsKind2, v_IsDepo, 0/*EDP*/, p_UseListClients);
         RSI_DeleteCashStockLimitsCur (p_CalcDate, v_IsKind2, v_IsDepo, 0/*EDP*/, p_UseListClients);
         
         IF (v_IsDepo <> 0)
         THEN             -- формируется только с установленным признаком Depo
            RSI_CreateSecurLimitsCur (p_CalcDate, v_IsKind2, v_DepoAcc);
         ELSE
            RSI_CreateSecurLimByKindCurZero (p_CalcDate, 0, v_DepoAcc, p_MarketID, p_MarketCode, 0/*EDP*/, p_UseListClients);
            RSI_DeleteZeroSecurLimByCur (p_CalcDate);
         END IF;

     --    RSI_DeleteClientErr (p_CalcDate); удаляются в RSI_FillContrTable
         TimeStamp_ ('Расчет лимитов по валютному рынку завершен. ',p_CalcDate,NULL,SYSTIMESTAMP,/*NULL,*/799);
      END IF;


      IF p_ByDeriv <> 0
      THEN
         TimeStamp_ ('Старт расчета по срочному рынку. Операционист  '|| RsbSessionData.Oper,p_CalcDate,NULL,SYSTIMESTAMP);
         TimeStamp_ ('Расчет лимитов срочный  рынок '||p_MarketCode,p_CalcDate,NULL,SYSTIMESTAMP,/* null,*/ 800);
         RSI_CheckFutureMarkLimits (p_CalcDate);
         RSI_CreateFutureMarkLimits (p_CalcDate, p_UseListClients);
         TimeStamp_ (
            'Расчет лимитов срочной секции завершен. ',
            p_CalcDate,
            NULL,
            SYSTIMESTAMP,
        --    NULL,
            899);
      END IF;

   --- НЕ ВНОСИТЬ ИЗМЕНЕНИЯ В КОД ПРОЦЕДУРЫ БЕЗ КОРРЕКТИРОВКИ  ПАКЕТА IT_LIMIT

      IF p_ByEDP = 1
      THEN

         TimeStamp_ ('Старт расчета по договорам ЕДП',p_CalcDate,NULL,SYSTIMESTAMP, /*NULL,*/ 900);

         --SetAccountTmp (p_CalcDate, 1);
         --TimeStamp_ ('SetAccountTmp ЕДП Завершена  ',p_CalcDate,NULL,SYSTIMESTAMP);
          IF (p_ByStock = 0) THEN--отбираем только если выключен расчет по фонде. если включен расчет по фондовому рынку, то значит сделки уже отобрали
             ts_ := SYSTIMESTAMP;
             ClearTickTmp (p_CalcDate,p_ByStock , 1, p_MarketID, p_UseListClients,901) ;
             SetTickTmp (p_CalcDate, p_ByStock, 1, p_MarketID, p_UseListClients); --t_calc_sid = '''||g_calc_market||''''
             TimeStamp_ ('SetTickTmp ЕДП Завершена  ',p_CalcDate,NULL, ts_);
          END IF;

          ts_ := SYSTIMESTAMP;
          TimeStamp_ ('Расчет по фондовому рынку ЕДП',p_CalcDate,NULL,SYSTIMESTAMP, /*null,*/ 902);
          RSI_ClearCashStockLimits  (p_CalcDate, p_ByStock, 0,1,p_MArketCode, p_MarketID, /*mainsessionid,*/ p_UseListClients);
          RSI_CreateCashStockLimits(p_CalcDate, p_ByStock, 0, 1, p_MarketCode, p_MarketID,/* mainsessionid,*/ p_UseListClients);
        --   CheckCashStockForDuplAndSetErr(p_CalcDate);
          TimeStamp_ ('Расчет по фондовому рынку ЕДП завершен',p_CalcDate,NULL, ts_);

         IF (p_MarketID = GetMicexID()) THEN
              
            ts_ := SYSTIMESTAMP;
            TimeStamp_ ('Расчет по валютному рынку ЕДП',p_CalcDate,NULL,SYSTIMESTAMP, /*NULL,*/ 903);
            v_IsDepo := 0;
            v_IsKind2 := 0;
            v_DepoAcc := CHR (1);

            getFlagLimitPrm (p_MarketID,
                          MARKET_KIND_CURR,
                          v_IsDepo,
                          v_IsKind2,
                          v_DepoAcc
                         );     -- для валютного определим признаки(настройки)
            RSI_ClearCashStockLimitsCur (p_CalcDate,  1/*EDP*/, p_UseListClients);
            RSI_CreateCashStockLimitsCur (p_CalcDate, v_IsKind2, v_IsDepo, 1/*IsEDP*/, p_UseListClients);
            RSI_DeleteCashStockLimitsCur (p_CalcDate, v_IsKind2, v_IsDepo, 1/*EDP*/, p_UseListClients);

            IF (v_IsDepo <> 0)
            THEN             -- формируется только с установленным признаком Depo
               RSI_CreateSecurLimitsCur (p_CalcDate, v_IsKind2, v_DepoAcc);
            ELSE
               RSI_CreateSecurLimByKindCurZero (p_CalcDate, 0, v_DepoAcc, p_MarketID, p_MarketCode, 1/*IsEDP*/, p_UseListClients);
               --RSI_DeleteZeroSecurLimByCur (p_CalcDate);
            END IF;
            TimeStamp_ ('Расчет по валютному рынку ЕДП завершен',p_CalcDate,NULL, ts_);
        END IF;

      END IF;
      InsertLIMITCASHSTOCKFromInt(p_CalcDate);
      DeleteWoOpenBalance(p_CalcDate => p_CalcDate);
      CheckCashStockForDuplAndSetErr(p_CalcDate);
      commit;
      if it_rs_interface.get_parm_varchar_path(p_parm_path => 'РСХБ\ИНТЕГРАЦИЯ\CHECK_WRITEOFF') = chr(88)  
          and p_CalcDate >= trunc(sysdate) then
        TimeStamp_ ('Старт корректировки по списаниям с неторгового счета',p_CalcDate,NULL,SYSTIMESTAMP);
        it_diasoft.PKO_CheckAndCorrectSecuritiesLimits(p_MarketID,p_CalcDate,p_UseListClients);
      end if;

      TimeStamp_ ('Расчет завершен. '||p_MarketCode,
                 p_CalcDate,
                 NULL,
                 SYSTIMESTAMP,
              --   NULL,
                 999
                );


    EXCEPTION
    WHEN OTHERS THEN
      TimeStamp_ ('ОШИБКА:'||sqlerrm, p_CalcDate, NULL, SYSTIMESTAMP,excepsqlcode_=> sqlcode );
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise ;
   END;  -- RSI_CreateLimits



   PROCEDURE RSI_CrLimitAdJNptxWrtByKind (
      Limit_Kind     IN     NUMBER,
      ID_Operation   IN     NUMBER,
      ID_Step        IN     NUMBER,
      rLimitAdj      IN OUT DDL_LIMITADJUST_DBT%ROWTYPE)
   AS
   BEGIN
      rLimitAdj.t_ID := 0;
      rLimitAdj.T_LIMITID := (ID_Operation * 10) || Limit_Kind;
      rLimitAdj.T_LIMIT_KIND := Limit_Kind;

      RSI_InsDfltIntoWRTBC (rLimitAdj);

      INSERT INTO DDL_LIMITADJUST_DBT
           VALUES rLimitAdj;
   END;                                         -- RSI_CrLimitAdJNptxWrtByKind

   -- Получение идентификатора субъекта по коду
   FUNCTION GetPartyIDByCode (p_Code IN VARCHAR2, p_CodeKind IN NUMBER)
      RETURN NUMBER
   IS
      v_PartyID   NUMBER := -1;
   BEGIN
      SELECT NVL (t_ObjectID, -1)
        INTO v_PartyID
        FROM dobjcode_dbt
       WHERE     t_ObjectType = 3
             AND t_CodeKind = p_CodeKind
             AND t_State = 0
             AND t_Code = p_Code
             AND ROWNUM <= 1;

      RETURN v_PartyID;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN -1;
   END;

   PROCEDURE RSI_CrLimitAdJNptxWrt (DocID IN NUMBER, ID_Operation IN NUMBER, ID_Step IN NUMBER)
   AS
      rLimitAdj       DDL_LIMITADJUST_DBT%ROWTYPE;
      rNptxop         DNPTXOP_DBT%ROWTYPE;
      v_MarketKind    NUMBER := -1;
      v_MMVB_ID       NUMBER := -1;
      v_MMVB_Code     VARCHAR2 (35) := CHR (1);
      v_IsDepo        NUMBER := 0;
      v_IsKind2       NUMBER := 0;
      v_DepoAcc       VARCHAR2 (20) := CHR (1);
      v_CURR_CODE     VARCHAR2 (3) := CHR (1);
      v_LEVERAGE      NUMBER := 0;
      v_LEVERAGECUR   NUMBER := 0;
   BEGIN
      InitError ();

      SELECT *
        INTO rNptxop
        FROM dnptxop_dbt
       WHERE t_ID = DocID;

      /*для начала заполним поля, общие для всех корректировок*/
      SELECT acc.t_AccountID,
             (CASE
                 WHEN sfcontr.t_ServKind = 1 AND sfcontr.t_ServKindSub = 8
                 THEN
                    MARKET_STOCK_EX
                 WHEN sfcontr.t_ServKind = 1 AND sfcontr.t_ServKindSub = 9
                 THEN
                    MARKET_STOCK_OUT
                 WHEN sfcontr.t_ServKind = 15
                 THEN
                    MARKET_DERIV
                 WHEN sfcontr.t_ServKind = 21
                 THEN
                    MARKET_CURR
                 ELSE
                    -1
              END),
             DECODE (acc.t_Code_Currency,
                     RSI_RSB_FIInstr.NATCUR, 'SUR',
                     curr.t_CCY
                    ),
             (CASE
                 WHEN RSB_SECUR.
                       GetGeneralMainObjAttr (
                         207              /*Договор брокерского обслуживания*/
                            ,
                         LPAD (dlc.t_DlContrID, 34, '0'),
                         1                              /*Признак блокировки*/
                          ,
                         rNptxop.t_OperDate) = 1 THEN 'X'
                 ELSE CHR (0)
              END),
             dlc.t_Leverage,
             dlc.t_LeverageCur
        INTO rLimitAdj.T_INTERNALACCOUNT,
             rLimitAdj.T_MARKET,
             v_CURR_CODE,
             rLimitAdj.T_ISBLOCKED,
             v_LEVERAGE,
             v_LEVERAGECUR
        FROM daccount_dbt acc,
             dsfcontr_dbt sfcontr,
             dfininstr_dbt curr,
             ddlcontrmp_dbt mp,
             ddlcontr_dbt dlc
       WHERE     acc.t_Chapter = 1
             AND acc.t_Account LIKE rNptxop.t_ACCOUNT
             AND acc.t_Code_Currency = rNptxop.t_Currency
             AND sfcontr.t_ID = rNptxop.t_Contract
             AND curr.t_FIID = acc.t_Code_Currency
             AND curr.t_FI_Kind = 1
             AND mp.t_SfContrID = sfcontr.t_ID
             AND dlc.t_DlContrID = mp.t_DlContrID;

      v_MarketKind :=
         (CASE
             WHEN rLimitAdj.T_MARKET = MARKET_STOCK_OUT
                  OR rLimitAdj.T_MARKET = MARKET_STOCK_EX
             THEN
                MARKET_KIND_STOCK
             WHEN rLimitAdj.T_MARKET = MARKET_DERIV
             THEN
                MARKET_KIND_DERIV
             WHEN rLimitAdj.T_MARKET = MARKET_CURR
             THEN
                MARKET_KIND_CURR
             ELSE
                -1
          END);

      v_MMVB_Code := TRIM (rsb_common.GetRegStrValue ('SECUR\MICEX_CODE', 0));

      IF v_MMVB_Code <> CHR (1)
      THEN
         v_MMVB_ID := GetPartyIDByCode (v_MMVB_Code, cnst.PTCK_CONTR);
         getFlagLimitPrm (
            CASE WHEN rLimitAdj.T_MARKET = 0 THEN 0 ELSE v_MMVB_ID END,
            v_MarketKind,
            v_IsDepo,
            v_IsKind2,
            v_DepoAcc);
      END IF;


      IF (v_MarketKind = MARKET_KIND_STOCK)
      THEN
         rLimitAdj.T_LIMIT_TYPE := 'MONEY';
      ELSE
         IF (v_MarketKind = MARKET_KIND_CURR)
         THEN                                                      -- валютный
            IF ( (v_IsDepo = 1) AND (rNptxop.t_Currency <> 0))
            THEN                              -- стоит признак Репо и не рубли
               rLimitAdj.T_LIMIT_TYPE := 'DEPO';
            ELSE
               rLimitAdj.T_LIMIT_TYPE := 'MONEY';
            END IF;
         END IF;
      END IF;

      IF (rLimitAdj.T_LIMIT_TYPE = 'DEPO')
      THEN
         rLimitAdj.T_TRDACCID := v_DepoAcc;
         rLimitAdj.T_SECCODE := v_CURR_CODE;
         rLimitAdj.T_TAG := CHR (1);
         rLimitAdj.T_CURR_CODE := CHR (1);
         rLimitAdj.T_LEVERAGE := 0;
      ELSE
         rLimitAdj.T_TRDACCID := CHR (1);
         rLimitAdj.T_SECCODE := CHR (1);
         rLimitAdj.T_TAG := GetTAG (CASE WHEN rLimitAdj.T_MARKET = 0 THEN 0 ELSE v_MMVB_ID END, v_MarketKind,1,1);
         rLimitAdj.T_CURR_CODE := v_CURR_CODE;

         IF (rLimitAdj.T_MARKET = MARKET_STOCK_OUT
             OR rLimitAdj.T_MARKET = MARKET_STOCK_EX)
         THEN
            rLimitAdj.T_LEVERAGE := v_LEVERAGE;
         ELSIF (rLimitAdj.T_MARKET = MARKET_CURR)
         THEN
            rLimitAdj.T_LEVERAGE := v_LEVERAGECUR;
         END IF;
      END IF;

      rLimitAdj.T_DATE := rNptxop.t_OperDate;
      rLimitAdj.T_TIME := rNptxop.t_Time;
      rLimitAdj.T_CLIENT := rNptxop.t_Client;
      rLimitAdj.T_FIRM_ID := GetFIRM_ID (CASE WHEN rLimitAdj.T_MARKET = MARKET_STOCK_OUT THEN 0 ELSE v_MMVB_ID END, v_MarketKind,1);
      rLimitAdj.T_CLIENT_CODE := RSI_RSBPARTY.PT_GetPartyCode (rNptxop.t_Client, 1);
      rLimitAdj.T_OPEN_BALANCE :=
         (CASE
             WHEN rNptxop.t_SubKind_Operation = 10 THEN rNptxop.t_OutSum
             ELSE -rNptxop.t_OutSum
          END); --Если операция <Зачисление> значение Сумма (S1), если <Списание> - Сумма (S1) со знаком
      rLimitAdj.T_OPEN_LIMIT := 0;                            --Не заполняется
      rLimitAdj.T_CURRENT_LIMIT := 0;                         --Не заполняется
      rLimitAdj.T_LIMIT_OPERATION := 'CORRECT_LIMIT';
      rLimitAdj.T_CURRID := rNptxop.t_Currency;
      rLimitAdj.T_ID_OPER := ID_Operation;
      rLimitAdj.T_ID_STEP := ID_Step;

      RSI_InsDfltIntoWRTBC (rLimitAdj);

      RSI_CrLimitAdJNptxWrtByKind (0,
                                   ID_Operation,
                                   ID_Step,
                                   rLimitAdj
                                  );
      RSI_CrLimitAdJNptxWrtByKind (1,
                                   ID_Operation,
                                   ID_Step,
                                   rLimitAdj
                                  );

      IF ( (v_MarketKind = MARKET_KIND_STOCK)
          OR ( (v_MarketKind = MARKET_KIND_CURR) AND (v_IsKind2 = 1)))
      THEN
         RSI_CrLimitAdJNptxWrtByKind (2,
                                      ID_Operation,
                                      ID_Step,
                                      rLimitAdj
                                     );
         RSI_CrLimitAdJNptxWrtByKind (365,
                                      ID_Operation,
                                      ID_Step,
                                      rLimitAdj
                                     );
      END IF;

      LockRecordsFrom('dnptxop_dbt','t_ID = '||DocID);

      UPDATE dnptxop_dbt
         SET t_LimitStatus = LIMITSTATUS_WAIT
       WHERE t_ID = DocID;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN;
   END;                                               -- RSI_CrLimitAdJNptxWrt

   PROCEDURE RSI_RestoreLimitAdJNptxWrt (DocID IN NUMBER, ID_Operation IN NUMBER, ID_Step IN NUMBER)
   AS
   BEGIN
      -- при откате шага удаляем корректировки лимитов только в том случае, если статус выгрузки корректировок = "ждет", иначе удаления строк не производим
     LockRecordsFrom('DDL_LIMITADJUST_DBT limad','limad.T_ID_OPER = '||ID_Operation||
                 ' AND limad.T_ID_STEP = '||ID_Step||
                 ' AND NVL ( (SELECT wrt.t_LimitStatus FROM dnptxop_dbt wrt  WHERE wrt.t_ID = '||DocID||
                 '),'||LIMITSTATUS_UNDEF||') = '||LIMITSTATUS_WAIT);

      DELETE FROM DDL_LIMITADJUST_DBT limad
            WHERE     limad.T_ID_OPER = ID_Operation
                  AND limad.T_ID_STEP = ID_Step
                  AND NVL ( (SELECT wrt.t_LimitStatus
                               FROM dnptxop_dbt wrt
                              WHERE wrt.t_ID = DocID),
                           LIMITSTATUS_UNDEF
                          ) = LIMITSTATUS_WAIT;

      LockRecordsFrom('dnptxop_dbt','t_ID = '||DocID||
         ' anf t_LimitStatus = '||LIMITSTATUS_WAIT);

      UPDATE dnptxop_dbt
         SET t_LimitStatus = LIMITSTATUS_UNDEF
       WHERE t_ID = DocID AND t_LimitStatus = LIMITSTATUS_WAIT;
   END;                                          -- RSI_RestoreLimitAdJNptxWrt


   /**
    @brief    DEF-62480, Процедура возвращает значения параметров расчета лимитов для суб-договора.
    @param[in]    p_SfContrID    	ID суб-договора
    @param[in]    p_LimitDate    	Дата
    @param[out]   p_FirmID           	Код участника торгов
    @param[out]   p_Tag              	Код позиции
    @param[out]   p_TrdAcc           	Торговый счет
   */
   PROCEDURE GetLimitPrm (
     p_SfContrID IN number
     , p_LimitDate IN date
     , p_FirmID OUT varchar2
     , p_Tag OUT varchar2
     , p_TrdAcc OUT varchar2
   )
   AS       
     x_Sql VARCHAR2(2000);
     x_DlContrID number;
     x_ServKind number;
     x_ImplKind number;
     x_MarketKind number;
     x_MarketID number;
     x_IsEdp number;
     x_NullDate DATE := to_date('01-01-0001', 'dd-mm-yyyy');
     x_FarDate DATE := to_date('31-12-3000', 'dd-mm-yyyy');
     x_CodesZeroLimit varchar2(12) ;
   BEGIN
 
     -- определение ДБО, вида обслуживания и биржи для полученного суб-договора
     x_Sql := 'SELECT sf.t_servkind, mp.t_marketid, mp.t_dlcontrid, RSHB_RSI_SCLIMIT.SfcontrIsEDP(sf.t_id) as SfcontrIsEDP 
               FROM dsfcontr_dbt sf, DDLCONTRMP_DBT mp 
               WHERE sf.t_id = :x_SfContrID AND mp.t_sfcontrid = sf.t_id 
               AND :x_LimitDate BETWEEN mp.t_mpregdate AND case when mp.t_mpclosedate = :x_NullDate then :x_FarDate ELSE mp.t_mpclosedate END'
     ;
     EXECUTE IMMEDIATE x_Sql INTO x_ServKind, x_MarketID, x_DlContrID, x_IsEdp USING p_SfContrID, p_LimitDate, x_NullDate, x_FarDate;

     x_MarketKind := GetMarketKindbyServKind(x_ServKind); -- фондовый рынок
 
     -- определение ТКС для суб-договора (определяется по категориям на ФР)
     IF(RSHB_RSI_SCLIMIT.GetImplKind(x_DlContrID, p_LimitDate) = 1) THEN
       x_ImplKind := 1; 	-- ТКС = "Основной"
     ELSE
       x_ImplKind := 2;		-- ТКС = "Для клиентов 2-го типа"
     END IF;

     -- есть все необходимое, считываются параметры из таблицы параметров расчета лимитов
     findLimitPrm(x_MarketKind, x_MarketID, x_ImplKind,x_IsEdp, p_FirmID, p_Tag, p_TrdAcc,x_CodesZeroLimit);
     p_TrdAcc := GetTRDACCID(p_sfcontrId ,p_LimitDate,x_MarketID,x_MarketKind,x_ImplKind ); 
   END; -- GetLimitPrm

   /**
    @brief     BIQ-16667, Сохранение в лог итогов расчета . Запускается по окончании расчета
   */
   PROCEDURE SetLogItog(p_calc_direct varchar2
                           , p_CalcDate IN DATE
                           , p_ByStockMB IN NUMBER default 1
                           , p_ByStockSPB IN NUMBER default 1
                           , p_ByCurMB IN NUMBER  default 1
                           , p_ByFortsMB IN NUMBER default 1
                           , p_ByEDP IN NUMBER   default 1
                           , p_MarketID IN NUMBER default -1 
                           , p_UseListClients IN NUMBER default 0)
                            as
   pragma autonomous_transaction ;
   v_cnt integer;
   v_group_action integer := 4000 ;
   v_Action integer := 100;
   v_recs integer := 0;
   begin
     if p_ByStockMB != 1 and p_ByStockSPB != 1 and p_ByFortsMB != 1 and p_ByCurMB !=1 and p_ByEDP != 1 then
       return ;
     end if ;
     it_limit.set_calc_sid(p_calc_direct);
    
     it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                           ,p_CalcDate     => p_CalcDate
                           ,p_action       => v_Action
                           ,p_label        => ''
                           ,p_group        => v_group_action);
     if p_ByCurMB =1 then
       if p_UseListClients = 0 then 
         SELECT  COUNT (*) into v_cnt 
              FROM (  SELECT t_client_code FROM ddl_limitcashstock_dbt WHERE t_market_kind = 'валютный' AND t_date = p_CalcDate
                union SELECT t_client_code FROM ddl_limitsecurites_dbt  WHERE t_market_kind = 'валютный' AND t_date = p_CalcDate );
       else
         with client as ( select /*+ materialize index(c DDL_CLIENTINFO_DBT_IDX2 )*/ distinct c.t_client 
                                       from ddl_clientinfo_dbt c 
                                       where c.t_calc_sid = g_calc_clientinfo
                                        and c.t_dlcontrid in ( select  p.t_dlcontrid 
                                                      from ddl_panelcontr_dbt p
                                                      where  p.t_calc_sid = g_calc_panelcontr
                                                        and t_setflag = chr(88) ))
          SELECT  COUNT (*) into v_cnt 
              FROM (     
                  select /*+ cardinality(c 10) index(l DDL_LIMITCASHSTOCK_DBT_IDX1 )*/ l.t_client_code 
                       from client c  
                       join ddl_limitcashstock_dbt l on l.t_client = c.t_client 
                     WHERE  l.t_market_kind = 'валютный' AND l.t_date = p_CalcDate
                union select /*+ cardinality(c 10) index(l DDL_LIMITSECURITES_DBT_IDX1 ) */ l.t_client_code 
                     from client c 
                       join ddl_limitsecurites_dbt l on l.t_client = c.t_client 
                     WHERE l.t_market_kind = 'валютный' AND l.t_date = p_CalcDate );
       end if;
       it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                           ,p_CalcDate     => p_CalcDate
                           ,p_action       => v_Action
                           ,p_label        => 'ВАЛЮТНЫЙ РЫНОК: Количество обработанных клиентов: '||v_cnt
                           ,p_group        => v_group_action);
       
       v_recs := greatest(v_recs,v_cnt);

     end if;

     if p_ByFortsMB = 1 then
       if p_UseListClients = 0 then 
         SELECT COUNT (distinct t_client) into v_cnt 
            FROM DDL_LIMITFUTURMARK_DBT 
               WHERE t_market_kind = 'срочный'
               AND t_date = p_CalcDate ;
       else
         with client as ( select /*+ materialize index(c DDL_CLIENTINFO_DBT_IDX2 )*/ distinct c.t_client 
                                       from ddl_clientinfo_dbt c 
                                       where c.t_calc_sid = g_calc_clientinfo
                                        and c.t_dlcontrid in ( select  p.t_dlcontrid 
                                                      from ddl_panelcontr_dbt p
                                                      where  p.t_calc_sid = g_calc_panelcontr
                                                        and t_setflag = chr(88) ))
         SELECT /*+ cardinality(c 10) */ COUNT (distinct l.t_client) into v_cnt 
            FROM DDL_LIMITFUTURMARK_DBT l
                 join client c on l.t_client = c.t_client 
               WHERE l.t_market_kind = 'срочный'
               AND l.t_date = p_CalcDate ;
       end if;
       it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                             ,p_CalcDate     => p_CalcDate
                             ,p_action       => v_Action
                             ,p_label        => 'СРОЧНЫЙ РЫНОК: Количество обработанных клиентов: '||v_cnt
                             ,p_group        => v_group_action);
      
       v_recs := greatest(v_recs,v_cnt);

     end if ;

     if p_ByStockMB = 1 or p_ByStockSPB = 1 or p_ByEDP = 1 then
       if p_UseListClients = 0 then 
         SELECT  COUNT (*) into v_cnt 
              FROM (  SELECT t_client_code FROM ddl_limitcashstock_dbt WHERE t_market_kind = 'фондовый' AND t_date = p_CalcDate
                union SELECT t_client_code FROM ddl_limitsecurites_dbt  WHERE t_market_kind = 'фондовый' AND t_date = p_CalcDate );
       else
          with client as ( select /*+ materialize index(c DDL_CLIENTINFO_DBT_IDX2 )*/ distinct c.t_client 
                                       from ddl_clientinfo_dbt c 
                                       where c.t_calc_sid = g_calc_clientinfo
                                        and c.t_dlcontrid in ( select  p.t_dlcontrid 
                                                      from ddl_panelcontr_dbt p
                                                      where  p.t_calc_sid = g_calc_panelcontr
                                                        and t_setflag = chr(88) ))
          SELECT  COUNT (*) into v_cnt 
              FROM (     
                  select /*+ cardinality(c 10) index(l DDL_LIMITCASHSTOCK_DBT_IDX1 )*/ l.t_client_code 
                       from client c  
                       join ddl_limitcashstock_dbt l on l.t_client = c.t_client 
                     WHERE  l.t_market_kind = 'фондовый' AND l.t_date = p_CalcDate
                union select /*+ cardinality(c 10) index(l DDL_LIMITSECURITES_DBT_IDX1 ) */ l.t_client_code 
                     from client c 
                       join ddl_limitsecurites_dbt l on l.t_client = c.t_client 
                     WHERE l.t_market_kind = 'фондовый' AND l.t_date = p_CalcDate );
  
       end if;

        it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                           ,p_CalcDate     => p_CalcDate
                           ,p_action       => v_Action
                           ,p_label        => 'ФОНДОВЫЙ РЫНОК: Количество обработанных клиентов: '||v_cnt
                           ,p_group        => v_group_action);

      
        v_recs := case when v_cnt = 0 then 0 else greatest(v_recs,v_cnt) end;
 
      end if;     
      if p_ByStockMB  = 1 and p_UseListClients = 0 then
           select count(1) into v_cnt from ( 
               SELECT t_seccode,t_client_code,t_market,t_market_kind,COUNT (1) cnt 
                     FROM ddl_limitsecurites_dbt t 
                       WHERE t_seccode IN 
                             (SELECT DISTINCT t_codesczerolimit FROM DDL_LIMITPRM_DBT 
                               WHERE t_marketid = t.t_market AND t_marketkind = 1 ) 
                             AND t_market = GetMicexID  AND t_market_kind = 'фондовый' and t_date = p_CalcDate
                        GROUP BY t_seccode,t_client_code,t_market,t_market_kind )
                       where cnt = 1 ;

           it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                           ,p_CalcDate     => p_CalcDate
                           ,p_action       => v_Action
                           ,p_label        => 'Количество нулевых позиций по ММВБ: '||v_cnt
                           ,p_group        => v_group_action);
          if v_cnt = 0 then
             it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                           ,p_CalcDate     => p_CalcDate
                           ,p_action       => (-v_Action)
                           ,p_label        => '!!!ПРЕДУПРЕЖДЕНИЕ!!! Отсутствуют нулевые лимиты по ММВБ'
                           ,p_group        => v_group_action);
          end if;
      end if;
      if p_ByStockSPB = 1  and p_UseListClients = 0 then
           select count(1) into v_cnt from ( 
               SELECT t_seccode,t_client_code,t_market,t_market_kind,COUNT (1) cnt 
                     FROM ddl_limitsecurites_dbt t 
                       WHERE t_seccode IN 
                             (SELECT DISTINCT t_codesczerolimit FROM DDL_LIMITPRM_DBT 
                               WHERE t_marketid = t.t_market AND t_marketkind = 1 ) 
                             AND t_market = GetSpbexID AND t_market_kind = 'фондовый' and t_date = p_CalcDate
                        GROUP BY t_seccode,t_client_code,t_market,t_market_kind )
                       where cnt = 1 ;

           it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                           ,p_CalcDate     => p_CalcDate
                           ,p_action       => v_Action
                           ,p_label        => 'Количество нулевых позиций по СПБ: '||v_cnt
                           ,p_group        => v_group_action);
          if v_cnt = 0 then
             it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                           ,p_CalcDate     => p_CalcDate
                           ,p_action       => (-v_Action)
                           ,p_label        => '!!!ПРЕДУПРЕЖДЕНИЕ!!! Отсутствуют нулевые лимиты по СПБ'
                           ,p_group        => v_group_action);
          end if;

      end if ;

       if (p_ByCurMB = 1 or p_ByEDP = 1)  and p_UseListClients = 0  then
             select count(1) into v_cnt from (
                SELECT t_seccode,t_client_code,t_market,t_market_kind,COUNT (1) cnt 
                   FROM ddl_limitsecurites_dbt t 
                   WHERE  t_market = GetMicexID AND t_market_kind = 'валютный' and t_date =  p_CalcDate
                GROUP BY t_seccode,t_client_code,t_market,t_market_kind)
             where cnt = 1 ;

           it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                           ,p_CalcDate     => p_CalcDate
                           ,p_action       => v_Action
                           ,p_label        => 'Количество нулевых позиций по валютному рынку: '||v_cnt
                           ,p_group        => v_group_action);
          if v_cnt = 0 then
             it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                           ,p_CalcDate     => p_CalcDate
                           ,p_action       => (-v_Action)
                           ,p_label        => '!!!ПРЕДУПРЕЖДЕНИЕ!!! Отсутствуют нулевые лимиты по Валютному рынку'
                           ,p_group        => v_group_action);
          end if;
       end if;
       
     update DDL_LIMITOP_DBT t set t.t_recs =  v_recs where t.t_calc_direct = p_calc_direct ;
     commit;
   end;

   /**
    @brief     BIQ-16667, Сохранение в лог ошибок заполнения списка договоров . Запускается по окончании расчета
   */
   PROCEDURE SetLogErrContr (p_calc_direct varchar2
                           , p_CalcDate IN DATE
                           , p_ByStockMB IN NUMBER default 1
                           , p_ByStockSPB IN NUMBER default 1
                           , p_ByCurMB IN NUMBER  default 1
                           , p_ByFortsMB IN NUMBER default 1
                           , p_ByEDP IN NUMBER   default 1
                           , p_MarketID IN NUMBER default -1 
                           , p_UseListClients IN NUMBER default 0)
                                as
  pragma autonomous_transaction;
  v_sql varchar2(32600);
  vc_err sys_refcursor ;
  v_ekk            ddl_clientinfo_dbt.t_ekk%type;
  v_ERRORS_REASON   varchar2(32000) ;
  v_LogText        varchar2(2000) ;
  vcl_LogText      clob ;
  function get_line(p_ekk ddl_clientinfo_dbt.t_ekk%type 
                   ,p_str varchar2) return varchar2 as
  begin              
    return  '| '||rpad(p_ekk,15)||'| '||p_str||chr(10);
  end;
  begin
    if p_ByStockMB=0 and  p_ByStockSPB=0 and  p_ByCurMB=0 and  p_ByFortsMB=0 and p_ByEDP = 0 then
      return;
    end if; 
    it_limit.set_calc_sid(p_calc_direct);  
    v_sql:=
'select  t_ekk
        ,listagg(T_ERRORS_REASON, '' ,'') WITHIN group(order by t_ekk) T_ERRORS_REASON
        from (select distinct
                     t_ekk
                     ,T_ERRORS_REASON
                from ddl_clientinfo_dbt c
               where c.t_calc_sid = '''||g_calc_clientinfo||'''
                  and c.t_haserrors = CHR(88)
                  ';
    IF  p_UseListClients = 1 THEN
        v_sql:=v_sql||' and  c.t_dlcontrid in  (select t_dlcontrid from ddl_panelcontr_dbt where t_calc_sid = '''||g_calc_panelcontr||'''  and t_setflag = chr(88)) 
                   ';
    END IF;
    if p_MarketID != -1 then
        v_sql:=v_sql||' AND  c.t_MarketID = '||p_MarketID;
    end if;
    if p_ByStockMB=0 or  p_ByStockSPB=0 or  p_ByCurMB=0 or  p_ByFortsMB=0 or p_ByEDP = 0 then 
            v_sql:=v_sql|| ' and (1 = 0';
        if p_ByStockMB = 1 then
           v_sql:=v_sql||' or (c.t_servkind = 1 and c.t_isedp = chr(0) and c.t_marketid = '||GetMicexID()||')';
        end if;
        if p_ByStockSPB = 1 then
           v_sql:=v_sql||' or (c.t_servkind = 1 and c.t_isedp = chr(0) and c.t_marketid = '||GetSpbexID()||')';
        end if;
       if p_ByCurMB = 1 then
           v_sql:=v_sql||' or (c.t_servkind = 21 and c.t_isedp = chr(0) and c.t_marketid = '||GetMicexID()||')';
        end if;
        if p_ByFortsMB = 1 then
           v_sql:=v_sql||' or (c.t_servkind = 15 and c.t_isedp = chr(0) and c.t_marketid = '||GetMicexID()||')';
        end if;
         if p_ByEDP = 1 then
           v_sql:=v_sql||' or c.t_IsEDP = CHR(88) ';
        end if;
        v_sql:=v_sql||' ) ';
    end if;
    v_sql:=v_sql||') c
     group by t_ekk order by T_EKK ';
     -- dbms_output.put_line(v_sql);
    open vc_err for v_sql ;    
    vcl_LogText := '';
    loop    
      fetch vc_err 
         into v_ekk           
              ,v_ERRORS_REASON;          
      exit when vc_err%notfound;
      v_LogText := null;
      if v_ERRORS_REASON is null then
         v_LogText := get_line(v_ekk,'Неизвестная причина');
      else
         v_LogText := get_line(v_ekk,v_ERRORS_REASON);
      end if;
      vcl_LogText:=vcl_LogText||v_LogText ;
    end loop;   
   if dbms_lob.getlength(vcl_LogText) != 0 then
         it_rsl_string.clear;
         it_rsl_string.append_varchar('Из расчета исключены:'||chr(10)||
                     '+----------------+-----------------------------------------------------------------'||chr(10)||
                     '| ЕКК            |   Причина                                                       '||chr(10)||
                     '+----------------+-----------------------------------------------------------------'||chr(10));
         it_rsl_string.append_clob(p_clob => vcl_LogText);
         it_rsl_string.append_varchar(
                     '+----------------+-----------------------------------------------------------------'||chr(10));
         update DDL_LIMITOP_DBT l set l.t_contrlog =  it_rsl_string.get_clob where l.t_calc_direct = p_calc_direct ;
    end if;
    commit;
  end;


   function GetSFContridLIMIT ( p_CalcDate date , p_marketid number,p_mpcode ddlcontrmp_dbt.t_mpcode%type, p_Client DDL_LIMITSECURITES_DBT.t_Client%type) return number deterministic as
   v_sfcontrid number ;
   begin
     SELECT t_sfcontrid into v_sfcontrid
        FROM   ddlcontrmp_dbt t 
        INNER JOIN dsfcontr_dbt s 
             ON t.t_sfcontrid = s.t_id 
                  AND s.t_servkind = 1 
                  AND S.T_DATECLOSE = TO_DATE ('01010001','ddmmyyyy') 
       WHERE t.t_mpcode = p_mpcode 
          AND t.t_marketid = p_marketid ;
      return v_sfcontrid ;
   exception
     when TOO_MANY_ROWS then
          it_limit.CALCLIMITLOG(p_calc_direct  => g_calc_DIRECT
                           ,p_CalcDate     => p_CalcDate
                           ,p_action       => -900
                           ,p_label        => 'Предупреждение! По клиенту '||p_Client||' MPCode '||p_mpcode||' не удалось однозначно определить ДБО. Цены не рассчитаны'
                           ,p_group        => 3000);

     return -1;
   end;
   /**
    @brief     BIQ-16667, Сохранение цен приобритения по чисти строк лимитов . Запускается по окончании расчета
   */
   PROCEDURE SetWAPositionPrice ( p_MarketID number, p_id_first number, p_id_last number, p_UseListClients IN NUMBER default 0)
   as 
   v_sql varchar2(30000);
   begin
      v_sql := 'MERGE /*+ index(t UDDL_LIMITSECURITES_DBT_IDX2) */ INTO DDL_LIMITSECURITES_DBT t 
           USING (SELECT distinct j.*, 
                      rshb_rsi_sclimit.GetWAPositionPrice (j.t_date, j.t_Client, j.t_SfContrID,j.t_security,j.t_client_code,j.t_seccode,j.t_firm_id, 2, j.t_TrdAccID ) price 
                    FROM (SELECT i.t_date, i.t_Client, i.t_TrdAccID,
                             RSHB_RSI_SCLIMIT.GetSFContridLIMIT(i.t_date,i.t_market,i.t_client_code,i.t_Client) t_sfcontrid,
                            i.t_security, i.t_client_code, i.t_seccode, i.t_firm_id, t_limit_kind, t_market_kind 
                            FROM DDL_LIMITSECURITES_DBT i 
                            WHERE t_market_kind = ''фондовый''  AND t_limit_kind = 2 
                                   AND t_open_balance <> 0 AND t_id BETWEEN '||p_id_first||'  and '||p_id_last||'
                                 ';
      if p_MarketID != -1 then
         v_sql := v_sql || ' AND t_market = '||p_MarketID;
      end if;
      if p_UseListClients=1 then
         v_sql := v_sql || ' AND t_client in ( select t_clientid from ddl_panelcontr_dbt where t_calc_sid = '''||g_calc_panelcontr||''' and t_setflag = chr(88)) 
         ';
      end if;
      v_sql := v_sql || '  ) j ';
      if p_UseListClients=1 then
         v_sql := v_sql || '  where  j.t_sfcontrid in ( select t_sfcontrid from ddlcontrmp_dbt where t_dlcontrid in (select t_dlcontrid from ddl_panelcontr_dbt where t_calc_sid = '''||g_calc_panelcontr||''' and t_setflag = chr(88))) 
         ';
      end if;
      v_sql := v_sql || ' ) n 
              ON (    t.t_client_code = n.t_client_code 
                  AND t.t_seccode = n.t_seccode 
                  AND t.t_trdaccid = n.t_trdaccid 
                  AND t.t_seccode <> CHR (1) 
                  AND t.t_client_code <> CHR (1) 
                  AND t.t_market_kind = n.t_market_kind 
                  AND T.T_LIMIT_KIND = n.t_limit_kind ) 
      WHEN MATCHED 
      THEN UPDATE SET t.T_wa_position_price = n.price ';
     execute immediate v_sql;
     --dbms_output.put_line(v_sql);
   end;

   /**
    @brief     BIQ-16667, Сохранение цен приобритения T365 . Запускается по окончании расчета
   */
   PROCEDURE SetWAPositionPrice365 (p_CalcDate  in date , p_MarketID number default -1, p_UseListClients IN NUMBER default 0)
   as 
   v_sql varchar2(30000);
   begin
      --it_limit.set_calc_sid(calc_direct);  
      v_sql := 'MERGE /*+ index(i DDL_LIMITSECURITES_DBT_IDX3) */ INTO DDL_LIMITSECURITES_DBT i 
            USING (SELECT t.t_market, t.t_client_code, t.t_security , t.t_TrdAccId , t.t_date , t.t_wa_position_price 
                     FROM DDL_LIMITSECURITES_DBT t 
                    WHERE  t_date = :p_CalcDate and t.t_market_kind = ''фондовый'' 
                                ';
      if p_MarketID != -1 then
         v_sql := v_sql || ' AND t_market = '||p_MarketID;
      end if;
          v_sql := v_sql || ' AND t.t_limit_kind = 2 
                             AND t.t_open_balance <> 0 
                             AND t_wa_position_price <> 0
                             group by t.t_market, t.t_client_code, t.t_security , t.t_TrdAccId , t.t_date , t.t_wa_position_price ) u 
                             ON (  i.t_date = :p_CalcDate and i.t_client_code = u.t_client_code 
                                   AND i.t_market = u.t_market
                                   AND i.t_security = u.t_security 
                                   AND i.t_TrdAccId = u.t_TrdAccId 
                                     AND i.t_market_kind = ''фондовый'' 
                                     AND i.t_date = u.t_date   
                                     AND i.t_limit_kind = 365 
                                     AND i.t_open_balance <> 0) 
            WHEN MATCHED 
            THEN UPDATE SET i.t_wa_position_price = u.t_wa_position_price';
     -- dbms_output.put_line(v_sql);
     execute immediate v_sql using p_CalcDate,p_CalcDate;
   end;

   /**
    @brief     BIQ-16667, Проверка расчитанных цен . Запускается по окончании расчета
   */
   PROCEDURE CheckWAPositionPrice ( p_calc_direct varchar2,p_CalcDate  in date )
   as 
   v_notZeroPriceCount integer ;
   v_zeroPriceCount integer ;
   v_totalCount integer ;
   v_group_action integer := 3000 ;
   v_Action integer := 200;
   c_path varchar2(2000) := 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ДОПУСТИМЫЙ ПРОЦЕНТ НУЛЕВЫХ ЦЕН' ;
   v_checkPRC integer := it_rs_interface.get_parm_number_path(c_path);
   v_calcPart number;
   begin
     select count(1) into v_notZeroPriceCount from DDL_LIMITSECURITES_DBT where t_date = p_CalcDate and t_limit_kind = 2  
                            and t_open_balance <> 0 and t_wa_position_price <> 0 and t_market_kind = 'фондовый' ;
     it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                           ,p_CalcDate     => p_CalcDate
                           ,p_action       => v_Action
                           ,p_label        => 'Количество позиций с рассчитанной ценой: '||v_notZeroPriceCount
                           ,p_group        => v_group_action);

     select count(1) into v_zeroPriceCount from DDL_LIMITSECURITES_DBT where t_date = p_CalcDate and t_limit_kind = 2  
                             and t_open_balance <> 0 and t_wa_position_price = 0 and t_market_kind = 'фондовый' ;
     it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                           ,p_CalcDate     => p_CalcDate
                           ,p_action       => v_Action
                           ,p_label        => 'Количество позиций с нулевой ценой: '||v_zeroPriceCount
                           ,p_group        => v_group_action);

     v_totalCount := v_notZeroPriceCount + v_zeroPriceCount;
      if (v_totalCount > 0) then
        if v_checkPRC is null then
           it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                           ,p_CalcDate     => p_CalcDate
                           ,p_action       => v_Action
                           ,p_label        => 'Ошибка при получении значения настройки \'||c_path
                           ,p_group        => v_group_action
                           ,p_EXCEPSQLCODE => 500);
        else
          v_calcPart := Round(v_notZeroPriceCount / v_totalCount * 100, 2);
          if (100.0 - v_calcPart) > v_checkPRC then
             it_limit.CALCLIMITLOG(p_calc_direct  => p_calc_direct
                             ,p_CalcDate     => p_CalcDate
                             ,p_action       => (-v_Action)
                             ,p_label        => '!!!ПРЕДУПРЕЖДЕНИЕ!!! Большое количество позиций без цены '
                             ,p_group        => v_group_action);
          end if;
        end if;
      end if;
  end;

  function add_text(p_txt varchar2, p_txtadd varchar2, p_sp varchar2  default ' ,',p_maxlen integer default 512 ) return varchar2 as 
  begin
    return substr(case when p_txt is not null then p_txt||p_sp end ||p_txtadd,1,p_maxlen) ;
  end;
END RSHB_RSI_SCLIMIT;
/
