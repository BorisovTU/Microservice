

CREATE OR REPLACE PACKAGE BODY RSI_RSB_PERCENT IS

    FUNCTION IsBissextileYear (p_Date IN DATE) RETURN NUMBER
    IS
    BEGIN

        IF (EXTRACT( DAY FROM(LAST_DAY(ADD_MONTHS(p_Date,(2 - EXTRACT( MONTH FROM(p_Date))) )))) = 29)
        THEN
            RETURN 1;
        ELSE
            RETURN 0;
        END IF;

    END;

    FUNCTION CalcRateVal(ContractID IN NUMBER, DateCalc IN DATE) RETURN FLOAT
    IS
        v_RetVal    FLOAT;

    BEGIN

        IF (DateCalc < TO_DATE('04/01/2008 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))
        THEN
            v_RetVal := 6.5;
        END IF;

        IF (DateCalc >= TO_DATE('01.04.2008', 'DD.MM.YYYY') AND DateCalc < TO_DATE('01.05.2008', 'DD.MM.YYYY'))
        THEN
            v_RetVal := 4.5;
        END IF;

        IF (DateCalc >= TO_DATE('01.05.2008', 'DD.MM.YYYY'))
        THEN
            v_RetVal := 8.0;
        END IF;

        RETURN v_RetVal;

    END;

    FUNCTION RefinRate
    (
        SPVal    out anydata,
        FIID     in number,
        CalcDate in date
    ) RETURN NUMBER
    IS
    BEGIN

        SPVal := anydata.convertnumber(TO_NUMBER (LoansKernel.GetCBRefRate(FIID, CalcDate)));
        RETURN 0;

    EXCEPTION
      WHEN others
      THEN
        RETURN 1;
    END RefinRate;

    -- Преобразование строки в действительное число
    FUNCTION  ConvertToMoney ( v_CurRate OUT dprccntrate_dbt.t_rate%TYPE,
                               v_CurRateStr IN dprcratevalvar_dbt.t_rate%TYPE
                             )  RETURN NUMBER
    IS
    BEGIN
        v_CurRate := TO_NUMBER(v_CurRateStr);

        RETURN 0;
    EXCEPTION
        WHEN OTHERS THEN
           RETURN 1;
    END;


    FUNCTION  RecIsInSchedule ( v_Date IN DATE,
                                v_EndDate IN DATE
                             )  RETURN NUMBER
    IS
        v_Count     NUMBER;
    BEGIN
        SELECT COUNT(t_date) INTO v_Count FROM dprccntsched_tmp
        WHERE t_date = v_Date OR t_periodenddate = v_EndDate;

        IF (v_Count = 0)
        THEN
            RETURN 0;
        END IF;

        RETURN 1;
    END;

    PROCEDURE Prc_GenerateCalendarGraph(ContractID IN NUMBER, SchedID IN NUMBER) IS

        v_ScheduleID            NUMBER(10);
        v_ParamID               NUMBER(5);
        v_Order                 NUMBER(5);
        v_ScheduleParamType     NUMBER(10);
        v_FirstDate_raw         RAW(4);
        v_FirstDate             DATE;
        v_FirstDate_num         NUMBER;

        v_PeriodType            NUMBER(5);
        v_PeriodLength          NUMBER(5);
        v_IsFirstDateInclude    CHAR(1);
        v_DelayType             NUMBER(5);
        v_Delay                 NUMBER(5);

        v_SchedType             NUMBER;
        v_SchedKind             NUMBER;
        v_BeginDate             DATE;
        v_EndDate               DATE;

        v_ChargeDate            DATE;
        v_FinishDate            DATE;

        v_LoopFirstDate         DATE;

        v_N                     NUMBER;
        v_Count                 NUMBER;
        v_CurOperDate           DATE;

        v_LastDayOfMonth        NUMBER;
        v_Date                  DATE;
        v_Prev_EndDate          DATE;
        v_MaxExistingDate       DATE;
        v_MaxExistingEndDate    DATE;
        v_NextDate              DATE;

        v_FlagIncludePeriod     NUMBER;

        TYPE SchedPrm_cur IS REF CURSOR RETURN dprcschedprm_dbt%ROWTYPE;
        TYPE MainSched_cur IS REF CURSOR RETURN dprcmainsched_dbt%ROWTYPE;

        prm_cursor          SchedPrm_cur;
        ms_cursor               MainSched_cur;
        CURSOR date_cursor IS SELECT t_periodenddate FROM dprccntsched_tmp ORDER BY t_date ASC;

    BEGIN
        DELETE FROM dprccntsched_tmp;
        SELECT t_schedulekind, t_scheduletype INTO v_SchedKind,v_SchedType FROM dprcsched_dbt where t_scheduleid = SchedID;
        SELECT t_begindate, t_enddate INTO v_BeginDate,v_EndDate FROM dprccontract_isrv_tmp where t_contractid = ContractID;

        OPEN prm_cursor
        FOR SELECT * FROM dprcschedprm_dbt WHERE t_scheduleid = SchedID ORDER BY t_order, t_paramid ASC;

        LOOP
            FETCH prm_cursor INTO v_ScheduleID, v_ParamID, v_Order, v_ScheduleParamType, v_FirstDate_raw,
                                  v_PeriodType, v_PeriodLength, v_IsFirstDateInclude, v_DelayType, v_Delay;
            EXIT WHEN prm_cursor%NOTFOUND;

            -- Protection from wrong value
            IF (v_PeriodLength <= 0)
            THEN
                v_PeriodLength := 1;
            END IF;

            -- Converting v_FirstDate into nessesary format due to v_ScheduleParamType
            IF (v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_CALEND)
            THEN
                v_FirstDate := REP_UTL.castRawToDate(v_FirstDate_raw);
            END IF;

            IF (v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_MONTH)
            THEN
                v_FirstDate_num := UTL_RAW.CAST_TO_BINARY_INTEGER(v_FirstDate_raw,2);
            END IF;

            -- 4.1. Processing Main Schedule Parameter
            IF (v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_MAIN)
            THEN
                OPEN ms_cursor
                FOR SELECT * FROM dprcmainsched_dbt
                WHERE v_BeginDate <= t_chargedate AND t_chargedate <= v_EndDate
                ORDER BY t_chargedate ASC;

                LOOP
                    FETCH ms_cursor INTO v_ChargeDate, v_FinishDate;
                    EXIT WHEN ms_cursor%NOTFOUND;

                    IF (RecIsInSchedule(v_ChargeDate,v_FinishDate) = 0)
                    THEN
                        INSERT INTO dprccntsched_tmp (t_date,t_periodenddate,t_delaytype,t_delay)
                        VALUES ( v_ChargeDate, v_FinishDate, v_DelayType, v_Delay);
                    END IF;
                END LOOP;

                CLOSE ms_cursor;
            END IF;

            -- 4.2.
            IF (v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_END)
            THEN
                IF (RecIsInSchedule(v_EndDate,v_EndDate) = 0)
                THEN
                    INSERT INTO dprccntsched_tmp(t_date,t_periodenddate,t_delaytype,t_delay)
                    VALUES(v_EndDate, v_EndDate, v_DelayType, v_Delay);
                END IF;
            END IF;

            -- 4.3.
            IF ((v_SchedType = CONST_PRC_SCHED_TYPE_ABSOLUTE) AND (v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_CALEND))
            THEN
                IF ((RecIsInSchedule(v_FirstDate, v_FirstDate) = 0) AND (v_BeginDate <= v_FirstDate) AND (v_FirstDate <= v_EndDate))
                THEN
                    INSERT INTO dprccntsched_tmp(t_date,t_periodenddate,t_delaytype,t_delay)
                    VALUES (v_FirstDate, v_FirstDate, v_DelayType, v_Delay);
                END IF;
            END IF;

            -- 4.4.
           IF (v_SchedType = CONST_PRC_SCHED_TYPE_RELATIVE) AND ((v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_BEGIN) OR
                (v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_CALEND) OR (v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_MONTH))
            THEN
                -- Here we count LoopFirstDate
                -- 4.4.1.
                IF(v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_CALEND)
                THEN
                    v_LoopFirstDate := v_FirstDate;
                END IF;
                -- 4.4.2.
                IF(v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_BEGIN)
                THEN
                    v_LoopFirstDate := v_BeginDate;
                END IF;
                -- 4.4.3.
                IF(v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_MONTH)
                THEN
                    v_LastDayOfMonth := EXTRACT( DAY FROM (LAST_DAY(v_BeginDate)));
                    -- Day of Month
                    IF (v_FirstDate_num >= 1 AND v_FirstDate_num <= 31)
                    THEN
                        IF (v_LastDayOfMonth <= v_FirstDate_num)
                        THEN
                            v_LoopFirstDate := LAST_DAY(v_BeginDate);
                        ELSE
                            v_LoopFirstDate := v_BeginDate - EXTRACT( DAY FROM(v_BeginDate)) + v_FirstDate_num;
                        END IF;
                    END IF;
                    -- Last Work Day Of Month
                    IF (v_FirstDate_num = 32)
                    THEN
                        v_LoopFirstDate := LAST_DAY(v_BeginDate);
                        IF (RSI_RSBCALENDAR.IsWorkDay(v_LoopFirstDate) = 0)
                        THEN
                            v_LoopFirstDate := RSI_RSBCALENDAR.GetDateAfterWorkDay(v_LoopFirstDate, -1);
                        END IF;

                    END IF;
                    -- Last Calendar Day of Month
                    IF (v_FirstDate_num = 33)
                    THEN
                        v_LoopFirstDate := LAST_DAY(v_BeginDate);
                    END IF;

                END IF;


                IF (v_IsFirstDateInclude <> 'X')
                THEN
                    v_FlagIncludePeriod := 0;
                ELSE
                    v_FlagIncludePeriod := 1;
                END IF;

                v_N := 0;

                LOOP
                    -- Month
                    IF ((v_PeriodType = CONST_PRC_SCHED_PERIOD_WRK_MON) OR (v_PeriodType = CONST_PRC_SCHED_PERIOD_CAL_MON))
                    THEN
                        v_CurOperDate :=  ADD_MONTHS(v_LoopFirstDate,v_PeriodLength * v_N);

                        IF ((v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_MONTH) AND((v_FirstDate_num = 32) OR (v_FirstDate_num = 33)))
                        THEN
                            v_CurOperDate := LAST_DAY(v_CurOperDate);
                        END IF;
                    END IF;

                     -- Day
                    IF ((v_PeriodType = CONST_PRC_SCHED_PERIOD_WRK_DAY) OR (v_PeriodType = CONST_PRC_SCHED_PERIOD_CAL_DAY))
                    THEN
                        v_CurOperDate :=  v_LoopFirstDate + v_PeriodLength * v_N;
                    END IF;

                    -- Work Day
                    IF (v_PeriodType = CONST_PRC_SCHED_PERIOD_WRK_DAY)
                    THEN
                        IF (RSI_RSBCALENDAR.IsWorkDay(v_CurOperDate) = 0)
                        THEN
                            v_CurOperDate := RSI_RSBCALENDAR.GetDateAfterWorkDay(v_CurOperDate, 1);
                        END IF;
                    END IF;

                    -- Work Month
                    IF (v_PeriodType = CONST_PRC_SCHED_PERIOD_WRK_MON)
                    THEN
                        IF (RSI_RSBCALENDAR.IsWorkDay(v_CurOperDate) = 0)
                        THEN
                            IF  ((v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_MONTH) AND (v_FirstDate_num = 32))--4.4.3.2.
                            THEN
                                v_CurOperDate := RSI_RSBCALENDAR.GetDateAfterWorkDay(v_CurOperDate, -1);
                            ELSIF ((v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_MONTH) AND (v_FirstDate_num = 33))
                            THEN
                                v_CurOperDate := v_CurOperDate;
                            ELSE
                                v_CurOperDate := RSI_RSBCALENDAR.GetDateAfterWorkDay(v_CurOperDate, 1);
                            END IF;
                        END IF;
                    END IF;

                    -- Calendar Month
                    IF (v_PeriodType = CONST_PRC_SCHED_PERIOD_CAL_MON)
                    THEN
                        IF (RSI_RSBCALENDAR.IsWorkDay(v_CurOperDate) = 0)
                        THEN
                            IF  ((v_ScheduleParamType = CONST_PRC_SCHED_PARTYPE_MONTH) AND (v_FirstDate_num = 32))--4.4.3.2.
                            THEN
                                v_CurOperDate := RSI_RSBCALENDAR.GetDateAfterWorkDay(v_CurOperDate, -1);
                            END IF;
                        END IF;
                    END IF;

                    EXIT WHEN (v_CurOperDate > v_EndDate);

                    -- 4.5.
                    SELECT COUNT(t_date) INTO v_Count FROM  dprccntsched_tmp WHERE t_date = v_CurOperDate;
                    IF ((v_Count = 0) AND (v_BeginDate <= v_CurOperDate) AND (v_CurOperDate <= v_EndDate))
                    THEN
                        IF ((v_FlagIncludePeriod = 1))
                        THEN
                            IF (RecIsInSchedule(v_CurOperDate, v_CurOperDate) = 0)
                            THEN
                                INSERT INTO dprccntsched_tmp(t_date,t_periodenddate,t_delaytype,t_delay)
                                VALUES (v_CurOperDate, v_CurOperDate, v_DelayType, v_Delay);
                            END IF;
                        ELSE
                            v_FlagIncludePeriod := 1;
                        END IF;
                    END IF;

                    v_N := v_N + 1;

                END LOOP;

            END IF;


        END LOOP;

        CLOSE prm_cursor;

        -- 5.1.
        UPDATE dprccntsched_tmp SET t_delaydate = t_date + t_delay;

        UPDATE dprccntsched_tmp SET t_delaydate = RSI_RSBCALENDAR.GetDateAfterWorkDay(t_delaydate, 0)
        WHERE t_delaytype = 1;

        -- 5.2.
        -- 5.2.1.
        v_Prev_EndDate := v_BeginDate - 1;
        -- 5.2.2.
        OPEN date_cursor;

        LOOP
            FETCH date_cursor INTO v_Date;
            EXIT WHEN date_cursor%NOTFOUND;

            UPDATE dprccntsched_tmp SET t_periodbegindate = v_Prev_EndDate + 1
            WHERE t_periodenddate = v_Date;

            v_Prev_EndDate := v_Date;
        END LOOP;

        CLOSE date_cursor;

        -- 5.3.
        DELETE FROM dprccntsched_tmp WHERE t_periodbegindate > v_EndDate;

        UPDATE dprccntsched_tmp SET t_periodenddate = v_EndDate
        WHERE t_date = (SELECT MAX(t_date)FROM dprccntsched_tmp);

        -- 7.
        SELECT COUNT(t_contractid) INTO v_Count FROM dprccntsched_isrv_tmp 
        WHERE t_contractid = ContractID AND t_schedulekind = v_SchedKind;
        -- Correction of existing graphic
        IF (v_Count > 0)
        THEN
            SELECT MAX(t_date) INTO v_MaxExistingDate FROM dprccntsched_isrv_tmp 
            WHERE t_contractid = ContractID AND t_schedulekind = v_SchedKind;

            SELECT MAX(t_periodenddate) INTO v_MaxExistingEndDate FROM dprccntsched_isrv_tmp 
            WHERE t_contractid = ContractID AND t_schedulekind = v_SchedKind;

            SELECT t_periodenddate INTO v_Prev_EndDate FROM dprccntsched_isrv_tmp 
            WHERE t_contractid = ContractID AND t_schedulekind = v_SchedKind AND t_date = v_MaxExistingDate;

            UPDATE dprccntsched_tmp SET t_periodbegindate = v_Prev_EndDate + 1
            WHERE t_date = (SELECT MIN(t_date)FROM dprccntsched_tmp WHERE t_date > v_MaxExistingDate);

            INSERT INTO dprccntsched_isrv_tmp (t_cntschedid, t_contractid,t_schedulekind,t_date,t_periodbegindate,t_periodenddate,
                                          t_delaydate,t_sumcalc,t_sumfact,t_paystate)
            SELECT 0, ContractID,v_SchedKind,t_date,t_periodbegindate,t_periodenddate,t_delaydate,0,0,0
            FROM dprccntsched_tmp
            WHERE t_date > v_MaxExistingDate AND t_periodenddate > v_MaxExistingEndDate
               AND t_periodbegindate <= v_EndDate AND t_periodenddate >= v_BeginDate;

            -- 8.2.
            -- SELECT MIN(t_date) INTO v_NextDate FROM dprccntsched_tmp WHERE t_date > v_MaxExistingDate;
            SELECT MIN(t_date) INTO v_NextDate FROM dprccntsched_isrv_tmp 
            WHERE t_contractid = ContractID AND t_schedulekind = v_SchedKind AND t_datereal IS NULL;

            CASE v_SchedKind
                WHEN 1  THEN
                        UPDATE dprccontract_isrv_tmp SET t_nextdatecalc = v_NextDate 
                        WHERE t_contractid = ContractID;
                WHEN 2  THEN
                        UPDATE dprccontract_isrv_tmp SET t_nextdatecharge = v_NextDate 
                        WHERE t_contractid = ContractID;
                WHEN 3  THEN
                        UPDATE dprccontract_isrv_tmp SET t_nextdatepay = v_NextDate 
                        WHERE t_contractid = ContractID;
                WHEN 0  THEN
                        UPDATE dprccontract_isrv_tmp SET t_nextdateuser = v_NextDate 
                        WHERE t_contractid = ContractID;
            END CASE;


        -- New graphic
        ELSE
            -- 6.
            INSERT INTO dprccntsched_isrv_tmp (t_cntschedid, t_contractid,t_schedulekind,t_date,t_periodbegindate,t_periodenddate,
                                          t_delaydate,t_sumcalc,t_sumfact,t_paystate)
            (SELECT 0, ContractID, v_SchedKind, t_date, t_periodbegindate, t_periodenddate, t_delaydate, 0, 0, 0 
            FROM dprccntsched_tmp
            WHERE t_periodbegindate <= v_EndDate AND t_periodenddate >= v_BeginDate);

            -- 8.1.
            SELECT MIN(t_date)INTO v_NextDate FROM dprccntsched_tmp;

            CASE v_SchedKind
                WHEN 1  THEN
                        UPDATE dprccontract_isrv_tmp SET t_nextdatecalc = v_NextDate 
                        WHERE t_contractid = ContractID;
                WHEN 2  THEN
                        UPDATE dprccontract_isrv_tmp SET t_nextdatecharge = v_NextDate 
                        WHERE t_contractid = ContractID;
                WHEN 3  THEN
                        UPDATE dprccontract_isrv_tmp SET t_nextdatepay = v_NextDate 
                        WHERE t_contractid = ContractID;
                WHEN 0  THEN
                        UPDATE dprccontract_isrv_tmp SET t_nextdateuser = v_NextDate 
                        WHERE t_contractid = ContractID;
            END CASE;
        END IF;


    END;

    -- Проверка корректности задания ППС формулой (содрано из ЛОАНСа)
    FUNCTION CheckFlRateFormula(SpFormula IN OUT VARCHAR2, err OUT NUMBER, varname OUT VARCHAR2, Point IN NUMBER)
                                RETURN NUMBER
    IS
        empty_string     exception;
        arithm_error     exception;
        invalid_spname   exception;
        no_spvar         exception;
        no_b15           exception;

        invalid_SQL_statement   exception;
        pragma EXCEPTION_INIT (invalid_SQL_statement, -00900);

        compilation_error   exception;
        pragma EXCEPTION_INIT (compilation_error, -06550);

        zero_division   exception;
        pragma EXCEPTION_INIT (zero_division, -01476);

        cnt      number        := 1;
        rateval  number        := 0;
        in_buff  varchar2(270) := trim (SpFormula);
        buff     varchar2(55);
        fSpVarFound number     := 0;

        tmpval   number;
        tmpstr   varchar2(270);
        templ    varchar2(200);

        e_column_does_not_number EXCEPTION;
        PRAGMA EXCEPTION_INIT( e_column_does_not_number, -6502 );

    BEGIN
        err := 0;
        varname := '';

        IF (in_buff is NULL or length(in_buff) = 0)
        THEN
            RAISE empty_string;
        END IF;

       /*  не учитывает '.'  и не может учесть '12.132.12'
        IF (regexp_instr(in_buff, '^[[:digit:]]+$') > 0)
        THEN    -- задано число
            RETURN 0;
        END IF;
      */
 
        BEGIN
          tmpval := TO_NUMBER(in_buff);

          IF tmpval < 0 THEN
            RAISE arithm_error;
          END IF;

          tmpval := ROUND(tmpval, Point);

          templ := '99999999999999999999';
  
          IF Point > 0 THEN
           templ := templ || '.' || substr('999999999999999', 1, Point) || '0';
          END IF;

          tmpstr := TO_CHAR(tmpval, templ);

          IF Point > 0 THEN
            tmpstr := substr(tmpstr, 1, length(tmpstr)-1);  -- обрежем завршающий 0
          END IF;

          tmpstr := LTRIM(tmpstr);
          tmpstr := RTRIM(tmpstr);

          IF LENGTH(tmpstr) > 15  THEN
            RAISE no_b15;
          ELSE
            SpFormula := tmpstr;
          END IF;

          RETURN 0;
        EXCEPTION
          WHEN e_column_does_not_number THEN
            NULL;
        END;    

        IF (regexp_instr(in_buff, '[-+/*]{2,}') > 0)
        THEN
            RAISE arithm_error;
        END IF;

        LOOP
            -- Выбираем из строки имя спецпеременной в фигурных скобках
            buff := regexp_substr(in_buff,'\{[[:alnum:]_]+\}', 1, cnt);
            buff := ltrim(buff,'{');
            buff := rtrim(buff,'}');
            exit when (trim(buff) is NULL or length(trim(buff)) = 0);

            IF (upper(buff) <> upper('CB_Ref'))
            THEN
                RAISE invalid_spname;
            END IF;

            cnt := cnt + 1;
            fSpVarFound := 1;

        END LOOP;

        -- Заменим модификаторы даты
        in_buff := regexp_replace(in_buff,'(\}\[(-?)[[:digit:]]+\])|(\}\[(\+?)[[:digit:]]+\])','}');
        -- Заменим спецпеременные на любые константы(например 0.1)
        in_buff := regexp_replace(in_buff,'\{[[:alnum:]_]+\}',' 0.1 ');

        in_buff := 'BEGIN :1 := ' || in_buff || '; END;';

        EXECUTE IMMEDIATE in_buff
        USING OUT rateval;

        IF (fSpVarFound = 0)
        THEN
            RAISE no_spvar;
        END IF;

        RETURN 0;

    EXCEPTION

        WHEN empty_string
        THEN
            err := 6847; -- Значение выражения не задано!
            RETURN 1;
        WHEN invalid_spname
        THEN
            err := 6849; -- В выражении указана неизвестная спецпеременная %s
            varname := buff;
            RETURN 1;
        WHEN no_spvar
        THEN
            err := 6859; -- В выражении отсутствует макропеременная
            RETURN 1;
        WHEN invalid_SQL_statement
        THEN
            err := 6848;  -- Введены недопустимые символы!
            RETURN 1;
        WHEN compilation_error
        THEN
            err := 6848;  -- Введены недопустимые символы!
            RETURN 1;
        WHEN arithm_error
        THEN
            err := 6848;  -- Введены недопустимые символы!
            RETURN 1;
        WHEN zero_division
        THEN
            RETURN 0;   --Деление на ноль
        WHEN no_b15
        THEN
            err := 6873; -- Недопустимый формат значения процентной ставки
            RETURN 1;   
        WHEN others
        THEN
            RETURN 1;

    END CheckFlRateFormula;


    PROCEDURE Prc_GenCalPercRatesTable(ContractID IN NUMBER) IS

        v_RateID                NUMBER;
        v_SchedKind             NUMBER;
        v_BeginDate             DATE;
        v_EndDate               DATE;
        v_RateDate              DATE;
        v_Rate                  FLOAT;

        CURSOR val_cursor IS SELECT t_ratedate FROM dprcrateval_dbt
                        WHERE t_rateid = v_RateID
                          AND t_ratedate >= (SELECT MAX(t_ratedate) FROM dprcrateval_dbt WHERE t_ratedate <= v_BeginDate)
                          AND t_ratedate <= v_EndDate
                        ORDER BY t_ratedate ASC;


    BEGIN
        SELECT t_rateid, t_begindate, t_enddate INTO v_RateID,v_BeginDate,v_EndDate FROM dprccontract_dbt where t_contractid = ContractID;


        OPEN val_cursor;

        LOOP

            FETCH val_cursor INTO v_RateDate;
            EXIT WHEN val_cursor%NOTFOUND;

            IF (v_RateDate < v_BeginDate)
            THEN
              v_RateDate := v_BeginDate;
            END IF;

            v_Rate := CalcRateVal(ContractID, v_RateDate);
            INSERT INTO dprccntrate_dbt (t_Contractid, t_ratedate, t_rate) VALUES (ContractID, v_RateDate, v_Rate);
        END LOOP;

        CLOSE val_cursor;

    END;


    -- Функция ищет максимальную дату, которая <= MaxDate, на начало которой остаток = 0
    FUNCTION GetMaxNullDateForAccount ( Account IN daccount_dbt.t_account%TYPE,
                                        Chapter IN daccount_dbt.t_chapter%TYPE,
                                        FIID    IN daccount_dbt.t_code_currency%TYPE,
                                        MaxDate IN DATE
                                      ) RETURN DATE
    IS
        v_NullRestDate      DATE;   -- Дата записи с rest = 0
        v_MaxNullDate       DATE;   -- Максимальная нулевая дата
        v_AccountID         INTEGER;
    BEGIN

        SELECT t_AccountID INTO v_AccountID 
          FROM daccount_dbt 
         WHERE t_Account = Account
           AND t_Chapter = Chapter
           AND t_Code_Currency = FIID;

        SELECT MAX(t_restdate) INTO v_NullRestDate
        FROM drestdate_dbt
        WHERE t_accountID    = v_AccountID 
          AND t_restcurrency = FIID
          AND t_rest = 0 AND t_restdate <= MaxDate;

        IF (v_NullRestDate IS NOT NULL)
        -- Если нашли запись с нулевой суммой
        THEN
            -- пытаемся обнаружить следующую запись, если найдем, то это
            -- и будет максимальная дата с нулевым остатком
            SELECT MIN(t_restdate) INTO v_MaxNullDate
              FROM drestdate_dbt
             WHERE t_accountID    = v_AccountID 
               AND t_restcurrency = FIID
               AND t_restdate > v_NullRestDate AND t_restdate <= MaxDate;
            -- не нашли следующую запись, значит на дату MaxDate остаток = 0
            IF  (v_MaxNullDate IS NULL)
            THEN
                v_MaxNullDate := MaxDate;
            END IF;

        -- нет записи с остатком = 0, значит берем самую первую запись
        ELSE
            SELECT MIN(t_restdate) INTO v_MaxNullDate
              FROM drestdate_dbt
             WHERE t_accountID    = v_AccountID 
               AND t_restcurrency = FIID
               AND t_restdate <= MaxDate;
            -- вообще нет записей, значит на дату MaxDate остаток = 0
            IF  (v_MaxNullDate IS NULL)
            THEN
                v_MaxNullDate := MaxDate;
            END IF;

        END IF;



        RETURN v_MaxNullDate;

    END;

    -- Функция находит мимнимальный остаток за период
    FUNCTION GetMinRestOnPeriod (   Account IN daccount_dbt.t_account%TYPE,
                                    Chapter IN daccount_dbt.t_chapter%TYPE,
                                    FIID    IN daccount_dbt.t_code_currency%TYPE,
                                    PeriodBegin IN DATE,
                                    PeriodEnd IN DATE
                                ) RETURN drestdate_dbt.t_rest%TYPE
    IS
        v_StartDate     DATE;
        v_MinRest       drestdate_dbt.t_rest%TYPE;
        v_AccountID         INTEGER;
    BEGIN

        SELECT t_AccountID INTO v_AccountID 
          FROM daccount_dbt 
         WHERE t_Account = Account
           AND t_Chapter = Chapter
           AND t_Code_Currency = FIID;


        SELECT MAX(t_restdate) INTO v_StartDate
          FROM drestdate_dbt
         WHERE t_accountID    = v_AccountID 
           AND t_restcurrency = FIID
           AND t_restdate <= PeriodBegin;

            IF v_StartDate IS NOT NULL
            THEN
                SELECT MIN(ABS(t_rest)) INTO v_MinRest FROM drestdate_dbt
                 WHERE t_accountID    = v_AccountID 
                   AND t_restcurrency = FIID
                   AND t_restdate >= v_StartDate AND t_restdate <= PeriodEnd;
            ELSE
                SELECT MIN(ABS(t_rest)) INTO v_MinRest FROM drestdate_dbt
                 WHERE t_accountID    = v_AccountID 
                   AND t_restcurrency = FIID
                   AND t_restdate <= PeriodEnd;
            END IF;


        RETURN v_MinRest;
    END;


        -- Функция находит максимальный остаток за период
    FUNCTION GetMaxRestOnPeriod (   Account IN daccount_dbt.t_account%TYPE,
                                    Chapter IN daccount_dbt.t_chapter%TYPE,
                                    FIID    IN daccount_dbt.t_code_currency%TYPE,
                                    PeriodBegin IN DATE,
                                    PeriodEnd IN DATE
                                ) RETURN drestdate_dbt.t_rest%TYPE
    IS
        v_StartDate     DATE;
        v_MaxRest       drestdate_dbt.t_rest%TYPE;
        v_AccountID         INTEGER;
    BEGIN

        SELECT t_AccountID INTO v_AccountID 
          FROM daccount_dbt 
         WHERE t_Account = Account
           AND t_Chapter = Chapter
           AND t_Code_Currency = FIID;

       SELECT MAX(t_restdate) INTO v_StartDate
         FROM drestdate_dbt
        WHERE t_accountID    = v_AccountID 
          AND t_restcurrency = FIID
          AND t_restdate <= PeriodBegin;

       IF v_StartDate IS NOT NULL
       THEN
           SELECT MAX(ABS(t_rest)) INTO v_MaxRest FROM drestdate_dbt
            WHERE t_accountID    = v_AccountID 
              AND t_restcurrency = FIID
              AND t_restdate >= v_StartDate AND t_restdate <= PeriodEnd;
       ELSE
           SELECT MAX(ABS(t_rest)) INTO v_MaxRest FROM drestdate_dbt
            WHERE t_accountID    = v_AccountID 
              AND t_restcurrency = FIID
              AND t_restdate <= PeriodEnd;
       END IF;


        RETURN v_MaxRest;
    END;

    -- Функция определяет  неснижаемый остаток
    FUNCTION GetUnlowerRestOnPeriod(Account IN daccount_dbt.t_account%TYPE,
                                    Chapter IN daccount_dbt.t_chapter%TYPE,
                                    FIID    IN daccount_dbt.t_code_currency%TYPE,
                                    PeriodBegin IN DATE,
                                    PeriodEnd IN DATE,
                                    VarDays IN NUMBER       -- кол-во дней поддержания неснижаемого остатка
                                   ) RETURN drestdate_dbt.t_rest%TYPE
    IS
        v_PeriodLong    NUMBER;
        v_Rest          drestdate_dbt.t_rest%TYPE;
        v_iRest         drestdate_dbt.t_rest%TYPE; -- остаток на итеррации
        v_i             NUMBER;
    BEGIN
        v_PeriodLong := PeriodEnd - PeriodBegin;
        v_Rest := 0;
        v_iRest := 0;
        IF (VarDays >= v_PeriodLong)
        THEN
            v_Rest := GetMinRestOnPeriod(Account,Chapter,FIID,PeriodBegin,PeriodEnd);
        ELSE
            v_i := 0;
            LOOP
                EXIT WHEN (PeriodBegin + VarDays + v_i > PeriodEnd);

                v_iRest := GetMinRestOnPeriod(Account, Chapter, FIID, PeriodBegin+v_i, PeriodBegin + VarDays + v_i);
                IF (v_iRest > v_Rest)
                THEN
                    v_Rest := v_iRest;
                END IF;

                v_i := v_i + 1;
            END LOOP;
        END IF;

        RETURN v_Rest;
    END;


   -- Определение выполняемости условия параметра
    FUNCTION GetProperParamID (  RateID         IN NUMBER,
                                 RateDate       IN DATE,
                                 ParentParamID  IN NUMBER,
                                 CurDate        IN DATE,
                                 ContractID     IN NUMBER
                              ) RETURN NUMBER
    IS
        v_ID                dprcratevalvar_dbt.t_paramid%TYPE;          -- ID параметра ППС
        v_Type              dprcratevalvar_dbt.t_type%TYPE;             -- тип ППС
        v_Param             dprcratevalvar_dbt.t_param%TYPE;            -- параметр ППС
        v_Unit              dprcratevalvar_dbt.t_unit%TYPE;             -- единица измерения

        v_Condition         dprcratevalvar_dbt.t_condition%TYPE;        -- условие
        v_ConditionValue    dprcratevalvar_dbt.t_conditionvalue%TYPE;   -- значение условия

        v_ContractLong      NUMBER;
        v_ContractBegin     DATE;
        v_ContractEnd       DATE;
        v_Chapter           dprccontract_dbt.t_chapter%TYPE;
        v_FIID              dprccontract_dbt.t_fiid%TYPE;
        v_Account           dprccontract_dbt.t_account%TYPE;

        v_VarPeriod         dprcrateval_dbt.t_variableperiod%TYPE;
        v_VarDays           dprcrateval_dbt.t_variabledays%TYPE;

        v_CalcPeriodBegin   DATE;               -- начало периода расчета остатка
        v_CalcPeriodEnd     DATE;               -- конец периода расчета остатка

        v_RestAcc           drestdate_dbt.t_rest%TYPE;
        v_AccNullDate       DATE;

        v_EndLoop           BOOLEAN;            -- флаг выхода из цикла

        v_RetID                dprcratevalvar_dbt.t_paramid%TYPE;   -- значение, возвращаемое функцией

        TYPE fr_rec IS RECORD
        (
            t_ParamID           dprcratevalvar_dbt.t_paramid%TYPE,
            t_Condition         dprcratevalvar_dbt.t_condition%TYPE,
            t_ConditionValue    dprcratevalvar_dbt.t_conditionvalue%TYPE
        );

        TYPE fr_cursor_type IS REF CURSOR RETURN fr_rec;
        fr_cursor         fr_cursor_type;

    BEGIN
        v_Type := 0;
        v_ID := 0;
        v_RetID := 0;

        -- Инициализируем переменные, значения которых не будут меняться в ходе выполнения функции
        SELECT t_begindate, t_enddate, t_chapter, t_fiid, t_account
        INTO v_ContractBegin, v_ContractEnd, v_Chapter, v_FIID, v_Account
        FROM dprccontract_isrv_tmp
        WHERE t_contractid = ContractID;

        v_ContractLong := v_ContractEnd - v_ContractBegin + 1;

        SELECT t_variableperiod, t_variabledays
        INTO v_VarPeriod, v_VarDays
        FROM dprcrateval_isrv_tmp
        WHERE t_rateid = RateID AND t_ratedate = RateDate;

        SELECT NVL(MAX(t_paramid),0), NVL(MAX(t_type),0), NVL(MAX(t_param),0), NVL(MAX(t_unit),0)
        INTO v_ID, v_Type, v_Param, v_Unit FROM dprcratevalvar_dbt
        WHERE t_rateid = RateID AND t_ratedate = RateDate AND t_parentparamid = ParentParamID
            AND rownum = 1;

        -- Ничего не нашли - значит на этом уровне уже ничего нет
        IF (v_Type = 0)
        THEN
            RETURN -1;
        END IF;

        -- Проверка допустимости сочетания Type + Param
        IF (v_Type = CONST_PRC_FR_TYPE_DURATION and
           (v_Param NOT IN (CONST_PRC_FR_PARAM_REST,CONST_PRC_FR_PARAM_MIN,
           CONST_PRC_FR_PARAM_MAX,CONST_PRC_FR_PARAM_AVG,CONST_PRC_FR_PARAM_UNLOWER)))
        THEN
            RETURN 0;
        END IF;


        -- По идее не должно быть, т.к этот тип мы "отловили" заранее
        IF (v_Type = CONST_PRC_FR_TYPE_NOTCOND)
        THEN
            RETURN v_ID;
        END IF;

        v_EndLoop := false;
        OPEN fr_cursor
        FOR SELECT t_paramid, t_condition, t_conditionvalue FROM dprcratevalvar_dbt
        WHERE t_rateid = RateID AND t_ratedate = RateDate AND t_parentparamid = ParentParamID ORDER BY t_conditionvalue DESC;

        LOOP
            FETCH fr_cursor INTO v_ID, v_Condition, v_ConditionValue;
            EXIT WHEN (fr_cursor%NOTFOUND OR v_EndLoop);

            -- Тип параметра - срок
            IF (v_Type = CONST_PRC_FR_TYPE_DURATION)
            THEN

                -- Дата начала договора
                IF (v_Param = CONST_PRC_FR_PARAM_CNTBEGDATE)
                THEN
                    IF (v_Condition = '=')
                    THEN
                        IF (CurDate >= v_ContractBegin + v_ConditionValue)
                        THEN
                            v_EndLoop := true;
                            v_RetID := v_ID;
                        END IF;
                    END IF;
                    IF (v_Condition = '>')
                    THEN
                        IF (CurDate > v_ContractBegin + v_ConditionValue)
                        THEN
                            v_EndLoop := true;
                            v_RetID := v_ID;
                        END IF;
                    END IF;
                END IF;

                -- Срок договора
                IF (v_Param = CONST_PRC_FR_PARAM_CNTDURATION)
                THEN
                    IF (v_Condition = '=')
                    THEN
                        IF (v_ContractLong >= v_ConditionValue)
                        THEN
                            v_EndLoop := true;
                            v_RetID := v_ID;
                        END IF;
                    END IF;
                    IF (v_Condition = '>')
                    THEN
                        IF (v_ContractLong > v_ConditionValue)
                        THEN
                            v_EndLoop := true;
                            v_RetID := v_ID;
                        END IF;
                    END IF;
                END IF;

                -- Дата обнуления счета
                IF (v_Param = CONST_PRC_FR_PARAM_CNTNULLDATE)
                THEN
                    v_AccNullDate := GetMaxNullDateForAccount(v_Account, v_Chapter, v_FIID, CurDate);
                    IF (v_Condition = '=')
                    THEN
                        IF (CurDate - v_AccNullDate >= v_ConditionValue)
                        THEN
                            v_EndLoop := true;
                            v_RetID := v_ID;
                        END IF;
                    END IF;
                    IF (v_Condition = '>')
                    THEN
                        IF (CurDate - v_AccNullDate > v_ConditionValue)
                        THEN
                            v_EndLoop := true;
                            v_RetID := v_ID;
                        END IF;
                    END IF;
                END IF;

            END IF;

            -- Тип параметра - остаток
            IF (v_Type = CONST_PRC_FR_TYPE_REST)
            THEN

                -- определяем период расчета остатка
                IF (v_Param <> CONST_PRC_FR_PARAM_REST)
                THEN
                    IF (v_VarPeriod = CONST_PRC_FR_PERIOD_MONTH)
                    THEN
                        v_CalcPeriodBegin := CurDate - EXTRACT( DAY FROM(CurDate)) + 1;
                        IF (v_CalcPeriodBegin < v_ContractBegin)
                        THEN
                            v_CalcPeriodBegin := v_ContractBegin;
                        END IF;
                        v_CalcPeriodEnd := LAST_DAY(CurDate);
                        IF (v_CalcPeriodEnd > v_ContractEnd)
                        THEN
                            v_CalcPeriodEnd := v_ContractEnd;
                        END IF;
                    ELSE
                        v_CalcPeriodBegin := v_ContractBegin;
                        v_CalcPeriodEnd := v_ContractEnd;
                    END IF;
                END IF;

                -- Остаток счета
                IF (v_Param = CONST_PRC_FR_PARAM_REST)
                THEN
                    v_RestAcc := ABS(RSI_RSB_ACCOUNT.RESTALL(v_Account,v_Chapter,v_FIID,CurDate-1));
                END IF;

                -- Минимальный
                IF (v_Param = CONST_PRC_FR_PARAM_MIN)
                THEN
                    v_RestAcc := GetMinRestOnPeriod(v_Account,v_Chapter,v_FIID,v_CalcPeriodBegin - 1, v_CalcPeriodEnd - 1);
                END IF;

                -- Максимальный
                IF (v_Param = CONST_PRC_FR_PARAM_MAX)
                THEN
                    v_RestAcc := GetMaxRestOnPeriod(v_Account,v_Chapter,v_FIID,v_CalcPeriodBegin - 1, v_CalcPeriodEnd - 1);
                END IF;

                -- Среднехронологический
                IF (v_Param = CONST_PRC_FR_PARAM_AVG)
                THEN
                    IF (v_FIID = 0)
                    THEN
                        v_RestAcc := ABS(RSI_RSB_ACCOUNT.RESTAP(v_Account,v_CalcPeriodBegin - 1,v_CalcPeriodEnd - 1,v_Chapter,0));
                    ELSE
                        v_RestAcc := ABS(RSI_RSB_ACCOUNT.RESTAPC(v_Account,v_FIID,v_CalcPeriodBegin - 1,v_CalcPeriodEnd - 1,v_Chapter,0));
                    END IF;
                END IF;

                -- Неснижаемый
                IF (v_Param = CONST_PRC_FR_PARAM_UNLOWER)
                THEN
                    v_RestAcc := GetUnlowerRestOnPeriod(v_Account, v_Chapter, v_FIID, v_CalcPeriodBegin-1, v_CalcPeriodEnd-1, v_VarDays );
                END IF;

                -- Конвертируем валюту
                IF (v_FIID <> v_Unit)
                THEN
                    v_RestAcc := RSI_RSB_FIInstr.ConvSum(v_RestAcc,v_FIID,v_Unit,CurDate);
                END IF;

                IF (v_Condition = '=')
                THEN
                    IF (v_RestAcc >= v_ConditionValue)
                    THEN
                        v_EndLoop := true;
                        v_RetID := v_ID;
                    END IF;
                END IF;
                IF (v_Condition = '>')
                THEN
                    IF (v_RestAcc > v_ConditionValue)
                    THEN
                        v_EndLoop := true;
                        v_RetID := v_ID;
                    END IF;
                END IF;

            END IF;
        END LOOP;

        RETURN v_RetID;
    END;

    -- Расчет формулы для ППС
    FUNCTION CalcFormula (  SPVal       OUT NOCOPY anydata,
                            Formula     dprcratevalvar_dbt.t_rate%TYPE,
                            FIID        IN NUMBER,
                            DateCalc    IN DATE
                         ) RETURN NUMBER
    IS
        invalid_string   exception;
        inavalid_spname  exception;
        spvar_calc_error exception;

        invalid_SQL_statement   exception;
        pragma EXCEPTION_INIT (invalid_SQL_statement, -00900);

        compilation_error   exception;
        pragma EXCEPTION_INIT (compilation_error, -06550);

        zero_division   exception;
        pragma EXCEPTION_INIT (zero_division, -01476);

        retval      NUMBER := 0;
        out_buff    VARCHAR2 (300) := '';
        in_buff     dprcratevalvar_dbt.t_rate%TYPE := trim (Formula);
        buff        dprcratevalvar_dbt.t_rate%TYPE;
        pos         NUMBER;
        calcdate    DATE := DateCalc;
        SpVarRate   NUMBER := 0;
        err         NUMBER := 0;
        SPVal_any   anydata;
   begin

      while length (in_buff) > 0
      loop
         calcdate := DateCalc;

         pos := instr (in_buff, '{', 1);

         if pos = 0
         then                               -- Спецпеременных в строке нет
            out_buff := concat (out_buff, in_buff);
            out_buff := trim (out_buff);
            in_buff := '';
         elsif pos = 1
         then                                       -- стали на спецпеременную
            pos := instr (in_buff, '}', 1);

            if pos = 0
            then                -- если фигурная скобка не закрыта, то ошибка
               RAISE invalid_string;
            end if;

            -- вытаскиваем название спецпеременной а входную строку усекаем
            buff := substr (in_buff, 2, pos - 2);
            in_buff := substr (in_buff, pos + 1, length (in_buff) - pos + 1);
            in_buff := trim (in_buff);
            buff := trim (buff);

            IF (upper(buff) <> upper('CB_Ref'))
            THEN
                RAISE inavalid_SpName;
            END IF;

            -- определим смещение даты
            pos := instr (in_buff, '[', 1);

            if pos = 1
            then
               pos := instr (in_buff, ']', 1);

               if pos = 0
               then
                  RAISE invalid_string;
               end if;

               buff := substr (in_buff, 2, pos - 2);
               in_buff := substr (in_buff, pos + 1, length (in_buff) - pos + 1);
               in_buff := trim (in_buff);
               buff := trim (buff);

               calcdate := calcdate + to_number(buff);

            end if;

            err := RefinRate(SPVal_any, FIID, calcdate);
            if err != 0 then
               RAISE SpVar_calc_error;
            end if;

            SpVarRate := SPVal_any.accessnumber;

            out_buff := concat (out_buff,trim(to_char(SpVarRate)));

         ELSE
            out_buff := concat (out_buff, substr (in_buff, 1, pos - 1));
            in_buff := substr (in_buff, pos, length (in_buff) - pos + 1);
            in_buff := trim (in_buff);
         END IF;

      END LOOP;

      out_buff := 'BEGIN :1 := ' || replace(out_buff, ',','.') || '; END;';

      EXECUTE IMMEDIATE out_buff
                  USING OUT retval;

      SPVal := anydata.convertnumber (TO_NUMBER (retval));
      return 0;

   exception
      when invalid_string then
         --Введены недопустимые символы
         return 1;
      when inavalid_spname then
         --Введена несуществующая спецпеременная
         return 1;
      when spvar_calc_error then
         --Ошибка при расчете спецпеременной
         return 1;
      when invalid_SQL_statement then
         -- Введены недопустимые символы!
         return 1;
      when compilation_error then
         -- Введены недопустимые символы!
         return 1;
      when zero_division then
         --Деление на ноль
         return 1;
      when others then
         return 1;

   end CalcFormula;


    -- Расчет календарной таблицы процентных ставок за период
    -- для плавающей ставки
    --
    -- dprccontract_dbt dprccontract_isrv_tmp с ContractID
    -- dprcrateval_dbt  dprcrateval_isrv_tmp
    -- dprcratevalvar_dbt ??

    FUNCTION Prc_CalcRateTableForFR (   ContractID IN NUMBER,
                                        BeginDate IN DATE,
                                        EndDate IN DATE
                                     )  RETURN NUMBER
    IS
        v_RateID            NUMBER;

        v_iCurDate          DATE;
        v_BufDate           DATE;
        v_CurRecDate        DATE;
        v_NextRecDate       DATE;

        v_Point             NUMBER;
        v_strRate           VARCHAR2(100);
        v_LenStrRate        NUMBER;

        v_ParamIDNotCond    NUMBER;     -- ParamID записи типа "Без условий"
        v_CurParamID        NUMBER;     -- ParamID подходящей записи
        v_FIID              NUMBER;
        v_CurRateStr        dprcratevalvar_dbt.t_rate%TYPE; -- Найденная ставка
        v_LastRateStr       dprcratevalvar_dbt.t_rate%TYPE; -- Ставка на предыдущей итерации
        v_CurRate           dprccntrate_dbt.t_rate%TYPE; -- Рассчитанная ставка (число)
        v_LastRate          dprccntrate_dbt.t_rate%TYPE; -- Предыдущая ставка (число)

        v_ParentParamID     dprcratevalvar_dbt.t_parentparamid%TYPE; -- родительский параметр
        v_flagNotCondCheck  BOOLEAN;
        v_DoLoop            BOOLEAN;
        v_Error             NUMBER;
        SPVal_any           anydata;

    BEGIN

        v_flagNotCondCheck := false;
        v_Error := 0;
        v_CurRateStr := '0';
        v_LastRateStr := '0';
        v_CurRate := 0;

        SELECT t_rateid, t_fiid INTO v_RateID, v_FIID FROM dprccontract_isrv_tmp WHERE t_contractid = ContractID;
        v_iCurDate := BeginDate;

        SELECT MAX(t_ratedate) INTO v_CurRecDate FROM dprcrateval_isrv_tmp WHERE t_rateid = v_RateID AND t_ratedate <= v_iCurDate;

        SELECT MIN(t_ratedate) INTO v_BufDate FROM dprcrateval_isrv_tmp WHERE t_rateid = v_RateID AND t_ratedate > v_iCurDate;
        IF (v_BufDate IS NOT NULL)
        THEN
            v_NextRecDate := v_BufDate;
        END IF;

        SELECT t_Point INTO v_Point FROM dprcrate_dbt where t_RateID = v_RateID;

        LOOP
            EXIT WHEN ((v_iCurDate > EndDate) OR (v_Error <> 0));

            -- Проверяем существование параметра "Без Условий" для текущей ППС
            IF ( v_flagNotCondCheck = false)
            THEN
                SELECT NVL(MAX(t_paramid), 0) INTO v_ParamIDNotCond FROM dprcratevalvar_dbt
                WHERE t_type = CONST_PRC_FR_TYPE_NOTCOND AND t_rateid = v_RateID AND t_ratedate = v_CurRecDate AND t_parentparamid = 0;

                IF (v_ParamIDNotCond > 0)  -- Если есть параметр "Без Условий", то читаем значение
                THEN
                    v_CurParamID := v_ParamIDNotCond;
                    SELECT t_rate INTO v_CurRateStr FROM dprcratevalvar_dbt
                    WHERE t_rateid = v_RateID AND t_ratedate = v_CurRecDate AND t_paramid = v_CurParamID;
                END IF;

                v_flagNotCondCheck := true;
            END IF;

            IF (v_ParamIDNotCond = 0)  -- Если нет параметра "Без Условий"
            THEN
                v_DoLoop := true;
                v_ParentParamID := 0;
                LOOP
                    EXIT WHEN (v_DoLoop = false);
                    v_CurParamID := GetProperParamID(v_RateID, v_CurRecDate, v_ParentParamID, v_iCurDate, ContractID);
                    -- Если нет параметров на этом уровне
                    IF (v_CurParamID = -1)
                    THEN
                        v_DoLoop := false;
                    END IF;
                    -- Если не нашли подходящего параметра или нет дочерних параметров
                    IF (v_CurParamID = 0)
                    THEN
                        v_CurRateStr := '0';
                        v_DoLoop := false;
                    END IF;
                    -- Нашли параметр - берем ставку и спускаемся на уровень ниже
                    IF (v_CurParamID > 0)
                    THEN
                        SELECT t_rate INTO v_CurRateStr FROM dprcratevalvar_dbt
                        WHERE t_rateid = v_RateID AND t_ratedate = v_CurRecDate AND t_paramid = v_CurParamID;
                        v_ParentParamID := v_CurParamID;
                    END IF;
                END LOOP;

            END IF;


            -- Если поменялось значение ППС
           -- IF (v_CurRateStr <> v_LastRateStr)
          --  THEN
                -- пытаемся преобразовать ставку в число
              --  v_Error := ConvertToMoney(v_CurRate, v_CurRateStr);
                v_Error := CalcFormula(SPVal_any, v_CurRateStr, v_FIID, v_iCurDate);
                v_CurRate := SPVal_any.accessnumber;
                v_CurRate := ROUND(v_CurRate, v_Point);
                v_strRate := TO_CHAR(v_CurRate);
                v_LenStrRate := LENGTH(v_strRate);
                
                IF (v_LenStrRate > 15) THEN 
                  v_Error := 6873; --Недопустимый формат значения процентной ставки
                END IF;

                IF (v_Error = 0)
                THEN
                    -- читаем значение действующей на дату v_iCurDate ставки
                    SELECT NVL(MAX(t_rate),0) INTO v_LastRate FROM dprccntrate_tmp
                    WHERE t_contractid = ContractID AND t_ratedate =
                       (SELECT MAX(t_ratedate) FROM dprccntrate_tmp
                        WHERE t_contractid = ContractID AND t_ratedate <= v_iCurDate);

                    IF (v_CurRate <> v_LastRate)
                    THEN
                        INSERT INTO dprccntrate_tmp (t_contractid, t_ratedate, t_rate)
                        VALUES(ContractID, v_iCurDate, v_CurRate);
                    END IF;
                END IF;
           --     v_LastRateStr := v_CurRateStr;
          --  END IF;

            v_iCurDate := v_iCurDate + 1;

            -- если достигли следующей записи ППС
            IF (v_iCurDate = v_NextRecDate)
            THEN
                v_CurRecDate := v_NextRecDate;
                v_flagNotCondCheck := false;
                v_CurParamID := 0;
                SELECT MIN(t_ratedate) INTO v_BufDate FROM dprcrateval_isrv_tmp WHERE t_rateid = v_RateID AND t_ratedate > v_iCurDate;
                IF (v_BufDate IS NOT NULL)
                THEN
                    v_NextRecDate := v_BufDate;
                ELSE
                    v_NextRecDate := TO_DATE('31.12.2099','DD.MM.YYYY');
                END IF;
            END IF;

        END LOOP;

        RETURN v_Error;

    EXCEPTION

      WHEN NO_DATA_FOUND THEN
           RETURN -1;
      WHEN OTHERS THEN
           RETURN -1;
    END;

    -- Тестовая процедура формирования базы для расчета процентов
    PROCEDURE PrcBaseRestTestFunc ( ContractID      IN       NUMBER,    -- ID процентного договора
                                    BeginDate       IN       DATE,      -- дата начала периода
                                    EndDate         IN       DATE       -- дата окончания периода
                          )
    IS
        v_iRest   dprcbaserest_tmp.t_rest%TYPE  := 0;
        v_iDate  DATE;
    BEGIN
         v_iDate := BeginDate;

        LOOP

            v_iRest := EXTRACT( MONTH FROM (BeginDate))* 1000; -- остаток берем равным числу месяца х 1000

            INSERT INTO dprcbaserest_tmp (t_contractid, t_date, t_rest)
            VALUES (ContractID, v_iDate, v_iRest);
            v_iDate := v_iDate + 1;

            EXIT WHEN (v_iDate >= EndDate);
        END LOOP;

    EXCEPTION
        WHEN OTHERS
        THEN
            NULL;
    END; -- PrcBaseRestTestFunc


    -- Расчет количества дней в году, исходя из тип календаря
    FUNCTION GetDaysInYearByCalendar (
                                        Calendar   IN NUMBER,
                                        DatePeriod IN DATE
                                     ) RETURN NUMBER
    IS
        v_Days  NUMBER;
    BEGIN

        v_Days := 0;

        IF ((Calendar = 1) OR (Calendar = 2) OR (Calendar = 5) OR (Calendar = 6))
        THEN
            v_Days := 360;
        END IF;

        IF ((Calendar = 7) OR (Calendar = 8))
        THEN
            v_Days := 365;
        END IF;

        IF ((Calendar = 3) OR (Calendar = 4))
        THEN
            IF (IsBissextileYear(DatePeriod) = 1)
            THEN
                v_Days := 366;
            ELSE
                v_Days := 365;
            END IF;
        END IF;

        RETURN v_Days;
    END;

    -- Расчет количества дней в периоде, исходя из тип календаря
    FUNCTION GetDaysInPeriodByCalendar (
                                         Calendar   IN NUMBER,
                                         StartDate  IN DATE,
                                         EndDate    IN DATE,
                                         CntEndDate IN DATE
                                       ) RETURN NUMBER
    IS
        v_Days          NUMBER;
        v_FebLastDate   DATE;
        v_BeginMonth    NUMBER;
        v_EndMonth      NUMBER;
        v_EndYear       NUMBER;
        v_BeginYear     NUMBER;
        v_iMonth        NUMBER;
        v_iYear         NUMBER;

    BEGIN

        -- количество дней по умолчанию
        v_Days := EndDate - StartDate + 1;

        -- находим последнюю дату февраля
        v_FebLastDate := LAST_DAY(ADD_MONTHS(StartDate,(2 - EXTRACT( MONTH FROM(StartDate))) ));

        IF ((Calendar = 6) OR (Calendar = 8))
        THEN
            IF ((IsBissextileYear(StartDate) = 1) AND (StartDate <= v_FebLastDate) AND
                (v_FebLastDate < EndDate) AND (EndDate <> CntEndDate))
            THEN      -- если в период входит 29 февраля
                v_Days := v_Days - 1;
            END IF;
        END IF;

        IF ((Calendar = 1) OR (Calendar = 3) OR (Calendar = 5) OR (Calendar = 7))
        THEN
            -- месяц и год начала периода
            v_BeginMonth := EXTRACT( MONTH FROM(StartDate));
            v_BeginYear  := EXTRACT( YEAR FROM(StartDate));
            -- месяц и год даты, следующей за окончанием периода
            v_EndMonth := EXTRACT( MONTH FROM(EndDate + 1));
            v_EndYear  := EXTRACT( YEAR FROM(EndDate + 1));

            IF (v_BeginMonth <> v_EndMonth) OR (v_BeginYear <> v_EndYear)
            THEN

                v_iMonth := v_BeginMonth;
                v_iYear := v_BeginYear;

                LOOP    -- в цикле проверяем все месяцы от начала до конца периода

                    IF (v_iMonth IN (1,3,5,7,8,10,12))
                    THEN
                        v_Days := v_Days - 1;
                    END IF;

                    IF (v_iMonth = 2)
                    THEN        -- в период расчета попала последняя дата февраля
                        IF (IsBissextileYear(TO_DATE('01.01.'|| TO_CHAR(v_iYear), 'dd.mm.yyyy')) = 1)
                        THEN    -- високосный год
                            v_Days := v_Days + 1;
                        ELSE    -- обычный год
                            v_Days := v_Days + 2;
                        END IF;
                    END IF;

                    v_iMonth := v_iMonth + 1;
                    IF (v_iMonth > 12)
                    THEN
                        v_iMonth := 1;
                        v_iYear := v_iYear + 1;
                    END IF;

                    EXIT WHEN ((v_iMonth >= v_EndMonth) AND (v_iYear >= v_EndYear));

                END LOOP;

                IF (EndDate = LAST_DAY(ADD_MONTHS(EndDate,(2 - EXTRACT( MONTH FROM(EndDate))) )) AND (EndDate = CntEndDate))
                THEN     -- если дата окончания периода равна последнему календ.дню февраля, коррект.обратно
                    IF (IsBissextileYear(EndDate) = 1)
                    THEN    -- високосный год
                        v_Days := v_Days - 1;
                    ELSE    -- обычный год
                        v_Days := v_Days - 2;
                    END IF;
                END IF;

            END IF;

        END IF;

        RETURN v_Days;
    END;




    -- Расчет процентов за период
    -- dprccalc_isrv_tmp
    -- dprccontract_isrv_tmp
    -- dprcrate_isrv_tmp
    -- dprcrateval_isrv_tmp
    FUNCTION Prc_CalcPercentValues ( TotalSum OUT dprccntsched_dbt.t_sumcalc%TYPE,
                                     ContractID IN NUMBER,
                                     BeginDate IN DATE,
                                     EndDate IN DATE,
                                     DailyFlag IN NUMBER
                                   ) RETURN NUMBER
    IS

        v_Account               dprccontract_dbt.t_account%TYPE;
        v_FIID                  dprccontract_dbt.t_fiid%TYPE;
        v_Chapter               dprccontract_dbt.t_chapter%TYPE;

        v_RestType              dprccalc_dbt.t_resttype%TYPE;
        v_Calendar              dprccalc_dbt.t_calendar%TYPE;
        v_CalcBaseType          dprccalc_dbt.t_calcbasetype%TYPE;
        v_MacroProc             dprccalc_dbt.t_macroproc%TYPE;
        v_RateType              dprcrate_dbt.t_ratetype%TYPE;
        v_RateID                dprcrate_dbt.t_rateid%TYPE;

        v_PeriodBeginDate       DATE;
        v_PeriodEndDate         DATE;
        v_iDate                 DATE;
        v_iDate_GetRest         DATE;   -- дата, за которую берем остаток для ежедневного расчета
        v_iRestDate             DATE;
        v_iRateDate             DATE;
        v_iRate                 dprcrateval_dbt.t_rate%TYPE;
        v_iPoint                dprcrate_dbt.t_point%TYPE;
        v_iRest                 dprccntsched_dbt.t_sumcalc%TYPE;
        v_iSum                  dprccntsched_dbt.t_sumcalc%TYPE;
        v_TotalSum              dprccntsched_dbt.t_sumcalc%TYPE;

        v_iStartDate            DATE;
        v_iEndDate              DATE;
        v_iEndYearDate          DATE;
        v_CntEndDate            DATE;
        v_Year1                 NUMBER(5);
        v_Year2                 NUMBER(5);

        v_DaysInYear            NUMBER(5);
        v_DaysInPeriod          NUMBER(5);

        v_AccountID             INTEGER;

        cmd                     VARCHAR2 (100) := '';

        flagExit                BOOLEAN;

        TYPE RestDate_rec IS RECORD
        (
            T_DATE      drestdate_dbt.t_restdate%TYPE
        );

        TYPE date_cursor_type IS REF CURSOR RETURN RestDate_rec;
        restdate_cursor         date_cursor_type;
        cntrate_cursor          date_cursor_type;

    BEGIN
        SELECT contr.t_account, contr.t_fiid, calc.t_resttype, contr.t_chapter, contr.t_enddate,
               calc.t_calendar, calc.t_calcbasetype, calc.t_macroproc, rate.t_ratetype, rate.t_rateid, rate.t_point
        INTO v_Account, v_FIID, v_RestType, v_Chapter, v_CntEndDate, v_Calendar, v_CalcBaseType,
             v_MacroProc, v_RateType, v_RateID, v_iPoint
        FROM dprccalc_isrv_tmp calc, dprccontract_isrv_tmp contr, dprcrate_isrv_tmp rate
        WHERE contr.t_rateid = rate.t_rateid AND contr.t_calcid = calc.t_calcid AND contr.t_contractid = ContractID;

        v_PeriodBeginDate   := BeginDate;
        v_PeriodEndDate     := EndDate;

        DELETE FROM dprccalcprcval_tmp;
        DELETE FROM dprcbaserest_tmp where t_contractid = ContractID;

        -- Составляем команду запуска пользовательской ХП для расчета базы
        IF (v_CalcBaseType = CONST_PRC_CALC_BASETYPE_USER)
        THEN
            IF (v_MacroProc IS NOT NULL)
            THEN
                cmd := 'BEGIN ' || v_MacroProc || '(:1, :2, :3); END;';

                EXECUTE IMMEDIATE cmd
                USING   IN    ContractID,
                        IN    BeginDate,
                        IN    EndDate;
            ELSE
                RETURN -1;  -- если имя ХП не указано, то вылетаем с ошибкой
            END IF;
        END IF;

        IF (DailyFlag = 1)
        THEN -- ежедневный расчет

            v_iDate := v_PeriodBeginDate;
            LOOP

                IF (v_CalcBaseType = CONST_PRC_CALC_BASETYPE_USER)
                THEN    -- База - пользовательская
                    SELECT NVL(SUM(t_rest), 0) INTO v_iRest FROM dprcbaserest_tmp
                    WHERE t_date = (SELECT MAX(t_date) FROM dprcbaserest_tmp
                                    WHERE t_date <= v_iDate AND t_contractid = ContractID)
                          AND t_contractid = ContractID;

                ELSE    -- База - остаток лицевого счета
                    IF (v_RestType = 1)
                    THEN        -- остаток - входящий
                        v_iDate_GetRest := v_iDate - 1;
                    ELSE        -- остаток - исходящий
                        v_iDate_GetRest := v_iDate;
                    END IF;

                    IF (v_FIID = 0)
                    THEN
                        v_iRest := ABS(RSI_RSB_ACCOUNT.RESTA(v_Account, v_iDate_GetRest, v_Chapter, v_iRest));
                    ELSE
                        v_iRest := ABS(RSI_RSB_ACCOUNT.RESTAC(v_Account, v_FIID, v_iDate_GetRest, v_Chapter, v_iRest));
                    END IF;
                END IF;

                IF (v_RateType = CONST_PRC_RATE_TYPE_USER OR v_RateType = CONST_PRC_RATE_TYPE_FLOAT)
                THEN    -- пользовательская или плавающая ставка
                    SELECT NVL(MAX(cntrate.t_rate),0) INTO v_iRate FROM dprccntrate_tmp cntrate
                    WHERE cntrate.t_contractid = ContractID AND cntrate.t_ratedate =
                    (SELECT MAX(t_ratedate) FROM dprccntrate_tmp WHERE t_ratedate <= v_iDate AND t_contractid = ContractID);
                ELSE    -- абсолютная ставка
                    SELECT NVL(MAX(rateval.t_rate),0) INTO v_iRate FROM dprcrateval_isrv_tmp rateval
                    WHERE rateval.t_rateid = v_RateID AND rateval.t_ratedate =
                    (SELECT MAX(t_ratedate) FROM dprcrateval_isrv_tmp WHERE t_ratedate <= v_iDate AND t_rateid = v_RateID);

                END IF;

                v_DaysInYear := GetDaysInYearByCalendar(v_Calendar, v_iDate);

                v_iSum := v_iRest * (v_iRate/100)/v_DaysInYear;
                v_iSum := ROUND(v_iSum, 5);

                INSERT INTO dprccalcprcval_tmp (t_startdate, t_enddate, t_daysnum, t_rest, t_rate, t_sum, t_point)
                VALUES (v_iDate, v_iDate, 1, v_iRest, v_iRate, v_iSum, v_iPoint);
                v_iDate := v_iDate + 1;

                EXIT WHEN (v_iDate > v_PeriodEndDate);
            END LOOP;

        ELSE -- обычный расчет (считаем периоды постоянства)

            IF (v_CalcBaseType = CONST_PRC_CALC_BASETYPE_USER)
            THEN    -- База - пользовательская
                OPEN restdate_cursor
                FOR SELECT t_date FROM dprcbaserest_tmp
                WHERE t_date > v_PeriodBeginDate AND t_date <= v_PeriodEndDate AND t_contractid = ContractID
                ORDER BY t_date ASC;

            ELSE    -- База - остаток лицевого счета
               SELECT t_AccountID INTO v_AccountID 
                 FROM daccount_dbt 
                WHERE t_Account = v_Account
                  AND t_Chapter = v_Chapter
                  AND t_Code_Currency = v_FIID;
                
                  IF (v_RestType = 1)
                  THEN    -- Тип остатка - входящий
                      OPEN restdate_cursor
                      FOR SELECT t_restdate FROM drestdate_dbt
                      WHERE t_AccountID = v_AccountID
                        AND t_restcurrency = v_FIID
                        AND t_restdate > v_PeriodBeginDate - 1 AND t_restdate <= v_PeriodEndDate - 1
                      ORDER BY t_restdate ASC;
                  ELSE
                      OPEN restdate_cursor
                      FOR SELECT t_restdate FROM drestdate_dbt
                      WHERE t_AccountID = v_AccountID
                        AND t_restcurrency = v_FIID
                        AND t_restdate > v_PeriodBeginDate AND t_restdate <= v_PeriodEndDate
                        ORDER BY t_restdate ASC;
                  END IF;

            END IF;

            IF (v_RateType = CONST_PRC_RATE_TYPE_USER OR v_RateType = CONST_PRC_RATE_TYPE_FLOAT)
            THEN    -- пользовательская или плавающая ставка
                OPEN cntrate_cursor
                FOR SELECT t_ratedate FROM dprccntrate_tmp
                WHERE t_contractid = ContractID AND t_ratedate > v_PeriodBeginDate AND t_ratedate <= v_PeriodEndDate
                ORDER BY t_ratedate ASC;
            ELSE    -- абсолютная ставка
                OPEN cntrate_cursor
                FOR SELECT t_ratedate FROM dprcrateval_isrv_tmp 
                WHERE t_rateid = v_RateID AND t_ratedate > v_PeriodBeginDate AND t_ratedate <= v_PeriodEndDate
                ORDER BY t_ratedate ASC;
            END IF;

            -- Задаем значения первой записи
            v_iStartDate := v_PeriodBeginDate;

            flagExit := false;
            LOOP
                -- определяем конечные даты периодов постоянства по базе и по ставке
                IF (v_iStartDate = v_PeriodBeginDate)
                THEN -- расчет первого периода
                        FETCH restdate_cursor INTO v_iRestDate;
                        IF (restdate_cursor%NOTFOUND)
                        THEN
                            v_iRestDate := v_PeriodEndDate;
                        ELSE
                        IF ((v_RestType = 1) AND (v_CalcBaseType = CONST_PRC_CALC_BASETYPE_REST))
                        THEN    -- Тип остатка - входящий (только для базы - остаток счета)
                                v_iRestDate := v_iRestDate + 1;
                            END IF;
                        END IF;

                    FETCH cntrate_cursor INTO v_iRateDate;
                    IF (cntrate_cursor%NOTFOUND)
                    THEN
                        v_iRateDate := v_PeriodEndDate;
                    END IF;

                ELSE -- расчет не первого периода
                    IF ((v_iRestDate < v_iRateDate) OR (cntrate_cursor%NOTFOUND))
                    THEN
                            FETCH restdate_cursor INTO v_iRestDate;
                            IF (restdate_cursor%NOTFOUND)
                            THEN
                                v_iRestDate := v_PeriodEndDate;
                            ELSE
                            IF ((v_RestType = 1) AND (v_CalcBaseType = CONST_PRC_CALC_BASETYPE_REST))
                            THEN    -- Тип остатка - входящий (только для базы - остаток счета)
                                    v_iRestDate := v_iRestDate + 1;
                                END IF;
                            END IF;

                    ELSE
                        FETCH cntrate_cursor INTO v_iRateDate;
                        IF (cntrate_cursor%NOTFOUND)
                        THEN
                            v_iRateDate := v_PeriodEndDate;
                        END IF;

                    END IF;

                END IF;

                -- Проверяем условие выхода из цикла
                    flagExit := (restdate_cursor%NOTFOUND AND cntrate_cursor%NOTFOUND AND v_iEndDate = v_PeriodEndDate);
                EXIT WHEN flagExit;

                -- определяем конечную дату периода постоянства
                IF (v_iRestDate < v_iRateDate)
                THEN
                    v_iEndDate := v_iRestDate - 1;
                ELSE
                    v_iEndDate := v_iRateDate - 1;
                END IF;

                -- если все даты закончились - значит последний период
                IF (restdate_cursor%NOTFOUND AND cntrate_cursor%NOTFOUND)
                THEN
                        v_iEndDate := v_PeriodEndDate;
                    END IF;

                -- Определяем базу расчета на данном периоде постоянства
                IF (v_CalcBaseType = CONST_PRC_CALC_BASETYPE_USER)
                THEN    -- База - пользовательская
                    -- TFS. без функции SUM вылетает по EXFEPTION NO_DATA_FOUND 
                    SELECT NVL(SUM(t_rest), 0) INTO v_iRest FROM dprcbaserest_tmp
                    WHERE t_date = (SELECT MAX(t_date) FROM dprcbaserest_tmp WHERE t_date <= v_iEndDate AND t_contractid = ContractID)
                      AND t_contractid = ContractID;

                ELSE    -- База - остаток лицевого счета
                    IF (v_FIID = 0)
                    THEN
                        IF (v_RestType = 1)
                        THEN   -- Тип остатка - входящий
                            v_iRest := ABS(RSI_RSB_ACCOUNT.RESTA(v_Account, v_iEndDate - 1, v_Chapter, v_iRest));
                        ELSE
                            v_iRest := ABS(RSI_RSB_ACCOUNT.RESTA(v_Account, v_iEndDate, v_Chapter, v_iRest));
                        END IF;
                    ELSE
                        IF (v_RestType = 1)
                        THEN   -- Тип остатка - входящий
                            v_iRest := ABS(RSI_RSB_ACCOUNT.RESTAC(v_Account, v_FIID, v_iEndDate - 1, v_Chapter, v_iRest));
                        ELSE
                            v_iRest := ABS(RSI_RSB_ACCOUNT.RESTAC(v_Account, v_FIID, v_iEndDate, v_Chapter, v_iRest));
                        END IF;
                    END IF;
                END IF;

                IF (v_RateType = CONST_PRC_RATE_TYPE_USER OR v_RateType = CONST_PRC_RATE_TYPE_FLOAT)
                THEN       -- пользовательская или плавающая ставка
                    SELECT NVL(MAX(cntrate.t_rate),0)INTO v_iRate FROM dprccntrate_tmp cntrate
                    WHERE cntrate.t_contractid = ContractID AND cntrate.t_ratedate =
                    (SELECT MAX(t_ratedate) FROM dprccntrate_tmp WHERE t_ratedate <= v_iStartDate AND t_contractid = ContractID);
                ELSE       -- абсолютная ставка
                    SELECT NVL(MAX(rateval.t_rate),0) INTO v_iRate FROM dprcrateval_isrv_tmp rateval
                    WHERE rateval.t_rateid = v_RateID AND rateval.t_ratedate =
                    (SELECT MAX(t_ratedate) FROM dprcrateval_isrv_tmp WHERE t_ratedate <= v_iStartDate AND t_rateid = v_RateID);

                END IF;

                v_Year1 := EXTRACT( YEAR FROM(v_iStartDate));
                v_Year2 := EXTRACT( YEAR FROM(v_iEndDate));
                IF (
                    (v_Year1 != v_Year2) AND
                    (v_iStartDate < v_iEndDate) 
                    )
                THEN -- Если период если период больше года
                LOOP
                    v_iEndYearDate := LAST_DAY(ADD_MONTHS(v_iStartDate,(12 - EXTRACT( MONTH FROM(v_iStartDate))) ));
                    if (v_iEndYearDate > v_iEndDate)
                    THEN
                        v_iEndYearDate := v_iEndDate;
                    END IF;
          
                    v_DaysInYear := GetDaysInYearByCalendar(v_Calendar, v_iStartDate);
                    v_DaysInPeriod := GetDaysInPeriodByCalendar(v_Calendar, v_iStartDate, v_iEndYearDate, v_CntEndDate);

                    v_iSum := v_iRest * v_DaysInPeriod * (v_iRate/100)/v_DaysInYear;
                    v_iSum := ABS(ROUND(v_iSum, 5));

                    INSERT INTO dprccalcprcval_tmp (t_startdate, t_enddate, t_daysnum, t_rest, t_rate, t_sum, t_point)
                    VALUES (v_iStartDate, v_iEndYearDate, v_iEndYearDate - v_iStartDate + 1 , v_iRest, v_iRate, v_iSum, v_iPoint);
          
                    v_iStartDate := v_iEndYearDate + 1;
      
                    EXIT WHEN v_iEndYearDate = v_iEndDate;
                END LOOP;
                ELSE -- Период период не более одного года
                    v_DaysInYear := GetDaysInYearByCalendar(v_Calendar, v_iStartDate);
                    v_DaysInPeriod := GetDaysInPeriodByCalendar(v_Calendar, v_iStartDate, v_iEndDate, v_CntEndDate);

                    v_iSum := v_iRest * v_DaysInPeriod * (v_iRate/100)/v_DaysInYear;
                    v_iSum := ABS(ROUND(v_iSum, 5));

                    INSERT INTO dprccalcprcval_tmp (t_startdate, t_enddate, t_daysnum, t_rest, t_rate, t_sum, t_point)
                    VALUES (v_iStartDate, v_iEndDate, v_iEndDate - v_iStartDate + 1 , v_iRest, v_iRate, v_iSum, v_iPoint);
                END IF;

                v_iStartDate := v_iEndDate + 1;

            END LOOP;

        END IF;

        SELECT NVL(SUM(t_sum), 0) "TotSum" INTO v_TotalSum FROM dprccalcprcval_tmp;
        TotalSum := ABS(ROUND(v_TotalSum, 5));

        RETURN 0;

    EXCEPTION

      WHEN NO_DATA_FOUND THEN
           RETURN -1;
      WHEN OTHERS THEN
           RETURN -1;
    END;


    -- Процедура формирования базы для расчета процентов для договора векселя
    PROCEDURE PrcBillRestList ( ContractID      IN       NUMBER,    -- ID процентного договора
                                BeginDate       IN       DATE,      -- дата начала периода
                                EndDate         IN       DATE       -- дата окончания периода
                              )
    IS
        v_iRest   dprcbaserest_tmp.t_rest%TYPE  := 0;
        v_iDate  DATE;
        v_Principal ddl_leg_dbt.t_Principal%TYPE  := 0;
        v_ContractBeginDate dprccontract_dbt.t_BeginDate%TYPE := TO_DATE('01.01.0001','DD.MM.YYYY');
    BEGIN

        IF BeginDate > EndDate THEN
          RETURN;
        END IF;

        v_iDate := BeginDate;

        BEGIN
          SELECT leg.t_Principal, prcc.t_BeginDate INTO v_Principal, v_ContractBeginDate
            FROM ddl_leg_dbt leg, dvsbanner_dbt bnr, dprccontract_dbt prcc
           WHERE prcc.t_ContractID = ContractID
             AND prcc.t_ObjectType = 651
             AND bnr.t_BCID = TO_NUMBER(prcc.t_ObjectID)
             AND leg.t_DealID = bnr.t_BCID
             AND leg.t_LegID = 0
             AND leg.t_LegKind = RSB_BILL.LEG_KIND_VSBANNER;

          EXCEPTION
           WHEN OTHERS
            THEN v_Principal := 0;
        END;

        LOOP

            v_iRest := v_Principal;

            IF v_iDate >= v_ContractBeginDate THEN
              INSERT INTO dprcbaserest_tmp (t_contractid, t_date, t_rest)
              VALUES (ContractID, v_iDate, v_iRest);
            END IF;

            v_iDate := v_iDate + 1;

            EXIT WHEN (v_iDate >= EndDate);
        END LOOP;

    EXCEPTION
        WHEN OTHERS
        THEN
            NULL;
    END; -- PrcBillRestList

    -- Генерация КТПС и последующий расчет процентов
    FUNCTION Prc_CalcCntPercents  (  TotalSum       OUT dprccntsched_dbt.t_sumcalc%TYPE, -- рассчитанная сумма
                                     ContractID     IN NUMBER,      -- ID договора
                                     BeginDate      IN DATE,        -- Дата начала периода
                                     EndDate        IN DATE,        -- Дата окончания периода
                                     FlagDailyCalc  IN NUMBER,      -- Флаг, 1 - ежедневный расчет
                                     FlagSaveRates  IN NUMBER       -- Флаг, 1 - сохранить КТПС в БД
                                   ) RETURN NUMBER
    IS

        v_RateType              dprcrate_dbt.t_ratetype%TYPE;
        v_RateID                dprcrate_dbt.t_rateid%TYPE;
        v_iPoint                dprcrate_dbt.t_point%TYPE;
        v_RetVal                NUMBER := 0;

    BEGIN
        v_RetVal := 0;

        SELECT rate.t_rateid, rate.t_ratetype, rate.t_point
        INTO v_RateID, v_RateType, v_iPoint
        FROM dprccontract_dbt cnt, dprcrate_dbt rate
        WHERE cnt.t_rateid = rate.t_rateid AND cnt.t_contractid = ContractID;

        IF (v_RateType = CONST_PRC_RATE_TYPE_USER)
        THEN
            v_RetVal := -2;  -- для пользовательской ставки функция не работает
        END IF;

        IF (v_RetVal = 0 ) THEN
            DELETE FROM dprccntrate_tmp WHERE t_contractid = ContractID AND t_ratedate >= BeginDate AND t_ratedate <= EndDate;

            -- dprccalc_isrv_tmp
            DELETE FROM DPRCCALC_ISRV_TMP WHERE t_CalcID IN (SELECT t_CalcID FROM dprccontract_dbt WHERE t_contractid = ContractID);

            INSERT INTO DPRCCALC_ISRV_TMP (T_CALCID, T_CALENDAR, T_RESTTYPE, T_CALCBASETYPE, T_MACROPROC)
            SELECT cl.t_CalcID, cl.t_Calendar, cl.t_RestType, cl.t_CalcBaseType, cl.t_MacroProc    
              FROM DPRCCALC_DBT cl, dprccontract_dbt cn
             WHERE cl.t_CalcID = cn.t_CalcID
               AND cn.t_contractid = ContractID;

            -- dprccontract_isrv_tmp
            DELETE FROM dprccontract_isrv_tmp  WHERE t_contractid = ContractID;

            INSERT INTO dprccontract_isrv_tmp (t_contractid, t_chapter, t_fiid, t_account, t_calcid, t_rateid, t_begindate, 
                                               t_enddate, t_nextdatecalc, t_nextdatecharge, t_nextdatepay, t_nextdateuser)
            SELECT t_contractid, t_chapter, t_fiid, t_account, t_calcid, t_rateid, t_begindate,
                   t_enddate, t_nextdatecalc, t_nextdatecharge, t_nextdatepay, t_nextdateuser
              FROM dprccontract_dbt
             WHERE t_contractid = ContractID;
            
            -- dprcrate_isrv_tmp
            DELETE FROM dprcrate_isrv_tmp WHERE t_rateid = v_RateID;  

            INSERT INTO dprcrate_isrv_tmp (t_RateID, t_RateType, t_Point) VALUES (v_RateID, v_RateType, v_iPoint);

            -- dprcrateval_isrv_tmp
            DELETE FROM dprcrateval_isrv_tmp WHERE t_rateid = v_RateID;  
            
            INSERT INTO dprcrateval_isrv_tmp (t_rateid, t_ratedate, t_rate, t_variableperiod, t_variabledays)
                 SELECT t_rateid, t_ratedate, t_rate, t_variableperiod, t_variabledays
                   FROM dprcrateval_dbt
                  WHERE t_rateid = v_RateID;
        END IF;

        IF (v_RetVal = 0 AND v_RateType = CONST_PRC_RATE_TYPE_FLOAT)  -- рассчитываем КТПС для плавающей ставки
        THEN
            v_RetVal := Prc_CalcRateTableForFR(ContractID, BeginDate, EndDate);

            IF (FlagSaveRates = 1)  -- сохраняем результаты расчета КТПС в БД
            THEN
                DELETE FROM dprccntrate_dbt WHERE t_contractid = ContractID AND t_ratedate >= BeginDate AND t_ratedate <= EndDate;

                INSERT INTO dprccntrate_dbt
                SELECT * FROM dprccntrate_tmp WHERE t_contractid = ContractID AND t_ratedate >= BeginDate AND t_ratedate <= EndDate;
            END IF;
        END IF;

        IF (v_RetVal = 0)
        THEN
            v_RetVal := Prc_CalcPercentValues(TotalSum, ContractID, BeginDate, EndDate, FlagDailyCalc);
        END IF;

        RETURN v_RetVal;

    EXCEPTION

      WHEN NO_DATA_FOUND THEN
           RETURN -1;
      WHEN OTHERS THEN
           RETURN -1;
    END;

    FUNCTION Prc_GetPercentSum  (    ContractID     IN NUMBER,      -- ID договора
                                     BeginDate      IN DATE,        -- Дата начала периода
                                     EndDate        IN DATE,
                                     TotalSum       IN OUT NUMBER   -- рассчитанная сумма
                                ) RETURN NUMBER

    IS
        v_TotalSum    dprccntsched_dbt.t_sumcalc%TYPE := 0; -- рассчитанная сумма
        v_Res         NUMBER;
    BEGIN

        v_Res := Prc_CalcCntPercents(v_TotalSum, ContractID, BeginDate, EndDate, 1, 0);
        IF (v_Res = 0) THEN
            TotalSum := v_TotalSum;
        END IF;

        RETURN v_Res;

    EXCEPTION

      WHEN NO_DATA_FOUND THEN
           RETURN -1;
      WHEN OTHERS THEN
           RETURN -1;
    END;


    PROCEDURE InsertErrorRecForReporting(p_ContractID IN NUMBER, p_Date IN DATE, p_Error IN NUMBER)
    IS
    BEGIN

     DELETE FROM dprcreporting_tmp WHERE t_contractid = p_ContractID;

     INSERT INTO dprcreporting_tmp (t_contractid, t_date, t_sum, t_Error)
     VALUES (p_ContractID, p_Date, NULL, p_Error);

    END; 


    -- Расчет процентов для Reporting
    FUNCTION PrcCalcPercentsForReporting(ContractID IN NUMBER, BeginDate IN DATE) RETURN NUMBER
    IS
        v_RateID            NUMBER  := 0;
        v_RateType          NUMBER;
        v_RetVal            NUMBER;
        v_PeriodBeginDateID DATE;
        v_PeriodEndDate     DATE;
        v_Date              DATE;
        v_DelayDate         DATE;
        v_PeriodSum         dprcreporting_tmp.T_SUM%type;
        v_SumCalc           dprcreporting_tmp.T_SUM%type;
        v_SumFact           dprcreporting_tmp.T_SUM%type;

        v_TmpVal            NUMBER;


        TYPE PeriodDates_rec IS RECORD
        (
            T_DATE          DATE,
            T_BEGINDATE     DATE,
            T_ENDDATE       DATE,
            T_SUMCALC       dprcreporting_tmp.T_SUM%type,
            T_SUMFACT       dprcreporting_tmp.T_SUM%type,
            T_DELAYDATE     DATE
        );

        TYPE cntsched_cur_type IS REF CURSOR RETURN PeriodDates_rec;
        cntsched_cursor          cntsched_cur_type;

    BEGIN
        v_RetVal := 0;

        SELECT rate.t_rateid, rate.t_ratetype
        INTO v_RateID, v_RateType
        FROM dprccontract_dbt cnt, dprcrate_dbt rate
        WHERE cnt.t_rateid = rate.t_rateid AND cnt.t_contractid = ContractID;

        IF (v_RateType = CONST_PRC_RATE_TYPE_USER)  THEN
            -- для пользовательской ставки функция не работает
            InsertErrorRecForReporting(ContractID, BeginDate, -2); 
            v_RetVal := -2;  
        END IF;

        IF v_RetVal = 0 THEN
          BEGIN 
            SELECT 1 INTO v_TmpVal
            FROM dprcreporting_tmp
            WHERE t_contractid = ContractID;

            v_RetVal := -3; 
          EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
          END;
        END IF;

        IF v_RetVal = 0 THEN
          OPEN cntsched_cursor
          FOR SELECT t_date, t_periodbegindate, t_periodenddate, t_sumcalc, t_sumfact, t_delaydate FROM dprccntsched_dbt
              WHERE t_contractid = ContractID AND t_schedulekind = 3
                  AND ((t_date > BeginDate) OR (t_date <= BeginDate AND t_delaydate > BeginDate AND t_paystate = 0));

          LOOP
              FETCH cntsched_cursor INTO v_Date, v_PeriodBeginDateID, v_PeriodEndDate, v_SumCalc, v_SumFact, v_DelayDate;
              EXIT WHEN cntsched_cursor%NOTFOUND OR v_RetVal <> 0;

              v_RetVal := Prc_GetPercentSum(ContractID, v_PeriodBeginDateID, v_PeriodEndDate, v_PeriodSum);

              IF v_RetVal = 0 THEN 
                v_PeriodSum := ROUND( v_PeriodSum, 2 );

                INSERT INTO dprcreporting_tmp (t_contractid, t_date, t_sum, t_Error)
                VALUES (ContractID, v_DelayDate, v_PeriodSum, 0);
              ELSE
                InsertErrorRecForReporting(ContractID, BeginDate, v_RetVal); 
              END IF;

          END LOOP;

          CLOSE cntsched_cursor;
        END IF;

        IF v_RetVal = 0 THEN
          OPEN cntsched_cursor
          FOR SELECT t_date, t_periodbegindate, t_periodenddate, t_sumcalc, t_sumfact, t_delaydate FROM dprccntsched_dbt
              WHERE t_contractid = ContractID AND t_schedulekind = 3
                  AND t_date <= BeginDate AND t_delaydate > BeginDate AND t_paystate IN(1,2,3,5);

          LOOP
              FETCH cntsched_cursor INTO v_Date, v_PeriodBeginDateID, v_PeriodEndDate, v_SumCalc, v_SumFact, v_DelayDate;
              EXIT WHEN cntsched_cursor%NOTFOUND;

              v_PeriodSum := v_SumCalc - v_SumFact;
              v_PeriodSum := ROUND( v_PeriodSum, 2 );

              INSERT INTO dprcreporting_tmp (t_contractid, t_date, t_sum, t_Error)
              VALUES (ContractID, v_DelayDate, v_PeriodSum, 0);

          END LOOP;

          CLOSE cntsched_cursor;
        END IF;

        RETURN v_RetVal;

    EXCEPTION

      WHEN NO_DATA_FOUND THEN
           RETURN -1;
      WHEN OTHERS THEN
           RETURN -1;

    END;    -- PrcCalcPercentsForReporting(ContractID IN NUMBER, BeginDate IN DATE)
    
    -- Получение имени sequence для указанного бэк-офиса
    FUNCTION GetPrcContractSequenceName(backOffice IN NUMBER) RETURN VARCHAR2
    IS
    BEGIN
      RETURN 'DPRCCONTRACT_DBT_BO' || backOffice || '_SEQ';
    END;

    -- Создание sequence для указанного бэк-офиса
    -- возвращает 0 если sequence существует или создан, -1 в случае ошибки
    FUNCTION PrcContractCreateSequence(backOffice IN NUMBER) RETURN NUMBER
    IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      v_result NUMBER := 0;
      v_cnt NUMBER;
      v_maxNumber NUMBER;
      v_sequenceName VARCHAR2(40);
      v_sql VARCHAR2(150);
    BEGIN
      v_sequenceName := GetPrcContractSequenceName(backOffice);
  
      SELECT COUNT(1) 
        INTO v_cnt 
        FROM user_sequences
       WHERE UPPER(sequence_name) = v_sequenceName;
  
      IF v_cnt = 0 THEN
        SELECT NVL(MAX(t_number), 0)
          INTO v_maxNumber
          FROM dPrcContract_dbt
         WHERE t_backOffice = backOffice;
     
        v_sql := 'CREATE SEQUENCE ' || v_sequenceName || ' ' ||
                 'START WITH ' || (v_maxNumber + 1) || ' ' ||
                 'MAXVALUE 999999999999999999999999999 ' ||
                 'NOCYCLE NOCACHE NOORDER';
        BEGIN
          EXECUTE IMMEDIATE v_sql;
        EXCEPTION WHEN OTHERS THEN
          v_result := -1;
        END;
      END IF;
  
      RETURN v_result;
    END;
    
    -- Получение номера процентного договора (t_number таблицы dPrcContract_dbt) в разрезе бэк-офисов с использованием sequence
    -- возвращает номер из sequence для указанного бэк-офиса или -1 в случае ошибки
    FUNCTION GetPrcContractNextNumber(backOffice IN NUMBER -- ID бэк-офиса
                                     ) RETURN NUMBER
    IS
      sequence_doesnt_exist EXCEPTION;
      PRAGMA EXCEPTION_INIT (sequence_doesnt_exist, -2289);
  
      v_result NUMBER := 0;
      v_BONumberCount NUMBER := -1;
      v_nextValSql VARCHAR(150);
    BEGIN
      v_nextValSql := 'SELECT ' || GetPrcContractSequenceName(backOffice) || '.NextVal FROM DUAL';
  
      LOOP
        BEGIN
          EXECUTE IMMEDIATE v_nextValSql INTO v_result;
        EXCEPTION
          WHEN sequence_doesnt_exist THEN -- в случае отсутствия sequense создается
            IF (PrcContractCreateSequence(backOffice) != 0) THEN
              v_result := -1;
            END IF;
        END;
        
        IF v_result > 0 THEN
          SELECT COUNT(1)
            INTO v_BONumberCount
            FROM dPrcContract_dbt
           WHERE t_backOffice = backOffice
             AND t_number = v_result;
        END IF;
        
        EXIT WHEN v_result < 0 
               OR v_BONumberCount = 0;
      END LOOP;
  
      RETURN v_result;
    END;
    
END RSI_RSB_PERCENT;
/
