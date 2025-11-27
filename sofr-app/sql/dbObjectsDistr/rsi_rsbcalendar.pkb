CREATE OR REPLACE PACKAGE BODY RSI_RsbCalendar IS


-- Получить вид обслуживания дня календаря
  FUNCTION GetCalenDaySType( p_CalenDays IN dcalendar_dbt.t_CalenDays%TYPE, p_YearNumberDate IN INTEGER )
  RETURN VARCHAR2 IS
  BEGIN

    RETURN UTL_RAW.CAST_TO_VARCHAR2(UTL_RAW.SUBSTR(UTL_RAW.SUBSTR(P_CALENDAYS, P_YEARNUMBERDATE * 3 + 1, 3), 2, 1));

  END;

-- Проверить, является ли дата рабочим днем
  FUNCTION IsWorkDay( p_Date IN DATE, p_CalendarID IN INTEGER DEFAULT NULL )
  RETURN INTEGER IS

    v_IsWorkDay INTEGER;

    v_CalenDays dcalendar_dbt.t_CalenDays%TYPE;

    v_Date DATE;
    v_CalendarID     INTEGER;
    v_Year           INTEGER;
    v_YearNumberDate INTEGER;

  BEGIN

    v_CalendarID := p_CalendarID;

    IF v_CalendarID IS NULL THEN v_CalendarID := c_CalendarID; END IF;

    v_Date := trunc(p_Date);

    v_Year := to_number(to_char(v_Date, 'YYYY'));

    BEGIN

      SELECT t_CalenDays INTO v_CalenDays
      FROM dcalendar_dbt
      WHERE t_ID   = v_CalendarID
        AND t_Year = v_Year;

    EXCEPTION
      WHEN no_data_found THEN v_CalenDays := NULL;
    END;

    IF v_CalenDays IS NULL THEN

      -- записи в календаре о данном годе еще нет

      v_IsWorkDay := 1;

      IF rtrim(upper(to_char(v_Date, 'DAY', 'NLS_DATE_LANGUAGE = AMERICAN'))) = 'SATURDAY' OR rtrim(upper(to_char(v_Date, 'DAY', 'NLS_DATE_LANGUAGE = AMERICAN'))) = 'SUNDAY' THEN
        v_IsWorkDay := 0;
      END IF;

    ELSE

      v_IsWorkDay := 0;

      v_YearNumberDate := v_Date - trunc(v_Date, 'YEAR' );

      IF GETCALENDAYSTYPE( V_CALENDAYS, V_YEARNUMBERDATE ) = CHR(1) THEN

        v_IsWorkDay := 1;

      END IF;

    END IF;

    RETURN v_IsWorkDay;

  END;


-- Получить дату через определенное число рабочих дней
  FUNCTION GetDateAfterWorkDay( p_Date IN DATE, p_DayOffset IN INTEGER, p_CalendarID IN INTEGER DEFAULT NULL )
  RETURN DATE IS

    v_Date DATE;

    v_CalendarID INTEGER;

    v_IndOffset INTEGER;

    v_DayOffset INTEGER;
    v_RevSign   INTEGER;

  BEGIN

    v_CalendarID := p_CalendarID;

    IF v_CalendarID IS NULL THEN v_CalendarID := c_CalendarID; END IF;

    v_Date := trunc(p_Date);

    v_DayOffset := p_DayOffset;

    IF v_DayOffset = 0 THEN

      WHILE IsWorkDay(v_Date, v_CalendarID) = 0 LOOP

        v_Date := v_Date + 1;

      END LOOP;

    ELSE

      v_RevSign := 0;

      IF v_DayOffset < 0 THEN
        v_RevSign := 1;
        v_DayOffset := -v_DayOffset;
      END IF;

      v_IndOffset := 0;

      WHILE v_IndOffset < v_DayOffset LOOP

        IF v_RevSign = 0 THEN
          v_Date := v_Date + 1;
        ELSE
          v_Date := v_Date - 1;
        END IF;

        IF IsWorkDay(v_Date, v_CalendarID) <> 0 THEN
          v_IndOffset := v_IndOffset + 1;
        END IF;

      END LOOP;

    END IF;

    RETURN trunc(v_Date);

  END;

  -- Получить календа в заданном подразделении
  FUNCTION GetCalendar( p_Branch IN INTEGER DEFAULT NULL )
  RETURN INTEGER IS

    v_Branch INTEGER;

    v_CALENDAR      DDP_DEP_DBT.T_CALENDAR%TYPE;
    v_CALENDARPARAM DDP_DEP_DBT.T_CALENDARPARAM%TYPE;

    CURSOR CV_CURSOR IS
                        SELECT T_CALENDAR, T_CALENDARPARAM
                        FROM DDP_DEP_DBT
                        START WITH T_CODE = v_Branch
                        CONNECT BY PRIOR T_PARENTCODE = T_CODE;

  BEGIN

    v_Branch := p_Branch;

    IF v_Branch IS NULL THEN v_Branch := RSBSESSIONDATA.OperDprtNode; END IF;

    OPEN CV_CURSOR;

    <<loop_label>>

    LOOP

      FETCH cv_Cursor INTO v_CALENDAR, v_CALENDARPARAM;

      EXIT WHEN cv_Cursor%NOTFOUND;

      IF v_CALENDARPARAM = 0 OR v_CALENDARPARAM = 1 THEN

        EXIT loop_label;

      END IF;

    END LOOP;

    CLOSE CV_CURSOR;

    RETURN v_CALENDAR;

  END;

  -- Возвращает количество рабочих дней между двумя датами.
  FUNCTION getWorkDayCount(dateFrom IN DATE, dateTo IN DATE, p_CalendarID IN INTEGER DEFAULT NULL)
  RETURN NUMBER
  AS
      v_counter NUMBER := 0;
      v_CalendarID INTEGER;
      v_date DATE := datefrom;
  BEGIN
      IF dateFrom IS null OR dateTo IS null OR dateFrom > dateTo
      THEN
          RETURN NULL;
      END IF;

      v_CalendarID := p_CalendarID;
      IF v_CalendarID IS NULL THEN v_CalendarID := c_CalendarID; END IF;

      LOOP
          EXIT WHEN (v_date > dateTo);

          IF (isWorkDay(v_date, v_CalendarID) = 1)
          THEN
              v_counter := v_counter + 1;
          END IF;

          v_date := v_date + 1;
      END LOOP;

      RETURN v_counter;
  END;

  -- Функция определяет дату по номеру дня в заданном году
  -- bdate GetYearDate( int dayNum, int year )
  FUNCTION GetYearDate(dayNum IN NUMBER, yearnum IN NUMBER)
  RETURN DATE
  AS
      v_date DATE;
  BEGIN

      v_date := TO_DATE('01.01.'||TO_CHAR(yearnum),'DD.MM.YYYY');

      v_date := v_date + (dayNum - 1);

      RETURN v_date;
  END;

  -- Вычисление числа дней в году
  FUNCTION GetDaysInYear(p_year IN NUMBER)
  RETURN NUMBER
  AS
      v_num NUMBER;
  BEGIN
      -- Без учета DayLeapYear
      IF ((mod(p_year, 4) = 0) AND ((mod(p_year,100) <> 0) OR (mod(p_year,400) = 0 ))) THEN
        v_num := 366;
      ELSE
        v_num := 365;
      END IF;

      RETURN v_num;
  END;


  -- Функция определяет номер указанного дня в году
  FUNCTION GetYearDay(p_date IN DATE)
  RETURN NUMBER
  AS
      v_num NUMBER;
  BEGIN
      -- Без учета DayLeapYear
      v_num := (p_date - TO_DATE('01.01.'||TO_CHAR(p_date, 'YYYY'),'DD.MM.YYYY')) + 1;

      RETURN v_num;
  END;

  -- Функция возвращает число дней от Рождества Христова
  FUNCTION NDaysAD(p_date IN DATE)
  RETURN INTEGER
  AS
      v_year INTEGER;
      v_num  INTEGER;
  BEGIN
      -- Без учета DayLeapYear
/*
       v_year = TO_NUMBER(TO_CHAR(p_Date,'YYYY'));

       v_num := v_year*365 + (v_year-1)/4 - (v_year-1)/100 + (v_year-1)/400;

       v_num := v_num + GetYearDay( p_date );
*/
      v_num := p_date - TO_DATE ('01.01.0001', 'DD.MM.YYYY') + 1;

      RETURN v_num;
  END;


  FUNCTION GetNextDayForYearDay(p_CalKindID IN NUMBER, p_yearDay IN OUT NUMBER,  p_year IN OUT NUMBER, p_daysInYear IN OUT NUMBER, p_NextDays IN NUMBER, p_calendar IN OUT DCALENDAR_DBT%ROWTYPE)
  RETURN NUMBER
  AS
      v_stat NUMBER := 0;
  BEGIN

      IF( p_NextDays >= 0 ) THEN
        p_yearDay := p_yearDay + 1;
        IF( p_yearDay > p_daysInYear) THEN
          p_year := p_year + 1;
          p_daysInYear := GetDaysInYear( p_year );
          p_yearDay := 1;
        END IF;
      ELSE
        p_yearDay := p_yearDay -1;
        IF p_yearDay < 1 THEN
          p_year := p_year -1;
          p_daysInYear := GetDaysInYear( p_year );
          p_yearDay := p_daysInYear;
        END IF;
      END IF;

      v_stat := GetCalendarExt( p_CalKindID, p_year, p_calendar );

      RETURN v_stat;
  END;

  FUNCTION CheckCalenDayBalServKind(p_calendar IN OUT DCALENDAR_DBT%ROWTYPE, p_yearDay IN OUT NUMBER, p_Balance IN NUMBER, p_ServiceKind IN NUMBER)
  RETURN NUMBER
  AS
      v_stat NUMBER := 0;
  BEGIN
    IF(v_stat = 0 AND p_Balance <> -1) THEN
      IF NOT (    (SUBSTR(SUBSTR(RAWTOHEX(p_calendar.T_CALENDAYS), ((p_yearDay-1) * 3*2)+1, 6), 1, 2) = CALENDAR_BALANCE_NO    AND  p_Balance = 0)   -- CALENDAR_BALANCE_NO
               OR (SUBSTR(SUBSTR(RAWTOHEX(p_calendar.T_CALENDAYS), ((p_yearDay-1) * 3*2)+1, 6), 1, 2) = CALENDAR_BALANCE_YES   AND  p_Balance = 1)   -- CALENDAR_BALANCE_YES
      ) THEN
        v_stat := 1;
      END IF;
    END IF;

    IF(v_stat = 0 AND p_ServiceKind <> -1) THEN
      IF ( p_ServiceKind = 2 ) THEN -- CALENDAR_SERVICE_RETAIL
           IF ( CALENDAR_SERVICE_BANK <> SUBSTR(SUBSTR(RAWTOHEX(p_calendar.T_CALENDAYS), ((p_yearDay-1) * 3*2)+1, 6), 3, 2)
            AND CALENDAR_SERVICE_RETAIL <> SUBSTR(SUBSTR(RAWTOHEX(p_calendar.T_CALENDAYS), ((p_yearDay-1) * 3*2)+1, 6), 3, 2) ) THEN
          v_stat := 1;
        END IF;
      ELSE
        IF NOT (    (SUBSTR(SUBSTR(RAWTOHEX(p_calendar.T_CALENDAYS), ((p_yearDay-1) * 3*2)+1, 6), 3, 2) = CALENDAR_SERVICE_NO     AND  p_ServiceKind = 0)   -- CALENDAR_SERVICE_NO
                 OR (SUBSTR(SUBSTR(RAWTOHEX(p_calendar.T_CALENDAYS), ((p_yearDay-1) * 3*2)+1, 6), 3, 2) = CALENDAR_SERVICE_BANK   AND  p_ServiceKind = 1)   -- CALENDAR_SERVICE_BANK
                 OR (SUBSTR(SUBSTR(RAWTOHEX(p_calendar.T_CALENDAYS), ((p_yearDay-1) * 3*2)+1, 6), 3, 2) = CALENDAR_SERVICE_RETAIL AND  p_ServiceKind = 2)   -- CALENDAR_SERVICE_RETAIL
        ) THEN
          v_stat := 1;
        END IF;
      END IF;
    END IF;

    RETURN v_stat;
  END;


  -- Функция определяет дату по номеру дня в заданном году
  FUNCTION FindDate( p_Date                IN DATE  ,  -- заданная дата
                     p_Branch              IN NUMBER,  -- подразделение ТС
                     p_ServiceKind         IN NUMBER,  -- вид обслуживания в искомом дне
                     p_Balance             IN NUMBER,  -- признак наличие баланса в искомом дне
                     p_NextDays            IN NUMBER,  -- число дней от даты Date, на которые отстает искомая дата
                     p_NextDaysServiceKind IN NUMBER,  -- при переборе дат вперед/назад по параметру NextDays необходимо учитывать только те даты,
                                                       -- которые соответствуют заданному виду обслуживания
                     p_NextDaysBalance     IN NUMBER   -- при переборе дат вперед/назад по параметру NextDays необходимо учитывать только те даты,
                                                       -- которые соответствуют заданному признаку наличия баланса
  )
  RETURN DATE
  AS
      v_stat INTEGER := 0;
      v_CalKind NUMBER;
      v_Branch NUMBER;
      v_calendar DCALENDAR_DBT%ROWTYPE; -- ??
      v_StartDate DATE;
      v_i NUMBER;
      v_yearDay NUMBER;
      v_year NUMBER;
      v_daysInYear NUMBER;
      v_absNextDays NUMBER;
      v_ResultDate DATE;
  BEGIN
      v_ResultDate := to_Date('01.01.0001', 'DD.MM.YYYY');

      IF(p_Branch IS NULL OR p_Branch = 0) THEN
        v_Branch :=  RSBSESSIONDATA.OperDprtNode;
      ELSE
        v_Branch :=  p_Branch;
      END IF;

      v_CalKind := GetCalendar( v_Branch );

      IF(p_Date = to_Date('01.01.0001', 'DD.MM.YYYY')) THEN
        v_stat := 1;
      END IF;

      IF(v_stat = 0) THEN
        v_stat := GetCalendarExt( v_CalKind, TO_NUMBER(TO_CHAR(p_Date,'YYYY')), v_calendar );
      END IF;

      IF(v_stat = 0) THEN

        IF(p_NextDays <> 0 AND p_NextDaysBalance = -1 AND p_NextDaysServiceKind = -1 ) THEN
          v_StartDate := p_Date + p_NextDays;
          v_stat := GetCalendarExt( v_CalKind, TO_NUMBER(TO_CHAR(v_StartDate,'YYYY')), v_calendar );
        ELSE IF(p_NextDays <> 0) THEN

           v_i := 0;
           v_yearDay := GetYearDay( p_Date );
           v_year :=  TO_NUMBER(TO_CHAR(p_Date,'YYYY')) ;
           v_daysInYear := GetDaysInYear( TO_NUMBER(TO_CHAR(p_Date,'YYYY')) );

           IF p_NextDays < 0 THEN
             v_absNextDays := -p_NextDays;
           ELSE
             v_absNextDays := p_NextDays;
           END IF;

           WHILE (v_stat = 0 AND v_i <= v_absNextDays) LOOP
             -- у меня подозрение, что для этого случая if(NextDays != 0) перескакивать на день вперед или назад нужно сразу
             v_stat := GetNextDayForYearDay(v_CalKind, v_yearDay, v_year, v_daysInYear, p_NextDays, v_calendar);

             IF(CheckCalenDayBalServKind(v_calendar, v_yearDay, p_NextDaysBalance, p_NextDaysServiceKind) = 0 ) THEN
               v_i := v_i + 1;
             END IF;

           END LOOP;

           IF (v_stat = 0) THEN
              v_StartDate :=  GetYearDate( v_yearDay, v_year );
           END IF;
        ELSE
           v_StartDate :=  p_Date;
        END IF;
        END IF;
       -- IF(v_stat = 0) THEN

       -- END IF;

      END IF;


      IF (v_stat = 0) THEN
        v_i := 0;
        v_yearDay := GetYearDay( v_StartDate );
        v_year :=  TO_NUMBER(TO_CHAR(v_StartDate,'YYYY')) ;
        v_daysInYear := GetDaysInYear( v_year );

         WHILE (v_stat = 0 AND v_i < 1) LOOP
           IF(CheckCalenDayBalServKind(v_calendar, v_yearDay, p_Balance, p_ServiceKind) = 0 ) THEN
             v_i := v_i + 1;
           END IF;

           IF(v_i < 1) THEN
             v_stat := GetNextDayForYearDay(v_CalKind, v_yearDay, v_year, v_daysInYear, p_NextDays, v_calendar);
           END IF;
         END LOOP;

        IF (v_stat = 0) THEN
         v_ResultDate :=  GetYearDate( v_yearDay, v_year );
        END IF;
      END IF;

      RETURN v_ResultDate;
  END;



  -- Функция считывания календаря
  FUNCTION GetCalendarExt(p_CalKindID IN NUMBER, p_year IN NUMBER, p_calendar IN OUT DCALENDAR_DBT%ROWTYPE)
  RETURN NUMBER
  AS
      v_stat NUMBER;
  BEGIN
      v_stat := 0;

      BEGIN
        SELECT * INTO p_calendar FROM dcalendar_dbt WHERE t_ID = p_CalKindID AND t_Year = p_year;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN

        v_stat := GenerateCalendarExt(p_CalKindID, TO_DATE('01.01.'||TO_CHAR(p_year),'DD.MM.YYYY'), p_calendar);

      END;

      RETURN v_stat;
  END;

  FUNCTION GetWeekDay(p_date IN DATE)
  RETURN NUMBER
  IS
    s VARCHAR2(32);
    n NUMBER;
  BEGIN

    s:=TO_CHAR(p_date, 'fmDAY', 'NLS_DATE_LANGUAGE=AMERICAN');

    CASE s
      WHEN 'MONDAY'
        THEN
          n:=1;
      WHEN 'TUESDAY'
        THEN
          n:=2;
      WHEN 'WEDNESDAY'
        THEN
          n:=3;
      WHEN 'THURSDAY'
        THEN
          n:=4;
      WHEN 'FRIDAY'
        THEN
          n:=5;
      WHEN 'SATURDAY'
        THEN
          n:=6;
      WHEN 'SUNDAY'
        THEN
          n:=7;
    END CASE;

    RETURN n;

  END GetWeekDay;


  -- Установить вид убслуживания и баланс если флаг взведен
  PROCEDURE OnSetServiceAndBalance(p_ServiceKind IN OUT VARCHAR2, p_Balance IN OUT VARCHAR2, p_KindBalance IN NUMBER )
  IS
  BEGIN

    CASE p_KindBalance
      WHEN ALLDAY_BALANCE
        THEN
          p_ServiceKind :=  CALENDAR_SERVICE_RETAIL;
          p_Balance     :=  CALENDAR_BALANCE_YES;
      WHEN BANKDAY_BALANCE
        THEN
          p_ServiceKind :=  CALENDAR_SERVICE_RETAIL;
          p_Balance     :=  CALENDAR_BALANCE_NO;
      WHEN RETAILDAY_BALANCE
        THEN
          p_ServiceKind :=  CALENDAR_SERVICE_RETAIL;
          p_Balance     :=  CALENDAR_BALANCE_YES;
--      WHEN UNDEF_BALANCE
--        THEN
    END CASE;

  END OnSetServiceAndBalance;


  -- Установить вид убслуживания и баланс если флаг не взведен
  PROCEDURE OffSetServiceAndBalance(p_ServiceKind IN OUT VARCHAR2, p_Balance IN OUT VARCHAR2, p_KindBalance IN NUMBER )
  IS
  BEGIN

    CASE p_KindBalance
      WHEN ALLDAY_BALANCE
        THEN
          p_ServiceKind :=  CALENDAR_SERVICE_NO;
          p_Balance     :=  CALENDAR_BALANCE_YES;
      WHEN BANKDAY_BALANCE
        THEN
          p_ServiceKind :=  CALENDAR_SERVICE_NO;
          p_Balance     :=  CALENDAR_BALANCE_NO;
      WHEN RETAILDAY_BALANCE
        THEN
          p_ServiceKind :=  CALENDAR_SERVICE_NO;
          p_Balance     :=  CALENDAR_BALANCE_NO;
--      WHEN UNDEF_BALANCE
--        THEN
    END CASE;

  END OffSetServiceAndBalance;


  FUNCTION ConvertToHex( p_string IN VARCHAR2 )
  RETURN RAW
  AS
    v_retval DCALENDAR_DBT.T_CALENDAYS%TYPE;
    v_i NUMBER;
    v_len NUMBER;
    v_s1 VARCHAR2(4);
    v_s2 VARCHAR2(2);
    v_s3 VARCHAR2(2);
    v_calenDay VARCHAR2(2196);
  BEGIN
     v_calenDay := '';
     v_len := length(p_string);
     v_i := 1;
     WHILE v_i <= v_len LOOP

       v_s1 := SUBSTR(p_string, v_i  , 4 );

       IF v_s1 = '0000' THEN
         v_s3 := '00';
       END IF;

       IF v_s1 = '0001' THEN
         v_s3 := '01';
       END IF;

       IF v_s1 = '0100' THEN
         v_s3 := '10';
       END IF;

       IF v_s1 = '0101' THEN
         v_s3 := '11';
       END IF;

       IF v_s1 = '1000' THEN
         v_s3 := '20';
       END IF;

       IF v_s1 = '0010' THEN
         v_s3 := '02';
       END IF;

       IF v_s1 = '1010' THEN
         v_s3 := '22';
       END IF;

       v_calenDay := v_calenDay || v_s3;

       v_i := v_i + 4;
     END LOOP;

     v_retval := HEXTORAW (v_calenDay);

     RETURN v_retval;
  END;



  -- Проверка на праздник
  FUNCTION IsDayOff(p_Date IN DATE, p_regValue IN VARCHAR2)
  RETURN NUMBER
  AS
    v_retval NUMBER;
  BEGIN
     IF NVL(INSTR(p_regValue, TO_CHAR(p_Date, 'DD.MM') ), 0) > 0 THEN
       v_retval := 1;
     ELSE
       v_retval := 0;
     END IF;

     RETURN v_retval;
  END;


 -- Функция генерации календаря на год. с сохранением календаря
  FUNCTION GenerateCalendarExt(p_CalKindID IN NUMBER, p_Date IN DATE, p_calendar IN OUT DCALENDAR_DBT%ROWTYPE)
  RETURN NUMBER
  AS
      v_stat              NUMBER;
      v_calendarKindMain  DCALKIND_DBT%ROWTYPE ;
      v_calendarMain      DCALENDAR_DBT%ROWTYPE;
      v_calendarKind      DCALKIND_DBT%ROWTYPE ;
      v_count             NUMBER;
      v_firstDate         DATE;
      v_calenDay          VARCHAR2(5000);
--      v_calenDaysRAW      DCALENDAR_DBT.T_CALENDAYS%TYPE;
      v_calenDaysRAW      RAW(5000); -- DELETE !!!
      v_weekDay           NUMBER;
      v_regValue          VARCHAR2(2048); -- ??
      v_num               NUMBER;
      v_i                 NUMBER;
      v_daysInYear        NUMBER;
      v_IsDayOff          NUMBER;

      v_s1_Balance        VARCHAR2(2);
      v_s2_ServiceKind    VARCHAR2(2);
      v_s3_Change         VARCHAR2(2);

  BEGIN
      v_stat := 0;

      SELECT * INTO v_calendarKindMain FROM DCALKIND_DBT WHERE t_ID = c_CalendarID;

      IF p_CalKindID < 0 THEN

        SELECT * INTO v_calendarMain FROM DCALENDAR_DBT WHERE t_ID = c_CalendarID AND t_Year = TO_NUMBER(TO_CHAR(p_Date,'YYYY'));

      END IF;


      BEGIN
        SELECT * INTO v_calendarKind FROM DCALKIND_DBT WHERE t_ID = p_CalKindID;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN

        INSERT INTO DCALKIND_DBT(T_ID, T_NAME, T_BALANCE, T_RETAILINSATURDAY, T_RETAILINSUNDAY, T_RETAILINHOLYDAY, T_PARENTID)
             VALUES
             (
               p_CalKindID,
               'Пользовательский календарь '|| TO_CHAR(p_CalKindID),
               v_calendarKindMain.t_Balance,
               v_calendarKindMain.t_RetailInSaturday,
               v_calendarKindMain.t_RetailInSunday,
               v_calendarKindMain.t_RetailInHolyday,
               0
             ) ; -- ?? returning ??
        SELECT * INTO v_calendarKind FROM DCALKIND_DBT WHERE t_ID = p_CalKindID;
      END;

      SELECT COUNT(*) INTO v_count FROM DCALENDAR_DBT WHERE T_ID = p_CalKindID AND T_YEAR = TO_NUMBER(TO_CHAR(p_Date,'YYYY'));

      IF v_count > 0 THEN
        v_firstDate := p_Date;
      ELSE
        v_firstDate := TO_DATE('01.01.'||TO_CHAR(p_Date,'YYYY'),'DD.MM.YYYY');
      END IF;

      v_regValue := rsb_common.GetRegStrValue( 'COMMON\КАЛЕНДАРИ\ПРАЗДНИЧНЫЕ ДНИ', 0 );

      v_num := GetYearDay(v_firstDate);
      v_calenDay := '';

      IF (v_num <> 1) THEN
        v_calenDay := UTL_RAW.CAST_TO_VARCHAR2(UTL_RAW.SUBSTR(p_calendar.T_CALENDAYS, 1, v_num * 3 ));
      END IF;

      v_weekDay := GetWeekDay(v_firstDate);
      v_daysInYear := GetDaysInYear( TO_NUMBER(TO_CHAR(p_Date,'YYYY')) );
      v_i := v_num - 1;
      WHILE v_i < v_daysInYear LOOP

        v_s1_Balance     := '00';
        v_s2_ServiceKind := '00';
        v_s3_Change      := '00';

        v_IsDayOff := IsDayOff(GetYearDate( v_i + 1, TO_NUMBER(TO_CHAR(p_Date,'YYYY')) ), v_regValue);

        IF (v_weekDay <=5  AND v_IsDayOff = 0 ) THEN -- если день рабочий
          v_s2_ServiceKind := CALENDAR_SERVICE_BANK;
          v_s1_Balance     := CALENDAR_BALANCE_YES;
        END IF;

        IF( v_weekDay = 6 AND v_IsDayOff = 0 ) THEN
          IF ( v_calendarKind.t_RetailInSaturday = 'X' ) THEN
            OnSetServiceAndBalance(v_s2_ServiceKind, v_s1_Balance, v_calendarKind.t_Balance );
          ELSE
            OffSetServiceAndBalance(v_s2_ServiceKind, v_s1_Balance, v_calendarKind.t_Balance );
          END IF;
        END IF;

        IF( v_weekDay = 7 AND v_IsDayOff = 0 ) THEN
          IF ( v_calendarKind.t_RetailInSunday = 'X' ) THEN
            OnSetServiceAndBalance(v_s2_ServiceKind, v_s1_Balance, v_calendarKind.t_Balance );
          ELSE
            OffSetServiceAndBalance(v_s2_ServiceKind, v_s1_Balance, v_calendarKind.t_Balance );
          END IF;
        END IF;


        IF( v_IsDayOff = 1 ) THEN
          IF ( v_calendarKind.t_RetailInHolyday = 'X' ) THEN
            OnSetServiceAndBalance(v_s2_ServiceKind, v_s1_Balance, v_calendarKind.t_Balance );
          ELSE
            OffSetServiceAndBalance(v_s2_ServiceKind, v_s1_Balance, v_calendarKind.t_Balance );
          END IF;
        END IF;


        IF p_CalKindID < 0 THEN
           IF ( v_s2_ServiceKind = SUBSTR(SUBSTR(RAWTOHEX(v_calendarMain.T_CALENDAYS), v_i * 3*2 + 1, 6), 3, 2)
            AND v_s1_Balance     = SUBSTR(SUBSTR(RAWTOHEX(v_calendarMain.T_CALENDAYS), v_i * 3*2 + 1, 6), 1, 2) ) THEN
              v_s3_Change := CHANGE_UNSET;
           ELSE
              v_s3_Change := CHANGE_SET;
           END IF;
        END IF;

        v_calenDay := v_calenDay || v_s1_Balance || v_s2_ServiceKind || v_s3_Change;

        IF (v_weekDay = 7) THEN
          v_weekDay := 1;
        ELSE
          v_weekDay := v_weekDay + 1;
        END IF;
        v_i := v_i + 1;
      END LOOP;

      v_calenDaysRAW := HEXTORAW (v_calenDay);
--      v_calenDaysRAW := ConvertToHex( v_calenDay );

      INSERT INTO DCALENDAR_DBT (T_ID, T_YEAR, T_DAYSINYEAR, T_CALENDAYS)
      VALUES(p_CalKindID, TO_NUMBER(TO_CHAR(p_Date,'YYYY')), v_daysInYear, v_calenDaysRAW);

      SELECT * INTO p_calendar FROM dcalendar_dbt WHERE t_ID = p_CalKindID AND t_Year = TO_NUMBER(TO_CHAR(p_Date,'YYYY'));

      RETURN v_stat;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
        v_stat := 1 ;
        return v_stat;
  END;



END;
/
