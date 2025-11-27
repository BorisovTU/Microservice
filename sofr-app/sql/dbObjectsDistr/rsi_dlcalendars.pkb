CREATE OR REPLACE PACKAGE BODY RSI_DlCalendars IS

  /**Тип - массив чисел*/
  type NUM_ARRAY is table of integer;

  FUNCTION CheckFI_Kind( FIID IN NUMBER ) RETURN  NUMBER DETERMINISTIC
  IS
    v_CheckFI_Kind NUMBER := 0;
  BEGIN
     SELECT 1 into v_CheckFI_Kind
       FROM dfininstr_dbt
      WHERE t_FIID = FIID
        AND t_FI_KIND in(RSI_RSB_FIInstr.FIKIND_CURRENCY,RSI_RSB_FIInstr.FIKIND_METAL);

     RETURN v_CheckFI_Kind;

  EXCEPTION
     WHEN NO_DATA_FOUND THEN RETURN 0;

  END CheckFI_Kind;

  procedure GetCalendarKindForPFI
   is
  begin
    CalendarKindForPFI := Rsb_Common.GetRegIntValue('COMMON\КАЛЕНДАРИ\КАЛЕНДАРЬ ДЛЯ ОПРЕДЕЛЕНИЯ ПФИ', 0);
  end GetCalendarKindForPFI;

  -- Получить календарь официальных рабочих дней в РФ
  FUNCTION GetOfficialCalendar
  RETURN NUMBER
  AS
    v_OfficialCalendar NUMBER := RSI_RsbCalendar.GetCalendar();
  BEGIN
    if( v_OfficialCalendar is null )then
      v_OfficialCalendar := RSI_RsbCalendar.c_CalendarID;
    end if;

    if CalendarKindForPFI is null then -- т.е., если первый вход и ещё ничего не закачивали
      GetCalendarKindForPFI;
    end if;

    if( CalendarKindForPFI != 0 )then
      begin
        select CL.T_CALENDARID into v_OfficialCalendar
          from dcalcor_dbt cl
         where CL.T_OBJECTTYPE = cnst.OBJTYPE_COUNTRY
           and CL.T_OBJECT = 'RUS';
      exception
        when no_data_found then null;
      end;
    end if;

    RETURN v_OfficialCalendar;
  END;

  -- Функция получения ID календаря
  FUNCTION GetLinkCalKind( pObjectType in dcalcor_dbt.t_ObjectType % TYPE,
                           pObject in dcalcor_dbt.t_Object % TYPE
                         ) RETURN NUMBER
  AS
     v_CalendarID NUMBER := -1;
  BEGIN

     BEGIN
       SELECT dcalcor_dbt.t_CalendarID INTO v_CalendarID
         FROM dcalcor_dbt
        WHERE t_ObjectType = pObjectType
          AND t_Object     = pObject;
     EXCEPTION
        WHEN OTHERS THEN v_CalendarID := -1;
     END;

     return v_CalendarID;

  END GetLinkCalKind;

  -- Получить календарь, привязанный к стране переданного субъекта
  FUNCTION GetLinkCalByPartyCountry(pPartyID in NUMBER) RETURN NUMBER
  AS
     v_NRCountry dparty_dbt.t_NRCountry % TYPE := chr(1);
  BEGIN

     BEGIN
       SELECT case when pt.t_NRCountry = chr(1) then 'RUS' else pt.t_NRCountry end INTO v_NRCountry --отключено по РФ
         FROM dparty_dbt pt
        WHERE pt.t_PartyID = pPartyID;
     EXCEPTION
        WHEN OTHERS THEN v_NRCountry := chr(1);
     END;

     return GetLinkCalKind(cnst.OBJTYPE_COUNTRY,v_NRCountry);

  END GetLinkCalByPartyCountry;

  -- Получить календарь, привязанный к FIID
  FUNCTION GetLinkCalByCurrency( pFIID in NUMBER) RETURN NUMBER
  AS
  BEGIN
     return GetLinkCalKind(cnst.OBJTYPE_CURRENCY,LPAD(pFIID, 10, '0'));
  END GetLinkCalByCurrency;
  
  PROCEDURE CheckCalendarExist( p_Date IN DATE, p_CalendarID IN INTEGER )
  IS
    v_calendarExist INTEGER := 0;
    v_Year      INTEGER := to_number(to_char(trunc(p_Date), 'YYYY'));
  BEGIN
   SELECT 1 INTO v_calendarExist
      FROM dcalendar_dbt
      WHERE t_ID   = p_CalendarID
        AND t_Year = v_Year;
  END;
  
    -- Проверить, является ли дата днем Баланса в указанном календаре
  FUNCTION IsBalanceDay( p_Date IN DATE, p_CalendarID IN INTEGER )
  RETURN INTEGER IS
    v_IsBalanceDay INTEGER := 0;
    v_Year      INTEGER := to_number(to_char(trunc(p_Date), 'YYYY'));
  BEGIN
    BEGIN
      SELECT 1 INTO v_IsBalanceDay
      FROM dcalendar_dbt
      WHERE t_ID   = p_CalendarID
        AND t_Year = v_Year
        AND SUBSTR(SUBSTR(RAWTOHEX(T_CALENDAYS), ((((p_Date - TO_DATE('01.01.'||TO_CHAR(p_Date, 'YYYY'),'DD.MM.YYYY')) + 1)-1) * 3*2)+1, 6), 1, 2) = RSI_RsbCalendar.CALENDAR_BALANCE_YES;
    EXCEPTION
      WHEN no_data_found THEN v_IsBalanceDay := 0;
    END;
    RETURN v_IsBalanceDay;
  END;

  FUNCTION IsDay( p_Date IN DATE, p_CalendarID IN INTEGER, p_CheckBalance IN NUMBER, p_CheckBank IN NUMBER, p_CheckRetail IN NUMBER)
  RETURN INTEGER IS
    v_IsDay     INTEGER := 0;
    v_Year      INTEGER := to_number(to_char(trunc(p_Date), 'YYYY'));
  BEGIN
    if (p_CheckBalance = 1) then
    BEGIN
      SELECT 1 INTO v_IsDay
      FROM dcalendar_dbt
      WHERE t_ID   = p_CalendarID
        AND t_Year = v_Year
        AND SUBSTR(SUBSTR(RAWTOHEX(T_CALENDAYS), ((((p_Date - TO_DATE('01.01.'||TO_CHAR(p_Date, 'YYYY'),'DD.MM.YYYY')) + 1)-1) * 3*2)+1, 6), 1, 2) = RSI_RsbCalendar.CALENDAR_BALANCE_YES;
    EXCEPTION
      WHEN no_data_found THEN NULL;
    END;
    end if;
    if (p_CheckBank = 1) then
    BEGIN
      SELECT 1 INTO v_IsDay
      FROM dcalendar_dbt
      WHERE t_ID   = p_CalendarID
        AND t_Year = v_Year
        AND SUBSTR(SUBSTR(RAWTOHEX(T_CALENDAYS), ((((p_Date - TO_DATE('01.01.'||TO_CHAR(p_Date, 'YYYY'),'DD.MM.YYYY')) + 1)-1) * 3*2)+1, 6), 3, 2) = RSI_RsbCalendar.CALENDAR_SERVICE_BANK;
    EXCEPTION
      WHEN no_data_found THEN NULL;
    END;
    end if;
    if (p_CheckRetail = 1) then
    BEGIN
      SELECT 1 INTO v_IsDay
      FROM dcalendar_dbt
      WHERE t_ID   = p_CalendarID
        AND t_Year = v_Year
        AND SUBSTR(SUBSTR(RAWTOHEX(T_CALENDAYS), ((((p_Date - TO_DATE('01.01.'||TO_CHAR(p_Date, 'YYYY'),'DD.MM.YYYY')) + 1)-1) * 3*2)+1, 6), 3, 2) = '02';
    EXCEPTION
      WHEN no_data_found THEN NULL;
    END;
    end if;
    RETURN v_IsDay;
  END;

  -- Проверить, является ли дата днем Банковского обслуживания в указанном календаре
  FUNCTION IsBankDay( p_Date IN DATE, p_CalendarID IN INTEGER )
  RETURN INTEGER IS
    v_IsBankDay INTEGER := 0;
    v_Year      INTEGER := to_number(to_char(trunc(p_Date), 'YYYY'));
  BEGIN
    BEGIN
      SELECT 1 INTO v_IsBankDay
      FROM dcalendar_dbt
      WHERE t_ID   = p_CalendarID
        AND t_Year = v_Year
        AND SUBSTR(SUBSTR(RAWTOHEX(T_CALENDAYS), ((((p_Date - TO_DATE('01.01.'||TO_CHAR(p_Date, 'YYYY'),'DD.MM.YYYY')) + 1)-1) * 3*2)+1, 6), 3, 2) = RSI_RsbCalendar.CALENDAR_SERVICE_BANK;
    EXCEPTION
      WHEN no_data_found THEN v_IsBankDay := 0;
    END;
    RETURN v_IsBankDay;
  END;

  FUNCTION GetTypeDay( p_Date IN DATE, p_CalendarID IN INTEGER )
  RETURN VARCHAR2 IS
    v_TypeDay VARCHAR2(2) := '00';
    v_Year      INTEGER := to_number(to_char(trunc(p_Date), 'YYYY'));
  BEGIN
    BEGIN
      SELECT SUBSTR(SUBSTR(RAWTOHEX(T_CALENDAYS), ((((p_Date - TO_DATE('01.01.'||TO_CHAR(p_Date, 'YYYY'),'DD.MM.YYYY')) + 1)-1) * 3*2)+1, 6), 3, 2)  INTO v_TypeDay
      FROM dcalendar_dbt
      WHERE t_ID   = p_CalendarID
        AND t_Year = v_Year
        ;
    EXCEPTION
      WHEN no_data_found THEN v_TypeDay := '00';
    END;
    RETURN v_TypeDay;
  END;

  -- Возвращает количество рабочих дней между двумя датами по массиву календарей
  FUNCTION CalcNumWorkDaysForPeriod(dateFrom IN DATE, dateTo IN DATE, p_arrCls IN CAL_ARRAY, p_NotMore3 IN INTEGER DEFAULT 0)
  RETURN NUMBER
  AS
    v_counter NUMBER := 0;
    v_count_cls NUMBER := 0;
    v_date DATE := datefrom;
  BEGIN
    IF p_arrCls.COUNT > 0 THEN
      LOOP
        EXIT WHEN (v_date > dateTo or (p_NotMore3 = 1 and v_counter = 3));
  
        v_count_cls := 0;

        FOR i IN p_arrCls.FIRST .. p_arrCls.LAST
        LOOP
           EXIT WHEN (IsBankDay(v_date, p_arrCls(i)) != 1 or dateFrom = v_date);
           v_count_cls := v_count_cls + 1;
        END LOOP;
  
        if(v_count_cls = p_arrCls.COUNT or dateTo = v_date)then
           v_counter := v_counter + 1;
        end if;
  
        v_date := v_date + 1;
      END LOOP;

    END IF;

    RETURN v_counter;
  END;

  -- Возвращает количество рабочих дней между двумя датами по массиву календарей (хотя бы один день должен быть рабочим)
  FUNCTION CalcNumWorkDaysForPeriodAtLeastOne(dateFrom IN DATE, dateTo IN DATE, p_arrCls IN CAL_ARRAY, p_NotMore3 IN INTEGER DEFAULT 0)
  RETURN NUMBER
  AS
    v_counter NUMBER := 0;
    v_count_cls NUMBER := 0;
    v_date DATE := datefrom;
  BEGIN
    IF p_arrCls.COUNT > 0 THEN
      LOOP
        EXIT WHEN (v_date > dateTo or (p_NotMore3 = 1 and v_counter = 3));

        v_count_cls := 0;

        FOR i IN p_arrCls.FIRST .. p_arrCls.LAST
        LOOP
           EXIT WHEN (IsBankDay(v_date, p_arrCls(i)) != 1 or dateFrom = v_date);
           v_count_cls := v_count_cls + 1;
        END LOOP;
  
        if(v_count_cls > 0) then
           v_counter := v_counter + 1;
        end if;
  
        v_date := v_date + 1;
      END LOOP;

    END IF;

    RETURN v_counter;
  END;
  
  -- Получить балансовую дату через определенное число рабочих дней по массиву календарей
  FUNCTION GetBalanceDateAfterWorkDayByCls( p_Date IN DATE, p_DayOffset IN INTEGER, p_arrCls IN NUM_ARRAY, p_isForward IN INTEGER DEFAULT 1 )
  RETURN DATE IS

    v_Date DATE;

    v_IndOffset INTEGER;

    v_DayOffset INTEGER;
    v_RevSign   INTEGER;

    v_count_cls NUMBER := 0;

  BEGIN

    v_Date := trunc(p_Date);

    v_DayOffset := p_DayOffset;

    IF v_DayOffset = 0 THEN

      LOOP
          v_count_cls := 0;
          FOR i IN p_arrCls.FIRST .. p_arrCls.LAST
          LOOP
             EXIT WHEN (IsBalanceDay(v_date, p_arrCls(i)) != 1);
             v_count_cls := v_count_cls + 1;
          END LOOP;
          EXIT WHEN v_count_cls = p_arrCls.COUNT;
          if (p_isForward = 1) then
            v_Date := v_Date + 1;
          else 
            v_Date := v_Date - 1;
          end if;
      END LOOP;

    ELSE

      v_RevSign := 0;

      IF v_DayOffset < 0 OR p_isForward = 0 THEN
        v_RevSign := 1;
      END IF;
      if v_DayOffset < 0 THEN
        v_DayOffset := -v_DayOffset;
      END IF;

      v_IndOffset := 0;

      WHILE v_IndOffset < v_DayOffset LOOP

        IF v_RevSign = 0 THEN
          v_Date := v_Date + 1;
        ELSE
          v_Date := v_Date - 1;
        END IF;

        v_count_cls := 0;
        FOR i IN p_arrCls.FIRST .. p_arrCls.LAST
        LOOP
           EXIT WHEN (IsBalanceDay(v_date, p_arrCls(i)) != 1);
           v_count_cls := v_count_cls + 1;
        END LOOP;

        if(v_count_cls = p_arrCls.COUNT)then
          v_IndOffset := v_IndOffset + 1;
        end if;

      END LOOP;

    END IF;

    RETURN trunc(v_Date);

  END;

  -- Получить дату через определенное число рабочих дней по массиву календарей
  FUNCTION GetDateAfterWorkDayByCls( p_Date IN DATE, p_DayOffset IN INTEGER, p_arrCls IN NUM_ARRAY, p_isForward IN INTEGER DEFAULT 1 )
  RETURN DATE IS

    v_Date DATE;

    v_IndOffset INTEGER;

    v_DayOffset INTEGER;
    v_RevSign   INTEGER;

    v_count_cls NUMBER := 0;

  BEGIN

    v_Date := trunc(p_Date);

    v_DayOffset := p_DayOffset;

    IF v_DayOffset = 0 THEN

      LOOP
          v_count_cls := 0;
          FOR i IN p_arrCls.FIRST .. p_arrCls.LAST
          LOOP
             EXIT WHEN (IsBankDay(v_date, p_arrCls(i)) != 1);
             v_count_cls := v_count_cls + 1;
          END LOOP;
          EXIT WHEN v_count_cls = p_arrCls.COUNT;
          if (p_isForward = 1) then
            v_Date := v_Date + 1;
          else 
            v_Date := v_Date - 1;
          end if;
      END LOOP;

    ELSE

      v_RevSign := 0;

      IF v_DayOffset < 0 OR p_isForward = 0 THEN
        v_RevSign := 1;
      END IF;
      if v_DayOffset < 0 THEN
        v_DayOffset := -v_DayOffset;
      END IF;

      v_IndOffset := 0;

      WHILE v_IndOffset < v_DayOffset LOOP

        IF v_RevSign = 0 THEN
          v_Date := v_Date + 1;
        ELSE
          v_Date := v_Date - 1;
        END IF;

        v_count_cls := 0;
        FOR i IN p_arrCls.FIRST .. p_arrCls.LAST
        LOOP
           EXIT WHEN (IsBankDay(v_date, p_arrCls(i)) != 1);
           v_count_cls := v_count_cls + 1;
        END LOOP;

        if(v_count_cls = p_arrCls.COUNT)then
          v_IndOffset := v_IndOffset + 1;
        end if;

      END LOOP;

    END IF;

    RETURN trunc(v_Date);

  END;
  
  -- Получить дату через определенное число рабочих дней по календарю
  FUNCTION GetBankDateAfterWorkDayByCalendar( p_Date IN DATE, p_DayOffset IN INTEGER, p_CalendarID IN INTEGER, p_isForward IN INTEGER DEFAULT 1, p_FIID IN INTEGER DEFAULT -1, p_Contractor IN INTEGER DEFAULT -1 ) 
  RETURN DATE
  AS
    v_arrCls NUM_ARRAY := NUM_ARRAY();
    v_CalendarID NUMBER := -1;
  BEGIN
    v_arrCls.EXTEND;
    v_arrCls(v_arrCls.COUNT) := p_CalendarID;
    CheckCalendarExist( p_Date, p_CalendarID);
    if (p_FIID > 0 and p_FIID != RSI_RSB_FIInstr.NATCUR) then --Если не нац. валюта, то берём ещё календарь той валюты
      v_CalendarID := GetLinkCalByCurrency(p_FIID); 
      if( v_CalendarID != -1)then
        BEGIN
          CheckCalendarExist( p_Date, v_CalendarID);
          v_arrCls.EXTEND;
          v_arrCls(v_arrCls.COUNT) := v_CalendarID;
        exception when others then NULL;
        END;
      end if;
      if( p_Contractor > 0 )then
         v_CalendarID := GetLinkCalByPartyCountry(p_Contractor);
         if( v_CalendarID != -1)then
            BEGIN
              CheckCalendarExist( p_Date, v_CalendarID);
              v_arrCls.EXTEND;
              v_arrCls(v_arrCls.COUNT) := v_CalendarID;
            exception when others then NULL;
            END;
         end if;
      end if;
    end if;
    RETURN GetDateAfterWorkDayByCls( p_Date, p_DayOffset, v_arrCls, p_isForward );
    exception
      when no_data_found then return p_Date;
  END;
  
  -- Получить балансовую дату через определенное число рабочих дней по календарю
  FUNCTION GetBalanceDateAfterWorkDayByCalendar( p_Date IN DATE, p_DayOffset IN INTEGER, p_CalendarID IN INTEGER, p_isForward IN INTEGER DEFAULT 1, p_FIID IN INTEGER DEFAULT -1, p_Contractor IN INTEGER DEFAULT -1 ) 
  RETURN DATE
  AS
    v_arrCls NUM_ARRAY := NUM_ARRAY();
    v_CalendarID NUMBER := -1;
  BEGIN
    v_arrCls.EXTEND;
    v_arrCls(v_arrCls.COUNT) := p_CalendarID;
    CheckCalendarExist( p_Date, p_CalendarID);
    if (p_FIID > 0 and p_FIID != RSI_RSB_FIInstr.NATCUR) then --Если не нац. валюта, то берём ещё календарь той валюты
      v_CalendarID := GetLinkCalByCurrency(p_FIID); 
      if( v_CalendarID != -1)then
        BEGIN
          CheckCalendarExist( p_Date, v_CalendarID);
          v_arrCls.EXTEND;
          v_arrCls(v_arrCls.COUNT) := v_CalendarID;
        exception when others then NULL;
        END;
      end if;
      if( p_Contractor > 0 )then
         v_CalendarID := GetLinkCalByPartyCountry(p_Contractor);
         if( v_CalendarID != -1)then
            BEGIN
              CheckCalendarExist( p_Date, v_CalendarID);
              v_arrCls.EXTEND;
              v_arrCls(v_arrCls.COUNT) := v_CalendarID;
            exception when others then NULL;
            END;
         end if;
      end if;
    end if;
    RETURN GetBalanceDateAfterWorkDayByCls( p_Date, p_DayOffset, v_arrCls, p_isForward );
    exception
      when no_data_found then return p_Date;
  END;

  -- Возвращает количество рабочих дней между двумя датами по массиву календарей, переданному в виде строки
  FUNCTION GetNumWorkDaysForPeriodByCls(dateFrom IN DATE, dateTo IN DATE, p_ClsStr IN VARCHAR2)
  RETURN NUMBER
  AS
    v_arrCls CAL_ARRAY := CAL_ARRAY();
    v_ClsStr  VARCHAR2(64) := p_ClsStr;
    v_counter NUMBER := 0;
    v_Pos NUMBER := 0;
  BEGIN
    IF dateFrom IS null OR dateTo IS null OR dateFrom > dateTo OR p_ClsStr IS NULL OR p_ClsStr = '' or p_ClsStr = chr(1) OR dateFrom = dateTo
    THEN
      RETURN 0;
    END IF;

    WHILE v_ClsStr IS NOT NULL
    LOOP
      v_arrCls.EXTEND;
      v_Pos := INSTR(v_ClsStr, ',', 1);
      if(v_Pos = 0)then
        v_arrCls(v_arrCls.COUNT) := TO_NUMBER(v_ClsStr);
        v_ClsStr := NULL;
      else
        v_arrCls(v_arrCls.COUNT) := TO_NUMBER(SUBSTR(v_ClsStr, 1, v_Pos - 1));
        v_ClsStr := SUBSTR(v_ClsStr, v_Pos + 1);
      end if;
    END LOOP;

    IF v_arrCls.COUNT > 0 THEN
      v_counter := CalcNumWorkDaysForPeriod(dateFrom,dateTo,v_arrCls);
      v_arrCls.delete;
    END IF;

    RETURN v_counter;
  END;

  -- Возвращает количество рабочих дней между двумя датами, учитывая календари переданных параметров
  FUNCTION GetNumWorkDaysForPeriod(dateFrom IN DATE, dateTo IN DATE, p_FIID IN NUMBER, p_CalcFIID IN NUMBER,
                                   p_PartyID IN NUMBER, p_NotMore3 IN INTEGER DEFAULT 1, p_MainCalendarId IN INTEGER DEFAULT CALENDAR_TYPE_BOOK,
                                   p_AddCalendarId IN INTEGER DEFAULT -1)
  RETURN NUMBER
  AS
    v_arrCls CAL_ARRAY := CAL_ARRAY();
    v_CalendarID NUMBER := -1;
    v_counter NUMBER := 0;
    v_i NUMBER := 0;
    v_j NUMBER := 0;
  BEGIN
    IF dateFrom IS null OR dateTo IS null OR dateFrom > dateTo OR dateFrom = dateTo
    THEN
      RETURN 0;
    END IF;

    if( p_FIID is not null and p_FIID != -1 and CheckFI_Kind(p_FIID) = 1)then
       v_CalendarID := GetLinkCalByCurrency(p_FIID);
       if( v_CalendarID != -1 and v_CalendarID != CALENDAR_TYPE_BOOK)then
          v_arrCls.EXTEND;
          v_arrCls(v_arrCls.COUNT) := v_CalendarID;
          v_i := v_i+1;
       end if;
       v_j := v_j+1;
    end if;

    if( p_CalcFIID is not null and p_CalcFIID != -1 and CheckFI_Kind(p_CalcFIID) = 1)then
       v_CalendarID := GetLinkCalByCurrency(p_CalcFIID);
       if( v_CalendarID != -1 and v_CalendarID != CALENDAR_TYPE_BOOK)then
          v_arrCls.EXTEND;
          v_arrCls(v_arrCls.COUNT) := v_CalendarID;
          v_i := v_i+1;
       end if;
       v_j := v_j+1;
    end if;

    if( p_PartyID is not null and p_PartyID != -1 )then
       v_CalendarID := GetLinkCalByPartyCountry(p_PartyID);
       if( v_CalendarID != -1 and v_CalendarID != CALENDAR_TYPE_BOOK)then
          v_arrCls.EXTEND;
          v_arrCls(v_arrCls.COUNT) := v_CalendarID;
          v_i := v_i+1;
       end if;
       v_j := v_j+1;
    end if;

    v_arrCls.EXTEND;
    v_arrCls(v_arrCls.COUNT) := p_MainCalendarId;

    if (p_AddCalendarId >= 0) then
      v_arrCls.EXTEND;
      v_arrCls(v_arrCls.COUNT) := p_AddCalendarId;
    end if;

    if( v_arrCls.COUNT > 0 )then
          v_counter := CalcNumWorkDaysForPeriod(dateFrom, dateTo, v_arrCls, p_NotMore3);
          v_arrCls.delete;
    end if;

    RETURN v_counter;
  END;

  -- Получить дату через определенное число рабочих дней, учитывая календари переданных параметров
  FUNCTION GetDateAfterWorkDay(p_Date IN DATE, p_DayOffset IN INTEGER, p_FIID IN NUMBER, p_CalcFIID IN NUMBER, p_PartyID IN NUMBER)
  RETURN DATE
  AS
    v_arrCls NUM_ARRAY := NUM_ARRAY();
    v_CalendarID NUMBER := -1;
    v_Date DATE := trunc(p_Date);
    v_i NUMBER := 0;
    v_j NUMBER := 0;
  BEGIN

    if( p_FIID is not null and p_FIID != -1 and CheckFI_Kind(p_FIID) = 1)then
       v_CalendarID := GetLinkCalByCurrency(p_FIID);
       if( v_CalendarID != -1)then
          v_arrCls.EXTEND;
          v_arrCls(v_arrCls.COUNT) := v_CalendarID;
          v_i := v_i+1;
       end if;
       v_j := v_j+1;
    end if;

    if( p_CalcFIID is not null and p_CalcFIID != -1 and CheckFI_Kind(p_CalcFIID) = 1)then
       v_CalendarID := GetLinkCalByCurrency(p_CalcFIID);
       if( v_CalendarID != -1)then
          v_arrCls.EXTEND;
          v_arrCls(v_arrCls.COUNT) := v_CalendarID;
          v_i := v_i+1;
       end if;
       v_j := v_j+1;
    end if;

    if( p_PartyID is not null and p_PartyID != -1 )then
       v_CalendarID := GetLinkCalByPartyCountry(p_PartyID);
       if( v_CalendarID != -1)then
          v_arrCls.EXTEND;
          v_arrCls(v_arrCls.COUNT) := v_CalendarID;
          v_i := v_i+1;
       end if;
       v_j := v_j+1;
    end if;

    --if( v_arrCls.COUNT = 0 or v_i < v_j )then
       v_arrCls.EXTEND;
       v_arrCls(v_arrCls.COUNT) := RSI_DlCalendars.CALENDAR_TYPE_BOOK;
    --end if;

    if( v_arrCls.COUNT > 0 )then
       v_Date := GetDateAfterWorkDayByCls(p_Date, p_DayOffset, v_arrCls);
       v_arrCls.delete;
    end if;

    RETURN v_Date;
  END;
  
  -- Получить дату для плановой даты шага ТО (платежа)
  FUNCTION GetDateWorkDayForPayStep(p_Date IN DATE, p_CalenKindId IN NUMBER, p_FIID IN NUMBER, p_isObl IN NUMBER, p_isEarly OUT NUMBER, p_EarlyOnlyForObl IN NUMBER DEFAULT 0, p_NoSettlCalend IN NUMBER DEFAULT 0)
  RETURN DATE
  AS
    v_arrCls NUM_ARRAY := NUM_ARRAY();
    v_bufCls NUM_ARRAY := NUM_ARRAY();
    v_CalendarID NUMBER := -1;
    v_shiftCount NUMBER := 1;
    v_earlySettl char := chr(0);
    v_dateSettl NUMBER := 0;
    v_Date DATE := trunc(p_Date);
  BEGIN
    p_isEarly := 0;
    
    v_arrCls.EXTEND;
    v_arrCls(v_arrCls.COUNT) := p_CalenKindId;
    
    CheckCalendarExist( p_Date, p_CalenKindId);

    select T_EARLYSETTLEMENT, T_DATESETTLEMENT into v_earlySettl, v_dateSettl from DFININSTR_DBT where T_FIID = p_FIID; 
    if (v_earlySettl = chr(88) and ((p_EarlyOnlyForObl = 0) or (p_EarlyOnlyForObl = 1 and p_isObl = 1))) then
      if (p_isObl = 1 and p_FIID != RSI_RSB_FIInstr.NATCUR) then 
        v_CalendarID := GetLinkCalByCurrency(p_FIID); 
        if( v_CalendarID != -1)then
          BEGIN
            CheckCalendarExist( p_Date, v_CalendarID);
            v_arrCls.EXTEND;
            v_arrCls(v_arrCls.COUNT) := v_CalendarID;
          exception when others then NULL;
          END;
        end if;
      end if;
      p_isEarly := 1;
    end if;

    if (p_isEarly = 1) then 
      v_Date := GetDateAfterWorkDayByCls( p_Date, (-1 * v_shiftCount), v_arrCls, 0);
    else
      if (GetDateAfterWorkDayByCls( p_Date, 0, v_arrCls, 1) <> p_Date) then
        if (p_isObl = 1) then
          if (v_dateSettl = 0) then
            v_Date := GetDateAfterWorkDayByCls( p_Date, v_shiftCount, v_arrCls, 1);
          elsif (v_dateSettl = 1) then
            v_Date := GetDateAfterWorkDayByCls( p_Date, v_shiftCount, v_arrCls, 1);
            if (to_number(to_char(trunc(p_Date), 'MM')) != to_number(to_char(trunc(v_Date), 'MM')) ) then
              v_Date := GetDateAfterWorkDayByCls( p_Date, (-1 * v_shiftCount), v_arrCls, 0);
            end if;
          elsif (v_dateSettl = 2) then
            v_Date := GetDateAfterWorkDayByCls( p_Date, (-1 * v_shiftCount), v_arrCls, 0);
          end if;
        else
          v_Date := GetDateAfterWorkDayByCls( p_Date, v_shiftCount, v_arrCls, 1);
        end if;
      end if;
    end if;

    if (p_NoSettlCalend > 0) then
      BEGIN
        CheckCalendarExist( p_Date, p_NoSettlCalend);
        v_bufCls.EXTEND;
        v_bufCls(v_bufCls.COUNT) := p_NoSettlCalend;
        if (GetDateAfterWorkDayByCls( v_Date, 0, v_bufCls, 1) <> v_Date) then
          v_arrCls.EXTEND;
          v_arrCls(v_arrCls.COUNT) := p_NoSettlCalend;
          v_Date := GetDateAfterWorkDayByCls( v_Date, (-1 * v_shiftCount), v_arrCls, 0);
          p_isEarly := 1;
        end if;
      exception when others then NULL;
      END;
    end if;

    RETURN v_Date;
    exception
      when no_data_found then return v_Date;
  END;
  
  FUNCTION SP_GetDateWorkDay(p_Date IN DATE, p_dockind in NUMBER, p_docid in NUMBER, p_marketId in NUMBER DEFAULT 0)
  RETURN DATE
  AS
    p_calparamarr calparamarr_t;
    v_Date DATE := trunc(p_Date);
    v_calendId NUMBER(10) := 0;
  begin
    p_calparamarr('Object') := DL_GetOperNameByFD(p_dockind, p_docid);
    p_calparamarr('MarketPlace') := RSI_DlCalendars.DL_CALLNK_MARKETPLACE_SEC;
       
    if ((p_marketId is not null) and (p_marketId > 0)) then
      p_calparamarr('Market') := p_marketId;
    end if;
    
    v_calendId := RSI_DlCalendars.DL_GetCalendByDynParam(83,p_calparamarr);
    v_Date := RSI_DlCalendars.GetBankDateAfterWorkDayByCalendar(v_Date, 0, v_calendId);

    return v_Date;

  END;
  
  FUNCTION DL_GetCalendByParamOld(p_operName IN VARCHAR2, p_objType IN NUMBER, p_identProgram IN NUMBER, p_marketId in NUMBER DEFAULT 0, p_daytype in NUMBER DEFAULT 0)
  RETURN NUMBER
  AS
    v_recCount  NUMBER(10) := 0;
    v_calKindId NUMBER(10) := 0;
    v_objType   NUMBER(10) := 0;
    v_DayType NUMBER(5) := p_daytype;
  begin
    SELECT COUNT(*) into v_recCount
      FROM DDLCALENOPRS_DBT dlcalenoprs
     WHERE dlcalenoprs.t_NAME = p_operName 
       AND dlcalenoprs.t_IDENTPROGRAM = p_identProgram
       AND dlcalenoprs.t_OBJTYPE = CASE WHEN p_objType > 0 THEN p_objType ELSE dlcalenoprs.t_OBJTYPE END;

    if (v_recCount = 1) then
      SELECT t_OBJTYPE into v_objType
        FROM DDLCALENOPRS_DBT dlcalenoprs
       WHERE dlcalenoprs.t_NAME = p_operName 
         AND dlcalenoprs.t_IDENTPROGRAM = p_identProgram
         AND dlcalenoprs.t_OBJTYPE = CASE WHEN p_objType > 0 THEN p_objType ELSE dlcalenoprs.t_OBJTYPE END;
    end if;

    if ((GREATEST(p_objType,v_objType) = DL_CALLNK_MARKET) and (v_DayType = 0)) then
      v_DayType := DL_CALLNK_MRKTDAY_TRADE;
    end if;

    BEGIN
      SELECT t_CALKINDID INTO v_calKindId FROM (SELECT * 
        FROM ddlcalendlnk_dbt dlcalendlnk
       WHERE     dlcalendlnk.t_IDENTPROGRAM = p_identProgram
             AND dlcalendlnk.t_OBJTYPE IN (v_objType, 0)
             AND (   dlcalendlnk.t_OBJNAME is NULL OR dlcalendlnk.t_OBJNAME in (chr(0),chr(1))
                  OR dlcalendlnk.t_OBJNAME = p_operName)
             AND dlcalendlnk.t_MARKETID IN (p_marketId, 0)
             AND dlcalendlnk.t_MARKETDAYTYPE IN (v_DayType, 0)
      ORDER BY dlcalendlnk.t_OBJTYPE DESC, dlcalendlnk.t_OBJNAME DESC, dlcalendlnk.T_MARKETID DESC, dlcalendlnk.t_MARKETDAYTYPE DESC) WHERE rownum = 1;
    exception
      when others then NULL;
    END;
    return v_calKindId;
  end;

  FUNCTION DL_GetCalendByParam(p_operName IN VARCHAR2, p_objType IN NUMBER, p_identProgram IN NUMBER, p_marketId in NUMBER DEFAULT 0, p_daytype in NUMBER DEFAULT 0)
  RETURN NUMBER
  AS
    p_calparamarr calparamarr_t;
  begin
    if ((p_operName is not null) and (p_operName <> chr(0)) and (p_operName <> chr(1))) then
      p_calparamarr('Object') := p_operName;
    end if;
    
    if ((p_objType is not null) and (p_objType > 0)) then
      p_calparamarr('ObjectType') := p_objType;
    end if;
    
    if ((p_marketId is not null) and (p_marketId > 0)) then
      p_calparamarr('Market') := p_marketId;
    end if;
    
    if ((p_daytype is not null) and (p_daytype > 0)) then
      p_calparamarr('DayType') := p_daytype;
    end if;

    return DL_GetCalendByDynParam(p_identProgram,p_calparamarr);
  end;
  
  FUNCTION DL_GetCalendByDynParam (p_identProgram IN NUMBER,
                                   p_CalParamArr     calparamarr_t)
      RETURN NUMBER
  AS
      v_recCount        NUMBER (10) := 0;
      v_calKindId       NUMBER (10) := 0;
      v_objType         NUMBER (10) := 0;
      v_objTypeFromParm NUMBER (10);
      v_DayType         NUMBER (5);
      v_operName        DDLCALENOPRS_DBT.T_NAME%TYPE;
      l_sql             VARCHAR2 (32767);
      l_whereSql        VARCHAR2 (32767);
      v_CalParamArrEmpty     calparamarr_t;
      v_CalParamArr     calparamarr_t;
      v_Cur             SYS_REFCURSOR; 
      TYPE CalendRow IS RECORD
    (
        T_CALKINDID     NUMBER,
        T_COUNT         NUMBER,
        t_paramCount    NUMBER
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
  
      IF (    (GREATEST (NVL(v_objTypeFromParm,-1), v_objType) = DL_CALLNK_MARKET)
          AND (v_DayType = 0))
      THEN
          v_CalParamArr ('DayType') := DL_CALLNK_MRKTDAY_TRADE;
      END IF;
  
      l_sql := 'SELECT prm.*, (select count(1) from DDLCALPARAMLNK_DBT where T_CALPARAMID = prm.T_ID) as t_paramCount, 0 ';
  
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
              || l_whereSql;
  
          l_sql :=
              'select T_CALKINDID, T_COUNT, T_PARAMCOUNT from (' || l_sql || ') q1 where q1.T_PARAMCOUNT > 0 order by q1.t_count desc, q1.T_PARAMCOUNT asc';
  
          OPEN v_Cur for l_sql;
         
          FETCH v_Cur BULK COLLECT INTO p_CalendTable LIMIT 2;
          
          if (p_CalendTable.COUNT > 0) then
            if NOT ((p_CalendTable.COUNT = 2) and (p_CalendTable(1).T_COUNT = p_CalendTable(2).T_COUNT) AND (p_CalendTable(1).t_paramCount = p_CalendTable(2).t_paramCount)) then
              v_calKindId := p_CalendTable(1).T_CALKINDID;
            end if;
          END IF;
          
          CLOSE v_Cur;
  
      EXCEPTION
          WHEN OTHERS
          THEN
              NULL;
      END;
  
      RETURN v_calKindId;
  END;

  FUNCTION DL_GetOperNameByFD(p_docKind IN NUMBER, p_docId IN NUMBER)
  RETURN VARCHAR2
  AS
    v_operName  VARCHAR2(80) := '';
  begin
    BEGIN
      SELECT oprk.T_NAME into v_operName
        FROM DOPROPER_DBT opro, DOPRKOPER_DBT oprk
       WHERE     opro.T_DOCUMENTID = LPAD (p_docId, 34, '0')
             AND opro.T_DOCKIND = p_docKind
             AND OPRK.T_KIND_OPERATION = opro.T_KIND_OPERATION;
    exception
      when no_data_found then NULL;
    END;
    return v_operName;
  end;
  
  FUNCTION DL_GetOperNameByKind(p_operKind IN NUMBER)
  RETURN VARCHAR2
  AS
    v_operName  VARCHAR2(80) := '';
  begin
    BEGIN
      SELECT oprk.T_NAME into v_operName
        FROM DOPRKOPER_DBT oprk
       WHERE     OPRK.T_KIND_OPERATION = p_operKind;
    exception
      when no_data_found then NULL;
    END;
    return v_operName;
  end;

  FUNCTION DL_GetCalendarArrByParam (p_CalParamArr  calparamarr_t)
      RETURN CAL_ARRAY
  AS
    l_sql             VARCHAR2 (32767) := '';
    l_whereSql        VARCHAR2 (32767) := '';
    cur_select        sys_refcursor;
    p_cal_array       CAL_ARRAY := CAL_ARRAY();
    p_calkindid       DDLCALENDLNK_DBT.T_CALKINDID%TYPE;
  BEGIN
    l_sql := ' SELECT DISTINCT parm.T_CALKINDID
                 FROM DDLCALPARAM_DBT parm
                WHERE 1=1 ';

    FOR param_knd IN (SELECT * FROM DDLCALPARAMKND_DBT)
    LOOP
      IF (p_CalParamArr.EXISTS (param_knd.T_CODE))
      THEN
        l_whereSql := l_whereSql || ' AND exists (select 1 from DDLCALPARAMLNK_DBT lnk where lnk.T_KNDCODE = ''' || param_knd.T_CODE || ''' and lnk.T_VALUE = ''' || p_CalParamArr(param_knd.T_CODE)  || ''' and LNK.T_CALPARAMID = PARM.T_ID) ';
      END IF;
    END LOOP;

    open cur_select for l_sql || l_whereSql;
    loop
      fetch cur_select
        into p_calkindid;
      exit when cur_select%notfound;
      p_cal_array.EXTEND;
      p_cal_array(p_cal_array.COUNT) := p_calkindid;
    end loop;
    close cur_select;
  
    RETURN p_cal_array;
  END;

  FUNCTION DL_GetMinDateOfCalends (p_FromDate DATE, p_DaysShift INTEGER, p_CalendArr CAL_ARRAY)
      RETURN DATE
  AS
    p_ResultDate      DATE := TO_DATE('31.12.2199','DD.MM.YYYY');
    p_arrCls          NUM_ARRAY := NUM_ARRAY();
  BEGIN
    IF p_CalendArr.COUNT = 0 THEN
      p_ResultDate := p_FromDate;
    ELSE
      FOR i IN p_CalendArr.FIRST .. p_CalendArr.LAST
      LOOP
         p_arrCls.DELETE;
         p_arrCls.EXTEND;
         p_arrCls(p_arrCls.COUNT) := p_CalendArr(i);
         p_ResultDate := LEAST(GetDateAfterWorkDayByCls(p_FromDate, p_DaysShift, p_arrCls), p_ResultDate);
      END LOOP;
    END IF;

    RETURN p_ResultDate;
  END;

END;
/
