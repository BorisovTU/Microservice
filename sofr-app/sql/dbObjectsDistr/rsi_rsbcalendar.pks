CREATE OR REPLACE PACKAGE RSI_RsbCalendar IS

  c_CalendarID INTEGER := 0; -- CALKINDIDMAINSYS

  CALENDAR_SERVICE_NO     VARCHAR2(2) := '00';    -- нет обслуживания
  CALENDAR_SERVICE_BANK   VARCHAR2(2) := '01';    -- банковское
  CALENDAR_SERVICE_RETAIL VARCHAR2(2) := '10';    -- 2 розничное ??

  CALENDAR_BALANCE_NO  VARCHAR2(2) := '00';    -- нет баланса
  CALENDAR_BALANCE_YES VARCHAR2(2) := '01';    -- есть баланс

  UNDEF_BALANCE      INTEGER := 0;   -- не определено, устанавливается в календарях ВСП
  ALLDAY_BALANCE     INTEGER := 1;   -- ежедневно
  BANKDAY_BALANCE    INTEGER := 2;   -- по дням банковского обслуживания
  RETAILDAY_BALANCE  INTEGER := 3;   -- по дням розничного обслуживания

  CHANGE_UNSET     VARCHAR2(2) := '00';    -- нет
  CHANGE_SET       VARCHAR2(2) := '01';    -- да

-- Получить вид обслуживания дня календаря
  FUNCTION GetCalenDaySType( p_CalenDays IN dcalendar_dbt.t_CalenDays%TYPE, p_YearNumberDate IN INTEGER )
  RETURN VARCHAR2;
  
  -- Проверить, является ли дата рабочим днем
  FUNCTION IsWorkDay( p_Date IN DATE, p_CalendarID IN INTEGER DEFAULT NULL ) RETURN INTEGER;

  -- Получить дату через определенное число рабочих дней
  FUNCTION GetDateAfterWorkDay( p_Date IN DATE, p_DayOffset IN INTEGER, p_CalendarID IN INTEGER DEFAULT NULL )
  RETURN DATE;

  -- Получить календа в заданном подразделении
  FUNCTION GetCalendar( p_Branch IN INTEGER DEFAULT NULL )
  RETURN INTEGER;

  -- Возвращает количество рабочих дней между двумя датами.
  FUNCTION getWorkDayCount(dateFrom IN DATE, dateTo IN DATE, p_CalendarID IN INTEGER DEFAULT NULL)
  RETURN NUMBER;

  -- Функция определяет дату по номеру дня в заданном году
  FUNCTION GetYearDate(dayNum IN NUMBER, yearnum IN NUMBER)
  RETURN DATE;

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
  RETURN DATE;

  -- Функция считывания календаря
  FUNCTION GetCalendarExt(p_CalKindID IN NUMBER, p_year IN NUMBER, p_calendar IN OUT DCALENDAR_DBT%ROWTYPE)
  RETURN NUMBER;

  -- Функция генерации календаря на год
  FUNCTION GenerateCalendarExt(p_CalKindID IN NUMBER, p_Date IN DATE, p_calendar IN OUT DCALENDAR_DBT%ROWTYPE)
  RETURN NUMBER;


END;
/
