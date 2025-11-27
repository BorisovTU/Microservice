CREATE OR REPLACE PACKAGE RSI_DlCalendars IS
  CalendarKindForPFI NUMBER(5) := null;

  CALENDAR_TYPE_SETTL     NUMBER(10) := 10001;    -- расчетный календарь
  CALENDAR_TYPE_ACC       NUMBER(10) := 10002;    -- учетный календарь
  CALENDAR_TYPE_BOOK      NUMBER(10) := 10003;    -- календарь бухучета

  DL_CALLNK_MARKET        NUMBER(5) := 1; --Биржевая операция
  DL_CALLNK_OUTMARKET     NUMBER(5) := 2; --Внебиржевая операция
  DL_CALLNK_SERVOP        NUMBER(5) := 3; --Сервисная операция

  DL_CALLNK_MRKTDAY_TRADE NUMBER(5) := 1; --Торговый тип биржевого дня
  DL_CALLNK_MRKTDAY_SETTL NUMBER(5) := 2; --Расчетный тип биржевого дня
  
  DL_CALLNK_MARKETPLACE_SEC NUMBER(5) := 1; --Фондовый
  DL_CALLNK_MARKETPLACE_CUR NUMBER(5) := 2; --Валютный
  DL_CALLNK_MARKETPLACE_DV NUMBER(5) := 3; --Срочный
  DL_CALLNK_MARKETPLACE_SPFI NUMBER(5) := 4; --СПФИ
  
  /**Тип - массив значений по текстовому индексу */
  TYPE calparamarr_t IS TABLE OF DDLCALPARAMLNK_DBT.T_VALUE%TYPE INDEX BY DDLCALPARAMLNK_DBT.T_KNDCODE%TYPE;

  /**Тип - массив календарей*/
  TYPE CAL_ARRAY IS TABLE OF DDLCALENDLNK_DBT.T_CALKINDID%TYPE;

  /**
  * Получить календарь, привязанный к стране переданного субъекта
  * @since        6.20.031.57.0
  * @param     pPartyID Идентификатор субъекта страны
  * @return     NUMBER Идентификатор календаря
  */
  FUNCTION GetLinkCalByPartyCountry(pPartyID in NUMBER) RETURN NUMBER;

  /**
  * Получить календарь, привязанный к FIID
  * @since        6.20.031.57.0
  * @param     p_FIID Идентификатор фин. инструмента
  * @return     NUMBER Идентификатор календаря
  */
  FUNCTION GetLinkCalByCurrency( pFIID in NUMBER) RETURN NUMBER;

  /**
  * Получить календарь официальных рабочих дней в РФ
  * @since        6.20.031.57.0
  * @return     NUMBER Идентификатор календаря
  */
  FUNCTION GetOfficialCalendar RETURN NUMBER;

  /**
  * Возвращает количество рабочих дней между двумя датами по массиву календарей, переданному в виде строки
  * @since        6.20.031.57.0
  * @param     dateFrom Дата ОТ
  * @param     dateTo Дата ДО
  * @param     p_ClsStr Идентификаторы календарей в виде строки через запятую
  * @return     NUMBER Количество рабочих дней
  */
  FUNCTION GetNumWorkDaysForPeriodByCls(dateFrom IN DATE, dateTo IN DATE, p_ClsStr IN VARCHAR2) RETURN NUMBER;

  /**
  * Получение количества рабочих дней между двумя датами, учитывая календари переданных параметров
  * День будет считаться рабочим, только если день будет рабочим во всех календарях (календарях обоих валют, и субъекта, если таковые были переданы, незаполненными считаются NULL или -1)
  * @since        6.20.031.57.0
  * @param     dateFrom Дата ОТ
  * @param     dateTo Дата ДО
  * @param     p_FIID Идентификатор фин. инструмента 1 (принимаются только валюты и металлы)
  * @param     p_CalcFIID Идентификатор фин. инструмента 2 (принимаются только валюты и металлы)
  * @param     p_PartyID ID Субъекта
  * @param     p_NotMore3 Если параметр равен "1", то возвращает не более 3 дней (требуется для классификации срока по сделкам)
  * @param     p_MainCalendarId Основной календарь вычисления рабочих дней (по умолчанию - бухучета)
  * @param     p_AddCalendarId Дополнительный календарь вычисления рабочих дней (по умолчанию - не установлен)
  * @return     NUMBER Количество рабочих дней
  */
  FUNCTION GetNumWorkDaysForPeriod(dateFrom IN DATE, dateTo IN DATE, p_FIID IN NUMBER, p_CalcFIID IN NUMBER,
                                   p_PartyID IN NUMBER, p_NotMore3 IN INTEGER DEFAULT 1, p_MainCalendarId IN INTEGER DEFAULT CALENDAR_TYPE_BOOK,
                                   p_AddCalendarId IN INTEGER DEFAULT -1) RETURN NUMBER;

  /**
  * Получение даты через определенное число рабочих дней, учитывая все календари переданных параметров
  * День будет считаться рабочим, только если день будет рабочим во всех календарях (календарях обоих валют, и субъекта, если таковые были переданы, незаполненными считаются NULL или -1)
  * @since        6.20.031.57.0
  * @param     p_Date Дата, от которой ищется рабочая дата
  * @param     p_DayOffset Количество рабочих дней смещения даты. Значение может равняться нулю, в таком случае смещения не происходит
  * @param     p_FIID Идентификатор фин. инструмента 1 (принимаются только валюты и металлы)
  * @param     p_CalcFIID Идентификатор фин. инструмента 2 (принимаются только валюты и металлы)
  * @param     p_PartyID ID Субъекта
  * @return     DATE Рабочая дата
  */
  FUNCTION GetDateAfterWorkDay(p_Date IN DATE, p_DayOffset IN INTEGER, p_FIID IN NUMBER, p_CalcFIID IN NUMBER, p_PartyID IN NUMBER) RETURN DATE;

  /**
  * Получение банковской даты
  * @since        6.20.031.62.6
  * @param     p_Date Дата, от которой ищется банковская дата
  * @param     p_DayOffset Количество банковских дней смещения даты. Значение может равняться нулю, в таком случае смещения не происходит
  * @param     p_CalendarID Идентификатор календаря, по которому осуществляется поиск
  * @param     p_isForward Направление поиска банковской даты (True ? в будущие даты, False ? в прошлые даты). По умолчанию ? в будущие даты.
  * @return     DATE Банковская дата
  */
  FUNCTION GetBankDateAfterWorkDayByCalendar( p_Date IN DATE, p_DayOffset IN INTEGER, p_CalendarID IN INTEGER, p_isForward IN INTEGER DEFAULT 1, p_FIID IN INTEGER DEFAULT -1, p_Contractor IN INTEGER DEFAULT -1) RETURN DATE;

  /**
  * Получение балансовой даты
  * @since        6.20.031.62.6
  * @param     p_Date Дата, от которой ищется балансовая дата
  * @param     p_DayOffset Количество балансовых дней смещения даты. Значение может равняться нулю, в таком случае смещения не происходит
  * @param     p_CalendarID Идентификатор календаря, по которому осуществляется поиск
  * @param     p_isForward Направление поиска балансовой даты (True ? в будущие даты, False ? в прошлые даты). По умолчанию ? в будущие даты.
  * @return     DATE Балансовая дата
  */
  FUNCTION GetBalanceDateAfterWorkDayByCalendar( p_Date IN DATE, p_DayOffset IN INTEGER, p_CalendarID IN INTEGER, p_isForward IN INTEGER DEFAULT 1, p_FIID IN INTEGER DEFAULT -1, p_Contractor IN INTEGER DEFAULT -1 ) RETURN DATE;

  /**
  * Возвращает дату рабочего дня по особому алгоритму нахождения дат для требований/обязательств по сделкам
  * @since        6.20.031.62.6
  * @param     p_Date Дата, от которой осуществляется поиск
  * @param     p_FIID Валюта требования/обязательства
  * @param     p_isObl Является ли искомая дата датой обязательства
  * @param     p_EarlyOnlyForObl Учитываем ранние расчёты только для обязательств
  * @return     DATE Дата рабочего дня
  */
  FUNCTION GetDateWorkDayForPayStep(p_Date IN DATE,  p_CalenKindId IN NUMBER, p_FIID IN NUMBER, p_isObl IN NUMBER, p_isEarly OUT NUMBER, p_EarlyOnlyForObl IN NUMBER DEFAULT 0, p_NoSettlCalend IN NUMBER DEFAULT 0) RETURN DATE;

  FUNCTION SP_GetDateWorkDay(p_Date IN DATE, p_dockind in NUMBER, p_docid in NUMBER, p_marketId in NUMBER DEFAULT 0) RETURN DATE;

  /**
  * Проверка даты по календарю на наличие признаков: балансовый, банковский, розничный.
  * @since        6.20.031.62.8
  * @param     p_Date Дата, от которой осуществляется поиск
  * @param     p_CalendarID Идентификатор календаря, по которому осуществляется поиск
  * @param     p_CheckBalance Проверять балансовый признак
  * @param     p_CheckBank Проверять банковский признак
  * @param     p_CheckRetail Проверять розничный признак
  * @return     INTEGER 1 - ИСТИНА, 0 - ЛОЖЬ
  */
  FUNCTION IsDay( p_Date IN DATE, p_CalendarID IN INTEGER, p_CheckBalance IN NUMBER, p_CheckBank IN NUMBER, p_CheckRetail IN NUMBER) RETURN INTEGER;

  /**
  *Получить тип обслуживания
  * @since        6.20.031
  * @param     p_Date Дата, для которой ищем тип обслуживания
  * @param     p_CalendarID Идентификатор календаря
  * @return    вид облуживания
  */
  FUNCTION GetTypeDay( p_Date IN DATE, p_CalendarID IN INTEGER ) RETURN VARCHAR2 ;

  /**
  *Получить вид календаря по параметрам
  * @since        6.20.031
  * @param     p_operName Наименование операции
  * @param     p_objType Вид объекта (биржевой, внебиржевой, сервисный)
  * @param     p_identProgram Подсистема
  * @param     p_marketId ID субъекта биржи для биржевой операции
  * @return    вид календаря
  */
  FUNCTION DL_GetCalendByParam(p_operName IN VARCHAR2, p_objType IN NUMBER, p_identProgram IN NUMBER, p_marketId in NUMBER DEFAULT 0, p_daytype in NUMBER DEFAULT 0) RETURN NUMBER;

  FUNCTION DL_GetOperNameByFD(p_docKind IN NUMBER, p_docId IN NUMBER) RETURN VARCHAR2;

  FUNCTION DL_GetOperNameByKind(p_operKind IN NUMBER) RETURN VARCHAR2;

  FUNCTION IsBalanceDay( p_Date IN DATE, p_CalendarID IN INTEGER ) RETURN INTEGER;
  
  FUNCTION DL_GetCalendByDynParam (p_identProgram IN NUMBER,
                                 p_CalParamArr     calparamarr_t) RETURN NUMBER;
                                 
  FUNCTION DL_GetCalendByParamOld(p_operName IN VARCHAR2, p_objType IN NUMBER, p_identProgram IN NUMBER, p_marketId in NUMBER DEFAULT 0, p_daytype in NUMBER DEFAULT 0)
  RETURN NUMBER;

  /**
  *Получить массив календарей по параметрам
  * @since        6.20.031
  * @param     p_CalParamArr Массив параметров подбора календаря
  * @return    Массив календарей
  */
  FUNCTION DL_GetCalendarArrByParam(p_CalParamArr calparamarr_t) RETURN CAL_ARRAY;

  /**
  *Получить минимальную рабочую дату по массиву календарей
  * @since        6.20.031
  * @param     p_CalParamArr Массив параметров подбора календаря
  * @return    Массив календарей
  */
  FUNCTION DL_GetMinDateOfCalends (p_FromDate DATE, p_DaysShift INTEGER, p_CalendArr CAL_ARRAY) RETURN DATE;

  /**
  * Получить количество рабочих дней между двумя датами по массиву календарей (по всем календарям день должен быть рабочим)
  * @since        6.20.031
  * @param     dateFrom Дата начала
  * @param     dateTo Дата окончания
  * @param     p_arrCls Массив календарей
  * @param     p_NotMore3 Выдавать результат не более 3
  * @return    Количество рабочих дней
  */
  FUNCTION CalcNumWorkDaysForPeriod(dateFrom IN DATE, dateTo IN DATE, p_arrCls IN CAL_ARRAY, p_NotMore3 IN INTEGER DEFAULT 0) RETURN NUMBER;

  /**
  * Получить количество рабочих дней между двумя датами по массиву календарей (хотя бы по одному день должен быть рабочим)
  * @since        6.20.031
  * @param     dateFrom Дата начала
  * @param     dateTo Дата окончания
  * @param     p_arrCls Массив календарей
  * @param     p_NotMore3 Выдавать результат не более 3
  * @return    Количество рабочих дней
  */
  FUNCTION CalcNumWorkDaysForPeriodAtLeastOne(dateFrom IN DATE, dateTo IN DATE, p_arrCls IN CAL_ARRAY, p_NotMore3 IN INTEGER DEFAULT 0) RETURN NUMBER;
END;
/