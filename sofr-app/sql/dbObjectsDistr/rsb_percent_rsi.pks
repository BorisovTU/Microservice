/**
 * Пакет функций для работы механизма "Проценты"
 */
CREATE OR REPLACE PACKAGE RSI_RSB_PERCENT IS

     -- Типы параметров ППС
    CONST_PRC_FR_TYPE_NOTCOND       CONSTANT NUMBER(5)  := 1;   -- Без условий
    CONST_PRC_FR_TYPE_DURATION      CONSTANT NUMBER(5)  := 2;   -- Срок
    CONST_PRC_FR_TYPE_REST          CONSTANT NUMBER(5)  := 3;   -- Остаток

    -- Параметры ППС
    CONST_PRC_FR_PARAM_CNTBEGDATE   CONSTANT NUMBER(5)  := 1;   -- Дата начала договора
    CONST_PRC_FR_PARAM_CNTDURATION  CONSTANT NUMBER(5)  := 2;   -- Срок договора
    CONST_PRC_FR_PARAM_CNTNULLDATE  CONSTANT NUMBER(5)  := 3;   -- Дата обнуления счета

    CONST_PRC_FR_PARAM_REST         CONSTANT NUMBER(5)  := 1;   -- Остаток счета
    CONST_PRC_FR_PARAM_MIN          CONSTANT NUMBER(5)  := 2;   -- Минимальный
    CONST_PRC_FR_PARAM_MAX          CONSTANT NUMBER(5)  := 3;   -- Максимальный
    CONST_PRC_FR_PARAM_AVG          CONSTANT NUMBER(5)  := 4;   -- Среднехронологический
    CONST_PRC_FR_PARAM_UNLOWER      CONSTANT NUMBER(5)  := 5;   -- Неснижаемый

    -- Период расчета остатка
    CONST_PRC_FR_PERIOD_MONTH      CONSTANT NUMBER(5)  := 1;   -- Календарный месяц
    CONST_PRC_FR_PERIOD_CONTRACT   CONSTANT NUMBER(5)  := 2;   -- Период договора

    -- Тип графика
    CONST_PRC_SCHED_TYPE_ABSOLUTE  CONSTANT NUMBER(5)  := 1;   -- Абсолютный
    CONST_PRC_SCHED_TYPE_RELATIVE  CONSTANT NUMBER(5)  := 2;   -- Относительный

    -- Тип параметра графика
    CONST_PRC_SCHED_PARTYPE_MAIN   CONSTANT NUMBER(5)  := 1;   -- Основной график
    CONST_PRC_SCHED_PARTYPE_END    CONSTANT NUMBER(5)  := 2;   -- Дата окончания
    CONST_PRC_SCHED_PARTYPE_BEGIN  CONSTANT NUMBER(5)  := 3;   -- Дата начала
    CONST_PRC_SCHED_PARTYPE_CALEND CONSTANT NUMBER(5)  := 4;   -- Календарная дата
    CONST_PRC_SCHED_PARTYPE_MONTH  CONSTANT NUMBER(5)  := 5;   -- Число месяца

    -- Тип периода
    CONST_PRC_SCHED_PERIOD_WRK_DAY CONSTANT NUMBER(5)  := 1;   -- Рабочий день
    CONST_PRC_SCHED_PERIOD_CAL_DAY CONSTANT NUMBER(5)  := 2;   -- Календарный день
    CONST_PRC_SCHED_PERIOD_WRK_MON CONSTANT NUMBER(5)  := 3;   -- Рабочий месяц
    CONST_PRC_SCHED_PERIOD_CAL_MON CONSTANT NUMBER(5)  := 4;   -- Календарный месяц

    -- Тип процентной ставки
    CONST_PRC_RATE_TYPE_ABSOLUTE   CONSTANT NUMBER(5)  := 1;   -- Абсолютный
    CONST_PRC_RATE_TYPE_USER       CONSTANT NUMBER(5)  := 2;   -- Пользовательская
    CONST_PRC_RATE_TYPE_FLOAT      CONSTANT NUMBER(5)  := 3;   -- Плавающая

    -- Тип базы для расчета процентов
    CONST_PRC_CALC_BASETYPE_REST   CONSTANT NUMBER(5)  := 1;   -- Остаток
    CONST_PRC_CALC_BASETYPE_USER   CONSTANT NUMBER(5)  := 2;   -- Пользовательская

    PROCEDURE Prc_GenerateCalendarGraph(ContractID IN NUMBER, SchedID IN NUMBER);

    -- Проверка корректности задания ППС формулой (содрано из ЛОАНСа)
    FUNCTION CheckFlRateFormula(SpFormula IN OUT VARCHAR2, err OUT NUMBER, varname OUT VARCHAR2, Point IN NUMBER) RETURN NUMBER;

    PROCEDURE Prc_GenCalPercRatesTable(ContractID IN NUMBER);

    FUNCTION Prc_CalcRateTableForFR (ContractID IN NUMBER, BeginDate IN DATE, EndDate IN DATE) RETURN NUMBER;

    PROCEDURE PrcBaseRestTestFunc ( ContractID IN NUMBER, BeginDate IN DATE, EndDate IN DATE);

    -- Расчет процентов за период
    FUNCTION Prc_CalcPercentValues ( TotalSum OUT dprccntsched_dbt.t_sumcalc%TYPE,
                                     ContractID IN NUMBER,
                                     BeginDate IN DATE,
                                     EndDate IN DATE,
                                     DailyFlag IN NUMBER
                                   ) RETURN NUMBER;

    PROCEDURE PrcBillRestList(ContractID IN NUMBER, BeginDate IN DATE, EndDate IN DATE);

    -- Генерация КТПС и последующий расчет процентов
    FUNCTION Prc_CalcCntPercents  (  TotalSum       OUT dprccntsched_dbt.t_sumcalc%TYPE, -- рассчитанная сумма
                                     ContractID     IN NUMBER,      -- ID договора
                                     BeginDate      IN DATE,        -- Дата начала периода
                                     EndDate        IN DATE,        -- Дата окончания периода
                                     FlagDailyCalc  IN NUMBER,      -- Флаг, 1 - ежедневный расчет
                                     FlagSaveRates  IN NUMBER       -- Флаг, 1 - сохранить КТПС в БД
                                   ) RETURN NUMBER;

    FUNCTION Prc_GetPercentSum  (    ContractID     IN NUMBER,      -- ID договора
                                     BeginDate      IN DATE,        -- Дата начала периода
                                     EndDate        IN DATE,
                                     TotalSum       IN OUT NUMBER   -- рассчитанная сумма
                                ) RETURN NUMBER;

    -- Расчет процентов для Reporting
    FUNCTION PrcCalcPercentsForReporting(ContractID IN NUMBER, BeginDate IN DATE) RETURN NUMBER;

    -- Расчет количества дней в периоде, исходя из тип календаря
    FUNCTION GetDaysInPeriodByCalendar (
                                         Calendar   IN NUMBER,
                                         StartDate  IN DATE,
                                         EndDate    IN DATE,
                                         CntEndDate IN DATE
                                       ) RETURN NUMBER;
                                       
    -- Расчет количества дней в году, исходя из тип календаря
    FUNCTION GetDaysInYearByCalendar (
                                        Calendar   IN NUMBER,
                                        DatePeriod IN DATE
                                     ) RETURN NUMBER;

    -- Получение номера процентного договора (t_number таблицы dPrcContract_dbt) в разрезе бэк-офисов с использованием sequence
    FUNCTION GetPrcContractNextNumber(
                                        backOffice IN NUMBER
                                      ) RETURN NUMBER;

END RSI_RSB_PERCENT;
/