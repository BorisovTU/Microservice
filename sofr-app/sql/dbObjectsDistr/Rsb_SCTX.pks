CREATE OR REPLACE PACKAGE Rsb_SCTX
IS

/**
 * рекорд рассчитанных рыночных параметров
 */
    TYPE R_MarketPrice IS RECORD
    (
      MarketPrice  NUMBER(32,12), -- цена для расчета отклонений
      QuoteValue   NUMBER(32,12), -- значение курса, используемого для определения  MarketPriсe
      Market       VARCHAR2(64),  -- источник этого курса
      DateMarket   DATE,          -- дата курса
      IfMarket     VARCHAR2(1),   -- признак, является ли ценная бумага обращающейся (только для сделок продажи, не являющихся срочными)
      T1           DATE,          -- дата, на которую нужно найти вспомогательный курс  (только для срочных сделок)
      Date2        DATE,          -- дата найденного вспомогательного курса (только для срочных сделок)
      MarketPrice2 NUMBER(32,12), -- значение вспомогательного курса  (только для срочных сделок)
      Market2      VARCHAR2(64),  -- источник вспомогательного курса  (только для срочных сделок)
      ErrorMsg     VARCHAR2(255), -- ошибка
      DealID       NUMBER(10),    -- сделка
      LotID       NUMBER(10)      -- лот
    );

/**
 * рекорд рассчитанных рыночных параметров (новых)
 */
    TYPE R_MarketPrice_NEW IS RECORD
    (
      MarketPrice_New  NUMBER(32,12), -- цена для расчета отклонений
      QuoteValue_New   NUMBER(32,12), -- значение курса, используемого для определения  MarketPriсe
      Market_New       VARCHAR2(64),  -- источник этого курса
      DateMarket_New   DATE,          -- дата курса
      IfMarket_New     VARCHAR2(1),   -- признак, является ли ценная бумага обращающейся (только для сделок продажи, не являющихся срочными)
      Val_Market_New   VARCHAR2(3),   -- валюта курса (может задаваться как кодом валюты, так и %)

      -- Рыночная котировка на отчетную дату, сложившаяся на ОРЦБ
      QuoteValueRep    NUMBER(32,12), -- значение
      MarketRep        VARCHAR2(64),  -- источник информации
      DateMarketRep    DATE,          -- дата курса
      Val_MarketRep    VARCHAR2(3),   -- валюта котировки

      ErrorMsg         VARCHAR2(255), -- ошибка
      DealID           NUMBER(10),    -- сделка
      LotID            NUMBER(10)      -- лот
    );

/**
 * рекорд рассчитанных рыночных параметров (используемых при расчете резерва для целей АРНУ)
 */
    TYPE R_MarketPrice_REZ IS RECORD
    (
      RezMrkt     NUMBER(32,12), -- минимальное значение из всех курсов вида "<Налоговый резерв>"
      MrktRez     VARCHAR2(64),  -- источник этого курса
      DMrktRez    DATE,          -- дата курса
      IfMarketRez VARCHAR2(1),   -- признак, является ли ценная бумага обращающейся (только для сделок продажи, не являющихся срочными)

      ErrorMsg VARCHAR2(255), -- ошибка
      FIID     NUMBER(10),    -- ц\б
      OnDate   DATE           -- дата расчета резерва
    );

/**
 * рекорд основных параметров сделки
 */
    TYPE R_DealParm IS RECORD
    (
      DealID       NUMBER(10),     -- сделка
      DealPart     NUMBER(5),      -- часть сделки
      CalcDate     DATE,           -- дата
      Price        NUMBER(32,12),  -- цена
      Cost         NUMBER(32,12),  -- стоимость
      TotalCost    NUMBER(32,12),  -- полная стоимость
      NKD          NUMBER(32,12)   -- нкд
    );

/**
 * рекорд числовых значений видов курсов ц/б
 */
    TYPE R_RateTypes IS RECORD
    (
      MinRate        NUMBER(5),   -- вид курса минимальная цена
      MaxRate        NUMBER(5),   -- вид курса максимальная цена
      MediumRate     NUMBER(5),   -- вид курса средневзвешенная цена
      ReuterRate     NUMBER(5),   -- вид курса средняя цена Рейтер
      TaxRate        NUMBER(5),   -- вид курса налоговая цена
      CloseRate      NUMBER(5),   -- вид курса цена закрытия
      CloseRateBl    NUMBER(5),   -- вид курса цена закрытия Bloomberg
      TaxReserv      NUMBER(5),   -- вид курса Налоговый резерв
      InvRate        NUMBER(5)    -- вид курса ПРИЗНАВАЕМАЯ КОТИРОВКА
    );

/**
 * расширенный рекорд сделки
 */
    TYPE R_Deal IS RECORD
    (
      Tick   ddl_tick_dbt%ROWTYPE, -- буфер сделки
      FI     dfininstr_dbt%ROWTYPE,-- анкета ФИ
      IsTerm BOOLEAN,              -- срочная сделка
      OGrp   NUMBER,               -- группа сделки
      DZ     DATE,                 -- дата заключения сделки или дата операции
      DPD    DATE,                 -- плановая  дата поставки из платежа по поставке
      DD     DATE,                 -- фактическая  дата поставки из платежа
      Price  NUMBER(32,12),        -- цена в сделке
      CFI    VARCHAR2(3),          -- валюта цены
      TaxGroup NUMBER              -- ГНУ
    );

/**
 * рекорд прочитанных значений настроек НУ
 */
    TYPE R_ReestrValue IS RECORD
    (
      V0 NUMBER(5),   -- Метод списания
      V1 NUMBER(5),   -- Сортировка выбытий по 2 части
      V2 NUMBER(5),   -- Сортировка видов выбытий
      V3 NUMBER(5),   -- Сортировка видов приобретений для открытия ко-ротких позиций
      V4 NUMBER(5),   -- Сортировка видов приобретений для связей ПР/РЗ
      V5 NUMBER(5),   -- Сортировка видов приобретений для связей  ППР/ПЗ
      V6 NUMBER(5),   -- Сортировка видов сделок для закрытия коротких позиций по репо
      V9 NUMBER(5),   -- Игнорировать внутридневные сделки РЕПО/займа
      V10 NUMBER(5),  -- Разрешить продажу блокированных приобретений
      V11 NUMBER(5),  -- Выполнять перетасовку покупок
      V12 NUMBER(5),  -- Расчет резерва с учетом доли ЧП
      V13 NUMBER(5) := null,  -- Учитывать НДС по комиссиям в затратах
      V14 NUMBER(5),  -- НУ списаний по портфелям
      V15 NUMBER(5),  -- Сортировка приобретений для прямого РЕПО: 0 - по убыванию, 1 - по возрастанию.
      V20 NUMBER(5),  -- Сортировка выбытий для прямого РЕПО: 0 - по убыванию, 1 - по возрастанию.
      ModeTax BOOLEAN := null -- Режим хранилища данных для НУ
    );

/**
 * Константы групп налогового учета */
      STATE_BOND_FED_PERC        CONSTANT NUMBER := 1;   -- Государственная облигация федерального уровня - процентная
      STATE_BOND_FED_DISC        CONSTANT NUMBER := 2;   -- Государственная облигация федерального уровня - дисконтная (ГКО)
      STATE_BOND_SUBFED_PERC     CONSTANT NUMBER := 3;   -- Государственная облигация субфедерального уровня - процентная
      STATE_BOND_SUBFED_DISC     CONSTANT NUMBER := 4;   -- Государственная облигация субфедерального уровня - дисконтная
      MOUN_BOND_15_PERC          CONSTANT NUMBER := 5;   -- Муниципальная облигация (ставка налога 15%) - процентная
      MOUN_BOND_15_DISC          CONSTANT NUMBER := 6;   -- Муниципальная облигация (ставка налога 15%) - дисконтная
      MOUN_BOND_9_PERC           CONSTANT NUMBER := 7;   -- Муниципальная облигация (ставка налога  9%) - процентная
      MOUN_BOND_9_DISC           CONSTANT NUMBER := 8;   -- Муниципальная облигация (ставка налога  9%) - дисконтная
      KORP_BOND_24_PERC          CONSTANT NUMBER := 9;   -- Корпоративная облигация (ставка налога 24%) - процентная
      KORP_BOND_24_DISC          CONSTANT NUMBER := 10;  -- Корпоративная облигация (ставка налога 24%) - дисконтная
      KORP_BOND_IP9              CONSTANT NUMBER := 11;  -- Корпоративная облигация с ипотечным покрытием (ставка налога 9 %)
      SHARE_NATCUR               CONSTANT NUMBER := 12;  -- Рублевая акция
      PIF_NATCUR                 CONSTANT NUMBER := 13;  -- Рублевый пай ПИФа
      BOND_INDEXNOM              CONSTANT NUMBER := 15;  -- ОФЗ с индексируемым номиналом
      BOND_2017_2021_NONINDEXNOM CONSTANT NUMBER := 17;  -- Облигации, номинированные в валюте РФ - процентные_2017-2021г.г.(17)
      NATCUR_BOND_PERC_NOTNKD    CONSTANT NUMBER := 18;  -- Облигации в валюте РФ - процентные_выручка без НКД (18)
      CORPORATE_BOND_PERC_NOT_ST CONSTANT NUMBER := 19;  -- Корпоративные облигации (номинал в вал. РФ с нестанд. алг. расч. КД)
      KORP_BOND_IP15             CONSTANT NUMBER := 21;  -- корпоративные облигации с ипотечным покрытием (ставка налога 15 %) (облигации, эмитированные после 01.01.2007 г.)
      SHARE_RUS_ISSUER           CONSTANT NUMBER := 22;  -- Российские депозитарные расписки
      NATCUR_BOND_PERC_NKD       CONSTANT NUMBER := 29;  -- Облигации в валюте РФ - процентные_выручка с НКД (29)
      STATE_BOND_IN_LOAN         CONSTANT NUMBER := 31;  -- Государственная облигация внутреннего валютного займа РФ
      STATE_BOND_OUT_LOAN        CONSTANT NUMBER := 32;  -- Облигация внешнего облигационного займа РФ
      OTHER_BOND_PERC            CONSTANT NUMBER := 33;  -- Прочие процентные облигации (ноты)
      OTHER_BOND_DISC            CONSTANT NUMBER := 34;  -- Прочие дисконтные облигации (ноты)
      PIF_NOT_NATCUR             CONSTANT NUMBER := 35;  -- Валютный пай ПИФа
      DEPOS_RECEIPT              CONSTANT NUMBER := 36;  -- Иностранные депозитарные расписки
      SHARE_NOT_NATCUR           CONSTANT NUMBER := 37;  -- Валютная акция
      OTHER_BOND_PERC_NOT_ST     CONSTANT NUMBER := 43;  -- Прочие облигации (номинал в ин. вал. с нестанд. алг. расч. КД)
      NOT_EMISS_PERC_NATCUR      CONSTANT NUMBER := 919; -- Фин. инструменты, не являющиеся ц/б, номинал кот. выр. в вал. РФ
      NOT_EMISS_PERC_NOT_NATCUR  CONSTANT NUMBER := 943; -- Фин. инструменты, не являющиеся ц/б, номинал кот. выр. в ин. валюте
      STATE_MOUNT_BOND_PERC      CONSTANT NUMBER := 101; -- Государственные и муниципальные облигации - процентные
      NATCUR_BOND_PERC_109       CONSTANT NUMBER := 109; -- Облигации, номинированные в валюте РФ - процентные
      CUR_BOND_PERC_130          CONSTANT NUMBER := 130; -- Облигации, номинированные в иностранной валюте - процентные

/**
 * Константы видов групп облигаций - для НУ */
      BOND_UNDEF         CONSTANT NUMBER := -1;  -- Неопред.
      BOND_FAVOUR        CONSTANT NUMBER := 0;  -- Льготная
      BOND_USUAL         CONSTANT NUMBER := 1;  -- Обычная

/**
 * Константы видов лотов НУ*/
      KINDLOT_NORMAL        CONSTANT NUMBER := 1;  -- обычные лоты
      KINDLOT_COMPDEL_MINUS CONSTANT NUMBER := 2;  -- комп. поставка, уменьшение
      KINDLOT_COMPDEL_PLUS  CONSTANT NUMBER := 3;  -- комп. поставка, увеличение

/**
 * Константы DSCTXMES_DBT */
/**
 *  Виды сообщений в протокол */
       TXMES_MESSAGE   CONSTANT NUMBER := 0;   -- Информационное сообщение
       TXMES_ERROR     CONSTANT NUMBER := 10;  -- Ошибка, продолжение работы невозможно
       TXMES_WARNING   CONSTANT NUMBER := 20;  -- Некритическая ошибка
       TXMES_DEBUG     CONSTANT NUMBER := 30;  -- Отладочное сообщение
       TXMES_TEST      CONSTANT NUMBER := 40;  -- Тестирование корректности построенных связей
       TXMES_PROCESS   CONSTANT NUMBER := 50;  -- Сообщение о текущем выполняемом действии
       TXMES_OPTIM     CONSTANT NUMBER := 60;  -- Отладочное сообщение для оптимизации

/**
 *  глобализмы */
       gl_WasError BOOLEAN; -- "Была ошибка при формированиии связей"
       gl_IsDebug  BOOLEAN := FALSE; -- Отладочный режим
       gl_IsOptim  BOOLEAN := FALSE; -- Отладочный режим для оптимизации

       MarketPrice     R_MarketPrice;
       MarketPrice_NEW R_MarketPrice_NEW;
       MarketPrice_REZ R_MarketPrice_REZ;
       DealParm        R_DealParm;
       RateTypes       R_RateTypes;
       ReestrValue     R_ReestrValue;

       RunParallel NUMBER := 0;

/**
 * Функция получает из настроек системы насройки НУ и заносит их в глобализм.
 * @since 6.20.029
 * @qtest NO
 * @param pOnlyRate 0 - Кроме основных получить значения курсов из настроек налогового учета, иначе !0
 */
    procedure GetSettingsTax(pOnlyRate IN NUMBER DEFAULT 0);

    FUNCTION iif( Cond IN BOOLEAN, n1 IN NUMBER, n2 IN NUMBER ) RETURN NUMBER;
    FUNCTION iif( Cond IN BOOLEAN, n1 IN DATE, n2 IN DATE ) RETURN DATE;
    FUNCTION iif( Cond IN BOOLEAN, n1 IN VARCHAR2, n2 IN VARCHAR2 ) RETURN VARCHAR2;

/**
 * Получить числовое значение из примечания.
 * @since 6.20.029
 * @qtest NO
 * @param v_ObjectType Тип объекта
 * @param v_ObjectID ID объекта
 * @param v_NoteKind Вид примечания
 * @param v_EndDate Дата (не используется)
 * @return Числовое значение
 */
    function GetNoteText( v_ObjectType IN NUMBER, v_ObjectID IN VARCHAR2, v_NoteKind IN NUMBER, v_EndDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) return NUMBER;

/**
 * Получить строковое значение из примечания.
 * @since 6.20.029
 * @qtest NO
 * @param v_ObjectType Тип объекта
 * @param v_ObjectID ID объекта
 * @param v_NoteKind Вид примечания
 * @param v_EndDate Дата (не используется)
 * @return Строковое значение
 */
    function GetNoteTextStr( v_ObjectType IN NUMBER, v_ObjectID IN VARCHAR2, v_NoteKind IN NUMBER, v_EndDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) return VARCHAR2;

/**
 * Проверить наличие категории.
 * @since 6.20.029
 * @qtest NO
 * @param ObjType Тип объекта
 * @param GroupID Категория
 * @param vObjID ID объекта
 * @param AttrID ID атрибута категории
 * @return 1 - есть, 0 - нет
 */
    FUNCTION CheckCateg( ObjType IN NUMBER, GroupID IN NUMBER, ObjID IN VARCHAR2, AttrID IN NUMBER ) RETURN NUMBER DETERMINISTIC;
/**
 * Получить страну происхождения из сдеки */
    function RSI_GetDealCountry(Deal IN R_Deal, pCountry OUT VARCHAR2) return VARCHAR2;

/**
 * Определить срочность сделки.
 * @since 6.20.029
 * @qtest NO
 * @param DealID ID сделки
 * @param FIID  ID ц/б
 * @param ValueDate Дата фактическая, а в её отсутствии - плановая дата поставки
 * @return 1 - срочная, 0 - нет
 */
    function DealIsTerm(DealID IN NUMBER,
                        FIID   IN NUMBER,
                        ValueDate IN DATE --фактическая, а в её отсутствии плановая дата поставки
                       ) return NUMBER;

/**
 * Расчет рыночных параметров по лоту с заполнением структуры R_MarketPrice.
 * @since 6.20.029
 * @qtest NO
 * @param LotID ID лота
 * @param LotBuyID ID лота покупки
 * @param IsFutures Признак - Срочная сделка
 * @param Is600Reg  Признак - Выполняется для 600 регистра
 */
    procedure CalcMarketPrice( LotID IN NUMBER, LotBuyID IN NUMBER, IsFutures IN NUMBER, Is600Reg IN NUMBER DEFAULT 0 );

/**
 * Расчет рыночных параметров по лоту с заполнением структуры R_MarketPrice_NEW.
 * @since 6.20.031
 * @qtest NO
 * @param LotID ID лота
 */
    procedure CalcMarketPrice_NEW( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') );

/**
 * Возвращает рыночную цену MarketPrice.MarketPrice
 * @since 6.20.029
 * @qtest NO
 * @param LotID ID лота
 * @param LotBuyID ID лота покупки
 * @param IsFutures Признак - Срочная сделка
 * @param Is600Reg  Признак - Выполняется для 600 регистра
 * @return Рыночная цена
 */
    function GetMarketPrice( LotID IN NUMBER, LotBuyID IN NUMBER, IsFutures IN NUMBER DEFAULT 0, Is600Reg IN NUMBER DEFAULT 0 ) return NUMBER;
/**
 * Возвращает источник курса MarketPrice.Market
 * @since 6.20.029
 * @qtest NO
 * @param LotID ID лота
 * @param LotBuyID ID лота покупки
 * @param IsFutures Признак - Срочная сделка
 * @return Источник курса
 */
    function GetMarket( LotID IN NUMBER, LotBuyID IN NUMBER, IsFutures IN NUMBER DEFAULT 0 ) return VARCHAR2;
/**
 * Возвращает дату курса MarketPrice.DateMarket
 * @since 6.20.029
 * @qtest NO
 * @param LotID ID лота
 * @param LotBuyID ID лота покупки
 * @param IsFutures Признак - Срочная сделка
 * @return Дата курса
 */
    function GetDateMarket( LotID IN NUMBER, LotBuyID IN NUMBER, IsFutures IN NUMBER DEFAULT 0 ) return DATE;
/**
 * Возвращает дату вспомогат. курса MarketPrice.Date2
 * @since 6.20.029
 * @qtest NO
 * @param LotID ID лота
 * @param LotBuyID ID лота покупки
 * @param IsFutures Признак - Срочная сделка
 * @return Дата вспомогат. курса
 */
    function GetDate2( LotID IN NUMBER, LotBuyID IN NUMBER, IsFutures IN NUMBER DEFAULT 0 ) return DATE;
/**
 * Возвращает признак обращающейся на ОРЦБ MarketPrice.IfMarket
 * @since 6.20.029
 * @qtest NO
 * @param LotID ID лота
 * @param LotBuyID ID лота покупки
 * @param IsFutures Признак - Срочная сделка
 * @return 'X' - обращается, иначе - нет
 */
    function IfMarket( LotID IN NUMBER, LotBuyID IN NUMBER, IsFutures IN NUMBER DEFAULT 0 ) return VARCHAR2;
/**
 * Возвращает рыночную цену MarketPrice_NEW.MarketPrice_NEW
 * @since 6.20.031
 * @qtest NO
 * @param LotID ID лота
 * @param RepDate Отчётная дата
 * @return Рыночная цена
 */
    function GetMarketPrice_NEW( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) return NUMBER;
/**
 * Возвращает значение курса MarketPrice_NEW.QuoteValue_New
 * @since 6.20.031
 * @qtest NO
 * @param LotID ID лота
 * @param RepDate Отчётная дата
 * @return Значение курса
 */
    function GetQuoteValue_New( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) return NUMBER;
/**
 * Возвращает источник курса MarketPrice_NEW.Market_NEW
 * @since 6.20.031
 * @qtest NO
 * @param LotID ID лота
 * @param RepDate Отчётная дата
 * @return Источник курса
 */
    function GetMarket_NEW( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) return VARCHAR2;
/**
 * Возвращает дату курса MarketPrice_NEW.DateMarket_NEW
 * @since 6.20.031
 * @qtest NO
 * @param LotID ID лота
 * @param RepDate Отчётная дата
 * @return Дата курса
 */
    function GetDateMarket_NEW( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) return DATE;
/**
 * Возвращает валюту курса MarketPrice_NEW.Val_Market_NEW
 * @since 6.20.031
 * @qtest NO
 * @param LotID ID лота
 * @param RepDate Отчётная дата
 * @return Валюта курса
 */
    function GetVal_Market_NEW( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) return VARCHAR2;
/**
 * Возвращает признак обращающейся на ОРЦБ MarketPrice.IfMarket_NEW
 * @since 6.20.031
 * @qtest NO
 * @param LotID ID лота
 * @param RepDate Отчётная дата
 * @return 'X' - обращается, иначе - нет
 */
    function IfMarket_NEW( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) return VARCHAR2;
/**
 * Возвращает значение курса MarketPrice_NEW.QuoteValueRep
 * @since 6.20.031
 * @qtest NO
 * @param LotID ID лота
 * @param RepDate Отчётная дата
 * @return Значение курса
 */
    function GetQuoteValueRep( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) return NUMBER;
/**
 * Возвращает источник курса MarketPrice_NEW.MarketRep
 * @since 6.20.031
 * @qtest NO
 * @param LotID ID лота
 * @param RepDate Отчётная дата
 * @return Источник курса
 */
    function GetMarketRep( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) return VARCHAR2;
/**
 * Возвращает дату курса MarketPrice_NEW.DateMarketRep
 * @since 6.20.031
 * @qtest NO
 * @param LotID ID лота
 * @param RepDate Отчётная дата
 * @return Дата курса
 */
    function GetDateMarketRep( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) return DATE;
/**
 * Возвращает валюту курса MarketPrice_NEW.Val_MarketRep
 * @since 6.20.031
 * @qtest NO
 * @param LotID ID лота
 * @param RepDate Отчётная дата
 * @return Валюта курса
 */
    function GetVal_MarketRep( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) return VARCHAR2;

    function GetErrorMsg return VARCHAR2;

/**
 * Функция получает курс типа для ценной бумаги на дату ratedate или за период
 * от ratedate-Ndays до ratedate с максимальной датой начала дествия курса
 * @since 6.20.029
 * @qtest NO
 * @param FIID ID ц/б
 * @param ToFIID В какой валюте вернуть значение курса
 * @param RateType Вид курса
 * @param RateDate Дата курса
 * @param NDays Количество дней
 * @param RD Выходной параметр - паспорт курса
 * @param pMarketCountry Страна биржи, на которой ищем курс
 * @param pOnlyRate Признак - в % от номинала
 * @param pIsMinMax Признак поиска минимального (1) или максимального (2) значения курса. Если не задан (0 или null) - игнорируется.
 * @param pCanUseCross Признак - Использовать кросскурс?
 * @param pMarket_Place Торговая площадка
 * @param pIsForeignMarket
 * @return 0 - в случае успеха, 1 - иначе
 */
    function SPGetRate( FIID IN NUMBER, ToFIID IN NUMBER, RateType IN NUMBER, RateDate IN DATE, NDays IN NUMBER, RD OUT DRATEDEF_DBT%ROWTYPE, pMarketCountry IN VARCHAR2 DEFAULT CHR(1), pOnlyRate IN NUMBER DEFAULT 0, pIsMinMax IN NUMBER DEFAULT 0, pCanUseCross IN NUMBER DEFAULT 0, pMarket_Place IN NUMBER DEFAULT -1, pIsForeignMarket IN NUMBER DEFAULT 0 ) return NUMBER;

/**
 * Функция проверяет есть ли курс на дату ratedate или за период
 * от ratedate-Ndays до ratedate с максимальной датой начала дествия курса
 * @since 6.20.031
 * @qtest NO
 * @param FIID ID ц/б
 * @param ToFIID В какой валюте вернуть значение курса
 * @param RateType Вид курса
 * @param RateDate Дата курса
 * @param NDays Количество дней
 * @param pMarketCountry Страна биржи, на которой ищем курс
 * @param pOnlyRate Признак - в % от номинала
 * @param pIsMinMax Признак поиска минимального (1) или максимального (2) значения курса. Если не задан (0 или null) - игнорируется.
 * @param pCanUseCross Признак - Использовать кросскурс?
 * @param pMarket_Place Торговая площадка
 * @param pIsForeignMarket
 * @return 0 - в случае успеха, 1 - иначе
 */
    function IsSPGetRate( FIID IN NUMBER, ToFIID IN NUMBER, RateType IN NUMBER, RateDate IN DATE, NDays IN NUMBER, pMarketCountry IN VARCHAR2 DEFAULT CHR(1), pOnlyRate IN NUMBER DEFAULT 0, pIsMinMax IN NUMBER DEFAULT 0, pCanUseCross IN NUMBER DEFAULT 0, pMarket_Place IN NUMBER DEFAULT -1, pIsForeignMarket IN NUMBER DEFAULT 0 ) return NUMBER;

/**
 * Функция получает курс типа для ценной бумаги ближайшую к дате ratedate
 * @since 6.20.029
 * @qtest NO
 * @param FIID ID ц/б
 * @param ToFIID В какой валюте вернуть значение курса
 * @param RateType Вид курса
 * @param RateDate Дата курса
 * @param MaxRateDate Максимальная дата курса
 * @param NDays Количество дней
 * @param RD Выходной параметр - паспорт курса
 * @param pMarketCountry Страна биржи, на которой ищем курс
 * @param pOnlyRate Признак - в % от номинала
 * @param pIsMinMax Признак поиска минимального (1) или максимального (2) значения курса. Если не задан (0 или null) - игнорируется.
 * @param pCanUseCross Признак - Использовать кросскурс?
 * @return 0 - в случае успеха, 1 - иначе
 */
    function SPGetRate_Ex( FIID IN NUMBER, ToFIID IN NUMBER, RateType IN NUMBER, RateDate IN DATE, MaxRateDate IN DATE, NDays IN NUMBER, RD OUT DRATEDEF_DBT%ROWTYPE, pOnlyRate IN NUMBER DEFAULT 0, pIsMinMax IN NUMBER DEFAULT 0, pCanUseCross IN NUMBER DEFAULT 0 ) return NUMBER;

/**
 * Вывод сообщений об ошибке в протокол
 * @since 6.20.029
 * @qtest NO
 * @param in_LotID ID лота
 * @param in_FIID ID ц/б
 * @param in_ErrorType Тип ошибки
 * @param in_ErrorStr Текст ошибки
 * @param in_IsTrigger Признак - ошибка выполнения триггера
 */
    PROCEDURE TXPutMsg( in_LotID IN NUMBER,
                        in_FIID IN NUMBER,
                        in_ErrorType IN NUMBER,
                        in_ErrorStr IN VARCHAR2,
                        in_IsTrigger IN BOOLEAN DEFAULT FALSE );

/**
 * Получить тип лота в зависимости от операции
 * @since 6.20.029
 * @qtest NO
 * @param oGrp Группа сделки
 * @param DealID ID сделки
 * @param DealPart Часть сделки
 * @return Тип лота
 */
    FUNCTION get_lotType( oGrp IN NUMBER, DealID IN NUMBER, DealPart IN NUMBER DEFAULT 1 ) RETURN NUMBER DETERMINISTIC;
/**
 * получить название лота в зависимости от типа
 * @since 6.20.029
 * @qtest NO
 * @param in_Type Тип лота
 * @return Наменование
 */
    FUNCTION get_lotName( in_Type IN NUMBER ) RETURN VARCHAR2 DETERMINISTIC;
/**
 * получить дату покупки лота в зависимости от операции
 * @since 6.20.029
 * @qtest NO
 * @param oGrp Группа сделки
 * @param DealID ID сделки
 * @param FactDate Дата покупки
 * @param DealPart Часть сделки
 * @return Дата покупки
 */
    FUNCTION get_lotBuyDate( oGrp IN NUMBER, DealID IN NUMBER, FactDate IN DATE, DealPart IN NUMBER DEFAULT 1 )
      RETURN DATE DETERMINISTIC;
/**
 * получить дату продажи лота в зависимости от операции
 * @since 6.20.029
 * @qtest NO
 * @param oGrp Группа сделки
 * @param DealID ID сделки
 * @param FactDate Дата продажи
 * @param DealPart Часть сделки
 * @return Дата продажи
 */
    FUNCTION get_lotSaleDate( oGrp IN NUMBER, DealID IN NUMBER, FactDate IN DATE, DealPart IN NUMBER DEFAULT 1 )
      RETURN DATE DETERMINISTIC;
/**
 * получить код лота по параметрам p_Code и p_Num
 * @since 6.20.029
 * @qtest NO
 * @param p_Code Код
 * @param p_Num Номер
 * @return Код лота
 */
    FUNCTION get_lotCode( p_Code IN VARCHAR2, p_Num IN NUMBER ) RETURN VARCHAR2;
/**
 * проверить наличие примечание определенного вида
 * @since 6.20.029
 * @qtest NO
 * @param in_NoteKind Вид примечания
 * @param in_ObjType Тип объекта
 * @param in_DocID ID объекта
 * @return 1 - примечание есть, 0 - иначе
 */
    FUNCTION CheckExistsNote( in_NoteKind IN dnotekind_dbt.t_NoteKind%TYPE,
                              in_ObjType IN dnotetext_dbt.t_ObjectType%TYPE,
                              in_DocID IN NUMBER ) RETURN NUMBER;

/**
 * Задает приоритет выбытий 2 частей  в зависимости от типа сделки.
 * @since 6.20.029
 * @qtest NO
 * @param v_Type Тип сделки
 * @return Номер приоритета
 */
    FUNCTION TXGetPart2Order( v_Type IN NUMBER ) RETURN NUMBER DETERMINISTIC;
/**
 * Задает приоритет выбытий 1 ч. Репо/Займа в зависимости от типа сделки.
 * @since 6.20.029
 * @qtest NO
 * @param v_Type Тип сделки
 * @return Номер приоритета
 */
    FUNCTION TXGetSaleOrder( v_Type IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Задает приоритет выбытий в зависимости от типа сделки для лотов покупки.
 * @since 6.20.029
 * @qtest NO
 * @param v_buyType Тип сделки покупки
 * @param v_saleType Тип сделки продажи
 * @return Номер приоритета
 */
    FUNCTION TXGetBuyOrder( v_buyType IN NUMBER, v_saleType IN NUMBER ) RETURN NUMBER DETERMINISTIC;
/**
 * Задает приоритет связей при формировании подстановок для подбора приобретений при обработке связей при подстановке.
 * @since 6.20.029
 * @qtest NO
 * @param v_Type Тип сделки
 * @return Номер приоритета
 */
    FUNCTION TXGetSubstOrderBuy( v_Type IN NUMBER ) RETURN NUMBER DETERMINISTIC;
/**
 * Задает приоритет связей при закрытии позиции для подбора сделок приобретений
 * @since 6.20.029
 * @qtest NO
 * @param v_buyType Тип сделки покупки
 * @return Номер приоритета
 */
    FUNCTION TXGetClPosOrderRepo( v_buyType IN NUMBER ) RETURN NUMBER DETERMINISTIC;
/**
 * Вычисляет значение признака наличия свободного остатка в зависимости от параметров лота.
 * @since 6.20.029
 * @qtest NO
 * @param v_AMOUNT Количество в сделке
 * @param v_NETTING Количество в  неттинге (в реальности не используется)
 * @param v_SALE Списанная сумма в приходах
 * @param v_RETFLAG Признак возврата из 2 части
 * @param v_INACC Признак участия лота в учете
 * @param v_BLOCKED Признак блокировки сделок
 * @return Значение признака наличия свободного остатка
 */
    FUNCTION TXGetIsFree( v_AMOUNT IN NUMBER, v_NETTING IN NUMBER, v_SALE IN NUMBER,
                          v_RETFLAG IN CHAR, v_INACC IN CHAR, v_BLOCKED IN CHAR ) RETURN CHAR DETERMINISTIC;
/**
 * Вычисляет перенастройку параметров лотов при изменении настроек.
 * Вызывается после изменения положения настроек или при апгрейде.
 * Аргументы равны "Да" (Да = 1, нет = 0), если требуется пересчитать значения соотв. полей, по умолчанию  - "Да"
 * @since 6.20.029
 * @qtest NO
 * @param v_OrdForSale Сортировка для продаж
 * @param v_OrdForRepo Сортировка для Репо
 * @param v_OrdForSubst Сортировка для подстановок
 * @param v_OrdForClPosRepo Сортировка для коротких позиций по Репо
 * @param v_IsFree Признак наличия свободного количества
 */
    PROCEDURE TXRetuningLots( v_OrdForSale IN NUMBER, v_OrdForRepo IN NUMBER,
                              v_OrdForSubst IN NUMBER, v_OrdForClPosRepo IN NUMBER, v_IsFree IN NUMBER);
/**
 * Получить кол-во виртуальных лотов по его номеру
 * @since 6.20.029
 * @qtest NO
 * @param in_Number Номер
 * @return Кол-во виртуальных лотов
 */
    FUNCTION TXGetVirtCountByNum( in_Number IN VARCHAR2 ) RETURN NUMBER DETERMINISTIC;
/**
 * Выполняет пересортировку сделок за дату DealDate и время DealTime, начиная с лота BegLotID
 */
    PROCEDURE RSI_TXDealSortOnDate (FIID IN NUMBER, DealDate IN DATE, DealTime IN DATE, BegLotID IN NUMBER);
/**
 * Выполняет пересортировку всех непронумерованных сделок
 */
    PROCEDURE RSI_TXDealSortAll;
/**
 * Выполняет возврат суммарного количества из всех внутридневных Репо за дату D при недостатке ц/б на прочих лотах.
 */
    PROCEDURE RSI_TXReturnAmountFromRepo (FIID IN NUMBER, D IN DATE, SaleID IN NUMBER);

/**
 * Функции списаний */
/**
 * Выполняет списание лота продажи SALELOT
 */
    PROCEDURE RSI_TXLinkSale( v_SaleLot IN dsctxlot_dbt%ROWTYPE );
/**
 * Выполняет списание лота прямого репо/размещения займа
 */
    PROCEDURE RSI_TXLinkDirectRepo( v_SaleLot_ IN dsctxlot_dbt%ROWTYPE );
/**
 * Выполняет списание лота продажи SALELOT из сделок покупки
 */
    PROCEDURE RSI_TXLinkSaleToBuy( v_SaleLot IN dsctxlot_dbt%ROWTYPE, S IN OUT NUMBER, Portfolio IN NUMBER DEFAULT -1, Except_Portfolio IN NUMBER DEFAULT 0 );
/**
 * Выполняет списание лота продажи SALELOT из сделок обратного Репо и привлечения займа, закрытых не в день продажи
 */
    PROCEDURE RSI_TXLinkSaleToReverseRepo( v_SaleLot IN dsctxlot_dbt%ROWTYPE, S IN OUT NUMBER );
/**
 * Выполняет списание лота выбытия прямого Репо/размещения займа SALELOT
 * из сделок покупок, обратного Репо и привлечения займа, закрытых не в день продажи
 */
    PROCEDURE RSI_TXLinkDirectRepoToBuy( v_SaleLot IN dsctxlot_dbt%ROWTYPE, S IN OUT NUMBER, Portfolio IN NUMBER DEFAULT -1 );
/**
 * Выполняет списание лота Прямого Репо или Займа Размещения однодневного SALELOT с лотов покупок
 */
    PROCEDURE RSI_TXLinkOneDayRepoToBuy( v_SaleLot IN dsctxlot_dbt%ROWTYPE, S IN OUT NUMBER, Portfolio IN NUMBER DEFAULT -1 );
/**
 * Выполняет связывание по 2 ч. лота Репо обратного/Займу размещения. PART2LOT
 */
    PROCEDURE RSI_TXLinkPart2ToBuy( v_Part2Lot  IN dsctxlot_dbt%ROWTYPE );
/**
 * Вставляет или корректирует запись остатка и списания
 */
    PROCEDURE RSI_TXCorrectRest (p_Type    IN NUMBER, p_SaleID IN NUMBER, p_SourceID IN NUMBER,
                                 p_BuyDate IN DATE,   p_SaleDate IN DATE, p_CorrectDate IN DATE,
                                 p_FIID    IN NUMBER, p_Amount IN NUMBER, in_IsTrigger IN BOOLEAN DEFAULT FALSE);
/**
 * Выполняет обработку остатков и списаний при выбытии 2 ч ОР/ПЗ
 */
    PROCEDURE RSI_TXUpdatePart2Rest (p_SourceID IN NUMBER, p_SaleDate IN DATE);
/**
 * Выполняет списание остатка при создании связи
 */
    PROCEDURE RSI_TXLinkRest (p_NewTypeBU IN NUMBER, p_NewTypeDRU IN NUMBER, p_SourceID IN NUMBER,
                              p_LinkDate  IN DATE,   p_FIID IN NUMBER,       p_Amount IN NUMBER, in_IsTrigger IN BOOLEAN DEFAULT FALSE);
/**
 * Выполняет обработку остатков и списаний при создании/обновлении связи
 */
    PROCEDURE RSI_TXUpdateRestByLink (p_LinkType IN NUMBER, p_BuyID IN NUMBER, p_SaleID IN NUMBER, p_SourceID IN NUMBER,
                                      p_Lot1ID IN NUMBER, p_LinkDate IN DATE, p_FIID IN NUMBER, p_Amount IN NUMBER, in_IsTrigger IN BOOLEAN DEFAULT FALSE);
/**
 * Закрытие налогового периода
 * @since 6.20.029
 * @qtest NO
 * @param v_CloseDate_in Дата закрытия НП
 */
    PROCEDURE TXClosePeriod( v_CloseDate_in IN DATE );
/**
 * Заполнение таблицы налоговых лотов
 */
    PROCEDURE RSI_TXInsertLots( v_BegDate_in IN DATE, v_EndDate_in IN DATE, v_TaxGroup_in NUMBER, v_FIID_in NUMBER );
/**
 * Заполняет таблицы налогового учета
 * @since 6.20.029
 * @qtest NO
 * @param v_BegDate_in Дата начала
 * @param v_EndDate_in Дата окончания
 * @param v_TaxGroup_in Группа налогового учета
 * @param v_FIID_in ID ц/б
 * @param v_IsDebug_in Признак работы в отладочном режиме
 * @param v_IsOptim_in Отладочный режим для оптимизации
 * @param v_IsRecalc_in Признак расчета/пересчета
 */
    PROCEDURE TXCreateLots( v_BegDate_in IN DATE, v_EndDate_in IN DATE, v_TaxGroup_in NUMBER, v_FIID_in NUMBER, v_IsDebug_in NUMBER DEFAULT 0, v_IsOptim_in NUMBER DEFAULT 0, v_IsRecalc_in NUMBER DEFAULT 0, v_Parallel_in NUMBER DEFAULT 0 );
/**
 * Проверка правильности связывания
 * @since 6.20.029
 * @qtest NO
 * @param v_BegDate_in Дата начала
 * @param v_EndDate_in Дата окончания
 * @param v_TaxGroup_in Группа налогового учета
 * @param v_FIID_in ID ц/б
 */
    PROCEDURE TXTestLots( v_BegDate_in IN DATE, v_EndDate_in IN DATE, v_TaxGroup_in NUMBER, v_FIID_in NUMBER );

/**
 * Признак процентной бумаги. Используется в регистрах.
 * @since 6.20.029
 * @qtest NO
 * @param v_TaxGroup ГНУ ц/б
 * @return 1 - является процентной, 0 - иначе
 */
    FUNCTION TXIsPercentAvoir( v_TaxGroup IN NUMBER ) RETURN NUMBER;
/**
 * Признак дисконтной бумаги. Используется в регистрах.
 * @since 6.20.029
 * @qtest NO
 * @param v_TaxGroup ГНУ ц/б
 * @return 1 - является дисконтной, 0 - иначе
 */
    FUNCTION TXIsDiscountAvoir( v_TaxGroup IN NUMBER ) RETURN NUMBER;
/**
 * Вернуть вид облигации.
 * @since 6.20.029
 * @qtest NO
 * @param v_TaxGroup ГНУ ц/б
 * @return 0 - льготная, 1 - обычная, -1 - иначе.
 */
    FUNCTION TXGetBondKind( v_TaxGroup IN NUMBER ) RETURN NUMBER;
/**
 * Сумма комиссий по сделке. Используется в регистрах НУ.
 * @since 6.20.029
 * @qtest NO
 * @param v_DealID ID операции
 * @param v_BOfficeKind Вид операции
 * @param v_CalcDate Дата
 * @param v_ToFIID Валюта, в которой требуется вернуть значение
 * @return Сумма комииссий по сделке
 */
    FUNCTION TXGetComissionsSum( v_DealID IN NUMBER,
                                 v_BOfficeKind IN NUMBER,
                                 v_CalcDate IN DATE,
                                 v_ToFIID IN NUMBER
                               ) RETURN NUMBER;

/**
 * Сумма НДС комиссий по сделке. Используется в регистрах НУ.
 * @since 6.20.029
 * @qtest NO
 * @param v_DealID ID операции
 * @param v_BOfficeKind Вид операции
 * @param v_CalcDate Дата
 * @param v_ToFIID Валюта, в которой требуется вернуть значение
 * @return Сумма комииссий по сделке
 */
    FUNCTION TXGetNDSComSum( v_DealID IN NUMBER,
                                 v_BOfficeKind IN NUMBER,
                                 v_CalcDate IN DATE,
                                 v_ToFIID IN NUMBER
                               ) RETURN NUMBER;

/**
 * Сумма комиссий по сделке за период. Используется в регистрах НУ.
 * @since 6.20.029
 * @qtest NO
 * @param v_DealID ID операции
 * @param v_BOfficeKind Вид операции
 * @param v_CalcDate Дата
 * @param v_ToFIID Валюта, в которой требуется вернуть значение
 * @param v_BegDate_in Дата начала
 * @param v_EndDate_in Дата окончания
 * @return Сумма комиссий по сделке за период
*/
    FUNCTION TXGetComissionsSumInPeriod( v_DealID IN NUMBER,
                                         v_BOfficeKind IN NUMBER,
                                         v_CalcDate IN DATE,
                                         v_ToFIID IN NUMBER,
                                         v_BegDate IN DATE,
                                         v_EndDate IN DATE,
                                         v_IfDD1 IN NUMBER DEFAULT 0,
                                         v_IfDD2 IN NUMBER DEFAULT 0
                                       ) RETURN NUMBER;

/**
 *Определение НДС ставки на дату
 * @since 6.20.31
 * @qtest NO
 * @param NDSRateID ID ставки
 * @param vDate    НДС на дату
 * @param NDSRate Возвращаемое значение НДС
 * @return 'X' - true, '' - false
*/
    FUNCTION TXGetNDSRateByDate(NDSRateID in NUMBER,
                                vDate in DATE,
                                NDSRate IN OUT NUMBER )
                                RETURN CHAR;

/**
 * Сумма НДС комиссий по сделке за период. Используется в регистрах НУ.
 * @since 6.20.029
 * @qtest NO
 * @param v_DealID ID операции
 * @param v_BOfficeKind Вид операции
 * @param v_CalcDate Дата
 * @param v_ToFIID Валюта, в которой требуется вернуть значение
 * @param v_BegDate_in Дата начала
 * @param v_EndDate_in Дата окончания
 * @return Сумма комиссий по сделке за период
*/
    FUNCTION TXGetNDSInPeriod          ( v_DealID IN NUMBER,
                                         v_BOfficeKind IN NUMBER,
                                         v_CalcDate IN DATE,
                                         v_ToFIID IN NUMBER,
                                         v_BegDate IN DATE,
                                         v_EndDate IN DATE,
                                         v_IfDD1 IN NUMBER DEFAULT 0,
                                         v_IfDD2 IN NUMBER DEFAULT 0
                                       ) RETURN NUMBER;

/**
 * Количество купонов по ц/б в периоде дат
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID ID ц/б
 * @param v_BegDate_in Дата начала
 * @param v_EndDate_in Дата окончания
 * @param v_ExcludeDate Дата погашения купона, который будет исключен из подсчета. Если не задана - игнорируется.
 * @return Количество купонов по ц/б
 */
    FUNCTION TXGetCountCoupon(v_FIID IN NUMBER,
                              v_BegDate IN DATE,
                              v_EndDate IN DATE,
                              v_ExcludeDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY')
                             ) RETURN NUMBER;
/**
 * Дата последней выплаты последнего купона в периоде дат.
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID ID ц/б
 * @param v_BegDate_in Дата начала
 * @param v_EndDate_in Дата окончания
 * @param v_ExcludeDate Дата погашения купона, который будет исключен из подсчета. Если не задана - игнорируется.
 * @param v_CoupRetData использовать данные из операции погашения купона
 * @return Дата последней выплаты последнего купона в периоде дат
 */
    FUNCTION TXGetMaxCouponDrawingDate(v_FIID IN NUMBER,
                                       v_BegDate IN DATE,
                                       v_EndDate IN DATE,
                                       v_ExcludeDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                       v_CoupRetData IN CHAR DEFAULT CHR(0)
                                      )  RETURN DATE;

/**
 * Получить количество ц/б, перевешенное на другие сделки-источники связями ППР на дату
 * @since 6.20.029
 * @qtest NO
 * @param LnkID ID связи
 * @param OnDate Дата
 * @return Количество ц/б
 */
    FUNCTION TXGetSumSCTXLSOnDate(LnkID IN NUMBER, OnDate IN DATE) RETURN NUMBER;

/**
 * Выполняет списание лотов в ГО и ИН
 */
    PROCEDURE RSI_TXProcessGO(p_GO IN DSCTXGO_DBT%ROWTYPE);
/**
 * Выполняет зачисление лотов в ИН
 */
    PROCEDURE RSI_TXProcessGON(p_GO IN DSCTXGO_DBT%ROWTYPE, p_SaleLot IN DSCTXLOT_DBT%ROWTYPE);
/**
 * Выполняет зачисление лотов в ГО
 */
    PROCEDURE RSI_TXProcessGOFI(p_GO IN DSCTXGO_DBT%ROWTYPE, p_TAXGROUP IN NUMBER, p_FIID IN NUMBER, p_BegDate IN DATE);

/**
 * Получить ценовые условия
 * @since 6.20.029
 * @qtest NO
 * @param v_DealID ID операции
 * @param p_Leg Выходной параметр - ценовые условия сделки
 * @return 0 - в случае успеха, 1 - иначе
 */
    FUNCTION GetLeg(p_DealID IN NUMBER, p_Leg OUT DDL_LEG_DBT%ROWTYPE) RETURN NUMBER;
/**
 * Цена в ВЦ
 * @since 6.20.029
 * @qtest NO
 * @param p_LotID ID лота
 * @return Цена в валюте цены
 */
    FUNCTION TXGetLotPrice(p_LotID IN NUMBER) RETURN NUMBER;
/**
 * Стоимость без НКД в ВЦ
 * @since 6.20.029
 * @qtest NO
 * @param p_LotID ID лота
 * @return Стоимость без НКД в ВЦ
 */
    FUNCTION TXGetLotCost(p_LotID IN NUMBER) RETURN NUMBER;
/**
 * НКД в ВН
 * @since 6.20.029
 * @qtest NO
 * @param p_LotID ID лота
 * @return НКД в валюте номинала
 */
    FUNCTION TXGetLotNKD(p_LotID IN NUMBER) RETURN NUMBER;
/**
 * Стоимость с НКД  в ВЦ
 * @since 6.20.029
 * @qtest NO
 * @param p_LotID ID лота
 * @return Стоимость с НКД  в ВЦ
 */
    FUNCTION TXGetLotTotalCost(p_LotID IN NUMBER) RETURN NUMBER;

/**
 * Вывод сообщения о начале операции вида NameCalc
 */
    PROCEDURE RSI_BeginCalculate( NameCalc IN VARCHAR2 );
/**
 * Удаление записи из DSCTXMES_DBT при завершении текущей операции
 */
    PROCEDURE EndCalculate;

/**
 * Получить полную стоимость по сделке на дату
 * @since 6.20.029
 * @qtest NO
 * @param v_DealID ID операции
 * @param v_BOfficeKind Вид операции
 * @param p_ChangeDate Дата изменений
 * @param p_Instance Состояние лота
 * @param p_Price Цена по сделке
 * @param p_Cost Стоимость по сделке
 * @param p_TotalCost Полная стоимость по сделке
 * @param p_NKD НКД по сделке
 * @param p_DealPart Часть сделки
 * @param v_CalcDate Дата расчета
 * @return Полная стоимость по сделке на дату
 */
    function GetDealTotalCostOnDate( p_DealID      IN NUMBER,
                                     p_BOfficeKind IN NUMBER,
                                     p_ChangeDate  IN DATE,
                                     p_Instance    IN NUMBER,
                                     p_Price       IN NUMBER,
                                     p_Cost        IN NUMBER,
                                     p_TotalCost   IN NUMBER,
                                     p_NKD         IN NUMBER,
                                     p_DealPart    IN NUMBER,
                                     p_CalcDate    IN DATE ) return NUMBER;

/**
 * установлен ли на объекте атрибут
 * @since 6.20.029
 * @qtest NO
 * @param objtype Тип объекта
 * @param groupID Номер категории
 * @param numInList Номер атрибута
 * @param objID ID объекта
 * @param dat Дата
 * @return 1 - атрибут установлен, 0 - иначе
 */
    function CheckObjAttrPresenceByNum( objtype    IN NUMBER,
                                        groupID    IN NUMBER,
                                        numInList  IN VARCHAR2,
                                        objID      IN VARCHAR2,
                                        dat        IN DATE
                                      ) RETURN NUMBER;

/**
 * Расчет рыночных параметров по ц\б с заполнением структуры R_MarketPrice_REZ
 * @since 6.20.031
 * @qtest NO
 * @param FIID ID ФИ
 * @param OnDate Дата расчета резерва
 */
    procedure CalcMarketPrice_REZ( FIID IN NUMBER, OnDate IN DATE );

/**
 * Возвращает рыночную цену MarketPrice_REZ.RezMrkt
 * @since 6.20.031
 * @qtest NO
 * @param FIID ID ФИ
 * @param OnDate Дата расчета резерва
 * @return Рыночная цена
 */
    function GetRezMrkt( FIID IN NUMBER, OnDate IN DATE ) return NUMBER;

/**
 * Возвращает признак обращающейся ц\б при расчете резерва для целей АРНУ MarketPrice_REZ.IfMarketRez
 * @since 6.20.031
 * @qtest NO
 * @param FIID ID ФИ
 * @param OnDate Дата расчета резерва
 * @return 'X' - обращается, иначе - нет
 */
    function IfMarketRez( FIID IN NUMBER, OnDate IN DATE ) return VARCHAR2;

/**
 * Возвращает источник курса MarketPrice_REZ.MrktRez
 * @since 6.20.031
 * @qtest NO
 * @param FIID ID ФИ
 * @param OnDate Дата расчета резерва
 * @return Источник курса
 */
    function GetMrktRez( FIID IN NUMBER, OnDate IN DATE ) return VARCHAR2;

/**
 * Возвращает дату курса MarketPrice_REZ.DMrktRez
 * @since 6.20.031
 * @qtest NO
 * @param FIID ID ФИ
 * @param OnDate Дата расчета резерва
 * @return Дата курса
 */
    function GetDMrktRez( FIID IN NUMBER, OnDate IN DATE ) return DATE;

/**
 * Возвращает текст ошибки, возникшей при поиске курса MarketPrice_REZ.RezMrkt
 * @since 6.20.031
 * @qtest NO
 * @return текст ошибки
 */
    function GetErrorMsgRez return VARCHAR2;

/**
 * Выполняет получение кода лота НУ
 * @since 6.20.031
 * @qtest NO
 * @param p_DealID     ID сделки
 * @param p_FIID       ID ФИ
 * @param p_DealCode   Код сделки
 * @param p_IsBasket   Признак корзины: != 0 - сделка РЕПО на корзину, 0 - не РЕПО на корзину
 * @param p_IsBuy      Вид покупки: 1 - покупка, 0 - иначе
 * @param p_Mode       Вид лота: 1 - обычные лоты, 2 - комп. поставка, уменьшение, 3 - комп. поставка, увеличение
 * @return строку - код лота НУ
 */
    function TXGetLotCode( p_DealID   IN NUMBER,
                           p_FIID     IN NUMBER,
                           p_DealCode IN VARCHAR2,
                           p_IsBasket IN NUMBER,
                           p_IsBuy    IN NUMBER,
                           p_Mode     IN NUMBER
                         ) return VARCHAR2;

    /**
     @brief Получить дату вывода из ДУ
     @param[in] p_DealId Номер сделки
     @return Дата вывода из ДУ
    */
    function GetInAvrWrtStartDate(p_DealId IN NUMBER, p_Date IN DATE DEFAULT TO_DATE('31-12-9999','DD-MM-YYYY')) return Date DETERMINISTIC;

    PROCEDURE RSI_TXDealSortByRangeFIID(pFIID_beg IN NUMBER, pFIID_end IN NUMBER);

    PROCEDURE CalcAV( v_Date IN DATE, v_FIID_beg IN NUMBER, v_FIID_end IN NUMBER, v_TaxGroup_in IN NUMBER);

END Rsb_SCTX;
/
