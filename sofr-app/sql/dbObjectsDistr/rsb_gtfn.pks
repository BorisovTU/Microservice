-- Author  : Dubovoy Andrey
-- Created : 02.04.2022
-- Purpose : Общие функции для импорта данных шлюза

CREATE OR REPLACE PACKAGE RSB_GTFN
IS
  SET_CHAR   CONSTANT CHAR(1) := 'X';
  UNSET_CHAR CONSTANT CHAR(1) := CHR(1); --поскольку в шлюзе для хранения признака используется строковое поле VARCHAR2

  DERIVATIVE_FUTURES CONSTANT NUMBER(5)  := 1; -- Фьючерс
  DERIVATIVE_OPTION  CONSTANT NUMBER(5)  := 2; -- Опцион
  DERIVATIVE_FORWARD CONSTANT NUMBER(5)  := 3; -- Форвард
  
  --FICK_CODETYPE. Виды кодов финансовых инструментов
  FICK_USERCODE CONSTANT NUMBER(5) := 1; -- fininstr.FI_Code
  
  -- Типы курсов (dratetype_dbt)
  RATETYPE_MIN_PRICE CONSTANT NUMBER(5)     := 2;  -- минимальная цена  
  RATETYPE_MAX_PRICE CONSTANT NUMBER(5)     := 3;  -- максимальная цена  
  RATETYPE_CALC_PRICE CONSTANT NUMBER(5)    := 6;  -- расчетная цена
  RATETYPE_MINSTEP_PRICE CONSTANT NUMBER(5) := 8;  -- стоимость минимального шага цены
  RATETYPE_CLOSE_PRICE CONSTANT NUMBER(5)   := 18; -- цена закрытия
  RATETYPE_TEORETIC_COST CONSTANT NUMBER(5) := 19; -- теоритическая цена
  RATETYPE_CALC_PRICE_G CONSTANT NUMBER(5)  := 34; -- расчетная цена для главы Г

  RG_PARTY          CONSTANT NUMBER(5) := 1;
  RG_CURRENCY       CONSTANT NUMBER(5) := 5;
  RG_AVOIRISS       CONSTANT NUMBER(5) := 6;
  RG_PFI            CONSTANT NUMBER(5) := 8;  --Производный инструмент
  RG_DEAL           CONSTANT NUMBER(5) := 21; --Сделка с ценными бумагами
  RG_REQ            CONSTANT NUMBER(5) := 25; --Заявка на сделку с ц/б
  RG_CLEARING       CONSTANT NUMBER(5) := 27; --Клиринг. Сделка с ценными бумагами
  RG_DLKSUWRT       CONSTANT NUMBER(5) := 30; --Списание/зачисление КСУ
  RG_RATE           CONSTANT NUMBER(5) := 50; --Курс финансового инструмента
  RG_ITOG           CONSTANT NUMBER(5) := 52; --Итоги валютных торгов
  RG_DVNDEAL        CONSTANT NUMBER(5) := 53; --Внебиржевая сделка с ПИ 
  RG_DVFXDEAL       CONSTANT NUMBER(5) := 54; --Конверсионная операция в ПИ 
  RG_MONEYMOTION    CONSTANT NUMBER(5) := 56; --Движение ДС
  RG_DEFCOMM        CONSTANT NUMBER(5) := 65; --Удержанная комиссия
  RG_MARKETREPORT   CONSTANT NUMBER(5) := 80; --Отчет биржи
  RG_DVDEAL         CONSTANT NUMBER(5) := 86; --Сделка срочного рынка
  RG_DVTRN          CONSTANT NUMBER(5) := 87; --Итоги дня по позиции ПИ
  RG_DVCALCREP      CONSTANT NUMBER(5) := 88; --Отчёт по операции расчетов
  RG_DVNDEALCALC    CONSTANT NUMBER(5) := 93; --Расчеты по ПФИ для внебирж. сделки
  RG_MON            CONSTANT NUMBER(5) := 94; --Информация о договорах обслуживания
  RG_DVREQ          CONSTANT NUMBER(5) := 95; --Заявка на сделку с ПИ
  RG_DLITOGCLVR     CONSTANT NUMBER(5) := 96; --Итоговые нетто-требования и нетто-обязательства 
  RG_DVCOMMISVR     CONSTANT NUMBER(5) := 97; --Комиссионное вознаграждение 
  RG_MONEYMOTION_CM CONSTANT NUMBER(5) := 5001; --Движение ДС ВБ
  
  OPERKIND_AVRWRTOUT CONSTANT NUMBER(5) := 2010; --Списание ц/б
  OPERKIND_AVRWRTIN  CONSTANT NUMBER(5) := 2011; --Зачисление ц/б
  
  OPERKIND_DVOPER CONSTANT NUMBER(10) := 12600; --СО расчётов на срочном рынке

  OPERKIND_OPTIONBUY   CONSTANT NUMBER(10) := 12615; --Покупка опционов
  OPERKIND_OPTIONSELL  CONSTANT NUMBER(10) := 12625; --Продажа опционов
  OPERKIND_OPTIONEXEC  CONSTANT NUMBER(10) := 12640; --Исполнение опционов
  OPERKIND_OPTIONEXP   CONSTANT NUMBER(10) := 2645;  --Экспирация опционов
  OPERKIND_FUTURESBUY  CONSTANT NUMBER(10) := 12610; --Покупка фьючерсов
  OPERKIND_FUTURESSELL CONSTANT NUMBER(10) := 12620; --Продажа фьючерсов
  OPERKIND_FUTURESEXEC CONSTANT NUMBER(10) := 12630; --Исполнение фьючерсов
  
  GT_MMVB3        CONSTANT NUMBER(5) := 112; --ММВБ - xml-формат
  GT_ITOGVR       CONSTANT NUMBER(5) := 117; --MOEX_Итоги, xml-формат
  GT_VR           CONSTANT NUMBER(5) := 118; --ММВБ_Валютный рынок, xml-формат
  GT_CSV          CONSTANT NUMBER(5) := 119; --ММВБ_СР, csv-формат
  GT_ICVR         CONSTANT NUMBER(5) := 127; --MOEX_Итоги клиринга по валютному рынку, xml-формат
  GT_CVVR         CONSTANT NUMBER(5) := 129; --MOEX_Комиссионные вознаграждения по валютному рынку, xml-формат
  GT_VARM         CONSTANT NUMBER(5) := 130; --ММВБ_ВР_вариационная_маржа, xml-формат
  GT_SPB          CONSTANT NUMBER(5) := 131; --ПАО СПБ, xml-формат
  GT_CX99         CONSTANT NUMBER(5) := 1001;--SRC-99 Загрузка CCX99 валютная брокерка
  GT_PAYMENTS     CONSTANT NUMBER(5) := 133; --ММВБ, Шлюз PAYMENTS
  GT_PAYMENTS_SPB CONSTANT NUMBER(5) := 134; --ПАО СПБ, Шлюз PAYMENTS
  GT_PAYMENTS_VR  CONSTANT NUMBER(5) := 135; --ММВБ_ВР, Шлюз PAYMENTS
  GT_PAYMENTS_CSV CONSTANT NUMBER(5) := 136; --ММВБ_СР, Шлюз PAYMENTS
  
  PTSK_STOCKDL CONSTANT NUMBER := 1;  --Фондовый дилинг
  PTSK_VEKSACC CONSTANT NUMBER := 14; --Учтенные векселя
  PTSK_DV      CONSTANT NUMBER := 15; --Срочные контракты
  PTSK_CM      CONSTANT NUMBER := 21; --Валютный рынок

  --Направление сделки в заявке (ALG_DL_REQ_DIRECTION)
  DL_REQ_DIRECTION_B  CONSTANT NUMBER := 1; --Покупка
  DL_REQ_DIRECTION_S  CONSTANT NUMBER := 2; --Продажа
  DL_REQ_DIRECTION_BS CONSTANT NUMBER := 3; --Покупка-продажа 
  DL_REQ_DIRECTION_SB CONSTANT NUMBER := 4; --Продажа-покупка 
  DL_REQ_DIRECTION_PR CONSTANT NUMBER := 5; --РЕПО прямое
  DL_REQ_DIRECTION_OR CONSTANT NUMBER := 6; --РЕПО обратное

  FileSEM03 CONSTANT NUMBER(5) := 1;
  FileEQM06 CONSTANT NUMBER(5) := 2;
  FileASTS  CONSTANT NUMBER(5) := 5;
  FileEQM2T CONSTANT NUMBER(5) := 6;
  FileEQM3T CONSTANT NUMBER(5) := 7;
  
  FileSPB03  CONSTANT NUMBER(5) := 1;
  FileMFB06C CONSTANT NUMBER(5) := 3;
  
  MMVB_CODE CONSTANT VARCHAR2(4) := 'ММВБ';    --Код ММВБ
  SPB_CODE  CONSTANT VARCHAR2(7) := 'ПАО СПБ'; --Код СПБ

  DL_PREPARING CONSTANT NUMBER(5) := 0; --Сделка на этапе подготовки

  SF_FEE_TYPE_ONCE CONSTANT INTEGER := 6; -- Разовая комиссия

  TECH_BOARD_CODES CONSTANT VARCHAR(1000) := 'DADM,DADE,DADU,DADY,NADM,SADM,TADM,TRAD,TRAN,RFND';

  TYPE STRMAP_T IS TABLE OF VARCHAR2(256) INDEX BY VARCHAR2(128); --Ассоциативная коллекция по строковым данным

/**
 * Создание документа-подтверждения "Отчет биржи"
 * @since RSHB 82
 * @qtest NO
 * @param p_SeanceID Идентификаторв сеанса
 * @param p_SourceCode Код источника данных
 * @param p_OutMarketReportID Идентификатор объекта
 * @param p_ErrMsg Сообщение об ошибке
 * @param p_ReportNumber Код объекта
 * @param p_ImpDate Дата импорта
 * @param p_RgPartyObject Код биржи
 * @return Код ошибки
 */
  FUNCTION WriteMarketReport(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_OutMarketReportID OUT NUMBER, p_ErrMsg OUT VARCHAR2,
                             p_ReportNumber IN VARCHAR2, p_ImpDate IN DATE, p_RgPartyObject IN VARCHAR2)
    RETURN NUMBER;

/**
 * Получение вида операции БОЦБ
 * @since 6.20.031.52
 * @qtest NO
 * @param p_GateDealPlace Вид размещения
 * @param p_GateDealType Тип
 * @param p_DealTypeID Вид операции
 * @param p_ErrMes Описание ошибки
 * @param p_IsDealDU Признак ДУ
 * @param p_IsTODAY Признак однодневной сделки
 * @param p_IsSEB Признак собственной сделки
 * @param p_UnderWr Признак UnderWr
 * @param p_UnderWr_Pokupka Признак UnderWr_Pokupka
 * @param p_IsRSHB Признак IsRSHB
 * @param p_IsOTC Признак сделки ОТС
 * @return Код ошибки
 */
  FUNCTION GetTypeOperationBOCB(p_GateDealPlace IN VARCHAR2, p_GateDealType IN VARCHAR2, p_DealTypeID OUT NUMBER, p_ErrMes OUT VARCHAR2, p_IsDealDU IN NUMBER, p_IsTODAY IN NUMBER, p_IsSEB IN NUMBER, p_UnderWr IN NUMBER, p_UnderWr_Pokupka IN NUMBER, p_IsRSHB IN NUMBER, p_IsOTC IN NUMBER)
    RETURN NUMBER;

/**
 * Получение наименования параметра для платежа с заданным назначением
 * @since RSHB 82
 * @qtest NO
 * @param p_Name Наименование параметра
 * @param p_Purp Назначение
 * @return Наименование параметра с учетом назначения
 */
  FUNCTION GetParmName(p_Name IN VARCHAR2, p_Purp IN NUMBER) RETURN VARCHAR2;

/**
 * Получение из строки параметров map-коллекции параметр-значение
 * @since RSHB 82
 * @qtest NO
 * @param p_PrmStr Параметры в виде строки (например 'prm1=15;prm2=дата2')
 * @return map-коллекция параметр-значение
 */
  FUNCTION GetPrmMapByStr(p_PrmStr IN VARCHAR2) RETURN STRMAP_T;

/**
 * Получение значения после разделителя
 * @since RSHB 82
 * @qtest NO
 * @param p_Code Строка
 * @param p_split Разделитель
 * @return Значение после разделителя
 */
  FUNCTION GetRefNote(p_Code IN VARCHAR2, p_split IN VARCHAR2) RETURN VARCHAR2;

/**
 * Получение кода клиента
 * @since RSHB 82
 * @qtest NO
 * @param p_ClientCodeInFile Строка содержащая код клиента
 * @return Код клиента
 */
  FUNCTION GetClientCode(p_ClientCodeInFile IN VARCHAR2) RETURN VARCHAR2;

/**
 * НайтиДанныеПо2Части
 * @since RSHB 82
 * @qtest NO
 * @param p_SettleDate Дата расчетов по 1 части
 * @param p_SettleDate2 Дата расчетов по 2 части
 * @param p_Amount Сумма сделки по 1 части
 * @param p_Amount2 Сумма сделки по 2 части
 * @param p_RepoRate Процентная ставка
 */
  PROCEDURE FindDataPart2(p_SettleDate IN DATE, p_SettleDate2 IN DATE, p_Amount IN NUMBER, p_Amount2 IN NUMBER, p_RepoRate IN OUT NOCOPY NUMBER);

/**
 * Получение идентификатора объекта
 * @since RSHB 84
 * @qtest NO
 * @param KINDOBJ Вид объекта
 * @param GATECODE Код объекта
 * @param GKBO_CODEKIND Вид кода
 * @param REALID Идентификатор объекта
 * @param ISRSHB Признак реализации РСХБ
 * @param ERRMES Сообщение об ошибке
 * @param CLIENTBYMPCODE Признак поиска клиента по ККК
 * @param SERVKIND Вид договора 
 * @param ONDATE Дата, на которую открыт договор
 * @param SFCONTRID Идентификатор договора 
 * @param MARKETID Биржа
 * @return Код ошибки
 */ 
  FUNCTION GETREALID(KINDOBJ IN NUMBER, GATECODE IN VARCHAR2, GKBO_CODEKIND IN NUMBER,
                     REALID OUT NUMBER, ISRSHB IN NUMBER DEFAULT 0, ERRMES OUT VARCHAR2, CLIENTBYMPCODE IN BOOLEAN DEFAULT NULL,
                     SERVKIND IN NUMBER DEFAULT 0, ONDATE IN DATE DEFAULT RSI_GT.ZeroDate, SFCONTRID OUT NUMBER, 
                     MARKETID IN NUMBER DEFAULT NULL) RETURN NUMBER;

/**
 * Проверка на вхождение кода в список кодов банка
 * @since RSHB 91
 * @qtest NO
 * @param p_Code Код клиента
 * @return 1 - код клиента является кодом банка, 0 - не является
 */                     
  FUNCTION CheckBankCode(p_Code IN VARCHAR2) RETURN NUMBER;

END RSB_GTFN;
/
