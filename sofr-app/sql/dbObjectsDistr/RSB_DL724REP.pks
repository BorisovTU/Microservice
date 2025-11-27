CREATE OR REPLACE PACKAGE RSB_DL724REP
IS

  gl_IsOptim  BOOLEAN := false; -- Отладочный режим для оптимизации

  OBJTYPE_PARTY             CONSTANT NUMBER := 3;      -- Субъект экономики
  PARTY_ATTR_GROUP_ACTIVITY CONSTANT NUMBER := 100009; -- Не учитывать в форме 707
  NOT_USE_DEALS_ATTR_GRP    CONSTANT NUMBER := 66;      --Категория "Не использовать в отчетах цену сделок" (Ц/б)
  ATTR_SPEC_DEAL            CONSTANT NUMBER := 103;     -- Признак специальной сделки
  

  PTSK_STOCKDL             CONSTANT NUMBER := 1;    --Фондовый дилинг
  PTSK_DEPOS               CONSTANT NUMBER := 7;    --Депозитарное обслуживание
  PTSK_VEKSACC             CONSTANT NUMBER := 14;   --Учтенные векселя
  PTSK_DV                  CONSTANT NUMBER := 15;   --Срочные контракты
  PTSK_CM                  CONSTANT NUMBER := 21;   --Валютный рынок
  
  DL_SECURITYDOC           CONSTANT NUMBER := 101;  -- Сделка с ценными бумагами
  DL_RETIREMENT            CONSTANT NUMBER := 117;  -- Погашение выпуска
  DL_ISSUE_UNION           CONSTANT NUMBER := 135;  -- Конвертация выпусков ц/б
  DL_CONVAVR               CONSTANT NUMBER := 138;  -- Конвертация ц/б
  DL_VAREPAY               CONSTANT NUMBER := 142;  -- Погашение учтенных векселей
  DL_VAPAWN                CONSTANT NUMBER := 143;  -- Залог учтенных векселей
  
  DL_PREPARING             CONSTANT NUMBER := 0;    --Статус сделки "Отложена"
  DL_READIED               CONSTANT NUMBER := 10;   --Сделка готова (создана операция)
  
  PTK_INTERNATIONAL_ORG    CONSTANT NUMBER := 34;   --Международная организация
  
  RATETYPE_MARKET_PRICE    CONSTANT NUMBER := 1;    --Вид курса "Рыночная цена"
  RATETYPE_REASONED_PRICE  CONSTANT NUMBER := 1001; --Вид курса "Мотивированное суждение"
  RATETYPE_BLOOMBERG_PRICE CONSTANT NUMBER := 1002; --Вид курса "Цена закрытия Bloomberg для ф.707"
  RATETYPE_CALC_PRICE      CONSTANT NUMBER := 6;    --Вид курса "Расчетная цена"
  RATETYPE_MIN_PRICE_STEP  CONSTANT NUMBER := 8;    --Вид курса "Стоимость мин. шага цены"
  RATETYPE_NKD             CONSTANT NUMBER := 15;   --Вид курса "НКД на одну бумагу"
  RATETYPE_CLOSE           CONSTANT NUMBER := 18;   --Вид курса "Цена закрытия"
  
  MOEX_CALENDAR_ID         CONSTANT NUMBER := 20;   --ID календаря ММВБ
  
  RISKLEVEL_NOTINSTALL     CONSTANT NUMBER := 0;    --Без уровня
  RISKLEVEL_USUAL          CONSTANT NUMBER := 1;    --Стандартный
  RISKLEVEL_ELEVATED       CONSTANT NUMBER := 2;    --Повышенный
  RISKLEVEL_SPECIAL        CONSTANT NUMBER := 3;    --Особый
  
  PARTY_ATTR_GROUP_RISKLEVEL CONSTANT NUMBER := 98; --Уровень риска

  -- Подвиды операции списания/зачисления денежных средств
  DL_NPTXOP_WRTKIND_ENROL  CONSTANT NUMBER := 10;   --Зачисление
  DL_NPTXOP_WRTKIND_WRTOFF CONSTANT NUMBER := 20;   --Списание

  --Виды биржевого рынка для заявок и сделок ФИССиКО, видов обязательств  
  DV_MARKETKIND_CURRENCY   CONSTANT NUMBER := 2;    --Валютный
  
  PM_PURP_GARANT           CONSTANT NUMBER := 71;   --Гарантийное обеспечение
  PM_PURP_COMMBANK         CONSTANT NUMBER := 72;   --Комиссия Банку
  
  CHAPTER_ALL              CONSTANT NUMBER := 0;
  CHAPTER_1_3              CONSTANT NUMBER := 1;
  CHAPTER_4                CONSTANT NUMBER := 2;
  CHAPTER_6                CONSTANT NUMBER := 3;
  CHAPTER_7                CONSTANT NUMBER := 4;
  CHAPTER_8                CONSTANT NUMBER := 5;
  CHAPTER_9                CONSTANT NUMBER := 6; 
  CHAPTER_11               CONSTANT NUMBER := 7; 
  
   FUNCTION IsDateAfterWorkDayM (
                                   p_Date IN DATE,
                                   p_SinceDate IN DATE,
                                   p_identProgram IN NUMBER,
                                   p_maxDaysCnt IN NUMBER, 
                                   p_CalParamArr RSI_DlCalendars.calparamarr_t)
   RETURN NUMBER DETERMINISTIC;

  FUNCTION GetBankRole(pSfContrId IN NUMBER, pEndDate IN DATE, pIsRep12 IN NUMBER)
  RETURN NUMBER DETERMINISTIC;

  FUNCTION GetGroup_Contr(pPartyId IN NUMBER, pEndDate IN DATE, pIsRep12 IN NUMBER, pDlContrID IN NUMBER) RETURN NUMBER DETERMINISTIC;

  FUNCTION is_activeClient(pSessionId IN NUMBER, pPartyId IN NUMBER, pBegDate IN DATE, pEndDate IN DATE, pIsSecur IN NUMBER, pIsDV IN NUMBER, pIsRep12 IN NUMBER)
  RETURN NUMBER DETERMINISTIC;

  FUNCTION GetOkatoCode(pParty IN NUMBER, pNRCountry IN VARCHAR2, pNotResident IN CHAR, pOnDate DATE, pIsRep12 IN NUMBER) RETURN VARCHAR DETERMINISTIC;

  FUNCTION GetOKSM_Code(pParty IN NUMBER, pNRCountry IN VARCHAR2, pNotResident IN CHAR, pSuperior IN NUMBER, pOnDate DATE, pIsRep12 IN NUMBER)
  RETURN VARCHAR DETERMINISTIC;
  
  PROCEDURE FillTableContr(pSessionId IN NUMBER, pBegDate IN DATE, pEndDate IN DATE, pIsSecur IN NUMBER, pIsDV IN NUMBER, pIsRep12 IN NUMBER);

  PROCEDURE FillTableClient(pSessionId IN NUMBER, pBegDate IN DATE, pEndDate IN DATE, pIsSecur IN NUMBER, pIsDV IN NUMBER, pIsRep12 IN NUMBER);

  -- Обработка ДО
  PROCEDURE ProcessDO(pSessionID IN NUMBER,
                      pmin_ClientContrId IN NUMBER,
                      pmax_ClientContrId IN NUMBER,
                      pBegDate IN DATE,
                      pEndDate IN DATE,
                      pIsRep12 IN NUMBER,
                      pChapter IN NUMBER,
                      pIsParallel IN NUMBER DEFAULT 1);


  --Формирование данных
  PROCEDURE CreateAllData( pBegDate IN DATE,
                           pEndDate IN DATE,
                           pChapter IN NUMBER,
                           pSessionID IN NUMBER,
                           pParallelLevel IN NUMBER,
                           pIsRep12 IN NUMBER );


  PROCEDURE FillTableR3CLIENT_GROUP( pBegDate IN DATE,
                           pEndDate IN DATE,
                           pSessionID IN NUMBER,
                           pParallelLevel IN NUMBER);

  --Чистка таблиц после запуска
  PROCEDURE ClearTables(pSessionId IN NUMBER,
                        pCountDay IN NUMBER default 3);
                        
  function ConvSum (
    p_sum     number,
    p_from_fi number,
    p_to_fi   number,
    p_date    date
  ) return number deterministic;

  procedure prepare_cost_avr_detail (
    p_sessionid number,
    p_date      date
  );

  procedure prepare_clients_detail (
    p_sessionid number,
    p_date      date
  );

END RSB_DL724REP;
/
