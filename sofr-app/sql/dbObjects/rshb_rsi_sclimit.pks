CREATE OR REPLACE PACKAGE RSHB_RSI_SCLIMIT
AS
  /**
   @file 		RSHB_RSI_SCLIMIT.pks
   @brief 		Утилиты для расчета лимитов
     
   # changeLog
   |date       |author         |tasks                                                     |note                                                        
   |-----------|---------------|----------------------------------------------------------|-------------------------------------------------------------
   |2024.12.27 |Зыков М.В.     | BOSS-6238 BOSS-5028                                      | Доработать параллельный расчет лимитов при включенном обособлении ДС
   |2024.03.22 |Зыков МВ.      | BIQ-16667                                                | Перевод процедуры расчета лимитов на обработчик сервисов QManager  
   |2024.03.04 |Велигжанин А.В.| 62480                                                    | GetLimitPrm(), процедура для получения данных
   |           |               |                                                          | из справочника расчета лимитов.                
    
  */
   UnknownDate            CONSTANT DATE := TO_DATE ('01.01.0001', 'DD.MM.YYYY');
   UnknownTime            CONSTANT DATE := TO_DATE ('01.01.0001 00:00:00', 'DD.MM.YYYY HH24:MI:SS');

   /**
    * Виды рынка */
   MARKET_KIND_STOCK      CONSTANT INTEGER := 1;                    --фондовый
   MARKET_KIND_DERIV      CONSTANT INTEGER := 2;                     --срочный
   MARKET_KIND_CURR       CONSTANT INTEGER := 3;                    --валютный
   MARKET_KIND_EDP       CONSTANT INTEGER := 4;                    --ЕДП
   SID_MARKET_KIND_STOCK  constant ddl_limitcashstock_dbt.t_market_kind%type := 'фондовый';
   SID_MARKET_KIND_DERIV  constant ddl_limitfuturmark_dbt.t_market_kind%type := 'срочный' ;
   SID_MARKET_KIND_CURR   constant ddl_limitcashstock_dbt.t_market_kind%type := 'валютный';
   SID_MARKET_KIND_EDP    constant ddl_limitcashstock_dbt.t_market_kind%type := 'ЕДП';


  /**
    * MARKET в таблице корректировок лимитов QUIK */
   MARKET_STOCK_OUT       CONSTANT INTEGER := 0;     --фондовый на внебиржевом
   MARKET_STOCK_EX        CONSTANT INTEGER := 1;        --фондовый на биржевом
   MARKET_DERIV           CONSTANT INTEGER := 2;                     --срочный
   MARKET_CURR            CONSTANT INTEGER := 3;                    --валютный

   /**
    * Статусы выгрузки корректировки лимитов QUIK */
   LIMITSTATUS_UNDEF      CONSTANT INTEGER := 0;                      -- пусто
   LIMITSTATUS_WAIT       CONSTANT INTEGER := 1;                       -- Ждет
   LIMITSTATUS_UNLOADED   CONSTANT INTEGER := 2;                  -- Выгружена
  LIMITSTATUS_REJECT   CONSTANT INTEGER :=  3; -- Отвергнута

/**
  * Условия */
  EQ_CHAR   CONSTANT CHAR := '=';
  LESS_CHAR CONSTANT CHAR := '<';
  LEQ_CHAR  CONSTANT CHAR := '{'; -- <=

/**
  * Подсистема*/
  IDENTPROG_SP CONSTANT INTEGER := 83;
  IDENTPROG_DV CONSTANT INTEGER := 158;
   

   ts_                             TIMESTAMP;
   CALENDAR_MB            CONSTANT NUMBER := 20; /* Календарь работы Московской Биржи*/

   LOGACTION_INFO_RUDATA_ADDED     CONSTANT INTEGER := 10100;
   LOGACTION_WARN_RUDATA_NOTFOUND  CONSTANT INTEGER := 10131;
   LOGACTION_ERR_RUDATA_DUPL       CONSTANT INTEGER := 10161;
   LOGACTION_ERR_NO_CODE_FOR_SEC   CONSTANT INTEGER := 10162;
   LOGACTION_ERR_RUDATA_INCORRECT  CONSTANT INTEGER := 10163;
   LOGACTION_ERR_RUDATA_OTHER      CONSTANT INTEGER := 10199;

   GC_CALC_SID_DEFAULT  constant ddl_clientinfo_dbt.t_calc_sid%type := 'X' ; -- SID расчета по умолчанию 
   g_log_add         varchar2(50);
   g_calc_DIRECT     varchar2(128) := GC_CALC_SID_DEFAULT;
   g_calc_clientinfo varchar2(128) := GC_CALC_SID_DEFAULT;
   g_calc_panelcontr varchar2(128) := GC_CALC_SID_DEFAULT;



   
     PROCEDURE TimeStamp_ (Label_           IN VARCHAR2,
                         date_               DATE,
                         start_              TIMESTAMP,
                         end_                TIMESTAMP,
                         action_             NUMBER DEFAULT NULL,
                         excepsqlcode_       NUMBER DEFAULT NULL,
                         all_log_            boolean default false
                        ) ;
    
   
   /**
    * Добавить корректировку лимита на шаге (DL_LIMITADJUST)
    * @since 6.20.031
    * @qtest NO
    * @param RecLimitAdj стуктура корректировки лимита
    * @param ID_Operation
    * @param ID_Step
    */

   FUNCTION GetKindMarketCodeOrNote (pMarketID IN NUMBER, IsSecCode IN NUMBER, IsTradeaccID IN NUMBER)
     RETURN NUMBER deterministic;


    PROCEDURE Gather_Table_Stats(p_TableName IN VARCHAR2) ;
    PROCEDURE InsertLIMITCASHSTOCKFromInt(p_CalcDate IN DATE);
    procedure DeleteWoOpenBalance(p_CalcDate date) ;
   
   PROCEDURE RSI_CreateLimitAdJust (RecLimitAdj IN RAW, ID_Operation IN NUMBER, ID_Step IN NUMBER);

   --пользовательский аналог для вставки не из Сишника
   PROCEDURE RSI_CreateLimitAdJust (RecLimitAdj IN OUT DDL_LIMITADJUST_DBT%ROWTYPE, ID_Operation IN NUMBER, ID_Step IN NUMBER);

   /**
    * Удалить корректировку лимита на шаге (DL_LIMITADJUST)
    * @since 6.20.031
    * @qtest NO
    * @param ID_Operation
    * @param ID_Step
    */
   PROCEDURE RSI_RestoreLimitAdJust (ID_Operation IN NUMBER, ID_Step IN NUMBER);



   /**
    * Проверить, нужно ли включать сделку в расчет неисполненных ТО
    * @since 6.20.031
    * @qtest NO
    * @param p_CalcDate Дата расчета
    * @param p_DealID   Идентификатор сделки
    * @return 0 - не нужно, 1 - нужно
    */
   FUNCTION UseNotExecRQbyDeal (p_CalcDate IN DATE, p_DealID IN NUMBER)
      RETURN NUMBER;

  /**
    * Получить суммы неоплаченной комиссии 
    * @since 6.20.031
    * @qtest NO
    * @param p_Client        Идентификатор клиента
    * @param p_ClientContrID Идентификатор договора клиента
    * @param p_ServKindSub   подвид обслуживания фондового дилинга
    * @param p_CalcDate      Дата расчета
    * @param p_CheckDate     Дата проверки
    * @param p_FIID          Валюта расчета
    * @return Сумма 
    */

   function GetSumPlanPeriodCom(p_Client          IN NUMBER,
                             p_ClientContrID   IN NUMBER,
                             p_ServKindSub     IN NUMBER,
                             p_StartDate        IN DATE,
                             p_CheckDate       IN DATE,
                             p_FIID            IN NUMBER,
                             p_MarketID IN NUMBER
                            ) 
       return number deterministic ;

   /**
    * Получить сумму неисполненных денежных ТО
    * @since 6.20.031
    * @qtest NO
    * @param p_Client        Идентификатор клиента
    * @param p_ClientContrID Идентификатор договора клиента
    * @param p_ServKindSub   подвид обслуживания фондового дилинга
    * @param p_CalcDate      Дата расчета
    * @param p_CheckDate     Дата проверки
    * @param p_AccountID     Идентификатор л/с
    * @param p_ToFI          Валюта, в которую необходимо конвертировать
    * @param p_IsReq         Признак видов отбираемых ТО: 0 - обязательства, 1 - требования
    * @return Сумма ТО в валюте p_ToFI
    */
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
      RETURN NUMBER deterministic;


  
   /**
    * Получить сумму неисполненных денежных ТО
    * @since 6.20.031
    * @qtest NO
    * @param p_Client        Идентификатор клиента
    * @param p_ClientContrID Идентификатор договора клиента
    * @param p_CalcDate      Дата расчета
    * @param p_CheckDate     Дата проверки
    * @param p_AccountID     Идентификатор л/с
    * @param p_FIID          FIID
    * @param p_IsReq         Признак видов отбираемых ТО: 0 - обязательства, 1 - требования
    * @return Сумма ТО
    */
   FUNCTION GetSumPlanCashCM (p_Client          IN NUMBER,
                              p_ClientContrID   IN NUMBER,
                              p_CalcDate        IN DATE,
                              p_CheckDate       IN DATE,
                              p_Account         IN VARCHAR2,
                              p_FIID            IN NUMBER,
                              p_IsReq           IN NUMBER
                             )
      RETURN NUMBER deterministic;

   /**
    * Получить сумму неисполненных ТО по ценным бумагам
    * @since 6.20.031
    * @qtest NO
    * @param p_Client        Идентификатор клиента
    * @param p_ClientContrID Идентификатор договора клиента
    * @param p_ServKindSub   подвид обслуживания фондового дилинга
    * @param p_CalcDate      Дата расчета
    * @param p_CheckDate     Дата проверки
    * @param p_FIID          Идентификатор ц/б
    * @param p_IsReq         Признак видов отбираемых ТО: 0 - обязательства, 1 - требования
    * @return Сумма ТО
    */
   FUNCTION GetSumPlanAvrRQ (p_Client          IN NUMBER,
                             p_ClientContrID   IN NUMBER,
                             p_ServKindSub     IN NUMBER,
                             p_CalcDate        IN DATE,
                             p_CheckDate       IN DATE,
                             p_FIID            IN NUMBER,
                            p_IsReq IN NUMBER,
                            p_MarketID IN NUMBER) RETURN NUMBER deterministic;

   /**
    * Получить сумму комиссий банка по сделкам, заключенным в предыдущий день
    * @since 6.20.031
    * @qtest NO
    * @param p_Client        Идентификатор клиента
    * @param p_ClientContrID Идентификатор договора клиента
    * @param p_AccountID     Идентификатор л/с
    * @param p_ToFI          Валюта, в которую необходимо конвертировать
    * @param p_CalcDate      Дата расчета
    * @param p_isDue474      1 - исключить из расчета сделки тудэй
    * @return Сумма комиссий в валюте p_ToFI
    */
   FUNCTION GetSumComPrevious (p_Client          IN NUMBER,
                               p_ClientContrID   IN NUMBER,
                               p_AccountID       IN NUMBER,
                               p_ToFI            IN NUMBER,
                               p_CalcDate        IN DATE,
                               p_IsDue474        IN NUMBER,
                              p_MarketID IN NUMBER) RETURN NUMBER;

   /**
    * Получить сумму комиссий банка по сделкам, заключенным в предыдущий день, для сверки
    * @since 6.20.031
    * @qtest NO
    * @param p_Client        Идентификатор клиента
    * @param p_ClientContrID Идентификатор договора клиента
    * @param p_FI            Валюта
    * @param p_CalcDate      Дата расчета
    * @return Сумма комиссий в валюте p_FI
    */
   FUNCTION GetSumComPreviousForRevise (p_Client          IN NUMBER,
                                        p_ClientContrID   IN NUMBER,
                                        p_FI              IN NUMBER,
                                        p_CalcDate        IN DATE,
                                        p_MarketID IN NUMBER)  RETURN NUMBER;

   /**
    * Получить сумму ГО по клиентским позициям предыдущего дня
    * @since 6.20.031
    * @qtest NO
    * @param p_Client        Идентификатор клиента
    * @param p_ClientContrID Идентификатор договора клиента
    * @param p_Department    Филиал
    * @param p_CalcDate      Дата расчета
    * @param p_PrevWorkDate  Предыдущий рабочий день
    * @param p_ToFI          Валюта, в которую необходимо конвертировать
    * @return Сумма ГО в валюте p_ToFI
    */
   FUNCTION GetSumGuarantyPrevious (p_Client          IN NUMBER,
                                    p_ClientContrID   IN NUMBER,
                                    p_Department      IN NUMBER,
                                    p_CalcDate        IN DATE,
                                    p_PrevWorkDate    IN DATE,
                                    p_ToFI            IN NUMBER
                                   )
      RETURN NUMBER;

   /**
    * Получить сумму комиссий банка по биржевым сделкам ФИССиКО, заключенным в предыдущий день
    * @since 6.20.031
    * @qtest NO
    * @param p_Client        Идентификатор клиента
    * @param p_ClientContrID Идентификатор договора клиента
    * @param p_Department    Филиал
    * @param p_CalcDate      Дата расчета
    * @param p_PrevWorkDate  Предыдущий рабочий день
    * @param p_AccCode_Currency Валюта, счета в которой ищем комиссии
    * @param p_ToFI          Валюта , в которую необходимо конвертировать
    * @return Сумма комиссий в рублях
    */
   FUNCTION GetSumFutureComPrevious (p_Client          IN NUMBER,
                                     p_ClientContrID   IN NUMBER,
                                     p_Department      IN NUMBER,
                                     p_CalcDate        IN DATE,
                                     p_PrevWorkDate    IN DATE,
                                     p_AccCode_Currency IN NUMBER, 
                                     p_ToFI IN NUMBER
                                    )
      RETURN NUMBER;

   --FUNCTION GetSumFutureComPrevious(p_Client IN NUMBER, p_ClientContrID IN NUMBER, p_Department IN NUMBER, p_CalcDate IN DATE, p_PrevWorkDate IN DATE, p_AccCode_Currency IN NUMBER, p_ToFI IN NUMBER) RETURN NUMBER;

/**
 * получить дату расчета лимита
 * @since 6.20.031
 * @qtest NO
 * @param Kind вид лимита
 * @param FIID валюта для которой считаем лимит
 * @param IsCur признак валютного рынка
 * @param IsNatCur признак национальной валюты(валюты расчетной организации)
 * @return дата лимита
 */

   FUNCTION GetDateLimitByKind( Kind IN INTEGER,
                                FIID IN INTEGER,
                                IsCur IN INTEGER ,
                                IsNatCur IN INTEGER  )RETURN DATE;


/**
 * получить последнюю дату расчета лимита
 * @since 6.20.031
 * @qtest NO
 * @param p_MarketKind вид рынка 1- Фондовый, 3 - Валютный, 2 - Срочный
 * @return максимальная дата расчета по рынку
 */
   FUNCTION RSI_GetLastDateCalc(p_MarketKind IN NUMBER, p_MarketID IN NUMBER) RETURN DATE;


   /**
    * Выполнить утренний расчет лимитов определенного вида по ц/б (используется в параллельном выполнении)
    * @since 6.20.031
    * @qtest NO
    * @param p_start_id          Начальный номер для обработки (нужно для параллельной обработки. Заменяет вид литима)
    * @param p_end_id            Конечный номер для обработки (нужно для параллельной обработки. Равно p_start_id)
    * @param p_CalcDate          Дата расчета
    * @param p_ByMarketStock     Признак расчета по биржевому рынку
    * @param p_ByOutMarketStock  Признак расчета по внебиржевому рынку
    */
   PROCEDURE RSI_CreateSecurLimByKind (p_start_id        IN NUMBER,
                                       p_end_id          IN NUMBER,
                                       p_CalcDate        IN DATE,
                                       p_ByMarket        IN NUMBER,
                                       p_ByOutMarket     IN NUMBER,
                                     --  p_ByEDP              IN NUMBER,
                                       p_DepoAcc         IN VARCHAR2,
                                       p_MarketCode         IN VARCHAR2,
                                       p_MarketID     IN NUMBER);

    PROCEDURE RSI_CreateSecurLimitsCur (p_CalcDate IN DATE, p_IsKind2 IN NUMBER, p_DepoAcc IN VARCHAR2) ;
    PROCEDURE RSI_ClearCashStockLimitsCur (p_CalcDate IN DATE, p_ByEDP IN NUMBER, p_UseListClients IN NUMBER DEFAULT 0);
    PROCEDURE RSI_DeleteCashStockLimitsCur (p_CalcDate IN DATE, p_IsKind2 IN NUMBER, p_IsDepo IN NUMBER, p_ByEDP IN NUMBER, p_UseListClients IN NUMBER DEFAULT 0) ;
    PROCEDURE RSI_CreateCashStockLimByKindCur_job(p_start_id      IN NUMBER,
                                                p_end_id        IN NUMBER,
                                                p_CalcDate      IN DATE,
                                                p_IsDepo        IN NUMBER,
                                                p_ByEDP         IN NUMBER);
   /**
    * Выполнить утренний расчет лимитов определенного вида по деньгам (используется в параллельном выполнении)
    * @since 6.20.031
    * @qtest NO
    * @param p_start_id          Начальный номер для обработки (нужно для параллельной обработки. Заменяет вид литима)
    * @param p_end_id            Конечный номер для обработки (нужно для параллельной обработки. Равно p_start_id)
    * @param p_CalcDate          Дата расчета
    * @param p_ByMarketStock     Признак расчета по биржевому рынку
    * @param p_ByOutMarketStock  Признак расчета по внебиржевому рынку
    */
   PROCEDURE RSI_CreateCashStockLimByKind (
      p_start_id        IN NUMBER,
      p_end_id          IN NUMBER,
      p_CalcDate        IN DATE,
      p_ByMarket        IN NUMBER,
      p_ByOutMarket     IN NUMBER,
      p_ByEDP               IN NUMBER,
      p_MarketCode      IN VARCHAR2,
      p_MarketID        IN NUMBER);


   /**
    * Выполнить утренний расчет лимитов
    * @since 6.20.031
    * @qtest NO
    * @param p_CalcDate          Дата расчета
    * @param p_ByMarketStock     Признак расчета по биржевому рынку
    * @param p_ByOutMarketStock  Признак расчета по внебиржевому рынку
    * @param p_ByFutureMark      Признак расчета по срочному рынку
    */
   PROCEDURE RSI_CreateLimits (p_MarketID     IN NUMBER,
                               p_MarketCode   IN VARCHAR2,
                               p_CalcDate     IN DATE,
                               p_ByStock      IN NUMBER,
                               p_ByCurr       IN NUMBER,
                               p_ByDeriv      IN NUMBER,
                               p_ByEDP       IN NUMBER default 0,
                               p_UseListClients IN NUMBER default 0
                              );
   PROCEDURE RSI_CheckCashStockLimits ( p_CalcDate        IN DATE) ;
   
   PROCEDURE RSI_ClearCashStockLimits (
      p_CalcDate        IN DATE,
      p_ByMarket       IN NUMBER,
      p_ByOutMarket  IN NUMBER,
      p_ByEDP           IN NUMBER,
      p_MarketCode IN VARCHAR,
      p_MarketID IN NUMBER,
      p_UseListClients IN NUMBER DEFAULT 0) ;
      
   PROCEDURE RSI_CreateCashStockLimits (
      p_CalcDate        IN DATE,
      p_ByMarket        IN NUMBER,
      p_ByOutMarket     IN NUMBER,
      p_ByEDP     IN NUMBER,
      p_MarketCode IN VARCHAR,
      p_MarketID IN NUMBER,
      p_UseListClients IN NUMBER DEFAULT 0);
   PROCEDURE CheckCashStockForDuplAndSetErr(p_CalcDate IN DATE) ;
   PROCEDURE RSI_CheckSecurLimits(p_CalcDate        IN DATE) ;
   PROCEDURE RSI_ClearSecurLimits (p_CalcDate        IN DATE,
                                    p_ByMarket        IN NUMBER,
                                    p_ByOutMarket     IN NUMBER,
           --                         p_ByEDP           IN NUMBER,
                                    p_DepoAcc         IN VARCHAR2,
                                    p_MarketCode      IN VARCHAR2,
                                    p_MarketID        IN NUMBER,
                                    p_UseListClients IN NUMBER DEFAULT 0
                                   ) ;
   PROCEDURE RSI_CreateSecurLimits (p_CalcDate        IN DATE,
                                    p_ByMarket        IN NUMBER,
                                    p_ByOutMarket     IN NUMBER,
                                    --p_ByEDP              IN NUMBER, 
                                    p_DepoAcc         IN VARCHAR2,
                                    p_MarketCode      IN VARCHAR2, 
                                    p_MarketID        IN NUMBER, 
                                    p_UseListClients IN NUMBER DEFAULT 0
                                   );

  PROCEDURE RSI_LOCKSecurLimits (p_CalcDate        IN DATE,
                                    p_ByMarket        IN NUMBER,
                                    p_ByOutMarket     IN NUMBER,
           --                         p_ByEDP           IN NUMBER,
                                    p_DepoAcc         IN VARCHAR2,
                                    p_MarketCode      IN VARCHAR2,
                                    p_MarketID        IN NUMBER,
                                    p_UseListClients IN NUMBER DEFAULT 0
                                   ) ;
   PROCEDURE SaveArchSecur (p_CalcDate IN DATE);

   PROCEDURE SaveArchMoney (p_CalcDate IN DATE);

   PROCEDURE SaveArchFuture (p_CalcDate IN DATE);

   FUNCTION GetObjCodeOnDate (pFIID         IN NUMBER,
                              pObjectType   IN NUMBER,
                              pCodeKind     IN NUMBER,
                              pDate         IN DATE
                             )
      RETURN VARCHAR2;

  FUNCTION CalcWaPrice (p_CalcDate     IN DATE,
                         p_Client       IN NUMBER,
                         p_SfContrID    IN NUMBER,
                         p_FIID         IN NUMBER,
                         p_PriceFIID         IN NUMBER,
                         p_ClientCode   IN VARCHAR2,
                         p_SecCode      IN VARCHAR2,
                         p_IsDebug      IN NUMBER)
      RETURN NUMBER;

   FUNCTION GetWAPositionPrice (p_CalcDate     IN DATE,
                                p_Client       IN NUMBER,
                                p_SfContrID    IN NUMBER,
                                p_FIID         IN NUMBER,
                                p_ClientCode   IN VARCHAR2,
                                p_SecCode      IN VARCHAR2,
                                p_FirmID       IN VARCHAR2,
                                p_LimitKind    IN NUMBER,
                                p_TrdAccID IN VARCHAR2,
                                p_Force IN NUMBER DEFAULT 0,
                                p_IsDebug IN NUMBER DEFAULT 0
                               )
      RETURN NUMBER;
      
      ----------
      PROCEDURE ClearFIIDTmp( p_MarketID IN NUMBER, p_UseListClients IN NUMBER) ;
      PROCEDURE SetFIIDTmp (p_CalcDate IN DATE, p_ByEDP IN NUMBER,p_MarketID IN NUMBER, p_UseListClients IN NUMBER) ;
      PROCEDURE ClearPlanSumCur (p_CalcDate IN DATE,p_ByCurr IN NUMBER, p_ByEDP IN NUMBER, p_MarketID IN NUMBER, p_UseListClients IN NUMBER,p_action number default 101);
      procedure CollectPlanSumCur(p_CalcDate  in date,p_ByCurr in number,p_ByEDP in number ,p_MarketID in number ,p_UseListClients in number);
      PROCEDURE ClearTickTmp (p_CalcDate IN DATE,p_ByStock IN NUMBER, p_ByEDP IN NUMBER, p_MarketID IN NUMBER, p_UseListClients IN NUMBER,p_action number default 101);
      PROCEDURE SetTickTmp (p_CalcDate IN DATE, p_ByStock IN NUMBER, p_ByEDP IN NUMBER, p_MarketID IN NUMBER, p_UseListClients IN NUMBER);
      PROCEDURE RSI_CreateFutureMarkLimits (p_CalcDate IN DATE, p_UseListClients IN NUMBER );
      PROCEDURE ClearLotTmp( p_CalcDate IN DATE,p_MarketID IN NUMBER, p_UseListClients IN NUMBER) ;
      function GetLotMaxChangeDate return date result_cache ;
      PROCEDURE SetLotTmp (p_CalcDate IN DATE, p_ByEDP IN NUMBER,p_MarketID IN NUMBER, p_UseListClients IN NUMBER);
      PROCEDURE getFlagLimitPrm(p_MarketID IN NUMBER,p_MarketKind IN NUMBER, v_IsDepo IN OUT NUMBER, v_IsKind2 IN OUT NUMBER, v_DepoAcc IN OUT VARCHAR2, p_ImplKind IN NUMBER DEFAULT 1);
      -----------
      PROCEDURE RSI_ClearContrTable ( p_MarketID IN NUMBER
                                 ,p_MarketCode IN VARCHAR2
                                 ,p_CalcDate IN DATE
                                , p_ByStock IN NUMBER
                                , p_ByCurr IN NUMBER
                                , p_ByEDP IN NUMBER
                                , p_byDeriv IN NUMBER
                                , p_UseListClients IN NUMBER) ;
      PROCEDURE  RSI_CheckContrTable  (p_CalcDate IN DATE, p_ByStock IN NUMBER, p_ByCurr IN NUMBER,
                  p_ByEDP IN NUMBER, p_byDeriv IN NUMBER, p_UseListClients IN NUMBER default 0);
      PROCEDURE RSI_FillContrTablenotDeriv ( p_MarketID IN NUMBER
                                 ,p_MarketCode IN VARCHAR2
                                 ,p_CalcDate IN DATE
                                , p_ByStock IN NUMBER
                                , p_ByCurr IN NUMBER
                                , p_ByEDP IN NUMBER
                                , p_byDeriv IN NUMBER
                                , p_UseListClients IN NUMBER);
       PROCEDURE RSI_FillContrTablebyDeriv ( p_MarketID IN NUMBER
                                 ,p_MarketCode IN VARCHAR2
                                 ,p_CalcDate IN DATE
                                , p_ByStock IN NUMBER
                                , p_ByCurr IN NUMBER
                                , p_ByEDP IN NUMBER
                                , p_byDeriv IN NUMBER
                                , p_UseListClients IN NUMBER) ;
      PROCEDURE RSI_FillContrTableAcc ( p_MarketID IN NUMBER
                                   ,p_MarketCode IN VARCHAR2
                                   ,p_CalcDate IN DATE
                                   , p_ByStock IN NUMBER
                                   , p_ByCurr IN NUMBER
                                   , p_ByEDP IN NUMBER
                                   , p_byDeriv IN NUMBER
                                   , p_UseListClients IN NUMBER) ;
      PROCEDURE RSI_FillContrTableCOM ( p_MarketID IN NUMBER
                                 ,p_MarketCode IN VARCHAR2
                                 ,p_CalcDate IN DATE
                                , p_ByStock IN NUMBER
                                , p_ByCurr IN NUMBER
                                , p_ByEDP IN NUMBER
                                , p_byDeriv IN NUMBER
                                , p_UseListClients IN NUMBER) ;
      PROCEDURE RSI_CreateSecurLimByKindCurZero (p_CalcDate IN DATE, p_Kind IN NUMBER, p_DepoAcc IN VARCHAR2, p_MarketID IN INTEGER, p_MarketCode IN VARCHAR2, p_ByEDP IN NUMBER, p_UseListClients IN NUMBER DEFAULT 0);
      PROCEDURE RSI_DeleteZeroSecurLimByCur (p_CalcDate IN DATE);
      PROCEDURE RSI_CreateFutureMarkLim (p_CalcDate IN DATE, p_UseListClients IN NUMBER);
      PROCEDURE RSI_CreateSecurLimGAZP (p_start_id      IN NUMBER,p_CalcDate      IN DATE,p_ByMarket      IN NUMBER,p_ByOutMarket   IN NUMBER,p_DepoAcc         IN VARCHAR2,p_MarketCode         IN VARCHAR2,p_MarketID     IN NUMBER);
      FUNCTION SfcontrIsEDP(p_SfcontrID IN NUMBER) RETURN  NUMBER DETERMINISTIC;
      FUNCTION GetMicexID  RETURN  NUMBER DETERMINISTIC;
      FUNCTION GetSpbexID  RETURN  NUMBER DETERMINISTIC;
      FUNCTION RSI_CheckCalcExists(p_MarketKind IN NUMBER, p_MarketID IN NUMBER, p_CheckDate IN DATE) RETURN NUMBER;
      
      FUNCTION GetCalendarIDForLimit(p_MarketID IN NUMBER, p_CurrencyId IN NUMBER DEFAULT -1, 
                                     p_MarketPlace IN NUMBER DEFAULT -1, p_SecFiidId IN NUMBER DEFAULT -1) RETURN NUMBER deterministic;
      FUNCTION GetCheckDateByParams(p_Kind IN NUMBER, p_Date IN DATE, p_MarketID IN NUMBER, p_IsEDP IN NUMBER, p_CurrencyId IN NUMBER DEFAULT -1,
                                    p_MarketPlace IN NUMBER DEFAULT -1, p_SecFiidId IN NUMBER DEFAULT -1) RETURN DATE deterministic;

      /**
       @brief    DEF-65531, Функция возвращает дату, на которую производится расчет лимита для соответствующего параметра.
       @param[in]    p_Kind    		Параметр расчета лимитов (T0, T1, T2 и т.д.)
       @param[in]    p_Date    		Дата расчета лимитов
       @param[in]    p_IsEDP            Если 1, то ЕДП
       @param[in]    p_MarketPlace      вид рынка (1 - Фондовый, 2 - Валютный, 3 - Срочный)
      */
      FUNCTION GetCheckDate(
        p_Kind IN NUMBER
        , p_Date IN DATE
        , p_IsEDP IN NUMBER
        , p_MarketPlace IN NUMBER DEFAULT -1
      ) 
      RETURN DATE deterministic;

   
     /**
       @brief    DEF-62480, Процедура возвращает значения параметров расчета лимитов для суб-договора.
       @param[in]    p_SfContrID    	ID суб-договора
       @param[in]    p_LimitDate    	Дата
       @param[out]   p_FirmID           Код участника торгов
       @param[out]   p_Tag              Код позиции
       @param[out]   p_TrdAcc           Торговый счет
      */
      PROCEDURE GetLimitPrm (
        p_SfContrID IN number
        , p_LimitDate IN date
        , p_FirmID OUT varchar2
        , p_Tag OUT varchar2
        , p_TrdAcc OUT varchar2
      );

   /**
    * BOSS-771, проверяет наличие категории
    * возвращает флаг для заполнения FirmID и Tag
    */
   function GetImplKind(p_dlcontrid IN number, p_CalcDate in DATE)
     return number deterministic;
     
   function GetDepoAccPrm(p_MarketID   in number
                        ,p_MarketKind in number
                        ,p_ImplKind   in number ) return varchar2 deterministic;

    function GetFIRM_IDbyServKind(p_MarketID   in number
                      ,p_ServKind in number
                      ,p_ImplKind   in number ) return varchar2 deterministic;



    function GetTAGbyServKind(p_MarketID   in number
                  ,p_ServKind in number
                  ,p_ImplKind   in number
                  ,p_IsEdp in number ) return varchar2 deterministic; 

 
    function GetTRDACCIDbyServKind(p_sfcontrId    in number 
                       ,p_CalcDate    in date 
                        ,p_MarketID   in number
                        ,p_ServKind in number
                        ,p_ImplKind   in number ) return varchar2 deterministic;
  
      /**
       @brief    DEF-68258, Функция возвращает параметр для расчета 0-го лимита.
       @param[in]    p_MarketKind    	Вид рынка
       @param[in]    p_MarketID    	Биржа
       @param[in]    p_ImplKind    	Принадлежность ТКС
      */
      FUNCTION GetCodeSCZeroLimit(p_MarketKind IN NUMBER
                                , p_MarketID IN INTEGER
                                , p_ImplKind IN INTEGER) RETURN VARCHAR2 deterministic;

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
                           , p_UseListClients IN NUMBER default 0) ;
     
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
                           , p_UseListClients IN NUMBER default 0) ;
     
      function GetSFContridLIMIT ( p_CalcDate date , p_marketid number,p_mpcode ddlcontrmp_dbt.t_mpcode%type, p_Client DDL_LIMITSECURITES_DBT.t_Client%type) return number deterministic ;
     /**
       @brief     BIQ-16667, Сохранение цен приобритения по чисти строк лимитов . Запускается по окончании расчета
     */
      PROCEDURE SetWAPositionPrice (p_MarketID number, p_id_first number, p_id_last number, p_UseListClients IN NUMBER default 0) ;
    
    /**
      @brief     BIQ-16667, Сохранение цен приобритения T365 . Запускается по окончании расчета
     */
      PROCEDURE SetWAPositionPrice365 (p_CalcDate  in date , p_MarketID number default -1 , p_UseListClients IN NUMBER default 0) ;

     /**
      @brief     BIQ-16667, Проверка расчитанных цен . Запускается по окончании расчета
     */
     PROCEDURE CheckWAPositionPrice ( p_calc_direct varchar2,p_CalcDate  in date ) ;
     /**
      @brief     BIQ-16667, Добавление строки 
     */
     function add_text(p_txt varchar2, p_txtadd varchar2, p_sp varchar2  default ' ,',p_maxlen integer default 512 ) return varchar2 ;
END RSHB_RSI_SCLIMIT;
/
