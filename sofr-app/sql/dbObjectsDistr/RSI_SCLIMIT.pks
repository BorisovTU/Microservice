CREATE OR REPLACE PACKAGE RSI_SCLIMIT AS

   UnknownDate  CONSTANT DATE   := TO_DATE( '01.01.0001', 'DD.MM.YYYY' );
   UnknownTime  CONSTANT DATE   := TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS');

/**
 * Виды рынка */
  MARKET_KIND_STOCK CONSTANT INTEGER :=  1; --фондовый
  MARKET_KIND_DERIV CONSTANT INTEGER :=  2; --срочный
  MARKET_KIND_CURR  CONSTANT INTEGER :=  3; --валютный

/**
 * MARKET в таблице корректировок лимитов QUIK */
  MARKET_STOCK_OUT CONSTANT INTEGER :=  0; --фондовый на внебиржевом
  MARKET_STOCK_EX  CONSTANT INTEGER :=  1; --фондовый на биржевом
  MARKET_DERIV     CONSTANT INTEGER :=  2; --срочный
  MARKET_CURR      CONSTANT INTEGER :=  3; --валютный

/**
 * Статусы выгрузки корректировки лимитов QUIK */
  LIMITSTATUS_UNDEF    CONSTANT INTEGER :=  0; -- пусто
  LIMITSTATUS_WAIT     CONSTANT INTEGER :=  1; -- Ждет
  LIMITSTATUS_UNLOADED CONSTANT INTEGER :=  2; -- Выгружена
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

  ts_       TIMESTAMP;

/**
 * Добавить корректировку лимита на шаге (DL_LIMITADJUST)
 * @since 6.20.031
 * @qtest NO
 * @param RecLimitAdj стуктура корректировки лимита
 * @param ID_Operation
 * @param ID_Step
 */

   PROCEDURE RSI_CreateLimitAdJust(RecLimitAdj IN RAW,
                                   ID_Operation IN NUMBER,
                                   ID_Step IN NUMBER);

/**
 * Удалить корректировку лимита на шаге (DL_LIMITADJUST)
 * @since 6.20.031
 * @qtest NO
 * @param ID_Operation
 * @param ID_Step
 */
   PROCEDURE RSI_RestoreLimitAdJust(ID_Operation IN NUMBER,
                                    ID_Step IN NUMBER);



/**
 * Проверить, нужно ли включать сделку в расчет неисполненных ТО
 * @since 6.20.031
 * @qtest NO
 * @param p_CalcDate Дата расчета
 * @param p_DealID   Идентификатор сделки
 * @return 0 - не нужно, 1 - нужно
 */
   FUNCTION UseNotExecRQbyDeal(p_CalcDate IN DATE,
                               p_DealID IN NUMBER) RETURN NUMBER;


/**
 * Получить сумму неисполненных денежных ТО
 * @since 6.20.031
 * @qtest NO
 * @param p_Client        Идентификатор клиента
 * @param p_ClientContrID Идентификатор договора клиента
 * @param p_ServKindSub   подвид обслуживания фондового дилинга
 * @param p_CalcDate      Дата расчета
 * @param p_Kind          Вид лимита
 * @param p_AccountID     Идентификатор л/с
 * @param p_ToFI          Валюта, в которую необходимо конвертировать
 * @param p_IsReq         Признак видов отбираемых ТО: 0 - обязательства, 1 - требования
 * @param p_MarketID      Биржа
 * @return Сумма ТО в валюте p_ToFI
 */
   FUNCTION GetSumPlanCashRQ(p_Client IN NUMBER,
                             p_ClientContrID IN NUMBER,
                             p_ServKindSub IN NUMBER,
                             p_CalcDate IN DATE,
                             p_Kind IN INTEGER,
                             p_AccountID IN NUMBER,
                             p_ToFI IN NUMBER,
                             p_IsReq IN NUMBER,
                             p_MarketID IN NUMBER) RETURN NUMBER;

/**
 * Получить сумму неисполненных денежных ТО
 * @since 6.20.031
 * @qtest NO
 * @param p_Client        Идентификатор клиента
 * @param p_ClientContrID Идентификатор договора клиента
 * @param p_CalcDate      Дата расчета
 * @param p_Kind          Вид лимита
 * @param p_AccountID     Идентификатор л/с
 * @param p_FIID          FIID
 * @param p_IsReq         Признак видов отбираемых ТО: 0 - обязательства, 1 - требования
 * @return Сумма ТО
 */
   FUNCTION GetSumPlanCashPM(p_Client IN NUMBER,
                             p_ClientContrID IN NUMBER,
                             p_CalcDate IN DATE,
                             p_Kind IN INTEGER,
                             p_Account IN VARCHAR2,
                             p_FIID IN NUMBER,
                             p_IsReq IN NUMBER) RETURN NUMBER;

/**
 * Получить сумму неисполненных ТО по ценным бумагам
 * @since 6.20.031
 * @qtest NO
 * @param p_Client        Идентификатор клиента
 * @param p_ClientContrID Идентификатор договора клиента
 * @param p_ServKindSub   подвид обслуживания фондового дилинга
 * @param p_CalcDate      Дата расчета
 * @param p_Kind          Вид лимита
 * @param p_FIID          Идентификатор ц/б
 * @param p_IsReq         Признак видов отбираемых ТО: 0 - обязательства, 1 - требования
 * @return Сумма ТО
 */
   FUNCTION GetSumPlanAvrRQ(p_Client IN NUMBER,
                            p_ClientContrID IN NUMBER,
                            p_ServKindSub IN NUMBER,
                            p_CalcDate IN DATE,
                            p_CheckDate IN DATE,
                            p_FIID IN NUMBER,
                            p_IsReq IN NUMBER,
                            p_MarketID IN NUMBER) RETURN NUMBER;

/**
 * Получить сумму комиссий банка по сделкам, заключенным в предыдущий день
 * @since 6.20.031
 * @qtest NO
 * @param p_Client        Идентификатор клиента
 * @param p_ClientContrID Идентификатор договора клиента
 * @param p_AccountID     Идентификатор л/с
 * @param p_ToFI          Валюта, в которую необходимо конвертировать
 * @param p_CalcDate      Дата расчета
 * @return Сумма комиссий в валюте p_ToFI
 */
   FUNCTION GetSumComPrevious(p_Client IN NUMBER,
                              p_ClientContrID IN NUMBER,
                              p_AccountID IN NUMBER,
                              p_ToFI IN NUMBER,
                              p_CalcDate IN DATE,
                              p_MarketID IN NUMBER) RETURN NUMBER;

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
   FUNCTION GetSumGuarantyPrevious(p_Client IN NUMBER, p_ClientContrID IN NUMBER, p_Department IN NUMBER, p_CalcDate IN DATE, p_PrevWorkDate IN DATE, p_ToFI IN NUMBER) RETURN NUMBER;

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
   FUNCTION GetSumFutureComPrevious(p_Client IN NUMBER, p_ClientContrID IN NUMBER, p_Department IN NUMBER, p_CalcDate IN DATE, p_PrevWorkDate IN DATE, p_AccCode_Currency IN NUMBER, p_ToFI IN NUMBER) RETURN NUMBER;

/**
 * Выполнить утренний расчет лимитов
 * @since 6.20.031
 * @qtest NO
 * @param p_MarketID   ID биржи
 * @param p_MarketCode Код биржи
 * @param p_CalcDate          Дата расчета
 * @param p_ByStock    Признак расчета по фондовому рынку
 * @param p_ByCurr     Признак расчета по валютному рынку
 * @param p_ByDeriv    Признак расчета по срочному рынку
 */
   PROCEDURE RSI_CreateLimits(p_MarketID IN NUMBER, p_MarketCode IN VARCHAR2, p_CalcDate IN DATE, p_ByStock IN NUMBER, p_ByCurr IN NUMBER, p_ByDeriv IN NUMBER);

/**
 * Создать записи корректировки лимитов QUIK по операции списания\зачисления ДС (на шаге корректировки)
 * @since 6.20.031
 * @qtest NO
 * @param DocID   ID записи из NPTXOP
 * @param ID_Operation
 * @param ID_Step
 */
   PROCEDURE RSI_CrLimitAdJNptxWrt(DocID IN NUMBER,
                                   ID_Operation IN NUMBER,
                                   ID_Step IN NUMBER
                                  );

/**
 * Выполнить откат созданного в RSI_CrLimitAdJNptxWrt
 * @since 6.20.031
 * @qtest NO
 * @param DocID   ID записи из NPTXOP
 * @param ID_Operation
 * @param ID_Step
 */
   PROCEDURE RSI_RestoreLimitAdJNptxWrt(DocID IN NUMBER,
                                        ID_Operation IN NUMBER,
                                        ID_Step IN NUMBER
                                       );
/**
 * получить дату T1,T2 для валюты
 * @since 6.20.031
 * @qtest NO
 * @param CalcDate   дата расчета
 * @param Days количество дней от даты расчета 1 или2
 * @param FIID валюта для которой считаем лимит
 * @param CurID   валюта расчетной оргнизации
 * @param CalendarID календать расчетной организации
 * @param IsCur признак валютного рынка
 * @return дата лимита
 */
   FUNCTION GetLimitDateKind( CalcDate IN DATE,
                              Days IN INTEGER,
                              FIID IN INTEGER,
                              CurID IN INTEGER,
                              CalendarID IN INTEGER,
                              IsCur IN INTEGER )RETURN DATE;
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
   FUNCTION RSI_GetLastDateCalc(p_MarketKind IN NUMBER, p_Market IN VARCHAR2) RETURN DATE;

   PROCEDURE RSI_DeleteLog(p_MarketID IN NUMBER, p_CondProtocol IN CHAR, p_DateProtocol IN DATE);
/**
 * очистить истории лимитов
 * @since 6.20.031
 * @qtest NO
 * @param p_ByStock удалять ли историю по фондовому рынку
 * @param p_CondStock условие для даты
 * @param p_DateStock дата
 * @param p_ByCurr удалять ли историю по валютному рынку
 * @param p_CondCurr условие для даты
 * @param p_DateCurr дата
 * @param p_ByDeriv удалять ли историю по срочному рынку
 * @param p_CondDeriv условие для даты
 * @param p_DateDeriv дата
 */
   PROCEDURE RSI_ClearLimitHistory(  p_MarketID     IN NUMBER,
                                     p_Market       IN VARCHAR2,
                                     p_ByStock      IN NUMBER,
                                     p_CondStock    IN CHAR,
                                     p_DateStock    IN DATE,
                                     p_ByCurr       IN NUMBER,
                                     p_CondCurr     IN CHAR,
                                     p_DateCurr     IN DATE,
                                     p_ByDeriv      IN NUMBER,
                                     p_CondDeriv    IN CHAR,
                                     p_DateDeriv    IN DATE,
                                     p_CondProtocol IN CHAR,
                                     p_DateProtocol IN DATE);


   FUNCTION RSI_InsertLimitHist(DocKind IN NUMBER, MarketID IN NUMBER, CurDate IN DATE, Oper IN NUMBER) RETURN NUMBER;

   FUNCTION GetSumComPrevious(p_ClientContrID IN NUMBER, p_CalcDate IN DATE, p_Kind IN INTEGER, p_Currency IN NUMBER, p_MarketID IN NUMBER) RETURN NUMBER;

   FUNCTION GetSumComPrevious_1(p_ClientContrID IN NUMBER, p_CalcDate IN DATE, p_Currency IN NUMBER, p_MarketID IN NUMBER) RETURN NUMBER;

   FUNCTION GetSumDebAndCredCash(p_Account IN VARCHAR2, p_CalcDate IN DATE, p_SubKind_Oper IN NUMBER) RETURN NUMBER;

   PROCEDURE RSI_CorBeforeRecon (p_CalcDate     IN DATE,
                                  p_MarketID     IN INTEGER,
                                  p_MarketCode   IN VARCHAR2,
                                  p_CheckSecur   IN CHAR,
                                  p_CheckCurr    IN CHAR);
   FUNCTION GetObjCodeOnDate (pFIID         IN NUMBER,
                             pObjectType   IN NUMBER,
                             pCodeKind     IN NUMBER,
                             pDate         IN DATE)
     RETURN VARCHAR2;

   FUNCTION GetSumCorr(p_Client IN NUMBER, p_FirmID IN VARCHAR2, p_Department IN NUMBER, p_CalcDate IN DATE, p_PrevWorkDate IN DATE, p_ToFI IN NUMBER) RETURN NUMBER;

   FUNCTION GetWAPositionPrice (p_CalcDate    IN DATE,
                                 p_Client      IN NUMBER,
                                 p_SfContrID   IN NUMBER,
                                 p_FIID        IN NUMBER,
                                 p_ClientCode  IN VARCHAR2,
                                 p_SecCode     IN VARCHAR2,
                                 p_FirmID      IN VARCHAR2,
                                 p_Limit_Kind  IN NUMBER,
                                 p_MarketID    IN NUMBER)
       RETURN NUMBER;

   PROCEDURE RSI_CreateSecurLimits(p_CalcDate IN DATE, p_ByMarket IN NUMBER, p_ByOutMarket IN NUMBER, p_DepoAcc IN VARCHAR2,
                                    p_MarketCode      IN VARCHAR2, p_MarketID     IN NUMBER, p_mainsessionid   IN NUMBER DEFAULT NULL);
   PROCEDURE RSI_CreateSecurLimByKind(p_start_id        IN NUMBER,
                                       p_end_id          IN NUMBER,
                                       p_CalcDate        IN DATE,
                                       p_ByMarket        IN NUMBER,
                                       p_ByOutMarket     IN NUMBER,
                                       p_DepoAcc         IN VARCHAR2,
                                       p_MarketCode      IN VARCHAR2,
                                       p_MarketID     IN NUMBER,
                                       p_RootSessionID   IN NUMBER,
                                       p_MainSessionID   IN NUMBER);
   PROCEDURE RSI_CreateCashStockLimByKind(p_start_id        IN NUMBER,
      p_end_id          IN NUMBER,
      p_CalcDate        IN DATE,
      p_ByMarket        IN NUMBER,
      p_ByOutMarket     IN NUMBER,
      p_MarketCode      IN VARCHAR2,
      p_MarketID     IN NUMBER,
      p_RootSessionID   IN NUMBER DEFAULT NULL,
      p_MainSessionID   IN NUMBER DEFAULT NULL);
   PROCEDURE RSI_CreateCashStockLimits(p_CalcDate IN DATE, p_ByMarket IN NUMBER, p_ByOutMarket IN NUMBER,p_MarketCode      IN VARCHAR2,
      p_MarketID     IN NUMBER, p_mainsessionid   IN NUMBER DEFAULT NULL);

   FUNCTION GetLastBalanceDay(p_CalcDate IN DATE, p_DayOffset IN INTEGER, p_DocKind IN NUMBER, p_DocID IN NUMBER, p_IdentProgram IN NUMBER, p_objType IN NUMBER) RETURN DATE;

   FUNCTION GetObjAtCor( p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE,
                           p_Object     IN dobjatcor_dbt.t_Object%TYPE,
                           p_GroupID    IN dobjatcor_dbt.t_GroupID%TYPE,
                           p_Date       IN dobjatcor_dbt.t_ValidFromDate%TYPE )
     RETURN dobjattr_dbt.t_AttrID%TYPE;

   FUNCTION GetKindMarketCodeOrNote (pMarketCode IN VARCHAR2, IsSecCode IN NUMBER, IsTradeaccID IN NUMBER)
     RETURN NUMBER;
END RSI_SCLIMIT;
/
