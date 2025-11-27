CREATE OR REPLACE PACKAGE IT_BrokerReport
IS
  /*Виды кодов субъектов*/
  PTCK_CFT  CONSTANT NUMBER(5) := 101; /*Код ЦФТ*/

  OBJTYPE_BRKREP_HEADERS constant number := 4190; --Справочник статических header-ов IPS по формир. ОБ
  
  OBJTYPE_BROKERCONTR_DL CONSTANT NUMBER(5) := 207;

  NOTEKIND_DEPO_OWNER constant number := 104;
  NOTEKIND_DEPO_OWNER_SECTION_MMVB constant number := 106;
  NOTEKIND_DEPO_OWNER_SECTION_SPB constant number := 115;

  /**
   @brief Подготовка отчетных данных
   @param[in]  p_DlContrID     идентификатор ДБО 
   @param[in]  p_BegDate       начало периода 
   @param[in]  p_EndDate       конец периода
   @param[in]  p_ByExchange    признак сбора информации по биржевому разделу 
   @param[in]  p_ByOutExchange признак сбора информации по внебиржевому разделу  
   @param[in]  p_IsEDP         признак счета ЕДП 
  */                      
  PROCEDURE CreateAllData( p_DlContrID     IN NUMBER,
                           p_BegDate       IN DATE,  
                           p_EndDate       IN DATE,
                           p_ByExchange    IN NUMBER,
                           p_ByOutExchange IN NUMBER,
                           p_IsEDP         IN NUMBER 
                         );

  --Получение ЕКК кода клиента по идентификатору договора
  FUNCTION GetClientCode(p_DlContrID IN NUMBER)
    RETURN VARCHAR2;

  --Получение ФИО клиента по его идентификатору
  FUNCTION GetClientName(p_PartyID IN NUMBER)
    RETURN VARCHAR2;

  --Получение даты заключения договора по его идентификатору
  FUNCTION GetContrDate(p_DlContrID IN NUMBER)
    RETURN DATE;
    
  /* Получение текущих параметров фин.инструмента */
  FUNCTION GetCurrentParamFIID(p_FIID in number, p_date in date) RETURN varchar2;
    
  --Получение идентификатора подписанта по-умолчанию
  FUNCTION GetDefaultSigner(p_ErrorText OUT VARCHAR2)
    RETURN NUMBER; 

  --Получение JSON по блоку "Сводная информация"
  FUNCTION GetSummaryInformationBlock(p_IsOtc IN NUMBER)
    RETURN CLOB;

  --Получение JSON по блоку "Курсы валют"
  FUNCTION GetRateExchangeList(p_IsOtc IN NUMBER)
    RETURN CLOB;

  --Получение JSON по блоку "Оценка денежной позиции"
  FUNCTION GetCashPositionValuationList(p_IsOtc IN NUMBER)
    RETURN CLOB;
    
  FUNCTION GetExchangePlatform(p_IsOtc IN NUMBER)
    RETURN CLOB;    

  --Получение JSON по блоку "Обязательства перед банком"
  FUNCTION GetCommitmentBlock(p_OnDate IN DATE)
    RETURN CLOB;

  --Получение формата числа
  FUNCTION GetFormatSample(p_Amount IN NUMBER)
    RETURN VARCHAR2;

  --Получение JSON по блоку "Оценка позиций по ценным бумагам и иностранным финансовым инструментам, не квалифицированным в качестве ценных бумаг"
  FUNCTION GetSecuritiesValuationBlock(p_IsOtc IN NUMBER)
    RETURN CLOB;

  --Получение JSON по блоку "Оценка позиций по Производным финансовым инструментам"
  FUNCTION GetDerivativeFinancialInstrumentBlock
    RETURN CLOB;

  --Получение JSON по блоку "Движения по счету"
  FUNCTION GetAccountMovementsList(p_IsOtc IN NUMBER)
    RETURN CLOB;

  --Получение JSON по блокам "Сделки на Фондовом рынке" и "Сделки на Внебиржевом рынке" в зависимости от раздела
  FUNCTION GetStockMarketOrOtcTransactionsBlock(p_IsOtc IN NUMBER, p_Mode IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE, p_MarketId in Number default 0, p_NumPart in Number default 3)
    RETURN CLOB;

  --Получение JSON по блокам "Сделки на Срочном рынке" в зависимости от раздела
  FUNCTION GetDerivativesMarketTransactionsBlock(p_Mode IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE)
    RETURN CLOB;

  --Получение JSON по блокам "Сделки на Валютном рынке" в зависимости от раздела
  FUNCTION GetForeignExchangeMarketTransactionsBlock(p_Mode IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE)
    RETURN CLOB;
    
  FUNCTION GetTradingResultsPortfolioBlock(p_Mode IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE, p_DlContrID IN NUMBER, p_marketid IN NUMBER, pNumStr in NUMBER)
    RETURN CLOB;

  FUNCTION GetAccountMovementsList_UL(p_IsOtc IN NUMBER)
    RETURN CLOB;

  FUNCTION GetCashPositionValuationList_UL(p_IsOtc IN NUMBER)
    RETURN CLOB;
    
  FUNCTION OtcCashPositionValuationTotalOTC
    RETURN CLOB;
    
  /**
   @brief Обработчик SINV.GetBrokerageReportInfo
          BOSS-3575. Интеграция брокерского отчета в мобильное приложение Свои Инвестиции
          Включает получение параметров отчета брокера из входщего JSON-сообщения, их проверку, 
          сбор и расчет данных для отчета брокера и формирование на их основании исходящего JSON-сообщения
   @param[in]  p_worklogid необходимо для QManager, можно не использовать внутри процедуры 
   @param[in]  p_messbody  тело сообщения (сюда приходит JSON-сообщение) 
   @param[in]  p_messmeta  мета-данные из KAFKA; необходимо для QManager, можно не использовать внутри процедуры
   @param[out] o_msgid     GUID ответного сообщения 
   @param[out] o_MSGCode   код ошибки  
   @param[out] o_MSGText   текст ошибки 
   @param[out] o_messbody  исходящее JSON-сообщение (ответ) 
   @param[out] o_messmeta  исходящие мета-данные для KAFKA 
  */                      
  PROCEDURE GetReportFromSINV(p_worklogid integer     
                             ,p_messbody  clob        
                             ,p_messmeta  xmltype     
                             ,o_msgid     out varchar2
                             ,o_MSGCode   out integer 
                             ,o_MSGText   out varchar2
                             ,o_messbody  out clob    
                             ,o_messmeta  out xmltype);
                                
  /**
   @brief Cбор и расчет данных для Отчета Брокера для ДБО ЮЛ
   @param[in]  p_buf_rec запись таблицы duserlebrokrepreq_dbt
   @param[in]  p_isOTC 1 - внебиржа, 0 - биржа
   @param[out] o_json_resp сформированный json с данными отчета
   @return текст ошибки/ОК
  */
  FUNCTION BrokerReportRun(p_buf_rec in duserlebrokrepreq_dbt%rowtype,
                           p_isOTC in number, 
                           o_json_resp out CLOB) RETURN varchar2;
                             
end IT_BrokerReport;
/