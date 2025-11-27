CREATE OR REPLACE PACKAGE RSB_BRKREP_RSHB_NEW
IS
--Группы данных данных
BROKERREP_PART_DEALINPERIOD CONSTANT NUMBER(5) := 1; --СДЕЛКИ, ЗАКЛЮЧЕННЫЕ В ПЕРИОД С : ПО :
BROKERREP_PART_REPOINPERIOD CONSTANT NUMBER(5) := 2; --СДЕЛКИ РЕПО, ЗАКЛЮЧЕННЫЕ В ПЕРИОД С : ПО :
BROKERREP_PART_EXECDEAL     CONSTANT NUMBER(5) := 3; --СДЕЛКИ, ЗАКЛЮЧЕННЫЕ И ИСПОЛНЕННЫЕ НА
BROKERREP_PART_EXECREPO     CONSTANT NUMBER(5) := 4; --СДЕЛКИ РЕПО, ЗАКЛЮЧЕННЫЕ И ИСПОЛНЕННЫЕ НА
BROKERREP_PART_CANCELDEAL   CONSTANT NUMBER(5) := 5; --СДЕЛКИ, ЗАКЛЮЧЕННЫЕ И ОТМЕНЕННЫЕ В ПЕРИОД
BROKERREP_PART_CANCELREPO   CONSTANT NUMBER(5) := 6; --СДЕЛКИ РЕПО, ЗАКЛЮЧЕННЫЕ И ОТМЕНЕННЫЕ В ПЕРИОД

--Виды обязательств
DEBT_INVEST_COM CONSTANT NUMBER(5) := 1; --Комиссия брокера в рамках услуги инвестиционного консультирования
DEBT_FIX_COM    CONSTANT NUMBER(5) := 2; --Минимальная брокерская комиссия
DEBT_EXPIRED    CONSTANT NUMBER(5) := 3; --Прочая просроченная задолженность
DEBT_DEAL_COM   CONSTANT NUMBER(5) := 4; --Просроченная брокерская комиссия по сделкам


/**
 @brief Получить наименования субъекта
 @param[in]  pPartyID   идентификатор клиента  
 @return псевдоним вида 4. Дополнительное наименование, если его нет то код вида 1
*/                                                                  
FUNCTION uGetPatyNameForBrkRep(pPartyID IN NUMBER)
    return varchar2;
 
/**
 @brief Установить запись о том, что данный договор обслуживания обрабатывался (для последующей печати по нему, даже если не было сделок)
 @param[in]  p_ClientID   идентификатор клиента  
 @param[in]  p_ContrID    идентификатор ДБО  
*/                                                                  
PROCEDURE SetUsingContr( p_ClientID IN NUMBER,
                         p_ContrID  IN NUMBER
                       );
                       
/**
 @brief Получить сумму НКД в сделке на корзину на дату
 @param[in]  DealID   идентификатор сделки 
 @param[in]  pDate    отчетная дата   
 @return сумма НКД
*/                                                                  
FUNCTION GetBasketNKDOnDate(DealID IN NUMBER, pDate IN DATE) RETURN NUMBER;

--Получить кол-во денежных средств на дату
FUNCTION GetRQAmountCashOnDate(pRqDocKind IN NUMBER, pRqDocID IN NUMBER, pDealPart IN NUMBER, pDate IN DATE) RETURN NUMBER;

--Получить кол-во ценных бумаг на дату
FUNCTION GetRQAmountSecuritiesOnDate(pRqDocKind IN NUMBER, pRqDocID IN NUMBER, pDealPart IN NUMBER, pDate IN DATE) RETURN NUMBER;

--Получить идентификатор тарифного плана по договору обслуживания на дату
FUNCTION GetSfPlanID(p_ContrID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

--Получить плановую дату исполнения части сделки (максимальная из плановых и фактических по ТО)
FUNCTION GetPlanExecDate(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN DATE;

--Получить фактическую дату исполнения части сделки (максимальная из фактических по ТО)
FUNCTION GetExecDate(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN DATE;

--Получить цену сделки
FUNCTION GetPrice(p_DealID IN NUMBER, p_Part IN NUMBER) RETURN FLOAT;

--Получить цену поручения
FUNCTION GetReqPrice(p_DealID IN NUMBER) RETURN FLOAT;

--Получить сумму обязательств
FUNCTION GetCommitSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER, p_SubKind IN NUMBER, p_CFI IN NUMBER default 0, p_Date IN DATE default to_date( '01010001', 'ddmmyyyy' )) RETURN NUMBER;

--Получить сумму комиссии брокера
FUNCTION GetBrokerComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER, p_CFI IN NUMBER DEFAULT -1) RETURN NUMBER;

--Получить валюту комиссии брокера (обязательно после вызова GetBrokerComissSum)
FUNCTION GetBrokerComissFIID(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER;

--Получить сумму комиссий торговой площадке
FUNCTION GetMarketComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER, p_MarketID IN NUMBER, p_DealDate IN DATE) RETURN NUMBER;

--Получить сумму комиссий торговой площадке по срочному рынку
FUNCTION DV_GetMarketComissSum(p_DealID IN NUMBER, p_MarketID IN NUMBER, p_DealDate IN DATE) RETURN NUMBER;

--Получить сумму по ТО с учетом знака
FUNCTION GetCurrentRQAmount(p_DocKInd IN NUMBER, p_DocID IN NUMBER, p_RqType IN NUMBER, p_EndDate IN DATE, p_FactDate IN DATE, p_IsBuy IN NUMBER, p_IsSale IN NUMBER, p_ToFIID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER;

--Получить валюту ТО 
FUNCTION GetCurrentRQFIID(p_DocKInd IN NUMBER, p_DocID IN NUMBER, p_RqType IN NUMBER, p_FactDate IN DATE, p_Part IN NUMBER) RETURN NUMBER;

--Получить идентификатор пула сччета
FUNCTION GetAccPoolID(p_ContrID IN NUMBER, p_Account IN VARCHAR2, p_Code_Currency IN NUMBER, p_Chapter IN NUMBER) RETURN NUMBER;

--Получить сумму проводок по операциям передачи в пул/возврата из пула по конкретному счету
FUNCTION GetPoolAccTrnSum(p_AccountID IN NUMBER, p_Debet IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE) RETURN NUMBER;

--Получить вид кода субъекта на бирже для конкретной биржи
FUNCTION GetPtCodeKindForMarket(p_MarketID IN NUMBER) RETURN NUMBER;
                                                                                                                        
--Получить плановый остаток по счету
FUNCTION GetPlanRestAcc(p_DlContrID IN NUMBER, p_Account IN VARCHAR2, p_Chapter IN NUMBER, p_Code_Currency IN NUMBER, p_EndDate IN DATE) RETURN NUMBER deterministic;

--Получить Id курса ФИ для определения рыночной стоимости на дату
FUNCTION GetActiveRateId(p_FIID IN NUMBER, p_Date IN DATE, p_IsEDP IN NUMBER) RETURN NUMBER deterministic result_cache ;

/**
 @brief Получить наименование инструмента по сделке валютного рынка
 @param[in]  p_DealDate   дата заключения  
 @param[in]  p_ExDealDate дата заполнения сделки 
 @param[in]  p_IsSwap     признак свопа 
 @param[in]  p_DealPart   часть сделки 
 @param[in]  p_BaseCCY    ISO-код валюты базового актива 
 @param[in]  p_ContrCCY   ISO-код валюты контрактива 
 @return строковое наименование инструмента по сделке
*/                                                                  
FUNCTION GetInstrName(p_DealDate IN DATE, p_ExDealDate IN DATE, p_IsSwap IN NUMBER, p_DealPart IN NUMBER, p_BaseCCY IN VARCHAR2, p_ContrCCY IN VARCHAR2) RETURN VARCHAR2;                        
                        
/**
 @brief Получить сумму комиссии брокера по сделке валютного рынка
 @param[in]  p_DealID  идентификатор сделки 
 @param[in]  p_DocKind вид документа 
 @param[in]  p_ToFIID  целевая валюта комиссии 
 @return сумма комиссии в заданной валюте
*/                                           
FUNCTION GetBrokerComissSumCurMarket(p_DealID IN NUMBER, p_DocKind IN NUMBER, p_ToFIID IN NUMBER) RETURN NUMBER; 

/**
 @brief Получить сумму комиссии брокера по сделке срочного рынка
 @param[in]  p_DealID  идентификатор сделки 
 @param[in]  p_Date    дата курса 
 @param[in]  p_ToFIID  целевая валюта комиссии 
 @return сумма комиссии в заданной валюте
*/                      
FUNCTION GetBrokerComissSumDvMarket(p_DealID IN NUMBER, p_Date IN DATE, p_ToFIID IN NUMBER) RETURN NUMBER;                       

/**
 @brief Загрузить счета во временную таблицу
 @param[in]  p_DlContrID     идентификатор ДБО 
 @param[in]  p_BegDate       начало периода 
 @param[in]  p_EndDate       конец периода
 @param[in]  p_IsEDP         признак счета ЕДП 
 @param[in]  p_ByOutExchange признак сбора информации по биржевому разделу
 @param[in]  p_ByExchange    признак необходимости расчета плановых остатков   
*/                      
PROCEDURE LoadAccInTmp( p_DlContrID     IN NUMBER,
                        p_BegDate       IN DATE,
                        p_EndDate       IN DATE,
                        p_IsEDP         IN NUMBER,
                        p_ByOutExchange IN NUMBER,
                        p_NeedPlanRest  IN NUMBER
                      );
                                                                                                                        
/**
 @brief Формирование данных по сделкам фондового рынка
 @param[in]  p_DlContrID     идентификатор ДБО 
 @param[in]  p_BegDate       начало периода 
 @param[in]  p_EndDate       конец периода
 @param[in]  p_ByExchange    признак сбора информации по биржевому разделу 
 @param[in]  p_ByOutExchange признак сбора информации по биржевому разделу  
 @param[in]  p_IsEDP         признак счета ЕДП 
*/                      
PROCEDURE CreateDealData( p_DlContrID     IN NUMBER,
                          p_BegDate       IN DATE,
                          p_EndDate       IN DATE,
                          p_ByExchange    IN NUMBER,
                          p_ByOutExchange IN NUMBER,
                          p_IsEDP         IN NUMBER
                        );
                        
/**
 @brief Формирование данных по сделкам валютного рынка
 @param[in]  p_DlContrID  идентификатор ДБО 
 @param[in]  p_BegDate    начало периода 
 @param[in]  p_EndDate    конец периода 
 @param[in]  p_IsEDP      признак счета ЕДП 
*/                      
PROCEDURE CreateCurDealData( p_DlContrID     IN NUMBER,
                             p_BegDate       IN DATE,
                             p_EndDate       IN DATE,
                             p_IsEDP         IN NUMBER
                           );

/**
 @brief Формирование данных по сделкам срочного рынка
 @param[in]  p_DlContrID  идентификатор ДБО 
 @param[in]  p_BegDate    начало периода 
 @param[in]  p_EndDate    конец периода 
 @param[in]  p_IsEDP      признак счета ЕДП 
*/                      
PROCEDURE CreateDvDealData( p_DlContrID      IN NUMBER, 
                            p_BegDate        IN DATE, 
                            p_EndDate        IN DATE,
                            p_IsEDP          IN NUMBER
                          );                       

/**
 @brief Формирование данных для раздела Оценка позиции по ЦБ и НЕФИ с учетом плановых движений
 @param[in]  p_DlContrID  идентификатор ДБО 
 @param[in]  p_BegDate    начало периода 
 @param[in]  p_EndDate    конец периода 
 @param[in]  p_IsEDP      признак счета ЕДП 
*/
PROCEDURE CreateActiveData( p_DlContrID     IN NUMBER, 
                            p_BegDate       IN DATE,
                            p_EndDate       IN DATE,
                            p_IsEDP         IN NUMBER
                          );
                           
/**
 @brief Формирование данных по ФИССиКО для раздела Оценка позиции по ПФИ
 @param[in]  p_DlContrID  идентификатор ДБО 
 @param[in]  p_BegDate    начало периода 
 @param[in]  p_EndDate    конец периода 
 @param[in]  p_IsEDP      признак счета ЕДП 
*/  
PROCEDURE CreateActiveDerivData( p_DlContrID     IN NUMBER, 
                                 p_BegDate       IN DATE,
                                 p_EndDate       IN DATE,
                                 p_IsEDP         IN NUMBER
                               );

/**
 @brief Формирование данных по движению д/с
 @param[in]  p_BegDate    начало периода 
 @param[in]  p_EndDate    конец периода 
 @param[in]  p_IsEDP      признак счета ЕДП 
*/  
PROCEDURE CreateCasheMoveData( p_BegDate  IN DATE,
                               p_EndDate  IN DATE,
                               p_IsEDP    IN NUMBER
                             );
                             
/**
 @brief Формирование данных по оценке денежной позиции
 @param[in]  p_BegDate    начало периода 
 @param[in]  p_EndDate    конец периода 
 @param[in]  p_IsEDP      признак счета ЕДП 
*/       
PROCEDURE CreateAccMoveData( p_BegDate  IN DATE,
                             p_EndDate  IN DATE,
                             p_IsEDP    IN NUMBER
                           );
                                                      
/**
 @brief Формирование данных по обязательствам перед банком
 @param[in]  p_DlContrID  идентификатор ДБО
 @param[in]  p_BegDate    начало периода  
 @param[in]  p_EndDate    конец периода 
*/ 
PROCEDURE CreateDebtData(p_DlContrID IN NUMBER, p_BeginDate IN DATE, p_EndDate IN DATE);

/**
 @brief Формирование сводной информации
 @param[in]  p_BegDate    начало периода 
 @param[in]  p_EndDate    конец периода 
*/   
PROCEDURE CreateSvodInfoData(p_BegDate IN DATE, p_EndDate IN DATE);

/**
 @brief Формирование данных по курсам валют
 @param[in]  p_BegDate    начало периода  
 @param[in]  p_EndDate    конец периода 
*/ 
PROCEDURE CreateCoursesData(p_BeginDate IN DATE, p_EndDate IN DATE);

/**
 @brief Получить шаблон письма об ошибках
 @param[in]   p_TemplID   номер шаблона 
 @param[out]  p_Subject   заголовок письма  
 @param[out]  p_Body      тело письма   
 @param[out]  p_err       номер ошибки    
 @return сумма НКД
*/                                                                  
PROCEDURE GetErrMailTempl(p_TemplID IN NUMBER, p_Subject OUT CLOB, p_Body OUT CLOB, p_err OUT NUMBER);

/**
 @brief Получение ФИО подписанта отчёта
 @param[in] p_SignerParty идентификатор подписанта 
 @return ФИО подписанта
*/                                                                  
FUNCTION GetSignerName(p_SignerParty IN NUMBER) RETURN VARCHAR2;

/**
 @brief Получение количества выводимых десятичных знаков
 @param[in] p_Amount десятичное число 
 @return количество выводимых десятичных знаков
*/                                                                  
FUNCTION AmountPrecision(p_Amount IN NUMBER, p_DecimalPlaces IN NUMBER default 6) RETURN NUMBER;

end RSB_BRKREP_RSHB_NEW;
/