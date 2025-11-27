CREATE OR REPLACE package rsb_brkrep
is
/**
 * Author  : nikonorov evgeny
 * created : 07.06.2017
 * пакет rsb_brkrep для подготовки данных для отчета брокера */

--Установить запись о том, что данный договор обслуживания обрабатывался (для последующей печати по нему, даже если не было сделок)
PROCEDURE SetUsingContr( p_ClientID IN NUMBER,
                         p_ContrID  IN NUMBER
                       );
 --получить сумму НКД в сделке на корзину
FUNCTION GetBasketNKDOnDate (DealID IN NUMBER, pDate IN DATE)  RETURN NUMBER;
 --получить кол-во денежных средств на дату
FUNCTION GetRQAmountCashOnDate (pRqDocKind IN NUMBER, pRqDocID IN NUMBER, pDealPart IN NUMBER, pDate IN DATE) RETURN NUMBER;
 --получить кол-во ценных бумаг на дату
FUNCTION GetRQAmountSecuritiesOnDate (pRqDocKind IN NUMBER, pRqDocID IN NUMBER, pDealPart IN NUMBER, pDate IN DATE) RETURN NUMBER;
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
FUNCTION GetBrokerComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER;

--Получить валюту комиссии брокера (обязательно после вызова GetBrokerComissSum)
FUNCTION GetBrokerComissFIID(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER;

--Получить сумму комиссий торговой площадке
FUNCTION GetMarketComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER, p_MarketID IN NUMBER) RETURN NUMBER;

--Получить сумму комиссий клирингово центра
FUNCTION GetCliringComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER;

--Получить сумму комиссий за ИТС
FUNCTION GetITSComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER;

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
                                                                                                                        
--Получить Id курса ФИ для определения рыночной стоимости на дату
FUNCTION GetActiveRateId(p_FIID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

--Получить балансовую стоимость бумаг клиента по договору
FUNCTION GetActiveBalanceCost( p_FIID IN NUMBER, p_Date IN DATE, p_ClientId IN NUMBER, p_ContrId IN NUMBER) RETURN NUMBER;

--Формирование данных по сделкам для разделов 1-3 отчета
PROCEDURE CreateDealData( p_ClientID      IN NUMBER,
                          p_ContrID       IN NUMBER,
                          p_Part          IN NUMBER,
                          p_BegDate       IN DATE,
                          p_EndDate       IN DATE,
                          p_ByExchange    IN NUMBER,
                          p_ByOutExchange IN NUMBER
                        );

--Формирование данных по компенсационным выплатам, компенсационным поставкам, купонным выплатам для раздела 4
PROCEDURE CreateCompData( p_ClientID      IN NUMBER,
                          p_ContrID       IN NUMBER,
                          p_Part          IN NUMBER,
                          p_BegDate       IN DATE,
                          p_EndDate       IN DATE,
                          p_ByExchange    IN NUMBER,
                          p_ByOutExchange IN NUMBER
                        );

--Формирование данных по компенсационным выплатам, компенсационным поставкам, купонным выплатам для раздела 5
PROCEDURE CreateRetireData( p_ClientID      IN NUMBER,
                            p_ContrID       IN NUMBER,
                            p_Part          IN NUMBER,
                            p_BegDate       IN DATE,
                            p_EndDate       IN DATE
                          );

--Формирование данных по внутреннему учёту для раздела 7 отчета (только фактические данные)
PROCEDURE CreateInAccData( p_ClientID      IN NUMBER,
                           p_ContrID       IN NUMBER,
                           p_BegDate       IN DATE,
                           p_EndDate       IN DATE
                         );

--Корректировка данных по внутреннему учёту для раздела 7 отчета с учетом плановых движений
PROCEDURE CorrectInAccData( p_ClientID      IN NUMBER,
                            p_ContrID       IN NUMBER,
                            p_EndDate       IN DATE
                          ); 

--Формирование данных по бумагам внесенным в пул
PROCEDURE CreatePoolData( p_ClientID      IN NUMBER,
                          p_ContrID       IN NUMBER,
                          p_BegDate       IN DATE,
                          p_EndDate       IN DATE
                        ); 

--Получить шаблон письма об ошибках
PROCEDURE GetErrMailTempl(p_TemplID IN NUMBER, p_Subject OUT CLOB, p_Body OUT CLOB, p_err OUT NUMBER);


end rsb_brkrep;
/
