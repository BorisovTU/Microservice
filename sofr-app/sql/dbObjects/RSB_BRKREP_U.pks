CREATE OR REPLACE PACKAGE rsb_brkrep_u
is
/**
 * Author  : nikonorov evgeny
 * created : 07.06.2017
 * пакет rsb_brkrep_u для подготовки данных для отчета брокера */

--type tr_dealid is record (T_DEALID DDL_TICK_DBT.T_DEALID%type) ;
type tt_DDL_TICK is table of DDL_TICK_DBT%rowtype; 

--Установить запись о том, что данный договор обслуживания обрабатывался (для последующей печати по нему, даже если не было сделок)
PROCEDURE SetUsingContr( p_ClientID IN NUMBER,
                         p_ContrID  IN NUMBER,
                         p_BegDate  IN DATE,
                         p_EndDate  IN DATE
                       );
 --получить сумму НКД в сделке на корзину
FUNCTION GetBasketNKDOnDate (DealID IN NUMBER, pDate IN DATE)  RETURN NUMBER;
 --получить кол-во денежных средств на дату
FUNCTION GetRQAmountCashOnDate (pRqDocKind IN NUMBER, pRqDocID IN NUMBER, pDealPart IN NUMBER, pDate IN DATE) RETURN NUMBER;
 --получить кол-во ценных бумаг на дату
FUNCTION GetRQAmountSecuritiesOnDate (pRqDocKind IN NUMBER, pRqDocID IN NUMBER, pDealPart IN NUMBER, pDate IN DATE) RETURN NUMBER;
 --получить кол-во ТО на дату
FUNCTION GetRQAmountOnDate (pRqID IN NUMBER, pDate IN DATE)   RETURN NUMBER;
--Получить идентификатор тарифного плана по договору обслуживания на дату
FUNCTION GetSfPlanID(p_ContrID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

--Получить плановую дату исполнения части сделки (максимальная из плановых и фактических по ТО)
FUNCTION GetPlanExecDate(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN DATE deterministic;
-- Табличная функция возвращает сделки за период (производная от GetPlanExecDate)
FUNCTION SelectDealExecDate(p_BegDate date,p_EndDate date) RETURN tt_DDL_TICK pipelined ;
--Получить фактическую дату исполнения части сделки (максимальная из фактических по ТО)
FUNCTION GetExecDate(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN DATE deterministic ;

--Получить фактическую дату исполнения части сделки (максимальная из фактических по ТО) без использования календарей
FUNCTION GetExecDateWOCalendar(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN DATE deterministic;

--Получить фактическую дату исполнения оплаты
FUNCTION GetExecDatePaym(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN DATE deterministic ;

--Получить цену сделки
FUNCTION GetPrice(p_DealID IN NUMBER, p_Part IN NUMBER) RETURN FLOAT;

--Получить цену поручения
FUNCTION GetReqPrice(p_DealID IN NUMBER) RETURN FLOAT;

--Получить сумму обязательств
FUNCTION GetCommitSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER, p_SubKind IN NUMBER, p_CFI IN NUMBER default 0, p_Date IN DATE default to_date( '01010001', 'ddmmyyyy' )) RETURN NUMBER;

--Получить сумму комиссии брокера
FUNCTION GetBrokerComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER deterministic;

--Получить валюту комиссии брокера (обязательно после вызова GetBrokerComissSum)
FUNCTION GetBrokerComissFIID(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER;

--Получить сумму комиссий торговой площадке
FUNCTION GetMarketComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER, p_MarketID IN NUMBER) RETURN NUMBER deterministic;

--ak
--Получить НДС комиссии торговой площадке (обязательно после вызова GetMarketComissSum)
FUNCTION GetMarketComissNDS(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER deterministic;
--~ak

--Получить сумму комиссий клирингово центра
FUNCTION GetCliringComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER deterministic;

--ak
--Получить НДС комиссии клирингово центра (обязательно после вызова GetCliringComissSum)
FUNCTION GetCliringComissNDS(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER;

--Получить валюту комиссий (обязательно после вызова функций расчета комиссий)
FUNCTION GetMarketCliringITSComissFIID(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER;
--~ak

--Получить сумму комиссий за ИТС
FUNCTION GetITSComissSum(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER deterministic;

--ak
--Получить НДС комиссии за ИТС(обязательно после вызова GetITSComissSum)
FUNCTION GetITSComissNDS(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Part IN NUMBER) RETURN NUMBER;
--~ak

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

--Формирование данных по внутреннему учёту для раздела 6 отчета (только фактические данные)
PROCEDURE CreateInAccData( p_ClientID      IN NUMBER,
                           p_ContrID       IN NUMBER,
                           p_BegDate       IN DATE,
                           p_EndDate       IN DATE
                         );

-- Получение котировки
FUNCTION GetRate( SumB     IN NUMBER
                 ,pFromFI IN NUMBER
                 ,pToFI    IN NUMBER
                 ,pType    IN NUMBER
                 ,pbdate   IN DATE
                )
  RETURN NUMBER;

--Golovkin стоимоть бумаг в валюте цены
FUNCTION GetAvrCost( SumB     IN NUMBER
                    ,pFromFI  IN NUMBER
                    ,pType    IN NUMBER
                    ,pbdate   IN DATE
                   )
  RETURN NUMBER;

--Корректировка данных по внутреннему учёту для раздела 6 отчета с учетом плановых движений
PROCEDURE CorrectInAccData( p_ClientID      IN NUMBER,
                            p_ContrID       IN NUMBER,
                            p_BegDate       IN DATE,
                            p_EndDate       IN DATE
                          );

--Формирование данных по бумагам внесенным в пул
PROCEDURE CreatePoolData( p_ClientID      IN NUMBER,
                          p_ContrID       IN NUMBER,
                          p_BegDate       IN DATE,
                          p_EndDate       IN DATE
                        );



end rsb_brkrep_u;
/
