CREATE OR REPLACE PACKAGE RSI_NPTO IS /*Спецификация пакета RSI_NPTO*/

/**
  * константы занчений категории субъекта "способ определения расчетной цены при отсутствии котировок" */
    CLIENT_ESTIMATE    CONSTANT NUMBER := 1;    --Оценщик
    CLIENT_CALCULATE   CONSTANT NUMBER := 2;    --Расчет

    PTSK_STOCKDL       CONSTANT NUMBER := 1;    --фондовый дилинг
    PTSK_DEPOS         CONSTANT NUMBER := 7;    --Депозитарное обслуживание
    SERVISE_CONTR      CONSTANT NUMBER := 6020; --Договор обслуживания
    PTSK_DV            CONSTANT NUMBER := 15;   --Срочные контракты (ФИССИКО)

    --Тип договора ИИС
    DLCONTR_IISTYPE_IIS  CONSTANT NUMBER := 1;  --ИИС
    DLCONTR_IISTYPE_IIS3 CONSTANT NUMBER := 2;  --ИИС-III

    g_StartSTBDate     DATE := NULL;
    g_StartProgressScaleDate DATE := NULL;

    DL_SNOBVER     CONSTANT NUMBER (5):= 4650;
    DL_SVERNOBNDFL CONSTANT NUMBER (5):= 4653;
/**
 * расширенный рекорд сделки */
    TYPE R_Deal IS RECORD
    (
      Tick   ddl_tick_dbt%ROWTYPE,
      FI     dfininstr_dbt%ROWTYPE,
      DvNDeal  ddvndeal_dbt%ROWTYPE,
      DZ     DATE,
      OGrp   NUMBER,
      Price    NUMBER(32,12)
    );

/**
 * рекорд рассчитанных рыночных параметров */
    TYPE R_MarketPrice IS RECORD
    (
      MarketPrice      NUMBER(32,12), -- цена для расчета отклонений
      MinMarketPrice   NUMBER(32,12), 
      MaxMarketPrice   NUMBER(32,12), 
      Market           VARCHAR2(255),  -- источник этого курса
      DateMarket       DATE,          -- дата курса
      IfMarket         VARCHAR2(1),   -- признак, является ли ценная бумага обращающейся (только для сделок продажи, не являющихся срочными)
      ErrorMsg         VARCHAR2(255),
      FIID             NUMBER(10),
      DZ               DATE,  
      FI_KIND          NUMBER(5),
      DealID           NUMBER(10),
      LotID            NUMBER(10),
      MinMarket        VARCHAR2(255),  -- источник этого курса MinMarketPrice
      MinDateMarket    DATE,          -- дата курса MinMarketPrice
      MaxMarket        VARCHAR2(255),  -- источник этого курса MaxMarketPrice
      MaxDateMarket    DATE,         -- дата курса MaxMarketPrice
      CircRecalcStatus NUMBER(5)     -- статус для протокола пересчета обращаемости
    );

/**
 * рекорд числовых значений видов курсов ц/б */
    TYPE R_RateTypes IS RECORD
    (
      MinRate        NUMBER(5),   -- вид курса минимальная цена
      MaxRate        NUMBER(5),   -- вид курса максимальная цена
      MediumRate     NUMBER(5),   -- вид курса средневзвешенная цена
      ReuterRate     NUMBER(5),   -- вид курса средняя цена Рейтер
      CloseRate      NUMBER(5),   -- вид курса цена закрытия
      BloombergRate  NUMBER(5),   -- вид курса Цена закрытия Bloomberg
      NPTXMarketRate NUMBER(5),   -- вид курса рыночная цена для НДФЛ
      NPTXCalcRate   NUMBER(5),   -- вид курса Расчетная цена для НДФЛ
      NPTXEstimRate  NUMBER(5)    -- вид курса Оценка для НДФЛ
    );

/**
 * Вывод сообщения об ошибке */
   PROCEDURE SetError( ErrNum IN INTEGER, ErrMes IN VARCHAR2 DEFAULT NULL );

/**
 * Получение сообщения об ошибке */
   PROCEDURE GetLastErrorMessage( ErrMes OUT VARCHAR2 );

/**
 * Получение даты запуска СНОБ */
   FUNCTION GetStartSTBDate RETURN DATE DETERMINISTIC;

   FUNCTION GetStartProgressScaleDate RETURN DATE DETERMINISTIC;

   FUNCTION GetLucreStartTaxPeriod RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает максимальную дату периода расчета для клиента */
   FUNCTION GetCalcPeriodDate( pKind IN NUMBER, pClientID IN NUMBER, pIIS IN CHAR DEFAULT CHR(0), pSubKind IN NUMBER DEFAULT 0, pDlContrID IN NUMBER DEFAULT 0, pExcludeFirstIISDate IN NUMBER DEFAULT 0 ) RETURN DATE;

/**
 * Возвращает дату периода расчета по всем клиентам */
   FUNCTION GetMaxCalcPeriodDate( pKind IN NUMBER ) RETURN DATE;

/**
 * Вставка даты периода расчета */
   PROCEDURE SetCalcPeriodDate( pKind IN NUMBER, pClientID IN NUMBER, pEndDate IN DATE, pProtocol IN NUMBER DEFAULT 1, pIIS IN CHAR DEFAULT CHR(0), pSubKind IN NUMBER DEFAULT 0, pContract IN NUMBER DEFAULT 0 );

/**
 * Проверка наличия договора ИИС по клиенту на дату операции */ 
   FUNCTION  ExistIISContr( pClientID IN NUMBER, pOperDate IN DATE ) RETURN NUMBER DETERMINISTIC;

/* 
* Проверка наличия договора не ИИС по клиенту на дату операции */
   FUNCTION  ExistNotIISContr( pClientID IN NUMBER, pOperDate IN DATE ) RETURN NUMBER DETERMINISTIC;

/* 
* Проверка наличия договора не ИИС по клиенту за период */
   FUNCTION  ExistNotIISContrPeriod( pClientID IN NUMBER, pBegDate IN DATE, pEndDate IN DATE ) RETURN NUMBER DETERMINISTIC;

/**
 @brief Проверка, является ли договор ИИС
 @param[in] sfContrID идентификатор субдоговора
 @return 1 - является, 0 - не является
*/           
   FUNCTION  CheckContrIIS (pSfContrID IN NUMBER) RETURN NUMBER DETERMINISTIC;

/**
 @brief Проверка, является ли договор ИИС с типом ИИС-III
 @param[in] sfContrID идентификатор субдоговора
 @return 1 - является, 0 - не является
*/           
   FUNCTION  CheckContrIIS3 (pSfContrID IN NUMBER) RETURN NUMBER DETERMINISTIC;

/**
 @brief Проверка наличия закрытых операций зачисления ц/б по всем субдоговорам с меньшей датой, чем у вводимой опрации
 @param[in] sfContrID идентификатор субдоговора
 @param[in] pOperDate дата вводимой операции зачисления ДС/ЦБ
 @return 1 - операции найдены, 0 - операции не найдены
*/           
   FUNCTION IsExistOperationAvrWrtIn (pSfContrID IN NUMBER, pOperDate IN DATE) RETURN NUMBER DETERMINISTIC;

/**
 @brief Проверка наличия закрытых операций зачисления денежных средств по всем субдоговорам с меньшей датой, чем у вводимой опрации
 @param[in] sfContrID идентификатор субдоговора
 @param[in] pOperDate дата вводимой операции зачисления ДС/ЦБ
 @return 1 - операции найдены, 0 - операции не найдены
*/ 
   FUNCTION IsExistOperationEnrol (pSfContrID IN NUMBER, pOperDate IN DATE) RETURN NUMBER DETERMINISTIC;

/**
 * Получить параметры NPTXOBJ из записи, переданной в виде Raw*/  
   PROCEDURE GetNptxObjFromRaw(pNptxObj IN RAW, v_rNptxObj IN OUT dnptxobj_dbt%rowtype) ;

/**
 * Проверка, что объект НДР по договору ИИС */ 
   FUNCTION  CheckObjIIS (pNptxObj IN RAW) RETURN NUMBER DETERMINISTIC;
   FUNCTION  CheckObjIIS (pNptxObj IN dnptxobj_dbt%rowtype)  RETURN NUMBER DETERMINISTIC;
   FUNCTION  CheckObjIIS (pAnaliticKind6 IN NUMBER, pAnalitic6 IN NUMBER) RETURN NUMBER RESULT_CACHE;

/**   
 * Поиск примечания в виде даты */
   function GetDateFromNoteText( v_ObjectType IN NUMBER, v_ObjectID IN VARCHAR2, v_NoteKind IN NUMBER ) RETURN DATE;

/**
 * Выполняет поиск даты ТО */
   function GetDateFromRQ( v_RQID IN NUMBER, v_FactDate IN DATE ) 
     return DATE;

/**
 * Выполняет поиск даты платежа */
   function GetDateFromPayment( v_PaymID IN NUMBER, v_ValueDate IN DATE ) 
     return DATE;

/**
 * Проверка, что за период существут данные для расчета НДФЛ */
   FUNCTION CheckExistDataForPeriod( pBegDate  IN DATE,            -- Дата начала
                                     pEndDate  IN DATE,            -- Дата окончания
                                     pClientID IN NUMBER,          -- Клиент
                                     pIIS      IN NUMBER DEFAULT 0,-- Признак ИИС
                                     pFIID     IN NUMBER DEFAULT -1,
                                     pExcludeBuy IN NUMBER DEFAULT 0, --Признак Исключить покупки
                                     pDlContrID IN NUMBER DEFAULT 0,
                                     pOnlyBuy  IN NUMBER DEFAULT 0
                                   ) RETURN NUMBER;

/**
 * Откат вставки даты периода расчета */
   PROCEDURE RecoilCalcPeriodDate( pKind IN NUMBER, pClientID IN NUMBER, pEndDate IN DATE, pIIS IN CHAR DEFAULT CHR(0), pSubKind IN NUMBER DEFAULT 0, pContract IN NUMBER DEFAULT 0 );

/**
 * Возвращает дату предыдущей операции заданного вида для клиента */
   FUNCTION GetPrevOpDate( pDocKind IN NUMBER, pClientID IN NUMBER ) RETURN DATE;

/**
 * Проверка для субъекта, что установлена КО "Является плательщиком НДФЛ" != "Нет" */
   FUNCTION IsPayerNPTX( PartyID IN NUMBER, OperDate IN DATE ) RETURN NUMBER DETERMINISTIC;

   /**
   @brief Выполняет вставку объекта НДР, возвращая идентификатор через параметр
   @param [in] pDate Дата создания объекта НДР
   @param [in] pClient ID клиента
   @param [in] pDirection Направление
   @param [in] pLevel Уровень
   @param [in] pUser Признак "Пользовательский"
   @param [in] pKind Вид объекта НДР
   @param [in] pSum Сумма объекта НДР
   @param [in] pCur Валюта объекта НДР
   @param [in] pAnaliticKind1 Вид аналитики 1
   @param [in] pAnalitic1 Значение аналитики 1
   @param [in] pAnaliticKind2 Вид аналитики 2
   @param [in] pAnalitic2 Значение аналитики 2
   @param [in] pAnaliticKind3 Вид аналитики 3
   @param [in] pAnalitic3 Значение аналитики 3
   @param [in] pAnaliticKind4 Вид аналитики 4
   @param [in] pAnalitic4 Значение аналитики 4
   @param [in] pAnaliticKind5 Вид аналитики 5
   @param [in] pAnalitic5 Значение аналитики 5
   @param [in] pAnaliticKind6 Вид аналитики 6
   @param [in] pAnalitic6 Значение аналитики 6
   @param [in] pComment Комментарий
   @param [in] pDocID ID текущей операции
   @param [in] pStep ID шага текущей операции
   @param [in] pNoCalcNOB Не расчитывать НОБ
   @param [out] pObjID ID объекта НДР
   @param [in] pFromOutSyst Признак "Из внешней системы"
   @param [in] pOutSystCode Код внешней системы
   @param [in] pOutObjID ID во внешней системе
   @param [in] pSourceObjID ID первоначального объекта
   @param [in] pTechnical Признак "Технический"
   @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
   @param [in] pTransfKind Период расчета
   */
   PROCEDURE InsertTaxObjectRetID(
                                  pDate IN DATE,           
                                  pClient IN NUMBER,        
                                  pDirection IN NUMBER,     
                                  pLevel IN NUMBER,         
                                  pUser IN CHAR,           
                                  pKind IN NUMBER,          
                                  pSum IN NUMBER,           
                                  pCur IN NUMBER,           
                                  pAnaliticKind1 IN NUMBER, 
                                  pAnalitic1 IN NUMBER,     
                                  pAnaliticKind2 IN NUMBER, 
                                  pAnalitic2 IN NUMBER,     
                                  pAnaliticKind3 IN NUMBER, 
                                  pAnalitic3 IN NUMBER,     
                                  pAnaliticKind4 IN NUMBER, 
                                  pAnalitic4 IN NUMBER,     
                                  pAnaliticKind5 IN NUMBER, 
                                  pAnalitic5 IN NUMBER,     
                                  pAnaliticKind6 IN NUMBER, 
                                  pAnalitic6 IN NUMBER,     
                                  pComment IN VARCHAR2,     
                                  pDocID IN NUMBER,  
                                  pStep IN NUMBER,
                                  pNoCalcNOB IN NUMBER,
                                  pObjID OUT NUMBER,
                                  pFromOutSyst IN CHAR DEFAULT CHR(0),
                                  pOutSystCode IN VARCHAR2 DEFAULT CHR(1),
                                  pOutObjID IN VARCHAR2 DEFAULT CHR(1),
                                  pSourceObjID IN NUMBER DEFAULT 0,
                                  pTechnical IN CHAR DEFAULT CHR(0),
                                  pConvDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                  pTransfDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                  pTransfKind IN NUMBER DEFAULT 0,
                                  pHolding_Period IN NUMBER DEFAULT 0,
                                  pChangeCode IN CHAR DEFAULT CHR(0)
                                );

   /**
   @brief Выполняет вставку объекта, вызывается в макросах шагов, без проверки наличия существующего
   @param [in] pDate Дата создания объекта НДР
   @param [in] pClient ID клиента
   @param [in] pDirection Направление
   @param [in] pLevel Уровень
   @param [in] pUser Признак "Пользовательский"
   @param [in] pKind Вид объекта НДР
   @param [in] pSum Сумма объекта НДР
   @param [in] pCur Валюта объекта НДР
   @param [in] pAnaliticKind1 Вид аналитики 1
   @param [in] pAnalitic1 Значение аналитики 1
   @param [in] pAnaliticKind2 Вид аналитики 2
   @param [in] pAnalitic2 Значение аналитики 2
   @param [in] pAnaliticKind3 Вид аналитики 3
   @param [in] pAnalitic3 Значение аналитики 3
   @param [in] pAnaliticKind4 Вид аналитики 4
   @param [in] pAnalitic4 Значение аналитики 4
   @param [in] pAnaliticKind5 Вид аналитики 5
   @param [in] pAnalitic5 Значение аналитики 5
   @param [in] pAnaliticKind6 Вид аналитики 6
   @param [in] pAnalitic6 Значение аналитики 6
   @param [in] pComment Комментарий
   @param [in] pDocID ID текущей операции
   @param [in] pStep ID шага текущей операции
   @param [in] pNoCalcNOB Не расчитывать НОБ
   @param [out] pObjID ID объекта НДР
   @param [in] pFromOutSyst Признак "Из внешней системы"
   @param [in] pOutSystCode Код внешней системы
   @param [in] pOutObjID ID во внешней системе
   @param [in] pSourceObjID ID первоначального объекта
   @param [in] pTechnical Признак "Технический"
   @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
   @param [in] pTransfKind Период расчета
   */
   PROCEDURE InsertTaxObject(
                              pDate IN DATE,           
                              pClient IN NUMBER,        
                              pDirection IN NUMBER,     
                              pLevel IN NUMBER,         
                              pUser IN CHAR,           
                              pKind IN NUMBER,          
                              pSum IN NUMBER,           
                              pCur IN NUMBER,           
                              pAnaliticKind1 IN NUMBER, 
                              pAnalitic1 IN NUMBER,     
                              pAnaliticKind2 IN NUMBER, 
                              pAnalitic2 IN NUMBER,     
                              pAnaliticKind3 IN NUMBER, 
                              pAnalitic3 IN NUMBER,     
                              pAnaliticKind4 IN NUMBER, 
                              pAnalitic4 IN NUMBER,     
                              pAnaliticKind5 IN NUMBER, 
                              pAnalitic5 IN NUMBER,     
                              pAnaliticKind6 IN NUMBER, 
                              pAnalitic6 IN NUMBER,     
                              pComment IN VARCHAR2,     
                              pDocID IN NUMBER,  
                              pStep IN NUMBER,
                              pNoCalcNOB IN NUMBER DEFAULT 0,
                              pFromOutSyst IN CHAR DEFAULT CHR(0),
                              pOutSystCode IN VARCHAR2 DEFAULT CHR(1),
                              pOutObjID IN VARCHAR2 DEFAULT CHR(1),
                              pSourceObjID IN NUMBER DEFAULT 0,
                              pTechnical IN CHAR DEFAULT CHR(0),
                              pConvDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                              pTransfDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                              pTransfKind IN NUMBER DEFAULT 0,
                              pHolding_Period IN NUMBER DEFAULT 0,
                              pChangeCode IN CHAR DEFAULT CHR(0)
                            );

   /**
   @brief Выполняет вставку объекта, вызывается в макросах шагов, без проверки наличия существующего
   @param [in] pDate Дата создания объекта НДР
   @param [in] pClient ID клиента
   @param [in] pDirection Направление
   @param [in] pLevel Уровень
   @param [in] pUser Признак "Пользовательский"
   @param [in] pKind Вид объекта НДР
   @param [in] pSum Сумма объекта НДР
   @param [in] pCur Валюта объекта НДР
   @param [in] pAnaliticKind1 Вид аналитики 1
   @param [in] pAnalitic1 Значение аналитики 1
   @param [in] pAnaliticKind2 Вид аналитики 2
   @param [in] pAnalitic2 Значение аналитики 2
   @param [in] pAnaliticKind3 Вид аналитики 3
   @param [in] pAnalitic3 Значение аналитики 3
   @param [in] pAnaliticKind4 Вид аналитики 4
   @param [in] pAnalitic4 Значение аналитики 4
   @param [in] pAnaliticKind5 Вид аналитики 5
   @param [in] pAnalitic5 Значение аналитики 5
   @param [in] pAnaliticKind6 Вид аналитики 6
   @param [in] pAnalitic6 Значение аналитики 6
   @param [in] pComment Комментарий
   @param [in] pDocID ID текущей операции
   @param [in] pStep ID шага текущей операции
   @param [in] pNoCalcNOB Не расчитывать НОБ
   @param [out] pObjID ID объекта НДР
   @param [in] pFromOutSyst Признак "Из внешней системы"
   @param [in] pOutSystCode Код внешней системы
   @param [in] pOutObjID ID во внешней системе
   @param [in] pSourceObjID ID первоначального объекта
   @param [in] pTechnical Признак "Технический"
   @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
   @param [in] pTransfKind Период расчета
   */
   PROCEDURE InsertTaxObjectWD(
                                pDate IN DATE,           
                                pClient IN NUMBER,        
                                pDirection IN NUMBER,     
                                pLevel IN NUMBER,         
                                pUser IN CHAR,           
                                pKind IN NUMBER,          
                                pSum IN NUMBER,           
                                pCur IN NUMBER,           
                                pAnaliticKind1 IN NUMBER, 
                                pAnalitic1 IN NUMBER,     
                                pAnaliticKind2 IN NUMBER, 
                                pAnalitic2 IN NUMBER,     
                                pAnaliticKind3 IN NUMBER, 
                                pAnalitic3 IN NUMBER,     
                                pAnaliticKind4 IN NUMBER, 
                                pAnalitic4 IN NUMBER,     
                                pAnaliticKind5 IN NUMBER, 
                                pAnalitic5 IN NUMBER,     
                                pAnaliticKind6 IN NUMBER, 
                                pAnalitic6 IN NUMBER,     
                                pComment IN VARCHAR2,     
                                pDocID IN NUMBER,  
                                pStep IN NUMBER,
                                pFromOutSyst IN CHAR DEFAULT CHR(0),
                                pOutSystCode IN VARCHAR2 DEFAULT CHR(1),
                                pOutObjID IN VARCHAR2 DEFAULT CHR(1),
                                pSourceObjID IN NUMBER DEFAULT 0,
                                pTechnical IN CHAR DEFAULT CHR(0),
                                pConvDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                pTransfDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                pTransfKind IN NUMBER DEFAULT 0,
                                pHolding_Period IN NUMBER DEFAULT 0,
                                pChangeCode IN CHAR DEFAULT CHR(0)
                              );

/**
 * Выполняет запуск вставки объектов НДР на шаге по временной таблице */
   PROCEDURE StartInsertTaxObject( pDocID IN NUMBER,  
                                   pStep  IN NUMBER        
                                 );

/**
 * Выполняет откат вставки объекта */
   PROCEDURE RecoilInsertTaxObject( pDocID IN NUMBER,
                                    pStep  IN NUMBER,
                                    pNoCalcNOB IN NUMBER DEFAULT 0
                                  );

/**
 * Выполняет вставку объекта и вставку в реестр */
   PROCEDURE InsertTaxObjectTaxPay(
                                  pDate IN DATE,
                                  pClient IN NUMBER,
                                  pDirection IN NUMBER,
                                  pLevel IN NUMBER,
                                  pUser IN CHAR,
                                  pKind IN NUMBER,
                                  pSum IN NUMBER,
                                  pCur IN NUMBER,
                                  pAnaliticKind1 IN NUMBER,
                                  pAnalitic1 IN NUMBER,
                                  pAnaliticKind2 IN NUMBER,
                                  pAnalitic2 IN NUMBER,
                                  pAnaliticKind3 IN NUMBER,
                                  pAnalitic3 IN NUMBER,
                                  pAnaliticKind4 IN NUMBER,
                                  pAnalitic4 IN NUMBER,
                                  pAnaliticKind5 IN NUMBER,
                                  pAnalitic5 IN NUMBER,
                                  pAnaliticKind6 IN NUMBER,
                                  pAnalitic6 IN NUMBER,
                                  pComment IN VARCHAR2,
                                  pDocID IN NUMBER,
                                  pStep IN NUMBER,
                                  pNoCalcNOB IN NUMBER,
                                  pFromOutSyst IN CHAR DEFAULT CHR(0),
                                  pOutSystCode IN VARCHAR2 DEFAULT CHR(1),
                                  pOutObjID IN VARCHAR2 DEFAULT CHR(1),
                                  pSourceObjID IN NUMBER DEFAULT 0,
                                  pConvDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                  pTransfDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                  pTransfKind IN NUMBER DEFAULT 0,
                                  pHolding_Period IN NUMBER DEFAULT 0,
                                  pChangeCode IN CHAR DEFAULT CHR(0)
                                );
                                
/**
 *  Выполняет откат вставки объекта и вставки в реестр */
    PROCEDURE RecoilInsertTaxObjectTaxPay( pDocID IN NUMBER,
                                    pStep  IN NUMBER,
                                    pNoCalcNOB IN NUMBER DEFAULT 0
                                  );

/**
 * функция получает из настроек системы насройки НУ и заносит их в глобализм */
   procedure GetSettingsTax;

/** 
 * Значение категории для субъекта "способ определения расчетной цены при отсутствии котировок" */
   FUNCTION CalPrMethodByClnt( PartyID IN NUMBER, OperDate IN DATE ) RETURN NUMBER DETERMINISTIC;

/**
 * Структуры рыночных параметров и настроек НУ */
   MarketPrice R_MarketPrice;
   RateTypes   R_RateTypes;

/**
 * Получить обращаемость ц/б на дату из таблицы с кешем */
   FUNCTION GetFICirculateNPTXFI(pFIID IN NUMBER, pDate IN DATE) RETURN NUMBER RESULT_CACHE;

/**
 * Получение значения выбранного курса
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ */
  function GetMarketPrice( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return NUMBER;

/**
 * Получение биржи, на кот. установлен курс
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ */
   function GetMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return VARCHAR2;

/**
 * Получение биржи, на кот. установлен курс
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ */
   function GetMinMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return VARCHAR2;

/**
 * Получение биржи, на кот. установлен курс
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ */
   function GetMaxMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return VARCHAR2;

/**
 * Получение даты курса (возможно, составной вида '28.02.2030/11.03.2030') 
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ */
   function GetDateMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return VARCHAR2;

/**
 * Получение даты курса 
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ */
   function GetMinDateMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return DATE;

/**
 * Получение даты курса 
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ */
   function GetMaxDateMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return DATE;

/**
 * Получение признака обращаемости на ОРЦБ */
   function IfMarket( pFIID IN NUMBER, pDate IN DATE ) return VARCHAR2;

/**
 * Получение мин. рыночной цены 
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ */
   FUNCTION GetMinMarketPrice( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) RETURN NUMBER;

/**
 * Получение макс. рыночной цены
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ */
   FUNCTION GetMaxMarketPrice( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) RETURN NUMBER;

/**
 * Получение значения выбранного курса */
  function GetMarketPrice_DV( pDealID IN NUMBER ) return NUMBER;

/**
 * Получение биржи и сектора, на кот. установлен курс */
   function GetMarket_DV( pDealID IN NUMBER ) return VARCHAR2;

/**
 * Получение даты курса */
   function GetDateMarket_DV( pDealID IN NUMBER ) return DATE;

/**
 * Получение признака обращаемости на ОРЦБ */
   function IfMarket_DV( pDealID IN NUMBER ) return VARCHAR2;

/**
 * Получение мин. рыночной цены */
   FUNCTION GetMinMarketPrice_DV( pDealID IN NUMBER ) RETURN NUMBER;

/**
 * Получение макс. рыночной цены */
   FUNCTION GetMaxMarketPrice_DV( pDealID IN NUMBER ) RETURN NUMBER;

/**
 * Получение даты покупки из зачисления */
   function GetDateFromAvrWrtIn( pDealID IN NUMBER, pStartDate IN DATE, pDealDate IN DATE ) 
     return DATE;

/**
 * Получение цены из зачисления */
   function GetPriceFromAvrWrtIn( pDealID IN NUMBER, pPrice IN NUMBER ) 
     return NUMBER;

/**
 * Получение валюты цены из зачисления */
   function GetPriceFIIDFromAvrWrtIn( pDealID IN NUMBER, pPriceFIID IN NUMBER ) 
     return NUMBER;

/**
 * Получение стоимости из зачисления */
   function GetCostFromAvrWrtIn( pDealID IN NUMBER, pCost IN NUMBER ) 
     return NUMBER;

/**
 * Получение НКД из зачисления */
   function GetNkdFromAvrWrtIn( pDealID IN NUMBER, pNKD IN NUMBER ) 
     return NUMBER;

/**
 * Получение затрат из зачисления */
   function GetOutlayFromAvrWrtIn( pDealID IN NUMBER, pOutlay IN NUMBER ) 
     return NUMBER;

/**
 * Определение категории ц/б - алгоритм 1 */
   FUNCTION Market1date( pFIID IN NUMBER, pDate IN DATE ) RETURN NUMBER;

/**
 * Определение категории ц/б - алгоритм 2 */
   FUNCTION Market2dates( pFIID IN NUMBER, pDate2 IN DATE, pDate1 IN DATE ) RETURN NUMBER;

/**
 * Определение категории ц/б - алгоритм 3 */
   FUNCTION Market3date( pFIID IN NUMBER, pDate IN DATE ) RETURN NUMBER;


/**
 * Получение для бумаги группы НУ для НДФЛ */
   FUNCTION GetPaperTaxGroupNPTX( pFIID IN NUMBER,  pIsDerivative IN NUMBER DEFAULT -1 ) RETURN NUMBER RESULT_CACHE;

/**
  * Выполняет удаление объектов НДР и связанных данных, а также вставку объектов (и связанных данных) в спец. таблицу данных для отката.
  * Не трогает пользовательские и депозитарные объекты, а также созданные в операции удержания НДФЛ.
  * Откат - RecoilDeleteNdrForRecalc */
   PROCEDURE DeleteNdrForRecalc( pDocID IN NUMBER, pBegRecalcDate IN DATE, pEndRecalcDate IN DATE, pClient IN NUMBER, pIIS IN NUMBER DEFAULT 0, pFIID IN NUMBER DEFAULT -1, pContract IN NUMBER DEFAULT 0  );

/**
 * Выполняет откат удаления объекта НДР и связанных данных, выполненный при пересчёте. */
   PROCEDURE RecoilDeleteNdrForRecalc( pDocID IN NUMBER );
   
   /**
   @brief Восстанавливает обекты НДР с признаком "Технческий расчет" = "Да" при откате операии расчета НОБ
   @param [in] DocID ID текущей операции расчета НОБ
   */
   PROCEDURE RecoilDeleteTechnicalNdr( pDocID IN NUMBER );

/**
 * Проверка что доход льготный
 */
   FUNCTION IsFavourIncome( DDS IN DATE, DDB IN DATE, FIID IN NUMBER ) RETURN NUMBER;

/**
 * Проверка что доход льготный в НДФЛ
 */
   FUNCTION IsFavourIncome_NPTX( DDS IN DATE, DDB IN DATE, FIID IN NUMBER ) RETURN NUMBER;

/**
 * Проверка наличия какого либо значения категории за период дат
 */
   FUNCTION IsExistsAnyAttr( ObjectType IN NUMBER, Object IN VARCHAR2, GroupID IN NUMBER, Date1 IN DATE, Date2 IN DATE ) RETURN NUMBER;

/**
 * Проверка наличия значения категории за период дат
 */
   FUNCTION IsExistsAttr( ObjectType IN NUMBER, Object IN VARCHAR2, GroupID IN NUMBER, Date1 IN DATE, Date2 IN DATE, NumInList IN VARCHAR2 ) RETURN NUMBER;

/**
 * Проверка наличия значения категории за весь период дат
 */
   FUNCTION IsExistsAttrAllDat( ObjectType IN NUMBER, Object IN VARCHAR2, GroupID IN NUMBER, Date1 IN DATE, Date2 IN DATE, NumInList IN VARCHAR2 ) RETURN NUMBER;

/**
 * Получить страну субъекта
 */
   FUNCTION GetCountryParty( pPartyID IN NUMBER, pCountry OUT VARCHAR2 ) RETURN VARCHAR2;

/**
 * Проверяем, что корпоративная облигация с данным купоном попадает под налогообложение по ставке 35%
 */         
   FUNCTION IsCorpBondAfter2018( p_FIID IN NUMBER, p_WarrantNum IN VARCHAR2 ) RETURN NUMBER;

/**
 * Проверяем, что корпоративная облигация с датой погашения (купона) попадает под налогообложение по ставке 35%
 */         
   FUNCTION IsCorpBondAfter2018byDrawDate( p_FIID IN NUMBER, p_DrawingDate IN DATE ) RETURN NUMBER;

   FUNCTION GetTaxBaseCorpBondAfter2018(p_FIID IN NUMBER, --ц/б, по которой считается доход
                                        p_WarrantNum IN VARCHAR2, --номер купона/чп/дивидендов, по которым считается доход
                                        p_Quantity IN NUMBER
                                       ) RETURN NUMBER;

  function IsAdmin(pOper IN NUMBER) return BOOLEAN;

/**
 * Получить дату начала первого договора ИИС (аналог одноименной ф-ции из nptxfun.mac)
 */
   FUNCTION GetFirstDateIIS (p_Client IN dparty_dbt.t_PartyId%TYPE, p_DlContrID IN NUMBER DEFAULT 0) RETURN DATE DETERMINISTIC;

/**
 * Рассчитать/пересчитать обращаемость ц/б для НДФЛ в кеш
 */
   PROCEDURE RecalcCirculate(p_ClientID IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE, p_NeedDel IN NUMBER, p_ParallelLevel IN NUMBER DEFAULT 1);

/**
 * Процедура вставки строки в таблицу значений НДФЛ на шаге операции
 */
  PROCEDURE RSI_InsertNPTXVAL(p_DocKind IN NUMBER, 
                              p_DocID IN NUMBER,
                              p_Kind IN NUMBER,
                              p_Date IN DATE,
                              p_Time IN DATE,
                              p_Value IN NUMBER,
                              p_ID_Operation IN NUMBER,
                              p_ID_Step IN NUMBER,
                              p_Rate IN NUMBER DEFAULT 0,
                              p_KBK IN VARCHAR2 DEFAULT CHR(1)
                             );

/**
 * Откат вставки строки в таблицу значений НДФЛ на шаге операции
 */
  PROCEDURE RSI_RollbackInsertNPTXVAL(p_ID_Operation IN NUMBER,
                                      p_ID_Step IN NUMBER
                                     );

/**
 * Сохранение события СНОБ на шаге операции
 */
  PROCEDURE RSI_SaveSTB(p_nptxtotalbase IN RAW, p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER);

/**
 * Откат события СНОБ на шаге операции
 */
  PROCEDURE RSI_RestoreSTB(p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER, p_TBID IN NUMBER DEFAULT 0 );

/**
 * Ищем дату прдыдущей операции, аналог DL_NptxCalcTaxPrevDate 
 * p_Client ID клиента
 * p_IIS признак ИИС
 * p_OperDate дата операции
 */         
   FUNCTION NptxCalcTaxPrevDate( p_Client IN NUMBER, p_IIS IN CHAR, p_OperDate IN DATE ) RETURN DATE;

/**
 * Изменить статус записи в буферной таблице СНОБ
 */
  PROCEDURE RSI_ChangeStatusTbBuf(p_BFID IN NUMBER, p_NewStatus IN NUMBER, p_ProcDate IN DATE, p_ProcTime IN DATE, p_CanclDate IN DATE, p_CanclTime IN DATE);


/**
  * Получить идентификатор последнй записи из буферной таблицы СНОБ для переданного события СНОБ
  */
  FUNCTION GetActualBFIDForTB(p_TBID IN NUMBER) RETURN NUMBER;

/**
  * Получить необходимость пересчета для переданного события СНОБ
  */
  FUNCTION GetNeedRecalcFlagForTB(p_TBID IN NUMBER) RETURN NUMBER;

  FUNCTION GetCircRecalcStatus RETURN NUMBER;

  PROCEDURE InsertCheckNDFL(p_ID_Operation IN NUMBER);
  PROCEDURE DeleteCheckNDFL(p_ID_Operation IN NUMBER);

  FUNCTION GetErrCntVer(p_ID_Operation IN NUMBER, p_DocKind IN NUMBER) RETURN NUMBER;

  FUNCTION IsLastOperVerForEventID(p_EventID IN NUMBER, p_BatchID IN NUMBER) RETURN NUMBER;

END RSI_NPTO;
/
