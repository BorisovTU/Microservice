CREATE OR REPLACE PACKAGE RSI_NPTX
IS
  ---------------------------------
  -- Author  : Makarov A.G.      --
  -- Created : 19 December 2010  --
  -- Descrip : ХП расчета связей НУ НДФЛ
  ---------------------------------
/**
 * рекорд прочитанных значений настроек НУ */
    TYPE R_ReestrValue IS RECORD
    (
      W1 NUMBER(5),  -- Разрешить продажу блокированных приобретений: 0 - Да, 1 - Нет
      W2 NUMBER(5),  -- Проводить перетасовку для НДФЛ: 0 - Да, 1 - Нет
      W3 BOOLEAN     -- Учитывать РЕПО для срока непрерывного владения = <да> или <нет>
    );

    ReestrValue R_ReestrValue;

    c_BegCourceDate CONSTANT DATE := TO_DATE( '01.01.1990', 'DD.MM.YYYY' );  -- Условная дата отсчета

    TYPE VECTOR_INT IS TABLE OF NUMBER(10) NOT NULL
       INDEX BY BINARY_INTEGER;

    TYPE VECTOR_DOUB IS TABLE OF NUMBER(32,12) NOT NULL
       INDEX BY BINARY_INTEGER;

    TYPE VECTOR_CHR IS TABLE OF VARCHAR(3) NOT NULL
       INDEX BY BINARY_INTEGER;

    TYPE VECTOR_DOUB_ID IS TABLE OF VECTOR_DOUB NOT NULL
       INDEX BY BINARY_INTEGER;

/**
 * Кэш валют номинала */
    v_FaceValueFIID    VECTOR_INT;
/**
 * Кэш значений курса рыночной цены */
    v_PriceCourceValue VECTOR_DOUB_ID;
/**
 * Кэш значений курса НКД */
    v_NKDCourceValue   VECTOR_DOUB_ID;

/**
 * Задает приоритет приходов зависимости от типов сделок прихода для сделок продаж. */
    FUNCTION GetBuyOrderForSale( p_BuyKind IN NUMBER )
       RETURN NUMBER DETERMINISTIC;

/**
 * Задает приоритет приходов зависимости от типов сделок прихода для сделок Репо прямых. */
    FUNCTION GetBuyOrderForRepo( p_BuyKind IN NUMBER )
       RETURN NUMBER DETERMINISTIC;

/**
 * Определяет по виду сделки, является ли она продажей (выбытием). Для сделки из двух частей - по первой части */
    FUNCTION IsSale( p_Kind IN NUMBER )
       RETURN NUMBER DETERMINISTIC;

/**
 * Определяет по типу сделки, является ли она виртуальной. */
    FUNCTION IsVirtual( p_Type IN NUMBER )
       RETURN NUMBER DETERMINISTIC;

/**
 * Вычисляет значение признака наличия свободного остатка в зависимости от параметров лота. */
    FUNCTION GetIsFree( p_AMOUNT IN NUMBER, p_SALE IN NUMBER, p_RETFLAG IN CHAR, p_INACC IN CHAR,
                        p_BLOCKED IN CHAR, p_BuyDate IN DATE, p_SaleDate IN DATE )
      RETURN CHAR DETERMINISTIC;

/**
 * Получить кол-во виртуальных лотов по его номеру */
    FUNCTION GetVirtCountByNum( in_Number IN VARCHAR2 )
      RETURN NUMBER;

/**
 * получить тип лота в зависимости от операции */
    FUNCTION get_lotKind( oGrp IN NUMBER, DealID IN NUMBER, DealPart IN NUMBER DEFAULT 1, IsPartyClient IN NUMBER DEFAULT 0 )
      RETURN NUMBER DETERMINISTIC;

/**
 * получить дату покупки лота в зависимости от операции */
    FUNCTION get_lotBuyDate( oGrp IN NUMBER, DealID IN NUMBER, FactDate IN DATE, DealPart IN NUMBER DEFAULT 1, IsPartyClient IN NUMBER DEFAULT 0 )
      RETURN DATE DETERMINISTIC;

/**
 * получить дату продажи лота в зависимости от операции */
    FUNCTION get_lotSaleDate( oGrp IN NUMBER, DealID IN NUMBER, FactDate IN DATE, DealPart IN NUMBER DEFAULT 1, IsPartyClient IN NUMBER DEFAULT 0 )
      RETURN DATE DETERMINISTIC;

/**
 * Проверить наличие категории */
    FUNCTION CheckCateg( ObjType IN NUMBER, GroupID IN NUMBER, ObjID IN VARCHAR2, AttrID IN NUMBER )
      RETURN NUMBER DETERMINISTIC;

/**
 * получить код лота */
    FUNCTION get_lotCode( p_Code IN VARCHAR2, p_Num IN NUMBER ) RETURN VARCHAR2;

/**
 * Выполняет обработку таблицы состояния при создании/обновлении связи */
    PROCEDURE UpdateTSByLink (pLinkType IN NUMBER, pBuyID IN NUMBER,  pSaleID IN NUMBER, 
                             pSourceID IN NUMBER, pLinkDate IN DATE, pAmount IN NUMBER);

/**
 * Выполняет обработку таблицы состояния при выбытии 2 ч ОР */
    PROCEDURE UpdateTSByReverseRepo (pRepoID IN NUMBER, pDate IN DATE);

/**
 * Выполняет обработку таблицы состояния при выбытии 2 ч ПР */
    PROCEDURE UpdateTSByDirectRepo (pRepoID IN NUMBER, pDate IN DATE);

/**
 * Заполняет таблицы налогового учета */
    PROCEDURE CreateLots( pOperDate IN DATE, 
                          pClient IN NUMBER, 
                          pIIS IN CHAR DEFAULT CHR(0), 
                          pFIID IN NUMBER DEFAULT -1, 
                          pBegDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'), 
                          pRecalc IN CHAR DEFAULT CHR(0), 
                          pID_Operation IN NUMBER DEFAULT 0, 
                          pID_Step IN NUMBER DEFAULT 0, 
                          pContract  IN NUMBER DEFAULT 0 );

/**
 * Процедура отката формирования регистров НДФЛ */
    PROCEDURE RecoilCreateLots( pOperDate IN DATE, pClient IN NUMBER, pIIS IN CHAR DEFAULT CHR(0), pFIID IN NUMBER DEFAULT -1, pRecalc IN CHAR DEFAULT CHR(0), pID_Operation IN NUMBER DEFAULT 0, pID_Step IN NUMBER DEFAULT 0, pContract  IN NUMBER DEFAULT 0 );

/**
 * Выполняет обработку таблицы состояния (тип записей - "покупки") при создании/обновлении связи.
 * Используется из триггеров по таблице связей */
    PROCEDURE UpdateTSBuyByLink( pLinkType IN NUMBER, pBuyID IN NUMBER, pSaleID IN NUMBER, pLinkDate IN DATE, pEndDate IN DATE, pAmount IN NUMBER );

/**
 * Процедура пересчета льготного кол-ва ц\б в связях КП (вызывается перед расчетом НДР 2 уровня) */
    PROCEDURE ReCalcPrivAmountByLink( pBegDate IN DATE, pEndDate IN DATE, pClient IN NUMBER, pIIS IN NUMBER DEFAULT 0, pFIID IN NUMBER DEFAULT -1 );

/**
 * Получить кол-во льготных по данной записи остатков*/
    FUNCTION GetPrivAmountByTS( pID IN NUMBER, pCalcDate IN DATE, pDDS IN DATE )  
       RETURN NUMBER DETERMINISTIC;

/**
 * Процедура отката предыдущих расчетов*/
    PROCEDURE DeletePrevCalc(pDocID         IN NUMBER, 
                             pClient        IN NUMBER,
                             pIIS           IN NUMBER DEFAULT 0, 
                             pFIID          IN NUMBER DEFAULT -1,
                             pID_Operation  IN NUMBER DEFAULT 0,
                             pID_Step       IN NUMBER DEFAULT 0,
                             pContract      IN NUMBER DEFAULT 0
                            );
/**
 * Процедура отката действий при откате предыдущих расчетов */
    PROCEDURE RecoilDeletePrevCalc(pDocID         IN NUMBER,
                                   pID_Operation  IN NUMBER DEFAULT 0,
                                   pID_Step       IN NUMBER DEFAULT 0);

/**
 * LISTAGG+DISTINCT */
    FUNCTION LISTAGG_DISTINCT(val IN VARCHAR2, sep IN VARCHAR2 DEFAULT ',')
      RETURN VARCHAR2 DETERMINISTIC;

/**
 * Получить статус резидентства
 * @qtest  NO
 * @since  6.20.031.057
 * @param  pClient Идентификатор клиента
 * @param  pDate Дата проверки
 * @return результат вызова функцииции (1 - резидент, в остальных случаях - нерезидент)
 */
    FUNCTION ResidenceStatus(pClient IN NUMBER, pDate IN DATE)
      RETURN NUMBER;
/**
 * Проверить категорию "Договор ИИС" на договоре и тип операции списания/зачисления денежных средств
 * @param pSubKind_Operation Операция списания (20) или зачисление (10) денежных средств
 * @param pContrID ID договора, указанного в операции
 */ 
    FUNCTION CheckContrIISForNPTXWrtOff(pSubKind_Operation IN NUMBER, pContrID IN NUMBER)
       RETURN NUMBER DETERMINISTIC;


    FUNCTION STB_IsResidentStatus(p_ClientID IN NUMBER, p_OnDate IN DATE) RETURN NUMBER;

    --Процедура определения налоговых ставок
    PROCEDURE CreateTaxRatesBySNOB(p_ClientID IN NUMBER,
                                   p_TaxPeriod IN NUMBER,
                                   p_TaxBaseType IN NUMBER,
                                   p_TaxBase IN NUMBER,
                                   p_SNOB IN NUMBER,
                                   p_CalcDate IN DATE,
                                   p_SpecialTag IN VARCHAR2 DEFAULT CHR(0)
                                  );

    --Процедура Расчета Исчисленного Налога (ПРИН)
    PROCEDURE CalcTaxByRanges(p_ClientID IN NUMBER,
                              p_TaxPeriod IN NUMBER,
                              p_TaxBaseType IN NUMBER,
                              p_TaxBase IN NUMBER,
                              p_SNOB IN NUMBER,
                              p_CalcDate IN DATE,
                              p_SpecialTag IN VARCHAR2 DEFAULT CHR(0)
                             );

    --Процедура расчета налога к удержанию (ПРасНаКу)
    PROCEDURE CalcHoldTaxByRanges(p_ClientID IN NUMBER,
                                  p_TaxPeriod IN NUMBER,
                                  p_TaxBaseType IN NUMBER,
                                  p_TaxBase IN NUMBER,
                                  p_SNOB IN NUMBER,
                                  p_CalcDate IN DATE,
                                  p_SpecialTag IN VARCHAR2 DEFAULT CHR(0)
                                 );

    FUNCTION GetTaxRegIntValue(p_KeyPath IN VARCHAR2, p_OnDate IN DATE ) RETURN NUMBER;
    FUNCTION GetTaxRegDoubleValue(p_KeyPath IN VARCHAR2, p_OnDate IN DATE ) RETURN FLOAT;
    FUNCTION GetTaxRegStringValue(p_KeyPath IN VARCHAR2, p_OnDate IN DATE ) RETURN VARCHAR2;
    FUNCTION GetTaxRegFlagValue(p_KeyPath IN VARCHAR2, p_OnDate IN DATE ) RETURN CHAR;
    FUNCTION GetTaxRegValueByType(p_KeyPath IN VARCHAR2, p_OnDate IN DATE, p_RegType IN INTEGER) RETURN VARCHAR2;

    FUNCTION DetermineIISCountingStatus (DocKind IN NUMBER, DocID IN NUMBER, pContract IN NUMBER DEFAULT 0) RETURN CHAR DETERMINISTIC;

    --Функция расчета налога TaxeCalcFunction
    PROCEDURE TaxeCalcFunctionPrm(pClientId IN NUMBER, pTaxPeriod IN NUMBER, pTaxBaseKind IN NUMBER, pSubKindOperation IN NUMBER);

    FUNCTION GetPriorOperNDFL(p_Rate IN INTEGER, p_TypeNOB IN INTEGER, p_KBK IN VARCHAR2, p_SpecialTag IN VARCHAR2 DEFAULT CHR(1), p_Sys IN INTEGER DEFAULT 1) RETURN NUMBER DETERMINISTIC;

END RSI_NPTX;
/
