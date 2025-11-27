CREATE OR REPLACE PACKAGE BODY NPTO IS
  -- Получение для бумаги группы НУ для НДФЛ
  FUNCTION GetPaperTaxGroupNPTX( pFIID IN NUMBER, pIsDerivative IN NUMBER DEFAULT -1 ) RETURN NUMBER
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetPaperTaxGroupNPTX( pFIID, pIsDerivative );
  END GetPaperTaxGroupNPTX;

  -- Возвращает максимальную дату периода расчета для клиента
  FUNCTION GetCalcPeriodDate( pKind IN NUMBER, pClientID IN NUMBER, pIIS IN CHAR DEFAULT CHR(0), pDlContrID IN NUMBER DEFAULT 0 ) RETURN DATE
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetCalcPeriodDate( pKind, pClientID, pIIS, 0, pDlContrID );
  END GetCalcPeriodDate;

  -- Выполняет поиск даты ТО
  function GetDateFromRQ( v_RQID IN NUMBER, v_FactDate IN DATE ) return DATE
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetDateFromRQ( v_RQID, v_FactDate );
  END GetDateFromRQ;

  -- Выполняет поиск даты платежа
  function GetDateFromPayment( v_PaymentID IN NUMBER, v_FactDate IN DATE ) return DATE
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetDateFromPayment( v_PaymentID, v_FactDate );
  END GetDateFromPayment;

  -- Определение категории ц/б - алгоритм 1
  FUNCTION Market1date( pFIID IN NUMBER, pDate IN DATE ) RETURN NUMBER
  IS
  BEGIN
    RETURN
      RSI_NPTO.Market1date( pFIID, pDate );
  END Market1date;

  -- Получение макс. рыночной цены
  FUNCTION GetMaxMarketPrice( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) RETURN NUMBER
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetMaxMarketPrice( pFIID, pDate, pDealID, pLotID );
  END GetMaxMarketPrice;

  -- Получение макс. рыночной цены
  FUNCTION GetMaxMarketPrice_DV( pDealID IN NUMBER ) RETURN NUMBER
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetMaxMarketPrice_DV( pDealID );
  END GetMaxMarketPrice_DV;

  -- Получение мин. рыночной цены
  FUNCTION GetMinMarketPrice( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) RETURN NUMBER
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetMinMarketPrice( pFIID, pDate, pDealID, pLotID );
  END GetMinMarketPrice;

  -- Получение мин. рыночной цены
  FUNCTION GetMinMarketPrice_DV( pDealID IN NUMBER ) RETURN NUMBER
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetMinMarketPrice_DV( pDealID );
  END GetMinMarketPrice_DV;

  -- Получение даты покупки из зачисления
  function GetDateFromAvrWrtIn( pDealID IN NUMBER, pStartDate IN DATE, pDealDate IN DATE ) return DATE
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetDateFromAvrWrtIn( pDealID, pStartDate, pDealDate );
  END GetDateFromAvrWrtIn;

  -- Получение цены из зачисления
  function GetPriceFromAvrWrtIn( pDealID IN NUMBER, pPrice IN NUMBER ) return NUMBER
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetPriceFromAvrWrtIn( pDealID, pPrice );
  END GetPriceFromAvrWrtIn;

  -- Получение стоимости из зачисления
  function GetCostFromAvrWrtIn( pDealID IN NUMBER, pCost IN NUMBER ) return NUMBER
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetCostFromAvrWrtIn( pDealID, pCost );
  END GetCostFromAvrWrtIn;

  -- Получение НКД из зачисления
  function GetNkdFromAvrWrtIn( pDealID IN NUMBER, pNKD IN NUMBER ) return NUMBER
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetNkdFromAvrWrtIn( pDealID, pNKD );
  END GetNkdFromAvrWrtIn;

  -- Получение затрат из зачисления
  function GetOutlayFromAvrWrtIn( pDealID IN NUMBER, pOutlay IN NUMBER ) return NUMBER
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetOutlayFromAvrWrtIn( pDealID, pOutlay );
  END GetOutlayFromAvrWrtIn;

  -- Определение категории ц/б - алгоритм 2
  FUNCTION Market2dates( pFIID IN NUMBER, pDate2 IN DATE, pDate1 IN DATE ) RETURN NUMBER
  IS
  BEGIN
    RETURN
      RSI_NPTO.Market2dates( pFIID, pDate2, pDate1 );
  END Market2dates;

  function GetMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return VARCHAR2
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetMarket( pFIID, pDate, pDealID, pLotID );
  END GetMarket;

  function GetMinMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return VARCHAR2
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetMinMarket( pFIID, pDate, pDealID, pLotID );
  END GetMinMarket;

  function GetMaxMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return VARCHAR2
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetMaxMarket( pFIID, pDate, pDealID, pLotID );
  END GetMaxMarket;

  function GetMarket_DV( pDealID IN NUMBER ) return VARCHAR2
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetMarket_DV( pDealID );
  END GetMarket_DV;

  function GetDateMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return VARCHAR2
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetDateMarket( pFIID, pDate, pDealID, pLotID );
  END GetDateMarket;

  function GetMinDateMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return DATE
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetMinDateMarket( pFIID, pDate, pDealID, pLotID );
  END GetMinDateMarket;

  function GetMaxDateMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return DATE
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetMaxDateMarket( pFIID, pDate, pDealID, pLotID );
  END GetMaxDateMarket;

  function GetDateMarket_DV( pDealID IN NUMBER ) return DATE
  IS
  BEGIN
    RETURN
      RSI_NPTO.GetDateMarket_DV( pDealID );
  END GetDateMarket_DV;

  function IfMarket( pFIID IN NUMBER, pDate IN DATE ) return VARCHAR2
  IS
  BEGIN
    RETURN
      RSI_NPTO.IfMarket( pFIID, pDate );
  END IfMarket;

  function IfMarket_DV( pDealID IN NUMBER ) return VARCHAR2
  IS
  BEGIN
    RETURN
      RSI_NPTO.IfMarket_DV( pDealID );
  END IfMarket_DV;

  -- Проверка, является ли договор ИИС
  FUNCTION CheckContrIIS (pContrID IN NUMBER) RETURN NUMBER
  IS
  BEGIN
    RETURN
      RSI_NPTO.CheckContrIIS( pContrID );
  END CheckContrIIS;

  -- Проверка, является ли договор ИИС-III
  FUNCTION CheckContrIIS3 (pContrID IN NUMBER) RETURN NUMBER
  IS
  BEGIN
    RETURN 
      RSI_NPTO.CheckContrIIS3( pContrID );
  END CheckContrIIS3;

  -- Проверка что доход льготный
  FUNCTION IsFavourIncome( DDS IN DATE, DDB IN DATE, FIID IN NUMBER ) RETURN NUMBER
  IS
  BEGIN
    return RSI_NPTO.IsFavourIncome(DDS, DDB, FIID);
  END IsFavourIncome;

  -- Проверка наличия какого либо значения категории за период дат
  FUNCTION IsExistsAnyAttr( ObjectType IN NUMBER, Object IN VARCHAR2, GroupID IN NUMBER, Date1 IN DATE, Date2 IN DATE ) RETURN NUMBER
  IS
  BEGIN
    return RSI_NPTO.IsExistsAnyAttr(ObjectType, Object, GroupID, Date1, Date2);
  END IsExistsAnyAttr;

  -- Проверка наличия значения категории за период дат
  FUNCTION IsExistsAttr( ObjectType IN NUMBER, Object IN VARCHAR2, GroupID IN NUMBER, Date1 IN DATE, Date2 IN DATE, NumInList IN VARCHAR2 ) RETURN NUMBER
  IS
  BEGIN
    return RSI_NPTO.IsExistsAttr(ObjectType, Object, GroupID, Date1, Date2, NumInList);
  END IsExistsAttr;

  -- Проверка наличия значения категории за весь период дат
  FUNCTION IsExistsAttrAllDat( ObjectType IN NUMBER, Object IN VARCHAR2, GroupID IN NUMBER, Date1 IN DATE, Date2 IN DATE, NumInList IN VARCHAR2 ) RETURN NUMBER
  IS
  BEGIN
    return RSI_NPTO.IsExistsAttrAllDat(ObjectType, Object, GroupID, Date1, Date2, NumInList);
  END IsExistsAttrAllDat;

  -- Получить страну субъекта
  FUNCTION GetCountryParty( pPartyID IN NUMBER ) RETURN VARCHAR2
  IS
    v_Country VARCHAR2(3);
  BEGIN
    return RSI_NPTO.GetCountryParty(pPartyID, v_Country);
  END GetCountryParty;

  -- Проверяем, что корпоративная облигация с данным купоном или датой погашения попадает под налогообложение по ставке 35%
  FUNCTION IsCorpBondAfter2018( p_FIID IN NUMBER, p_WarrantNum IN VARCHAR2 ) RETURN NUMBER
  IS
  BEGIN
    return RSI_NPTO.IsCorpBondAfter2018(p_FIID, p_WarrantNum);
  END IsCorpBondAfter2018;

  -- Проверяем, что корпоративная облигация с датой погашения (купона) попадает под налогообложение по ставке 35%
  FUNCTION IsCorpBondAfter2018byDrawDate( p_FIID IN NUMBER, p_DrawingDate IN DATE ) RETURN NUMBER
  IS
  BEGIN
    return RSI_NPTO.IsCorpBondAfter2018byDrawDate(p_FIID, p_DrawingDate);
  END IsCorpBondAfter2018byDrawDate;

  FUNCTION GetTaxBaseCorpBondAfter2018(p_FIID IN NUMBER, --ц/б, по которой считается доход
                                       p_WarrantNum IN VARCHAR2, --номер купона/чп/дивидендов, по которым считается доход
                                       p_Quantity IN NUMBER
                                      ) RETURN NUMBER
  IS
  BEGIN
    return RSI_NPTO.GetTaxBaseCorpBondAfter2018(p_FIID, p_WarrantNum, p_Quantity);
  END GetTaxBaseCorpBondAfter2018;

END NPTO;
/
