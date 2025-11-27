CREATE OR REPLACE PACKAGE NPTO IS

/**
 * Получение для бумаги группы НУ для НДФЛ.
 * @since 6.20.030
 * @qtest NO
 * @param pFIID ID ц/б
 * @return Группа НУ для НДФЛ
 */
   FUNCTION GetPaperTaxGroupNPTX( pFIID IN NUMBER, pIsDerivative IN NUMBER DEFAULT -1 ) RETURN NUMBER;

/**
 * Возвращает максимальную дату периода расчета для клиента.
 * @since 6.20.030
 * @qtest NO
 * @param pKind Вид периода расчета
 * @param pClientID ID клиента
 * @param pIIS ID признак Индивидуальный инвестиционный счет(ИИС)
 * @return Дата
 */
   FUNCTION GetCalcPeriodDate( pKind IN NUMBER, pClientID IN NUMBER, pIIS IN CHAR DEFAULT CHR(0), pDlContrID IN NUMBER DEFAULT 0  ) RETURN DATE;

/**
 * Выполняет поиск даты ТО по примечанию "Дата для пересчета в рублевый эквивалент"
 * @since 6.20.030
 * @qtest NO
 * @param v_RQID ID платежа
 * @param v_FactDate Дата, которую вернет ф-я, если примечание по ТО не задано
 * @return Дата
 */
   function GetDateFromRQ( v_RQID IN NUMBER, v_FactDate IN DATE ) return DATE;

/**
 * Выполняет поиск даты платежа по примечанию "Дата для пересчета в рублевый эквивалент"
 * @since 6.20.031
 * @qtest NO
 * @param v_PaymentID ID платежа
 * @param v_FactDate Дата, которую вернет ф-я, если примечание по ТО не задано
 * @return Дата
 */
   function GetDateFromPayment( v_PaymentID IN NUMBER, v_FactDate IN DATE ) return DATE;

/**
 * Определить обращаемость на ОРЦБ для НДФЛ - алгоритм 1
 * @since 6.20.030
 * @qtest NO
 * @param pFIID ID ц/б
 * @param pDate Дата
 * @return 1 - обращается, 2 - не обращается
 */
   FUNCTION Market1date( pFIID IN NUMBER, pDate IN DATE ) RETURN NUMBER;

/**
 * Получение макс. рыночной цены для сделок с ценными бумагами
 * @since 6.20.030
 * @qtest NO
 * @param pFIID ID ц/б
 * @param pDate Дата
 * @param pDealID ID реальной сделки
 * @param pLotID ID лота НДФЛ
 * @return Значение рыночной цены
 */
   FUNCTION GetMaxMarketPrice( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) RETURN NUMBER;

/**
 * Получение макс. рыночной цены для сделок с ПФИ
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID сделки
 * @return Значение рыночной цены
 */
   FUNCTION GetMaxMarketPrice_DV( pDealID IN NUMBER ) RETURN NUMBER;

/**
 * Получение мин. рыночной цены для сделок с ценными бумагами
 * @since 6.20.030
 * @qtest NO
 * @param pFIID ID ц/б
 * @param pDate Дата
 * @param pDealID ID реальной сделки
 * @param pLotID ID лота НДФЛ
 * @return Значение рыночной цены
 */
   FUNCTION GetMinMarketPrice( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) RETURN NUMBER;

/**
 * Получение мин. рыночной цены для сделок с ПФИ
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID сделки
 * @return Значение рыночной цены
 */
   FUNCTION GetMinMarketPrice_DV( pDealID IN NUMBER ) RETURN NUMBER;

/**
 * Получение даты покупки из зачисления
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID сделки
 * @param pStartDate Дата исходной покупки
 * @param pDealDate Дата операции
 * @return Дата первоначальной покупки в НУ, если она задана, иначе pStartDate (если задана) или pDealDate.
 */
   function GetDateFromAvrWrtIn( pDealID IN NUMBER, pStartDate IN DATE, pDealDate IN DATE ) return DATE;

/**
 * Получение цены из зачисления
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID сделки
 * @param pPrice Цена из паспорта сделки
 * @return Вернуть цену покупки в НУ, а если она на задана - цену из паспорта сделки pPrice.
 */
   function GetPriceFromAvrWrtIn( pDealID IN NUMBER, pPrice IN NUMBER ) return NUMBER;

/**
 * Получение стоимости из зачисления
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID сделки
 * @param pCost Стоимость сделки
 * @return Стоимость при покупке в НУ из соотв. DDL_SUM, а если ее нет - Стоимость pCost.
 */
   function GetCostFromAvrWrtIn( pDealID IN NUMBER, pCost IN NUMBER ) return NUMBER;

/**
 * Получение НКД из зачисления
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID сделки
 * @param pNKD НКД
 * @return НКД, уплаченный при покупке в НУ из соотв. DDL_SUM, а если его нет - НКД pNKD.
 */
   function GetNkdFromAvrWrtIn( pDealID IN NUMBER, pNKD IN NUMBER ) return NUMBER;

/**
 * Получение затрат из зачисления
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID сделки
 * @param pOutlay Предв. затраты + Затраты на приобретение
 * @return Затраты при покупке в НУ из соотв. DDL_SUM, а если ее нет - (Предв. затраты + Затраты на приобретение) pOutlay.
 */
   function GetOutlayFromAvrWrtIn( pDealID IN NUMBER, pOutlay IN NUMBER ) return NUMBER;

/**
 * Определить обращаемость на ОРЦБ для НДФЛ - алгоритм 2
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID сделки
 * @param pDate2 Дата, на которую определяется обращаемость ц/б
 * @param pDate1 Дата, с которой определяется обращаемость ц/б
 * @return 1 - обращается, 2 - не обращаетсяб 3 - Потеряла обращаемость.
 */
   FUNCTION Market2dates( pFIID IN NUMBER, pDate2 IN DATE, pDate1 IN DATE ) RETURN NUMBER;

/**
 * Получение биржи, на кот. установлены курсы вида MinMarketPrice и MaxMarketPrice, для сделок с ценными бумагами
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ
 * @since 6.20.030
 * @qtest NO
 * @param pFIID ID ц/б
 * @param pDate Дата
 * @param pDealID ID реальной сделки
 * @param pLotID ID лота НДФЛ
 * @return Наименование биржи и сектора, на кот. установлен курс
 */
   function GetMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return VARCHAR2;

/**
 * Получение биржи, на кот. установлен курс вида MinMarketPrice, для сделок с ценными бумагами
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ
 * @since 6.20.030
 * @qtest NO
 * @param pFIID ID ц/б
 * @param pDate Дата
 * @param pDealID ID реальной сделки
 * @param pLotID ID лота НДФЛ
 * @return Наименование биржи и сектора, на кот. установлен курс
 */
   function GetMinMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return VARCHAR2;

/**
 * Получение биржи, на кот. установлен курс вида MaxMarketPrice, для сделок с ценными бумагами
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ
 * @since 6.20.030
 * @qtest NO
 * @param pFIID ID ц/б
 * @param pDate Дата
 * @param pDealID ID реальной сделки
 * @param pLotID ID лота НДФЛ
 * @return Наименование биржи и сектора, на кот. установлен курс
 */
   function GetMaxMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return VARCHAR2;

/**
 * Получение биржи, на кот. установлен курс, для сделок с ПФИ
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID сделки
 * @return Наименование биржи, на кот. установлен курс
 */
   function GetMarket_DV( pDealID IN NUMBER ) return VARCHAR2;

/**
 * Получение дат курсов вида MinMarketPrice и MaxMarketPrice для сделок с ценными бумагами
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ
 * @since 6.20.030
 * @qtest NO
 * @param pFIID ID ц/б
 * @param pDate Дата
 * @param pDealID ID реальной сделки
 * @param pLotID ID лота НДФЛ
 * @return Дата курса (VARCHAR2)
 */
   function GetDateMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return VARCHAR2;

/**
 * Получение даты курса MinMarketPrice для сделок с ценными бумагами
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ
 * @since 6.20.030
 * @qtest NO
 * @param pFIID ID ц/б
 * @param pDate Дата
 * @param pDealID ID реальной сделки
 * @param pLotID ID лота НДФЛ
 * @return Дата курса
 */
   function GetMinDateMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return DATE;

/**
 * Получение даты курса MaxMarketPrice для сделок с ценными бумагами
 * для ц\б кроме pFIID и pDate передавать или pDealID (ID реальной сделки) или pLotID - ID лота НДФЛ
 * @since 6.20.030
 * @qtest NO
 * @param pFIID ID ц/б
 * @param pDate Дата
 * @param pDealID ID реальной сделки
 * @param pLotID ID лота НДФЛ
 * @return Дата курса
 */
   function GetMaxDateMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) return DATE;

/**
 * Получение даты курса для сделок с ПФИ
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID сделки
 * @return Дата курса
 */
   function GetDateMarket_DV( pDealID IN NUMBER ) return DATE;

/**
 * Получение признака обращаемости на ОРЦБ для сделок с ценными бумагами
 * @since 6.20.030
 * @qtest NO
 * @param pFIID ID ц/б
 * @param pDate Дата
 * @return 'X' - обращается, иначе - нет.
 */
   function IfMarket( pFIID IN NUMBER, pDate IN DATE ) return VARCHAR2;

/**
 * Получение признака обращаемости на ОРЦБ для сделок с ПФИ
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID сделки
 * @return 'X' - обращается, иначе - нет.
 */
   function IfMarket_DV( pDealID IN NUMBER ) return VARCHAR2;

/**
 * Проверка, является ли договор ИИС
 * @since 6.20.031
 * @qtest NO
 * @param pContrID ID ДО
 * @return 1 - ДО ИИС, 0 - ДО не ИИС.
 */
   FUNCTION CheckContrIIS (pContrID IN NUMBER) RETURN NUMBER DETERMINISTIC;

/**
 * Проверка, является ли договор ИИС-III
 * @since 6.20.031
 * @qtest NO   
 * @param pContrID ID ДО
 * @return 1 - ДО ИИС-III, 0 - ДО не ИИС-III. 
 */
   FUNCTION CheckContrIIS3 (pContrID IN NUMBER) RETURN NUMBER DETERMINISTIC;

/**
 * Проверка что доход льготный
 * @since 6.20.031
 * @qtest NO
 * @param DDS Дата поставки в сделке продажи из связи
 * @param DDB Дата поставки в сделке покупки из связи
 * @param FIID ID ц/б
 * @return 0 - не льготный, иначе - льготный.
 */
   FUNCTION IsFavourIncome( DDS IN DATE, DDB IN DATE, FIID IN NUMBER ) RETURN NUMBER;

/**
 * Проверка наличия какого либо значения категории за период дат
 * @since 6.20.031
 * @qtest NO
 * @param ObjectType Тип объекта
 * @param Object Объект
 * @param GroupID Группа
 * @param Date1 Дата начала периода
 * @param Date2 Дата конца периода
 * @return 0 - нет, 1 - да.
 */
   FUNCTION IsExistsAnyAttr( ObjectType IN NUMBER, Object IN VARCHAR2, GroupID IN NUMBER, Date1 IN DATE, Date2 IN DATE ) RETURN NUMBER;

/**
 * Проверка наличия значения категории за период дат
 * @since 6.20.031
 * @qtest NO
 * @param ObjectType Тип объекта
 * @param Object Объект
 * @param GroupID Группа
 * @param Date1 Дата начала периода
 * @param Date2 Дата конца периода
 * @param NumInList Номер признака
 * @return 0 - нет, 1 - да.
 */
   FUNCTION IsExistsAttr( ObjectType IN NUMBER, Object IN VARCHAR2, GroupID IN NUMBER, Date1 IN DATE, Date2 IN DATE, NumInList IN VARCHAR2 ) RETURN NUMBER;

/**
 * Проверка наличия значения категории за весь период дат
 * @since 6.20.031
 * @qtest NO
 * @param ObjectType Тип объекта
 * @param Object Объект
 * @param GroupID Группа
 * @param Date1 Дата начала периода
 * @param Date2 Дата конца периода
 * @param NumInList Номер признака
 * @return 0 - нет, 1 - да.
 */
   FUNCTION IsExistsAttrAllDat( ObjectType IN NUMBER, Object IN VARCHAR2, GroupID IN NUMBER, Date1 IN DATE, Date2 IN DATE, NumInList IN VARCHAR2 ) RETURN NUMBER;

/**
 * Получить страну субъекта
 * @since 6.20.031
 * @qtest NO
 * @param pPartyID Субъект
 * @return Страна субъекта (VARCHAR2)
 */
   FUNCTION GetCountryParty( pPartyID IN NUMBER ) RETURN VARCHAR2;

/**
 * Проверяем, что корпоративная облигация с данным купоном попадает под налогообложение по ставке 35%
 * @since 6.20.031
 * @qtest NO
 * @param p_FIID ID ц/б
 * @param p_WarrantNum номер купона ц/б
 * @return 1 - да, 0 - нет
 */
  FUNCTION IsCorpBondAfter2018( p_FIID IN NUMBER, p_WarrantNum IN VARCHAR2 ) RETURN NUMBER;

/**
 * Проверяем, что корпоративная облигация с датой погашения (купона) попадает под налогообложение по ставке 35%
 * @since 6.20.031
 * @qtest NO
 * @param p_FIID ID ц/б
 * @param p_DrawingDate дата погашения (купона)
 * @return 1 - да, 0 - нет
 */
  FUNCTION IsCorpBondAfter2018byDrawDate( p_FIID IN NUMBER, p_DrawingDate IN DATE ) RETURN NUMBER;

/**
 * Получит НОБ по корпоративной облигации с данным купоном, попадающей под налогообложение по ставке 35%
 * @since 6.20.031
 * @qtest NO
 * @param p_FIID ID ц/б
 * @param p_WarrantNum номер купона ц/б
 * @param p_Quantity кол-во ц/б
 * @return сумма НОБ
 */
  FUNCTION GetTaxBaseCorpBondAfter2018(p_FIID IN NUMBER, --ц/б, по которой считается доход
                                       p_WarrantNum IN VARCHAR2, --номер купона/чп/дивидендов, по которым считается доход
                                       p_Quantity IN NUMBER
                                      ) RETURN NUMBER;

END NPTO;
/
