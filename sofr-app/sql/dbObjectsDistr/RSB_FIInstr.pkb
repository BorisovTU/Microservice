CREATE OR REPLACE PACKAGE BODY RSB_FIInstr IS

  -- Функция определения суммы конверсии за дату
  FUNCTION ConvSum2
  (
    SumB     IN NUMBER  --Исходная сумма
   ,pFromFI  IN NUMBER  --Исходный Фин. инструмент
   ,pToFI    IN NUMBER  --Фин. инструмент, в который надо пересчитать
   ,pbdate   IN DATE    --Дата курса
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл. (<>0 - округл.)
   ,pRateType  IN OUT NUMBER --Вид курса, если <-1 то возвращать параметры [pRateType; pIsInverse] не нужно
   ,pRate      OUT NUMBER --Курс
   ,pScale     OUT NUMBER --Масштаб
   ,pPoint     OUT NUMBER --Округление
   ,pIsInverse OUT CHAR   --Признак обратной котировки
  )
  RETURN NUMBER
  IS
  BEGIN

    RETURN
      RSI_RSB_FIInstr.ConvSum2
      (
        SumB
       ,pFromFI
       ,pToFI
       ,pbdate
       ,pround
       ,pRateType
       ,pRate
       ,pScale
       ,pPoint
       ,pIsInverse
      );

  END ConvSum2;

  -- Функция определения суммы конверсии за дату
  FUNCTION ConvSum
  (
    SumB     IN NUMBER
   ,pFromFI  IN NUMBER
   ,pToFI    IN NUMBER
   ,pbdate   IN DATE
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл.
  )
  RETURN NUMBER
  IS
  BEGIN

    RETURN
      RSI_RSB_FIInstr.ConvSum
      (
        SumB
       ,pFromFI
       ,pToFI
       ,pbdate
       ,pround
      );

  END ConvSum;

  -- Функция получения курса заданного типа
  FUNCTION ConvSumType
  (
    SumB     IN NUMBER  --Исходная сумма
   ,pFromFI  IN NUMBER  --Исходный Фин. инструмент
   ,pToFI    IN NUMBER  --Фин. инструмент, в который надо пересчитать
   ,pType    IN NUMBER  --Тип курса
   ,pbdate   IN DATE    --Дата курса
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл.
  )
  RETURN NUMBER
  IS
  BEGIN

    RETURN
      RSI_RSB_FIInstr.ConvSumType
      (
        SumB
       ,pFromFI
       ,pToFI
       ,pType
       ,pbdate
       ,pround
      );

  END ConvSumType;

  -- Определить относится ли подвид CheckAvrKind к виду AvrKind
  -- т.е. является ли AvrKind родителем CheckAvrKind
  FUNCTION FI_AvrKindsEQ( FI_Kind IN NUMBER, AvoirKind IN NUMBER, CheckAvoirKind IN NUMBER )
  RETURN NUMBER DETERMINISTIC
  IS
  BEGIN
    RETURN
      RSI_RSB_FIInstr.FI_AvrKindsEQ(FI_Kind, AvoirKind, CheckAvoirKind);
  END FI_AvrKindsEQ;

  -- Функция возвращает корневой подвид ц/б (первого уровня) подвида AvoirKind
  FUNCTION FI_AvrKindsGetRoot( FI_Kind IN NUMBER, AvoirKind IN NUMBER )
  RETURN NUMBER DETERMINISTIC
    IS
  BEGIN
    RETURN
      RSI_RSB_FIInstr.FI_AvrKindsGetRoot( FI_Kind, AvoirKind );
  END FI_AvrKindsGetRoot;

  --Вычисляет текущий объем выпуска на дату
  function FI_GetQTYOnDate( pFIID  IN NUMBER,
                            pDate  IN DATE
                          )
  RETURN NUMBER
    IS
  BEGIN
    RETURN
      RSI_RSB_FIInstr.FI_GetQTYOnDate(pFIID, pDate);
  END FI_GetQTYOnDate;

  --Вычисляет текущего эмитента на дату
  function FI_GetIssuerOnDate( pFIID  IN NUMBER,
                            pDate  IN DATE
                          )
  RETURN NUMBER
    IS
  BEGIN
    RETURN
      RSI_RSB_FIInstr.FI_GetIssuerOnDate(pFIID, pDate);
  END FI_GetIssuerOnDate;

  --возвращает t_AutoKey кода, действующего на дату, без учета активен код или нет
  function FI_GetObjCodeOnDate( pFIID  IN NUMBER,
                                pObjectType IN NUMBER,
                                pCodeKind IN NUMBER,
                                pDate  IN DATE
                              )
  RETURN NUMBER
    IS
  BEGIN
    RETURN
      RSI_RSB_FIInstr.FI_GetObjCodeOnDate(pFIID, pObjectType, pCodeKind,  pDate);
  END FI_GetObjCodeOnDate;

  --квалифицированная/неквалифицированная ц/б на дату
  /*
  Неквалифицированные ценные бумаги:
  -  ценные бумаги, эмитент которых не является резидентом, и для которых задано значение кате-гории "Квалификация в качестве ценной бумаги" равное "Не квалифицирована в качестве ценной бумаги"

  Квалифицированные ценные бумаги:
  -  все ценные бумаги, эмитент которых является резидентом,
  -  ценные бумаги, эмитент которых не является резидентом, и для которых НЕ задано значение категории "Квалификация:" равное "Не квалифицирована в качестве ценной бумаги"

  !!! т.е.  квалифицированными считаем все бумаги, кроме отмеченных как "неквалифицирован-ные";
  Другими словами, ц/б, для которых категория "Квалификация:" не задана, считаем квалифицированными.
  */
  FUNCTION FI_IsQualified( FIID IN NUMBER, OnDate IN DATE )
  RETURN NUMBER DETERMINISTIC
    IS
  BEGIN
    RETURN
      RSI_RSB_FIInstr.FI_IsQualified( FIID, OnDate);
  END FI_IsQualified;

  -- проверяет, имеет ли ц\б признак индивидуальной
  FUNCTION FI_IsSecurIndividual( FIID IN NUMBER )
  RETURN NUMBER DETERMINISTIC
    IS
  BEGIN
    RETURN
      RSI_RSB_FIInstr.FI_IsSecurIndividual( FIID );
  END FI_IsSecurIndividual;

  -- проверяет, имеет ли ц\б признак эмиссионной
  FUNCTION FI_IsSecurEmissive( FIID IN NUMBER )
  RETURN NUMBER DETERMINISTIC
    IS
  BEGIN
    RETURN
      RSI_RSB_FIInstr.FI_IsSecurEmissive( FIID );
  END FI_IsSecurEmissive;

  -- Проверяет, является ли ц/б купонной
  function  FI_IsCouponAvoiriss( FIID IN NUMBER )
  return NUMBER
    IS
  BEGIN
    RETURN
      RSI_RSB_FIInstr.FI_IsCouponAvoiriss( FIID );
  END FI_IsCouponAvoiriss;

  -- проверка - является ли данная ц/б КСУ
  FUNCTION FI_IsKSU( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
  begin
    return RSI_RSB_FIInstr.FI_IsKSU(FIID);
  END FI_IsKSU;

  -- проверка - является ли данная ц/б корзиной
  FUNCTION FI_IsBasket( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
  begin
    return RSI_RSB_FIInstr.FI_IsBasket(FIID);
  END FI_IsBasket;

  -- проверка - является ли данная ц/б ИСУ
  FUNCTION FI_IsISU( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
  begin
    return RSI_RSB_FIInstr.FI_IsISU(FIID);
  END FI_IsISU;

  --минимальное значение из всех курсов вида pType на дату pDate,
  --но эта дата должна быть не ранее, чем за pNDays дней от pDate
  function FI_GetMinRateMonth(
                    pFromFI     IN  NUMBER,
                    pToFI       IN  NUMBER,
                    pType       IN  NUMBER,
                    pDate       IN  DATE,
                    pNMonths    IN  NUMBER,
                    pRateID     OUT NUMBER,
                    pSinceDate  OUT DATE,
                    pMarketCountry IN VARCHAR2 DEFAULT CHR(1),
                    pIsForeignMarket IN NUMBER DEFAULT 0,
                    pOnlyRate IN NUMBER DEFAULT 0)
  return NUMBER
    IS
  BEGIN
    RETURN
      RSI_RSB_FIInstr.FI_GetMinRateMonth( pFromFI,
                                          pToFI,
                                          pType,
                                          pDate,
                                          pNMonths,
                                          pRateID,
                                          pSinceDate,
                                          pMarketCountry,
                                          pIsForeignMarket,
                                          pOnlyRate );
  END FI_GetMinRateMonth;

  -- Расчет НКД
  FUNCTION  CalcNKD_Ex( FIID       IN NUMBER,
                        CalcDate   IN DATE,
                        Amount     IN NUMBER,
                        LastDate   IN NUMBER,
                        CorrectDate IN NUMBER DEFAULT 0
                      ) return NUMBER
  IS
  BEGIN
     return RSI_RSB_FIInstr.CalcNKD_Ex( FIID, CalcDate, Amount, LastDate, CorrectDate );
  END CalcNKD_Ex;

  -- проверяем нужно ли искать курс НКД за дату и если нужно то ищем */
  FUNCTION FindNKDCource( p_FIID IN NUMBER, p_CalcDate IN DATE, p_IsTrust IN NUMBER DEFAULT 0) RETURN NUMBER
  IS
  BEGIN
     return RSI_RSB_FIInstr.FindNKDCource( p_FIID, p_CalcDate, p_IsTrust );
  END FindNKDCource;

  -- Вернуть процент ЧП
  FUNCTION FI_GetPartialPersentByName( pFIID              IN NUMBER,
                                       pNumber            IN VARCHAR2
                                     ) return NUMBER DETERMINISTIC
  IS
  BEGIN
     return RSI_RSB_FIInstr.FI_GetPartialPersentByName( pFIID, pNumber );
  END FI_GetPartialPersentByName;

  -- Функция определяет значение курса вида для указанной торговой площаддки на дату ... если указан pOnlyThisDate то только за эту дату */
  function FI_GetRateMP( pFromFI       IN NUMBER,
                         pToFI         IN NUMBER,
                         pType         IN NUMBER,
                         pDate         IN DATE,
                         pMarket_Place IN NUMBER, --Торговая площадка
                         pSection      IN NUMBER, --Секция торговой площадки
                         pOnlyThisDate IN NUMBER DEFAULT 0 ) return NUMBER
  IS
  BEGIN
     return RSI_RSB_FIInstr.FI_GetRateMP( pFromFI,
                                          pToFI,
                                          pType,
                                          pDate,
                                          pMarket_Place,
                                          pSection,
                                          pOnlyThisDate);
  END FI_GetRateMP;

  function FI_GetNominalOnDate( pFIID              IN NUMBER,
                                pDate              IN DATE,
                                pIsClosed          IN NUMBER
                              ) return NUMBER
  is
  begin
    return RSI_RSB_FIInstr.FI_GetNominalOnDate( pFIID, pDate, pIsClosed );
  end FI_GetNominalOnDate;

  -- Функция определяет, есть ли купоны с нулевой суммой или ставкой по бумаге, до даты
  FUNCTION FI_HasZeroCoupons( FIID IN NUMBER, CalcDate IN DATE ) RETURN NUMBER DETERMINISTIC
  is
  begin
    return RSI_RSB_FIInstr.FI_HasZeroCoupons( FIID, CalcDate );
  end FI_HasZeroCoupons;

  -- Проверка - является ли финансовый инструмент или ценная бумага котируемой
  -- на заданную дату. Если дата не задана, то проверяем котируемость на
  -- текущую операционную дату. Если финансовый инструмент не ценная бумага,
  -- то считаем его котируемым.
  FUNCTION FI_IsQuoted (p_FIID IN NUMBER, p_OnDate IN DATE)
     RETURN NUMBER
     DETERMINISTIC
  IS
  BEGIN
    RETURN RSI_RSB_FIInstr.FI_IsQuoted (p_FIID, p_OnDate);
  END;

  -- Проверка - есть ли на данный выпуск ссылки.
  -- Есть fininstr c MainFIID равным FIID данного.
  FUNCTION FI_IsMainAvr( FIID IN NUMBER )
     RETURN NUMBER
     DETERMINISTIC
  IS
  BEGIN
    RETURN RSI_RSB_FIInstr.FI_IsMainAvr( FIID );
  END;

   --получение статуса финансового инструмента на заданную дату
 FUNCTION FI_GetStatus ( pFIID IN NUMBER, pDate IN DATE )
   RETURN NUMBER
   --DETERMINISTIC
 IS
   Iss              davoiriss_dbt%ROWTYPE;
   Fi               dfininstr_dbt%ROWTYPE;
   pCheckDate       DATE;
   pExpiryDate      DATE;
   stat             NUMBER;
   v_DrawingDate    DATE;

 BEGIN
   stat :=  FI_STATE_UNDEFINE; --не задан
   pCheckDate := TO_DATE('01010001', 'DDMMYYYY');
   BEGIN
     select * into Iss from davoiriss_dbt where t_FIID = pFIID;
     v_DrawingDate := RSI_RSB_FIInstr.FI_GetNominalDrawingDate(pFIID, Iss.t_Termless);
   EXCEPTION
     when NO_DATA_FOUND then stat := FI_STATE_UNDEFINE;
   END;

   BEGIN
     select * into Fi from dfininstr_dbt where t_FIID = pFIID;
   EXCEPTION
     when NO_DATA_FOUND then stat := FI_STATE_UNDEFINE;
   END;

   if (Iss.t_BegPlacementDate != TO_DATE('01010001', 'DDMMYYYY'))then
      if (Iss.t_BegPlacementDate < Fi.t_Issued) then
        pCheckDate := Iss.t_BegPlacementDate;
      else
        pCheckDate := Fi.t_Issued;
      end if;
   else
      pCheckDate := Fi.t_Issued;
   end if;
   --размещение
   if( pDate >= pCheckDate AND pDate < Iss.t_InCirculationDate ) then
      stat := FI_STATE_PLACEMENT;
   end if;
   --в обращении
   if( pDate >= Iss.t_InCirculationDate AND ( v_DrawingDate = TO_DATE('01010001', 'DDMMYYYY') OR pDate < v_DrawingDate) AND (Fi.t_ExpiryDate =TO_DATE('01010001', 'DDMMYYYY') OR pDate < Fi.t_ExpiryDate) ) then
      stat := FI_STATE_INCIRCULATION;
   end if;
   --погашение
   if( v_DrawingDate != TO_DATE('01010001', 'DDMMYYYY') AND pDate >= v_DrawingDate AND (Fi.t_ExpiryDate = TO_DATE('01010001', 'DDMMYYYY') OR pDate < Fi.t_ExpiryDate) ) then
      stat := FI_STATE_DRAWING;
   end if;
   --закрыт
   if( pDate < pCheckDate OR (pDate > Fi.t_ExpiryDate AND  Fi.t_ExpiryDate != TO_DATE('01010001', 'DDMMYYYY')) ) then
      stat := FI_STATE_CLOSE;
   end if;

   return stat;
  END;

END RSB_FIInstr;
/