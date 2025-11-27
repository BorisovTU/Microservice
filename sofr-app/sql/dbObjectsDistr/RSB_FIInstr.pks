CREATE OR REPLACE PACKAGE RSB_FIInstr IS

 /*вычисляемые состояния ценных бумаг на заданный момент*/

   FI_STATE_UNDEFINE       CONSTANT INTEGER := 0;   --не задан
   FI_STATE_PLACEMENT      CONSTANT INTEGER := 1;   --размещение
   FI_STATE_INCIRCULATION  CONSTANT INTEGER := 2;   --в обращении
   FI_STATE_DRAWING        CONSTANT INTEGER := 3;   --погашение
   FI_STATE_CLOSE          CONSTANT INTEGER := 4;   --закрыт
/**
 * Функция определения суммы конверсии за дату
 * @param SumB       Исходная сумма
 * @param pFromFI    Исходный Фин. инструмент
 * @param pToFI      Фин. инструмент, в который надо пересчитать
 * @param pbdate     Дата курса
 * @param pround     признак округл. до копеек, по умолч. не округл. (<>0 - округл.)
 * @param pRateType  Вид курса, если <-1 то возвращать параметры [pRateType; pIsInverse] не нужно
 * @param pRate      Курс
 * @param pScale     Масштаб
 * @param pPoint     Округление
 * @param pIsInverse Признак обратной котировки
 * @return NUMBER    Возвращается значение суммы
 */
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
  RETURN NUMBER;       --Возвращается значение суммы

/**
 * Функция определения суммы конверсии за дату
 * @param SumB     Исходная сумма
 * @param pFromFI  Исходная Фин. инструмент
 * @param pToFI    Фин. инструмент, в который надо пересчитать
 * @param pbdate   Дата курса
 * @param pround   признак округл. до копеек, по умолч. не округл. (<>0 - округл.)
 * @return NUMBER  Возвращается значение суммы
 */
  FUNCTION ConvSum
  (
    SumB     IN NUMBER  --Исходная сумма
   ,pFromFI  IN NUMBER  --Исходный Фин. инструмент
   ,pToFI    IN NUMBER  --Фин. инструмент, в который надо пересчитать
   ,pbdate   IN DATE    --Дата курса
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл. (<>0 - округл.)
  )
  RETURN NUMBER;       --Возвращается значение суммы

/**
 * Функция получения курса заданного типа
 * @param SumB     Исходная сумма
 * @param pFromFI  Исходный Фин. инструмент
 * @param pToFI    Фин. инструмент, в который надо пересчитать
 * @param pType    Тип курса
 * @param pbdate   Дата курса
 * @param pround   признак округл. до копеек, по умолч. не округл.
 * @return NUMBER  Возвращается значение суммы
 */
  FUNCTION ConvSumType
  (
    SumB     IN NUMBER  --Исходная сумма
   ,pFromFI  IN NUMBER  --Исходный Фин. инструмент
   ,pToFI    IN NUMBER  --Фин. инструмент, в который надо пересчитать
   ,pType    IN NUMBER  --Тип курса
   ,pbdate   IN DATE    --Дата курса
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл.
  )
  RETURN NUMBER;      --Возвращается значение суммы

/**
 * Функция определяет, относится ли подвид CheckAvrKind к виду AvrKind ( иначе - является ли вид ц/б AvoirKind родителем CheckAvoirKind)
 * @param FI_Kind
 * @param AvoirKind
 * @param CheckAvoirKind
 * @return NUMBER
 */
  FUNCTION FI_AvrKindsEQ( FI_Kind IN NUMBER, AvoirKind IN NUMBER, CheckAvoirKind IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Функция возвращает корневой подвид ц/б (первого уровня) подвида AvoirKind
 * @param FI_Kind
 * @param AvoirKind
 * @return NUMBER
 */
  FUNCTION FI_AvrKindsGetRoot( FI_Kind IN NUMBER, AvoirKind IN NUMBER ) RETURN  NUMBER DETERMINISTIC;

/**
 * Функция получения объема выпуска ц/б на дату
 * @param pFIID  ид. ценной бумаги
 * @param pDate  дата
 * @return NUMBER объем выпуска
 */
  function FI_GetQTYOnDate( pFIID  IN NUMBER,
                            pDate  IN DATE
                          ) return NUMBER;

/**
 * Функция определяет текущего эмитента по выпуску на дату
 * @param pFIID  ид. ценной бумаги
 * @param pDate  дата
 * @return NUMBER эмитент на дату
 */
  function FI_GetIssuerOnDate( pFIID  IN NUMBER,
                            pDate  IN DATE
                          ) return NUMBER;

/**
 * Функция возвращает t_AutoKey кода, действующего на дату, без учета активен код или нет
 * @param pFIID  ид. ценной бумаги
 * @param pObjectType  тип объекта
 * @param pCodeKind  тип кода
 * @param pDate  дата
 * @return NUMBER t_AutoKey кода (может не быть)
 */
function FI_GetObjCodeOnDate( pFIID  IN NUMBER,
                              pObjectType IN NUMBER,
                              pCodeKind IN NUMBER,
                              pDate  IN DATE
                            ) return NUMBER;


/**
 * Проверяет, является ли ц/б квалифицированной на дату
 * @param FIID    ид. ценной бумаги
 * @param OnDate  дата
 * @return NUMBER Возвращает 1 если бумага квалифицированная, иначе 0
 */
  FUNCTION FI_IsQualified( FIID IN NUMBER, OnDate IN DATE ) RETURN NUMBER DETERMINISTIC;

/**
 * Проверяет, является ли ц/б индивидуальной
 * @param FIID    ид. ценной бумаги
 * @return NUMBER Возвращает 1 если бумага индивидуальная, иначе 0
 */
  FUNCTION FI_IsSecurIndividual( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Проверяет, является ли ц/б эмиссионной
 * @param FIID    ид. ценной бумаги
 * @return NUMBER Возвращает 1 если бумага эмиссионная, иначе 0
 */
  FUNCTION FI_IsSecurEmissive( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Проверяет, является ли ц/б купонной
 * @param FIID     ид. ценной бумаги
 * @return NUMBER  Возвращает 1 если бумага купонная, иначе 0
 */
  function  FI_IsCouponAvoiriss( FIID IN NUMBER  --FIID ценной бумаги
                               ) return NUMBER;  --Возвращает 1 если бумага купонная иначе 0


/**
 * Проверяет, является ли ц/б клиринговым сертификатом участия
 * @param FIID     ид. ценной бумаги
 * @return NUMBER  Возвращает 1 если бумага является КСУ, иначе 0
 */
  function  FI_IsKSU( FIID IN NUMBER  --FIID ценной бумаги
                    ) return NUMBER DETERMINISTIC;  --Возвращает 1 если бумага КСУ иначе 0


/**
 * Проверяет, является ли ц/б коризной
 * @param FIID     ид. ценной бумаги
 * @return NUMBER  Возвращает 1 если бумага является коризной, иначе 0
 */
  function  FI_IsBasket( FIID IN NUMBER  --FIID ценной бумаги
                    ) return NUMBER DETERMINISTIC;  --Возвращает 1 если бумага корзина иначе 0

/**
 * Проверяет, является ли ц/б ипотечным сертификатом участия
 * @param FIID     ид. ценной бумаги
 * @return NUMBER  Возвращает 1 если бумага является ИСУ, иначе 0
 */
  function  FI_IsISU( FIID IN NUMBER  --FIID ценной бумаги
                    ) return NUMBER DETERMINISTIC;  --Возвращает 1 если бумага ИСУ иначе 0



/**
 * Функция определяет минимальное значение из всех курсов вида pType на дату pDate,
 * но эта дата должна быть не ранее, чем за pNDays дней от pDate
 * @param pFromFI
 * @param pToFI
 * @param pType
 * @param pDate
 * @param pNMonths
 * @param pRateID
 * @param pSinceDate
 * @param pMarketCountry
 * @param pIsForeignMarket
 * @param pOnlyRate
 * @return NUMBER
 */
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
                    pOnlyRate IN NUMBER DEFAULT 0 ) return NUMBER;

/**
 * Функция расчета НКД
 * @param FIID
 * @param CalcDate
 * @param Amount
 * @param LastDate
 * @param CorrectDate
 * @return NUMBER
 */
  function  CalcNKD_Ex( FIID     IN NUMBER,
                        CalcDate IN DATE,
                        Amount   IN NUMBER,
                        LastDate IN NUMBER,
                        CorrectDate IN NUMBER DEFAULT 0
                      ) return NUMBER;

/**
 * проверяем нужно ли искать курс НКД за дату и если нужно то ищем
 * @param p_FIID
 * @param p_CalcDate
 * @param p_IsTrust
 * @return NUMBER
 */
  FUNCTION FindNKDCource( p_FIID IN NUMBER, p_CalcDate IN DATE, p_IsTrust IN NUMBER DEFAULT 0) RETURN NUMBER;

/**
 * Вернуть процент ЧП
 * @param pFIID
 * @param pNumber
 * @return NUMBER
 */
  FUNCTION FI_GetPartialPersentByName( pFIID              IN NUMBER,
                                       pNumber            IN VARCHAR2
                                     ) return NUMBER DETERMINISTIC;

/**
 * Функция определяет значение курса вида для указанной торговой площаддки на дату ... если указан pOnlyThisDate то только за эту дату
 * @param pFromFI
 * @param pToFI
 * @param pType
 * @param pDate
 * @param pMarket_Place  Торговая площадка
 * @param pSection       Секция торговой площадки
 * @param pOnlyThisDate
 * @return NUMBER
 */
  function FI_GetRateMP( pFromFI       IN NUMBER,
                         pToFI         IN NUMBER,
                         pType         IN NUMBER,
                         pDate         IN DATE,
                         pMarket_Place IN NUMBER, --Торговая площадка
                         pSection      IN NUMBER, --Секция торговой площадки
                         pOnlyThisDate IN NUMBER DEFAULT 0 ) return NUMBER;

/**
 * Функция определяет значение номинала на дату
 * @param pFIID
 * @param pDate
 * @param pIsClosed
 * @return NUMBER
 */
  function FI_GetNominalOnDate( pFIID              IN NUMBER,
                                pDate              IN DATE,
                                pIsClosed          IN NUMBER DEFAULT 0
                              ) return NUMBER;

/**
 * Функция определяет, есть ли купоны с нулевой суммой или ставкой по бумаге, до даты
 * @param FIID
 * @param CalcDate
 * @return NUMBER
 */
  FUNCTION FI_HasZeroCoupons( FIID IN NUMBER, CalcDate IN DATE ) RETURN NUMBER DETERMINISTIC;

/**
 * Проверка - является ли финансовый инструмент или ценная бумага котируемой
 * на заданную дату. Если дата не задана, то проверяем котируемость на
 * текущую операционную дату. Если финансовый инструмент не ценная бумага,
 * то считаем его котируемым.
 * @qtest  NO
 * @since  6.20.031.15.0
 * @param  p_FIID Идентификатор финансового инструмента
 * @param  p_OnDate Дата на которую проверяется котируемость
 * @return NUMBER Признак котируемости
 */
  FUNCTION FI_IsQuoted (p_FIID IN NUMBER, p_OnDate IN DATE)
     RETURN NUMBER
     DETERMINISTIC;

/**
 * Проверка - есть ли на данный выпуск ссылки.
 * Есть ли fininstr c MainFIID равным FIID данного.
 * @qtest  NO
 * @since  6.20.031.20.0
 * @param  FIID Идентификатор финансового инструмента
 * @return NUMBER Признак наличия ссылок
 */
  FUNCTION FI_IsMainAvr( FIID IN NUMBER )
     RETURN NUMBER
     DETERMINISTIC;

  /**получение статуса финансового инструмента на заданную дату
  * @param pFIID
  * @param pDate
  * @return NUMBER
  */
  FUNCTION FI_GetStatus ( pFIID IN NUMBER, pDate IN DATE )
    RETURN NUMBER
    /*DETERMINISTIC*/;

END RSB_FIInstr;
/