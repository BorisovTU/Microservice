/**
 * Пакет для работы с финансовыми инструментами
 */
CREATE OR REPLACE PACKAGE RSI_RSB_FIInstr IS
  -- Author  : Popov S
  -- Created : 23.06.2005
  ZERO_DATE CONSTANT DATE := TO_DATE('01010001', 'DDMMYYYY');
  MAX_TIME CONSTANT DATE := TO_DATE('0001-01-01 23:59:59','YYYY-MM-DD HH24:MI:SS');
  UNSET_CHAR CONSTANT CHAR(1) := CHR(0);
  SET_CHAR CONSTANT CHAR(1) := CHR(88);

  g_CrossCurrency NUMBER;

  -- Ошибки
  ERR_RATETYPE_NOT_FOUND CONSTANT INTEGER := 1477; -- Не задан вид курса

  -- Виды вычисляемы курсов
  RATEEXP_MLT_PR CONSTANT INTEGER := 1; --
  RATEEXP_MLT_CF CONSTANT INTEGER := 2; --
  RATEEXP_PLS_PR CONSTANT INTEGER := 3; --
  RATEEXP_MNS_PR CONSTANT INTEGER := 4; --
  RATEEXP_PLS_VL CONSTANT INTEGER := 5; --
  RATEEXP_MNS_VL CONSTANT INTEGER := 6; --

-- Виды ц/б
       FIKIND_ALLAVOIRISS         CONSTANT NUMBER := 0;  -- Все ценные бумаги

       AVOIRKIND_SHARE               CONSTANT NUMBER := 20; -- Акция
          AVOIRKIND_EQUITY_SHARE     CONSTANT NUMBER := 1;  -- Акция обыкновенная
          AVOIRKIND_PREFERENCE_SHARE CONSTANT NUMBER := 2;  -- Акция привилегированная
          AVOIRKIND_PREF_CONV_SHARE  CONSTANT NUMBER := AVOIRKIND_PREFERENCE_SHARE;  -- Акция привилегированная с правом конверсии

       AVOIRKIND_BOND                  CONSTANT NUMBER := 17; -- Облигация
          AVOIRKIND_BOND_STATE         CONSTANT NUMBER := 21; -- Государственная
             AVOIRKIND_BOND_FED        CONSTANT NUMBER := 22; -- Федеральная облигация
                AVOIRKIND_BOND_GKO     CONSTANT NUMBER := 23;
                AVOIRKIND_BOND_OFZ     CONSTANT NUMBER := 24;
                AVOIRKIND_BOND_OBR     CONSTANT NUMBER := 25;
                AVOIRKIND_BOND_OGVZ    CONSTANT NUMBER := 26;
                AVOIRKIND_BOND_OVVZ    CONSTANT NUMBER := 27;
                AVOIRKIND_BOND_EVRO    CONSTANT NUMBER := 28;
                AVOIRKIND_BOND_OVERFED CONSTANT NUMBER := 29;
             AVOIRKIND_BOND_PARTY      CONSTANT NUMBER := 30; --Субъектов федерации
                AVOIRKIND_BOND_PARTY_BOND CONSTANT NUMBER := 38;--Субъектов федерации->Облигация
                AVOIRKIND_BOND_PARTY_EVRO CONSTANT NUMBER := 39;-- Субъектов федерации->Еврооблигация
          AVOIRKIND_BOND_MUNICIPAL           CONSTANT NUMBER := 31;-- Муниципальная
            AVOIRKIND_BOND_MUNICIPAL_BOND CONSTANT NUMBER := 40;-- Муниципальная->Облигация
            AVOIRKIND_BOND_MUNICIPAL_EVRO CONSTANT NUMBER := 41;-- Муниципальная->Еврооблигация
          AVOIRKIND_BOND_CORPORATE           CONSTANT NUMBER := 32;-- Корпоративная
            AVOIRKIND_BOND_CORPORATE_BOND CONSTANT NUMBER := 42;-- Корпоративная->Облигация
            AVOIRKIND_BOND_CORPORATE_EVRO CONSTANT NUMBER := 43;-- Корпоративная->Еврооблигация
            AVOIRKIND_BOND_CORPORATE_BIO  CONSTANT NUMBER := 52;-- Корпоративная->БИО
          AVOIRKIND_BOND_CREDIT           CONSTANT NUMBER := 37; -- Кредитная нота

 -- Устаревшие значения
      AVOIRKIND_ORDINARY_BOND          CONSTANT NUMBER := AVOIRKIND_BOND;-- Облигация бескупонная
      AVOIRKIND_COUPON_BOND            CONSTANT NUMBER := AVOIRKIND_BOND;-- Облишация купонная
      AVOIRKIND_CONV_BOND              CONSTANT NUMBER := AVOIRKIND_BOND;-- Облигация с правом конверсии
      AVOIRKIND_PROMISSORY_NOTE        CONSTANT NUMBER := AVOIRKIND_BOND;-- Долговое обязательство
      AVOIRKIND_COUPON_BOND_AD         CONSTANT NUMBER := AVOIRKIND_BOND;-- Облишация купонная АД
      AVOIRKIND_ORDINARY_BOND_AD       CONSTANT NUMBER := AVOIRKIND_BOND;-- Облигация бескупонная АД
 --

      AVOIRKIND_INVESTMENT_SHARE     CONSTANT NUMBER := 16;-- Инвестиционный пай
      AVOIRKIND_OPTION               CONSTANT NUMBER := 36;-- Опцион эмитента
      AVOIRKIND_DEPOSITORY_RECEIPT   CONSTANT NUMBER := 10;-- Депозитарная расписка
         AVOIRISSKIND_AMERICAN_DEPREC  CONSTANT NUMBER := 45;-- Американская депозитарная расписка
         AVOIRISSKIND_GLOBAL_DEPREC    CONSTANT NUMBER := 46;-- Глобальная депозитарная расписка
         AVOIRISSKIND_RUSSIAN_DEPREC   CONSTANT NUMBER := 47;-- Российская депозитарная расписка
      AVOIRKIND_DEPOS_RECEIPT        CONSTANT NUMBER := AVOIRKIND_DEPOSITORY_RECEIPT;-- Депозитарная расписка
      AVOIRKIND_BILL                 CONSTANT NUMBER := 5; -- Вексель

      AVOIRKIND_BANKCERT                CONSTANT NUMBER := 33;-- Банковский сертификат
         AVOIRKIND_DEPOSIT_CERTIFICATE  CONSTANT NUMBER := 9; -- Депозитный сертификат
         AVOIRKIND_DEPOS_CERTIF         CONSTANT NUMBER := AVOIRKIND_DEPOSIT_CERTIFICATE;  -- Депозитный сертификат
         AVOIRKIND_SAVING_CERTIF        CONSTANT NUMBER := 12; -- Сберегательный сертификат
         AVOIRKIND_SAVING_CERTIFICATE   CONSTANT NUMBER := AVOIRKIND_SAVING_CERTIF;-- Сберегательный сертификат

      AVOIRKIND_STORAGE_CERTIF        CONSTANT NUMBER := 13; -- Складское свидетельство
      AVOIRKIND_STORAGE_CERTIFICATE   CONSTANT NUMBER := AVOIRKIND_STORAGE_CERTIF; -- Складское свидетельство
      AVOIRKIND_DRAFT                 CONSTANT NUMBER := 6; -- Чек
      AVOIRKIND_BILL_OF_LADING        CONSTANT NUMBER := 34;-- Коносамент
      AVOIRKIND_MORTGAGE              CONSTANT NUMBER := 18;-- Закладная
      AVOIRKIND_HYPOTHECARY_CERT      CONSTANT NUMBER := 35;-- Ипотечный сертификат
      AVOIRKIND_WARRANT               CONSTANT NUMBER := 44;-- Варрант
      AVOIRISSKIND_BASKET             CONSTANT NUMBER := 48;-- Корзина ценных бумаг
      AVOIRKIND_KSU                   CONSTANT NUMBER := 51;-- Клиринговый сертификат участия
      AVOIRKIND_BOND_USSR             CONSTANT NUMBER := 53;--облигации - государственные ценные бумаги бывшего СССР и стран-участников Союзного государства
-- END "Виды ц/б"

-- Вид дохода Ц/Б
      FI_INCOME_TYPE_UNKNOWN  CONSTANT NUMBER := 0;
      FI_INCOME_TYPE_PERCENT  CONSTANT NUMBER := 1;
      FI_INCOME_TYPE_DISCOUNT CONSTANT NUMBER := 2;
      FI_INCOME_TYPE_COUPON   CONSTANT NUMBER := 3;


      FIKIND_ALLFI      CONSTANT NUMBER := 0;    --Все финансовые инструменты
      FIKIND_CURRENCY   CONSTANT NUMBER := 1;    --Валюта
      FIKIND_AVOIRISS   CONSTANT NUMBER := 2;    --Ценная бумага
      FIKIND_INDEX      CONSTANT NUMBER := 3;    --Индекс
      FIKIND_DERIVATIVE CONSTANT NUMBER := 4;    --Производные инструменты (ПИ)
      FIKIND_METAL      CONSTANT NUMBER := 6;    --Драгоценный металл
      FIKIND_ARTICLE    CONSTANT NUMBER := 7;    --Артикул
      FIKIND_SPECIAL    CONSTANT NUMBER := 8;    --Специальные финансовые инструменты
      FIKIND_CREDIT     CONSTANT NUMBER := 9;    --Кредит

--перенести в исходники
--Вид расчета дохода цб
--      FI_INCOMECALC_TYPE_NO     CONSTANT NUMBER := 0;
--      FI_INCOMECALC_TYPE_COUPON CONSTANT NUMBER := 1;
--      FI_INCOMECALC_TYPE_ISSUE  CONSTANT NUMBER := 2;

      NATCUR   CONSTANT NUMBER := 0; --   национальная валюта

   FI_ERROR_20200    CONSTANT INTEGER := -20200; --Не финансовый инструмент
   FI_ERROR_20201    CONSTANT INTEGER := -20201; --Пересечение купонных периодов

   EURO_BOND_BASE    CONSTANT NUMBER := 1;   -- Базис расчет 30E/360 - в месяце 30 дней, в году 360 дней (Eurobond)

   TYPE FI_KIND_TYPE IS TABLE OF dfininstr_dbt.t_fi_kind%TYPE INDEX BY PLS_INTEGER;

/*
  --Функция приводит ставку к нормальному виду: точность не меньше 4х и повышается, если ставка меньше 1.
  PROCEDURE MakeNormRate
  (
    p_Numerator NUMBER
   ,p_Denominator NUMBER
   ,p_Rate OUT NUMBER
   ,p_Scale OUT NUMBER
   ,p_Point OUT NUMBER
   ,p_IsInverse OUT CHAR
  );
*/
/*
  --Функция определения курса по формуле (1): Rate = (r1*m2*(10**s2)) / (r2*m1*(10**s1))
  PROCEDURE DetermineRate_1
  (
    p_Rate1 NUMBER
   ,p_Scale1 NUMBER
   ,p_Point1 NUMBER
   ,p_Rate2 NUMBER
   ,p_Scale2 NUMBER
   ,p_Point2 NUMBER
   ,p_Rate OUT NUMBER
   ,p_Scale OUT NUMBER
   ,p_Point OUT NUMBER
   ,p_IsInverse OUT CHAR
  );
*/
/*
  --Функция определения курса по формуле (2): Rate = (m1*m2*(10**(s2+s1))) / (r1*r2)
  PROCEDURE DetermineRate_2
  (
    p_Rate1 NUMBER
   ,p_Scale1 NUMBER
   ,p_Point1 NUMBER
   ,p_Rate2 NUMBER
   ,p_Scale2 NUMBER
   ,p_Point2 NUMBER
   ,p_Rate OUT NUMBER
   ,p_Scale OUT NUMBER
   ,p_Point OUT NUMBER
   ,p_IsInverse OUT CHAR
  );
*/
/*
  --Функция определения курса по формуле (3): Rate = (r1*r2) / (m1*m2*(10**(s2+s1)))
  PROCEDURE DetermineRate_3
  (
    p_Rate1 NUMBER
   ,p_Scale1 NUMBER
   ,p_Point1 NUMBER
   ,p_Rate2 NUMBER
   ,p_Scale2 NUMBER
   ,p_Point2 NUMBER
   ,p_Rate OUT NUMBER
   ,p_Scale OUT NUMBER
   ,p_Point OUT NUMBER
   ,p_IsInverse OUT CHAR
  );
*/
/*
  --Функция определения курса по формуле (4): Rate = (r2*m1*(10**s1)) / (r1*m2*(10**s2))
  PROCEDURE DetermineRate_4
  (
    p_Rate1 NUMBER
   ,p_Scale1 NUMBER
   ,p_Point1 NUMBER
   ,p_Rate2 NUMBER
   ,p_Scale2 NUMBER
   ,p_Point2 NUMBER
   ,p_Rate OUT NUMBER
   ,p_Scale OUT NUMBER
   ,p_Point OUT NUMBER
   ,p_IsInverse OUT CHAR
  );
*/
  /*
    Определение курса между валютами, на основании кросс-курсов между этими валютами и третьей валюты.

    Замечания к методике определения курса:   Rate =  BRate / QRate = R1 / R2.

    Как известно, курс определяется так:
       в прямой котировке                R = r / (m * (10**s)), r - норм.курс, m - масштаб, s - точность
       в обратной котировке              R = (m * (10**s)) / r

    Существует 4 варианта отношения (в зависимости от прямой или обратной котировки):

    1) R1 - прямая,   R2 - прямая.   Rate = (r1*m2*(10**s2)) / (r2*m1*(10**s1))
    2) R1 - обратная, R2 - прямая.   Rate = (m1*m2*(10**(s2+s1))) / (r1*r2)
    3) R1 - прямая,   R2 - обратная. Rate = (r1*r2) / (m1*m2*(10**(s2+s1)))
    4) R1 - обратная, R2 - обратная. Rate = (r2*m1*(10**s1)) / (r1*m2*(10**s2))
  */
  PROCEDURE FI_DetermineRate
  (
    p_Rate1 NUMBER --числитель (курс между базовой и кроссируемой валютой)
   ,p_Scale1 NUMBER
   ,p_Point1 NUMBER
   ,p_IsInverse1 CHAR
   ,p_Rate2 NUMBER --знаменатель (курс между котируемой и кроссируемой валютой)
   ,p_Scale2 NUMBER
   ,p_Point2 NUMBER
   ,p_IsInverse2 CHAR
   ,p_Rate OUT NUMBER --возвращаемый курс
   ,p_Scale OUT NUMBER
   ,p_Point OUT NUMBER
   ,p_IsInverse OUT CHAR
  );

  PROCEDURE GetLastErrorMessage( ErrMes OUT VARCHAR2 );

  FUNCTION FI_ReturnIncomeRate return NUMBER;

  FUNCTION ConvSum_ex
  (
    SumB     IN NUMBER
   ,pFromFI  IN NUMBER
   ,pToFI    IN NUMBER
   ,pbdate   IN DATE
   ,pRecRateDef IN  DRATEDEF_DBT%ROWTYPE
   ,pRevflag  IN OUT VARCHAR2
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл.
   ,pOnlyRate IN NUMBER DEFAULT 0
   ,pISMANUALINPUT   IN NUMBER DEFAULT -1
  )
  RETURN NUMBER;

  FUNCTION ConvSum_ex2
  (
    SumB     IN NUMBER
   ,pFromFI  IN NUMBER
   ,pToFI    IN NUMBER
   ,pbdate   IN DATE
   ,pRecRateDef IN  DRATEDEF_DBT%ROWTYPE
   ,pRevflag  IN OUT VARCHAR2
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл.
   ,pOnlyRate IN NUMBER DEFAULT 0
   ,pRateType  IN OUT NUMBER --Вид курса, если < -1 не нужно возвращать параметры [pRateType; pIsInverse],= -1 курс еще не определен, = 0 определять не нужно
   ,pRate      OUT NUMBER --Курс
   ,pScale     OUT NUMBER --Масштаб
   ,pPoint     OUT NUMBER --Округление
   ,pIsInverse OUT CHAR   --Признак обратной котировки
   ,pISMANUALINPUT   IN NUMBER DEFAULT -1
  )
  RETURN NUMBER;

/*
  --Функция пересчёта суммы в валюте по курсу к национальной валюте
  FUNCTION ConvSumNat
  (
    SumB       IN NUMBER           --конвертируемая сумма
   ,pFromFI    IN NUMBER           --валюта, для которой осуществляется конвертация
   ,pDirection IN CHAR             --направление конвертации: 'X' соответствует конвертации из заданной валюты в национальную, chr(0) - обратной конвертации из национальной валюты в заданную
   ,pConvDate  IN DATE             --Дата курса
   ,pRound     IN NUMBER DEFAULT 0 --признак округления (см. функцию ConvSum)
   ,pRateType  IN OUT NUMBER --Вид курса, если < -1 не нужно возвращать параметры [pRateType; pIsInverse],= -1 курс еще не определен, = 0 определять не нужно
   ,pRate      OUT NUMBER --Курс
   ,pScale     OUT NUMBER --Масштаб
   ,pPoint     OUT NUMBER --Округление
   ,pIsInverse OUT CHAR   --Признак обратной котировки
  )
  RETURN NUMBER;                 --Возвращается значение суммы
*/
/*
  --Функция пересчёта суммы в валюте по курсу заданного вида к национальной валюте
  FUNCTION ConvSumNatType
  (
    SumB       IN NUMBER           --конвертируемая сумма
   ,pFI        IN NUMBER           --валюта, для которой осуществляется конвертация
   ,pDirection IN CHAR             --направление конвертации: 'X' соответствует конвертации из заданной валюты в национальную, chr(0) - обратной конвертации из национальной валюты в заданную
   ,pConvDate  IN DATE             --Дата курса
   ,pType      IN NUMBER           --вид курса
   ,pRound     IN NUMBER DEFAULT 0 --признак округления (см. функцию ConvSum)
   ,pRateType  IN OUT NUMBER --Вид курса, если < -1 не нужно возвращать параметры [pRateType; pIsInverse],= -1 курс еще не определен, = 0 определять не нужно
   ,pRate      OUT NUMBER --Курс
   ,pScale     OUT NUMBER --Масштаб
   ,pPoint     OUT NUMBER --Округление
   ,pIsInverse OUT CHAR   --Признак обратной котировки
  )
  RETURN NUMBER;
*/

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
  RETURN NUMBER;       --Возвращается значение суммы

  -- Функция определения суммы конверсии за дату
  FUNCTION ConvSum
  (
    SumB     IN NUMBER  --Исходная сумма
   ,pFromFI  IN NUMBER  --Исходный Фин. инструмент
   ,pToFI    IN NUMBER  --Фин. инструмент, в который надо пересчитать
   ,pbdate   IN DATE    --Дата курса
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл. (<>0 - округл.)
  )
  RETURN NUMBER;       --Возвращается значение суммы

  -- Функция получения курса заданного типа
  FUNCTION ConvSumType2
  (
    SumB     IN NUMBER  --Исходная сумма
   ,pFromFI  IN NUMBER  --Исходный Фин. инструмент
   ,pToFI    IN NUMBER  --Фин. инструмент, в который надо пересчитать
   ,pType    IN NUMBER  --Тип курса
   ,pbdate   IN DATE    --Дата курса
   ,pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл.
   ,pRateType  IN OUT NUMBER --Вид курса, если <-1 то возвращать параметры [pRateType; pIsInverse] не нужно
   ,pRate      OUT NUMBER --Курс
   ,pScale     OUT NUMBER --Масштаб
   ,pPoint     OUT NUMBER --Округление
   ,pIsInverse OUT CHAR   --Признак обратной котировки
  )
  RETURN NUMBER;       --Возвращается значение суммы

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
  RETURN NUMBER;      --Возвращается значение суммы

  -- Функция получения курса
  function ConvSumMP(   SumB          IN NUMBER, --Исходная сумма
                        pFromFI       IN NUMBER, --Исходный Фин. инструмент
                        pToFI         IN NUMBER, --Фин. инструмент, в который надо пересчитать
                        pType         IN NUMBER, --Тип курса
                        pMarket_Place IN NUMBER, --Торговая площадка
                        pSection      IN NUMBER, --Секция торговой площадки
                        pbdate        IN DATE,   --Дата курса
                        pround   IN NUMBER DEFAULT 0 --признак округл. до копеек, по умолч. не округл.
                      ) return NUMBER;           --Возвращается значение суммы

-- Функция определения суммы конверсии с использованием данных о номинале ФИ
  function  ConvertSum(
                    SumB       IN NUMBER,               -- Исходная сумма
                    Rate       IN NUMBER,               -- Курс
                    Scale      IN NUMBER,               -- Масштаб
                    Point      IN NUMBER,               -- Округление
                    OutRate    IN CHAR,                 -- Признак обратной котировки
                    IsRelative IN CHAR,                 -- Признак относительной котировки
                    FaceValue  IN NUMBER,               -- Номинал (относительно чего задается курс, если он относительный)
                    FV_Scale   IN NUMBER,               -- Масштаб ФИ
                    FV_Point   IN NUMBER,               -- Округление ФИ
                    IsInverse  IN CHAR,                 -- Признак обратной котировки ФИ
          pround   IN NUMBER DEFAULT 0, --признак округл. до копеек, по умолч. не округл.
          pOnlyRate IN NUMBER DEFAULT 0
           ) return NUMBER;                              --Возвращается значение суммы
---------------------------------------------------------------------------------------------

  function FI_GetCurrentNominal( pFIID              IN NUMBER,
                                 pCurrentNominal    IN OUT NUMBER,
                                 pNominalPoint      IN OUT NUMBER,
                                 pDate              IN DATE,
                                 pIsClosed          IN NUMBER DEFAULT 0
                               ) return NUMBER;

  function FI_GetNominalOnDate( pFIID              IN NUMBER,
                                pDate              IN DATE,
                                pIsClosed          IN NUMBER DEFAULT 0
                              ) return NUMBER;


  function FI_GetNominal( pFIID              IN NUMBER,
                          pPoint             IN OUT NUMBER,
                          pNominal_lrate     IN OUT NUMBER,
                          pDate              IN DATE DEFAULT TO_DATE( '01.01.0001', 'DD.MM.YYYY' )
                        ) return NUMBER;

  function FI_GetQTYOnDate( pFIID  IN NUMBER,
                            pDate  IN DATE
                          ) return NUMBER;

  function FI_GetIssuerOnDate( pFIID  IN NUMBER,
                            pDate  IN DATE
                          ) return NUMBER;

  function FI_GetObjCodeOnDate( pFIID  IN NUMBER,
                                pObjectType IN NUMBER,
                                pCodeKind IN NUMBER,
                                pDate  IN DATE
                              ) return NUMBER;


  function FI_GetFaceValueFI( pFIID          IN NUMBER
                            ) return NUMBER;


  function  PrepareRate(
                    Rate       IN NUMBER,           -- Курс
                    Scale      IN NUMBER,           -- Масштаб
                    Point      IN NUMBER,           -- Округление
                    OutRate    IN BOOLEAN,             -- Признак обратной котировки
                    IsRelative IN BOOLEAN,             -- Признак относительной котировки
                    FaceValue  IN NUMBER,           -- Номинал (относительно чего задается курс, если он относительный)
                    FV_Scale   IN NUMBER,
                    FV_Point   IN NUMBER,
                    IsInverse  IN BOOLEAN,
                    vnumerator   IN OUT NUMBER,    -- Числитель !!!
                    vdenominator IN OUT NUMBER     -- Знаменатель !!!
                    ) return NUMBER;

  function  FI_IsAvoirissPartly(
                    FIID                     IN NUMBER,
                    sum_all_rate_partly      IN OUT NUMBER,
                    pDate                    IN DATE,
                    IsClose                  IN NUMBER DEFAULT 0 /* 1 - T_ISCLOSE, 2 - T_SPISCLOSE, 3 - T_TSISCLOSE.  */
                    ) return NUMBER;

  function  FI_GetIncomeType( FIID IN NUMBER    --FIID ценной бумаги
                            ) return NUMBER;    --Возвращает тип дохода из анкеты ЦБ

  function  FI_IsCouponAvoiriss( FIID IN NUMBER  --FIID ценной бумаги
                               ) return NUMBER;  --Возвращает 1 если бумага купонная иначе 0

  function  FI_IsCouponFI( FIID IN NUMBER  --FIID ценной бумаги
                         ) return NUMBER;

  function  CalcNKD_Ex( FIID     IN NUMBER,
                        CalcDate IN DATE,
                        Amount   IN NUMBER,
                        LastDate IN NUMBER,
                        CorrectDate IN NUMBER DEFAULT 0,
                        UseCoupRateHist IN NUMBER DEFAULT 1
                      ) return NUMBER;

  function  CalcNKD_Ex_NoRound( FIID     IN NUMBER,
                                CalcDate IN DATE,
                                Amount   IN NUMBER,
                                LastDate IN NUMBER,
                                CorrectDate IN NUMBER DEFAULT 0,
                                UseCoupRateHist IN NUMBER DEFAULT 1
                              ) return NUMBER;

  function  CalcNKD( FIID       IN NUMBER,
                     CalcDate   IN DATE,
                     Amount     IN NUMBER,
                     LastDate   IN NUMBER,
                     CorrectDate IN NUMBER DEFAULT 0,
                     NoRound    IN NUMBER DEFAULT 0,
                     UseCoupRateHist IN NUMBER DEFAULT 1
                   ) return NUMBER;

  function rsNDaysf(DATE1 IN DATE, DATE2 IN DATE) return NUMBER;

  function rsNDaysp( d1 IN DATE, CorrectDate IN NUMBER DEFAULT 0, IsEuroBondBase IN NUMBER DEFAULT 0 ) return NUMBER; -- возвращает количество дней в базисе 360/30
  --AV 07.08.00 - теперь здесь считается сумма НКД одного купона за весь период его дествия
  function CouponsSum( FIID       IN NUMBER,
                       CoupNumber IN DFIWARNTS_DBT.T_NUMBER%TYPE,
                       FaceValue  IN NUMBER,
                       CouponSum  IN OUT NUMBER,
                       DaysInYear IN NUMBER,
                       T_all      IN NUMBER,
                       NKDBase_Kind IN NUMBER,
                       Amount     IN NUMBER DEFAULT 1,
                       UseLatestKnownRate IN NUMBER DEFAULT 0, -- Признак использовать последнюю известную ставку для неопределенных купонов
                       IsFirstPeriodFirstCoupon IN NUMBER DEFAULT 0,
                       CntCoupPaymsInYear IN NUMBER DEFAULT 0,
                       CoupHistID IN NUMBER DEFAULT 0 -- ID записи истории изменения ставки купона
                     ) return NUMBER;

  function FI_GetMinRate(
                    pFromFI     IN  NUMBER,
                    pToFI       IN  NUMBER,
                    pType       IN  NUMBER,
                    pDate       IN  DATE,
                    pNDays      IN  NUMBER,
                    pRateID     OUT NUMBER,
                    pSinceDate  OUT DATE ) return NUMBER;

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

  function FI_GetRate(
                    pFromFI     IN  NUMBER,
                    pToFI       IN  NUMBER,
                    pType       IN  NUMBER,
                    pDate       IN  DATE,
                    pNDays      IN  NUMBER,
                    pIsMaxMin   IN  NUMBER,
                    pRateID     OUT NUMBER,
                    pSinceDate  OUT DATE,
                    pIsMrkt     IN  BOOLEAN DEFAULT FALSE,
                    pMarketCountry IN VARCHAR2 DEFAULT CHR(1),
                    pIsForeignMarket IN NUMBER DEFAULT 0,
                    pOnlyRate IN NUMBER DEFAULT 0,
                    pCanUseCross IN NUMBER DEFAULT 0,
                    pMarket_Place IN NUMBER DEFAULT -1,
                    pISMANUALINPUT  IN NUMBER DEFAULT -1 ) return NUMBER;
  /*Функция определяет значение курса вида для указанной торговой площаддки на дату ... если указан pOnlyThisDate то только за эту дату */
  function FI_GetRateMP( pFromFI       IN NUMBER,
                         pToFI         IN NUMBER,
                         pType         IN NUMBER,
                         pDate         IN DATE,
                         pMarket_Place IN NUMBER, --Торговая площадка
                         pSection      IN NUMBER, --Секция торговой площадки
                         pOnlyThisDate IN NUMBER DEFAULT 0 ) return NUMBER;

  FUNCTION FI_IsAvrKindBond( AvoirKind IN NUMBER ) RETURN BOOLEAN DETERMINISTIC;
  FUNCTION FI_AvrKindsGetRoot( FI_Kind IN NUMBER, AvoirKind IN NUMBER ) RETURN  NUMBER DETERMINISTIC;
  FUNCTION FI_AvrKindsGetRootByFIID( FIID IN NUMBER ) RETURN  NUMBER DETERMINISTIC;
  FUNCTION FI_AvrKindsEQ( FI_Kind IN NUMBER, AvoirKind IN NUMBER, CheckAvoirKind IN NUMBER ) RETURN NUMBER DETERMINISTIC;
  FUNCTION FI_IsKSU( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;
  FUNCTION FI_IsBasket( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;
  FUNCTION FI_IsISU( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;
  FUNCTION FI_IsBIO( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

  -- Функция определения суммы конверсии по кросскурсам
  FUNCTION CalcSumCross2
  (
    SumB     IN NUMBER   --Исходная сумма
   ,pFromFI  IN NUMBER   --Исходный Фин. инструмент
   ,pToFI    IN NUMBER   --Фин. инструмент, в который надо пересчитать
   ,pbdate   IN DATE     --Дата курса
   ,pround   IN NUMBER DEFAULT 1 --признак округл. до копеек, по умолч округл.
   ,pRateType  IN OUT NUMBER --Вид курса, если <-1 то возвращать параметр pRateType не нужно
   ,pRate      OUT NUMBER --Курс
   ,pScale     OUT NUMBER --Масштаб
   ,pPoint     OUT NUMBER --Округление
   ,pIsInverse OUT CHAR   --Признак обратной котировки
  ) RETURN NUMBER;       --Возвращается значение суммы

  -- Функция определения суммы конверсии по кросскурсам
  FUNCTION CalcSumCross
  (
    SumB     IN NUMBER   --Исходная сумма
   ,pFromFI  IN NUMBER   --Исходный Фин. инструмент
   ,pToFI    IN NUMBER   --Фин. инструмент, в который надо пересчитать
   ,pbdate   IN DATE     --Дата курса
   ,pround   IN NUMBER DEFAULT 1 --признак округл. до копеек, по умолч округл.
  ) RETURN NUMBER;       --Возвращается значение суммы


  -- Возвращает накопленный процентный доход по облигаци
  -- Применяется как для купонных, так и для бескупонных облигаций с указанным доходом.
  FUNCTION FI_CalcIncomeValue( FIID      IN NUMBER, -- Выпуск ц/б
                               CalcDate  IN DATE,   -- Дата начисления
                               Amount    IN NUMBER, -- Количество
                               LastDate  IN NUMBER, -- Признак начисления дохода в дату погашения:
                                                    --   0 - в дату погашения доход равен нулю
                                                    --   1 - в дату погашения доход начисляется
                               CorrectDate IN NUMBER DEFAULT 0, --  Признак коррекции последней даты месяца
                                                    --   0 - CalcDate не корректируется в соответствии с базисом расчета (умолч.)
                                                    --   1 - CalcDate корректируется в соответствии с базисом расчета
                               NoRound IN NUMBER DEFAULT 0, --  Признак округления
                                                    --   0 - округлять (умолч.)
                                                    --   1 - не округлять
                               UseLatestKnownRate IN NUMBER DEFAULT 0 -- Признак использовать последнюю известную ставку для неопределенных купонов
                             ) RETURN NUMBER DETERMINISTIC;

  FUNCTION FI_CalcTotalIncome( FIID      IN NUMBER, -- Выпуск ц/б
                               Coupon    IN VARCHAR2,--Номер купона (не задается для выпуска)
                               Amount    IN NUMBER  -- Количество
                             ) RETURN NUMBER DETERMINISTIC;

  -- Проверяет, имеет ли ц/б купоны
  FUNCTION FI_HasCoupon( FIID IN NUMBER ) RETURN BOOLEAN DETERMINISTIC;
  -- Проверяет, имеет ли ц/б купоны. Можно использовать в SQL
  FUNCTION FI_HasCouponSQL( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;
  -- Проверяет, имеет ли ц/б частичные погашения
  FUNCTION FI_HasPartialDischarge( FIID IN NUMBER ) RETURN BOOLEAN DETERMINISTIC;
  -- Проверяет, имеет ли ц/б частичные погашения. Можно использовать в SQL
  FUNCTION FI_HasPartialDischargeSQL( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;
  -- Проверяет, является ли ц/б надежной
  FUNCTION FI_IsResponsible( FIID IN NUMBER, OperDate IN DATE ) RETURN NUMBER DETERMINISTIC;
  -- Проверяет, является ли ц/б квалифицированной
  FUNCTION FI_IsQualified( FIID IN NUMBER, OnDate IN DATE ) RETURN NUMBER DETERMINISTIC;
  -- Проверяет, является ли ц/б обращающейся
  FUNCTION FI_CirculateInMarket( FIID IN NUMBER, OperDate IN DATE ) RETURN NUMBER DETERMINISTIC;
  --Получение значения категории с номером GroupID по ц/б (OBJTYPE_AVOIRISS) на дату
  PROCEDURE FI_FindObjAttrOnDate( FIID      IN NUMBER,
                                  OperDate  IN DATE,
                                  GroupID   IN NUMBER,
                                  NumInList OUT dobjattr_dbt.t_NumInList%TYPE
                                );
  -- определяет надежность определения ТСС
  FUNCTION FI_ExistNOSS( FIID IN NUMBER, OperDate IN DATE, GroupID IN NUMBER ) RETURN NUMBER DETERMINISTIC;
  -- Вычисляет суммарный процент ЧП на дату, с учетом статуса "Оплачена", если IsClosed != 0.
  FUNCTION FI_GetPartialPersent( FIID IN NUMBER, CalcDate IN DATE, IsClosed IN NUMBER DEFAULT 0 ) RETURN NUMBER DETERMINISTIC;
  -- Вернуть процент ЧП
  FUNCTION FI_GetPartialPersentByName( pFIID              IN NUMBER,
                                       pNumber            IN VARCHAR2
                                     ) return NUMBER DETERMINISTIC;
  -- Вернуть дату погашения ЧП
  FUNCTION FI_GetPartialDrawingDate( pFIID              IN NUMBER,
                                     pNumber            IN VARCHAR2
                                   ) return DATE DETERMINISTIC;

  -- Вернуть дату погашения купона
  FUNCTION FI_GetCouponDrawingDate( pFIID              IN NUMBER,
                                    pNumber            IN VARCHAR2
                                  ) return DATE DETERMINISTIC;

  -- Получить дату погашения последнего известного купона
  FUNCTION FI_GetDateLastKnownCoupon(pFIID IN NUMBER) RETURN DATE;

  -- Получить ближайшую дату оферты
  FUNCTION FI_GetOfferDate( FIID IN NUMBER, BegDate IN DATE ) RETURN DATE;

  -- Вернуть дату погашения выпуска
  FUNCTION FI_GetNominalDrawingDate( pFIID IN NUMBER,
                                     pTermless IN CHAR DEFAULT '',
                                     pBegDate IN DATE DEFAULT ZERO_DATE
                                   ) return DATE /*DETERMINISTIC*/;

  -- проверяем нужно ли искать курс НКД за дату и если нужно то ищем
  FUNCTION FindNKDCource( p_FIID IN NUMBER, p_CalcDate IN DATE, p_IsTrust IN NUMBER DEFAULT 0) RETURN NUMBER;
  -- Вычисляет  накопленный процентный доход по облигации на дату
  FUNCTION FI_CalcNKD( FIID      IN NUMBER,
                       CalcDate  IN DATE,
                       Amount    IN NUMBER,
                       IsTrust   IN NUMBER
                     ) RETURN NUMBER DETERMINISTIC;

  FUNCTION FI_IsSecurIndividual( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

  FUNCTION FI_IsSecurEmissive( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

  -- является ли фининстумент валютой
  FUNCTION FI_IsCurrency( p_FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

  -- Проверка, что на ЦБ установлен признак Право отказа от выплаты купона
  FUNCTION FI_IsAvoirissCouponRefuseRight( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

  -- Проверить установлен ли признак "Отказ от выплаты" на купоне
  FUNCTION FI_IsCouponPaymentRefuse( FIID IN NUMBER,
                                      CouponNumber IN dfiwarnts_dbt.t_Number%TYPE
                                    ) RETURN NUMBER;

  PROCEDURE SetNewQuotdefBuff
  (
    p_NewQuotdefBuff IN dquotdef_dbt%ROWTYPE
  );

  PROCEDURE SetNewQuothistBuff
  (
    p_NewQuothistBuff IN dquothist_dbt%ROWTYPE
  );

  PROCEDURE SetNewRatedefBuff
  (
    p_NewRatedefBuff IN dratedef_dbt%ROWTYPE
  );

  PROCEDURE SetNewRatehistBuff
  (
    p_NewRatehistBuff IN dratehist_dbt%ROWTYPE
  );

  -- Функция конвертации суммы по типу курса (внешние и внутренние курсы)
  FUNCTION FI_CalcSumType
  (
    p_Sum IN NUMBER -- сумма для пересчета в другую валюту
   ,p_FromFI IN NUMBER -- валюта
   ,p_ToFI IN NUMBER -- валюта
   ,p_Type IN NUMBER -- тип курса
   ,p_Date IN DATE DEFAULT ZERO_DATE -- дата курса
   ,p_Time IN DATE DEFAULT MAX_TIME  -- время курса
   ,p_Branch IN NUMBER DEFAULT 0-- подразделение
   ,p_CalcSum OUT NUMBER -- сумма пересчитанная по курсу
   ,p_RateValType OUT NUMBER -- параметр курса пересчета: тип курса
   ,p_RateValRate OUT NUMBER -- значение курса пересчета
   ,p_RateValScale OUT NUMBER -- масштаб курса пересчета
   ,p_RateValPoint OUT NUMBER -- точность курса пересчета
   ,p_RateValIsInverse OUT CHAR -- признак обратного курса пересчета
  )
  RETURN NUMBER;

  FUNCTION FI_CalcSumTypeEx
  (
    p_Sum IN NUMBER -- сумма для пересчета в другую валюту
   ,p_FromFI IN NUMBER -- валюта
   ,p_ToFI IN NUMBER -- валюта
   ,p_Type IN NUMBER -- тип курса
   ,p_Date IN DATE DEFAULT ZERO_DATE -- дата курса
   ,p_Time IN DATE DEFAULT MAX_TIME  -- время курса
  )
  RETURN NUMBER; -- сумма пересчитанная по курсу



  -- Функция определяет, есть ли купоны с нулевой суммой или ставкой по бумаге, до даты
  FUNCTION FI_HasZeroCoupons( FIID IN NUMBER, CalcDate IN DATE ) RETURN NUMBER DETERMINISTIC;

  -- Функция конвертации суммы обязательства/эквивалента по типу курса
  FUNCTION FI_GetRateCalcSumEqv
  (
    p_Code_Currency IN NUMBER -- Валюта обязательства
   ,p_CurrencyEq IN NUMBER -- Валюта-эквивалент
   ,p_CurrencyEq_RateDate IN NUMBER -- Смещение даты курса валюты-эквивалента в меньшую сторону от даты проводки для счетов НВПИ
   ,p_CurrencyEq_RateType IN NUMBER -- Вид курса валюты-эквивалента
   ,p_CurrencyEq_RateExtra IN NUMBER -- Наценка на курс валюты-эквивалента
   ,p_Date IN DATE DEFAULT ZERO_DATE -- Дата
   ,p_Direct IN NUMBER -- Направление конвертации: = 0 - из p_CurrencyEq в p_Code_Currency, != 0 - наоборот.
   ,p_Sum IN NUMBER -- Сумма для пересчета в другую валюту
   ,p_CalcSum OUT NUMBER -- Сумма пересчитанная по курсу
   ,p_Rate OUT NUMBER -- Курс пересчета
  )
  RETURN NUMBER;

  -- Определение суммы в ВО
  FUNCTION FI_CalcSumAccountFromEqv
  (
    p_Code_Currency IN NUMBER -- Валюта обязательства
   ,p_CurrencyEq IN NUMBER -- Валюта-эквивалент
   ,p_CurrencyEq_RateDate IN NUMBER -- Смещение даты курса валюты-эквивалента в меньшую сторону от даты проводки для счетов НВПИ
   ,p_CurrencyEq_RateType IN NUMBER -- Вид курса валюты-эквивалента
   ,p_CurrencyEq_RateExtra IN NUMBER -- Наценка на курс валюты-эквивалента
   ,p_Date IN DATE DEFAULT ZERO_DATE -- Дата
   ,p_Sum IN NUMBER -- Сумма для пересчета в другую валюту
   ,p_CalcSum OUT NUMBER -- Сумма пересчитанная по курсу
  )
  RETURN NUMBER;

  -- Определение суммы в ВЭ
  FUNCTION FI_CalcSumEqvFromAccount
  (
    p_Code_Currency IN NUMBER -- Валюта обязательства
   ,p_CurrencyEq IN NUMBER -- Валюта-эквивалент
   ,p_CurrencyEq_RateDate IN NUMBER -- Смещение даты курса валюты-эквивалента в меньшую сторону от даты проводки для счетов НВПИ
   ,p_CurrencyEq_RateType IN NUMBER -- Вид курса валюты-эквивалента
   ,p_CurrencyEq_RateExtra IN NUMBER -- Наценка на курс валюты-эквивалента
   ,p_Date IN DATE DEFAULT ZERO_DATE -- Дата
   ,p_Sum IN NUMBER -- Сумма для пересчета в другую валюту
   ,p_CalcSum OUT NUMBER -- Сумма пересчитанная по курсу
  )
  RETURN NUMBER;

  FUNCTION GetCrossCurrency
  RETURN NUMBER;

  -- Возвращает основной курс между валютами
  FUNCTION FI_GetDominantRate
  (
    p_QuotFI IN NUMBER       -- котируемый ФИ
   ,p_BaseFI IN NUMBER       -- базовый ФИ
   ,p_Rate OUT NUMBER        -- курс
   ,p_Scale OUT NUMBER       -- масштаб
   ,p_Point OUT NUMBER       -- точность
   ,p_IsInverse OUT CHAR     -- признак обратной котировки
   ,p_Date IN DATE           -- дата курса
  )
  RETURN NUMBER;

  -- Проверка - является ли финансовый инструмент или ценная бумага котируемой
  -- на заданную дату. Если дата не задана, то проверяем котируемость на
  -- текущую операционную дату. Если финансовый инструмент не ценная бумага,
  -- то считаем его котируемым.
   FUNCTION FI_IsQuoted (p_FIID IN NUMBER, p_OnDate IN DATE)
      RETURN NUMBER
      DETERMINISTIC;

  -- Проверка - есть ли на данный выпуск ссылки.
  -- Есть ли fininstr c MainFIID равным FIID данного.
   FUNCTION FI_IsMainAvr( FIID IN NUMBER )
     RETURN NUMBER
     DETERMINISTIC;

  -- число дней в году по базису.
  FUNCTION FI_GetDaysInYearByBase( pFIID IN NUMBER, pCalcDate IN DATE )
     return NUMBER;

  -- число дней в году
  FUNCTION FI_GetDaysInYear( CurYear IN NUMBER )
     return NUMBER;

  -- Получить последнюю известную ставку купона ФИ.
  FUNCTION FI_GetLatestKnownRate( FIID IN NUMBER )
     return NUMBER;
     
  -- количество выплат купонного дохода в году
  FUNCTION FI_CntCoupPayms( FIID IN NUMBER ) RETURN NUMBER;

 /**
 * Получение реального вида ФИ
 * Т.к. драгметаллы для клиентов заведены как базовый актив Валюта, то вид базового инструмента необходимо определять по буквенному ISO-коду валюты 
 * Если ISO-код начинается с латинской буквы "A", то ФИ - драгметалл
 * @since RSHB 108
 * @qtest NO
 * @param p_FIID Идентификатор ФИ
 * @return вид ФИ 
 */
  FUNCTION FI_GetRealFIKind(p_FIID IN NUMBER) RETURN NUMBER deterministic;

END RSI_RSB_FIInstr;
/
