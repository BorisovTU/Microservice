/**
 * Пакет для работы с рублевыми и валютными счетами
 */
CREATE OR REPLACE PACKAGE rsi_rsb_account
IS
  -- Author  : Nikonorov Evgeny
  -- Created : 17.03.2005
  -- Purpose :

  SUBTYPE DpDepCode_t          IS ddp_dep_dbt.t_Code%type;
  SUBTYPE DpDepStatus_t        IS ddp_dep_dbt.t_Status%type;
  SUBTYPE DpDepPartyID_t       IS ddp_dep_dbt.t_PartyID%type;
  SUBTYPE DpDepParentCode_t    IS ddp_dep_dbt.t_ParentCode%type;
  SUBTYPE BankDprtCheckData_t  IS dbankdprt_dbt.t_CheckData%type;

  SUBTYPE ObjcodeCode_t        IS dobjcode_dbt.t_Code%type;
  SUBTYPE BankDprtUERType_t    IS dbankdprt_dbt.t_UERType%type;

--  FUNCTION getBankDprtCheckData(p_Code in DpDepCode_t) RETURN BankDprtCheckData_t DETERMINISTIC;
--  FUNCTION getBankDprtUERType(p_Code IN ObjcodeCode_t) RETURN BankDprtUERType_t DETERMINISTIC;
--  PROCEDURE initAccountKeyData;

  /*
  ** CheckAccountKeyByDprtData - Установить ключ для счета
  ** IN
  **   p_Account - номер счета
  **   p_DprtData - БИК Банка счета
  ** RETURN
  **   VARCHAR2 - Номер счета с ключом
  */
  FUNCTION GetAccountKeyByDprtData(p_Account IN VARCHAR2, p_DprtData IN BankDprtCheckData_t) RETURN VARCHAR2;
  /*
  ** GetAccountKeyByDprtCode - Установить ключ для счета
  ** IN
  **   p_Account - номер счета
  **   p_Code - Ид. подразделения банка (по умолчанию тек.филиал банка)
  ** RETURN
  **   VARCHAR2 - Номер счета с ключом
  */
  FUNCTION GetAccountKeyByDprtCode(p_Account IN VARCHAR2, p_Code IN DpDepCode_t DEFAULT NULL) RETURN VARCHAR2;
  /*
  ** GetAccountKeyByPartyID - Установить ключ для счета
  ** IN
  **   p_Account - номер счета
  **   p_PartyID - Ид. банка
  ** RETURN
  **   VARCHAR2 - Номер счета с ключом
  */
  FUNCTION GetAccountKeyByPartyID(p_Account IN VARCHAR2, p_PartyID IN DpDepPartyID_t) RETURN VARCHAR2;
  /*
  ** CheckAccountKeyByDprtData - Проверить ключ счета
  ** IN
  **   p_Account - номер счета
  **   p_DprtData - БИК Банка счета
  ** RETURN
  **   NUMBER - Код ошибки
  **     0 - Ошибок в ключе счета нет
  **     1 - Ошибка в ключе счета
  */
  FUNCTION CheckAccountKeyByDprtData(p_Account IN VARCHAR2, p_DprtData IN BankDprtCheckData_t) RETURN NUMBER;
  /*
  ** CheckAccountKeyByDprtCode - Проверить ключ счета
  ** IN
  **   p_Account - номер счета
  **   p_Code - Ид. подразделения банка (по умолчанию тек.филиал банка)
  ** RETURN
  **   NUMBER - Код ошибки
  **     0 - Ошибок в ключе счета нет
  **     1 - Ошибка в ключе счета
  */
  FUNCTION CheckAccountKeyByDprtCode(p_Account IN VARCHAR2, p_Code IN DpDepCode_t DEFAULT NULL) RETURN NUMBER;
  /*
  ** CheckAccountKeyByPartyID - Проверить ключ счета
  ** IN
  **   p_Account - номер счета
  **   p_PartyID - Ид. банка
  ** RETURN
  **   NUMBER - Код ошибки
  **     0 - Ошибок в ключе счета нет
  **     1 - Ошибка в ключе счета
  */
  FUNCTION CheckAccountKeyByPartyID(p_Account IN VARCHAR2, p_PartyID IN DpDepPartyID_t) RETURN NUMBER;

  -- Функция возвращает параметр RsVox объекта по имени
  FUNCTION GetRsVoxPrmVal(PrmName IN VARCHAR2) RETURN RAW;

  -- Функция определения кредита для рублевых л/с за любой месяц ТЕКУЩЕГО года
FUNCTION kreditmontha
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_month    IN NUMBER
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
)
RETURN  drestdate_dbt.t_Credit%TYPE;

  -- Функция определения дебета для рублевых л/с за любой месяц ТЕКУЩЕГО года
FUNCTION debetmontha
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_month    IN NUMBER
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN drestdate_dbt.t_Debet%TYPE;
  -- Функция определения кредита для валютных л/с за любой месяц ТЕКУЩЕГО года
FUNCTION kreditmonthac
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_cur      IN NUMBER
 ,p_month    IN NUMBER
 ,p_rest_cur IN NUMBER DEFAULT NULL
)
RETURN drestdate_dbt.t_Credit%TYPE;
  -- Функция определения дебета для валютных л/с за любой месяц ТЕКУЩЕГО года
FUNCTION debetmonthac
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_cur      IN NUMBER
 ,p_month    IN NUMBER
 ,p_rest_cur IN NUMBER DEFAULT NULL
)
RETURN  drestdate_dbt.t_Debet%TYPE;
  -- Функция определения кредита рублевых л/с за период
FUNCTION kredita
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_date_t   IN DATE
 ,p_date_b   IN DATE
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN drestdate_dbt.t_Credit%TYPE;
  -- Функция определения полного кредита рублевых л/с за период (оборот + СПОД)
FUNCTION kredita_full
(
  p_account IN VARCHAR2
 ,p_chapter IN NUMBER
 ,p_date_t  IN DATE
 ,p_date_b  IN DATE
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN drestdate_dbt.t_Credit%TYPE;
  -- Функция определения дебета рублевых л/с за период
FUNCTION debeta
(
  p_account   IN VARCHAR2
 ,p_chapter   IN NUMBER
 ,p_date_t    IN DATE
 ,p_date_b    IN DATE
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN drestdate_dbt.t_Debet%TYPE;
  -- Функция определения полного дебета рублевых л/с за период (оборот + СПОД)
FUNCTION debeta_full
(
  p_account   IN VARCHAR2
 ,p_chapter   IN NUMBER
 ,p_date_t    IN DATE
 ,p_date_b    IN DATE
 ,p_cur      IN NUMBER DEFAULT NULL
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN drestdate_dbt.t_Debet%TYPE;
  -- Функция определения кредита валютных л/с за период
FUNCTION kreditac
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_cur      IN NUMBER
 ,p_date_t   IN DATE
 ,p_date_b   IN DATE
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN  drestdate_dbt.t_Credit%TYPE;
  -- Функция определения полного кредита валютных л/с за период (оборот + СПОД)
FUNCTION kreditac_full
(
  p_account IN VARCHAR2
 ,p_chapter IN NUMBER
 ,p_cur     IN NUMBER
 ,p_date_t  IN DATE
 ,p_date_b  IN DATE
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN drestdate_dbt.t_Credit%TYPE;
  -- Функция определения дебета валютных л/с за период
FUNCTION debetac
(
  p_account  IN VARCHAR2
 ,p_chapter  IN NUMBER
 ,p_cur      IN NUMBER
 ,p_date_t   IN DATE
 ,p_date_b   IN DATE
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN  drestdate_dbt.t_Debet%TYPE;
  -- Функция определения полного дебета валютных л/с за период (оборот + СПОД)
FUNCTION debetac_full
(
  p_account IN VARCHAR2
 ,p_chapter IN NUMBER
 ,p_cur     IN NUMBER
 ,p_date_t  IN DATE
 ,p_date_b  IN DATE
 ,p_rest_cur IN NUMBER DEFAULT NULL
) RETURN drestdate_dbt.t_Debet%TYPE;

  -- Функция нахождения остатков на рублевых л/с за любую дату
  FUNCTION resta
  ( 
    p_account  IN VARCHAR2
   ,p_date     IN DATE
   ,p_chapter  IN NUMBER
   ,p_r0       IN NUMBER
   ,p_cur      IN NUMBER DEFAULT NULL
   ,p_rest_cur IN NUMBER DEFAULT NULL
  ) RETURN NUMBER;

  -- Функция нахождения остатков на валютных л/с за любую дату
  FUNCTION restac
  ( 
    p_account  IN VARCHAR2
   ,p_cur      IN NUMBER
   ,p_date     IN DATE
   ,p_chapter  IN NUMBER
   ,p_r0       IN NUMBER
   ,p_rest_cur IN NUMBER DEFAULT NULL
  ) RETURN NUMBER
  ;

  -- Функция нахождения плановых остатков на рублевых л/с за любую дату
  FUNCTION planresta
  ( 
    p_account IN VARCHAR2
   ,p_date    IN DATE
   ,p_chapter IN NUMBER
   ,p_cur      IN NUMBER DEFAULT NULL
   ,p_rest_cur IN NUMBER DEFAULT NULL
  ) RETURN NUMBER;

  -- Функция нахождения плановых остатков на валютных л/с за любую дату
  FUNCTION planrestac
  ( 
    p_account IN VARCHAR2
   ,p_cur     IN NUMBER
   ,p_date    IN DATE
   ,p_chapter IN NUMBER
   ,p_rest_cur IN NUMBER DEFAULT NULL
  ) RETURN NUMBER;

   -- функция нахождения остатков на рублевых л/с (daccount_dbt) за период
   -- аналог макрофунции RestA
   FUNCTION restap (
      p_account   IN   VARCHAR2,
      p_dateb     IN   DATE,
      p_datee     IN   DATE,
      p_chapter   IN   NUMBER,
      p_r0        IN   drestdate_dbt.t_Rest%TYPE,
      p_cur      IN NUMBER DEFAULT NULL,
      p_rest_cur IN NUMBER DEFAULT NULL
   ) RETURN drestdate_dbt.t_Rest%TYPE;

   -- функция нахождения остатков на валютных л/с (daccount$_dbt) за период
   -- аналог макрофунции RestAC
   FUNCTION restapc (
      p_account   IN VARCHAR2,
      p_cur       IN NUMBER,
      p_dateb     IN DATE,
      p_datee     IN DATE,
      p_chapter   IN NUMBER,
      p_r0        IN drestdate_dbt.t_Rest%TYPE,
      p_rest_cur  IN NUMBER DEFAULT NULL
   ) RETURN drestdate_dbt.t_Rest%TYPE;

   -- Функция получения остатка на счёте по любой валюте на любую дату
   FUNCTION restall( p_account  IN daccount_dbt.t_account%type,         -- номер счёта
                     p_chapter  IN daccount_dbt.t_chapter%type,         -- уыртр
                     p_cur      IN daccount_dbt.t_code_currency%type,   -- валюта
                     p_date     IN DATE,                                -- дата
                     p_rest_cur IN NUMBER DEFAULT NULL                  -- валюта остатка
                   ) RETURN drestdate_dbt.t_Rest%TYPE;

  -- функция нахождения остатков на субсчетах (daccsub_dbt) на любую дату в рублях
  FUNCTION restsa
  (
    p_analitica  IN NUMBER,
    p_subaccount IN NUMBER,
    p_date       IN DATE,
    p_notuseconv IN NUMBER DEFAULT 0
  )   
  RETURN daccsubrd_dbt.t_Rest%TYPE;

  -- Функция определения дебета на субсчетах (daccsub_dbt) за период
  FUNCTION debetsa
  (
    p_analitica  IN NUMBER,
    p_subaccount IN NUMBER,
    p_date_from  IN DATE,
    p_date_till  IN DATE
  )
  RETURN daccsubrd_dbt.t_Debet%TYPE;

  -- Функция определения кредита на субсчетах (daccsub_dbt) за период
  FUNCTION creditsa
  (
    p_analitica  IN NUMBER,
    p_subaccount IN NUMBER,
    p_date_from  IN DATE,
    p_date_till  IN DATE
  )
  RETURN daccsubrd_dbt.t_Credit%TYPE;
  
  -- функция нахождения остатков на счетах балансового счета
  FUNCTION RestB( p_Chapter   IN INTEGER,
                  p_Balance   IN STRING,
                  p_NumPlan   IN INTEGER,
                  p_FIID      IN INTEGER,
                  p_RestDate  IN DATE,
                  sqlFilter   IN STRING DEFAULT NULL,
                  p_rest_cur  IN NUMBER DEFAULT NULL
                ) RETURN drestdate_dbt.t_Rest%TYPE;

  -- функция нахождения средних остатков на счетах балансового счета
  FUNCTION RestBalanceAverage( p_Chapter  IN NUMBER,
                               p_Balance  IN STRING,
                               p_NumPlan  IN NUMBER,
                               p_FIID     IN INTEGER,
                               p_DateTop  IN DATE,
                               p_DateBot  IN DATE,
                               sqlFilter  IN STRING DEFAULT NULL,
                               p_rest_cur IN NUMBER DEFAULT NULL
                             ) RETURN drestdate_dbt.t_Rest%TYPE ;

  -- функция нахождения дебетовых оборотов на счетах балансового счета
  FUNCTION DebetB( p_Chapter  IN NUMBER,
                   p_Balance  IN STRING,
                   p_NumPlan  IN NUMBER,
                   p_FIID     IN INTEGER,
                   p_DateTop  IN DATE,
                   p_DateBot  IN DATE,
                   sqlFilter  IN STRING DEFAULT NULL,
                   p_rest_cur IN NUMBER DEFAULT NULL
                 ) RETURN drestdate_dbt.t_Debet%TYPE;

  -- функция нахождения дебетовых оборотов на счетах балансового счета
  FUNCTION CreditB( p_Chapter  IN NUMBER,
                    p_Balance  IN STRING,
                    p_NumPlan  IN NUMBER,
                    p_FIID     IN INTEGER,
                    p_DateTop  IN DATE,
                    p_DateBot  IN DATE,
                    sqlFilter  IN STRING DEFAULT NULL,
                    p_rest_cur IN NUMBER DEFAULT NULL

                 ) RETURN drestdate_dbt.t_Credit%TYPE;

  --Функция получения остатка на счете за дату
/*  
  FUNCTION GetRestEx(
    p_Account    IN  VARCHAR2, --Номер счета
    p_Chapter    IN  NUMBER,   --Глава счета
    p_FIID       IN  NUMBER,   --Валюта счета
    p_OnDate     IN  DATE,     --Дата, на которую необходимо получить остаток
    p_TableName  IN  VARCHAR2, --Имя таблицы с остатками
    p_RestExists OUT NUMBER,   --выходной параметр - Остаток существует, если > 0
    p_Rest       OUT NUMBER,   --выходной параметр - Значение остатка
    p_RestPlan   OUT NUMBER,   --выходной параметр - Значение планового остатка
    p_RestDate   OUT DATE,     --Дата, на которую существует остаток
    p_rest_cur   IN NUMBER DEFAULT NULL -- Валюта остстка
  ) RETURN INTEGER;     
*/  

  --
  -- Получить значение лимита на дату
  --
  FUNCTION GetAccLimit(
                        p_Account  IN  VARCHAR2   -- Номер счета
                       ,p_Chapter  IN  INTEGER    -- Глава счета
                       ,p_FIID     IN  INTEGER    -- Валюта счета
                       ,p_BankDate IN  DATE       -- Дата, на которую необходимо вычислить значение лимита
                      )
  RETURN NUMBER;

  -- Получить дату последней записи об остатке для заданных счета и даты
  FUNCTION GetAccLastRestDate
  (
     p_AccountID     IN INTEGER -- Ид. счета
    ,p_Code_Currency IN INTEGER -- Валюта счета
    ,p_OnDate        IN DATE    -- Дата, на которую необходимо получить значение
  )
  RETURN DATE;

  -- Получить дату последней записи об остатке в ВЭ для заданных счета и даты
  FUNCTION GetAccLastRestDateEqv
  (
     p_AccountID     IN INTEGER -- Ид. счета
    ,p_OnDate        IN DATE    -- Дата, на которую необходимо получить значение
  )
  RETURN DATE;

  /**
  @brief Подсчитывает количество остатков на заданную дату
  @param[in] AccountID ID счета из таблицы daccount_dbt
  @param[in] restDate Дата, на которую необходимо подстчитать записи остатков
  @param[out] curZeroCount Количество нулевых остатков в валюте (RestCurrency != 0)
  @param[out] natcurNonZeroCount Количество ненулевых остатков в нац.валюте (RestCurrency = 0)
  @param[out] recordsCount Общее количество найденных записей на дату
  */ 
  PROCEDURE CheckAccountRest
  (
     AccountID           IN NUMBER  -- ///> ID счета
    ,restDate            IN DATE    -- ///> Дата остатков
    ,curZeroCount       OUT NUMBER  -- ///> Количество нулевых остатков в валюте (RestCurrency != 0)
    ,natcurNonZeroCount OUT NUMBER  -- ///> Количество ненулевых остатков в нац.валюте (RestCurrency = 0)
    ,recordsCount       OUT NUMBER  -- ///> Общее количество найденных записей на дату
  );

END rsi_rsb_account;
/
