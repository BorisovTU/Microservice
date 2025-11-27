CREATE OR REPLACE PACKAGE BODY rsb_account
IS
  type tt_cache_NewNumAccDeal is table of pls_integer index by varchar2(9);
  T_CACHE_NEWNUMACCDEAL tt_cache_NewNumAccDeal;
  
   type tt_cache_NewNumAccContr is table of pls_integer index by varchar2(17);
  T_CACHE_NEWNUMACCCONTR tt_cache_NewNumAccContr;
  
   type tt_cache_NewNumAccContrF is table of pls_integer index by varchar2(14);
  T_CACHE_NEWNUMACCCONTRF tt_cache_NewNumAccContrF;

  ---------------------------------------------------------------------------
  -- Функция определения кредита рублевых л/с за период
  ---------------------------------------------------------------------------
  FUNCTION kredita
  (
    p_account IN VARCHAR2
   ,p_chapter IN NUMBER
   ,p_date_t  IN DATE
   ,p_date_b  IN DATE
   ,p_cur      IN NUMBER DEFAULT NULL
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER
  AS
  BEGIN

    RETURN
      rsi_rsb_account.kredita
      (
        p_account
       ,p_chapter
       ,p_date_t
       ,p_date_b
       ,p_cur
       ,p_rest_cur
      );

  END kredita;

  ---------------------------------------------------------------------------
  -- Функция определения дебета рублевых л/с за период
  ---------------------------------------------------------------------------
  FUNCTION debeta
  (
    p_account IN VARCHAR2
   ,p_chapter IN NUMBER
   ,p_date_t  IN DATE
   ,p_date_b  IN DATE
   ,p_cur      IN NUMBER DEFAULT NULL
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER
  AS
  BEGIN

    RETURN
      rsi_rsb_account.debeta
      (
        p_account
       ,p_chapter
       ,p_date_t
       ,p_date_b
       ,p_cur
       ,p_rest_cur
      );

  END debeta;

  ---------------------------------------------------------------------------
  -- Функция определения кредита валютных л/с за период
  ---------------------------------------------------------------------------
  FUNCTION kreditac
  (
    p_account IN VARCHAR2
   ,p_chapter IN NUMBER
   ,p_cur     IN NUMBER
   ,p_date_t  IN DATE
   ,p_date_b  IN DATE
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN  NUMBER deterministic
  AS
  BEGIN

    RETURN
      rsi_rsb_account.kreditac
      (
        p_account
       ,p_chapter
       ,p_cur
       ,p_date_t
       ,p_date_b
       ,p_rest_cur
      );

  END kreditac;

  ---------------------------------------------------------------------------
  -- Функция определения дебета валютных л/с за период
  ---------------------------------------------------------------------------
  FUNCTION debetac
  (
    p_account IN VARCHAR2
   ,p_chapter IN NUMBER
   ,p_cur     IN NUMBER
   ,p_date_t  IN DATE
   ,p_date_b  IN DATE
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN  NUMBER deterministic
  AS
  BEGIN

    RETURN
      rsi_rsb_account.debetac
      (
        p_account
       ,p_chapter
       ,p_cur
       ,p_date_t
       ,p_date_b
       ,p_rest_cur
      );

  END debetac;

  ---------------------------------------------------------------------------
  -- Функция нахождения остатков на рублевых л/с за любую дату.
  ---------------------------------------------------------------------------
  FUNCTION resta
  (
    p_account IN VARCHAR2
   ,p_date    IN DATE
   ,p_chapter IN NUMBER
   ,p_r0      IN NUMBER
   ,p_cur      IN NUMBER DEFAULT NULL
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER
  AS
  BEGIN

    RETURN
      rsi_rsb_account.resta
      (
        p_account
       ,p_date
       ,p_chapter
       ,p_r0
       ,p_cur
       ,p_rest_cur
      );

  END resta;

  -----------------------------------------------------------------------------
  -- Функция нахождения остатков на валютных л/с за любую дату
  -----------------------------------------------------------------------------
  FUNCTION restac
  (
    p_account IN VARCHAR2
   ,p_cur     IN NUMBER
   ,p_date    IN DATE
   ,p_chapter IN NUMBER
   ,p_r0      IN NUMBER
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER DETERMINISTIC
  AS
  BEGIN

    RETURN
      rsi_rsb_account.restac
      (
        p_account
       ,p_cur
       ,p_date
       ,p_chapter
       ,p_r0
       ,p_rest_cur
      );

  END restac;

  FUNCTION restap
  (
    p_account   IN   VARCHAR2
   ,p_dateb     IN   DATE
   ,p_datee     IN   DATE
   ,p_chapter   IN   NUMBER
   ,p_r0        IN   NUMBER
   ,p_cur       IN NUMBER DEFAULT NULL
   ,p_rest_cur  IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER
  AS
  BEGIN

    RETURN
      rsi_rsb_account.restap
      (
        p_account
       ,p_dateb
       ,p_datee
       ,p_chapter
       ,p_r0
       ,p_cur
       ,p_rest_cur
      );

  END restap;

  FUNCTION restapc
  (
    p_account   IN   VARCHAR2
   ,p_cur       IN   NUMBER
   ,p_dateb     IN   DATE
   ,p_datee     IN   DATE
   ,p_chapter   IN   NUMBER
   ,p_r0        IN   NUMBER
   ,p_rest_cur  IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER
  AS
  BEGIN

    RETURN
      rsi_rsb_account.restapc
      (
        p_account
       ,p_cur
       ,p_dateb
       ,p_datee
       ,p_chapter
       ,p_r0
       ,p_rest_cur
      );

  END restapc;

  ---------------------------------------------------------
  -- Функция получения остатка на счёте по любой валюте на любую дату
  ---------------------------------------------------------
  FUNCTION restall
  (
    p_account IN VARCHAR2 -- номер счёта
   ,p_chapter IN NUMBER   -- глава
   ,p_cur     IN NUMBER   -- валюта
   ,p_date    IN DATE     -- дата
   ,p_rest_cur  IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER deterministic
  AS
  BEGIN

    RETURN
      rsi_rsb_account.restall
      (
        p_account
       ,p_chapter
       ,p_cur
       ,p_date
       ,p_rest_cur
      );

  END restall;

  ---------------------------------------------------------
  -- функция нахождения остатков на субсчетах (daccsub_dbt) на любую дату в рублях
  ---------------------------------------------------------
  FUNCTION restsa
  (
    p_analitica  IN NUMBER
   ,p_subaccount IN NUMBER
   ,p_date       IN DATE
   ,p_notuseconv IN NUMBER
  )
  RETURN NUMBER
  AS
  BEGIN

    RETURN
      rsi_rsb_account.restsa
      (
        p_analitica
       ,p_subaccount
       ,p_date
       ,p_notuseconv
      );

  END restsa;

  ---------------------------------------------------------
  -- Функция нахождения дебета на субсчетах (daccsub_dbt) за период
  ---------------------------------------------------------
  FUNCTION debetsa
  (
    p_analitica  IN NUMBER
   ,p_subaccount IN NUMBER
   ,p_date_from  IN DATE
   ,p_date_till  IN DATE
  )
  RETURN NUMBER
  AS
  BEGIN

    RETURN
      rsi_rsb_account.debetsa
      (
        p_analitica
       ,p_subaccount
       ,p_date_from
       ,p_date_till
      );

  END debetsa;

  ---------------------------------------------------------
  -- Функция нахождения кредита на субсчетах (daccsub_dbt) за период
  ---------------------------------------------------------
  FUNCTION creditsa
  (
    p_analitica  IN NUMBER
   ,p_subaccount IN NUMBER
   ,p_date_from  IN DATE
   ,p_date_till  IN DATE
  )
  RETURN NUMBER
  AS
  BEGIN

    RETURN
      rsi_rsb_account.creditsa
      (
        p_analitica
       ,p_subaccount
       ,p_date_from
       ,p_date_till
      );

  END creditsa;
  ---------------------------------------------------------
  -- Функция вычисления следующено свободного номера счета
  ---------------------------------------------------------
 
  FUNCTION GetNewNumAccDeal
   (p_balance dbusyacc_dbt.t_balance%type,
    p_CURRENCY dbusyacc_dbt.t_currency%type,
    p_DealID dbusyacc_dbt.t_dealid%type,
    p_filialAddNumb VARCHAR2 DEFAULT '00')
  RETURN NUMBER 
  as
  v_cache_key varchar2(9) := p_balance||'*'||p_CURRENCY;
  v_last_val pls_integer := 0 ;
  v_res NUMBER := -1 ;
  BEGIN
    if T_CACHE_NEWNUMACCDEAL.exists(v_cache_key) then
       v_last_val:= t_cache_NewNumAccDeal(v_cache_key) ;
    end if ;
    select val.t_val
      into v_res
      from (select v_last_val+level t_val from dual connect by level < 1000000-v_last_val) val
     where not exists (select 1
              from dbusyacc_dbt acc
             where val.t_val = acc.t_value
               and acc.t_dealid <> p_DealID
               and acc.t_balance = p_balance
               and acc.t_currency = p_CURRENCY)
       and not exists (select 1
              from daccount_dbt acc
             where acc.t_sort = p_balance || p_CURRENCY || '99'||p_filialAddNumb||'8' || LPAD(TO_CHAR(val.t_val), 6, '0')
               and acc.t_account like (p_balance || p_CURRENCY || '_99'||p_filialAddNumb||'8' || LPAD(TO_CHAR(val.t_val), 6, '0')))
       and ROWNUM < 2;
       
    T_CACHE_NEWNUMACCDEAL(v_cache_key) := v_res ;
    return v_res ;
  END;
  
    ---------------------------------------------------------
  -- Функция вычисления свободного номера счета в разрезе контрагентов
  ---------------------------------------------------------
 
  FUNCTION GetNewNumAccContr
   (p_balance dbusyacccontr_dbt.t_balance%type,
    p_Currency dbusyacccontr_dbt.t_currency%type,
    p_BA dbusyacccontr_dbt.t_ba%type,
    p_ContrCode dbusyacccontr_dbt.t_contrcode%type,
    p_numbInFill dbusyacccontr_dbt.t_numbinfill%type)
  RETURN NUMBER 
  as
  v_cache_key varchar2(17) := p_balance||p_CURRENCY||p_numbInFill||p_BA||p_ContrCode;
  v_last_val pls_integer := 0 ;
  v_res NUMBER := -1 ;
  BEGIN
    if T_CACHE_NEWNUMACCCONTR.exists(v_cache_key) then
       v_res:= t_cache_NewNumAccContr(v_cache_key) ;
    ELSE
      select NVL(MAX(acc.t_value), 0) INTO v_last_val
              from dbusyacccontr_dbt acc
             where acc.t_balance = p_balance
               and acc.t_currency = p_CURRENCY
               and acc.t_BA = p_BA
               and acc.t_numbinfill = p_numbInFill;
    select val.t_val
      into v_res
      from (select v_last_val+level t_val from dual connect by level < 10000-v_last_val) val
     where not exists (select 1
              from dbusyacccontr_dbt acc
             where val.t_val = acc.t_value
               and acc.t_balance = p_balance
               and acc.t_currency = p_CURRENCY
               and acc.t_BA = p_BA
               and acc.t_numbinfill = p_numbInFill)
       and not exists (select 1
              from daccount_dbt acc
             where acc.t_sort = p_balance || p_CURRENCY || '99' || p_numbInFill || p_BA|| LPAD(TO_CHAR(val.t_val), 4, '0')
               and acc.t_account like (p_balance || p_CURRENCY || '_99' || p_numbInFill ||p_BA|| LPAD(TO_CHAR(val.t_val), 4, '0')))
       and ROWNUM < 2;
       
    T_CACHE_NEWNUMACCCONTR(v_cache_key) := v_res ;
    end if ;
    return v_res ;
  END;
  
  FUNCTION GetNewNumAccContrF
   (p_balance dbusyacccontr_dbt.t_balance%type,
    p_Currency dbusyacccontr_dbt.t_currency%type,
    p_BA dbusyacccontr_dbt.t_ba%type,
    p_ContrCode dbusyacccontr_dbt.t_contrcode%type,
    p_numbInFill dbusyacccontr_dbt.t_numbinfill%type)
  RETURN NUMBER 
  as
  v_cache_key varchar2(17) := p_balance||p_CURRENCY||p_ContrCode||p_BA;
  v_last_val pls_integer := 0 ;
  v_res NUMBER := -1 ;
  BEGIN
    if T_CACHE_NEWNUMACCCONTRF.exists(v_cache_key) then
       v_res:= t_cache_NewNumAccContrF(v_cache_key) ;
    ELSE
      select NVL(MAX(acc.t_value), 0) INTO v_last_val
              from dbusyacccontr_dbt acc
             where acc.t_balance = p_balance
               and acc.t_currency = p_CURRENCY
               and acc.t_ContrCode = p_ContrCode
               and acc.t_numbinfill = p_numbInFill;
     select val.t_val
        into v_res
      from (select v_last_val+level t_val from dual connect by level < 1000-v_last_val) val
     where not exists (select 1
              from dbusyacccontr_dbt acc
             where val.t_val = acc.t_value
               and acc.t_balance = p_balance
               and acc.t_currency = p_CURRENCY
               and acc.t_ContrCode = p_ContrCode
               and acc.t_numbinfill = p_numbInFill)
       and not exists (select 1
              from daccount_dbt acc
             where acc.t_sort = p_balance || p_CURRENCY || '99' || p_numbInFill || '4' || p_ContrCode|| LPAD(TO_CHAR(val.t_val), 3, '0')
               and acc.t_account like (p_balance || p_CURRENCY || '_99' || p_numbInFill || '4' ||p_ContrCode|| LPAD(TO_CHAR(val.t_val), 3, '0')))
       and ROWNUM < 2;
       
    T_CACHE_NEWNUMACCCONTRF(v_cache_key) := v_res ;
    end if ;
    return v_res ;
  END;

END rsb_account;
/
