/**
 * Пакет для работы с рублевыми и валютными счетами
 */
CREATE OR REPLACE PACKAGE rsb_account
IS

/**
 * Функция определения кредита рублевых л/с за период
 * @param p_account номер счета
 * @param p_chapter глава счета
 * @param p_date_t  дата начала периода
 * @param p_date_b  дата окончания периода
 * @param p_cur       валюта счета
 * @param p_rest_cur  валюта суммы
 * @return NUMBER кредит рублевого л/с за период
 */
  FUNCTION kredita
  (
    p_account IN VARCHAR2
   ,p_chapter IN NUMBER
   ,p_date_t  IN DATE
   ,p_date_b  IN DATE
   ,p_cur      IN NUMBER DEFAULT NULL
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER;

/**
 * Функция определения дебета рублевых л/с за период
 * @param p_account номер счета
 * @param p_chapter глава счета
 * @param p_date_t  дата начала периода
 * @param p_date_b  дата окончания периода
 * @param p_cur       валюта счета
 * @param p_rest_cur  валюта суммы
 * @return NUMBER дебет рублевого л/с за период
 */
  FUNCTION debeta
  (
    p_account   IN VARCHAR2
   ,p_chapter   IN NUMBER
   ,p_date_t    IN DATE
   ,p_date_b    IN DATE
   ,p_cur      IN NUMBER DEFAULT NULL
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER;

/**
 * Функция определения кредита валютных л/с за период
 * @param p_account номер счета
 * @param p_chapter глава счета
 * @param p_cur     валюта счета
 * @param p_date_t  дата начала периода
 * @param p_date_b  дата окончания периода
 * @param p_rest_cur  валюта суммы
 * @return NUMBER кредит валютного л/с за период
 */
  FUNCTION kreditac
  (
    p_account IN VARCHAR2
   ,p_chapter IN NUMBER
   ,p_cur     IN NUMBER
   ,p_date_t  IN DATE
   ,p_date_b  IN DATE
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER deterministic;

/**
 * Функция определения дебета валютных л/с за период
 * @param p_account номер счета
 * @param p_chapter глава счета
 * @param p_cur     валюта счета
 * @param p_date_t  дата начала периода
 * @param p_date_b  дата окончания периода
 * @param p_rest_cur  валюта суммы
 * @return NUMBER дебет валютного л/с за период
 */
  FUNCTION debetac
  (
    p_account IN VARCHAR2
   ,p_chapter IN NUMBER
   ,p_cur     IN NUMBER
   ,p_date_t  IN DATE
   ,p_date_b  IN DATE
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER deterministic;

/**
 * Функция нахождения остатков на рублевых л/с за любую дату
 * @param p_account номер счета
 * @param p_date    дата
 * @param p_chapter глава счета
 * @param p_r0      не используется
 * @param p_cur       валюта счета
 * @param p_rest_cur  валюта суммы
 * @return NUMBER остаток на счете
 */
  FUNCTION resta
  (
    p_account IN VARCHAR2
   ,p_date    IN DATE
   ,p_chapter IN NUMBER
   ,p_r0      IN NUMBER
   ,p_cur      IN NUMBER DEFAULT NULL
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER;

/**
 * Функция нахождения остатков на валютных л/с за любую дату
 * @param p_account номер счета
 * @param p_cur     валюта счета
 * @param p_date    дата
 * @param p_chapter глава счета
 * @param p_r0      не используется
 * @param p_rest_cur  валюта суммы
 * @return NUMBER остаток на счете
 */
  FUNCTION restac
  (
    p_account IN VARCHAR2
   ,p_cur     IN NUMBER
   ,p_date    IN DATE
   ,p_chapter IN NUMBER
   ,p_r0      IN NUMBER
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER DETERMINISTIC ;

/**
 * Функция нахождения остатков на рублевых л/с (daccount_dbt) за период
 * аналог макрофунции RestA
 * @param p_account номер счета
 * @param p_dateb   дата начала периода
 * @param p_datee   дата окончания периода
 * @param p_chapter глава счета
 * @param p_r0      не используется
 * @param p_cur       валюта счета
 * @param p_rest_cur  валюта суммы
 * @return NUMBER остаток на счете
 */
  FUNCTION restap
  (
    p_account   IN   VARCHAR2
   ,p_dateb     IN   DATE
   ,p_datee     IN   DATE
   ,p_chapter   IN   NUMBER
   ,p_r0        IN   NUMBER
   ,p_cur      IN NUMBER DEFAULT NULL
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER;

/**
 * Функция нахождения остатков на валютных л/с (daccount$_dbt) за период
 * аналог макрофунции RestAC
 * @param p_account номер счета
 * @param p_cur     валюта счета
 * @param p_dateb   дата начала периода
 * @param p_datee   дата окончания периода
 * @param p_chapter глава счета
 * @param p_r0      не используется
 * @param p_rest_cur  валюта суммы
 * @return NUMBER остаток на счете
 */
  FUNCTION restapc
  (
    p_account   IN   VARCHAR2
   ,p_cur       IN   NUMBER
   ,p_dateb     IN   DATE
   ,p_datee     IN   DATE
   ,p_chapter   IN   NUMBER
   ,p_r0        IN   NUMBER
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER;

/**
 * Функция получения остатка на счёте по любой валюте на любую дату
 * @param p_account номер счета
 * @param p_chapter глава счета
 * @param p_cur     валюта счета
 * @param p_date    дата
 * @param p_rest_cur  валюта суммы
 * @return NUMBER остаток на счете
 */
  FUNCTION restall
  (
    p_account IN VARCHAR2 -- номер счёта
   ,p_chapter IN NUMBER   -- глава
   ,p_cur     IN NUMBER   -- валюта
   ,p_date    IN DATE     -- дата
   ,p_rest_cur IN NUMBER DEFAULT NULL
  )
  RETURN NUMBER deterministic ;

/**
 * Функция нахождения остатков на субсчетах (daccsub_dbt) на любую дату в рублях
 * @param p_analitica  ид. аналитики
 * @param p_subaccount ид. субсчета
 * @param p_date       дата
 * @param p_notuseconv признак "не использовать конвертацию", по умолчанию 0 - валютный остаток конвертируется в нац.валюту
 * @return NUMBER остаток на счете
 */
  FUNCTION restsa
  ( p_analitica  IN NUMBER
   ,p_subaccount IN NUMBER
   ,p_date       IN DATE
   ,p_notuseconv IN NUMBER DEFAULT 0
  )
  RETURN NUMBER;

/**
 * Функция нахождения остатков на субсчетах (daccsub_dbt) на любую дату в рублях
 * @param p_analitica  ид. аналитики
 * @param p_subaccount ид. субсчета
 * @param p_date_from  дата начала периода
 * @param p_date_till  дата окончания периода
 * @return NUMBER остаток на счете
 */
  FUNCTION debetsa
  (
    p_analitica  IN NUMBER,
    p_subaccount IN NUMBER,
    p_date_from  IN DATE,
    p_date_till  IN DATE
  )
  RETURN NUMBER;

/**
 * Функция нахождения кредита на субсчетах (daccsub_dbt) за период
 * @param p_analitica  ид. аналитики
 * @param p_subaccount ид. субсчета
 * @param p_date_from  дата начала периода
 * @param p_date_till  дата окончания периода
 * @return NUMBER остаток на счете
 */
  FUNCTION creditsa
  (
    p_analitica  IN NUMBER,
    p_subaccount IN NUMBER,
    p_date_from  IN DATE,
    p_date_till  IN DATE
  )
  RETURN NUMBER;

/**
 * Функция вычисления следующено свободного номера счета 
 * @param p_balance  
 * @param p_CURRENCY     
 * @param p_DealID
 * @return NUMBER свободный номер
 */
  FUNCTION GetNewNumAccDeal
   (p_balance dbusyacc_dbt.t_balance%type,
    p_CURRENCY dbusyacc_dbt.t_currency%type,
    p_DealID dbusyacc_dbt.t_dealid%type,
    p_filialAddNumb VARCHAR2 DEFAULT '00')
  RETURN NUMBER ;

/**
 * Функция вычисления следующено свободного номера счета в разрезе контагента
 * @param p_balance  
 * @param p_CURRENCY     
 * @param p_BA
 * @param p_ContrCode
 * @return NUMBER свободный номер
 */
  FUNCTION GetNewNumAccContr
   (p_balance dbusyacccontr_dbt.t_balance%type,
    p_Currency dbusyacccontr_dbt.t_currency%type,
    p_BA dbusyacccontr_dbt.t_ba%type,
    p_ContrCode dbusyacccontr_dbt.t_contrcode%type,
    p_numbInFill dbusyacccontr_dbt.t_numbinfill%type)
   RETURN NUMBER ;

/**
 * Функция вычисления следующено свободного номера счета в разрезе контагента
 * @param p_balance  
 * @param p_CURRENCY     
 * @param p_BA
 * @param p_ContrCode
 * @return NUMBER свободный номер
 */
  FUNCTION GetNewNumAccContrF
   (p_balance dbusyacccontr_dbt.t_balance%type,
    p_Currency dbusyacccontr_dbt.t_currency%type,
    p_BA dbusyacccontr_dbt.t_ba%type,
    p_ContrCode dbusyacccontr_dbt.t_contrcode%type,
    p_numbInFill dbusyacccontr_dbt.t_numbinfill%type)
   RETURN NUMBER ;

END rsb_account;
/
