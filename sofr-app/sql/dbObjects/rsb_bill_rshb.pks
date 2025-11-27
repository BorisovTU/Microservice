CREATE OR REPLACE PACKAGE rsb_bill_rshb
IS

  VSBANNER_DOCKIND_MCACCDOC CONSTANT NUMBER := 164; --Вид документа в mcaccdoc
  
  FIROLE_BANNER CONSTANT NUMBER := 5; --роль фин. инструмента - вексель
  
  VS_FORMULA_DISCOUNT CONSTANT NUMBER := 0; --форма дохода - дисконт
  
  NULL_DATE CONSTANT DATE := to_date('01.01.0001', 'dd.mm.yyyy');
  
  /** Формулировка вексельного срока - На определенный день */
    VS_TERMF_FIXEDDAY CONSTANT NUMBER := 10;
  /** Формулировка вексельного срока - Во столько-то времени от составления */
    VS_TERMF_INATIME  CONSTANT NUMBER := 15;
  /** Формулировка вексельного срока - По предъявлении */
    VS_TERMF_ATSIGHT  CONSTANT NUMBER := 20;
  /** Формулировка вексельного срока - Во столько-то времени предъявления */
    VS_TERMF_DURING   CONSTANT NUMBER := 30;
      
  --Найти счёт учета номинала учтенного векселя на дату
  FUNCTION VA_GetBannerAccount (p_bcid in dvsbanner_dbt.t_BCID%TYPE, p_date in DATE) RETURN VARCHAR2;
  
  --Плановая дата погашения векселя
  FUNCTION GetBnrPlanRepayDate (p_LegId IN NUMBER) RETURN DATE;
  
  --Ставка по векселю. так же, считает и ставку дисконта
  FUNCTION GetBnrRate(p_bcid in dvsbanner_dbt.t_BCID%TYPE) RETURN number;
  
   -- Получить процент на дату
   FUNCTION GetPrecentOnDate (p_LegId IN NUMBER, p_CalcDate IN DATE, p_bcid in dvsbanner_dbt.t_BCID%TYPE) RETURN NUMBER;
   
   /**
    * Получить кол-во дней в году по базису СВ
    * @param  p_Basis Базис векселя
    * @param  p_dateC Дата начала действия векселя
    */
   FUNCTION GetDaysInYearByBasis (
     p_Basis     IN NUMBER
    ,p_dateC     IN DATE
    ,p_as_period in number default 0
   ) RETURN INTEGER;
   
   FUNCTION GetDiscountOnDateVA (p_LegId    IN NUMBER,
                                 p_bcid     IN NUMBER,
                                 p_CalcDate IN DATE) RETURN NUMBER;
 
  --Найти счёт учета номинала выпущенного векселя на дату
  FUNCTION VS_GetBannerAccount (p_bcid in dvsbanner_dbt.t_BCID%TYPE, p_date in DATE) RETURN VARCHAR2;
  
  --Счет учета процентов или дисконта выпущенного векселя на дату
  FUNCTION VS_GetPDDAccount (p_bcid in dvsbanner_dbt.t_BCID%TYPE,
                             p_date in DATE,
                             p_formula in ddl_leg_dbt.t_formula%TYPE) return VARCHAR2;
  
  --Счет отражения доходов/расходов по выпущенному векселю на дату
  FUNCTION VS_GetFinresAccount (p_bcid in dvsbanner_dbt.t_BCID%TYPE, p_date in DATE) return VARCHAR2;
   
  function vs_min_repay_date (
    p_bcid  dvsbanner_dbt.t_bcid%type
  ) return date;
  
END rsb_bill_rshb;
/