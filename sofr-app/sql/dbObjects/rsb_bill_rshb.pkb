CREATE OR REPLACE PACKAGE BODY rsb_bill_rshb
IS

  FUNCTION VA_GetBannerAccount (p_bcid in dvsbanner_dbt.t_BCID%TYPE, p_date in DATE) return VARCHAR2
  IS
    v_account varchar2(20 char) := '';
  BEGIN

    SELECT t_account INTO v_account
    FROM dmcaccdoc_Dbt mcacc
    WHERE     t_dockind = VSBANNER_DOCKIND_MCACCDOC 
          AND t_docid = p_bcid
          AND t_catnum = CASE WHEN (SELECT min(t_changedate) FROM dvsbnrbck_Dbt
                                    WHERE     instr(t_newbcstate, 'Ч') > 0
                                          AND t_bcid = t_docid) <= p_date
                              THEN 1492 /*КУ - Просроч_вексель*/
                              ELSE 462 /*КУ - Учтенные векселя*/
                         END
          AND t_firole = FIROLE_BANNER
          AND t_iscommon = chr(0)
          AND (    p_date BETWEEN t_activatedate AND t_disablingdate-1
                OR (t_disablingdate = to_date('01.01.0001', 'dd.mm.yyyy') AND t_activatedate <= p_date AND t_isusable = chr(88))
              );
    RETURN v_account;
    
  EXCEPTION
    WHEN others THEN
      RETURN '';
  END VA_GetBannerAccount;
  
  FUNCTION GetBnrPlanRepayDate (p_LegId IN NUMBER)
     RETURN DATE
  IS
     v_PlanRepayDate DATE;
     v_stat number;
  BEGIN
    v_stat := rsb_bill.GetBnrPlanRepayDate(p_legid, v_PlanRepayDate);
    
    RETURN v_PlanRepayDate;
  END GetBnrPlanRepayDate;
  
  FUNCTION GetBnrRate(p_bcid in dvsbanner_dbt.t_BCID%TYPE) return number
  IS
    v_facevalue number := 0;
    v_Cost number := 0;
    v_rate number := 0;
    v_formula number := 0;
    v_duration integer := 0;
  BEGIN
    SELECT lnk.t_bccost,
           leg.t_principal,
           leg.t_price/power(10, leg.t_point),
           leg.t_formula,
           leg.t_duration
    INTO v_cost,
         v_facevalue,
         v_rate,
         v_formula,
         v_duration
    FROM dvsordlnk_dbt lnk, dvsbanner_dbt bnr, ddl_leg_dbt leg
    WHERE     lnk.t_bcid = bnr.t_bcid
          AND lnk.t_dockind = CASE WHEN EXISTS(SELECT 1 FROM ddp_dep_dbt WHERE t_partyid = bnr.t_issuer)
                                   THEN 109 --выдача в СВ
                                   ELSE 141 --покупка в УВ
                              END
          AND leg.t_Dealid = bnr.t_bcid
          AND leg.t_legid = 0
          AND leg.t_legkind = 1
          AND bnr.t_bcid = p_bcid;
    
    IF (v_formula = VS_FORMULA_DISCOUNT) THEN
      v_rate := 100*(v_facevalue - v_cost)/v_cost * 365/v_duration;
    END IF;

    RETURN v_rate;
  EXCEPTION
    WHEN others THEN
      RETURN 0;
  END GetBnrRate;
  
  FUNCTION GetPresentDate(p_bcid in dvsbanner_dbt.t_BCID%TYPE) RETURN DATE
  IS
    v_PresentDate DATE;
  BEGIN
    SELECT NVL(MIN(t_changedate), TO_DATE('01.01.0001', 'dd.mm.yyyy'))
    INTO v_PresentDate
    FROM dvsbnrbck_dbt WHERE t_bcid = p_bcid AND INSTR(t_newbcstate, 'П') > 0;
    
    RETURN v_PresentDate;
    
  EXCEPTION
    WHEN others THEN
      RETURN NULL_DATE;
  END GetPresentDate;
  
   -- Получить процент на дату
   FUNCTION GetPrecentOnDate (p_LegId    IN NUMBER,
                              p_CalcDate IN DATE,
                              p_bcid in dvsbanner_dbt.t_BCID%TYPE)
      RETURN NUMBER
   IS
      v_stat        NUMBER;
      v_NumDay      NUMBER;
      v_RepayDate   DATE;
      v_SartDate    DATE;
      v_Basis       NUMBER;
      v_Date        DATE;
      v_N           NUMBER (32, 12);
      v_C           NUMBER (32, 12);
      v_CalcPercent NUMBER;
      v_PresentDate DATE;
   BEGIN
      v_Date := p_CalcDate;
      v_CalcPercent := 0;
      v_PresentDate := GetPresentDate(p_bcid);

      SELECT t_Basis,
             T_INTERESTSTART,
             T_PRINCIPAL,
             T_PRICE / 1000000
        INTO v_Basis,
             v_SartDate,
             v_N,
             v_C
        FROM DDL_LEG_DBT
       WHERE t_ID = p_LegId;

      IF rsb_bill.GetBnrPlanRepayDate (p_LegId, v_RepayDate) != 0 THEN
         RETURN 1;
      END IF;

      IF v_PresentDate <> NULL_DATE THEN
         v_Date := v_PresentDate;
      ELSIF v_Date < v_SartDate THEN
         RETURN 0;
      ELSIF v_Date > v_RepayDate THEN
         v_Date := v_RepayDate;
      END IF;

      v_stat := rsb_bill.GetDaysInYearByBasis (v_Basis,
                                               v_SartDate,
                                               TRUE,
                                               v_NumDay);
      IF v_stat != 0 THEN
        RETURN 1;
      ELSE
        v_CalcPercent := ROUND (v_N * v_C * (v_Date - v_SartDate) / v_NumDay, 2);
      END IF;

      RETURN v_CalcPercent;

      EXCEPTION
        WHEN OTHERS THEN
            RETURN 0;
   END GetPrecentOnDate;
   
   FUNCTION GetDiscountOnDateVA (p_LegId    IN NUMBER,
                                 p_bcid     IN NUMBER,
                                 p_CalcDate IN DATE)
   RETURN NUMBER
   IS
     V_SumDiscount NUMBER;
     v_stat INTEGER;
   BEGIN
     v_stat := rsb_bill.GetDiscountOnDateVA(p_LegId, p_bcid, p_CalcDate, V_SumDiscount);
     
     RETURN V_SumDiscount;
   END GetDiscountOnDateVA;
   
/**
 * Получить кол-во дней в году по базису СВ
 * @param  p_Basis Базис векселя
 * @param  p_dateC Дата начала действия векселя
 */
  FUNCTION GetDaysInYearByBasis (
     p_Basis     IN NUMBER
    ,p_dateC     IN DATE
    ,p_as_period in number default 0
  ) RETURN INTEGER
  IS
    v_days integer := 0;
    v_stat integer := 0;
    l_as_period boolean := p_as_period != 0;
  BEGIN
    v_stat := rsb_bill.GetDaysInYearByBasis(p_Basis     => p_Basis,
                                            p_dateC     => p_dateC,
                                            p_asPeriod  => l_as_period,
                                            p_DayInYear => v_days);
    return v_days;
  END GetDaysInYearByBasis;      
                                 
   FUNCTION VS_GetBannerAccount (p_bcid in dvsbanner_dbt.t_BCID%TYPE, p_date in DATE) return VARCHAR2
   IS
    v_account varchar2(20 char) := '';
  BEGIN

    SELECT t_account INTO v_account
    FROM dmcaccdoc_Dbt mcacc
    WHERE     t_dockind = VSBANNER_DOCKIND_MCACCDOC 
          AND t_docid = p_bcid
                    and t_catnum = case when (select nvl(min(t_changedate), to_date('31.12.9999', 'dd.mm.yyyy'))
                                              from DVSBNRBCK_DBT
                                              where instr(t_newbcstate, 'И') > 0 and t_bcid = t_docid) <= p_date
                                        then 451 /*КУ - СВексель к исполнению*/
                                        else 450 /*КУ - Наш вексель*/
                                   end
          AND t_iscommon = chr(0)
          AND (    p_date BETWEEN t_activatedate AND t_disablingdate-1
                OR (t_disablingdate = to_date('01.01.0001', 'dd.mm.yyyy') AND t_activatedate <= p_date AND t_isusable = chr(88))
              );

    RETURN v_account;
    
  EXCEPTION
    WHEN others THEN
      RETURN '';
   END VS_GetBannerAccount;
   
   FUNCTION VS_GetPDDAccount (p_bcid in dvsbanner_dbt.t_BCID%TYPE,
                              p_date in DATE,
                              p_formula in ddl_leg_dbt.t_formula%TYPE) return VARCHAR2
   IS
    v_account varchar2(20 char) := '';
  BEGIN

    SELECT t_account INTO v_account
    FROM dmcaccdoc_Dbt mcacc
    WHERE     t_dockind = VSBANNER_DOCKIND_MCACCDOC 
          AND t_docid = p_bcid
                    and t_catnum = case when (select nvl(min(t_changedate), to_date('31.12.9999', 'dd.mm.yyyy'))
                                              from DVSBNRBCK_DBT
                                              where instr(t_newbcstate, 'И') > 0 and t_bcid = t_docid) <= p_date
                                        then 451 /*КУ - СВексель к исполнению*/
                                        when p_formula = 1 then 452 /*КУ - Обяз%,СВексель*/
                                        else 1384 /*КУ - Дисконт, Свексель*/
                                   end
          AND t_iscommon = chr(0)
          AND (    p_date BETWEEN t_activatedate AND t_disablingdate-1
                OR (t_disablingdate = to_date('01.01.0001', 'dd.mm.yyyy') AND t_activatedate <= p_date AND t_isusable = chr(88))
              );

    RETURN v_account;
    
  EXCEPTION
    WHEN others THEN
      RETURN '';
   END VS_GetPDDAccount;
   
   
   FUNCTION VS_GetFinresAccount (p_bcid in dvsbanner_dbt.t_BCID%TYPE, p_date in DATE) return VARCHAR2
   IS
    v_account varchar2(20 char) := '';
  BEGIN

    SELECT t_account INTO v_account
    FROM dmcaccdoc_Dbt mcacc
    WHERE     t_dockind = VSBANNER_DOCKIND_MCACCDOC 
          AND t_docid = p_bcid
                    and t_catnum = 34 /*КУ - %Расход, СВ*/
          AND t_iscommon = chr(0)
          AND (    p_date BETWEEN t_activatedate AND t_disablingdate-1
                OR (t_disablingdate = to_date('01.01.0001', 'dd.mm.yyyy') AND t_activatedate <= p_date AND t_isusable = chr(88))
              );

    RETURN v_account;
    
  EXCEPTION
    WHEN others THEN
      RETURN '';
   END VS_GetFinresAccount;
   
  function vs_min_repay_date (
    p_bcid  dvsbanner_dbt.t_bcid%type
  ) return date is
    l_rep_date        date;
    l_bc_term_formula dvsbanner_dbt.t_bctermformula%type;
    l_maturity        ddl_leg_dbt.t_maturity%type;
    l_expiry          ddl_leg_dbt.t_expiry%type;
    l_present_date    dvsbanner_dbt.t_bcpresentationdate%type;
    l_diff            ddl_leg_dbt.t_diff%type;
    l_start_date      ddl_leg_dbt.t_start%type;
    l_basis           ddl_leg_dbt.t_basis%type;
    l_repayment_date  dvsbanner_dbt.t_repaymentdate%type;
    l_bcstatus        dvsbanner_dbt.t_bcstatus%type;
  begin
    select b.t_bctermformula
          ,b.t_bcpresentationdate
          ,b.t_repaymentdate
          ,b.t_bcstatus
          ,l.t_maturity
          ,l.t_expiry
          ,l.t_diff
          ,l.t_start
          ,l.t_basis
      into l_bc_term_formula
          ,l_present_date
          ,l_repayment_date
          ,l_bcstatus
          ,l_maturity
          ,l_expiry
          ,l_diff
          ,l_start_date
          ,l_basis
      from dvsbanner_dbt b
      join ddl_leg_dbt l on l.t_dealid = b.t_bcid
                        and l.t_legkind = 1
                        and l.t_legid = 0
     where b.t_bcid = p_bcid;

     if l_bcstatus = 30 and l_repayment_date != NULL_DATE
     then 
       l_rep_date := l_repayment_date;
    elsif    l_bc_term_formula = VS_TERMF_FIXEDDAY
          or l_bc_term_formula = VS_TERMF_INATIME --срочные
          or (l_bc_term_formula = VS_TERMF_ATSIGHT and l_maturity != NULL_DATE) --По предъявлении, но не ранее
    then
      l_rep_date := l_maturity;
    elsif l_bc_term_formula = VS_TERMF_ATSIGHT and l_expiry != NULL_DATE
    then
      l_rep_date := l_start_date + 1;
    elsif l_bc_term_formula = VS_TERMF_DURING and l_present_date != NULL_DATE --от предъявления, уже предъявленные
    then
      l_rep_date := l_present_date + l_diff;
    else
      l_rep_date := l_start_date + GetDaysInYearByBasis(p_Basis     => l_basis,
                                                        p_dateC     => l_start_date,
                                                        p_as_period => 1);
    end if;
  
    return l_rep_date;
  exception
    when others then
      return null;
  end vs_min_repay_date;
END rsb_bill_rshb;
/
