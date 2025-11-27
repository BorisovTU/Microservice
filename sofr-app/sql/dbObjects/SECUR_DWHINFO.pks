CREATE OR REPLACE PACKAGE SECUR_DWHINFO
AS
   --Номер категории ОКВЭД
   CATEG_OKVED      CONSTANT INTEGER := 17;

   --Номер вида субъекта 'является Банком'
   PARTYKIND_BANK   CONSTANT INTEGER := 2;

   /*
    * Получить ISO-код валюты
    * @param     fiid - РС-Банковский ID фин.инструмента
   */
   FUNCTION GetCurrencyISO (fiid IN dfininstr_dbt.t_fiid%TYPE)
      RETURN VARCHAR2 deterministic;

   /*
    * Получить остаток по лицевому счету
    * @param     accountnum - номер ЛС
    * @param     currence      - код валюты ЛС
    * @param     restdate      - дата, на к-рую надо получить остаток
   */
   FUNCTION GetAccountRest (
      accountnum   IN daccount_dbt.t_account%TYPE,
      currency     IN daccount_dbt.t_code_currency%TYPE,
      restdate     IN drestdate_dbt.t_restdate%TYPE)
      RETURN NUMBER;

   /*
    * Определить признак ФО / НФО
    * @param     partyid
   */
   FUNCTION DefineIsNFO (partyid IN dparty_dbt.t_partyid%TYPE)
      RETURN VARCHAR2;

   /*
    * Подготовка данных - отчет РЕПО
    * @param     DateRep1 - дата 'с'
    * @param     DateRep2 - дата 'по'
   */
   FUNCTION GetBalanceCost (accin   IN V_SCWRTHISTEX.T_DEALCODE%TYPE,
                            Ndate   IN DATE, coursedate in date)
      RETURN V_SCWRTHISTEX.t_balancecost%TYPE;

   FUNCTION GetOveramount (accin   IN V_SCWRTHISTEX.T_DEALCODE%TYPE,
                           Ndate   IN DATE, REPO_DIRECTION in trshb_repo_pkl_2chd.REPO_DIRECTION%type )
      RETURN V_SCWRTHISTEX.T_OVERAMOUNT%TYPE;
      
FUNCTION GetOveramountRep (accin in V_SCWRTHISTEX.T_DEALCODE%type,
                       Ndate in Date)
RETURN V_SCWRTHISTEX.T_OVERAMOUNT%TYPE;

   FUNCTION GetAmount (Paper IN dfininstr_dbt.T_FIID%TYPE, Ndate IN DATE)
      RETURN TRSHB_REPO_PKL_2CHD.TSS_AMOUNT%TYPE;

   FUNCTION GetSPV ( partyid IN dparty_dbt.t_partyid%TYPE)
      RETURN TRSHB_REPO_PKL_2CHD.IS_SPV%type;

   FUNCTION GetHypoCover (fiid IN dparty_dbt.t_partyid%TYPE)
      RETURN TRSHB_REPO_PKL_2CHD.IS_HYPO_COVER%type;


   FUNCTION GetRate (Tid     IN dobjatcor_dbt.T_OBJECT%TYPE,
                     ObId    IN dobjattr_dbt.t_objecttype%TYPE,
                     GrId    IN dobjattr_dbt.t_groupid%TYPE,
                     ParId   IN dobjattr_dbt.t_parentid%TYPE,
                     Ndate   IN DATE)
      RETURN dobjattr_dbt.t_name%TYPE RESULT_CACHE ;

   PROCEDURE PrepareREPO (DateRep1 IN DATE);
END SECUR_DWHINFO;
/
