CREATE OR REPLACE package QB_DWH_EXPORT_SECUR is

  -- Author  : MALASHKEVICH-DP
  -- Created : 16.07.2019 9:18:40
  -- Purpose : Выгрузка ценных бумаг в DWH

  /*
  AS 2020-05-23 (по требованию ЦК ЦХД пришлось удалить префиксы '0000#SOFR#' в DET_ROLEACCOUNT_DEAL, ASS_ACCOUNTCECURITY)
  */

  cSecur     number := 1; -- 0 - не выгружать 1 выгружать
  cDateSecur date := to_date('30052019', 'ddmmyyyy');
  -- Событие - Выгрузка Ценных бумаг
  cEvent_EXPORT_Secur number := 10;
  cEvent_EXPORT_Deals number := 11;
  cEvent_EXPORT_Commissions pls_integer := 12;

  cFIID      number := 11;
  cDeal      number := 12;
  cComm      number := 13;

  type acc_deal_t is record
  ( acc_code    varchar2(250)
  , deal_code varchar2(100)
  , cat_code    varchar2(100)
  , cat_name  varchar2 (250)
  , cat_date  date
  );

  type acc_deal_tt is table of acc_deal_t;
-->
  TYPE scwrthistex_aat IS TABLE OF DWH_scwrthistex_TMP%ROWTYPE
      INDEX BY BINARY_INTEGER;
--<
/*  procedure InitExportData(in_EventID       in number,
                           out_dwhRecStatus out varchar2,
                           out_dwhDT        out varchar2,
                           out_dwhSysMoment out varchar2,
                           out_dwhEXT_FILE  out varchar2);*/
  function GetAccountsDeal (p_bofficekind in ddl_tick_dbt.t_bofficekind%type,
                            p_dealid in ddl_tick_dbt.t_dealid%type) return acc_deal_tt pipelined;

  /** <font color=teal><b>Процедура определения счета по лоту</b></font>
  *   @param p_sumid Идентификатор лота
  *   @param p_date Дата
  */
  function GetAccountByLot (p_sumid IN NUMBER, p_date IN DATE) return VARCHAR2;
  /** <font color=teal><b>Процедура определения портфеля по лоту</b></font>
  *   @param fiid Идентификатор ценной бумаги
  *   @param portf Идентификатор портфеля в СОФР
  *   @param acc Номер счета по лоту
  *   @param cdate Дата
  *   @param sumid Идентификатор лота
  */
  FUNCTION GetPortfolioMSFO(fiid IN NUMBER, portf IN NUMBER, acc IN VARCHAR2, cdate IN DATE, sumid in number) RETURN VARCHAR2;

  procedure  add2Fct_Securitydeal_basket(legid in ddl_leg_dbt.t_id%type,
                                         dealid in ddl_tick_dbt.t_dealid%type,
                                         deal_code in varchar2,
                                         totalcost in ddl_tick_ens_dbt.t_totalcost%type,
                                         principal in ddl_tick_ens_dbt.t_principal%type,
                                         nkd       in ddl_tick_ens_dbt.t_nkd%type,
                                         code_fi   in varchar2,
                                         id_cur    in dfininstr_dbt.t_fiid%type,
                                         dwhs in varchar2,
                                         dwhm in varchar2,
                                         dwhf in varchar2 );


  procedure RunExport(in_Date date, procid number, export_mode number default 0);

  procedure call_scwrthistex(p_date IN DATE);
  --9996
  procedure export_SecurKIND(procid in number);
  procedure RunExport_9996(in_Date date, procid number, export_mode number default 0);
  
end QB_DWH_EXPORT_SECUR;
/
