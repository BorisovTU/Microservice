create or replace package rshb_limit_util is

  TYPE quikRepoDealRec IS RECORD
  (
     T_SELL               NUMBER (5),
     T_TSCOMMMISSION      DQUIK_DEALS_DBT.T_TSCOMMMISSION%TYPE,
     T_BROKERCOMMISSION   DQUIK_DEALS_DBT.T_BROKERCOMMISSION%TYPE,
     T_REPOVALUE          DQUIK_DEALS_DBT.T_REPOVALUE%TYPE,
     T_REPO2VALUE         DQUIK_DEALS_DBT.T_REPO2VALUE%TYPE,
     T_QTY                DQUIK_DEALS_DBT.T_QTY%TYPE,
     T_VALUE              DQUIK_DEALS_DBT.T_VALUE%TYPE,
     T_LIMITKIND          DLIMIT_BSDATE_DBT.T_LIMITKIND%TYPE
  );
  TYPE quikRepoDealArr IS TABLE OF quikRepoDealRec;

  procedure CheckDataSecur_ds(p_micex_code_curmarket   varchar2
                             ,p_micex_code_stockmarket varchar2
                             ,p_secur_checked          integer
                             ,p_cur_checked            integer
                             ,p_is_advanced_check      integer);

  procedure CheckDataSecur_ss(p_micex_code_curmarket   varchar2
                             ,p_micex_code_stockmarket varchar2
                             ,p_secur_checked          integer
                             ,p_cur_checked            integer
                             ,p_record_limit           integer default null);

  procedure CheckDataSecur_qs(p_micex_code_curmarket   varchar2
                             ,p_micex_code_stockmarket varchar2
                             ,p_secur_checked          integer
                             ,p_cur_checked            integer
                             ,p_record_limit           integer default null);

  procedure CheckDataSecur_is(p_micex_code_curmarket   varchar2
                             ,p_micex_code_stockmarket varchar2
                             ,p_secur_checked          integer
                             ,p_cur_checked            integer);

  procedure CheckDataSecur_wp(p_micex_code_curmarket   varchar2
                             ,p_micex_code_stockmarket varchar2
                             ,p_secur_checked          integer
                             ,p_cur_checked            integer);

  procedure CheckDataFutur_df;

  procedure CheckDataFutur_sf;

  procedure CheckDataFutur_qf;

  procedure CheckDataFutur_if;

  -- Загрузка данных QUIK по срочному рынку возвращает кол-во строк с ошибками 0 - ок
  function LoadDataFutur(p_DataLimit clob
                        ,p_trunc_t   number default 1
                        ,o_mess      out varchar2) return number;

  -- Загрузка данных QUIK по фондовому и  валютным рынкам возвращает кол-во строк с ошибками 0 - ок
  function LoadDataSecur(p_DataLimit clob
                        ,p_trunc_t   number default 1
                        ,o_mess      out varchar2) return number;

  -- Возвращает кол-во строк при обработке уже загруженных данных
  function LoadDataGetLineCount(p_FILE_CODE itt_file.file_code%type) return number;

  procedure FillBaseDateForLimit(p_basedate DATE);

  FUNCTION GetDifSecurRepoByQuikData (p_basedate      DATE,
                                      p_tradedate     DATE,
                                      p_limkind       NUMBER,
                                      p_seccode       VARCHAR2,
                                      p_clientcode    VARCHAR2,
                                      p_quikdataexists integer) RETURN NUMBER DETERMINISTIC;

  FUNCTION GetDifMoneyRepoByQuikData (p_basedate      DATE,
                                      p_tradedate     DATE,
                                      p_limkind       NUMBER,
                                      p_currency      VARCHAR2,
                                      p_clientcode    VARCHAR2,
                                      p_quikdataexists integer) RETURN NUMBER DETERMINISTIC;

  FUNCTION GetDifMoneyNoUnloadedNptx (p_tradedate      DATE,
                                      p_currency      VARCHAR2,
                                      p_clientcode    VARCHAR2,
                                      p_marketid      NUMBER,
                                      p_market_kind   VARCHAR2,
                                      p_quikdataexists integer)
     RETURN NUMBER
     DETERMINISTIC;

  FUNCTION GetDifMoneyComissSofr (p_basedate      DATE,
                                  p_tradedate     DATE,
                                  p_limkind       NUMBER,
                                  p_currency      VARCHAR2,
                                  p_clientcode    VARCHAR2,
                                  p_marketid      NUMBER,
                                  p_market_kind   VARCHAR2,
                                  p_isBankComis   NUMBER,
                                  p_quikdataexists integer,
                                  p_isBrokerComis NUMBER)
     RETURN NUMBER
     DETERMINISTIC;

  FUNCTION GetDifBrokerComisByQuikData (p_basedate DATE,
                                        p_tradedate date,
                                        p_limkind NUMBER,
                                        p_currency VARCHAR2,
                                        p_clientcode VARCHAR2,
                                        p_quikdataexists integer)
     return NUMBER
     deterministic;

  FUNCTION GetDifSecurAvrWrtBySofrData (p_tradedate     DATE,
                                        p_seccode       VARCHAR2,
                                        p_clientcode    VARCHAR2,
                                        p_marketid      NUMBER,
                                        p_market_kind   VARCHAR2,
                                        p_quikdataexists integer)
     RETURN NUMBER
     DETERMINISTIC;

  FUNCTION GetDifMoneyBrokerFixComissSofr (p_tradedate     DATE,
                                           p_basedate      DATE,
                                           p_currency      VARCHAR2,
                                           p_clientcode    VARCHAR2,
                                           p_marketid      NUMBER,
                                           p_market_kind   VARCHAR2,
                                           p_quikdataexists integer)
     RETURN NUMBER
     DETERMINISTIC;


  FUNCTION GetDifMoneyDueSofr (p_basedate      DATE,
                               p_tradedate     DATE,
                               p_accountid     NUMBER,
                               p_currency      VARCHAR2,
                               p_client_code   VARCHAR2,
                               p_market_kind   VARCHAR2,
                               p_market        NUMBER,
                               p_currid        NUMBER,
                               p_client_id     NUMBER)
     RETURN NUMBER
     DETERMINISTIC;

  FUNCTION GetDifDueRestSofr  (p_tradedate     DATE,
                               p_client_code   VARCHAR2,
                               p_market_kind   VARCHAR2,
                               p_market        NUMBER,
                               p_currid        NUMBER,
                               p_client_id     NUMBER)
     RETURN NUMBER
     DETERMINISTIC;

  FUNCTION GetDifMoneyOrSecurRepoSofr (p_basedate      DATE,
                                       p_tradedate     DATE,
                                       p_ismoney       NUMBER,
                                       p_currency      VARCHAR2,
                                       p_fiid          NUMBER,
                                       p_clientcode    VARCHAR2,
                                       p_limkind       NUMBER,
                                       p_marketid      NUMBER,
                                       p_market_kind   VARCHAR2,
                                       p_quikdataexists integer)
     RETURN NUMBER
     DETERMINISTIC;

  FUNCTION GetDifMoneyPlanComissSofr (p_basedate       DATE,
                                      p_tradedate      DATE,
                                      p_currency       VARCHAR2,
                                      p_clientcode     VARCHAR2,
                                      p_marketid       NUMBER,
                                      p_market_kind    VARCHAR2)
     RETURN NUMBER
     DETERMINISTIC;

  /**
  * Заполнить таблицу подсчета разницы торгового и расчетного дней в разрезе валют
  * @since        6.20.031
  */
  procedure FillBaseDateCurrForLimit;

 -- Возвращаем  время когда операция зачисления могла попасть в расчет лимитов 
  function GetDT306Limit_dy_nptxop(p_nptxop_id dnptxop_dbt.t_id%type) return date;
  
end rshb_limit_util;
/
