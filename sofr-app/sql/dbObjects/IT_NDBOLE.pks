CREATE OR REPLACE PACKAGE it_ndbole AS
/******************************************************************************
   NAME:       it_ndbole
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        05.09.2024      Geraskina-TV       1. Created this package.
******************************************************************************/

  C_C_SYSTEM_NAME constant varchar2(128) := 'NDBOLE';  /*ДБО ЮЛ*/

  ERROR_UNEXPECTED_GET_DATA CONSTANT NUMBER(5) := 5;     /*Непредвиденная ошибка получения данных в СОФР*/  
  ERROR_CLIENT_NOTFOUND     CONSTANT NUMBER(5) := 2;     /*Не найден клиент*/
  ERROR_CONTRACT_NOTFOUND   CONSTANT NUMBER(5) := 3;     /*Не найден договор*/
  ERROR_CLIENT_NOTMATCH     CONSTANT NUMBER(5) := 4;     /*Не совпадает клиент по договору*/
  ERROR_ERROR_RUN_REPORT    CONSTANT NUMBER(5) := 6;     /*Ошибка формирования отчета*/
  
  BROKERAGRTERMS_REQ_STATE_NEW        CONSTANT NUMBER(5)  := 0;
  BROKERAGRTERMS_REQ_STATE_SENDANSWER CONSTANT NUMBER(5)  := 1;
  
  PTCK_CFT  CONSTANT NUMBER(5) := 101;  /*Код ЦФТ*/
  DLCK_EKK  CONSTANT NUMBER(5) := 1;    /*Код ЕКК*/
  
  OBJECTTYPE_CONTRACT    CONSTANT NUMBER(5) := 207;    /* тип объекта договор*/
  OBJECTTYPE_SUBCONTRACT CONSTANT NUMBER(5) := 659;    /* тип объекта субдоговор*/
  
  CONTRACT_CATEGORY_STATUS  CONSTANT number(5) := 101;  /* статус договора БО */
  STATUS_CONTRACT_FINISHED  CONSTANT number(5) := 3;    /* обработка завершена */
  
  SUBCONTRACT_CATEGORY_RIGHTUSESTOCK  CONSTANT number(5)    := 6; /* Предоставлять брокеру право использования активов в его интересах */
  SUBCONTRACT_CATEGORY_RIGHTUSESTOCK_YES CONSTANT number(5) := 1; /* Значение = да */
  SUBCONTRACT_CATEGORY_RIGHTUSESTOCK_NO CONSTANT number(5)  := 2; /* Значение = нет */
  SUBCONTRACT_CATEGORY_EDP CONSTANT number(5)               := 102; /* Признак ЕДП на субдоговоре */
  SUBCONTRACT_CATEGORY_EDP_YES CONSTANT number(5)           := 1; /* Значение = да */
  SUBCONTRACT_CATEGORY_EDP_NO CONSTANT number(5)            := 2; /* Значение = нет */

  CATEGORY_IMPERFECT_TRANSACTION  CONSTANT number(3) := 208;    /* Тест НКИ_8. Необеспеченные сделки */
  CATEGORY_VALUE_YES              CONSTANT number(3) := 2;      /* Значение = да */
  
  CONTRACT_NOTEKIND_DEPO_CONTRACTNUMBER CONSTANT number(3):= 102;   /* Номер договора Депо */
  CONTRACT_NOTEKIND_DEPO_CONTRACTDATE CONSTANT number(3)  := 103;   /* Дата заключения договора Депо */
  CONTRACT_NOTEKIND_DEPO_ACCOUNT CONSTANT number(3)       := 101;   /* Счет Депо Владельца */
  CONTRACT_NOTEKIND_DEPO_TRADEACCOUNT CONSTANT number(3)  := 104;   /* Торговый счет Депо */
  
  CONTRACT_NOTEKIND_TARIFFID_REPO CONSTANT number(3)  := 145;
  CONTRACT_NOTEKIND_TARIFFPLAN_REPO CONSTANT number(3):= 146;
  CONTRACT_NOTEKIND_BINDDATE_REPO CONSTANT number(3)  := 147;
  
  C_NAME_REGVAL_NDBOLE_BROKERREPORT_ONOFF  varchar2(104) := 'РСХБ\ИНТЕГРАЦИЯ\ЗАПРОС ОТ ДБО ЮЛ GETREPORT';  
  C_NAME_REGVAL_NDBOLE_BROKERAGRTERMS_ONOFF varchar2(40) := 'РСХБ\ИНТЕГРАЦИЯ\ЗАГРУЗКА_УСЛОВИЙ_ДБО';
  
  -- Упаковщик исходящх сообшений в ДБО ЮЛ через KAFKA
  PROCEDURE out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype);
  
  FUNCTION GetNoteKindFromContract(p_DlcontrID in NUMBER, p_Notekind in NUMBER) 
    RETURN varchar2;

  FUNCTION GetEKK(p_DlcontrID IN NUMBER) 
    RETURN varchar2;    
    
  FUNCTION GetIsSelectImperfectTransactions(p_partyid IN number) 
    RETURN varchar2;
    
  FUNCTION GetBrokerCategoryValue(p_DlcontrID in NUMBER, p_Groupid in number, p_Value_yes in number) 
    RETURN varchar2;
    
  FUNCTION GetTradingPlatformList(p_DlcontrID in NUMBER) 
    RETURN clob;
    
  FUNCTION GetTariffList(p_sfcontrID in NUMBER, p_DlcontrID in NUMBER) 
    RETURN clob;
    
  FUNCTION GetBrokerAccountList(p_sfcontrID in NUMBER, p_ShowRest in number) 
    RETURN clob;
    
  FUNCTION GetCost(p_fiid in number, p_facevaluefi in number, p_daterate in date, p_mp_mmvb in number, p_mp_spb in number) 
    RETURN number;

  FUNCTION GetGCur
    RETURN varchar2;
    
  FUNCTION GetDepoAccountList(p_DlcontrID in NUMBER, p_sfcontrID in NUMBER, p_servkind in varchar2, pmarketid_moex in number, pmarketid_spb in number)  
    RETURN clob;
    
  FUNCTION GetFutureList(p_DlcontrID in NUMBER) 
    RETURN clob;
 
  PROCEDURE GetBrokerageAgreementInfo_json( p_worklogid integer
                                      ,p_messbody  clob
                                      ,p_messmeta  xmltype
                                      ,o_msgid     out varchar2
                                      ,o_MSGCode   out integer
                                      ,o_MSGText   out varchar2
                                      ,o_messbody  out clob
                                      ,o_messmeta  out xmltype);

  function get_rate_on_date (
    p_from_fi dratedef_dbt.t_otherfi%type,
    p_to_fi   dratedef_dbt.t_fiid%type,
    p_date    dratedef_dbt.t_sincedate%type
  ) return number;

  function get_bill_info_json (
    p_bcid dvsbanner_dbt.t_bcid%type
  ) return clob;

  function get_bill_info_issue_json (
    p_bcid dvsbanner_dbt.t_bcid%type
  ) return clob;
  
  function get_bill_info_redempt_json (
    p_bcid dvsbanner_dbt.t_bcid%type
  ) return clob;

  function get_connected_bills_json (
    p_contractid  ddl_order_dbt.t_contractid%type
  ) return clob;

  function get_redeemed_bills_json (
    p_contractid  ddl_order_dbt.t_contractid%type
  ) return clob;

  function get_bill_deal_json (
    p_contractid  ddl_order_dbt.t_contractid%type,
    p_deal_status number default null
  ) return clob;

  procedure send_bill_deal_json (
    p_contractid  ddl_order_dbt.t_contractid%type,
    p_deal_status number default null
  );

  procedure send_bill_json (
    p_bcid  dvsbanner_dbt.t_bcid%type
  );
  
  procedure GetReportLEFromNDBOLE(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype);
                                 
  procedure SendFileNotification (p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype);

  -- BOSS-5842 Запрос из ДБО "Свой бизнес" на открытие/изменение ДБО СОФР
  procedure SendBrokerageAgreementTerms( p_worklogid integer
                                      ,p_messbody  clob
                                      ,p_messmeta  xmltype
                                      ,o_msgid     out varchar2
                                      ,o_MSGCode   out integer
                                      ,o_MSGText   out varchar2
                                      ,o_messbody  out clob
                                      ,o_messmeta  out xmltype);
  
  -- BOSS-5842 Ответ в ДБО "Свой бизнес" о статусе открытия/измнения ДБО СОФР
  procedure SendBrokerageAgreementTermsResp(p_requestID integer
                                           ,o_errorCode out number
                                           ,o_errorDesc OUT varchar2);
  
  -- BOSS-5842 Информирование сопровождения о возникших ошибках
  procedure SupportInforming(p_requestID integer
                            ,o_errorCode out number
                            ,o_errorDesc out varchar2);
  
  -- BOSS-5842 Отправка запроса в Фабрику документов на формирование уведомления 4.4
  procedure GenerateDocument(p_requestID integer
                            ,o_errorCode out number
                            ,o_errorDesc out varchar2);
  
  -- BOSS-5842 Ответ на запрос в Фабрику документов на формирование уведомления 4.4
  procedure GenerateDocumentResp(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype);
  
  -- BOSS-5842 Установить статус обработки запроса
  procedure BrokerageAgreementTermsReqSetState(p_requestID integer
                                              ,p_state     integer
                                              ,p_errorCode integer
                                              ,p_errorText varchar2);

END it_ndbole;
/