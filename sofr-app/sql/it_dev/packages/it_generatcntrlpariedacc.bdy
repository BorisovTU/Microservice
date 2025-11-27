CREATE OR REPLACE package body it_generatcntrlpariedacc is

  FUNCTION IsDV(pDocKind IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
  BEGIN
    IF (((pDocKind between DL_DVDEAL and DL_DVNDEAL) or (pDocKind between DL_DVNDEALPMGR and DL_DVDEALT3) or pDocKind in ( RSB_Secur.DL_FIXING,  RSB_Secur.DL_DVCURMARKET, RSB_Secur.DV_CSA, RSB_Secur.DL_SETTLEMENTFCURM) )) THEN
       RETURN 1;
    END IF;
    
    RETURN 0;
   
  END IsDV;
  
  FUNCTION IsMmark(pDocKind IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
  BEGIN
    IF (pDocKind in (DL_IBCDOC, DL_CREDITLN, DL_MMPAWN, DL_GENAGRFOREXDOC, DL_GENAGRIBCDOC)) THEN
       RETURN 1;
    END IF;
    
    RETURN 0;
   
  END IsMmark;
  
  FUNCTION IsSecur(pDocKind IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
  BEGIN
    IF (pDocKind in (RSB_Secur.DL_SECURITYDOC, RSB_SECUR.DL_RETIREMENT, RSB_SECUR.DL_AVRWRT, RSB_Secur.DL_MOVINGDOC, RSB_Secur.DL_RETIREMENT_OWN, RSB_Secur.DL_SECURLEG )) THEN
       RETURN 1;
    END IF;
    
    RETURN 0;
   
  END IsSecur;
  
  FUNCTION IsBankVeksel(pDocKind IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
  BEGIN
    IF (pDocKind in ( RSB_Secur.DL_VEKSELORDER,  RSB_Secur.DL_VSBARTERORDER, RSB_Secur.DL_VSBANNER, DL_VEKSELDRAWORDER)) THEN
       RETURN 1;
    END IF;
    
    RETURN 0;
   
  END IsBankVeksel;
  
  FUNCTION IsAccVeksel(pDocKind IN NUMBER)
  RETURN NUMBER
  DETERMINISTIC
  IS
  BEGIN
    IF (pDocKind in (DL_VEKSELACCOUNTED, DL_VAREPAY, RSB_Secur.DL_VSBANNER)) THEN
       RETURN 1;
    END IF;
    
    RETURN 0;
   
  END IsAccVeksel;
  
  
  PROCEDURE CreateJson(pGuID IN varchar2, pRepDate in date, pRepStartDate in timestamp, oErrorCode out number, oErrorDesc out varchar2) is
     vJson CLOB;
     vMessMeta XMLType;
     vaddparams it_ips_dfactory.tt_addparams;
  BEGIN
     select JSON_OBJECT(
              'pariedAccControl' value JSON_OBJECT(
                    'reportMetadata' value JSON_OBJECT (
                       'reportStartDate' value to_char(pRepStartDate, 'DD.MM.YYYY'),
                       'reportTime' value to_char(pRepStartDate, 'HH24:MI:SS'),
                       'processedBy' value nvl((select persn.t_Name1 || ' ' || persn.t_Name2 || ' ' || persn.t_Name3 as t_Name from dperson_dbt person join dpersn_dbt persn on persn.t_PersonID = persOn.t_PartyID where person.t_oper = RsbSessionData.oper), ''),
                       'reportForDate' value TO_CHAR(pRepDate, 'DD.MM.YYYY')
                       RETURNING CLOB),
                    'reportModules' value JSON_OBJECT (
                       'fissiko' value JSON_OBJECT (
                          'sheetName' value 'ФИССиКО',
                          'errorCount' value (select count(1) from DREPORT_PARIED_ACC_CONTROL_TMP  where t_module_name='ФИССиКО'),
                          'transactions' value (select JSON_ARRAYAGG(JSON_OBJECT('transactionNum' value ROWNUM, 'accountNum' value d.t_account_number, 'balance' value d.t_balance, 'currencyCode' value d.t_currency_code, 'errorReason' value d.t_error_reason RETURNING CLOB ) RETURNING CLOB) from DREPORT_PARIED_ACC_CONTROL_TMP d where t_module_name='ФИССиКО')
                         RETURNING CLOB ),
                       'securities' value JSON_OBJECT (
                          'sheetName' value 'Ценные бумаги',
                          'errorCount' value (select count(1) from DREPORT_PARIED_ACC_CONTROL_TMP  where t_module_name='БО ЦБ'),
                          'transactions' value (select JSON_ARRAYAGG(JSON_OBJECT('transactionNum' value ROWNUM, 'accountNum' value d.t_account_number, 'balance' value d.t_balance, 'currencyCode' value d.t_currency_code, 'errorReason' value d.t_error_reason RETURNING CLOB) RETURNING CLOB) from DREPORT_PARIED_ACC_CONTROL_TMP d where t_module_name='БО ЦБ')
                         RETURNING CLOB ),
                       'discountedBills' value JSON_OBJECT (
                          'sheetName' value 'Учтенные векселя',
                          'errorCount' value (select count(1) from DREPORT_PARIED_ACC_CONTROL_TMP  where t_module_name='УВ'),
                          'transactions' value (select JSON_ARRAYAGG(JSON_OBJECT('transactionNum' value ROWNUM, 'accountNum' value d.t_account_number, 'balance' value d.t_balance, 'currencyCode' value d.t_currency_code, 'errorReason' value d.t_error_reason RETURNING CLOB) RETURNING CLOB) from DREPORT_PARIED_ACC_CONTROL_TMP d where t_module_name='УВ')
                         RETURNING CLOB ),
                       'bankBills' value JSON_OBJECT (
                          'sheetName' value 'Векселя банка',
                          'errorCount' value (select count(1) from DREPORT_PARIED_ACC_CONTROL_TMP  where t_module_name='СВ'),
                          'transactions' value (select JSON_ARRAYAGG(JSON_OBJECT('transactionNum' value ROWNUM, 'accountNum' value d.t_account_number, 'balance' value d.t_balance, 'currencyCode' value d.t_currency_code, 'errorReason' value d.t_error_reason RETURNING CLOB) RETURNING CLOB) from DREPORT_PARIED_ACC_CONTROL_TMP d where t_module_name='СВ')
                         RETURNING CLOB ),
                       'interBankCredits' value JSON_OBJECT (
                          'sheetName' value 'Межбанк. кредиты',
                          'errorCount' value (select count(1) from DREPORT_PARIED_ACC_CONTROL_TMP  where t_module_name='МБК'),
                          'transactions' value (select JSON_ARRAYAGG(JSON_OBJECT('transactionNum' value ROWNUM, 'accountNum' value d.t_account_number, 'balance' value d.t_balance, 'currencyCode' value d.t_currency_code, 'errorReason' value d.t_error_reason RETURNING CLOB) RETURNING CLOB) from DREPORT_PARIED_ACC_CONTROL_TMP d where t_module_name='МБК')
                         RETURNING CLOB )
                      RETURNING CLOB )
                 RETURNING CLOB ) 
     RETURNING CLOB ) into vJson from dual;
     
      vMessMeta := IT_IPS_DFactory.add_KafkaHeader_Xmessmeta( p_List_dllvalues_dbt => 5080
                                                            ,p_traceid => it_q_message.get_sys_guid
                                                            ,p_requestid => pGuID
                                                            ,p_templateparams => NULL
                                                            ,p_outputfilename => 'Контроль по привязке парных счетов за ' || TO_CHAR(pRepDate, 'DD.MM.YYYY') || SUBSTR(pGuID, 1, 6) );
                                                            
                    
     IT_Kafka.load_msg_S3(p_msgid => pGuID
                        ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                        ,p_ServiceName => 'IPS_DFACTORY.GenerateCntrlPariedAccRep' 
                        ,p_Receiver => IT_IPS_DFactory.C_C_SYSTEM_NAME
                        ,p_CORRmsgid => NULL
                        ,p_MESSBODY => vJson  -- JSON c данными   
                        ,p_MessMETA => vMessMETA
                        ,p_isquery => 0
                        ,o_ErrorCode => oErrorCode
                        ,o_ErrorDesc => oErrorDesc);
     
  END CreateJson;

    --BIQ-27551. Обработка ответа о получении данных по операции списания ценных бумаг ПКО и корректировки лимитов по ценным бумагам
  procedure ReportRun(pRepDate in date, pIsSecur IN CHAR, pIsDV IN CHAR, pIsMMark IN CHAR, pIsBankVeksel IN CHAR, pIsAccVeksel IN CHAR, oGuID out varchar2, oErrorCode out NUMBER, oErrorDesc OUT VARCHAR2) is 
                                 
    TYPE rep_cntrlparacc_t IS TABLE OF DREPORT_PARIED_ACC_CONTROL_TMP%ROWTYPE;
     g_rep_cntrlparacc_ins rep_cntrlparacc_t := rep_cntrlparacc_t();
     rep_cntrlparacc DREPORT_PARIED_ACC_CONTROL_TMP%rowtype;
     
     RepStartDate TIMESTAMP := CURRENT_TIMESTAMP;
     
     rest NUMBER := 0;
  begin
  oGuID := it_q_message.get_sys_guid;
  
  FOR one_rec IN (select distinct accdoc.t_account, accdoc.t_chapter, accdoc.t_currency, accdoc.t_dockind, fin.t_iso_number, account.t_type_account, account.t_pairaccount, bal.t_balance, bal.t_pairbalance
                            from dmcaccdoc_dbt accdoc
                            join dfininstr_dbt fin on fin.t_fiid = accdoc.t_currency
                            join daccount_dbt account on account.t_account = accdoc.t_account and account.t_chapter = accdoc.t_chapter and account.t_code_currency = accdoc.t_currency
                            join dbalance_dbt bal on bal.t_balance = account.t_balance and bal.t_pairbalance != chr(1)
                            where pRepDate between bal.t_bdincludebwp and bal.t_bdexcludebwp and account.t_open_date <= pRepDate and (account.t_close_date = to_date('01.01.0001','DD.MM.YYYY') or account.t_close_date > pRepDate)
                              and (account.t_pairaccount = chr(1) or instr(account.t_type_account, 'Ш') = 0 or substr(account.t_pairaccount, 1, LENGTH(bal.t_pairbalance)) != bal.t_pairbalance)
                              and bal.t_balance in (
                                 SELECT REGEXP_SUBSTR(t_balance, '[^'||delimiter||']+', 1, LEVEL) AS split_part
                                   FROM
                                        (SELECT t_accountnumber AS t_balance, '/' AS delimiter FROM dacc_paried_check_dbt chck WHERE chck.t_catid = 0 or chck.t_catid = accdoc.t_catid)
                              CONNECT BY
                                         LEVEL <= LENGTH(REGEXP_REPLACE(t_balance, '[^'||delimiter||']+', '')) + 1
                                 )
                              and (    (pIsDV = chr(88) and IsDV(t_dockind) = 1 )
                                      or (pIsMmark = chr(88) and IsMmark(t_dockind) = 1)
                                      or (pIsSecur = chr(88) and IsSecur(t_dockind) = 1)
                                      or (pIsBankVeksel = chr(88) and IsBankVeksel(t_dockind) = 1 )
                                      or (pIsAccVeksel = chr(88) and IsAccVeksel(t_dockind) = 1 )
                                )
                             )
                             
    LOOP
       rest := rsb_account.restac (one_rec.t_Account, one_rec.t_Currency, pRepDate, one_rec.t_Chapter, NULL);
       
       rep_cntrlparacc.t_report_start_date := SYSDATE;
       rep_cntrlparacc.t_report_start_time := CURRENT_TIMESTAMP;
       rep_cntrlparacc.t_report_generation_date := pRepDate;
       rep_cntrlparacc.t_account_number := one_rec.t_account;
       rep_cntrlparacc.t_balance := rsb_account.restac (one_rec.t_Account, one_rec.t_Currency, pRepDate, one_rec.t_Chapter, NULL);
       rep_cntrlparacc.t_currency_code := one_rec.t_iso_number;
       
       rep_cntrlparacc.t_error_reason := '';
       if (one_rec.t_pairaccount = chr(1)) then
          rep_cntrlparacc.t_error_reason := 'Для лицевого счета не задан парный счет';
       elsif (instr(one_rec.t_type_account, 'Ш') = 0) then
          rep_cntrlparacc.t_error_reason := 'Для лицевого счета не указан признак парности';
       elsif (substr(one_rec.t_pairaccount, 1, LENGTH(one_rec.t_pairbalance)) != one_rec.t_pairbalance) then
          rep_cntrlparacc.t_error_reason := 'Указанный парный счет в лицевом счете не соответствует балансовому';
       end if;
       
       rep_cntrlparacc.t_module_name := '';
       if (IsDV(one_rec.t_dockind) = 1 ) then
          rep_cntrlparacc.t_module_name := 'ФИССиКО';
       elsif (IsMmark(one_rec.t_dockind) = 1) then
          rep_cntrlparacc.t_module_name := 'МБК';
        elsif (IsSecur(one_rec.t_dockind) = 1) then
          rep_cntrlparacc.t_module_name := 'БО ЦБ';
       elsif (IsBankVeksel(one_rec.t_dockind) = 1) then
          rep_cntrlparacc.t_module_name := 'СВ'; 
       elsif (IsAccVeksel(one_rec.t_dockind) = 1) then
          rep_cntrlparacc.t_module_name := 'УВ';
        end if;

        g_rep_cntrlparacc_ins.extend;
        g_rep_cntrlparacc_ins(g_rep_cntrlparacc_ins.LAST) := rep_cntrlparacc;
    END LOOP;
    
      IF g_rep_cntrlparacc_ins IS NOT EMPTY THEN
         FORALL i IN g_rep_cntrlparacc_ins.FIRST .. g_rep_cntrlparacc_ins.LAST
              INSERT INTO DREPORT_PARIED_ACC_CONTROL_TMP
                   VALUES g_rep_cntrlparacc_ins(i);
         g_rep_cntrlparacc_ins.delete;
     END IF;
     
     CreateJson(oGuID, pRepDate, RepStartDate, oErrorCode, oErrorDesc);

  end; 

end it_generatcntrlpariedacc;
/
