CREATE OR REPLACE PACKAGE BODY RSB_CORRECT_ACCOUNT IS
   
 /**
  @brief Поиск ближайшей доступной даты из списка опердней
  @param[in] p_date Дата открытия субдоговора 
  @param[in] p_date Подразделение тс  
 */    
 FUNCTION getNextCurDate(p_date DATE, p_branch NUMBER) RETURN DATE
   
 IS
 l_cnt      NUMBER;
 l_out_date DATE := trunc(p_date);  
 BEGIN
  SELECT  COUNT(*)
   INTO l_cnt
   FROM dcurdate_dbt d
  WHERE d.t_branch = p_branch
    AND d.t_isclosed <> chr(88)
    AND d.t_curdate = l_out_date;
    
  IF(L_CNT = 0) THEN
   BEGIN
    SELECT DISTINCT 
       first_value(d.t_curdate) over (order by d.t_curdate)
      INTO l_out_date
      FROM dcurdate_dbt d
     WHERE d.t_branch = p_branch
       AND d.t_isclosed <> chr(88)
       AND d.t_curdate > l_out_date;
       
    EXCEPTION 
      WHEN OTHERS
        THEN
           
     SELECT MAX(d.t_curdate) 
       INTO l_out_date
       FROM dcurdate_dbt d
      WHERE d.t_branch = p_branch
        AND d.t_isclosed <> chr(88);
    END;   
  END IF;
   
 RETURN l_out_date;

END getNextCurDate;  
 
 /**
   @brief Вставка данных в таблицу во временную таблицу DDACCOUNTCORRECTION_TMP
          Отбор счетов, которыее привязаны одновременно к 2 договорам
  */  
 PROCEDURE CreateActiveAcc
 IS

 BEGIN
   
 EXECUTE IMMEDIATE 'TRUNCATE TABLE DDACCOUNTCORRECTION_TMP';
 
 FOR R IN (
    SELECT distinct
         mc.t_Account,
         acc.t_NameAccount,
         mc.t_currency,
         (select fin.t_ccy from DFININSTR_DBT FIN where fin.t_fiid = mc.t_currency) t_ccy,
         sf_root.t_Number as RightNumber,
         sf_root.t_acccode as Rightacccode,
         to_char(sf_root.t_datebegin,'dd.mm.yyyy') as RightRootDateBegin,
         mc.t_Owner as RightClientID,
         acc.t_usertypeaccount,
         (select pt.t_shortname from dparty_dbt pt where pt.t_partyid = mc.t_Owner) as RightClientName,
         sf_root1.t_Number as WrongRootNumber,
         sf1.t_id as WrongtSfId,
         sf1.t_Number as WrongSfNumber,
         sf_root1.t_acccode as WrongtRootAcccode,
         sf_root1.t_id as WrongtSfRootId,
         sf_root1.t_datebegin as WrongRootDateBegin,
         mc1.t_Owner as WrongClientID,
         (select pt.t_shortname from dparty_dbt pt where pt.t_partyid = mc1.t_Owner) as WrongClientName,
         dl1.t_dlcontrid as WrongDlContrid,
         dl.t_dlcontrid as RightDlContrid,
         RSB_Account.restac(mc.t_Account, mc.t_Currency, SYSDATE, mc.t_Chapter, null) t_Rest,
         NVL((select sum(t_debet) from drestdate_dbt where t_accountid = acc.t_accountid and t_restdate >= acc.t_open_date and t_restdate <= sysdate), 0) as debet,
         NVL((select sum(t_credit) from drestdate_dbt where t_accountid = acc.t_accountid and t_restdate >= acc.t_open_date and t_restdate <= sysdate), 0) as credit
                          FROM ddlcontrmp_dbt mp,
                               dsfcontr_dbt sf,
                               dmcaccdoc_dbt mc,
                               ddlcontr_dbt dl,
                               dsfcontr_dbt sf_root,
                               dmcaccdoc_dbt mc1,
                               daccount_dbt acc,
                               dsfcontr_dbt sf1,
                               ddlcontrmp_dbt mp1,
                               ddlcontr_dbt dl1,
                               dsfcontr_dbt sf_root1
                         WHERE mc.t_CatID = 818
                           AND mc.t_IsCommon = 'X'
                           AND mc.t_Owner = sf.t_PartyID
                           AND mc.t_ClientContrID = sf.t_ID
                           AND sf.t_ID = mp.t_SfContrID
                           AND mp.t_DlContrID = dl.t_DlContrID
                           AND dl.t_SfContrID = sf_root.t_ID
                           AND mc1.t_CatID = 818
                           AND mc1.t_IsCommon = 'X'
                           AND mc1.t_Account = mc.t_Account
                           AND mc1.t_Owner != mc.t_Owner
                           AND acc.t_Account = mc.t_Account
                           AND mc.t_Owner = acc.t_Client
                           AND mc1.t_Owner = sf1.t_PartyID
                           AND mc1.t_ClientContrID = sf1.t_ID
                           AND sf1.t_ID = mp1.t_SfContrID
                           AND mp1.t_DlContrID = dl1.t_DlContrID
                           AND dl1.t_SfContrID = sf_root1.t_ID
                           AND mc.t_currency not in (19, 20)
                           )
         LOOP

           INSERT INTO  DDACCOUNTCORRECTION_TMP (T_ACCOUNT,
                                          T_RIGHTCLIENTNAME,
                                          T_CURRENCYACC,
                                          T_CCYACC,
                                          T_REST,
                                          T_TURNOVER,
                                          T_RIGHTROOTDATEBEGIN, 
                                          T_RIGHTROOTNUMBER,
                                          T_BEFORENAMEACCOUNT,
                                          T_AFTERNAMEACCOUNT,
                                          T_WRONGSUBNUMBER,
                                          T_NEWACCOUNT,
                                          T_USERTYPEACCOUNT,
                                          T_RIGHTCLIENTID,
                                          T_RIGHTSERVKINDSUB,
                                          T_RIGHTSERVKIND,
                                          T_RIGHTSFID,
                                          T_RIGHTMARKETID,
                                          T_RIGHTDLCONTRID,
                                          T_WRONGROOTNUMBER,
                                          T_WRONGTSFROOTID,
                                          T_WRONGROOTDATEBEGIN,
                                          T_WRONGCLIENTID,
                                          T_WRONGCLIENTNAME,
                                          T_WRONGDLCONTRID,
                                          T_WRONGSFID )
         SELECT r.t_account,
                r.rightclientname,
                r.t_currency,
                r.t_ccy,
                r.t_rest,
                CASE
                  WHEN r.credit > 0 or r.debet > 0
                    THEN 'X'
                   ELSE chr(0)
                END,
                r.rightrootdatebegin,
                r.rightnumber,
                r.t_nameaccount,
                chr(0),
                r.wrongsfnumber,
                chr(0),
                r.t_usertypeaccount,
                r.rightclientid,
                0,--r.rightservkindsub,
                0,--r.rightservkind,
                0,--r.rightsfid,
                0,--r.rightmarketid,
                r.rightdlcontrid,
                r.wrongrootnumber,
                r.wrongtsfrootid,
                r.wrongrootdatebegin,
                r.wrongclientid,
                r.wrongclientname,
                r.wrongdlcontrid,
                r.wrongtsfid
         FROM dual;
    END LOOP;
    
 COMMIT;
 
 END CreateActiveAcc;


END RSB_CORRECT_ACCOUNT;
/