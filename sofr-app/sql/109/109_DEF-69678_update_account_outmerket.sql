BEGIN
  UPDATE daccount_dbt 
     SET T_NAMEACCOUNT = REPLACE(T_NAMEACCOUNT, SUBSTR(T_NAMEACCOUNT, INSTR(T_NAMEACCOUNT, '('), (CASE WHEN INSTR(T_NAMEACCOUNT, '(') > 0 THEN INSTR(T_NAMEACCOUNT, ')') - INSTR(T_NAMEACCOUNT, '(') + 1 ELSE 0 END)), '(внебиржевой рынок)')
   WHERE T_ACCOUNTID IN (select ac.t_accountid 
                           from daccount_dbt ac, dparty_dbt par, dmcaccdoc_dbt doc, dpersn_dbt per, dsfcontr_dbt sf 
                          where par.t_partyid = ac.t_client 
                            and doc.t_catid = 70 
                            and doc.t_iscommon = CHR(88) 
                            and doc.t_account = ac.t_account 
                            and doc.t_chapter = ac.t_chapter 
                            and doc.t_currency = ac.t_code_currency 
                            and ac.t_close_date = to_date('01/01/0001','dd/mm/yyyy')
                            and par.t_legalform = 2
                            and sf.t_id = doc.t_clientcontrid
                            and sf.t_servkindsub = 9
                            and per.t_personid = par.t_partyid 
                            and per.t_isemployer <> CHR(88)
                            and ac.t_nameaccount like '%ПАО%'
                        );
END;
/