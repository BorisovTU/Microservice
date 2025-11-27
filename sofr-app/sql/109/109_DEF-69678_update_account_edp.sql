BEGIN
  UPDATE daccount_dbt 
     SET T_NAMEACCOUNT = REPLACE(T_NAMEACCOUNT, SUBSTR(T_NAMEACCOUNT, INSTR(T_NAMEACCOUNT, '('), (CASE WHEN INSTR(T_NAMEACCOUNT, '(') > 0 THEN INSTR(T_NAMEACCOUNT, ')') - INSTR(T_NAMEACCOUNT, '(') + 1 ELSE 0 END)), '(ÖÑè)')
   WHERE T_ACCOUNTID IN (SELECT AC.T_ACCOUNTID
                           FROM dmcaccdoc_dbt dmc
                     INNER JOIN daccount_dbt ac
                             ON AC.T_ACCOUNT = DMC.T_ACCOUNT
                            AND AC.T_CLOSE_DATE = to_date('01/01/0001','dd/mm/yyyy')
                            AND AC.T_NAMEACCOUNT NOT LIKE '%ÖÑè%'
                      LEFT JOIN dsfcontr_dbt sf
                             ON SF.T_PARTYID = DMC.T_OWNER 
                            AND SF.T_ID = DMC.T_CLIENTCONTRID
                          WHERE DMC.T_CATID = 70
                            AND DMC.T_ISCOMMON = CHR(88)
                            AND EXISTS (SELECT kat.*
                                          FROM dobjatcor_dbt kat
                                         WHERE KAT.T_OBJECTTYPE = 659
                                           AND KAT.T_GROUPID = 102
                                           AND KAT.T_OBJECT = LPAD(DMC.T_CLIENTCONTRID, 10, '0')
                                           AND KAT.T_ATTRID = 1
                                       )
                        );
END;
/