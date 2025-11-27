--Закрытие счетов 458* с ненулевыми остатками, которые соответствуют договорам, которые уже закрыты
BEGIN
  UPDATE daccount_dbt
     SET t_Close_Date = TRUNC(SYSDATE), 
         t_Open_Close = 'З'
   WHERE t_AccountID IN
             (SELECT /*+ ordered full(acc)*/ acc.t_AccountID
                FROM daccount_dbt acc, daccount_dbt acc_306, dmcaccdoc_dbt mc, dsfcontr_dbt sf, ddlcontrmp_dbt mp, ddlcontr_dbt dl, dsfcontr_dbt sf_root  
               WHERE acc.t_Balance LIKE '458%'
                 AND acc.t_Balance <> '45818'
                 AND acc.t_Close_Date = TO_DATE('01.01.0001', 'DD.MM.YYYY')
                 AND acc.t_Client <> 1
                 AND acc.t_Client = acc_306.t_Client
                 AND SUBSTR(acc.t_Account, 12) = SUBSTR(acc_306.t_Account, 12)
                 AND acc.t_Code_Currency = acc_306.t_Code_Currency
                 AND mc.t_CatID = 70
                 AND acc_306.t_Account = mc.t_Account
                 AND acc_306.t_Chapter = mc.t_Chapter
                 AND acc_306.t_Code_Currency = mc.t_Currency
                 AND mc.t_IsCommon = 'X'
                 AND mc.t_Owner = sf.t_PartyID
                 AND mc.t_ClientContrID = sf.t_ID
                 AND mp.t_SfContrID = sf.t_ID
                 AND dl.t_DlContrID = mp.t_DlContrID
                 AND dl.t_SfContrID = sf_root.t_ID
                 AND sf_root.t_DateClose <> TO_DATE('01.01.0001', 'DD.MM.YYYY')
                 AND NVL((SELECT r.t_rest
                            FROM drestdate_dbt r
                           WHERE r.t_accountID = acc.t_AccountID
                             AND r.t_restcurrency = acc.t_Code_Currency
                             AND r.t_RestDate =
                                     (SELECT MAX(r1.t_RestDate)
                                        FROM drestdate_dbt r1
                                       WHERE r1.t_accountID = acc.t_AccountID
                                         AND r1.t_restcurrency = acc.t_Code_Currency
                                         AND r1.t_RestDate <= SYSDATE)
                         ), 0) = 0
             );
END;
/