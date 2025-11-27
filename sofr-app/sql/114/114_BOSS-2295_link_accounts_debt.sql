--Привязка счетов 458*, открытых в банке по соответствующей категории учета к соответствующим субдоговорам
BEGIN
  INSERT INTO dmcaccdoc_dbt
        (t_id,
         t_iscommon,
         t_dockind,
         t_docid,
         t_catid,
         t_catnum,
         t_chapter,
         t_account,
         t_currency,
         t_templnum,
         t_groupnum,
         t_periodid,
         t_activatedate,
         t_disablingdate,
         t_isusable,
         t_fiid,
         t_owner,
         t_place,
         t_issuer,
         t_kind_account,
         t_centr,
         t_centroffice,
         t_actiondate,
         t_fiid2,
         t_clientcontrid,
         t_bankcontrid,
         t_marketplaceid,
         t_marketplaceofficeid,
         t_firole,
         t_indexdate,
         t_departmentid,
         t_contractor,
         t_branch,
         t_corrdepartmentid,
         t_currencyeq,
         t_currencyeq_ratetype,
         t_currencyeq_ratedate,
         t_currencyeq_rateextra,
         t_mcbranch)
  SELECT /*+ ordered full(acc)*/
         dmcaccdoc_dbt_seq.nextval, --t_id
         CASE WHEN ((pt.t_NotResident = 'X' AND acc.t_Balance <> '45817') OR (pt.t_NotResident <> 'X' AND acc.t_Balance = '45817')) THEN CHR(0) ELSE 'X' END, --t_iscommon
         0, --t_dockind
         0, --t_docid
         cat.t_ID, --t_catid
         cat.t_Number, --t_catnum
         acc.t_Chapter, --t_chapter
         acc.t_Account, --t_account
         acc.t_Code_Currency, --t_currency
         templ.t_Number, --t_templnum
         0, --t_groupnum
         0, --t_periodid
         acc.t_Open_Date, --t_activatedate
         CASE WHEN ((pt.t_NotResident = 'X' AND acc.t_Balance <> '45817') OR (pt.t_NotResident <> 'X' AND acc.t_Balance = '45817')) THEN TRUNC(SYSDATE) ELSE TO_DATE('01.01.0001', 'DD.MM.YYYY') END, --t_disablingdate
         CASE WHEN ((pt.t_NotResident = 'X' AND acc.t_Balance <> '45817') OR (pt.t_NotResident <> 'X' AND acc.t_Balance = '45817')) THEN CHR(0) ELSE 'X' END, --t_isusable
         -1, --t_fiid
         sf.t_PartyID, --t_owner
         -1, --t_place
         -1, --t_issuer
         acc.t_Kind_Account, --t_kind_account
         -1, --t_centr
         -1, --t_centroffice
         acc.t_Open_Date, --t_actiondate
         0, --t_fiid2
         sf.t_ID, --t_clientcontrid
         -1, --t_bankcontrid
         -1, --t_marketplaceid
         -1, --t_marketplaceofficeid,
         -1, --t_firole
         -1, --t_indexdate
         1, --t_departmentid
         -1, --t_contractor
         1, --t_branch
         0, --t_corrdepartmentid
         -1, --t_currencyeq
         0, --t_currencyeq_ratetype
         0, --t_currencyeq_ratedate
         0, --t_currencyeq_rateextra
         1 --t_mcbranch
    FROM daccount_dbt acc, daccount_dbt acc_306, dmcaccdoc_dbt mc, dsfcontr_dbt sf, ddlcontrmp_dbt mp, ddlcontr_dbt dl, dmccateg_dbt cat, dmctempl_dbt templ, dparty_dbt pt  
   WHERE acc.t_Balance LIKE '458%'
     AND acc.t_Balance <> '45818'
     AND acc.t_Close_Date = TO_DATE('01.01.0001', 'DD.MM.YYYY')
     AND acc.t_Client <> 1
     AND acc.t_Client = acc_306.t_Client
     AND (    SUBSTR(acc.t_Account, 12) = SUBSTR(acc_306.t_Account, 12)
           OR (acc.t_Account = '45815810699000559026' AND acc_306.t_Account = '30601810399000087839')
           OR (acc.t_Account = '45815810599000050764' AND acc_306.t_Account = '30601810899000115719')
           OR (acc.t_Account = '45815810699000900002' AND acc_306.t_Account = '30601810799000104587')
         )
     AND acc.t_Code_Currency = acc_306.t_Code_Currency
     AND mc.t_CatID = 70
     AND acc_306.t_Account = mc.t_Account
     AND acc_306.t_Chapter = mc.t_Chapter
     AND acc_306.t_Code_Currency = mc.t_Currency
     AND mc.t_IsCommon = 'X'
     AND mc.t_Owner = sf.t_PartyID
     AND mc.t_ClientContrID = sf.t_ID
     AND sf.t_DateClose = TO_DATE('01.01.0001', 'DD.MM.YYYY')
     AND mp.t_SfContrID = sf.t_ID
     AND dl.t_DlContrID = mp.t_DlContrID
     AND cat.t_LevelType = 1 
     AND cat.t_Code = 'Треб. с н.с. брок'
     AND templ.t_CatID = cat.t_ID
     AND templ.t_Balance = acc.t_Balance
     AND templ.t_Mask = DECODE(dl.t_IIS, 'X', '1', '0')
     AND pt.t_PartyID = sf.t_PartyID;
END;
/