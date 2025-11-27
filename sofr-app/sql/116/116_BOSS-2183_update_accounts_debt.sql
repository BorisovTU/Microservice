BEGIN
  UPDATE daccount_dbt
     SET t_UserTypeAccount = t_UserTypeAccount||'E'
   WHERE t_AccountID IN (
         SELECT DISTINCT acc.t_AccountID 
           FROM daccount_dbt acc, dmcaccdoc_dbt mc, dmccateg_dbt cat, dsfcontr_dbt sf, dobjatcor_dbt at  
          WHERE INSTR(acc.t_UserTypeAccount, 'E') = 0
            AND acc.t_Close_Date = TO_DATE('01.01.0001', 'DD.MM.YYYY')
            AND acc.t_Account = mc.t_Account
            AND acc.t_Chapter = mc.t_Chapter
            AND acc.t_Code_Currency = mc.t_Currency
            AND mc.t_CatID = cat.t_ID
            AND cat.t_LevelType = 1 
            AND cat.t_Code IN ('Треб. с н.с. брок', 'Треб. с н.с. брок. резерв')
            AND mc.t_Owner = sf.t_PartyID
            AND mc.t_ClientContrID = sf.t_ID
            AND at.t_ObjectType = 659
            AND at.t_GroupID = 102
            AND at.t_Object = LPAD(sf.t_ID, 10, '0')
            AND at.t_AttrID = 1);
END;
/