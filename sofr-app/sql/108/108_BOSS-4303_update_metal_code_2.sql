BEGIN
  UPDATE dobjcode_dbt c
     SET c.t_ObjectID = (SELECT t_FIID 
                           FROM dfininstr_dbt 
                          WHERE t_CCY = decode(c.t_Code, 'SLV', 'XAG', 'XAU') 
                            AND t_FI_Kind = 6)
   WHERE c.t_ObjectType = 9
     AND c.t_CodeKind = 11
     AND c.t_Code in ('SLV', 'GLD');
END;
/

BEGIN
  INSERT INTO dobjcode_dbt (t_objecttype,
                            t_codekind,
                            t_objectid,
                            t_code,
                            t_state,
                            t_bankdate,
                            t_sysdate,
                            t_systime,
                            t_userid,
                            t_unique,
                            t_autokey,
                            t_bankclosedate,
                            t_normcode)
  SELECT 9, 
         11, 
         f.t_FIID, 
         'CLT_'||f.t_CCY, 
         0, 
         trunc(sysdate - 1), 
         trunc(sysdate), 
         to_date('01010001'||to_char(sysdate, 'hh24miss'), 'ddmmyyyyhh24miss'), 
         1, 
         chr(0), 
         0, 
         to_date('01.01.0001', 'dd.mm.yyyy'), 
         chr(0)
    FROM dfininstr_dbt f
   WHERE f.t_CCY in ('SLV', 'GLD')
     AND f.t_FI_Kind = 1;
END;
/

