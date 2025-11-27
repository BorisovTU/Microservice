BEGIN
  UPDATE dobjcode_dbt c
     SET c.t_ObjectID = (SELECT t_FIID 
                           FROM dfininstr_dbt 
                          WHERE t_CCY = c.t_Code 
                            AND t_FI_Kind = 1)
   WHERE c.t_ObjectType = 9
     and c.t_CodeKind = 11
     and c.t_Code in ('SLV', 'GLD');
END;
/