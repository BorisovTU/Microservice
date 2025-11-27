-- update для отображения кода в панели фьючерса
  UPDATE dobjcode_dbt c
     SET c.t_State = 0
   WHERE c.t_ObjectType = 9  -- ФИ
     AND c.t_CodeKind   = 11 -- Код на ММВБ
     AND c.t_ObjectID   = (SELECT t_FIID 
                             FROM dfininstr_dbt 
                            WHERE t_FI_CODE = 'GOLD'
                              AND t_FI_Kind = 7 /*артикул*/)
/