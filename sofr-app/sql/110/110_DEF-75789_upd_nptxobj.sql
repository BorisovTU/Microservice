/*Обновление НДР*/
DECLARE
BEGIN
  UPDATE DNPTXOBJ_DBT
     SET T_KIND = 1115
   WHERE T_OBJID IN (SELECT obj.t_ObjID
                       FROM dnptxobj_dbt obj
                      WHERE obj.t_Kind = 120
                        AND obj.t_OutSystCode = 'DEPO'
                        AND obj.t_AnaliticKind2 = 2030
                        AND obj.t_AnaliticKind1 > 0
                    );

END;
/