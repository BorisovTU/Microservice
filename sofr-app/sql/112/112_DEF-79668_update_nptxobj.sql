/*Обновление НДР*/
DECLARE
   v_cnt   NUMBER := 0;
BEGIN
   FOR one_obj
      IN (SELECT obj.*
            FROM dnptxobj_dbt obj
           WHERE     obj.t_Kind = 370
                 AND obj.t_OutSystCode = 'ДЕПО'
                 AND obj.t_OutObjID <> CHR (1)
                 AND obj.t_AnaliticKind2 = 2030
                 AND obj.t_AnaliticKind3 = 3010
                 AND obj.t_Analitic3 > 0
                 AND obj.t_AnaliticKind4 = 4010
                 AND obj.t_Analitic4 = 2                     /*Не обращается*/
                                        )
   LOOP
      SELECT COUNT (1)
        INTO v_cnt
        FROM dnptxfi_dbt f
       WHERE     f.t_FIID = one_obj.t_Analitic3
             AND f.t_Date < one_obj.t_Date
             AND f.t_Circulate = 1
             AND ROWNUM = 1;

      IF v_cnt > 0
      THEN
         UPDATE dnptxobj_dbt
            SET t_Analitic4 = 3
          WHERE t_ObjID = one_obj.t_ObjID;

         INSERT INTO DNPTXDEFCLIENTS_DBT (T_DEFNUM, T_CLIENTID, T_TAXPERIOD)
              VALUES (
                        'DEF-79668',
                        one_obj.t_Client,
                        EXTRACT (YEAR FROM one_obj.t_Date));
      END IF;
   END LOOP;
END;
/