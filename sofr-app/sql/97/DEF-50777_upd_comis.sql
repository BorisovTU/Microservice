/* обновление меню под новые пункты */

DECLARE
   v_ParentId NUMBER;
   v_cnt NUMBER := 0;
   v_ID  NUMBER := 0;
BEGIN
   UPDATE DSFCALCAL_DBT
      SET T_SCALETYPE = 1, T_ISBATCHMODE = CHR (88)
    WHERE T_COMMNUMBER = 1063 AND T_FEETYPE = 1;

   INSERT INTO DSFTARSCL_DBT (T_FEETYPE,
                              T_COMMNUMBER,
                              T_ALGKIND,
                              T_ALGNUMBER,
                              T_BEGINDATE,
                              T_ISBLOCKED,
                              T_ENDDATE,
                              T_CONCOMID)
        VALUES (1,
                1063,
                8,
                3,
                TO_DATE ('01/01/2023', 'MM/DD/YYYY'),
                CHR (0),
                TO_DATE ('01/01/0001', 'MM/DD/YYYY'),
                0) RETURNING T_ID INTO v_ID;
                
   INSERT INTO DSFTARIF_DBT (T_TARSCLID,
                             T_SIGN,
                             T_BASETYPE,
                             T_BASESUM,
                             T_TARIFTYPE,
                             T_TARIFSUM,
                             T_MINVALUE,
                             T_MAXVALUE,
                             T_SORT)
        VALUES (v_ID,
                2,
                1,
                0,
                2,
                1000000,
                0,
                3000000,
                5);
        
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/