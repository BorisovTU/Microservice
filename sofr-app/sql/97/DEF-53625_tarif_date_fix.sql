/* починка тарифов */

DECLARE
   v_ID  NUMBER := 0;
BEGIN

   FOR i IN (SELECT DISTINCT T_ID
               FROM DSFTARSCL_DBT
              WHERE T_FEETYPE = 1 AND T_COMMNUMBER = 1063 AND T_ALGKIND = 8)
   LOOP
      DELETE FROM dsfcomtarscl_dbt WHERE t_tarSclID = i.T_ID and t_level = 4;
      DELETE FROM dsftarif_dbt WHERE T_TARSCLID = i.T_ID;
   END LOOP;

   DELETE FROM DSFTARSCL_DBT WHERE T_FEETYPE = 1 AND T_COMMNUMBER = 1063 AND T_ALGKIND = 8;

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
                TO_DATE ('06/01/2022', 'MM/DD/YYYY'),
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

   INSERT INTO dsfcomtarscl_dbt (t_concomid, t_tarSclID, t_level)
   SELECT concom.t_id, v_ID, 4
     FROM dsfconcom_dbt concom
    WHERE     concom.t_objecttype = 659
          AND concom.t_feetype = 1
          AND concom.t_commnumber = 1063
          AND (TO_DATE('01.06.2022','DD.MM.YYYY') BETWEEN concom.t_datebegin
                     AND DECODE (concom.t_dateend, TO_DATE('01.01.0001','DD.MM.YYYY'), TO_DATE('31.12.9999','DD.MM.YYYY'), concom.t_dateend)
               OR TO_DATE('01.01.0001','DD.MM.YYYY') BETWEEN concom.t_datebegin
                        AND DECODE (concom.t_dateend, TO_DATE('01.01.0001','DD.MM.YYYY'), TO_DATE('31.12.9999','DD.MM.YYYY'), concom.t_dateend)
               OR TO_DATE('01.01.0001','DD.MM.YYYY')= TO_DATE('01.01.0001','DD.MM.YYYY'))
          AND NOT EXISTS
                     (SELECT 1
                        FROM DSFCOMTARSCL_DBT comtar
                       WHERE     comtar.t_concomid = concom.t_id
                             AND comtar.t_tarSclID = v_ID
                             AND comtar.t_level = 4);

   UPDATE DSFCALCAL_DBT
      SET T_SCALETYPE = 1, T_ISBATCHMODE = CHR (88)
    WHERE T_COMMNUMBER = 1063 AND T_FEETYPE = 1;

   COMMIT;
        
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/