/* починка тарифов */

DECLARE
   v_ID  NUMBER := 0;
BEGIN
   SELECT T_ID INTO v_ID
     FROM DSFTARSCL_DBT
    WHERE T_FEETYPE = 1 AND T_COMMNUMBER = 1063 AND T_ALGKIND = 8;

   INSERT INTO dsfcomtarscl_dbt (t_concomid, t_tarSclID, t_level)
   SELECT concom.t_id, v_ID, 4
     FROM dsfconcom_dbt concom
    WHERE     concom.t_objecttype = 659
          AND concom.t_feetype = 1
          AND concom.t_commnumber = 1063
          AND (TO_DATE('01.01.2023','DD.MM.YYYY') BETWEEN concom.t_datebegin
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
        
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/