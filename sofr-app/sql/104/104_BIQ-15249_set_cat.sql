BEGIN
    FOR cData
        IN (SELECT LPAD (tick.t_DealId, 34, '0')     AS T_OBJECT
              FROM ddl_tick_dbt tick
             WHERE     tick.t_dealtype = 32732
                   AND RSB_SECUR.GetMainObjAttrNoDate (
                           RSB_SECUR.OBJTYPE_SECDEAL,
                           LPAD (tick.t_DealId, 34, '0'),
                           118) =
                       0)
    LOOP
        INSERT INTO DOBJATCOR_DBT (T_OBJECTTYPE,
                                   T_GROUPID,
                                   T_ATTRID,
                                   T_OBJECT,
                                   T_GENERAL,
                                   T_VALIDFROMDATE,
                                   T_OPER,
                                   T_VALIDTODATE,
                                   T_SYSDATE,
                                   T_SYSTIME,
                                   T_ISAUTO,
                                   T_ID)
                 VALUES (
                            RSB_SECUR.OBJTYPE_SECDEAL,             --T_OBJECTTYPE
                            118,                                   --T_GROUPID
                            1,                                     --T_ATTRID
                            cData.T_OBJECT,                        --T_OBJECT
                            'X',                                   --T_GENERAL
                            TRUNC (SYSDATE),                       --T_VALIDFROMDATE
                            RsbSessionData.Oper,                   --T_OPER
                            TO_DATE ('31-12-9999', 'DD-MM-YYYY'),  --T_VALIDTODATE
                            TRUNC (SYSDATE),                       --T_SYSDATE
                            TO_DATE (
                                   '01-01-0001:'
                                || TO_CHAR (SYSDATE, 'HH24:MI:SS'),
                                'DD-MM-YYYY:HH24:MI:SS'),          --T_SYSTIME
                            'X',                                   --T_ISAUTO
                            0                                      --T_ID
                             );
    END LOOP;
END;
/