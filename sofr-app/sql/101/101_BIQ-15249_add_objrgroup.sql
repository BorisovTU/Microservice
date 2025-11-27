BEGIN
    INSERT INTO DOBJGROUP_DBT (T_OBJECTTYPE,
                               T_GROUPID,
                               T_TYPE,
                               T_NAME,
                               T_SYSTEM,
                               T_ORDER,
                               T_MACRONAME,
                               T_KEEPOLDVALUES,
                               T_UPDATEFLAG,
                               T_SUCCESSOPFLAG,
                               T_ATTROBJECTTYPE,
                               T_ATTRGROUPID,
                               T_ISHIDDEN,
                               T_FULLNAMEISBASIC,
                               T_SYSHIDDEN,
                               T_NOTUSEFIELDUSE,
                               T_ISMEANPARENTNODE,
                               T_ISMANUALFIRST,
                               T_COMMENT)
         VALUES (101,
                 117,
                 'X',
                 'Дата вывода облигаций из ДУ',
                 CHR (0),
                 117,
                 CHR (1),
                 'X',
                 0,
                 CHR (0),
                 0,
                 0,
                 CHR (0),
                 CHR (0),
                 CHR (0),
                 CHR (0),
                 CHR (0),
                 CHR (0),
                 CHR (1));
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/

BEGIN
    INSERT INTO DOBJGROUP_DBT (T_OBJECTTYPE,
                               T_GROUPID,
                               T_TYPE,
                               T_NAME,
                               T_SYSTEM,
                               T_ORDER,
                               T_MACRONAME,
                               T_KEEPOLDVALUES,
                               T_UPDATEFLAG,
                               T_SUCCESSOPFLAG,
                               T_ATTROBJECTTYPE,
                               T_ATTRGROUPID,
                               T_ISHIDDEN,
                               T_FULLNAMEISBASIC,
                               T_SYSHIDDEN,
                               T_NOTUSEFIELDUSE,
                               T_ISMEANPARENTNODE,
                               T_ISMANUALFIRST,
                               T_COMMENT)
         VALUES (101,
                 118,
                 'X',
                 'Исключить из расчета связей',
                 CHR (0),
                 117,
                 CHR (1),
                 'X',
                 0,
                 CHR (0),
                 0,
                 0,
                 CHR (0),
                 CHR (0),
                 CHR (0),
                 CHR (0),
                 CHR (0),
                 CHR (0),
                 CHR (1));
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/

DECLARE
    val   VARCHAR2 (10) := 'Да';
BEGIN
    INSERT INTO DOBJATTR_DBT (T_OBJECTTYPE,
                              T_GROUPID,
                              T_ATTRID,
                              T_PARENTID,
                              T_CODELIST,
                              T_NUMINLIST,
                              T_NAMEOBJECT,
                              T_CHATTR,
                              T_LONGATTR,
                              T_INTATTR,
                              T_NAME,
                              T_FULLNAME,
                              T_OPENDATE,
                              T_CLOSEDATE,
                              T_CLASSIFICATOR,
                              T_CORRACTYPE,
                              T_BALANCE,
                              T_ISOBJECT)
         VALUES (101,
                 118,
                 (SELECT NVL (MAX (t_attrid), 0) + 1
                    FROM dobjattr_dbt
                   WHERE t_objecttype = 101 AND t_groupid = 118),
                 0,
                 CHR (1),
                 val,
                 val,
                 '',
                 0,
                 0,
                 val,
                 val,
                 TO_DATE ('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
                 TO_DATE ('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
                 0,
                 +CHR (1),
                 CHR (1),
                 '');
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/

DECLARE
    val   VARCHAR2 (10) := 'Нет';
BEGIN
    INSERT INTO DOBJATTR_DBT (T_OBJECTTYPE,
                              T_GROUPID,
                              T_ATTRID,
                              T_PARENTID,
                              T_CODELIST,
                              T_NUMINLIST,
                              T_NAMEOBJECT,
                              T_CHATTR,
                              T_LONGATTR,
                              T_INTATTR,
                              T_NAME,
                              T_FULLNAME,
                              T_OPENDATE,
                              T_CLOSEDATE,
                              T_CLASSIFICATOR,
                              T_CORRACTYPE,
                              T_BALANCE,
                              T_ISOBJECT)
         VALUES (101,
                 118,
                 (SELECT NVL (MAX (t_attrid), 0) + 1
                    FROM dobjattr_dbt
                   WHERE t_objecttype = 101 AND t_groupid = 118),
                 0,
                 CHR (1),
                 val,
                 val,
                 '',
                 0,
                 0,
                 val,
                 val,
                 TO_DATE ('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
                 TO_DATE ('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
                 0,
                 +CHR (1),
                 CHR (1),
                 '');
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/


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
                            TO_DATE ('31-12-2000', 'DD-MM-YYYY'),  --T_VALIDFROMDATE
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

BEGIN
  UPDATE ddl_tick_dbt
     SET t_ofbu = CHR (88)
   WHERE t_dealtype = 32732;
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/

BEGIN
  COMMIT;
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/
