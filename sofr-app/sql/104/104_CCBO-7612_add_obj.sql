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
         VALUES (12,
                 131,
                 'X',
                 'Кассовый метод учета доходов',
                 CHR (0),
                 131,
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
         VALUES (12,
                 131,
                 1,
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
         VALUES (12,
                 131,
                 2,
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
END;
/
