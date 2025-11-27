BEGIN
   INSERT INTO DOBJECTS_DBT (T_OBJECTTYPE,
                             T_NAME,
                             T_CODE,
                             T_USERNUMBER,
                             T_PARENTOBJECTTYPE,
                             T_SERVICEMACRO,
                             T_MODULE)
        VALUES (36,
                'Вид курса фин.инструмента',
                'ВИД_КУРСА',
                0,
                0,
                CHR (1),
                CHR (0));

   INSERT INTO DOBJFEAT_DBT (T_FEATUREKIND, T_OBJECTTYPE)
        VALUES (1, 36);

   INSERT INTO DOBJFEAT_DBT (T_FEATUREKIND, T_OBJECTTYPE)
        VALUES (2, 36);

   INSERT INTO DOBJFEAT_DBT (T_FEATUREKIND, T_OBJECTTYPE)
        VALUES (4, 36);

   COMMIT;
END;
/

BEGIN
   INSERT INTO DNOTEKIND_DBT (T_OBJECTTYPE,
                              T_NOTEKIND,
                              T_NOTETYPE,
                              T_NAME,
                              T_KEEPOLDVALUES,
                              T_NOTINUSE,
                              T_ISPROTECTED,
                              T_MAXLEN,
                              T_NOTUSEFIELDUSE,
                              T_MACRONAME,
                              T_DECPL,
                              T_ISPROGONLY)
           VALUES (
                     36,
                     101,
                     7,
                     'Дата окончания действия значение курса',
                     CHR (88),
                     CHR (0),
                     CHR (0),
                     200,
                     CHR (0),
                     CHR (1),
                     0,
                     CHR (0));

   INSERT INTO DNOTEKIND_DBT (T_OBJECTTYPE,
                              T_NOTEKIND,
                              T_NOTETYPE,
                              T_NAME,
                              T_KEEPOLDVALUES,
                              T_NOTINUSE,
                              T_ISPROTECTED,
                              T_MAXLEN,
                              T_NOTUSEFIELDUSE,
                              T_MACRONAME,
                              T_DECPL,
                              T_ISPROGONLY)
           VALUES (
                     36,
                     102,
                     7,
                     'Количество календарных дней действия значение курса',
                     CHR (88),
                     CHR (0),
                     CHR (0),
                     200,
                     CHR (0),
                     CHR (1),
                     0,
                     CHR (0));

   COMMIT;
END;
/