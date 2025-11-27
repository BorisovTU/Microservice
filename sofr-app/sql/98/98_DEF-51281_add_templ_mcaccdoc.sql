DECLARE
    v_CatID   NUMBER := 0;
    v_Num     NUMBER := 0;
BEGIN
    BEGIN
        SELECT T_ID
          INTO v_CatID
          FROM DMCCATEG_DBT
         WHERE T_CODE = '+å†‡¶†, àÇ ® Ñå';
    EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
            v_CatID := 0;
    END;

    BEGIN
        SELECT MAX (T_NUMBER)
          INTO v_Num
          FROM DMCTEMPL_DBT
         WHERE T_CATID = v_CatID;
    EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
            v_Num := 0;
    END;

    IF v_CatID > 0 AND v_Num < 29 AND v_Num > 0
    THEN
        INSERT INTO DMCTEMPL_DBT (T_CATID,
                                  T_NUMBER,
                                  T_VALUE1,
                                  T_VALUE2,
                                  T_VALUE3,
                                  T_VALUE4,
                                  T_VALUE5,
                                  T_VALUE6,
                                  T_VALUE7,
                                  T_VALUE8,
                                  T_CHAPTER,
                                  T_BALANCE,
                                  T_MASK,
                                  T_KIND_ACCOUNT,
                                  T_GROUPNUM,
                                  T_HASPERIOD,
                                  T_NOTAUTO,
                                  T_ISCLOSE,
                                  T_ACCOUNTCLIENT,
                                  T_TYPEACCOUNT,
                                  T_RESERVE)
             VALUES (v_CatID,
                     29,
                     1,
                     0,
                     2,
                     10,
                     -1,
                     -1,
                     -1,
                     -1,
                     1,
                     '70601',
                     '26201ZZ',
                     'è',
                     0,
                     CHR (0),
                     'X',
                     CHR (0),
                     0,
                     CHR (1),
                     CHR (1));
    END IF;
EXCEPTION
    WHEN DUP_VAL_ON_INDEX
    THEN
        NULL;
END;
/

DECLARE
    v_CatID   NUMBER := 0;
    v_Num     NUMBER := 0;
BEGIN
    BEGIN
        SELECT T_ID
          INTO v_CatID
          FROM DMCCATEG_DBT
         WHERE T_CODE = '-å†‡¶†, àÇ ® Ñå';
    EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
            v_CatID := 0;
    END;

    BEGIN
        SELECT MAX (T_NUMBER)
          INTO v_Num
          FROM DMCTEMPL_DBT
         WHERE T_CATID = v_CatID;
    EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
            v_Num := 0;
    END;

    IF v_CatID > 0 AND v_Num < 29 AND v_Num > 0
    THEN
        INSERT INTO DMCTEMPL_DBT (T_CATID,
                                  T_NUMBER,
                                  T_VALUE1,
                                  T_VALUE2,
                                  T_VALUE3,
                                  T_VALUE4,
                                  T_VALUE5,
                                  T_VALUE6,
                                  T_VALUE7,
                                  T_VALUE8,
                                  T_CHAPTER,
                                  T_BALANCE,
                                  T_MASK,
                                  T_KIND_ACCOUNT,
                                  T_GROUPNUM,
                                  T_HASPERIOD,
                                  T_NOTAUTO,
                                  T_ISCLOSE,
                                  T_ACCOUNTCLIENT,
                                  T_TYPEACCOUNT,
                                  T_RESERVE)
             VALUES (v_CatID,
                     29,
                     1,
                     0,
                     2,
                     10,
                     -1,
                     -1,
                     -1,
                     -1,
                     1,
                     '70606',
                     '46201ZZ',
                     'Ä',
                     0,
                     CHR (0),
                     'X',
                     CHR (0),
                     0,
                     CHR (1),
                     CHR (1));
    END IF;
EXCEPTION
    WHEN DUP_VAL_ON_INDEX
    THEN
        NULL;
END;
/

DECLARE
    v_Cnt   NUMBER := 0;
BEGIN
    BEGIN
        SELECT COUNT (*)
          INTO v_Cnt
          FROM DMCACCDOC_DBT
         WHERE     T_ACCOUNT = '70601810900002620106'
               AND T_CATID = 466
               AND T_TEMPLNUM = 29
               AND T_ISCOMMON = 'X';
    EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
            v_Cnt := 0;
    END;

    IF v_Cnt = 0
    THEN
        INSERT INTO DMCACCDOC_DBT (T_ID,
                                   T_ISCOMMON,
                                   T_DOCKIND,
                                   T_DOCID,
                                   T_CATID,
                                   T_CATNUM,
                                   T_CHAPTER,
                                   T_ACCOUNT,
                                   T_CURRENCY,
                                   T_TEMPLNUM,
                                   T_GROUPNUM,
                                   T_PERIODID,
                                   T_ACTIVATEDATE,
                                   T_DISABLINGDATE,
                                   T_ISUSABLE,
                                   T_FIID,
                                   T_OWNER,
                                   T_PLACE,
                                   T_ISSUER,
                                   T_KIND_ACCOUNT,
                                   T_CENTR,
                                   T_CENTROFFICE,
                                   T_ACTIONDATE,
                                   T_FIID2,
                                   T_CLIENTCONTRID,
                                   T_BANKCONTRID,
                                   T_MARKETPLACEID,
                                   T_MARKETPLACEOFFICEID,
                                   T_FIROLE,
                                   T_INDEXDATE,
                                   T_DEPARTMENTID,
                                   T_CONTRACTOR,
                                   T_BRANCH,
                                   T_CORRDEPARTMENTID,
                                   T_CURRENCYEQ,
                                   T_CURRENCYEQ_RATETYPE,
                                   T_CURRENCYEQ_RATEDATE,
                                   T_CURRENCYEQ_RATEEXTRA,
                                   T_MCBRANCH)
             VALUES (0,
                     'X',
                     0,
                     0,
                     466,
                     1230,
                     1,
                     '70601810900002620106',
                     0,
                     29,
                     0,
                     0,
                     TO_DATE ('01/01/2019', 'dd/mm/yyyy'),
                     TO_DATE ('01/01/0001', 'dd/mm/yyyy'),
                     'X',
                     -1,
                     -1,
                     -1,
                     -1,
                     'è',
                     -1,
                     -1,
                     TO_DATE ('01/01/2019', 'dd/mm/yyyy'),
                     -1,
                     -1,
                     -1,
                     -1,
                     -1,
                     0,
                     -1,
                     1,
                     -1,
                     1,
                     0,
                     -1,
                     0,
                     0,
                     0,
                     1);
    END IF;
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/

DECLARE
    v_Cnt   NUMBER := 0;
BEGIN
    BEGIN
        SELECT COUNT (*)
          INTO v_Cnt
          FROM DMCACCDOC_DBT
         WHERE     T_ACCOUNT = '70606810000004620106'
               AND T_CATID = 467
               AND T_TEMPLNUM = 29
               AND T_ISCOMMON = 'X';
    EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
            v_Cnt := 0;
    END;

    IF v_Cnt = 0
    THEN
        INSERT INTO DMCACCDOC_DBT (T_ID,
                                   T_ISCOMMON,
                                   T_DOCKIND,
                                   T_DOCID,
                                   T_CATID,
                                   T_CATNUM,
                                   T_CHAPTER,
                                   T_ACCOUNT,
                                   T_CURRENCY,
                                   T_TEMPLNUM,
                                   T_GROUPNUM,
                                   T_PERIODID,
                                   T_ACTIVATEDATE,
                                   T_DISABLINGDATE,
                                   T_ISUSABLE,
                                   T_FIID,
                                   T_OWNER,
                                   T_PLACE,
                                   T_ISSUER,
                                   T_KIND_ACCOUNT,
                                   T_CENTR,
                                   T_CENTROFFICE,
                                   T_ACTIONDATE,
                                   T_FIID2,
                                   T_CLIENTCONTRID,
                                   T_BANKCONTRID,
                                   T_MARKETPLACEID,
                                   T_MARKETPLACEOFFICEID,
                                   T_FIROLE,
                                   T_INDEXDATE,
                                   T_DEPARTMENTID,
                                   T_CONTRACTOR,
                                   T_BRANCH,
                                   T_CORRDEPARTMENTID,
                                   T_CURRENCYEQ,
                                   T_CURRENCYEQ_RATETYPE,
                                   T_CURRENCYEQ_RATEDATE,
                                   T_CURRENCYEQ_RATEEXTRA,
                                   T_MCBRANCH)
             VALUES (0,
                     'X',
                     0,
                     0,
                     467,
                     1231,
                     1,
                     '70606810000004620106',
                     0,
                     29,
                     0,
                     0,
                     TO_DATE ('01/01/2019', 'dd/mm/yyyy'),
                     TO_DATE ('01/01/0001', 'dd/mm/yyyy'),
                     'X',
                     -1,
                     -1,
                     -1,
                     -1,
                     'Ä',
                     -1,
                     -1,
                     TO_DATE ('01/01/2019', 'dd/mm/yyyy'),
                     -1,
                     -1,
                     -1,
                     -1,
                     -1,
                     0,
                     -1,
                     1,
                     -1,
                     1,
                     0,
                     -1,
                     0,
                     0,
                     0,
                     1);
    END IF;
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/