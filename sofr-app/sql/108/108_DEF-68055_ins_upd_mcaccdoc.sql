DECLARE
    vDocKind   NUMBER := 199;
    PROCEDURE LinkAccDoc (vDocID     IN NUMBER,
                            vCatID     IN NUMBER,
                            vAccount   IN VARCHAR2,
                            vPeriod    IN NUMBER,
                            vADate     IN DATE,
                            vDDate     IN DATE,
                            vActDate   IN DATE,
                            vContr     IN NUMBER)
    IS
        v_ID   NUMBER := 0;
    BEGIN
        BEGIN
            SELECT T_ID
              INTO v_ID
              FROM DMCACCDOC_DBT
             WHERE     T_ACCOUNT = vAccount
                   AND T_DOCID = vDocID
                   AND T_DOCKIND = vDocKind;
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                v_ID := 0;
        END;

        IF v_ID > 0
        THEN
            UPDATE DMCACCDOC_DBT
               SET T_PERIODID = vPeriod, T_DISABLINGDATE = vDDate, T_TEMPLNUM = 1
             WHERE T_ID = v_ID;
        ELSE
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
                SELECT 0,
                       CHR (0),
                       vDocKind,
                       vDocID,
                       vCatID,
                       CASE WHEN vCatID = 461 THEN 1225 ELSE 1224 END,
                       4,
                       vAccount,
                       0,
                       1,
                       6,
                       vPeriod,
                       vADate,
                       vDDate,
                       CHR (0),
                       mc_oposit.T_FIID,
                       mc_oposit.T_OWNER,
                       mc_oposit.T_PLACE,
                       mc_oposit.T_ISSUER,
                       CASE WHEN vCatID = 461 THEN 'è' ELSE 'Ä' END,
                       mc_oposit.T_CENTR,
                       mc_oposit.T_CENTROFFICE,
                       vActDate,
                       mc_oposit.T_FIID2,
                       mc_oposit.T_CLIENTCONTRID,
                       mc_oposit.T_BANKCONTRID,
                       mc_oposit.T_MARKETPLACEID,
                       mc_oposit.T_MARKETPLACEOFFICEID,
                       CASE WHEN vCatID = 461 THEN 31 ELSE 30 END,
                       mc_oposit.T_INDEXDATE,
                       mc_oposit.T_DEPARTMENTID,
                       vContr,
                       mc_oposit.T_BRANCH,
                       mc_oposit.T_CORRDEPARTMENTID,
                       mc_oposit.T_CURRENCYEQ,
                       mc_oposit.T_CURRENCYEQ_RATETYPE,
                       mc_oposit.T_CURRENCYEQ_RATEDATE,
                       mc_oposit.T_CURRENCYEQ_RATEEXTRA,
                       mc_oposit.T_MCBRANCH
                  FROM (SELECT *
                          FROM (  SELECT mcacc.*
                                    FROM DMCACCDOC_DBT mcacc
                                   WHERE     mcacc.T_DOCID = vDocID
                                         AND mcacc.T_DOCKIND = vDocKind
                                         AND mcacc.T_CATID =
                                             CASE
                                                 WHEN vCatID = 461 THEN 460
                                                 ELSE 461
                                             END
                                ORDER BY mcacc.T_ACTIVATEDATE)
                         WHERE ROWNUM = 1) mc_oposit;
        END IF;
    END;
BEGIN
/*C_and_F:68A*/
    LinkAccDoc (1530604, 461, '96305810099008000013', 47, to_date('04.12.2023', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),to_date('04.12.2023', 'dd.mm.yyyy'),132094);
    LinkAccDoc (1530604, 461, '96304810099008000013', 46, to_date('06.03.2024', 'dd.mm.yyyy'),to_date('06.05.2024', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),132094);
    LinkAccDoc (1530604, 461, '96303810099008000013', 45, to_date('06.05.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),to_date('05.05.2024', 'dd.mm.yyyy'),132094);
    LinkAccDoc (1530604, 461, '96302810099008001013', 44, to_date('28.05.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),132094);
    LinkAccDoc (1530604, 461, '96301810099008111113', 43, to_date('03.06.2024', 'dd.mm.yyyy'),to_date('04.06.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),132094);
    
/*C_and_F:73A*/
    LinkAccDoc (1530605, 461, '96305810099008000012', 47, to_date('04.12.2023', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),to_date('04.12.2023', 'dd.mm.yyyy'),337563);
    LinkAccDoc (1530605, 461, '96304810099008000012', 46, to_date('06.03.2024', 'dd.mm.yyyy'),to_date('06.05.2024', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),337563);
    LinkAccDoc (1530605, 461, '96303810099008000012', 45, to_date('06.05.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),to_date('05.05.2024', 'dd.mm.yyyy'),337563);
    LinkAccDoc (1530605, 461, '96302810099008001012', 44, to_date('28.05.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),337563);
    LinkAccDoc (1530605, 461, '96301810099008111112', 43, to_date('03.06.2024', 'dd.mm.yyyy'),to_date('04.06.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),337563);
    
/*C_and_F:76A*/
    LinkAccDoc (1530606, 461, '96305810099008000111', 47, to_date('04.12.2023', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),to_date('04.12.2023', 'dd.mm.yyyy'),337564);
    LinkAccDoc (1530606, 461, '96304810099008001111', 46, to_date('06.03.2024', 'dd.mm.yyyy'),to_date('06.05.2024', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),337564);
    LinkAccDoc (1530606, 461, '96303810099008000111', 45, to_date('06.05.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),to_date('05.05.2024', 'dd.mm.yyyy'),337564);
    LinkAccDoc (1530606, 461, '96302810099008001111', 44, to_date('28.05.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),337564);
    LinkAccDoc (1530606, 461, '96301810199008000111', 43, to_date('03.06.2024', 'dd.mm.yyyy'),to_date('04.06.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),337564);
    
/*C_and_F:79A*/
    LinkAccDoc (1530607, 461, '96305810099008000019', 47, to_date('04.12.2023', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),to_date('04.12.2023', 'dd.mm.yyyy'),337565);
    LinkAccDoc (1530607, 461, '96304810099008000019', 46, to_date('06.03.2024', 'dd.mm.yyyy'),to_date('06.05.2024', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),337565);
    LinkAccDoc (1530607, 461, '96303810099008000019', 45, to_date('06.05.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),to_date('05.05.2024', 'dd.mm.yyyy'),337565);
    LinkAccDoc (1530607, 461, '96302810099008001119', 44, to_date('28.05.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),337565);
    LinkAccDoc (1530607, 461, '96301810199008001119', 43, to_date('03.06.2024', 'dd.mm.yyyy'),to_date('04.06.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),337565);


/*C_and_F:85A*/
    LinkAccDoc (1530608, 461, '96305810099008001010', 47, to_date('04.12.2023', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),to_date('04.12.2023', 'dd.mm.yyyy'),132019);
    LinkAccDoc (1530608, 461, '96304810099008001010', 46, to_date('06.03.2024', 'dd.mm.yyyy'),to_date('06.05.2024', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),132019);
    LinkAccDoc (1530608, 461, '96303810099008001010', 45, to_date('06.05.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),to_date('05.05.2024', 'dd.mm.yyyy'),132019);
    LinkAccDoc (1530608, 461, '96302810099008001010', 44, to_date('28.05.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),132019);
    LinkAccDoc (1530608, 461, '96301810199008000010', 43, to_date('03.06.2024', 'dd.mm.yyyy'),to_date('04.06.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),132019);

/*C_and_F:72A*/
    LinkAccDoc (1530609, 460, '93305810099008000024', 47, to_date('04.12.2023', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),to_date('04.12.2023', 'dd.mm.yyyy'),810);
    LinkAccDoc (1530609, 460, '93304810099008000024', 46, to_date('06.03.2024', 'dd.mm.yyyy'),to_date('06.05.2024', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),810);
    LinkAccDoc (1530609, 460, '93303810099008000024', 45, to_date('06.05.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),to_date('05.05.2024', 'dd.mm.yyyy'),810);
    LinkAccDoc (1530609, 460, '93302810099008000024', 44, to_date('28.05.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),810);
    LinkAccDoc (1530609, 460, '93301810199008000024', 43, to_date('03.06.2024', 'dd.mm.yyyy'),to_date('04.06.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),810);

/*C_and_F:82A*/
    LinkAccDoc (1530613, 461, '96305810099008000018', 47, to_date('04.12.2023', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),to_date('04.12.2023', 'dd.mm.yyyy'),337567);
    LinkAccDoc (1530613, 461, '96304810099008000018', 46, to_date('06.03.2024', 'dd.mm.yyyy'),to_date('06.05.2024', 'dd.mm.yyyy'),to_date('06.03.2024', 'dd.mm.yyyy'),337567);
    LinkAccDoc (1530613, 461, '96303810099008000018', 45, to_date('06.05.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),to_date('05.05.2024', 'dd.mm.yyyy'),337567);
    LinkAccDoc (1530613, 461, '96302810099008001118', 44, to_date('28.05.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),to_date('28.05.2024', 'dd.mm.yyyy'),337567);
    LinkAccDoc (1530613, 461, '96301810199008000018', 43, to_date('03.06.2024', 'dd.mm.yyyy'),to_date('04.06.2024', 'dd.mm.yyyy'),to_date('03.06.2024', 'dd.mm.yyyy'),337567);

/*C_and_F:91A*/
    LinkAccDoc (1552253, 461, '96305810099008000017', 47, to_date('04.12.2023', 'dd.mm.yyyy'),to_date('18.03.2024', 'dd.mm.yyyy'),to_date('04.12.2023', 'dd.mm.yyyy'),337564);
    LinkAccDoc (1552253, 461, '96304810099008000017', 46, to_date('18.03.2024', 'dd.mm.yyyy'),to_date('15.05.2024', 'dd.mm.yyyy'),to_date('16.03.2024', 'dd.mm.yyyy'),337564);

/*C_and_F:90A*/
    LinkAccDoc (1552254, 461, '96305810099008002254', 47, to_date('04.12.2023', 'dd.mm.yyyy'),to_date('18.03.2024', 'dd.mm.yyyy'),to_date('04.12.2023', 'dd.mm.yyyy'),132019);
    LinkAccDoc (1552254, 461, '96304810099008002254', 46, to_date('18.03.2024', 'dd.mm.yyyy'),to_date('15.05.2024', 'dd.mm.yyyy'),to_date('16.03.2024', 'dd.mm.yyyy'),132019);

/*C_and_F:94A*/
    LinkAccDoc (1552595, 460, '93305810099008552595', 47, to_date('04.12.2023', 'dd.mm.yyyy'),to_date('18.03.2024', 'dd.mm.yyyy'),to_date('04.12.2023', 'dd.mm.yyyy'),810);
    LinkAccDoc (1552595, 460, '93304810099008552595', 46, to_date('18.03.2024', 'dd.mm.yyyy'),to_date('15.05.2024', 'dd.mm.yyyy'),to_date('16.03.2024', 'dd.mm.yyyy'),810);

COMMIT;
END;
/