DECLARE
   v_MarketID     NUMBER := 0;
   v_OfficeID     NUMBER := 0;
BEGIN
   BEGIN
      SELECT T_OBJECTID
        INTO v_MarketID
        FROM DOBJCODE_DBT
       WHERE T_OBJECTTYPE = 3 AND T_CODEKIND = 1 AND T_CODE = 'АО СПВБ';
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         v_MarketID := 0;
   END;

   IF v_MarketID > 0
   THEN
      BEGIN
         SELECT T_OFFICEID
           INTO v_OfficeID
           FROM DPTOFFICE_DBT
          WHERE T_PARTYID = v_MarketID
                AND LOWER (T_OFFICENAME) LIKE '%валютный рынок%';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            v_OfficeID := 0;
      END;

      IF v_OfficeID = 0
      THEN
         INSERT INTO DPTOFFICE_DBT (T_PARTYID,
                                    T_OFFICEID,
                                    T_OFFICECODE,
                                    T_OFFICENAME,
                                    T_DESCRIPTION,
                                    T_KPP,
                                    T_REGNUMBER,
                                    T_REGDATE,
                                    T_DOCKINDID,
                                    T_SERIES,
                                    T_NUMBER,
                                    T_MAINCASHIER,
                                    T_DATECLOSE,
                                    T_POSTADDRESS,
                                    T_STATUSBRANCH,
                                    T_NOAUTO,
                                    T_STRINGFORODBACC,
                                    T_DBACCESS,
                                    T_PLACEMENT,
                                    T_MAINRATE,
                                    T_PHONE,
                                    T_OWNCURRRATE,
                                    T_OWNEXPOPERSET,
                                    T_STATUPREGIME,
                                    T_BANKID,
                                    T_DEPID,
                                    T_CLIENT51,
                                    T_RESERVE)
              VALUES (v_MarketID,
                      1,
                      '1',
                      'Валютный рынок',
                      CHR (1),
                      CHR (1),
                      CHR (1),
                      TRUNC (SYSDATE),
                      0,
                      CHR (1),
                      CHR (1),
                      CHR (1),
                      TO_DATE ('01.01.0001', 'dd.mm.yyyy'),
                      CHR (1),
                      0,
                      CHR (0),
                      CHR (1),
                      0,
                      0,
                      0,
                      CHR (1),
                      CHR (0),
                      CHR (0),
                      CHR (0),
                      0,
                      0,
                      CHR (0),
                      CHR (1));

         v_OfficeID := 1;
      END IF;

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
                   CHR (88),
                   0,
                   0,
                   964,
                   1550,
                   1,
                   '30424810599000000479',
                   0,
                   3,
                   0,
                   0,
                   (SELECT acc.T_OPEN_DATE
                      FROM DACCOUNT_DBT acc
                     WHERE acc.T_ACCOUNT = '30424810599000000479'),
                   TO_DATE ('01.01.0001', 'dd.mm.yyyy'),
                   CHR (88),
                   -1,
                   -1,
                   -1,
                   -1,
                   'А',
                   -1,
                   -1,
                   (SELECT acc.T_OPEN_DATE
                      FROM DACCOUNT_DBT acc
                     WHERE acc.T_ACCOUNT = '30424810599000000479'),
                   -1,
                   -1,
                   -1,
                   v_MarketID,
                   v_OfficeID,
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
END;
/