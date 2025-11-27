CREATE OR REPLACE PACKAGE BODY RSB_IMPORT_DU
IS
    FUNCTION GetDealCode (pDealNum NUMBER)
        RETURN VARCHAR2
        DETERMINISTIC
    IS
        cnt          NUMBER (10) := 0;
        exsistDeal   NUMBER (5) := 0;
        DealNumA     NUMBER (10) := 0;
    BEGIN
        SELECT COUNT (1)
          INTO cnt
          FROM ddl_tick_dbt
         WHERE T_DEALCODE LIKE DealCodeTempl || gl_DateDU || '%';

        LOOP
            cnt := cnt + 1;

            SELECT COUNT (1)
              INTO exsistDeal
              FROM ddl_tick_dbt
             WHERE T_DEALCODE LIKE DealCodeTempl || gl_DateDU || '_' || cnt;

            IF exsistDeal = 0
              THEN
               DealNumA := DealNumA + 1;
            END IF;

            EXIT WHEN (exsistDeal = 0 AND DealNumA = pDealNum);
        END LOOP;

        RETURN DealCodeTempl || gl_DateDU || '_' || cnt;
    END GetDealCode;

    PROCEDURE pumpDataSOFR (DateImportDU IN VARCHAR2)
    IS
    BEGIN
        gl_DateDU := SUBSTR(DateImportDU, 5, 4) || SUBSTR(DateImportDU, 3, 2) || SUBSTR(DateImportDU, 1, 2);

        UPDATE DIMPOPRDU_TMP
           SET t_fiid =
                   (SELECT NVL (t_fiid, -1)
                      FROM DAVOIRISS_DBT
                     WHERE t_isin = DIMPOPRDU_TMP.T_ISIN),
               t_PFI =
                   (SELECT NVL (t_fiid, 0)
                      FROM DFININSTR_DBT
                     WHERE T_ISO_NUMBER = DIMPOPRDU_TMP.T_ISO AND ROWNUM = 1),
               t_dealcode =
                   (SELECT T_DEALCODE
                      FROM DDL_TICK_DBT
                     WHERE DDL_TICK_DBT.T_DEALID =
                           GetDealIDSOFR (DIMPOPRDU_TMP.T_ISIN, DIMPOPRDU_TMP.T_IMPORTID, DateImportDU)),
               t_dealID = GetDealIDSOFR (DIMPOPRDU_TMP.T_ISIN, DIMPOPRDU_TMP.T_IMPORTID, DateImportDU)
         WHERE GetDealIDSOFR (DIMPOPRDU_TMP.T_ISIN, DIMPOPRDU_TMP.T_IMPORTID, DateImportDU) > 0;

        UPDATE DIMPOPRDU_TMP
           SET t_fiid =
                   (SELECT NVL (t_fiid, -1)
                      FROM DAVOIRISS_DBT
                     WHERE t_isin = DIMPOPRDU_TMP.T_ISIN),
               t_PFI =
                   (SELECT NVL (t_fiid, 0)
                      FROM DFININSTR_DBT
                     WHERE T_ISO_NUMBER = DIMPOPRDU_TMP.T_ISO AND ROWNUM = 1),
               t_dealcode = RSB_IMPORT_DU.GetDealCode (ROWNUM)
         WHERE t_dealID = 0;

        COMMIT;
    END;


    FUNCTION GetSUMNDKCFI (NKD        IN NUMBER,
                           NKDFIID    IN NUMBER,
                           CFI        IN NUMBER,
                           DealDate   IN DATE)
        RETURN NUMBER
    IS
        NDKSUMCFI    NUMBER (16, 2) := 0;
        pRate        NUMBER (32, 12) := 0;
        pScale       NUMBER (32, 12) := 0;
        pPoint       NUMBER (32, 12) := 0;
        pRateType    NUMBER (10) := -2;
        pIsInverse   VARCHAR2 (3);
    BEGIN
        NDKSUMCFI :=
            RSI_RSB_FIInstr.ConvSum2 (NKD,
                                      NKDFIID,
                                      CFI,
                                      DealDate,
                                      0,
                                      pRateType,
                                      pRate,
                                      pScale,
                                      pPoint,
                                      pIsInverse);
        RETURN NDKSUMCFI;
    END;

    FUNCTION GetDealIDSOFR (ISIN IN VARCHAR2, ImportID IN VARCHAR2, DateImportDU IN VARCHAR2)
        RETURN NUMBER
        DETERMINISTIC
    IS
        DealID   NUMBER (10) := 0;
    BEGIN
        SELECT NVL (tick.T_DEALID, 0)
          INTO DealID
          FROM ddl_tick_dbt tick, DAVOIRISS_DBT av
         WHERE     tick.T_DEALTYPE = 2011
               AND tick.T_PFI = av.T_FIID
               AND av.T_ISIN = ISIN
               AND tick.t_Ofbu = CHR (88)
               AND RSB_SECUR.GetObjAttrName (RSB_SECUR.OBJTYPE_SECDEAL,
                                             117, /*Дата вывода облигаций из ДУ*/
                                             RSB_SECUR.GetMainObjAttr (
                                                 RSB_SECUR.OBJTYPE_SECDEAL,
                                                 LPAD (tick.t_DealId, 34, '0'),
                                                 117, /*Дата вывода облигаций из ДУ*/
                                                 TRUNC (SYSDATE))) = DateImportDU
               AND pm_common.GetNoteTextStr(LPAD (tick.t_DealId, 34, '0'), 101, 39, TRUNC (SYSDATE)) = ImportID 
               AND ROWNUM = 1;

        RETURN DealID;
    END;

    PROCEDURE updateSOFRdeals
    IS
        CURSOR c_IMPOPRTDU IS
            SELECT *
              FROM DIMPOPRDU_TMP
             WHERE DIMPOPRDU_TMP.t_dealid <> 0;

        cnt   NUMBER (10) := 0;
    BEGIN
        FOR v_IMPOPRTDU IN c_IMPOPRTDU
        LOOP
            UPDATE DDL_TICK_DBT
               SET T_DEALDATE = v_IMPOPRTDU.t_Dealdate,
                   T_REGDATE = v_IMPOPRTDU.t_Transferdate,
                   T_TAXOWNBEGDATE = v_IMPOPRTDU.t_Dealdate,
                   T_DEALSTATUS = 20,
                   T_FLAG3 = CHR (88)
             WHERE DDL_TICK_DBT.t_dealid = v_IMPOPRTDU.t_Dealid;

            UPDATE DDL_LEG_DBT
               SET T_START = v_IMPOPRTDU.t_Transferdate,
                   T_MATURITY = v_IMPOPRTDU.t_Transferdate,
                   T_EXPIRY = v_IMPOPRTDU.t_Transferdate,
                   T_PRICE = v_IMPOPRTDU.t_Pricenom,
                   T_RELATIVEPRICE = chr(88),
                   T_PRINCIPAL = v_IMPOPRTDU.t_Amountdu,
                   T_COST =
                       (  v_IMPOPRTDU.t_Pricenom
                        * v_IMPOPRTDU.t_Transfernom
                        * v_IMPOPRTDU.t_Amountdu
                        / 100),
                   T_NKD = v_IMPOPRTDU.t_Nkd,
                   T_TOTALCOST =
                       (    v_IMPOPRTDU.t_Pricenom
                          * v_IMPOPRTDU.t_Transfernom
                          / 100
                        + GetSUMNDKCFI (v_IMPOPRTDU.T_NKD, 0, 6, v_IMPOPRTDU.T_DealDate))
             WHERE DDL_LEG_DBT.t_dealid = v_IMPOPRTDU.t_Dealid;

            UPDATE DDLRQ_DBT
               SET T_AMOUNT = v_IMPOPRTDU.t_Amountdu,
                   T_FACTDATE = v_IMPOPRTDU.t_Transferdate,
                   T_PLANDATE = v_IMPOPRTDU.t_Transferdate,
                   T_CHANGEDATE = v_IMPOPRTDU.t_Transferdate,
                   T_FACTAMOUNT = v_IMPOPRTDU.t_Amountdu,
                   T_STATE = 2
             WHERE DDLRQ_DBT.T_DOCID = v_IMPOPRTDU.t_Dealid;

            UPDATE ddlsum_dbt
               SET t_sum =
                       (v_IMPOPRTDU.t_Pricenom * v_IMPOPRTDU.t_Transfernom / 100)
             WHERE     t_dockind = 127
                   AND t_kind = 1220
                   AND t_docid = v_IMPOPRTDU.t_Dealid;

            UPDATE ddlsum_dbt
               SET t_sum =
                       (  v_IMPOPRTDU.t_Pricenom
                        * v_IMPOPRTDU.t_Transfernom
                        * v_IMPOPRTDU.t_Amountdu
                        / 100)
             WHERE     t_dockind = 127
                   AND t_kind = 1230
                   AND t_docid = v_IMPOPRTDU.t_Dealid;

            UPDATE ddlsum_dbt
               SET t_sum = v_IMPOPRTDU.t_Nkd
             WHERE     t_dockind = 127
                   AND t_kind = 1240
                   AND t_docid = v_IMPOPRTDU.t_Dealid;

            SELECT COUNT (1)
              INTO cnt
              FROM ddlsum_dbt
             WHERE     t_docid = v_IMPOPRTDU.t_Dealid
                   AND T_DOCKIND = 127
                   AND t_kind = 1100;

            IF cnt = 0
            THEN
                INSERT INTO ddlsum_dbt (t_Dockind, t_Docid, t_kind, t_date, t_sum, t_nds, t_currency, t_fiid)
                     VALUES (127, v_IMPOPRTDU.t_Dealid, 1100, v_IMPOPRTDU.t_Transferdate, v_IMPOPRTDU.t_Sumother, 0, 4, -1);
            ELSE
                UPDATE ddlsum_dbt
                   SET t_sum = v_IMPOPRTDU.t_Sumother
                 WHERE     t_dockind = 127
                       AND t_kind = 1100
                       AND t_docid = v_IMPOPRTDU.t_Dealid;
            END IF;

            COMMIT;
        END LOOP;
    END;

    PROCEDURE TXCreateLotsForDUImport
    IS
        TYPE T_MESSAGE_t IS TABLE OF VARCHAR2 (1024);

        T_MESSAGE   T_MESSAGE_t := T_MESSAGE_t ();
    BEGIN
        FOR cData
            IN (  SELECT DIMPOPRDU_TMP.T_FIID,
                         MIN (DIMPOPRDU_TMP.T_DEALDATE)     AS t_date
                    FROM DIMPOPRDU_TMP
                GROUP BY T_FIID)
        LOOP
            rsb_sctx.TXCreateLots (cData.t_date,
                                   TRUNC (SYSDATE) - 1,
                                   -1,
                                   cData.T_FIID,
                                   0,
                                   0,
                                   1);

            FOR csctxmes IN (SELECT *
                               FROM dsctxmes_dbt
                              WHERE t_fiid <> -1)
            LOOP
                T_MESSAGE.EXTEND;
                T_MESSAGE (T_MESSAGE.LAST) :=
                       csctxmes.t_message
                    || ' для ЦБ с идентификатором: '
                    || csctxmes.t_fiid;
            END LOOP;
        END LOOP;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE DSCTXMES_DBT';

        IF T_MESSAGE IS NOT EMPTY
        THEN
            FORALL i IN T_MESSAGE.FIRST .. T_MESSAGE.LAST
                INSERT INTO dsctxmes_dbt (T_MESSAGE)
                     VALUES (T_MESSAGE (i));

            T_MESSAGE.delete;
        END IF;
    END;
END RSB_IMPORT_DU;
