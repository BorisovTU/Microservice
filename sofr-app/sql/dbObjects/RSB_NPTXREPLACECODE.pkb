CREATE OR REPLACE PACKAGE BODY RSB_NPTXREPLACECODE
IS
    PARALLEL_LEVEL   CONSTANT NUMBER (5) := 8;
    ExecPackSize     CONSTANT NUMBER (5) := 100;

    PROCEDURE InsertClients (pSessionID IN VARCHAR2)
    IS
    BEGIN
        INSERT /*+ parallel(6) enable_parallel_dml */
               INTO DNPTXGenReplaceOp_DBT (t_partyid,
                                           t_sessionid,
                                           t_state,
                                           t_PACKNUM)
            (SELECT t.t_partyid,
                    pSessionID,
                    STATE_OPEN,
                    TO_NUMBER (ROUND (ROWNUM / ExecPackSize, 0) + 1)
               FROM dparty_dbt t LEFT JOIN dpersn_dbt p ON p.t_personid = t.t_partyid
              WHERE t.t_legalform = 2 OR (t.t_legalform = 1 AND p.t_isemployer = CHR (88)));

        COMMIT;
    END;

    FUNCTION ConvertDateTimeToDay (pDate in DATE, pTime in DATE)
        RETURN NUMBER
    IS
        v_days   NUMBER;
    BEGIN
        v_days := pDate - to_date('01.01.1970','DD.MM.YYYY');
        
        if(pTime - to_date('01.01.0001','DD.MM.YYYY') < 1) THEN -- Может быть заданно только время, а может быть еще и дата
          v_days := v_days + (pTime - to_date('01.01.0001','DD.MM.YYYY'));
        else
          v_days := v_days + (pTime - pDate);-- Если в pTime есть дата считаем ее равной pDate
        end if;
        
        RETURN ROUND(v_days, 6);
    END;

 PROCEDURE AddSaleDealCat (pID DNPTXOP_DBT.T_ID%TYPE) 
  IS
  BEGIN
    INSERT INTO DOBJATCOR_DBT (T_OBJECTTYPE,
                               T_GROUPID,
                               T_ATTRID,
                               T_OBJECT,
                               T_GENERAL,
                               T_VALIDFROMDATE,
                               T_OPER,
                               T_VALIDTODATE,
                               T_ISAUTO)
    VALUES (188,
            1,
            1,
            LPAD(pID, 34, '0'),
            'X',
            RSBSESSIONDATA.CURDATE,
            RSBSESSIONDATA.OPER,
            TO_DATE('31.12.9999', 'DD.MM.YYYY'),
            'X');
  END;

  PROCEDURE AddChangeCodeIncomeCat (pID DNPTXOP_DBT.T_ID%TYPE) 
  IS
  BEGIN
    INSERT INTO DOBJATCOR_DBT (T_OBJECTTYPE,
                               T_GROUPID,
                               T_ATTRID,
                               T_OBJECT,
                               T_GENERAL,
                               T_VALIDFROMDATE,
                               T_OPER,
                               T_VALIDTODATE,
                               T_ISAUTO)
    VALUES (188,
            2,
            1,
            LPAD(pID, 34, '0'),
            'X',
            RSBSESSIONDATA.CURDATE,
            RSBSESSIONDATA.OPER,
            TO_DATE('31.12.9999', 'DD.MM.YYYY'),
            'X');
  END;

    FUNCTION GetContrByType (pClientID     IN NUMBER,
                             pOpenContr    IN CHAR,
                             pCloseContr   IN CHAR,
                             pIIS          IN CHAR,
                             pTaxPeriod    IN NUMBER)
        RETURN NUMBER
    IS
        v_SfContrID     NUMBER;
        v_SfContrDate   DATE;
    BEGIN
          SELECT T_ID, T_DateClose
            INTO v_SfContrID, v_SfContrDate
            FROM ( select T_ID, T_DateClose FROM DSFCONTR_DBT
           WHERE     T_PartyID = pClientID AND T_ServKind in (1,15,7) 
                 AND (   (pIIS = CHR (88) AND RSI_NPTO.CheckContrIIS (T_ID) = 1)
                      OR (pIIS <> CHR (88) AND RSI_NPTO.CheckContrIIS (T_ID) = 0))
        ORDER BY T_DateClose) WHERE ROWNUM = 1;

        IF (pOpenContr = CHR (88)) AND (v_SfContrDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY')) THEN
            RETURN v_SfContrID;
        ELSIF     (pCloseContr = CHR (88))
              AND (v_SfContrDate <> TO_DATE ('01.01.0001', 'DD.MM.YYYY'))
              AND (v_SfContrDate >= TO_DATE ('01.01.' || pTaxPeriod || '', 'DD.MM.YYYY'))
              AND (v_SfContrDate <= TO_DATE ('31.12.' || pTaxPeriod || '', 'DD.MM.YYYY')) THEN
            RETURN v_SfContrID;
        ELSE
            RETURN 0;
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 0;
    END;

    FUNCTION GetOprDate (pClientID     IN NUMBER,
                         pOpenContr    IN CHAR,
                         pCloseContr   IN CHAR,
                         pIIS          IN CHAR,
                         pEndDate      IN DATE,
                         pTaxPeriod    IN NUMBER)
        RETURN DATE
    IS
        v_DateLastCalc   DATE := TO_DATE ('01010001', 'DDMMYYYY');
        v_Contr          NUMBER := 0;
        v_SfContrDate    DATE;
    BEGIN
        v_Contr :=
            GetContrByType (pClientID,
                            pOpenContr,
                            pCloseContr,
                            pIIS,
                            pTaxPeriod);

        IF (v_Contr <> 0) THEN
            SELECT T_DateClose
              INTO v_SfContrDate
              FROM DSFCONTR_DBT
             WHERE t_id = v_Contr;
        END IF;

        IF v_Contr = 0 THEN
            RETURN v_DateLastCalc;
        ELSIF (pOpenContr = CHR (88)) AND (v_SfContrDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY')) THEN
            IF ( (pEndDate is null) or (pEndDate < TO_DATE ('01.01.2000', 'DD.MM.YYYY'))) THEN
                IF pTaxPeriod = EXTRACT (YEAR FROM RSBSESSIONDATA.curdate) THEN
                    RETURN RSBSESSIONDATA.curdate;
                ELSE
                    RETURN TO_DATE ('3112' || pTaxPeriod || '', 'DDMMYYYY');
                END IF;
            ELSE
                RETURN  pEndDate;
            END IF;
        ELSIF     (pCloseContr = CHR (88))
              AND (v_SfContrDate <> TO_DATE ('01.01.0001', 'DD.MM.YYYY'))
              AND (v_SfContrDate >= TO_DATE ('01.01.' || pTaxPeriod || '', 'DD.MM.YYYY'))
              AND (v_SfContrDate <= TO_DATE ('31.12.' || pTaxPeriod || '', 'DD.MM.YYYY')) THEN
            SELECT NVL(MAX (op.t_OperDate), TO_DATE ('01010001', 'DDMMYYYY'))
              INTO v_DateLastCalc
              FROM dnptxop_dbt op
             WHERE     op.t_DocKind = 4605
                   AND op.t_SubKind_Operation IN (10, 50)
                   AND op.t_client = pClientID
                   AND OP.T_OPERDATE <= TO_DATE ('31.12.' || pTaxPeriod || '', 'DD.MM.YYYY')
                   AND OP.T_OPERDATE >= TO_DATE ('01.01.' || pTaxPeriod || '', 'DD.MM.YYYY');

            IF (v_DateLastCalc <> TO_DATE ('01010001', 'DDMMYYYY')) THEN
                RETURN v_DateLastCalc;
            ELSE
                IF pTaxPeriod = EXTRACT (YEAR FROM RSBSESSIONDATA.curdate) THEN
                    RETURN RSBSESSIONDATA.curdate;
                ELSE
                    RETURN TO_DATE ('3112' || pTaxPeriod || '', 'DDMMYYYY');
                END IF;
            END IF;
        END IF;

        RETURN v_DateLastCalc;
    END;

    PROCEDURE pumpData (pSessionID    IN VARCHAR2,
                        pClientType   IN NUMBER,
                        pOpenContr    IN CHAR,
                        pCloseContr   IN CHAR,
                        pIIS          IN CHAR,
                        pEndDate      IN DATE,
                        pTaxPeriod    IN NUMBER)
    IS
        SQL_ST     VARCHAR2 (5000);
        SQL_STW    VARCHAR2 (5000);
        l_try      NUMBER;
        l_status   NUMBER;
        l_task     VARCHAR2 (128) := '';
        v_schema   VARCHAR2 (1024);
    BEGIN
        UPDATE /*+ parallel(6) enable_parallel_dml */
               DNPTXGenReplaceOp_DBT grop
           SET t_PartyID =
                   (CASE
                        WHEN pClientType = CLIENT_ID_TYPE_CFT THEN
                            NVL (
                                (SELECT CODE.T_OBJECTID
                                   FROM DOBJCODE_DBT CODE
                                  WHERE     CODE.T_CODE = grop.t_CFTID
                                        AND CODE.T_OBJECTTYPE = 3
                                        AND CODE.T_CODEKIND = 101
                                        AND CODE.t_state = 0),
                                0)
                        ELSE
                            grop.t_PartyID
                    END),
               t_CFTID =
                   (CASE
                        WHEN pClientType = CLIENT_ID_TYPE_SOFR THEN
                            NVL (
                                (SELECT CODE.T_CODE
                                   FROM DOBJCODE_DBT CODE
                                  WHERE     CODE.T_OBJECTID = grop.t_PartyID
                                        AND CODE.T_OBJECTTYPE = 3
                                        AND CODE.T_CODEKIND = 101
                                        AND CODE.t_state = 0),
                                0)
                        ELSE
                            grop.t_CFTID
                    END)
         WHERE t_SessionID = pSessionID;

        COMMIT;

        SQL_ST :=
               'UPDATE DNPTXGENREPLACEOP_DBT 
           SET t_ContrID =
                   RSB_NPTXREPLACECODE.GetContrByType (t_PartyID,
                                   '''
            || pOpenContr
            || ''',
                                   '''
            || pCloseContr
            || ''',
                                   '''
            || pIIS
            || ''',
                                   '''
            || pTaxPeriod
            || '''),
               t_OprDate =
                   RSB_NPTXREPLACECODE.GetOprDate (t_PartyID,
                               '''
            || pOpenContr
            || ''',
                               '''
            || pCloseContr
            || ''',
                               '''
            || pIIS
            || ''',
                               TO_DATE('''
            || pEndDate
            || '''),
                               '''
            || pTaxPeriod
            || ''') 
                               
           where t_SessionID = ''' 
            || pSessionID
            || ''' and rowid BETWEEN :start_id AND :end_id ';


        SELECT DBMS_PARALLEL_EXECUTE.generate_task_name INTO l_task FROM DUAL;

        DBMS_PARALLEL_EXECUTE.CREATE_TASK (l_task);

        SELECT username INTO v_schema FROM user_users;

        DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID (l_task,
                                                      v_schema,
                                                      'DNPTXGENREPLACEOP_DBT',
                                                      TRUE,
                                                      ExecPackSize);


        DBMS_PARALLEL_EXECUTE.RUN_TASK (l_task,
                                        SQL_ST,
                                        DBMS_SQL.NATIVE,
                                        parallel_level   => PARALLEL_LEVEL);

        L_try := 0;
        L_status := DBMS_PARALLEL_EXECUTE.TASK_STATUS (l_task);

        WHILE (l_try < 2 AND L_status != DBMS_PARALLEL_EXECUTE.FINISHED)
        LOOP
            L_try := l_try + 1;
            DBMS_PARALLEL_EXECUTE.RESUME_TASK (l_task);
            L_status := DBMS_PARALLEL_EXECUTE.TASK_STATUS (l_task);
        END LOOP;

        DBMS_PARALLEL_EXECUTE.DROP_TASK (l_task);

        COMMIT;
    END;

    PROCEDURE CheckClient (pPackNum       IN NUMBER,
                           pEndID         IN NUMBER,
                           pSessionID     IN VARCHAR2,
                           pClientGroup   IN NUMBER DEFAULT 0,
                           pClientType    IN NUMBER DEFAULT 0,
                           pOpenContr     IN CHAR,
                           pCloseContr    IN CHAR,
                           pSOFR          IN CHAR,
                           pDiasoft       IN CHAR,
                           pBeginDate     IN DATE,
                           pEndDate       IN DATE)
    IS
        v_ExistGenOpCode    VARCHAR2 (25);
        v_ExistGenOpDate    DATE := TO_DATE ('01010001', 'DDMMYYYY');
        v_ExistDCDRECORDS   NUMBER := 0;
        v_ExistParty        NUMBER := 0;
    BEGIN
        FOR GenData
            IN (SELECT *
                  FROM DNPTXGenReplaceOp_DBT
                 WHERE t_SessionID = pSessionID AND t_PackNum = pPackNum AND t_state = STATE_OPEN)
        LOOP
            IF (pClientGroup = CLIENT_FROM_FILE) THEN
                SELECT count(1)
                  INTO v_ExistParty
                  FROM dparty_dbt pt
                 WHERE pt.t_partyid = GenData.t_partyid;

                IF ((pClientType = CLIENT_ID_TYPE_SOFR) AND (0 = v_ExistParty)) THEN
                    UPDATE DNPTXGenReplaceOp_DBT gdt
                       SET gdt.t_state = STATE_ERR,
                           gdt.t_error = 'Некорректный ID СОФР клиента'
                     WHERE GenData.t_id = gdt.t_id;

                    CONTINUE;
                ELSIF ((pClientType = CLIENT_ID_TYPE_CFT) AND (v_ExistParty = 0)) THEN
                    UPDATE DNPTXGenReplaceOp_DBT gdt
                       SET gdt.t_state = STATE_ERR,
                           gdt.t_error = 'Некорректный ID ЦФТ клиента'
                     WHERE GenData.t_id = gdt.t_id;

                    CONTINUE;
                END IF;
            END IF;


            SELECT COUNT (1)
              INTO v_ExistParty
              FROM dparty_dbt t, dpersn_dbt p
             WHERE     t.t_partyid = GenData.t_partyid
                   AND p.t_personid = t.t_partyid
                   AND (t.t_legalform = 2 OR (t.t_legalform = 1 AND p.t_isemployer = CHR (88)));

            IF (0 = v_ExistParty) THEN
                UPDATE DNPTXGenReplaceOp_DBT gdt
                   SET gdt.t_state = STATE_ERR,
                       gdt.t_error =
                           'Недопустимая форма субъекта - ЮЛ/другое'
                 WHERE GenData.t_id = gdt.t_id;

                CONTINUE;
            END IF;


            IF (GenData.t_ContrID = 0) THEN
                IF ((pOpenContr = CHR (88)) AND (pCloseContr <> CHR (88))) THEN
                    UPDATE DNPTXGenReplaceOp_DBT gdt
                       SET gdt.t_state = STATE_ERR,
                           gdt.t_error =
                               'Отсутствуют открытые договоры БО'
                     WHERE GenData.t_id = gdt.t_id;
                ELSIF ((pCloseContr = CHR (88)) AND (pOpenContr <> CHR (88))) THEN
                    UPDATE DNPTXGenReplaceOp_DBT gdt
                       SET gdt.t_state = STATE_ERR,
                           gdt.t_error =
                               'Отсутствуют закрытые договоры БО'
                     WHERE GenData.t_id = gdt.t_id;
                ELSE
                    UPDATE DNPTXGenReplaceOp_DBT gdt
                       SET gdt.t_state = STATE_ERR,
                           gdt.t_error = 'Отсутствуют договоры БО'
                     WHERE GenData.t_id = gdt.t_id;
                END IF;

                CONTINUE;
            END IF;

            IF(pSOFR = chr(88)) THEN
                SELECT count(1)
                  INTO v_ExistDCDRECORDS
                  FROM DCDRECORDS_DBT cdr
                 WHERE     cdr.T_CORPORATEACTIONTYPE = 'INTR'
                       AND cdr.t_partyid = GenData.t_partyid
                       AND cdr.T_ACCOUNTNUMBER LIKE '306%' 
                       and cdr.T_ISGETTAX <> chr(88)
                       AND cdr.T_PAYMENTDATE >= pBeginDate
                       AND cdr.T_PAYMENTDATE <= GenData.t_OprDate
                       AND cdr.T_OPERATIONSTATUS = 'активна'
                       AND CDR.T_ISIIS <> chr(88)
                       AND RSB_NPTXREPLACECODE.ConvertDateTimeToDay(cdr.T_REQUESTDATE,cdr.t_requesttime) =
                           (SELECT MAX (RSB_NPTXREPLACECODE.ConvertDateTimeToDay(cdrd.T_REQUESTDATE,cdrd.t_requesttime))
                              FROM DCDRECORDS_DBT cdrd
                             WHERE     cdrd.T_RECORDPAYMENTQTYID = cdr.T_RECORDPAYMENTQTYID
                                   AND cdrd.T_OPERATIONSTATUS = 'активна')
                       AND ((SELECT NVL (MAX (RSB_NPTXREPLACECODE.ConvertDateTimeToDay(cdrd.T_REQUESTDATE,cdrd.t_requesttime)),
                                         0)
                               FROM DCDRECORDS_DBT cdrd
                              WHERE     cdrd.T_RECORDPAYMENTQTYID = cdr.T_RECORDPAYMENTQTYID
                                    AND cdrd.T_OPERATIONSTATUS = 'отменена') <=
                            (SELECT MAX (RSB_NPTXREPLACECODE.ConvertDateTimeToDay(cdrd.T_REQUESTDATE,cdrd.t_requesttime))
                               FROM DCDRECORDS_DBT cdrd
                              WHERE     cdrd.T_RECORDPAYMENTQTYID = cdr.T_RECORDPAYMENTQTYID
                                    AND cdrd.T_OPERATIONSTATUS = 'активна'))
                       AND ROWNUM = 1;
            ELSIF (pDiasoft = chr(88)) THEN
                UPDATE DNPTXGenReplaceOp_DBT gdt
                   SET gdt.t_state = STATE_ERR,
                       gdt.t_error =
                           'Генерация операций Диасофт не доступна'
                 WHERE GenData.t_id = gdt.t_id;

                CONTINUE;
            ELSE
                v_ExistDCDRECORDS := 0;
            END IF;

            IF (v_ExistDCDRECORDS = 0) THEN
                UPDATE DNPTXGenReplaceOp_DBT gdt
                   SET gdt.t_state = STATE_ERR,
                       gdt.t_error =
                           'Отсутствуют выплаты купонного дохода за заданный период'
                 WHERE GenData.t_id = gdt.t_id;

                CONTINUE;
            END IF;

                SELECT NVL(MAX(t_code), '0'), NVL(MAX(t_OperDate), TO_DATE ('01.01.0001', 'DD.MM.YYYY'))
                  INTO v_ExistGenOpCode, v_ExistGenOpDate
                  FROM dnptxop_dbt op
                 WHERE     op.t_DocKind = 4652
                       AND op.t_client = GenData.t_partyid
                       AND op.t_OperDate =
                           (SELECT MAX (opD.t_OperDate)
                              FROM dnptxop_dbt opD
                             WHERE     opD.t_DocKind = 4652
                                   AND opD.t_client = GenData.t_partyid);

            IF (v_ExistGenOpDate > GenData.t_OprDate) THEN
                UPDATE DNPTXGenReplaceOp_DBT gdt
                   SET gdt.t_state = STATE_CLOSE,
                       gdt.t_error = 'Найдена более поздняя операция',
                       gdt.t_genOprCode = v_ExistGenOpCode
                 WHERE GenData.t_id = gdt.t_id;

                CONTINUE;
            END IF;
        END LOOP;

        COMMIT;
    END;


    PROCEDURE CreateReplaceOp (pPackNum       IN NUMBER,
                               pEndID         IN NUMBER,
                               pSessionID     IN VARCHAR2, 
                               pOpPrefix      IN VARCHAR2 DEFAULT CHR (0),
                               pSaleDeal      IN CHAR,
                               pChangeCodeIncome   IN CHAR,
                               pSOFR          IN CHAR,
                               pDiasoft       IN CHAR)
    IS
        v_nptxop       DNPTXOP_DBT%ROWTYPE;

        TYPE ListNPTXOP_t IS TABLE OF DNPTXOP_DBT%ROWTYPE;

        v_ListNPTXOP   ListNPTXOP_t := ListNPTXOP_t ();
        v_stat         NUMBER (10);
    BEGIN
        FOR cData IN (SELECT *
                        FROM DNPTXGenReplaceOp_DBT
                       WHERE t_SessionID = pSessionID AND t_state = STATE_OPEN AND t_PackNum = pPackNum)
        LOOP
            v_stat :=
                RSI_RSB_REFER.WldGenerateReference (v_nptxop.t_Code,
                                                    371,
                                                    188,
                                                    0,
                                                    NULL,
                                                    NULL,
                                                    CHR (0),
                                                    CHR (0),
                                                    1);
            v_nptxop.t_Code :=
                CASE
                    WHEN pOpPrefix <> CNST.UNSET_CHAR THEN pOpPrefix || v_nptxop.t_Code
                    ELSE v_nptxop.t_Code
                END;

            v_nptxop.t_ID := dnptxop_dbt_seq.NEXTVAL;
            v_nptxop.t_DocKind := 4652;
            v_nptxop.t_OperDate := cData.t_OprDate;
            v_nptxop.t_Kind_Operation := 2048;
            v_nptxop.t_Client := cData.t_PartyID;
            v_nptxop.t_Department := RsbSessionData.OperDprt;
            v_nptxop.t_Oper := RsbSessionData.oper;
            v_nptxop.t_Status := RSI_NPTXC.DL_TXOP_Prep;
            v_nptxop.t_SubKind_Operation := CASE WHEN pSOFR = chr(88) then 10
                                                 WHEN pDiasoft = chr(88) then 20 else 0 end;
            v_nptxop.t_IIS := CHR (0);
            v_nptxop.t_Account := RSI_RsbOperation.ZERO_STR;
            v_nptxop.t_AccountTax := RSI_RsbOperation.ZERO_STR;
            v_nptxop.t_BegRecalcDate := TO_DATE ('01.01.0001', 'DD.MM.YYYY');
            v_nptxop.t_CalcNDFL := CNST.UNSET_CHAR;
            v_nptxop.t_Contract := 0;
            v_nptxop.t_Currency := 0;
            v_nptxop.t_CurrencySum := 0;
            v_nptxop.t_CurrentYear_Sum := 0;
            v_nptxop.t_EndRecalcDate := TO_DATE ('01.01.0001', 'DD.MM.YYYY');
            v_nptxop.t_FIID := -1;
            v_nptxop.t_FlagTax := CNST.UNSET_CHAR;
            v_nptxop.t_LimitStatus := 0;
            v_nptxop.t_MarketPlace := 0;
            v_nptxop.t_MarketPlace2 := 0;
            v_nptxop.t_MarketSector := 0;
            v_nptxop.t_MarketSector2 := 0;
            v_nptxop.t_Method := 0;
            v_nptxop.t_OutCost := 0;
            v_nptxop.t_OutSum := 0;
            v_nptxop.t_Partial := CNST.UNSET_CHAR;
            v_nptxop.t_Place := 0;
            v_nptxop.t_Place2 := 0;
            v_nptxop.t_PlaceKind := 0;
            v_nptxop.t_PlaceKind2 := 0;
            v_nptxop.t_PrevDate := TO_DATE ('01.01.0001', 'DD.MM.YYYY');
            v_nptxop.t_PrevTaxSum := 0;
            v_nptxop.t_Recalc := CNST.UNSET_CHAR;
            v_nptxop.t_Tax := 0;
            v_nptxop.t_TaxBase := 0;
            v_nptxop.t_TaxSum := 0;
            v_nptxop.t_TaxSum2 := 0;
            v_nptxop.t_TaxToPay := 0;
            v_nptxop.t_Time := NPTAX.UnknownTime;
            v_nptxop.t_TotalTaxSum := 0;
            v_nptxop.t_TOUT := 0;
            v_nptxop.t_TaxDp := 0;
            v_nptxop.t_PayPurpose := 0;
            v_ListNPTXOP.EXTEND ();
            v_ListNPTXOP (v_ListNPTXOP.LAST) := v_nptxop;

            IF(pSaleDeal = chr(88)) then
                AddSaleDealCat (v_nptxop.t_id);
            end if;
            
            IF(pChangeCodeIncome = chr(88)) then
                AddChangeCodeIncomeCat (v_nptxop.t_id);
            end if;


            IF (v_stat = 0) THEN
                UPDATE DNPTXGenReplaceOp_DBT gdt
                   SET gdt.t_state = STATE_CLOSE, gdt.t_genOprCode = v_nptxop.t_Code
                 WHERE cData.t_id = gdt.t_id;

                COMMIT;
            ELSE
                UPDATE DNPTXGenReplaceOp_DBT gdt
                   SET gdt.t_state = STATE_ERR, gdt.t_genOprCode = v_nptxop.t_Code
                 WHERE cData.t_id = gdt.t_id;

                COMMIT;
            END IF;
        END LOOP;

        IF v_ListNPTXOP.COUNT > 0 THEN
            FORALL i IN v_ListNPTXOP.FIRST .. v_ListNPTXOP.LAST
                INSERT INTO DNPTXOP_DBT
                     VALUES v_ListNPTXOP (i);

            v_ListNPTXOP.DELETE;
        END IF;

        COMMIT;
    END;

    PROCEDURE RunProcInParallel (pSQLProc IN VARCHAR2, pSessionID IN VARCHAR2)
    IS
        v_task_name    VARCHAR2 (300);
        v_sql_chunks   CLOB;
        v_try          NUMBER (5) := 0;
        v_status       NUMBER;

        v_MaxPackNum   NUMBER (10);
    BEGIN
        SELECT NVL(MAX (t_packnum),0)
          INTO v_MaxPackNum
          FROM DNPTXGenReplaceOp_DBT
         WHERE t_SessionID = pSessionID;

        v_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;
        DBMS_PARALLEL_EXECUTE.create_task (task_name => v_task_name);

        v_sql_chunks := 'SELECT level, 0 FROM DUAL CONNECT BY LEVEL <= ' || TO_CHAR (v_MaxPackNum);

        DBMS_PARALLEL_EXECUTE.create_chunks_by_sql (task_name   => v_task_name,
                                                    sql_stmt    => v_sql_chunks,
                                                    by_rowid    => FALSE);

        DBMS_PARALLEL_EXECUTE.run_task (task_name        => v_task_name,
                                        sql_stmt         => pSQLProc,
                                        language_flag    => DBMS_SQL.NATIVE,
                                        parallel_level   => PARALLEL_LEVEL);

        v_status := DBMS_PARALLEL_EXECUTE.task_status (v_task_name);

        WHILE (v_try < 2 AND v_status != DBMS_PARALLEL_EXECUTE.FINISHED)
        LOOP
            v_try := v_try + 1;
            DBMS_PARALLEL_EXECUTE.resume_task (v_task_name);
            v_status := DBMS_PARALLEL_EXECUTE.task_status (v_task_name);
        END LOOP;

        DBMS_PARALLEL_EXECUTE.drop_task (v_task_name);
    END;


    PROCEDURE ExuGenReplaceOp (pBeginDate          IN DATE,
                               pEndDate            IN DATE,
                               pTaxPeriod          IN NUMBER,
                               pIIS                IN CHAR,
                               pOpenContr          IN CHAR,
                               pCloseContr         IN CHAR,
                               pSaleDeal           IN CHAR,
                               pChangeCodeIncome   IN CHAR,
                               pSOFR               IN CHAR,
                               pDiasoft            IN CHAR,
                               pOpPrefix           IN VARCHAR2 DEFAULT CHR (0),
                               pClientGroup        IN NUMBER DEFAULT 0,
                               pClientType         IN NUMBER DEFAULT 0,
                               pSessionID          IN VARCHAR2)
    IS

    BEGIN
        IF (pClientGroup = CLIENT_FROM_ALL) THEN
            InsertClients (pSessionID);
        ELSIF(pClientGroup = CLIENT_FROM_FILE) THEN
          UPDATE DNPTXGenReplaceOp_DBT set t_PACKNUM = TO_NUMBER (ROUND (ROWNUM / ExecPackSize, 0) + 1) where t_sessionid = pSessionID;
          COMMIT;
        ELSIF(pClientGroup = CLIENT_FROM_FILE_DIASOFT) THEN
          INSERT INTO DNPTXGenReplaceOp_DBT (t_partyid, t_sessionid, t_state, t_PACKNUM, t_OprDate) 
            VALUES (-1, pSessionID, STATE_OPEN, 1, CASE WHEN pTaxPeriod = EXTRACT (YEAR FROM RSBSESSIONDATA.curdate) THEN RSBSESSIONDATA.curdate ELSE TO_DATE ('3112' || pTaxPeriod || '', 'DDMMYYYY') END);
          RSB_NPTXREPLACECODE.CreateReplaceOp (1, 0, pSessionID, pOpPrefix, pSaleDeal, pChangeCodeIncome, pSOFR, pDiasoft );
          RETURN;
        END IF;

        pumpData (pSessionID,
                  pClientType,
                  pOpenContr,
                  pCloseContr,
                  pIIS,
                  pEndDate,
                  pTaxPeriod);

        RunProcInParallel (
               'CALL RSB_NPTXREPLACECODE.CheckClient ( :start_id, :end_id, '''
            || pSessionID
            || ''' , '''
            || pClientGroup
            || ''' , '''
            || pClientType
            || ''' , '''
            || pOpenContr
            || ''' , '''
            || pCloseContr
            || ''' , '''
            || pSOFR
            || ''' , '''
            || pDiasoft
            || ''' , TO_DATE('''
            || pBeginDate
            || ''') , TO_DATE('''
            || pEndDate
            || ''') ) ',
            pSessionID);

        RunProcInParallel (
               'CALL RSB_NPTXREPLACECODE.CreateReplaceOp ( :start_id, :end_id, '''
            || pSessionID
            || ''' , '''
            || pOpPrefix
            || ''' , '''
            || pSaleDeal
            || ''' , '''
            || pChangeCodeIncome
            || ''' , '''
            || pSOFR
            || ''' , '''
            || pDiasoft
            || ''' ) ',
            pSessionID);
    END;
    
    PROCEDURE makeDataDiasoft (pOprID    IN NUMBER)
    IS
        SQL_ST   VARCHAR2 (5000);
        l_try        NUMBER;
        l_status   NUMBER;
        l_task      VARCHAR2 (128) := '';
        v_schema   VARCHAR2 (1024);
    BEGIN

      SQL_ST := ' UPDATE DNPTXCODECHANGE_DBT ch
                           SET ch.T_TAXPERIOD = EXTRACT (YEAR FROM T_OPERDATE),
                                  ch.T_BEGINDATE = TO_DATE (''0101'' || TO_CHAR (EXTRACT (YEAR FROM T_OPERDATE)),  ''DDMMYYYY''),
                                  ch. T_ISCHANGEFLAG = chr(88),
                                  ch.T_FIID = NVL ((SELECT av.t_fiid FROM davoiriss_dbt av WHERE av.t_isin = ch.t_isin), -1),
                                  ch.T_ISCIRC = RSI_NPTO.Market3date ( (SELECT av.t_fiid FROM davoiriss_dbt av WHERE av.t_isin = ch.t_isin),  ch.T_PAYMENTDATE),
                                  ch.T_CLIENTID = NVL (  (SELECT CODE.T_OBJECTID
                                                                           FROM DOBJCODE_DBT CODE
                                                                         WHERE   CODE.T_CODE = ch.t_CFTID
                                                                               AND CODE.T_OBJECTTYPE = 3
                                                                               AND CODE.T_CODEKIND = 101
                                                                               AND CODE.t_state = 0),  -1)
                           WHERE T_OPERATIONID = '||pOprID|| ' and rowid BETWEEN :start_id AND :end_id ';
 
 
        SELECT DBMS_PARALLEL_EXECUTE.generate_task_name INTO l_task FROM DUAL;

        DBMS_PARALLEL_EXECUTE.CREATE_TASK (l_task);

        SELECT username INTO v_schema FROM user_users;

        DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID (l_task,
                                                      v_schema,
                                                      'DNPTXCODECHANGE_DBT',
                                                      TRUE,
                                                      100);


        DBMS_PARALLEL_EXECUTE.RUN_TASK (l_task,
                                        SQL_ST,
                                        DBMS_SQL.NATIVE,
                                        parallel_level   => 4);

        L_try := 0;
        L_status := DBMS_PARALLEL_EXECUTE.TASK_STATUS (l_task);

        WHILE (l_try < 2 AND L_status != DBMS_PARALLEL_EXECUTE.FINISHED)
        LOOP
            L_try := l_try + 1;
            DBMS_PARALLEL_EXECUTE.RESUME_TASK (l_task);
            L_status := DBMS_PARALLEL_EXECUTE.TASK_STATUS (l_task);
        END LOOP;

        DBMS_PARALLEL_EXECUTE.DROP_TASK (l_task);

        COMMIT;

        UPDATE  /*+ parallel(4) enable_parallel_dml */  DNPTXCODECHANGE_DBT ch
           SET T_ERRORTEXT =
                  CASE  WHEN T_CLIENTID = -1 THEN 'Не удалось определить клиента по ЦФТ ID'
                           WHEN T_FIID = -1 THEN 'Не удалось определить цб по Isin'
                     ELSE CHR (0) END
        WHERE ( (T_CLIENTID = -1) OR (T_FIID = -1)) AND T_OPERATIONID = pOprID;

       COMMIT;
       
        UPDATE  /*+ parallel(4) enable_parallel_dml */  DNPTXCODECHANGE_DBT ch
           SET T_GROUP =
                  CASE
                     WHEN T_ISCIRC = 2 THEN 3
                     WHEN T_ISCIRC = 3 THEN 4
                     WHEN T_ISCIRC = 1 THEN CASE
                                               WHEN (SELECT COUNT (1)
                                                       FROM DFININSTR_DBT fin, DPARTY_DBT pt
                                                      WHERE     fin.T_ISSUER = pt.T_PARTYID
                                                            AND pt.T_NRCOUNTRY IN ('RUS', 'BLR', 'KAZ')
                                                            AND fin.T_FACEVALUEFI = 0
                                                            AND RSI_rsb_fiinstr.FI_AvrKindsEQ (2, 32, fin.T_AVOIRKIND) = 1
                                                            AND (select av.t_incirculationdate from DAVOIRISS_DBT av where av.t_fiid = fin.t_fiid) >= TO_DATE ('01012017', 'DDMMYYYY')
                                                            AND fin.t_fiid = ch.t_fiid) > 0 THEN 1 ELSE 2 END
                     ELSE 0
                  END
         WHERE ((T_ERRORTEXT is null) or (T_ERRORTEXT = chr(0))) AND T_OPERATIONID = pOprID;

        COMMIT;

        UPDATE  /*+ parallel(4) enable_parallel_dml */ DNPTXCODECHANGE_DBT ch
           SET T_CODE_BEFOR =
                  CASE
                     WHEN LENGTH (T_CODE_BEFOR) < 2
                     THEN
                        CASE
                           WHEN T_GROUP = 1 THEN '3023'
                           WHEN T_GROUP = 2 THEN CASE WHEN T_ACCOUNTNUMBER LIKE '306%' THEN '1011_1' ELSE '1011' END
                           WHEN T_GROUP = 3 THEN CASE WHEN T_ACCOUNTNUMBER LIKE '306%' THEN '1011_3' ELSE '1011' END
                           WHEN T_GROUP = 4 THEN CASE WHEN T_ACCOUNTNUMBER LIKE '306%' THEN '1011_2' ELSE '1011' END
                           ELSE CHR (0)
                        END
                     ELSE T_CODE_BEFOR
                  END,
               T_CODE_AFTER =
                  CASE
                     WHEN T_GROUP = 1 THEN '1530'
                     WHEN T_GROUP = 2 THEN '1530'
                     WHEN T_GROUP = 3 THEN '1531'
                     WHEN T_GROUP = 4 THEN '1536'
                     ELSE CHR (0)
                  END,
               T_ISSUEDDATE =
                  (SELECT fin.T_ISSUED
                     FROM DFININSTR_DBT fin
                    WHERE fin.t_fiid = ch.T_FIID),
               T_KBK_AFTER =
                  CASE
                     WHEN (T_GROUP = 1) AND (T_KBK_BEFOR = '18210102010011000110') THEN '18210102070011000110'
                     WHEN (T_GROUP = 3) AND (T_KBK_BEFOR = '18210102070011000110') THEN '18210102010011000110'
                     WHEN (T_GROUP = 4) AND (T_KBK_BEFOR = '18210102070011000110') THEN '18210102010011000110'
                     ELSE T_KBK_BEFOR
                  END
         WHERE ((T_ERRORTEXT is null) or (T_ERRORTEXT = chr(0))) AND T_OPERATIONID = pOprID;

        COMMIT;
    END;
    
    PROCEDURE deleteNDRbyPay  (pOprID   IN NUMBER,
                                                   pPayID    IN NUMBER,
                                                   pClientID IN NUMBER)
    IS
      v_CalcEx NUMBER(5) := 0;
    BEGIN
        SELECT count(1)
          INTO v_CalcEx
          FROM dnptxobj_dbt
        WHERE     ( (t_kind IN (610, 620, 630) AND t_fromoutsyst = CHR (88)) OR (t_kind = 1185))
             AND T_CHANGECODE = CHR (88)
             AND T_OUTOBJID = pPayID
             AND T_DATE < (SELECT NVL (MAX (op.t_OperDate), TO_DATE ('01.01.0001', 'DD.MM.YYYY'))
                                                                FROM dnptxop_dbt op
                                                               WHERE op.t_DocKind = 4605 AND op.t_client = pClientID);

        IF(v_CalcEx > 0) THEN
          UPDATE DNPTXCODECHANGE_DBT
             SET T_ERRORTEXT = 'Требуется пересчет'
           WHERE T_OPERATIONID = pOprID AND T_RECORDPAYMENTID =pPayID;
        END IF;
        
        UPDATE DNPTXCODECHANGE_DBT
           SET T_ERRORTEXT = 'Откат'
        WHERE T_OPERATIONID IN
                  (SELECT t_docid
                     FROM DNPTXOBDC_DBT
                    WHERE t_objid IN
                             (SELECT t_objid
                                FROM dnptxobj_dbt
                               WHERE     ( (t_kind IN (610, 620, 630) AND t_fromoutsyst = CHR (88)) OR (t_kind = 1185))
                                     AND T_CHANGECODE = CHR (88)
                                     AND T_OUTOBJID = T_RECORDPAYMENTID))
               AND T_RECORDPAYMENTID = pPayID
               AND T_OPERATIONID <> pOprID;

        DELETE FROM DNPTXOBDC_DBT
              WHERE t_objid IN
                       (SELECT t_objid
                          FROM dnptxobj_dbt
                         WHERE     ( (t_kind IN (610, 620, 630) AND t_fromoutsyst = CHR (88)) OR (t_kind = 1185))
                               AND T_CHANGECODE = CHR (88)
                               AND T_OUTOBJID = pPayID);

        DELETE FROM dnptxobj_dbt
              WHERE ( (t_kind IN (610, 620, 630) AND t_fromoutsyst = CHR (88)) OR (t_kind = 1185)) AND T_CHANGECODE = CHR (88) AND T_OUTOBJID = pPayID;
    END;
    
END RSB_NPTXREPLACECODE;
/