CREATE OR REPLACE PACKAGE BODY RSB_NDFLREG
IS
    FUNCTION delSlice (pSliceNumDel IN NUMBER, pBeginDate IN DATE, pEndDate IN DATE)
        RETURN NUMBER
    IS
        cnt       NUMBER (10);
        SLICEID   NUMBER (10);
    BEGIN
        SELECT COUNT (1)
          INTO cnt
          FROM DNPTXSLICE_DBT
         WHERE T_SLICENUM = pSliceNumDel AND T_DATEFROM = pBeginDate AND T_DATETO = pEndDate;

        IF cnt = 1 THEN
            SELECT T_SLICEID
              INTO SLICEID
              FROM DNPTXSLICE_DBT
             WHERE T_SLICENUM = pSliceNumDel AND T_DATEFROM = pBeginDate AND T_DATETO = pEndDate;

            DELETE FROM
                DNPTXSLICE_DBT
                  WHERE     T_SLICENUM = pSliceNumDel
                        AND T_DATEFROM = pBeginDate
                        AND T_DATETO = pEndDate;

            DELETE FROM DNPTXSLICEOBJ_DBT
                  WHERE T_SLICEID = SLICEID;

            DELETE FROM DNPTXSLICEBASE_DBT
                  WHERE T_SLICEID = SLICEID;

            RETURN 1;
        END IF;

        RETURN 0;
    END;

    PROCEDURE insertSlice (pSliceNum    IN NUMBER,
                           pBeginDate   IN DATE,
                           pEndDate     IN DATE,
                           pReportID    IN NUMBER)
    IS
        cnt        NUMBER (10);
        SLICEID    NUMBER (10);
        SliceNum   NUMBER (10) := 0;
    BEGIN
        SELECT COUNT (1)
          INTO cnt
          FROM DNPTXSLICE_DBT
         WHERE T_SLICENUM = pSliceNum AND T_DATEFROM = pBeginDate AND T_DATETO = pEndDate;

        IF cnt = 0 THEN
            SliceNum := pSliceNum;
        ELSE
            SELECT NVL (MAX (T_SLICENUM + 1), 0)
              INTO SliceNum
              FROM DNPTXSLICE_DBT
             WHERE T_DATEFROM = pBeginDate AND T_DATETO = pEndDate;
        END IF;

        INSERT INTO DNPTXSLICE_DBT (T_DATEFROM,
                                    T_DATETO,
                                    T_SYSDATE,
                                    T_SLICENUM,
                                    T_REPORTID)
             VALUES (pBeginDate,
                     pEndDate,
                     TRUNC (SYSDATE),
                     SliceNum,
                     pReportID)
          RETURNING T_SLICEID
               INTO SLICEID;

        INSERT /*+ parallel(4) enable_parallel_dml */
               INTO DNPTXSLICEOBJ_DBT (T_ANALITIC1,
                                       T_ANALITIC2,
                                       T_ANALITIC3,
                                       T_ANALITIC4,
                                       T_ANALITIC5,
                                       T_ANALITIC6,
                                       T_ANALITICKIND1,
                                       T_ANALITICKIND2,
                                       T_ANALITICKIND3,
                                       T_ANALITICKIND4,
                                       T_ANALITICKIND5,
                                       T_ANALITICKIND6,
                                       T_CLIENT,
                                       T_COMMENT,
                                       T_CUR,
                                       T_DATE,
                                       T_DIRECTION,
                                       T_FROMOUTSYST,
                                       T_KIND,
                                       T_LEVEL,
                                       T_OBJID,
                                       T_SLICEID,
                                       T_SUM,
                                       T_SUM0,
                                       T_USER,
                                       T_TECHNICAL,
                                       T_TAXPERIOD)
            SELECT /*+ index(txobj, DNPTXOBJ_DBT_IDX3) */
                   T_ANALITIC1,
                   T_ANALITIC2,
                   T_ANALITIC3,
                   T_ANALITIC4,
                   T_ANALITIC5,
                   T_ANALITIC6,
                   T_ANALITICKIND1,
                   T_ANALITICKIND2,
                   T_ANALITICKIND3,
                   T_ANALITICKIND4,
                   T_ANALITICKIND5,
                   T_ANALITICKIND6,
                   T_CLIENT,
                   T_COMMENT,
                   T_CUR,
                   T_DATE,
                   T_DIRECTION,
                   T_FROMOUTSYST,
                   T_KIND,
                   T_LEVEL,
                   T_OBJID,
                   SLICEID,
                   T_SUM,
                   T_SUM0,
                   T_USER,
                   T_TECHNICAL,
                   T_TAXPERIOD
              FROM dnptxobj_dbt txobj, DNPTXREGKIND_DBT txkind, DNPTX6CLIENT_TMP c
             WHERE     txobj.t_Kind = txkind.T_NPTXKIND
                   AND c.t_partyid = txobj.t_client
                   AND txobj.t_Date >= pBeginDate
                   AND txobj.t_Date <= pEndDate
                   AND txobj.t_User <> CHR (88);


        INSERT /*+ parallel(4) enable_parallel_dml */
               INTO DNPTXSLICEBASE_DBT (T_SLICEID,
                                        T_TBID,
                                        T_CLIENTID,
                                        T_TYPE,
                                        T_DESCRIPTION,
                                        T_INCREGIONDATE,
                                        T_INCDATE,
                                        T_INCTIME,
                                        T_CONFIRMSTATE,
                                        T_SOURCESYSTEM,
                                        T_STORSTATE,
                                        T_DOCKIND,
                                        T_DOCID,
                                        T_TAXPERIOD,
                                        T_TAXBASEKIND,
                                        T_TAXBASECURRPAY,
                                        T_CALCPITAX,
                                        T_RATECALCPITAX,
                                        T_HOLDPITAX,
                                        T_RATEHOLDPITAX,
                                        T_BCCCALCPITAX,
                                        T_BCCHOLDPITAX,
                                        T_TAXPAYERSTATUS,
                                        T_APPLSTAXBASEINCLUDE,
                                        T_APPLSTAXBASEEXCLUDE,
                                        T_RECSTAXBASE,
                                        T_RECSTAXBASEDATE,
                                        T_ORIGTBID,
                                        T_INSTANCE,
                                        T_ID_OPERATION,
                                        T_ID_STEP,
                                        T_NEEDRECALC,
                                        T_RECTAXBASEBYKIND,
                                        T_INITIAL_DOCKIND,
                                        T_INITIAL_DOCID)
            SELECT SLICEID,
                   T_TBID,
                   T_CLIENTID,
                   T_TYPE,
                   T_DESCRIPTION,
                   T_INCREGIONDATE,
                   T_INCDATE,
                   T_INCTIME,
                   T_CONFIRMSTATE,
                   T_SOURCESYSTEM,
                   T_STORSTATE,
                   T_DOCKIND,
                   T_DOCID,
                   T_TAXPERIOD,
                   T_TAXBASEKIND,
                   T_TAXBASECURRPAY,
                   T_CALCPITAX,
                   T_RATECALCPITAX,
                   T_HOLDPITAX,
                   T_RATEHOLDPITAX,
                   T_BCCCALCPITAX,
                   T_BCCHOLDPITAX,
                   T_TAXPAYERSTATUS,
                   T_APPLSTAXBASEINCLUDE,
                   T_APPLSTAXBASEEXCLUDE,
                   T_RECSTAXBASE,
                   T_RECSTAXBASEDATE,
                   T_ORIGTBID,
                   T_INSTANCE,
                   T_ID_OPERATION,
                   T_ID_STEP,
                   T_NEEDRECALC,
                   T_RECTAXBASEBYKIND,
                   T_INITIAL_DOCKIND,
                   T_INITIAL_DOCID
              FROM dnptxtotalbase_dbt tb, DNPTX6CLIENT_TMP c
             WHERE     tb.t_ClientID = c.t_partyid
                   AND tb.t_StorState = 1 /*NPTXTOTALBASE_STORSTATE_ACTIVE*/
                   AND tb.t_Type IN (1, 2, 3)
                   AND (   (tb.t_IncDate BETWEEN pBeginDate AND pEndDate)
                        OR     (    tb.t_TaxPeriod = EXTRACT (YEAR FROM pEndDate)
                                AND EXTRACT (YEAR FROM tb.t_IncDate) <>
                                    EXTRACT (YEAR FROM pEndDate))
                           AND pEndDate = TO_DATE ('31.12.' || TO_CHAR (tb.t_TaxPeriod), 'DD.MM.YYYY'));
    END;
END RSB_NDFLREG;
/