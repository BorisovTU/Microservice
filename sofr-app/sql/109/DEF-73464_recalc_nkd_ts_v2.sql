/* Formatted on 10.10.2024 6:52:03 (QP5 v5.149.1003.31008) */
DECLARE
   TYPE paymentid_rec_type IS RECORD
   (
      T_PAYMENTID   DNPTXNKDREQDIAS_DBT.T_PAYMENTID%TYPE
   );

   TYPE payment_table_type IS TABLE OF paymentid_rec_type;

   v_payment_table    payment_table_type;

   v_Cursor           SYS_REFCURSOR;

   v_reqdiasreq       dnptxnkdreqdias_dbt%ROWTYPE;

   TYPE dias_pay_rec_type IS RECORD
   (
      t_NKDBuy     dnptxobj_dbt.t_Sum0%TYPE,
      t_NKDSale    dnptxlot_dbt.t_NKD%TYPE,
      t_BuyID      dnptxobj_dbt.t_Analitic1%TYPE,
      t_DealDate   ddl_tick_dbt.t_DealDate%TYPE,
      t_DealTime   ddl_tick_dbt.t_DealTime%TYPE
   );

   TYPE dias_pay_table_type IS TABLE OF dias_pay_rec_type;

   v_dias_pay_table   dias_pay_table_type;

   v_sum              NUMBER (32, 12);
   v_calcsum          NUMBER (32, 12);
   v_restsum          NUMBER (32, 12);

   v_unkd             NUMBER (32, 12);
   v_recca            NUMBER (32, 12);

   v_txObj            DNPTXOBJ_DBT%ROWTYPE;
   v_ContractID       NUMBER := 0;
BEGIN
   OPEN v_Cursor FOR
      SELECT DISTINCT REQ.T_PAYMENTID
        FROM DNPTXOBJ_DBT obj
             INNER JOIN DNPTXLNKDIASPAY_DBT dp
                ON dp.T_NKDBTSOBJID = obj.t_objid
             INNER JOIN dnptxnkdreqdias_dbt req
                ON REQ.T_PAYMENTID = DP.T_PAYMENTID
       WHERE obj.t_kind = 1115 AND obj.t_cur <> 0;

   FETCH v_Cursor
   BULK COLLECT INTO v_payment_table;

   CLOSE v_Cursor;

   IF (v_payment_table.COUNT = 0)
   THEN
      OPEN v_Cursor FOR
         SELECT DISTINCT req.t_paymentid
           FROM (  SELECT TO_NUMBER (
                             NVL (
                                REGEXP_REPLACE (MSG,
                                                '^(.*)(id )(\d+)(.*)$',
                                                '\3'),
                                0))
                             AS t_id
                     FROM ITT_LOG
                    WHERE OBJECT_NAME = 'RECALCNKDTS'
                 ORDER BY CREATE_SYSDATE ASC) q1,
                dnptxnkdreqdias_dbt req
          WHERE req.t_id = q1.t_id AND q1.t_id > 0;

      FETCH v_Cursor
      BULK COLLECT INTO v_payment_table;

      CLOSE v_Cursor;

      IF (v_payment_table.COUNT > 0)
      THEN
         DELETE FROM ITT_LOG WHERE ID_LOG IN (SELECT ID_LOG FROM ITT_LOG WHERE OBJECT_NAME = 'RECALCNKDTS');

         FORALL i IN v_payment_table.FIRST .. v_payment_table.LAST
            DELETE FROM DNPTXOBJ_DBT obj
                  WHERE obj.t_objid IN
                           (SELECT T_NKDBTSOBJID
                              FROM DNPTXLNKDIASPAY_DBT
                             WHERE T_PAYMENTID =
                                      v_payment_table (i).T_PAYMENTID);

         FORALL i IN v_payment_table.FIRST .. v_payment_table.LAST
            DELETE FROM DNPTXLNKDIASPAY_DBT
                  WHERE T_PAYMENTID = v_payment_table (i).T_PAYMENTID;
      END IF;
   ELSE
      DELETE FROM DNPTXLNKDIASPAY_DBT
            WHERE T_NKDBTSOBJID IN
                     (SELECT DISTINCT obj.t_objid
                        FROM DNPTXOBJ_DBT obj
                       WHERE obj.t_kind = 1115 AND obj.t_cur <> 0);

      DELETE FROM DNPTXOBJ_DBT obj
            WHERE obj.t_kind = 1115 AND obj.t_cur <> 0;
   END IF;

   IF (v_payment_table.COUNT > 0)
   THEN
      FOR indx IN 1 .. v_payment_table.COUNT
      LOOP
         SELECT *
           INTO v_reqdiasreq
           FROM dnptxnkdreqdias_dbt
          WHERE T_PAYMENTID = v_payment_table (indx).T_PAYMENTID
                AND ROWNUM = 1;

         OPEN v_Cursor FOR
              SELECT SUM (obj.t_Sum0) AS NKDBuy,
                     NVL (
                        SUM (
                           (SELECT NVL (SUM (  RSI_RSB_FIInstr.
                                                ConvSum (
                                                  sale.t_NKD,
                                                  Fin.t_FaceValueFI,
                                                  0,
                                                  lnk.T_DATE)
                                             * lnk.t_Amount
                                             / sale.t_Amount),
                                        0)
                              FROM dnptxlot_dbt buy,
                                   dnptxlnk_dbt lnk,
                                   dnptxlot_dbt sale,
                                   dfininstr_dbt Fin
                             WHERE     buy.t_DocKind IN (101, 127)
                                   AND buy.t_DocID = obj.t_Analitic1
                                   AND lnk.t_BuyID = buy.t_ID
                                   AND lnk.t_Client = obj.t_Client
                                   AND sale.t_ID = lnk.t_SaleID
                                   AND Fin.t_FIID = sale.t_fiid)),
                        0)
                        AS NKDSale,
                     obj.t_Analitic1 AS BuyID,
                     tk.t_DealDate,
                     tk.t_DealTime
                FROM dnptxobj_dbt obj, ddl_tick_dbt tk
               WHERE obj.t_Client = v_reqdiasreq.T_PARTYID
                     AND obj.t_Date BETWEEN v_reqdiasreq.t_CouponStartDate
                                        AND v_reqdiasreq.t_FixingDate
                     AND obj.t_Kind = 20
                     AND obj.t_Direction = 2
                     AND obj.t_AnaliticKind1 IN (1010, 1070)
                     AND obj.t_AnaliticKind3 = 3010
                     AND obj.t_Analitic3 = v_reqdiasreq.t_FIID
                     AND obj.t_AnaliticKind6 = 6020
                     AND obj.t_Analitic6 =
                            (SELECT DISTINCT sf_mp.T_ID
                               FROM dnptxnkdreqdias_dbt req
                                    INNER JOIN ddlcontr_dbt dlc
                                       ON dlc.t_SfContrID =
                                             req.T_CONTRACTID
                                    INNER JOIN ddlcontrmp_dbt mp
                                       ON mp.t_DlContrID =
                                             dlc.t_DlContrID
                                          AND mp.t_MarketID =
                                                 CASE
                                                    WHEN LOWER (
                                                            req.
                                                             T_MARKETPLACE) =
                                                            'nrd'
                                                    THEN
                                                       2
                                                    ELSE
                                                       151337
                                                 END
                                    INNER JOIN dsfcontr_dbt sf_mp
                                       ON sf_mp.t_ID =
                                             mp.t_SfContrID
                                          AND sf_mp.t_ServKind =
                                                 1
                                          AND (sf_mp.t_DateClose =
                                                  TO_DATE (
                                                     '01.01.0001',
                                                     'DD.MM.YYYY')
                                               OR sf_mp.
                                                   t_DateClose >=
                                                     req.
                                                      t_FixingDate)
                              WHERE req.T_PAYMENTID =
                                       v_payment_table (indx).T_PAYMENTID)
                     AND tk.t_DealID = obj.t_Analitic1
            GROUP BY obj.t_Analitic1, tk.t_DealDate, tk.t_DealTime
            ORDER BY tk.t_DealDate ASC,
                     tk.t_DealTime ASC,
                     obj.t_Analitic1 ASC;

         v_sum := 0;

         LOOP
            FETCH v_Cursor
            BULK COLLECT INTO v_dias_pay_table
            LIMIT 1000;

            FOR indx2 IN 1 .. v_dias_pay_table.COUNT
            LOOP
               v_sum :=
                  v_sum
                  + GREATEST (
                       (v_dias_pay_table (indx2).t_NkdBuy
                        - v_dias_pay_table (indx2).t_NkdSale),
                       0);
            END LOOP;

            v_CalcSum := LEAST (v_reqdiasreq.t_ReceivedCouponAmount, v_sum);
            v_restsum := v_CalcSum;

            FOR indx2 IN 1 .. v_dias_pay_table.COUNT
            LOOP
               v_Unkd :=
                  GREATEST (
                     (v_dias_pay_table (indx2).t_NkdBuy
                      - v_dias_pay_table (indx2).t_NkdSale),
                     0);
               v_recca := LEAST (v_unkd, v_restsum);

               IF (v_recca > 0)
               THEN
                  INSERT INTO DNPTXLNKDIASPAY_DBT (t_Date,
                                                   t_ConfirmDate,
                                                   t_PartyID,
                                                   t_ContractID,
                                                   t_FIID,
                                                   t_BuyID,
                                                   t_NkdBuy,
                                                   t_NkdSale,
                                                   t_Sum,
                                                   t_Comment,
                                                   t_PaymentID,
                                                   t_NkdBtsObjID,
                                                   t_ReceivedCouponAmount)
                       VALUES (TRUNC (SYSDATE),
                               TO_DATE ('01.01.0001', 'DD.MM.YYYY'),
                               v_reqdiasreq.t_PartyID,
                               v_reqdiasreq.t_ContractID,
                               v_reqdiasreq.t_FIID,
                               v_dias_pay_table (indx2).t_BuyID,
                               v_dias_pay_table (indx2).t_NKDBuy,
                               ROUND (v_dias_pay_table (indx2).t_NKDSale, 2),
                               v_Unkd,
                               CHR (0),
                               v_payment_table (indx).T_PAYMENTID,
                               0,
                               v_RecCA);
               END IF;

               v_restsum := v_restsum - v_recca;
            END LOOP;


            EXIT WHEN v_Cursor%NOTFOUND;
         END LOOP;

         it_log.
          log_handle (
            'RecalcNKDTS',
               'Обновлена запись dnptxnkdreqdias_dbt с id '
            || v_reqdiasreq.t_id
            || ' (paymid: '
            || v_reqdiasreq.t_paymentid
            || '). t_Sum(old): '
            || v_reqdiasreq.t_Sum
            || '; t_Sum(new): '
            || v_sum);

         UPDATE dnptxnkdreqdias_dbt
            SET t_Sum = v_sum
          WHERE t_id = v_reqdiasreq.t_id;

         SELECT sf_mp.T_ID
           INTO v_ContractID
           FROM dnptxnkdreqdias_dbt req
                INNER JOIN ddlcontr_dbt dlc
                   ON dlc.t_SfContrID = req.T_CONTRACTID
                INNER JOIN ddlcontrmp_dbt mp
                   ON mp.t_DlContrID = dlc.t_DlContrID
                      AND mp.t_MarketID =
                             CASE
                                WHEN LOWER (req.T_MARKETPLACE) = 'nrd' THEN 2
                                ELSE 151337
                             END
                INNER JOIN dsfcontr_dbt sf_mp
                   ON sf_mp.t_ID = mp.t_SfContrID AND sf_mp.t_ServKind = 1
                      AND (sf_mp.t_DateClose =
                              TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                           OR sf_mp.t_DateClose >= req.t_FixingDate)
          WHERE req.T_PAYMENTID = v_payment_table (indx).T_PAYMENTID
                AND ROWNUM = 1;

         FOR one_pay
            IN (SELECT DP.T_ID,
                       DP.T_RECEIVEDCOUPONAMOUNT,
                       TK.T_BOFFICEKIND,
                       TK.T_DEALID
                  FROM DNPTXLNKDIASPAY_DBT DP, DDL_TICK_DBT TK
                 WHERE DP.T_PAYMENTID = v_payment_table (indx).T_PAYMENTID
                       AND DP.T_NKDBTSOBJID = 0
                       AND TK.T_DEALID = DP.T_BUYID)
         LOOP
            v_txObj.T_OBJID := 0;

            v_txObj.T_OUTSYSTCODE := 'DEPO';
            v_txObj.T_OUTOBJID := v_payment_table (indx).T_PAYMENTID;
            v_txObj.T_SOURCEOBJID := 0;

            v_txObj.T_ANALITICKIND1 :=
               (CASE
                   WHEN one_pay.T_BOFFICEKIND = RSB_SECUR.DL_SECURITYDOC
                   THEN
                      RSI_NPTXC.TXOBJ_KIND1010
                   WHEN one_pay.T_BOFFICEKIND = RSB_SECUR.DL_AVRWRT
                   THEN
                      RSI_NPTXC.TXOBJ_KIND1070
                   ELSE
                      0
                END);
            v_txObj.T_ANALITIC1 := one_pay.T_DEALID;
            v_txObj.T_ANALITICKIND2 := RSI_NPTXC.TXOBJ_KIND2030;
            v_txObj.T_ANALITIC2 := v_reqdiasreq.T_COUPONNUMBER;
            v_txObj.T_ANALITICKIND3 := RSI_NPTXC.TXOBJ_KIND3010;
            v_txObj.T_ANALITIC3 := v_reqdiasreq.T_FIID;
            v_txObj.T_ANALITICKIND4 := RSI_NPTXC.TXOBJ_KIND4010;
            v_txObj.T_ANALITIC4 :=
               RSI_NPTO.
                Market2dates (v_reqdiasreq.T_FIID,
                              v_reqdiasreq.T_FIXINGDATE,
                              NULL);
            v_txObj.T_ANALITICKIND5 := RSI_NPTXC.TXOBJ_KIND5010;
            v_txObj.T_ANALITIC5 :=
               npto.GetPaperTaxGroupNPTX (v_reqdiasreq.T_FIID);
            v_txObj.T_ANALITICKIND6 := RSI_NPTXC.TXOBJ_KIND6020;
            v_txObj.T_ANALITIC6 := v_ContractID;
            v_txObj.T_DATE := v_reqdiasreq.T_FIXINGDATE;
            v_txObj.T_CLIENT := v_reqdiasreq.T_PARTYID;
            v_txObj.T_CUR := RSI_RSB_FIInstr.NATCUR;
            v_txObj.T_LEVEL := 2;
            v_txObj.T_USER := CHR (0);
            v_txObj.T_TECHNICAL := CHR (0);
            v_txObj.T_KIND := RSI_NPTXC.TXOBJ_NKDB_TS;
            v_txObj.T_DIRECTION := RSI_NPTXC.TXOBJ_DIR_OUT;
            v_txObj.T_FROMOUTSYST := CHR (0);
            v_txObj.T_SUM := one_pay.T_RECEIVEDCOUPONAMOUNT;
            v_txObj.T_SUM0 := v_txObj.T_SUM;

            INSERT INTO DNPTXOBJ_DBT
                 VALUES v_txObj
              RETURNING T_OBJID
                   INTO v_txObj.T_OBJID;

            UPDATE DNPTXLNKDIASPAY_DBT
               SET T_NKDBTSOBJID = v_txObj.T_OBJID,
                   T_CONFIRMDATE = TRUNC (SYSDATE)
             WHERE T_ID = one_pay.T_ID;
         END LOOP;

         CLOSE v_Cursor;
      END LOOP;
   END IF;
END;
\