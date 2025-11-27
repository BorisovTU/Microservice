CREATE OR REPLACE TRIGGER "DDLGRACC_DBT_TAU" 
  AFTER UPDATE OF T_STATE ON DDLGRACC_DBT
DECLARE
  v_N  NUMBER := -1;
  v_N_for_DEF40883 NUMBER := -1;

  TYPE nettid_t IS TABLE OF DDL_NETT_DBT.T_NETTINGID%TYPE;
  TYPE nettst_t IS TABLE OF DDL_NETT_DBT.T_STATUS%TYPE;

  v_nettid      nettid_t := nettid_t();
  v_nettsetst   nettst_t := nettst_t();
  v_nettnotst   nettst_t := nettst_t();

  TYPE dealid_t IS TABLE OF DDL_TICK_DBT.T_DEALID%TYPE;
  TYPE dealbk_t IS TABLE OF DDL_TICK_DBT.T_BOFFICEKIND%TYPE;
  TYPE dealst_t IS TABLE OF DDL_TICK_DBT.T_DEALSTATUS%TYPE;

  v_dealid      dealid_t := dealid_t();
  v_dealbk      dealbk_t := dealbk_t();
  v_dealsetst   dealst_t := dealst_t();
  v_dealnotst   dealst_t := dealst_t();

  TYPE commid_t IS TABLE OF DDL_COMM_DBT.T_DOCUMENTID%TYPE;
  TYPE commst_t IS TABLE OF DDL_COMM_DBT.T_COMMSTATUS%TYPE;

  v_commid      commid_t := commid_t();
  v_commsetst   commst_t := commst_t();
  v_commnotst   commst_t := commst_t();

BEGIN

  IF RSI_TRG_DDLGRACC_DBT.v_NumEnt > 0 THEN
    FOR v_i in 1..RSI_TRG_DDLGRACC_DBT.v_NumEnt LOOP
      SELECT Count(1) INTO v_N
       FROM DDLGRDEAL_DBT GRDEAL
      WHERE GRDEAL.T_DOCKIND = RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i)
        AND GRDEAL.T_DOCID = RSI_TRG_DDLGRACC_DBT.v_DocID(v_i)
        AND EXISTS(SELECT 1 FROM DDLGRACC_DBT GRACC WHERE GRACC.T_GRDEALID = GRDEAL.T_ID AND GRACC.T_STATE = RSI_DLGR.DLGRACC_STATE_PLAN)
        AND ROWNUM = 1;

      IF v_N = 0 THEN
        IF RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i) = 154 THEN --Неттинг
          v_nettid.extend;
          v_nettid(v_nettid.LAST) := RSI_TRG_DDLGRACC_DBT.v_DocID(v_i);

          v_nettsetst.extend;
          v_nettsetst(v_nettsetst.LAST) := 20;

          v_nettnotst.extend;
          v_nettnotst(v_nettnotst.LAST) := 20;

        ELSIF ( RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i) = 4619 OR RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i) = 4621 ) THEN --Передача в пул/возрат из пула, перевод остатков в КДУ

          v_commid.extend;
          v_commid(v_commid.LAST) := RSI_TRG_DDLGRACC_DBT.v_DocID(v_i);

          v_commsetst.extend;
          v_commsetst(v_commsetst.LAST) := 2;

          v_commnotst.extend;
          v_commnotst(v_commnotst.LAST) := 2;

        ELSIF RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i) = 4832 
        or RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i) = 127 
        THEN --Погашение ОЭБ
          SELECT Count(1) INTO v_N
            FROM DDLRQ_DBT RQ
           WHERE RQ.T_DOCKIND = RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i)
             AND RQ.T_DOCID = RSI_TRG_DDLGRACC_DBT.v_DocID(v_i)
             AND RQ.T_STATE = RSI_DLRQ.DLRQ_STATE_FORPROCESSING
             AND ROWNUM = 1;
          IF v_N = 0 THEN
            SELECT Count(1) INTO v_N
              FROM DOPROPER_DBT OP, DOPRSTEP_DBT ST
             WHERE OP.T_DOCKIND = RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i)
               AND OP.T_DOCUMENTID = lpad(RSI_TRG_DDLGRACC_DBT.v_DocID(v_i), 34, '0')
               AND ST.T_ID_OPERATION = OP.T_ID_OPERATION
               AND ST.T_ISEXECUTE = 'R'
               AND ROWNUM = 1;
            IF v_N = 0 THEN

              v_dealid.extend;
              v_dealid(v_dealid.LAST) := RSI_TRG_DDLGRACC_DBT.v_DocID(v_i);

              v_dealbk.extend;
              v_dealbk(v_dealbk.LAST) := RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i);

              v_dealsetst.extend;
              v_dealsetst(v_dealsetst.LAST) := 20;

              v_dealnotst.extend;
              v_dealnotst(v_dealnotst.LAST) := 20;

            END IF;
          END IF;
        ELSE
          v_dealid.extend;
          v_dealid(v_dealid.LAST) := RSI_TRG_DDLGRACC_DBT.v_DocID(v_i);

          v_dealbk.extend;
          v_dealbk(v_dealbk.LAST) := RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i);

          v_dealsetst.extend;
          v_dealsetst(v_dealsetst.LAST) := 20;

          v_dealnotst.extend;
          v_dealnotst(v_dealnotst.LAST) := 20;
        END IF;
      ELSE
        IF RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i) = 154 THEN --Неттинг
          v_nettid.extend;
          v_nettid(v_nettid.LAST) := RSI_TRG_DDLGRACC_DBT.v_DocID(v_i);

          v_nettsetst.extend;
          v_nettsetst(v_nettsetst.LAST) := 10;

          v_nettnotst.extend;
          v_nettnotst(v_nettnotst.LAST) := 10;

        ELSIF ( RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i) = 4619 OR RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i) = 4621 ) THEN --Передача в пул/возрат из пула, перевод остатков в КДУ
          v_commid.extend;
          v_commid(v_commid.LAST) := RSI_TRG_DDLGRACC_DBT.v_DocID(v_i);

          v_commsetst.extend;
          v_commsetst(v_commsetst.LAST) := 1;

          v_commnotst.extend;
          v_commnotst(v_commnotst.LAST) := 1;

        ELSE
          v_dealid.extend;
          v_dealid(v_dealid.LAST) := RSI_TRG_DDLGRACC_DBT.v_DocID(v_i);

          v_dealbk.extend;
          v_dealbk(v_dealbk.LAST) := RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i);

          v_dealsetst.extend;
          v_dealsetst(v_dealsetst.LAST) := 10;

          v_dealnotst.extend;
          v_dealnotst(v_dealnotst.LAST) := 10;

          SELECT Count(1) INTO v_N_for_DEF40883
            FROM DDLGRDEAL_DBT GRDEAL
          WHERE GRDEAL.T_DOCKIND = RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i)
            AND GRDEAL.T_DOCID = RSI_TRG_DDLGRACC_DBT.v_DocID(v_i)
            AND EXISTS(SELECT 1 FROM DDLGRACC_DBT GRACC WHERE GRACC.T_GRDEALID = GRDEAL.T_ID AND GRACC.T_STATE = RSI_DLGR.DLGRACC_STATE_PLAN)
            AND ROWNUM = 1;
          IF v_N_for_DEF40883 = 0 THEN 
            -- DEF-40883 Неообходимо было закрыть операции, но это не будет сделано. Делаем запись в логе для дальнейшего изучения
            it_log.log('Все строки графика обработаны в конкурирующем процессе'||
              '. DocKind='||RSI_TRG_DDLGRACC_DBT.v_DocKind(v_i)||
              '. DocID='  ||RSI_TRG_DDLGRACC_DBT.v_DocID(v_i)
              );
          END IF;  
        END IF;
      END IF;
    END LOOP;

    IF v_nettid IS NOT EMPTY THEN

      forall i in v_nettid.first .. v_nettid.last
       update DDL_NETT_DBT
          set T_STATUS = v_nettsetst(i)
        where T_NETTINGID = v_nettid(i)
          and T_STATUS <> v_nettnotst(i);

      v_nettid.delete;
      v_nettsetst.delete;
      v_nettnotst.delete;
    END IF;

    IF v_dealid IS NOT EMPTY THEN

      forall i in v_dealid.first .. v_dealid.last
       update DDL_TICK_DBT
          set T_DEALSTATUS = v_dealsetst(i)
        where T_BOFFICEKIND = v_dealbk(i)
          and T_DEALID = v_dealid(i)
          and T_DEALSTATUS <> v_dealnotst(i);

      v_dealid.delete;
      v_dealbk.delete;
      v_dealsetst.delete;
      v_dealnotst.delete;
    END IF;

    IF v_commid IS NOT EMPTY THEN

      forall i in v_commid.first .. v_commid.last
       update DDL_COMM_DBT
          set T_COMMSTATUS = v_commsetst(i)
        where T_DOCUMENTID = v_commid(i)
          and T_COMMSTATUS <> v_commnotst(i);

      v_commid.delete;
      v_commsetst.delete;
      v_commnotst.delete;
    END IF;

    RSI_TRG_DDLGRACC_DBT.v_NumEnt := 0;
  END IF;

END DDLGRACC_DBT_TAU;
/
