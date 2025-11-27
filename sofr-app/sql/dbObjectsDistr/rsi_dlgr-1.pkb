CREATE OR REPLACE PACKAGE BODY RSI_DLGR AS

  TYPE dlgrdeal_t IS TABLE OF ddlgrdeal_dbt%ROWTYPE;
  TYPE dlgracc_t IS TABLE OF ddlgracc_dbt%ROWTYPE;
  TYPE dlgraccbc_t IS TABLE OF ddlgraccbc_dbt%ROWTYPE;
  TYPE dlgrdoc_t IS TABLE OF ddlgrdoc_dbt%ROWTYPE;

  g_gracc_ins dlgracc_t := dlgracc_t();
  g_gracc_upd dlgracc_t := dlgracc_t();
  g_graccbc_ins dlgraccbc_t := dlgraccbc_t();
  g_grdeal_upd dlgrdeal_t := dlgrdeal_t();

  g_dlgrdoc_ins dlgrdoc_t := dlgrdoc_t();

  LastErrorMessage VARCHAR2(1024) := '';

  PROCEDURE InitError
  AS
  BEGIN
     LastErrorMessage := '';
  END;

  PROCEDURE SetError( ErrNum IN INTEGER, ErrMes IN VARCHAR2 DEFAULT NULL )
  AS
  BEGIN
     IF( ErrMes IS NULL ) THEN
        LastErrorMessage := '';
     ELSE
        LastErrorMessage := ErrMes;
     END IF;
     RAISE_APPLICATION_ERROR( ErrNum,'' );
  END;

  PROCEDURE GetLastErrorMessage( ErrMes OUT VARCHAR2 )
  AS
  BEGIN
     ErrMes := LastErrorMessage;
  END;

  FUNCTION GetDLGRACCBC( GRACCID IN NUMBER, Instance IN NUMBER, DlGrAccBc OUT ddlgraccbc_dbt%ROWTYPE ) RETURN INTEGER
  AS
  BEGIN
     SELECT * INTO DlGrAccBc 
       FROM ddlgraccbc_dbt
      WHERE t_GRACCID = GRACCID
           AND t_Instance = Instance;

     RETURN 0;

  EXCEPTION WHEN OTHERS THEN
     RETURN 1;
  END; --  GetDLGRACCBC

  --Процедура вставки строки графика исполнения сделки вне шага операции с возвратом ID вставленной записи
  PROCEDURE RSI_InsertGrDealRet( pDocKind IN NUMBER, pDocID IN NUMBER, pID_Operation IN NUMBER, pID_Step IN NUMBER, pTemplNum IN NUMBER, pDate IN DATE, pTime IN DATE DEFAULT TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS'), pFIID IN NUMBER, pGrDealID OUT NUMBER )
  AS
    vTemplNum NUMBER;
    vCalc CHAR(1);
  BEGIN

    BEGIN
      SELECT T_NUM INTO vTemplNum 
        FROM DDLGRTEMPL_DBT
       WHERE T_NUM = pTemplNum;

      EXCEPTION WHEN NO_DATA_FOUND THEN SetError( GR_ERROR_20903, TO_CHAR(pTemplNum) );
    END;

    pGrDealID := 0;

    TRGPCKG_DDLGRDEAL_DBT_TAI.v_ID_Operation := pID_Operation;
    TRGPCKG_DDLGRDEAL_DBT_TAI.v_ID_Step      := pID_Step;
    TRGPCKG_DDLGRDEAL_DBT_TAI.v_State        := -1;

    IF( (pID_Operation > 0) and (pID_Step > 0) ) THEN
       IF( pTemplNum = DLGR_TEMPL_DELIVERY2 ) THEN -- Если РЕПО на корзину, бумаги в обеспечении не было, то на шаге пополнения обеспечения создаём ТО по поставке 2ч и соотв. строки графика по 2ч. БОУ по всему что вставляется на шаге операции - всегда Ф, а тут для поставки 2ч исключение:
          TRGPCKG_DDLGRDEAL_DBT_TAI.v_State := DLGRACC_STATE_PLAN;
       ELSIF( pTemplNum = DLGR_TEMPL_RECDELIVERY2 
           or pTemplNum = DLGR_TEMPL_DEPODRAFT2 
           or pTemplNum = DLGR_TEMPL_CHANGEMSG 
           or pTemplNum = DLGR_TEMPL_CHANGEMSGWTHREQUEST 
           or pTemplNum = DLGR_TEMPL_EXECHOLDMSG 
           or pTemplNum = DLGR_TEMPL_EXECHOLDMSGREQUEST
           or pTemplNum = DLGR_TEMPL_EXECDELAYMSG 
           or pTemplNum = DLGR_TEMPL_EXECDELAYMSGREQUEST 
           or pTemplNum = DLGR_TEMPL_EARLYEXECMSG 
           or pTemplNum = DLGR_TEMPL_EARLYEXECMSGREQUEST
           or pTemplNum = DLGR_TEMPL_REJECTIONMSG 
           or pTemplNum = DLGR_TEMPL_REJECTIONMSGREQUEST

           or pTemplNum = DLGR_TEMPL_NETTINGSUSP 
           or pTemplNum = DLGR_TEMPL_NETTINGSUSPREQUEST
           or pTemplNum = DLGR_TEMPL_MAKECONTRACT 
           or pTemplNum = DLGR_TEMPL_MAKECONTRACTREQUEST
           or pTemplnum = DLGR_TEMPL_CSA_MARGIN_PAYMENT
           or pTemplnum = DLGR_TEMPL_FAIRVALUE

           ) THEN
          TRGPCKG_DDLGRDEAL_DBT_TAI.v_State := DLGRACC_STATE_NOTNEED;
       END IF;
    END IF;

    INSERT INTO DDLGRDEAL_DBT (T_ID, T_DOCKIND, T_DOCID, T_TEMPLNUM, T_PLANDATE, T_PLANTIME, T_FIID)
                       VALUES (0, pDocKind, pDocID, pTemplNum, pDate, pTime, pFIID)
                       RETURNING t_ID INTO pGrDealID;

    TRGPCKG_DDLGRDEAL_DBT_TAI.v_ID_Operation := 0;
    TRGPCKG_DDLGRDEAL_DBT_TAI.v_ID_Step      := 0;
    TRGPCKG_DDLGRDEAL_DBT_TAI.v_State        := -1;

  END RSI_InsertGrDealRet;

  --Процедура вставки строки графика исполнения сделки на шаге операции
  PROCEDURE RSI_InsertGrDeal( pDocKind IN NUMBER, pDocID IN NUMBER, pID_Operation IN NUMBER, pID_Step IN NUMBER, pTemplNum IN NUMBER, pDate IN DATE, pTime IN DATE DEFAULT TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS'), pFIID IN NUMBER, pGUID IN VARCHAR DEFAULT CHR(1) )
  AS
    pGrDealID NUMBER;
  BEGIN
    RSI_InsertGrDealRet(pDocKind, pDocID, pID_Operation, pID_Step, pTemplNum, pDate, pTime, pFIID, pGrDealID);
    IF pGUID <> CHR(1) AND pGrDealID > 0 THEN
        FOR one_doctmp IN (SELECT * FROM DDLGRDOC_TMP WHERE T_GUID = pGUID)
        LOOP
           RSI_SetDocGrDeal( pGrDealID, one_doctmp.t_DocKind, one_doctmp.t_DocID, 0, 0, 0, one_doctmp.t_SourceType);
        END LOOP;
    END IF;
  END RSI_InsertGrDeal;

  --Процедура отката вставки строки графика исполнения сделки на шаге операции
  PROCEDURE RSI_BackInsertGrDeal( pDocKind IN NUMBER, pDocID IN NUMBER, pID_Operation IN NUMBER, pID_Step IN NUMBER, pTemplNum IN NUMBER, pFIID IN NUMBER)
  AS
    v_N NUMBER := 0;
  BEGIN

    IF pID_Operation = 0 OR pID_Step = 0 THEN
      SetError( GR_ERROR_20908, ''); --Неверный запуск процедуры отката вставки строки графика
    END IF;

    SELECT Count(1) INTO v_N
      FROM DDLGRDEAL_DBT GD
     WHERE GD.T_DOCKIND = pDocKind
       AND GD.T_DOCID = pDocID
       AND GD.T_FIID = pFIID
       AND GD.T_TEMPLNUM = pTemplNum
       AND EXISTS(SELECT 1 FROM DDLGRACC_DBT ACC, DDLGRACCBC_DBT ACCBC
                   WHERE ACC.T_GRDEALID = GD.T_ID
                     AND ACCBC.T_GRACCID = ACC.T_ID
                     AND ACCBC.T_ID_OPERATION = pID_Operation
                     AND ACCBC.T_ID_STEP = pID_Step);

    IF v_N > 0 THEN
      SetError( GR_ERROR_20900, ''); --Ошибка последовательности отката изменений графика по сделке
    END IF;

    FOR one_grdeal IN ( SELECT GD.T_ID,
                               (SELECT Count(1) FROM DDLGRACC_DBT ACC1 
                                 WHERE ACC1.T_GRDEALID = GD.T_ID 
                                   AND ACC1.T_STATE = DLGRACC_STATE_FACTEXEC 
                                   AND ACC1.T_ACCNUM <> DLGR_ACCKIND_BACKOFFICE ) AS T_CNT
                          FROM DDLGRDEAL_DBT GD
                         WHERE GD.T_DOCKIND = pDocKind
                           AND GD.T_DOCID = pDocID
                           AND GD.T_FIID = pFIID
                           AND GD.T_TEMPLNUM = pTemplNum
                           AND EXISTS(SELECT 1 FROM DDLGRACC_DBT ACC
                                       WHERE ACC.T_GRDEALID = GD.T_ID
                                         AND ACC.T_ID_OPERATION = pID_Operation
                                         AND ACC.T_ID_STEP = pID_Step)
                      )
    LOOP

      IF one_grdeal.T_CNT > 0 THEN
        SetError( GR_ERROR_20909, ''); --Ошибка отката вставки строки графика - есть исполненные действия
      END IF;

      DELETE FROM DDLGRDEAL_DBT WHERE T_ID = one_grdeal.T_ID;

    END LOOP;

  END RSI_BackInsertGrDeal;


  --Процедура удаления строки графика исполнения сделки
  PROCEDURE RSI_DeleteGrDeal( pDocKind IN NUMBER, pDocID IN NUMBER, pTemplNum IN NUMBER, pDate IN DATE )
  AS
  BEGIN

    DELETE FROM DDLGRDEAL_DBT GRDEAL
     WHERE GRDEAL.T_DOCKIND = pDocKind
       AND GRDEAL.T_DOCID = pDocID
       AND GRDEAL.T_TEMPLNUM = pTemplNum
       AND GRDEAL.T_PLANDATE = pDate
       AND NOT EXISTS (SELECT 1 FROM DDLGRACC_DBT ACC WHERE ACC.T_GRDEALID = GRDEAL.T_ID AND (ACC.T_STATE = DLGRACC_STATE_FACTEXEC OR ACC.T_INSTANCE > 0));

     -- DLGRACC будут удалены в триггере

  END RSI_DeleteGrDeal;

  PROCEDURE RSI_ExecCommitDLGR
  AS

  BEGIN
    IF g_gracc_ins IS NOT EMPTY THEN
      FORALL i IN g_gracc_ins.FIRST .. g_gracc_ins.LAST
           INSERT INTO ddlgracc_dbt
                VALUES g_gracc_ins (i);

      g_gracc_ins.delete;
    END IF;

    IF g_graccbc_ins IS NOT EMPTY THEN
      FORALL i IN g_graccbc_ins.FIRST .. g_graccbc_ins.LAST
           INSERT INTO ddlgraccbc_dbt
                VALUES g_graccbc_ins (i);

      g_graccbc_ins.delete;
    END IF;

    IF g_grdeal_upd IS NOT EMPTY THEN

      forall i in g_grdeal_upd.first .. g_grdeal_upd.last
       update ddlgrdeal_dbt
          set T_PLANDATE = g_grdeal_upd(i).T_PLANDATE,
              T_PLANTIME = g_grdeal_upd(i).T_PLANTIME  
        where t_ID = g_grdeal_upd(i).t_ID;

      g_grdeal_upd.delete;
    END IF;

    IF g_gracc_upd IS NOT EMPTY THEN

      forall i in g_gracc_upd.first .. g_gracc_upd.last
       update ddlgracc_dbt
          set T_FACTDATE = g_gracc_upd(i).T_FACTDATE, 
              T_STATE    = g_gracc_upd(i).T_STATE,
              T_INSTANCE = g_gracc_upd(i).T_INSTANCE,
              T_ID_OPERATION = g_gracc_upd(i).T_ID_OPERATION,
              T_ID_STEP  = g_gracc_upd(i).T_ID_STEP
        where t_ID = g_gracc_upd(i).t_ID;

      g_gracc_upd.delete;
    END IF;

    IF g_dlgrdoc_ins IS NOT EMPTY THEN
      FORALL i IN g_dlgrdoc_ins.FIRST .. g_dlgrdoc_ins.LAST
           INSERT INTO ddlgrdoc_dbt
                VALUES g_dlgrdoc_ins (i);

      g_dlgrdoc_ins.delete;
    END IF;

  END;



  --Процедура обновления информации о состоянии учёта по событию графика исполнения сделки, на шаге операции
  PROCEDURE RSI_UpdateGrDealAcc( pDocKind IN NUMBER, 
                                 pDocID IN NUMBER, 
                                 pFIID IN NUMBER,
                                 pTemplNum IN NUMBER, 
                                 pAccNum IN NUMBER,
                                 pID_Operation IN NUMBER,
                                 pID_Step IN NUMBER,
                                 pPlanDate IN DATE,
                                 pState IN NUMBER,
                                 pExecCommit IN NUMBER DEFAULT 1
                               ) 
  AS
    v_PlanDate  DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
    v_FactDate  DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
    v_GrDealID  NUMBER := 0;
    v_ExistsAcc BOOLEAN := FALSE;
    v_State     NUMBER;
    v_N         NUMBER := 0;
    v_PlanTime DATE;
    p_PlanTime DATE;

    v_GrDeal    ddlgrdeal_dbt%rowtype;
    v_GrAcc     ddlgracc_dbt%rowtype;
    v_GrAccBc   ddlgraccbc_dbt%rowtype;
  BEGIN

    SELECT Count(1) INTO v_N
      FROM DDLGRDEAL_DBT GD
     WHERE GD.T_DOCKIND  = pDocKind
       AND GD.T_DOCID    = pDocID
       AND GD.T_FIID     = pFIID
       AND GD.T_TEMPLNUM = pTemplNum
       AND NOT EXISTS (SELECt 1 FROM DDLGRACC_DBT ACC WHERE ACC.T_GRDEALID = GD.T_ID AND ACC.T_ACCNUM = pAccNum AND ACC.T_STATE = DLGRACC_STATE_FACTEXEC);
    
    IF v_N > 1 THEN
      SetError( GR_ERROR_20913, ''); --Ошибка По сделке найдено более одной строки графика одного вида к обработке
    ELSIF v_N = 0 THEN -- обрабатывать нечего
      RETURN;
    END IF;
    
    BEGIN
      SELECT GD.T_PLANDATE, GD.T_ID, GD.T_PLANTIME INTO v_PlanDate, v_GrDealID, v_PlanTime
        FROM DDLGRDEAL_DBT GD
       WHERE GD.T_DOCKIND  = pDocKind
         AND GD.T_DOCID    = pDocID
         AND GD.T_FIID     = pFIID
         AND GD.T_TEMPLNUM = pTemplNum
         AND NOT EXISTS (SELECT 1 FROM DDLGRACC_DBT ACC WHERE ACC.T_GRDEALID = GD.T_ID AND ACC.T_ACCNUM = pAccNum AND ACC.T_STATE = DLGRACC_STATE_FACTEXEC);
    END;

    IF pState = DLGRACC_STATE_FACTEXEC THEN
      v_FactDate := v_PlanDate;
    END IF;

    BEGIN
       SELECT ACC.* INTO v_GrAcc
         FROM DDLGRACC_DBT ACC 
        WHERE ACC.T_GRDEALID = v_GrDealID
          AND ACC.T_ACCNUM = pAccNum;

       v_ExistsAcc := TRUE;

       EXCEPTION WHEN NO_DATA_FOUND THEN v_ExistsAcc := FALSE;
    END;

    IF v_ExistsAcc = FALSE THEN

       v_GrAcc.T_ID           := 0;
       v_GrAcc.T_GRDEALID     := v_GrDealID;
       v_GrAcc.T_ACCNUM       := pAccNum;
       v_GrAcc.T_STATE        := pState;
       v_GrAcc.T_FACTDATE     := v_FactDate;
       v_GrAcc.T_INSTANCE     := 0;
       v_GrAcc.T_ID_OPERATION := pID_Operation;
       v_GrAcc.T_ID_STEP      := pID_Step;

       g_gracc_ins.extend;
       g_gracc_ins(g_gracc_ins.LAST) := v_GrAcc;

    ELSE
       IF v_GrAcc.T_ID_OPERATION = pID_Operation AND v_GrAcc.T_ID_STEP = pID_Step THEN
          SetError( GR_ERROR_20904, ''); --Неверные параметры: данные вида учёта по графику уже изменены на этом шаге
       END IF;

       v_GrAccBc.T_ID           := 0;
       v_GrAccBc.T_GRACCID      := v_GrAcc.T_ID;
       v_GrAccBc.T_PLANDATE     := v_PlanDate;
       v_GrAccBc.T_FACTDATE     := v_GrAcc.T_FactDate;
       v_GrAccBc.T_STATE        := v_GrAcc.T_State;
       v_GrAccBc.T_INSTANCE     := v_GrAcc.T_Instance;
       v_GrAccBc.T_ID_OPERATION := v_GrAcc.T_ID_Operation;
       v_GrAccBc.T_ID_STEP      := v_GrAcc.T_ID_Step;

       g_graccbc_ins.extend;
       g_graccbc_ins(g_graccbc_ins.LAST) := v_GrAccBc;
       
       IF pPlanDate <> TO_DATE('01.01.0001','DD.MM.YYYY') THEN
          p_PlanTime := v_PlanTime;
          IF v_FactDate = TO_DATE('31.12.9999','DD.MM.YYYY') THEN
            p_PlanTime := TO_DATE('01.01.0001 '||TO_CHAR( SYSDATE, 'HH24:MI:SS'),'DD.MM.YYYY HH24:MI:SS');
          END IF;  

          IF v_PlanDate <> pPlanDate OR v_PlanTime <> p_PlanTime THEN

            v_GrDeal.T_ID       := v_GrDealID;
            v_GrDeal.T_PLANDATE := pPlanDate;
            v_GrDeal.T_PLANTIME := p_PlanTime;

            g_grdeal_upd.extend;
            g_grdeal_upd(g_grdeal_upd.LAST) := v_GrDeal;
          END IF;

       END IF;

       v_State := v_GrAcc.T_STATE;
       IF pState >= DLGRACC_STATE_NOTNEED THEN
         v_State := pState;
       END IF;

       IF v_FactDate = TO_DATE('31.12.9999','DD.MM.YYYY') THEN
          v_FactDate := pPlanDate;
       END IF;

       v_GrAcc.T_FACTDATE := v_FactDate;
       v_GrAcc.T_STATE    := v_State;
       v_GrAcc.T_INSTANCE := v_GrAcc.T_INSTANCE + 1;
       v_GrAcc.T_ID_OPERATION := pID_Operation;
       v_GrAcc.T_ID_STEP  := pID_Step;

       g_gracc_upd.extend;
       g_gracc_upd(g_gracc_upd.LAST) := v_GrAcc;

    END IF;

    IF pExecCommit > 0 THEN
      RSI_ExecCommitDLGR;
    END IF;

  END RSI_UpdateGrDealAcc;

  --Процедура отката обновления информации о состоянии учёта по событию графика исполнения сделки
  PROCEDURE RSI_BackUpdateGrDealAcc( pDocKind IN NUMBER, 
                                     pDocID IN NUMBER,
                                     pFIID IN NUMBER,
                                     pTemplNum IN NUMBER,
                                     pAccNum IN NUMBER,
                                     pID_Operation IN NUMBER,
                                     pID_Step IN NUMBER
                                   )
  AS
    v_N NUMBER := 0;
    v_GrAccID NUMBER := 0;
    v_GrDealID NUMBER := 0;
    v_ExistExec BOOLEAN := FALSE;
    v_PlanDate DATE;

    v_GrAccBc     ddlgraccbc_dbt%rowtype;
  BEGIN

    SELECT Count(1) INTO v_N
      FROM DDLGRACC_DBT ACC, DDLGRACCBC_DBT ACCBC, DDLGRDEAL_DBT GRDEAL
     WHERE GRDEAL.T_DOCKIND  = pDocKind
       AND GRDEAL.T_DOCID    = pDocID
       AND GRDEAL.T_FIID     = pFIID
       AND GRDEAL.T_TEMPLNUM = pTemplNum
       AND ACC.T_GRDEALID    = GRDEAL.T_ID
       AND ACC.T_ACCNUM      = pAccNum
       AND ACCBC.T_GRACCID   = ACC.T_ID
       AND ACCBC.T_ID_OPERATION = pID_Operation
       AND ACCBC.T_ID_STEP   = pID_Step
       AND ROWNUM = 1;

    IF v_N > 0 THEN
       SetError( GR_ERROR_20900, ''); --Ошибка последовательности отката изменений графика по сделке
    END IF;

    FOR one_gracc IN (SELECT ACC.*
                        FROM DDLGRACC_DBT ACC, DDLGRDEAL_DBT GD
                       WHERE ACC.T_ID_OPERATION = pID_Operation
                         AND ACC.T_ID_STEP = pID_Step
                         AND ACC.T_ACCNUM = pAccNum
                         AND GD.T_ID = ACC.T_GRDEALID
                         AND GD.T_DOCKIND = pDocKind
                         AND GD.T_DOCID = pDocID
                         AND GD.T_FIID = pFIID
                         AND GD.T_TEMPLNUM = pTemplNum
                     )
    LOOP
      v_GrAccID := one_gracc.T_ID;
      v_GrDealID := one_gracc.T_GRDEALID;
      v_ExistExec := FALSE;

      IF pAccNum = DLGR_ACCKIND_BACKOFFICE THEN
        SELECT Count(1) INTO v_N
          FROM DDLGRACC_DBT
         WHERE T_GRDEALID = v_GrDealID
           AND T_STATE = DLGRACC_STATE_FACTEXEC
           AND T_ACCNUM <> DLGR_ACCKIND_BACKOFFICE AND ROWNUM = 1;

        IF v_N > 0 THEN
          v_ExistExec := TRUE;
        END IF;     

      END IF;

      IF one_gracc.T_INSTANCE = 0 THEN
        DELETE FROM DDLGRACC_DBT WHERE T_ID = v_GrAccID;
      ELSE
        v_GrAccBc := NULL;

        IF(GetDLGRACCBC( v_GrAccID, one_gracc.T_INSTANCE-1, v_GrAccBc ) != 0) THEN
          SetError( GR_ERROR_20905, ''); --Ошибка отката изменения графика на шаге. Данные по виду учёта графика не найдены
        ELSE
        
          IF v_ExistExec = TRUE THEN
            SELECT T_PLANDATE INTO v_PlanDate
              FROM DDLGRDEAL_DBT
             WHERE T_ID = v_GrDealID;

            IF v_PlanDate <> v_GrAccBc.T_PLANDATE THEN
              SetError( GR_ERROR_20906, ''); --При попытке отката изменения плановой даты, найдены выполненные дейстия по другим видам учёта строки графика
            END IF;

            IF one_gracc.T_STATE <> v_GrAccBc.T_STATE AND one_gracc.T_STATE = DLGRACC_STATE_FACTEXEC THEN
              SetError( GR_ERROR_20907, ''); --При попытке отката изменения статуса, найдены выполненные дейстия по другим видам учёта строки графика
            END IF;

          END IF;

          UPDATE DDLGRDEAL_DBT
             SET T_PLANDATE = v_GrAccBc.T_PLANDATE
           WHERE T_ID = v_GrDealID;

          UPDATE DDLGRACC_DBT
             SET T_FACTDATE = v_GrAccBc.T_FACTDATE,
                 T_STATE = v_GrAccBc.T_STATE,
                 T_INSTANCE = v_GrAccBc.T_INSTANCE,
                 T_ID_OPERATION = v_GrAccBc.T_ID_OPERATION,
                 T_ID_STEP = v_GrAccBc.T_ID_STEP
           WHERE T_ID = v_GrAccID;

          DELETE FROM DDLGRACCBC_DBT WHERE T_ID = v_GrAccBc.T_ID;


        END IF;

      END IF;

    END LOOP;

  END RSI_BackUpdateGrDealAcc;


  --Процедура установки даты по сделке на шаге операции, с сохранением информации для отката
  PROCEDURE RSI_SetDateGrDeal( pDocKind IN NUMBER, 
                               pDocID IN NUMBER, 
                               pID_Operation IN NUMBER,
                               pID_Step IN NUMBER,
                               pDateKind IN NUMBER,
                               pDate IN DATE
                             )
  AS
    v_N NUMBER := 0;
  BEGIN

    SELECT Count(1) INTO v_N
      FROM DDLGRDEAL_DBT GD, DDLGRTEMPL_DBT T
     WHERE GD.T_DOCKIND = pDocKind
       AND GD.T_DOCID = pDocID
       AND T.T_NUM = GD.T_TEMPLNUM
       AND T.T_DATEKIND = pDateKind
       AND EXISTS(SELECT 1 FROM DDLGRACC_DBT ACC WHERE ACC.T_GRDEALID = GD.T_ID AND ACC.T_STATE = DLGRACC_STATE_FACTEXEC);

    IF v_N > 0 THEN
      SetError( GR_ERROR_20910, ''); --Существуют исполненные учётные действия в изменяемую дату
    END IF;

    FOR one_rec IN (SELECT GD.T_TEMPLNUM, GD.T_FIID
                         FROM DDLGRDEAL_DBT GD, DDLGRTEMPL_DBT T
                        WHERE GD.T_DOCKIND = pDocKind
                          AND GD.T_DOCID = pDocID
                          AND T.T_NUM = GD.T_TEMPLNUM
                          AND T.T_DATEKIND = pDateKind)
    LOOP

      RSI_UpdateGrDealAcc(pDocKind, pDocID, one_rec.T_FIID, one_rec.T_TEMPLNUM, DLGR_ACCKIND_BACKOFFICE, pID_Operation, pID_Step, pDate, -1);

    END LOOP;

  END RSI_SetDateGrDeal;


  --Процедура отката изменения даты по сделке на шаге операции
  PROCEDURE RSI_BackSetDateGrDeal( pDocKind IN NUMBER, 
                                   pDocID IN NUMBER, 
                                   pID_Operation IN NUMBER,
                                   pID_Step IN NUMBER,
                                   pDateKind IN NUMBER
                                 )
  AS
  BEGIN
    FOR one_rec IN (SELECT GD.T_TEMPLNUM, GD.T_FIID
                      FROM DDLGRDEAL_DBT GD, DDLGRTEMPL_DBT T
                     WHERE GD.T_DOCKIND = pDocKind
                       AND GD.T_DOCID = pDocID
                       AND T.T_NUM = GD.T_TEMPLNUM
                       AND T.T_DATEKIND = pDateKind)
    LOOP

      RSI_BackUpdateGrDealAcc(pDocKind, pDocID, one_rec.T_FIID, one_rec.T_TEMPLNUM, DLGR_ACCKIND_BACKOFFICE, pID_Operation, pID_Step);

    END LOOP;
  END RSI_BackSetDateGrDeal;


  --Процедура вставки информации о проводке на шаге операции, с привязкой к графику
  PROCEDURE RSI_SetDocGrDeal( pGrDealID IN NUMBER, 
                              pDocKind IN NUMBER,
                              pDocID IN NUMBER,
                              pServDocKind IN NUMBER,
                              pServDocID IN NUMBER,
                              pGrpID IN NUMBER DEFAULT 0,
                              pSourceType IN NUMBER DEFAULT 0,
                              pExecCommit IN NUMBER DEFAULT 1
                             )
  AS
    v_GrDoc    ddlgrdoc_dbt%rowtype;
  BEGIN

    v_GrDoc.T_ID          := 0;           
    v_GrDoc.T_GRDEALID    := pGrDealID;   
    v_GrDoc.T_DOCKIND     := pDocKind;    
    v_GrDoc.T_DOCID       := pDocID;      
    v_GrDoc.T_SERVDOCKIND := pServDocKind;
    v_GrDoc.T_SERVDOCID   := pServDocID;  
    v_GrDoc.T_GRPID       := pGrpID;      
    v_GrDoc.T_SOURCETYPE  := pSourceType; 

    g_dlgrdoc_ins.extend;
    g_dlgrdoc_ins(g_dlgrdoc_ins.LAST) := v_GrDoc;

    IF pExecCommit <> 0 THEN
      RSI_ExecCommitDLGR;
    END IF;

  END RSI_SetDocGrDeal;

  --Процедура удаления информации о документе при откате шага операции
  PROCEDURE RSI_BackDocGrDeal( pGrDealID IN NUMBER, 
                               pDocKind IN NUMBER,
                               pDocID IN NUMBER,
                               pServDocKind IN NUMBER,
                               pServDocID IN NUMBER,
                               pGrpID IN NUMBER DEFAULT 0,
                               pSourceType IN NUMBER DEFAULT 0
                              )
  AS
  BEGIN

    DELETE FROM ddlgrdoc_dbt
     WHERE T_GRDEALID    = pGrDealID   
       AND T_DOCKIND     = pDocKind    
       AND T_DOCID       = pDocID      
       AND T_SERVDOCKIND = pServDocKind
       AND T_SERVDOCID   = pServDocID  
       AND T_GRPID       = pGrpID      
       AND T_SOURCETYPE  = pSourceType; 

  END RSI_BackDocGrDeal;

  --Процедура обновления информации о состоянии учёта по событию графика исполнения сделки по конкретной строке графика, без привязки изменения к шагу операции
  PROCEDURE RSI_UpdateGrDealAccByID( pGrDealID IN NUMBER, 
                                     pAccNum IN NUMBER,
                                     pState IN NUMBER,
                                     pPlanDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                     pExecCommit IN NUMBER DEFAULT 1
                                   ) 
  AS
    v_PlanDate  DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
    v_FactDate  DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
    v_State     NUMBER;

    v_GrDeal    ddlgrdeal_dbt%rowtype;
    v_GrAcc     ddlgracc_dbt%rowtype;
    v_GrAccBc   ddlgraccbc_dbt%rowtype;

    v_GrAccExists NUMBER;
  BEGIN

    SELECT * INTO v_GrDeal
      FROM ddlgrdeal_dbt
     WHERE t_ID = pGrDealID;

    v_PlanDate := v_GrDeal.t_PlanDate;

    BEGIN
      v_GrAccExists := 1;

      SELECT * INTO v_GrAcc
        FROM ddlgracc_dbt
       WHERE t_GrDealID = pGrDealID
         AND t_AccNum = pAccNum;

      EXCEPTION WHEN NO_DATA_FOUND THEN v_GrAccExists := 0;
    END;

    IF v_GrAccExists = 0 THEN
      
      IF pState <> DLGRACC_STATE_FACTEXEC THEN
        v_PlanDate := TO_DATE('01.01.0001','DD.MM.YYYY');
      END IF;

      v_GrAcc.T_ID           := 0;
      v_GrAcc.T_GRDEALID     := pGrDealID;
      v_GrAcc.T_ACCNUM       := pAccNum;
      v_GrAcc.T_STATE        := pState;
      v_GrAcc.T_FACTDATE     := v_PlanDate;
      v_GrAcc.T_INSTANCE     := 0;
      v_GrAcc.T_ID_OPERATION := 0;
      v_GrAcc.T_ID_STEP      := 0;

      g_gracc_ins.extend;
      g_gracc_ins(g_gracc_ins.LAST) := v_GrAcc;

    ELSE
      IF pState = v_GrAcc.t_State THEN 
        SetError( GR_ERROR_20919, ''); --Ошибка при изменении статуса вида учета: ранее это значение уже было установлено
      END IF;

      v_GrAccBc.T_ID           := 0;
      v_GrAccBc.T_GRACCID      := v_GrAcc.T_ID;
      v_GrAccBc.T_PLANDATE     := v_PlanDate;
      v_GrAccBc.T_FACTDATE     := v_GrAcc.T_FactDate;
      v_GrAccBc.T_STATE        := v_GrAcc.T_State;
      v_GrAccBc.T_INSTANCE     := v_GrAcc.T_Instance;
      v_GrAccBc.T_ID_OPERATION := v_GrAcc.T_ID_Operation;
      v_GrAccBc.T_ID_STEP      := v_GrAcc.T_ID_Step;

      g_graccbc_ins.extend;
      g_graccbc_ins(g_graccbc_ins.LAST) := v_GrAccBc;


      IF pPlanDate <> TO_DATE('01.01.0001','DD.MM.YYYY') AND v_GrDeal.t_PlanDate <> pPlanDate THEN
        v_GrDeal.T_PLANDATE := pPlanDate;

        g_grdeal_upd.extend;
        g_grdeal_upd(g_grdeal_upd.LAST) := v_GrDeal;
      END IF;

      IF pState = DLGRACC_STATE_FACTEXEC THEN
        v_FactDate := v_PlanDate;
      END IF;

      v_State := v_GrAcc.T_State;
      IF pState >= 0 THEN
        v_State := pState;
      END IF;

      v_GrAcc.T_FACTDATE := v_FactDate;
      v_GrAcc.T_STATE    := v_State;
      v_GrAcc.T_INSTANCE := v_GrAcc.T_INSTANCE + 1;
      v_GrAcc.T_ID_OPERATION := 0;
      v_GrAcc.T_ID_STEP  := 0;

      g_gracc_upd.extend;
      g_gracc_upd(g_gracc_upd.LAST) := v_GrAcc;

    END IF;

    IF pExecCommit <> 0 THEN
      RSI_ExecCommitDLGR;
    END IF;

  END RSI_UpdateGrDealAccByID;

  -- Откатить статус действия по графикам
  PROCEDURE RSI_BackUpdateAllGrDealAcc( pDealID         IN NUMBER,
                                        pDocKind        IN NUMBER,
                                        pCommDocKind    IN NUMBER,
                                        pCommDocumentID IN NUMBER
                                      )
  AS
  BEGIN
     IF( pDealID > 0 ) THEN
        IF( pDocKind = Rsb_Secur.DL_NTGSEC ) THEN
           FOR one_grdeal IN (SELECT grdoc.t_GrDealID
                                FROM ddlgrdoc_dbt grdoc, ddlgrdeal_dbt grdeal
                               WHERE grdoc.t_ServDocKind  = pCommDocKind
                                 AND grdoc.t_ServDocID    = pCommDocumentID
                                 AND grdoc.t_DocKind      = 0
                                 AND grdoc.t_DocID        = 0
                                 AND grdeal.t_ID          = grdoc.t_GrDealID
                                 AND grdeal.t_DocKind     = Rsb_Secur.DL_NTGSEC
                                 AND grdeal.t_DocID       = pDealID
                             ) LOOP
              RSI_DLGR.RSI_BackUpdateGrDealAccByID( one_grdeal.t_GrDealID, DLGR_ACCKIND_INNER, 0);
              RSI_DLGR.RSI_BackUpdateGrDealAccByID(one_grdeal.t_GrDealID, DLGR_ACCKIND_REPOSITORY, 0);
           END LOOP;
        ELSE
           FOR one_grdeal IN (SELECT grdoc.t_GrDealID
                                FROM ddlgrdoc_dbt grdoc, ddlgrdeal_dbt grdeal, ddl_tick_dbt tk
                               WHERE grdoc.t_ServDocKind  = pCommDocKind
                                 AND grdoc.t_ServDocID    = pCommDocumentID
                                 AND grdoc.t_DocKind      = 0
                                 AND grdoc.t_DocID        = 0
                                 AND grdeal.t_ID          = grdoc.t_GrDealID
                                 AND grdeal.t_DocKind     = tk.t_BOfficeKind
                                 AND grdeal.t_DocID       = tk.t_DealID
                                 AND tk.t_DealID          = pDealID
                             ) LOOP
              RSI_DLGR.RSI_BackUpdateGrDealAccByID( one_grdeal.t_GrDealID, DLGR_ACCKIND_INNER, 0);
              RSI_DLGR.RSI_BackUpdateGrDealAccByID(one_grdeal.t_GrDealID, DLGR_ACCKIND_REPOSITORY, 0);
           END LOOP;
        END IF;
     ELSE
        FOR one_grdeal IN (SELECT grdoc.t_GrDealID, grdeal.t_TemplNum
                             FROM ddlgrdoc_dbt grdoc, ddlgrdeal_dbt grdeal
                            WHERE grdoc.t_ServDocKind  = pCommDocKind
                              AND grdoc.t_ServDocID    = pCommDocumentID
                              AND grdoc.t_DocKind      = 0
                              AND grdoc.t_DocID        = 0
                              AND grdeal.t_ID          = grdoc.t_GrDealID
                          ) LOOP
            IF (one_grdeal.t_TemplNum != 65 /*DLGR_TEMPL_RECONCILIATION*/) THEN  -- если не сверка
                RSI_DLGR.RSI_BackUpdateGrDealAccByID( one_grdeal.t_GrDealID, DLGR_ACCKIND_INNER, 0);
            END IF;
           RSI_DLGR.RSI_BackUpdateGrDealAccByID(one_grdeal.t_GrDealID, DLGR_ACCKIND_REPOSITORY, 0);
        END LOOP;
     END IF;
  END RSI_BackUpdateAllGrDealAcc;

  --Процедура отката обновления информации о состоянии учёта по событию графика исполнения сделки
  PROCEDURE RSI_BackUpdateGrDealAccByID( pGrDealID IN NUMBER, 
                                         pAccNum IN NUMBER,
                                         pCanDelete IN NUMBER
                                       )
  AS
    v_GrAccBc     ddlgraccbc_dbt%rowtype;
    v_GrAcc       ddlgracc_dbt%rowtype;

    v_GrAccExists NUMBER;

    v_MaxInstance NUMBER;
  BEGIN

    BEGIN
      v_GrAccExists := 1;

      SELECT * INTO v_GrAcc
        FROM ddlgracc_dbt
       WHERE t_GrDealID = pGrDealID
         AND t_AccNum = pAccNum;

      EXCEPTION WHEN NO_DATA_FOUND THEN v_GrAccExists := 0;
    END;

    IF v_GrAccExists = 1 THEN

      IF v_GrAcc.t_ID_Operation <> 0 AND v_GrAcc.t_ID_Step <> 0 THEN
        SetError( GR_ERROR_20900, ''); --Ошибка последовательности отката изменений графика по сделке
      END IF;

      IF v_GrAcc.t_Instance > 0 THEN
        IF(GetDLGRACCBC( v_GrAcc.t_ID, v_GrAcc.T_INSTANCE-1, v_GrAccBc ) != 0) THEN
          SetError( GR_ERROR_20905, ''); --Ошибка отката изменения графика на шаге. Данные по виду учёта графика не найдены
        ELSE
          UPDATE ddlgrdeal_dbt 
             SET t_PlanDate = v_GrAccBc.t_PlanDate
           WHERE t_ID = pGrDealID;

          UPDATE DDLGRACC_DBT
             SET T_FACTDATE = v_GrAccBc.T_FACTDATE,
                 T_STATE = v_GrAccBc.T_STATE,
                 T_INSTANCE = v_GrAccBc.T_INSTANCE,
                 T_ID_OPERATION = v_GrAccBc.T_ID_OPERATION,
                 T_ID_STEP = v_GrAccBc.T_ID_STEP
           WHERE T_ID = v_GrAcc.t_ID;

          IF pCanDelete <> 0 THEN
            v_GrAcc.T_FACTDATE := v_GrAccBc.T_FACTDATE;         
            v_GrAcc.T_STATE := v_GrAccBc.T_STATE;              
            v_GrAcc.T_INSTANCE := v_GrAccBc.T_INSTANCE;        
            v_GrAcc.T_ID_OPERATION := v_GrAccBc.T_ID_OPERATION;
            v_GrAcc.T_ID_STEP := v_GrAccBc.T_ID_STEP;           
          END IF;
           
          DELETE FROM DDLGRACCBC_DBT WHERE T_ID = v_GrAccBc.T_ID;
          
        END IF;

      ELSIF v_GrAcc.t_Instance = 0 and pCanDelete = 0 THEN

        DELETE FROM DDLGRACC_DBT WHERE T_ID = v_GrAcc.t_ID;

      END IF;

      IF pCanDelete <> 0 THEN
        IF v_GrAcc.T_INSTANCE = 0 AND v_GrAcc.T_ID_OPERATION = 0 AND v_GrAcc.T_ID_STEP = 0 THEN
          v_MaxInstance := 0;

          SELECT NVL(Max(t_Instance), 0) INTO v_MaxInstance
            FROM ddlgracc_dbt
           WHERE t_GrDealID = pGrDealID;

          IF v_MaxInstance > 0 THEN
            SetError( GR_ERROR_20900, ''); --Ошибка последовательности отката изменений графика по сделке
          ELSE
            DELETE FROM ddlgrdeal_dbt WHERE t_ID = pGrDealID;
          END IF;
        END IF;
      END IF;

    END IF;

  END RSI_BackUpdateGrDealAccByID;

    --Обновить сумму по документу. Зовётся при выполнении сервисной операции БУ
  PROCEDURE SetDLSUM(p_DocKind IN NUMBER,
                     p_DocID IN NUMBER,
                     p_Kind IN NUMBER,
                     p_Currency IN NUMBER,
                     p_Date IN DATE,
                     p_Sum IN NUMBER,
                     p_NDS IN NUMBER,
                     p_GrpID IN NUMBER,
                     p_FIID IN NUMBER
                    )
  AS
    v_ExistDLSUM BOOLEAN := TRUE;
    v_ExistDLSUMHIST BOOLEAN := TRUE;
    v_dlsum ddlsum_dbt%rowtype;
    v_dlsumhist ddlsumhist_dbt%rowtype;
  BEGIN

    BEGIN
      SELECT * INTO v_dlsum
        FROM ddlsum_dbt
       WHERE t_DocKind = p_DocKind
         AND t_DocID = p_DocID
         AND t_Kind = p_Kind
         AND t_Date = p_Date
         AND t_Currency = p_Currency
         AND t_FIID = p_FIID;

      EXCEPTION
           WHEN OTHERS THEN v_ExistDLSUM := FALSE;
    END;

    IF v_ExistDLSUM = TRUE THEN
      BEGIN
        SELECT * INTO v_dlsumhist
          FROM ddlsumhist_dbt
         WHERE t_DlSumID = v_dlsum.t_DlSumID
           AND t_GrpID = v_dlsum.t_GrpID;

        EXCEPTION
           WHEN OTHERS THEN v_ExistDLSUMHIST := FALSE;
      END;

      IF v_ExistDLSUMHIST = FALSE THEN
        INSERT INTO DDLSUMHIST_DBT (T_ID, T_DLSUMID, T_SUM, T_NDS, T_INSTANCE, T_GRPID)
                            VALUES (0, v_dlsum.t_DlSumID, v_dlsum.t_Sum, v_dlsum.t_NDS, v_dlsum.t_Instance, v_dlsum.t_GrpID);
      END IF;

      UPDATE ddlsum_dbt
         SET t_Sum = v_dlsum.t_Sum + p_Sum,
             t_NDS = v_dlsum.t_NDS + p_NDS,
             t_Instance = v_dlsum.t_Instance + 1,
             t_GrpID = p_GrpID
       WHERE t_DlSumID = v_dlsum.t_DlSumID;
          
    ELSE
      INSERT INTO DDLSUM_DBT (T_DLSUMID, t_DOCKIND, T_DOCID, T_KIND, T_DATE, T_SUM, T_NDS, T_CURRENCY, T_IMMATERIAL, T_INSTANCE, T_GRPID, T_FIID)
                      VALUES (0, p_DocKind, p_DocID, p_Kind, p_Date, p_Sum, p_NDS, p_Currency, CHR(0), 0, p_GrpID, p_FIID);
    END IF;
  END SetDLSUM;

  --Откат всех действий с DLSUM по группе
  PROCEDURE BackSetDLSUM(p_GrpID IN NUMBER)
  AS
    v_ExistDLSUMHIST BOOLEAN := TRUE;
    v_dlsumhist ddlsumhist_dbt%rowtype;
  BEGIN
    BEGIN
      SELECT * INTO v_dlsumhist
        FROM ddlsumhist_dbt
       WHERE t_GrpID = p_GrpID;

      EXCEPTION
         WHEN OTHERS THEN v_ExistDLSUMHIST := FALSE;
    END;

    IF v_ExistDLSUMHIST = TRUE THEN
      SetError( GR_ERROR_20911, ''); --Нарушена последовательность отката суммы по ПД
    END IF;

    DELETE FROM DDLSUM_DBT WHERE T_GRPID = p_GrpID AND T_INSTANCE = 0;

    FOR one_dlsum IN (SELECT * FROM DDLSUM_DBT WHERE T_GRPID = p_GrpID)
    LOOP
      BEGIN
        v_ExistDLSUMHIST := TRUE;

        SELECT * INTO v_dlsumhist
          FROM ddlsumhist_dbt
         WHERE t_DlSumID = one_dlsum.t_DlSumID
           AND t_Instance = one_dlsum.t_Instance - 1;

        EXCEPTION
           WHEN OTHERS THEN v_ExistDLSUMHIST := FALSE;
      END;

      IF v_ExistDLSUMHIST = FALSE THEN
        SetError( GR_ERROR_20912, ''); --Не найдена сумма по документу
      ELSE
        UPDATE DDLSUM_DBT
           SET T_SUM = v_dlsumhist.t_SUM,
               T_NDS = v_dlsumhist.t_NDS,
               T_INSTANCE = v_dlsumhist.t_INSTANCE,
               T_GRPID = v_dlsumhist.t_GRPID
         WHERE t_DlSumID = v_dlsumhist.t_DlSumID;

        DELETE FROM ddlsumhist_dbt WHERE T_ID = v_dlsumhist.t_ID;

      END IF;

    END LOOP;

  END BackSetDLSUM;

  --Процедура установки времени по сделке на график исполнения сделки при редактировании времени в панели сделки
  PROCEDURE RSI_SetTimeGrDeal(p_DocKind IN NUMBER, p_DocID IN NUMBER, p_Mode IN NUMBER, p_Time IN DATE )
  AS
    v_N NUMBER := 0;
    v_DateKind INTEGER := 0;
  BEGIN

    BEGIN
    
      SELECT Count(1) INTO v_N
        FROM DDLGRDEAL_DBT GD
       WHERE GD.T_DOCKIND = p_DocKind
         AND GD.T_DOCID = p_DocID;
    END;
    
    IF v_N = 0 THEN
      return; --Нет графика - сделка в отложенных
    END IF;

    IF p_Mode = DLGRDEAL_TIMEMODE_Dz THEN

      UPDATE DDLGRDEAL_DBT GD
         SET GD.T_PLANTIME = p_Time
       WHERE GD.T_DOCKIND = p_DocKind
         AND GD.T_DOCID = p_DocID
         AND (select T.T_NUM
                from DDLGRTEMPL_DBT T
               where T.T_NUM = GD.T_TEMPLNUM
                 and T.T_DATEKIND != DLGR_DATEKIND_DELIVERY
                 and T.T_DATEKIND != DLGR_DATEKIND_DELIVERY2
                 and T.T_DATEKIND != DLGR_DATEKIND_EXECCALC
             ) > 0
         AND not exists( select GA.*
                           from DDLGRACC_DBT GA
                          where GA.T_GRDEALID = GD.T_ID
                            and GA.T_STATE = DLGRACC_STATE_FACTEXEC
                       );

    ELSIF p_Mode = DLGRDEAL_TIMEMODE_Dp or p_Mode = DLGRDEAL_TIMEMODE_Dp2 THEN

      v_DateKind := DLGR_DATEKIND_DELIVERY;
      IF p_Mode = DLGRDEAL_TIMEMODE_Dp2 THEN
         v_DateKind := DLGR_DATEKIND_DELIVERY2;
      END IF;
      
      UPDATE DDLGRDEAL_DBT GD
         SET GD.T_PLANTIME = p_Time
       WHERE GD.T_DOCKIND = p_DocKind
         AND GD.T_DOCID = p_DocID
         AND (select T.T_NUM
                from DDLGRTEMPL_DBT T
               where T.T_NUM = GD.T_TEMPLNUM
                 and T.T_DATEKIND = v_DateKind
             ) > 0
         AND not exists( select GA.*
                           from DDLGRACC_DBT GA
                          where GA.T_GRDEALID = GD.T_ID
                            and GA.T_STATE = DLGRACC_STATE_FACTEXEC
                       );

    END IF;

  END RSI_SetTimeGrDeal;

  --Проверить необходимость установки флага "Неттинг" в СО БУ
  FUNCTION RSI_GetDefaultFlagNetting(p_CommDate DATE) RETURN NUMBER
  IS
    v_Cnt NUMBER := 0;
  BEGIN

    SELECT Count(1) INTO v_Cnt
      FROM DDL_NETT_DBT NTG, DDLGRDEAL_DBT GRDEAL
     WHERE GRDEAL.T_DOCKIND = NTG.T_DOCKIND
       AND GRDEAL.T_DOCID = NTG.T_NETTINGID
       AND GRDEAL.T_PLANDATE = p_CommDate
       AND EXISTS(SELECT 1 FROM DDLGRACC_DBT GRACC WHERE GRACC.T_GRDEALID = GRDEAL.T_ID AND GRACC.T_ACCNUM = DLGR_ACCKIND_ACCOUNTING AND GRACC.T_STATE = DLGRACC_STATE_PLAN)
       AND EXISTS(SELECT 1 FROM DDLGRACC_DBT BOU WHERE BOU.T_GRDEALID = GRDEAL.T_ID AND BOU.T_ACCNUM = DLGR_ACCKIND_BACKOFFICE AND BOU.T_STATE <> DLGRACC_STATE_PLAN);

    RETURN v_Cnt;
  END RSI_GetDefaultFlagNetting;

  --Проверить необходимость установки флага "Клиентские комиссии за обороты" в СО БУ
  FUNCTION RSI_GetDefaultFlagClientCom(p_CommDate DATE, p_ClientID NUMBER, p_ContractID NUMBER, p_IsExclude NUMBER) RETURN NUMBER
  IS
    v_Cnt NUMBER := 0;
  BEGIN
                                             
    SELECT Count(1) INTO v_Cnt
      FROM DSFCONTR_DBT CONTR, DDLGRDEAL_DBT GRDEAL
     WHERE GRDEAL.T_PLANDATE = p_CommDate
       AND GRDEAL.T_DOCKIND = 659 --Договор обслуживания
       AND CONTR.T_ID = GRDEAL.T_DOCID
       AND EXISTS(SELECT 1 FROM DDLGRACC_DBT GRACC WHERE GRACC.T_GRDEALID = GRDEAL.T_ID AND GRACC.T_ACCNUM = DLGR_ACCKIND_ACCOUNTING AND GRACC.T_STATE = DLGRACC_STATE_PLAN)
       AND CONTR.T_PARTYID = (CASE WHEN p_IsExclude = 0 AND p_ClientID > 0 THEN p_ClientID ELSE CONTR.T_PARTYID END )
       AND CONTR.T_ID = (CASE WHEN p_IsExclude = 0 AND p_ContractID > 0 THEN p_ContractID ELSE CONTR.T_ID END )
       AND CONTR.T_PARTYID <> (CASE WHEN p_IsExclude <> 0 AND p_ClientID > 0 THEN p_ClientID ELSE -1 END )
       AND CONTR.T_ID <> (CASE WHEN p_IsExclude <> 0 AND p_ContractID > 0 THEN p_ContractID ELSE 0 END );

    RETURN v_Cnt;
  END RSI_GetDefaultFlagClientCom;

  --Проверить необходимость установки флага "Расчеты на бирже" в СО БУ
  FUNCTION RSI_GetDefaultFlagCalcExchange(p_CommDate DATE, 
                                          p_ClientID NUMBER, 
                                          p_ContractID NUMBER, 
                                          p_AvoirKind NUMBER,
                                          p_FIID NUMBER,
                                          p_Currency NUMBER,
                                          p_IsExclude NUMBER) RETURN NUMBER
  IS
    v_Flag NUMBER := 0;
  BEGIN
 
    IF( p_IsExclude = 0 ) THEN
       SELECT /*+ leading(grdeal) index(grdeal ddlgrdeal_dbt_idx3)*/ 1 INTO v_Flag
         FROM DDL_TICK_DBT TK, DDLGRDEAL_DBT GRDEAL, DOPRKOPER_DBT OPR, DFININSTR_DBT FIN
        WHERE GRDEAL.T_PLANDATE = p_CommDate
          AND GRDEAL.T_DOCKIND = TK.T_BOFFICEKIND
          AND GRDEAL.T_DOCID = TK.T_DEALID
          AND TK.T_BOFFICEKIND IN (101, 117, 4830) --Сделка с ц/б или погашение
          AND Exists(SELECT 1 FROM DDLGRACC_DBT GRACC WHERE GRACC.T_GRDEALID = GRDEAL.T_ID AND GRACC.T_ACCNUM = DLGR_ACCKIND_ACCOUNTING AND GRACC.T_STATE = DLGRACC_STATE_PLAN)
          AND Exists(SELECT 1 FROM DDLGRACC_DBT BOU WHERE BOU.T_GRDEALID = GRDEAL.T_ID AND BOU.T_ACCNUM = DLGR_ACCKIND_BACKOFFICE AND BOU.T_STATE <> DLGRACC_STATE_PLAN)
          AND OPR.T_KIND_OPERATION = TK.T_DEALTYPE
          AND RSB_SECUR.IsEXCHANGE(RSB_SECUR.get_OperationGroup(OPR.T_SYSTYPES))=1
          AND OPR.T_DOCKIND = TK.T_BOFFICEKIND
          AND FIN.T_FIID = GRDEAL.T_FIID
          AND (p_ClientID = -1 OR (p_ClientID > 0 AND TK.T_CLIENTID = p_ClientID))
          AND (p_ContractID = 0 OR (p_ContractID > 0 AND TK.T_CLIENTCONTRID = p_ContractID))
          AND (p_AvoirKind = 0 OR (p_AvoirKind > 0 AND (FIN.T_AVOIRKIND = p_AvoirKind OR RSB_FIInstr.FI_AvrKindsEQ(FIN.T_FI_KIND, p_AvoirKind, FIN.T_AVOIRKIND) = 1) ))
          AND (p_FIID = -1 OR (p_FIID <> -1 AND GRDEAL.T_FIID = p_FIID))
          AND (p_Currency = -1 OR (p_Currency <> -1 AND FIN.T_FACEVALUEFI = p_Currency))
          AND ROWNUM = 1;
     ELSE
         SELECT /*+ leading(grdeal) index(grdeal ddlgrdeal_dbt_idx3)*/ 1 INTO v_Flag
           FROM DDL_TICK_DBT TK, DDLGRDEAL_DBT GRDEAL, DOPRKOPER_DBT OPR, DFININSTR_DBT FIN
          WHERE GRDEAL.T_PLANDATE = p_CommDate
            AND GRDEAL.T_DOCKIND = TK.T_BOFFICEKIND
            AND GRDEAL.T_DOCID = TK.T_DEALID
            AND TK.T_BOFFICEKIND IN (101, 117, 4830) --Сделка с ц/б или погашение
            AND Exists(SELECT 1 FROM DDLGRACC_DBT GRACC WHERE GRACC.T_GRDEALID = GRDEAL.T_ID AND GRACC.T_ACCNUM = DLGR_ACCKIND_ACCOUNTING AND GRACC.T_STATE = DLGRACC_STATE_PLAN)
            AND Exists(SELECT 1 FROM DDLGRACC_DBT BOU WHERE BOU.T_GRDEALID = GRDEAL.T_ID AND BOU.T_ACCNUM = DLGR_ACCKIND_BACKOFFICE AND BOU.T_STATE <> DLGRACC_STATE_PLAN)
            AND OPR.T_KIND_OPERATION = TK.T_DEALTYPE
            AND RSB_SECUR.IsEXCHANGE(RSB_SECUR.get_OperationGroup(OPR.T_SYSTYPES))=1
            AND OPR.T_DOCKIND = TK.T_BOFFICEKIND
            AND FIN.T_FIID = GRDEAL.T_FIID
            AND (p_ClientID = -1 OR (p_ClientID > 0 AND TK.T_CLIENTID <> p_ClientID))
            AND (p_ContractID = 0 OR (p_ContractID > 0 AND TK.T_CLIENTCONTRID <> p_ContractID))
            AND (p_AvoirKind = 0 OR (p_AvoirKind > 0 AND (FIN.T_AVOIRKIND <> p_AvoirKind AND RSB_FIInstr.FI_AvrKindsEQ(FIN.T_FI_KIND, p_AvoirKind, FIN.T_AVOIRKIND) <> 1) ))
            AND (p_FIID = -1 OR (p_FIID <> -1 AND GRDEAL.T_FIID <> p_FIID))
            AND (p_Currency = -1 OR (p_Currency <> -1 AND FIN.T_FACEVALUEFI <> p_Currency))
            AND ROWNUM = 1;
     END IF;

     RETURN v_Flag;
      
     EXCEPTION 
        WHEN NO_DATA_FOUND THEN
          RETURN 0;
    
  END RSI_GetDefaultFlagCalcExchange;

  --Получить номер шаблона графика по комиссии
  FUNCTION GetTemplNumByCom(pDocKind IN NUMBER, pDocID IN NUMBER, pCONTRACT IN NUMBER) RETURN NUMBER DETERMINISTIC
  AS
    v_tick     DDL_TICK_DBT%ROWTYPE;
    v_Payer    NUMBER := 0;
    v_TemplNum NUMBER := 0;
   BEGIN

    IF pDocKind = RSB_SECUR.DL_SECURITYDOC THEN 
      BEGIN
        SELECT T_PARTYID into v_Payer
          FROM DSFCONTR_DBT
         WHERE T_ID = pCONTRACT;
      END;

      BEGIN
        SELECT TICK.* INTO v_tick
          FROM DDL_TICK_DBT TICK
         WHERE TICK.T_BOFFICEKIND = pDocKind
           AND TICK.T_DEALID = pDocID;      
      EXCEPTION 
         WHEN NO_DATA_FOUND THEN
           RSI_DLGR.SetError(RSI_DLGR.GR_ERROR_20914,to_char(pDocID));
           return 0;
         WHEN OTHERS THEN
           return 0;
      END;

      IF v_tick.T_ISPARTYCLIENT = 'X' AND v_tick.T_PartyID = v_Payer THEN
        v_TemplNum := DLGR_TEMPL_PAYCOMCONTR;
      ELSE
        v_TemplNum := DLGR_TEMPL_PAYCOM;
      END IF;
    ELSIF pDocKind = RSB_SECUR.DL_RETIREMENT_OWN THEN
      v_TemplNum := DLGR_TEMPL_PAYCOMOWN;
    ELSE
      v_TemplNum := DLGR_TEMPL_PAYCOM;
    END IF;

    return v_TemplNum;

  END GetTemplNumByCom;  

  --Получить, есть ли строки графика нужного статуса по комиссии
  FUNCTION CheckExistGrDealByCom(pDocKind IN NUMBER, pDocID IN NUMBER, pCONTRACT IN NUMBER, pPLANPAYDATE IN DATE, pState IN NUMBER, pTemplNum IN NUMBER DEFAULT 0) RETURN NUMBER DETERMINISTIC
  AS
    v_TemplNum    NUMBER := 0;
    v_N           NUMBER := 0;
    v_ExistGrDeal NUMBER := 0;
   BEGIN

    --если pTemplNum задали
    if pTemplNum = DLGR_TEMPL_PAYCOMCONTR or pTemplNum = DLGR_TEMPL_PAYCOM then
       v_TemplNum := pTemplNum;
    else
       v_TemplNum := GetTemplNumByCom(pDocKind,pDocID,pCONTRACT);
    end if;

    SELECT Count(1) INTO v_N
      FROM DDLGRDEAL_DBT GD
     WHERE GD.T_DOCKIND   = pDocKind
       AND GD.T_DOCID     = pDocID
       AND GD.T_TEMPLNUM  = v_TemplNum
       AND GD.T_PLANDATE  = pPLANPAYDATE 
       AND EXISTS(SELECT 1 FROM DDLGRACC_DBT ACC WHERE ACC.T_GRDEALID = GD.T_ID AND ACC.T_STATE = pState)
       AND ROWNUM = 1;

    if v_N > 0 then
      v_ExistGrDeal  := 1;
    end if;

    return v_ExistGrDeal;

  END CheckExistGrDealByCom;  

  --Функция проверки наличия запланированных строк графика по бумаге до даты
  FUNCTION RSI_ExistPlanGrDealBeforeDate(p_FIID IN NUMBER, p_Date IN DATE) RETURN NUMBER
  AS
    v_N NUMBER := 0;
    v_ExistGrDeal NUMBER := 0;
  BEGIN

/* КД Отключена проверка по графикам ВУ и по покупке\продаже ОЭБ */
/* DEF-47874, запрос изменен (Велигжанин), кроме того созданы индексы:
      CREATE INDEX DDLGRDEAL_DBT_IDX4 ON DDLGRDEAL_DBT (T_FIID, T_PLANDATE, T_DOCKIND, T_DOCID);
      CREATE INDEX DDLGRACC_DBT_IDX4 ON DDLGRACC_DBT (T_STATE, T_ACCNUM);
      CREATE INDEX DDL_TICK_DBT_USRA ON DDL_TICK_DBT (T_CLIENTID, T_BOFFICEKIND, T_DEALID);
*/

   SELECT COUNT(1) INTO v_N
     FROM DDLGRDEAL_DBT d
   WHERE 
     d.t_fiid = p_FIID
     AND d.T_PLANDATE < p_Date
     AND d.T_PLANDATE <> TO_DATE('01.01.0001','DD.MM.YYYY')
     AND (d.t_dockind, d.t_docid) in (
        SELECT /*+INDEX (t DDL_TICK_DBT_USRA) */ 
        t_bofficekind, t_dealid 
        FROM ddl_tick_dbt t 
        WHERE t.t_clientid = -1 and t.t_bofficekind <> 4830 
        -- index on ("T_CLIENTID","T_BOFFICEKIND","T_DEALID"), created by DEF-47874
     )
     AND d.t_id IN (
        SELECT /*+INDEX (a DDLGRACC_DBT_IDX4) cardinality(a 100) */ 
        T_GRDEALID 
        FROM DDLGRACC_DBT a 
        WHERE a.T_STATE = 1 and a.T_accnum != 3
        -- index on ("T_STATE", "T_ACCNUM"), created by DEF-47874
     )
     AND ROWNUM < 2;

--     SELECT /*+ leading(GRACC,GRDEAL,TK) index(GRACC DDLGRACC_DBT_IDX2) index(tk DDL_TICK_DBT_IDX0) cardinality(GRACC 100)*/ Count(1) INTO v_N /* КД*/
--      FROM DDLGRACC_DBT GRACC,DDL_TICK_DBT TK, DDLGRDEAL_DBT GRDEAL, ddl_tick_dbt tick
--     WHERE GRDEAL.T_FIID = p_FIID
--       AND GRDEAL.T_PLANDATE < p_Date
--       AND GRDEAL.T_PLANDATE <> TO_DATE('01.01.0001','DD.MM.YYYY')
--       and tick.t_bofficekind = grdeal.t_dockind
--       AND tick.t_dealid = grdeal.t_docid  
--       and tick.t_clientid = -1 and tick.t_bofficekind <> 4830 
--       AND GRACC.T_GRDEALID = GRDEAL.T_ID AND GRACC.T_STATE = DLGRACC_STATE_PLAN and GRACC.T_accnum != 3/*ВУ*/
--       AND TK.T_BOFFICEKIND = GRDEAL.T_DOCKIND AND TK.T_DEALID = GRDEAL.T_DOCID AND TK.T_CLIENTID = -1
--       AND ROWNUM < 2;

    /*SELECT Count(1) INTO v_N \* КД*\
      FROM DDLGRDEAL_DBT GRDEAL, ddl_tick_dbt tick
     WHERE GRDEAL.T_FIID = p_FIID
       AND GRDEAL.T_PLANDATE < p_Date
       AND GRDEAL.T_PLANDATE <> TO_DATE('01.01.0001','DD.MM.YYYY')
       and tick.t_bofficekind = grdeal.t_dockind
       AND tick.t_dealid = grdeal.t_docid  
       and tick.t_clientid = -1 and t_bofficekind <> 4830 
       AND EXISTS(SELECT 1 FROM DDLGRACC_DBT GRACC WHERE GRACC.T_GRDEALID = GRDEAL.T_ID AND GRACC.T_STATE = DLGRACC_STATE_PLAN and GRACC.T_accnum != 3\*ВУ*\)
       AND EXISTS(SELECT 1 FROM DDL_TICK_DBT TK WHERE TK.T_BOFFICEKIND = GRDEAL.T_DOCKIND AND TK.T_DEALID = GRDEAL.T_DOCID AND TK.T_CLIENTID = -1)
       AND ROWNUM = 1;*/

    IF v_N > 0 THEN
      v_ExistGrDeal := 1;
    END IF;

    RETURN v_ExistGrDeal;

  END RSI_ExistPlanGrDealBeforeDate;

  --Функция получения суммы регистра на дату
  FUNCTION RSI_GetOverRegistrValue(p_FIID IN NUMBER, p_Kind IN NUMBER, p_Date IN DATE, p_SumFIID IN NUMBER, p_Account IN VARCHAR2) RETURN NUMBER
  AS 
    v_Date DATE;
    v_Sum1 ddl_value_dbt.t_Sum%TYPE;
    v_Sum2 ddl_value_dbt.t_Sum%TYPE;
  BEGIN

    SELECT NVL(MAX(T_DATE), TO_DATE('01.01.0001','DD.MM.YYYY')) INTO v_Date
      FROM DDL_VALUE_DBT
     WHERE T_DOCKIND = RSB_SECUR.DLDOC_ISSUE
       AND T_DOCID = p_FIID
       AND T_KIND = p_Kind
       AND T_SUMFIID = p_SumFIID
       AND T_GRPID = 0; --условие, что запись регистра создана на шаге операции - переоценка, перемещение или ГО, а значит является точным остатком требуемого счёта на тот момент

    IF( v_Date = TO_DATE('01.01.0001','DD.MM.YYYY') )THEN
      return rsi_rsb_account.restall(p_Account, 1, p_SumFIID, p_Date);
    END IF;

    IF v_Date > p_Date THEN
      SetError( GR_ERROR_20917, ''); --Уже выполнены проводки переоценки за более позднюю дату
    END IF;

    SELECT NVL(SUM(T_SUM),0) INTO v_Sum1
      FROM DDL_VALUE_DBT
     WHERE T_DOCKIND = RSB_SECUR.DLDOC_ISSUE
       AND T_DOCID = p_FIID
       AND T_KIND = p_Kind
       AND T_DATE = v_Date
       AND T_SUMFIID = p_SumFIID
       AND T_GRPID = 0
       AND T_ID = (SELECT NVL(MAX(V.T_ID),0)
                     FROM DDL_VALUE_DBT V
                    WHERE V.T_DOCKIND = RSB_SECUR.DLDOC_ISSUE
                      AND V.T_DOCID = p_FIID
                      AND V.T_KIND = p_Kind
                      AND V.T_DATE = v_Date
                      AND V.T_SUMFIID = p_SumFIID
                      AND V.T_GRPID = 0
                  );

    --Сумма всех корректировок регистра переоценки, выполненных после последней переоценки
    SELECT NVL(SUM(T_SUM), 0) INTO v_Sum2
      FROM DDL_VALUE_DBT
     WHERE T_DOCKIND = RSB_SECUR.DLDOC_ISSUE
       AND T_DOCID = p_FIID
       AND T_KIND = p_Kind
       AND T_DATE > v_Date
       AND T_SUMFIID = p_SumFIID;

    RETURN (v_Sum1 + v_Sum2);

  END RSI_GetOverRegistrValue;

  
  --Процедура вставки строки регистра переоценки на шаге операции
  PROCEDURE RSI_InsertDL_VALUE(p_DocKind IN NUMBER, 
                               p_DocID IN NUMBER,
                               p_Kind IN NUMBER,
                               p_Date IN DATE,
                               p_Sum IN NUMBER,
                               p_SumFIID IN NUMBER,
                               p_ID_Operation IN NUMBER,
                               p_ID_Step IN NUMBER,
                               p_GrpID IN NUMBER
                              )
  AS 
  BEGIN

    INSERT INTO DDL_VALUE_DBT ( T_ID, T_DOCKIND, T_DOCID, T_KIND, T_DATE, T_SUM, T_SUMFIID, T_ID_OPERATION, T_ID_STEP, T_GRPID)
                       VALUES (0, p_DocKind, p_DocID, p_Kind, p_Date, p_Sum, p_SumFIID, p_ID_Operation, p_ID_Step, p_GrpID);

  END RSI_InsertDL_VALUE;

  --Откат вставки строки регистра переоценки на шаге операции
  PROCEDURE RSI_RollbackInsertDL_VALUE(p_ID_Operation IN NUMBER,
                                       p_ID_Step IN NUMBER
                                      )
  AS 
  BEGIN

    DELETE FROM DDL_VALUE_DBT 
     WHERE T_ID_OPERATION = p_ID_Operation
       AND T_ID_STEP = p_ID_Step;

  END RSI_RollbackInsertDL_VALUE;

  PROCEDURE Mass_UpdateGrDealAcc( TemplNum IN NUMBER, AccNum IN NUMBER, State IN NUMBER ) IS
     v_cnt NUMBER := 0;
     v_i NUMBER := 0;
  
  BEGIN
     SELECT COUNT(1) INTO v_Cnt 
       FROM DV_MKDEAL_MASS_EXEC Deals;

     FOR UpdtDeal_rec IN (SELECT Deals.T_BofficeKind, Deals.T_DealID, Deals.t_PFI, Deals.t_ID_Operation, Deals.t_ID_Step 
                           FROM DV_MKDEAL_MASS_EXEC Deals
                        ) LOOP
       v_i := v_i + 1;
       
       RSI_DLGR.RSI_UpdateGrDealAcc( UpdtDeal_rec.T_BofficeKind, UpdtDeal_rec.T_DealID, UpdtDeal_rec.t_PFI, TemplNum, AccNum, UpdtDeal_rec.t_ID_Operation, UpdtDeal_rec.t_ID_Step, TO_DATE('01.01.0001','DD.MM.YYYY'), State, (CASE WHEN v_i = v_cnt THEN 1 ELSE 0 END) );
     END LOOP;
  END;

  PROCEDURE Mass_UpdateGrDealAcc_ExecOper( DealPart IN NUMBER ) IS
    v_TemplPayment  NUMBER := RSI_DLGR.DLGR_TEMPL_PAYMENT;
    v_TemplDelivery NUMBER := RSI_DLGR.DLGR_TEMPL_DELIVERY;

    v_cnt NUMBER := 0;
    v_i NUMBER := 0;

  BEGIN
     if( DealPart = 2 ) then
        v_TemplPayment  := RSI_DLGR.DLGR_TEMPL_PAYMENT2;
        v_TemplDelivery := RSI_DLGR.DLGR_TEMPL_DELIVERY2;
     end if;

     SELECT COUNT(1) INTO v_Cnt 
       FROM DV_MKDEAL_MASS_EXEC Deals;

     FOR UpdtDeal_rec IN (SELECT Deals.T_BofficeKind, Deals.T_DealID, Deals.t_PFI, Deals.t_ID_Operation, Deals.t_ID_Step 
                           FROM DV_MKDEAL_MASS_EXEC Deals
                        ) LOOP
       v_i := v_i + 1;
       
       RSI_DLGR.RSI_UpdateGrDealAcc( UpdtDeal_rec.T_BofficeKind, UpdtDeal_rec.T_DealID, UpdtDeal_rec.t_PFI, v_TemplPayment,  RSI_DLGR.DLGR_ACCKIND_BACKOFFICE, UpdtDeal_rec.t_ID_Operation, UpdtDeal_rec.t_ID_Step, TO_DATE('01.01.0001','DD.MM.YYYY'), RSI_DLGR.DLGRACC_STATE_FACTEXEC, 0 );
       RSI_DLGR.RSI_UpdateGrDealAcc( UpdtDeal_rec.T_BofficeKind, UpdtDeal_rec.T_DealID, UpdtDeal_rec.t_PFI, v_TemplDelivery, RSI_DLGR.DLGR_ACCKIND_BACKOFFICE, UpdtDeal_rec.t_ID_Operation, UpdtDeal_rec.t_ID_Step, TO_DATE('01.01.0001','DD.MM.YYYY'), RSI_DLGR.DLGRACC_STATE_FACTEXEC, (CASE WHEN v_i = v_cnt THEN 1 ELSE 0 END) );
     END LOOP;
  END;

  FUNCTION RSI_GetDealPartByGrTemplNum(p_TemplNum IN NUMBER) RETURN NUMBER
  AS 
    v_DealPart NUMBER;
  BEGIN
    v_DealPart := 1;

    IF((p_TemplNum = RSI_DLGR.DLGR_TEMPL_PAYMENT2) OR            --Оплата 2-й части РЕПО
       (p_TemplNum = RSI_DLGR.DLGR_TEMPL_DELIVERY2) OR           --Поставка по 2-й части РЕПО
       (p_TemplNum = RSI_DLGR.DLGR_TEMPL_PAYAVANCE2) OR          --Оплата аванса по 2-й части РЕПО
       (p_TemplNum = RSI_DLGR.DLGR_TEMPL_OVERDUEAVANCE2) OR      --Вынос на просрочку аванса 2ч
       (p_TemplNum = RSI_DLGR.DLGR_TEMPL_OVERDUEPAY2) OR         --Вынос на просрочку оплаты 2ч
       (p_TemplNum = RSI_DLGR.DLGR_TEMPL_OVERDUEDELIVERY2) OR    --Вынос на просрочку поставки 2ч
       (p_TemplNum = RSI_DLGR.DLGR_TEMPL_PROLONGPAY2) OR         --Пролонгация оплаты 2ч
       (p_TemplNum = RSI_DLGR.DLGR_TEMPL_PROLONGDELIVERY2) OR    --Пролонгация поставки 2ч
       (p_TemplNum = RSI_DLGR.DLGR_TEMPL_RECDELIVERY2) OR        --Учет поставки по 2-й части РЕПО
       (p_TemplNum = RSI_DLGR.DLGR_TEMPL_DELIVERYCONTR2) OR      --Поставка клиента-контрагента по 2-й части РЕПО
       (p_TemplNum = RSI_DLGR.DLGR_TEMPL_COMPPAYMENT) OR         --Компенсационная оплата
       (p_TemplNum = RSI_DLGR.DLGR_TEMPL_RECCOMPDELIVERY)        --Учет компенсационной поставка
      ) THEN
      v_DealPart := 2;
    END IF;

    RETURN v_DealPart;
  END RSI_GetDealPartByGrTemplNum;


END RSI_DLGR;
/
