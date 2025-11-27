CREATE OR REPLACE PACKAGE BODY RSI_DLLOG AS

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

  PROCEDURE InsertHeader(pDocID IN NUMBER, pDocKind IN NUMBER, pOper IN INTEGER, pLogData IN VARCHAR2)
  AS
     vDl_LogData DDL_LOGDATA_DBT%rowtype;
     vLogID NUMBER := 0;
     vCount NUMBER := 0;
     vLogData CLOB := pLogData;
  BEGIN

     SELECT MAX(T_ID) INTO vLogID
       FROM DDL_LOG_DBT
      WHERE T_DOCKIND = pDocKind
        AND T_DOCID   = pDocID
        AND T_OPER    = pOper;

     --проверим, что заголовка по операции еще нет(должен быть один на всю операцию)
     SELECT count(1) INTO vCount
       FROM DDL_LOGDATA_DBT LOGDATA, DDL_LOG_DBT DLLOG
      WHERE LOGDATA.T_LOGID = DLLOG.T_ID
        AND LOGDATA.T_TYPE  = 0
        AND DLLOG.T_DOCKIND = pDocKind
        AND DLLOG.T_DOCID   = pDocID;

     if( vCount = 0 and vLogID > 0 )then

        vDl_LogData.T_ID           := 0;
        vDl_LogData.T_LOGID        := vLogID;
        vDl_LogData.T_ID_OPERATION := 0;
        vDl_LogData.T_ID_STEP      := 0;
        vDl_LogData.T_TYPE         := 0;
        vDl_LogData.T_LOGDATA      := vLogData;

        INSERT INTO DDL_LOGDATA_DBT VALUES vDl_LogData;

     end if;

  END;-- InsertHeader

  PROCEDURE InsertDL_LOGDATA(pDocID IN NUMBER, pDocKind IN NUMBER, pType IN NUMBER, pID_Operation IN INTEGER, pID_Step IN INTEGER, pOper IN INTEGER, pLogData IN VARCHAR2)
  AS
     vDl_LogData DDL_LOGDATA_DBT%rowtype;
     vLogID NUMBER := 0;
     vLogData CLOB := pLogData;
  BEGIN

     SELECT MAX(T_ID) INTO vLogID
       FROM DDL_LOG_DBT
      WHERE T_DOCKIND = pDocKind
        AND T_DOCID = pDocID
        AND T_OPER = pOper;

     if( vLogID > 0 ) then

        vDl_LogData.T_ID           := 0;
        vDl_LogData.T_LOGID        := vLogID;
        vDl_LogData.T_ID_OPERATION := pID_Operation;
        vDl_LogData.T_ID_STEP      := pID_Step;
        vDl_LogData.T_TYPE         := pType;
        vDl_LogData.T_LOGDATA      := vLogData;

        INSERT INTO DDL_LOGDATA_DBT VALUES vDl_LogData;

     end if;

  END;-- InsertDL_LOGDATA

  PROCEDURE DeleteDL_LOGDATAByStep( pID_Operation IN INTEGER, pID_Step IN INTEGER )
  AS
  BEGIN

     delete from ddl_logdata_dbt
           where t_ID_Operation = pID_Operation
             and t_ID_Step = pID_Step;

  END;-- DeleteDL_LOGDATAByStep

  PROCEDURE InsertDL_LOG( pDocID IN NUMBER, pDocKind IN NUMBER, pOper IN INTEGER )
  AS
     vDl_Log DDL_LOG_DBT%rowtype;
  BEGIN

     if( pDocKind = Rsb_Secur.DL_OVERVALUE      OR --Переоценка ц/б
         pDocKind = Rsb_Secur.DL_OVERVALUE_RD   OR --Переоценка внебаланса
         pDocKind = Rsb_Secur.DL_OVERVALUE_NVPI OR --Переоценка НВПИ
         pDocKind = Rsb_Secur.DL_GET_INCOME     OR
         pDocKind = Rsb_Secur.DL_OFFBALTRANSFSRVOP OR
         pDocKind = Rsb_Secur.DL_INACCSRVOP OR
         pDocKind = Rsb_Secur.DL_SCACCOUNTING OR
         pDocKind = Rsb_Secur.DL_DEPOACCSRVOP OR
         pDocKind = Rsb_Secur.DL_CRPAYMSRVOP OR
         pDocKind = Rsb_Secur.DL_DVOPER_OVERVALUE OR
         pDocKind = Rsb_Secur.SP_CALCRESERV OR
         pDocKind = Rsb_Secur.DL_RESERVEDOC OR
         pDocKind = Rsb_Secur.DL_CALCTSS OR
         pDocKind = Rsb_Secur.DL_RENUMBERINGDEALS OR
         pDocKind = Rsb_Secur.SP_SRVBROKERREP OR
         pDocKind = Rsb_Secur.SP_SRVBROKERREPNEW OR
         pDocKind = Rsb_Secur.DP_CALCCOMMRESERV OR
         pDocKind = Rsb_Secur.REPOS_SERVICEOPERATION OR
         pDocKind = Rsb_Secur.REPOS_MESSAGEGENERATION OR
         pDocKind = Rsb_Secur.SP_DEPOPER_DIVIDEND OR
         pDocKind = Rsb_Secur.REPOS_INBOUNDMSG OR
         pDocKind = Rsb_Secur.SP_AJUSTVALOEB OR
         pDocKind = Rsb_Secur.SP_ACCEXPREVOEB OR
         pDocKind = Rsb_Secur.SP_TRANSFERPA OR
         pDocKind = Rsb_Secur.SP_COMISSION_PA OR
         pDocKind = Rsb_Secur.SP_IMPOWNLIST OR
         pDocKind = Rsb_Secur.DV_CSAPSO OR
         pDocKind = Rsb_Secur.DL_FIXING OR
         pDocKind = Rsb_Secur.LIMITQ_PROTOCOL OR
         pDocKind = Rsb_Secur.SP_AMORTHEDGCORR OR
         pDocKind = Rsb_Secur.DV_SRVOP_AMORT_RHDP
       )then

        vDl_Log.T_ID      := 0;
        vDl_Log.T_DOCID   := pDocID;
        vDl_Log.T_DOCKIND := pDocKind;
        vDl_Log.T_OPER    := pOper;

        INSERT INTO DDL_LOG_DBT VALUES vDl_Log;
     end if;

  END;-- InsertDL_LOG

END RSI_DLLOG;
/