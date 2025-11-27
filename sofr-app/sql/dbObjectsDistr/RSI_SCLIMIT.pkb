CREATE OR REPLACE PACKAGE BODY RSI_SCLIMIT AS

   TYPE limitadj_t IS TABLE OF DDL_LIMITADJUST_DBT%ROWTYPE;

   LastErrorMessage VARCHAR2(1024) := '';

   MarketID         NUMBER := 0;
   MarketCode       VARCHAR2(35) := chr(1);
   mainsessionid    NUMBER := USERENV ('sessionid');

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

   PROCEDURE TimeStamp_ (Label_           IN VARCHAR2,
                         date_               DATE,
                         start_              TIMESTAMP,
                         end_                TIMESTAMP,
                         rootsessionid_      NUMBER DEFAULT NULL, /* чтобы понять из какой сессии запущен поток при многопоточном расчете*/
                         action_             NUMBER DEFAULT NULL)
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN

      INSERT INTO DCALCLIMITLOG_DBT (--T_SESSIONID,
                                     T_DATE,
                                     T_LABEL,
                                     T_START,
                                     T_END,
                                     T_ACTION
                                     --T_ROOTSESSIONID,
                                     --T_MAINSESSIONID
                                     )
           VALUES (
                   --  USERENV ('sessionid')                   /* T_SESSIONID */ ,
                     date_                                        /* T_DATE */
                          ,
                     label_                                      /* T_LABEL */
                           ,
                     NVL (start_, NVL (end_, SYSTIMESTAMP))      /* T_START */
                                                           ,
                     NVL (end_, NVL (start_, SYSTIMESTAMP))        /* T_END */
                                                           ,
                     action_ --,

                     --NVL (rootsessionid_, USERENV ('sessionid')) /* T_ROOTSESSIONID */,
                     --mainsessionid
                     );

      COMMIT;
   END;

   FUNCTION GetLimitDateKind( CalcDate IN DATE, Days IN INTEGER, FIID IN INTEGER, CurID IN INTEGER, CalendarID IN INTEGER, IsCur IN INTEGER )RETURN DATE
   IS
   v_date Date;
   v_CalendarID INTEGER;
   v_type VARCHAR2(2);
   v_IsFind INTEGER;
   v_Day INTEGER;
   v_count INTEGER;

   BEGIN
     v_IsFind :=0;
     v_Day    :=1;
     v_date:=CalcDate + 1;
     v_count :=0;
     IF (FIID = CurID) THEN
       v_CalendarID := CalendarID;
       WHILE v_IsFind = 0
       LOOP
         v_type := rsi_dlcalendars.GetTypeDay(v_date, v_CalendarID);
         IF(v_type = '00' AND IsCur = 0) OR (v_type != '01' AND IsCur = 1) THEN
           v_date:=v_date + 1;
           v_count:= v_count + 1;
           IF(v_count > 50) THEN --если неверно ввели данные, чтобы не зацикливался
             v_IsFind :=1;
           END IF;
         ELSE
           IF(v_Day = Days) THEN
             v_IsFind :=1;
           ELSE
             v_Day:= v_Day + 1;
             v_date:=v_date + 1;
           END IF;
         END IF;
       END LOOP;

     ELSE
       v_CalendarID := rsi_dlcalendars.GetLinkCalByCurrency(FIID);
       WHILE v_IsFind = 0
       LOOP
         v_type := rsi_dlcalendars.GetTypeDay(v_date, v_CalendarID);
         --ищем, пока не найде мдень, где по календарям "Б"
         IF(rsi_dlcalendars.GetTypeDay(v_date, v_CalendarID) != '01') OR (rsi_dlcalendars.GetTypeDay(v_date, CalendarID) != '01') THEN
           v_date:=v_date + 1;
           v_count:= v_count + 1;
           IF(v_count > 50) THEN --если неверно ввели данные, чтобы не зацикливался
             v_IsFind :=1;
           END IF;
         ELSE
           IF(v_Day = Days) THEN
             v_IsFind :=1;
           ELSE
             v_Day:= v_Day + 1;
             v_date:=v_date + 1;
           END IF;
         END IF;
       END LOOP;

     END IF;

     RETURN v_date;
   END;

   FUNCTION GetDateLimitByKind(  Kind IN INTEGER, FIID IN INTEGER, IsCur IN INTEGER ,IsNatCur IN INTEGER  )RETURN DATE
   IS
   v_date Date;

   BEGIN
    IF( IsNatCur = 1) THEN
      BEGIN

      Select
      (CASE WHEN  Kind = 0 THEN T_DATE0
       WHEN  Kind = 1 THEN T_DATE1
       WHEN  Kind = 2 THEN T_DATE2
       WHEN  Kind = 365 THEN T_DATE365
       END
       )
      INTO v_date
      FROM DDL_LIMITCHECKDATE_DBT
      WHERE t_IsNatCur = CHR(88)
      AND T_ISCUR = DECODE( IsCur, 1, CHR(88), CHR(1) );

      EXCEPTION
         WHEN NO_DATA_FOUND THEN RETURN TO_DATE('31.12.2999','DD.MM.YYYY');
      END;
    ELSE
      BEGIN
      Select
      (CASE WHEN  Kind = 0 THEN T_DATE0
       WHEN  Kind = 1 THEN T_DATE1
       WHEN  Kind = 2 THEN T_DATE2
       WHEN  Kind = 365 THEN T_DATE365
       END
       )
      INTO v_date
      FROM DDL_LIMITCHECKDATE_DBT
      WHERE t_CurID = FIID
      AND T_ISCUR = DECODE( IsCur, 1, CHR(88), CHR(1) );

    EXCEPTION
       WHEN NO_DATA_FOUND THEN RETURN TO_DATE('31.12.2999','DD.MM.YYYY');
      END;

    END IF;

    RETURN v_date;
   END;

  -- получить DL_LIMITADJUST из записи, переданной из системы в виде RAW
   PROCEDURE RSI_GetLimitAdjFromRAW( RecLimitAdj IN RAW, rLimitAdj IN OUT DDL_LIMITADJUST_DBT%ROWTYPE )
   AS
   BEGIN
     InitError();
     rsb_struct.readStruct('DDL_LIMITADJUST_DBT');

     rLimitAdj.t_ID              :=  rsb_struct.getlong   ('T_ID',              RecLimitAdj );
     rLimitAdj.T_LIMITID         :=  rsb_struct.getlong   ('T_LIMITID',         RecLimitAdj );
     rLimitAdj.T_LIMIT_KIND      :=  rsb_struct.getInt    ('T_LIMIT_KIND',      RecLimitAdj );
     rLimitAdj.T_DATE            :=  rsb_struct.getdate   ('T_DATE',            RecLimitAdj );
     rLimitAdj.T_TIME            :=  rsb_struct.gettime   ('T_TIME',            RecLimitAdj );
     rLimitAdj.T_MARKET          :=  rsb_struct.getInt    ('T_MARKET',          RecLimitAdj );
     rLimitAdj.T_CLIENT          :=  rsb_struct.getlong   ('T_CLIENT',          RecLimitAdj );
     rLimitAdj.T_INTERNALACCOUNT :=  rsb_struct.getlong   ('T_INTERNALACCOUNT', RecLimitAdj );
     rLimitAdj.T_LIMIT_TYPE      :=  rsb_struct.getString ('T_LIMIT_TYPE',      RecLimitAdj );
     rLimitAdj.T_FIRM_ID         :=  rsb_struct.getString ('T_FIRM_ID',         RecLimitAdj );
     rLimitAdj.T_CLIENT_CODE     :=  rsb_struct.getString ('T_CLIENT_CODE',     RecLimitAdj );
     rLimitAdj.T_OPEN_BALANCE    :=  rsb_struct.getmoney  ('T_OPEN_BALANCE',    RecLimitAdj );
     rLimitAdj.T_OPEN_LIMIT      :=  rsb_struct.getmoney  ('T_OPEN_LIMIT',      RecLimitAdj );
     rLimitAdj.T_CURRENT_LIMIT   :=  rsb_struct.getmoney  ('T_CURRENT_LIMIT',   RecLimitAdj );
     rLimitAdj.T_LIMIT_OPERATION :=  rsb_struct.getString ('T_LIMIT_OPERATION', RecLimitAdj );
     rLimitAdj.T_TRDACCID        :=  rsb_struct.getString ('T_TRDACCID',        RecLimitAdj );
     rLimitAdj.T_SECCODE         :=  rsb_struct.getString ('T_SECCODE',         RecLimitAdj );
     rLimitAdj.T_TAG             :=  rsb_struct.getString ('T_TAG',             RecLimitAdj );
     rLimitAdj.T_CURRID          :=  rsb_struct.getlong   ('T_CURRID',          RecLimitAdj );
     rLimitAdj.T_CURR_CODE       :=  rsb_struct.getString ('T_CURR_CODE',       RecLimitAdj );
     rLimitAdj.T_LIMIT_KIND      :=  rsb_struct.getInt    ('T_LIMIT_KIND',      RecLimitAdj );
     rLimitAdj.T_LEVERAGE        :=  rsb_struct.getmoney  ('T_LEVERAGE',        RecLimitAdj );
     rLimitAdj.T_ID_OPER         :=  rsb_struct.getlong   ('T_ID_OPER',         RecLimitAdj );
     rLimitAdj.T_ID_STEP         :=  rsb_struct.getInt    ('T_ID_STEP',         RecLimitAdj );
     rLimitAdj.T_ISBLOCKED       :=  rsb_struct.getchar   ('T_ISBLOCKED',       RecLimitAdj );
     rLimitAdj.T_CURRENT_BALANCE :=  rsb_struct.getmoney  ('T_CURRENT_BALANCE', RecLimitAdj );

   END; -- RSI_GetLimitAdjFromRAW

   PROCEDURE RSI_InsDfltIntoWRTBC( p_LimitAdj IN OUT DDL_LIMITADJUST_DBT%ROWTYPE )
   IS
   BEGIN

     p_LimitAdj.t_ID              :=   NVL(p_LimitAdj.t_ID,0);
     p_LimitAdj.T_LIMITID         :=   NVL(p_LimitAdj.T_LIMITID,0);
     p_LimitAdj.T_LIMIT_KIND      :=   NVL(p_LimitAdj.T_LIMIT_KIND,0);
     p_LimitAdj.T_DATE            :=   NVL(p_LimitAdj.T_DATE,UnknownDate);
     p_LimitAdj.T_TIME            :=   NVL(p_LimitAdj.T_TIME,UnknownTime);
     p_LimitAdj.T_MARKET          :=   NVL(p_LimitAdj.T_MARKET,-1);
     p_LimitAdj.T_CLIENT          :=   NVL(p_LimitAdj.T_CLIENT,0);
     p_LimitAdj.T_INTERNALACCOUNT :=   NVL(p_LimitAdj.T_INTERNALACCOUNT,0);
     p_LimitAdj.T_LIMIT_TYPE      :=   NVL(p_LimitAdj.T_LIMIT_TYPE,CHR(1));
     p_LimitAdj.T_FIRM_ID         :=   NVL(p_LimitAdj.T_FIRM_ID,CHR(1));
     p_LimitAdj.T_CLIENT_CODE     :=   NVL(p_LimitAdj.T_CLIENT_CODE,CHR(1));
     p_LimitAdj.T_OPEN_BALANCE    :=   NVL(p_LimitAdj.T_OPEN_BALANCE,0);
     p_LimitAdj.T_OPEN_LIMIT      :=   NVL(p_LimitAdj.T_OPEN_LIMIT,0);
     p_LimitAdj.T_CURRENT_LIMIT   :=   NVL(p_LimitAdj.T_CURRENT_LIMIT,0);
     p_LimitAdj.T_LIMIT_OPERATION :=   NVL(p_LimitAdj.T_LIMIT_OPERATION,CHR(1));
     p_LimitAdj.T_TRDACCID        :=   NVL(p_LimitAdj.T_TRDACCID,CHR(1));
     p_LimitAdj.T_SECCODE         :=   NVL(p_LimitAdj.T_SECCODE,CHR(1));
     p_LimitAdj.T_TAG             :=   NVL(p_LimitAdj.T_TAG,CHR(1));
     p_LimitAdj.T_CURRID          :=   NVL(p_LimitAdj.T_CURRID,-1);
     p_LimitAdj.T_CURR_CODE       :=   NVL(p_LimitAdj.T_CURR_CODE,CHR(1));
     p_LimitAdj.T_LIMIT_KIND      :=   NVL(p_LimitAdj.T_LIMIT_KIND,0);
     p_LimitAdj.T_LEVERAGE        :=   NVL(p_LimitAdj.T_LEVERAGE,0);
     p_LimitAdj.T_ID_OPER         :=   NVL(p_LimitAdj.T_ID_OPER,0);
     p_LimitAdj.T_ID_STEP         :=   NVL(p_LimitAdj.T_ID_STEP,0);
     p_LimitAdj.T_ISBLOCKED       :=   NVL(p_LimitAdj.T_ISBLOCKED,CHR(0));
     p_LimitAdj.T_CURRENT_BALANCE :=   NVL(p_LimitAdj.T_CURRENT_BALANCE,0);
   END;

   PROCEDURE RSI_CreateLimitAdJust(RecLimitAdj IN RAW,
                                   ID_Operation IN NUMBER,
                                   ID_Step IN NUMBER
                                               )
   AS
     rLimitAdj    DDL_LIMITADJUST_DBT%rowtype;
   BEGIN
      InitError();

      RSI_GetLimitAdjFromRAW( RecLimitAdj, rLimitAdj );
      rLimitAdj.t_ID :=0;
      rLimitAdj.T_ID_OPER := ID_Operation;
      rLimitAdj.T_ID_STEP := ID_Step;

      RSI_InsDfltIntoWRTBC( rLimitAdj );

      INSERT INTO DDL_LIMITADJUST_DBT VALUES rLimitAdj;

   END;   -- RSI_CreateLimitAdJust


   PROCEDURE RSI_RestoreLimitAdJust(ID_Operation IN NUMBER,
                                    ID_Step IN NUMBER
                                               )
   AS
   BEGIN
        DELETE FROM DDL_LIMITADJUST_DBT limad
         WHERE limad.T_ID_OPER = ID_Operation
          AND limad.T_ID_STEP = ID_Step;

   END;   -- RSI_RestoreLimitAdJust

   FUNCTION UseNotExecRQbyDeal(p_CalcDate IN DATE, p_DealID IN NUMBER) RETURN NUMBER
   IS
     v_sign NUMBER := 0;
     v_Note VARCHAR2(2);
   BEGIN

     v_Note := substr(rsb_struct.getString(rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(p_DealID, 34, '0'), 35/*Код расчетов по сделке*/, p_CalcDate)), 1, 2);
     IF v_Note IS NOT NULL THEN
       IF substr(v_Note,1,2) IN ('Y1','Y2','Y3','Y4','Y5','Y6','Y7','Y8','Y9') THEN
         v_sign := 1;
       END IF;
     END IF;


     RETURN v_sign;

   END; -- UseNotExecRQbyDeal

   FUNCTION GetLastBalanceDay(p_CalcDate IN DATE, p_DayOffset IN INTEGER, p_DocKind IN NUMBER, p_DocID IN NUMBER, p_IdentProgram IN NUMBER, p_objType IN NUMBER) RETURN DATE
   IS
     v_CalendID NUMBER(10) := 0;
     v_OperName VARCHAR2(80) := '';
     v_LastBalanceDay DATE;
   BEGIN

     v_OperName := RSI_DlCalendars.DL_GetOperNameByFD(p_DocKind, p_DocID);
     v_CalendID := RSI_DlCalendars.DL_GetCalendByParam(v_OperName, p_objType, p_IdentProgram);
     v_LastBalanceDay := RSI_DlCalendars.GetBalanceDateAfterWorkDayByCalendar( p_CalcDate, p_DayOffset, v_CalendID, 0);

     RETURN v_LastBalanceDay;

   END; -- GetLastBalanceDay


   FUNCTION GetSumPlanCashRQ(p_Client IN NUMBER, p_ClientContrID IN NUMBER, p_ServKindSub IN NUMBER, p_CalcDate IN DATE, p_Kind IN INTEGER, p_AccountID IN NUMBER, p_ToFI IN NUMBER, p_IsReq IN NUMBER, p_MarketID IN NUMBER) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
     v_ClientRqKind NUMBER;
     v_ContrRqKind NUMBER;
   BEGIN

     IF p_IsReq <> 0 THEN
       v_ClientRqKind := RSI_DLRQ.DLRQ_KIND_REQUEST;
       v_ContrRqKind := RSI_DLRQ.DLRQ_KIND_COMMIT;
     ELSE
       v_ClientRqKind := RSI_DLRQ.DLRQ_KIND_COMMIT;
       v_ContrRqKind := RSI_DLRQ.DLRQ_KIND_REQUEST;
     END IF;


     SELECT NVL(SUM(RSI_RSB_FIInstr.ConvSum(rq.t_Amount, rq.t_FIID, p_ToFI, p_CalcDate, 1)), 0) INTO v_Sum
       FROM ddlrq_dbt rq, ddl_tick_dbt tk
      WHERE ((   tk.t_ClientID = p_Client
             AND tk.t_ClientContrID = p_ClientContrID
             ) OR
             (    tk.t_IsPartyClient = 'X'
              AND tk.t_PartyID = p_Client
              AND tk.t_PartyContrID = p_ClientContrID
             )
            )
        AND (p_ServKindSub = 9 /*Внебиржевой рынок*/ OR (UseNotExecRQbyDeal(p_CalcDate, tk.t_DealID) != 0 AND tk.t_MarketID = p_MarketID) )
        AND rq.t_DocKind = tk.t_BOfficeKind
        AND rq.t_DocID = tk.t_DealID
        AND rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY
        AND EXISTS (Select 1 from DDL_LIMITCHECKDATE_DBT limdate where limdate.t_curID = rq.t_FIID AND T_ISCUR = CHR(1))
        AND rq.t_PlanDate <= GetDateLimitByKind( p_Kind, rq.t_FIID,0, 0)--p_CheckDate
        AND tk.t_DealDate < p_CalcDate --p_CheckDate
        AND RQ.T_FIID = p_ToFI
        AND ((rq.t_Kind = v_ClientRqKind AND tk.t_ClientID = p_Client) OR (rq.t_Kind = v_ContrRqKind AND tk.t_PartyID = p_Client))
        /*AND EXISTS(SELECT 1
                     FROM ddlrqacc_dbt rqacc, daccount_dbt acc
                    WHERE rqacc.t_DocKind = rq.t_DocKind
                      AND rqacc.t_DocID = rq.t_DocID
                      AND rqacc.t_SubKind = rq.t_SubKind
                      AND rqacc.t_Party = p_Client
                      AND rqacc.t_Type IN (rq.t_Type, -1)
                      AND acc.t_AccountID = p_AccountID
                      AND rqacc.t_Account = acc.t_Account
                      AND rqacc.t_Chapter = acc.t_Chapter
                      AND rqacc.t_FIID = acc.t_Code_Currency
                  )*/
        AND (( (rq.t_State != RSI_DLRQ.DLRQ_STATE_EXEC)
              OR
              (EXISTS(SELECT 1
                        FROM ddlgrdeal_dbt gr
                       WHERE gr.t_DocKind = rq.t_DocKind
                         AND gr.t_DocID   = rq.t_DocID
                         AND gr.t_PlanDate <= GetDateLimitByKind( p_Kind, rq.t_FIID,0, 0)--p_CheckDate
                         AND (   (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_COMISS)                             AND tk.t_ClientID = p_Client                             AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYCOM))
                              OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_COMISS)                             AND tk.t_IsPartyClient = 'X' AND tk.t_PartyID = p_Client AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYCOMCONTR))
                              OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_AVANCE, RSI_DLRQ.DLRQ_TYPE_DEPOSIT) AND rq.t_DealPart = 1                                    AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYAVANCE))
                              OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMENT)                            AND rq.t_DealPart = 1                                    AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYMENT))
                              OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_AVANCE)                             AND rq.t_DealPart = 2                                    AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYAVANCE2))
                              OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMENT,RSI_DLRQ.DLRQ_TYPE_INCREPO) AND rq.t_DealPart = 2                                    AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PAYMENT2,RSI_DLGR.DLGR_TEMPL_PAYPC))
                              OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_COMPPAYM)                                                                                    AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_COMPPAYMENT))
                              OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMREPOCOUP)                                                                                AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_COUP))
                              OR (rq.t_Type IN (RSI_DLRQ.DLRQ_TYPE_PAYMREPOPART)                                                                                AND gr.t_TemplNum IN (RSI_DLGR.DLGR_TEMPL_PARTREP))
                             )
                         AND EXISTS(SELECT 1
                                      FROM ddlgracc_dbt gracc
                                     WHERE gracc.t_GrDealID = gr.t_ID
                                       AND gracc.t_AccNum = RSI_DLGR.DLGR_ACCKIND_ACCOUNTING
                                       AND ( gracc.t_State = RSI_DLGR.DLGRACC_STATE_PLAN
                                           OR (    gracc.t_State = RSI_DLGR.DLGRACC_STATE_FACTEXEC
                                               AND gracc.t_FactDate <= GetDateLimitByKind( p_Kind, rq.t_FIID,0, 0)
                                               AND gracc.t_FactDate >= p_CalcDate )
                                           )
                                   )
                     )
              )
            )
            OR (     CASE
                        WHEN rq.t_FactDate = UnknownDate THEN rq.t_PlanDate
                        ELSE rq.t_FactDate
                      END < p_CalcDate
                 AND CASE
                        WHEN rq.t_FactDate = UnknownDate THEN rq.t_PlanDate
                        ELSE rq.t_FactDate
                     END > GetLastBalanceDay (p_CalcDate, -1, TK.T_BOFFICEKIND, TK.T_DEALID, IDENTPROG_SP, 0)
                 AND rq.t_State = RSI_DLRQ.DLRQ_STATE_EXEC
                )
            );

     RETURN v_Sum;

   END; -- GetSumPlanCashRQ


   FUNCTION GetSumPlanCashPM(p_Client IN NUMBER, p_ClientContrID IN NUMBER, p_CalcDate IN DATE, p_Kind IN INTEGER , p_Account IN VARCHAR2, p_FIID IN NUMBER, p_IsReq IN NUMBER) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
   BEGIN

     SELECT NVL(SUM(pm.t_Amount), 0) INTO v_Sum
       FROM dpmpaym_dbt pm, ddvndeal_dbt ndeal, ddvnfi_dbt nfi, dfininstr_dbt bafi
      WHERE ndeal.t_Client = p_Client
        AND ndeal.t_ClientContr = p_ClientContrID
        AND ((ndeal.t_DocKind = 4813)  -- Конверсионная сделка ФИСС и КО
            OR (ndeal.t_DocKind = 199
                AND ndeal.t_DVKind = 6/*DV_CURSWAP_FX*/ ))-- СВОП
        AND EXISTS (Select 1 from DDL_LIMITCHECKDATE_DBT limdate where limdate.t_curID = pm.t_PayFIID AND T_ISCUR = CHR(88))
        AND ndeal.t_Date < GetDateLimitByKind( p_Kind, pm.t_PayFIID,1, 0)--p_CheckDate
        AND ndeal.t_Sector = CHR(88)
        AND ndeal.t_MarketKind IN (2) -- валютный , возможно нужен еще 5 - все(единый пул обеспечения)
        AND ndeal.t_state > 0  -- не отложенная
        AND nfi.t_dealID = ndeal.t_ID
        AND nfi.t_Type = 0
        AND bafi.t_FIID = nfi.t_FIID
        AND bafi.t_fi_kind = 1 -- валюта
        AND pm.t_DocKind = ndeal.t_DocKind
        AND pm.t_DocumentID = ndeal.t_ID
        AND (CASE WHEN p_IsReq <> 0 THEN pm.t_ReceiverAccount ELSE pm.t_PayerAccount END) = p_Account              --  p_IsReq == 1 - требование
        AND pm.t_PayFIID = p_FIID
        AND pm.t_valueDate <= GetDateLimitByKind( p_Kind, pm.t_PayFIID,1, 0)-- p_CheckDate
        AND pm.t_valueDate > GetLastBalanceDay(p_CalcDate, -1, NDEAL.T_DOCKIND, NDEAL.T_ID, IDENTPROG_DV, 0 )
     --   AND pm.t_State = 1000               -- уточнить насчет статусов платежей
        ;
     RETURN v_Sum;

   END; -- GetSumPlanCashPM

   FUNCTION GetSumPlanAvrRQ(p_Client IN NUMBER, p_ClientContrID IN NUMBER, p_ServKindSub IN NUMBER, p_CalcDate IN DATE, p_CheckDate IN DATE, p_FIID IN NUMBER, p_IsReq IN NUMBER, p_MarketID IN NUMBER) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
     v_ClientRqKind NUMBER;
     v_ContrRqKind NUMBER;
   BEGIN

     IF p_IsReq <> 0 THEN
       v_ClientRqKind := RSI_DLRQ.DLRQ_KIND_REQUEST;
       v_ContrRqKind := RSI_DLRQ.DLRQ_KIND_COMMIT;
     ELSE
       v_ClientRqKind := RSI_DLRQ.DLRQ_KIND_COMMIT;
       v_ContrRqKind := RSI_DLRQ.DLRQ_KIND_REQUEST;
     END IF;


     SELECT NVL(SUM(rq.t_Amount), 0) INTO v_Sum
       FROM ddlrq_dbt rq, ddl_tick_dbt tk
      WHERE ((   tk.t_ClientID = p_Client
             AND tk.t_ClientContrID = p_ClientContrID
             ) OR
             (    tk.t_IsPartyClient = 'X'
              AND tk.t_PartyID = p_Client
              AND tk.t_PartyContrID = p_ClientContrID
             )
            )
        AND tk.t_DealDate <= p_CheckDate
        AND (p_ServKindSub = 9 /*Внебиржевой рынок*/ OR (UseNotExecRQbyDeal(p_CalcDate, tk.t_DealID) != 0 AND tk.t_MarketID = p_MarketID))
        AND rq.t_DocKind = tk.t_BOfficeKind
        AND rq.t_DocID = tk.t_DealID
        AND rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
        AND rq.t_FIID = p_FIID
        AND rq.t_PlanDate <= p_CheckDate
        AND ((rq.t_Kind = v_ClientRqKind AND tk.t_ClientID = p_Client) OR (rq.t_Kind = v_ContrRqKind AND tk.t_PartyID = p_Client))
        AND (  rq.t_FactDate = TO_DATE('01.01.0001','DD.MM.YYYY')
            OR NOT EXISTS(SELECT 1 FROM dpmwrtsum_dbt lot WHERE lot.t_DocKind = 29 AND lot.t_DocID = rq.t_ID AND lot.t_Party = p_Client AND lot.t_Contract = p_ClientContrID)
            OR rq.t_FactDate > p_CheckDate
            );

     RETURN v_Sum;

   END; -- GetSumPlanAvrRQ

   FUNCTION GetSumComPrevious(p_Client IN NUMBER, p_ClientContrID IN NUMBER, p_AccountID IN NUMBER, p_ToFI IN NUMBER, p_CalcDate IN DATE, p_MarketID IN NUMBER) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
   BEGIN

     WITH tk AS (SELECT tick.t_DealDate, tick.t_BOfficeKind, tick.t_DealID
                   FROM ddl_tick_dbt tick
                  WHERE tick.t_DealDate = p_CalcDate - 1 --Сделки, заключенные в предыдущий день
                    AND ((   tick.t_ClientID = p_Client
                         AND tick.t_ClientContrID = p_ClientContrID
                         ) OR
                         (    tick.t_IsPartyClient = 'X'
                          AND tick.t_PartyID = p_Client
                          AND tick.t_PartyContrID = p_ClientContrID
                         )
                        )
                    AND TICK.T_MARKETID = p_MarketID
                    AND EXISTS(SELECT 1
                                 FROM ddlrqacc_dbt rqacc, daccount_dbt acc
                                WHERE rqacc.t_DocKind = tick.t_BOfficeKind
                                  AND rqacc.t_DocID = tick.t_DealID
                                  AND rqacc.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY
                                  AND rqacc.t_Party = p_Client
                                  AND acc.t_AccountID = p_AccountID
                                  AND rqacc.t_Account = acc.t_Account
                                  AND rqacc.t_Chapter = acc.t_Chapter
                                  AND rqacc.t_FIID = acc.t_Code_Currency
                              )
                )
     SELECT NVL(SUM(q.t_CommSum), 0) INTO v_Sum
       FROM(
            SELECT NVL(SUM(RSI_RSB_FIInstr.ConvSum(dlcm.t_Sum, cm.t_FIID_Comm, p_ToFI, p_CalcDate, 1)), 0) as t_CommSum
              FROM ddlcomis_dbt dlcm, dsfcomiss_dbt cm, tk
             WHERE dlcm.t_Contract = p_ClientContrID
               AND dlcm.t_DocKind = tk.t_BOfficeKind
               AND dlcm.t_DocID = tk.t_DealID
               AND cm.t_FeeType = dlcm.t_FeeType
               AND cm.t_Number = dlcm.t_ComNumber
               AND cm.t_ReceiverID IN (SELECT d.t_PartyID from ddp_dep_dbt d)
            UNION
            SELECT NVL(SUM(RSI_RSB_FIInstr.ConvSum(basobj.t_CommSum, cm.t_FIID_Comm, p_ToFI, p_CalcDate, 1)), 0) as t_CommSum
              FROM dsfbasobj_dbt basobj, dsfdefcom_dbt defcom, dsfcomiss_dbt cm, tk
             WHERE basobj.t_BaseObjectType = tk.t_BOfficeKind
               AND basobj.t_BaseObjectID = tk.t_DealID
               AND defcom.t_ID = basobj.t_DefCommID
               AND cm.t_FeeType = defcom.t_Feetype
               AND cm.t_Number  = defcom.t_CommNumber
               AND cm.t_ReceiverID IN (SELECT d.t_PartyID from ddp_dep_dbt d)
           ) q;

     RETURN v_Sum;
   END; -- GetSumComPrevious


   FUNCTION GetSumGuarantyPrevious(p_Client IN NUMBER, p_ClientContrID IN NUMBER, p_Department IN NUMBER, p_CalcDate IN DATE, p_PrevWorkDate IN DATE, p_ToFI IN NUMBER) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
   BEGIN

     SELECT NVL(SUM(TURN.t_Guaranty), 0) INTO v_Sum
       FROM ddvfiturn_dbt TURN, dfininstr_dbt fin
      WHERE TURN.T_IsTrust     != 'X'
        AND TURN.T_DEPARTMENT   = p_Department
        AND TURN.T_CLIENTCONTR  = p_ClientContrID
        AND TURN.T_DATE         = p_PrevWorkDate
        AND fin.t_FIID          = TURN.T_FIID
        AND fin.t_ParentFI      = p_ToFI;

     RETURN v_Sum;

   END; -- GetSumGuarantyPrevious

   FUNCTION GetSumFutureComPrevious(p_Client IN NUMBER, p_ClientContrID IN NUMBER, p_Department IN NUMBER, p_CalcDate IN DATE, p_PrevWorkDate IN DATE, p_AccCode_Currency IN NUMBER, p_ToFI IN NUMBER) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
   BEGIN

     SELECT NVL(SUM(RSI_RSB_FIInstr.ConvSum(COM.T_SUM, sfcom.t_FIID_COMM, p_ToFI, p_CalcDate, 1)),0) INTO v_Sum
       FROM ddvfi_com_dbt COM, dsfcomiss_dbt sfcom
      WHERE COM.T_IsTrust      != 'X'
        AND COM.T_DEPARTMENT    = p_Department
        AND COM.T_CLIENTCONTR   = p_ClientContrID
        AND COM.T_DATE          = p_PrevWorkDate -- комиссиия всегда формируется за дату заключения сделки
        AND sfcom.t_ComissID    = COM.t_ComissID
        AND sfcom.t_FIID_COMM   = p_AccCode_Currency
        AND sfcom.t_ReceiverID  IN (SELECT d.t_PartyID from ddp_dep_dbt d);

     RETURN v_Sum;

   END; -- GetSumFutureComPrevious

   FUNCTION GetSumCorr(p_Client IN NUMBER, p_FirmID IN VARCHAR2, p_Department IN NUMBER, p_CalcDate IN DATE, p_PrevWorkDate IN DATE, p_ToFI IN NUMBER) RETURN NUMBER
   IS
     v_Sum          NUMBER := 0;
     v_SumMargIN    NUMBER := 0;
     v_SumMargOUT   NUMBER := 0;
     v_SumRecBonus  NUMBER := 0;
     v_SumPayBonus  NUMBER := 0;
     v_SumComm      NUMBER := 0;
   BEGIN

    SELECT NVL (SUM (TURN.T_MARGIN), 0)
      INTO v_SumMargIN
      FROM ddvfiturn_dbt TURN,
           dfininstr_dbt fin,
           (SELECT MP.T_SFCONTRID
              FROM ddlcontrmp_dbt mp, dsfcontr_dbt sfcontr
             WHERE     mp.t_FirmID = p_FirmID
                   AND SFCONTR.T_ID = MP.T_SFCONTRID
                   AND sfcontr.t_ServKind = 15                 --Срочные контракты
                   AND (   sfcontr.t_DateClose =
                              TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                        OR sfcontr.t_DateClose >= p_CalcDate)
                   AND sfcontr.t_ServKindSub = 8
                   AND SFCONTR.T_PARTYID = p_Client) contr
     WHERE     TURN.T_IsTrust != 'X'
           AND TURN.T_DEPARTMENT = p_Department
           AND TURN.T_CLIENTCONTR = contr.T_SFCONTRID
           AND TURN.T_DATE > p_PrevWorkDate
           AND TURN.T_DATE < p_CalcDate
           AND fin.t_FIID = TURN.T_FIID
           AND fin.t_ParentFI = p_ToFI
           AND TURN.T_MARGIN > 0;

    SELECT NVL (SUM (TURN.T_MARGIN), 0)
      INTO v_SumMargOUT
      FROM ddvfiturn_dbt TURN,
           dfininstr_dbt fin,
           (SELECT MP.T_SFCONTRID
              FROM ddlcontrmp_dbt mp, dsfcontr_dbt sfcontr
             WHERE     mp.t_FirmID = p_FirmID
                   AND SFCONTR.T_ID = MP.T_SFCONTRID
                   AND sfcontr.t_ServKind = 15                 --Срочные контракты
                   AND (   sfcontr.t_DateClose =
                              TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                        OR sfcontr.t_DateClose >= p_CalcDate)
                   AND sfcontr.t_ServKindSub = 8
                   AND SFCONTR.T_PARTYID = p_Client) contr
     WHERE     TURN.T_IsTrust != 'X'
           AND TURN.T_DEPARTMENT = p_Department
           AND TURN.T_CLIENTCONTR = contr.T_SFCONTRID
           AND TURN.T_DATE > p_PrevWorkDate
           AND TURN.T_DATE < p_CalcDate
           AND fin.t_FIID = TURN.T_FIID
           AND fin.t_ParentFI = p_ToFI
           AND TURN.T_MARGIN < 0;

    SELECT NVL (SUM (TURN.T_RECEIVEDBONUS), 0), NVL (SUM (TURN.T_PAIDBONUS), 0)
      INTO v_SumRecBonus, v_SumPayBonus
      FROM ddvfiturn_dbt TURN,
           dfininstr_dbt fin,
           (SELECT MP.T_SFCONTRID
              FROM ddlcontrmp_dbt mp, dsfcontr_dbt sfcontr
             WHERE     mp.t_FirmID = p_FirmID
                   AND SFCONTR.T_ID = MP.T_SFCONTRID
                   AND sfcontr.t_ServKind = 15                 --Срочные контракты
                   AND (   sfcontr.t_DateClose =
                              TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                        OR sfcontr.t_DateClose >= p_CalcDate)
                   AND sfcontr.t_ServKindSub = 8
                   AND SFCONTR.T_PARTYID = p_Client) contr
     WHERE     TURN.T_IsTrust != 'X'
           AND TURN.T_DEPARTMENT = p_Department
           AND TURN.T_CLIENTCONTR = contr.T_SFCONTRID
           AND TURN.T_DATE > p_PrevWorkDate
           AND TURN.T_DATE < p_CalcDate
           AND fin.t_FIID = TURN.T_FIID
           AND fin.t_ParentFI = p_ToFI;

    SELECT NVL (SUM (RSI_RSB_FIInstr.ConvSum (COM.T_SUM,
                                              sfcom.t_FIID_COMM,
                                              p_ToFI,
                                              p_CalcDate,
                                              1)),
                0)
      INTO v_SumComm
      FROM ddvfi_com_dbt COM,
           dsfcomiss_dbt sfcom,
           (SELECT MP.T_SFCONTRID
              FROM ddlcontrmp_dbt mp, dsfcontr_dbt sfcontr
             WHERE     mp.t_FirmID = p_FirmID
                   AND SFCONTR.T_ID = MP.T_SFCONTRID
                   AND sfcontr.t_ServKind = 15                 --Срочные контракты
                   AND (   sfcontr.t_DateClose =
                              TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                        OR sfcontr.t_DateClose >= p_CalcDate)
                   AND sfcontr.t_ServKindSub = 8
                   AND SFCONTR.T_PARTYID = p_Client) contr
     WHERE     COM.T_IsTrust != 'X'
           AND COM.T_DEPARTMENT = p_Department
           AND COM.T_CLIENTCONTR = contr.T_SFCONTRID
           AND COM.T_DATE > p_PrevWorkDate
           AND COM.T_DATE < p_CalcDate
           AND sfcom.t_ComissID = COM.t_ComissID
           AND sfcom.t_FIID_COMM = p_ToFI
           AND sfcom.t_ReceiverID IN (SELECT d.t_PartyID
                                        FROM ddp_dep_dbt d);

     v_Sum := v_SumMargIN + v_SumRecBonus - v_SumMargOUT + v_SumPayBonus - v_SumComm;

     RETURN v_Sum;

   END; -- GetSumCorr

   FUNCTION GetFIRM_ID(p_MarketID IN NUMBER, p_MarketKind IN NUMBER) RETURN VARCHAR2
   IS
     v_FIRM_ID VARCHAR2(12) := chr(1);
   BEGIN

     select cast(PRM.T_FIRMCODE as VARCHAR2(12)) into v_FIRM_ID
       from ddl_limitprm_dbt prm
      where PRM.T_MARKETID = p_MarketID
        and PRM.T_MARKETKIND = p_MarketKind;

     RETURN v_FIRM_ID;

   EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN chr(1);
   END; -- GetFIRM_ID

   FUNCTION GetTAG(p_MarketID IN NUMBER, p_MarketKind IN NUMBER) RETURN VARCHAR2
   IS
     v_TAG VARCHAR2(5) := chr(1);
   BEGIN

     select cast(PRM.T_POSCODE as VARCHAR2(5)) into v_TAG
       from ddl_limitprm_dbt prm
      where PRM.T_MARKETID = p_MarketID
        and PRM.T_MARKETKIND = p_MarketKind;

     RETURN v_TAG;

   EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN chr(1);
   END; -- GetTAG

   PROCEDURE getFlagLimitPrm(p_MarketID IN NUMBER,p_MarketKind IN NUMBER, v_IsDepo IN OUT NUMBER, v_IsKind2 IN OUT NUMBER, v_DepoAcc IN OUT VARCHAR2)
   AS
   BEGIN

     select
             DECODE(prm.T_ISDEPO, 'X', 1, 0),
             DECODE(prm.T_KINDLARGERTWO, 'X', 1, 0),
             prm.T_DEPOACC
       into v_IsDepo, v_IsKind2,v_DepoAcc
       from ddl_limitprm_dbt prm
      where PRM.T_MARKETID = p_MarketID
        and PRM.T_MARKETKIND = p_MarketKind;

   EXCEPTION
      WHEN NO_DATA_FOUND THEN
      v_IsDepo := 0;
      v_IsKind2 := 0;
      v_DepoAcc := chr(1);
   END; -- GetTAG

   FUNCTION GetSumComPrevious(p_ClientContrID IN NUMBER, p_CalcDate IN DATE, p_Kind IN INTEGER, p_Currency IN NUMBER, p_MarketID IN NUMBER) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
   BEGIN
    SELECT NVL (SUM (DLCOM.T_SUM), 0)
      INTO v_Sum
      FROM ddlcomis_dbt dlcom, dsfcomiss_dbt sfcom, ddl_tick_dbt tick
     WHERE     DLCOM.T_CONTRACT = p_ClientContrID
           AND SFCOM.T_FEETYPE = dlcom.t_feetype
           AND SFCOM.T_NUMBER = DLCOM.T_COMNUMBER
           AND SFCOM.T_FIID_COMM = p_Currency
           AND TICK.T_BOFFICEKIND = DLCOM.T_DOCKIND
           AND tick.t_dealid = DLCOM.T_DOCID
           AND TICK.T_DEALSTATUS <> 0
           AND TICK.T_DEALDATE < p_CalcDate
           AND TICK.T_MARKETID = p_MarketID
           AND (   (    DLCOM.T_FACTPAYDATE = TO_DATE('01.01.0001', 'dd.mm.yyyy') AND
                        DLCOM.T_PLANPAYDATE <=
                           GetDateLimitByKind (p_Kind,
                                               SFCOM.T_FIID_COMM,
                                               0,
                                               0))
                OR (    DLCOM.T_FACTPAYDATE <=
                           GetDateLimitByKind (p_Kind,
                                               SFCOM.T_FIID_COMM,
                                               0,
                                               0)
                    AND DLCOM.T_FACTPAYDATE >= p_CalcDate));

     RETURN v_Sum;

   END; -- GetSumComPrevious

   FUNCTION GetSumComPrevious_1(p_ClientContrID IN NUMBER, p_CalcDate IN DATE, p_Currency IN NUMBER, p_MarketID IN NUMBER) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
   BEGIN
    SELECT NVL (SUM (DLCOM.T_SUM), 0)
      INTO v_Sum
      FROM ddlcomis_dbt dlcom, dsfcomiss_dbt sfcom, ddl_tick_dbt tick
     WHERE     DLCOM.T_CONTRACT = p_ClientContrID
           AND SFCOM.T_FEETYPE = dlcom.t_feetype
           AND SFCOM.T_NUMBER = DLCOM.T_COMNUMBER
           AND SFCOM.T_FIID_COMM = p_Currency
           AND TICK.T_BOFFICEKIND = DLCOM.T_DOCKIND
           AND tick.t_dealid = DLCOM.T_DOCID
           AND TICK.T_DEALSTATUS <> 0
           AND TICK.T_DEALDATE < p_CalcDate
           AND TICK.T_MARKETID = p_MarketID
           AND DLCOM.T_FACTPAYDATE = RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,-1) ;

     RETURN v_Sum;

   END; -- GetSumComPrevious_1

   FUNCTION GetSumDebAndCredCash(p_Account IN VARCHAR2, p_CalcDate IN DATE, p_SubKind_Oper IN NUMBER) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
   BEGIN
    SELECT NVL (SUM (NPTXOP.T_OUTSUM), 0)
      INTO v_Sum
      FROM dnptxop_dbt nptxop
     WHERE     NPTXOP.T_STATUS = 2                                     -- Закрытая
           AND NPTXOP.T_DOCKIND = 4607 -- Операция Списания/Зачисления денежных средств
           AND NPTXOP.T_OPERDATE =
                  RSI_RsbCalendar.GetDateAfterWorkDay (p_CalcDate, -1)
           AND NPTXOP.T_ACCOUNT = p_Account
           AND NPTXOP.T_LIMITSTATUS <> 2
           AND NPTXOP.T_SUBKIND_OPERATION = p_SubKind_Oper;
     RETURN v_Sum;

   END; -- GetSumDebAndCredCash


  FUNCTION GetObjCodeOnDate (pFIID         IN NUMBER,
                             pObjectType   IN NUMBER,
                             pCodeKind     IN NUMBER,
                             pDate         IN DATE)
     RETURN VARCHAR2
  IS
     vBankDate   DOBJCODE_DBT.T_BankDate%TYPE;
     vCode       DOBJCODE_DBT.T_CODE%TYPE;
  BEGIN
     BEGIN
        SELECT objcode.T_BankDate
          INTO vBankDate
          FROM (  SELECT t_BankDate
                    FROM DOBJCODE_DBT
                   WHERE t_ObjectType = pObjectType
                     AND t_CodeKind = pCodeKind
                     AND t_ObjectID = pFIID
                     AND t_BankDate <= pDate
                ORDER BY t_BankDate DESC) objcode
         WHERE ROWNUM = 1;
     EXCEPTION
        WHEN NO_DATA_FOUND  THEN vBankDate := TO_DATE ('01.01.0001', 'dd.mm.yyyy');
        WHEN OTHERS  THEN vBankDate := TO_DATE ('01.01.0001', 'dd.mm.yyyy');
     END;

     IF vBankDate <> TO_DATE ('01.01.0001', 'dd.mm.yyyy') THEN
        BEGIN
           SELECT objcode.T_Code
             INTO vCode
             FROM (  SELECT t_Code
                       FROM DOBJCODE_DBT
                      WHERE t_ObjectType = pObjectType
                        AND t_CodeKind = pCodeKind
                        AND t_ObjectID = pFIID
                        AND t_BankDate = vBankDate
                        AND t_BankCloseDate = TO_DATE ('01.01.0001', 'dd.mm.yyyy')) objcode
            WHERE ROWNUM = 1;
        EXCEPTION
           WHEN NO_DATA_FOUND  THEN vCode := CHR(1);
           WHEN OTHERS  THEN vCode := CHR(1);
        END;
     END IF;

     RETURN vCode;
  END;

    FUNCTION GetWAPositionPrice (p_CalcDate    IN DATE,
                                 p_Client      IN NUMBER,
                                 p_SfContrID   IN NUMBER,
                                 p_FIID        IN NUMBER,
                                 p_ClientCode  IN VARCHAR2,
                                 p_SecCode     IN VARCHAR2,
                                 p_FirmID      IN VARCHAR2,
                                 p_Limit_Kind  IN NUMBER,
                                 p_MarketID    IN NUMBER)
       RETURN NUMBER
    IS
       v_sum_prev        NUMBER := 0;
       v_sum             NUMBER := 0;
       v_sumid_t0        NUMBER := 0;
       v_t0              DATE;
       v_sum_t0          NUMBER := 0;
       v_first           NUMBER := 1;
       v_long            CHAR := CHR (0);
       v_short           CHAR := CHR (0);
       v_wa_price_prev   NUMBER := 0;
       v_position_prev   NUMBER := 0;
       v_wa_price        NUMBER := 0;
       v_RegPath         VARCHAR2(200) := 'SECUR\НОМЕР КАТ-ИИ - ЯВЛ.ТЕХН.РЕПО';
       v_RegVal          NUMBER(5):= 0;
       Sum_tmp           NUMBER := 0;
       v_CheckTechREPO   NUMBER := -1;
       v_GO_coef         DSCDLFI_DBT%ROWTYPE;
       v_wa_priceGO      NUMBER := 0;
       v_lastGOID        NUMBER := 0;
       v_firstGO         NUMBER := 0;
    BEGIN
        BEGIN
            SELECT NVL (SECUR.T_WA_POSITION_PRICE, 0)
              INTO v_wa_price
              FROM ddl_limitsecuritesin_dbt secur
             WHERE     SECUR.T_FIRM_ID = p_FirmId
                   AND SECUR.T_CLIENT_CODE = p_ClientCode
                   AND SECUR.T_SECCODE = p_SecCode
                   AND SECUR.T_LIMIT_KIND = p_Limit_Kind;
        EXCEPTION
          WHEN NO_DATA_FOUND
          THEN
            NULL;
        END;

        v_RegVal := RSB_COMMON.GetRegIntValue( v_RegPath );

       IF v_wa_price = 0 THEN
       FOR rec
          IN (  SELECT lot.*
                  FROM v_scwrthistex lot
                 WHERE     lot.t_changedate < p_CalcDate
                       AND lot.t_Party = p_Client
                       AND lot.T_contract = p_SfContrID
                       AND lot.t_fiid = p_FIID
                       AND lot.t_Buy_Sale IN (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY,
                                              RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO,
                                              RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE)
                       AND lot.t_instance = 0
                       AND lot.t_sum != 0
                       AND (   (    lot.t_DocKind = 29
                                  AND EXISTS
                                         (SELECT tk.*
                                            FROM ddlrq_dbt rq, ddl_tick_dbt tk
                                           WHERE     rq.t_ID = lot.t_DocID
                                                 AND tk.t_DealID = rq.t_docid
                                                 AND tk.T_BOFFICEKIND = rq.t_dockind
                                                 AND tk.t_marketid = p_MarketID))
                              OR (    lot.t_DocKind = 135
                                  AND EXISTS
                                         (SELECT tk.*
                                            FROM ddl_tick_dbt tk
                                           WHERE     tk.t_DealID = lot.t_dealid
                                                 AND tk.T_BOFFICEKIND = 101
                                                 AND tk.t_marketid = p_MarketID)))
                       AND NOT EXISTS                              -- убираем репо
                              (SELECT *
                                 FROM ddlrq_dbt rq, ddl_tick_dbt tk
                                WHERE     rq.t_ID = lot.t_DocID
                                      AND tk.t_BOfficeKind = rq.t_DocKind
                                      AND tk.t_DealID = rq.t_DocID
                                      AND 1 = RSB_SECUR.IsRepo(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind)))
                                      /*Берем только первую часть из орепо как тех.репo*/
                                      AND NOT (RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) = 1 AND rq.t_DealPart = 1
                                      AND TK.T_MARKETID = p_MarketID
                                      AND 1 = (CASE WHEN v_RegVal != -1 THEN NVL(RSB_SECUR.GetMainObjAttr (101,LPAD (tk.T_DEALID,34,'0'),v_RegVal,p_CalcDate), 0) ELSE 0 END)))
              ORDER BY lot.t_changedate ASC, lot.t_sumid ASC)
       LOOP
          IF rec.t_DocKind = 29 THEN
          SELECT RSB_SECUR.IsRepo (
                    rsb_secur.get_OperationGroup (
                       rsb_secur.get_OperSysTypes (tk.t_DealType, tk.t_BofficeKind)))
            INTO v_CheckTechREPO
            FROM ddlrq_dbt rq, ddl_tick_dbt tk
           WHERE     rq.t_ID = rec.t_DocID
                 AND tk.t_BOfficeKind = rq.t_DocKind
                 AND tk.t_DealID = rq.t_DocID;
          ELSE
            v_CheckTechREPO := 0;
          END IF;
          IF rec.t_DocKind = 135 THEN
                     SELECT coef.*
                          INTO v_GO_coef
                          FROM DSCDLFI_DBT coef
                         WHERE coef.t_dealid = rec.t_docid
                           AND coef.t_dealkind = rec.t_dockind;
                 if v_GO_coef.t_ID <> v_lastGOID THEN
                    v_sum := v_sum * (v_GO_coef.T_NUMERATOR/v_GO_coef.T_DENOMINATOR);
                    v_lastGOID := rec.t_docid;
                 ELSE
                    v_sum := v_sum + rec.t_amount;
                 END IF;
                 v_lastGOID := rec.t_docid;
          ELSE
              IF rec.t_Buy_Sale IN (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY,
                                    RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO) AND v_CheckTechREPO <> 1
              THEN
                    v_sum := v_sum + rec.t_amount;
              ELSE
                    v_sum := v_sum - rec.t_amount;
              END IF;
          END IF;

          IF v_first = 0
          THEN
             IF v_sum > 0 AND v_sum > v_sum_prev AND v_sum_prev <= 0
             THEN
                v_sumid_t0 := rec.t_sumid;
                v_t0 := rec.t_changedate;
                v_sum_t0 := v_sum;
                v_long := CHR (88);
                v_short := CHR (0);
             ELSE
                IF v_sum < 0 AND v_sum < v_sum_prev AND v_sum_prev >= 0
                THEN
                   v_sumid_t0 := rec.t_sumid;
                   v_t0 := rec.t_changedate;
                   v_sum_t0 := v_sum;
                   v_long := CHR (0);
                   v_short := CHR (88);
                END IF;
             END IF;

             v_sum_prev := v_sum;
          END IF;

          IF v_first = 1
          THEN
             v_first := 0;
             IF v_sum > 0 THEN
                v_long := CHR (88);
             ELSE
                v_short := CHR (88);
             END IF;
             v_sumid_t0 := rec.t_sumid;
             v_t0 := rec.t_changedate;
             v_sum_t0 := v_sum;
             v_sum_prev := v_sum;
          END IF;
       END LOOP;

       v_first := 1;
       v_lastGOID := 0;

       if v_sum_t0 < 0 THEN
         v_sum_t0 := v_sum_t0 * (-1);
       END IF;

       FOR rec
          IN (  SELECT lot.*
                  FROM v_scwrthistex lot
                 WHERE     lot.t_changedate >= v_t0
                       AND lot.t_changedate < p_CalcDate
                       AND lot.t_Party = p_Client
                       AND lot.T_contract = p_SfContrID
                       AND lot.t_fiid = p_FIID
                       AND lot.t_sumid >= v_sumid_t0
                       AND lot.t_instance = 0
                       AND lot.t_sum != 0
                       AND (   (    lot.t_DocKind = 29
                                  AND EXISTS
                                         (SELECT tk.*
                                            FROM ddlrq_dbt rq, ddl_tick_dbt tk
                                           WHERE     rq.t_ID = lot.t_DocID
                                                 AND tk.t_DealID = rq.t_docid
                                                 AND tk.T_BOFFICEKIND = rq.t_dockind
                                                 AND tk.t_marketid = p_MarketID))
                              OR (    lot.t_DocKind = 135
                                  AND EXISTS
                                         (SELECT tk.*
                                            FROM ddl_tick_dbt tk
                                           WHERE     tk.t_DealID = lot.t_dealid
                                                 AND tk.T_BOFFICEKIND = 101
                                                 AND tk.t_marketid = p_MarketID)))
                       AND NOT EXISTS                              -- убираем репо
                              (SELECT *
                                 FROM ddlrq_dbt rq, ddl_tick_dbt tk
                                WHERE     rq.t_ID = lot.t_DocID
                                      AND tk.t_BOfficeKind = rq.t_DocKind
                                      AND tk.t_DealID = rq.t_DocID
                                      AND 1 = RSB_SECUR.IsRepo(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind)))
                                      /*Берем только первую часть из орепо как тех.репo, оставляем*/
                                      AND NOT (RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tk.t_DealType, tk.t_BofficeKind))) = 1 AND rq.t_DealPart = 1
                                      AND TK.T_MARKETID = p_MarketID
                                      AND 1 = (CASE WHEN v_RegVal != -1 THEN NVL(RSB_SECUR.GetMainObjAttr (101,LPAD (tk.T_DEALID,34,'0'),v_RegVal,p_CalcDate), 0) ELSE 0 END)))
              ORDER BY lot.t_changedate ASC, lot.t_sumid ASC)
       LOOP
           IF rec.t_DocKind = 29 THEN
           SELECT RSB_SECUR.IsRepo (
                    rsb_secur.get_OperationGroup (
                       rsb_secur.get_OperSysTypes (tk.t_DealType, tk.t_BofficeKind)))
            INTO v_CheckTechREPO
            FROM ddlrq_dbt rq, ddl_tick_dbt tk
           WHERE     rq.t_ID = rec.t_DocID
                 AND tk.t_BOfficeKind = rq.t_DocKind
                 AND tk.t_DealID = rq.t_DocID;
           ELSE
            v_CheckTechREPO := 0;
          END IF;
          IF v_first = 1
          THEN
             v_wa_price :=
                  ROUND((RSI_RSB_FIInstr.ConvSum (rec.t_sum,
                                           rec.t_Currency,
                                           RSI_RSB_FIInstr.NATCUR,
                                           rec.t_Date,
                                           1)
                / rec.t_amount), 2);
             v_wa_price_prev := v_wa_price;
             v_position_prev := v_sum_t0;
             v_first := 0;
             v_wa_priceGO := v_wa_price;
             IF rec.t_dockind = 135 THEN
               v_firstGO := 1;
             END IF;
          ELSE
             IF (   (    v_long = 'X'
                     AND rec.t_Buy_Sale IN (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY,
                                            RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO))
                 OR (    v_short = 'X'
                     AND (rec.t_Buy_Sale IN (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY,
                                            RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO) OR v_CheckTechREPO = 1)))
             THEN
                IF rec.t_dockind = 29 THEN
                    v_wa_price :=
                         (  v_wa_price_prev * v_position_prev
                          + ROUND(RSI_RSB_FIInstr.ConvSum (rec.t_sum,
                                                     rec.t_Currency,
                                                     RSI_RSB_FIInstr.NATCUR,
                                                     rec.t_Date,
                                                     1),2))
                       / (v_position_prev + rec.t_amount);
                    v_position_prev := v_position_prev + rec.t_amount;
                    v_wa_priceGO := v_wa_priceGO + (RSI_RSB_FIInstr.ConvSum (rec.t_sum,
                                           rec.t_Currency,
                                           RSI_RSB_FIInstr.NATCUR,
                                           rec.t_Date,
                                           1)
                                    / rec.t_amount);

                ELSE
                    IF rec.t_dockind = 135 THEN
                        SELECT coef.*
                          INTO v_GO_coef
                          FROM DSCDLFI_DBT coef
                         WHERE coef.t_dealid = rec.t_docid
                           AND coef.t_dealkind = rec.t_dockind;
                        if v_firstGO = 1 AND v_lastGOID = rec.t_docid THEN
                            v_wa_price := v_wa_price + ROUND(RSI_RSB_FIInstr.ConvSum (rec.t_sum,
                                                         rec.t_Currency,
                                                         RSI_RSB_FIInstr.NATCUR,
                                                         rec.t_Date,
                                                         1),2)
                           / rec.t_amount;
                           v_wa_priceGO := v_wa_price;
                           v_position_prev := v_position_prev + rec.t_amount;
                        ELSE if v_lastGOID <> rec.t_docid THEN
                            v_wa_price := ROUND(v_wa_priceGO,2)
                              / ((v_GO_coef.T_NUMERATOR/v_GO_coef.T_DENOMINATOR));
                            v_position_prev := (v_GO_coef.T_NUMERATOR/v_GO_coef.T_DENOMINATOR) * v_position_prev;
                            v_wa_priceGO := v_wa_price;
                            END IF;
                        END IF;
                    END IF;
                END IF;
                v_wa_price_prev := v_wa_price;
             ELSE
                IF (rec.t_Buy_Sale = RSB_PMWRTOFF.PM_WRITEOFF_SUM_SALE)
                THEN
                   v_wa_price := v_wa_price_prev;
                   v_position_prev := v_position_prev - rec.t_amount;
                END IF;
             END IF;
          END IF;
          IF rec.t_dockind = 135 AND v_lastGOID <> rec.t_docid THEN
            if v_lastGOID <> 0 THEN
             v_firstGO := 0;
            END IF;
            v_lastGOID := rec.t_docid;
          END IF;
       END LOOP;
       END IF;

       RETURN ROUND(v_wa_price,2);
    END;                                                   -- GetWAPositionPrice

   PROCEDURE UpdSumPlanAvrRQ (p_CalcDate IN DATE, p_CheckDate IN DATE, p_MarketID IN NUMBER)
   AS
   BEGIN
      UPDATE DDL_LIMITSECURITES_INT_TMP
         SET T_PLAN_PLUS_DEAL =
                GetSumPlanAvrRQ (T_CLIENT,
                                 T_CONTRACT,
                                 T_SERVKINDSUB,
                                 T_DATE,
                                 p_CheckDate,
                                 T_SECURITY,
                                 1,
                                 p_MarketID),
             T_PLAN_MINUS_DEAL =
                GetSumPlanAvrRQ (T_CLIENT,
                                 T_CONTRACT,
                                 T_SERVKINDSUB,
                                 T_DATE,
                                 p_CheckDate,
                                 T_SECURITY,
                                 0,
                                 p_MarketID)
       WHERE EXISTS
                (SELECT 1
                   FROM DDL_FIID_DBT FIID
                  WHERE FIID.T_CLIENTID = DDL_LIMITSECURITES_INT_TMP.T_CLIENT
                        AND FIID.T_CLIENTCONTRID =
                               DDL_LIMITSECURITES_INT_TMP.T_CONTRACT
                        AND FIID.T_FIID =
                               DDL_LIMITSECURITES_INT_TMP.t_SECURITY);

      UPDATE DDL_LIMITSECURITES_INT_TMP
         SET T_OPEN_BALANCE =
                (T_QUANTITY + T_PLAN_PLUS_DEAL - T_PLAN_MINUS_DEAL);
   END;

   PROCEDURE InsertLimitFromInt
   AS
   BEGIN
      INSERT INTO DDL_LIMITCASHSTOCK_DBT
         (SELECT 0,
                 int_tmp.T_DATE,
                 int_tmp.T_TIME,
                 int_tmp.T_MARKET,
                 int_tmp.T_CLIENT,
                 int_tmp.T_INTERNALACCOUNT,
                 int_tmp.T_FIRM_ID,
                 int_tmp.T_TAG,
                 int_tmp.T_CURRID,
                 int_tmp.T_CURR_CODE,
                 int_tmp.T_CLIENT_CODE,
                 int_tmp.T_OPEN_BALANCE,
                 int_tmp.T_OPEN_LIMIT,
                 int_tmp.T_CURRENT_LIMIT,
                 int_tmp.T_LEVERAGE,
                 int_tmp.T_LIMIT_KIND,
                 int_tmp.T_MONEY306,
                 int_tmp.T_DUE474,
                 int_tmp.T_PLAN_PLUS_DEAL,
                 int_tmp.T_PLAN_MINUS_DEAL,
                 int_tmp.T_COMPREVIOUS,
                 int_tmp.T_ISBLOCKED,
                 int_tmp.T_MARKET_KIND,
                 INT_TMP.T_COMPREVIOUS_1,
                 INT_TMP.T_SP,
                 INT_TMP.T_ZCH
            FROM DDL_LIMITCASHSTOCK_INT_TMP int_tmp);
   END;

   PROCEDURE InsertLimitFromIntSecur
   AS
   BEGIN
      INSERT INTO DDL_LIMITSECURITES_DBT
         (SELECT 0,
                 int_tmp.T_DATE,
                 int_tmp.T_TIME,
                 int_tmp.T_MARKET,
                 int_tmp.T_CLIENT,
                 int_tmp.T_SECURITY,
                 int_tmp.T_FIRM_ID,
                 int_tmp.T_SECCODE,
                 int_tmp.T_CLIENT_CODE,
                 int_tmp.T_OPEN_BALANCE,
                 int_tmp.T_OPEN_LIMIT,
                 int_tmp.T_CURRENT_LIMIT,
                 int_tmp.T_TRDACCID,
                 int_tmp.T_WA_POSITION_PRICE,
                 int_tmp.T_LIMIT_KIND,
                 int_tmp.T_QUANTITY,
                 int_tmp.T_PLAN_PLUS_DEAL,
                 int_tmp.T_PLAN_MINUS_DEAL,
                 int_tmp.T_ISBLOCKED,
                 int_tmp.T_MARKET_KIND,
                 int_tmp.T_MoneyConsolidated
            FROM DDL_LIMITSECURITES_INT_TMP int_tmp);
   END;

   FUNCTION GetObjAtCor( p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE,
                           p_Object     IN dobjatcor_dbt.t_Object%TYPE,
                           p_GroupID    IN dobjatcor_dbt.t_GroupID%TYPE,
                           p_Date       IN dobjatcor_dbt.t_ValidFromDate%TYPE )
     RETURN dobjattr_dbt.t_AttrID%TYPE
   IS
     p_AttrID dobjattr_dbt.t_AttrID%TYPE;
  BEGIN

    BEGIN
      SELECT AtCor.t_AttrID INTO p_AttrID
        FROM dobjatcor_dbt AtCor
       WHERE AtCor.t_ObjectType  = p_ObjectType
         AND AtCor.t_GroupID     = p_GroupID
         AND AtCor.t_Object      = p_Object
         AND AtCor.t_ValidToDate >= p_Date
         AND AtCor.t_ValidFromDate = (SELECT MAX(t.t_ValidFromDate)
                                        FROM dobjatcor_dbt t
                                       WHERE t.t_ObjectType     = p_ObjectType
                                         AND t.t_GroupID        = p_GroupID
                                         AND t.t_Object         = p_Object
                                         AND t.t_ValidFromDate <= p_Date
                                         AND t.t_ValidToDate    >= p_Date
                                     );
    EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
        p_AttrID := 0;
      WHEN OTHERS
      THEN
        p_AttrID := 0;
    END;

    RETURN p_AttrID;

  END GetObjAtCor;

  FUNCTION GetKindMarketCodeOrNote (pMarketCode IN VARCHAR2, IsSecCode IN NUMBER, IsTradeaccID IN NUMBER)
     RETURN NUMBER
  IS
     vCode NUMBER := 0;
     vMICEX_CODE VARCHAR2(2000) := CHR(1);
     vSPBEX_CODE VARCHAR2(2000) := CHR(1);
  BEGIN
     vMICEX_CODE := RSB_Common.GetRegStrValue('SECUR\MICEX_CODE');
     vSPBEX_CODE := RSB_Common.GetRegStrValue('SECUR\SPBEX_CODE');

     IF IsSecCode = 1
     THEN
     IF pMarketCode = vMICEX_CODE
     THEN
     vCode  := 11;
     END IF;

     IF pMarketCode = vSPBEX_CODE
     THEN
     vCode  := 22;
     END IF;
     END IF;

     IF IsTradeaccID = 1
     THEN
     IF pMarketCode = vMICEX_CODE
     THEN
     vCode  := 5;
     END IF;

     IF pMarketCode = vSPBEX_CODE
     THEN
     vCode  := 10;
     END IF;
     END IF;


     RETURN vCode;
  END;

   PROCEDURE RSI_CreateLimitsKindParallel (p_ExecStr IN VARCHAR2)
   AS
      l_task_name   VARCHAR2 (30);
      l_try         NUMBER;
      l_status      NUMBER;

      l_stmt        CLOB;
   BEGIN
      l_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;
      DBMS_PARALLEL_EXECUTE.create_task (task_name => l_task_name);

      l_stmt := 'SELECT level, level FROM DUAL CONNECT BY LEVEL <= 4';
      DBMS_PARALLEL_EXECUTE.
       create_chunks_by_sql (task_name   => l_task_name,
                             sql_stmt    => l_stmt,
                             by_rowid    => FALSE);

      DBMS_PARALLEL_EXECUTE.run_task (task_name        => l_task_name,
                                      sql_stmt         => p_ExecStr,
                                      language_flag    => DBMS_SQL.NATIVE,
                                      parallel_level   => 4);

      l_try := 0;
      l_status := DBMS_PARALLEL_EXECUTE.task_status (l_task_name);

      WHILE (l_try < 2 AND l_status != DBMS_PARALLEL_EXECUTE.FINISHED)
      LOOP
         l_try := l_try + 1;
         DBMS_PARALLEL_EXECUTE.resume_task (l_task_name);
         l_status := DBMS_PARALLEL_EXECUTE.task_status (l_task_name);
      END LOOP;

      DBMS_PARALLEL_EXECUTE.drop_task (l_task_name);
   END;                                        -- RSI_CreateLimitsKindParallel

   PROCEDURE RSI_CreateCashStockLimByKind(
      p_start_id        IN NUMBER,
      p_end_id          IN NUMBER,
      p_CalcDate        IN DATE,
      p_ByMarket        IN NUMBER,
      p_ByOutMarket     IN NUMBER,
      p_MarketCode      IN VARCHAR2,
      p_MarketID        IN NUMBER,
      p_RootSessionID   IN NUMBER DEFAULT NULL,
      p_MainSessionID   IN NUMBER DEFAULT NULL)
   AS
     v_CheckDate    DATE;
     v_Time         DATE;
     v_Kind         NUMBER;
     v_FIRM_ID      VARCHAR2(12) := chr(1);
     v_TAG          VARCHAR2(5) := chr(1);


     TYPE limcashstock_t IS TABLE OF DDL_LIMITCASHSTOCK_INT_TMP%ROWTYPE
                                INDEX BY BINARY_INTEGER;

      v_limcashstock   limcashstock_t;
   BEGIN
     ts_ := SYSTIMESTAMP;
     v_Time := TO_DATE('01-01-0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');

     IF p_start_id = 1
      THEN
         v_Kind := 0;
      ELSIF p_start_id = 2
      THEN
         v_Kind := 1;
      ELSIF p_start_id = 3
      THEN
         v_Kind := 2;
      ELSE
         v_Kind := 365;
      END IF;

     IF    v_Kind = 0 THEN v_CheckDate := p_CalcDate;
     ELSIF v_Kind = 1 THEN v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,1);
     ELSIF v_Kind = 2 THEN v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,2);
     ELSE                  v_CheckDate := TO_DATE('31.12.9999','DD.MM.YYYY');
     END IF;

     --пока считаем, что всё на ММВБ и разделения на биржи нет
     v_FIRM_ID := GetFIRM_ID(p_MarketID,MARKET_KIND_STOCK);
     v_TAG := GetTAG(p_MarketID,MARKET_KIND_STOCK);

     SELECT
       --T_ID
            0,
       --T_DATE
            p_CalcDate,
       --T_TIME
            v_Time,
       --T_MARKET
            p_MarketCode,
       --T_CLIENT
            q.t_Client,
       --T_INTERNALACCOUNT
            q.t_AccountID,
       --T_FIRM_ID
            v_FIRM_ID,
       --T_TAG
            v_TAG,
       --T_CURRID
            q.t_Code_Currency,
       --T_CURR_CODE
            DECODE(q.t_Code_Currency, RSI_RSB_FIInstr.NATCUR, 'SUR', q.t_CCY),
       --T_CLIENT_CODE
            q.Client_code,
       --T_OPEN_BALANCE
            (q.Money306 + q.Plan_Plus_Deal - q.Plan_Minus_Deal ),
       --T_OPEN_LIMIT
            q.Open_Limit,
       --T_CURRENT_LIMIT
            q.Open_Limit,
       --T_LEVERAGE
            q.t_Leverage,
       --T_LIMIT_KIND
            v_Kind,
       --T_MONEY306
            q.Money306,
       --T_DUE474
            0,
       --T_PLAN_PLUS_DEAL
            q.Plan_Plus_Deal,
       --T_PLAN_MINUS_DEAL
            q.Plan_Minus_Deal,
       --T_COMPREVIOUS
            q.ComPrevious,
       --T_ISBLOCKED
            q.IsBlocked,
       --T_MARKET_KIND
            'фондовый',
            q.ContrID,
            q.t_ServKindSub,
            v_CheckDate,
       --T_COMPREVIOUS_1
            q.ComPrevious_1,
       --T_SP
            q.Sp,
       --T_ZCH
            q.Zch
       BULK COLLECT INTO v_limcashstock
       FROM (SELECT DISTINCT
                    t_AccountID,
                    t_Client,
                    t_Account,
                    t_Code_Currency,
                    t_Money306 as Money306,
                    DECODE(t_ServKindSub, 8, 1, 0) as ByMarket,
                    t_ServKindSub,
                    t_CCY,
                    t_Leverage,
                    0 as Open_Limit,
                    0 as Due474,
                    GetSumPlanCashRQ(t_Client, t_ID, t_ServKindSub, p_CalcDate, v_Kind, t_AccountID, t_Code_Currency, 1, p_MarketID) as Plan_Plus_Deal,
                    GetSumPlanCashRQ(t_Client, t_ID, t_ServKindSub, p_CalcDate, v_Kind, t_AccountID, t_Code_Currency, 0, p_MarketID) as Plan_Minus_Deal,
                    GetSumComPrevious(t_ID, p_CalcDate, v_Kind, t_Code_Currency, p_MarketID) as ComPrevious,
                    (CASE WHEN GetObjAtCor(207/*Договор брокерского обслуживания*/, LPAD(t_DlContrID, 34, '0'), 1 /*Признак блокировки*/, p_CalcDate) = 1 THEN 'X' ELSE CHR(0) END) as IsBlocked,
                    t_mpcode AS Client_code,
                    GetSumComPrevious_1(t_ID, p_CalcDate, t_Code_Currency, p_MarketID) as ComPrevious_1,
                    GetSumDebAndCredCash(t_Account, p_CalcDate, 20) as Sp,
                    GetSumDebAndCredCash(t_Account, p_CalcDate, 10) as Zch,
                    t_ID AS ContrID
                FROM dcashstock_dbt
             ) q;

     IF v_limcashstock.COUNT > 0
      THEN
         FORALL indx IN v_limcashstock.FIRST .. v_limcashstock.LAST
            INSERT INTO DDL_LIMITCASHSTOCK_INT_TMP
                 VALUES v_limcashstock (indx);
     END IF;

     InsertLimitFromInt ();
     TimeStamp_ ('Расчет лимита Т' || v_Kind || ' MONEY',
                  p_CalcDate,
                  ts_,
                  SYSTIMESTAMP,
                  p_RootSessionID,
                  p_start_id * 10);
   END; -- RSI_CreateCashStockLimByKind

   PROCEDURE RSI_CreateCashStockLimByKindCur(p_CalcDate IN DATE, p_Kind IN NUMBER, p_IsDepo IN NUMBER)
   AS
     v_CheckDate DATE;
     v_Time DATE;
     v_FIRM_ID VARCHAR2(12) := chr(1);
     v_TAG VARCHAR2(5) := chr(1);

     TYPE limcashstock_t IS TABLE OF DDL_LIMITCASHSTOCK_DBT%ROWTYPE INDEX BY BINARY_INTEGER;
     v_limcashstock limcashstock_t;
   BEGIN
     ts_ := SYSTIMESTAMP;
     v_Time := TO_DATE('01-01-0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');

     IF    p_Kind = 0 THEN v_CheckDate := p_CalcDate;
     ELSIF p_Kind = 1 THEN v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,1);
     ELSIF p_Kind = 2 THEN v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,2);
     ELSE                  v_CheckDate := TO_DATE('31.12.9999','DD.MM.YYYY');
     END IF;

     --пока считаем, что всё на ММВБ и разделения на биржи нет
     v_FIRM_ID := GetFIRM_ID(MarketID,MARKET_KIND_CURR);
     v_TAG := GetTAG(MarketID,MARKET_KIND_CURR);

     SELECT
       --T_ID
            0,
       --T_DATE
            p_CalcDate,
       --T_TIME
            v_Time,
       --T_MARKET
            MarketCode,
       --T_CLIENT
            q.t_Client,
       --T_INTERNALACCOUNT
            q.t_AccountID,
       --T_FIRM_ID
            v_FIRM_ID,
       --T_TAG
            v_TAG,
       --T_CURRID
            q.t_Code_Currency,
       --T_CURR_CODE
            DECODE(q.t_Code_Currency, RSI_RSB_FIInstr.NATCUR, 'SUR', q.t_CCY),
       --T_CLIENT_CODE
            q.Client_code,
       --T_OPEN_BALANCE
            (q.Money306 + q.Plan_Plus_Deal - q.Plan_Minus_Deal),
       --T_OPEN_LIMIT
            q.Open_Limit,
       --T_CURRENT_LIMIT
            q.Open_Limit,
       --T_LEVERAGE
            q.t_LeverageCur,
       --T_LIMIT_KIND
            p_Kind,
       --T_MONEY306
            q.Money306,
       --T_DUE474
            q.ComPrevious,
       --T_PLAN_PLUS_DEAL
            q.Plan_Plus_Deal,
       --T_PLAN_MINUS_DEAL
            q.Plan_Minus_Deal,
       --T_COMPREVIOUS
            q.ComPrevious,
       --T_ISBLOCKED
            q.IsBlocked,
       --T_MARKET_KIND
            'валютный',
       --T_COMPREVIOUS_1
            q.ComPrevious_1,
       --T_SP
            q.Sp,
       --T_ZCH
            q.Zch
       BULK COLLECT INTO v_limcashstock
       FROM (SELECT DISTINCT acc.t_AccountID, acc.t_Client, acc.t_Account, acc.t_Code_Currency,
                    abs(rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_CalcDate-1, acc.t_Chapter, null)) as Money306,
                    1 as ByMarket,
                    sfcontr.t_ServKindSub, curr.t_CCY,
                    dlc.t_LeverageCur,
                    0 as Open_Limit,
                    0 as Due474,
                    GetSumPlanCashPM(acc.t_Client, sfcontr.t_ID, p_CalcDate, p_Kind, acc.t_Account, acc.t_Code_Currency, 1) as Plan_Plus_Deal,
                    GetSumPlanCashPM(acc.t_Client, sfcontr.t_ID, p_CalcDate, p_Kind, acc.t_Account, acc.t_Code_Currency, 0) as Plan_Minus_Deal,
                    GetSumComPrevious(sfcontr.t_ID, p_CalcDate, p_Kind, acc.t_Code_Currency, MarketID) as ComPrevious,
                    (CASE WHEN GetObjAtCor(207/*Договор брокерского обслуживания*/, LPAD(dlc.t_DlContrID, 34, '0'), 1 /*Признак блокировки*/, p_CalcDate) = 1 THEN 'X' ELSE CHR(0) END) as IsBlocked,
                    mp.t_mpcode AS Client_code,
                    GetSumComPrevious_1(sfcontr.t_ID, p_CalcDate, acc.t_Code_Currency, MarketID) as ComPrevious_1,
                    GetSumDebAndCredCash(acc.t_Account, p_CalcDate, 20) as Sp,
                    GetSumDebAndCredCash(acc.t_Account, p_CalcDate, 10) as Zch
               FROM daccount_dbt acc, dmcaccdoc_dbt accdoc, dsfcontr_dbt sfcontr, dfininstr_dbt curr, ddlcontrmp_dbt mp, ddlcontr_dbt dlc
              WHERE acc.t_Chapter = 1
                AND acc.t_Account LIKE '306%'
                AND acc.t_Client NOT IN (SELECT d.t_PartyID from ddp_dep_dbt d)
                AND acc.t_Open_Date < p_CalcDate
                AND (acc.t_Close_Date = TO_DATE('01.01.0001','DD.MM.YYYY') OR acc.t_Close_Date >= p_CalcDate) --!!!
                AND accdoc.t_Chapter = acc.t_Chapter
                AND accdoc.t_Account = acc.t_Account
                AND accdoc.t_Currency = acc.t_Code_Currency
                AND accdoc.t_ClientContrID > 0
                AND sfcontr.t_ID = accdoc.t_ClientContrID
                AND sfcontr.t_ServKind = 21 --Валютный
                AND (sfcontr.t_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY') OR sfcontr.t_DateClose >= p_CalcDate)
                AND curr.t_FIID = acc.t_Code_Currency
                AND curr.t_FI_Kind = 1
                AND EXISTS (Select 1 from DDL_LIMITCHECKDATE_DBT limdate where limdate.t_curID = curr.t_FIID AND T_ISCUR = CHR(88))
                AND mp.t_SfContrID = sfcontr.t_ID
                AND mp.t_MarketID = MarketID
                AND dlc.t_DlContrID = mp.t_DlContrID
                AND curr.t_FIID =  (CASE WHEN p_IsDepo != 1 THEN curr.t_FIID
                      ELSE  RSI_RSB_FIInstr.NATCUR END)
                AND RSI_RSB_GATE.CheckClientIDByStatus(acc.t_Client, 3 /*Обработан (импорт)*/) = CHR(0)
/*
                (CASE WHEN p_IsDepo != 1 THEN 1 = 1
                      ELSE curr.t_FIID = RSI_RSB_FIInstr.NATCUR END)
*/
             ) q;
     TimeStamp_ (
            'Расчет лимита по валютному рынку Т'
         || p_Kind
         || ' MONEY',
         p_CalcDate,
         ts_,
         SYSTIMESTAMP);

     IF v_limcashstock.COUNT > 0 THEN
        FORALL indx IN v_limcashstock.FIRST .. v_limcashstock.LAST
           INSERT INTO DDL_LIMITCASHSTOCK_DBT
                VALUES v_limcashstock(indx);
     END IF;

   END; -- RSI_CreateCashStockLimByKindCur

   PROCEDURE RSI_CreateFutureMarkLim(p_CalcDate IN DATE)
   AS
     v_Time DATE := TO_DATE('01-01-0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');
     v_PrevWorkDate DATE;
     CalendMarketID INTEGER := -1;

     TYPE limfuturemark_t IS TABLE OF DDL_LIMITFUTURMARK_DBT%ROWTYPE INDEX BY BINARY_INTEGER;
     v_limfuturemark limfuturemark_t;
   BEGIN

     BEGIN
       SELECT cal.T_CALENDARID Into CalendMarketID
       FROM dcalcor_dbt cal
       WHERE cal.t_objecttype = 3 -- субъект экономики
            AND LPAD (MarketID, 10, '0') = cal.t_object;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN CalendMarketID := 0;
     END;

     v_PrevWorkDate := RSI_DlCalendars.GetBalanceDateAfterWorkDayByCalendar( p_CalcDate, -1, CalendMarketID, 0);

     SELECT
       --T_ID
            0,
       --T_DATE
            p_CalcDate,
       --T_TIME
            v_Time,
       --T_CLIENT
            q.t_Client,
       --T_INTERNALACCOUNT
            q.t_AccountID,
       --T_CLASS_CODE
            'SPBFUT',
       --T_ACCOUNT
            CAST((CASE WHEN q.TrdAccID IS NULL THEN chr(1) ELSE q.TrdAccID END) as VARCHAR2(25)),
       --T_VOLUMEMN
            (q.Money306 - q.ComPrevious_306),
       --T_VOLUMEPL
            0,
       --T_KFL
            1,
       --T_KGO
            cast((case when q.KGO is null then 1 else rsb_struct.getdouble(q.KGO) end) as NUMBER),
       --T_USE_KGO
            'Да',
       --T_FIRM_ID
            FirmID,
       --T_SECCODE
            chr(1),
       --T_MONEY306
            q.Money306,
       --T_DUE474
            q.ComPrevious_306,
       --T_SUMGO
            q.SUMGO,
       --T_COMPREVIOUS
            q.ComPrevious_RUB,
       --T_ISBLOCKED
            q.IsBlocked,
       --T_MARKET_KIND
            'срочный',
       --T_MARKET
            MarketCode
       BULK COLLECT INTO v_limfuturemark
       FROM (SELECT DISTINCT acc.t_AccountID, acc.t_Client, acc.t_Account, acc.t_Code_Currency,
                    (abs(rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_CalcDate-1, acc.t_Chapter, null)) + GetSumCorr(acc.t_Client, mp.t_FirmID, accdoc.t_DepartmentID, p_CalcDate, v_PrevWorkDate, acc.t_Code_Currency)) as Money306,
                    rsb_struct.getString(rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_SFCONTR, LPAD(sfcontr.t_ID, 10, '0'), 6/*Счет клиента на ММВБ Срочный рынок*/, p_CalcDate)) as TrdAccID,
                    rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_SFCONTR, LPAD(sfcontr.t_ID, 10, '0'), 7/*Коэффициент гарантийного обеспечения*/, p_CalcDate) as KGO,
                    0 /*GetSumFutureComPrevious(acc.t_Client, sfcontr.t_ID, accdoc.t_DepartmentID, p_CalcDate, v_PrevWorkDate, acc.t_Code_Currency, RSI_RSB_FIInstr.NATCUR)*/ as ComPrevious_RUB,
                    0 /*GetSumFutureComPrevious(acc.t_Client, sfcontr.t_ID, accdoc.t_DepartmentID, p_CalcDate, v_PrevWorkDate, acc.t_Code_Currency, acc.t_Code_Currency)*/ as ComPrevious_306,
                    GetSumGuarantyPrevious(acc.t_Client, sfcontr.t_ID, accdoc.t_DepartmentID, p_CalcDate, v_PrevWorkDate, acc.t_Code_Currency) as SUMGO,
                    (CASE WHEN GetObjAtCor(207/*Договор брокерского обслуживания*/, LPAD(dlc.t_DlContrID, 34, '0'), 1 /*Признак блокировки*/, p_CalcDate) = 1 THEN 'X' ELSE CHR(0) END) as IsBlocked,
                    mp.t_FirmID as FirmID
               FROM daccount_dbt acc, dmcaccdoc_dbt accdoc, dsfcontr_dbt sfcontr, dfininstr_dbt curr, ddlcontrmp_dbt mp, ddlcontr_dbt dlc
              WHERE acc.t_Chapter = 1
                AND acc.t_Account LIKE '306%'
                AND acc.t_Client NOT IN (SELECT d.t_PartyID from ddp_dep_dbt d)
                AND acc.t_Open_Date < p_CalcDate
                AND (acc.t_Close_Date = TO_DATE('01.01.0001','DD.MM.YYYY') OR acc.t_Close_Date >= p_CalcDate)
                AND accdoc.t_Chapter = acc.t_Chapter
                AND accdoc.t_Account = acc.t_Account
                AND accdoc.t_Currency = acc.t_Code_Currency
                AND accdoc.t_ClientContrID > 0
                AND sfcontr.t_ID = accdoc.t_ClientContrID
                AND sfcontr.t_ServKind = 15 --Срочные контракты
                AND (sfcontr.t_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY') OR sfcontr.t_DateClose >= p_CalcDate)
                AND sfcontr.t_ServKindSub = 8 --Биржевой рынок
                AND curr.t_FIID = acc.t_Code_Currency
                AND curr.t_FI_Kind = 1
                AND mp.t_SfContrID = sfcontr.t_ID
                AND mp.t_MarketID = MarketID
                AND dlc.t_DlContrID = mp.t_DlContrID
                AND RSI_RSB_GATE.CheckClientIDByStatus(acc.t_Client, 3 /*Обработан (импорт)*/) = CHR(0)
             ) q;

     IF v_limfuturemark.COUNT > 0 THEN
        FORALL indx IN v_limfuturemark.FIRST .. v_limfuturemark.LAST
           INSERT INTO DDL_LIMITFUTURMARK_DBT
                VALUES v_limfuturemark(indx);
     END IF;

   END; -- RSI_CreateFutureMarkLim


   PROCEDURE RSI_CreateCashStockLimits(p_CalcDate IN DATE, p_ByMarket IN NUMBER, p_ByOutMarket IN NUMBER, p_MarketCode IN VARCHAR2,
      p_MarketID     IN NUMBER,p_mainsessionid   IN NUMBER DEFAULT NULL)
   AS

   BEGIN
     IF p_mainsessionid IS NOT NULL
     THEN
         mainsessionid := p_mainsessionid;
     END IF;

     DELETE FROM ddl_limitcashstock_dbt
      WHERE 1 = (CASE WHEN p_ByMarket != 0 AND p_ByOutMarket != 0 THEN 1
                      WHEN p_ByMarket != 0 AND t_Market = p_MarketCode THEN 1
                      WHEN p_ByOutMarket != 0 AND NVL(t_Market,chr(1)) = chr(1) THEN 1
                      ELSE 0 END)
        AND t_Date = p_CalcDate
        AND LOWER(t_Market_Kind) = 'фондовый';

     TimeStamp_ (
         'Старт расчета лимитов по денежным средствам',
         p_CalcDate,
         NULL,
         SYSTIMESTAMP);
      ts_ := SYSTIMESTAMP;

     RSI_CreateLimitsKindParallel (
         'BEGIN rsi_sclimit.RSI_CreateCashStockLimByKind(:start_id, :end_id, TO_DATE('''
         || TO_CHAR (p_CalcDate, 'DD.MM.YYYY')
         || ''',''DD.MM.YYYY''), '
         || TO_CHAR (p_ByMarket)
         || ', '
         || TO_CHAR (p_ByOutMarket)
         || ', '''
         || TO_CHAR (p_MarketCode)
         || ''', '
         || TO_CHAR (p_MarketID)
         || ', '
         || TO_CHAR (USERENV ('sessionid'))
         || ','
         || TO_CHAR (mainsessionid)
         || ' ); END;');

     TimeStamp_ (
         'Завершен расчет лимитов по денежным средствам',
         p_CalcDate,
         NULL,
         SYSTIMESTAMP);
      ts_ := SYSTIMESTAMP;

   END; -- RSI_CreateCashStockLimits

   PROCEDURE RSI_CreateCashStockLimitsCur(p_CalcDate IN DATE, p_IsKind2 IN NUMBER, p_IsDepo IN NUMBER)
   AS

   BEGIN

     DELETE FROM ddl_limitcashstock_dbt
      WHERE t_Date = p_CalcDate AND LOWER(t_Market_Kind) = 'валютный';   -- для валютных только биржа

     RSI_CreateCashStockLimByKindCur(p_CalcDate, 0, p_IsDepo);
     RSI_CreateCashStockLimByKindCur(p_CalcDate, 1, p_IsDepo);
     IF (p_IsKind2 = 1)THEN  -- формировать лимиты для kind 2,365
       RSI_CreateCashStockLimByKindCur(p_CalcDate, 2, p_IsDepo);
       RSI_CreateCashStockLimByKindCur(p_CalcDate, 365, p_IsDepo);
     END IF;
   END; -- RSI_CreateCashStockLimits

   PROCEDURE RSI_CreateSecurLimByKind(p_start_id        IN NUMBER,
                                       p_end_id          IN NUMBER,
                                       p_CalcDate        IN DATE,
                                       p_ByMarket        IN NUMBER,
                                       p_ByOutMarket     IN NUMBER,
                                       p_DepoAcc         IN VARCHAR2,
                                       p_MarketCode         IN VARCHAR2,
                                       p_MarketID     IN NUMBER,
                                       p_RootSessionID   IN NUMBER,
                                       p_MainSessionID   IN NUMBER)
   AS
     v_CheckDate DATE;
     v_Time DATE;
     v_FIRM_ID VARCHAR2(12) := chr(1);
     CalendMarketID INTEGER := -1;
     v_Kind           NUMBER;
p_count NUMBER:= -1;
     TYPE limsecur_t IS TABLE OF DDL_LIMITSECURITES_DBT%ROWTYPE INDEX BY BINARY_INTEGER;
     v_limsecur limsecur_t;

     TYPE limsecur_int IS TABLE OF DDL_LIMITSECURITES_INT_TMP%ROWTYPE
                              INDEX BY BINARY_INTEGER;

      v_limsecur_int   limsecur_int;
   BEGIN

     v_Time := TO_DATE('01-01-0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');

     IF p_MainSessionID IS NOT NULL
      THEN
         mainsessionid := p_MainSessionID;
     END IF;

     EXECUTE IMMEDIATE 'truncate table DDL_LIMITSECURITES_INT_TMP';

     BEGIN
       SELECT cal.T_CALENDARID Into CalendMarketID
       FROM dcalcor_dbt cal
       WHERE cal.t_objecttype = 3 -- субъект экономики
            AND LPAD (p_MarketID, 10, '0') = cal.t_object;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN CalendMarketID:=-1;
     END;

     IF p_start_id = 1
      THEN
         v_Kind := 0;
      ELSIF p_start_id = 2
      THEN
         v_Kind := 1;
      ELSIF p_start_id = 3
      THEN
         v_Kind := 2;
      ELSE
         v_Kind := 365;
      END IF;

     IF CalendMarketID <> -1 THEN
       IF    v_Kind = 0 THEN v_CheckDate := p_CalcDate;
       ELSIF v_Kind = 1 THEN v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,1,CalendMarketID);
       ELSIF v_Kind = 2 THEN v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,2,CalendMarketID);
       ELSE                  v_CheckDate := TO_DATE('31.12.9999','DD.MM.YYYY');
       END IF;
     ELSE
       IF    v_Kind = 0 THEN v_CheckDate := p_CalcDate;
       ELSIF v_Kind = 1 THEN v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,1);
       ELSIF v_Kind = 2 THEN v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,2);
       ELSE                  v_CheckDate := TO_DATE('31.12.9999','DD.MM.YYYY');
       END IF;
     END IF;

     ts_ := SYSTIMESTAMP;

     --пока считаем, что всё на ММВБ и разделения на биржи нет
     v_FIRM_ID := GetFIRM_ID(p_MarketID,RSI_SCLIMIT.MARKET_KIND_STOCK);

     -- Бумаги в наличии
     SELECT
       --T_ID
            0,
       --T_DATE
            p_CalcDate,
       --T_TIME
            v_Time,
       --T_MARKET
            p_MarketCode,
       --T_CLIENT
            q.t_Party,
       --T_SECURITY
            q.t_FIID,
       --T_FIRM_ID
            v_FIRM_ID,
       --T_SECCODE
            CAST((CASE WHEN q.SecCode IS NULL THEN CHR(1) ELSE q.SecCode END) as VARCHAR2(35)),
       --T_CLIENT_CODE
            CAST((CASE WHEN q.ClientCode IS NULL THEN CHR(1) ELSE q.ClientCode END) as VARCHAR2(35)),
       --T_OPEN_BALANCE
            (q.SumQuantity + q.Plan_Plus_Deal - q.Plan_Minus_Deal),
       --T_OPEN_LIMIT
            q.Open_Limit,
       --T_CURRENT_LIMIT
            q.Open_Limit,
       --T_TRDACCID
            CAST((CASE WHEN q.TrdAccID IS NULL THEN p_DepoAcc ELSE q.TrdAccID END) as VARCHAR2(25)),
       --T_WA_POSITION_PRICE
            q.wa_position_price,
       --T_LIMIT_KIND
            v_Kind,
       --T_QUANTITY
            q.SumQuantity,
       --T_PLAN_PLUS_DEAL
            q.Plan_Plus_Deal,
       --T_PLAN_MINUS_DEAL
            q.Plan_Minus_Deal,
       --T_ISBLOCKED
            q.IsBlocked,
       --T_MARKET_KIND
            'фондовый',
            q.t_ServKindSub,
            q.t_Contract,
       -- T_MoneyConsolidated
            CHR(0)
       BULK COLLECT INTO v_limsecur_int
       FROM (SELECT l.t_Sum as SumQuantity, l.t_Party, l.t_Contract, l.t_FIID,
                    0 as Open_Limit,
                    RSI_SCLIMIT.GetObjCodeOnDate(l.t_FIID, 9/*Финансовый инструмент*/, RSI_SCLIMIT.GetKindMarketCodeOrNote(p_MarketCode, 1, 0), p_CalcDate) as SecCode,
                    RSI_RSBPARTY.PT_GetPartyCode(l.t_Party, 1) as ClientCode,
                    rsb_struct.getString(rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_SFCONTR, LPAD(l.t_Contract, 10, '0'), RSI_SCLIMIT.GetKindMarketCodeOrNote(p_MarketCode, 0, 1), p_CalcDate)) as TrdAccID,
                    0 as Plan_Plus_Deal,
                    0 as Plan_Minus_Deal,
                    (CASE WHEN GetObjAtCor(207/*Договор брокерского обслуживания*/, LPAD(dlc.t_DlContrID, 34, '0'), 1 /*Признак блокировки*/, p_CalcDate) = 1 THEN 'X' ELSE CHR(0) END) as IsBlocked,
                    (CASE
                        WHEN p_MarketID > 0
                        THEN
                            RSI_SCLIMIT.GetWAPositionPrice (p_CalcDate,l.t_Party,l.t_Contract,l.t_FIID,RSI_RSBPARTY.PT_GetPartyCode(l.t_Party, 1),RSI_SCLIMIT.GetObjCodeOnDate(l.t_FIID, 9/*Финансовый инструмент*/, 11/*Код на ММВБ*/, p_CalcDate),v_FIRM_ID, v_Kind, p_MarketID)
                        ELSE
                            0
                    END) as wa_position_price,
                    sfcontr.t_ServKindSub
               FROM dsfcontr_dbt sfcontr,
                     DD_LIMITLOTS_DBT l,
                     ddlcontrmp_dbt mp,
                     ddlcontr_dbt dlc
               WHERE     sfcontr.t_ID = l.t_Contract
                     AND mp.t_SfContrID = sfcontr.t_ID
                     AND mp.t_MarketID = p_MarketID
                     AND dlc.t_DlContrID = mp.t_DlContrID
            ) q;

     IF v_limsecur_int.COUNT > 0
      THEN
         FORALL indx IN v_limsecur_int.FIRST .. v_limsecur_int.LAST
            INSERT INTO DDL_LIMITSECURITES_INT_TMP
                 VALUES v_limsecur_int (indx);
      END IF;

     UpdSumPlanAvrRQ (p_CalcDate, v_CheckDate, p_MarketID);
     InsertLimitFromIntSecur;

     TimeStamp_ (
            'Расчет лимита Т'
         || v_Kind
         || ' DEPO по бумагам в наличии',
         p_CalcDate,
         ts_,
         SYSTIMESTAMP,
         p_RootSessionID,
         p_start_id * 10 + 40);

     ts_ := SYSTIMESTAMP;

     --Выпуски, по которым нет остатка, но имеются неисполненные ТО
     WITH l AS (SELECT q.Party, q.Contract, q.FIID,
                       GetSumPlanAvrRQ(q.Party, q.Contract, sfcontr.t_ServKindSub, p_CalcDate, v_CheckDate, q.FIID, 1, p_MarketID) as Plan_Plus_Deal,
                       GetSumPlanAvrRQ(q.Party, q.Contract, sfcontr.t_ServKindSub, p_CalcDate, v_CheckDate, q.FIID, 0, p_MarketID) as Plan_Minus_Deal
                  FROM (SELECT DISTINCT tk.t_ClientID as Party, tk.t_ClientContrID as Contract, rq.t_FIID as FIID
                          FROM ddlrq_dbt rq, ddl_tick_dbt tk
                         WHERE rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                           AND (  rq.t_FactDate = TO_DATE('01.01.0001','DD.MM.YYYY')
                               OR NOT EXISTS(SELECT 1 FROM dpmwrtsum_dbt lot WHERE lot.t_DocKind = 29 AND lot.t_DocID = rq.t_ID)
                               OR rq.t_FactDate > p_CalcDate
                               )
                           AND tk.t_BOfficeKind = rq.t_DocKind
                           AND tk.t_DealID = rq.t_DocID
                           AND tk.t_ClientContrID > 0
                           AND tk.t_MarketID = p_MarketID
                         UNION
                         SELECT DISTINCT tk.t_PartyID as Party, tk.t_PartyContrID as Contract, rq.t_FIID as FIID
                           FROM ddlrq_dbt rq, ddl_tick_dbt tk
                          WHERE rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                            AND (  rq.t_FactDate = TO_DATE('01.01.0001','DD.MM.YYYY')
                                OR NOT EXISTS(SELECT 1 FROM dpmwrtsum_dbt lot WHERE lot.t_DocKind = 29 AND lot.t_DocID = rq.t_ID)
                                OR rq.t_FactDate > p_CalcDate
                                )
                            AND tk.t_BOfficeKind = rq.t_DocKind
                            AND tk.t_DealID = rq.t_DocID
                            AND tk.t_IsPartyClient = 'X'
                            AND tk.t_PartyContrID > 0
                            AND tk.t_MarketID = p_MarketID
                        ) q, dsfcontr_dbt sfcontr
                 WHERE sfcontr.t_ID = q.Contract
                   AND RSI_RSB_GATE.CheckClientIDByStatus(q.Party, 3 /*Обработан (импорт)*/) = CHR(0)
                   AND NOT EXISTS(SELECT 1
                                       FROM DD_LIMITLOTS_DBT lot
                                      WHERE     lot.t_Party = q.Party
                                            AND lot.t_Contract = q.Contract
                                            AND lot.t_FIID = q.FIID)
               )
     SELECT
       --T_ID
            0,
       --T_DATE
            p_CalcDate,
       --T_TIME
            v_Time,
       --T_MARKET
            p_MarketCode,
       --T_CLIENT
            q.Party,
       --T_SECURITY
            q.FIID,
       --T_FIRM_ID
            v_FIRM_ID,
       --T_SECCODE
            CAST((CASE WHEN q.SecCode IS NULL THEN CHR(1) ELSE q.SecCode END) as VARCHAR2(35)),
       --T_CLIENT_CODE
            CAST((CASE WHEN q.ClientCode IS NULL THEN CHR(1) ELSE q.ClientCode END) as VARCHAR2(35)),
       --T_OPEN_BALANCE
            (q.SumQuantity + q.Plan_Plus_Deal - q.Plan_Minus_Deal),
       --T_OPEN_LIMIT
            q.Open_Limit,
       --T_CURRENT_LIMIT
            q.Open_Limit,
       --T_TRDACCID
            CAST((CASE WHEN q.TrdAccID IS NULL THEN p_DepoAcc ELSE q.TrdAccID END) as VARCHAR2(25)),
       --T_WA_POSITION_PRICE
            q.wa_position_price,
       --T_LIMIT_KIND
            v_Kind,
       --T_QUANTITY
            q.SumQuantity,
       --T_PLAN_PLUS_DEAL
            q.Plan_Plus_Deal,
       --T_PLAN_MINUS_DEAL
            q.Plan_Minus_Deal,
       --T_ISBLOCKED
            q.IsBlocked,
       --T_MARKET_KIND
            'фондовый',
       --T_MoneyConsolidated
            CHR(0)
       BULK COLLECT INTO v_limsecur
       FROM (SELECT 0 as SumQuantity, 0 as SumCostRub, l.Party, l.Contract, l.FIID,
                    DECODE(sfcontr.t_ServKindSub, 8, 1, 0) as ByMarket,
                    0 as Open_Limit,
                    GetObjCodeOnDate(l.FIID, 9/*Финансовый инструмент*/, RSI_SCLIMIT.GetKindMarketCodeOrNote(p_MarketCode, 1, 0), p_CalcDate) as SecCode,
                    RSI_RSBPARTY.PT_GetPartyCode(l.Party, 1) as ClientCode,
                    rsb_struct.getString(rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_SFCONTR, LPAD(l.Contract, 10, '0'), RSI_SCLIMIT.GetKindMarketCodeOrNote(p_MarketCode, 0, 1), p_CalcDate)) as TrdAccID,
                    l.Plan_Plus_Deal,
                    l.Plan_Minus_Deal,
                    (CASE WHEN GetObjAtCor(207/*Договор брокерского обслуживания*/, LPAD(dlc.t_DlContrID, 34, '0'), 1 /*Признак блокировки*/, p_CalcDate) = 1 THEN 'X' ELSE CHR(0) END) as IsBlocked,
                    (CASE
                        WHEN p_MarketID > 0
                        THEN
                            GetWAPositionPrice (p_CalcDate,l.Party,l.Contract,l.FIID,RSI_RSBPARTY.PT_GetPartyCode(l.Party, 1),GetObjCodeOnDate(l.FIID, 9/*Финансовый инструмент*/, 11/*Код на ММВБ*/, p_CalcDate),v_FIRM_ID, v_Kind, p_MarketID)
                        ELSE
                            0
                    END) as wa_position_price
               FROM dsfcontr_dbt sfcontr, l, ddlcontrmp_dbt mp, ddlcontr_dbt dlc
              WHERE sfcontr.t_ID = l.Contract
                AND mp.t_SfContrID = sfcontr.t_ID
                AND mp.t_MarketID = p_MarketID
                AND dlc.t_DlContrID = mp.t_DlContrID
                AND (l.Plan_Plus_Deal > 0 OR l.Plan_Minus_Deal > 0)
            ) q;

     TimeStamp_ (
            'Расчет лимита Т'
         || v_Kind
         || ' DEPO по бумагам в поставке',
         p_CalcDate,
         ts_,
         SYSTIMESTAMP,
         p_RootSessionID,
         p_start_id * 10 + 41);

     IF v_limsecur.COUNT > 0 THEN
        FORALL indx IN v_limsecur.FIRST .. v_limsecur.LAST
           INSERT INTO DDL_LIMITSECURITES_DBT
                VALUES v_limsecur(indx);
     END IF;

   END; -- RSI_CreateSecurLimByKind

   PROCEDURE RSI_CreateSecurLimByKindCur(p_CalcDate IN DATE, p_Kind IN NUMBER, p_DepoAcc IN VARCHAR2)
   AS
     v_CheckDate DATE;
     v_Time DATE;
     v_FIRM_ID VARCHAR2(12) := chr(1);

     TYPE limsecur_t IS TABLE OF DDL_LIMITSECURITES_DBT%ROWTYPE INDEX BY BINARY_INTEGER;
     v_limsecur limsecur_t;
   BEGIN

     v_Time := TO_DATE('01-01-0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');

     IF    p_Kind = 0 THEN v_CheckDate := p_CalcDate;
     ELSIF p_Kind = 1 THEN v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,1);
     ELSIF p_Kind = 2 THEN v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,2);
     ELSE                  v_CheckDate := TO_DATE('31.12.9999','DD.MM.YYYY');
     END IF;

     --пока считаем, что всё на ММВБ и разделения на биржи нет
     v_FIRM_ID := GetFIRM_ID(MarketID,MARKET_KIND_CURR);

     ts_ := SYSTIMESTAMP;

     SELECT
       --T_ID
            0,
       --T_DATE
            p_CalcDate,
       --T_TIME
            v_Time,
       --T_MARKET
            MarketCode,
       --T_CLIENT
            q.t_Client,
       --T_SECURITY
            -1,   -- не заполняется -- или 0?
       --T_FIRM_ID
            v_FIRM_ID,
       --T_SECCODE
            q.t_CCY,
       --T_CLIENT_CODE
            CAST((CASE WHEN q.Client_code IS NULL THEN CHR(1) ELSE q.Client_code END) as VARCHAR2(35)),
       --T_OPEN_BALANCE
            (q.SumQuantity + q.Plan_Plus_Deal - q.Plan_Minus_Deal),
       --T_OPEN_LIMIT
            q.Open_Limit,
       --T_CURRENT_LIMIT
            q.Open_Limit,
       --T_TRDACCID
            CAST((CASE WHEN q.TrdAccID IS NULL THEN p_DepoAcc ELSE q.TrdAccID END) as VARCHAR2(25)),
       --T_WA_POSITION_PRICE
            0,
       --T_LIMIT_KIND
            p_Kind,
       --T_QUANTITY
            q.SumQuantity,
       --T_PLAN_PLUS_DEAL
            q.Plan_Plus_Deal,
       --T_PLAN_MINUS_DEAL
            q.Plan_Minus_Deal,
       --T_ISBLOCKED
            q.IsBlocked,
       --T_MARKET_KIND
            'валютный',
       -- T_MoneyConsolidated
            CHR(0)
       BULK COLLECT INTO v_limsecur

       FROM (SELECT DISTINCT acc.t_AccountID, acc.t_Client, acc.t_Account, acc.t_Code_Currency,
                    abs(rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_CalcDate-1, acc.t_Chapter, null)) as SumQuantity, -- Money306
                    1 as ByMarket,
                    sfcontr.t_ServKindSub, curr.t_CCY,
                    dlc.t_LeverageCur,
                    0 as Open_Limit,
                    0 as Due474,
                    GetSumPlanCashPM(acc.t_Client, sfcontr.t_ID, p_CalcDate, p_Kind, acc.t_Account, acc.t_Code_Currency, 1) as Plan_Plus_Deal,
                    GetSumPlanCashPM(acc.t_Client, sfcontr.t_ID, p_CalcDate, p_Kind, acc.t_Account, acc.t_Code_Currency, 0) as Plan_Minus_Deal,
                    0 as ComPrevious,  -- пока 0
                    (CASE WHEN GetObjAtCor(207/*Договор брокерского обслуживания*/, LPAD(dlc.t_DlContrID, 34, '0'), 1 /*Признак блокировки*/, p_CalcDate) = 1 THEN 'X' ELSE CHR(0) END) as IsBlocked,
                    mp.t_mpcode AS Client_code,
                    rsb_struct.getString(rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_SFCONTR, LPAD(sfcontr.t_ID, 10, '0'), 8/*Счет клиента на ММВБ Валютный сектор*/, p_CalcDate)) as TrdAccID
               FROM daccount_dbt acc, dmcaccdoc_dbt accdoc, dsfcontr_dbt sfcontr, dfininstr_dbt curr, ddlcontrmp_dbt mp, ddlcontr_dbt dlc
              WHERE acc.t_Chapter = 1
                AND acc.t_Account LIKE '306%'
                AND acc.t_Client NOT IN (SELECT d.t_PartyID from ddp_dep_dbt d)
                AND acc.t_Open_Date < p_CalcDate
                AND (acc.t_Close_Date = TO_DATE('01.01.0001','DD.MM.YYYY') OR acc.t_Close_Date >= p_CalcDate) --!!!
                AND accdoc.t_Chapter = acc.t_Chapter
                AND accdoc.t_Account = acc.t_Account
                AND accdoc.t_Currency = acc.t_Code_Currency
                AND accdoc.t_ClientContrID > 0
                AND sfcontr.t_ID = accdoc.t_ClientContrID
                AND sfcontr.t_ServKind = 21 --Валютный
                AND (sfcontr.t_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY') OR sfcontr.t_DateClose >= p_CalcDate)
                AND curr.t_FIID = acc.t_Code_Currency
                AND curr.t_FI_Kind = 1
                AND EXISTS (Select 1 from DDL_LIMITCHECKDATE_DBT limdate where limdate.t_curID = curr.t_FIID AND T_ISCUR = CHR(88))
                AND mp.t_SfContrID = sfcontr.t_ID
                AND mp.t_MarketID = MarketID
                AND dlc.t_DlContrID = mp.t_DlContrID
                AND curr.t_FIID <> RSI_RSB_FIInstr.NATCUR
                AND RSI_RSB_GATE.CheckClientIDByStatus(acc.t_Client, 3 /*Обработан (импорт)*/) = CHR(0)
             ) q;

     TimeStamp_ ('Расчет по валютному рынку',
                  p_CalcDate,
                  NULL,
                  SYSTIMESTAMP);

     IF v_limsecur.COUNT > 0 THEN
        FORALL indx IN v_limsecur.FIRST .. v_limsecur.LAST
           INSERT INTO DDL_LIMITSECURITES_DBT
                VALUES v_limsecur(indx);
     END IF;

   END; -- RSI_CreateSecurLimByKindCur

   PROCEDURE RSI_CreateSecurLimByKindCurZero (p_CalcDate IN DATE, p_Kind IN NUMBER, p_DepoAcc IN VARCHAR2)
   AS
      v_CheckDate   DATE;
      v_Time        DATE;
      v_FIRM_ID     VARCHAR2 (12) := CHR (1);

      TYPE limsecur_t IS TABLE OF DDL_LIMITSECURITES_DBT%ROWTYPE
                            INDEX BY BINARY_INTEGER;

      v_limsecur    limsecur_t;
   BEGIN
      v_Time := TO_DATE ('01-01-0001:' || TO_CHAR (SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');
      IF p_Kind = 0
      THEN
         v_CheckDate := p_CalcDate;
      ELSIF p_Kind = 1
      THEN
         v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay (p_CalcDate, 1);
      ELSIF p_Kind = 2
      THEN
         v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay (p_CalcDate, 2);
      ELSE
         v_CheckDate := TO_DATE ('31.12.9999', 'DD.MM.YYYY');
      END IF;

      --пока считаем, что всё на ММВБ и разделения на биржи нет
      v_FIRM_ID := GetFIRM_ID (MarketID, MARKET_KIND_CURR);

      DELETE FROM DDL_LIMITSECURITES_DBT where t_Date = p_CalcDate AND t_market_kind = 'валютный';

     ts_ := SYSTIMESTAMP;

   SELECT    --DISTINCT --T_ID
             0,
             --T_DATE
             p_CalcDate,
             --T_TIME
             v_Time,
             --T_MARKET
             MarketCode,
             --T_CLIENT
             q.t_Client,
             --T_SECURITY
             -1,
             --T_FIRM_ID
             v_FIRM_ID,
             --T_SECCODE
             'USD000UTSTOM' t_ccy,
             --T_CLIENT_CODE
             CAST (
                (CASE
                    WHEN q.Client_code IS NULL THEN CHR (1)
                    ELSE q.Client_code
                 END) AS VARCHAR2 (35)),
             --T_OPEN_BALANCE
             --(q.SumQuantity + q.Plan_Plus_Deal - q.Plan_Minus_Deal),
             0,
             --T_OPEN_LIMIT
             q.Open_Limit,
             --T_CURRENT_LIMIT
             q.Open_Limit,
             --T_TRDACCID
             CAST (
                (CASE
                    WHEN q.TrdAccID IS NULL THEN p_DepoAcc
                    ELSE q.TrdAccID
                 END) AS VARCHAR2 (25)),
             --T_WA_POSITION_PRICE
             0,
             --T_LIMIT_KIND
             p_Kind,
             --T_QUANTITY
             q.SumQuantity,
             --T_PLAN_PLUS_DEAL
             q.Plan_Plus_Deal,
             --T_PLAN_MINUS_DEAL
             q.Plan_Minus_Deal,
             --T_ISBLOCKED
             q.IsBlocked,
             --T_MARKET_KIND
             'валютный',
             --T_MoneyConsolidated
             CHR(0)
        BULK COLLECT INTO v_limsecur
        FROM (SELECT DISTINCT sfcontr.t_PartyID as t_Client,
                              0 AS SumQuantity,                   -- Money306
                              1 AS ByMarket,
                              sfcontr.t_ServKindSub,
                              dlc.t_LeverageCur,
                              0 AS Open_Limit,
                              0 AS Due474,
                              0
                                 AS Plan_Plus_Deal,
                             0
                                 AS Plan_Minus_Deal,
                              0 AS ComPrevious,
                              (CASE
                                  WHEN GetObjAtCor (
                                          207 /*Договор брокерского обслуживания*/
                                             ,
                                          LPAD (dlc.t_DlContrID, 34, '0'),
                                          1             /*Признак блокировки*/
                                           ,
                                          p_CalcDate) = 1 THEN 'X'
                                  ELSE CHR (0)
                               END)
                                 AS IsBlocked,
                              mp.t_mpcode AS Client_code,
                              rsb_struct.getString (rsi_rsb_kernel.
                                                     GetNote (
                                                       RSB_SECUR.
                                                        OBJTYPE_SFCONTR,
                                                       LPAD (sfcontr.t_ID, 10, '0'),
                                                       8 /*Счет клиента на ММВБ Валютный сектор*/
                                                        ,
                                                       p_CalcDate))
                                 AS TrdAccID
                FROM dsfcontr_dbt sfcontr,
                     ddlcontrmp_dbt mp,
                     ddlcontr_dbt dlc
               WHERE  sfcontr.t_ServKind = 21                    --Валютный
                 AND (sfcontr.t_DateClose = TO_DATE ('01.01.0001', 'DD.MM.YYYY') OR sfcontr.t_DateClose >= p_CalcDate)
                 AND mp.t_SfContrID = sfcontr.t_ID
                 AND mp.t_MarketID = MarketID
                 AND dlc.t_DlContrID = mp.t_DlContrID
                 AND RSI_RSB_GATE.CheckClientIDByStatus(sfcontr.t_PartyID, 3 /*Обработан (импорт)*/) = CHR(0)
                 AND sfcontr.t_ID IN(SELECT DISTINCT  accdoc.t_ClientContrID
                                       FROM daccount_dbt acc, dmcaccdoc_dbt accdoc
                                      WHERE acc.t_Chapter = 1
                                        AND acc.t_Account LIKE '306%'
                                        AND acc.t_Client NOT IN (SELECT d.t_PartyID FROM ddp_dep_dbt d)
                                        AND acc.t_Open_Date < p_CalcDate
                                        AND (acc.t_Close_Date = TO_DATE ('01.01.0001', 'DD.MM.YYYY') OR acc.t_Close_Date >= p_CalcDate)
                                        AND accdoc.t_Chapter = acc.t_Chapter
                                        AND accdoc.t_Account = acc.t_Account
                                        AND accdoc.t_Currency = acc.t_Code_Currency
                                        AND accdoc.t_ClientContrID > 0
                                        AND abs(rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_CalcDate-1, acc.t_Chapter, null))<> 0)
                 ) q ;

     TimeStamp_ ('Расчет по валютному рынку',
                  p_CalcDate,
                  NULL,
                  SYSTIMESTAMP);

      IF v_limsecur.COUNT > 0
      THEN
         FORALL indx IN v_limsecur.FIRST .. v_limsecur.LAST
            INSERT INTO DDL_LIMITSECURITES_DBT
                 VALUES v_limsecur (indx);
      END IF;
   END; -- RSI_CreateSecurLimByKindCurZero

   PROCEDURE RSI_CreateSecurLimByKindZero (p_CalcDate IN DATE, p_Kind IN NUMBER, p_DepoAcc IN VARCHAR2, p_MarketID IN INTEGER, p_MarketCode IN VARCHAR2)
   AS
      v_CheckDate   DATE;
      v_Time        DATE;
      v_FIRM_ID     VARCHAR2 (12) := CHR (1);
      v_CodeSCZeroLimit VARCHAR2 (12) := CHR(1);

      TYPE limsecur_t IS TABLE OF DDL_LIMITSECURITES_DBT%ROWTYPE
                            INDEX BY BINARY_INTEGER;

      v_limsecur    limsecur_t;
   BEGIN
      v_Time := TO_DATE ('01-01-0001:' || TO_CHAR (SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');
      IF p_Kind = 0
      THEN
         v_CheckDate := p_CalcDate;
      ELSIF p_Kind = 1
      THEN
         v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay (p_CalcDate, 1);
      ELSIF p_Kind = 2
      THEN
         v_CheckDate := RSI_RsbCalendar.GetDateAfterWorkDay (p_CalcDate, 2);
      ELSE
         v_CheckDate := TO_DATE ('31.12.9999', 'DD.MM.YYYY');
      END IF;

      --пока считаем, что всё на ММВБ и разделения на биржи нет
      v_FIRM_ID := GetFIRM_ID (p_MarketID, MARKET_KIND_STOCK);

      BEGIN
         SELECT t_CodeSCZeroLimit INTO v_CodeSCZeroLimit
           FROM ddl_limitprm_dbt
          WHERE t_MarketKind = 1 AND t_MarketID = p_MarketID;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN v_CodeSCZeroLimit:= CHR(1);
      END;

      ts_ := SYSTIMESTAMP;

   SELECT    --DISTINCT --T_ID
             0,
             --T_DATE
             p_CalcDate,
             --T_TIME
             v_Time,
             --T_MARKET
             p_MarketCode,
             --T_CLIENT
             q.t_Client,
             --T_SECURITY
             -1,
             --T_FIRM_ID
             v_FIRM_ID,
             --T_SECCODE
             v_CodeSCZeroLimit,
             --T_CLIENT_CODE
             CAST (
                (CASE
                    WHEN q.Client_code IS NULL THEN CHR (1)
                    ELSE q.Client_code
                 END) AS VARCHAR2 (35)),
             --T_OPEN_BALANCE
             --(q.SumQuantity + q.Plan_Plus_Deal - q.Plan_Minus_Deal),
             0,
             --T_OPEN_LIMIT
             q.Open_Limit,
             --T_CURRENT_LIMIT
             q.Open_Limit,
             --T_TRDACCID
             CAST (
                (CASE
                    WHEN q.TrdAccID IS NULL THEN p_DepoAcc
                    ELSE q.TrdAccID
                 END) AS VARCHAR2 (25)),
             --T_WA_POSITION_PRICE
             0,
             --T_LIMIT_KIND
             p_Kind,
             --T_QUANTITY
             q.SumQuantity,
             --T_PLAN_PLUS_DEAL
             q.Plan_Plus_Deal,
             --T_PLAN_MINUS_DEAL
             q.Plan_Minus_Deal,
             --T_ISBLOCKED
             q.IsBlocked,
             --T_MARKET_KIND
             'фондовый',
             --T_MoneyConsolidated
             CHR(0)
        BULK COLLECT INTO v_limsecur
        FROM (SELECT DISTINCT sfcontr.t_PartyID as t_Client,
                              0 AS SumQuantity,                   -- Money306
                              1 AS ByMarket,
                              sfcontr.t_ServKindSub,
                              dlc.t_LeverageCur,
                              0 AS Open_Limit,
                              0 AS Due474,
                              0
                                 AS Plan_Plus_Deal,
                             0
                                 AS Plan_Minus_Deal,
                              0 AS ComPrevious,
                              (CASE
                                  WHEN GetObjAtCor (
                                          207 /*Договор брокерского обслуживания*/
                                             ,
                                          LPAD (dlc.t_DlContrID, 34, '0'),
                                          1             /*Признак блокировки*/
                                           ,
                                          p_CalcDate) = 1 THEN 'X'
                                  ELSE CHR (0)
                               END)
                                 AS IsBlocked,
                              mp.t_mpcode AS Client_code,
                              rsb_struct.getString (rsi_rsb_kernel.
                                                     GetNote (
                                                       RSB_SECUR.
                                                        OBJTYPE_SFCONTR,
                                                       LPAD (sfcontr.t_ID, 10, '0'),
                                                       RSI_SCLIMIT.GetKindMarketCodeOrNote(p_MarketCode, 0, 1),
                                                       p_CalcDate))
                                 AS TrdAccID
                FROM dsfcontr_dbt sfcontr,
                     ddlcontrmp_dbt mp,
                     ddlcontr_dbt dlc
               WHERE  sfcontr.t_ServKind = 1                    --Фондовый
                 AND sfcontr.t_ServKindSub = 8
                 AND NOT EXISTS (SELECT limsec.t_Client FROM DDL_LIMITSECURITES_DBT limsec WHERE limsec.t_Client = sfcontr.t_PartyID AND LOWER(limsec.t_Market_Kind) = 'фондовый' )
                 AND (sfcontr.t_DateClose = TO_DATE ('01.01.0001', 'DD.MM.YYYY') OR sfcontr.t_DateClose >= p_CalcDate)
                 AND mp.t_SfContrID = sfcontr.t_ID
                 AND mp.t_MarketID = p_MarketID
                 AND dlc.t_DlContrID = mp.t_DlContrID
                 AND RSI_RSB_GATE.CheckClientIDByStatus(sfcontr.t_PartyID, 3 /*Обработан (импорт)*/) = CHR(0)
                 AND sfcontr.t_ID IN(SELECT DISTINCT  accdoc.t_ClientContrID
                                       FROM daccount_dbt acc, dmcaccdoc_dbt accdoc
                                      WHERE acc.t_Chapter = 1
                                        AND acc.t_Account LIKE '306%'
                                        AND acc.t_Client NOT IN (SELECT d.t_PartyID FROM ddp_dep_dbt d)
                                        AND acc.t_Open_Date < p_CalcDate
                                        AND (acc.t_Close_Date = TO_DATE ('01.01.0001', 'DD.MM.YYYY') OR acc.t_Close_Date >= p_CalcDate)
                                        AND accdoc.t_Chapter = acc.t_Chapter
                                        AND accdoc.t_Account = acc.t_Account
                                        AND accdoc.t_Currency = acc.t_Code_Currency
                                        AND accdoc.t_ClientContrID > 0
                                        AND abs(rsb_account.restac(acc.t_Account, acc.t_Code_Currency, p_CalcDate-1, acc.t_Chapter, null))<> 0)
                 ) q ;

     TimeStamp_ (
            'Расчет лимита Т'
         || p_Kind
         || ' DEPO нулевые лимиты GAZP',
         p_CalcDate,
         ts_,
         SYSTIMESTAMP);

      IF v_limsecur.COUNT > 0
      THEN
         FORALL indx IN v_limsecur.FIRST .. v_limsecur.LAST
            INSERT INTO DDL_LIMITSECURITES_DBT
                 VALUES v_limsecur (indx);
      END IF;
   END; -- RSI_CreateSecurLimByKindZero

   PROCEDURE RSI_CreateSecurLimits(p_CalcDate IN DATE, p_ByMarket IN NUMBER, p_ByOutMarket IN NUMBER, p_DepoAcc IN VARCHAR2,
                                     p_MarketCode IN VARCHAR2, p_MarketID     IN NUMBER,p_mainsessionid   IN NUMBER DEFAULT NULL)
   AS

   BEGIN
     IF p_mainsessionid IS NOT NULL
      THEN
         mainsessionid := p_mainsessionid;
     END IF;

     DELETE FROM ddl_limitsecurites_dbt
      WHERE 1 = (CASE WHEN p_ByMarket != 0 AND p_ByOutMarket != 0 THEN 1
                      WHEN p_ByMarket != 0 AND t_Market = p_MarketCode THEN 1
                      WHEN p_ByOutMarket != 0 AND NVL(t_Market,chr(1)) = chr(1) THEN 1
                      ELSE 0 END)
                  AND t_Date = p_CalcDate
                  AND LOWER(t_Market_Kind) = 'фондовый';

     TimeStamp_ (
         'Старт расчета лимитов по ценным бумагам',
         p_CalcDate,
         NULL,
         SYSTIMESTAMP);
      ts_ := SYSTIMESTAMP;

     RSI_CreateLimitsKindParallel (
         'BEGIN rsi_sclimit.RSI_CreateSecurLimByKind(:start_id, :end_id, TO_DATE('''
         || TO_CHAR (p_CalcDate, 'DD.MM.YYYY')
         || ''',''DD.MM.YYYY''), '
         || TO_CHAR (p_ByMarket)
         || ', '
         || TO_CHAR (p_ByOutMarket)
         || ', '''
         || TO_CHAR(p_DepoAcc)
         || ''', '''
         || TO_CHAR(p_MarketCode)
         || ''', '
         || TO_CHAR (p_MarketID)
         || ', '
         || TO_CHAR (USERENV ('sessionid'))
         || ', '
         || TO_CHAR (mainsessionid)
         || ' ); END;');

     IF p_ByMarket != 0 THEN
        RSI_CreateSecurLimByKindZero(p_CalcDate, 0, p_DepoAcc, p_MarketID, p_MarketCode);
     END IF;

     TimeStamp_ (
         'Завершен расчет лимитов по ценным бумагам',
         p_CalcDate,
         NULL,
         SYSTIMESTAMP);
   END; -- RSI_CreateSecurLimits

   PROCEDURE RSI_CreateSecurLimitsCur (p_CalcDate IN DATE, p_IsKind2 IN NUMBER, p_DepoAcc IN VARCHAR2 )
   AS

   BEGIN

     DELETE FROM ddl_limitsecurites_dbt
      WHERE t_Date = p_CalcDate AND LOWER(t_Market_Kind) = 'валютный';    -- для валютного только биржа

     RSI_CreateSecurLimByKindCur(p_CalcDate, 0, p_DepoAcc);
     RSI_CreateSecurLimByKindCur(p_CalcDate, 1, p_DepoAcc);
     IF (p_IsKind2 = 1)THEN  -- формировать лимиты для kind 2,365
       RSI_CreateSecurLimByKindCur(p_CalcDate, 2, p_DepoAcc);
       RSI_CreateSecurLimByKindCur(p_CalcDate, 365, p_DepoAcc);
     END IF;
   END; -- RSI_CreateSecurLimits

   PROCEDURE RSI_CreateFutureMarkLimits(p_CalcDate IN DATE)
   AS

   BEGIN

     DELETE FROM DDL_LIMITFUTURMARK_DBT WHERE t_Date = p_CalcDate;

     RSI_CreateFutureMarkLim(p_CalcDate);

   END; -- RSI_CreateSecurLimits

   --общие данные для всех лимитов можно собрать однократно
   PROCEDURE RSI_FillContrTable (p_CalcDate      IN DATE,
                                 p_ByMarket      IN NUMBER,
                                 p_ByOutMarket   IN NUMBER,
                                 p_MarketID      IN NUMBER)
   AS
    v_PrevWorkDate DATE;
    CalendMarketID INTEGER := -1;
   BEGIN
      ts_ := SYSTIMESTAMP;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE dcashstock_dbt';

      EXECUTE IMMEDIATE 'TRUNCATE TABLE dtick_dbt';

      BEGIN
       SELECT cal.T_CALENDARID Into CalendMarketID
       FROM dcalcor_dbt cal
       WHERE cal.t_objecttype = 3 -- субъект экономики
            AND LPAD (p_MarketID, 10, '0') = cal.t_object;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN CalendMarketID:=-1;
     END;

     IF CalendMarketID <> -1 THEN
       v_PrevWorkDate := RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,-1,CalendMarketID);
     ELSE
       v_PrevWorkDate := RSI_RsbCalendar.GetDateAfterWorkDay(p_CalcDate,1);
     END IF;

      INSERT INTO dcashstock_dbt
         SELECT                                                 /*+ ORDERED */
               DISTINCT sfcontr.t_id,
                        sfcontr.t_partyid,
                        dlc.t_dlcontrid,
                        acc.t_AccountID,
                        acc.t_Client,
                        acc.t_Account,
                        acc.t_Code_Currency,
                        acc.t_chapter,
                        (rsb_account.restac (acc.t_Account,
                                             acc.t_Code_Currency,
                                             p_CalcDate - 1,
                                             acc.t_Chapter,
                                             NULL))
                           AS Money306,
                        0,
                        DECODE (sfcontr.t_ServKindSub, 8, 1, 0) AS Market,
                        mp.t_mpcode,
                        sfcontr.t_ServKindSub,
                        curr.t_CCY,
                        dlc.t_Leverage,
                        0 AS Open_Limit,
                        (CASE
                            WHEN GetObjAtCor (
                                    207,
                                    LPAD (dlc.t_DlContrID, 34, '0'),
                                    1,
                                    p_CalcDate) = 1 THEN 'X'
                            ELSE CHR (0)
                         END)
                           AS IsBlocked,
                         CHR (0) AS NotExcludeRepo
           FROM ddlcontr_dbt dlc,
                ddlcontrmp_dbt mp,
                dsfcontr_dbt sfcontr,
                dmcaccdoc_dbt accdoc,
                daccount_dbt acc,
                dfininstr_dbt curr
          WHERE     acc.t_Chapter = 1
                AND acc.t_Account LIKE '306%'
                AND acc.t_Client NOT IN (SELECT d.t_PartyID from ddp_dep_dbt d)
                AND acc.t_Open_Date < p_CalcDate
                AND (acc.t_Close_Date = TO_DATE('01.01.0001','DD.MM.YYYY') OR acc.t_Close_Date >= p_CalcDate) --!!!
                AND accdoc.t_Chapter = acc.t_Chapter
                AND accdoc.t_Account = acc.t_Account
                AND accdoc.t_Currency = acc.t_Code_Currency
                AND accdoc.t_ClientContrID > 0
                AND sfcontr.t_ID = accdoc.t_ClientContrID
                AND sfcontr.t_ServKind = 1 --Фондовый дилинг
                AND (sfcontr.t_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY') OR sfcontr.t_DateClose >= p_CalcDate)
                AND 1 = (CASE WHEN p_ByMarket != 0 AND p_ByOutMarket != 0 THEN 1
                              WHEN p_ByMarket != 0 AND sfcontr.t_ServKindSub = 8 /*Биржевой рынок*/ THEN 1
                              WHEN p_ByOutMarket != 0 AND sfcontr.t_ServKindSub = 9 /*Внебиржевой рынок*/ THEN 1
                              ELSE 0 END)
                AND curr.t_FIID = acc.t_Code_Currency
                AND curr.t_FI_Kind = 1
                AND EXISTS (Select 1 from DDL_LIMITCHECKDATE_DBT limdate where limdate.t_curID = curr.t_FIID AND T_ISCUR = CHR(1))
                AND mp.t_SfContrID = sfcontr.t_ID
                AND mp.t_MarketID = p_MarketID
                AND dlc.t_DlContrID = mp.t_DlContrID
                AND RSI_RSB_GATE.CheckClientIDByStatus(acc.t_Client, 3 /*Обработан (импорт)*/) = CHR(0);

      TimeStamp_ (
         ' Сбор общих для всех видов лимитов данных по деньгам ',
         p_CalcDate,
         ts_,
         SYSTIMESTAMP);

      ts_ := SYSTIMESTAMP;
      INSERT INTO dtick_dbt
         SELECT DISTINCT tk.t_BOfficeKind,
                         tk.t_DealID,
                         tk.t_dealtype,
                         tk.t_ClientContrID,
                         tk.t_ClientID,
                         t_DEALdate,
                         t_partyid,
                         t_ispartyclient
           FROM ddl_tick_dbt tk
          WHERE tk.t_BOfficeKind IN (RSB_SECUR.DL_SECURITYDOC, RSB_SECUR.DL_RETIREMENT, RSB_SECUR.DL_AVRWRT) AND tk.t_ClientID > 0
                AND (tk.t_bofficekind IN (RSB_SECUR.DL_SECURITYDOC, RSB_SECUR.DL_RETIREMENT) OR tk.t_flag3 <> CHR (88))
                AND tk.t_ClientContrID > 0
                AND tk.t_DealDate < p_CalcDate
                AND tk.t_MarketID = p_MarketID
                AND (EXISTS
                        (SELECT 1
                           FROM ddlrq_dbt
                          WHERE t_docid = tk.t_dealid
                                AND t_dockind = tk.t_bofficekind
                                AND t_factdate =
                                       TO_DATE ('01010001', 'ddmmyyyy'))
                     OR (SELECT MAX (t_factdate)
                           FROM ddlrq_dbt
                          WHERE t_docid = tk.t_dealid
                                AND t_dockind = tk.t_bofficekind
                                AND t_factdate <>
                                       TO_DATE ('01010001', 'ddmmyyyy')) >=
                           p_CalcDate - 1);

     TimeStamp_ (
         ' Сбор общих для всех видов лимитов данных по ценным бумагам',
         p_CalcDate,
         ts_,
         SYSTIMESTAMP);
   END;

   PROCEDURE SetLotTmp (p_CalcDate IN DATE, p_MarketID IN NUMBER)
   AS
      p_CheckDate   DATE;
      TYPE limacc_t IS TABLE OF DD_LIMITLOTS_DBT%ROWTYPE
                          INDEX BY BINARY_INTEGER;
      p_limacc      limacc_t;

   BEGIN
      DELETE FROM DD_LIMITLOTS_DBT;

      SELECT NVL (SUM (lot.t_Amount), 0) SumQuantity,
             lot.t_Party,
             lot.t_Contract,
             lot.t_FIID
        BULK COLLECT INTO p_limacc
        FROM v_scwrthistex lot, dfininstr_dbt fin
       WHERE     lot.t_Party > 0
             AND lot.t_Contract > 0
             AND lot.t_Amount > 0
             AND lot.t_DocKind IN (29, 135)
             AND lot.t_DocID > 0
             AND lot.t_ChangeDate < p_CalcDate
               AND lot.t_Buy_Sale IN
                      (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY,
                                    RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO)
             AND lot.t_Instance =
                    (SELECT MAX (bc.t_Instance)
                       FROM v_scwrthistex bc
                      WHERE     bc.t_SumID = lot.t_SumID
                            AND bc.t_ChangeDate < p_CalcDate)
             AND fin.t_FIID = lot.t_FIID
             AND fin.t_FI_Kind = 2                                 --Ценные бумаги
             AND RSI_RSB_GATE.CheckClientIDByStatus (lot.t_Party, 3 /*Обработан (импорт)*/
                                                                   ) = CHR (0)
             AND (   (    lot.t_DocKind = 29
                      AND EXISTS
                             (SELECT tk.*
                                FROM ddlrq_dbt rq, ddl_tick_dbt tk
                               WHERE     rq.t_ID = lot.t_DocID
                                     AND tk.t_DealID = rq.t_docid
                                     AND tk.T_BOFFICEKIND = rq.t_dockind
                                     AND tk.t_marketid = p_MarketID))
                  OR (    lot.t_DocKind = 135
                      AND EXISTS
                             (SELECT tk.*
                                FROM ddl_tick_dbt tk
                               WHERE     tk.t_DealID = lot.t_dealid
                                     AND tk.T_BOFFICEKIND = 101
                                     AND tk.t_marketid = p_MarketID)))
    GROUP BY lot.t_Party, lot.t_Contract, lot.t_FIID;

      IF p_limacc.COUNT > 0
      THEN
         FORALL indx IN p_limacc.FIRST .. p_limacc.LAST
            INSERT INTO DD_LIMITLOTS_DBT
                 VALUES p_limacc (indx);
      END IF;
   END;


   PROCEDURE SetFIIDTmp (p_CalcDate IN DATE)
   AS
      p_CheckDate   DATE;

      TYPE limacc_t IS TABLE OF DDL_FIID_DBT%ROWTYPE
                          INDEX BY BINARY_INTEGER;

      p_limacc      limacc_t;
   BEGIN
     rshb_rsi_sclimit.ClearFIIDTmp(p_MarketID => -1, p_UseListClients => 0);
     rshb_rsi_sclimit.SetFIIDTmp(p_CalcDate       => p_CalcDate,
                                 p_ByEDP          => 0,
                                 p_MarketID       => -1,
                                 p_UseListClients => 0);
      /*DELETE FROM DDL_FIID_DBT;

      p_CheckDate := TO_DATE ('31.12.9999', 'DD.MM.YYYY');

      SELECT DISTINCT q.t_ClientID, q.t_ClientContrID, rq.t_FIID
        BULK COLLECT INTO p_limacc
        FROM ddlrq_dbt rq, dtick_dbt q
       WHERE rq.t_DocKind = q.t_BOfficeKind AND rq.t_DocID = q.t_DealID
             AND rq.t_Type IN
                    (RSI_DLRQ.DLRQ_TYPE_DELIVERY,
                     RSI_DLRQ.DLRQ_TYPE_COMPDELIVERY)
             AND rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
             AND rq.t_state <> -1 --bpv техничский статус чтобы отрубить старые неисполненные ТО
             AND rq.t_Kind IN
                    (RSI_DLRQ.DLRQ_KIND_REQUEST, RSI_DLRQ.DLRQ_KIND_COMMIT)
             AND rq.t_PlanDate <= p_CheckDate
             AND (rq.t_FactDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                  OR rq.t_FactDate >= p_CalcDate
                  OR NOT EXISTS
                            (SELECT 1
                               FROM dpmwrtsum_dbt lot
                              WHERE     lot.t_DocKind = 29
                                    AND lot.t_DocID = rq.t_ID
                                    AND lot.t_Party = q.t_ClientID
                                    AND lot.t_Contract = q.t_ClientContrID));

      IF p_limacc.COUNT > 0
      THEN
         FORALL indx IN p_limacc.FIRST .. p_limacc.LAST
            INSERT INTO DDL_FIID_DBT
                 VALUES p_limacc (indx);
      END IF;*/
   END;

   PROCEDURE RSI_CreateLimits(p_MarketID IN NUMBER, p_MarketCode IN VARCHAR2, p_CalcDate IN DATE, p_ByStock IN NUMBER, p_ByCurr IN NUMBER, p_ByDeriv IN NUMBER)
   AS
     v_ByMarketStock NUMBER := 0;
     v_ByOutMarketStock NUMBER := 0;
     v_IsDepo     NUMBER := 0;
     v_IsKind2    NUMBER := 0;
     v_DepoAcc    VARCHAR2(20) := chr(1);

     v_e              NUMBER;
     v_action         VARCHAR2 (200);
     v_job1           VARCHAR2 (20) := 'JOB_LIM_1';
     v_job2           VARCHAR2 (20) := 'JOB_LIM_2';
     v_runningcash    NUMBER := 0;
     v_runningsecur   NUMBER := 0;
     v_state          VARCHAR2 (50);
   BEGIN

      TimeStamp_ (
         'Запуск. Операционист  ' || RsbSessionData.Oper,
         p_CalcDate,
         NULL,
         SYSTIMESTAMP);
      TimeStamp_ ('Старт расчета',
                  p_CalcDate,
                  NULL,
                  SYSTIMESTAMP,
                  NULL);

     MarketID := p_MarketID;
     MarketCode := nvl(p_MarketCode,chr(1));

     v_ByMarketStock := (case when MarketID > 0 then 1 else 0 end);
     v_ByOutMarketStock := (case when MarketID <= 0 then 1 else 0 end);
     IF p_ByStock <> 0 THEN
       BEGIN
         DBMS_SCHEDULER.drop_job (v_job1, TRUE);
         DBMS_SCHEDULER.drop_job (v_job2, TRUE);
       EXCEPTION
         WHEN OTHERS
         THEN
           NULL;
       END;
       getFlagLimitPrm( (case when MarketID > 0 then MarketID else 0 end), MARKET_KIND_STOCK, v_IsDepo, v_IsKind2, v_DepoAcc);
       RSI_FillContrTable (p_CalcDate, p_ByStock, 0, MarketID);
       TimeStamp_ ('RSI_FillContrTable  Завершена  ',
                     p_CalcDate,
                     NULL,
                     SYSTIMESTAMP);

       SetLotTmp (p_CalcDate, (CASE WHEN p_MarketID > 0 THEN p_MarketID ELSE -1 END));
       TimeStamp_ ('SetLotTmp  Завершена  ',
                     p_CalcDate,
                     NULL,
                     SYSTIMESTAMP);

       SetFIIDTmp (p_CalcDate);
       TimeStamp_ ('SetFIIDTmp  Завершена  ',
                     p_CalcDate,
                     NULL,
                     SYSTIMESTAMP);

       v_action :=
               'begin rsi_sclimit.RSI_CreateCashStockLimits (to_date('''
            || TO_CHAR (p_CalcDate, 'dd.mm.yyyy')
            || ''',''dd.mm.yyyy''), '
            || TO_CHAR (v_ByMarketStock)
            || ', '
            || TO_CHAR (v_ByOutMarketStock)
            || ', '''
            || TO_CHAR (MarketCode)
            || ''', '
            || TO_CHAR (p_MarketID)
            || ', '
            || TO_CHAR (mainsessionid)
            || '); end;';
         DBMS_SCHEDULER.
          create_job (job_name     => v_job1,
                      job_type     => 'PLSQL_BLOCK',
                      job_action   => v_action);
         DBMS_SCHEDULER.run_job (v_job1, FALSE);

       v_action :=
               'begin rsi_sclimit.RSI_CreateSecurLimits (to_date('''
            || TO_CHAR (p_CalcDate, 'dd.mm.yyyy')
            || ''',''dd.mm.yyyy''), '
            || TO_CHAR (v_ByMarketStock)
            || ', '
            || TO_CHAR (v_ByOutMarketStock)
            || ', '''
            || TO_CHAR (v_DepoAcc)
            || ''', '''
            || TO_CHAR (MarketCode)
            || ''', '
            || TO_CHAR (p_MarketID)
            || ', '
            || TO_CHAR (mainsessionid)
            || '); end;';
         DBMS_SCHEDULER.
          create_job (job_name     => v_job2,
                      job_type     => 'PLSQL_BLOCK',
                      job_action   => v_action);
         DBMS_SCHEDULER.run_job (v_job2, FALSE);
     END IF;

     IF (p_ByCurr <> 0 AND MarketID > 0)  THEN   -- для валютного только биржа
       v_IsDepo    := 0;
       v_IsKind2   := 0;
       v_DepoAcc   := chr(1);

       TimeStamp_ (
            'Старт расчета по валютному рынку. Операционист  '
            || RsbSessionData.Oper,
            p_CalcDate,
            NULL,
            SYSTIMESTAMP);

       getFlagLimitPrm(p_MarketID, MARKET_KIND_CURR, v_IsDepo, v_IsKind2, v_DepoAcc);  -- для валютного определим признаки(настройки)
       RSI_CreateCashStockLimitsCur(p_CalcDate, v_IsKind2, v_IsDepo);

       IF(v_IsDepo <> 0) THEN  -- формируется только с установленным признаком Depo
         RSI_CreateSecurLimitsCur(p_CalcDate, v_IsKind2, v_DepoAcc);
       ELSE
         RSI_CreateSecurLimByKindCurZero (p_CalcDate, 0, v_DepoAcc);
       END IF;

       TimeStamp_ (
            'Расчет лимитов по валютному рыку завершен. ',
            p_CalcDate,
            NULL,
            SYSTIMESTAMP,
            NULL,
            0);
     END IF;

     IF p_ByDeriv <> 0 THEN
       TimeStamp_ (
            'Старт расчета по срочному рынку. Операционист  '
            || RsbSessionData.Oper,
            p_CalcDate,
            NULL,
            SYSTIMESTAMP);
       RSI_CreateFutureMarkLimits(p_CalcDate);
       TimeStamp_ (
            'Расчет лимитов срочной секции завершен. ',
            p_CalcDate,
            NULL,
            SYSTIMESTAMP,
            NULL);
     END IF;

    /*Взяла из пользовательского кода рсхб. Как временное решение. Может позже смогу придумать более корректный вариант. *
    * При формировании протокола неверно опредеделяются иногда количество рассчитанных лимитов
    * из-за параллельных вычилений.*/
    IF p_ByStock <> 0 THEN
        WHILE v_runningcash < 2 OR v_runningsecur < 2
          LOOP

             BEGIN
                SELECT state
                  INTO v_state
                  FROM user_scheduler_jobs
                 WHERE job_name = v_job1 AND ROWNUM = 1;

                IF v_state = 'RUNNING' AND v_runningcash = 0
                THEN
                   v_runningcash := 1;
                ELSIF v_state = 'DISABLED'
                THEN
                   v_runningcash := 2;
                END IF;

                SELECT state
                  INTO v_state
                  FROM user_scheduler_jobs
                 WHERE job_name = v_job2 AND ROWNUM = 1;

                IF v_state = 'RUNNING' AND v_runningsecur = 0
                THEN
                   v_runningsecur := 1;
                ELSIF v_state = 'DISABLED'
                THEN
                   v_runningsecur := 2;
                END IF;
             EXCEPTION
                WHEN NO_DATA_FOUND
                THEN
                   NULL;                                  --v_runningcash := 0;
             END;
          END LOOP;
    END IF;

   END; -- RSI_CreateLimits

   PROCEDURE RSI_CrLimitAdJNptxWrtByKind(Limit_Kind IN NUMBER,
                                         ID_Operation IN NUMBER,
                                         ID_Step IN NUMBER,
                                         rLimitAdj IN OUT DDL_LIMITADJUST_DBT%rowtype
                                        )
   AS
   BEGIN
      rLimitAdj.t_ID              := 0;
      rLimitAdj.T_LIMITID         := (ID_Operation * 10)||(CASE WHEN Limit_Kind = 365 THEN 3 ELSE Limit_Kind END);
      rLimitAdj.T_LIMIT_KIND      := Limit_Kind;

      RSI_InsDfltIntoWRTBC( rLimitAdj );

      INSERT INTO DDL_LIMITADJUST_DBT VALUES rLimitAdj;

   END; -- RSI_CrLimitAdJNptxWrtByKind

   -- Получение идентификатора субъекта по коду
   FUNCTION GetPartyIDByCode( p_Code IN VARCHAR2, p_CodeKind IN NUMBER ) RETURN NUMBER
   IS
      v_PartyID NUMBER := -1;
   BEGIN
      SELECT NVL(t_ObjectID, -1) INTO v_PartyID
        FROM dobjcode_dbt
       WHERE t_ObjectType = 3
         AND t_CodeKind   = p_CodeKind
         AND t_State      = 0
         AND t_Code       = p_Code
         AND rownum      <= 1;

      RETURN v_PartyID;

   EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN -1;
   END;

   PROCEDURE RSI_CrLimitAdJNptxWrt(DocID IN NUMBER,
                                   ID_Operation IN NUMBER,
                                   ID_Step IN NUMBER
                                  )
   AS
     rLimitAdj    DDL_LIMITADJUST_DBT%rowtype;
     rNptxop      DNPTXOP_DBT%rowtype;
     v_MarketKind NUMBER := -1;
     v_MMVB_ID    NUMBER := -1;
     v_MMVB_Code  VARCHAR2(35) := chr(1);
     v_IsDepo     NUMBER := 0;
     v_IsKind2    NUMBER := 0;
     v_DepoAcc    VARCHAR2(20) := chr(1);
     v_CURR_CODE  VARCHAR2(3) := chr(1);
     v_LEVERAGE   NUMBER := 0;
     v_LEVERAGECUR NUMBER := 0;
     v_Client_code VARCHAR2(35) := chr(1);
   BEGIN
      InitError();

      select * into rNptxop from dnptxop_dbt where t_ID = DocID;

      /*для начала заполним поля, общие для всех корректировок*/
      SELECT acc.t_AccountID,
             (case when sfcontr.t_ServKind = 1 and sfcontr.t_ServKindSub = 8 then MARKET_STOCK_EX
                     when sfcontr.t_ServKind = 1 and sfcontr.t_ServKindSub = 9 then MARKET_STOCK_OUT
                     when sfcontr.t_ServKind = 15 then MARKET_DERIV
                     when sfcontr.t_ServKind = 21 then MARKET_CURR
                  else -1 end),
             DECODE(acc.t_Code_Currency, RSI_RSB_FIInstr.NATCUR, 'SUR', curr.t_CCY),
             (CASE WHEN GetObjAtCor(207/*Договор брокерского обслуживания*/, LPAD(dlc.t_DlContrID, 34, '0'), 1 /*Признак блокировки*/, rNptxop.t_OperDate) = 1 THEN 'X' ELSE CHR(0) END),
             dlc.t_Leverage, dlc.t_LeverageCur, CASE WHEN MP.T_MPCODE IS NULL THEN CHR(1) ELSE MP.T_MPCODE END
             into rLimitAdj.T_INTERNALACCOUNT,  rLimitAdj.T_MARKET, v_CURR_CODE, rLimitAdj.T_ISBLOCKED, v_LEVERAGE, v_LEVERAGECUR, v_Client_code
        FROM daccount_dbt acc, dsfcontr_dbt sfcontr, dfininstr_dbt curr, ddlcontrmp_dbt mp, ddlcontr_dbt dlc
       WHERE acc.t_Chapter = 1
         AND acc.t_Account LIKE rNptxop.t_ACCOUNT
         AND acc.t_Code_Currency = rNptxop.t_Currency
         AND sfcontr.t_ID = rNptxop.t_Contract
         AND curr.t_FIID = acc.t_Code_Currency
         AND curr.t_FI_Kind = 1
         AND mp.t_SfContrID = sfcontr.t_ID
         AND dlc.t_DlContrID = mp.t_DlContrID;

      v_MarketKind := (case when rLimitAdj.T_MARKET = MARKET_STOCK_OUT or rLimitAdj.T_MARKET = MARKET_STOCK_EX then MARKET_KIND_STOCK
                            when rLimitAdj.T_MARKET = MARKET_DERIV then MARKET_KIND_DERIV
                            when rLimitAdj.T_MARKET = MARKET_CURR then MARKET_KIND_CURR
                       else -1 end);

      v_MMVB_Code := trim(rsb_common.GetRegStrValue('SECUR\MICEX_CODE', 0));
      IF v_MMVB_Code <> CHR(1) THEN
         v_MMVB_ID := GetPartyIDByCode(v_MMVB_Code,cnst.PTCK_CONTR);
         getFlagLimitPrm(case when rLimitAdj.T_MARKET = 0 then 0 else v_MMVB_ID end,v_MarketKind, v_IsDepo, v_IsKind2, v_DepoAcc);
      END IF;


      IF(v_MarketKind = MARKET_KIND_STOCK) THEN
        rLimitAdj.T_LIMIT_TYPE      := 'MONEY';
      ELSE
        IF(v_MarketKind = MARKET_KIND_CURR) THEN  -- валютный
          IF( (v_IsDepo = 1) AND (rNptxop.t_Currency <> 0 )) THEN    -- стоит признак Репо и не рубли
            rLimitAdj.T_LIMIT_TYPE      := 'DEPO';
          ELSE
            rLimitAdj.T_LIMIT_TYPE      := 'MONEY';
          END IF;
        END IF;
      END IF;

      IF( rLimitAdj.T_LIMIT_TYPE = 'DEPO') THEN
        rLimitAdj.T_TRDACCID        := v_DepoAcc;
        rLimitAdj.T_SECCODE         := v_CURR_CODE;
        rLimitAdj.T_TAG             := CHR(1);
        rLimitAdj.T_CURR_CODE       := CHR(1);
        rLimitAdj.T_LEVERAGE        := 0;
      ELSE
        rLimitAdj.T_TRDACCID        := CHR(1);
        rLimitAdj.T_SECCODE         := CHR(1);
        rLimitAdj.T_TAG             := GetTAG(case when rLimitAdj.T_MARKET = 0 then 0 else v_MMVB_ID end,v_MarketKind);
        rLimitAdj.T_CURR_CODE       := v_CURR_CODE;
        IF(rLimitAdj.T_MARKET = MARKET_STOCK_OUT or rLimitAdj.T_MARKET = MARKET_STOCK_EX ) THEN
           rLimitAdj.T_LEVERAGE        := v_LEVERAGE;
        ELSIF(rLimitAdj.T_MARKET = MARKET_CURR) THEN
           rLimitAdj.T_LEVERAGE        := v_LEVERAGECUR;
        END IF;
      END IF;

      rLimitAdj.T_DATE            := rNptxop.t_OperDate;
      rLimitAdj.T_TIME            := rNptxop.t_Time;
      rLimitAdj.T_CLIENT          := rNptxop.t_Client;
      rLimitAdj.T_FIRM_ID         := GetFIRM_ID(case when rLimitAdj.T_MARKET = MARKET_STOCK_OUT then 0 else v_MMVB_ID end,v_MarketKind);
      rLimitAdj.T_CLIENT_CODE     := v_Client_code;
      rLimitAdj.T_OPEN_BALANCE    := (case when rNptxop.t_SubKind_Operation = 10 then rNptxop.t_OutSum else -rNptxop.t_OutSum end);--Если операция <Зачисление> значение Сумма (S1), если <Списание> - Сумма (S1) со знаком
      rLimitAdj.T_OPEN_LIMIT      := 0;--Не заполняется
      rLimitAdj.T_CURRENT_LIMIT   := 0;--Не заполняется
      rLimitAdj.T_LIMIT_OPERATION := 'CORRECT_LIMIT';
      rLimitAdj.T_CURRID          := rNptxop.t_Currency;
      rLimitAdj.T_ID_OPER         := ID_Operation;
      rLimitAdj.T_ID_STEP         := ID_Step;

      RSI_InsDfltIntoWRTBC( rLimitAdj );

      RSI_CrLimitAdJNptxWrtByKind(0,ID_Operation,ID_Step,rLimitAdj);
      RSI_CrLimitAdJNptxWrtByKind(1,ID_Operation,ID_Step,rLimitAdj);

      if( (v_MarketKind = MARKET_KIND_STOCK) OR ((v_MarketKind = MARKET_KIND_CURR) AND (v_IsKind2 = 1) ) )then
         RSI_CrLimitAdJNptxWrtByKind(2,ID_Operation,ID_Step,rLimitAdj);
         RSI_CrLimitAdJNptxWrtByKind(365,ID_Operation,ID_Step,rLimitAdj);
      end if;

      update dnptxop_dbt
         set t_LimitStatus = LIMITSTATUS_WAIT
       where t_ID = DocID;

   EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN;
   END; -- RSI_CrLimitAdJNptxWrt

   PROCEDURE RSI_RestoreLimitAdJNptxWrt(DocID IN NUMBER,
                                        ID_Operation IN NUMBER,
                                        ID_Step IN NUMBER
                                       )
   AS
   BEGIN
      -- при откате шага удаляем корректировки лимитов только в том случае, если статус выгрузки корректировок = "ждет", иначе удаления строк не производим
      DELETE FROM DDL_LIMITADJUST_DBT limad
       WHERE limad.T_ID_OPER = ID_Operation
        AND limad.T_ID_STEP = ID_Step
        AND nvl((select wrt.t_LimitStatus from dnptxop_dbt wrt where wrt.t_ID = DocID),LIMITSTATUS_UNDEF) = LIMITSTATUS_WAIT;

      update dnptxop_dbt
         set t_LimitStatus = LIMITSTATUS_UNDEF
       where t_ID = DocID
         and t_LimitStatus = LIMITSTATUS_WAIT;

   END; -- RSI_RestoreLimitAdJNptxWrt

   FUNCTION RSI_GetLastDateCalc(p_MarketKind IN NUMBER, p_Market IN VARCHAR2) RETURN DATE
   IS
    v_MaxCalcDate DATE;
    v_MarketCode VARCHAR2(35) := CHR(1);
   BEGIN
      v_MarketCode := nvl(p_Market,chr(1));
      IF p_MarketKind = MARKET_KIND_STOCK THEN
        SELECT NVL(MAX (t_Date), NULL) INTO v_MaxCalcDate
          FROM (SELECT cash.t_Date
                  FROM ddl_limitcashstock_dbt cash
                 WHERE LOWER (cash.t_market_kind) = 'фондовый' AND cash.t_market = v_MarketCode
                UNION ALL
                SELECT securites.t_Date
                  FROM ddl_limitsecurites_dbt securites
                 WHERE LOWER (securites.t_market_kind) = 'фондовый' AND securites.t_market = v_MarketCode);
      ELSE
          IF p_MarketKind = MARKET_KIND_CURR THEN
            SELECT NVL(MAX (t_Date), NULL)  INTO v_MaxCalcDate
              FROM (SELECT cash.t_Date
                      FROM ddl_limitcashstock_dbt cash
                     WHERE LOWER (cash.t_market_kind) = 'валютный' AND cash.t_market = v_MarketCode
                    UNION ALL
                    SELECT securites.t_Date
                      FROM ddl_limitsecurites_dbt securites
                     WHERE LOWER (securites.t_market_kind) = 'валютный' AND securites.t_market = v_MarketCode);
          ELSE
            IF p_MarketKind = MARKET_KIND_DERIV THEN
                SELECT NVL(MAX (t_Date), NULL) INTO v_MaxCalcDate
                  FROM ddl_limitfuturmark_dbt
                  WHERE t_market = v_MarketCode;
            END IF;
          END IF;
      END IF;

      RETURN v_MaxCalcDate;
      EXCEPTION
       WHEN NO_DATA_FOUND THEN RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
   END; -- RSI_GetLastDateCalc

   PROCEDURE RSI_DeleteLog(p_MarketID IN NUMBER, p_CondProtocol IN CHAR, p_DateProtocol IN DATE)
   AS
   BEGIN
      CASE p_CondProtocol
         WHEN EQ_CHAR THEN
            DELETE FROM DDL_LOGDATA_DBT dl_logdata
                  WHERE EXISTS
                           (SELECT dl_logdata.T_ID
                              FROM DDL_LOG_DBT dl_log, DDL_LIMITHIST_DBT lh
                             WHERE     lh.T_CALCDATE = p_DateProtocol
                                   AND lh.T_MARKETID = 2
                                   AND dl_log.T_DOCKIND = lh.T_DOCKIND
                                   AND dl_log.T_DOCID = lh.T_ID
                                   AND dl_log.T_ID = dl_logdata.T_LOGID);

            DELETE FROM DDL_LOG_DBT dl_log
                  WHERE EXISTS
                           (SELECT dl_log.T_ID
                              FROM DDL_LOG_DBT dl_log_temp, DDL_LIMITHIST_DBT lh
                             WHERE     lh.T_CALCDATE = p_DateProtocol
                                   AND lh.T_MARKETID = p_MarketID
                                   AND dl_log_temp.T_DOCKIND = lh.T_DOCKIND
                                   AND dl_log_temp.T_DOCID = lh.T_ID
                                   AND dl_log.T_ID = dl_log_temp.T_ID);

            DELETE FROM DDL_LIMITHIST_DBT
                  WHERE t_CalcDate = p_DateProtocol AND T_MARKETID = p_MarketID;
         WHEN LESS_CHAR THEN
            DELETE FROM DDL_LOGDATA_DBT dl_logdata
                  WHERE EXISTS
                           (SELECT dl_logdata.T_ID
                              FROM DDL_LOG_DBT dl_log, DDL_LIMITHIST_DBT lh
                             WHERE     lh.T_CALCDATE < p_DateProtocol
                                   AND lh.T_MARKETID = 2
                                   AND dl_log.T_DOCKIND = lh.T_DOCKIND
                                   AND dl_log.T_DOCID = lh.T_ID
                                   AND dl_log.T_ID = dl_logdata.T_LOGID);

            DELETE FROM DDL_LOG_DBT dl_log
                  WHERE EXISTS
                           (SELECT dl_log.T_ID
                              FROM DDL_LOG_DBT dl_log_temp, DDL_LIMITHIST_DBT lh
                             WHERE     lh.T_CALCDATE < p_DateProtocol
                                   AND lh.T_MARKETID = p_MarketID
                                   AND dl_log_temp.T_DOCKIND = lh.T_DOCKIND
                                   AND dl_log_temp.T_DOCID = lh.T_ID
                                   AND dl_log.T_ID = dl_log_temp.T_ID);

            DELETE FROM DDL_LIMITHIST_DBT
                  WHERE t_CalcDate < p_DateProtocol AND T_MARKETID = p_MarketID;
         WHEN LEQ_CHAR THEN
            DELETE FROM DDL_LOGDATA_DBT dl_logdata
                  WHERE EXISTS
                           (SELECT dl_logdata.T_ID
                              FROM DDL_LOG_DBT dl_log, DDL_LIMITHIST_DBT lh
                             WHERE     lh.T_CALCDATE <= p_DateProtocol
                                   AND lh.T_MARKETID = 2
                                   AND dl_log.T_DOCKIND = lh.T_DOCKIND
                                   AND dl_log.T_DOCID = lh.T_ID
                                   AND dl_log.T_ID = dl_logdata.T_LOGID);

            DELETE FROM DDL_LOG_DBT dl_log
                  WHERE EXISTS
                           (SELECT dl_log.T_ID
                              FROM DDL_LOG_DBT dl_log_temp, DDL_LIMITHIST_DBT lh
                             WHERE     lh.T_CALCDATE <= p_DateProtocol
                                   AND lh.T_MARKETID = p_MarketID
                                   AND dl_log_temp.T_DOCKIND = lh.T_DOCKIND
                                   AND dl_log_temp.T_DOCID = lh.T_ID
                                   AND dl_log.T_ID = dl_log_temp.T_ID);

            DELETE FROM DDL_LIMITHIST_DBT
                  WHERE t_CalcDate <= p_DateProtocol AND T_MARKETID = p_MarketID;
      END CASE;
   END; -- RSI_DeleteLog

   PROCEDURE RSI_ClearLimitHistory(  p_MarketID     IN NUMBER,
                                     p_Market       IN VARCHAR2,
                                     p_ByStock      IN NUMBER,
                                     p_CondStock    IN CHAR,
                                     p_DateStock    IN DATE,
                                     p_ByCurr       IN NUMBER,
                                     p_CondCurr     IN CHAR,
                                     p_DateCurr     IN DATE,
                                     p_ByDeriv      IN NUMBER,
                                     p_CondDeriv    IN CHAR,
                                     p_DateDeriv    IN DATE,
                                     p_CondProtocol IN CHAR,
                                     p_DateProtocol IN DATE)
   AS
   BEGIN
    IF p_ByStock <> 0 THEN
        CASE p_CondStock
            WHEN EQ_CHAR THEN
                execute immediate 'delete from ddl_limitcashstock_dbt where t_date = :p_DateStock AND LOWER (t_market_kind) = ''фондовый'' AND t_market = :p_Market' USING p_DateStock, p_Market;
                execute immediate 'delete from ddl_limitsecurites_dbt where t_date = :p_DateStock AND LOWER (t_market_kind) = ''фондовый'' AND t_market = :p_Market' USING p_DateStock, p_Market;
            WHEN LESS_CHAR THEN
                execute immediate 'delete from ddl_limitcashstock_dbt where t_date < :p_DateStock AND LOWER (t_market_kind) = ''фондовый'' AND t_market = :p_Market' USING p_DateStock, p_Market;
                execute immediate 'delete from ddl_limitsecurites_dbt where t_date < :p_DateStock AND LOWER (t_market_kind) = ''фондовый'' AND t_market = :p_Market' USING p_DateStock, p_Market;
            WHEN LEQ_CHAR THEN
                execute immediate 'delete from ddl_limitcashstock_dbt where t_date <= :p_DateStock AND LOWER (t_market_kind) = ''фондовый'' AND t_market = :p_Market' USING p_DateStock, p_Market;
                execute immediate 'delete from ddl_limitsecurites_dbt where t_date <= :p_DateStock AND LOWER (t_market_kind) = ''фондовый'' AND t_market = :p_Market' USING p_DateStock, p_Market;
        END CASE;
    END IF;
    IF p_ByCurr <> 0 THEN
        CASE p_CondCurr
            WHEN EQ_CHAR THEN
                execute immediate 'delete from ddl_limitcashstock_dbt where t_date = :p_DateCurr AND LOWER (t_market_kind) = ''валютный'' AND t_market = :p_Market' USING p_DateCurr, p_Market;
                execute immediate 'delete from ddl_limitsecurites_dbt where t_date = :p_DateCurr AND LOWER (t_market_kind) = ''валютный'' AND t_market = :p_Market' USING p_DateCurr, p_Market;
            WHEN LESS_CHAR THEN
                execute immediate 'delete from ddl_limitcashstock_dbt where t_date < :p_DateCurr AND LOWER (t_market_kind) = ''валютный'' AND t_market = :p_Market' USING p_DateCurr, p_Market;
                execute immediate 'delete from ddl_limitsecurites_dbt where t_date < :p_DateCurr AND LOWER (t_market_kind) = ''валютный'' AND t_market = :p_Market' USING p_DateCurr, p_Market;
            WHEN LEQ_CHAR THEN
                execute immediate 'delete from ddl_limitcashstock_dbt where t_date <= :p_DateCurr AND LOWER (t_market_kind) = ''валютный'' AND t_market = :p_Market' USING p_DateCurr, p_Market;
                execute immediate 'delete from ddl_limitsecurites_dbt where t_date <= :p_DateCurr AND LOWER (t_market_kind) = ''валютный'' AND t_market = :p_Market' USING p_DateCurr, p_Market;
        END CASE;
    END IF;
    IF p_ByDeriv <> 0 THEN
        CASE p_CondDeriv
            WHEN EQ_CHAR THEN
                execute immediate 'delete from ddl_limitfuturmark_dbt where t_date = :p_DateDeriv AND LOWER (t_market_kind) = ''срочный'' AND t_market = :p_Market' USING p_DateDeriv, p_Market;
            WHEN LESS_CHAR THEN
                execute immediate 'delete from ddl_limitfuturmark_dbt where t_date < :p_DateDeriv AND LOWER (t_market_kind) = ''срочный'' AND t_market = :p_Market' USING p_DateDeriv, p_Market;
            WHEN LEQ_CHAR THEN
                execute immediate 'delete from ddl_limitfuturmark_dbt where t_date <= :p_DateDeriv AND LOWER (t_market_kind) = ''срочный'' AND t_market = :p_Market' USING p_DateDeriv, p_Market;
        END CASE;
    END IF;
    RSI_DeleteLog(p_MarketID, p_CondProtocol, p_DateProtocol);
   END; -- RSI_ClearLimitHistory


   FUNCTION RSI_InsertLimitHist(DocKind IN NUMBER, MarketID IN NUMBER, CurDate IN DATE, Oper IN NUMBER) RETURN NUMBER
   IS
    v_CurID NUMBER;
   BEGIN
      INSERT INTO DDL_LIMITHIST_DBT (t_id,
                               t_dockind,
                               t_marketid,
                               t_calcdate,
                               t_calctimefrom,
                               t_calctimeto,
                               t_calculated,
                               t_recontimefrom,
                               t_recontimeto,
                               t_recon)
     VALUES (0,
             DocKind,
             MarketID,
             CurDate,
             TO_DATE ('01.01.0001 00:00:00', 'DD.MM.YYYY HH24:MI:SS'),
             TO_DATE ('01.01.0001 00:00:00', 'DD.MM.YYYY HH24:MI:SS'),
             CHR (0),
             TO_DATE ('01.01.0001 00:00:00', 'DD.MM.YYYY HH24:MI:SS'),
             TO_DATE ('01.01.0001 00:00:00', 'DD.MM.YYYY HH24:MI:SS'),
             CHR (0)) RETURNING t_id INTO v_CurID;
     RSI_DLLOG.InsertDL_LOG(v_CurID, DocKind, Oper);
     RETURN v_CurID;
   END; -- RSI_InsertLimitHist

    PROCEDURE RSI_CorBeforeRecon (p_CalcDate     IN DATE,
                                  p_MarketID     IN INTEGER,
                                  p_MarketCode   IN VARCHAR2,
                                  p_CheckSecur   IN CHAR,
                                  p_CheckCurr    IN CHAR)
    AS
       v_Open_Balance_Cor   NUMBER;
    BEGIN
       FOR rec
          IN (  SELECT qlim.ROWID,
                       qlim.t_limit_kind,
                       qlim.t_open_balance,
                       LIM.T_SP,
                       LIM.T_ZCH,
                       lim.t_comprevious_1,
                       lim.t_comprevious
                  FROM (SELECT cash.t_curr_code,
                               cash.t_client_code,
                               cash.t_limit_kind,
                               cash.t_firm_id,
                               cash.t_tag,
                               CASE
                                  WHEN cash.t_limit_kind = 0
                                  THEN
                                     cash.t_money306
                                  ELSE
                                     LAG (
                                        cash.t_open_balance,
                                        1,
                                        0)
                                     OVER (
                                        PARTITION BY cash.t_curr_code,
                                                     cash.t_client_code
                                        ORDER BY
                                           cash.t_curr_code,
                                           cash.t_client_code,
                                           cash.t_limit_kind)
                               END
                                  AS t_open_balance,
                               cash.t_comprevious,
                               cash.t_comprevious_1,
                               cash.t_sp,
                               cash.t_zch
                          FROM DDL_LIMITCASHSTOCK_dbt cash
                         WHERE     cash.t_date = p_CalcDate
                               AND cash.t_market = p_MarketCode
                               AND EXISTS
                                      (SELECT *
                                         FROM ddl_limitprm_dbt prm
                                        WHERE     prm.t_marketid = p_MarketID
                                              AND PRM.T_FIRMCODE = CASH.T_FIRM_ID
                                              AND PRM.T_POSCODE = cash.t_tag
                                              AND PRM.T_CORDIVERG > 0
                                              AND PRM.T_MARKETKIND IN (CASE
                                                                          WHEN p_CheckSecur = 'X'
                                                                          THEN
                                                                             1
                                                                          ELSE
                                                                             0
                                                                       END,
                                                                       CASE
                                                                          WHEN p_CheckCurr = 'X'
                                                                          THEN
                                                                             3
                                                                          ELSE
                                                                             0
                                                                       END))) lim
                       LEFT JOIN DDL_LIMITCASHSTOCKIN_DBT qlim
                          ON     lim.t_client_code = qlim.t_client_code
                             AND lim.t_curr_code = qlim.t_curr_code
                             AND lim.t_limit_kind = qlim.t_limit_kind
                             AND lim.t_open_balance <> QLIM.t_open_balance
                             AND QLIM.T_FIRM_ID = lim.t_firm_id
                             AND QLIM.T_TAG = lim.t_tag
                 WHERE QLIM.t_open_balance IS NOT NULL
              ORDER BY lim.t_client_code, lim.t_curr_code, lim.t_limit_kind)
       LOOP
          v_Open_Balance_Cor :=
             rec.t_open_balance - rec.t_sp + rec.t_zch - rec.t_comprevious_1;

          IF rec.t_limit_kind <> 0
          THEN
             v_Open_Balance_Cor := v_Open_Balance_Cor - rec.t_comprevious;
          END IF;

          UPDATE DDL_LIMITCASHSTOCKIN_DBT
             SET t_open_balance_cor = v_Open_Balance_Cor
           WHERE ROWID = rec.ROWID;
       END LOOP;
    END;                                                     -- RSI_CorBeforeRecon

END RSI_SCLIMIT;
/
