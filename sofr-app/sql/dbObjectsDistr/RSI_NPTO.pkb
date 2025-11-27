CREATE OR REPLACE PACKAGE BODY RSI_NPTO IS  /*Тело пакета RSI_NPTO*/

   LastErrorMessage VARCHAR2(1024) := '';

   PROCEDURE InitError
   AS
   BEGIN
      LastErrorMessage := '';
   END;

   PROCEDURE SetError( ErrNum IN INTEGER, ErrMes IN VARCHAR2 DEFAULT NULL )
   AS
   BEGIN
      IF RSI_TRG_DNPTXLOT_DBT.v_ID_Operation > 0 AND RSI_TRG_DNPTXLOT_DBT.v_ID_Step > 0 AND RSI_TRG_DNPTXLOT_DBT.v_IsCreateLots = True THEN
        RETURN;
      END IF;
      
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

   FUNCTION GetStartSTBDate RETURN DATE DETERMINISTIC
   is
      v_Str VARCHAR2(10);
   begin
      if( g_StartSTBDate IS NULL ) then
         v_Str := trim(rsb_common.GetRegStrValue('COMMON\СНОБ\ДАТА ЗАПУСКА СНОБ', 0));
         IF v_Str <> CHR(1) THEN
           g_StartSTBDate := TO_DATE(v_Str, 'DD.MM.YYYY');
         END IF;
      end if;
      return g_StartSTBDate;
   end; 

   FUNCTION GetPartyId(p_PartyCode VARCHAR) RETURN NUMBER
   IS
     v_PartyId NUMBER;
   BEGIN
     SELECT t_ObjectId INTO v_PartyId
       FROM dobjcode_dbt
      WHERE t_ObjectType = 3
        AND t_CodeKind = 1
        AND t_Code = p_PartyCode;
        
     RETURN v_PartyId;
        
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
     RETURN -1;
   END;

   FUNCTION HasObjCodeOnDate(
       p_object_id   IN NUMBER,
       p_code_kind   IN NUMBER,
       p_date        IN DATE
   ) RETURN NUMBER IS
       -- Возвращает 1, если активен, 0 ? если не активен
       v_dummy       NUMBER;
   BEGIN
       SELECT 1
       INTO v_dummy
       FROM DOBJCODE_DBT
       WHERE T_OBJECTTYPE = 9
         AND T_CODEKIND = p_code_kind
         AND T_OBJECTID = p_object_id
         AND T_BANKDATE <= p_date
         AND (
               T_BANKCLOSEDATE IS NULL
            OR T_BANKCLOSEDATE = TO_DATE('01.01.0001', 'DD.MM.YYYY')
            OR T_BANKCLOSEDATE >= p_date
         )
       FETCH FIRST ROW ONLY;
   
       RETURN 1; -- активен
   EXCEPTION
       WHEN NO_DATA_FOUND THEN
           RETURN 0; -- не активен
   END;


   FUNCTION GetStartProgressScaleDate RETURN DATE DETERMINISTIC
   is
      v_Str VARCHAR2(10);
   begin
      if( g_StartProgressScaleDate IS NULL ) then
         v_Str := trim(rsb_common.GetRegStrValue('COMMON\НДФЛ\ДАТА_ВВОДА_ПРОГРЕССИВНОЙ_ШКАЛЫ', 0));
         IF v_Str <> CHR(1) THEN
           g_StartProgressScaleDate := TO_DATE(v_Str, 'DD.MM.YYYY');
         END IF;
      end if;
      return g_StartProgressScaleDate;
   end;

   FUNCTION GetLucreStartTaxPeriod RETURN NUMBER DETERMINISTIC
   IS
   BEGIN
     RETURN RSI_NPTX.GetTaxRegIntValue('COMMON\НДФЛ\НП_ВВОДА_ДОРАБОТОК_МАТ.ВЫГОДА', TRUNC(SYSDATE));
   END;

   -- Возвращает максимальную дату периода расчета для клиента
   FUNCTION GetCalcPeriodDate( pKind IN NUMBER, pClientID IN NUMBER, pIIS IN CHAR DEFAULT CHR(0), pSubKind IN NUMBER DEFAULT 0, pDlContrID IN NUMBER DEFAULT 0, pExcludeFirstIISDate IN NUMBER DEFAULT 0 )
     RETURN DATE
   IS
     v_Date DATE;
   BEGIN
     SELECT NVL(MAX(T_ENDDATE), TO_DATE('01.01.0001','DD.MM.YYYY'))
       INTO v_Date
       FROM DNPTXCALC_DBT
      WHERE T_KIND   = pKind
        AND T_IIS    = pIIS
        AND T_CLIENT = pClientID
        AND (T_SUBKIND = pSubKind OR (pSubKind = 0 AND T_SUBKIND <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE))
        AND (T_CONTRACT = pDlContrID OR pDlContrID = 0);

     IF pExcludeFirstIISDate = 0 AND pKind = RSI_NPTXC.NPTXCALC_CALCLINKS and pIIS = 'X' and v_Date = TO_DATE('01.01.0001', 'DD.MM.YYYY')
     THEN
        v_Date := GetFirstDateIIS (pClientID, pDlContrID);
     END IF;

     RETURN v_Date;
   END;

   -- Возвращает дату периода расчета по всем клиентам
   FUNCTION GetMaxCalcPeriodDate( pKind IN NUMBER )
     RETURN DATE
   IS
     v_Date DATE;
   BEGIN
     SELECT NVL(MAX(T_ENDDATE), TO_DATE('01.01.0001','DD.MM.YYYY'))
       INTO v_Date
       FROM DNPTXCALC_DBT
      WHERE T_KIND   = pKind;

     RETURN v_Date;
   END;

   -- Вставка даты периода расчета
   PROCEDURE SetCalcPeriodDate( pKind IN NUMBER, pClientID IN NUMBER, pEndDate IN DATE, pProtocol IN NUMBER DEFAULT 1, pIIS IN CHAR DEFAULT CHR(0), pSubKind IN NUMBER DEFAULT 0, pContract IN NUMBER DEFAULT 0 )
   IS
      v_Count NUMBER;
      v_ExistsCalc NUMBER;
      v_ExistsCalcContract NUMBER;
      v_LucreStartTaxPeriod NUMBER := 0;
   BEGIN
      v_ExistsCalc := 0;

      v_LucreStartTaxPeriod := RSI_NPTO.GetLucreStartTaxPeriod();

      SELECT count(1)
        INTO v_Count
        FROM DNPTXCALC_DBT
       WHERE T_KIND   = pKind
         AND T_CLIENT = pClientID
         AND T_IIS    = pIIS
         AND T_ENDDATE > pEndDate
         AND (T_CONTRACT = pContract OR T_CONTRACT = 0)
         AND (EXTRACT(YEAR FROM pEndDate) < v_LucreStartTaxPeriod 
              OR (pSubKind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND T_SUBKIND = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE)
              OR (pSubKind <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND T_SUBKIND <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE)
             );

      IF v_Count > 0 THEN
         IF pKind = RSI_NPTXC.NPTXCALC_CALCLINKS THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Попытка выполнить повторный расчет связей за период' );
            SetError( RSI_NPTXC.NPTX_ERROR_20605,'');

         ELSIF pKind = RSI_NPTXC.NPTXCALC_CALCNDFL THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Попытка выполнить повторный расчет НОБ для НДФЛ за период' );
            SetError( RSI_NPTXC.NPTX_ERROR_20606,'');

         ELSIF pKind = RSI_NPTXC.NPTXCALC_CLOSE THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Попытка выполнить повторное закрытие налогового периода' );
            SetError( RSI_NPTXC.NPTX_ERROR_20607,'');
         END IF;

      END IF;

      IF pKind <> RSI_NPTXC.NPTXCALC_CLOSE THEN
         SELECT count(1)
           INTO v_Count
           FROM DNPTXCALC_DBT
          WHERE T_KIND   = RSI_NPTXC.NPTXCALC_CLOSE
            AND T_CLIENT = pClientID
            AND T_IIS    = pIIS
            AND T_ENDDATE >= pEndDate
            AND (T_CONTRACT = pContract OR T_CONTRACT = 0);

         IF ((v_Count > 0) AND (pIIS <> 'X') and (IsAdmin(RsbSessionData.Oper) = false)) THEN
            IF pKind = RSI_NPTXC.NPTXCALC_CALCLINKS THEN
               RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Попытка выполнить расчет связей в закрытом налоговом периоде' );
            SetError( RSI_NPTXC.NPTX_ERROR_20608,'');

            ELSIF pKind = RSI_NPTXC.NPTXCALC_CALCNDFL THEN
               RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Попытка выполнить расчет НОБ для НДФЛ в закрытом налоговом периоде' );
               SetError( RSI_NPTXC.NPTX_ERROR_20609,'');

            END IF;
         END IF;
      END IF;

      IF pKind = RSI_NPTXC.NPTXCALC_CALCLINKS OR pKind = RSI_NPTXC.NPTXCALC_CALCNDFL THEN
        SELECT Count(1) INTO v_ExistsCalc
          FROM DNPTXCALC_DBT
         WHERE T_KIND = pKind
           AND T_CLIENT = pClientID
           AND T_IIS     = pIIS
           AND T_ENDDATE = pEndDate
           AND ( T_SUBKIND = pSubKind OR pSubKind = 0 )
           AND T_CONTRACT = 0;

        SELECT Count(1) INTO v_ExistsCalcContract
          FROM DNPTXCALC_DBT
         WHERE T_KIND = pKind
           AND T_CLIENT = pClientID
           AND T_IIS     = pIIS
           AND T_ENDDATE = pEndDate
           AND ( T_SUBKIND = pSubKind OR pSubKind = 0 )
           AND T_CONTRACT = pContract;

        IF v_ExistsCalcContract > 0 THEN
          UPDATE DNPTXCALC_DBT
             SET T_COUNT = T_COUNT + 1
           WHERE T_KIND = pKind
             AND T_CLIENT = pClientID
             AND T_ENDDATE = pEndDate
             AND ( T_SUBKIND = pSubKind OR pSubKind = 0 )
             AND T_CONTRACT = pContract;
        ELSIF v_ExistsCalc > 0 THEN
          UPDATE DNPTXCALC_DBT
             SET T_COUNT = T_COUNT + 1
           WHERE T_KIND = pKind
             AND T_CLIENT = pClientID
             AND T_ENDDATE = pEndDate
             AND ( T_SUBKIND = pSubKind OR pSubKind = 0 )
             AND T_CONTRACT = 0;
        END IF;
      END IF;

      IF v_ExistsCalc = 0 and v_ExistsCalcContract = 0 THEN
        INSERT INTO DNPTXCALC_DBT (T_KIND,
                                   T_CLIENT,
                                   T_ENDDATE,
                                   T_COUNT,
                                   T_IIS,
                                   T_SUBKIND,
                                   T_CONTRACT
                                  )
                           VALUES (pKind,
                                   pClientID,
                                   pEndDate,
                                   1,
                                   pIIS,
                                   pSubKind,
                                   pContract
                                  );
      END IF;

      IF pProtocol = 1 THEN
         IF pKind = RSI_NPTXC.NPTXCALC_CALCLINKS THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'Расчет связей за период по дату '||pEndDate );

         ELSIF pKind = RSI_NPTXC.NPTXCALC_CALCNDFL THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'Расчет НОБ для НДФЛ за период по дату '||pEndDate );

         ELSIF pKind = RSI_NPTXC.NPTXCALC_CLOSE THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'Закрытие налогового периода по дату '||pEndDate );
         END IF;
      END IF;
   END;

   -- Откат вставки даты периода расчета
   PROCEDURE RecoilCalcPeriodDate( pKind IN NUMBER, pClientID IN NUMBER, pEndDate IN DATE, pIIS IN CHAR DEFAULT CHR(0), pSubKind IN NUMBER DEFAULT 0, pContract IN NUMBER DEFAULT 0 )
   IS
      v_Date DATE;
      v_PrevDate DATE;
      v_CalcCount NUMBER;
      v_CalcCountContract NUMBER;
   BEGIN
      v_Date := GetCalcPeriodDate (pKind, pClientID, pIIS, pSubKind, pContract);

      v_CalcCount := 0;

      IF v_Date > pEndDate THEN
         IF pKind = RSI_NPTXC.NPTXCALC_CALCLINKS THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Попытка откатить не последний период расчета связей' );
            SetError( RSI_NPTXC.NPTX_ERROR_20610,'');

         ELSIF pKind = RSI_NPTXC.NPTXCALC_CALCNDFL THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Попытка откатить не последний период расчета НОБ для НДФЛ' );
            SetError( RSI_NPTXC.NPTX_ERROR_20611,'');

         ELSIF pKind = RSI_NPTXC.NPTXCALC_CLOSE THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Попытка откатить не последний период закрытия налогового периода' );
            SetError( RSI_NPTXC.NPTX_ERROR_20612,'');
         END IF;
      END IF;

      IF v_Date < pEndDate THEN
         IF pKind = RSI_NPTXC.NPTXCALC_CALCLINKS THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Неверный период расчета связей' );
            SetError( RSI_NPTXC.NPTX_ERROR_20613,'');

         ELSIF pKind = RSI_NPTXC.NPTXCALC_CALCNDFL THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Неверный период расчета НОБ для НДФЛ' );
            SetError( RSI_NPTXC.NPTX_ERROR_20614,'');

         ELSIF pKind = RSI_NPTXC.NPTXCALC_CLOSE THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Неверный период закрытия налогового периода' );
            SetError( RSI_NPTXC.NPTX_ERROR_20615,'');
         END IF;
      END IF;

      IF (pKind <> RSI_NPTXC.NPTXCALC_CLOSE) THEN
         SELECT NVL(MAX(T_ENDDATE), TO_DATE('01.01.0001','DD.MM.YYYY'))
           INTO v_PrevDate
           FROM DNPTXCALC_DBT
          WHERE T_KIND   = pKind
            AND T_CLIENT = pClientID
            AND T_IIS    = pIIS
            AND T_ENDDATE < v_Date;

         IF ((v_PrevDate + 1 <= GetCalcPeriodDate(RSI_NPTXC.NPTXCALC_CLOSE, pClientID, pIIS)) and (pIIS <> 'X') and (IsAdmin(RsbSessionData.Oper) = false)) THEN
           IF pKind = RSI_NPTXC.NPTXCALC_CALCLINKS THEN
              RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Попытка откатить расчет связей в закрытом налоговом периоде' );
              SetError( RSI_NPTXC.NPTX_ERROR_20616,'');

           ELSIF pKind = RSI_NPTXC.NPTXCALC_CALCNDFL THEN
              RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Попытка откатить расчет НОБ для НДФЛ в закрытом налоговом периоде' );
              SetError( RSI_NPTXC.NPTX_ERROR_20617,'');

           END IF;
         END IF;
      END IF;

      IF pKind = RSI_NPTXC.NPTXCALC_CALCLINKS OR pKind = RSI_NPTXC.NPTXCALC_CALCNDFL THEN

        BEGIN
          SELECT T_COUNT INTO v_CalcCount
            FROM DNPTXCALC_DBT
           WHERE T_KIND = pKind
             AND T_CLIENT = pClientID
             AND T_IIS    = pIIS
             AND T_ENDDATE = pEndDate
             AND ( T_SUBKIND = pSubKind OR pSubKind = 0 )
             AND T_CONTRACT = 0;
        EXCEPTION
           WHEN NO_DATA_FOUND THEN
             v_CalcCount := 0;
        END;

        BEGIN
          SELECT T_COUNT INTO v_CalcCountContract
            FROM DNPTXCALC_DBT
           WHERE T_KIND = pKind
             AND T_CLIENT = pClientID
             AND T_IIS    = pIIS
             AND T_ENDDATE = pEndDate
             AND ( T_SUBKIND = pSubKind OR pSubKind = 0 )
             AND T_CONTRACT = pContract;
        EXCEPTION
           WHEN NO_DATA_FOUND THEN
             v_CalcCountContract := 0;
        END;

        IF v_CalcCountContract > 1 THEN 
          UPDATE DNPTXCALC_DBT
             SET T_COUNT = T_COUNT - 1
           WHERE T_KIND = pKind
             AND T_CLIENT = pClientID
             AND T_IIS    = pIIS
             AND T_ENDDATE = pEndDate
             AND ( T_SUBKIND = pSubKind OR pSubKind = 0 )
             AND T_CONTRACT = pContract;

          RETURN;
        ELSIF v_CalcCount > 1 THEN
          UPDATE DNPTXCALC_DBT
             SET T_COUNT = T_COUNT - 1
           WHERE T_KIND = pKind
             AND T_CLIENT = pClientID
             AND T_IIS    = pIIS
             AND T_ENDDATE = pEndDate
             AND ( T_SUBKIND = pSubKind OR pSubKind = 0 )
             AND T_CONTRACT = 0;
             
          RETURN;
        END IF;

      END IF;

      IF v_CalcCountContract > 0 THEN
        DELETE FROM DNPTXCALC_DBT
              WHERE T_KIND    = pKind
                AND T_CLIENT  = pClientID
                AND T_IIS     = pIIS
                AND T_ENDDATE = pEndDate
                AND ( T_SUBKIND = pSubKind OR pSubKind = 0 )
                AND T_CONTRACT = pContract;
      ELSE
        DELETE FROM DNPTXCALC_DBT
              WHERE T_KIND    = pKind
                AND T_CLIENT  = pClientID
                AND T_IIS     = pIIS
                AND T_ENDDATE = pEndDate
                AND ( T_SUBKIND = pSubKind OR pSubKind = 0 )
                AND T_CONTRACT = 0;
      END IF;

   END;

   function IsAdmin(pOper IN NUMBER)
   return BOOLEAN
    is
    v_Count NUMBER;
   begin
      select count(1) into v_Count from DPERSON_DBT where t_OPER = pOPER and t_cTypePerson = 'А';

      if v_Count = 1 then
        return TRUE;
      end if;

      return FALSE;
   end;

   FUNCTION  ExistIISContr( pClientID IN NUMBER, pOperDate IN DATE )
     RETURN NUMBER DETERMINISTIC
   IS
      v_CountSfContr NUMBER;
   BEGIN
       v_CountSfContr := 0;

      SELECT Count(1)   INTO v_CountSfContr
        FROM DSFCONTR_DBT
       WHERE (T_ServKind    = PTSK_STOCKDL or T_ServKind    = PTSK_DV )
         AND T_PartyID     = pClientID
         AND T_DateBegin <= pOperDate
         AND (T_DateClose >= pOperDate OR T_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY') )
         AND CheckContrIIS(T_ID) = 1;

        IF v_CountSfContr > 0 THEN
            RETURN 1;
        ELSE
            RETURN 0;
        END IF;

        EXCEPTION
           WHEN NO_DATA_FOUND THEN
        RETURN 0;
   END;

   FUNCTION  ExistNotIISContr( pClientID IN NUMBER, pOperDate IN DATE )
     RETURN NUMBER DETERMINISTIC
   IS
      v_CountSfContr NUMBER;
   BEGIN
       v_CountSfContr := 0;

      SELECT Count(1)   INTO v_CountSfContr
        FROM DSFCONTR_DBT
       WHERE (T_ServKind    = PTSK_STOCKDL or T_ServKind    = PTSK_DV )
         AND T_PartyID     = pClientID
         AND T_DateBegin <= pOperDate
         AND (T_DateClose >= pOperDate OR T_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY') )
         AND CheckContrIIS(T_ID) = 0;

        IF v_CountSfContr > 0 THEN
            RETURN 1;
        ELSE
            RETURN 0;
        END IF;

        EXCEPTION
           WHEN NO_DATA_FOUND THEN
        RETURN 0;
   END;

   FUNCTION  ExistNotIISContrPeriod( pClientID IN NUMBER, pBegDate IN DATE, pEndDate IN DATE )
     RETURN NUMBER DETERMINISTIC
   IS
      v_CountSfContr NUMBER;
   BEGIN
       v_CountSfContr := 0;

      SELECT Count(1)   INTO v_CountSfContr
        FROM DSFCONTR_DBT
       WHERE (T_ServKind    = PTSK_STOCKDL or T_ServKind    = PTSK_DV )
         AND T_PartyID     = pClientID
         AND T_DateBegin <= pEndDate
         AND (T_DateClose >= pBegDate OR T_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY') )
         AND CheckContrIIS(T_ID) = 0;

        IF v_CountSfContr > 0 THEN
            RETURN 1;
        ELSE
            RETURN 0;
        END IF;

        EXCEPTION
           WHEN NO_DATA_FOUND THEN
        RETURN 0;
   END;

/* Проверка, является ли договор ИИС */
   FUNCTION CheckContrIIS(pSfContrID IN NUMBER) 
      RETURN NUMBER DETERMINISTIC
   IS
      v_RetVal NUMBER(5) := 0;
   BEGIN
      SELECT 1 INTO v_RetVal
        FROM ddlcontrmp_dbt mp, ddlcontr_dbt dl
       WHERE (mp.t_SfContrID = pSfContrID
          OR  dl.t_SfContrID = pSfContrID)
         AND dl.t_DlContrID = mp.t_DlContrID
         AND dl.t_IIS = 'X'
         AND ROWNUM = 1;  
      RETURN v_RetVal;
   EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN 0;
   END;--CheckContrIIS

/* Проверка, является ли договор ИИС с типом ИИС-III */
   FUNCTION CheckContrIIS3 (pSfContrID IN NUMBER)
      RETURN NUMBER DETERMINISTIC
   IS
      v_RetVal NUMBER(5) := 0;
   BEGIN
      SELECT 1 INTO v_RetVal
        FROM ddlcontrmp_dbt mp, ddlcontr_dbt dl
       WHERE mp.t_SfContrID = pSfContrID
         AND dl.t_DlContrID = mp.t_DlContrID
         AND dl.t_IIS = 'X'
         AND dl.t_IISType = DLCONTR_IISTYPE_IIS3
         AND ROWNUM = 1;  
      RETURN v_RetVal;
   EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN 0;
   END;--CheckContrIIS3

/* Проверка наличия операций зачисления ц/б по всем субдоговорам*/
   FUNCTION IsExistOperationAvrWrtIn (pSfContrID IN NUMBER, pOperDate IN DATE)
      RETURN NUMBER DETERMINISTIC
   IS
      v_RetVal NUMBER(5) := 0;
   BEGIN
      SELECT 1 INTO v_RetVal
        FROM ddl_tick_dbt tick, ddlcontrmp_dbt mp1, ddlcontrmp_dbt mp2
       WHERE tick.t_ClientContrID = mp2.t_SfContrID
         AND mp1.t_SfContrID = pSfContrID
         AND mp2.t_DlContrID = mp1.t_DlContrID
         AND tick.t_BOfficeKind = RSI_NPTXC.DL_AVRWRT
         AND RSB_SECUR.IsAvrWrtIn(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) = 1
         AND tick.t_DealStatus = 20 --DL_CLOSED
         AND tick.t_DealDate < pOperDate
         AND ROWNUM = 1;  
      RETURN v_RetVal;
   EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN 0;
   END;--IsExistOperationAvrWrtIn

/* Проверка наличия операций зачисления денежных средств по всем субдоговорам*/
   FUNCTION IsExistOperationEnrol (pSfContrID IN NUMBER, pOperDate IN DATE)
      RETURN NUMBER DETERMINISTIC
   IS
      v_RetVal NUMBER(5) := 0;
   BEGIN
      SELECT 1 INTO v_RetVal
        FROM dnptxop_dbt nptxop, ddlcontrmp_dbt mp1, ddlcontrmp_dbt mp2
       WHERE nptxop.t_Contract = mp2.t_SfContrID
         AND mp1.t_SfContrID = pSfContrID
         AND mp2.t_DlContrID = mp1.t_DlContrID
         AND nptxop.t_DocKind = RSB_SECUR.DL_WRTMONEY
         AND nptxop.t_SubKind_Operation = 10 --DL_NPTXOP_WRTKIND_ENROL
         AND nptxop.t_Status = 2 --DL_TXOP_Close
         AND nptxop.t_OperDate < pOperDate
         AND ROWNUM = 1;  
      RETURN v_RetVal;
   EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN 0;
   END;--IsExistOperationEnrol

/*Получить параметры NPTXOBJ из записи, переданной в виде Raw*/
   PROCEDURE GetNptxObjFromRaw(pNptxObj IN RAW, v_rNptxObj IN OUT dnptxobj_dbt%rowtype)
   AS
   BEGIN
    InitError();
    rsb_struct.readStruct('dnptxobj_dbt');
    v_rNptxObj.t_Analitic1      := rsb_struct.getlong('t_Analitic1', pNptxObj);
    v_rNptxObj.t_Analitic2      := rsb_struct.getlong('t_Analitic2', pNptxObj);
    v_rNptxObj.t_Analitic3      := rsb_struct.getlong('t_Analitic3', pNptxObj);
    v_rNptxObj.t_Analitic4      := rsb_struct.getlong('t_Analitic4', pNptxObj);
    v_rNptxObj.t_Analitic5      := rsb_struct.getlong('t_Analitic5', pNptxObj);
    v_rNptxObj.t_Analitic6      := rsb_struct.getlong('t_Analitic6', pNptxObj);

    v_rNptxObj.t_AnaliticKind1  := rsb_struct.getInt('t_AnaliticKind1', pNptxObj);
    v_rNptxObj.t_AnaliticKind2  := rsb_struct.getInt('t_AnaliticKind2', pNptxObj);
    v_rNptxObj.t_AnaliticKind3  := rsb_struct.getInt('t_AnaliticKind3', pNptxObj);
    v_rNptxObj.t_AnaliticKind4  := rsb_struct.getInt('t_AnaliticKind4', pNptxObj);
    v_rNptxObj.t_AnaliticKind5  := rsb_struct.getInt('t_AnaliticKind5', pNptxObj);
    v_rNptxObj.t_AnaliticKind6  := rsb_struct.getInt('t_AnaliticKind6', pNptxObj);

    v_rNptxObj.t_Client         := rsb_struct.getlong('t_Client', pNptxObj);
    v_rNptxObj.t_Comment        := rsb_struct.getString('t_Comment', pNptxObj);
    v_rNptxObj.t_Cur            := rsb_struct.getlong('t_Cur', pNptxObj);

    v_rNptxObj.T_DATE                           := rsb_struct.getdate('T_DATE', pNptxObj);
    v_rNptxObj.T_DIRECTION                      := rsb_struct.getInt('T_DIRECTION', pNptxObj);
    v_rNptxObj.T_KIND                         := rsb_struct.getInt('T_KIND', pNptxObj);
    v_rNptxObj.T_LEVEL                            := rsb_struct.getInt('T_LEVEL', pNptxObj);
    v_rNptxObj.T_OBJID                            := rsb_struct.getlong('T_OBJID', pNptxObj);
    v_rNptxObj.T_SUM                                := rsb_struct.getmoney('T_SUM', pNptxObj);
    v_rNptxObj.T_SUM0                               := rsb_struct.getmoney('T_SUM0', pNptxObj);
    v_rNptxObj.T_USER                               := rsb_struct.getChar('T_USER', pNptxObj);

   END;-- GetNptxObjFromRaw

/* Проверка, что объект НДР - по договору ИИС */
   FUNCTION CheckObjIIS (pNptxObj IN RAW)
      RETURN NUMBER DETERMINISTIC
   IS
      v_IsObjIIS   NUMBER;
      v_rNptxObj   dnptxobj_dbt%rowtype;
   BEGIN
      v_IsObjIIS := 0;                                               /*false*/
      InitError();
      GetNptxObjFromRaw(pNptxObj, v_rNptxObj);

      IF ( (v_rNptxObj.t_AnaliticKind6 = SERVISE_CONTR) AND ( CheckContrIIS( v_rNptxObj.t_Analitic6 ) = 1 ) )
      THEN
         v_IsObjIIS := 1;
      END IF;

      RETURN v_IsObjIIS;
   END;

   FUNCTION CheckObjIIS (pAnaliticKind6 IN NUMBER, pAnalitic6 IN NUMBER)
      RETURN NUMBER RESULT_CACHE RELIES_ON(DOBJATCOR_DBT)
   IS
      v_IsObjIIS   NUMBER;
   BEGIN
      v_IsObjIIS := 0;                                               /*false*/
      InitError();

      IF ( (pAnaliticKind6 = SERVISE_CONTR) AND ( CheckContrIIS( pAnalitic6 ) = 1 ) )
      THEN
         v_IsObjIIS := 1;
      END IF;

      RETURN v_IsObjIIS;
   END;

   FUNCTION CheckObjIIS(pNptxObj IN dnptxobj_dbt%rowtype)
      RETURN NUMBER DETERMINISTIC
   IS
      v_IsObjIIS   NUMBER;

   BEGIN
      v_IsObjIIS := 0;                                               /*false*/
      InitError();

      IF ( (pNptxObj.t_AnaliticKind6 = SERVISE_CONTR) AND ( CheckContrIIS( pNptxObj.t_Analitic6 ) = 1 ) )
      THEN
         v_IsObjIIS := 1;
      END IF;

      RETURN v_IsObjIIS;
   END;

   --Поиск примечания (в виде даты)
   function GetDateFromNoteText( v_ObjectType IN NUMBER, v_ObjectID IN VARCHAR2, v_NoteKind IN NUMBER )
     return DATE
     is
     v_Text dnotetext_dbt.t_text%TYPE;
   begin
     begin
       select t_Text into v_Text
         from dnotetext_dbt
        where t_DocumentID = v_ObjectID and
              t_ObjectType = v_ObjectType and
              t_NoteKind = v_NoteKind and
              ROWNUM = 1
       order by t_Date desc;

       return rsb_struct.getDate(v_Text);

     exception
       when NO_DATA_FOUND then return NULL;
       when OTHERS then return NULL;
     end;
   end;

   function GetDateFromRQ( v_RQID IN NUMBER, v_FactDate IN DATE )
     return DATE
     is
     v_RetDate DATE;
   begin
     v_RetDate := GetDateFromNoteText( 993, LPAD( v_RQID, 10, '0' ), 47 );

     if (v_RetDate is null) or
        (v_RetDate = TO_DATE('01.01.0001', 'DD.MM.YYYY')) then
         v_RetDate := v_FactDate;
     end if;

     return v_RetDate;
   end;

   function GetDateFromPayment( v_PaymID IN NUMBER, v_ValueDate IN DATE )
     return DATE
     is
     v_RetDate DATE;
   begin
     v_RetDate := GetDateFromNoteText( 501, LPAD( v_PaymID, 10, '0' ), 47 );

     if (v_RetDate is null) or
        (v_RetDate = TO_DATE('01.01.0001', 'DD.MM.YYYY')) then
         v_RetDate := v_ValueDate;
     end if;

     return v_RetDate;
   end;

   -- Проверка, что за период существуют данные для расчета НДФЛ
   FUNCTION CheckExistDataForPeriod( pBegDate  IN DATE,            -- Дата начала
                                     pEndDate  IN DATE,            -- Дата окончания
                                     pClientID IN NUMBER,          -- Клиент
                                     pIIS      IN NUMBER DEFAULT 0,-- Признак ИИС
                                     pFIID     IN NUMBER DEFAULT -1,
                                     pExcludeBuy IN NUMBER DEFAULT 0, --Признак Исключить покупки
                                     pDlContrID IN NUMBER DEFAULT 0,
                                     pOnlyBuy  IN NUMBER DEFAULT 0 --Признак Только покупки
                                   ) RETURN NUMBER
   IS
      v_Count NUMBER := 0;
   BEGIN

      -- 1.1
      SELECT COUNT(1) INTO v_Count
        FROM DDL_TICK_DBT Tick
       WHERE Tick.T_DEALDATE >= pBegDate
         AND Tick.T_DEALDATE <= pEndDate
         AND Tick.t_ClientID = pClientID 
         AND CheckContrIIS(Tick.t_ClientContrID) = pIIS
         AND (pDlContrID = 0 OR Tick.t_ClientContrID IN (SELECT mp.t_SfContrID FROM ddlcontrmp_dbt mp WHERE mp.t_DlContrID = pDlContrID)) 
         AND EXISTS( SELECT RQ.t_ID
                       FROM DDLRQ_DBT RQ
                      WHERE RQ.t_DocID   = Tick.t_DealID
                        AND RQ.t_DocKind = Tick.t_BOfficekind
                        AND RQ.t_FIID = case when pFIID != -1 then pFIID else RQ.t_FIID end
            )
         AND 1 = (CASE WHEN pExcludeBuy > 0 AND RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 1
                            AND RSB_SECUR.IsTwoPart(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 0
                            THEN 0
                       ELSE 1 END)
         AND 1 = (CASE WHEN pOnlyBuy = 0 OR 
                            (pOnlyBuy > 0 
                             AND (  RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 1
                                OR RSB_SECUR.IsAvrWrtIn(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 1
                                )
                             AND RSB_SECUR.IsTwoPart(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 0
                            )
                            THEN 1
                       ELSE 0 END)                    
         and rownum < 2;

      IF( v_Count > 0 ) THEN
         RETURN 1;
      END IF;

     --1.2
     SELECT /*+ index(tk DDL_TICK_DBT_IDX_U2)*/ COUNT(1) INTO v_Count
        FROM DDL_TICK_DBT Tick
       WHERE Tick.T_DEALDATE >= pBegDate
         AND Tick.T_DEALDATE <= pEndDate
         AND Tick.t_PartyID = pClientID 
         AND Tick.t_IsPartyClient = 'X' 
         AND CheckContrIIS(Tick.t_PartyContrID) = pIIS
         AND (pDlContrID = 0 OR Tick.t_PartyContrID IN (SELECT mp.t_SfContrID FROM ddlcontrmp_dbt mp WHERE mp.t_DlContrID = pDlContrID))
         AND EXISTS( SELECT RQ.t_ID
                       FROM DDLRQ_DBT RQ
                      WHERE RQ.t_DocID   = Tick.t_DealID
                        AND RQ.t_DocKind = Tick.t_BOfficekind
                        AND RQ.t_FIID = case when pFIID != -1 then pFIID else RQ.t_FIID end
            )
         and rownum < 2;

      IF( v_Count > 0 ) THEN
         RETURN 1;
      END IF;

      -- 2.1
      SELECT COUNT(1) INTO v_Count
        FROM DDL_TICK_DBT Tick
       WHERE Tick.T_DEALDATE < pBegDate
         AND Tick.T_DEALDATE >= TO_DATE('01.01.'||TO_CHAR( (EXTRACT( YEAR FROM pBegDate) - 1)), 'dd.mm.yyyy')
         AND t_dealtype <> 32011
         AND Tick.t_ClientID = pClientID AND CheckContrIIS(Tick.t_ClientContrID) = pIIS 
         AND (pDlContrID = 0 OR Tick.t_ClientContrID IN (SELECT mp.t_SfContrID FROM ddlcontrmp_dbt mp WHERE mp.t_DlContrID = pDlContrID))
         AND EXISTS( SELECT RQ.t_ID
                       FROM DDLRQ_DBT RQ
                      WHERE RQ.t_DocID   = Tick.t_DealID
                        AND RQ.t_DocKind = Tick.t_BOfficekind
                        AND RQ.t_FactDate >= pBegDate
                        AND RQ.t_FIID = case when pFIID != -1 then pFIID else RQ.t_FIID end
                   )
         AND 1 = (CASE WHEN pExcludeBuy > 0 AND RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 1
                            AND RSB_SECUR.IsTwoPart(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 0
                            THEN 0
                       ELSE 1 END)
         AND 1 = (CASE WHEN pOnlyBuy = 0 OR 
                            (pOnlyBuy > 0 
                             AND (  RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 1
                                OR RSB_SECUR.IsAvrWrtIn(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 1
                                )
                             AND RSB_SECUR.IsTwoPart(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 0
                            )
                            THEN 1
                       ELSE 0 END)
         and rownum < 2;
      
      IF( v_Count > 0 ) THEN
         RETURN 1;
      END IF;

      SELECT COUNT(1) INTO v_Count
        FROM DDL_TICK_DBT Tick
       WHERE Tick.T_DEALDATE < pBegDate
         AND Tick. t_dealtype = 32011
         AND Tick.t_ClientID = pClientID AND CheckContrIIS(Tick.t_ClientContrID) = pIIS 
         AND (pDlContrID = 0 OR Tick.t_ClientContrID IN (SELECT mp.t_SfContrID FROM ddlcontrmp_dbt mp WHERE mp.t_DlContrID = pDlContrID))
         AND EXISTS( SELECT RQ.t_ID
                       FROM DDLRQ_DBT RQ
                      WHERE RQ.t_DocID   = Tick.t_DealID
                        AND RQ.t_DocKind = Tick.t_BOfficekind
                        AND RQ.t_FactDate >= pBegDate
                        AND RQ.t_FIID = case when pFIID != -1 then pFIID else RQ.t_FIID end
                   )
         AND 1 = (CASE WHEN pExcludeBuy > 0 AND RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 1
                            AND RSB_SECUR.IsTwoPart(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 0
                            THEN 0
                       ELSE 1 END) 
         AND 1 = (CASE WHEN pOnlyBuy = 0 OR 
                            (pOnlyBuy > 0 
                             AND (  RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 1
                                OR RSB_SECUR.IsAvrWrtIn(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 1
                                )
                             AND RSB_SECUR.IsTwoPart(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(Tick.t_DealType, Tick.t_BofficeKind))) = 0
                            )
                            THEN 1
                       ELSE 0 END)
         and rownum < 2;
      
      IF( v_Count > 0 ) THEN
         RETURN 1;
      END IF;

      -- 2.2
      SELECT /*+ index(tk DDL_TICK_DBT_IDX_U2)*/ COUNT(1) INTO v_Count
        FROM DDL_TICK_DBT Tick
       WHERE Tick.T_DEALDATE < pBegDate
         AND Tick.t_PartyID = pClientID AND Tick.t_IsPartyClient = 'X' AND CheckContrIIS(Tick.t_PartyContrID) = pIIS
         AND (pDlContrID = 0 OR Tick.t_PartyContrID IN (SELECT mp.t_SfContrID FROM ddlcontrmp_dbt mp WHERE mp.t_DlContrID = pDlContrID))
         AND EXISTS( SELECT RQ.t_ID
                       FROM DDLRQ_DBT RQ
                      WHERE RQ.t_DocID   = Tick.t_DealID
                        AND RQ.t_DocKind = Tick.t_BOfficekind
                        AND RQ.t_FactDate >= pBegDate
                        AND RQ.t_FIID = case when pFIID != -1 then pFIID else RQ.t_FIID end
                   )
         and rownum < 2;

      IF( v_Count > 0 ) THEN
         RETURN 1;
      END IF;

      -- 5
      IF( pIIS = 0 ) THEN
         SELECT COUNT(1) INTO v_Count
           FROM DDVFITURN_DBT
          WHERE T_CLIENT = pClientID
            AND T_DATE >= pBegDate
            AND T_DATE <= pEndDate
            AND (pDlContrID = 0 OR T_CLIENTCONTR IN (SELECT mp.t_SfContrID FROM ddlcontrmp_dbt mp WHERE mp.t_DlContrID = pDlContrID))
         and rownum < 2;

         IF( v_Count > 0 ) THEN
            RETURN 1;
         END IF;

         SELECT COUNT(1) INTO v_Count
           FROM DDVNDEAL_DBT D, DPMPAYM_DBT P
          WHERE P.T_DOCKIND = RSB_Derivatives.DL_DVNDEAL
            AND P.T_DOCUMENTID = D.T_ID
            AND P.T_PAYMSTATUS = Rsb_Payment.PM_FINISHED
            AND D.T_CLIENT = pClientID
            AND P.T_VALUEDATE >= pBegDate
            AND P.T_VALUEDATE <= pEndDate
            AND (pDlContrID = 0 OR T_CLIENTCONTR IN (SELECT mp.t_SfContrID FROM ddlcontrmp_dbt mp WHERE mp.t_DlContrID = pDlContrID))
            and rownum < 2;

         IF( v_Count > 0 ) THEN
            RETURN 1;
         END IF;

      END IF;

      RETURN 0;
   END;

   -- Возвращает дату предыдущей операции заданного вида для клиента
   FUNCTION GetPrevOpDate( pDocKind IN NUMBER, pClientID IN NUMBER )
     RETURN DATE
   IS
     v_Date DATE;
   BEGIN
     SELECT NVL(MAX(T_OPERDATE), TO_DATE('01.01.0001','DD.MM.YYYY'))
       INTO v_Date
       FROM DNPTXOP_DBT
      WHERE T_DOCKIND = pDocKind
        AND T_CLIENT = pClientID;

     RETURN v_Date;
   END;

   -- Проверка для субъекта, что установлена КО "Является плательщиком НДФЛ" != "Нет"
   FUNCTION IsPayerNPTX( PartyID IN NUMBER, OperDate IN DATE ) RETURN NUMBER DETERMINISTIC
   IS
      CategoryValue dobjattr_dbt.t_NumInList % TYPE;
   BEGIN
      BEGIN
          SELECT Attr.t_NumInList INTO CategoryValue
            FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
           WHERE     AtCor.t_ObjectType = 3  -- OBJTYPE_PARTY
                 AND AtCor.t_GroupID    = 42 -- Является плательщиком НДФЛ
                 AND AtCor.t_Object     = LPAD( PartyID, 10, '0' )
                 AND AtCor.t_ValidFromDate  = ( SELECT MAX(t.T_ValidFromDate)
                                                  FROM DOBJATCOR_DBT t
                                                 WHERE     t.T_ObjectType = AtCor.T_ObjectType
                                                       AND t.T_GroupID    = AtCor.T_GroupID
                                                       AND t.t_Object     = AtCor.t_Object
                                                       AND t.T_ValidFromDate <= OperDate
                                              )
                 AND Attr.t_AttrID      = AtCor.t_AttrID
                 AND Attr.t_ObjectType  = AtCor.t_ObjectType
                 AND Attr.t_GroupID     = AtCor.t_GroupID
                 AND AtCor.t_ValidToDate >= OperDate;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN CategoryValue := chr(0);
         WHEN OTHERS THEN
            return 0;
      END;

      IF( CategoryValue = '1' OR CategoryValue = '2' ) THEN
         return 1;
      ELSIF( CategoryValue = '3' ) THEN
         return 0;
      END IF;

      RETURN 0;
   END; -- IsPayerNPTX


   /**
   @brief Выполняет вставку объекта, вызывается в макросах шагов, без проверки наличия существующего
   @param [in] pDate Дата создания объекта НДР
   @param [in] pClient ID клиента
   @param [in] pDirection Направление
   @param [in] pLevel Уровень
   @param [in] pUser Признак "Пользовательский"
   @param [in] pKind Вид объекта НДР
   @param [in] pSum Сумма объекта НДР
   @param [in] pCur Валюта объекта НДР
   @param [in] pAnaliticKind1 Вид аналитики 1
   @param [in] pAnalitic1 Значение аналитики 1
   @param [in] pAnaliticKind2 Вид аналитики 2
   @param [in] pAnalitic2 Значение аналитики 2
   @param [in] pAnaliticKind3 Вид аналитики 3
   @param [in] pAnalitic3 Значение аналитики 3
   @param [in] pAnaliticKind4 Вид аналитики 4
   @param [in] pAnalitic4 Значение аналитики 4
   @param [in] pAnaliticKind5 Вид аналитики 5
   @param [in] pAnalitic5 Значение аналитики 5
   @param [in] pAnaliticKind6 Вид аналитики 6
   @param [in] pAnalitic6 Значение аналитики 6
   @param [in] pComment Комментарий
   @param [in] pDocID ID текущей операции
   @param [in] pStep ID шага текущей операции
   @param [in] pNoCalcNOB Не расчитывать НОБ
   @param [out] pObjID ID объекта НДР
   @param [in] pFromOutSyst Признак "Из внешней системы"
   @param [in] pOutSystCode Код внешней системы
   @param [in] pOutObjID ID во внешней системе
   @param [in] pSourceObjID ID первоначального объекта
   @param [in] pTechnical Признак "Технический"
   @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
   @param [in] pTransfKind Период расчета
   */
   PROCEDURE InsertTaxObjectRetID(
                                  pDate IN DATE,
                                  pClient IN NUMBER,
                                  pDirection IN NUMBER,
                                  pLevel IN NUMBER,
                                  pUser IN CHAR,
                                  pKind IN NUMBER,
                                  pSum IN NUMBER,
                                  pCur IN NUMBER,
                                  pAnaliticKind1 IN NUMBER,
                                  pAnalitic1 IN NUMBER,
                                  pAnaliticKind2 IN NUMBER,
                                  pAnalitic2 IN NUMBER,
                                  pAnaliticKind3 IN NUMBER,
                                  pAnalitic3 IN NUMBER,
                                  pAnaliticKind4 IN NUMBER,
                                  pAnalitic4 IN NUMBER,
                                  pAnaliticKind5 IN NUMBER,
                                  pAnalitic5 IN NUMBER,
                                  pAnaliticKind6 IN NUMBER,
                                  pAnalitic6 IN NUMBER,
                                  pComment IN VARCHAR2,
                                  pDocID IN NUMBER,
                                  pStep IN NUMBER,
                                  pNoCalcNOB IN NUMBER,
                                  pObjID OUT NUMBER,
                                  pFromOutSyst IN CHAR DEFAULT CHR(0),
                                  pOutSystCode IN VARCHAR2 DEFAULT CHR(1),
                                  pOutObjID IN VARCHAR2 DEFAULT CHR(1),
                                  pSourceObjID IN NUMBER DEFAULT 0,
                                  pTechnical IN CHAR DEFAULT CHR(0),
                                  pConvDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                  pTransfDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                  pTransfKind IN NUMBER DEFAULT 0,
                                  pHolding_Period IN NUMBER DEFAULT 0,
                                  pChangeCode IN CHAR DEFAULT CHR(0)
                                )
   IS
     v_ConvDate DATE;
     v_TaxPeriod NUMBER;

     v_AnaliticKind6 NUMBER;
     v_Analitic6     NUMBER;
   BEGIN
      pObjID := 0;
      v_TaxPeriod := 0;

      v_AnaliticKind6 := pAnaliticKind6;
      v_Analitic6     := pAnalitic6;    
      
      v_ConvDate := pDate;
      IF pConvDate > TO_DATE('01.01.0001','DD.MM.YYYY') THEN
        v_ConvDate := pConvDate;
      END IF;

      IF (pSum <> 0) THEN
         IF pLevel = 8 THEN
           
           IF pDocID > 0 THEN
             BEGIN

               SELECT EXTRACT(YEAR FROM T_PREVDATE)
                 INTO v_TaxPeriod
                 FROM DNPTXOP_DBT
                WHERE T_ID = pDocID
                  AND T_DOCKIND = RSB_SECUR.DL_HOLDNDFL;

               EXCEPTION
                    WHEN NO_DATA_FOUND THEN v_TaxPeriod := 0;
             END;

             IF v_TaxPeriod = 0 THEN
               BEGIN

                 SELECT EXTRACT(YEAR FROM T_PREVDATE)
                   INTO v_TaxPeriod
                   FROM DNPTXOP_DBT
                  WHERE T_ID = pDocID
                    AND T_DOCKIND = 4649 /*Зачет удержанного НДФЛ*/;

                 EXCEPTION
                      WHEN NO_DATA_FOUND THEN v_TaxPeriod := 0;
               END;
             END IF;
           END IF;

           IF v_TaxPeriod = 0 THEN
             v_TaxPeriod := EXTRACT(YEAR FROM pDate);
           END IF;

         ELSIF pLevel >= 3 AND pLevel <= 7 AND pAnaliticKind6 = RSI_NPTXC.TXOBJ_KIND6020 AND RSI_NPTO.CheckContrIIS(pAnalitic6) = 0 THEN
           v_AnaliticKind6 := 0;
           v_Analitic6     := -1;
         END IF;

         INSERT INTO DNPTXOBJ_DBT (
                                   T_DATE         ,     -- T_DATE
                                   T_CLIENT       ,     -- T_CLIENT
                                   T_DIRECTION    ,     -- T_DIRECTION
                                   T_LEVEL        ,     -- T_LEVEL
                                   T_USER         ,     -- T_USER
                                   T_KIND         ,     -- T_KIND
                                   T_SUM          ,     -- T_SUM
                                   T_CUR          ,     -- T_CUR
                                   T_SUM0         ,     -- T_SUM0
                                   T_ANALITICKIND1,     -- T_ANALITICKIND1
                                   T_ANALITIC1    ,     -- T_ANALITIC1
                                   T_ANALITICKIND2,     -- T_ANALITICKIND2
                                   T_ANALITIC2    ,     -- T_ANALITIC2
                                   T_ANALITICKIND3,     -- T_ANALITICKIND3
                                   T_ANALITIC3    ,     -- T_ANALITIC3
                                   T_ANALITICKIND4,     -- T_ANALITICKIND4
                                   T_ANALITIC4    ,     -- T_ANALITIC4
                                   T_ANALITICKIND5,     -- T_ANALITICKIND5
                                   T_ANALITIC5    ,     -- T_ANALITIC5
                                   T_ANALITICKIND6,     -- T_ANALITICKIND6
                                   T_ANALITIC6    ,     -- T_ANALITIC6
                                   T_COMMENT      ,     -- T_COMMENT
                                   T_FROMOUTSYST  ,
                                   T_OUTSYSTCODE  ,
                                   T_OUTOBJID     ,
                                   T_SOURCEOBJID  , 
                                   T_TECHNICAL    ,
                                   T_TAXPERIOD    ,
                                   T_TRANSFDATE   ,
                                   T_TRANSFKIND   ,
                                   T_HOLDING_PERIOD,
                                   T_CHANGECODE
                                  )
                           VALUES (
                                   pDate,                                               -- T_DATE
                                   pClient,                                             -- T_CLIENT
                                   pDirection,                                          -- T_DIRECTION
                                   pLevel,                                              -- T_LEVEL
                                   pUser,                                               -- T_USER
                                   pKind,                                               -- T_KIND
                                   pSum,                                                -- T_SUM
                                   pCur,                                                -- T_CUR
                                   RSI_RSB_FIInstr.ConvSum( pSum, pCur, 0, v_ConvDate, 1 ),  -- T_SUM0
                                   pAnaliticKind1,                                      -- T_ANALITICKIND1
                                   pAnalitic1,                                          -- T_ANALITIC1
                                   pAnaliticKind2,                                      -- T_ANALITICKIND2
                                   pAnalitic2,                                          -- T_ANALITIC2
                                   pAnaliticKind3,                                      -- T_ANALITICKIND3
                                   pAnalitic3,                                          -- T_ANALITIC3
                                   pAnaliticKind4,                                      -- T_ANALITICKIND4
                                   pAnalitic4,                                          -- T_ANALITIC4
                                   pAnaliticKind5,                                      -- T_ANALITICKIND5
                                   pAnalitic5,                                          -- T_ANALITIC5
                                   v_AnaliticKind6,                                     -- T_ANALITICKIND6
                                   v_Analitic6,                                         -- T_ANALITIC6
                                   pComment,                                            -- T_COMMENT
                                   pFromOutSyst,
                                   pOutSystCode,
                                   pOutObjID,
                                   pSourceObjID,
                                   pTechnical,
                                   v_TaxPeriod,
                                   pTransfDate,
                                   pTransfKind,
                                   pHolding_Period,
                                   pChangeCode
                                  ) RETURNING t_ObjID INTO pObjID;
         IF pUser <> 'X' THEN

            INSERT INTO DNPTXOBDC_DBT (
                                       T_DOCID    ,  -- T_DOCID
                                       T_STEP     ,  -- T_STEP
                                       T_OBJID       -- T_OBJID
                                      )
                               VALUES (
                                       pDocID,       -- T_DOCID
                                       pStep,        -- T_STEP
                                       pObjID       -- T_OBJID
                                      );
         END IF;

      END IF;
   END; -- InsertTaxObjectRetID


   /**
   @brief Выполняет вставку объекта, вызывается в макросах шагов, без проверки наличия существующего
   @param [in] pDate Дата создания объекта НДР
   @param [in] pClient ID клиента
   @param [in] pDirection Направление
   @param [in] pLevel Уровень
   @param [in] pUser Признак "Пользовательский"
   @param [in] pKind Вид объекта НДР
   @param [in] pSum Сумма объекта НДР
   @param [in] pCur Валюта объекта НДР
   @param [in] pAnaliticKind1 Вид аналитики 1
   @param [in] pAnalitic1 Значение аналитики 1
   @param [in] pAnaliticKind2 Вид аналитики 2
   @param [in] pAnalitic2 Значение аналитики 2
   @param [in] pAnaliticKind3 Вид аналитики 3
   @param [in] pAnalitic3 Значение аналитики 3
   @param [in] pAnaliticKind4 Вид аналитики 4
   @param [in] pAnalitic4 Значение аналитики 4
   @param [in] pAnaliticKind5 Вид аналитики 5
   @param [in] pAnalitic5 Значение аналитики 5
   @param [in] pAnaliticKind6 Вид аналитики 6
   @param [in] pAnalitic6 Значение аналитики 6
   @param [in] pComment Комментарий
   @param [in] pDocID ID текущей операции
   @param [in] pStep ID шага текущей операции
   @param [in] pNoCalcNOB Не расчитывать НОБ
   @param [out] pObjID ID объекта НДР
   @param [in] pFromOutSyst Признак "Из внешней системы"
   @param [in] pOutSystCode Код внешней системы
   @param [in] pOutObjID ID во внешней системе
   @param [in] pSourceObjID ID первоначального объекта
   @param [in] pTechnical Признак "Технический"
   */
   PROCEDURE InsertTaxObject(
                              pDate IN DATE,
                              pClient IN NUMBER,
                              pDirection IN NUMBER,
                              pLevel IN NUMBER,
                              pUser IN CHAR,
                              pKind IN NUMBER,
                              pSum IN NUMBER,
                              pCur IN NUMBER,
                              pAnaliticKind1 IN NUMBER,
                              pAnalitic1 IN NUMBER,
                              pAnaliticKind2 IN NUMBER,
                              pAnalitic2 IN NUMBER,
                              pAnaliticKind3 IN NUMBER,
                              pAnalitic3 IN NUMBER,
                              pAnaliticKind4 IN NUMBER,
                              pAnalitic4 IN NUMBER,
                              pAnaliticKind5 IN NUMBER,
                              pAnalitic5 IN NUMBER,
                              pAnaliticKind6 IN NUMBER,
                              pAnalitic6 IN NUMBER,
                              pComment IN VARCHAR2,
                              pDocID IN NUMBER,
                              pStep IN NUMBER,
                              pNoCalcNOB IN NUMBER DEFAULT 0,
                              pFromOutSyst IN CHAR DEFAULT CHR(0),
                              pOutSystCode IN VARCHAR2 DEFAULT CHR(1),
                              pOutObjID IN VARCHAR2 DEFAULT CHR(1),
                              pSourceObjID IN NUMBER DEFAULT 0,
                              pTechnical IN CHAR DEFAULT CHR(0),
                              pConvDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                              pTransfDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                              pTransfKind IN NUMBER DEFAULT 0,
                              pHolding_Period IN NUMBER DEFAULT 0,
                              pChangeCode IN CHAR DEFAULT CHR(0)
                            )
   IS
      v_ObjID NUMBER;
   BEGIN
      InsertTaxObjectRetID(pDate,
                           pClient,
                           pDirection,
                           pLevel,
                           pUser,
                           pKind,
                           pSum,
                           pCur,
                           pAnaliticKind1,
                           pAnalitic1,
                           pAnaliticKind2,
                           pAnalitic2,
                           pAnaliticKind3,
                           pAnalitic3,
                           pAnaliticKind4,
                           pAnalitic4,
                           pAnaliticKind5,
                           pAnalitic5,
                           pAnaliticKind6,
                           pAnalitic6,
                           pComment,
                           pDocID,
                           pStep,
                           pNoCalcNOB,
                           v_ObjID,
                           pFromOutSyst,
                           pOutSystCode,
                           pOutObjID,
                           pSourceObjID,
                           pTechnical,
                           pConvDate,
                           pTransfDate,
                           pTransfKind,
                           pHolding_Period,
                           pChangeCode
                         );
   END; -- InsertTaxObject

   /**
   @brief Выполняет вставку объекта, вызывается в макросах шагов, с проверкой дублирования
   @param [in] pDate Дата создания объекта НДР
   @param [in] pClient ID клиента
   @param [in] pDirection Направление
   @param [in] pLevel Уровень
   @param [in] pUser Признак "Пользовательский"
   @param [in] pKind Вид объекта НДР
   @param [in] pSum Сумма объекта НДР
   @param [in] pCur Валюта объекта НДР
   @param [in] pAnaliticKind1 Вид аналитики 1
   @param [in] pAnalitic1 Значение аналитики 1
   @param [in] pAnaliticKind2 Вид аналитики 2
   @param [in] pAnalitic2 Значение аналитики 2
   @param [in] pAnaliticKind3 Вид аналитики 3
   @param [in] pAnalitic3 Значение аналитики 3
   @param [in] pAnaliticKind4 Вид аналитики 4
   @param [in] pAnalitic4 Значение аналитики 4
   @param [in] pAnaliticKind5 Вид аналитики 5
   @param [in] pAnalitic5 Значение аналитики 5
   @param [in] pAnaliticKind6 Вид аналитики 6
   @param [in] pAnalitic6 Значение аналитики 6
   @param [in] pComment Комментарий
   @param [in] pDocID ID текущей операции
   @param [in] pStep ID шага текущей операции
   @param [in] pNoCalcNOB Не расчитывать НОБ
   @param [out] pObjID ID объекта НДР
   @param [in] pFromOutSyst Признак "Из внешней системы"
   @param [in] pOutSystCode Код внешней системы
   @param [in] pOutObjID ID во внешней системе
   @param [in] pSourceObjID ID первоначального объекта
   @param [in] pTechnical Признак "Технический"
   @param [in] pTransfDate Дата трансформации ИИС в ИИС-3
   @param [in] pTransfKind Период расчета
   */
   PROCEDURE InsertTaxObjectWD(
                                pDate IN DATE,
                                pClient IN NUMBER,
                                pDirection IN NUMBER,
                                pLevel IN NUMBER,
                                pUser IN CHAR,
                                pKind IN NUMBER,
                                pSum IN NUMBER,
                                pCur IN NUMBER,
                                pAnaliticKind1 IN NUMBER,
                                pAnalitic1 IN NUMBER,
                                pAnaliticKind2 IN NUMBER,
                                pAnalitic2 IN NUMBER,
                                pAnaliticKind3 IN NUMBER,
                                pAnalitic3 IN NUMBER,
                                pAnaliticKind4 IN NUMBER,
                                pAnalitic4 IN NUMBER,
                                pAnaliticKind5 IN NUMBER,
                                pAnalitic5 IN NUMBER,
                                pAnaliticKind6 IN NUMBER,
                                pAnalitic6 IN NUMBER,
                                pComment IN VARCHAR2,
                                pDocID IN NUMBER,
                                pStep IN NUMBER,
                                pFromOutSyst IN CHAR DEFAULT CHR(0),
                                pOutSystCode IN VARCHAR2 DEFAULT CHR(1),
                                pOutObjID IN VARCHAR2 DEFAULT CHR(1),
                                pSourceObjID IN NUMBER DEFAULT 0,
                                pTechnical IN CHAR DEFAULT CHR(0),
                                pConvDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                pTransfDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                pTransfKind IN NUMBER DEFAULT 0,
                                pHolding_Period IN NUMBER DEFAULT 0,
                                pChangeCode IN CHAR DEFAULT CHR(0)
                              )
   IS
      v_Obj dnptxobj_dbt%ROWTYPE;
   BEGIN
      IF (pSum <> 0) THEN
         IF pUser = 'X' THEN
            -- Запрещено создавать пользовательский объект НДР
            SetError( RSI_NPTXC.NPTX_ERROR_20639,'');
         ELSE
            BEGIN
              SELECT *
                INTO v_Obj
                FROM dnptxobj_dbt
               WHERE t_Date = pDate
                 AND t_Client = pClient
                 AND t_Direction = pDirection
                 AND t_Level = pLevel
                 AND t_Kind = pKind
                 AND (pAnaliticKind1 =  0 OR t_AnaliticKind1 = pAnaliticKind1)
                 AND (pAnalitic1     = -1 OR t_Analitic1     = pAnalitic1    )
                 AND (pAnaliticKind2 =  0 OR t_AnaliticKind2 = pAnaliticKind2)
                 AND (pAnalitic2     = -1 OR t_Analitic2     = pAnalitic2    )
                 AND (pAnaliticKind3 =  0 OR t_AnaliticKind3 = pAnaliticKind3)
                 AND (pAnalitic3     = -1 OR t_Analitic3     = pAnalitic3    )
                 AND (pAnaliticKind4 =  0 OR t_AnaliticKind4 = pAnaliticKind4)
                 AND (pAnalitic4     = -1 OR t_Analitic4     = pAnalitic4    )
                 AND (pAnaliticKind5 =  0 OR t_AnaliticKind5 = pAnaliticKind5)
                 AND (pAnalitic5     = -1 OR t_Analitic5     = pAnalitic5    )
                 AND (pAnaliticKind6 =  0 OR t_AnaliticKind6 = pAnaliticKind6)
                 AND (pAnalitic6     = -1 OR t_Analitic6     = pAnalitic6    );

            IF ( (v_Obj.t_Sum  <> pSum) OR
                 (v_Obj.t_Cur  <> pCur)
               ) THEN
               RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_WARN, 'Уже существует объект НДР t_ObjID = '||v_Obj.t_ObjID||' с данными параметрами и другой суммой или валютой' );
            ELSE

               INSERT INTO DNPTXOBDC_DBT (
                                          T_DOCID    ,  -- T_DOCID
                                          T_STEP     ,  -- T_STEP
                                          T_OBJID       -- T_OBJID
                                         )
                                  VALUES (
                                          pDocID,       -- T_DOCID
                                          pStep,        -- T_STEP
                                          v_Obj.t_ObjID -- T_OBJID
                                         );
            END IF;

            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                BEGIN
                   InsertTaxObject( pDate,
                                    pClient,
                                    pDirection,
                                    pLevel,
                                    pUser,
                                    pKind,
                                    pSum,
                                    pCur,
                                    pAnaliticKind1,
                                    pAnalitic1,
                                    pAnaliticKind2,
                                    pAnalitic2,
                                    pAnaliticKind3,
                                    pAnalitic3,
                                    pAnaliticKind4,
                                    pAnalitic4,
                                    pAnaliticKind5,
                                    pAnalitic5,
                                    pAnaliticKind6,
                                    pAnalitic6,
                                    pComment,
                                    pDocID,
                                    pStep,
                                    0,
                                    pFromOutSyst,
                                    pOutSystCode,
                                    pOutObjID,
                                    pSourceObjID,
                                    pTechnical,
                                    pConvDate,
                                    pTransfDate,
                                    pTransfKind,
                                    pHolding_Period,
                                    pChangeCode
                                  );
                END;
            END;
         END IF;
      END IF;
   END; -- InsertTaxObject

   --Выполняет запуск вставки объектов НДР на шаге по временной таблице
   PROCEDURE StartInsertTaxObject( pDocID IN NUMBER,
                                   pStep  IN NUMBER
                                 )
   IS
      pPayID NUMBER := 0;
      v_ObjID NUMBER;
      CURRECNUM NUMBER := 0;
      v_BCID NUMBER;
      v_ID_Operation NUMBER := 0;
      v_i NUMBER := 0;  
   BEGIN
     
      --удаление
      FOR CurObj IN (SELECT OBJ.*
                       FROM DNPTXOBJ_TMP TMP, DNPTXOBJ_DBT OBJ
                      WHERE TMP.T_DOCID = pDocID
                        AND TMP.T_STEP  = pStep
                        AND TMP.T_ACTION = RSI_NPTXC.NPTXBC_ACTION_DELETE
                        AND OBJ.T_OBJID = TMP.T_REALOBJID
                      ORDER BY TMP.T_REALOBJID
                    ) 
      LOOP
        IF v_i = 0 THEN
          SELECT op.t_ID_Operation INTO v_ID_Operation
            FROM dnptxop_dbt nptxop, doproper_dbt op
           WHERE nptxop.t_ID = pDocID
             AND op.t_DocKind = nptxop.t_DocKind
             AND op.t_DocumentID = LPAD(nptxop.t_ID, 34, '0');
        END IF;

        v_i := v_i + 1;

        INSERT INTO DNPTXOBJBC_DBT (
                                       T_DATE,
                                       T_CLIENT,
                                       T_DIRECTION,
                                       T_LEVEL,
                                       T_KIND,
                                       T_SUM,
                                       T_CUR,
                                       T_SUM0,
                                       T_ANALITICKIND1,
                                       T_ANALITIC1,
                                       T_ANALITICKIND2,
                                       T_ANALITIC2,
                                       T_ANALITICKIND3,
                                       T_ANALITIC3,
                                       T_ANALITICKIND4,
                                       T_ANALITIC4,
                                       T_ANALITICKIND5,
                                       T_ANALITIC5,
                                       T_ANALITICKIND6,
                                       T_ANALITIC6,
                                       T_COMMENT,
                                       T_DOCID,
                                       T_FROMOUTSYST,
                                       T_OUTSYSTCODE,
                                       T_OUTOBJID,
                                       T_SOURCEOBJID,
                                       T_TECHNICAL,
                                       T_TAXPERIOD,
                                       T_TRANSFDATE,
                                       T_TRANSFKIND,
                                       T_HOLDING_PERIOD,
                                       T_CHANGECODE
                                    )
                             VALUES (
                                       CurObj.T_DATE         , -- T_DATE
                                       CurObj.T_CLIENT       , -- T_CLIENT
                                       CurObj.T_DIRECTION    , -- T_DIRECTION
                                       CurObj.T_LEVEL        , -- T_LEVEL
                                       CurObj.T_KIND         , -- T_KIND
                                       CurObj.T_SUM          , -- T_SUM
                                       CurObj.T_CUR          , -- T_CUR
                                       CurObj.T_SUM0         , -- T_SUM0
                                       CurObj.T_ANALITICKIND1, -- T_ANALITICKIND1
                                       CurObj.T_ANALITIC1    , -- T_ANALITIC1
                                       CurObj.T_ANALITICKIND2, -- T_ANALITICKIND2
                                       CurObj.T_ANALITIC2    , -- T_ANALITIC2
                                       CurObj.T_ANALITICKIND3, -- T_ANALITICKIND3
                                       CurObj.T_ANALITIC3    , -- T_ANALITIC3
                                       CurObj.T_ANALITICKIND4, -- T_ANALITICKIND4
                                       CurObj.T_ANALITIC4    , -- T_ANALITIC4
                                       CurObj.T_ANALITICKIND5, -- T_ANALITICKIND5
                                       CurObj.T_ANALITIC5    , -- T_ANALITIC5
                                       CurObj.T_ANALITICKIND6, -- T_ANALITICKIND6
                                       CurObj.T_ANALITIC6    , -- T_ANALITIC6
                                       CurObj.T_COMMENT      , -- T_COMMENT
                                       pDocID             ,
                                       CurObj.T_FROMOUTSYST  ,
                                       CurObj.T_OUTSYSTCODE  ,
                                       CurObj.T_OUTOBJID     ,
                                       CurObj.T_SOURCEOBJID  ,
                                       CurObj.T_TECHNICAL    ,
                                       CurObj.T_TAXPERIOD    ,
                                       CurObj.T_TRANSFDATE   ,
                                       CurObj.T_TRANSFKIND   ,
                                       CurObj.T_HOLDING_PERIOD,
                                       CurObj.T_CHANGECODE
                                    ) RETURNING t_ObjID INTO v_BCID;

         INSERT INTO DNPTXBC_DBT (T_BCID, T_OBJKIND, T_OBJID, T_ID_OPERATION, T_ID_STEP, T_ACTION, T_BACKOBJID)
         VALUES(0,
                RSI_NPTXC.NPTXBC_OBJKIND_OBJ,
                CurObj.t_ObjID,
                v_ID_Operation,
                pStep,
                RSI_NPTXC.NPTXBC_ACTION_DELETE,
                v_BCID
               ); 
         
         FOR CurDC IN (SELECT * FROM DNPTXOBDC_DBT OBDC WHERE OBDC.t_ObjID = CurObj.t_ObjID)
         LOOP
            INSERT INTO DNPTXOBDCBC_DBT (
                                           T_DOCID,
                                           T_STEP,
                                           T_OBJID
                                        )
                                 VALUES (
                                           CurDC.T_DOCID,
                                           CurDC.T_STEP,
                                           v_BCID
                                        );
         END LOOP;
         
         DELETE FROM DNPTXOBDC_DBT OBDC WHERE OBDC.t_ObjID = CurObj.t_ObjID;
         DELETE FROM DNPTXOBJ_DBT OBJ WHERE OBJ.t_ObjID = CurObj.t_ObjID;
      END LOOP;

      DELETE 
        FROM DNPTXOBJ_TMP
       WHERE T_DOCID = pDocID
         AND T_STEP  = pStep
         AND T_ACTION = RSI_NPTXC.NPTXBC_ACTION_DELETE;
      
      --обновление
      v_i := 0;
      FOR CurObj IN (SELECT OBJ.*,
                            TMP.T_ANALITICKIND2  TMP_ANALITICKIND2,
                            TMP.T_ANALITIC2      TMP_ANALITIC2,
                            TMP.T_ANALITICKIND6  TMP_ANALITICKIND6,
                            TMP.T_ANALITIC6      TMP_ANALITIC6   
                       FROM DNPTXOBJ_TMP TMP, DNPTXOBJ_DBT OBJ
                      WHERE TMP.T_DOCID = pDocID
                        AND TMP.T_STEP  = pStep
                        AND TMP.T_ACTION = RSI_NPTXC.NPTXBC_ACTION_UPDATE
                        AND OBJ.T_OBJID = TMP.T_REALOBJID
                      ORDER BY TMP.T_REALOBJID
                    ) 
      LOOP
        IF v_i = 0 THEN
          SELECT op.t_ID_Operation INTO v_ID_Operation
            FROM dnptxop_dbt nptxop, doproper_dbt op
           WHERE nptxop.t_ID = pDocID
             AND op.t_DocKind = nptxop.t_DocKind
             AND op.t_DocumentID = LPAD(nptxop.t_ID, 34, '0');
        END IF;
        
        v_i := v_i + 1;
        
        INSERT INTO DNPTXOBJBC_DBT (
                                       T_DATE,
                                       T_CLIENT,
                                       T_DIRECTION,
                                       T_LEVEL,
                                       T_KIND,
                                       T_SUM,
                                       T_CUR,
                                       T_SUM0,
                                       T_ANALITICKIND1,
                                       T_ANALITIC1,
                                       T_ANALITICKIND2,
                                       T_ANALITIC2,
                                       T_ANALITICKIND3,
                                       T_ANALITIC3,
                                       T_ANALITICKIND4,
                                       T_ANALITIC4,
                                       T_ANALITICKIND5,
                                       T_ANALITIC5,
                                       T_ANALITICKIND6,
                                       T_ANALITIC6,
                                       T_COMMENT,
                                       T_DOCID,
                                       T_FROMOUTSYST,
                                       T_OUTSYSTCODE,
                                       T_OUTOBJID,
                                       T_SOURCEOBJID,
                                       T_TECHNICAL,
                                       T_TAXPERIOD,
                                       T_TRANSFDATE,
                                       T_TRANSFKIND,
                                       T_HOLDING_PERIOD,
                                       T_CHANGECODE
                                    )
                             VALUES (
                                       CurObj.T_DATE         , -- T_DATE
                                       CurObj.T_CLIENT       , -- T_CLIENT
                                       CurObj.T_DIRECTION    , -- T_DIRECTION
                                       CurObj.T_LEVEL        , -- T_LEVEL
                                       CurObj.T_KIND         , -- T_KIND
                                       CurObj.T_SUM          , -- T_SUM
                                       CurObj.T_CUR          , -- T_CUR
                                       CurObj.T_SUM0         , -- T_SUM0
                                       CurObj.T_ANALITICKIND1, -- T_ANALITICKIND1
                                       CurObj.T_ANALITIC1    , -- T_ANALITIC1
                                       CurObj.T_ANALITICKIND2, -- T_ANALITICKIND2
                                       CurObj.T_ANALITIC2    , -- T_ANALITIC2
                                       CurObj.T_ANALITICKIND3, -- T_ANALITICKIND3
                                       CurObj.T_ANALITIC3    , -- T_ANALITIC3
                                       CurObj.T_ANALITICKIND4, -- T_ANALITICKIND4
                                       CurObj.T_ANALITIC4    , -- T_ANALITIC4
                                       CurObj.T_ANALITICKIND5, -- T_ANALITICKIND5
                                       CurObj.T_ANALITIC5    , -- T_ANALITIC5
                                       CurObj.T_ANALITICKIND6, -- T_ANALITICKIND6
                                       CurObj.T_ANALITIC6    , -- T_ANALITIC6
                                       CurObj.T_COMMENT      , -- T_COMMENT
                                       pDocID             ,
                                       CurObj.T_FROMOUTSYST  ,
                                       CurObj.T_OUTSYSTCODE  ,
                                       CurObj.T_OUTOBJID     ,
                                       CurObj.T_SOURCEOBJID  ,
                                       CurObj.T_TECHNICAL    ,
                                       CurObj.T_TAXPERIOD    ,
                                       CurObj.T_TRANSFDATE   ,
                                       CurObj.T_TRANSFKIND   ,
                                       CurObj.T_HOLDING_PERIOD,
                                       CurObj.T_CHANGECODE
                                    ) RETURNING t_ObjID INTO v_BCID;

         INSERT INTO DNPTXBC_DBT (T_BCID, T_OBJKIND, T_OBJID, T_ID_OPERATION, T_ID_STEP, T_ACTION, T_BACKOBJID)
         VALUES(0,
                RSI_NPTXC.NPTXBC_OBJKIND_OBJ,
                CurObj.t_ObjID,
                v_ID_Operation,
                pStep,
                RSI_NPTXC.NPTXBC_ACTION_UPDATE,
                v_BCID
               ); 
         
         UPDATE DNPTXOBJ_DBT
            SET T_ANALITICKIND2  = CurObj.TMP_ANALITICKIND2, -- T_ANALITICKIND2
                T_ANALITIC2      = CurObj.TMP_ANALITIC2,      -- T_ANALITIC2    
                T_ANALITICKIND6  = CurObj.TMP_ANALITICKIND6,
                T_ANALITIC6      = CurObj.TMP_ANALITIC6      
          WHERE T_OBJID = CurObj.t_ObjID;
      END LOOP;

      DELETE 
        FROM DNPTXOBJ_TMP
       WHERE T_DOCID = pDocID
         AND T_STEP  = pStep
         AND T_ACTION = RSI_NPTXC.NPTXBC_ACTION_UPDATE;


      
      --вставка
      FOR Obj IN (SELECT *
                    FROM DNPTXOBJ_TMP
                   WHERE T_DOCID = pDocID
                     AND T_STEP  = pStep
                   ORDER BY T_ID
                 ) 
      LOOP
        
        IF (Obj.T_REESTRSUM <> 0 AND CURRECNUM = 0) THEN
            INSERT INTO DNPTXPAY_DBT (
                                      T_PARTYID,
                                      T_SUMHOLD,
                                      T_DATEHOLD,
                                      T_DATEPAY,
                                      T_ISCUSTOM
                                     )
                                     VALUES (
                                             Obj.T_CLIENT,
                                             Obj.T_REESTRSUM,
                                             Obj.T_DATE,
                                             Obj.T_DATE,
                                             CHR(0)
                                            ) RETURNING t_ID INTO pPayID;
            CURRECNUM := 1;
         END IF;
      
      
         IF Obj.T_FUNCTIONKIND = RSI_NPTXC.DL_INSERTTAXOBJECTWD THEN
            InsertTaxObjectWD( Obj.T_DATE         ,
                               Obj.T_CLIENT       ,
                               Obj.T_DIRECTION    ,
                               Obj.T_LEVEL        ,
                               Obj.T_USER         ,
                               Obj.T_KIND         ,
                               Obj.T_SUM          ,
                               Obj.T_CUR          ,
                               Obj.T_ANALITICKIND1,
                               Obj.T_ANALITIC1    ,
                               Obj.T_ANALITICKIND2,
                               Obj.T_ANALITIC2    ,
                               Obj.T_ANALITICKIND3,
                               Obj.T_ANALITIC3    ,
                               Obj.T_ANALITICKIND4,
                               Obj.T_ANALITIC4    ,
                               Obj.T_ANALITICKIND5,
                               Obj.T_ANALITIC5    ,
                               Obj.T_ANALITICKIND6,
                               Obj.T_ANALITIC6    ,
                               Obj.T_COMMENT      ,
                               Obj.T_DOCID        ,
                               Obj.T_STEP         ,
                               Obj.T_FROMOUTSYST  ,
                               Obj.T_OUTSYSTCODE  ,
                               Obj.T_OUTOBJID     ,
                               Obj.T_SOURCEOBJID  ,
                               Obj.T_TECHNICAL    ,
                               Obj.T_CONVDATE     ,
                               Obj.T_TRANSFDATE   ,
                               Obj.T_TRANSFKIND   ,
                               Obj.T_HOLDING_PERIOD,
                               Obj.T_CHANGECODE
                              );
         ELSE
            InsertTaxObjectRetID ( Obj.T_DATE         ,
                                   Obj.T_CLIENT       ,
                                   Obj.T_DIRECTION    ,
                                   Obj.T_LEVEL        ,
                                   Obj.T_USER         ,
                                   Obj.T_KIND         ,
                                   Obj.T_SUM          ,
                                   Obj.T_CUR          ,
                                   Obj.T_ANALITICKIND1,
                                   Obj.T_ANALITIC1    ,
                                   Obj.T_ANALITICKIND2,
                                   Obj.T_ANALITIC2    ,
                                   Obj.T_ANALITICKIND3,
                                   Obj.T_ANALITIC3    ,
                                   Obj.T_ANALITICKIND4,
                                   Obj.T_ANALITIC4    ,
                                   Obj.T_ANALITICKIND5,
                                   Obj.T_ANALITIC5    ,
                                   Obj.T_ANALITICKIND6,
                                   Obj.T_ANALITIC6    ,
                                   Obj.T_COMMENT      ,
                                   Obj.T_DOCID        ,
                                   Obj.T_STEP         ,
                                   0                  ,
                                   v_ObjID            ,
                                   Obj.T_FROMOUTSYST  ,
                                   Obj.T_OUTSYSTCODE  ,
                                   Obj.T_OUTOBJID     ,
                                   Obj.T_SOURCEOBJID  ,
                                   Obj.T_TECHNICAL    ,
                                   Obj.T_CONVDATE     ,
                                   Obj.T_TRANSFDATE   ,
                                   Obj.T_TRANSFKIND   ,
                                   Obj.T_HOLDING_PERIOD,
                                   Obj.T_CHANGECODE
                                );
         END IF;
         
         IF (pPayID > 0 AND Obj.T_REESTRSUM <> 0) THEN
             INSERT INTO DNPTXPAYOBJ_DBT (
                                          T_NPTXPAYID,
                                          T_OBJID,
                                          T_ID_OPERATION,
                                          T_ID_STEP
                                         )
                                  VALUES (
                                           pPayID,
                                           v_ObjID,
                                           Obj.T_DOCID,
                                           Obj.T_STEP
                              );
         END IF;
      END LOOP;

      DELETE FROM DNPTXOBJ_TMP;

   END; -- StartInsertTaxObject

   --Выполняет откат вставки объекта
   PROCEDURE RecoilInsertTaxObject( pDocID IN NUMBER,
                                    pStep  IN NUMBER,
                                    pNoCalcNOB IN NUMBER DEFAULT 0
                                  )
   IS
      v_Count NUMBER;
      TYPE VarCur IS REF CURSOR RETURN DNPTXOBDC_DBT%ROWTYPE;
      cObj VarCur;
      Obj DNPTXOBDC_DBT%ROWTYPE;
      v_ObjID NUMBER;
   BEGIN

      --Сначала восстановим удаленные объекты
     FOR BC IN (SELECT OBJBC.*, XBC.T_BCID as XBC_BCID 
                    FROM dnptxop_dbt nptxop, doproper_dbt op, DNPTXBC_DBT XBC, DNPTXOBJBC_DBT OBJBC 
                   WHERE nptxop.t_ID = pDocID
                     AND op.t_DocKind = nptxop.t_DocKind
                     AND op.t_DocumentID = LPAD(nptxop.t_ID, 34, '0')
                     AND XBC.T_ID_Operation = op.t_ID_Operation 
                     AND XBC.T_ID_STEP = pStep 
                     AND XBC.T_OBJKIND = RSI_NPTXC.NPTXBC_OBJKIND_OBJ
                     AND XBC.T_ACTION = RSI_NPTXC.NPTXBC_ACTION_DELETE
                     AND OBJBC.T_ObjID = XBC.T_BackObjID)
      LOOP


         INSERT INTO DNPTXOBJ_DBT ( 
                                   T_DATE         ,     -- T_DATE 
                                   T_CLIENT       ,     -- T_CLIENT 
                                   T_DIRECTION    ,     -- T_DIRECTION 
                                   T_LEVEL        ,     -- T_LEVEL 
                                   T_USER         ,     -- T_USER 
                                   T_KIND         ,     -- T_KIND 
                                   T_SUM          ,     -- T_SUM 
                                   T_CUR          ,     -- T_CUR 
                                   T_SUM0         ,     -- T_SUM0 
                                   T_ANALITICKIND1,     -- T_ANALITICKIND1 
                                   T_ANALITIC1    ,     -- T_ANALITIC1 
                                   T_ANALITICKIND2,     -- T_ANALITICKIND2 
                                   T_ANALITIC2    ,     -- T_ANALITIC2 
                                   T_ANALITICKIND3,     -- T_ANALITICKIND3 
                                   T_ANALITIC3    ,     -- T_ANALITIC3 
                                   T_ANALITICKIND4,     -- T_ANALITICKIND4 
                                   T_ANALITIC4    ,     -- T_ANALITIC4 
                                   T_ANALITICKIND5,     -- T_ANALITICKIND5 
                                   T_ANALITIC5    ,     -- T_ANALITIC5 
                                   T_ANALITICKIND6,     -- T_ANALITICKIND6 
                                   T_ANALITIC6    ,     -- T_ANALITIC6 
                                   T_COMMENT      ,     -- T_COMMENT
                                   T_TECHNICAL    ,     -- T_TECHNICAL 
                                   T_TAXPERIOD    ,
                                   T_OUTSYSTCODE  ,
                                   T_TRANSFDATE   ,
                                   T_TRANSFKIND   ,
                                   T_HOLDING_PERIOD,
                                   T_CHANGECODE
                                  ) 
                           VALUES ( 
                                   BC.t_Date,           -- T_DATE 
                                   BC.t_Client,         -- T_CLIENT 
                                   BC.t_Direction,      -- T_DIRECTION 
                                   BC.t_Level,          -- T_LEVEL 
                                   chr(0),              -- T_USER 
                                   BC.t_Kind,           -- T_KIND 
                                   BC.t_Sum,            -- T_SUM 
                                   BC.t_Cur,            -- T_CUR 
                                   BC.t_SUM0,           -- T_SUM0 
                                   BC.t_AnaliticKind1,  -- T_ANALITICKIND1 
                                   BC.t_Analitic1,      -- T_ANALITIC1 
                                   BC.t_AnaliticKind2,  -- T_ANALITICKIND2 
                                   BC.t_Analitic2,      -- T_ANALITIC2 
                                   BC.t_AnaliticKind3,  -- T_ANALITICKIND3 
                                   BC.t_Analitic3,      -- T_ANALITIC3 
                                   BC.t_AnaliticKind4,  -- T_ANALITICKIND4 
                                   BC.t_Analitic4,      -- T_ANALITIC4 
                                   BC.t_AnaliticKind5,  -- T_ANALITICKIND5 
                                   BC.t_Analitic5,      -- T_ANALITIC5 
                                   BC.t_AnaliticKind6,  -- T_ANALITICKIND6 
                                   BC.t_Analitic6,      -- T_ANALITIC6 
                                   BC.t_Comment,        -- T_COMMENT
                                   BC.t_Technical,      -- T_TECHNICAL 
                                   BC.t_TaxPeriod,
                                   BC.t_OutSystCode,
                                   BC.T_TransfDate,
                                   BC.T_TransfKind,
                                   BC.T_HOLDING_PERIOD,
                                   BC.T_CHANGECODE
                                  ) RETURNING t_ObjID INTO v_ObjID; 

            INSERT INTO DNPTXOBDC_DBT ( 
                                       T_DOCID, 
                                       T_STEP, 
                                       T_OBJID 
                                      ) 
                               select  T_DOCID, 
                                       T_STEP, 
                                       v_ObjID 
                                       from DNPTXOBDCBC_DBT DC WHERE DC.T_ObjID = BC.t_ObjID ; 


         DELETE FROM DNPTXOBDCBC_DBT DC WHERE DC.T_ObjID = BC.t_ObjID; 
         DELETE FROM DNPTXOBJBC_DBT WHERE T_ObjID = BC.t_ObjID;
         DELETE FROM DNPTXBC_DBT WHERE T_BCID = BC.XBC_BCID; 

      END LOOP;

      --Восстановим обновленные объекты
      FOR BC IN (SELECT OBJBC.*, XBC.T_BCID as XBC_BCID, XBC.T_OBJID as RealObjID 
                    FROM dnptxop_dbt nptxop, doproper_dbt op, DNPTXBC_DBT XBC, DNPTXOBJBC_DBT OBJBC 
                   WHERE nptxop.t_ID = pDocID
                     AND op.t_DocKind = nptxop.t_DocKind
                     AND op.t_DocumentID = LPAD(nptxop.t_ID, 34, '0')
                     AND XBC.T_ID_Operation = op.t_ID_Operation 
                     AND XBC.T_ID_STEP = pStep 
                     AND XBC.T_OBJKIND = RSI_NPTXC.NPTXBC_OBJKIND_OBJ
                     AND XBC.T_ACTION = RSI_NPTXC.NPTXBC_ACTION_UPDATE
                     AND OBJBC.T_ObjID = XBC.T_BackObjID)
      LOOP


         UPDATE DNPTXOBJ_DBT 
            SET T_ANALITICKIND2 =  BC.t_AnaliticKind2,  -- T_ANALITICKIND2
                T_ANALITIC2     =  BC.t_Analitic2,      -- T_ANALITIC2  
                T_ANALITICKIND6 =  BC.t_AnaliticKind6,
                T_ANALITIC6     =  BC.t_Analitic6  
           WHERE T_OBJID = BC.RealObjID;
 

         DELETE FROM DNPTXBC_DBT WHERE T_BCID = BC.XBC_BCID;
         DELETE FROM DNPTXOBJBC_DBT WHERE T_ObjID = BC.t_ObjID; 

      END LOOP;



      IF( pNoCalcNOB != 0 ) THEN
         OPEN cObj FOR
         SELECT N.*
           FROM DNPTXOBDC_DBT N
          WHERE N.T_DOCID = pDocID
            AND N.T_STEP  = pStep
            AND EXISTS ( SELECT txobj.t_OBJID
                           FROM DNPTXOBJ_DBT txobj
                          WHERE txobj.t_OBJID = N.t_OBJID
                            AND txobj.t_AnaliticKind1 in(0, 1020, 1095, 1098, 1115, 1120, 1125, 1130)
                       );
      ELSE
         OPEN cObj FOR
         SELECT N.*
           FROM DNPTXOBDC_DBT N
          WHERE N.T_DOCID = pDocID
            AND N.T_STEP  = pStep
            AND EXISTS ( SELECT txobj.t_OBJID
                           FROM DNPTXOBJ_DBT txobj
                          WHERE txobj.t_OBJID = N.t_OBJID
                            AND txobj.t_AnaliticKind1 not in(1095, 1098, 1115, 1120, 1125, 1130)
                       );
      END IF;

      LOOP
         FETCH cObj INTO Obj;
         EXIT WHEN cObj%NOTFOUND;

         DELETE FROM DNPTXOBDC_DBT
               WHERE T_DOCID = Obj.t_DocID
                 AND T_STEP  = Obj.t_Step
                 AND T_OBJID = Obj.t_ObjID;

         Select count(1)
           Into v_Count
           From DNPTXOBDC_DBT
          Where T_OBJID = Obj.t_ObjID;

         IF v_Count = 0 THEN
            DELETE FROM DNPTXOBJ_DBT
                  WHERE T_OBJID = Obj.t_ObjID;
         END IF;

         DELETE FROM DNPTXPAY_DBT -- Мз DNPTXPAYOBJ_DBT запись удалит триггер
               WHERE T_ID = (SELECT T_NPTXPAYID
                               FROM DNPTXPAYOBJ_DBT
                              WHERE T_OBJID = Obj.t_ObjID); 
      END LOOP;   
      CLOSE cObj;

   END; -- RecoilInsertTaxObject


   --Выполняет вставку объекта и вставку в реестр
   PROCEDURE InsertTaxObjectTaxPay(
                                  pDate IN DATE,
                                  pClient IN NUMBER,
                                  pDirection IN NUMBER,
                                  pLevel IN NUMBER,
                                  pUser IN CHAR,
                                  pKind IN NUMBER,
                                  pSum IN NUMBER,
                                  pCur IN NUMBER,
                                  pAnaliticKind1 IN NUMBER,
                                  pAnalitic1 IN NUMBER,
                                  pAnaliticKind2 IN NUMBER,
                                  pAnalitic2 IN NUMBER,
                                  pAnaliticKind3 IN NUMBER,
                                  pAnalitic3 IN NUMBER,
                                  pAnaliticKind4 IN NUMBER,
                                  pAnalitic4 IN NUMBER,
                                  pAnaliticKind5 IN NUMBER,
                                  pAnalitic5 IN NUMBER,
                                  pAnaliticKind6 IN NUMBER,
                                  pAnalitic6 IN NUMBER,
                                  pComment IN VARCHAR2,
                                  pDocID IN NUMBER,
                                  pStep IN NUMBER,
                                  pNoCalcNOB IN NUMBER,
                                  pFromOutSyst IN CHAR DEFAULT CHR(0),
                                  pOutSystCode IN VARCHAR2 DEFAULT CHR(1),
                                  pOutObjID IN VARCHAR2 DEFAULT CHR(1),
                                  pSourceObjID IN NUMBER DEFAULT 0,
                                  pConvDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                  pTransfDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                  pTransfKind IN NUMBER DEFAULT 0,
                                  pHolding_Period IN NUMBER DEFAULT 0,
                                  pChangeCode  IN CHAR DEFAULT CHR(0)
                                )
   IS
      pObjID NUMBER;
      pPayID NUMBER;
      v_ConvDate DATE;
      v_TaxPeriod NUMBER;
   BEGIN
      v_TaxPeriod := 0;

      v_ConvDate := pDate;
      IF pConvDate > TO_DATE('01.01.0001','DD.MM.YYYY') THEN
        v_ConvDate := pConvDate;
      END IF;

      IF (pSum <> 0) THEN
         IF pLevel = 8 THEN
           v_TaxPeriod := EXTRACT(YEAR FROM pDate);
           IF pDocID > 0 THEN
             BEGIN

               SELECT EXTRACT(YEAR FROM T_PREVDATE)
                 INTO v_TaxPeriod
                 FROM DNPTXOP_DBT
                WHERE T_ID = pDocID
                  AND T_DOCKIND = RSB_SECUR.DL_HOLDNDFL;

               EXCEPTION
                    WHEN NO_DATA_FOUND THEN v_TaxPeriod := EXTRACT(YEAR FROM pDate);
             END;
           END IF;
         END IF;

         INSERT INTO DNPTXOBJ_DBT (
                                   T_DATE         ,     -- T_DATE
                                   T_CLIENT       ,     -- T_CLIENT
                                   T_DIRECTION    ,     -- T_DIRECTION
                                   T_LEVEL        ,     -- T_LEVEL
                                   T_USER         ,     -- T_USER
                                   T_KIND         ,     -- T_KIND
                                   T_SUM          ,     -- T_SUM
                                   T_CUR          ,     -- T_CUR
                                   T_SUM0         ,     -- T_SUM0
                                   T_ANALITICKIND1,     -- T_ANALITICKIND1
                                   T_ANALITIC1    ,     -- T_ANALITIC1
                                   T_ANALITICKIND2,     -- T_ANALITICKIND2
                                   T_ANALITIC2    ,     -- T_ANALITIC2
                                   T_ANALITICKIND3,     -- T_ANALITICKIND3
                                   T_ANALITIC3    ,     -- T_ANALITIC3
                                   T_ANALITICKIND4,     -- T_ANALITICKIND4
                                   T_ANALITIC4    ,     -- T_ANALITIC4
                                   T_ANALITICKIND5,     -- T_ANALITICKIND5
                                   T_ANALITIC5    ,     -- T_ANALITIC5
                                   T_ANALITICKIND6,     -- T_ANALITICKIND6
                                   T_ANALITIC6    ,     -- T_ANALITIC6
                                   T_COMMENT      ,     -- T_COMMENT
                                   T_FROMOUTSYST  ,
                                   T_OUTSYSTCODE  ,
                                   T_OUTOBJID     ,
                                   T_SOURCEOBJID  ,
                                   T_TECHNICAL    ,
                                   T_TAXPERIOD    ,
                                   T_TRANSFDATE   ,
                                   T_TRANSFKIND   ,
                                   T_HOLDING_PERIOD,
                                   T_CHANGECODE
                                  )
                           VALUES (
                                   pDate,                                               -- T_DATE
                                   pClient,                                             -- T_CLIENT
                                   pDirection,                                          -- T_DIRECTION
                                   pLevel,                                              -- T_LEVEL
                                   pUser,                                               -- T_USER
                                   pKind,                                               -- T_KIND
                                   pSum,                                                -- T_SUM
                                   pCur,                                                -- T_CUR
                                   RSI_RSB_FIInstr.ConvSum( pSum, pCur, 0, v_ConvDate, 1 ),  -- T_SUM0
                                   pAnaliticKind1,                                      -- T_ANALITICKIND1
                                   pAnalitic1,                                          -- T_ANALITIC1
                                   pAnaliticKind2,                                      -- T_ANALITICKIND2
                                   pAnalitic2,                                          -- T_ANALITIC2
                                   pAnaliticKind3,                                      -- T_ANALITICKIND3
                                   pAnalitic3,                                          -- T_ANALITIC3
                                   pAnaliticKind4,                                      -- T_ANALITICKIND4
                                   pAnalitic4,                                          -- T_ANALITIC4
                                   pAnaliticKind5,                                      -- T_ANALITICKIND5
                                   pAnalitic5,                                          -- T_ANALITIC5
                                   pAnaliticKind6,                                      -- T_ANALITICKIND6
                                   pAnalitic6,                                          -- T_ANALITIC6
                                   pComment,                                            -- T_COMMENT
                                   pFromOutSyst,
                                   pOutSystCode,
                                   pOutObjID,
                                   pSourceObjID,
                                   CHR(0),
                                   v_TaxPeriod,
                                   pTransfDate,
                                   pTransfKind,
                                   pHolding_Period,
                                   pChangeCode
                                  ) RETURNING t_ObjID INTO pObjID;
         
         IF pUser <> 'X' THEN

            INSERT INTO DNPTXOBDC_DBT (
                                       T_DOCID    ,  -- T_DOCID
                                       T_STEP     ,  -- T_STEP
                                       T_OBJID       -- T_OBJID
                                      )
                               VALUES (
                                       pDocID,       -- T_DOCID
                                       pStep,        -- T_STEP
                                       pObjID       -- T_OBJID
                                      );
         END IF;
         
         INSERT INTO DNPTXPAY_DBT (
                                   T_PARTYID,
                                   T_SUMHOLD,
                                   T_DATEHOLD,
                                   T_DATEPAY,
                                   T_ISCUSTOM
                                  )
                           VALUES (
                                   pClient,
                                   pSum,
                                   pDate,
                                   pDate,
                                   CHR(0)
                                  ) RETURNING t_ID INTO pPayID;
         
         INSERT INTO DNPTXPAYOBJ_DBT (
                                      T_NPTXPAYID,
                                      T_OBJID,
                                      T_ID_OPERATION,
                                      T_ID_STEP
                                     )
                              VALUES (
                                       pPayID,
                                       pObjID,
                                       pDocID,
                                       pStep
                                     );
      END IF;
   END; -- InsertTaxObjectTaxPay

    --Выполняет откат вставки объекта и вставки в реестр
   PROCEDURE RecoilInsertTaxObjectTaxPay( pDocID IN NUMBER,
                                          pStep  IN NUMBER,
                                          pNoCalcNOB IN NUMBER DEFAULT 0
                                         )
   IS
      v_Count NUMBER;
      payID NUMBER := 0;
      TYPE VarCur IS REF CURSOR RETURN DNPTXOBDC_DBT%ROWTYPE;
      cObj VarCur;
      Obj DNPTXOBDC_DBT%ROWTYPE;
   BEGIN

      IF( pNoCalcNOB != 0 ) THEN
         OPEN cObj FOR
         SELECT N.*
           FROM DNPTXOBDC_DBT N
          WHERE N.T_DOCID = pDocID
            AND N.T_STEP  = pStep
            AND EXISTS ( SELECT txobj.t_OBJID
                           FROM DNPTXOBJ_DBT txobj
                          WHERE txobj.t_OBJID = N.t_OBJID
                            AND txobj.t_AnaliticKind1 in(1020, 1095, 1098, 1115, 1120, 1125, 1130)
                       );
      ELSE
         OPEN cObj FOR
         SELECT N.*
           FROM DNPTXOBDC_DBT N
          WHERE N.T_DOCID = pDocID
            AND N.T_STEP  = pStep
            AND EXISTS ( SELECT txobj.t_OBJID
                           FROM DNPTXOBJ_DBT txobj
                          WHERE txobj.t_OBJID = N.t_OBJID
                            AND txobj.t_AnaliticKind1 not in(1095, 1098, 1115, 1120, 1125, 1130)
                       );
      END IF;

      LOOP
         FETCH cObj INTO Obj;
         EXIT WHEN cObj%NOTFOUND;

         DELETE FROM DNPTXOBDC_DBT
               WHERE T_DOCID = Obj.t_DocID
                 AND T_STEP  = Obj.t_Step
                 AND T_OBJID = Obj.t_ObjID;

         Select count(1)
           Into v_Count
           From DNPTXOBDC_DBT
          Where T_OBJID = Obj.t_ObjID;

         IF v_Count = 0 THEN
            DELETE FROM DNPTXOBJ_DBT
                  WHERE T_OBJID = Obj.t_ObjID;
         END IF;
         
         DELETE FROM DNPTXPAY_DBT -- Мз DNPTXPAYOBJ_DBT запись удалит триггер
               WHERE T_ID = (SELECT T_NPTXPAYID
                               FROM DNPTXPAYOBJ_DBT
                              WHERE T_OBJID = Obj.t_ObjID);
      END LOOP;

      CLOSE cObj;

   END; -- RecoilInsertTaxObjectTaxPay

   ------------------------------------------------------------------------------------------------------------------
   -- функция получает из настроек системы насройки НУ и заносит их в глобализм
   ------------------------------------------------------------------------------------------------------------------
   procedure GetSettingsTax
    is
   begin

     RateTypes.MaxRate        := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА МАКСИМАЛЬНАЯ ЦЕНА'    , 0);
     RateTypes.MinRate        := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА МИНИМАЛЬНАЯ ЦЕНА'     , 0);
     RateTypes.MediumRate     := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0);
     RateTypes.ReuterRate     := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СРЕДНЯЯ ЦЕНА РЕЙТЕР'  , 0);
     RateTypes.CloseRate      := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА ЦЕНА ЗАКРЫТИЯ'        , 0);
     RateTypes.BloombergRate  := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА ЦЕНА ЗАКРЫТ. БЛУМБЕРГ', 0);
     RateTypes.NPTXEstimRate  := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА ОЦЕНКА ДЛЯ НДФЛ'      , 0);
     RateTypes.NPTXCalcRate   := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА РАСЧЕТ. ЦЕНА ДЛЯ НДФЛ', 0);
     RateTypes.NPTXMarketRate := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА РЫН. ЦЕНА ДЛЯ НДФЛ'   , 0);

   end; --GetSettingsTax

   ------------------------------------------------------------------------------------------------------------------
   ---- функция получает курс типа для ценной бумаги на дату ratedate или за период
   ---- от ratedate-Ndays до ratedate с максимальной датой начала дествия курса
   ------------------------------------------------------------------------------------------------------------------
   function SPGetRate( FIID IN NUMBER, ToFIID IN NUMBER, RateType IN NUMBER, RateDate IN DATE, NDays IN NUMBER, RD OUT DRATEDEF_DBT%ROWTYPE, pMarketCountry IN VARCHAR2 DEFAULT CHR(1), pOnlyRate IN NUMBER DEFAULT 0, pIsMinMax IN NUMBER, pCanUseCross IN NUMBER, pIsForeignMarket IN NUMBER DEFAULT 0, pMarket_Place IN NUMBER DEFAULT -1 )
    return NUMBER
     is
    t_RD        DRATEDEF_DBT%ROWTYPE;
    v_RateDate  DATE;
    v_RateID    NUMBER;
    v_SinceDate DATE;
    v_Rate      NUMBER;
    v_IsMrkt    BOOLEAN;
    v_IsMinMax  NUMBER;
   begin
     t_RD := NULL;

     if( RateType in (RateTypes.MinRate,RateTypes.MaxRate,RateTypes.MediumRate,RateTypes.NPTXMarketRate,RateTypes.CloseRate) ) then
       v_IsMrkt := True;
     else
       v_IsMrkt := False;
     end if;

     v_IsMinMax := 0;

     if pIsMinMax > 0 then
       v_IsMinMax := pIsMinMax;
     end if;

     v_RateDate := RateDate;

     v_Rate := RSI_RSB_FIInstr.FI_GetRate( FIID, ToFIID, RateType, v_RateDate, NDays, v_IsMinMax, v_RateID, v_SinceDate, v_IsMrkt, pMarketCountry, pIsForeignMarket, pOnlyRate, pCanUseCross, pMarket_Place );
     begin
       select * into t_RD
         from dratedef_dbt
        where t_RateID = v_RateID;
     exception
       when OTHERS then return 1;
     end;

     t_RD.t_SinceDate := v_SinceDate;
     t_RD.t_Rate      := v_Rate;

     RD := t_RD;

     return 0;
   exception
     when OTHERS then return 1;
   end;

   -- Значение категории для субъекта "способ определения расчетной цены при отсутствии котировок"
   FUNCTION CalPrMethodByClnt( PartyID IN NUMBER, OperDate IN DATE ) RETURN NUMBER DETERMINISTIC
   IS
      CategoryValue dobjattr_dbt.t_NumInList % TYPE;
   BEGIN
   BEGIN
          SELECT Attr.t_NumInList INTO CategoryValue
            FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
           WHERE     AtCor.t_ObjectType = 3  -- OBJTYPE_PARTY
                 AND AtCor.t_GroupID    = 46 -- способ определения расчетной цены при отсутствии котировок
                 AND AtCor.t_Object     = LPAD( PartyID, 10, '0' )
                 AND AtCor.t_ValidFromDate  = ( SELECT MAX(t.T_ValidFromDate)
                                                  FROM DOBJATCOR_DBT t
                                                 WHERE     t.T_ObjectType = AtCor.T_ObjectType
                                                       AND t.T_GroupID    = AtCor.T_GroupID
                                                       AND t.t_Object     = AtCor.t_Object
                                                       AND t.T_ValidFromDate <= OperDate
                                              )
                 AND Attr.t_AttrID      = AtCor.t_AttrID
                 AND Attr.t_ObjectType  = AtCor.t_ObjectType
                 AND Attr.t_GroupID     = AtCor.t_GroupID;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN CategoryValue := chr(0);
         WHEN OTHERS THEN
            return 0;
      END;

      IF( CategoryValue <> chr(0) ) THEN
         return to_number(CategoryValue);
      ELSE
         return 0;
      END IF;

      RETURN 0;
   END; -- CalPrMethodByClnt

   function PartyIsMarket( PartyID IN NUMBER )
   return NUMBER
   is
      v_Count   NUMBER := 0;
   begin

      begin
        SELECT COUNT(1) INTO v_Count
          FROM DPARTYOWN_DBT
         WHERE T_PARTYID   = PartyID
           AND T_PARTYKIND = 3;
      exception
        when NO_DATA_FOUND then v_Count := 0;
      end;

      return v_Count;

   end; --PartyIsMarket

   FUNCTION GetCountryParty( pPartyID IN NUMBER, pCountry OUT VARCHAR2 ) RETURN VARCHAR2
   IS
      vCountry dparty_dbt.t_NRCountry%TYPE;
   BEGIN
      begin
         select t_NRCountry into vCountry
           from dparty_dbt
          where t_PartyID = pPartyID;

         if vCountry = CHR(1) then
            vCountry := 'RUS';
         end if;

         exception when NO_DATA_FOUND then vCountry := 'RUS';
      end;

      pCountry := vCountry;

      return vCountry;
   END; --GetCountryParty

   function GetDealCountry(Deal IN R_Deal, pCountry OUT VARCHAR2)
   return VARCHAR2
   is
     vCountry dparty_dbt.t_NRCountry%TYPE;
   begin
     begin
       if Deal.Tick.t_MarketID <> -1 then
          select t_NRCountry into vCountry
            from dparty_dbt
           where t_PartyID = Deal.Tick.t_MarketID;
       else
          vCountry := Deal.Tick.t_Country;
       end if;

       if vCountry = CHR(1) then
          vCountry := 'RUS';
       end if;

       exception
          when NO_DATA_FOUND then vCountry := 'RUS';
     end;

     pCountry := vCountry;

     return vCountry;
   end; --GetDealCountry

   ------------------------------------------------------------------------------------------------------------------
   ---- функция получает параметры сделки необходимые для расчета
   ------------------------------------------------------------------------------------------------------------------
   function GetParmDeal( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER, pLotID IN NUMBER, ForIfMarket IN BOOLEAN DEFAULT FALSE, Deal OUT R_Deal )
    return NUMBER
     is
       TXLot     dnptxlot_dbt%ROWTYPE;
       SLot      dnptxlot_dbt%ROWTYPE;
       Leg       ddl_leg_dbt%ROWTYPE;
       v_IsReal  NUMBER;
       Nominal   NUMBER(32,12);
   begin

     -- получаем финансовый инструмент
     begin
       select * into Deal.FI from dfininstr_dbt where t_FIID = pFIID;
     exception
       when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении финансового инструмента FIID = '||pFIID;
                               return 1;
     end;

     -- и в случае если получаем параметр обращаемости, достаточно pFIID и pDate
     if( ForIfMarket = true )then
        Deal.DZ := pDate;
        return 0;
     end if;

     -- получение данных по лоту(реальному или виртуальному)
     if( pLotID > 0 )then

        begin
          select * into TXLot from dnptxlot_dbt where t_ID = pLotID;
        exception
           when NO_DATA_FOUND then
             MarketPrice.ErrorMsg := 'Не возможно найти лот с ID = '||pLotID;
             return 1;
        end;

        if( rsi_nptx.IsVirtual(TXLot.t_Type) = 1 )then
           v_IsReal := 0;
        else
            v_IsReal := 1;
        end if;

        if( v_IsReal = 1 )then

           -- получаем тикет сделки
           begin
             select * into Deal.Tick from ddl_tick_dbt where t_DealID = TXLot.t_DocID;
           exception
             when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении записи dl_tick.dbt для сделки DealID = '||TXLot.t_DocID;
                                     return 1;
           end;

           Deal.DZ := Deal.Tick.t_DealDate;

        else
           -- получить реальную сделку продажи
           begin
             select * into SLot from dnptxlot_dbt where t_ID = TXLot.t_RealID;
           exception
              when NO_DATA_FOUND then
                MarketPrice.ErrorMsg := 'Не возможно найти лот с ID = '||TXLot.t_RealID;
                return 1;
           end;

           -- получаем тикет сделки
           begin
             select * into Deal.Tick from ddl_tick_dbt where t_DealID = SLot.t_DocID;
           exception
             when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении записи dl_tick.dbt для сделки DealID = '||SLot.t_DocID;
                                     return 1;
           end;

           Deal.DZ := pDate;

        end if;

     -- получаем запись по ID реальной сделки
     elsif( pDealID > 0 )then

        -- получаем тикет сделки
        begin
          select * into Deal.Tick from ddl_tick_dbt where t_DealID = pDealID;
        exception
          when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении записи dl_tick.dbt для сделки DealID = '||pDealID;
                                  return 1;
        end;

        Deal.DZ := Deal.Tick.t_DealDate;

     else
        MarketPrice.ErrorMsg := 'Для получения рыночных параметров необходимо передать ID лота НДФЛ или ID реальной сделки';
        return 1;
     end if;

     IF Deal.Tick.t_BOfficeKind = RSI_NPTXC.DL_AVRWRT AND Deal.Tick.t_Flag3 = 'X' THEN
        Deal.DZ := pDate;  
     END IF;

     -- получаем группу операций
     begin
       select rsb_secur.get_OperationGroup(t_SysTypes) into Deal.OGrp
         from doprkoper_dbt
        where t_Kind_Operation = Deal.Tick.t_DealType and
              t_DocKind = Deal.Tick.t_BOfficeKind;
     exception
       when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении группы операций для сделки DealID = '||Deal.Tick.t_DealID;
                               return 1;
     end;

     -- Получаем данные по сделке
     begin
       select * into Leg from ddl_leg_dbt where t_DealID = Deal.Tick.t_DealID and t_LegKind = 0 and t_LegID = 0;
     exception
       when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении записи dl_leg.dbt для сделки DealID = '||Deal.Tick.t_DealID;
                               return 1;
     end;

     Deal.Price := Leg.t_Price;
     if( RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, Deal.FI.t_AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_BOND ) then
        if( Leg.t_RelativePrice != 'X' ) then
           if( Leg.T_CFI != Deal.FI.t_FaceValueFI ) then
              Deal.Price := RSI_RSB_FIInstr.ConvSum( Deal.Price, Leg.T_CFI, Deal.FI.t_FaceValueFI, Deal.DZ);
           end if;
           Nominal := RSI_RSB_FIInstr.FI_GetNominalOnDate(Deal.FI.t_FIID, Deal.DZ);
           if( Nominal != 0 ) then
              Deal.Price := Deal.Price / Nominal * 100.0;
           else
              Deal.Price := 0;
           end if;
        end if;
     elsif( Leg.t_RelativePrice = 'X' ) then
        Nominal := RSI_RSB_FIInstr.FI_GetNominalOnDate(Deal.FI.t_FIID, Deal.DZ);
        if( Nominal != 0 ) then
           Deal.Price := Deal.Price * Nominal / 100.0;
        else
           Deal.Price := 0;
        end if;
     elsif( Leg.T_CFI != Deal.FI.t_FaceValueFI ) then
        Deal.Price := RSI_RSB_FIInstr.ConvSum( Deal.Price, Leg.T_CFI, Deal.FI.t_FaceValueFI, Deal.DZ);
     end if;

     return 0;

   end; --RSI_GetParmDeal

   FUNCTION GetFICirculateNPTXFI(pFIID IN NUMBER, pDate IN DATE) RETURN NUMBER RESULT_CACHE RELIES_ON(DNPTXFI_DBT)
   AS
     v_Circulate NUMBER := 0;
   BEGIN
     SELECT f.t_Circulate INTO v_Circulate
       FROM dnptxfi_dbt f
      WHERE f.t_FIID = pFIID
        AND f.t_Date = pDate;

     RETURN v_Circulate;

     EXCEPTION
       WHEN NO_DATA_FOUND THEN RETURN 0; 
   END;

   -- pIsMinMax - если 0- рассчитывать мин цену, если 1 - рассчитывать максимальную цену (мы всегда ищем или минимальную или максимальную цену)
   procedure CalcMarketPrice( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER, pLotID IN NUMBER, ForIfMarket IN BOOLEAN DEFAULT FALSE )
    is
     Deal          R_Deal;
     C0            DRATEDEF_DBT%ROWTYPE;
     v_IsShare     NUMBER;
     v_AvrRoot     NUMBER;
     NumInList     dobjattr_dbt.t_NumInList%TYPE;
     NoteValue     NUMBER(32,12);
     NoteValueFI   VARCHAR2(3);
     NoteValueFIID NUMBER;
     Nominal       NUMBER(32,12);
     MinMarketPriceCalc NUMBER := 0;
     MaxMarketPriceCalc NUMBER := 0;
     v_Country     VARCHAR2(3);
     v_Circulate   NUMBER := 0;
     c_MICEX_code CONSTANT VARCHAR(2000) := Rsb_Common.GetRegStrValue('SECUR\MICEX_CODE');
     c_SPBEX_code CONSTANT VARCHAR(2000) := Rsb_Common.GetRegStrValue('SECUR\SPBEX_CODE');
     v_MICEX      NUMBER := GetPartyId(c_MICEX_code);
     v_SPBEX      NUMBER := GetPartyId(c_SPBEX_code);

     v_CloseRate_Market_Place     NUMBER(10) := 0;
     v_MediumRate_Market_Place    NUMBER(10) := 0;
     v_BloombergRate_Market_Place NUMBER(10) := 0;
     v_CloseRate_Stat     NUMBER := 0;
     v_MediumRate_Stat    NUMBER := 0;
     v_BloombergRate_Stat NUMBER := 0;
   begin

     IF -1 in (v_MICEX, v_SPBEX) THEN
       SetError(RSI_NPTXC.NPTX_ERROR_20648); --Не заданы значения кодов субъектов СПБ или ММВБ
     END IF;

     MarketPrice := NULL;

     v_Circulate := GetFICirculateNPTXFI(pFIID, pDate);
     IF v_Circulate != 0 THEN

       MarketPrice.ifMarket := CHR(0);
       IF v_Circulate = 1 THEN
         MarketPrice.ifMarket := 'X';
       END IF;

       IF ForIfMarket = TRUE THEN
         RETURN;
       END IF;
     END IF;

     if RateTypes.MinRate = 0 or RateTypes.MinRate is null then -- т.е., если первый вход и ещё ничего не закачивали
       GetSettingsTax();
     end if;

     if( GetParmDeal( pFIID, pDate, pDealID, pLotID, ForIfMarket, Deal ) != 0 ) then
        return;
     end if;

     MarketPrice.FIID    := Deal.FI.t_FIID;
     MarketPrice.FI_KIND := Deal.FI.t_FI_Kind;
     MarketPrice.DZ      := Deal.DZ;

     MarketPrice.CircRecalcStatus := RSI_NPTXC.NPTXCIRCRECALC_STATUS_NO_SUITABLE_QUOTES;

     v_IsShare := 0;

     v_AvrRoot := RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, Deal.FI.t_AvoirKind );
     if( v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_SHARE OR
         v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_INVESTMENT_SHARE OR
         v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_DEPOS_RECEIPT
       ) then
       v_IsShare := 1;
     end if;

     IF v_Circulate = 0 THEN
       RSI_RSB_FIInstr.FI_FindObjAttrOnDate(Deal.FI.t_FIID,Deal.DZ,RSI_NPTXC.TXAVR_ATTR_CIRCULATE,NumInList);
       if( NumInList = chr(0) )then
          v_CloseRate_Stat := SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.CloseRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-3)-1,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 0, v_IsShare);
          v_CloseRate_Market_Place := C0.T_MARKET_PLACE;
          v_MediumRate_Stat := SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MediumRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-3)-1,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 0, v_IsShare);
          v_MediumRate_Market_Place := C0.T_MARKET_PLACE;
          v_BloombergRate_Stat := SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.BloombergRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-3)-1,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 0, v_IsShare);
          v_BloombergRate_Market_Place := C0.T_MARKET_PLACE;
          if (
                (v_MediumRate_Stat = 0)
                AND
                (v_MediumRate_Market_Place in (v_MICEX, v_SPBEX))
             ) then
             MarketPrice.ifMarket    := 'X';
             MarketPrice.CircRecalcStatus := RSI_NPTXC.NPTXCIRCRECALC_STATUS_RUS_EXCHANGE_QUOTE;
          elsif (
                  (
                    (v_CloseRate_Stat = 0 AND (v_CloseRate_Market_Place is not null and v_CloseRate_Market_Place > 0) AND v_CloseRate_Market_Place not in (v_MICEX, v_SPBEX))
                    OR
                    (v_MediumRate_Stat = 0 AND (v_MediumRate_Market_Place is not null and v_MediumRate_Market_Place > 0) AND v_MediumRate_Market_Place not in (v_MICEX, v_SPBEX))
                    OR
                    (v_BloombergRate_Stat = 0 AND (v_BloombergRate_Market_Place is not null and v_BloombergRate_Market_Place > 0) AND v_BloombergRate_Market_Place not in (v_MICEX, v_SPBEX))
                  )
                ) then
             if (
                   (HasObjCodeOnDate(Deal.FI.t_FIID,11,Deal.DZ) = 1)
                   OR
                   (HasObjCodeOnDate(Deal.FI.t_FIID,22,Deal.DZ) = 1)
                ) then
                MarketPrice.ifMarket    := chr(0);
                MarketPrice.CircRecalcStatus := RSI_NPTXC.NPTXCIRCRECALC_STATUS_FOREIGN_EXCHANGE_QUOTE;
             else
                MarketPrice.ifMarket    := 'X';
                MarketPrice.CircRecalcStatus := RSI_NPTXC.NPTXCIRCRECALC_STATUS_FOREIGN_EXCHANGE_ONLY;
             end if;
          else
             MarketPrice.ifMarket    := chr(0);
          end if;
       elsif( NumInList = '1' )then
          MarketPrice.ifMarket    := 'X';
          MarketPrice.CircRecalcStatus := RSI_NPTXC.NPTXCIRCRECALC_STATUS_ORCB_LISTED;
       else
          MarketPrice.CircRecalcStatus := RSI_NPTXC.NPTXCIRCRECALC_STATUS_ORCB_LISTED;
          MarketPrice.ifMarket    := chr(0);
       end if;
     END IF;

     if( ForIfMarket = true )then
        return;
     end if;

     MarketPrice.LotID   := pLotID;
     MarketPrice.DealID  := Deal.Tick.t_DealID;

     -- если на сделке задано примечание "Расчетная цена"
     NoteValue := Rsb_SCTX.GetNoteText(Rsb_Secur.OBJTYPE_SECDEAL, LPAD(Deal.Tick.t_DealID, 34, '0'), 23);
     if( NoteValue != 0 ) then
        if( v_AvrRoot in (RSI_RSB_FIInstr.AVOIRKIND_SHARE, RSI_RSB_FIInstr.AVOIRKIND_BOND) ) then -- для акций и облигаций возможно необходимо перевести сумму
           NoteValueFI := Rsb_SCTX.GetNoteTextStr(Rsb_Secur.OBJTYPE_SECDEAL, LPAD(Deal.Tick.t_DealID, 34, '0'), 28);
           if( NoteValueFI != chr(1) ) then -- если задано примечание "Единица измерения расчетной цены"
              if( NoteValueFI <> '%' ) then
                 begin
                    SELECT fin.t_FIID INTO NoteValueFIID
                      FROM dfininstr_dbt fin
                     WHERE t_ISO_Number = NoteValueFI;
                 exception
                    when OTHERS then NoteValueFIID := Deal.FI.t_FaceValueFI;
                 end;

                 if( NoteValueFIID != Deal.FI.t_FaceValueFI ) then
                    NoteValue := RSI_RSB_FIInstr.ConvSum(NoteValue, NoteValueFIID, Deal.FI.t_FaceValueFI, Deal.DZ);
                 end if;

                 if( v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND ) then -- для облигаций в % от номинала
                    Nominal := RSI_RSB_FIInstr.FI_GetNominalOnDate(Deal.FI.t_FIID, Deal.DZ);
                    if( Nominal != 0 ) then
                       NoteValue := NoteValue / Nominal * 100.0;
                    else
                       NoteValue := 0;
                    end if;
                 end if;
              end if;
           end if;
        end if;

        MarketPrice.MarketPrice    := NoteValue;
        MarketPrice.MinMarketPrice := 0.8 * NoteValue;
        MarketPrice.MaxMarketPrice := 1.2 * NoteValue;

        MarketPrice.MinMarket      := 'Расчет';
        MarketPrice.MaxMarket      := 'Расчет';
        MarketPrice.MinDateMarket  := Deal.DZ;
        MarketPrice.MaxDateMarket  := Deal.DZ;

     elsif( ((Rsb_Secur.IsExchange(Deal.oGrp, 1)=1) OR (Rsb_Secur.IsOutExchange(Deal.oGrp, 1)=1 and PartyIsMarket(Deal.Tick.t_PartyID)!=0)) and 
             RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(Deal.Tick.t_DealID, 34, '0'), 53, Deal.DZ) = 2 ) then

        if( (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MinRate,Deal.DZ,0,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 1, v_IsShare) = 0) OR
             (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MinRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-3)-1,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 1, v_IsShare) = 0)
          ) then

           --биржа - источник выбранного курса
           begin
             select t_ShortName into MarketPrice.MinMarket from dparty_dbt where t_PartyID = C0.t_Market_Place;
           exception
             when NO_DATA_FOUND then MarketPrice.MinMarket := chr(0);
           end;

           MarketPrice.MinDateMarket  := C0.t_SinceDate;
           MarketPrice.MinMarketPrice := C0.t_Rate;

           MinMarketPriceCalc := 1;
        end if;

        if( (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MaxRate,Deal.DZ,0,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 1, v_IsShare) = 0) OR
             (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MaxRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-3)-1,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 1, v_IsShare) = 0)
          ) then

           --биржа - источник выбранного курса
           begin
             select t_ShortName into MarketPrice.MaxMarket from dparty_dbt where t_PartyID = C0.t_Market_Place;
           exception
             when NO_DATA_FOUND then MarketPrice.MaxMarket := chr(0);
           end;

           MarketPrice.MaxDateMarket  := C0.t_SinceDate;
           MarketPrice.MaxMarketPrice := C0.t_Rate;

           MaxMarketPriceCalc := 1;
        end if;

        if( MinMarketPriceCalc = 0 or MaxMarketPriceCalc = 0 ) then

           if( SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.NPTXCalcRate,Deal.DZ,0,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 1, v_IsShare) = 0 ) then

              if( MinMarketPriceCalc = 0 and MaxMarketPriceCalc = 0 ) then
                 MarketPrice.MarketPrice          := C0.t_Rate;
                 MarketPrice.MinMarketPrice       := 0.8 * C0.t_Rate;
                 MarketPrice.MaxMarketPrice       := 1.2 * C0.t_Rate;
                 MarketPrice.MinMarket            := 'Расчет';
                 MarketPrice.MaxMarket            := 'Расчет';
                 MarketPrice.MinDateMarket        := C0.t_SinceDate;
                 MarketPrice.MaxDateMarket        := C0.t_SinceDate;
              elsif( MinMarketPriceCalc = 0 ) then
                 MarketPrice.MinMarketPrice       := 0.8 * C0.t_Rate;
                 MarketPrice.MinMarket            := 'Расчет';
                 MarketPrice.MinDateMarket        := C0.t_SinceDate;
              else
                 MarketPrice.MaxMarketPrice       := 1.2 * C0.t_Rate;
                 MarketPrice.MaxMarket            := 'Расчет';
                 MarketPrice.MaxDateMarket        := C0.t_SinceDate;
              end if;

           else

              if( MinMarketPriceCalc = 0 and MaxMarketPriceCalc = 0 ) then
                 MarketPrice.MarketPrice          := Deal.Price;
                 MarketPrice.MinMarketPrice       := Deal.Price;
                 MarketPrice.MaxMarketPrice       := Deal.Price;
                 MarketPrice.MinMarket            := 'Факт';
                 MarketPrice.MaxMarket            := 'Факт';
                 MarketPrice.MinDateMarket        := Deal.DZ;
                 MarketPrice.MaxDateMarket        := Deal.DZ;
              elsif( MinMarketPriceCalc = 0 ) then
                 MarketPrice.MinMarketPrice       := Deal.Price;
                 MarketPrice.MinMarket            := 'Факт';
                 MarketPrice.MinDateMarket        := Deal.DZ;
              else
                 MarketPrice.MaxMarketPrice       := Deal.Price;
                 MarketPrice.MaxMarket            := 'Факт';
                 MarketPrice.MaxDateMarket        := Deal.DZ;
              end if;

           end if;

        end if;


     elsif( ((Rsb_Secur.IsExchange(Deal.oGrp, 1)=1) OR (Rsb_Secur.IsOutExchange(Deal.oGrp, 1)=1 and PartyIsMarket(Deal.Tick.t_PartyID)!=0)) and
             RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(Deal.Tick.t_DealID, 34, '0'), 53, Deal.DZ) <> 2 ) then

        MarketPrice.MarketPrice    := Deal.Price;
        MarketPrice.MinMarketPrice := Deal.Price;
        MarketPrice.MaxMarketPrice := Deal.Price;

        MarketPrice.MinMarket      := 'Биржа';
        MarketPrice.MaxMarket      := 'Биржа';
        MarketPrice.MinDateMarket  := Deal.DZ;
        MarketPrice.MaxDateMarket  := Deal.DZ;

     elsif( MarketPrice.ifMarket = 'X' ) then

        if( (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MinRate,Deal.DZ,0,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 1, v_IsShare) = 0) OR
             (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MinRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-3)-1,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 1, v_IsShare) = 0)
          ) then

           --биржа - источник выбранного курса
           begin
             select t_ShortName into MarketPrice.MinMarket from dparty_dbt where t_PartyID = C0.t_Market_Place;
           exception
             when NO_DATA_FOUND then MarketPrice.MinMarket := chr(0);
           end;

           MarketPrice.MinDateMarket  := C0.t_SinceDate;
           MarketPrice.MinMarketPrice := C0.t_Rate;

           MinMarketPriceCalc := 1;
        end if;

        if( (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MaxRate,Deal.DZ,0,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 1, v_IsShare) = 0) OR
             (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MaxRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-3)-1,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 1, v_IsShare) = 0)
          ) then

           --биржа - источник выбранного курса
           begin
             select t_ShortName into MarketPrice.MaxMarket from dparty_dbt where t_PartyID = C0.t_Market_Place;
           exception
             when NO_DATA_FOUND then MarketPrice.MaxMarket := chr(0);
           end;

           MarketPrice.MaxDateMarket  := C0.t_SinceDate;
           MarketPrice.MaxMarketPrice := C0.t_Rate;

           MaxMarketPriceCalc := 1;
        end if;

        if( MinMarketPriceCalc = 0 or MaxMarketPriceCalc = 0 ) then

           if( SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.NPTXCalcRate,Deal.DZ,0,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 1, v_IsShare) = 0 ) then

              if( MinMarketPriceCalc = 0 and MaxMarketPriceCalc = 0 ) then
                 MarketPrice.MarketPrice          := C0.t_Rate;
                 MarketPrice.MinMarketPrice       := 0.8 * C0.t_Rate;
                 MarketPrice.MaxMarketPrice       := 1.2 * C0.t_Rate;
                 MarketPrice.MinMarket            := 'Расчет';
                 MarketPrice.MaxMarket            := 'Расчет';
                 MarketPrice.MinDateMarket        := C0.t_SinceDate;
                 MarketPrice.MaxDateMarket        := C0.t_SinceDate;
              elsif( MinMarketPriceCalc = 0 ) then
                 MarketPrice.MinMarketPrice       := 0.8 * C0.t_Rate;
                 MarketPrice.MinMarket            := 'Расчет';
                 MarketPrice.MinDateMarket        := C0.t_SinceDate;
              else
                 MarketPrice.MaxMarketPrice       := 1.2 * C0.t_Rate;
                 MarketPrice.MaxMarket            := 'Расчет';
                 MarketPrice.MaxDateMarket        := C0.t_SinceDate;
              end if;

           else

              if( MinMarketPriceCalc = 0 and MaxMarketPriceCalc = 0 ) then
                 MarketPrice.MarketPrice          := Deal.Price;
                 MarketPrice.MinMarketPrice       := Deal.Price;
                 MarketPrice.MaxMarketPrice       := Deal.Price;
                 MarketPrice.MinMarket            := 'Факт';
                 MarketPrice.MaxMarket            := 'Факт';
                 MarketPrice.MinDateMarket        := Deal.DZ;
                 MarketPrice.MaxDateMarket        := Deal.DZ;
              elsif( MinMarketPriceCalc = 0 ) then
                 MarketPrice.MinMarketPrice       := Deal.Price;
                 MarketPrice.MinMarket            := 'Факт';
                 MarketPrice.MinDateMarket        := Deal.DZ;
              else
                 MarketPrice.MaxMarketPrice       := Deal.Price;
                 MarketPrice.MaxMarket            := 'Факт';
                 MarketPrice.MaxDateMarket        := Deal.DZ;
              end if;

           end if;

        end if;

     elsif( MarketPrice.ifMarket != 'X') then

        if( SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.ReuterRate,Deal.DZ,0,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 1, v_IsShare) = 0 ) then

           MarketPrice.MarketPrice    := C0.t_Rate;
           MarketPrice.MinMarketPrice := 0.8 * C0.t_Rate;
           MarketPrice.MaxMarketPrice := 1.2 * C0.t_Rate;
           MarketPrice.MinMarket      := 'Рейтерс';
           MarketPrice.MaxMarket      := 'Рейтерс';
           MarketPrice.MinDateMarket  := C0.t_SinceDate;
           MarketPrice.MaxDateMarket  := C0.t_SinceDate;

        elsif( SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.BloombergRate,Deal.DZ,0,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 1, v_IsShare) = 0 ) then

              MarketPrice.MarketPrice    := C0.t_Rate;
           MarketPrice.MinMarketPrice := 0.8 * C0.t_Rate;
           MarketPrice.MaxMarketPrice := 1.2 * C0.t_Rate;
           MarketPrice.MinMarket      := 'Блумберг';
           MarketPrice.MaxMarket      := 'Блумберг';
           MarketPrice.MinDateMarket  := C0.t_SinceDate;
           MarketPrice.MaxDateMarket  := C0.t_SinceDate;

        elsif( SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.NPTXCalcRate,Deal.DZ,0,C0,CHR(1),(CASE WHEN v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN 1 ELSE 0 END), 1, v_IsShare) = 0 ) then

           MarketPrice.MarketPrice    := C0.t_Rate;
           MarketPrice.MinMarketPrice := 0.8 * C0.t_Rate;
           MarketPrice.MaxMarketPrice := 1.2 * C0.t_Rate;
           MarketPrice.MinMarket      := 'Расчет';
           MarketPrice.MaxMarket      := 'Расчет';
           MarketPrice.MinDateMarket  := C0.t_SinceDate;
           MarketPrice.MaxDateMarket  := C0.t_SinceDate;

        else

           MarketPrice.MarketPrice    := Deal.Price;
           MarketPrice.MinMarketPrice := Deal.Price;
           MarketPrice.MaxMarketPrice := Deal.Price;
           MarketPrice.MinMarket      := 'Факт';
           MarketPrice.MaxMarket      := 'Факт';
           MarketPrice.MinDateMarket  := Deal.DZ;
           MarketPrice.MaxDateMarket  := Deal.DZ;

       end if;

     end if;

   end; --CalcMarketPrice

   ------------------------------------------------------------------------------------------------------------------
   ---- функция получает параметры бнебиржевой сделки ПИ, необходимые для расчета
   ------------------------------------------------------------------------------------------------------------------
   function GetParmDeal_DV( pDealID IN NUMBER, Deal OUT R_Deal )
   return NUMBER
    is
   begin
      begin
         select * into Deal.DvNDeal from ddvndeal_dbt where t_ID = pDealID;
      exception
       when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении записи dvndeal.dbt для сделки ID = '||pDealID;
         return 1;
      end;

      if( Deal.DvNDeal.t_DVKind != 1 /*DV_FORWARD*/ and Deal.DvNDeal.t_DVKind != 2 /*DV_OPTION*/ ) then
         return 1;
      end if;

      Deal.DZ := Deal.DvNDeal.t_Date;

      return 0;
   end; --GetParmDeal_DV

   procedure CalcMarketPrice_DV( pDealID IN NUMBER )
    is
     Deal        R_Deal;
     NoteValue   NUMBER(32,12);
     v_Rate      NUMBER(32,12);
     v_Type      NUMBER := 0;--DV_NFIType_BaseActiv
     v_Amount    NUMBER(32,12);
     v_PriceFIID NUMBER;
     v_Price     NUMBER(32,12);
   begin

     MarketPrice := NULL;

     if RateTypes.MinRate = 0 or RateTypes.MinRate is null then -- т.е., если первый вход и ещё ничего не закачивали
       GetSettingsTax();
     end if;

     if( GetParmDeal_DV( pDealID, Deal ) != 0 ) then
        return;
     end if;

     MarketPrice.FI_KIND := RSI_RSB_FIInstr.FIKIND_DERIVATIVE;
     MarketPrice.DZ      := Deal.DZ;
     MarketPrice.DealID  := Deal.DvNDeal.t_ID;

     MarketPrice.ifMarket := chr(0);
     if( Deal.DvNDeal.t_Contractor > 0 and PartyIsMarket(Deal.DvNDeal.t_Contractor) != 0 ) then
        MarketPrice.ifMarket := 'X';
     end if;

     if( Deal.DvNDeal.t_Forvard = 'X' )then
        v_Type := 1;--DV_NFIType_Forward
     end if;

     select t_Amount, t_Price, t_PriceFIID into v_Amount, v_Price, v_PriceFIID
       from ddvnfi_dbt
      where t_dealid = Deal.DvNDeal.t_ID
        and t_type = v_Type;

     if( Deal.DvNDeal.t_DVKind = 1 /*DV_FORWARD*/ ) then
        NoteValue := Rsb_SCTX.GetNoteText(Rsb_Secur.OBJTYPE_OUTOPER_DV, LPAD(Deal.DvNDeal.t_ID, 34, '0'), 2);--NOTEKIND_DVNDEAL_MARKETPRICE - Рыночная цена контракта за единицу базового актива
     elsif( Deal.DvNDeal.t_DVKind = 2 /*DV_OPTION*/ ) then
        NoteValue := Rsb_SCTX.GetNoteText(Rsb_Secur.OBJTYPE_OUTOPER_DV, LPAD(Deal.DvNDeal.t_ID, 34, '0'), 3);--NOTEKIND_DVNDEAL_MARKETCOST - Рыночная стоимость премии по сделке
     end if;

     --Если примечания не заданы и во всех остальных случаях
     if( NoteValue is NULL ) then
        if( Deal.DvNDeal.t_DVKind = 1 /*DV_FORWARD*/ ) then
           v_Rate := v_Price * v_Amount;
           if( v_PriceFIID != 0 ) then
              v_Rate := RSI_RSB_FIInstr.ConvSum( v_Rate, v_PriceFIID, 0, Deal.DZ);
           end if;
        else
           v_Rate := Deal.DvNDeal.t_Bonus;
           if( Deal.DvNDeal.t_BonusFIID != 0 ) then
              v_Rate := RSI_RSB_FIInstr.ConvSum( v_Rate, Deal.DvNDeal.t_BonusFIID, 0, Deal.DZ);
           end if;
        end if;

        MarketPrice.MarketPrice    := v_Rate;
        MarketPrice.MinMarketPrice := v_Rate;
        MarketPrice.MaxMarketPrice := v_Rate;
        MarketPrice.Market         := 'Факт';
        MarketPrice.DateMarket     := Deal.DZ;
     else
        if( Deal.DvNDeal.t_DVKind = 1 /*DV_FORWARD*/ ) then
           v_Rate := NoteValue * v_Amount;
           if( v_PriceFIID != 0 ) then
              v_Rate := RSI_RSB_FIInstr.ConvSum( v_Rate, v_PriceFIID, 0, Deal.DZ);
           end if;
        elsif( Deal.DvNDeal.t_DVKind = 2 /*DV_OPTION*/ ) then
           v_Rate := NoteValue;
           if( Deal.DvNDeal.t_BonusFIID != 0 ) then
              v_Rate := RSI_RSB_FIInstr.ConvSum( v_Rate, Deal.DvNDeal.t_BonusFIID, 0, Deal.DZ);
           end if;
        end if;

        MarketPrice.MarketPrice    := v_Rate;
        MarketPrice.MinMarketPrice := 0.8 * v_Rate;
        MarketPrice.MaxMarketPrice := 1.2 * v_Rate;
        MarketPrice.Market         := 'Расчет';
        MarketPrice.DateMarket     := Deal.DZ;
     end if;

   end; --CalcMarketPrice_DV

   function GetMarketPrice( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 )
    return NUMBER
     is
   begin
     if( MarketPrice.FIID is NULL or MarketPrice.FIID <> pFIID or MarketPrice.DZ <> pDate or
         MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_AVOIRISS or
        ((MarketPrice.DealID is NULL or MarketPrice.DealID <> pDealID) and pDealID > 0) or
        ((MarketPrice.LotID is NULL or MarketPrice.LotID <> pLotID) and pLotID > 0)
       )then
        CalcMarketPrice( pFIID, pDate, pDealID, pLotID );
     end if;
     if(MarketPrice.MarketPrice is not NULL)then
        return MarketPrice.MarketPrice;
     else return 0.0;
     end if;
   end;

   function GetMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 )
    return VARCHAR2
     is
       Market        VARCHAR2(255);
   begin
     if( MarketPrice.FIID is NULL or MarketPrice.FIID <> pFIID or MarketPrice.DZ <> pDate or
         MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_AVOIRISS or
        ((MarketPrice.DealID is NULL or MarketPrice.DealID <> pDealID) and pDealID > 0) or
        ((MarketPrice.LotID is NULL or MarketPrice.LotID <> pLotID) and pLotID > 0)
       )then
        CalcMarketPrice( pFIID, pDate, pDealID, pLotID );
     end if;

     if( MarketPrice.MinMarket != MarketPrice.MaxMarket ) then
        Market := MarketPrice.MinMarket||'/'||MarketPrice.MaxMarket;
     else
        Market := MarketPrice.MinMarket;
     end if;

     return Market;
   end;

   function GetMinMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 )
    return VARCHAR2
     is
   begin
     if( MarketPrice.FIID is NULL or MarketPrice.FIID <> pFIID or MarketPrice.DZ <> pDate or
         MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_AVOIRISS or
        ((MarketPrice.DealID is NULL or MarketPrice.DealID <> pDealID) and pDealID > 0) or
        ((MarketPrice.LotID is NULL or MarketPrice.LotID <> pLotID) and pLotID > 0)
       )then
        CalcMarketPrice( pFIID, pDate, pDealID, pLotID );
     end if;

     return MarketPrice.MinMarket;
   end;

   function GetMaxMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 )
    return VARCHAR2
     is
   begin
     if( MarketPrice.FIID is NULL or MarketPrice.FIID <> pFIID or MarketPrice.DZ <> pDate or
         MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_AVOIRISS or
        ((MarketPrice.DealID is NULL or MarketPrice.DealID <> pDealID) and pDealID > 0) or
        ((MarketPrice.LotID is NULL or MarketPrice.LotID <> pLotID) and pLotID > 0)
       )then
        CalcMarketPrice( pFIID, pDate, pDealID, pLotID );
     end if;

     return MarketPrice.MaxMarket;
   end;

   function GetDateMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 )
    return VARCHAR2
     is
       DateMarket        VARCHAR2(255);
   begin
     if( MarketPrice.FIID is NULL or MarketPrice.FIID <> pFIID or MarketPrice.DZ <> pDate or
         MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_AVOIRISS or
        ((MarketPrice.DealID is NULL or MarketPrice.DealID <> pDealID) and pDealID > 0) or
        ((MarketPrice.LotID is NULL or MarketPrice.LotID <> pLotID) and pLotID > 0)
       )then
        CalcMarketPrice( pFIID, pDate, pDealID, pLotID );
     end if;

     if( MarketPrice.MinDateMarket != MarketPrice.MaxDateMarket ) then
        DateMarket := to_char(MarketPrice.MinDateMarket,'DD.MM.YYYY')||'/'||to_char(MarketPrice.MaxDateMarket,'DD.MM.YYYY');
     else
        DateMarket := to_char(MarketPrice.MinDateMarket,'DD.MM.YYYY');
     end if;

     return DateMarket;
   end;

   function GetMinDateMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 )
    return DATE
     is
   begin
     if( MarketPrice.FIID is NULL or MarketPrice.FIID <> pFIID or MarketPrice.DZ <> pDate or
         MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_AVOIRISS or
        ((MarketPrice.DealID is NULL or MarketPrice.DealID <> pDealID) and pDealID > 0) or
        ((MarketPrice.LotID is NULL or MarketPrice.LotID <> pLotID) and pLotID > 0)
       )then
        CalcMarketPrice( pFIID, pDate, pDealID, pLotID );
     end if;

     return MarketPrice.MinDateMarket;
   end;

   function GetMaxDateMarket( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 )
    return DATE
     is
   begin
     if( MarketPrice.FIID is NULL or MarketPrice.FIID <> pFIID or MarketPrice.DZ <> pDate or
         MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_AVOIRISS or
        ((MarketPrice.DealID is NULL or MarketPrice.DealID <> pDealID) and pDealID > 0) or
        ((MarketPrice.LotID is NULL or MarketPrice.LotID <> pLotID) and pLotID > 0)
       )then
        CalcMarketPrice( pFIID, pDate, pDealID, pLotID );
     end if;

     return MarketPrice.MaxDateMarket;
   end;

   function IfMarket( pFIID IN NUMBER, pDate IN DATE )
    return VARCHAR2
     is
   begin
     if( MarketPrice.FIID is NULL or MarketPrice.FIID <> pFIID or MarketPrice.DZ <> pDate or MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_AVOIRISS )then
        CalcMarketPrice( pFIID, pDate, -1, -1, true );
     end if;
     return MarketPrice.IfMarket;
   end;

   function GetErrorMsg
    return VARCHAR2
     is
   begin
     return MarketPrice.ErrorMsg;
   end;

   -- Получение мин. рыночной цены
   FUNCTION GetMinMarketPrice( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) RETURN NUMBER
   IS
   BEGIN
     if( MarketPrice.FIID is NULL or MarketPrice.FIID <> pFIID or MarketPrice.DZ <> pDate or
         MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_AVOIRISS or
        ((MarketPrice.DealID is NULL or MarketPrice.DealID <> pDealID) and pDealID > 0) or
        ((MarketPrice.LotID is NULL or MarketPrice.LotID <> pLotID) and pLotID > 0)
       )then
        CalcMarketPrice( pFIID, pDate, pDealID, pLotID );
     end if;

     if(MarketPrice.MinMarketPrice is not NULL)then
        return MarketPrice.MinMarketPrice;
     else return 0.0;
     end if;
   END; -- GetMinMarketPrice

   -- Получение макс. рыночной цены
   FUNCTION GetMaxMarketPrice( pFIID IN NUMBER, pDate IN DATE, pDealID IN NUMBER default -1, pLotID IN NUMBER default -1 ) RETURN NUMBER
   IS
   BEGIN
     if( MarketPrice.FIID is NULL or MarketPrice.FIID <> pFIID or MarketPrice.DZ <> pDate or
         MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_AVOIRISS or
        ((MarketPrice.DealID is NULL or MarketPrice.DealID <> pDealID) and pDealID > 0) or
        ((MarketPrice.LotID is NULL or MarketPrice.LotID <> pLotID) and pLotID > 0)
       )then
        CalcMarketPrice( pFIID, pDate, pDealID, pLotID );
     end if;

     if(MarketPrice.MaxMarketPrice is not NULL)then
        return MarketPrice.MaxMarketPrice;
     else return 0.0;
     end if;
   END; -- GetMaxMarketPrice

   function GetMarketPrice_DV( pDealID IN NUMBER )
    return NUMBER
     is
   begin
     if( MarketPrice.DealID is NULL or MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_DERIVATIVE or MarketPrice.DealID <> pDealID )then
        CalcMarketPrice_DV( pDealID );
     end if;
     if(MarketPrice.MarketPrice is not NULL)then
        return MarketPrice.MarketPrice;
     else return 0.0;
     end if;
   end;

   function GetMarket_DV( pDealID IN NUMBER )
    return VARCHAR2
     is
   begin
     if( MarketPrice.DealID is NULL or MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_DERIVATIVE or MarketPrice.DealID <> pDealID )then
        CalcMarketPrice_DV( pDealID );
     end if;
     return MarketPrice.Market;
   end;

   function GetDateMarket_DV( pDealID IN NUMBER )
    return DATE
     is
   begin
     if( MarketPrice.DealID is NULL or MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_DERIVATIVE or MarketPrice.DealID <> pDealID )then
        CalcMarketPrice_DV( pDealID );
     end if;
     return MarketPrice.DateMarket;
   end;

   function IfMarket_DV( pDealID IN NUMBER )
    return VARCHAR2
     is
   begin
     if( MarketPrice.DealID is NULL or MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_DERIVATIVE or MarketPrice.DealID <> pDealID )then
        CalcMarketPrice_DV( pDealID );
     end if;
     return MarketPrice.IfMarket;
   end;

   -- Получение мин. рыночной цены
   FUNCTION GetMinMarketPrice_DV( pDealID IN NUMBER ) RETURN NUMBER
   IS
   BEGIN
     if( MarketPrice.DealID is NULL or MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_DERIVATIVE or MarketPrice.DealID <> pDealID )then
        CalcMarketPrice_DV( pDealID );
     end if;

     if(MarketPrice.MinMarketPrice is not NULL)then
        return MarketPrice.MinMarketPrice;
     else return 0.0;
     end if;
   END; -- GetMinMarketPrice_DV

   -- Получение макс. рыночной цены
   FUNCTION GetMaxMarketPrice_DV( pDealID IN NUMBER ) RETURN NUMBER
   IS
   BEGIN
     if( MarketPrice.DealID is NULL or MarketPrice.FI_KIND != RSI_RSB_FIInstr.FIKIND_DERIVATIVE or MarketPrice.DealID <> pDealID )then
        CalcMarketPrice_DV( pDealID );
     end if;

     if(MarketPrice.MaxMarketPrice is not NULL)then
        return MarketPrice.MaxMarketPrice;
     else return 0.0;
     end if;
   END; -- GetMaxMarketPrice_DV

   -- Получение даты покупки из зачисления
   function GetDateFromAvrWrtIn( pDealID IN NUMBER, pStartDate IN DATE, pDealDate IN DATE )
     return DATE
     is
     v_RetDate DATE;
   begin
     -- дата покупки:
     -- дата первоначальной покупки в НУ, если она задана
     -- иначе - дата исходной покупки, если она задана
     -- иначе - дата операции

     BEGIN
        Select t_Date
          Into v_RetDate
          From ( Select t_Date
                   From ddlsum_dbt
                  Where t_DocKind = RSI_NPTXC.DL_AVRWRT
                    and t_DocID   = pDealID
                    and t_Kind    = RSI_NPTXC.DLSUM_KIND_COSTWRTTAX
                  Order by t_DlSumId Desc
               )
         Where ROWNUM = 1;

     EXCEPTION
        WHEN NO_DATA_FOUND THEN v_RetDate := TO_DATE('01.01.0001', 'DD.MM.YYYY');
        WHEN OTHERS THEN
           v_RetDate := TO_DATE('01.01.0001', 'DD.MM.YYYY');
     END;

     IF v_RetDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') THEN
        v_RetDate := pStartDate;
     END IF;

     IF v_RetDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') THEN
        v_RetDate := pDealDate;
     END IF;

     RETURN v_RetDate;
   end;


   -- Получение цены из зачисления
   function GetPriceFromAvrWrtIn( pDealID IN NUMBER, pPrice IN NUMBER )
     return NUMBER
     is
     v_RetPrice NUMBER;
   begin
     -- Вернуть цену покупки в НУ, а если она на задана - цену из паспорта

     BEGIN
        Select t_Sum
          Into v_RetPrice
          From ( Select t_Sum
                   From ddlsum_dbt
                  Where t_DocKind = RSI_NPTXC.DL_AVRWRT
                    and t_DocID   = pDealID
                    and t_Kind    = RSI_NPTXC.DLSUM_KIND_PRICEWRTTAX
                  Order by t_DlSumId Desc
               )
         Where ROWNUM = 1;

     EXCEPTION
        WHEN NO_DATA_FOUND THEN v_RetPrice := pPrice;
        WHEN OTHERS THEN
           v_RetPrice := pPrice;
     END;

     RETURN v_RetPrice;
   end;

   -- Получение валюты цены из зачисления
   function GetPriceFIIDFromAvrWrtIn( pDealID IN NUMBER, pPriceFIID IN NUMBER )
     return NUMBER
     is
     v_RetPriceFIID NUMBER;
   begin
     -- Вернуть цену покупки в НУ, а если она на задана - цену из паспорта

     BEGIN
        Select t_Currency
          Into v_RetPriceFIID
          From ( Select t_Currency
                   From ddlsum_dbt
                  Where t_DocKind = RSI_NPTXC.DL_AVRWRT
                    and t_DocID   = pDealID
                    and t_Kind    = RSI_NPTXC.DLSUM_KIND_PRICEWRTTAX
                  Order by t_DlSumId Desc
               )
         Where ROWNUM = 1;

     EXCEPTION
        WHEN NO_DATA_FOUND THEN v_RetPriceFIID := pPriceFIID;
        WHEN OTHERS THEN
           v_RetPriceFIID := pPriceFIID;
     END;

     RETURN v_RetPriceFIID;
   end;

   -- Получение стоимости из зачисления
   function GetCostFromAvrWrtIn( pDealID IN NUMBER, pCost IN NUMBER )
     return NUMBER
     is
     v_RetVal NUMBER;
   begin
     -- Стоимость при покупке в НУ из соотв. DDL_SUM, а если ее нет - Стоимость

     BEGIN
        Select t_Sum
          Into v_RetVal
          From ( Select t_Sum
                   From ddlsum_dbt
                  Where t_DocKind = RSI_NPTXC.DL_AVRWRT
                    and t_DocID   = pDealID
                    and t_Kind    = RSI_NPTXC.DLSUM_KIND_COSTWRTTAX
                  Order by t_DlSumId Desc
               )
         Where ROWNUM = 1;

     EXCEPTION
        WHEN NO_DATA_FOUND THEN v_RetVal := pCost;
        WHEN OTHERS THEN
           v_RetVal := pCost;
     END;

     RETURN v_RetVal;
   end;

   -- Получение НКД из зачисления
   function GetNkdFromAvrWrtIn( pDealID IN NUMBER, pNKD IN NUMBER )
     return NUMBER
     is
     v_RetVal NUMBER;
   begin
     -- НКД, уплаченный при покупке в НУ из соотв. DDL_SUM, а если ее нет - НКД

     BEGIN
        Select t_Sum
          Into v_RetVal
          From ( Select t_Sum
                   From ddlsum_dbt
                  Where t_DocKind = RSI_NPTXC.DL_AVRWRT
                    and t_DocID   = pDealID
                    and t_Kind    = RSI_NPTXC.DLSUM_KIND_NKDWRTTAX
                  Order by t_DlSumId Desc
               )
         Where ROWNUM = 1;

     EXCEPTION
        WHEN NO_DATA_FOUND THEN v_RetVal := pNKD;
        WHEN OTHERS THEN
           v_RetVal := pNKD;
     END;

     RETURN v_RetVal;
   end;

   -- Получение затрат из зачисления
   function GetOutlayFromAvrWrtIn( pDealID IN NUMBER, pOutlay IN NUMBER )
     return NUMBER
     is
     v_RetVal NUMBER;
   begin
     -- Затраты при покупке в НУ из соотв. DDL_SUM, а если ее нет - Предв. затраты + Затраты на приобретение

     BEGIN
        Select t_Sum
          Into v_RetVal
          From ( Select t_Sum
                   From ddlsum_dbt
                  Where t_DocKind = RSI_NPTXC.DL_AVRWRT
                    and t_DocID   = pDealID
                    and t_Kind    = RSI_NPTXC.DLSUM_KIND_OUTLAYWRTTAX
                  Order by t_DlSumId Desc
               )
         Where ROWNUM = 1;

        RETURN v_RetVal;

     EXCEPTION
        WHEN NO_DATA_FOUND THEN v_RetVal := pOutlay;
        WHEN OTHERS THEN
           v_RetVal := pOutlay;
     END;

     BEGIN
        Select t_Sum + pOutlay
          Into v_RetVal
          From ( Select t_Sum
                   From ddlsum_dbt
                  Where t_DocKind = RSI_NPTXC.DL_AVRWRT
                    and t_DocID   = pDealID
                    and t_Kind    = RSI_NPTXC.DLSUM_KIND_OUTLAY
                  Order by t_DlSumId Desc
               )
         Where ROWNUM = 1;

     EXCEPTION
        WHEN NO_DATA_FOUND THEN v_RetVal := pOutlay;
        WHEN OTHERS THEN
           v_RetVal := pOutlay;
     END;

     RETURN v_RetVal;
   end;

   -- Определение категории ц/б - алгоритм 1
   FUNCTION Market1date( pFIID IN NUMBER, pDate IN DATE ) RETURN NUMBER
   IS
   BEGIN
      IF IfMarket(pFIID, pDate) = CHR(88) THEN
         RETURN to_number(RSI_NPTXC.NPTX_FI_CIRCULATE);
      ELSE
         RETURN to_number(RSI_NPTXC.NPTX_FI_NOCIRCULATE);
      END IF;
   END; -- Market1date


   -- Определение категории ц/б - алгоритм 2
   FUNCTION Market2dates( pFIID IN NUMBER, pDate2 IN DATE, pDate1 IN DATE ) RETURN NUMBER
   IS
   BEGIN
      IF IfMarket(pFIID, pDate2) = CHR(88) THEN
         RETURN to_number(RSI_NPTXC.NPTX_FI_CIRCULATE);

      ELSIF IfMarket(pFIID, pDate1) = CHR(88) THEN
         RETURN to_number(RSI_NPTXC.NPTX_FI_LOSTCIRCULATE);

      ELSE
         RETURN to_number(RSI_NPTXC.NPTX_FI_NOCIRCULATE);
      END IF;
   END; -- Market2dates

   -- Определение категории ц/б - алгоритм 3
   FUNCTION Market3date( pFIID IN NUMBER, pDate IN DATE ) RETURN NUMBER
   IS
      v_cnt NUMBER := 0;
   BEGIN
      IF IfMarket(pFIID, pDate) = CHR(88) THEN
         RETURN to_number(RSI_NPTXC.NPTX_FI_CIRCULATE);
      ELSE
         SELECT COUNT(1) INTO v_cnt
           FROM dnptxfi_dbt f
          WHERE f.t_FIID = pFIID
            AND f.t_Date < pDate
            AND f.t_Circulate = to_number(RSI_NPTXC.NPTX_FI_CIRCULATE)
            AND ROWNUM = 1;
         
         IF v_cnt > 0 THEN
           RETURN to_number(RSI_NPTXC.NPTX_FI_LOSTCIRCULATE);
         END IF;

         RETURN to_number(RSI_NPTXC.NPTX_FI_NOCIRCULATE);
      END IF;
   END; -- Market2dates


   -- Получение для бумаги группы НУ для НДФЛ
   FUNCTION GetPaperTaxGroupNPTX( pFIID IN NUMBER , pIsDerivative IN NUMBER DEFAULT -1) RETURN NUMBER RESULT_CACHE RELIES_ON(DOBJATCOR_DBT)
   IS
      CategoryValue dobjattr_dbt.t_NumInList % TYPE;
      vRecFinIstr     DFININSTR_DBT%ROWTYPE;
      vBaseRecFinIstr DFININSTR_DBT%ROWTYPE;
      v_AvrRoot       NUMBER;
   BEGIN
      BEGIN
       IF (pIsDerivative != -1 ) THEN
         BEGIN
           SELECT * INTO vRecFinIstr
              FROM DFININSTR_DBT
             WHERE T_FIID = pFIID;

         EXCEPTION
            WHEN NO_DATA_FOUND THEN
               RETURN -1;
         END;

         if (vRecFinIstr.t_FI_Kind = RSI_NPTXC.FIKIND_AVOIRISS) THEN
            CategoryValue := RSI_NPTXC.TXGROUP_80; -- ФИСС фондовые
         elsif (vRecFinIstr.t_FI_Kind = RSI_RSB_FIInstr.FIKIND_INDEX) THEN
            if(vRecFinIstr.t_Settlement_Code = 2 ) then  /*фондовый*/
              CategoryValue := RSI_NPTXC.TXGROUP_80; -- ФИСС фондовые
            else
              CategoryValue := RSI_NPTXC.TXGROUP_90; -- ФИСС фондовые
            end if;
         elsif (vRecFinIstr.t_FI_Kind = RSI_NPTXC.FIKIND_DERIVATIVE) THEN
            BEGIN
               SELECT * INTO vBaseRecFinIstr
                 FROM DFININSTR_DBT
                WHERE T_FIID = vRecFinIstr.t_FaceValueFI;

            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  RETURN -1;
            END;

            if (vBaseRecFinIstr.t_FI_Kind = RSI_NPTXC.FIKIND_AVOIRISS) THEN
              CategoryValue := RSI_NPTXC.TXGROUP_80; -- ФИСС фондовые
            elsif (vBaseRecFinIstr.t_FI_Kind = RSI_RSB_FIInstr.FIKIND_INDEX) THEN
              if(vBaseRecFinIstr.t_Settlement_Code = 2 ) then  /*фондовый*/
                CategoryValue := RSI_NPTXC.TXGROUP_80; -- ФИСС фондовые
              else
                CategoryValue := RSI_NPTXC.TXGROUP_90; -- ФИСС не фондовые
              end if;
            else
              CategoryValue := RSI_NPTXC.TXGROUP_90; -- ФИСС не фондовые
            end if;
            
         else
            RETURN -1;
         end if;

       ELSE
          -- дату не проверяем, т.к. не исторична.
          SELECT Attr.t_NumInList INTO CategoryValue
            FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
           WHERE     AtCor.t_ObjectType = RSI_NPTXC.OBJTYPE_AVOIRISS
                 AND AtCor.t_GroupID    = RSI_NPTXC.TXAVR_ATTR_TXGROUP
                 AND AtCor.t_Object     = LPAD( pFIID, 10, '0' )
                 AND Attr.t_AttrID      = AtCor.t_AttrID
                 AND Attr.t_ObjectType  = AtCor.t_ObjectType
                 AND Attr.t_GroupID     = AtCor.t_GroupID;
       END IF;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN
         BEGIN
           BEGIN
              SELECT * INTO vRecFinIstr
                FROM DFININSTR_DBT
               WHERE T_FIID = pFIID;

           EXCEPTION
              WHEN NO_DATA_FOUND THEN
                 RETURN -1;
           END;

           if (vRecFinIstr.t_FI_Kind = RSI_NPTXC.FIKIND_AVOIRISS) THEN
              v_AvrRoot := RSI_rsb_fiinstr.FI_AvrKindsGetRoot( vRecFinIstr.t_FI_Kind, vRecFinIstr.t_AvoirKind );

              if( v_AvrRoot = RSI_rsb_fiinstr.AVOIRKIND_SHARE OR
                  v_AvrRoot = RSI_rsb_fiinstr.AVOIRKIND_INVESTMENT_SHARE OR
                  v_AvrRoot = RSI_rsb_fiinstr.AVOIRKIND_DEPOS_RECEIPT
                ) then
                 CategoryValue := RSI_NPTXC.TXGROUP_10; -- акция

              elsif( v_AvrRoot = RSI_rsb_fiinstr.AVOIRKIND_BOND ) then
                If(vRecFinIstr.t_AvoirKind = RSI_rsb_fiinstr. AVOIRKIND_BOND_GKO or
                  vRecFinIstr.t_AvoirKind = RSI_rsb_fiinstr. AVOIRKIND_BOND_OFZ    or
                  vRecFinIstr.t_AvoirKind = RSI_rsb_fiinstr.AVOIRKIND_BOND_USSR or
                  RSI_rsb_fiinstr.FI_AvrKindsEQ(vRecFinIstr.t_FI_Kind, RSI_RSB_FIInstr. AVOIRKIND_BOND_PARTY , vRecFinIstr.t_AvoirKind) = 1 or
                  RSI_rsb_fiinstr.FI_AvrKindsEQ(vRecFinIstr.t_FI_Kind, RSI_RSB_FIInstr. AVOIRKIND_BOND_MUNICIPAL , vRecFinIstr.t_AvoirKind) = 1
                 )then
               CategoryValue := RSI_NPTXC.TXGROUP_40;
                 else
                 CategoryValue := RSI_NPTXC.TXGROUP_20; --Облигации обыкновенные
                End if;


              elsif( v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BILL ) then
                 CategoryValue := RSI_NPTXC.TXGROUP_60; --Векселя

              elsif( vRecFinIstr.t_AvoirKind = RSI_RSB_FIInstr.AVOIRKIND_DEPOSIT_CERTIFICATE ) then
                 CategoryValue := RSI_NPTXC.TXGROUP_70; --Сертификаты

              elsif( vRecFinIstr.t_AvoirKind = RSI_RSB_FIInstr.AVOIRKIND_SAVING_CERTIF ) then
                 CategoryValue := RSI_NPTXC.TXGROUP_70; --Сертификаты

              else
                 RETURN -1;
              end if;

           elsif (vRecFinIstr.t_FI_Kind = RSI_NPTXC.FIKIND_DERIVATIVE) THEN
              BEGIN
                 SELECT * INTO vBaseRecFinIstr
                   FROM DFININSTR_DBT
                  WHERE T_FIID = vRecFinIstr.t_FaceValueFI;

              EXCEPTION
                 WHEN NO_DATA_FOUND THEN
                    RETURN -1;
              END;

              if (vBaseRecFinIstr.t_FI_Kind = RSI_NPTXC.FIKIND_AVOIRISS) THEN
                CategoryValue := RSI_NPTXC.TXGROUP_80; -- ФИСС фондовые
              elsif (vBaseRecFinIstr.t_FI_Kind = RSI_RSB_FIInstr.FIKIND_INDEX) THEN
                if(vBaseRecFinIstr.t_Settlement_Code = 2 ) then  /*фондовый*/
                  CategoryValue := RSI_NPTXC.TXGROUP_80; -- ФИСС фондовые
                else
                  CategoryValue := RSI_NPTXC.TXGROUP_90; -- ФИСС не фондовые
                end if;
              else
                CategoryValue := RSI_NPTXC.TXGROUP_90; -- ФИСС не фондовые
              end if;

           else
              RETURN -1;
           end if;
         END;

      END;

      RETURN to_number(CategoryValue);
   END; -- GetPaperTaxGroupNPTX

   PROCEDURE DeleteNdrForRecalc( pDocID IN NUMBER, pBegRecalcDate IN DATE, pEndRecalcDate IN DATE, pClient IN NUMBER, pIIS IN NUMBER DEFAULT 0, pFIID IN NUMBER DEFAULT -1, pContract IN NUMBER DEFAULT 0 )
    IS
      v_BCID NUMBER;
      v_LucreStartTaxPeriod NUMBER := 0;
   BEGIN
      v_LucreStartTaxPeriod := RSI_NPTO.GetLucreStartTaxPeriod(); 

      FOR Obj IN (SELECT OBJ.*
                    FROM DNPTXOBJ_DBT OBJ
                   WHERE OBJ.t_Date <= pEndRecalcDate
                     AND OBJ.t_Date >= pBegRecalcDate
                     AND OBJ.t_Client = pClient
                     AND CheckContrIIS(OBJ.t_Analitic6) = pIIS
                     AND (pContract IS NULL OR pContract <= 0 OR OBJ.t_AnaliticKind6 != SERVISE_CONTR OR OBJ.t_Analitic6 in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract))
                     AND OBJ.t_User != 'X'
                     AND OBJ.t_AnaliticKind1 not in(1095, 1098, 1115, 1120, 1125, 1130)
                     AND OBJ.t_Analitic3 = case when pFIID != -1 and OBJ.t_AnaliticKind3 = RSI_NPTXC.TXOBJ_KIND3010 then pFIID else OBJ.t_Analitic3 end
                     AND (OBJ.T_OUTSYSTCODE = CHR(1) OR OBJ.T_OUTSYSTCODE IS NULL OR OBJ.T_OUTSYSTCODE = 'др.ПУ')
                     AND (EXTRACT(YEAR FROM pEndRecalcDate) < v_LucreStartTaxPeriod 
                          OR OBJ.T_KIND NOT IN (RSI_NPTXC.TXOBJ_MATERIAL, 
                                                RSI_NPTXC.TXOBJ_BASEMATERIAL, 
                                                RSI_NPTXC.TXOBJ_DUEMATERIAL, 
                                                RSI_NPTXC.TXOBJ_MATERIAL_SEC, 
                                                RSI_NPTXC.TXOBJ_PLUSG_2640, 
                                                RSI_NPTXC.TXOBJ_PLUSG_2641)
                          OR (EXISTS(SELECT 1
                                       FROM DNPTXOP_DBT OPER
                                      WHERE OPER.T_DOCKIND = RSI_NPTXC.DL_CALCNDFL
                                        AND OPER.T_ID =  pDocID
                                        AND OPER.T_RECALC = 'X'
                                        AND OPER.T_SUBKIND_OPERATION <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE 
                                    )
                              AND NOT EXISTS(SELECT 1
                                               FROM DNPTXOBDC_DBT DC, DNPTXOP_DBT Oper
                                              WHERE DC.T_ObjID = Obj.t_ObjID
                                                AND Oper.T_ID = DC.T_DocID
                                                AND Oper.T_DocKind = RSI_NPTXC.DL_CALCNDFL
                                                AND Oper.T_SubKind_Operation = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE
                                            )
                             )
                         )
                     AND NVL( (select 1
                                 from DNPTXOP_DBT Oper
                                where OPER.T_DOCKIND = 4605
                                  and OPER.T_ID =  pDocID
                                  and (EXTRACT(YEAR FROM pEndRecalcDate) < v_LucreStartTaxPeriod OR OPER.T_SUBKIND_OPERATION <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE)
                                  and (   (    (OPER.T_RECALC != 'X' or OPER.T_CALCNDFL != 'X' ) 
                                           and (OBJ.T_LEVEL in(1,2,3) OR (OBJ.T_LEVEL = 4 AND OBJ.T_KIND = case when pIIS != 1  then RSI_NPTXC.TXOBJ_GENERAL else RSI_NPTXC.TXOBJ_GENERAL_IIS end) )
                                          )
                                       or (OPER.T_RECALC = 'X' and OPER.T_CALCNDFL = 'X' and OBJ.T_LEVEL in(1, 2, 3, 4, 5, 6, 7))
                                       or (OPER.T_IIS = 'X' and OPER.T_SUBKIND_OPERATION IN (RSI_NPTXC.DL_TXBASECALC_OPTYPE_ENDYEAR, RSI_NPTXC.DL_TXBASECALC_OPTYPE_CLOSE_IIS) and OBJ.T_LEVEL in(1, 2, 3, 4, 5, 6, 7))
                                      )
                              ),0) = 1
                     AND (select count(1)
                            from DNPTXOBDC_DBT DC, DNPTXOP_DBT Oper
                           where DC.T_ObjID = Obj.t_ObjID
                             and DC.T_DocID = Oper.T_ID
                             and Oper.T_Dockind = 4608) <= 0
                     AND NOT EXISTS(SELECT 1 
                                      FROM DNPTXOBDC_DBT DC, doproper_dbt opr
                                     WHERE DC.T_ObjID = Obj.t_ObjID
                                       AND OBJ.t_AnaliticKind1 = 1020
                                       AND opr.t_ID_Operation = DC.T_DocID
                                       AND opr.t_DocKind = RSB_SECUR.DL_RETIREMENT_OWN
                                   )         
                     /*DEF-31726 Неверный расчет финансового результата и суммы возврата НДФЛ*/
                     /*+ Объекты НДР 1 уровня даты которых не входят в период, но они сформированы по операциям зачисления у которых дата входит в период пересчета и установлен признак учитывать в НУ*/
                  UNION 
                  SELECT OBJ.*
                    FROM DNPTXOBJ_DBT OBJ
                   WHERE (OBJ.t_Date >= pEndRecalcDate
                     OR OBJ.t_Date <= pBegRecalcDate) --дата объектов не входит в период пересчета
                     AND OBJ.t_Client = pClient
                     AND CheckContrIIS(OBJ.t_Analitic6) = pIIS
                     AND (pContract IS NULL OR pContract <= 0 OR OBJ.t_AnaliticKind6 != SERVISE_CONTR OR OBJ.t_Analitic6 in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract))
                     AND OBJ.t_User != 'X'
                     AND OBJ.t_AnaliticKind1  = RSI_NPTXC.TXOBJ_KIND1070
                     AND OBJ.T_LEVEL = 1 
                     AND OBJ.t_Analitic3 = case when pFIID != -1 and OBJ.t_AnaliticKind3 = RSI_NPTXC.TXOBJ_KIND3010 then pFIID else OBJ.t_Analitic3 end
                     AND (OBJ.T_OUTSYSTCODE = CHR(1) OR OBJ.T_OUTSYSTCODE IS NULL OR OBJ.T_OUTSYSTCODE = 'др.ПУ')
                     AND NVL( (select 1
                                       from DNPTXOP_DBT Oper
                                     where OPER.T_DOCKIND = 4605
                                        and OPER.T_ID =  pDocID
                                        and (EXTRACT(YEAR FROM pEndRecalcDate) < v_LucreStartTaxPeriod OR OPER.T_SUBKIND_OPERATION <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE)
                                        and ((    (OPER.T_RECALC != 'X' or OPER.T_CALCNDFL != 'X' ) 
                                              and (OBJ.T_LEVEL in(1,2,3) OR (OBJ.T_LEVEL = 4 AND OBJ.T_KIND = case when pIIS != 1  then RSI_NPTXC.TXOBJ_GENERAL else RSI_NPTXC.TXOBJ_GENERAL_IIS end))
                                             ) 
                                             or (OPER.T_RECALC = 'X' and OPER.T_CALCNDFL = 'X')
                                             or (OPER.T_IIS = 'X' and OPER.T_SUBKIND_OPERATION IN (RSI_NPTXC.DL_TXBASECALC_OPTYPE_ENDYEAR, RSI_NPTXC.DL_TXBASECALC_OPTYPE_CLOSE_IIS) and OBJ.T_LEVEL in(1, 2, 3, 4, 5, 6, 7))
                                            )
                                    ),0) = 1
                     AND (select count(1)
                            from DNPTXOBDC_DBT DC, DNPTXOP_DBT Oper
                           where DC.T_ObjID = Obj.t_ObjID
                             and DC.T_DocID = Oper.T_ID
                             and Oper.T_Dockind = 4608) <= 0 
                     /*по поперациям зачисления ц\б входящим в период пересчета с признаком учитывать в НДФЛ*/
                     AND (SELECT COUNT (1)
                            FROM DDL_TICK_DBT TICK
                           WHERE TICK.t_dealid = OBJ.t_Analitic1
                             AND TICK.t_bofficekind = RSB_SECUR.OBJTYPE_AVRWRT 
                             AND TICK.T_CLIENTID = OBJ.t_Client
                             AND RSB_SECUR.IsAvrWrtIn(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(TICK.t_DealType, TICK.t_BofficeKind))) > 0 /*зачисление*/
                             AND TICK.T_FLAG3 = CHR (88)
                             AND TICK.t_dealdate BETWEEN pBegRecalcDate and pEndRecalcDate ) > 0
                   UNION
                   SELECT OBJ.*
                    FROM DNPTXOBJ_DBT OBJ
                   WHERE EXTRACT(YEAR FROM pEndRecalcDate) >= v_LucreStartTaxPeriod
                     AND OBJ.t_Date <= pEndRecalcDate
                     AND OBJ.t_Date >= pBegRecalcDate
                     AND OBJ.t_Client = pClient
                     AND OBJ.t_Kind IN (RSI_NPTXC.TXOBJ_MATERIAL)
                     AND OBJ.t_User != 'X'
                     AND (OBJ.T_OUTSYSTCODE = CHR(1) OR OBJ.T_OUTSYSTCODE IS NULL OR OBJ.T_OUTSYSTCODE = 'др.ПУ')
                     AND NVL( (select 1
                                 from DNPTXOP_DBT Oper
                                where OPER.T_DOCKIND = 4605
                                  and OPER.T_ID =  pDocID
                                  and OPER.T_SUBKIND_OPERATION = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE
                                  and OPER.T_RECALC = 'X'
                              ),0) = 1
                     AND (select count(1)
                            from DNPTXOBDC_DBT DC, DNPTXOP_DBT Oper
                           where DC.T_ObjID = Obj.t_ObjID
                             and DC.T_DocID = Oper.T_ID
                             and Oper.T_Dockind = 4608) <= 0
                     AND NOT EXISTS(SELECT 1 
                                      FROM DNPTXOBDC_DBT DC, doproper_dbt opr
                                     WHERE DC.T_ObjID = Obj.t_ObjID
                                       AND OBJ.t_AnaliticKind1 = 1020
                                       AND opr.t_ID_Operation = DC.T_DocID
                                       AND opr.t_DocKind = RSB_SECUR.DL_RETIREMENT_OWN
                                   )         

                  )
      LOOP

         INSERT INTO DNPTXOBJBC_DBT (
                                     T_DATE         ,
                                     T_CLIENT       ,
                                     T_DIRECTION    ,
                                     T_LEVEL        ,
                                     T_KIND         ,
                                     T_SUM          ,
                                     T_CUR          ,
                                     T_SUM0         ,
                                     T_ANALITICKIND1,
                                     T_ANALITIC1    ,
                                     T_ANALITICKIND2,
                                     T_ANALITIC2    ,
                                     T_ANALITICKIND3,
                                     T_ANALITIC3    ,
                                     T_ANALITICKIND4,
                                     T_ANALITIC4    ,
                                     T_ANALITICKIND5,
                                     T_ANALITIC5    ,
                                     T_ANALITICKIND6,
                                     T_ANALITIC6    ,
                                     T_COMMENT      ,
                                     T_DOCID        ,
                                     T_FROMOUTSYST  ,
                                     T_OUTSYSTCODE  ,
                                     T_OUTOBJID     ,
                                     T_SOURCEOBJID  ,
                                     T_TECHNICAL    ,
                                     T_TAXPERIOD    ,
                                     T_TRANSFDATE   ,
                                     T_TRANSFKIND   ,
                                     T_HOLDING_PERIOD,
                                     T_CHANGECODE
                                    )
                             VALUES (
                                     Obj.T_DATE         , -- T_DATE
                                     Obj.T_CLIENT       , -- T_CLIENT
                                     Obj.T_DIRECTION    , -- T_DIRECTION
                                     Obj.T_LEVEL        , -- T_LEVEL
                                     Obj.T_KIND         , -- T_KIND
                                     Obj.T_SUM          , -- T_SUM
                                     Obj.T_CUR          , -- T_CUR
                                     Obj.T_SUM0         , -- T_SUM0
                                     Obj.T_ANALITICKIND1, -- T_ANALITICKIND1
                                     Obj.T_ANALITIC1    , -- T_ANALITIC1
                                     Obj.T_ANALITICKIND2, -- T_ANALITICKIND2
                                     Obj.T_ANALITIC2    , -- T_ANALITIC2
                                     Obj.T_ANALITICKIND3, -- T_ANALITICKIND3
                                     Obj.T_ANALITIC3    , -- T_ANALITIC3
                                     Obj.T_ANALITICKIND4, -- T_ANALITICKIND4
                                     Obj.T_ANALITIC4    , -- T_ANALITIC4
                                     Obj.T_ANALITICKIND5, -- T_ANALITICKIND5
                                     Obj.T_ANALITIC5    , -- T_ANALITIC5
                                     Obj.T_ANALITICKIND6, -- T_ANALITICKIND6
                                     Obj.T_ANALITIC6    , -- T_ANALITIC6
                                     Obj.T_COMMENT      , -- T_COMMENT
                                     pDocID             ,
                                     Obj.T_FROMOUTSYST  ,
                                     Obj.T_OUTSYSTCODE  ,
                                     Obj.T_OUTOBJID     ,
                                     Obj.T_SOURCEOBJID  ,
                                     Obj.T_TECHNICAL    ,
                                     Obj.T_TAXPERIOD    ,
                                     Obj.T_TRANSFDATE   ,
                                     Obj.T_TRANSFKIND   ,
                                     Obj.T_HOLDING_PERIOD,
                                     Obj.T_CHANGECODE
                                    ) RETURNING t_ObjID INTO v_BCID;

         FOR DC IN (SELECT * FROM DNPTXOBDC_DBT DC WHERE DC.T_ObjID = Obj.t_ObjID)
         LOOP
            INSERT INTO DNPTXOBDCBC_DBT (
                                         T_DOCID,
                                         T_STEP,
                                         T_OBJID
                                        )
                                 VALUES (
                                         DC.T_DOCID,
                                         DC.T_STEP,
                                         v_BCID
                                        );
         END LOOP;

         DELETE FROM DNPTXOBDC_DBT DC WHERE DC.T_ObjID = Obj.t_ObjID;
         DELETE FROM DNPTXOBJ_DBT WHERE T_ObjID = Obj.t_ObjID;

      END LOOP;
   END; -- DeleteNdrForRecalc

   PROCEDURE RecoilDeleteNdrForRecalc( pDocID IN NUMBER ) 
    IS 
      v_ObjID NUMBER; 
   BEGIN 
      FOR BC IN (SELECT * FROM DNPTXOBJBC_DBT BC WHERE BC.T_DocID = pDocID) 
      LOOP 
         INSERT INTO DNPTXOBJ_DBT ( 
                                   T_DATE         ,     -- T_DATE 
                                   T_CLIENT       ,     -- T_CLIENT 
                                   T_DIRECTION    ,     -- T_DIRECTION 
                                   T_LEVEL        ,     -- T_LEVEL 
                                   T_USER         ,     -- T_USER 
                                   T_KIND         ,     -- T_KIND 
                                   T_SUM          ,     -- T_SUM 
                                   T_CUR          ,     -- T_CUR 
                                   T_SUM0         ,     -- T_SUM0 
                                   T_ANALITICKIND1,     -- T_ANALITICKIND1 
                                   T_ANALITIC1    ,     -- T_ANALITIC1 
                                   T_ANALITICKIND2,     -- T_ANALITICKIND2 
                                   T_ANALITIC2    ,     -- T_ANALITIC2 
                                   T_ANALITICKIND3,     -- T_ANALITICKIND3 
                                   T_ANALITIC3    ,     -- T_ANALITIC3 
                                   T_ANALITICKIND4,     -- T_ANALITICKIND4 
                                   T_ANALITIC4    ,     -- T_ANALITIC4 
                                   T_ANALITICKIND5,     -- T_ANALITICKIND5 
                                   T_ANALITIC5    ,     -- T_ANALITIC5 
                                   T_ANALITICKIND6,     -- T_ANALITICKIND6 
                                   T_ANALITIC6    ,     -- T_ANALITIC6 
                                   T_COMMENT      ,     -- T_COMMENT
                                   T_TECHNICAL    ,     -- T_TECHNICAL 
                                   T_TAXPERIOD    ,
                                   T_OUTSYSTCODE  ,
                                   T_TRANSFDATE   ,
                                   T_TRANSFKIND   ,
                                   T_HOLDING_PERIOD,
                                   T_CHANGECODE
                                  ) 
                           VALUES ( 
                                   BC.t_Date,           -- T_DATE 
                                   BC.t_Client,         -- T_CLIENT 
                                   BC.t_Direction,      -- T_DIRECTION 
                                   BC.t_Level,          -- T_LEVEL 
                                   chr(0),              -- T_USER 
                                   BC.t_Kind,           -- T_KIND 
                                   BC.t_Sum,            -- T_SUM 
                                   BC.t_Cur,            -- T_CUR 
                                   BC.t_SUM0,           -- T_SUM0 
                                   BC.t_AnaliticKind1,  -- T_ANALITICKIND1 
                                   BC.t_Analitic1,      -- T_ANALITIC1 
                                   BC.t_AnaliticKind2,  -- T_ANALITICKIND2 
                                   BC.t_Analitic2,      -- T_ANALITIC2 
                                   BC.t_AnaliticKind3,  -- T_ANALITICKIND3 
                                   BC.t_Analitic3,      -- T_ANALITIC3 
                                   BC.t_AnaliticKind4,  -- T_ANALITICKIND4 
                                   BC.t_Analitic4,      -- T_ANALITIC4 
                                   BC.t_AnaliticKind5,  -- T_ANALITICKIND5 
                                   BC.t_Analitic5,      -- T_ANALITIC5 
                                   BC.t_AnaliticKind6,  -- T_ANALITICKIND6 
                                   BC.t_Analitic6,      -- T_ANALITIC6 
                                   BC.t_Comment,        -- T_COMMENT
                                   BC.t_Technical,      -- T_TECHNICAL 
                                   BC.t_TaxPeriod,
                                   BC.t_OutSystCode,
                                   BC.t_TransfDate,
                                   BC.t_TransfKind,
                                   BC.T_HOLDING_PERIOD,
                                   BC.T_CHANGECODE
                                  ) RETURNING t_ObjID INTO v_ObjID; 

            INSERT INTO DNPTXOBDC_DBT ( 
                                       T_DOCID, 
                                       T_STEP, 
                                       T_OBJID 
                                      ) 
                               select  T_DOCID, 
                                       T_STEP, 
                                       v_ObjID 
                                       from DNPTXOBDCBC_DBT DC WHERE DC.T_ObjID = BC.t_ObjID ; 


         DELETE FROM DNPTXOBDCBC_DBT DC WHERE DC.T_ObjID = BC.t_ObjID; 
         DELETE FROM DNPTXOBJBC_DBT WHERE T_ObjID = BC.t_ObjID; 

      END LOOP; 
   END; -- RecoilDeleteNdrForRecalc 

   /**
   @brief Восстанавливает обекты НДР с признаком "Технческий расчет" = "Да" при откате операии расчета НОБ
   @param [in] DocID ID текущей операции расчета НОБ
   */
   PROCEDURE RecoilDeleteTechnicalNdr( pDocID IN NUMBER )
   IS
      v_ObjID NUMBER;
   BEGIN
      FOR BC IN (SELECT * FROM DNPTXOBJBC_DBT BC WHERE BC.T_DocID = pDocID AND BC.t_Technical = 'X') 
      LOOP 
         INSERT INTO DNPTXOBJ_DBT ( 
                                   T_DATE         ,     -- T_DATE 
                                   T_CLIENT       ,     -- T_CLIENT 
                                   T_DIRECTION    ,     -- T_DIRECTION 
                                   T_LEVEL        ,     -- T_LEVEL 
                                   T_USER         ,     -- T_USER 
                                   T_KIND         ,     -- T_KIND 
                                   T_SUM          ,     -- T_SUM 
                                   T_CUR          ,     -- T_CUR 
                                   T_SUM0         ,     -- T_SUM0 
                                   T_ANALITICKIND1,     -- T_ANALITICKIND1 
                                   T_ANALITIC1    ,     -- T_ANALITIC1 
                                   T_ANALITICKIND2,     -- T_ANALITICKIND2 
                                   T_ANALITIC2    ,     -- T_ANALITIC2 
                                   T_ANALITICKIND3,     -- T_ANALITICKIND3 
                                   T_ANALITIC3    ,     -- T_ANALITIC3 
                                   T_ANALITICKIND4,     -- T_ANALITICKIND4 
                                   T_ANALITIC4    ,     -- T_ANALITIC4 
                                   T_ANALITICKIND5,     -- T_ANALITICKIND5 
                                   T_ANALITIC5    ,     -- T_ANALITIC5 
                                   T_ANALITICKIND6,     -- T_ANALITICKIND6 
                                   T_ANALITIC6    ,     -- T_ANALITIC6 
                                   T_COMMENT      ,     -- T_COMMENT
                                   T_TECHNICAL    ,     -- T_TECHNICAL 
                                   T_TAXPERIOD    ,     -- T_TAXPERIOD
                                   T_OUTSYSTCODE  ,
                                   T_TRANSFDATE   ,
                                   T_TRANSFKIND   ,
                                   T_HOLDING_PERIOD,
                                   T_CHANGECODE
                                  ) 
                           VALUES ( 
                                   BC.t_Date,           -- T_DATE 
                                   BC.t_Client,         -- T_CLIENT 
                                   BC.t_Direction,      -- T_DIRECTION 
                                   BC.t_Level,          -- T_LEVEL 
                                   chr(0),              -- T_USER 
                                   BC.t_Kind,           -- T_KIND 
                                   BC.t_Sum,            -- T_SUM 
                                   BC.t_Cur,            -- T_CUR 
                                   BC.t_SUM0,           -- T_SUM0 
                                   BC.t_AnaliticKind1,  -- T_ANALITICKIND1 
                                   BC.t_Analitic1,      -- T_ANALITIC1 
                                   BC.t_AnaliticKind2,  -- T_ANALITICKIND2 
                                   BC.t_Analitic2,      -- T_ANALITIC2 
                                   BC.t_AnaliticKind3,  -- T_ANALITICKIND3 
                                   BC.t_Analitic3,      -- T_ANALITIC3 
                                   BC.t_AnaliticKind4,  -- T_ANALITICKIND4 
                                   BC.t_Analitic4,      -- T_ANALITIC4 
                                   BC.t_AnaliticKind5,  -- T_ANALITICKIND5 
                                   BC.t_Analitic5,      -- T_ANALITIC5 
                                   BC.t_AnaliticKind6,  -- T_ANALITICKIND6 
                                   BC.t_Analitic6,      -- T_ANALITIC6 
                                   BC.t_Comment,        -- T_COMMENT
                                   BC.t_Technical,      -- T_TECHNICAL 
                                   BC.t_TaxPeriod,
                                   BC.t_OutSystCode,
                                   BC.t_TransfDate,
                                   BC.t_TransfKind,
                                   BC.T_HOLDING_PERIOD,
                                   BC.T_CHANGECODE
                                  ) RETURNING t_ObjID INTO v_ObjID; 

            INSERT INTO DNPTXOBDC_DBT ( 
                                       T_DOCID, 
                                       T_STEP, 
                                       T_OBJID 
                                      ) 
                               select  T_DOCID, 
                                       T_STEP, 
                                       v_ObjID 
                                       from DNPTXOBDCBC_DBT DC WHERE DC.T_ObjID = BC.t_ObjID ; 


         DELETE FROM DNPTXOBDCBC_DBT DC WHERE DC.T_ObjID = BC.t_ObjID; 
         DELETE FROM DNPTXOBJBC_DBT WHERE T_ObjID = BC.t_ObjID; 

      END LOOP;
   END; -- RecoilDeleteTechnicalNdr

   -- Проверка наличия какого либо значения категории за период дат
   FUNCTION IsExistsAnyAttr( ObjectType IN NUMBER, Object IN VARCHAR2, GroupID IN NUMBER, Date1 IN DATE, Date2 IN DATE ) RETURN NUMBER
   IS
      v_RetVal NUMBER;
      v_Count  NUMBER;
   BEGIN
      v_RetVal := 0; /*Нет*/

      SELECT count(1) INTO v_Count
        FROM dobjatcor_dbt AtCor
       WHERE AtCor.t_ObjectType = ObjectType
         AND AtCor.t_GroupID    = GroupID
         AND AtCor.t_Object     = Object
         AND ((AtCor.t_ValidToDate > Date1 and AtCor.t_ValidToDate <= Date2) or
              (AtCor.t_ValidFromDate >= Date1 and AtCor.t_ValidFromDate < Date2) or
              (AtCor.t_ValidToDate = TO_DATE('31.12.9999','DD.MM.YYYY') and AtCor.t_ValidFromDate <= Date2));

      IF( v_Count > 0 ) THEN
         v_RetVal := 1; /*Да*/
      END IF;

      RETURN v_RetVal;
   END; -- IsExistsAnyAttr

   -- Проверка наличия значения категории за период дат
   FUNCTION IsExistsAttr( ObjectType IN NUMBER, Object IN VARCHAR2, GroupID IN NUMBER, Date1 IN DATE, Date2 IN DATE, NumInList IN VARCHAR2 ) RETURN NUMBER
   IS
      v_RetVal NUMBER;
      v_Count  NUMBER;
   BEGIN
      v_RetVal := 0; /*Нет*/

      SELECT count(1) INTO v_Count
        FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
       WHERE AtCor.t_ObjectType = ObjectType
         AND AtCor.t_GroupID    = GroupID
         AND AtCor.t_Object     = Object
         AND ((AtCor.t_ValidToDate > Date1 and AtCor.t_ValidToDate <= Date2) or
              (AtCor.t_ValidFromDate >= Date1 and AtCor.t_ValidFromDate < Date2) or
              (AtCor.t_ValidToDate = TO_DATE('31.12.9999','DD.MM.YYYY') and AtCor.t_ValidFromDate <= Date2))
         AND Attr.t_AttrID      = AtCor.t_AttrID
         AND Attr.t_ObjectType  = AtCor.t_ObjectType
         AND Attr.t_GroupID     = AtCor.t_GroupID
         AND Attr.t_NumInList   = NumInList;

      IF( v_Count > 0 ) THEN
         v_RetVal := 1; /*Да*/
      END IF;

      RETURN v_RetVal;
   END; -- IsExistsAttr

   -- Проверка наличия значения категории за весь период дат
   FUNCTION IsExistsAttrAllDat( ObjectType IN NUMBER, Object IN VARCHAR2, GroupID IN NUMBER, Date1 IN DATE, Date2 IN DATE, NumInList IN VARCHAR2 ) RETURN NUMBER
   IS
      v_RetVal NUMBER;
      v_Count  NUMBER;
   BEGIN
      v_RetVal := 0; /*Нет*/

      SELECT count(1) INTO v_Count
        FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
       WHERE AtCor.t_ObjectType = ObjectType
         AND AtCor.t_GroupID    = GroupID
         AND AtCor.t_Object     = Object
         AND ((AtCor.t_ValidFromDate <= Date1) and ((AtCor.t_ValidToDate > Date2) or (AtCor.t_ValidToDate = TO_DATE('31.12.9999','DD.MM.YYYY'))))
         AND Attr.t_AttrID      = AtCor.t_AttrID
         AND Attr.t_ObjectType  = AtCor.t_ObjectType
         AND Attr.t_GroupID     = AtCor.t_GroupID
         AND Attr.t_NumInList   = NumInList;

      IF( v_Count > 0 ) THEN
         v_RetVal := 1; /*Да*/
      END IF;

      RETURN v_RetVal;
   END; -- IsExistsAttrAllDat

   -- Проверка что доход льготный
   FUNCTION IsFavourIncome( DDS IN DATE, DDB IN DATE, FIID IN NUMBER ) RETURN NUMBER
   IS
      v_RetVal      NUMBER;
      v_FI_Kind     NUMBER;
      v_AvoirKind   NUMBER;
      v_Issuer      NUMBER;
      v_FaceValueFI NUMBER;
      v_AvrRoot     NUMBER;
      v_Country     VARCHAR2(3);
      v_Rate        DRATEDEF_DBT%ROWTYPE;
      v_ExRate      NUMBER;
      v_NumInList   dobjattr_dbt.t_NumInList%TYPE;
   BEGIN

      v_RetVal := 0; /*Нет*/

      IF( (DDS > TO_DATE('01.01.2016','DD.MM.YYYY')) and ((MONTHS_BETWEEN(DDS, DDB)/12) > 1) ) THEN /*проверка чтобы отсечь "тяжёлые" проверки для большинства случаев*/

         SELECT t_FI_Kind, t_AvoirKind, t_Issuer, t_FaceValueFI INTO v_FI_Kind, v_AvoirKind, v_Issuer, v_FaceValueFI
           FROM dfininstr_dbt
          WHERE t_FIID = FIID;

         IF( v_FI_Kind = RSI_RSB_FIInstr.FIKIND_AVOIRISS ) THEN

            v_AvrRoot := RSI_RSB_FIInstr.FI_AvrKindsGetRoot(v_FI_Kind, v_AvoirKind);

            IF( (RateTypes.MinRate = 0) or (RateTypes.MinRate is null) ) THEN -- т.е., если первый вход и ещё ничего не закачивали
               GetSettingsTax();
            END IF;

            IF( (SPGetRate(FIID, v_FaceValueFI, RateTypes.MinRate, DDS, DDS-DDB-1, v_Rate, CHR(1), 0, 0, 1) = 0 ) OR
                (SPGetRate(FIID, v_FaceValueFI, RateTypes.MaxRate, DDS, DDS-DDB-1, v_Rate, CHR(1), 0, 0, 1) = 0 )
              ) THEN
               v_ExRate := 1;
            ELSE
               v_ExRate := 0;
            END IF;

            IF( (v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_SHARE) and
                (GetCountryParty(v_Issuer, v_Country) = 'RUS') and
                (DDB > TO_DATE('01.01.2011','DD.MM.YYYY')) and
                ((MONTHS_BETWEEN(DDS, DDB)/12) > 5) and
                (v_ExRate = 0) AND
                (/*(IsExistsAnyAttr(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS) = 0) or -- не задана*/
                 ((IsExistsAttrAllDat(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '1') = 1) and -- задана "Акции российских эмитентов"
                  (IsExistsAttr(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '2') = 0) and -- весь
                  (IsExistsAttr(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '3') = 0)     -- период
                 )
                )
              ) THEN
               v_RetVal := 1; /*Да*/
            ELSIF( (v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_SHARE) and
                   (GetCountryParty(v_Issuer, v_Country) = 'RUS') and
                   (DDB > TO_DATE('01.01.2011','DD.MM.YYYY')) and
                   ((MONTHS_BETWEEN(DDS, DDB)/12) > 5) and
                   (/*(IsExistsAnyAttr(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS) = 0) or -- не задана*/
                    ((IsExistsAttrAllDat(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '2') = 1) and -- задана "Не более 50% активов недвижимое имущество в РФ"
                     (IsExistsAttr(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '1') = 0) and -- весь
                     (IsExistsAttr(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '3') = 0)     -- период
                    )
                   )
                 ) THEN
               v_RetVal := 2; /*Да*/
            ELSIF( (Market1date(FIID, DDS) = to_number(RSI_NPTXC.NPTX_FI_CIRCULATE) ) and
                   (GetCountryParty(v_Issuer, v_Country) = 'RUS') and
                   ((MONTHS_BETWEEN(DDS, DDB)/12) > 1) and
                   ((IsExistsAttrAllDat(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '3') = 1) and -- задана "Ц/б высокотехнологичного сектора экономики"
                    (IsExistsAttr(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '1') = 0) and -- весь
                    (IsExistsAttr(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '2') = 0)     -- период
                   )
                 ) THEN
               v_RetVal := 3; /*Да*/
            ELSIF( (Market1date(FIID, DDB) = to_number(RSI_NPTXC.NPTX_FI_NOCIRCULATE) ) and
                   (Market1date(FIID, DDS) = to_number(RSI_NPTXC.NPTX_FI_CIRCULATE) ) and
                   (GetCountryParty(v_Issuer, v_Country) = 'RUS') and
                   ((MONTHS_BETWEEN(DDS, DDB)/12) > 1)
                 ) THEN
               RSI_RSB_FIInstr.FI_FindObjAttrOnDate(FIID, DDS, RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, v_NumInList);
               IF( v_NumInList = '3' ) THEN -- "Ц/б высокотехнологичного сектора экономики"
                  v_RetVal := 4; /*Да*/
               END IF;
            END IF;

         END IF;
      END IF;

      return v_RetVal;
   END IsFavourIncome;

   -- Проверка что доход льготный
   FUNCTION IsFavourIncome_NPTX( DDS IN DATE, DDB IN DATE, FIID IN NUMBER ) RETURN NUMBER
   IS
      v_RetVal      NUMBER;
      v_FI_Kind     NUMBER;
      v_AvoirKind   NUMBER;
      v_Issuer      NUMBER;
      v_FaceValueFI NUMBER;
      v_AvrRoot     NUMBER;
      v_Country     VARCHAR2(3);
      v_Rate        DRATEDEF_DBT%ROWTYPE;
      v_ExRate      NUMBER;
      v_NumInList   dobjattr_dbt.t_NumInList%TYPE;
      v_TXGroup     NUMBER;
   BEGIN

      v_RetVal := 0; /*Нет*/

      IF( (MONTHS_BETWEEN(DDS, DDB)/12) > 1 ) THEN /*проверка чтобы отсечь "тяжёлые" проверки для большинства случаев*/

         SELECT t_FI_Kind, t_AvoirKind, t_Issuer, t_FaceValueFI INTO v_FI_Kind, v_AvoirKind, v_Issuer, v_FaceValueFI
           FROM dfininstr_dbt
          WHERE t_FIID = FIID;

         IF( v_FI_Kind = RSI_RSB_FIInstr.FIKIND_AVOIRISS ) THEN

            v_TXGroup := GetPaperTaxGroupNPTX(FIID);
            v_Country := GetCountryParty(v_Issuer, v_Country);

            IF( v_Country = 'RUS' and (v_TXGroup = RSI_NPTXC.TXGROUP_10 or v_TXGroup = RSI_NPTXC.TXGROUP_20) ) THEN

               v_AvrRoot := RSI_RSB_FIInstr.FI_AvrKindsGetRoot(v_FI_Kind, v_AvoirKind);

               IF( (RateTypes.MinRate = 0) or (RateTypes.MinRate is null) ) THEN -- т.е., если первый вход и ещё ничего не закачивали
                  GetSettingsTax();
               END IF;

               IF( (SPGetRate(FIID, v_FaceValueFI, RateTypes.MinRate, DDS, DDS-DDB-1, v_Rate, CHR(1), 0, 0, 1) = 0 ) OR
                   (SPGetRate(FIID, v_FaceValueFI, RateTypes.MaxRate, DDS, DDS-DDB-1, v_Rate, CHR(1), 0, 0, 1) = 0 )
                 ) THEN
                  v_ExRate := 1;
               ELSE
                  v_ExRate := 0;
               END IF;

               IF( (v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_SHARE) and
                   (v_ExRate = 0) AND
                   ((IsExistsAnyAttr(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS) = 0) or -- не задана
                    ((IsExistsAttrAllDat(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '1') = 1) and -- задана "Акции российских эмитентов"
                     (IsExistsAttr(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '2') = 0) and -- весь
                     (IsExistsAttr(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '3') = 0)     -- период
                    )
                   )
                 ) THEN
                  v_RetVal := 1; /*Да*/
               ELSIF( v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_SHARE
                    ) THEN
                  RSI_RSB_FIInstr.FI_FindObjAttrOnDate(FIID, DDS, RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, v_NumInList);
                  if( v_NumInList = '2' )then
                     v_RetVal := 2;  -- задана "Не более 50% активов недвижимое имущество в РФ"
                  end if;
               END IF;

               IF( v_RetVal = 0 )THEN
                  IF( (Market1date(FIID, DDS) = to_number(RSI_NPTXC.NPTX_FI_CIRCULATE) ) and
                      ((IsExistsAttrAllDat(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '3') = 1) and -- задана "Ц/б высокотехнологичного сектора экономики"
                       (IsExistsAttr(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '1') = 0) and -- весь
                       (IsExistsAttr(cnst.OBJTYPE_AVOIRISS, LPAD(FIID, 10, '0'), RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, DDB, DDS, '2') = 0)     -- период
                      )
                    ) THEN
                     v_RetVal := 3; /*Да*/
                  ELSIF( (Market1date(FIID, DDB) = to_number(RSI_NPTXC.NPTX_FI_NOCIRCULATE) ) and
                         (Market1date(FIID, DDS) = to_number(RSI_NPTXC.NPTX_FI_CIRCULATE) )
                       ) THEN
                     RSI_RSB_FIInstr.FI_FindObjAttrOnDate(FIID, DDS, RSI_NPTXC.TXAVR_ATTR_FAVOURINCOME, v_NumInList);
                     if( v_NumInList = '3' )then
                        v_RetVal := 4; -- "Ц/б высокотехнологичного сектора экономики"
                     end if;
                  END IF;
               END IF;

            END IF;

         END IF;
      END IF;

      return v_RetVal;
   END IsFavourIncome_NPTX;

   -- Проверяем, что корпоративная облигация с датой погашения (купона) попадает под налогообложение по ставке 35%
   FUNCTION IsCorpBondAfter2018byDrawDate( p_FIID IN NUMBER, p_DrawingDate IN DATE ) RETURN NUMBER
   IS
      v_RetVal           NUMBER := 0;
      v_AvoirKind        NUMBER;
      v_FaceValueFI      NUMBER;
      v_BegPlacementDate DATE := to_date('01.01.0001','DD.MM.YYYY');
   BEGIN

      IF p_FIID > 0 THEN

        SELECT fin.t_AvoirKind, fin.t_FaceValueFI, av.t_BegPlacementDate INTO v_AvoirKind, v_FaceValueFI, v_BegPlacementDate
          FROM dfininstr_dbt fin, davoiriss_dbt av
         WHERE fin.t_FIID = p_FIID
           AND av.t_FIID = fin.t_FIID;

        IF RSI_NPTO.GetPaperTaxGroupNPTX(p_FIID) = RSI_NPTXC.TXGROUP_20 AND
           v_AvoirKind = RSI_RSB_FIInstr.AVOIRKIND_BOND_CORPORATE_BOND AND
           v_FaceValueFI = 0 AND
           v_BegPlacementDate >= to_date('01.01.2017','DD.MM.YYYY') AND
           p_DrawingDate > to_date('01.01.2018','DD.MM.YYYY') AND
           IfMarket(p_FIID,p_DrawingDate) = 'X'
        THEN
           v_RetVal := 1;

        END IF;

      END IF;

      return v_RetVal;
   END IsCorpBondAfter2018byDrawDate;

   -- Проверяем, что корпоративная облигация с данным купоном или датой погашения попадает под налогообложение по ставке 35%
   FUNCTION IsCorpBondAfter2018( p_FIID IN NUMBER, p_WarrantNum IN VARCHAR2 ) RETURN NUMBER
   IS
      v_RetVal           NUMBER := 0;
      v_DrawingDate      DATE := to_date('01.01.0001','DD.MM.YYYY');
   BEGIN

      IF p_FIID > 0 THEN

         BEGIN
           SELECT t_DrawingDate INTO v_DrawingDate
             FROM dfiwarnts_dbt
            WHERE t_IsPartial = CHR(0)
              AND t_FIID = p_FIID
              AND t_Number = p_WarrantNum;
         EXCEPTION
            WHEN NO_DATA_FOUND THEN
              return 0;
         END;

         v_RetVal := IsCorpBondAfter2018byDrawDate(p_FIID, v_DrawingDate);
      END IF;

      return v_RetVal;
   END IsCorpBondAfter2018;

   --число дней в году
   FUNCTION GetDaysInYear( CurYear IN NUMBER ) return NUMBER
   IS
   BEGIN
     return TO_DATE('01.01.'||TO_CHAR(CurYear+1),'DD.MM.YYYY')-TO_DATE('01.01.'||TO_CHAR(CurYear),'DD.MM.YYYY');
   END;

  FUNCTION GetDaysInYearByDate( CalcDate IN DATE ) return NUMBER
  IS
  BEGIN
    return (TO_DATE('01.01.'||TO_CHAR(TO_NUMBER(TO_CHAR( CalcDate, 'YYYY'))+1),'DD.MM.YYYY')-TO_DATE('01.01.'||TO_CHAR(TO_NUMBER(TO_CHAR( CalcDate, 'YYYY'))),'DD.MM.YYYY'));
  END;

  PROCEDURE CalcBasisNKD(FIID           IN  NUMBER,
                         BasisNKD       IN  NUMBER,
                         FirstDate      IN  DATE,
                         LastDate       IN  DATE,
                         EndCoupon      IN  DATE,
                         IndexNom       IN  DAVOIRISS_DBT.T_IndexNom%TYPE,
                         RelativeIncome IN CHAR,
                         Period         OUT NUMBER,
                         DaysInYear     OUT NUMBER) AS
    cursor c_fiwarntsper is SELECT t_DRAWINGDATE
                             FROM DFIWARNTS_DBT
                            WHERE T_ISPARTIAL != 'X'
                              AND t_FIID       = FIID
                         ORDER BY T_DrawingDate;
    DrawingDate       DATE;
    LastWarntDate     DATE;
    FirstWarntDate    DATE;
    DaysInLastYear    NUMBER;
    LastWarntPer      DATE;
    FirstWarntPer     DATE;
    FirstPer          DATE;
    FirstWarntLastPer DATE;
  BEGIN

    if (BasisNKD = 0) then -- 365 в году, по календарю в месяце
      Period := LastDate - FirstDate;
      DaysInYear :=365;
    elsif (BasisNKD = 1) then -- 360 в году, 30 в месяце
      Period := RSI_RSB_FIInstr.rsNDaysp(LastDate) - RSI_RSB_FIInstr.rsNDaysp(FirstDate);
      DaysInYear :=360;
    elsif (BasisNKD = 2) then -- 360 в году, по календарю в месяце
      Period := LastDate - FirstDate;
      DaysInYear :=360;
    elsif (BasisNKD = 3) then -- 365 в году, 30 в месяце
      Period := RSI_RSB_FIInstr.rsNDaysp(LastDate) - RSI_RSB_FIInstr.rsNDaysp(FirstDate);
      DaysInYear :=365;
    elsif (BasisNKD = 4) then -- дней в году по календарю, по календарю в месяце
      Period := LastDate - FirstDate;
      if IndexNom = 'X' then
        DaysInYear := 365; -- Так в официальной формуле
      else
        DaysInYear := GetDaysInYearByDate(LastDate);
      end if;
    elsif (BasisNKD = 5) then -- дней в году по календарю, в месяце 30
      Period := RSI_RSB_FIInstr.rsNDaysp(LastDate) - RSI_RSB_FIInstr.rsNDaysp(FirstDate);
      DaysInYear := GetDaysInYearByDate(LastDate);
    elsif (BasisNKD = 6) then -- дней в году по продолжительности купонных периодов, в месяце по календарю
      SELECT min(F.t_FIRSTDATE) INTO FirstPer --начало первого куп периода
         FROM DFIWARNTS_DBT F
        WHERE F.T_ISPARTIAL != 'X'
          AND F.t_FIID       = FIID;
     -- находим очередной год, на кот приходится дата расчета
       FirstWarntPer := FirstPer;

        begin
           for fiwarnts_rec in c_fiwarntsper loop
              LastWarntPer := fiwarnts_rec.T_DrawingDate;
              if( ((LastWarntPer - FirstWarntPer) >= 359) AND ((LastWarntPer - FirstWarntPer) <= 367) ) then
                 EXIT WHEN ( (LastDate >= FirstWarntPer) AND (LastDate <= LastWarntPer) );
                 FirstWarntPer := LastWarntPer + 1;
              end if;
           end loop;
        end;

     -- находим начало последнего очередного года
       FirstWarntLastPer := FirstPer;

        begin
           for fiwarnts_rec in c_fiwarntsper loop
              if( ((fiwarnts_rec.T_DrawingDate - FirstWarntPer) >= 359) AND
                  ((fiwarnts_rec.T_DrawingDate - FirstWarntPer) <= 367) ) then
                 FirstWarntLastPer := fiwarnts_rec.T_DrawingDate + 1 ;
              end if;
           end loop;
        end;

       -- Самый последний купон
       SELECT F.t_FIRSTDATE, F.t_DRAWINGDATE INTO FirstWarntDate, LastWarntDate
         FROM DFIWARNTS_DBT F
        WHERE F.T_ISPARTIAL != 'X'
          AND F.t_FIID       = FIID
          AND F.t_DRAWINGDATE = (SELECT MAX(F1.t_DRAWINGDATE)
                                   FROM DFIWARNTS_DBT F1
                                  WHERE F1.T_ISPARTIAL != 'X'
                                    AND F1.t_FIID       = FIID);
       --находим дату погашения обл
       select RSI_RSB_FIInstr.FI_GetNominalDrawingDate(t_FIID)
         into DrawingDate
         from dfininstr_dbt
        where T_FIID = FIID;

       --находим количество дней в последнем очередном году
       SELECT NVL( Sum( F.t_DRAWINGDATE - F.t_FIRSTDATE + 1 ), 0) INTO DaysInLastYear
         FROM DFIWARNTS_DBT F
        WHERE F.T_ISPARTIAL   != 'X'
          AND F.t_FIID         = FIID
          AND F.t_DRAWINGDATE >= FirstWarntLastPer
          AND F.t_DRAWINGDATE <= DrawingDate;  --дата погашения облигации
      --------------------------------------------------------------------------
      --------------------------------------------------------------------------
      --------------------------------------------------------------------------
      if (RelativeIncome != chr(0)) then  --% от номинала
        if ((TO_NUMBER(TO_CHAR(LastDate, 'YYYY')) = TO_NUMBER(TO_CHAR(LastWarntDate, 'YYYY'))) AND (DaysInLastYear < 360)) then
          DaysInYear := (TO_DATE('01.01.'||TO_CHAR(TO_NUMBER(TO_CHAR(FirstWarntDate, 'YYYY'))+1),'DD.MM.YYYY')-TO_DATE('01.01.'||TO_CHAR(TO_NUMBER(TO_CHAR(FirstWarntDate, 'YYYY'))),'DD.MM.YYYY'));
          Period := LastDate - FirstDate;
        else
        --количество купонов в очередном году
          SELECT Count(1) INTO DaysInYear
            FROM DFIWARNTS_DBT F
           WHERE F.T_ISPARTIAL   != 'X'
             AND F.t_FIID         = FIID
             AND F.t_DRAWINGDATE >= FirstWarntPer
             AND F.t_DRAWINGDATE <= LastWarntPer;

           Period := 1;
        end if;
      else
        Period := LastDate - FirstDate;
      end if;
      --------------------------------------------------------------------------
      --------------------------------------------------------------------------
      --------------------------------------------------------------------------
    elsif (BasisNKD = 7) then -- Act/365L - в месяце по календарю, в году по календарю по окончанию куп. периода
      Period := LastDate - FirstDate;
      DaysInYear := GetDaysInYearByDate(EndCoupon);
    elsif (BasisNKD = 8) then -- 364 в году, по календарю в месяце
      Period := LastDate - FirstDate;
      DaysInYear := 364;
    elsif (BasisNKD = 9) then --30E/360 в году 360 дней, в месяце 30 дней (Eurobond)
      Period := RSI_RSB_FIInstr.rsNDaysp(LastDate, 0, 1) - RSI_RSB_FIInstr.rsNDaysp(FirstDate, 0, 1);
      DaysInYear := 360;
    elsif (BasisNKD = 10) then -- 30/360 ISDA
      Period := RSI_RSB_FIInstr.rsNDaysf(FirstDate, LastDate);
      DaysInYear := 360;
    end if;

    if (BasisNKD <> 10) then
      Period := Period + 1;
    end if;

  END;

   FUNCTION GetTaxBaseCorpBondAfter2018(p_FIID IN NUMBER, --ц/б, по которой считается доход
                                        p_WarrantNum IN VARCHAR2, --номер купона/чп/дивидендов, по которым считается доход
                                        p_Quantity IN NUMBER
                                       )
   RETURN NUMBER
   AS
     v_TaxAmount   NUMBER;
     v_D1          NUMBER;
     v_DrawingDate DATE;
     v_FirstDate   DATE;
     v_RelativeIncome CHAR := chr(0);
     v_IncomeVolume NUMBER;
     v_IncomePoint  NUMBER;
     v_DaysInYear  NUMBER;
     v_BegDate     DATE;
     v_EndDate     DATE;
     v_CurYear           NUMBER;
     v_FirstDateCurYear  DATE;
     v_FirstDatePrevYear DATE;
     v_NeedContinue      BOOLEAN := true;
     v_Period            NUMBER;
     vtmpFiPoint   NUMBER;
     v_Nominal     NUMBER;
     V_INCOMERATE  NUMBER;
     fwnum NUMBER;
     v_CoupDrawingDate DATE;
     v_CoupFirstDate   DATE;
     v_TotalTaxAmount   NUMBER;
     v_CoupSum NUMBER;
     v_NewCoupSum NUMBER;
     v_TotalCoupSum NUMBER;
     AvoirKind         NUMBER;
     NKDBase_Kind      DAVOIRISS_DBT.T_NKDBase_Kind%TYPE;
     NKDRound_Kind     DAVOIRISS_DBT.T_NKDRound_Kind%TYPE;
     IndexNom          DAVOIRISS_DBT.T_IndexNom%TYPE;
     v_FirstDateYear DATE;
     v_FirstCoupDrawingDate DATE;
     IsFirstCoupon NUMBER := 0;
     M NUMBER := 0; -- количество выплат купонного дохода в году
   BEGIN
     v_D1        := 0;
     v_TotalTaxAmount := 0;
     v_TotalCoupSum := 0;

     SELECT avr.T_NKDBase_Kind, avr.T_NKDRound_Kind, fin.t_AvoirKind, avr.t_IndexNom
        INTO NKDBase_Kind, NKDRound_Kind, AvoirKind, IndexNom
        FROM davoiriss_dbt avr, dfininstr_dbt fin
       WHERE avr.T_FIID = p_FIID
         AND fin.t_FIID = avr.t_FIID;

     SELECT t_DrawingDate, t_FirstDate, t_RelativeIncome, t_IncomeRate, t_IncomeVolume, t_IncomePoint INTO v_CoupDrawingDate, v_CoupFirstDate, v_RelativeIncome, v_IncomeRate, v_IncomeVolume, v_IncomePoint
       FROM dfiwarnts_dbt
      WHERE t_IsPartial = CHR(0)
        AND t_FIID = p_FIID
        AND t_Number = p_WarrantNum;

     -- дата погашения первого купона
     SELECT min(t_DrawingDate) INTO v_FirstCoupDrawingDate
       FROM dfiwarnts_dbt
      WHERE t_IsPartial = CHR(0)
        AND t_FIID = p_FIID;

     if( v_CoupDrawingDate = v_FirstCoupDrawingDate ) then
       IsFirstCoupon := 1;
     end if;

     if(NKDBase_Kind = 11 and IsFirstCoupon = 0) then 
       M := RSI_RSB_FIInstr.FI_CntCoupPayms(p_FIID);
     end if;

     if( v_RelativeIncome != 'X')then
        select RSI_RSB_FIINSTR.CalcNKD_Ex_NoRound(p_FIID, v_CoupDrawingDate, 1, 1, 0), round(RSI_RSB_FIInstr.FI_ReturnIncomeRate(),v_IncomePoint) into v_D1, v_IncomeRate from dual;
     else
        select round(RSI_RSB_FIINSTR.CalcNKD_Ex_NoRound(p_FIID, v_CoupDrawingDate, 1, 1, 0),v_IncomePoint) into v_IncomeVolume from dual;
     end if;

     select count(1) into fwnum from dfiwarnts_dbt where t_ispartial = 'X' and t_fiid = p_FIID and t_DrawingDate between v_CoupFirstDate and v_CoupDrawingDate;

     v_FirstDate := v_CoupFirstDate;

     for nom_date in (select t_DrawingDate DrawingDate from dfiwarnts_dbt where t_ispartial = 'X' and t_fiid = p_FIID and t_DrawingDate between v_CoupFirstDate and v_CoupDrawingDate
                      union
                      select v_CoupDrawingDate DrawingDate from dual
                      order by DrawingDate)
     loop

     v_DrawingDate := nom_date.DrawingDate;
     v_TaxAmount := 0;
     v_CoupSum := 0;

     FOR one_prm IN (select cbrate.t_datefrom AS T_EFFECTIVEDATE, cbrate.t_rate, (lead(cbrate.T_DATEFROM,1,v_DrawingDate+1) over(order by cbrate.T_DATEFROM))-1 as T_END_EFFECTIVEDATE,
                            min(cbrate.T_RATE) over() T_MinRate
                       from ( select rate.*
                                from dcbkeyrate_dbt rate
                               where RATE.T_DATEFROM between (v_FirstDate + 1) and v_DrawingDate
                             union
                              select rate.*
                                from dcbkeyrate_dbt rate
                               where RATE.T_DATEFROM = (select max(t.T_DATEFROM) from dcbkeyrate_dbt t where t.T_DATEFROM <= v_FirstDate)
                            ) cbrate
                    )                            
     LOOP
        EXIT WHEN ((one_prm.T_MinRate + 5) >= v_IncomeRate);

        -- Аналитик: по-хорошему, конечно, надо применять тот же алгоритм, который задан в базисе расчета дохода на бумаге
        -- Аналитик: Но если сложно, то берем в году и в месяце по календарю, т.к. бумаги российские и рублевые
        -- поэтому реализуем по алгоритму в году и в месяце по календарю

        if( one_prm.T_EFFECTIVEDATE < v_FirstDate )then
           v_BegDate := v_FirstDate;
        else
           v_BegDate := one_prm.T_EFFECTIVEDATE;
        end if;

        v_CurYear := TO_NUMBER(TO_CHAR( v_BegDate, 'YYYY'));
        v_EndDate := one_prm.T_END_EFFECTIVEDATE;

        v_NeedContinue := true;

        if( TO_CHAR( v_BegDate, 'YYYY') != TO_CHAR( v_EndDate, 'YYYY') ) then

           while( v_NeedContinue = true ) LOOP

              v_FirstDateCurYear  := TO_DATE('31.12.'||(TO_CHAR( v_CurYear)),'DD.MM.YYYY');
              v_FirstDatePrevYear := TO_DATE('31.12.'||(TO_CHAR( v_CurYear-1)),'DD.MM.YYYY');
              v_FirstDateYear := TO_DATE('01.01.'||(TO_CHAR( v_CurYear)),'DD.MM.YYYY');

              if( v_CurYear = TO_NUMBER(TO_CHAR( v_BegDate, 'YYYY')) ) then
                 --v_Period := v_FirstDateCurYear - v_BegDate + 1;
                 CalcBasisNKD(p_FIID, NKDBase_Kind, v_BegDate, v_FirstDateCurYear, v_CoupDrawingDate, IndexNom, v_RelativeIncome, v_Period, v_DaysInYear);
              elsif( v_CurYear = TO_NUMBER(TO_CHAR( v_EndDate, 'YYYY')) ) then
                 --v_Period := v_EndDate - v_FirstDatePrevYear;
                 CalcBasisNKD(p_FIID, NKDBase_Kind, v_FirstDateYear, v_EndDate, v_CoupDrawingDate, IndexNom, v_RelativeIncome, v_Period, v_DaysInYear);
              else
                 --v_Period := GetDaysInYear( v_CurYear );
                 CalcBasisNKD(p_FIID, NKDBase_Kind, v_FirstDateYear, v_FirstDateCurYear, v_CoupDrawingDate, IndexNom, v_RelativeIncome, v_Period, v_DaysInYear);
              end if;

              --v_DaysInYear := GetDaysInYear( v_CurYear );

              if( (v_Period != 0) and (v_DaysInYear != 0) ) then
                v_TaxAmount := v_TaxAmount + ((one_prm.T_Rate + 5) * v_Period / v_DaysInYear);
                if (RSI_RSB_FIInstr.FI_GetCurrentNominal( p_FIID, v_Nominal, vtmpFiPoint, v_FirstDate ) IS NOT NULL) then
                  if( v_RelativeIncome = 'X') then
                    v_NewCoupSum := RSI_RSB_FIInstr.CouponsSum(p_FIID, p_WarrantNum, v_Nominal, v_NewCoupSum, v_DaysInYear, v_Period, NKDBase_Kind, p_Quantity, 0, IsFirstCoupon, M);
                    v_CoupSum := v_CoupSum + v_NewCoupSum;
                  end if;
                end if;
              end if;

              v_CurYear := v_CurYear + 1;

              if( v_CurYear > TO_NUMBER(TO_CHAR( v_EndDate, 'YYYY')) ) then
                 v_NeedContinue := false;
              end if;

           end loop;

        else
           --v_Period := v_EndDate - v_BegDate + 1;
           --v_DaysInYear := GetDaysInYear(v_CurYear);
           CalcBasisNKD(p_FIID, NKDBase_Kind, v_BegDate, v_EndDate, v_CoupDrawingDate, IndexNom, v_RelativeIncome, v_Period, v_DaysInYear);
           if( v_Period != 0 ) then
              v_TaxAmount := v_TaxAmount + Round(((one_prm.T_Rate + 5) *  v_Period / v_DaysInYear), 3);
              if (RSI_RSB_FIInstr.FI_GetCurrentNominal( p_FIID, v_Nominal, vtmpFiPoint, v_FirstDate ) IS NOT NULL) then
                if( v_RelativeIncome = 'X') then
                  v_NewCoupSum := RSI_RSB_FIInstr.CouponsSum(p_FIID, p_WarrantNum, v_Nominal, v_NewCoupSum, v_DaysInYear, v_Period, NKDBase_Kind, p_Quantity, 0, IsFirstCoupon, M);
                  v_CoupSum := v_CoupSum + v_NewCoupSum;
                end if;
              end if;
           end if;
        end if;

     END LOOP;

     if( v_TaxAmount > 0.0 )then

        v_TaxAmount := v_TaxAmount * 0.01;
        if( RSI_RSB_FIInstr.FI_GetCurrentNominal( p_FIID, v_Nominal, vtmpFiPoint, v_FirstDate ) IS NULL ) then
           v_TaxAmount := 0.0;
        else
          v_TaxAmount := round(v_TaxAmount * v_Nominal,v_IncomePoint);
          if (fwnum > 0) then
            v_TaxAmount := round(v_TaxAmount * p_Quantity, 2);
          else
            v_TaxAmount := round(GREATEST( v_IncomeVolume - v_TaxAmount, 0 ) * p_Quantity,2);
          end if;
        end if;

     end if;

     v_FirstDate := v_DrawingDate + 1;
     v_TotalTaxAmount := v_TotalTaxAmount + v_TaxAmount;

     if( v_RelativeIncome = 'X')then
       v_TotalCoupSum := v_TotalCoupSum + v_CoupSum;
     end if;

     end loop;

     if( v_RelativeIncome <> 'X')then
       v_TotalCoupSum := RSI_RSB_FIInstr.CouponsSum(p_FIID, p_WarrantNum, v_Nominal, v_TotalCoupSum, 1, 1, NKDBase_Kind, p_Quantity, 0, IsFirstCoupon, M);
     end if;

    if (fwnum > 0) then
      return Greatest(v_TotalCoupSum - v_TotalTaxAmount, 0);
    else
      return v_TotalTaxAmount;
    end if;

   END GetTaxBaseCorpBondAfter2018;

   FUNCTION GetFirstDateIIS (p_Client IN dparty_dbt.t_PartyId%TYPE, p_DlContrID IN NUMBER DEFAULT 0)
      RETURN DATE
      DETERMINISTIC
   IS
      v_DBO_firstdate   DATE;
      v_DO_firstdate    DATE;
      v_sfdatebegin     DATE;
      v_sfid            dsfcontr_dbt.t_Id%TYPE;

      CURSOR c_do
      IS
           SELECT NVL (s.t_datebegin, TO_DATE ('01010001', 'ddmmyyyy')) dofirstdate, s.t_id
             FROM dsfcontr_dbt s
            WHERE     CheckContrIIS (s.t_id) = 1
                  AND (    (s.T_ServKind = 1/* PTSK_STOCKDL */ OR s.T_ServKind = 15/* PTSK_DV */) AND s.T_ServKindSub = 0
                        OR s.T_ServKind = 7 /* PTSK_DEPOS */)
                  AND s.t_partyid = p_Client
         ORDER BY s.t_datebegin ASC;
   BEGIN

      BEGIN
         SELECT *
         INTO v_DBO_firstdate
         FROM (  SELECT CASE WHEN d.t_iistransfer = 'X' AND d.t_iislastopendate > TO_DATE ('01.01.0001', 'DD.MM.YYYY') THEN d.t_iislastopendate ELSE s.t_datebegin END
                   FROM ddlcontr_dbt d, dsfcontr_dbt s
                  WHERE d.t_sfcontrid = s.t_id AND d.t_iis = 'X' AND s.t_partyid = p_Client
                    AND (p_DlContrID <= 0 OR d.t_DlContrID = p_DlContrID)
               ORDER BY s.t_datebegin ASC)
         WHERE ROWNUM = 1;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN v_DBO_firstdate := TO_DATE('01.01.0001', 'dd.mm.yyyy');
      END;

      IF p_DlContrID > 0 THEN
        RETURN v_DBO_firstdate;
      END IF;

      OPEN c_do;

      FETCH c_do INTO v_sfdatebegin, v_sfid;

      IF c_do%FOUND
      THEN
         v_DO_firstdate := GetDateFromNoteText (cnst.OBJTYPE_SFCONTR, LPAD (v_sfid, 10, '0'), 3);

         IF v_DO_firstdate IS NULL
         THEN
            v_DO_firstdate := v_sfdatebegin;
         END IF;
      END IF;

      CLOSE c_do;

      v_DBO_firstdate := NVL (v_DBO_firstdate, TO_DATE ('01.01.0001', 'DD.MM.YYYY'));
      v_DO_firstdate := NVL (v_DO_firstdate, TO_DATE ('01.01.0001', 'DD.MM.YYYY'));

      IF  (v_DBO_firstdate < v_DO_firstdate OR v_DBO_firstdate != TO_DATE ('01.01.0001', 'DD.MM.YYYY'))
              AND v_DO_firstdate = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
      THEN
         RETURN v_DBO_firstdate;
      END IF;

      RETURN v_DO_firstdate;
   END GetFirstDateIIS;

   PROCEDURE RecalcCirculate(p_ClientID IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE, p_NeedDel IN NUMBER, p_ParallelLevel IN NUMBER DEFAULT 1)
   AS
     v_task_name      VARCHAR2(30);
     v_sql_chunks     CLOB;
     v_sql_query      CLOB;
     v_sql_query_add  CLOB;
     v_sql_process    VARCHAR2(2000);
     v_try            NUMBER(5) := 0;
     v_status         NUMBER;
     v_sign           NUMBER := 0;
     v_cnt            NUMBER := 0;

     v_fiid_is_empty  NUMBER := 0;

     v_Query VARCHAR2(3000);
   BEGIN

     select count(*) into v_cnt from DFIID_TMP;
     IF (v_cnt = 0) THEN
       v_fiid_is_empty := 1;
     END IF;

     v_cnt := 0;

     IF p_NeedDel != 0 THEN
       DELETE FROM DNPTXFI_DBT 
              WHERE (v_fiid_is_empty = 1 or t_FIID in (select FIID from DFIID_TMP))
                AND t_Date >= p_BegDate 
                AND t_Date <= p_EndDate;
     END IF;

     v_Query := 'SELECT DISTINCT tk.t_DealDate, tk.t_PFI ' ||
                '  FROM ddl_tick_dbt tk ' ||
                ' WHERE tk.t_BOfficeKind IN ('||RSB_SECUR.DL_SECURITYDOC||', '||RSB_SECUR.DL_RETIREMENT||', '||RSB_SECUR.DL_AVRWRT||') ' ||
                '   AND tk.t_ClientID  > 0 ' ||
                '   AND tk.t_DealDate >= :BegDate ' || 
                '   AND tk.t_DealDate <= :EndDate ' ||
                '   AND NOT EXISTS(SELECT 1 ' ||
                                '    FROM dnptxfi_dbt f ' ||
                                '   WHERE f.t_FIID = tk.t_PFI ' ||
                                '     AND f.t_Date = tk.t_DealDate ' ||
                                ' ) ';

     IF v_fiid_is_empty = 0 THEN
       v_Query := v_Query || ' AND tk.t_PFI in (select FIID from DFIID_TMP) ';
     END IF;

     IF p_ClientID > 0 THEN
       v_Query := v_Query || ' AND tk.t_ClientID = ' || p_ClientID;
     END IF;

     v_Query := v_Query ||
                'UNION ' ||
                'SELECT DISTINCT tk.t_DealDate, tk.t_PFI ' ||
                '  FROM ddl_tick_dbt tk  ' ||
                ' WHERE tk.t_BOfficeKind IN ('||RSB_SECUR.DL_SECURITYDOC||', '||RSB_SECUR.DL_RETIREMENT||', '||RSB_SECUR.DL_AVRWRT||') ' ||
                '   AND tk.t_PartyID  > 0 ' ||
                '   AND tk.t_DealDate >= :BegDate1 ' || 
                '   AND tk.t_DealDate <= :EndDate1 ' ||
                '   AND tk.t_IsPartyClient = CHR(88) ' ||
                '   AND NOT EXISTS(SELECT 1 ' ||
                                '    FROM dnptxfi_dbt f ' ||
                                '   WHERE f.t_FIID = tk.t_PFI ' ||
                                '     AND f.t_Date = tk.t_DealDate ' ||
                                ' ) ';

     IF v_fiid_is_empty = 0 THEN
       v_Query := v_Query || ' AND tk.t_PFI in (select FIID from DFIID_TMP) ';
     END IF;

     IF p_ClientID > 0 THEN
       v_Query := v_Query || ' AND tk.t_PartyID = ' || p_ClientID;
     END IF;

     v_Query := v_Query ||
                'UNION ' ||
                'SELECT DISTINCT dls.t_Date as t_DealDate, tk.t_PFI ' ||
                '  FROM ddl_tick_dbt tk, ddlsum_dbt dls  ' ||
                ' WHERE tk.t_BOfficeKind = '||RSB_SECUR.DL_AVRWRT||
                '   AND tk.t_ClientID  > 0 ' ||
                '   AND tk.t_DealDate >= :BegDate2 ' ||
                '   AND tk.t_DealDate <= :EndDate2 ' ||
                '   AND tk.t_Flag3 = CHR(88) '||
                '   AND dls.t_DocKind = tk.t_BOfficeKind ' ||
                '   AND dls.t_DocID   = tk.t_DealID ' ||
                '   AND dls.t_Kind  = ' ||RSI_NPTXC.DLSUM_KIND_COSTWRTTAX||
                '   AND NOT EXISTS(SELECT 1 ' ||
                                '    FROM dnptxfi_dbt f ' ||
                                '   WHERE f.t_FIID = tk.t_PFI ' ||
                                '     AND f.t_Date = dls.t_Date ' ||
                                ' ) ';

     IF v_fiid_is_empty = 0 THEN
       v_Query := v_Query || ' AND tk.t_PFI in (select FIID from DFIID_TMP) ';
     END IF;

     IF p_ClientID > 0 THEN
       v_Query := v_Query || ' AND tk.t_PartyID = ' || p_ClientID;
     END IF;

     v_Query := v_Query ||
                'UNION ' ||
                'SELECT DISTINCT dls.t_Date as t_DealDate, tk.t_PFI ' ||
                '  FROM ddl_tick_dbt tk, ddlsum_dbt dls  ' ||
                ' WHERE tk.t_BOfficeKind = '||RSB_SECUR.DL_AVRWRT||
                '   AND tk.t_ClientID  > 0 ' ||
                '   AND tk.t_Flag3 = CHR(88) ' ||
                '   AND dls.t_DocKind = tk.t_BOfficeKind ' ||
                '   AND dls.t_DocID   = tk.t_DealID ' ||
                '   AND dls.t_Kind  = ' ||RSI_NPTXC.DLSUM_KIND_COSTWRTTAX||
                '   AND dls.t_Date BETWEEN :BegDate3 AND :EndDate3 ' ||
                '   AND NOT EXISTS(SELECT 1 ' ||
                                '    FROM dnptxfi_dbt f ' ||
                                '   WHERE f.t_FIID = tk.t_PFI ' ||
                                '     AND f.t_Date = dls.t_Date ' ||
                                ' ) ';

     IF v_fiid_is_empty = 0 THEN
       v_Query := v_Query || ' AND tk.t_PFI in (select FIID from DFIID_TMP) ';
     END IF;

     IF p_ClientID > 0 THEN
       v_Query := v_Query || ' AND tk.t_PartyID = ' || p_ClientID;
     END IF;

     IF p_ParallelLevel <= 1 OR ( v_fiid_is_empty = 0 AND p_BegDate = p_EndDate) THEN

       v_Query :=
       ' BEGIN ' ||
       ' FOR one_rec IN ('||v_Query||') ' ||
       ' LOOP ' ||
       '  BEGIN ' ||
       '    INSERT INTO DNPTXFI_DBT(T_FIID, T_DATE, T_CIRCULATE, T_STATUS) VALUES(one_rec.t_PFI, one_rec.t_DealDate, RSI_NPTO.Market1date(one_rec.t_PFI, one_rec.t_DealDate), RSI_NPTO.GetCircRecalcStatus); ' ||
       '    EXCEPTION ' ||
       '      WHEN OTHERS THEN NULL; ' ||
       '  END; ' ||
       ' END LOOP; ' ||
       ' END; ';

       EXECUTE IMMEDIATE v_Query USING IN p_BegDate, p_EndDate, p_BegDate, p_EndDate, p_BegDate, p_EndDate, p_BegDate, p_EndDate;
     ELSE

       DELETE FROM DNPTXFI_S_DBT;

       EXECUTE IMMEDIATE 'INSERT INTO DNPTXFI_S_DBT (T_ID, T_FIID, T_DATE) ' ||
                         'SELECT 0, T_PFI, T_DEALDATE FROM ('||v_Query||') ' USING IN p_BegDate, p_EndDate, p_BegDate, p_EndDate, p_BegDate, p_EndDate, p_BegDate, p_EndDate;


       SELECT Count(1) INTO v_cnt FROM dnptxfi_s_dbt WHERE ROWNUM = 1;

       IF v_cnt > 0 THEN
         v_sql_chunks := 'SELECT t_ID, 0 FROM DNPTXFI_S_DBT ';

         v_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;   
         DBMS_PARALLEL_EXECUTE.create_task (task_name => v_task_name);      


         DBMS_PARALLEL_EXECUTE.create_chunks_by_sql(task_name => v_task_name,
                                                    sql_stmt  => v_sql_chunks, 
                                                    by_rowid  => FALSE);
         
         v_sql_process :=
         ' DECLARE ' ||
         '  v_FIID  NUMBER; ' ||
         '  v_Date  DATE; ' || 
         '  v_ID    NUMBER := :start_id; ' ||
         '  v_ID1   NUMBER := :end_id; ' ||
         ' BEGIN ' ||
         '   SELECT t_FIID, t_Date INTO v_FIID, v_Date FROM DNPTXFI_S_DBT WHERE T_ID = v_ID; ' ||
         '   INSERT INTO DNPTXFI_DBT(T_FIID, T_DATE, T_CIRCULATE, T_STATUS) VALUES(v_FIID, v_Date, RSI_NPTO.Market1date(v_FIID, v_Date), RSI_NPTO.GetCircRecalcStatus); ' ||
         '   EXCEPTION ' ||
         '      WHEN OTHERS THEN NULL; ' ||
         ' END; ';

         DBMS_PARALLEL_EXECUTE.run_task(task_name => v_task_name,
                                        sql_stmt => v_sql_process,
                                        language_flag => DBMS_SQL.NATIVE,
                                        parallel_level => p_ParallelLevel);

         v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
         WHILE(v_try < 2 AND v_status != DBMS_PARALLEL_EXECUTE.FINISHED)
         LOOP
           v_try := v_try + 1;
           DBMS_PARALLEL_EXECUTE.resume_task(v_task_name);
           v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
         END LOOP;

         DBMS_PARALLEL_EXECUTE.drop_task(v_task_name);

       END IF;

     END IF;
   END;

     --Процедура вставки строки в таблицу значений НДФЛ на шаге операции
  PROCEDURE RSI_InsertNPTXVAL(p_DocKind IN NUMBER, 
                              p_DocID IN NUMBER,
                              p_Kind IN NUMBER,
                              p_Date IN DATE,
                              p_Time IN DATE,
                              p_Value IN NUMBER,
                              p_ID_Operation IN NUMBER,
                              p_ID_Step IN NUMBER,
                              p_Rate IN NUMBER DEFAULT 0,
                              p_KBK IN VARCHAR2 DEFAULT CHR(1)
                             )
  AS 
  BEGIN

    INSERT INTO DNPTXVAL_DBT ( T_ID, T_DOCKIND, T_DOCID, T_KIND, T_DATE, T_TIME, T_VAL, T_ID_OPERATION, T_ID_STEP, T_RATE, T_KBK)
                       VALUES (0, p_DocKind, p_DocID, p_Kind, p_Date, p_Time, p_Value, p_ID_Operation, p_ID_Step, p_Rate, p_KBK);

  END RSI_InsertNPTXVAL;

  --Откат вставки строки в таблицу значений НДФЛ на шаге операции
  PROCEDURE RSI_RollbackInsertNPTXVAL(p_ID_Operation IN NUMBER,
                                      p_ID_Step IN NUMBER
                                     )
  AS 
  BEGIN

    DELETE FROM DNPTXVAL_DBT 
     WHERE T_ID_OPERATION = p_ID_Operation
       AND T_ID_STEP = p_ID_Step;

  END RSI_RollbackInsertNPTXVAL;


  PROCEDURE RSI_CopyRAWtoNPTXTOTALBASE( nptxtotalbase IN RAW, p_STB OUT DNPTXTOTALBASE_DBT%ROWTYPE )
  IS
  BEGIN
    rsb_struct.readStruct('dnptxtotalbase_dbt');

    p_STB.T_TBID                := rsb_struct.getLong(   'T_TBID',                 nptxtotalbase );
    p_STB.T_CLIENTID            := rsb_struct.getLong(   'T_CLIENTID',             nptxtotalbase );
    p_STB.T_TYPE                := rsb_struct.getLong(   'T_TYPE',                 nptxtotalbase );
    p_STB.T_DESCRIPTION         := rsb_struct.getString( 'T_DESCRIPTION',          nptxtotalbase );
    p_STB.T_INCREGIONDATE       := rsb_struct.getDate(   'T_INCREGIONDATE',        nptxtotalbase );
    p_STB.T_INCREGIONTIME       := rsb_struct.getTime(   'T_INCREGIONTIME',        nptxtotalbase );
    p_STB.T_INCDATE             := rsb_struct.getDate(   'T_INCDATE',              nptxtotalbase );
    p_STB.T_INCTIME             := rsb_struct.getTime(   'T_INCTIME',              nptxtotalbase );
    p_STB.T_CONFIRMSTATE        := rsb_struct.getInt(    'T_CONFIRMSTATE',         nptxtotalbase );
    p_STB.T_SOURCESYSTEM        := rsb_struct.getInt(    'T_SOURCESYSTEM',         nptxtotalbase );
    p_STB.T_STORSTATE           := rsb_struct.getInt(    'T_STORSTATE',            nptxtotalbase );
    p_STB.T_DOCKIND             := rsb_struct.getInt(    'T_DOCKIND',              nptxtotalbase );
    p_STB.T_DOCID               := rsb_struct.getLong(   'T_DOCID',                nptxtotalbase );
    p_STB.T_TAXPERIOD           := rsb_struct.getInt(    'T_TAXPERIOD',            nptxtotalbase );
    p_STB.T_TAXBASEKIND         := rsb_struct.getLong(   'T_TAXBASEKIND',          nptxtotalbase );
    p_STB.T_TAXBASECURRPAY      := rsb_struct.getMoney(  'T_TAXBASECURRPAY',       nptxtotalbase );
    p_STB.T_CALCPITAX           := rsb_struct.getMoney(  'T_CALCPITAX',            nptxtotalbase );
    p_STB.T_RATECALCPITAX       := rsb_struct.getInt(    'T_RATECALCPITAX',        nptxtotalbase );
    p_STB.T_HOLDPITAX           := rsb_struct.getMoney(  'T_HOLDPITAX',            nptxtotalbase );
    p_STB.T_RATEHOLDPITAX       := rsb_struct.getInt(    'T_RATEHOLDPITAX',        nptxtotalbase );
    p_STB.T_BCCCALCPITAX        := rsb_struct.getString( 'T_BCCCALCPITAX',         nptxtotalbase );
    p_STB.T_BCCHOLDPITAX        := rsb_struct.getString( 'T_BCCHOLDPITAX',         nptxtotalbase );
    p_STB.T_TAXPAYERSTATUS      := rsb_struct.getInt(    'T_TAXPAYERSTATUS',       nptxtotalbase );
    p_STB.T_APPLSTAXBASEINCLUDE := rsb_struct.getMoney(  'T_APPLSTAXBASEINCLUDE',  nptxtotalbase );
    p_STB.T_APPLSTAXBASEEXCLUDE := rsb_struct.getMoney(  'T_APPLSTAXBASEEXCLUDE',  nptxtotalbase );
    p_STB.T_RECSTAXBASE         := rsb_struct.getMoney(  'T_RECSTAXBASE',          nptxtotalbase );
    p_STB.T_RECSTAXBASEDATE     := rsb_struct.getDate(   'T_RECSTAXBASEDATE',      nptxtotalbase );
    p_STB.T_RECSTAXBASETIME     := rsb_struct.getTime(   'T_RECSTAXBASETIME',      nptxtotalbase );
    p_STB.T_ORIGTBID            := rsb_struct.getLong(   'T_ORIGTBID',             nptxtotalbase );
    p_STB.T_STORID              := rsb_struct.getString( 'T_STORID',               nptxtotalbase );
    p_STB.T_INSTANCE            := rsb_struct.getInt(    'T_INSTANCE',             nptxtotalbase );
    p_STB.T_ID_OPERATION        := rsb_struct.getLong(   'T_ID_OPERATION',         nptxtotalbase );
    p_STB.T_ID_STEP             := rsb_struct.getInt(    'T_ID_STEP',              nptxtotalbase );
    p_STB.T_IDENTERRSTR         := rsb_struct.getString( 'T_IDENTERRSTR',          nptxtotalbase );
    p_STB.T_NEEDRECALC          := rsb_struct.getString( 'T_NEEDRECALC',           nptxtotalbase );
    p_STB.T_RECTAXBASEBYKIND    := rsb_struct.getMoney(  'T_RECTAXBASEBYKIND',     nptxtotalbase );
    p_STB.T_INITIAL_DOCKIND     := rsb_struct.getInt(    'T_INITIAL_DOCKIND',      nptxtotalbase );
    p_STB.T_INITIAL_DOCID       := rsb_struct.getLong(   'T_INITIAL_DOCID',        nptxtotalbase );
    p_STB.T_SENDDATE            := rsb_struct.getDate(   'T_SENDDATE',             nptxtotalbase );
    p_STB.T_SENDTIME            := rsb_struct.getTime(   'T_SENDTIME',             nptxtotalbase );
    p_STB.T_CANCELDATE          := rsb_struct.getDate(   'T_CANCELDATE',           nptxtotalbase );
    p_STB.T_CANCELTIME          := rsb_struct.getTime(   'T_CANCELTIME',           nptxtotalbase );
    p_STB.T_SYMBOL              := rsb_struct.getChar(   'T_SYMBOL',               nptxtotalbase );
    p_STB.t_dlcontrid           := rsb_struct.getInt(    't_dlcontrid',            nptxtotalbase );
    p_STB.t_specialtag          := rsb_struct.getString( 't_specialtag',           nptxtotalbase );
    p_STB.t_syscome             := rsb_struct.getInt(    't_syscome',              nptxtotalbase );


  END;

  /**
   * Заполнение дефолтными значениями незаполненных(null-х) полей в переданной структуре СНОБ
   */
  PROCEDURE RSI_InsDfltIntoNPTXTOTALBASE( p_STB IN OUT DNPTXTOTALBASE_DBT%ROWTYPE )
  IS
  BEGIN

    p_STB.T_TBID                := NVL(p_STB.T_TBID,0);               
    p_STB.T_CLIENTID            := NVL(p_STB.T_CLIENTID,-1);           
    p_STB.T_TYPE                := NVL(p_STB.T_TYPE,0);               
    p_STB.T_DESCRIPTION         := NVL(p_STB.T_DESCRIPTION,CHR(1));        
    p_STB.T_INCREGIONDATE       := NVL(p_STB.T_INCREGIONDATE,NPTAX.UnknownDate);      
    p_STB.T_INCREGIONTIME       := NVL(p_STB.T_INCREGIONTIME,NPTAX.UnknownDate);      
    p_STB.T_INCDATE             := NVL(p_STB.T_INCDATE,NPTAX.UnknownDate);            
    p_STB.T_INCTIME             := NVL(p_STB.T_INCTIME,NPTAX.UnknownDate);            
    p_STB.T_CONFIRMSTATE        := NVL(p_STB.T_CONFIRMSTATE,0);       
    p_STB.T_SOURCESYSTEM        := NVL(p_STB.T_SOURCESYSTEM,0);       
    p_STB.T_STORSTATE           := NVL(p_STB.T_STORSTATE,0);          
    p_STB.T_DOCKIND             := NVL(p_STB.T_DOCKIND,0);            
    p_STB.T_DOCID               := NVL(p_STB.T_DOCID,0);              
    p_STB.T_TAXPERIOD           := NVL(p_STB.T_TAXPERIOD,0);          
    p_STB.T_TAXBASEKIND         := NVL(p_STB.T_TAXBASEKIND ,0);       
    p_STB.T_TAXBASECURRPAY      := NVL(p_STB.T_TAXBASECURRPAY,0);     
    p_STB.T_CALCPITAX           := NVL(p_STB.T_CALCPITAX,0);          
    p_STB.T_RATECALCPITAX       := NVL(p_STB.T_RATECALCPITAX,0);      
    p_STB.T_HOLDPITAX           := NVL(p_STB.T_HOLDPITAX,0);          
    p_STB.T_RATEHOLDPITAX       := NVL(p_STB.T_RATEHOLDPITAX,0);      
    p_STB.T_BCCCALCPITAX        := NVL(p_STB.T_BCCCALCPITAX,CHR(1));       
    p_STB.T_BCCHOLDPITAX        := NVL(p_STB.T_BCCHOLDPITAX,CHR(1));       
    p_STB.T_TAXPAYERSTATUS      := NVL(p_STB.T_TAXPAYERSTATUS,0);     
    p_STB.T_APPLSTAXBASEINCLUDE := NVL(p_STB.T_APPLSTAXBASEINCLUDE,0);
    p_STB.T_APPLSTAXBASEEXCLUDE := NVL(p_STB.T_APPLSTAXBASEEXCLUDE,0);
    p_STB.T_RECSTAXBASE         := NVL(p_STB.T_RECSTAXBASE,0);        
    p_STB.T_RECSTAXBASEDATE     := NVL(p_STB.T_RECSTAXBASEDATE,NPTAX.UnknownDate);    
    p_STB.T_RECSTAXBASETIME     := NVL(p_STB.T_RECSTAXBASETIME,NPTAX.UnknownDate);    
    p_STB.T_ORIGTBID            := NVL(p_STB.T_ORIGTBID,0);           
    p_STB.T_STORID              := NVL(p_STB.T_STORID,CHR(1));             
    p_STB.T_INSTANCE            := NVL(p_STB.T_INSTANCE,0);           
    p_STB.T_ID_OPERATION        := NVL(p_STB.T_ID_OPERATION,0);       
    p_STB.T_ID_STEP             := NVL(p_STB.T_ID_STEP,0);
    p_STB.T_IDENTERRSTR         := NVL(p_STB.T_IDENTERRSTR, CHR(1));       
    p_STB.T_NEEDRECALC          := NVL(p_STB.T_NEEDRECALC, CHR(1));        
    p_STB.T_RECTAXBASEBYKIND    := NVL(p_STB.T_RECTAXBASEBYKIND, 0);  
    p_STB.T_INITIAL_DOCKIND     := NVL(p_STB.T_INITIAL_DOCKIND,0);            
    p_STB.T_INITIAL_DOCID       := NVL(p_STB.T_INITIAL_DOCID,0);
    p_STB.T_SENDDATE            := NVL(p_STB.T_SENDDATE,NPTAX.UnknownDate);
    p_STB.T_SENDTIME            := NVL(p_STB.T_SENDTIME,NPTAX.UnknownDate);
    p_STB.T_CANCELDATE          := NVL(p_STB.T_CANCELDATE,NPTAX.UnknownDate);
    p_STB.T_CANCELTIME          := NVL(p_STB.T_CANCELTIME,NPTAX.UnknownDate);
    p_STB.T_SYMBOL              := NVL(p_STB.T_SYMBOL,CHR(0));
    p_STB.t_dlcontrid           := NVL(p_STB.t_dlcontrid,0);
    p_STB.t_specialtag          := NVL(p_STB.t_specialtag, CHR(1));
    p_STB.t_syscome             := NVL(p_STB.t_syscome, 0);
    
  END;

  /**
   * Заполнение дефолтными значениями незаполненных(null-х) полей в переданной структуре истории СНОБ
   */
  PROCEDURE RSI_InsDfltIntoNPTXTOTALBASEBC(p_STB IN OUT DNPTXTOTALBASEBC_DBT%ROWTYPE )
  IS
  BEGIN

    p_STB.T_BCID                := NVL(p_STB.T_BCID,0);
    p_STB.T_TBID                := NVL(p_STB.T_TBID,0);               
    p_STB.T_CLIENTID            := NVL(p_STB.T_CLIENTID,-1);           
    p_STB.T_TYPE                := NVL(p_STB.T_TYPE,0);               
    p_STB.T_DESCRIPTION         := NVL(p_STB.T_DESCRIPTION,CHR(1));        
    p_STB.T_INCREGIONDATE       := NVL(p_STB.T_INCREGIONDATE,NPTAX.UnknownDate);      
    p_STB.T_INCREGIONTIME       := NVL(p_STB.T_INCREGIONTIME,NPTAX.UnknownDate);      
    p_STB.T_INCDATE             := NVL(p_STB.T_INCDATE,NPTAX.UnknownDate);            
    p_STB.T_INCTIME             := NVL(p_STB.T_INCTIME,NPTAX.UnknownDate);            
    p_STB.T_CONFIRMSTATE        := NVL(p_STB.T_CONFIRMSTATE,0);       
    p_STB.T_SOURCESYSTEM        := NVL(p_STB.T_SOURCESYSTEM,0);       
    p_STB.T_STORSTATE           := NVL(p_STB.T_STORSTATE,0);          
    p_STB.T_DOCKIND             := NVL(p_STB.T_DOCKIND,0);            
    p_STB.T_DOCID               := NVL(p_STB.T_DOCID,0);              
    p_STB.T_TAXPERIOD           := NVL(p_STB.T_TAXPERIOD,0);          
    p_STB.T_TAXBASEKIND         := NVL(p_STB.T_TAXBASEKIND ,0);       
    p_STB.T_TAXBASECURRPAY      := NVL(p_STB.T_TAXBASECURRPAY,0);     
    p_STB.T_CALCPITAX           := NVL(p_STB.T_CALCPITAX,0);          
    p_STB.T_RATECALCPITAX       := NVL(p_STB.T_RATECALCPITAX,0);      
    p_STB.T_HOLDPITAX           := NVL(p_STB.T_HOLDPITAX,0);          
    p_STB.T_RATEHOLDPITAX       := NVL(p_STB.T_RATEHOLDPITAX,0);      
    p_STB.T_BCCCALCPITAX        := NVL(p_STB.T_BCCCALCPITAX,CHR(1));       
    p_STB.T_BCCHOLDPITAX        := NVL(p_STB.T_BCCHOLDPITAX,CHR(1));       
    p_STB.T_TAXPAYERSTATUS      := NVL(p_STB.T_TAXPAYERSTATUS,0);     
    p_STB.T_APPLSTAXBASEINCLUDE := NVL(p_STB.T_APPLSTAXBASEINCLUDE,0);
    p_STB.T_APPLSTAXBASEEXCLUDE := NVL(p_STB.T_APPLSTAXBASEEXCLUDE,0);
    p_STB.T_RECSTAXBASE         := NVL(p_STB.T_RECSTAXBASE,0);        
    p_STB.T_RECSTAXBASEDATE     := NVL(p_STB.T_RECSTAXBASEDATE,NPTAX.UnknownDate);    
    p_STB.T_RECSTAXBASETIME     := NVL(p_STB.T_RECSTAXBASETIME,NPTAX.UnknownDate);    
    p_STB.T_ORIGTBID            := NVL(p_STB.T_ORIGTBID,0);           
    p_STB.T_STORID              := NVL(p_STB.T_STORID,CHR(1));             
    p_STB.T_INSTANCE            := NVL(p_STB.T_INSTANCE,0);           
    p_STB.T_ID_OPERATION        := NVL(p_STB.T_ID_OPERATION,0);       
    p_STB.T_ID_STEP             := NVL(p_STB.T_ID_STEP,0);
    p_STB.T_IDENTERRSTR         := NVL(p_STB.T_IDENTERRSTR, CHR(1));       
    p_STB.T_NEEDRECALC          := NVL(p_STB.T_NEEDRECALC, CHR(1));        
    p_STB.T_RECTAXBASEBYKIND    := NVL(p_STB.T_RECTAXBASEBYKIND, 0);            
    p_STB.T_INITIAL_DOCKIND     := NVL(p_STB.T_INITIAL_DOCKIND,0);            
    p_STB.T_INITIAL_DOCID       := NVL(p_STB.T_INITIAL_DOCID,0);
    p_STB.T_SENDDATE            := NVL(p_STB.T_SENDDATE,NPTAX.UnknownDate);
    p_STB.T_SENDTIME            := NVL(p_STB.T_SENDTIME,NPTAX.UnknownDate);
    p_STB.T_CANCELDATE          := NVL(p_STB.T_CANCELDATE,NPTAX.UnknownDate);
    p_STB.T_CANCELTIME          := NVL(p_STB.T_CANCELTIME,NPTAX.UnknownDate);
    p_STB.T_SYMBOL              := NVL(p_STB.T_SYMBOL,CHR(0));
    p_STB.t_dlcontrid           := NVL(p_STB.t_dlcontrid,0);
    p_STB.t_specialtag          := NVL(p_STB.t_specialtag, CHR(1));
    p_STB.t_syscome             := NVL(p_STB.t_syscome, 0);
  END;

  PROCEDURE RSI_SaveSTB(p_nptxtotalbase IN RAW, p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER)
  IS
    v_STB      DNPTXTOTALBASE_DBT%ROWTYPE;
    v_FindSTB  DNPTXTOTALBASE_DBT%ROWTYPE;
    v_STBbc    DNPTXTOTALBASEBC_DBT%ROWTYPE;
    v_Count    NUMBER;
  BEGIN
    InitError();

    RSI_CopyRAWtoNPTXTOTALBASE(p_nptxtotalbase, v_STB);

    IF v_STB.T_TBID = 0 THEN --Создание

      v_STB.T_INSTANCE     := 0;
      v_STB.T_ID_OPERATION := p_ID_Operation;
      v_STB.T_ID_STEP      := p_ID_Step;

      RSI_InsDfltIntoNPTXTOTALBASE(v_STB);
      INSERT INTO DNPTXTOTALBASE_DBT VALUES v_STB;

    ELSE --Обновление

      BEGIN
        SELECT * INTO v_FindSTB
          FROM DNPTXTOTALBASE_DBT
         WHERE t_TBID = v_STB.t_TBID;
      EXCEPTION
         WHEN OTHERS THEN
             SetError(RSI_NPTXC.NPTX_ERROR_20644); --Не найдена запись события СНОБ
      END;

      BEGIN
        v_STBbc.T_BCID                :=  0;                 
        v_STBbc.T_TBID                :=  v_FindSTB.T_TBID;                 
        v_STBbc.T_CLIENTID            :=  v_FindSTB.T_CLIENTID;             
        v_STBbc.T_TYPE                :=  v_FindSTB.T_TYPE;                 
        v_STBbc.T_DESCRIPTION         :=  v_FindSTB.T_DESCRIPTION;          
        v_STBbc.T_INCREGIONDATE       :=  v_FindSTB.T_INCREGIONDATE;        
        v_STBbc.T_INCREGIONTIME       :=  v_FindSTB.T_INCREGIONTIME;        
        v_STBbc.T_INCDATE             :=  v_FindSTB.T_INCDATE;              
        v_STBbc.T_INCTIME             :=  v_FindSTB.T_INCTIME;              
        v_STBbc.T_CONFIRMSTATE        :=  v_FindSTB.T_CONFIRMSTATE;         
        v_STBbc.T_SOURCESYSTEM        :=  v_FindSTB.T_SOURCESYSTEM;         
        v_STBbc.T_STORSTATE           :=  v_FindSTB.T_STORSTATE;            
        v_STBbc.T_DOCKIND             :=  v_FindSTB.T_DOCKIND;              
        v_STBbc.T_DOCID               :=  v_FindSTB.T_DOCID;                
        v_STBbc.T_TAXPERIOD           :=  v_FindSTB.T_TAXPERIOD;            
        v_STBbc.T_TAXBASEKIND         :=  v_FindSTB.T_TAXBASEKIND;          
        v_STBbc.T_TAXBASECURRPAY      :=  v_FindSTB.T_TAXBASECURRPAY;       
        v_STBbc.T_CALCPITAX           :=  v_FindSTB.T_CALCPITAX;            
        v_STBbc.T_RATECALCPITAX       :=  v_FindSTB.T_RATECALCPITAX;        
        v_STBbc.T_HOLDPITAX           :=  v_FindSTB.T_HOLDPITAX;            
        v_STBbc.T_RATEHOLDPITAX       :=  v_FindSTB.T_RATEHOLDPITAX;        
        v_STBbc.T_BCCCALCPITAX        :=  v_FindSTB.T_BCCCALCPITAX;         
        v_STBbc.T_BCCHOLDPITAX        :=  v_FindSTB.T_BCCHOLDPITAX;         
        v_STBbc.T_TAXPAYERSTATUS      :=  v_FindSTB.T_TAXPAYERSTATUS;       
        v_STBbc.T_APPLSTAXBASEINCLUDE :=  v_FindSTB.T_APPLSTAXBASEINCLUDE;  
        v_STBbc.T_APPLSTAXBASEEXCLUDE :=  v_FindSTB.T_APPLSTAXBASEEXCLUDE;  
        v_STBbc.T_RECSTAXBASE         :=  v_FindSTB.T_RECSTAXBASE;          
        v_STBbc.T_RECSTAXBASEDATE     :=  v_FindSTB.T_RECSTAXBASEDATE;      
        v_STBbc.T_RECSTAXBASETIME     :=  v_FindSTB.T_RECSTAXBASETIME;      
        v_STBbc.T_ORIGTBID            :=  v_FindSTB.T_ORIGTBID;             
        v_STBbc.T_STORID              :=  v_FindSTB.T_STORID;               
        v_STBbc.T_INSTANCE            :=  v_FindSTB.T_INSTANCE;             
        v_STBbc.T_ID_OPERATION        :=  v_FindSTB.T_ID_OPERATION;         
        v_STBbc.T_ID_STEP             :=  v_FindSTB.T_ID_STEP;
        v_STBbc.T_IDENTERRSTR         :=  v_FindSTB.T_IDENTERRSTR;          
        v_STBbc.T_NEEDRECALC          :=  v_FindSTB.T_NEEDRECALC;           
        v_STBbc.T_RECTAXBASEBYKIND    :=  v_FindSTB.T_RECTAXBASEBYKIND;     
        v_STBbc.T_INITIAL_DOCKIND     :=  v_FindSTB.T_INITIAL_DOCKIND;              
        v_STBbc.T_INITIAL_DOCID       :=  v_FindSTB.T_INITIAL_DOCID;
        v_STBbc.T_SENDDATE            :=  v_FindSTB.T_SENDDATE;
        v_STBbc.T_SENDTIME            :=  v_FindSTB.T_SENDTIME;
        v_STBbc.T_CANCELDATE          :=  v_FindSTB.T_CANCELDATE;
        v_STBbc.T_CANCELTIME          :=  v_FindSTB.T_CANCELTIME;
        v_STBbc.T_SYMBOL              :=  v_FindSTB.T_SYMBOL;
        v_STBbc.t_dlcontrid           :=  v_FindSTB.t_dlcontrid;
        v_STBbc.t_specialtag          :=  v_FindSTB.t_specialtag;
        v_STBbc.t_syscome             :=  v_FindSTB.t_syscome;

        RSI_InsDfltIntoNPTXTOTALBASEBC(v_STBbc);
        INSERT INTO DNPTXTOTALBASEBC_DBT VALUES v_STBbc;      
        
        EXCEPTION
        WHEN OTHERS THEN
            SetError(RSI_NPTXC.NPTX_ERROR_20645); --Ошибка при сохранении истории события СНОБ

      END;

      UPDATE DNPTXTOTALBASE_DBT
         SET T_DESCRIPTION         = v_STB.T_DESCRIPTION,
             T_INCREGIONDATE       = v_STB.T_INCREGIONDATE,
             T_INCREGIONTIME       = v_STB.T_INCREGIONTIME,
             T_INCDATE             = v_STB.T_INCDATE,
             T_INCTIME             = v_STB.T_INCTIME,
             T_CONFIRMSTATE        = v_STB.T_CONFIRMSTATE,
             T_SOURCESYSTEM        = v_STB.T_SOURCESYSTEM,
             T_STORSTATE           = v_STB.T_STORSTATE,
             T_TAXBASEKIND         = v_STB.T_TAXBASEKIND,
             T_TAXBASECURRPAY      = v_STB.T_TAXBASECURRPAY,
             T_CALCPITAX           = v_STB.T_CALCPITAX,
             T_RATECALCPITAX       = v_STB.T_RATECALCPITAX,
             T_HOLDPITAX           = v_STB.T_HOLDPITAX,
             T_RATEHOLDPITAX       = v_STB.T_RATEHOLDPITAX,
             T_BCCCALCPITAX        = v_STB.T_BCCCALCPITAX,
             T_BCCHOLDPITAX        = v_STB.T_BCCHOLDPITAX,
             T_TAXPAYERSTATUS      = v_STB.T_TAXPAYERSTATUS,
             T_APPLSTAXBASEINCLUDE = v_STB.T_APPLSTAXBASEINCLUDE,
             T_APPLSTAXBASEEXCLUDE = v_STB.T_APPLSTAXBASEEXCLUDE,
             T_RECSTAXBASE         = v_STB.T_RECSTAXBASE,
             T_RECSTAXBASEDATE     = v_STB.T_RECSTAXBASEDATE,
             T_RECSTAXBASETIME     = v_STB.T_RECSTAXBASETIME,
             T_STORID              = v_STB.T_STORID,
             T_ID_OPERATION        = p_ID_Operation,
             T_ID_STEP             = p_ID_Step,
             T_INSTANCE            = t_Instance + 1,
             T_IDENTERRSTR         = v_STB.T_IDENTERRSTR,     
             T_NEEDRECALC          = v_STB.T_NEEDRECALC,      
             T_RECTAXBASEBYKIND    = v_STB.T_RECTAXBASEBYKIND,
             T_INITIAL_DOCKIND     = v_STB.T_INITIAL_DOCKIND,
             T_INITIAL_DOCID       = v_STB.T_INITIAL_DOCID,
             T_SENDDATE            = v_STB.T_SENDDATE,
             T_SENDTIME            = v_STB.T_SENDTIME,
             T_CANCELDATE          = v_STB.T_CANCELDATE,
             T_CANCELTIME          = v_STB.T_CANCELTIME,
             T_SYMBOL              = v_STB.T_SYMBOL,
             t_dlcontrid           = v_STB.t_dlcontrid,
             t_specialtag          = v_STB.t_specialtag,
             t_syscome             = v_STB.t_syscome
       WHERE t_TBID = v_STB.t_TBID;

    END IF;

  END; --RSI_SaveSTB

  -- Выполняет восстановление события СНОБ по архивным данным
  PROCEDURE RSI_RestoreSTB(p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER, p_TBID IN NUMBER DEFAULT 0 )
  IS
    v_Count    NUMBER;
    v_stbbc    DNPTXTOTALBASEBC_DBT%ROWTYPE;
    v_N        NUMBER := 0;
  BEGIN

    -- 1.
    IF (p_ID_Operation = 0 AND p_ID_Step = 0) THEN
       RETURN;
    END IF;

    IF p_TBID = 0 THEN --Откатывать только те, которые были созданы на шаге, т.е. на тот момент TBID был не известен

      SELECT COUNT(1) INTO v_N
        FROM DNPTXTOTALBASEBC_DBT BC
       WHERE BC.T_ID_OPERATION = p_ID_Operation
         AND BC.T_ID_STEP = p_ID_Step 
         AND BC.T_INSTANCE = 0
         AND EXISTS(SELECT 1 FROM DNPTXTOTALBASE_DBT TB WHERE TB.T_TBID = BC.T_TBID AND (TB.T_ID_OPERATION <> p_ID_Operation OR TB.T_ID_STEP <> p_ID_Step));

      IF v_N > 0 THEN
        SetError(RSI_NPTXC.NPTX_ERROR_20646); --Откатываемая операция не является последней для события СНОБ
      END IF;
    END IF;


    SELECT COUNT(1) INTO v_N
      FROM DNPTXTOTALBASE_DBT
     WHERE T_ID_OPERATION = p_ID_Operation
       AND T_ID_STEP = p_ID_Step
       AND T_TBID = (CASE WHEN p_TBID > 0 THEN p_TBID ELSE T_TBID END)
       AND T_INSTANCE = (CASE WHEN p_TBID > 0 THEN T_INSTANCE ELSE 0 END);

    WHILE v_N > 0 LOOP

      FOR one_stb IN (SELECT *
                       FROM DNPTXTOTALBASE_DBT
                      WHERE t_ID_Operation = p_ID_Operation 
                        AND t_ID_Step      = p_ID_Step 
                        AND T_TBID = (CASE WHEN p_TBID > 0 THEN p_TBID ELSE T_TBID END)
                        AND T_INSTANCE = (CASE WHEN p_TBID > 0 THEN T_INSTANCE ELSE 0 END)
                    ) LOOP

         --Вставить запись во временную таблицу с идентификатором из внешнего Хранилища, чтобы использовать его в PostStep при откате,
         --так как он может быть потерян в этом месте отката.
         RSI_NPTMSG.AddTbMesTMP(one_stb.T_TBID, 22, one_stb.T_STORID||';'||TO_CHAR(one_stb.T_STORSTATE));

         IF one_stb.t_Instance = 0 THEN
           DELETE FROM DNPTXTOTALBASE_DBT
               WHERE t_TBID = one_stb.t_TBID;
         ELSE
            BEGIN
              SELECT * INTO v_stbbc
                FROM DNPTXTOTALBASEBC_DBT
               WHERE t_TBID     = one_stb.t_TBID AND
                     t_Instance = (one_stb.t_Instance - 1);
              EXCEPTION
                 WHEN OTHERS THEN
                   v_stbbc := NULL;
            END;

            IF (v_stbbc.T_BCID IS NOT NULL) THEN
              UPDATE DNPTXTOTALBASE_DBT
                 SET T_CLIENTID            = v_stbbc.T_CLIENTID,             
                     T_TYPE                = v_stbbc.T_TYPE,                 
                     T_DESCRIPTION         = v_stbbc.T_DESCRIPTION,          
                     T_INCREGIONDATE       = v_stbbc.T_INCREGIONDATE,        
                     T_INCREGIONTIME       = v_stbbc.T_INCREGIONTIME,        
                     T_INCDATE             = v_stbbc.T_INCDATE,              
                     T_INCTIME             = v_stbbc.T_INCTIME,              
                     T_CONFIRMSTATE        = v_stbbc.T_CONFIRMSTATE,         
                     T_SOURCESYSTEM        = v_stbbc.T_SOURCESYSTEM,         
                     T_STORSTATE           = v_stbbc.T_STORSTATE,            
                     T_DOCKIND             = v_stbbc.T_DOCKIND,              
                     T_DOCID               = v_stbbc.T_DOCID,                
                     T_TAXPERIOD           = v_stbbc.T_TAXPERIOD,            
                     T_TAXBASEKIND         = v_stbbc.T_TAXBASEKIND,          
                     T_TAXBASECURRPAY      = v_stbbc.T_TAXBASECURRPAY,       
                     T_CALCPITAX           = v_stbbc.T_CALCPITAX,            
                     T_RATECALCPITAX       = v_stbbc.T_RATECALCPITAX,        
                     T_HOLDPITAX           = v_stbbc.T_HOLDPITAX,            
                     T_RATEHOLDPITAX       = v_stbbc.T_RATEHOLDPITAX,        
                     T_BCCCALCPITAX        = v_stbbc.T_BCCCALCPITAX,         
                     T_BCCHOLDPITAX        = v_stbbc.T_BCCHOLDPITAX,         
                     T_TAXPAYERSTATUS      = v_stbbc.T_TAXPAYERSTATUS,       
                     T_APPLSTAXBASEINCLUDE = v_stbbc.T_APPLSTAXBASEINCLUDE,  
                     T_APPLSTAXBASEEXCLUDE = v_stbbc.T_APPLSTAXBASEEXCLUDE,  
                     T_RECSTAXBASE         = v_stbbc.T_RECSTAXBASE,          
                     T_RECSTAXBASEDATE     = v_stbbc.T_RECSTAXBASEDATE,      
                     T_RECSTAXBASETIME     = v_stbbc.T_RECSTAXBASETIME,      
                     T_ORIGTBID            = v_stbbc.T_ORIGTBID,             
                     T_STORID              = v_stbbc.T_STORID,               
                     T_INSTANCE            = v_stbbc.T_INSTANCE,             
                     T_ID_OPERATION        = v_stbbc.T_ID_OPERATION,         
                     T_ID_STEP             = v_stbbc.T_ID_STEP,
                     T_IDENTERRSTR         = v_stbbc.T_IDENTERRSTR,     
                     T_NEEDRECALC          = v_stbbc.T_NEEDRECALC,      
                     T_RECTAXBASEBYKIND    = v_stbbc.T_RECTAXBASEBYKIND,
                     T_INITIAL_DOCKIND     = v_stbbc.T_INITIAL_DOCKIND,              
                     T_INITIAL_DOCID       = v_stbbc.T_INITIAL_DOCID,
                     T_SENDDATE            = v_stbbc.T_SENDDATE,
                     T_SENDTIME            = v_stbbc.T_SENDTIME,
                     T_CANCELDATE          = v_stbbc.T_CANCELDATE,
                     T_CANCELTIME          = v_stbbc.T_CANCELTIME,
                     T_SYMBOL              = v_stbbc.T_SYMBOL,
                     t_dlcontrid           = v_stbbc.t_dlcontrid,
                     t_specialtag          = v_stbbc.t_specialtag,
                     t_syscome             = v_stbbc.t_syscome
               WHERE t_TBID = v_stbbc.T_TBID;

               DELETE FROM DNPTXTOTALBASEBC_DBT
                WHERE t_BCID = v_stbbc.T_BCID;

             END IF;

         END IF;

      END LOOP;

      SELECT COUNT(1) INTO v_N
        FROM DNPTXTOTALBASE_DBT
       WHERE T_ID_OPERATION = p_ID_Operation
         AND T_ID_STEP = p_ID_Step
         AND T_TBID = (CASE WHEN p_TBID > 0 THEN p_TBID ELSE T_TBID END)
         AND T_INSTANCE = (CASE WHEN p_TBID > 0 THEN T_INSTANCE ELSE 0 END);

    END LOOP;

    SELECT COUNT(1) INTO v_N
      FROM DNPTXTOTALBASEBC_DBT
     WHERE T_ID_OPERATION = p_ID_Operation
       AND T_ID_STEP = p_ID_Step
       AND T_TBID = (CASE WHEN p_TBID > 0 THEN p_TBID ELSE T_TBID END)
       AND T_INSTANCE = (CASE WHEN p_TBID > 0 THEN T_INSTANCE ELSE 0 END);

    IF v_N > 0 THEN
      SetError(RSI_NPTXC.NPTX_ERROR_20646); --Откатываемая операция не является последней для события СНОБ
    END IF;

  END;--RSI_RestoreSTB

  FUNCTION NptxCalcTaxPrevDate( p_Client IN NUMBER, p_IIS IN CHAR, p_OperDate IN DATE ) RETURN DATE
  IS
     v_Date DATE;
  BEGIN
     SELECT NVL(MAX(T_OPERDATE), TO_DATE('01.01.0001','DD.MM.YYYY'))
       INTO v_Date
       FROM DNPTXOP_DBT
      WHERE T_DOCKIND = RSI_NPTXC.DL_CALCNDFL
        AND T_RECALC = CHR(0)
        AND T_CLIENT = p_Client
        AND T_IIS    = p_IIS
        AND T_OPERDATE <= p_OperDate
        AND T_STATUS <> RSI_NPTXC.DL_TXOP_Prep
        AND T_SUBKIND_OPERATION <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_DIVIDEND;
     RETURN v_Date;
  END;


  PROCEDURE RSI_ChangeStatusTbBuf(p_BFID IN NUMBER, p_NewStatus IN NUMBER, p_ProcDate IN DATE, p_ProcTime IN DATE, p_CanclDate IN DATE, p_CanclTime IN DATE)
  IS
  BEGIN
    UPDATE DNPTXTBBUF_DBT
       SET T_STATUS = p_NewStatus,
           T_TIMESTAMPPROCDATE = p_ProcDate,
           T_TIMESTAMPPROCTIME = p_ProcTime,
           T_TIMESTAMPCANCLDATE = p_CanclDate,
           T_TIMESTAMPCANCLTIME = p_CanclTime
     WHERE T_BFID = p_BFID;
  END;  
  

  FUNCTION GetActualBFIDForTB(p_TBID IN NUMBER) RETURN NUMBER
  IS
    v_BFID NUMBER := 0;
  BEGIN

    SELECT NVL(MAX(t_BFID), 0)
      INTO v_BFID
      FROM dnptxtbbuf_dbt
     WHERE t_EventID = p_TBID;

    RETURN v_BFID;
  END;

  FUNCTION GetNeedRecalcFlagForTB(p_TBID IN NUMBER) RETURN NUMBER
  IS
    v_Flag NUMBER := 0;
  BEGIN

    SELECT CASE WHEN t_needrecalc <> chr(1) THEN 1 ELSE 0 END
      INTO v_Flag
      FROM dnptxtotalbase_dbt
     WHERE t_TBID = p_TBID;

    RETURN v_Flag;
  END;

  FUNCTION GetCircRecalcStatus RETURN NUMBER
  IS
  BEGIN
    RETURN MarketPrice.CircRecalcStatus;
  END;

  PROCEDURE InsertCheckNDFL (p_ID_Operation IN NUMBER)
  IS
  BEGIN
      INSERT INTO DNPTXCHECK_NDR_SKR_DBT (T_ID,
                                          T_CLIENTID,
                                          T_OPERATION_ID,
                                          T_INCOME,
                                          T_EXPENSE,
                                          T_NOB_NDR,
                                          T_NOB_SKR,
                                          T_NOB13,
                                          T_NOB30,
                                          T_NOB15,
                                          T_NOB18,
                                          T_NOB20,
                                          T_NOB22,
                                          T_CHECK_NOB,
                                          T_NDFL_NDR,
                                          T_NDFL_SKR,
                                          T_NDFL_13,
                                          T_NDFL_30,
                                          T_NDFL_15,
                                          T_NDFL_18,
                                          T_NDFL_20,
                                          T_NDFL_22,
                                          T_CHECK_NDFL,
                                          T_CHECK_STG)
         SELECT 0,
                T_CLIENTID,
                T_OPERATION_ID,
                T_INCOME,
                T_EXPENSE,
                T_NOB_NDR,
                T_NOB_SKR,
                T_NOB13,
                T_NOB30,
                T_NOB15,
                T_NOB18,
                T_NOB20,
                T_NOB22,
                T_CHECK_NOB,
                T_NDFL_NDR,
                T_NDFL_SKR,
                T_NDFL_13,
                T_NDFL_30,
                T_NDFL_15,
                T_NDFL_18,
                T_NDFL_20,
                T_NDFL_22,
                T_CHECK_NDFL,
                T_CHECK_STG
           FROM DNPTXCHECK_NDR_SKR_TMP;
  END;

  PROCEDURE DeleteCheckNDFL (p_ID_Operation IN NUMBER)
  IS
  BEGIN
    DELETE FROM DNPTXCHECK_NDR_SKR_DBT
          WHERE T_OPERATION_ID = p_ID_Operation;
  END;
  
  FUNCTION GetErrCntVer(p_ID_Operation IN NUMBER, p_DocKind IN NUMBER) RETURN NUMBER
  IS
    v_Cnt NUMBER := 0;
  BEGIN
    
    IF p_DocKind = DL_SNOBVER THEN
      SELECT COUNT (1)
        INTO v_Cnt
        FROM dnptxsnobvertb_dbt nptxsnobvertb
       WHERE nptxsnobvertb.t_BatchID = p_ID_Operation
         AND nptxsnobvertb.t_Checked <> 'X';
    ELSIF p_DocKind = DL_SVERNOBNDFL THEN
      SELECT COUNT (1)
        INTO v_Cnt
        FROM DNPTXCHECK_NDR_SKR_DBT t 
       WHERE (t.T_CHECK_NDFL <> 0 OR t.T_CHECK_NOB <> 0) AND t.T_OPERATION_ID = p_ID_Operation;
    END IF;
  
    RETURN v_Cnt;
  END;
  
  FUNCTION IsLastOperVerForEventID(p_EventID IN NUMBER, p_BatchID IN NUMBER) RETURN NUMBER
  IS
    v_BatchID NUMBER := 0;
  BEGIN
  
    SELECT t2.t_BatchID
       INTO  v_BatchID
       FROM dnptxsnobvertb_dbt t2, dnptxsnobver_dbt t1
     WHERE  t2.t_BatchID = t1.t_ID AND t2.t_event_id = p_EventID ORDER BY t_operdate DESC FETCH FIRST 1 ROW ONLY;

    RETURN CASE WHEN v_BatchID = p_BatchID THEN 1 ELSE 0 END;
  END;
  
END RSI_NPTO;
/
