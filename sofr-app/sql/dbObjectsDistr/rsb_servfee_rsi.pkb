
CREATE OR REPLACE PACKAGE BODY RSI_RSB_SERVFEE AS
/******************************************************************************
   NAME:       RSI_RSB_SERVFEE
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        13.03.2009  SolAN            1. Created this package body.
******************************************************************************/

  OBJTYPE_DEPARTMENT            CONSTANT PLS_INTEGER := 80;

  SFCOMISS_LOCALIZATION_COMMON  CONSTANT PLS_INTEGER := 0;
  SFCOMISS_LOCALIZATION_FILIAL  CONSTANT PLS_INTEGER := 1;
  SFCOMISS_LOCALIZATION_VSP     CONSTANT PLS_INTEGER := 2;

  SF_PLAN_LOCALIZATION          CONSTANT PLS_INTEGER := 3;

  I_DEPARTMENT_TYPE_FILIAL      CONSTANT PLS_INTEGER := 1;



  -- Определить, локализована ли комиссия в узле ТС
  FUNCTION  DefineLocalLevel( feeType IN NUMBER, commNumber IN NUMBER, Node IN NUMBER, SfPlanID IN NUMBER, LocalMode IN NUMBER )
  RETURN INTEGER AS

    LocalLevel INTEGER DEFAULT 0;
    v_Localized PLS_INTEGER := 0;

    v_NodeType PLS_INTEGER := -1;
    v_ParentCode PLS_INTEGER := -1;

  BEGIN

    IF LocalMode = SF_PLAN_LOCALIZATION THEN
      LocalLevel := SFCOMISS_LOCALIZATION_COMMON;
    ELSE
      SELECT t_NodeType, t_ParentCode INTO v_NodeType, v_ParentCode FROM ddp_dep_dbt WHERE t_Code = Node;

      SELECT Count(1) INTO v_Localized FROM dsfconcom_dbt t
        WHERE t.t_feeType = feeType AND t.t_commNumber = commNumber
          AND t.t_ObjectID = Node AND t.t_ObjectType = OBJTYPE_DEPARTMENT AND t.t_SfPlanID = SfPlanID;

      IF v_NodeType = I_DEPARTMENT_TYPE_FILIAL THEN
        IF v_Localized = 0 THEN
          LocalLevel := SFCOMISS_LOCALIZATION_COMMON;
        ELSE
          LocalLevel := SFCOMISS_LOCALIZATION_FILIAL;
        END IF;
      ELSE
        IF v_Localized > 0 THEN
          LocalLevel := SFCOMISS_LOCALIZATION_VSP;
        ELSE
          v_Localized := 0;
          SELECT Count(1) INTO v_Localized FROM dsfconcom_dbt t
            WHERE t.t_feeType = feeType AND t.t_commNumber = commNumber
              AND t.t_ObjectID = v_ParentCode AND t.t_ObjectType = OBJTYPE_DEPARTMENT AND t.t_SfPlanID = SfPlanID;

          IF v_Localized = 0 THEN
            LocalLevel := SFCOMISS_LOCALIZATION_COMMON;
          ELSE
            LocalLevel := SFCOMISS_LOCALIZATION_FILIAL;
          END IF;
        END IF;
      END IF;
    END IF;

    RETURN LocalLevel;

  END DefineLocalLevel;


  FUNCTION  SetSfDefIsIncluded( p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER, p_FeeType IN NUMBER, p_DefComID IN NUMBER )
  RETURN INTEGER AS

  BEGIN

    RSI_RSBOPERATION.SetBkoutDataForStep( p_ID_Operation, p_ID_Step,
                ' UPDATE dsfdef_dbt SET t_IsIncluded = chr(0) WHERE t_FeeType =' || to_char(p_FeeType)
             || ' AND t_ID = ' || to_char(p_DefComID) );

    UPDATE dsfdef_dbt SET t_IsIncluded = 'X' WHERE t_FeeType = p_FeeType AND t_ID = p_DefComID;

    RETURN 0;

  END SetSfDefIsIncluded;

  FUNCTION SetSfInvIsIncluded( p_InvoiceID IN NUMBER, p_ValueDate IN DATE, p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER )
  RETURN INTEGER AS

    CURSOR sfdefs IS SELECT * FROM dsfdef_dbt WHERE t_InvoiceID = p_InvoiceID;
    v_AccrueID NUMBER;

  BEGIN
    RSI_RSBOPERATION.SetBkoutDataForStep( p_ID_Operation, p_ID_Step,
                'UPDATE dsfinv_dbt SET t_IsIncluded = chr(0) WHERE t_InvoiceID = ' || to_char(p_InvoiceID) );
    UPDATE dsfinv_dbt SET t_IsIncluded = 'X' WHERE t_InvoiceID = p_InvoiceID;

    RSI_RSBOPERATION.SetBkoutDataForStep( p_ID_Operation, p_ID_Step,
                ' UPDATE dsfdef_dbt SET t_IsIncluded = chr(0) WHERE t_InvoiceID = ' || to_char(p_InvoiceID) );
    UPDATE dsfdef_dbt SET t_IsIncluded = 'X' WHERE t_InvoiceID = p_InvoiceID;

    FOR rec IN sfdefs LOOP

      INSERT INTO dsfaccrue_dbt
        ( t_ID, t_SfDefComID, t_BeginDate, t_EndDate, t_TransactionDate, t_Amount, t_NDSAmount, t_IsFinal )
      VALUES
        ( 0, rec.t_ID, rec.t_DatePeriodEnd, rec.t_DatePeriodEnd, p_ValueDate, rec.t_Sum, rec.t_SumNDS, chr(88) )
      RETURNING t_ID INTO v_AccrueID;

      RSI_RSBOPERATION.SetBkoutDataForStep( p_ID_Operation, p_ID_Step,
                  'DELETE FROM dsfaccrue_dbt WHERE t_ID = ' || to_char(v_AccrueID) );

    END LOOP;

    RETURN 0;

  END SetSfInvIsIncluded;

  FUNCTION CorrectSfInv( p_InvoiceID IN NUMBER, p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER )
  RETURN INTEGER AS

    inv dsfinv_dbt%ROWTYPE;
  BEGIN
    SELECT * INTO inv FROM dsfinv_dbt WHERE t_InvoiceID = p_InvoiceID;

    RSI_RSBOPERATION.SetBkoutDataForStep( p_ID_Operation, p_ID_Step,
         ' UPDATE dsfinv_dbt SET t_BeneChapter = ' || to_char(inv.t_BeneChapter)
      || ', t_BeneAccount = ' || chr(39) || inv.t_BeneAccount || chr(39) || ', t_BeneID = ' || to_char(inv.t_BeneID)
      || ', t_BeneCodeKind = '|| to_char(inv.t_BeneCodeKind)
      || ', t_BeneCode = ' || chr(39) || inv.t_BeneCode || chr(39) || ', t_BeneName = ' || chr(39) || inv.t_BeneName || chr(39)
      || ', t_BeneINN = ' || chr(39) || inv.t_BeneINN || chr(39)
      || ', t_BeneBankID = '|| to_char(inv.t_BeneBankID) || ', t_BeneBankCodeKind = ' || to_char(inv.t_BeneBankCodeKind)
      || ', t_BeneBankCode = '|| chr(39) || inv.t_BeneBankCode || chr(39)
      || ', t_BeneBankName = ' || chr(39) || inv.t_BeneBankName || chr(39)
      || ', t_BeneCorrAcc = ' || chr(39) || inv.t_BeneCorrAcc || chr(39)
      || ', t_BeneCorrBankCodeKind = '|| to_char(inv.t_BeneCorrBankCodeKind)
      || ', t_BeneCorrBankID = ' || to_char(inv.t_BeneCorrBankID)
      || ', t_BeneCorrBankCode = ' || chr(39) || inv.t_BeneCorrBankCode || chr(39)
      || ', t_BeneCorrBankName = ' || chr(39) || inv.t_BeneCorrBankName || chr(39)
      || ' WHERE t_InvoiceID = ' || to_char(p_InvoiceID) );

    RETURN 0;
  END CorrectSfInv;

  FUNCTION SetBkoutDataOfChargeComCrypt( p_IsForSfInv IN CHAR, p_ContextID IN VARCHAR2, p_BankID IN NUMBER ) RETURN INTEGER AS

    cur_BkoutData  RSI_RSBOPERATION.BkoutData_cur;
  BEGIN
    IF p_IsForSfInv = 'X' THEN
      OPEN cur_BkoutData FOR
        SELECT tmp.t_ID_Operation, tmp.t_ID_Step,
               'DELETE FROM dsgnamark_dbt sgn WHERE sgn.t_DocKind = ' || to_char(pm.t_DocKind) ||
                 ' AND sgn.t_DocID = ' || LPAD( to_char(pm.t_PaymentID), 34, '0') ||
                 ' AND sgn.t_ContextID = ' || chr(39) || p_ContextID || chr(39)
          FROM doprtemp_tmp tmp, dpmpaym_dbt pm, dsfinvlnk_dbt lnk, dsfinv_dbt inv
          WHERE pm.t_PaymentID = tmp.t_OrderID AND pm.t_PaymentID = lnk.t_PaymentID AND lnk.t_InvoiceID = inv.t_InvoiceID
            AND tmp.t_ErrorStatus = 0 AND pm.t_Payer != p_BankID AND t_IsIncluded = chr(88);
    ELSE
      OPEN cur_BkoutData FOR
        SELECT tmp.t_ID_Operation, tmp.t_ID_Step,
               'DELETE FROM dsgnamark_dbt sgn WHERE sgn.t_DocKind = ' || to_char(pm.t_DocKind) ||
               ' AND sgn.t_DocID = ' || LPAD( to_char(pm.t_PaymentID), 34, '0') ||
               ' AND sgn.t_ContextID = ' || chr(39) || p_ContextID || chr(39)
          FROM doprtemp_tmp tmp, dpmpaym_dbt pm, dsfdef_dbt def
          WHERE pm.t_PaymentID = tmp.t_OrderID AND (pm.t_FeeType = 1 OR pm.t_FeeType = 3 OR pm.t_FeeType = 6)
            AND pm.t_FeeType = def.t_FeeType AND pm.t_DefComID = def.t_ID AND def.t_IsIncluded = chr(88)
            AND tmp.t_ErrorStatus = 0 AND pm.t_Payer != p_BankID;
   END IF;

   RSI_RSBOPERATION.SetBkoutDataForAll( cur_BkoutData );
   CLOSE cur_BkoutData;

   RETURN 0;
  END SetBkoutDataOfChargeComCrypt;


  FUNCTION DeletePmAddPI( p_PaymentID IN NUMBER, p_DebetCredit IN NUMBER, p_Account IN VARCHAR2, p_AccountNDS IN VARCHAR2,
                          p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER ) RETURN INTEGER AS

    --cur_BkoutData  RSI_RSBOPERATION.BkoutData_cur;
  BEGIN
    --OPEN cur_BkoutData FOR
    --   SELECT t_ID_Operation, t_ID_Step,
    --RSI_RSBOPERATION.SetBkoutDataForAll( cur_BkoutData );
    --CLOSE cur_BkoutData;

    DELETE FROM dpmaddpi_dbt WHERE t_PaymentID = p_PaymentID AND t_DebetCredit = p_DebetCredit;
    RETURN 0;
  END DeletePmAddPI;

  -- Определение предельной даты окончания периода начисления
  -- p_sfsrvdocEnd      - дата окнчания периода начисления, заданная в сервисной операции
  -- p_sfcomissEnd      - дата окончания комиссии dsfcomiss_dbt
  -- p_sfcontrDateClose - дата закрытия ДО
  -- p_sfconcomEnd      - дата окончания комиссии ДО
  FUNCTION GetFinishFeeDate( p_sfsrvdocEnd IN DATE, p_sfcomissEnd IN DATE, p_sfcontrDateClose IN DATE, p_sfconcomEnd IN DATE ) RETURN DATE AS
    v_ZeroDate DATE := to_date('01.01.0001', 'DD.MM.YYYY');
    v_FinishDate DATE;
    v_EndDate DATE;
  BEGIN
    v_FinishDate := p_sfcomissEnd;

    IF( v_FinishDate = v_ZeroDate ) THEN
      v_FinishDate := p_sfcontrDateClose;
    ELSE 
      IF( p_sfcontrDateClose <> v_ZeroDate ) THEN
        IF (v_FinishDate < p_sfcontrDateClose) THEN
           v_FinishDate := v_FinishDate;
        ELSE
           v_FinishDate := p_sfcontrDateClose;
        END IF;
      END IF;
    END IF;

    IF( p_sfconcomEnd <> v_ZeroDate ) THEN
      IF( v_FinishDate = v_ZeroDate ) THEN
        v_FinishDate := p_sfconcomEnd/* - 1*/;/* убрано -1 по def-38951*/
      ELSE
        v_EndDate := p_sfconcomEnd/* - 1*/; /* убрано -1 по def-38951*/
        IF (v_FinishDate >= v_EndDate) THEN
          v_FinishDate := v_EndDate;
        END IF;
      END IF;
    END IF;
     
    IF(v_FinishDate = v_ZeroDate) THEN
      v_FinishDate := p_sfsrvdocEnd;
    END IF;
 
    RETURN v_FinishDate;
  END GetFinishFeeDate;


  -- Функция получения среднего остатка, для алгоритма "по остатку на счете" 

   FUNCTION RestAPC (
      p_account_id IN NUMBER,
      p_dateb      IN DATE,
      p_datee      IN DATE,
      p_r0         IN drestdate_dbt.t_Rest%TYPE,
      p_rest_cur   IN NUMBER DEFAULT NULL
   )
      RETURN drestdate_dbt.t_Rest%TYPE
        AS
      v_rest        drestdate_dbt.t_Rest%TYPE         := 0;
      v_temp_rest   drestdate_dbt.t_Rest%TYPE;
      v_datestart   drestdate_dbt.t_RestDate%TYPE;
      v_datee       drestdate_dbt.t_RestDate%TYPE;
      v_dateend     drestdate_dbt.t_RestDate%TYPE;
      v_restdate    drestdate_dbt.t_RestDate%TYPE;
      v_prevdate    drestdate_dbt.t_RestDate%TYPE;
      v_flagfirst   BOOLEAN                           := TRUE;
      v_date_b      DATE;
      v_rest_cur    NUMBER;
      CURSOR c_rest (cp_startdate DATE, cp_enddate DATE, cp_account_id NUMBER, cp_rest_cur NUMBER)
      IS                               --параметризованный курсор для выборки
         SELECT   t_rest, t_RestDate
             FROM drestdate_dbt
            WHERE t_accountID = cp_account_id
              AND t_restcurrency = cp_rest_cur
              AND t_RestDate <= TO_DATE( TO_CHAR(cp_enddate,'DD.MM.YYYY') || ' 23:59:59', 'DD.MM.YYYY HH24:MI:SS' )
              AND t_RestDate >= TRUNC (cp_startdate)
         ORDER BY t_RestDate DESC;
   BEGIN
      IF p_account_id IS NULL
      THEN
         RETURN v_rest;
      END IF;

      v_datee := p_datee;

      -- Если валюта отстатка не передана, считаем что остаток в той же валюте что и счет.
      IF (p_rest_cur IS NULL ) THEN
        SELECT t_code_currency INTO v_rest_cur
          FROM daccount_dbt 
         WHERE t_AccountID = p_account_id;
      ELSE
        v_rest_cur := p_rest_cur;
      END IF; 


      IF TRUNC (p_dateb) >= TRUNC (v_datee)
      THEN
        v_date_b := TO_DATE( TO_CHAR(v_datee,'DD.MM.YYYY') || ' 23:59:59', 'DD.MM.YYYY HH24:MI:SS' );
         -- если дата начала больше или равна дате конца - вернуть остаток на этот день
        BEGIN
            SELECT NVL (t_rest, 0)
              INTO v_rest
              FROM drestdate_dbt
             WHERE t_RestDate =
                      (SELECT MAX (t_RestDate)
                         FROM drestdate_dbt
                        WHERE t_accountID = p_account_id
                          AND t_restcurrency = v_rest_cur
                          AND t_RestDate <= v_date_b)
               AND t_accountID = p_account_id
               AND t_restcurrency = v_rest_cur;
        EXCEPTION
            WHEN TOO_MANY_ROWS THEN
                RETURN NULL;
        END;
      ELSE                                  --расчет среднего хронологического
         v_date_b := TO_DATE( TO_CHAR(p_dateb,'DD.MM.YYYY') || ' 23:59:59', 'DD.MM.YYYY HH24:MI:SS' );
         BEGIN
            SELECT t_rest, t_RestDate
              INTO v_rest, v_datestart
              FROM drestdate_dbt
             WHERE t_RestDate =
                      (SELECT MAX (t_RestDate)
                         FROM drestdate_dbt
                        WHERE t_accountID = p_account_id
                          AND t_restcurrency = v_rest_cur
                          AND t_RestDate <= v_date_b)
               AND t_accountID = p_account_id
               AND t_restcurrency = v_rest_cur;

         EXCEPTION
            WHEN TOO_MANY_ROWS THEN
                RETURN NULL;
            WHEN NO_DATA_FOUND
            THEN
               v_datestart := cnst.mindate;
               v_rest := 0;
         END;

         --Определение даты начала и остатка на эту дату
         v_rest := v_rest / 2;               --половина остатка начала периода
         v_restdate := v_datee;
         v_prevdate := v_datee;

         FOR v_restdata IN c_rest (v_datestart, v_datee, p_account_id, v_rest_cur)
         LOOP
            IF (v_flagfirst = TRUE)
            THEN
               v_flagfirst := FALSE;

                  v_rest := v_rest + v_restdata.t_rest / 2;
               --половина остатка конца периода
               --конец считается два раза (как конец и как часть периода)
            --, так и надо.
            END IF;

            v_restdate := v_restdata.t_RestDate;

            IF (TRUNC (v_restdate) <= TRUNC (v_datestart) + 1)
            THEN
               v_restdate := p_dateb + 1;
            END IF;

            v_rest := v_rest + v_restdata.t_rest * (v_prevdate - v_restdate);
            v_prevdate := v_restdate;
         END LOOP;

         v_rest := v_rest / (v_datee - p_dateb);
      END IF;

      RETURN v_rest;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN v_rest;
      WHEN OTHERS
      THEN
         rsi_errors.err_msg := 'restapc ' || SQLERRM (SQLCODE);
         RETURN v_rest;
   END RestAPC;


END RSI_RSB_SERVFEE;
//
