CREATE OR REPLACE TRIGGER "DACCTRN_DBT_PICKUPDEL"
   AFTER DELETE OR UPDATE
   ON dacctrn_dbt
   REFERENCING NEW AS NEW OLD AS OLD
   FOR EACH ROW
DECLARE
   v_is_exists NUMBER(5); -- Признак наличия записи
   v_deletesource UPICKUPDEL_DBT.T_DELETESOURCE%TYPE := chr(1);
   v_upickupdel UPICKUPDEL_DBT%rowtype;
BEGIN
      v_deletesource :=sys_context ('USERENV','CURRENT_USER')      || ';'
                    ||sys_context ('USERENV','OS_USER')|| ';'
                    ||sys_context ('USERENV','HOST')|| ';'
                    ||sys_context ('USERENV','TERMINAL')|| ';'
                    ||sys_context ('USERENV','MODULE')|| ';'
                    ||sys_context ('USERENV','SESSIONID');
         
   -- В upickupdel_dbt храним только данные по факту первого удаления (либо смены статуса в 4)
   BEGIN
      SELECT 1 INTO v_is_exists FROM upickupdel_dbt WHERE t_acctrnid = :old.T_ACCTRNID;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         v_is_exists := 0;
      WHEN OTHERS
      THEN
         v_is_exists := 0;
   END;

   IF v_is_exists = 0 AND (DELETING 
                          OR 
                          (UPDATING AND :new.T_STATE = 4))
   THEN
      v_upickupdel := NULL;
      
      v_upickupdel.T_RECID := NULL;
      v_upickupdel.T_TIMESTAMP := SYSTIMESTAMP;
      v_upickupdel.T_ACCTRNID := :old.T_ACCTRNID;
      v_upickupdel.T_CHAPTER := :old.T_CHAPTER;
      v_upickupdel.T_DATE_CARRY := :old.T_DATE_CARRY;
      v_upickupdel.T_DATE_RATE := :old.T_DATE_RATE;
      v_upickupdel.T_FIID_PAYER := :old.T_FIID_PAYER;
      v_upickupdel.T_FIID_RECEIVER := :old.T_FIID_RECEIVER;
      v_upickupdel.T_ACCOUNTID_PAYER := :old.T_ACCOUNTID_PAYER;
      v_upickupdel.T_ACCOUNTID_RECEIVER := :old.T_ACCOUNTID_RECEIVER;
      v_upickupdel.T_ACCOUNT_PAYER := :old.T_ACCOUNT_PAYER;
      v_upickupdel.T_ACCOUNT_RECEIVER := :old.T_ACCOUNT_RECEIVER;
      v_upickupdel.T_SUM_NATCUR := :old.T_SUM_NATCUR;
      v_upickupdel.T_SUM_PAYER := :old.T_SUM_PAYER;
      v_upickupdel.T_SUM_RECEIVER := :old.T_SUM_RECEIVER;
      v_upickupdel.T_FIIDEQ_PAYER := :old.T_FIIDEQ_PAYER;
      v_upickupdel.T_FIIDEQ_RECEIVER := :old.T_FIIDEQ_RECEIVER;
      v_upickupdel.T_SKIPRESTEQCHANGE := :old.T_SKIPRESTEQCHANGE;
      v_upickupdel.T_SUMEQ_PAYER := :old.T_SUMEQ_PAYER;
      v_upickupdel.T_SUMEQ_RECEIVER := :old.T_SUMEQ_RECEIVER;
      v_upickupdel.T_RESULT_CARRY := :old.T_RESULT_CARRY;
      v_upickupdel.T_NUMBER_PACK := :old.T_NUMBER_PACK;
      v_upickupdel.T_OPER := :old.T_OPER;
      v_upickupdel.T_DEPARTMENT := :old.T_DEPARTMENT;
      v_upickupdel.T_BRANCH := :old.T_BRANCH;
      v_upickupdel.T_NUMB_DOCUMENT := :old.T_NUMB_DOCUMENT;
      v_upickupdel.T_GROUND := :old.T_GROUND;
      v_upickupdel.T_SHIFR_OPER := :old.T_SHIFR_OPER;
      v_upickupdel.T_KIND_OPER := :old.T_KIND_OPER;
      v_upickupdel.T_TYPEDOCUMENT := :old.T_TYPEDOCUMENT;
      v_upickupdel.T_USERTYPEDOCUMENT := :old.T_USERTYPEDOCUMENT;
      v_upickupdel.T_PRIORITY := :old.T_PRIORITY;
      v_upickupdel.T_MINPHASE := :old.T_MINPHASE;
      v_upickupdel.T_MAXPHASE := :old.T_MAXPHASE;
      v_upickupdel.T_SYSTEMDATE := :old.T_SYSTEMDATE;
      v_upickupdel.T_SYSTEMTIME := :old.T_SYSTEMTIME;
      v_upickupdel.T_CHECKSUM := :old.T_CHECKSUM;
      v_upickupdel.T_EXRATEACCTRNID := :old.T_EXRATEACCTRNID;
      v_upickupdel.T_CLAIMID := :old.T_CLAIMID;
      v_upickupdel.T_METHODID := :old.T_METHODID;
      v_upickupdel.T_MINIMIZATIONTURN := :old.T_MINIMIZATIONTURN;
      v_upickupdel.T_EXRATEACCPLUSDEBET := :old.T_EXRATEACCPLUSDEBET;
      v_upickupdel.T_EXRATEACCPLUSCREDIT := :old.T_EXRATEACCPLUSCREDIT;
      v_upickupdel.T_EXRATEACCMINUSDEBET := :old.T_EXRATEACCMINUSDEBET;
      v_upickupdel.T_EXRATEACCMINUSCREDIT := :old.T_EXRATEACCMINUSCREDIT;
      v_upickupdel.T_SKIPRECALCSUMNATCUR := :old.T_SKIPRECALCSUMNATCUR;
      v_upickupdel.T_FLAGRECALCSUM := :old.T_FLAGRECALCSUM;
      v_upickupdel.T_RATE := :old.T_RATE;
      v_upickupdel.T_SCALE := :old.T_SCALE;
      v_upickupdel.T_POINT := :old.T_POINT;
      v_upickupdel.T_ISINVERSE := :old.T_ISINVERSE;
      v_upickupdel.T_USERFIELD1 := :old.T_USERFIELD1;
      v_upickupdel.T_USERFIELD2 := :old.T_USERFIELD2;
      v_upickupdel.T_USERFIELD3 := :old.T_USERFIELD3;
      v_upickupdel.T_USERFIELD4 := :old.T_USERFIELD4;
      v_upickupdel.T_NU_STATUS := :old.T_NU_STATUS;
      v_upickupdel.T_NU_KIND := :old.T_NU_KIND;
      v_upickupdel.T_NU_STARTDATE := :old.T_NU_STARTDATE;
      v_upickupdel.T_NU_ENDDATE := :old.T_NU_ENDDATE;
      v_upickupdel.T_NU_ACKDATE := :old.T_NU_ACKDATE;
      v_upickupdel.T_FU_ACCTRNID := :old.T_FU_ACCTRNID;
      v_upickupdel.T_SIDETRANSACTION := :old.T_SIDETRANSACTION;
      v_upickupdel.T_EXRATEEXTRA := :old.T_EXRATEEXTRA;
      v_upickupdel.T_USERGROUPID := :old.T_USERGROUPID;
      v_upickupdel.T_OFRRECID := :old.T_OFRRECID;
      v_upickupdel.T_DELETESOURCE := V_DELETESOURCE;

      IF DELETING
      THEN
         v_upickupdel.T_STATE := 0;
      END IF;

      IF UPDATING AND :new.T_STATE = 4
      THEN
         v_upickupdel.T_STATE := 4;
      END IF;
      
      trgpckg_dacctrn_dbt.v_upickupdel(trgpckg_dacctrn_dbt.v_upickupdel.count) := v_upickupdel;
   END IF;
EXCEPTION
   WHEN OTHERS
   THEN
      DBMS_OUTPUT.PUT_LINE (SQLERRM);
END;
/