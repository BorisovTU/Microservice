CREATE GLOBAL TEMPORARY TABLE DREPORT_PARIED_ACC_CONTROL_TMP
(
  t_id                     NUMBER(10),
  t_report_start_date      DATE,
  t_report_start_time      TIMESTAMP,
  t_executor_name          VARCHAR2(100),
  t_report_generation_date DATE,
  t_module_name            VARCHAR2(20),
  t_line_number            VARCHAR2(50),
  t_account_number         VARCHAR2(50),
  t_balance                NUMBER(20,2),
  t_currency_code          VARCHAR2(3),
  t_error_reason           VARCHAR2(50),
  t_error_count            NUMBER(10)
) ON COMMIT PRESERVE ROWS
/

CREATE UNIQUE INDEX DREPORT_PARIED_ACC_CONTROL_TMP_IDX0 ON DREPORT_PARIED_ACC_CONTROL_TMP(t_id)
/

CREATE INDEX DREPORT_PARIED_ACC_CONTROL_TMP_IDX1 ON DREPORT_PARIED_ACC_CONTROL_TMP(t_report_start_date)
/

DECLARE
   vcnt   NUMBER;
BEGIN
   SELECT COUNT (*)
     INTO vcnt
     FROM user_sequences
    WHERE UPPER (sequence_name) = 'DREPORT_PARIED_ACC_CONTROL_TMP_SEQ';

   IF vcnt = 0
   THEN
      EXECUTE IMMEDIATE 'CREATE SEQUENCE DREPORT_PARIED_ACC_CONTROL_TMP_SEQ
                                START WITH 1
                                MAXVALUE 9999999999999999999999999999
                                MINVALUE 1
                                NOCYCLE
                                NOCACHE
                                NOORDER';
   END IF;
END;
/

CREATE OR REPLACE TRIGGER DREPORT_PARIED_ACC_CONTROL_TMP_T0_AINC
  BEFORE INSERT OR UPDATE OF T_ID ON DREPORT_PARIED_ACC_CONTROL_TMP FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.T_ID = 0 OR :NEW.T_ID IS NULL) THEN
    SELECT DREPORT_PARIED_ACC_CONTROL_TMP_SEQ.NEXTVAL INTO :NEW.T_ID FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('DREPORT_PARIED_ACC_CONTROL_TMP_SEQ');
    IF :NEW.T_ID >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/