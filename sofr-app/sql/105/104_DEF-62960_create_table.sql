-- Таблица "getbroker_tmp

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE getbroker_tmp CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

DECLARE
    E_OBJECT_EXISTS EXCEPTION;
    PRAGMA EXCEPTION_INIT( E_OBJECT_EXISTS, -955);
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE getbroker_tmp (
    t_id NUMBER(10)    NOT NULL
   , t_encoding         VARCHAR2(50)       
  , t_message    clob
)';
EXCEPTION
    WHEN E_OBJECT_EXISTS THEN NULL;
END;
/

CREATE UNIQUE INDEX getbroker_tmp_IDX0 ON getbroker_tmp
(T_ID)
LOGGING
TABLESPACE INDX
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
           )
NOPARALLEL
/

BEGIN
  EXECUTE IMMEDIATE 'DROP SEQUENCE getbroker_tmp_SEQ';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

DECLARE
   e_exist_seq EXCEPTION;
   PRAGMA EXCEPTION_INIT(  e_exist_seq,    -955 );
BEGIN
   EXECUTE IMMEDIATE 'CREATE SEQUENCE getbroker_tmp_SEQ 
  START WITH 1
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  NOCACHE
  NOORDER';
EXCEPTION
   WHEN e_exist_seq THEN NULL;
END;
/

CREATE OR REPLACE TRIGGER "getbroker_tmpT_T0_AINC" 
 BEFORE INSERT OR UPDATE OF t_id ON getbroker_tmp FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.t_id = 0 OR :new.t_id IS NULL) THEN
 SELECT dxr_log_dbt_seq.nextval INTO :new.t_id FROM dual;
 ELSE
 select last_number into v_id from user_sequences where sequence_name = upper ('getbroker_tmp_SEQ');
 IF :new.t_id >= v_id THEN
 RAISE DUP_VAL_ON_INDEX;
 END IF;
 END IF;
END;
/
