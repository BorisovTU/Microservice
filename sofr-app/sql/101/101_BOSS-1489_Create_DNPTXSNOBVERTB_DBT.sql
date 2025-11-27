DECLARE
   ex_table_not_exists EXCEPTION;
   PRAGMA EXCEPTION_INIT(ex_table_not_exists, -942);
BEGIN
   BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE DNPTXSNOBVERTB_DBT';
   EXCEPTION
      WHEN ex_table_not_exists
      THEN NULL;
   END;

   execute immediate 'CREATE TABLE DNPTXSNOBVERTB_DBT ( ' ||
                                                     '   t_ID NUMBER(10) ' ||
                                                     '  ,t_BatchID NUMBER(10) ' ||
                                                     '  ,t_ClientID NUMBER(10) ' ||
                                                     '  ,t_StartDate DATE ' ||
                                                     '  ,t_EndDate DATE ' ||
                                                     '  ,t_Operation_ID NUMBER(10) ' ||
                                                     '  ,t_Event_ID NUMBER(10) ' ||
                                                     '  ,t_Return_Canceled CHAR(1) ' ||
                                                     '  ,t_Record_Found CHAR(1) ' ||
                                                     '  ,t_Record_ID VARCHAR2(50) ' ||
                                                     '  ,t_Record_Requested CHAR(1) ' ||
                                                     '  ,t_Error VARCHAR2(50) ' ||
                                                     ')';
END;
/

COMMENT ON COLUMN DNPTXSNOBVERTB_DBT.T_ID IS 'Уникальный идентификатор потока'/
COMMENT ON COLUMN DNPTXSNOBVERTB_DBT.T_BATCHID IS 'ID операции технической сверки'/
COMMENT ON COLUMN DNPTXSNOBVERTB_DBT.T_CLIENTID IS 'ID клиента'/
COMMENT ON COLUMN DNPTXSNOBVERTB_DBT.T_STARTDATE IS 'Начало периода сверки'/
COMMENT ON COLUMN DNPTXSNOBVERTB_DBT.T_ENDDATE IS 'Окончание периода сверки'/
COMMENT ON COLUMN DNPTXSNOBVERTB_DBT.T_OPERATION_ID IS 'ID операции'/
COMMENT ON COLUMN DNPTXSNOBVERTB_DBT.T_EVENT_ID IS 'ID события'/
COMMENT ON COLUMN DNPTXSNOBVERTB_DBT.T_RETURN_CANCELED IS 'Признак возврата отмененных'/
COMMENT ON COLUMN DNPTXSNOBVERTB_DBT.T_RECORD_FOUND IS 'Признак что запись найдена'/
COMMENT ON COLUMN DNPTXSNOBVERTB_DBT.T_RECORD_ID IS 'ID записи Хранилища'/
COMMENT ON COLUMN DNPTXSNOBVERTB_DBT.T_RECORD_REQUESTED IS 'Признак запись запрашивалась СИ'/
COMMENT ON COLUMN DNPTXSNOBVERTB_DBT.T_ERROR IS 'Описание ошибки'/
COMMENT ON TABLE DNPTXSNOBVERTB_DBT IS 'Записи хранилища СНОБ для сверки'/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DNPTXSNOBVERTB_DBT_IDX0';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE UNIQUE INDEX DNPTXSNOBVERTB_DBT_IDX0 ON DNPTXSNOBVERTB_DBT (
   T_ID ASC
)
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DNPTXSNOBVERTB_DBT_IDX1';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE UNIQUE INDEX DNPTXSNOBVERTB_DBT_IDX1 ON DNPTXSNOBVERTB_DBT (
   T_BATCHID ASC,
   T_EVENT_ID ASC
)
/

DECLARE
   e_exist_seq EXCEPTION;
   PRAGMA EXCEPTION_INIT(  e_exist_seq,    -955 );
BEGIN
   EXECUTE IMMEDIATE 'CREATE SEQUENCE DNPTXSNOBVERTB_DBT_SEQ 
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


CREATE OR REPLACE TRIGGER DNPTXSNOBVERTB_DBT_T0_AINC
  BEFORE INSERT OR UPDATE OF T_ID ON DNPTXSNOBVERTB_DBT FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.T_ID = 0 OR :NEW.T_ID IS NULL) THEN
    SELECT DNPTXSNOBVERTB_DBT_SEQ.NEXTVAL INTO :NEW.T_ID FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('DNPTXSNOBVERTB_DBT_SEQ');
    IF :NEW.T_ID >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/
