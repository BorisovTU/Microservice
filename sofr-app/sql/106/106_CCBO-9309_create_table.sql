-- BIQ-16474 Создание таблицы KONDOR_SOFR_BUFFER_DBT
DECLARE
  cnt NUMBER;
  newTableName VARCHAR2(100) := 'KONDOR_SOFR_BUFFER_DBT';
BEGIN
  SELECT COUNT(1)
    INTO cnt
    FROM user_tables 
   WHERE UPPER(table_name) = UPPER(newTableName);
  
  IF cnt = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE KONDOR_SOFR_BUFFER_DBT
        (
          t_recID        NUMBER(10) NOT NULL,
          t_requestID    NUMBER(10) NOT NULL,
          t_dealCode     VARCHAR2(30) NOT NULL,
          t_seqID        NUMBER(10) NOT NULL,
          t_seqType      VARCHAR2(10) NOT NULL,
          t_errorStatus  NUMBER(5) NOT NULL,
          t_errorMessage VARCHAR2(200),
          t_dateCreate   DATE NOT NULL,
          t_dateComplete DATE
        )';
        
    EXECUTE IMMEDIATE 'COMMENT ON TABLE KONDOR_SOFR_BUFFER_DBT IS ''Буферная таблица идентификаторов таблиц сделок СОФР для загружаемых сделок Кондор''';

    EXECUTE IMMEDIATE 'COMMENT ON COLUMN KONDOR_SOFR_BUFFER_DBT.t_recID IS ''Идентификатор записи''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN KONDOR_SOFR_BUFFER_DBT.t_requestID IS ''Идентификатор запроса(поле ReqID XML)''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN KONDOR_SOFR_BUFFER_DBT.t_dealCode IS ''Код сделки''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN KONDOR_SOFR_BUFFER_DBT.t_seqID IS ''Идентификатор сделки в таблице СОФР (ddvndeal_dbt, ddl_tick_dbt)''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN KONDOR_SOFR_BUFFER_DBT.t_seqType IS ''Тип используемого sequence''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN KONDOR_SOFR_BUFFER_DBT.t_errorStatus IS ''Статус обработки''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN KONDOR_SOFR_BUFFER_DBT.t_errorMessage IS ''Текст ошибки''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN KONDOR_SOFR_BUFFER_DBT.t_dateCreate IS ''Дата/время создания строки''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN KONDOR_SOFR_BUFFER_DBT.t_dateComplete IS ''Дата/время обработки строки''';

    EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX KONDOR_SOFR_BUFFER_DBT_IDX0 ON KONDOR_SOFR_BUFFER_DBT (t_recID) tablespace INDX';
    EXECUTE IMMEDIATE 'CREATE INDEX KONDOR_SOFR_BUFFER_DBT_IDX1 ON KONDOR_SOFR_BUFFER_DBT (t_dealCode, t_seqType) tablespace INDX';
    
    EXECUTE IMMEDIATE 'CREATE SEQUENCE KONDOR_SOFR_BUFFER_DBT_SEQ 
                         START WITH 1
                         MAXVALUE 999999999999999999999999999
                         MINVALUE 1
                         NOCYCLE
                         NOCACHE
                         NOORDER';

    
 END IF;
END;
/

CREATE OR REPLACE TRIGGER KONDOR_SOFR_BUFFER_DBT_T0_AINC
  BEFORE INSERT OR UPDATE OF t_recID ON KONDOR_SOFR_BUFFER_DBT FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.t_recID = 0 OR :NEW.t_recID IS NULL) THEN
    SELECT KONDOR_SOFR_BUFFER_DBT_SEQ.NEXTVAL INTO :NEW.t_recID FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('KONDOR_SOFR_BUFFER_DBT_SEQ');
    IF :NEW.t_recID >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/
