declare
    vcnt number;
    vsql varchar2(2000);
begin

   select count(*) into vcnt from user_sequences  where sequence_name = upper('UQIACCREDITATION_STEP_SEQ');
   if vcnt = 0 then
      execute immediate  'CREATE SEQUENCE UQIACCREDITATION_STEP_SEQ
                             START WITH 1
                             MAXVALUE 9999999999999999999999999999
                             MINVALUE 1';
   end if;
   
   select count(*) into vcnt from user_tables where table_name = upper('UQIACCREDITATION_STEP_DBT');
   if vcnt = 0 then
      execute immediate  'CREATE TABLE UQIACCREDITATION_STEP_DBT
                           (
                             T_STEPID      NUMBER(10),
                             T_ACCREDID    NUMBER(10),
                             T_TIMESTAMP   DATE,
                             T_STATUS      NUMBER(5),
                             T_COMMENT     VARCHAR2(200 BYTE)
                           )';
      execute immediate 'comment on table UQIACCREDITATION_STEP_DBT is ''Шаги событий по квал.инвесторам''';
      execute immediate 'COMMENT ON COLUMN UQIACCREDITATION_STEP_DBT.T_STEPID IS ''Шаг''';
      execute immediate 'COMMENT ON COLUMN UQIACCREDITATION_STEP_DBT.T_ACCREDID IS ''ID события''';
      execute immediate 'COMMENT ON COLUMN UQIACCREDITATION_STEP_DBT.T_TIMESTAMP IS ''Дата шага''';
      execute immediate 'COMMENT ON COLUMN UQIACCREDITATION_STEP_DBT.T_STATUS IS ''Статус''';

      execute immediate 'CREATE INDEX UQIACCREDITATION_STEP_DBT_IDX0 ON UQIACCREDITATION_STEP_DBT (T_ACCREDID)';
      execute immediate 'CREATE INDEX UQIACCREDITATION_STEP_DBT_IDX1 ON UQIACCREDITATION_STEP_DBT(T_TIMESTAMP)';
   end if;
   
   execute immediate 'CREATE OR REPLACE TRIGGER "UQIACCREDITATION_STEP_T0_AINC" 
                         BEFORE INSERT OR UPDATE OF T_STEPID ON UQIACCREDITATION_STEP_DBT FOR EACH ROW
                        DECLARE
                         v_id INTEGER;
                        BEGIN
                         IF (:new.T_STEPID = 0 OR :new.T_STEPID IS NULL) THEN
                           SELECT UQIACCREDITATION_STEP_SEQ.nextval INTO :new.T_STEPID FROM dual;
                         ELSE
                           select last_number into v_id from user_sequences where sequence_name = upper (''UQIACCREDITATION_STEP_SEQ'');
                           IF :new.T_STEPID >= v_id THEN
                              RAISE DUP_VAL_ON_INDEX;
                           END IF;
                         END IF;
                        END;';
   
end;