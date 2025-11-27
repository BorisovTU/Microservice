declare
    vcnt number;
    vsql varchar2(2000);
begin

   select count(*) into vcnt from user_sequences  where sequence_name = upper('UNPTXOP_PAYMENT_LINK_STEP_SEQ');
   if vcnt = 0 then
      execute immediate  'CREATE SEQUENCE UNPTXOP_PAYMENT_LINK_STEP_SEQ
                             START WITH 1
                             MAXVALUE 9999999999999999999999999999
                             MINVALUE 1';
   end if;

   select count(*) into vcnt from user_tables where table_name = upper('UNPTXOP_PAYMENT_LINK_STEP_DBT');
   if vcnt = 0 then
      execute immediate  'create table UNPTXOP_PAYMENT_LINK_STEP_DBT (
                                       T_STEPID             NUMBER(10),
                                       T_NPTXOPID           NUMBER(10),
                                       T_SYSTEMDATE         DATE,
                                       T_ACTION             NUMBER(5),
                                       T_COMMENT            VARCHAR2(200 BYTE),
                                       T_OPER               NUMBER(10)
                                     )';
      execute immediate 'comment on table UNPTXOP_PAYMENT_LINK_STEP_DBT is ''Шаги обработки операции подкрепления''';
      execute immediate 'CREATE UNIQUE INDEX UNPTXOP_PAYMENT_LINK_STEP_IDX0 ON UNPTXOP_PAYMENT_LINK_STEP_DBT (T_STEPID)';
      execute immediate 'CREATE INDEX UNPTXOP_PAYMENT_LINK_STEP_DBT_IDX1 ON UNPTXOP_PAYMENT_LINK_STEP_DBT (T_NPTXOPID)';
      execute immediate 'CREATE INDEX UNPTXOP_PAYMENT_LINK_STEP_DBT_IDX2 ON UNPTXOP_PAYMENT_LINK_STEP_DBT (T_SYSTEMDATE)';
   end if;

   execute immediate '  CREATE OR REPLACE TRIGGER "UNPTXOP_PAYMENT_LINK_STEP_DBT_T0_AINC" 
                           BEFORE INSERT OR UPDATE OF T_STEPID ON UNPTXOP_PAYMENT_LINK_STEP_DBT FOR EACH ROW
                           DECLARE
                            v_id INTEGER;
                           BEGIN
                            IF (:new.T_STEPID = 0 OR :new.T_STEPID IS NULL) THEN
                            SELECT UNPTXOP_PAYMENT_LINK_STEP_SEQ.nextval INTO :new.T_STEPID FROM dual;
                            ELSE
                              select last_number into v_id from user_sequences where sequence_name = upper (''UNPTXOP_PAYMENT_LINK_STEP_SEQ'');
                              IF :new.T_STEPID >= v_id THEN
                                 RAISE DUP_VAL_ON_INDEX;
                              END IF;
                            END IF;
                           END;';
end;
