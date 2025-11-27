declare
    vcnt number;
    vsql varchar2(2000);
begin

   select count(*) into vcnt from user_sequences  where sequence_name = upper('UREFILL_MANUAL_SEQ');
   if vcnt = 0 then
      execute immediate  'CREATE SEQUENCE UREFILL_MANUAL_SEQ
                             START WITH 1
                             MAXVALUE 9999999999999999999999999999
                             MINVALUE 1';
   end if;

   select count(*) into vcnt from user_tables where table_name = upper('UREFILL_MANUAL_DBT');
   if vcnt = 0 then
      execute immediate  'create table UREFILL_MANUAL_DBT (
                                       T_ID                 NUMBER(10),
                                       T_DATE_OPERATION     DATE,
                                       T_TIME_OPERATION     DATE,
                                       T_TIME_ACCEPT        DATE,
                                       T_TYPE_REFILL        NUMBER(10),
                                       T_TYPE_REFILL_NAME   VARCHAR2(40 char),
                                       T_SUM_OPERATION      NUMBER(32,2),
                                       T_RCODE              VARCHAR2(64),
                                       T_ACCOUNT          VARCHAR2(25),
                                       T_CONTRACT           NUMBER(10),
                                       T_CONTRACT_NUMBER    VARCHAR2(20 byte),
                                       T_STATUS             NUMBER(10),
                                       T_STATUS_NAME        VARCHAR2(15),
                                       T_OPER               NUMBER(10),
                                       T_OPER_NAME          VARCHAR2(70 byte)
                                     )';
      execute immediate 'comment on table UREFILL_MANUAL_DBT is ''Подкрепление расчетного кода клиента''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_DATE_OPERATION IS ''Дата подкрепления''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_TIME_OPERATION IS ''Время создания записи''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_TIME_ACCEPT IS ''Время подтверждения ''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_TYPE_REFILL IS ''Тип подкрепления:
      1 - собственные счета РСХБ в НКЦ
      2 - единый пул клиентов - обособленные 
      3 - единый пул клиентов - не обособленные
      4 - индивидуальный РК(ТКС)''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_SUM_OPERATION IS ''Сумма подкрепления''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_RCODE IS ''Расчетный код в НКЦ''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_ACCOUNT IS ''Счет плательщика''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_CONTRACT IS ''Номер субдоговора''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_STATUS IS ''Статус операции:
      0 - Отложенная
      1 - Открытая
      2 - Закрытая''';
      execute immediate 'COMMENT ON COLUMN UREFILL_MANUAL_DBT.T_OPER IS ''Операционист''';

      execute immediate 'CREATE UNIQUE INDEX UREFILL_MANUAL_IDX0 ON UREFILL_MANUAL_DBT (T_ID)';
      execute immediate 'CREATE INDEX UREFILL_MANUAL_IDX1 ON UREFILL_MANUAL_DBT (T_DATE_OPERATION)';
      execute immediate 'CREATE INDEX UREFILL_MANUAL_IDX2 ON UREFILL_MANUAL_DBT (T_STATUS)';
      execute immediate 'CREATE INDEX UREFILL_MANUAL_IDX3 ON UREFILL_MANUAL_DBT (T_CONTRACT)';
      
   end if;
   
   execute immediate 'CREATE OR REPLACE TRIGGER "UREFILL_MANUAL_T0_AINC" 
                      BEFORE INSERT OR UPDATE OF T_ID ON UREFILL_MANUAL_DBT FOR EACH ROW
                     DECLARE
                      v_id INTEGER;
                     BEGIN
                      IF (:new.T_ID = 0 OR :new.T_ID IS NULL) THEN
                        SELECT UREFILL_MANUAL_SEQ.nextval INTO :new.T_ID FROM dual;
                      ELSE
                        select last_number into v_id from user_sequences where sequence_name = upper (''UREFILL_MANUAL_SEQ'');
                        IF :new.T_ID >= v_id THEN
                           RAISE DUP_VAL_ON_INDEX;
                        END IF;
                      END IF;
                     END;';
                     
   select count(*) into vcnt from user_sequences  where sequence_name = upper('UREFILL_MANUAL_STEP_SEQ');
   if vcnt = 0 then
      execute immediate  'CREATE SEQUENCE UREFILL_MANUAL_STEP_SEQ
                             START WITH 1
                             MAXVALUE 9999999999999999999999999999
                             MINVALUE 1';
   end if;

   select count(*) into vcnt from user_tables where table_name = upper('UREFILL_MANUAL_STEP_DBT');
   if vcnt = 0 then
      execute immediate  'create table UREFILL_MANUAL_STEP_DBT (
                                       T_STEPID             NUMBER(10),
                                       T_REFILLID           NUMBER(10),
                                       T_DATE_OPERATION     DATE,
                                       T_SYSTEMDATE         DATE,
                                       T_ACTION             NUMBER(5),
                                       T_COMMENT            VARCHAR2(200 BYTE),
                                       T_OPER               NUMBER(10)
                                     )';
      execute immediate 'comment on table UREFILL_MANUAL_STEP_DBT is ''Шаги операции подкрепления расчетного кода клиента''';
      execute immediate 'CREATE UNIQUE INDEX UREFILL_MANUAL_STEP_IDX0 ON UREFILL_MANUAL_STEP_DBT (T_STEPID)';
      execute immediate 'CREATE INDEX UREFILL_MANUAL_STEP_IDX1 ON UREFILL_MANUAL_STEP_DBT (T_REFILLID)';
      execute immediate 'CREATE INDEX UREFILL_MANUAL_STEP_IDX2 ON UREFILL_MANUAL_STEP_DBT (T_DATE_OPERATION)';
   end if;

   execute immediate '  CREATE OR REPLACE TRIGGER "UREFILL_MANUAL_STEP_DBT_T0_AINC" 
                           BEFORE INSERT OR UPDATE OF T_STEPID ON UREFILL_MANUAL_STEP_DBT FOR EACH ROW
                           DECLARE
                            v_id INTEGER;
                           BEGIN
                            IF (:new.T_STEPID = 0 OR :new.T_STEPID IS NULL) THEN
                            SELECT UREFILL_MANUAL_STEP_SEQ.nextval INTO :new.T_STEPID FROM dual;
                            ELSE
                              select last_number into v_id from user_sequences where sequence_name = upper (''UREFILL_MANUAL_STEP_SEQ'');
                              IF :new.T_STEPID >= v_id THEN
                                 RAISE DUP_VAL_ON_INDEX;
                              END IF;
                            END IF;
                           END;';
end;
