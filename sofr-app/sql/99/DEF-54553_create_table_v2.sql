-- Таблица "DCNVPRIOR_DBT"

declare
    vcnt number;
begin
   select count(*) into vcnt from user_tables where upper(table_name) = 'DMASSDLCARRY_TMP';
   if vcnt =1 then
     execute immediate 'drop table DMASSDLCARRY_TMP cascade constraint';
   end if;
   EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE DMASSDLCARRY_TMP (
        T_TYPEDOC NUMBER(5)
      , T_AUTOKEY NUMBER(10)
      , T_UNIQUEKEY VARCHAR2(2000)
      , T_FMTBLOBDATA_XXXX BLOB
    ) ON COMMIT PRESERVE ROWS';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where upper(i.TABLE_NAME)='DMASSDLCARRY_TMP' and upper(i.INDEX_NAME)='DMASSDLCARRY_TMP_IDX0' ;
  if cnt =1 then
    execute immediate 'drop index DMASSDLCARRY_TMP_IDX0';
  end if;
  execute immediate 'CREATE INDEX DMASSDLCARRY_TMP_IDX0 ON DMASSDLCARRY_TMP (T_TYPEDOC)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where upper(i.TABLE_NAME)='DMASSDLCARRY_TMP' and upper(i.INDEX_NAME)='DMASSDLCARRY_TMP_IDX1' ;
  if cnt =1 then
    execute immediate 'drop index DMASSDLCARRY_TMP_IDX1';
  end if;
  execute immediate 'CREATE UNIQUE INDEX DMASSDLCARRY_TMP_IDX1 ON DMASSDLCARRY_TMP (T_AUTOKEY)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where upper(i.TABLE_NAME)='DMASSDLCARRY_TMP' and upper(i.INDEX_NAME)='DMASSDLCARRY_TMP_IDX2' ;
  if cnt =1 then
    execute immediate 'drop index DMASSDLCARRY_TMP_IDX2';
  end if;
  execute immediate 'CREATE INDEX DMASSDLCARRY_TMP_IDX2 ON DMASSDLCARRY_TMP (T_UniqueKey)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_sequences i where (i.SEQUENCE_NAME)='DMASSDLCARRY_TMP_SEQ';
  if cnt =0 then
     execute immediate '
         CREATE SEQUENCE DMASSDLCARRY_TMP_SEQ 
           START WITH 1
           MAXVALUE 999999999999999999999999999
           MINVALUE 1
           NOCYCLE
           NOCACHE
           NOORDER
     ';
  end if;
end;
/

declare
 cnt number;
begin
     execute immediate q'[
       CREATE OR REPLACE TRIGGER DMASSDLCARRY_TMP_T1_AINC
         BEFORE INSERT OR UPDATE OF T_AUTOKEY ON DMASSDLCARRY_TMP FOR EACH ROW
       DECLARE
         v_id INTEGER;
       BEGIN
         IF (:NEW.T_AUTOKEY = 0 OR :NEW.T_AUTOKEY IS NULL) THEN
           SELECT DMASSDLCARRY_TMP_SEQ.NEXTVAL INTO :NEW.T_AUTOKEY FROM DUAL;
         ELSE
           SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('DMASSDLCARRY_TMP_SEQ');
           IF :NEW.T_AUTOKEY >= v_id THEN
             RAISE DUP_VAL_ON_INDEX;
           END IF;
         END IF;
       END;
     ]';
end;
/
