-- ‚§οβ® ¨§ DEF-54553

declare
    vcnt number;
    logID VARCHAR2(50) := 'DEF-59339 recreate DMASSDLCARRY_TMP';
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
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
end;
/

declare
 cnt number;
 logID VARCHAR2(50) := 'DEF-59339 recreate DMASSDLCARRY_TMP_IDX0';
begin
  select count(*) into cnt from user_indexes i where upper(i.TABLE_NAME)='DMASSDLCARRY_TMP' and upper(i.INDEX_NAME)='DMASSDLCARRY_TMP_IDX0' ;
  if cnt =1 then
    execute immediate 'drop index DMASSDLCARRY_TMP_IDX0';
  end if;
  execute immediate 'CREATE INDEX DMASSDLCARRY_TMP_IDX0 ON DMASSDLCARRY_TMP (T_TYPEDOC)';
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
end;
/

declare
 cnt number;
 logID VARCHAR2(50) := 'DEF-59339 recreate DMASSDLCARRY_TMP_IDX1';
begin
  select count(*) into cnt from user_indexes i where upper(i.TABLE_NAME)='DMASSDLCARRY_TMP' and upper(i.INDEX_NAME)='DMASSDLCARRY_TMP_IDX1' ;
  if cnt =1 then
    execute immediate 'drop index DMASSDLCARRY_TMP_IDX1';
  end if;
  execute immediate 'CREATE UNIQUE INDEX DMASSDLCARRY_TMP_IDX1 ON DMASSDLCARRY_TMP (T_AUTOKEY)';
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
end;
/

declare
 cnt number;
 logID VARCHAR2(50) := 'DEF-59339 recreate DMASSDLCARRY_TMP_IDX2';
begin
  select count(*) into cnt from user_indexes i where upper(i.TABLE_NAME)='DMASSDLCARRY_TMP' and upper(i.INDEX_NAME)='DMASSDLCARRY_TMP_IDX2' ;
  if cnt =1 then
    execute immediate 'drop index DMASSDLCARRY_TMP_IDX2';
  end if;
  execute immediate 'CREATE INDEX DMASSDLCARRY_TMP_IDX2 ON DMASSDLCARRY_TMP (T_UniqueKey)';
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
end;
/

declare
 cnt number;
 logID VARCHAR2(50) := 'DEF-59339 recreate DMASSDLCARRY_TMP_SEQ';
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
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
end;
/

declare
 cnt number;
 logID VARCHAR2(50) := 'DEF-59339 recreate DMASSDLCARRY_TMP_T1_AINC';
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
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
end;
/
