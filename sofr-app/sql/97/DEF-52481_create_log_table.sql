-- Таблица "DNPTXSNOBREP_TMP"

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE DDL_REGIA_LOG_DBT CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

DECLARE
    E_OBJECT_EXISTS EXCEPTION;
    PRAGMA EXCEPTION_INIT( E_OBJECT_EXISTS, -955);
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE DDL_REGIA_LOG_DBT (
    T_SESSIONID NUMBER(20)
  , T_CALCID NUMBER (20)
  , T_LOGMESSAGE VARCHAR2(500)
  , T_PROGRESSMESSAGE VARCHAR2(500)
  , T_ACTIONID NUMBER(10)
  , T_TIMESTAMP TIMESTAMP(6)
)';
EXCEPTION
    WHEN E_OBJECT_EXISTS THEN NULL;
END;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='DDL_REGIA_LOG_DBT' and i.INDEX_NAME='DDL_REGIA_LOG_DBT_IDX0' ;
  if cnt =1 then
    execute immediate 'drop index DDL_REGIA_LOG_DBT_IDX0';
  end if;
  execute immediate 'CREATE INDEX DDL_REGIA_LOG_DBT_IDX0 ON DDL_REGIA_LOG_DBT (T_SESSIONID)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='DDL_REGIA_LOG_DBT' and i.INDEX_NAME='DDL_REGIA_LOG_DBT_IDX1' ;
  if cnt =1 then
    execute immediate 'drop index DDL_REGIA_LOG_DBT_IDX1';
  end if;
  execute immediate 'CREATE INDEX DDL_REGIA_LOG_DBT_IDX1 ON DDL_REGIA_LOG_DBT (T_CALCID)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='DDL_REGIA_LOG_DBT' and i.INDEX_NAME='DDL_REGIA_LOG_DBT_IDX2' ;
  if cnt =1 then
    execute immediate 'drop index DDL_REGIA_LOG_DBT_IDX2';
  end if;
  execute immediate 'CREATE INDEX DDL_REGIA_LOG_DBT_IDX2 ON DDL_REGIA_LOG_DBT (T_TIMESTAMP)';
end;
/

DECLARE
   vcnt   NUMBER;
BEGIN
   SELECT COUNT (*)
     INTO vcnt
     FROM user_sequences
    WHERE UPPER (sequence_name) = 'DDL_REGIA_LOG_SEQ';

   IF vcnt = 0
   THEN
      EXECUTE IMMEDIATE 'CREATE SEQUENCE DDL_REGIA_LOG_SEQ
                                START WITH 1
                                MAXVALUE 9999999999999999999999999999
                                MINVALUE 1
                                NOCYCLE
                                NOCACHE
                                NOORDER';
   END IF;
END;
/