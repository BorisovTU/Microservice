--Добавление поля
DECLARE
   e_exist_field EXCEPTION;

   PRAGMA EXCEPTION_INIT( e_exist_field, -1430);

BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE DLIMIT_MONEY_REVISE_DBT ADD T_SOFR_DUE NUMBER (32,12)';
   EXCEPTION WHEN e_exist_field THEN NULL;
END;
/

DECLARE
   e_exist_field EXCEPTION;

   PRAGMA EXCEPTION_INIT( e_exist_field, -1430);

BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE DLIMIT_MONEY_REVISE_DBT ADD T_CALCID NUMBER (32,12)';
   EXCEPTION WHEN e_exist_field THEN NULL;
END;
/

DECLARE
   e_exist_field EXCEPTION;

   PRAGMA EXCEPTION_INIT( e_exist_field, -1430);

BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE DLIMIT_SECUR_REVISE_DBT ADD T_CALCID NUMBER (32,12)';
   EXCEPTION WHEN e_exist_field THEN NULL;
END;
/

DECLARE
   e_exist_field EXCEPTION;

   PRAGMA EXCEPTION_INIT( e_exist_field, -1430);

BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE DLIMIT_MONEY_REVISE_DBT ADD T_LIMACCOUNTID NUMBER (20)';
   EXCEPTION WHEN e_exist_field THEN NULL;
END;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='DLIMIT_SECUR_REVISE_DBT' and i.INDEX_NAME='DLIMIT_SECUR_REVISE_DBT_IDX0' ;
  if cnt =1 then
    execute immediate 'drop index DLIMIT_SECUR_REVISE_DBT_IDX0';
  end if;
  execute immediate 'CREATE INDEX DLIMIT_SECUR_REVISE_DBT_IDX0 ON DLIMIT_SECUR_REVISE_DBT (T_SESSIONID,T_CALCID)';
end;
/


declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='DLIMIT_MONEY_REVISE_DBT' and i.INDEX_NAME='DLIMIT_MONEY_REVISE_DBT_IDX0' ;
  if cnt =1 then
    execute immediate 'drop index DLIMIT_MONEY_REVISE_DBT_IDX0';
  end if;
  execute immediate 'create index DLIMIT_MONEY_REVISE_DBT_IDX0 on DLIMIT_MONEY_REVISE_DBT (T_SESSIONID,T_CALCID)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='DLIMIT_SECUR_REVISE_DBT' and i.INDEX_NAME='DLIMIT_SECUR_REVISE_DBT_IDX1' ;
  if cnt =1 then
    execute immediate 'drop index DLIMIT_SECUR_REVISE_DBT_IDX1';
  end if;
  execute immediate 'CREATE INDEX DLIMIT_SECUR_REVISE_DBT_IDX1 ON DLIMIT_SECUR_REVISE_DBT (T_DATE)';
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='DLIMIT_MONEY_REVISE_DBT' and i.INDEX_NAME='DLIMIT_MONEY_REVISE_DBT_IDX1' ;
  if cnt =1 then
    execute immediate 'drop index DLIMIT_MONEY_REVISE_DBT_IDX1';
  end if;
  execute immediate 'CREATE INDEX DLIMIT_MONEY_REVISE_DBT_IDX1 ON DLIMIT_MONEY_REVISE_DBT (T_DATE)';
end;
/