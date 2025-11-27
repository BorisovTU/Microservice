-- Таблица "DCNVPRIOR_DBT"

declare
    vcnt number;
begin
   select count(*) into vcnt from user_tables where upper(table_name) = 'DCNVPRIOR_DBT';
   if vcnt = 0 then
         EXECUTE IMMEDIATE 'CREATE TABLE DCNVPRIOR_DBT (
         T_CONVTASKID   NUMBER(10)
       , T_OBJECTID     NUMBER(20)
       , T_EXECSECONDS  NUMBER(20)
       , T_CALCEDPRIOR  NUMBER(5)
     )';
   end if;
end;
/

declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='DCNVPRIOR_DBT' and i.INDEX_NAME='DCNVPRIOR_DBT_IDX0' ;
  if cnt =1 then
    execute immediate 'drop index DCNVPRIOR_DBT_IDX0';
  end if;
  execute immediate 'CREATE UNIQUE INDEX DCNVPRIOR_DBT_IDX0 ON DCNVPRIOR_DBT (T_CONVTASKID, T_OBJECTID)';
end;
/
