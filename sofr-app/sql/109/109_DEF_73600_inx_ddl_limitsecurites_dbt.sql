-- Перестроение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'UDDL_LIMITSECURITES_DBT_IDX2' ;
  if cnt =1 then
    execute immediate 'drop index UDDL_LIMITSECURITES_DBT_IDX2';
  end if;
  execute immediate 'create index UDDL_LIMITSECURITES_DBT_IDX2 on ddl_limitsecurites_dbt (t_client_code, t_seccode)  tablespace indx';
end;
/
