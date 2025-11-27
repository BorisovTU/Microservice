-- Создание индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDL_LIMITSECURITES_DBT_IDX3' ;
  if cnt =0 then
     execute immediate 'create index DDL_LIMITSECURITES_DBT_IDX3 on ddl_limitsecurites_dbt (t_market, t_security) tablespace INDX';
  end if;
end;
/
