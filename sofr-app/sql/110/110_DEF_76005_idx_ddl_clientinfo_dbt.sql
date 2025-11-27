-- Перестроение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDL_CLIENTINFO_DBT_IDX3' ;
  if cnt =1 then
    execute immediate 'drop index DDL_CLIENTINFO_DBT_IDX3';
  end if;
  execute immediate 'create index DDL_CLIENTINFO_DBT_IDX3 on DDL_CLIENTINFO_DBT (t_partyid) local ';
end;
/
