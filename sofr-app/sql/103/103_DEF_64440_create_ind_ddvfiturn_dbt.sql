-- Построение индекса
declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DDVFITURN_DBT_USR1';
  if cnt = 0
  then
    execute immediate 'create index DDVFITURN_DBT_USR1 on ddvfiturn_dbt (t_client,t_date)  tablespace indx';
  end if;
end;
/
