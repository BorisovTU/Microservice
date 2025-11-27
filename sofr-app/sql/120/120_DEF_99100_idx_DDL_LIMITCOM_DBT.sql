declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='DDL_LIMITCOM_DBT' and i.INDEX_NAME= 'DDL_LIMITCOM_DBT_IDX1' ;
  if cnt =1 then
    execute immediate 'drop index DDL_LIMITCOM_DBT_IDX1';
  end if;
  execute immediate 'create index DDL_LIMITCOM_DBT_IDX1 on DDL_LIMITCOM_DBT (T_SFCONTRID, T_PLANDATE)  local';
end;

