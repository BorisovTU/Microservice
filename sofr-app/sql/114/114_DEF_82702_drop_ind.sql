declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.TABLE_NAME='DNPTXOBJ_DBT' and i.INDEX_NAME= 'DDL_REGIABUF_DBT_IDX0' ;
  if cnt =1 then
    execute immediate 'drop index ddl_regiabuf_dbt_idx0';
  end if;
end;
/
