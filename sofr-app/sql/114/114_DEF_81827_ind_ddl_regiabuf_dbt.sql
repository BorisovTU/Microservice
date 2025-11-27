declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDL_REGIABUF_DBT_IDX0' ;
  if cnt =0 then
     execute immediate 'create index DDL_REGIABUF_DBT_IDX0 on DDL_REGIABUF_DBT (t_autoinc)  local';
  end if;
end;
/
