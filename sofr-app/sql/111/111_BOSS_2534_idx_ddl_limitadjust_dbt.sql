declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDL_LIMITADJUST_DBT_IDX3' ;
  if cnt =0 then
     execute immediate 'create index DDL_LIMITADJUST_DBT_IDX3 on ddl_limitadjust_dbt (t_date) tablespace INDX';
  end if;
end;
/
