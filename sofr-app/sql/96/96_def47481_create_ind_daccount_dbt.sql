-- Построение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'DACCOUNT_DBT_IDX10' ;
  if cnt =0 then
     execute immediate 'create index DACCOUNT_DBT_IDX10 on daccount_dbt (t_sort) tablespace INDX';
  end if;
end;
/
