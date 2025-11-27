-- Построение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'DDL_LEG_DBT_IDX4' ;
  if cnt =0 then
     execute immediate 'create index DDL_LEG_DBT_IDX4 on ddl_leg_dbt (t_pfi) tablespace INDX';
  end if;
end;
/
