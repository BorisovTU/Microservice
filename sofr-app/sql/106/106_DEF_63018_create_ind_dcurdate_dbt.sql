declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DCURDATE_DBT_IDX1' ;
  if cnt =0 then
     execute immediate 'create index DCURDATE_DBT_IDX1 on dcurdate_dbt (t_branch, t_isclosed) tablespace INDX';
  end if;
end;
/
