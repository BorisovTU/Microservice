declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DPMWRTSUM_DBT_IDX6' ;
  if cnt =1 then
    execute immediate 'drop index DPMWRTSUM_DBT_IDX6';
  end if;
  execute immediate 'create index DPMWRTSUM_DBT_IDX6 on dpmwrtsum_dbt (t_dockind, t_party, t_contract, t_amount)  tablespace indx';
end;
/
