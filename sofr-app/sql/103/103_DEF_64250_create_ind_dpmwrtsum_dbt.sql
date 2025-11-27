-- Пострение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DPMWRTSUM_DBT_USR1' ;
  if cnt =0 then
     execute immediate 'create index DPMWRTSUM_DBT_USR1 on dpmwrtsum_dbt ( t_contract, t_state ) tablespace INDX';
  end if;
end;
/
