-- Построение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'D712IIS_DBT_IDX1' ;
  if cnt =0 then
     execute immediate 'create index D712IIS_DBT_IDX1 on d712iis_dbt (SESSIONID, T_ACCOUNT) tablespace INDX';
  end if;
end;
/
