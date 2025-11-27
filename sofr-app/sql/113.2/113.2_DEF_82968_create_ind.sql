declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DACCOUNT_DBT_IDXA' ;
  if cnt =1 then
    execute immediate 'drop index DACCOUNT_DBT_IDXA';
  end if;
  execute immediate 'create index DACCOUNT_DBT_IDXA on daccount_dbt (SUBSTR(T_ACCOUNT,1,8)||SUBSTR(T_ACCOUNT,10))  tablespace indx';
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DACCOUNT_DBT_IDXB' ;
  if cnt =1 then
    execute immediate 'drop index DACCOUNT_DBT_IDXB';
  end if;
  execute immediate 'create index DACCOUNT_DBT_IDXB on daccount_dbt (SUBSTR(T_ACCOUNT,1,5)||SUBSTR(T_ACCOUNT,10))  tablespace indx';
end;
/
