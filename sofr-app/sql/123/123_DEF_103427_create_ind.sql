declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDLGRACC_DBT_IDX4' ;
  if cnt =1 then
    execute immediate 'drop index DDLGRACC_DBT_IDX4';
  end if;
  execute immediate 'create index DDLGRACC_DBT_IDX4 on DDLGRACC_DBT (T_STATE, T_ACCNUM, T_FACTDATE)  tablespace indx';
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDLRQ_DBT_IDX7' ;
  if cnt =1 then
    execute immediate 'drop index DDLRQ_DBT_IDX7';
  end if;
  execute immediate 'create index DDLRQ_DBT_IDX7 on DDLRQ_DBT (t_fiid, t_state) tablespace indx';
end;
/
