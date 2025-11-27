declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDL_LIMITCASHSTOCK_DBT_IDX4' ;
  if cnt =0 then
     execute immediate 'create index DDL_LIMITCASHSTOCK_DBT_IDX4 on DDL_LIMITCASHSTOCK_DBT (t_market_kind, t_market) tablespace INDX';
  end if;
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDL_LIMITSECURITES_DBT_IDX3' ;
  if cnt > 0 then
     execute immediate 'drop index DDL_LIMITSECURITES_DBT_IDX3';
  end if;
  execute immediate 'create index DDL_LIMITSECURITES_DBT_IDX3 on DDL_LIMITSECURITES_DBT (t_market, t_security, t_limit_kind, t_client_code, t_trdaccid) tablespace INDX';
end;
/
