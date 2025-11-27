-- Перестроение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DPMAUTOAC_DBT_IDX0' ;
  if cnt > 0 then
     execute immediate 'drop index DPMAUTOAC_DBT_IDX0';
  end if;
  execute immediate 'create unique index DPMAUTOAC_DBT_IDX0 on dpmautoac_dbt (t_settaccid,t_partyid,t_account,t_fikind,t_fiid,t_servicekind,t_kindoper,t_purpose,t_order,t_reserve) tablespace INDX';

  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DPMAUTOAC_DBT_IDX4' ;
  if cnt > 0 then
     execute immediate 'drop index DPMAUTOAC_DBT_IDX4';
  end if;
  execute immediate 'create unique index DPMAUTOAC_DBT_IDX4 on DPMAUTOAC_DBT (T_SERVICEKIND, T_PURPOSE, T_FIKIND, T_FIID, T_PARTYID, T_ACCOUNT, T_ORDER) tablespace INDX';
end;
