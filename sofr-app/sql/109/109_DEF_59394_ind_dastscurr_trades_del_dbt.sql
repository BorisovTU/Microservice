declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DASTSCURR_TRADES_DEL_DBT_IDX1' ;
  if cnt =0 then
     execute immediate 'create index DASTSCURR_TRADES_DEL_DBT_IDX1 on DASTSCURR_TRADES_DEL_DBT (t_tradedate) tablespace INDX';
  end if;
end;
/
