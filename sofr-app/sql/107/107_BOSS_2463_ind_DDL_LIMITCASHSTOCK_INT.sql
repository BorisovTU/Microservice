--Построение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DDL_LIMITCASHSTOCK_INT_IDX2' ;
  if cnt =0 then
     execute immediate 'create index DDL_LIMITCASHSTOCK_INT_IDX2 on DDL_LIMITCASHSTOCK_INT (t_market_kind, t_client, t_internalaccount, t_currid)  local';
  end if;
end;
/
