-- Построение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'DDL_LIMITCASHSTOCKARCH_DBT_IDX3' ;
  if cnt =0 then
     execute immediate 'create index DDL_LIMITCASHSTOCKARCH_DBT_IDX3 on DDL_LIMITCASHSTOCKARCH_DBT (t_date) tablespace INDX ';
  end if;
end;
/
