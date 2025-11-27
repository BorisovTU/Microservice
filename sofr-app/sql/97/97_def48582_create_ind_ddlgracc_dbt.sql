-- Построение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'DDLGRACC_DBT_IDX4' ;
  if cnt =0 then
     execute immediate 'create index DDLGRACC_DBT_IDX4 on DDLGRACC_DBT (t_accnum, t_state) tablespace INDX ';
  end if;
end;
/
