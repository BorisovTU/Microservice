-- Перестроение индексов
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DNPTXLOT_DBT_IDX5' ;
  if cnt =1 then
     execute immediate 'drop index DNPTXLOT_DBT_IDX5';
  end if;
  execute immediate 'create index DNPTXLOT_DBT_IDX5 on dnptxlot_dbt (t_buydate, t_client) tablespace INDX';
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'DNPTXLOT_DBT_IDX6' ;
  if cnt =1 then
     execute immediate 'drop index DNPTXLOT_DBT_IDX6';
  end if;
  execute immediate 'create index DNPTXLOT_DBT_IDX6 on DNPTXLOT_DBT (t_saledate, t_client) tablespace INDX';
end;
/