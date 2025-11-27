-- Построение индекса
declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME= 'U_CONV_ADR_DATA_DBT_IDX2' ;
  if cnt =0 then
     execute immediate 'create index u_conv_adr_data_dbt_idx2 on u_conv_adr_data_dbt (t_dealid)';
  end if;
end;
/
