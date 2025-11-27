-- вырезание BOSS-2607 

begin 
  execute immediate 'alter table ddl_comm_dbt drop (t_CorpActionDate, t_ConsiderFirstBuyDate)';
  it_log.log('Удалены столбцы ddl_comm_dbt t_CorpActionDate, t_ConsiderFirstBuyDate');
  dbms_output.put_line('Удалены столбцы ddl_comm_dbt t_CorpActionDate, t_ConsiderFirstBuyDate');
exception 
  when others then 
    it_log.log('Ошибка удаления столбцов ddl_comm_dbt t_CorpActionDate, t_ConsiderFirstBuyDate');
    dbms_output.put_line('Ошибка удаления столбцов ddl_comm_dbt t_CorpActionDate, t_ConsiderFirstBuyDate');
end;
