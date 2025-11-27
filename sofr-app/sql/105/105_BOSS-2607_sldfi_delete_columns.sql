-- вырезание BOSS-2607 

begin

  begin 
    execute immediate 'alter table DSCDLFI_TMP drop (t_sumprecision, t_sumprecision_from)';
    it_log.log('Удалены столбцы DSCDLFI_TMP t_sumprecision, t_sumprecision_from');
    dbms_output.put_line('Удалены столбцы DSCDLFI_TMP t_sumprecision, t_sumprecision_from');
  exception 
    when others then 
      it_log.log('Ошибка удаления столбцов DSCDLFI_TMP t_sumprecision, t_sumprecision_from');
      dbms_output.put_line('Ошибка удаления столбцов DSCDLFI_TMP t_sumprecision, t_sumprecision_from');
  end;


  begin 
    execute immediate 'alter table DSCDLFI_DBT drop (t_sumprecision)';
    it_log.log('Удалены столбцы DSCDLFI_DBT t_sumprecision');
    dbms_output.put_line('Удалены столбцы DSCDLFI_DBT t_sumprecision');
  exception 
    when others then 
      it_log.log('Ошибка удаления столбцов DSCDLFI_DBT t_sumprecision');
      dbms_output.put_line('Ошибка удаления столбцов DSCDLFI_DBT t_sumprecision');
  end;

end;