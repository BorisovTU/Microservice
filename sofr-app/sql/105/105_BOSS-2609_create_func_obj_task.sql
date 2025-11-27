declare
  l_func_id number(10) := 11500;
begin
  insert into dfunc_dbt (t_funcid,
                         t_code,
                         t_name,
                         t_type,
                         t_filename,
                         t_functionname,
                         t_interval,
                         t_version)
  values (l_func_id,
          'load_sp_deals_844',
          'Создание отложенных сделок в БОЦБ по 844 указу',
          1,
          '844_create_deals.mac',
          'CreateDeal',
          0,
          0);

  commit;
exception
  when dup_val_on_index then
    null;
end;
/