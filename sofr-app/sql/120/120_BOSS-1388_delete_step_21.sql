DECLARE
  begin
    delete from doprostep_dbt t where t.t_blockid = 40269072 
      and  t.t_number_step = 21 and t.t_kind_action = 1;
  exception 
    when others then 
      it_log.log('Ошибка обновления шага 21');
  end;