declare
  g_service_name varchar2(100);
  g_service_id number(9);
  
  function get_service_id (p_name varchar2)
    return number is
    l_service_id number(9);
  begin
    select nvl(max(case when s.t_name = p_name then s.t_id end), max(s.t_id) + 1)
      into l_service_id
      from dsimpleservice_dbt s;

    return l_service_id;
  end get_service_id;
  
  procedure delete_service (p_service_id number) is
  begin
    delete from dss_func_dbt where t_service = p_service_id;
    delete from dss_sheduler_dbt where t_service = p_service_id;
    delete from dsimpleservice_dbt where t_id = p_service_id;
  end delete_service;
  
  procedure create_service (
    p_id         number,
    p_name       varchar2,
    p_start      date,
    p_end        date,
    p_periodtype number,
    p_period     number,
    p_macrofile  varchar2,
    p_macrofunc  varchar2
   ) is
   begin
     insert into dsimpleservice_dbt (t_id, t_name, t_description) values (p_id, p_name, p_name);

    insert into dss_sheduler_dbt (t_id,
                                  t_name,
                                  t_description,
                                  t_service,
                                  t_module,
                                  t_shedulertype,
                                  t_workstarttime,
                                  t_workendtime,
                                  t_starttime,
                                  t_periodtype,
                                  t_period,
                                  t_nextstamp,
                                  t_parameters,
                                  t_sessionid)
    values (p_id,
            p_name,
            p_name,
            p_id,
            1,
            1,
            p_start,
            p_end,
            p_start,
            p_periodtype,
            p_period,
            trunc(sysdate) + 1 + numtodsinterval(8, 'hour')  + numtodsinterval(1, 'second'),
            chr(1),
            null);

    insert into dss_func_dbt (t_id,
                              t_service,
                              t_level,
                              t_name,
                              t_type,
                              t_executorname,
                              t_executorfunc,
                              t_startdelay,
                              t_period,
                              t_maxattempt,
                              t_timeout,
                              t_parameters)
    values (trunc(p_id*10, -3) + mod(p_id, 1000),
            p_id,
            1,
            p_name,
            1,
            p_macrofile,
            p_macrofunc,
            0,
            0,
            1,
            null,
            null);
  end create_service;
begin
  g_service_name := 'Загрузка поручений на перевод ДС';
  g_service_id := get_service_id(p_name => g_service_name);
  
  delete_service(p_service_id => g_service_id);
  create_service(p_id         => g_service_id,
                 p_name       => g_service_name,
                 p_start      => to_date('01.01.0001 08:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                 p_end        => to_date('01.01.0001 23:59:59', 'dd.mm.yyyy hh24:mi:ss'),
                 p_periodtype => 2, --минуты
                 p_period     => 10, --раз в 10 минут
                 p_macrofile  => 'nptx_money_load_from_file.mac',
                 p_macrofunc  => 'RunTransfersLoadAuto');

  g_service_name := 'Загрузка поручений на вывод ДС с биржи';
  g_service_id := get_service_id(p_name => g_service_name);
  
  delete_service(p_service_id => g_service_id);
  create_service(p_id         => g_service_id,
                 p_name       => g_service_name,
                 p_start      => to_date('01.01.0001 08:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                 p_end        => to_date('01.01.0001 23:59:59', 'dd.mm.yyyy hh24:mi:ss'),
                 p_periodtype => 2, --минуты
                 p_period     => 10, --раз в 10 минут
                 p_macrofile  => 'nptx_money_load_from_file.mac',
                 p_macrofunc  => 'RunExchangeLoadAuto');


  g_service_name := 'Загрузка поручений на вывод ДС с внебиржи';
  g_service_id := get_service_id(p_name => g_service_name);
  
  delete_service(p_service_id => g_service_id);
  create_service(p_id         => g_service_id,
                 p_name       => g_service_name,
                 p_start      => to_date('01.01.0001 08:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                 p_end        => to_date('01.01.0001 23:59:59', 'dd.mm.yyyy hh24:mi:ss'),
                 p_periodtype => 2, --минуты
                 p_period     => 10, --раз в 10 минут
                 p_macrofile  => 'nptx_money_load_from_file.mac',
                 p_macrofunc  => 'RunOTCLoadAuto');

  g_service_name := 'Создание отложенных операций на вывод и перевод ДС';
  g_service_id := get_service_id(p_name => g_service_name);
  
  delete_service(p_service_id => g_service_id);
  create_service(p_id         => g_service_id,
                 p_name       => g_service_name,
                 p_start      => to_date('01.01.0001 08:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                 p_end        => to_date('01.01.0001 17:30:00', 'dd.mm.yyyy hh24:mi:ss'),
                 p_periodtype => 2, --минуты
                 p_period     => 10, --раз в 10 минут
                 p_macrofile  => 'nptx_money_create_operations.mac',
                 p_macrofunc  => 'RunCreateOperations');

  update dss_sheduler_dbt t set t.t_shedulertype = 0 where t_id = 10072;

  commit;
end;
/