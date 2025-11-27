declare
  v_id integer;
  v_id_d integer;
  v_funcid integer;
begin

    select max(t_id)+1 i
    into v_id 
    from dsimpleservice_dbt;

    insert into dsimpleservice_dbt
    values (v_id, --t_id
            'Обработка запросов УНКД от Диасофта', --t_name
            'Обработка запросов УНКД от Диасофта', --t_description
            'X' --t_isactive
            );

    select max(t_id)+1 
    into v_id_d 
    from dss_func_dbt;

    insert into dss_func_dbt
    values ( v_id_d, --t_id
             v_id, --t_service
             1, --t_level
             'Обработка запросов УНКД от Диасофта', --t_name
             1, --t_type
             'nptxnkdreqdias_sheduler.mac', --t_executorname
             'ExecNkdReqDias_Shed', --t_executorfunc
             0, --t_startdelay,
             0, --t_period
             1, --t_maxattempt,
             0, --t_timeout
             null --t_parameters
             );



    --Periodtype 1 секунды, 2 минута, 3 часы, 4 дни. period это количество или секунд или минут и т.д.
    insert into dss_sheduler_dbt
    values ( v_id, --t_id  10085
             'Обработка запросов УНКД от Диасофта', --t_name
             'Обработка запросов УНКД от Диасофта', --t_description
             v_id, --t_service
             0, --t_module
             2, --t_shedulertype. с учетом рабочего времени
             TO_DATE('01.01.0001 00:00:01','dd.mm.yyyy hh24:mi:ss'), --t_workstarttime. Рабочее время начало
             TO_DATE('01.01.0001 23:59:59','dd.mm.yyyy hh24:mi:ss'), --t_workendtime Рабочее время окончание
             TO_DATE('01.01.0001 00:00:01','dd.mm.yyyy hh24:mi:ss'), --t_starttime. начало запуска
             2, --t_periodtype. минуты
             5, --t_period. через 5 мин
             TO_DATE('01.01.0001','dd.mm.yyyy'), --t_nextstamp
             null,
             0);
end;
/