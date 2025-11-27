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
            'Контроль отправки уведомлений об открытии ДБО\ИИС', --t_name
            'Контроль отправки уведомлений об открытии ДБО\ИИС', --t_description
            'X' --t_isactive
            );

    select max(t_id)+1 
    into v_id_d 
    from dss_func_dbt;

    insert into dss_func_dbt
    values ( v_id_d, --t_id
             v_id, --t_service
             1, --t_level
             'Контроль отправки уведомлений об открытии ДБО\ИИС', --t_name
             1, --t_type
             'ws_dboMessage', --t_executorname
             'CheckAndSendDBOMessage', --t_executorfunc
             0, --t_startdelay,
             0, --t_period
             1, --t_maxattempt,
             null, --t_timeout
             null --t_parameters
             );

    --Periodtype 1 секунды, 2 минута, 3 часы, 4 дни. period это количество или секунд или минут и т.д.
    insert into dss_sheduler_dbt
    values ( v_id, --t_id  10085
             'Контроль отправки уведомлений об открытии ДБО\ИИС', --t_name
             'Контроль отправки уведомлений об открытии ДБО\ИИС', --t_description
             v_id, --t_service
             1, --t_module
             0, --t_shedulertype. с учетом рабочего времени
             TO_DATE('01.01.0001 00:00:00','dd.mm.yyyy hh24:mi:ss'), --t_workstarttime. Рабочее время начало
             TO_DATE('01.01.0001 23:59:59','dd.mm.yyyy hh24:mi:ss'), --t_workendtime Рабочее время окончание
             TO_DATE('01.01.0001 00:00:00','dd.mm.yyyy hh24:mi:ss'), --t_starttime. начало запуска
             3, --t_periodtype. час
             3, --t_period. через три час
             trunc(sysdate), --t_nextstamp
             null,
             null);
   
       
    
    select max(t_funcid)+1 into v_funcid from dfunc_dbt;
    
    insert into dfunc_dbt
    values (v_funcid, --t_funcid
            'ws_dboMessage', --t_code
            'Контроль отправки уведомлений об открытии ДБО\ИИС', --t_name
            1, --t_type
            'ws_dboMessage', --t_filename
            'CheckAndSendDBOMessage', --t_functionname
            0, --t_interval
            null, --t_version
            null --t_module
            );

    
    insert into dllvalues_dbt
    values (5002,             
            v_funcid,
            'ws_dboMessage',
            'Контроль отправки уведомлений об открытии ДБО\ИИС',
            v_funcid,
            'Контроль отправки уведомлений об открытии ДБО\ИИС',
            chr(1));
end;
/
