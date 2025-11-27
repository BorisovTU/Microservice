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
            'Заполнение буферной таблицы результатами БО', --t_name
            'Заполнение буферной таблицы dkl11sofr2dwh_dbt ', --t_description
            'X' --t_isactive
            );

    select max(t_id)+1 
    into v_id_d 
    from dss_func_dbt;

    insert into dss_func_dbt
    values ( v_id_d, --t_id
             v_id, --t_service
             1, --t_level
             'Заполнение буферной таблицы результатами БО', --t_name
             1, --t_type
             'ws_w475sofr2chd', --t_executorname
             'ws_check_third_workday', --t_executorfunc
             0, --t_startdelay,
             0, --t_period
             1, --t_maxattempt,
             null, --t_timeout
             null --t_parameters
             );



      --Periodtype 1 секунды, 2 минута, 3 часы, 4 дни. period это количество или секунд или минут и т.д.
    insert into dss_sheduler_dbt
    values ( v_id, --t_id  10085
             'Заполнение буферной таблицы результатами БО', --t_name
             'Заполнение буферной таблицы СОФР dkl11sofr2dwh_dbt', --t_description
             v_id, --t_service
             1, --t_module
             1, --t_shedulertype. с учетом рабочего времени
             TO_DATE('01.01.0001 01:00:01','dd.mm.yyyy hh24:mi:ss'), --t_workstarttime. Рабочее время начало
             TO_DATE('01.01.0001 06:59:59','dd.mm.yyyy hh24:mi:ss'), --t_workendtime Рабочее время окончание
             TO_DATE('01.01.0001 01:00:01','dd.mm.yyyy hh24:mi:ss'), --t_starttime. начало запуска
             4, --t_periodtype. день
             1, --t_period. через день
             trunc(sysdate) + 1 + NUMTODSINTERVAL(1, 'HOUR')  + NUMTODSINTERVAL(1, 'SECOND'), --t_nextstamp
             null,
             null);
   
       
    
    select max(t_funcid)+1 into v_funcid from dfunc_dbt;
    
    insert into dfunc_dbt
    values (v_funcid, --t_funcid
            'ws_w475sofr2chd', --t_code
            'Заполнение буферной таблицы результатами БО', --t_name
            1, --t_type
            'ws_w475sofr2chd', --t_filename
            'ws_check_third_workday', --t_functionname
            0, --t_interval
            null, --t_version
            null --t_module
            );

    
    insert into dllvalues_dbt
    values (5002,             
            v_funcid,
            'ws_w475sofr2chd',
            'Заполнение буферной таблицы результатами БО',
            v_funcid,
            'Заполнение буферной таблицы результатами БО',
            chr(1));
    commit;
end;
/