declare
  v_id integer;
  v_id_d integer;
  v_funcid integer;
  t_count integer;
begin
select count(*) into t_count from dsimpleservice_dbt where t_name = 'Запуск операций зачисления д/с ЮЛ по ИРК'; 
if t_count = 0 then
    select max(t_id)+1 i
    into v_id 
    from dsimpleservice_dbt;

    insert into dsimpleservice_dbt
    values (v_id, --t_id
            'Запуск операций зачисления д/с ЮЛ по ИРК', --t_name
            'Запуск на выполнение операций зачисления д/с ЮЛ по ИРК, имеющих индивидуальный расчетный код', --t_description
            'X' --t_isactive
            );

    select max(t_id)+1 
    into v_id_d 
    from dss_func_dbt;

    insert into dss_func_dbt
    values ( v_id_d, --t_id
             v_id, --t_service
             1, --t_level
             'Запуск операций зачисления д/с ЮЛ по ИРК', --t_name
             1, --t_type
             'enrollSchedIRK.mac', --t_executorname
             'PushEnrollsIRK', --t_executorfunc
             0, --t_startdelay,
             0, --t_period
             1, --t_maxattempt,
             0, --t_timeout
             null --t_parameters
             );



    --Periodtype 1 секунды, 2 минута, 3 часы, 4 дни. period это количество или секунд или минут и т.д.
    insert into dss_sheduler_dbt
    values ( v_id, --t_id  10085
            'Запуск операций зачисления д/с ЮЛ по ИРК', --t_name
            'Запуск на выполнение операций зачисления д/с ЮЛ по ИРК, имеющих индивидуальный расчетный код', --t_description
             v_id, --t_service
             0, --t_module
             0, --t_shedulertype. приостановлен
             TO_DATE('01.01.0001 08:00:01','dd.mm.yyyy hh24:mi:ss'), --t_workstarttime. Рабочее время начало
             TO_DATE('01.01.0001 23:59:59','dd.mm.yyyy hh24:mi:ss'), --t_workendtime Рабочее время окончание
             TO_DATE('01.01.0001 08:00:01','dd.mm.yyyy hh24:mi:ss'), --t_starttime. начало запуска
             4, --t_periodtype. дни
             1, --t_period. раз в день
             TO_DATE('01.01.0001','dd.mm.yyyy'), --t_nextstamp
             null,
             0);
 end if;
end;
/