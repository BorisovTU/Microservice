declare
  v_id integer;
  v_id_d integer;
  v_funcid integer;
begin

    select max(t_id)+1 into v_id from dsimpleservice_dbt;

    insert into dsimpleservice_dbt
    values (v_id, --t_id
            'Отчет по договорам ИИС', --t_name
            'Отчет по договорам ИИС, по которым не предоставлены подтверждающие документы', --t_description
            'X' --t_isactive
            );


    select max(t_id)+1 into v_id_d from dss_func_dbt;


    insert into dss_func_dbt
    values ( v_id_d, --t_id
             v_id, --t_service
             1, --t_level
             'Отчет по договорам ИИС', --t_name
             1, --t_type
             'ws_check_job', --t_executorname
             'ws_Report_AC', --t_executorfunc
             0, --t_startdelay,
             0, --t_period
             1, --t_maxattempt,
             null, --t_timeout
             null --t_parameters
             );

    --Periodtype 1 секунды, 2 минута, 3 часы, 4 дни. period это количество или секунд или минут и т.д.
    insert into dss_sheduler_dbt
    values ( v_id, --t_id
             'Отчет по договорам ИИС', --t_name
             'Отчет по договорам ИИС, по которым не предоставлены подтверждающие документы', --t_description
             v_id, --t_service
             1, --t_module
             2, --t_shedulertype
             TO_DATE('01.01.0001 09:00:01','dd.mm.yyyy hh24:mi:ss'), --t_workstarttime
             TO_DATE('01.01.0001 23:59:59','dd.mm.yyyy hh24:mi:ss'), --t_workendtime
             TO_DATE('01.01.0001 09:00:01','dd.mm.yyyy hh24:mi:ss'), --t_starttime
             4, --t_periodtype
             1, --t_period
             TO_DATE('09.08.2023 13:00:01','dd.mm.yyyy hh24:mi:ss'), --t_nextstamp
             null,
             null);

    commit;
end;
/