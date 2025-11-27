declare
  v_id integer;
  v_funcid integer;
  cnt number;
begin

    select max(t_id)+1 into v_id from dsimpleservice_dbt;

    select count(*) into cnt from DSIMPLESERVICE_DBT where T_NAME = 'Загрузка сделок из РСХБ-Брокер для лимитов';

    IF CNT = 0 THEN

       insert into dsimpleservice_dbt
       values (v_id, --t_id
               'Загрузка сделок из РСХБ-Брокер для лимитов', --t_name
               'Загрузка клиентских сделок из РСХБ-Брокер для сверки лимитов', --t_description
               'X' --t_isactive
               );
   
   
       insert into dss_func_dbt
       values ( v_id-10000+100000, --t_id
                v_id, --t_service
                1, --t_level
                'Загрузка сделок из РСХБ-Брокер для лимитов', --t_name
                1, --t_type
                'quik_load_auto', --t_executorname
                'RunProcess', --t_executorfunc
                0, --t_startdelay,
                0, --t_period
                1, --t_maxattempt,
                null, --t_timeout
                null  --t_parameters
                );
   
       --Periodtype 1 секунды, 2 минута, 3 часы, 4 дни. period это количество или секунд или минут и т.д.
       insert into dss_sheduler_dbt /*(t_id, t_name, t_description, t_service, t_module, t_shedulertype, t_workstarttime, t_workendtime, t_starttime, t_periodtype, t_period, t_nextstamp)*/
       values ( v_id, --t_id
                'Загрузка сделок из РСХБ-Брокер для лимитов', --t_name
                'Загрузка клиентских сделок из РСХБ-Брокер для сверки лимитов', --t_description
                v_id, --t_service
                1, --t_module
                1, --t_shedulertype
                TO_DATE('01.01.0001 01:00:01','dd.mm.yyyy hh24:mi:ss'), --t_workstarttime
                TO_DATE('01.01.0001 01:20:00','dd.mm.yyyy hh24:mi:ss'), --t_workendtime
                TO_DATE('01.01.0001 01:00:01','dd.mm.yyyy hh24:mi:ss'), --t_starttime
                2, --t_periodtype
                40, --t_period
                TO_DATE('01.01.2023 00:59:00','dd.mm.yyyy hh24:mi:ss'), --t_nextstamp
                null, null);

   END IF;

   commit;
end;
