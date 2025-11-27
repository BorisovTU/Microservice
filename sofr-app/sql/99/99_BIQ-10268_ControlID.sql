DECLARE
    v_id   INTEGER;
    cnt    NUMBER;
BEGIN
    SELECT MAX (t_id) + 1 INTO v_id FROM dsimpleservice_dbt;

    SELECT COUNT (*)
      INTO cnt
      FROM DSIMPLESERVICE_DBT
     WHERE T_NAME = 'Обмен данными с AC CDI - КонтрID';

    IF CNT = 0
    THEN
        INSERT INTO dsimpleservice_dbt
                 VALUES (v_id,                                 --t_id
                         'Обмен данными с AC CDI - КонтрID',   --t_name
                         'Обмен данными с AC CDI - КонтрID',   --t_description
                         'X'                                   --t_isactive
                        );


        INSERT INTO dss_func_dbt
                 VALUES (v_id - 10000 + 100000,                --t_id
                         v_id,                                 --t_service
                         1,                                    --t_level
                         'Обмен данными с AC CDI - КонтрID',   --t_name
                         1,                                    --t_type
                         'ws_check_job',                       --t_executorname
                         'ws_check_cdi_ID',                    --t_executorfunc
                         0,                                    --t_startdelay,
                         0,                                    --t_period
                         1,                                    --t_maxattempt,
                         NULL,                                 --t_timeout
                         NULL                                  --t_parameters
                        );

        --Periodtype 1 секунды, 2 минута, 3 часы, 4 дни. period это количество или секунд или минут и т.д.
        INSERT INTO dss_sheduler_dbt 
                 VALUES (v_id,                                 --t_id
                         'Обмен данными с AC CDI - КонтрID',   --t_name
                         'Обмен данными с AC CDI - КонтрID',   --t_description
                         v_id,                                 --t_service
                         1,                                    --t_module
                         1,                                    --t_shedulertype
                         TO_DATE ('01.01.0001 03:53:01',
                                  'dd.mm.yyyy hh24:mi:ss'),    --t_workstarttime
                         TO_DATE ('01.01.0001 08:59:59',
                                  'dd.mm.yyyy hh24:mi:ss'),    --t_workendtime
                         TO_DATE ('01.01.0001 03:53:01',
                                  'dd.mm.yyyy hh24:mi:ss'),    --t_starttime
                         4,                                    --t_periodtype
                         1,                                    --t_period
                         TO_DATE ('01.01.2023 03:00:01',
                                  'dd.mm.yyyy hh24:mi:ss'),    --t_nextstamp
                         NULL,
                         NULL);
    END IF;

    COMMIT;
END;
/
