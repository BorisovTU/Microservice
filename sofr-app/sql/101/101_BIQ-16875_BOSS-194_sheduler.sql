declare
    cnt number;
    v_id number := 10052;
    v_ssid number := 100052;
    v_shortname varchar2(200) := 'Уведомление об окончании статуса КИ по ЮЛ';
    v_longname varchar2(200) := 'Рассылка уведомлений об истечении срока действия статуса КИ по ЮЛ';
begin
    select count(*) into cnt from DSIMPLESERVICE_DBT where t_id = v_id;
    if cnt = 0 then 
        Insert into DSIMPLESERVICE_DBT
           (T_ID, T_NAME, T_DESCRIPTION, T_ISACTIVE)
         Values
           (v_id, v_shortname, v_longname, 'X');
    end if;

    select count(*) into cnt from DSS_FUNC_DBT where t_id = v_ssid;
    if cnt = 0 then 
        Insert into DSS_FUNC_DBT
           (T_ID, T_SERVICE, T_LEVEL, T_NAME, T_TYPE, T_EXECUTORNAME, T_EXECUTORFUNC, T_STARTDELAY, T_PERIOD, T_MAXATTEMPT)
         Values
           (v_ssid, v_id, 1, v_shortname, 1, 
            'qi_job', 'check_end_qi', 0, 0, 1);
    end if;

    select count(*) into cnt from DSS_SHEDULER_DBT where t_id = v_id;
    if cnt = 0 then 
        Insert into DSS_SHEDULER_DBT
           (T_ID, T_NAME, T_DESCRIPTION, T_SERVICE, T_MODULE, 
            T_SHEDULERTYPE, T_WORKSTARTTIME, T_WORKENDTIME, T_STARTTIME, T_PERIODTYPE, T_PERIOD, T_NEXTSTAMP)
         Values
           (v_id, v_shortname, v_longname, v_id, 1, 
            2, TO_DATE('01/01/0001 09:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 19:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 09:00:00', 'MM/DD/YYYY HH24:MI:SS'), 
            3, 2, TO_DATE('01/01/2023 06:00:00', 'MM/DD/YYYY HH24:MI:SS'));
    end if;

    COMMIT;
end;