declare
    cnt number;
    vsimpleservicenum number := 10055;
    vfuncnum number := 100055;
    vshedulernum number := 10055;
begin

    select count(*) into cnt from DSIMPLESERVICE_DBT where t_id = vsimpleservicenum;
    if cnt = 0 then 
        Insert into DSIMPLESERVICE_DBT
           (T_ID, T_NAME, T_DESCRIPTION, T_ISACTIVE)
         Values
           (vsimpleservicenum, 'Запуск операций', 'Запуск на выполнение операций зачисления д/с BIQ-15498', 'X');
    end if;

    select count(*) into cnt from DSS_FUNC_DBT where t_id = vfuncnum;
    if cnt = 0 then 
        Insert into DSS_FUNC_DBT
           (T_ID, T_SERVICE, T_LEVEL, T_NAME, T_TYPE, T_EXECUTORNAME, T_EXECUTORFUNC, T_STARTDELAY, T_PERIOD, T_MAXATTEMPT)
         Values
           (vfuncnum, vsimpleservicenum, 1, 'Запуск операций зачисления д/с', 1, 
            'Enroll_Sched', 'PushEnrolls', 0, 0, 1);
    end if;

    select count(*) into cnt from DSS_SHEDULER_DBT where t_id = vshedulernum;
    if cnt = 0 then 
        Insert into DSS_SHEDULER_DBT
           (T_ID, T_NAME, T_DESCRIPTION, T_SERVICE, T_MODULE, T_SHEDULERTYPE, T_WORKSTARTTIME, T_WORKENDTIME, T_STARTTIME, T_PERIODTYPE, T_PERIOD, T_NEXTSTAMP)
         Values
           (vshedulernum, 'Запуск операций зачисления д/с', 'Запуск на выполнение операций зачисления д/с BIQ-15498', vsimpleservicenum, 1, 
            2, TO_DATE('01/01/0001 09:00:01', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 23:59:59', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 09:00:01', 'MM/DD/YYYY HH24:MI:SS'), 
            2, 5, TO_DATE('10/20/2023 23:00:00', 'MM/DD/YYYY HH24:MI:SS'));
    end if;

    COMMIT;
end;