declare
    cnt number;
    vsimpleservicenum number := 10088;
    vfuncnum number := 100088;
    vshedulernum number := 10088;
begin

    select count(*) into cnt from DSIMPLESERVICE_DBT where t_id = vsimpleservicenum;
    if cnt = 0 then 
        Insert into DSIMPLESERVICE_DBT
           (T_ID, T_NAME, T_DESCRIPTION, T_ISACTIVE)
         Values
           (vsimpleservicenum, 'Создание операции тех. сверки СНОБ', 'Создание операции тех. сверки СНОБ', 'X');
    end if;

    select count(*) into cnt from DSS_FUNC_DBT where t_id = vfuncnum;
    if cnt = 0 then 
        Insert into DSS_FUNC_DBT
           (T_ID, T_SERVICE, T_LEVEL, T_NAME, T_TYPE, T_EXECUTORNAME, T_EXECUTORFUNC, T_STARTDELAY, T_PERIOD, T_MAXATTEMPT)
         Values
           (vfuncnum, vsimpleservicenum, 1, 'Создание операции тех. сверки СНОБ', 1, 
            'nptxsnobvershed', 'ExecSverSched', 0, 0, 1);
    end if;

    select count(*) into cnt from DSS_SHEDULER_DBT where t_id = vshedulernum;
    if cnt = 0 then 
        Insert into DSS_SHEDULER_DBT
           (T_ID, T_NAME, T_DESCRIPTION, T_SERVICE, T_MODULE, T_SHEDULERTYPE, T_WORKSTARTTIME, T_WORKENDTIME, T_STARTTIME, T_PERIODTYPE, T_PERIOD, T_NEXTSTAMP)
         Values
           (vshedulernum, 'Создание операции тех. сверки СНОБ', 'Создание операции тех. сверки СНОБ', vsimpleservicenum, 1, 
            2, TO_DATE('01/01/0001 00:03:01', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 23:59:59', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:03:01', 'MM/DD/YYYY HH24:MI:SS'), 
            4, 1, TO_DATE('12/04/2024 00:03:01', 'MM/DD/YYYY HH24:MI:SS'));
    end if;
end;