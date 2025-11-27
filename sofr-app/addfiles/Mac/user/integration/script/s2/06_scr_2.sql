--задание планировщику TomCat  uTableProcessOut_dbt
DECLARE
    v_SimpleServiceId     NUMBER(10);
    v_SServiceName        VARCHAR2(50);
    v_SServiceDesctiption VARCHAR2(500);

    v_WorkStartTime       DATE := TO_DATE('01.01.0001 9:00:01', 'dd.mm.yyyy hh24:mi:ss');
    v_WorkEndTime         DATE := TO_DATE('01.01.0001 23:59:59', 'dd.mm.yyyy hh24:mi:ss');
    v_StartTime           DATE := TO_DATE('01.01.0001 9:00:01', 'dd.mm.yyyy hh24:mi:ss');

    v_FuncId              NUMBER(10);
    v_SchedulerId         NUMBER(10);

    v_Exec_Name           VARCHAR2(256);
    v_Exec_Func           VARCHAR2(50);

    v_Level               NUMBER(10); -- Заполнять внимательно, если требуется последовательность вызовов
    v_Module              NUMBER(10); -- Если будет играть критичную роль в RS-Bank, то заполнять осмысленно

BEGIN
/*
T_SHEDULERTYPE:
    0 - выключена,
    1 - в переделах т_воркстарттайм-т_воркендтайм с учётом т_старттайм,
    2 - запускается в любое время суток

T_WORKTIMETYPE (использование не подтверждено):
    1 - без учета выходных,
    2 - с учетом праздничных,
    3 - с учетом выходных
*/

  v_SimpleServiceId := 10001;
  v_SServiceName := 'Обработчик исходящих потоков uTableProcessOut_dbt';
  v_SServiceDesctiption := 'Процедура переноса данных в буферные таблицы по Лимитам QUIK для ETL';

  INSERT INTO dsimpleservice_dbt( T_ID, T_NAME, T_DESCRIPTION, T_ISACTIVE )
  VALUES( v_SimpleServiceId, v_SServiceName,  v_SServiceDesctiption, CHR(88) );


/*
T_LEVEL: уникальный индекс (T_SERVICE, T_LEVEL) - характеризует последовательность запуска

T_TYPE (тип выполняемой функции):
    1 - RSL,
    2 - Java
*/

    v_FuncId := 100001;
    v_Level := 1;
    v_Exec_Name := 'ws_check_job';
    v_Exec_Func := 'ws_check_job_Out';

    INSERT INTO dss_func_dbt( T_ID, T_SERVICE, T_LEVEL, T_NAME, T_TYPE, T_EXECUTORNAME, T_EXECUTORFUNC, T_STARTDELAY, T_PERIOD, T_MAXATTEMPT )
    VALUES( v_FuncId, v_SimpleServiceId, v_Level, v_SServiceName, 1,  v_Exec_Name, v_Exec_Func, 0, 0, 1 );


/*
T_SHEDULERTYPE:
    0 - приостановлен
    1 - с учетом рабочего времени
    2 - без учета рабочего времени

T_PERIODTYPE:
    1 - Секунда
    2 - Минута
    3 - Час
    4 - Календарный день
    5 - Рабочий день
    6 - Неделя
    7 - Месяц
    8 - Квартал
    9 - Год
*/

    v_SchedulerId := 10001;
    v_Module := 1;

    INSERT INTO dss_sheduler_dbt( T_ID, T_NAME, T_DESCRIPTION, T_SERVICE, T_MODULE, T_SHEDULERTYPE, T_WORKSTARTTIME, T_WORKENDTIME, T_STARTTIME, T_PERIODTYPE, T_PERIOD )
    VALUES( v_SchedulerId, v_SServiceName, v_SServiceDesctiption, v_SimpleServiceId, v_Module, 2, v_WorkStartTime, v_WorkEndTime, v_StartTime, 2, 10 );

END;
--задание планировщику TomCat uTableProcessOut_dbt
