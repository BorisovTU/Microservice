-- Создание CLEAR_METRICS_JOB

declare

  N number;

begin

  select count(*) into n from user_scheduler_programs p where p.PROGRAM_NAME = 'CLEAR_METRICS_PROGRAM';

  if n = 0

  then

    DBMS_SCHEDULER.CREATE_PROGRAM(program_name => 'CLEAR_METRICS_PROGRAM'

                                 ,program_type => 'PLSQL_BLOCK'

                                 ,program_action => 'BEGIN RSB_BROKER_MONITORING.CLEARTABLEMETRICS; END;'

                                 ,enabled => true);

  end if;

  select count(*) into n
  from user_scheduler_schedules p where p.SCHEDULE_NAME = 'CLEAR_METRICS_SCHEDULE';

  if n = 0

  then
  --HOURLY DAILY
  --Каждый день проверка на очистку
    DBMS_SCHEDULER.CREATE_SCHEDULE(schedule_name => 'CLEAR_METRICS_SCHEDULE', start_date => trunc(sysdate) + Interval '6' hour, repeat_interval => 'FREQ=DAILY; INTERVAL=1;');

  end if;
  

  select count(*) into n from user_scheduler_jobs p where p.job_name = 'CLEAR_METRICS_JOB';

  if n = 0

  then

    DBMS_SCHEDULER.CREATE_JOB(job_name => 'CLEAR_METRICS_JOB'

                             ,program_name => 'CLEAR_METRICS_PROGRAM'

                             ,schedule_name => 'CLEAR_METRICS_SCHEDULE'

                             ,enabled => true);

  end if;

  DBMS_SCHEDULER.ENABLE('CLEAR_METRICS_JOB');

end;

/