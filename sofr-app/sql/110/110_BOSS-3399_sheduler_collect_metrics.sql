-- Создание COLLECT_METRICS_JOB

declare

  N number;

begin

  select count(*) into n from user_scheduler_programs p where p.PROGRAM_NAME = 'COLLECT_METRICS_PROGRAM';

  if n = 0

  then

    DBMS_SCHEDULER.CREATE_PROGRAM(program_name => 'COLLECT_METRICS_PROGRAM'

                                 ,program_type => 'PLSQL_BLOCK'

                                 ,program_action => 'BEGIN RSB_BROKER_MONITORING.AGGREGATEMETRICS(SYSDATE); END;'

                                 ,enabled => true);

  end if;

  select count(*) into n
  from user_scheduler_schedules p where p.SCHEDULE_NAME = 'COLLECT_METRICS_SCHEDULE';

  if n = 0

  then

    DBMS_SCHEDULER.CREATE_SCHEDULE(schedule_name => 'COLLECT_METRICS_SCHEDULE', start_date =>trunc(sysdate) + Interval '10' hour, repeat_interval => 'FREQ=HOURLY; INTERVAL=1;');

  end if;
  

  select count(*) into n from user_scheduler_jobs p where p.job_name = 'COLLECT_METRICS_JOB';

  if n = 0

  then

    DBMS_SCHEDULER.CREATE_JOB(job_name => 'COLLECT_METRICS_JOB'

                             ,program_name => 'COLLECT_METRICS_PROGRAM'

                             ,schedule_name => 'COLLECT_METRICS_SCHEDULE'

                             ,enabled => true);

  end if;

  DBMS_SCHEDULER.ENABLE('COLLECT_METRICS_JOB');

end;

/