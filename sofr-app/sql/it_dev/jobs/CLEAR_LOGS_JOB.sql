-- Создание CLEAR_LOGS_JOB
declare
  N number;
begin
  select count(*) into n from user_scheduler_programs p where p.PROGRAM_NAME = 'CLEAR_LOGS_PROGRAM';
  if n = 0
  then
    DBMS_SCHEDULER.CREATE_PROGRAM(program_name => 'CLEAR_LOGS_PROGRAM'
                                 ,program_type => 'STORED_PROCEDURE'
                                 ,program_action => 'USR_CLEAR_LOGS'
                                 ,enabled => true);
  end if;
  select count(*) into n from user_scheduler_schedules p where p.SCHEDULE_NAME = 'CLEAR_LOGS_SCHEDULE';
  if n = 0
  then
    DBMS_SCHEDULER.CREATE_SCHEDULE(schedule_name => 'CLEAR_LOGS_SCHEDULE', start_date => SYSTIMESTAMP, repeat_interval => 'FREQ=DAILY; BYHOUR=23;');
  end if;
  select count(*) into n from user_scheduler_jobs p where p.job_name = 'CLEAR_LOGS_JOB';
  if n = 0
  then
    DBMS_SCHEDULER.CREATE_JOB(job_name => 'CLEAR_LOGS_JOB'
                             ,program_name => 'CLEAR_LOGS_PROGRAM'
                             ,schedule_name => 'CLEAR_LOGS_SCHEDULE'
                             ,enabled => true);
  end if;
  DBMS_SCHEDULER.ENABLE('CLEAR_LOGS_JOB');
end;
/
