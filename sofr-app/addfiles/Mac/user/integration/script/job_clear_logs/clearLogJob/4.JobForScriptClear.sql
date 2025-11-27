BEGIN
   DBMS_SCHEDULER.CREATE_PROGRAM (program_name     => 'CLEAR_LOGS_PROGRAM',
                                  program_type     => 'STORED_PROCEDURE',
                                  program_action   => 'USR_CLEAR_LOGS',
                                  enabled          => TRUE);
                                  
                                
   DBMS_SCHEDULER.CREATE_SCHEDULE (
      schedule_name     => 'CLEAR_LOGS_SCHEDULE',
      start_date        => SYSTIMESTAMP,
      repeat_interval   => 'FREQ=DAILY; BYHOUR=23;'
   );
   DBMS_SCHEDULER.CREATE_JOB (job_name        => 'CLEAR_LOGS_JOB',
                              program_name    => 'CLEAR_LOGS_PROGRAM',
                              schedule_name   => 'CLEAR_LOGS_SCHEDULE',
                              enabled         => TRUE);
   DBMS_SCHEDULER.ENABLE('CLEAR_LOGS_JOB');
END;
/