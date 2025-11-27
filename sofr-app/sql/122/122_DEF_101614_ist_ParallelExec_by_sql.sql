begin
  delete ITT_Q_SERVICE s where upper(s.servicename) in ('PROCESSSTART','PARALLELEXEC_BY_SQL');
  insert into itt_q_service
    (message_type,
     servicename,
     subscription,
     service_caption,
     stop_apostq,
     service_price)
  values
    ('R',
     'ProcessStart',
     1,
     'Служебный сервис генерации GUID процесса',
    0,
     1);
  insert into itt_q_service
    (message_type,
     servicename,
     subscription,
     service_proc,
     service_caption,
     stop_apostq,
     service_price)
  values
    ('R',
     'ParallelExec_by_SQL',
     1,
     'it_parallel_exec.ParallelExec_by_sql',
     'Cервис параллельного выполнения заданий ',
     1,
     1000);
end;
