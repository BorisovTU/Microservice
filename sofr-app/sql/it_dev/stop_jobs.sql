-- Стоп Jobs Обязательно запускать после stop_qmanager
declare
  v_stoping boolean;
begin
  for t in (select * from user_parallel_execute_tasks t where t.status = 'PROCESSING')
  loop
    dbms_parallel_execute.stop_task(t.task_name);
  end loop;
  dbms_lock.sleep(5);
  for n in 1 .. 100
  loop
    v_stoping := false;
    for j in (select j.job_name
                    ,j.ENABLED
                from user_scheduler_jobs j
               where j.state = 'RUNNING'
               order by 1)
    loop
      v_stoping := true;
      begin
        sys.dbms_scheduler.stop_job(j.job_name);
      exception
        when others then
          sys.dbms_scheduler.stop_job(j.job_name, true);
      end;
    end loop;
    exit when not v_stoping;
    dbms_lock.sleep(5);
  end loop;
exception
  when others then
    it_log.log(p_msg => 'STOP SCHEDULER_JOBS: ERROR', p_msg_clob => sqlerrm);
end;
