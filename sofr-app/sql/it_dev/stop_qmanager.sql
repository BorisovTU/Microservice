-- Остановка QManager
begin
  begin
    execute immediate 'begin it_q_manager.cmdmanager(''EXIT''); end ;';
    dbms_lock.sleep(5);
  exception
    when others then
      it_log.log(p_msg => 'STOP IT_Q_MANAGER: ERROR', p_msg_clob => sqlerrm);
  end;
  begin
    for j in (select j.job_name
                    ,j.ENABLED
                    ,j.state 
                from sys.user_scheduler_jobs j
               where j.job_name like 'IT_Q_WORKER%' or 
                      j.job_name like 'IT_Q_MANAGER%' or 
                      j.job_name like 'IT_P_MANAGER%' 
               order by 1)
    loop
      begin
      if j.enabled = 'TRUE'
      then
        sys.dbms_scheduler.disable(j.job_name, force => true);
      end if;
      if j.state = 'RUNNING'
      then
        sys.dbms_scheduler.stop_job(j.job_name);
      end if;
      sys.dbms_scheduler.DROP_JOB(j.job_name);
      exception when others then
        null;
      end ;
    end loop;
  exception
    when others then
      it_log.log(p_msg => 'DROP IT_Q_MANAGER: ERROR', p_msg_clob => sqlerrm);
  end;
  begin
    execute immediate 'begin upgrader.set_alter_system_first; end;';
  exception
    when others then
      it_log.log(p_msg => 'UPGRADER.SET_ALTER_SYSTEM_FIRST: ERROR', p_msg_clob => sqlerrm);
  end;
end;
