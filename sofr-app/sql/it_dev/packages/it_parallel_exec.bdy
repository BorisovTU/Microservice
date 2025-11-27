create or replace package body it_parallel_exec is

  /**************************************************************************************************\
    Распараллеливание вычислений
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
     05.12.2024  Зыков М.В.       DEF-77577                        Рефакторинг
     04.12.2023  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/
  function init_calc return number as
    pragma autonomous_transaction;
    v_id number := to_number(to_char(sysdate, 'yyyymmdd') || abs(its_main.nextval));
  begin
    begin
      execute immediate 'alter table itt_parallel_exec drop partition p' || v_id;
    exception
      when others then
        null;
    end;
    execute immediate 'alter table itt_parallel_exec add partition p' || v_id || ' values (' || v_id || ')';
    return v_id;
  end;

  procedure clear_calc(p_id number) as
    v_msg itt_q_message_log.msgid%type;
  begin
    it_q_message.do_a_service(p_servicename => 'ExecuteCode'
                             ,p_comment => 'it_parallel_exec.clear_calc'
                             ,io_msgid => v_msg
                             ,p_messbody => 'declare
    p_worklogid integer := :1 ;
    p_messmeta  xmltype := :2 ;
  begin
    for cur_par in (select p.partition_name
                      from user_tab_partitions p
                     where p.table_name = ''ITT_PARALLEL_EXEC''
                       and (substr(p.partition_name, 2, 8) < to_char(sysdate - 5, ''yyyymmdd'') or p.partition_name = upper(''p' || p_id ||
                                            '''))
                     order by p.partition_name)
    loop
      begin
        execute immediate ''alter table itt_parallel_exec drop partition '' || cur_par.partition_name;
      exception
      when others then
        null;
      end;
    end loop;
  end;');
  end;

  function Date_to_sql(p_date date) return varchar2 as
  begin
    return 'to_date(' || to_char(p_date, 'yyyymmdd') || ',''yyyymmdd'')';
  end;

  function DateTime_to_sql(p_date date) return varchar2 as
  begin
    return 'to_date(' || to_char(p_date, 'yyyymmddhh24miss') || ',''yyyymmddhh24miss'')';
  end;

  function Str_to_sql(p_str varchar2) return varchar2 as
  begin
    return 'q''[' || p_str || ']''';
  end;

  /*procedure run_task_chunks_by_sql(p_parallel_level integer
                                  ,p_chunk_sql      varchar2
                                  ,p_sql_stmt       varchar2
                                  ,p_comment varchar2 default null ) as
    l_task_name varchar2(30);
    l_try       number;
    l_status    number;
    l_logtxt    varchar(2000);
  begin
    l_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;
    l_logtxt    := 'task_name:' || l_task_name;
    it_log.log(p_msg => l_logtxt || ' Parallel:' || p_parallel_level || '[chunk_sql]', p_msg_clob => p_chunk_sql);
    it_log.log(p_msg => l_logtxt || ' Parallel:' || p_parallel_level || '[sql_stmt]', p_msg_clob => p_sql_stmt);
    DBMS_PARALLEL_EXECUTE.CREATE_TASK(l_task_name);
    DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_SQL(l_task_name, p_chunk_sql, false);
    DBMS_PARALLEL_EXECUTE.RUN_TASK(l_task_name, p_sql_stmt, DBMS_SQL.NATIVE, parallel_level => p_parallel_level);
    L_try    := 0;
    L_status := DBMS_PARALLEL_EXECUTE.TASK_STATUS(l_task_name);
    while (l_try < 2 and L_status != DBMS_PARALLEL_EXECUTE.FINISHED)
    loop
      L_try := l_try + 1;
      DBMS_PARALLEL_EXECUTE.RESUME_TASK(l_task_name);
      L_status := DBMS_PARALLEL_EXECUTE.TASK_STATUS(l_task_name);
    end loop;
    it_log.log(p_msg => 'task_name:' || l_task_name || ' TASK_STATUS:' || L_status);
    if  L_status = DBMS_PARALLEL_EXECUTE.FINISHED_WITH_ERROR
    then
      for cur in (select distinct t.ERROR_MESSAGE from USER_PARALLEL_EXECUTE_CHUNKS t where t.TASK_NAME = l_task_name )
      loop
        l_logtxt := substr(l_logtxt || chr(10) || cur.error_message,1,2000);
      end loop;
      raise_application_error(-20000, l_logtxt);
    end if;
    DBMS_PARALLEL_EXECUTE.DROP_TASK(l_task_name);
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg => l_logtxt || ' ERROR ', p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;*/
  procedure get_proc_calc_status(p_msgid     itt_q_message_log.msgid%type
                                ,o_work_cnt  out integer
                                ,o_error_cnt out integer
                                ,o_error_msg out varchar2) as
  begin
    select nvl(sum(case
                     when chk_state = 0 then
                      1
                     else
                      0
                   end)
              ,0)
          ,nvl(sum(case
                     when chk_state < 0 then
                      1
                     else
                      0
                   end)
              ,0)
          ,max(case
                 when chk_state < 0 then
                  '[' || servicename || ']: ' || nvl(it_q_message.get_errtxt(commenttxt), status)
                 else
                  null
               end)
      into o_work_cnt
          ,o_error_cnt
          ,o_error_msg
      from (select it_q_message.chk_process_state(status) chk_state
                  ,commenttxt
                  ,servicename
                  ,status
                  ,statusdt
            --from table(it_q_message.select_answer_msg(p_msgid => p_msgid, p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT))
              from (select msgid
                          ,commenttxt
                          ,servicename
                          ,status
                          ,statusdt
                          ,queuetype
                          ,min(queuetype) over(partition by l.msgid) min_queuetype
                      from itt_q_message_log l
                     where l.corrmsgid = p_msgid
                    union all
                    select msgid
                          ,commenttxt
                          ,servicename
                          ,status
                          ,statusdt
                          ,queuetype
                          ,min(queuetype) over(partition by l.msgid) min_queuetype
                      from itt_q_message_log l
                     where l.msgid = p_msgid)
             where min_queuetype = queuetype)
     where chk_state <= 0;
  end;

  procedure run_task_chunks_by_sql(p_parallel_level integer
                                  ,p_chunk_sql      varchar2 -- select с 2 полями number  
                                  ,p_sql_stmt       varchar2
                                  ,p_comment        varchar2 default null) as
    v_msg_proc       itt_q_message_log.msgid%type;
    v_msg_chunk      itt_q_message_log.msgid%type;
    v_RootTag        varchar2(128) := 'ParallelExec_by_SQL';
    v_comment        varchar2(2000) := substr(nvl(p_comment, v_RootTag), 1, 1000);
    v_chunk_cnt      integer;
    v_chunk_n        integer := 0;
    v_chunk_x        xmltype;
    cur_chunk        sys_refcursor;
    v_startid        number;
    v_endid          number;
    v_proc_comment   itt_q_message_log.commenttxt%type;
    v_work_cnt       integer := 0;
    v_error_cnt      integer;
    v_parallel_level integer := p_parallel_level;
    function Get_parallel_level return integer as
      v_worker_load_percent pls_integer := it_q_manager.get_worker_load_percent;
    begin
      return greatest(round(LEAST(abs(nvl(p_parallel_level, 8)), 16) / 100 * case when v_worker_load_percent > 90 then 30 when v_worker_load_percent > 80 then 50 when
                            v_worker_load_percent > 70 then 80 else 100 end)
                     ,2);
    end;
  
  begin
    it_log.log(p_msg => v_comment || ' Parallel:' || p_parallel_level || ' [chunk_sql]', p_msg_clob => p_chunk_sql);
    it_log.log(p_msg => v_comment || ' Parallel:' || p_parallel_level || ' [sql_stmt]', p_msg_clob => p_sql_stmt);
    it_q_message.do_a_service(p_servicename => 'ProcessStart', p_comment => v_comment, io_msgid => v_msg_proc);
    execute immediate 'select count(1) from (' || p_chunk_sql || ')'
      into v_chunk_cnt;
    open cur_chunk for p_chunk_sql;
    <<outer_loop>>
    loop
      v_parallel_level := Get_parallel_level();
      for n in v_work_cnt + 1 .. v_parallel_level
      loop
        fetch cur_chunk
          into v_startid
              ,v_endid;
        exit outer_loop when cur_chunk%notfound;
        v_chunk_n := v_chunk_n + 1;
        select xmlelement(evalname v_RootTag, xmlelement("StartID", v_startid), xmlelement("EndID", v_endid), xmlelement("ChunkN", v_chunk_n), xmlelement("ChunkCnt", v_chunk_cnt))
          into v_chunk_x
          from dual;
        v_msg_chunk := null;
        it_q_message.do_a_service(p_servicename => v_RootTag
                                 ,p_messbody => p_sql_stmt
                                 ,p_messmeta => v_chunk_x
                                 ,p_corrmsgid => v_msg_proc
                                 ,p_comment => v_comment || '#' || v_chunk_n || '/' || v_chunk_cnt || ' PL=' || n || '/' || v_parallel_level
                                 ,io_msgid => v_msg_chunk);
      end loop;
      dbms_lock.sleep(0.5);
      get_proc_calc_status(p_msgid => v_msg_proc, o_work_cnt => v_work_cnt, o_error_cnt => v_error_cnt, o_error_msg => v_proc_comment);
      if v_error_cnt > 0
      then
        raise_application_error(-20000, v_comment || ' ERROR:' || v_proc_comment);
      end if;
    end loop;
    close cur_chunk;
    loop
      get_proc_calc_status(p_msgid => v_msg_proc, o_work_cnt => v_work_cnt, o_error_cnt => v_error_cnt, o_error_msg => v_proc_comment);
      exit when v_work_cnt = 0 or v_error_cnt > 0;
      dbms_lock.sleep(0.5);
    end loop;
    if v_error_cnt > 0
    then
      raise_application_error(-20000, v_comment || ' ERROR:' || v_proc_comment);
    end if;
    it_log.log(p_msg => v_comment || ': Выполнено ' || v_chunk_n);
  exception
    when others then
      if cur_chunk%isopen
      then
        close cur_chunk;
      end if;
      it_error.put_error_in_stack;
      it_log.log(p_msg => v_comment || ' ERROR ', p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  procedure run_task_chunks_by_calc(p_parallel_level integer
                                   ,p_id             number
                                   ,p_sql_stmt       varchar2 -- Обязательно должно быть условие 'FROM itt_parallel_exec where calc_sid = ???'
                                   ,p_force          number default 1 -- Если задания различны по времени исполнения  > 1
                                   ,p_comment        varchar2 default null) as
    v_parallel_level pls_integer := p_parallel_level;
    v_force          number := least(greatest(p_force, 1), 5);
    v_chunk_sql      varchar2(32600);
  begin
    v_chunk_sql := 'select min(row_id) as start_id, max(row_id) as end_id 
 from (SELECT row_id, NTILE(' || to_char(v_parallel_level * v_force) || ') over (order by row_id) as ntile 
          FROM itt_parallel_exec partition (p' || p_id || ') where calc_id = ' || p_id || ')
 group by ntile';
    run_task_chunks_by_sql(p_parallel_level => v_parallel_level, p_chunk_sql => v_chunk_sql, p_sql_stmt => p_sql_stmt, p_comment => p_comment);
  end;

  -- Cервис параллельного выполнения заданий ;
  procedure ParallelExec_by_sql(p_worklogid integer
                               ,p_messbody  clob
                               ,p_messmeta  xmltype
                               ,o_msgid     out varchar2
                               ,o_MSGCode   out integer
                               ,o_MSGText   out varchar2
                               ,o_messbody  out clob
                               ,o_messmeta  out xmltype) as
    v_startid number;
    v_endid   number;
  begin
    with x_in as
     (select p_messmeta as x from dual)
    select it_xml.char_to_number(EXTRACTVALUE(x_in.x, '*/StartID'))
          ,it_xml.char_to_number(EXTRACTVALUE(x_in.x, '*/EndID'))
      into v_startid
          ,v_endid
      from x_in;
    execute immediate p_messbody
      using v_startid, v_endid;
  end;

end it_parallel_exec;
/
