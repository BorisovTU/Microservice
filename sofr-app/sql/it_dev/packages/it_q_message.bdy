create or replace package body it_q_message is

  /**************************************************************************************************\
    BIQ-9225. Разработка очереди и журнала событий
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
    06.05.2025  Зыков М.В.       CCBO-11849                       Создание механизма нового типа очередей
    27.01.2025  Зыков М.В.       BOSS-7573                        Доработки QMessage в части взаимодействия с адаптером для S3
    23.10.2023  Зыков М.В.       BOSS-1230                        BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
    15.05.2023  Зыков М.В.       CCBO-4870                        BIQ-13171. Разработка процедур работы с очередью событий
    19.09.2022  Зыков М.В.       BIQ-9225                         + Универсальные процедуры для сообщений
    02.09.2022  Зыков М.В.       BIQ-9225                         + load_extmessage
    01.09.2022  Зыков М.В.       BIQ-9225                         Правка универсальных процедур
    19.08.2022  Зыков М.В.       BIQ-9225                         Добавление функции get_errtxt
    18.08.2022  Зыков М.В.       BIQ-9225                         Доработка 
    03.08.2022  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/
  gс_userenv_current_schema varchar(100) := sys_context('userenv', 'current_schema');

  gс_RsbSessionData_oper number := RsbSessionData.oper;

  type tt_queue_num_key is table of pls_integer index by varchar2(2);

  t_queue_num            tt_queue_num := tt_queue_num(); -- Список всех очередей 
  t_queue_num_key        tt_queue_num_key; -- Список всех очередей для контроля 
  t_queue_num_parallel   tt_queue_num := tt_queue_num(); -- Список очередей для распараллеливания обработки сообщений (цифровой queue_num ) 
  gn_find_queue_num_last pls_integer := 1; --
  gn_next_queue_num_last pls_integer := 1;

  gn_message_log_hist integer := it_q_manager.get_gn_message_log_hist;

  type t_corrsystem_tt is table of itt_q_corrsystem%rowtype index by varchar2(128); -- Список систем корреспондентов.
  gt_corrsystem t_corrsystem_tt;

  gd_last_refresh_spr date; -- Время последнего обновленич справочников;
  gc_q_user           itt_q_message_log.senderuser%type;

  --------------------------------------------------------------------------------
  -- Структуры для хранения потоков исполнения зарезервированных пользователем 
  type t_thread_queue_num_tt is table of itt_q_message_log.queue_num%type index by varchar2(128); -- 
  gt_thread_queue_num t_thread_queue_num_tt;

  type t_thread_sysname_tt is table of pls_integer index by varchar2(128); -- 
  gt_thread_current t_thread_sysname_tt; --
  gt_thread_cnt     t_thread_sysname_tt;

  C_С_THREAD_PREF_SERVICEGROUP constant varchar2(128) := sys_guid; --
  C_N_THREAD_CNT_DEFAULT        constant pls_integer := abs(nvl(get_qset_number(p_qset_name => 'QMESSAGE_USER_THREADS'), 8)); --  число потоков исполнения пользователя (Настройка QMESSAGE_USER_THREADS) 
  --------------------------------------------------------------------------------
  -- Табличная функция получения списка статусов
  function select_status(p_kind_status varchar2) return tt_status_sp
    pipelined as
    vt_list_status tt_status_sp;
  begin
    case upper(p_kind_status)
      when C_KIND_STATUS_DONE then
        vt_list_status := T_STATUS_MSG_DONE;
      when C_KIND_STATUS_ERROR then
        vt_list_status := T_STATUS_MSG_ERROR;
      when C_KIND_STATUS_SEND then
        vt_list_status := T_STATUS_MSG_SEND;
      when C_KIND_STATUS_ERRSEND then
        vt_list_status := T_STATUS_MSG_ERRSEND;
      when C_KIND_STATUS_WORK then
        vt_list_status := T_STATUS_MSG_WORK;
      else
        raise_application_error(-20000, 'Ошибка параметра  it_q_message.select_status ');
    end case;
    for i in 1 .. vt_list_status.COUNT
    loop
      pipe row(vt_list_status(i));
    end loop;
  end;

  -- Возвращает группу статуса
  function get_kind_status(p_status varchar2) return varchar2 deterministic as
  begin
    case
      when (p_status member of T_STATUS_MSG_DONE) then
        return C_KIND_STATUS_DONE;
      when (p_status member of T_STATUS_MSG_ERROR) then
        return C_KIND_STATUS_ERROR;
      when (p_status member of T_STATUS_MSG_WORK) then
        return C_KIND_STATUS_WORK;
      else
        raise_application_error(-20000, 'Ошибка параметра  it_q_message.get_kind_status: p_status=' || p_status);
    end case;
  end;

  -- Проверка состояние процесса по статусу ( -1 - ошибка, 0- процесс идет , 1 - процесс закончен )
  function chk_process_state(p_status varchar2) return number deterministic as
  begin
    case
      when (p_status member of T_STATUS_MSG_DONE) then
        return 1;
      when (p_status member of T_STATUS_MSG_ERROR) then
        return - 1;
      when (p_status member of T_STATUS_MSG_WORK) then
        return 0;
      else
        raise_application_error(-20000, 'Ошибка параметра  it_q_message.chk_proc_stat: p_status=' || p_status);
    end case;
  end;

  -- Возвращает значение пакетной переменной по имени
  function get_constant_str(p_constant varchar2) return varchar2 deterministic as
    v_ret varchar2(32676);
  begin
    execute immediate ' begin :1 := it_q_message.' || p_constant || '; end;'
      using out v_ret;
    return v_ret;
  exception
    when others then
      return null;
  end;

  procedure thread_chk_systemname_init(p_systemname itt_q_message_log.receiver%type) as
    v_systemname itt_q_message_log.receiver%type := nvl(p_systemname, C_C_SYSTEMNAME);
  begin
    if not gt_thread_current.exists(v_systemname)
    then
      raise_application_error(-20000
                             ,'Для управленияч потоками исполнения необходимо инициализировать потоки (thread_init) для системы ' || p_systemname);
    end if;
  end;

  procedure thread_systemname_init(p_systemname itt_q_message_log.receiver%type
                                  ,p_queue_num  itt_q_message_log.queue_num%type) as
    v_systemname itt_q_message_log.receiver%type := nvl(p_systemname, C_C_SYSTEMNAME);
  begin
    if not gt_thread_current.exists(v_systemname)
       or not gt_thread_cnt.exists(v_systemname)
       or not gt_thread_queue_num.exists(v_systemname)
    then
      gt_thread_current(v_systemname) := 1;
      gt_thread_cnt(v_systemname) := C_N_THREAD_CNT_DEFAULT;
      gt_thread_queue_num(v_systemname) := p_queue_num;
    end if;
  end;

  --  Установка кол-ва потоков исполнения пользователя <= 0 - восстановление по умолчанию
  procedure thread_init(p_count_thread pls_integer default null
                       ,p_systemname   itt_q_message_log.receiver%type default C_C_SYSTEMNAME
                       ,p_queue_num    itt_q_message_log.queue_num%type default null) is
    v_systemname   itt_q_message_log.receiver%type := nvl(p_systemname, C_C_SYSTEMNAME);
    V_queue_num itt_q_message_log.queue_num%type := nvl(p_queue_num
                                                       ,case
                                                          when v_systemname = C_C_SYSTEMNAME then
                                                           t_queue_num(1)
                                                        end);
    v_count_thread integer;
  begin
    thread_systemname_init(v_systemname, V_queue_num);
    -- Контроль количества .
    if p_count_thread <= 0
    then
      v_count_thread := C_N_THREAD_CNT_DEFAULT;
    elsif p_count_thread is null
    then
      v_count_thread := gt_thread_cnt(v_systemname);
    elsif p_count_thread > C_N_THREAD_MAX
    then
      v_count_thread := C_N_THREAD_MAX;
    else
      v_count_thread := abs(p_count_thread);
    end if;
    v_count_thread := trunc(v_count_thread);
    gt_thread_cnt(v_systemname) := v_count_thread;
  end;

  --  Переход к  потоку исполнения ( если 0 - следующему)
  procedure thread_next(p_num_thread pls_integer default null
                       ,p_systemname itt_q_message_log.receiver%type default C_C_SYSTEMNAME) is
    v_num_thread pls_integer := abs(nvl(p_num_thread, 0));
    v_systemname itt_q_message_log.receiver%type := nvl(p_systemname, C_C_SYSTEMNAME);
    v_sg         varchar2(128);
  begin
    thread_chk_systemname_init(v_systemname);
    if v_num_thread > C_N_THREAD_MAX
    then
      thread_init(p_count_thread => C_N_THREAD_MAX, p_systemname => v_systemname);
      v_num_thread := mod(v_num_thread, C_N_THREAD_MAX);
      if v_num_thread = 0
      then
        v_num_thread := C_N_THREAD_MAX;
      end if;
    elsif v_num_thread > gt_thread_cnt(v_systemname)
    then
      thread_init(p_count_thread => v_num_thread, p_systemname => v_systemname);
    end if;
    if v_num_thread <= 0
       or v_num_thread > gt_thread_cnt(v_systemname)
    then
      v_sg := get_next_servicegroup(p_queue_num => gt_thread_queue_num(v_systemname)
                                   ,p_pref_servicegroup => C_С_THREAD_PREF_SERVICEGROUP
                                   ,io_last_thread => gt_thread_current(v_systemname)
                                   ,p_max_thread => gt_thread_cnt(v_systemname)
                                   ,p_systemname => v_systemname);
      if v_sg is null
      then
        null;
      end if;
    else
      gt_thread_current(v_systemname) := v_num_thread;
    end if;
  end;

  -- Возвращает servicegroup текущего потока исполнения пользователя
  function thread_get_servicegroup(p_systemname itt_q_message_log.receiver%type default C_C_SYSTEMNAME) return itt_q_message_log.servicegroup%type is
    v_systemname itt_q_message_log.receiver%type := nvl(p_systemname, C_C_SYSTEMNAME);
  begin
    thread_chk_systemname_init(v_systemname);
    return C_С_THREAD_PREF_SERVICEGROUP || '#' || gt_thread_current(v_systemname);
  end;

  -- Возвращает очередь для текущего потока исполнения пользователя
  function thread_get_queue_num(p_systemname itt_q_message_log.receiver%type default C_C_SYSTEMNAME) return itt_q_message_log.queue_num%type is
    v_systemname itt_q_message_log.receiver%type := nvl(p_systemname, C_C_SYSTEMNAME);
  begin
    thread_chk_systemname_init(v_systemname);
    return gt_thread_queue_num(v_systemname);
  end;

  -- Балансировщик потоков. Получение следующего servicegroup для префикса потоков
  function get_next_servicegroup(p_pref_servicegroup varchar2
                                ,io_last_thread      in out pls_integer
                                ,p_max_thread        pls_integer default null
                                ,p_systemname        itt_q_message_log.receiver%type default C_C_SYSTEMNAME
                                ,p_queue_num         itt_q_message_log.queue_num%type default null -- Номер очереди 
                                 ) return varchar2 is
    --v_last_thread pls_integer := abs(nvl(io_last_thread, gn_thread_current));
    v_systemname itt_q_message_log.receiver%type := nvl(p_systemname, C_C_SYSTEMNAME);
    v_max_thread pls_integer;
    v_res        pls_integer := io_last_thread + 1;
    v_dmin       date := date '0001-01-01';
    v_queue_num  itt_q_message_log.queue_num%type;
  begin
    thread_chk_systemname_init(v_systemname);
    v_queue_num  := nvl(p_queue_num, gt_thread_queue_num(v_systemname));
    v_max_thread := abs(nvl(p_max_thread, gt_thread_cnt(v_systemname)));
    if v_systemname = C_C_SYSTEMNAME
    then
      if v_queue_num = C_C_QUEUENUM_XX
      then
        select nn
          into v_res
          from (select tread.nn
                      ,nvl(wrk.cnt, 0) + nvl(task.cnt, 0) as run_count
                      ,greatest(nvl(wrk.starttime, v_dmin), nvl(task.lasttime, v_dmin)) as lasttime
                  from (select level nn from dual connect by level <= v_max_thread) tread
                  left join (select t.SERVICEGROUP
                                  ,count(*) cnt
                                  ,max(run_starttime) starttime
                              from itt_q_workerx t
                             where t.SERVICEGROUP like p_pref_servicegroup || '#%'
                             group by t.SERVICEGROUP) wrk
                    on (wrk.servicegroup = p_pref_servicegroup || '#' || tread.nn)
                  left join (select q.SERVICEGROUP
                                  ,count(*) cnt
                                  ,max(q.ENQDT) lasttime
                              from itv_q_taskXX q
                             where q.SERVICEGROUP like p_pref_servicegroup || '#%'
                             group by q.SERVICEGROUP) task
                    on (task.SERVICEGROUP = p_pref_servicegroup || '#' || tread.nn)
                 order by 2
                         ,3
                         ,tread.nn)
         fetch first rows only;
      else
        select nn
          into v_res
          from (select tread.nn
                      ,nvl(wrk.cnt, 0) + nvl(task.cnt, 0) as run_count
                      ,greatest(nvl(wrk.lasttime, v_dmin), nvl(task.lasttime, v_dmin)) as lasttime
                  from (select level nn from dual connect by level <= v_max_thread) tread
                  left join (select t.SERVICEGROUP
                                  ,sum(run_count) cnt
                                  ,max(run_lasttime) lasttime
                              from itt_q_worker t
                             where t.SERVICEGROUP like p_pref_servicegroup || '#%'
                             group by t.SERVICEGROUP) wrk
                    on (wrk.servicegroup = p_pref_servicegroup || '#' || tread.nn)
                  left join (select q.SERVICEGROUP
                                  ,count(*) cnt
                                  ,max(q.ENQDT) lasttime
                              from itv_q_in q
                             where q.SERVICEGROUP like p_pref_servicegroup || '#%'
                               and q.QMSGID not in (select qmsgid from itt_q_work_messages)
                             group by q.SERVICEGROUP) task
                    on (task.SERVICEGROUP = p_pref_servicegroup || '#' || tread.nn)
                 order by 2
                         ,3
                         ,tread.nn)
         fetch first rows only;
      end if;
    elsif v_res > p_max_thread
    then
      v_res := 1;
    end if;
    io_last_thread := v_res;
    return p_pref_servicegroup || '#' || v_res;
  end;

  --------------------------------------------------------------------------------
  -- Получение копии таблицы itt_q_corrsystem в коллекцию 
  procedure refresh_table_corrsystem is
    vc_sname itt_q_corrsystem.system_name%type;
    pragma autonomous_transaction;
  begin
    gt_corrsystem.delete;
    for rec in (select * from itt_q_corrsystem)
    loop
      vc_sname := upper(rec.system_name);
      gt_corrsystem(vc_sname) := rec;
    end loop;
  end;

  -------------------------------------------------------------------
  -- Генерим GUID с разделителями
  function get_sys_guid return varchar2 is
    vc_guid varchar2(36);
  begin
    vc_guid := sys_guid();
    vc_guid := substr(vc_guid, 1, 8) || '-' || substr(vc_guid, 9, 4) || '-' || substr(vc_guid, 13, 4) || '-' || substr(vc_guid, 17, 4) || '-' || substr(vc_guid, 21);
    return vc_guid;
  end;

  --функция, возвращает цифровое значение настройки
  function get_qset_number(p_qset_name itt_q_settings.qset_name%type) return itt_q_settings.value_number%type is
    res itt_q_settings.value_number%type;
  begin
    select s.value_number into res from itt_q_settings s where s.qset_name = p_qset_name;
    return res;
  exception
    when no_data_found then
      return null;
  end;

  --функция, возвращает текстовое значение настройки
  function get_qset_char(p_qset_name itt_q_settings.qset_name%type) return itt_q_settings.value_varchar%type is
    res itt_q_settings.value_varchar%type;
  begin
    select s.value_varchar into res from itt_q_settings s where s.qset_name = p_qset_name;
    return res;
  exception
    when no_data_found then
      return null;
  end;

  -- Процедура сохраняет настройку типа ДАТА
  procedure set_qset_data(p_qset_name itt_q_settings.qset_name%type
                         ,p_date      date) is
    pragma autonomous_transaction;
  begin
    update itt_q_settings s set s.value_date = p_date where s.qset_name = p_qset_name;
    if sql%rowcount = 0
    then
      insert into itt_q_settings
        (qset_name
        ,value_date)
      values
        (p_qset_name
        ,p_date);
    end if;
    commit; -- AUTONOMOUS
  end;

  --функция, возвращает значение настройки типа ДАТА
  function get_qset_data(p_qset_name itt_q_settings.qset_name%type) return itt_q_settings.value_date%type is
    res itt_q_settings.value_date%type;
  begin
    select s.value_date into res from itt_q_settings s where s.qset_name = p_qset_name;
    return res;
  exception
    when no_data_found then
      return null;
  end;

  -- Получение типа очереди I/O ; null - ошибка (ITQ_IN_01)
  function get_queuetype(p_queuename itt_q_message_log.queuename%type) return itt_q_message_log.queuetype%type deterministic as
    v_queuename varchar2(32) := upper(p_queuename);
  begin
    case
      when substr(v_queuename, 1, length(C_C_QUEUE_IN_PREFIX)) = C_C_QUEUE_IN_PREFIX then
        return C_C_QUEUE_TYPE_IN;
      when substr(v_queuename, 1, length(C_C_QUEUE_OUT_PREFIX)) = C_C_QUEUE_OUT_PREFIX then
        return C_C_QUEUE_TYPE_OUT;
      else
        return C_C_QUEUE_TYPE_NO;
    end case;
  end;

  -- Обновление списка инсталированных очередей.
  procedure refresh_table_queue_num as
  begin
    select queue_num
      bulk collect
      into t_queue_num
      from (select distinct vi.queue_num
              from (select it_q_message.get_queue_num(v.view_name) queue_num from user_views v where v.view_name like C_C_QVIEW_IN_PREFIX || '__') vi
              join (select it_q_message.get_queue_num(v.view_name) queue_num from user_views v where v.view_name like C_C_QVIEW_OUT_PREFIX || '__') vo
                on vi.queue_num = vo.queue_num
              join (select it_q_message.get_queue_num(v.view_name) queue_num from user_views v where v.view_name like C_C_QVIEW_TASK_PREFIX || '__') vt
                on vi.queue_num = vt.queue_num)
     where queue_num != C_C_QUEUENUM_XX -- Выключение табличной очереди
     order by decode(queue_num, C_C_QUEUENUM_XX, 1, 0)
             ,queue_num;
    if t_queue_num.COUNT = 0
    then
      raise_application_error(-20000, 'Нет доступных очередей обмена сообщениями');
    end if;
    t_queue_num_key.delete;
    for n in 1 .. t_queue_num.COUNT
    loop
      t_queue_num_key(t_queue_num(n)) := n;
    end loop;
    select distinct column_value bulk collect into t_queue_num_parallel from table(it_q_message.select_queue_num) where regexp_like(column_value, '^[0-9]+$');
  end;

  -- Табличная функция получения списка инсталированных очередей
  function select_queue_num return tt_queue_num
    pipelined as
  begin
    for i in 1 .. t_queue_num.COUNT
    loop
      pipe row(t_queue_num(i));
    end loop;
  end;

  -- Получение номера очереди из имени обекта (ITQ_IN_01,ITV_Q_IN01 .....)
  function get_queue_num(p_objname varchar2) return itt_q_message_log.queue_num%type deterministic as
  begin
    return substr(p_objname, -2, 2);
  end;

  -- Получение парвметра кореспонлирующей системы 
  function get_corrsystem_param(p_system_name varchar2
                               ,p_param       varchar2) return varchar2 deterministic as
    v_param_value varchar2(4000);
    v_xml_param   varchar2(30000);
  begin
    if gt_corrsystem.exists(upper(p_system_name))
       and gt_corrsystem(upper(p_system_name)).system_param is not null
    then
      v_xml_param := '<XML ' || gt_corrsystem(upper(p_system_name)).system_param || '></XML>';
      begin
        select EXTRACTVALUE(xmltype(v_xml_param), '/XML/@' || p_param) into v_param_value from dual;
      exception
        when others then
          v_param_value := null;
      end;
    end if;
    return v_param_value;
  end;

  -- Получение полного имени  очереди 
  function get_full_queue_name(p_queue_name itt_q_message_log.queuename%type) return varchar2 as
  begin
    return gс_userenv_current_schema || '.' || p_queue_name;
  end;

  -- Имя пользователя 
  function get_q_user return itt_q_message_log.senderuser%type as
    v_current_user varchar2(128);
    v_current_oper varchar2(30) := case
                                     when gс_RsbSessionData_oper is not null then
                                      RsbSessionData.oper
                                   end;
  begin
    gс_RsbSessionData_oper := to_number(v_current_oper);
    if gc_q_user is null
       or (v_current_oper is not null and substr(gc_q_user, length(gc_q_user) - length(v_current_oper)) != '>' || v_current_oper)
    then
      v_current_user := user; --sys_context('USERENV', 'CURRENT_USER');
      if sys_context('USERENV', 'OS_USER') = 'oracle'
      then
        gc_q_user := 'oracle';
      else
        gc_q_user := userenv('TERMINAL') || '>' || sys_context('USERENV', 'OS_USER') || case
                       when v_current_user != sys_context('USERENV', 'CURRENT_USER') then
                        '>' || v_current_user
                       when v_current_oper is not null then
                        '>' || v_current_oper
                     end;
      end if;
    end if;
    return case when gc_q_user != 'oracle' then gc_q_user end;
  end;

  -- Выбор номера очереди для отправки сообщения (пока все последовательно из общих)
  function find_queue_num(p_message it_q_message_t default null) return itt_q_message_log.queue_num%type as
  begin
    if p_message is null
       or p_message.queue_num is null
    then
      if t_queue_num_parallel.COUNT = 0
      then
        return t_queue_num(1); -- Нет очередей для распараллеливания берем первую доступную
      else
        gn_find_queue_num_last := gn_find_queue_num_last + 1;
        if gn_find_queue_num_last > t_queue_num_parallel.COUNT
        then
          gn_find_queue_num_last := 1;
        end if;
        return t_queue_num_parallel(gn_find_queue_num_last);
      end if;
    else
      return p_message.queue_num;
    end if;
  end;

  --  Получение последовательно номера очереди из всего списка
  function next_queue_num return itt_q_message_log.queue_num%type as
  begin
    gn_next_queue_num_last := gn_next_queue_num_last + 1;
    if gn_next_queue_num_last > t_queue_num.COUNT
    then
      gn_next_queue_num_last := 1;
    end if;
    return t_queue_num(gn_next_queue_num_last);
  end;

  -- Возвращает временную метку создания сообщения в очереди  
  function get_enqdt(p_qmsgid    raw
                    ,p_queuename itt_q_message_log.queuename%type) return timestamp as
    v_ret timestamp;
    v_vw  varchar2(32);
  begin
    case get_queuetype(p_queuename)
      when C_C_QUEUE_TYPE_IN then
        v_vw := C_C_QVIEW_IN_PREFIX || get_queue_num(p_queuename);
      when C_C_QUEUE_TYPE_OUT then
        v_vw := C_C_QVIEW_OUT_PREFIX || get_queue_num(p_queuename);
      else
        raise_application_error(-20000, 'Ошибка определения типа очереди ' || p_queuename);
    end case;
    begin
      execute immediate 'select q.enqdt from ' || v_vw || ' q where q.qmsgid = :qmsgid and rownum < 2'
        into v_ret
        using p_qmsgid;
    exception
      when no_data_found then
        v_ret := null;
    end;
    return v_ret;
  end;

  -- Возвращает временную метку создания сообщения в очереди при чтении 
  function get_enqdt_deq(p_qmsgid    raw
                        ,p_queuename itt_q_message_log.queuename%type) return timestamp as
    pragma autonomous_transaction;
  begin
    return get_enqdt(p_qmsgid, p_queuename);
  end;

  -- Возвращает временную метку создания сообщения в очереди при записи 
  function get_enqdt_enq(p_qmsgid    raw
                        ,p_queuename itt_q_message_log.queuename%type) return timestamp as
  begin
    return get_enqdt(p_qmsgid, p_queuename);
  end;

  --- Количество заданий во входящей очереди
  function get_count_task(p_queue_num itt_q_message_log.queue_num%type default null -- null во всех очередяж
                         ,p_max_count number default null -- граница подстчета 
                         ,p_not_qXX   number default 0 -- 1-   без табличной очереди 
                          ) return integer as
    v_ret       integer;
    v_sel       varchar2(32600) := 'select sum(cnt) from (';
    v_max_count varchar2(100) := case
                                   when p_max_count is null then
                                    'rownum'
                                   else
                                    to_char(p_max_count + 1)
                                 end;
  begin
    if p_queue_num is null
    then
      for q in (select rownum       as rn
                      ,column_value queue_num
                  from table(it_q_message.select_queue_num)
                 where nvl(p_not_qXX, 0) = 0
                    or column_value != it_q_message.C_C_QUEUENUM_XX) -- По всем очередям
      loop
        if q.rn > 1
        then
          v_sel := v_sel || chr(10) || ' union all ';
        end if;
        v_sel := v_sel || chr(10) || ' select count(*) as cnt from ' || C_C_QVIEW_TASK_PREFIX || q.queue_num || ' where rownum <=' || v_max_count;
      end loop;
      v_sel := v_sel || ')';
    else
      if p_queue_num != upper(p_queue_num)
         or it_q_message.check_queue_num(p_queue_num) != 1
      then
        raise_application_error(-20000, 'Очередь ID ' || p_queue_num || ' не инсталлирована в системе');
      end if;
      v_sel := 'select count(*) from ' || C_C_QVIEW_TASK_PREFIX || p_queue_num || ' where rownum <=' || v_max_count;
    end if;
    execute immediate v_sel
      into v_ret;
    return v_ret;
  end;

  --- Количество сообщений во входящей очереди
  function get_count_in_msg(p_queue_num itt_q_message_log.queue_num%type default null -- null во всех очередяж
                           ,p_max_count number default null -- граница подстчета 
                            ) return integer as
    v_ret       integer;
    v_sel       varchar2(32600);
    v_max_count varchar2(100) := case
                                   when p_max_count is null then
                                    'rownum'
                                   else
                                    to_char(p_max_count + 1)
                                 end;
    v_sel_where varchar2(200) := ' where state = 0  and (message_type = ''' || C_C_MSG_TYPE_R || ''' or (message_type = ''' || C_C_MSG_TYPE_A || ''' and delivery_type = ''' ||
                                 C_C_MSG_DELIVERY_A || ''')) and rownum <=' || v_max_count;
  begin
    if p_queue_num is not null
       and (p_queue_num != upper(p_queue_num) or it_q_message.check_queue_num(p_queue_num) != 1)
    then
      raise_application_error(-20000, 'Очередь ID ' || p_queue_num || ' не инсталлирована в системе');
    end if;
    v_sel := 'select count(*) from ' || C_C_QVIEW_IN_PREFIX || p_queue_num || v_sel_where;
    execute immediate v_sel
      into v_ret;
    return v_ret;
  end;

  -- Возвращает 1 в случае правильного queue_num
  function check_queue_num(p_queue_num itt_q_message_log.queue_num%type default null) return number as
  begin
    return case when t_queue_num_key.exists(p_queue_num) then 1 else 0 end;
  end;

  -- Возвращает кол-во инсталлированных очередей
  function get_count_queue return integer as
  begin
    return t_queue_num_key.count;
  end;

  -- Возвращает время ожидания синхронного сообщения(сек)
  function get_qwait(p_timeout number) return integer as
    v_wait          number;
    vn_qset_timeout integer;
  begin
    if nvl(p_timeout, 0) <= 0
    then
      vn_qset_timeout := nvl(get_qset_number(p_qset_name => 'QMESSAGE_TIMEOUT'), 20);
      v_wait := case
                  when vn_qset_timeout <= 0 then
                   20
                  else
                   vn_qset_timeout
                end;
    else
      v_wait := p_timeout;
    end if;
    return v_wait;
  end;

  -- Формируем correlation сообщения
  function get_correlation(p_message_type  itt_q_message_log.message_type%type
                          ,p_delivery_type itt_q_message_log.delivery_type%type
                          ,p_priority      itt_q_message_log.priority%type
                          ,p_msgid         itt_q_message_log.msgid%type
                          ,p_corrmsgid     itt_q_message_log.corrmsgid%type default null) return varchar2 is
    vc_cor varchar2(128);
  begin
    if p_message_type = C_C_MSG_TYPE_R
    then
      vc_cor := p_message_type || p_delivery_type || p_priority || p_msgid;
    elsif p_message_type = C_C_MSG_TYPE_A
          and p_delivery_type = C_C_MSG_DELIVERY_A
    then
      vc_cor := C_C_MSG_TYPE_R || p_delivery_type || p_priority || p_corrmsgid;
    else
      vc_cor := p_message_type || p_delivery_type || p_priority || p_corrmsgid;
    end if;
    return vc_cor;
  end;

  -- Возвращает 1 в случае правильното correlation или 0 и сообщение об ошибке в OUT
  function check_correlation_outerr(p_correlation   varchar2
                                   ,p_message_type  itt_q_message_log.message_type%type
                                   ,p_delivery_type itt_q_message_log.delivery_type%type
                                   ,p_priority      itt_q_message_log.priority%type
                                   ,p_msgid         itt_q_message_log.msgid%type
                                   ,p_corrmsgid     itt_q_message_log.corrmsgid%type
                                   ,p_qmcheck       integer default 0 -- Предпроверка в QMANAGERе
                                   ,o_errmess       out varchar2) return integer is
  begin
    if p_qmcheck is not null
       and p_qmcheck = 1
       and p_correlation = GC_CORR_COMMAND -- Управляющая команда
    then
      return 1;
    elsif p_msgid is null
    then
      o_errmess := 'Не указан ID сообщения';
      return 0;
    end if;
    if p_message_type is null
       or p_message_type not in (C_C_MSG_TYPE_R, C_C_MSG_TYPE_A)
    then
      o_errmess := 'Тип сообщения (Message_Type) может бать только R или A получен [' || p_message_type || ']';
      return 0;
    end if;
    if p_delivery_type is null
       or p_delivery_type not in (C_C_MSG_DELIVERY_A, C_C_MSG_DELIVERY_S)
    then
      o_errmess := 'Тип обработки сообщения (Delivery_Type) может бать только S или A получен "' || p_delivery_type || '"';
      return 0;
    end if;
    if p_priority is null
       or p_priority not in ('F', 'N')
    then
      o_errmess := 'Очередность сообщения (Priority) может бать только F или N получен ' || p_priority;
      return 0;
    end if;
    if p_message_type = C_C_MSG_TYPE_A
       and p_corrmsgid is null
    then
      o_errmess := 'Для ответного сообщения (Message_Type = A) должен быть указан CorrMsgId';
      return 0;
    end if;
    if p_correlation !=
       get_correlation(p_message_type => p_message_type, p_delivery_type => p_delivery_type, p_priority => p_priority, p_msgid => p_msgid, p_corrmsgid => p_corrmsgid)
    then
      o_errmess := 'Ошибка Сorrelation сообщения ' || chr(13) || 'Получено ' || p_correlation || chr(13) || 'Ожидалось' ||
                   get_correlation(p_message_type => p_message_type, p_delivery_type => p_delivery_type, p_priority => p_priority, p_msgid => p_msgid, p_corrmsgid => p_corrmsgid);
      return 0;
    end if;
    return 1;
  end;

  -- Функция проверки возможности отката сообщения 1 - если ОК , 0- пропуск , < 0 - ошибка 
  function check_rollback_message(p_msgid    itt_q_message_log.msgid%type
                                 ,p_withlock integer default null) return integer deterministic is
    v_errmess varchar2(2000);
  begin
    return check_rollback_message_out(p_msgid => p_msgid, p_withlock => p_withlock, o_errmess => v_errmess);
  end;

  function check_rollback_message_out(p_msgid    itt_q_message_log.msgid%type
                                     ,p_withlock integer default null -- > 1 c блокировкой
                                     ,o_errmess  out varchar2) return integer is
    row_locked exception;
    pragma exception_init(row_locked, -54);
    vr_log itt_q_message_log%rowtype;
    -- v_msgcode_answ     itt_q_message_log.msgcode%type;
    -- v_msgtext_answ     itt_q_message_log.msgtext%type;
    v_withlock         boolean := (nvl(p_withlock, 0) > 0);
    v_service_rollback itt_q_service.service_rollback%type;
    v_tmp              integer;
  begin
    if v_withlock
    then
      begin
        vr_log := messlog_get_withlock(p_msgid => p_msgid, p_queuetype => C_C_QUEUE_TYPE_IN);
      exception
        when row_locked then
          o_errmess := 'MsgID ' || p_msgid || ' занят другим процессом в системе ';
          return - 1;
      end;
    else
      vr_log := messlog_get(p_msgid => p_msgid, p_queuetype => C_C_QUEUE_TYPE_IN);
    end if;
    if vr_log.log_id is null
    then
      o_errmess := 'MsgID ' || p_msgid || ' не найдено во входящей очереди !';
      return - 1;
    elsif not (vr_log.message_type = C_C_MSG_TYPE_R or (vr_log.message_type = C_C_MSG_TYPE_A and vr_log.delivery_type = C_C_MSG_DELIVERY_A))
    then
      o_errmess := 'MsgID ' || p_msgid || ' должно быть запросом или асинхронным ответом ';
      return - 1;
    elsif vr_log.status in (C_STATUS_ROLLBACK)
    then
      o_errmess := 'MsgID ' || p_msgid || ' уже  отменено !';
      return 0;
    end if;
    select count(*) into v_tmp from table(it_q_message.select_status('DONE')) where column_value = vr_log.status;
    if v_tmp = 0 -- не (C_STATUS_DONE, C_STATUS_ANSWER)
    then
      o_errmess := 'MsgID ' || p_msgid || 'в статусе ' || vr_log.status || ' не имеет возможность отката !';
      return 0;
    end if;
    select max(s.service_rollback)
      into v_service_rollback
      from itt_q_service s
     where s.message_type = vr_log.message_type
       and upper(trim(s.servicename)) = upper(trim(vr_log.servicename));
    if v_service_rollback is null
    then
      o_errmess := 'MsgID ' || p_msgid || ' для обработчика сообщения (ServiceName) "' || vr_log.servicename || '" не определена поцедура отката';
      return - 1;
    end if;
    return 1;
  end;

  -- Возвращает 1 в случае правильното correlation
  function check_correlation(p_correlation   varchar2
                            ,p_message_type  itt_q_message_log.message_type%type
                            ,p_delivery_type itt_q_message_log.delivery_type%type
                            ,p_priority      itt_q_message_log.priority%type
                            ,p_msgid         itt_q_message_log.msgid%type
                            ,p_corrmsgid     itt_q_message_log.corrmsgid%type
                            ,p_qmcheck       integer default 0 -- Предпроверка в QMANAGERе
                             ) return integer is
    v_c_msg varchar2(2000);
  begin
    return check_correlation_outerr(p_correlation => p_correlation
                                   ,p_message_type => p_message_type
                                   ,p_delivery_type => p_delivery_type
                                   ,p_priority => p_priority
                                   ,p_msgid => p_msgid
                                   ,p_corrmsgid => p_corrmsgid
                                   ,p_qmcheck => p_qmcheck
                                   ,o_errmess => v_c_msg);
  end;

  -- Возвращает текст ошибки из sqlerrm
  function get_errtxt(p_sqlerrm varchar2) return varchar2 is
    v_errtxt   varchar2(2000);
    v_sqlerrm  varchar2(32600) := trim(translate(p_sqlerrm, 'A' || chr(10) || chr(13), 'A'));
    v_last_ora integer;
  begin
    while v_sqlerrm like 'ORA-_____:%'
    loop
      v_sqlerrm := trim(substr(v_sqlerrm, 11));
      while v_sqlerrm like '%:PLS-_____:%'
      loop
        v_sqlerrm := trim(substr(v_sqlerrm, instr(v_sqlerrm, ':PLS-') + 12));
      end loop;
    end loop;
    v_last_ora := instr(v_sqlerrm, 'ORA-');
    if v_last_ora > 0
    then
      v_errtxt := trim(substr(v_sqlerrm, 1, instr(v_sqlerrm, 'ORA-') - 1));
    else
      v_errtxt := trim(v_sqlerrm);
    end if;
    v_errtxt := nvl(trim(v_errtxt), trim(translate(p_sqlerrm, 'A' || chr(10) || chr(13), 'A')));
    return v_errtxt;
  end;

  -- Формирование строкм комментария 
  function get_comment_add(p_msgcode     integer
                          ,p_add_comment varchar2
                          ,p_comenttxt   varchar2 default null
                          ,p_len         integer default 4000) return varchar2 as
  begin
    return substr(case when nvl(p_msgcode, 0) != 0 then 'MSGCODE#' || p_msgcode || ':' end || p_add_comment || case when p_add_comment is not null or nvl(p_msgcode, 0) != 0 then
                  chr(10) end || p_comenttxt
                 ,1
                 ,p_len);
  end;

  -- процедура добавления записи в лог мусорного сообщения  
  procedure messlog_insert_trash(p_message_type  itt_q_message_log.message_type%type
                                ,p_delivery_type itt_q_message_log.delivery_type%type
                                ,p_Priority      itt_q_message_log.Priority%type default C_C_MSG_PRIORITY_N
                                ,p_CORRmsgid     itt_q_message_log.CORRmsgid%type default null
                                ,p_ServiceName   itt_q_message_log.ServiceName%type default null
                                ,p_Receiver      itt_q_message_log.Receiver%type default null
                                ,p_ServiceGroup  itt_q_message_log.ServiceGroup%type default null
                                ,p_BTUID         itt_q_message_log.BTUID%type default null
                                ,p_Sender        itt_q_message_log.sender%type default C_C_SYSTEMNAME
                                ,p_SenderUser    itt_q_message_log.senderuser%type default null
                                ,p_MSGCode       itt_q_message_log.MSGCode%type default 0
                                ,p_MSGText       itt_q_message_log.MSGText%type default null
                                ,p_MESSBODY      itt_q_message_log.MESSBODY%type default null
                                ,p_MessMETA      xmltype default null
                                ,p_queue_num     itt_q_message_log.queue_num%type default null
                                ,p_RequestDT     itt_q_message_log.requestdt%type default null
                                ,p_ESBDT         itt_q_message_log.esbdt%type default null
                                ,p_correlation   varchar2 default null
                                ,p_queuename     varchar2 default 'NOQUEUE'
                                ,p_enqdt         timestamp default systimestamp
                                ,p_comment       varchar2 default null
                                ,p_workuser      varchar2 default null
                                ,io_msgid        in out itt_q_message_log.msgid%type
                                ,o_logid         out integer) is
    v_message  it_q_message_t;
    v_messbody clob;
    v_messmeta xmltype;
  begin
    v_message       := new_message(p_message_type => p_message_type
                                  ,p_delivery_type => p_delivery_type
                                  ,p_Priority => p_Priority
                                  ,p_CORRmsgid => p_CORRmsgid
                                  ,p_ServiceName => p_ServiceName
                                  ,p_Receiver => p_Receiver
                                  ,p_ServiceGroup => p_ServiceGroup
                                  ,p_BTUID => p_BTUID
                                  ,p_Sender => p_Sender
                                  ,p_SenderUser => p_SenderUser
                                  ,p_MSGCode => p_MSGCode
                                  ,p_MSGText => p_MSGText
                                  ,p_MESSBODY => p_MESSBODY
                                  ,p_MessMETA => p_MessMETA
                                  ,p_queue_num => p_queue_num
                                  ,p_RequestDT => p_RequestDT
                                  ,p_ESBDT => p_ESBDT
                                  ,p_check => false);
    v_message.msgid := nvl(io_msgid, v_message.msgid);
    io_msgid        := v_message.msgid;
    messlog_insert_out(p_message => v_message
                      ,p_correlation => p_correlation
                      ,p_queuename => p_queuename
                      ,p_status => C_STATUS_TRASH
                      ,p_enqdt => p_enqdt
                      ,p_comment => p_comment
                      ,p_workuser => p_workuser
                      ,p_not_out_xml => 1
                      ,o_logid => o_logid
                      ,o_messbody => v_messbody
                      ,o_messmeta => v_messmeta);
  end;

  -- процедура добавления записи в лог с возвратом данных из лога
  procedure messlog_insert_out(p_message     it_q_message_t
                              ,p_correlation varchar2
                              ,p_queuename   varchar2
                              ,p_status      itt_q_message_log.status%type
                              ,p_qmsgid      itt_q_message_log.msgid%type default null
                              ,p_enqdt       timestamp default systimestamp
                              ,p_startdt     timestamp default null
                              ,p_commanddt   timestamp default null
                              ,p_workdt      timestamp default null
                              ,p_workername  itt_q_message_log.workername%type default null
                              ,p_comment     varchar2 default null
                              ,p_workuser    varchar2 default null
                              ,p_logid       number default null
                              ,p_not_out_xml integer default 0 -- != 0 не возвращать xml
                              ,o_logid       out integer
                              ,o_messbody    out clob
                              ,o_messmeta    out xmltype) is
    v_messmeta     clob;
    v_systimestamp timestamp := systimestamp;
  begin
    insert into itt_q_message_log
      (log_id
      ,queuetype
      ,msgid
      ,message_type
      ,delivery_type
      ,priority
      ,correlation
      ,corrmsgid
      ,servicename
      ,sender
      ,receiver
      ,requestdt
      ,esbdt
      ,senderuser
      ,servicegroup
      ,btuid
      ,uteg
      ,msgcode
      ,msgtext
      ,messbody
      ,messmeta
      ,queue_num
      ,status
      ,statusdt
      ,queuename
      ,enqdt
      ,startdt
      ,commanddt
      ,workdt
      ,workername
      ,commenttxt
      ,workuser)
    values
      (nvl(p_logid, its_q_log.nextval)
      ,get_queuetype(p_queuename)
      ,nvl(p_message.msgid, p_qmsgid)
      ,p_message.message_type
      ,p_message.delivery_type
      ,p_message.priority
      ,p_correlation
      ,p_message.corrmsgid
      ,p_message.servicename
      ,p_message.sender
      ,p_message.receiver
      ,p_message.requestdt
      ,p_message.esbdt
      ,p_message.senderuser
      ,p_message.servicegroup
      ,p_message.BTUID
      ,p_message.uteg
      ,p_message.MSGCode
      ,p_message.MSGText
      ,p_message.MESSBODY
      ,p_message.MESSMeta.getclobval()
      ,p_message.queue_num
      ,p_status
      ,v_systimestamp
      ,p_queuename
      ,p_enqdt
      ,p_startdt
      ,p_commanddt
      ,nvl(p_workdt, case when p_status = C_STATUS_WORK then v_systimestamp else null end)
      ,p_workername
      ,substr(p_comment, 1, 2000)
      ,p_workuser)
    returning log_id, messbody, messmeta into o_logid, o_messbody, v_messmeta;
    if nvl(p_not_out_xml, 0) = 0
    then
      o_messmeta := it_xml.Clob_to_xml(v_messmeta, 'Ошибка формата XML itt_q_message_log.messmeta msgid=' || nvl(p_message.msgid, p_qmsgid));
    end if;
  end;

  -- процедура добавления записи в лог 
  procedure messlog_insert(p_message     it_q_message_t
                          ,p_correlation varchar2
                          ,p_queuename   varchar2
                          ,p_status      itt_q_message_log.status%type
                          ,p_qmsgid      itt_q_message_log.msgid%type default null
                          ,p_enqdt       timestamp default systimestamp
                          ,p_startdt     timestamp default null
                          ,p_commanddt   timestamp default null
                          ,p_workdt      timestamp default null
                          ,p_workername  itt_q_message_log.workername%type default null
                          ,p_comment     varchar2 default null
                          ,p_workuser    varchar2 default null
                          ,p_logid       number default null) as
    v_logid    integer;
    v_messbody clob;
    v_messmeta xmltype;
  begin
    messlog_insert_out(p_message => p_message
                      ,p_correlation => p_correlation
                      ,p_queuename => p_queuename
                      ,p_status => p_status
                      ,p_enqdt => p_enqdt
                      ,p_qmsgid => p_qmsgid
                      ,p_startdt => p_startdt
                      ,p_commanddt => p_commanddt
                      ,p_workdt => p_workdt
                      ,p_workername => p_workername
                      ,p_comment => p_comment
                      ,p_workuser => p_workuser
                      ,p_logid => p_logid
                      ,p_not_out_xml => 1
                      ,o_logid => v_logid
                      ,o_messbody => v_messbody
                      ,o_messmeta => v_messmeta);
  end;

  -- процедура апдейта статуса сообщения в логе
  procedure messlog_upd_status(p_msgid         itt_q_message_log.msgid%type
                              ,p_delivery_type itt_q_message_log.delivery_type%type
                              ,p_queuetype     itt_q_message_log.queuetype%type
                              ,p_status        itt_q_message_log.status%type
                              ,p_workername    itt_q_message_log.workername%type default null
                              ,p_comment       varchar2 default null) is
    v_systimestamp timestamp := systimestamp;
  begin
    -- Если переводим в WORK, то заполним WorkDT и WorkerName
    if p_status = C_STATUS_WORK
    then
      update itt_q_message_log t
         set t.status     = p_status
            ,t.statusdt   = v_systimestamp
            ,t.workdt     = v_systimestamp
            ,t.workername = nvl(p_workername, t.workername)
            ,t.commenttxt = substr(p_comment || nvl2(p_comment, chr(10), null) || t.commenttxt, 1, 4000)
       where t.msgid = p_msgid
         and t.delivery_type = p_delivery_type
         and t.queuetype = p_queuetype
         and t.status != C_STATUS_TRASH;
    else
      update itt_q_message_log t
         set t.status     = p_status
            ,t.statusdt   = v_systimestamp
            ,t.commenttxt = substr(p_comment || nvl2(p_comment, chr(10), null) || t.commenttxt, 1, 4000)
       where t.msgid = p_msgid
         and t.delivery_type = p_delivery_type
         and t.queuetype = p_queuetype
         and t.status != C_STATUS_TRASH;
    end if;
  end;

  -- процедура апдейта информации о вычитки сообщения в логе
  procedure messlog_upd_deq(p_msgid     itt_q_message_log.msgid%type
                           ,p_queuetype itt_q_message_log.queuetype%type
                           ,p_errno     integer default 0
                           ,p_comment   varchar2 default null) is
    v_errno integer := nvl(p_errno, 0);
  begin
    update itt_q_message_log l
       set l.status = case
                        when v_errno = 0
                             and l.status = it_q_message.C_STATUS_SENDQUERY then
                         it_q_message.C_STATUS_DELIVEREDQUERY
                        when v_errno = 0 then
                         it_q_message.C_STATUS_DELIVERED
                        else
                         it_q_message.C_STATUS_ERRSEND
                      end
          ,l.statusdt   = systimestamp
          ,l.workdt     = systimestamp
          ,l.workuser   = get_q_user()
          ,l.commenttxt = substr(case
                                   when p_errno < 0 then
                                    '-> ' || it_q_message.C_STATUS_TRASH || ': '
                                 end || p_comment || nvl2(p_comment, chr(10), null) || l.commenttxt
                                ,1
                                ,4000)
     where l.msgid = p_msgid
       and l.queuetype = p_queuetype
       and l.status not in (C_STATUS_TIMEOUT, C_STATUS_TRASH);
  end;

  -- функция возвращает 1 если есть сообжение в LOGE
  function messlog_is(p_msgid        itt_q_message_log.msgid%type
                     ,p_queuetype    itt_q_message_log.queuetype%type
                     ,p_message_type itt_q_message_log.message_type%type default null
                     ,p_status       itt_q_message_log.status%type default null) return integer is
    vn_res integer;
  begin
    if p_message_type is null
       and p_status is null
    then
      select 1
        into vn_res
        from itt_q_message_log l
       where l.msgid = p_msgid
         and l.queuetype = p_queuetype
         and rownum < 2;
    elsif p_message_type is not null
          and p_status is not null
    then
      select 1
        into vn_res
        from itt_q_message_log l
       where l.msgid = p_msgid
         and l.queuetype = p_queuetype
         and l.message_type = p_message_type
         and l.status = p_status
         and rownum < 2;
    elsif p_message_type is not null
          and p_status is null
    then
      select 1
        into vn_res
        from itt_q_message_log l
       where l.msgid = p_msgid
         and l.queuetype = p_queuetype
         and l.message_type = p_message_type
         and l.status != C_STATUS_TRASH
         and rownum < 2;
    elsif p_message_type is null
          and p_status is not null
    then
      select 1
        into vn_res
        from itt_q_message_log l
       where l.msgid = p_msgid
         and l.queuetype = p_queuetype
         and l.status = p_status
         and rownum < 2;
    else
      raise_application_error(-20000, 'Ошибка параметров функции messlog_is ! ');
    end if;
    return 1;
  exception
    when no_data_found then
      return 0;
    when too_many_rows then
      return 1;
  end;

  -- функция возвращает сообщение из лога
  function messlog_get(p_logid     itt_q_message_log.log_id%type default null
                      ,p_msgid     itt_q_message_log.msgid%type default null
                      ,p_queuetype itt_q_message_log.queuetype%type default C_C_QUEUE_TYPE_IN) return itt_q_message_log%rowtype is
    vr_log      itt_q_message_log%rowtype;
    v_queuetype itt_q_message_log.queuetype%type := nvl(p_queuetype, C_C_QUEUE_TYPE_IN);
  begin
    if p_logid is not null
    then
      select * into vr_log from itt_q_message_log where log_id = p_logid;
    elsif p_msgid is not null
          and v_queuetype is not null
    then
      select *
        into vr_log
        from itt_q_message_log
       where msgid = p_msgid
         and queuetype = v_queuetype
         and status != C_STATUS_TRASH;
    elsif p_msgid is null
    then
      raise_application_error(-20000, 'Не указан идентификатор сообщения для процедуры it_q_message.messlog_get !');
    else
      raise_application_error(-20000, 'Ошибка в параметрах процедуры it_q_message.messlog_get !');
    end if;
    return vr_log;
  exception
    when no_data_found then
      return vr_log;
  end;

  -- функция возвращает сообщение из лога с блокировкой
  function messlog_get_withlock(p_logid     itt_q_message_log.log_id%type default null
                               ,p_msgid     itt_q_message_log.msgid%type default null
                               ,p_queuetype itt_q_message_log.queuetype%type default C_C_QUEUE_TYPE_IN) return itt_q_message_log%rowtype is
    row_locked exception;
    pragma exception_init(row_locked, -54);
    vr_log      itt_q_message_log%rowtype;
    v_queuetype itt_q_message_log.queuetype%type := nvl(p_queuetype, C_C_QUEUE_TYPE_IN);
  begin
    if p_logid is not null
    then
      select * into vr_log from itt_q_message_log where log_id = p_logid for update nowait;
    elsif p_msgid is not null
          and v_queuetype is not null
    then
      select *
        into vr_log
        from itt_q_message_log
       where msgid = p_msgid
         and queuetype = v_queuetype
         and status != C_STATUS_TRASH
         for update nowait;
    else
      raise_application_error(-20000, 'Ошибка в параметрах процедуры it_q_message.messlog_get !');
    end if;
    return vr_log;
  exception
    when no_data_found
         or row_locked then
      return vr_log;
  end;

  -- Функция проверки атрибутов сообщения 1 - если ОК; 0- EROR ;<0 - TRASH
  function check_atr_message(p_correlation varchar2
                            ,p_queuename   itt_q_message_log.queuename%type
                            ,p_r_msg       it_q_message_t
                            ,o_errcode     out integer -- код ошибки если > 0 то в ERROR если < 0 то в TRASH
                            ,o_errmess     out varchar2 -- Ошибка
                             ) return integer is
    v_queuetype itt_q_message_log.queuetype%type := get_queuetype(p_queuename => p_queuename);
    v_queue_num itt_q_message_log.queue_num%type := get_queue_num(p_objname => p_queuename);
    vr_log      itt_q_message_log%rowtype;
    v_res       pls_integer := 1;
  begin
    o_errcode := 0;
    if v_queuetype = C_C_QUEUE_TYPE_NO
    then
      o_errmess := 'Название очереди сообщений ' || p_queuename || ' не соответствует формату';
      o_errcode := -1;
      v_res     := 0;
    elsif p_r_msg.uteg is null
          or p_r_msg.uteg != 'Ю'
    then
      o_errmess := 'Ошибка кодировки сообщения !';
      o_errcode := 1;
      v_res     := 0;
    elsif check_correlation_outerr(p_correlation => p_correlation
                                  ,p_message_type => p_r_msg.message_type
                                  ,p_delivery_type => p_r_msg.delivery_type
                                  ,p_priority => p_r_msg.priority
                                  ,p_msgid => p_r_msg.msgid
                                  ,p_corrmsgid => p_r_msg.CORRmsgid
                                  ,o_errmess => o_errmess) != 1
    then
      o_errcode := -1;
      v_res     := 0;
    elsif messlog_is(p_msgid => p_r_msg.msgid, p_queuetype => v_queuetype) = 1
    then
      o_errmess := 'Сообщение MsgID ' || p_r_msg.msgid || ' из очереди ' || p_queuename || ' дублируется !';
      o_errcode := -1;
      v_res     := 0;
    elsif p_r_msg.message_type = C_C_MSG_TYPE_A
          and p_r_msg.corrmsgid is not null
          and v_queuetype = C_C_QUEUE_TYPE_IN
          and messlog_is(p_msgid => p_r_msg.corrmsgid, p_queuetype => C_C_QUEUE_TYPE_OUT, p_message_type => C_C_MSG_TYPE_R) != 1
    then
      o_errmess := 'Получен ответ на неизвестное сообщение !';
      o_errcode := -1;
      v_res     := 0;
    elsif p_r_msg.message_type = C_C_MSG_TYPE_R
          and p_r_msg.corrmsgid is not null
          and v_queuetype = C_C_QUEUE_TYPE_IN
          and messlog_is(p_msgid => p_r_msg.corrmsgid, p_queuetype => C_C_QUEUE_TYPE_OUT) != 1
    then
      o_errmess := 'Получен запрос связанный с неизвестным сообщением !';
      o_errcode := -1;
      v_res     := 0;
    elsif p_r_msg.sender is null
    then
      o_errmess := ' Не указан отправитель сообщения !';
      o_errcode := -1;
      v_res     := 0;
    elsif trunc(nvl(p_r_msg.RequestDT, sysdate)) <= trunc(sysdate - gn_message_log_hist)
    then
      o_errmess := ' Устаревшее сообщение  !';
      o_errcode := -1;
      v_res     := 0;
    elsif trunc(nvl(p_r_msg.RequestDT, sysdate)) >= sysdate + 1/24/60*5 -- до 5 мин.  
    then
      o_errmess := ' Сообщения из будущего не обрабатываются !';
      o_errcode := -1;
      v_res     := 0;
    elsif p_r_msg.receiver is null
    then
      o_errmess := ' Не указан получатель сообщения !';
      o_errcode := 1;
      v_res     := 0;
    elsif v_queuetype = C_C_QUEUE_TYPE_IN
          and p_r_msg.receiver != C_C_SYSTEMNAME
    then
      o_errmess := 'Ошибка маршрутизации сообщения !' || chr(10) || 'В ' || C_C_SYSTEMNAME || ' поступило сообщение для ' || p_r_msg.receiver;
      o_errcode := 1;
      v_res     := 0;
    elsif p_r_msg.queue_num is not null
          and p_r_msg.queue_num != v_queue_num
    then
      o_errmess := 'Сообщение для очереди ID ' || p_r_msg.queue_num || ' отправлено в ' || p_queuename;
      o_errcode := 1;
      v_res     := 0;
    elsif p_r_msg.queue_num is not null
          and check_queue_num(p_r_msg.queue_num) != 1
    then
      o_errmess := 'ID очереди не может быть = ' || p_r_msg.queue_num;
      o_errcode := 1;
      v_res     := 0;
    elsif p_r_msg.ServiceGroup is not null
          and p_r_msg.queue_num is null
    then
      o_errmess := 'При указании потока исполнения (ServiceGroup) необходимо указать ID очереди';
      o_errcode := 1;
      v_res     := 0;
    end if;
    if o_errcode >= 0
       and p_r_msg.message_type = C_C_MSG_TYPE_R
       and p_r_msg.delivery_type = C_C_MSG_DELIVERY_S
       and v_queuetype = C_C_QUEUE_TYPE_IN
       and p_r_msg.Sender = C_C_SYSTEMNAME
    then
      vr_log := messlog_get(p_msgid => p_r_msg.msgid, p_queuetype => C_C_QUEUE_TYPE_OUT);
      if vr_log.status = C_STATUS_TIMEOUT
      then
        o_errmess := 'Выполнение запроса не ожидается потребителем !';
        o_errcode := -1;
        v_res     := 0;
      end if;
    end if;
    return v_res;
  end;

  -- Функция проверки входящего сообщения в записимости от отправителя  < 0 в 'TRASH'; >0 - отправляем ошибку
  function in_check_message(p_message it_q_message_t
                           ,o_errmess out varchar2 -- Сообщение об ошибке
                            ) return integer is
    v_func        itt_q_corrsystem.in_check_message_func%type;
    v_system_name itt_q_corrsystem.system_name%type;
    v_check       integer;
  begin
    if p_message.Sender = C_C_SYSTEMNAME
    then
      return 0;
    end if;
    if gt_corrsystem.exists(upper(p_message.Sender))
    then
      v_func        := gt_corrsystem(upper(p_message.Sender)).in_check_message_func;
      v_system_name := gt_corrsystem(upper(p_message.Sender)).system_name;
    else
      v_func := null;
    end if;
    if v_func is not null
    then
      begin
        execute immediate 'BEGIN :RESULT := ' || v_func || '(:p1,:o1); END;'
          using out v_check, in p_message, out o_errmess;
      exception
        when others then
          o_errmess := 'Ошибка! Проверка входящего сообщения из ' || v_system_name || '[' || v_func || ']: ' || get_errtxt(sqlerrm);
          if sqlcode in (-4061, -4065, -4068, -6508)
          then
            return 4061; -- Константа для перезапуска WORKERов
          else
            return - 1;
          end if;
      end;
    else
      v_check := 0;
    end if;
    if v_check != 0
       and o_errmess is null
    then
      o_errmess := 'Ошибка проверки входящего сообщения из ' || v_system_name;
    end if;
    return v_check;
  end;

  -- Процедура упаковки сообщения в записимости от получателя
  procedure out_pack_message(io_message    in out nocopy it_q_message_t
                            ,p_expire      date default null -- сообщение актуально до ....  
                            ,o_correlation out itt_q_message_log.correlation%type) is
    v_proc        itt_q_corrsystem.out_pack_message_proc%type;
    v_system_name itt_q_corrsystem.system_name%type;
    v_cl_outmess  clob;
    v_x_outmeta   xmltype;
  begin
    if io_message.Receiver = C_C_SYSTEMNAME
    then
      return;
    end if;
    if gt_corrsystem.exists(upper(io_message.Receiver))
    then
      v_proc        := gt_corrsystem(upper(io_message.Receiver)).out_pack_message_proc;
      v_system_name := gt_corrsystem(upper(io_message.Receiver)).system_name;
    else
      v_proc := null;
    end if;
    if v_proc is not null
    then
      begin
        execute immediate 'BEGIN ' || v_proc || '(:p1,:p2,:o1,:o2,:o3); END;'
          using in io_message, in p_expire, out o_correlation, out v_cl_outmess, out v_x_outmeta;
      exception
        when others then
          raise_application_error(-20000, 'Ошибка! Упаковщик сообщения для ' || v_system_name || ' [' || v_proc || ']: ' || get_errtxt(sqlerrm));
      end;
      io_message.MESSBODY := v_cl_outmess;
      io_message.MESSMeta := v_x_outmeta;
    end if;
  end;

  -- Создает тип сообщения 
  function new_message(p_message_type  itt_q_message_log.message_type%type
                      ,p_delivery_type itt_q_message_log.delivery_type%type
                      ,p_Priority      itt_q_message_log.Priority%type default C_C_MSG_PRIORITY_N
                      ,p_CORRmsgid     itt_q_message_log.CORRmsgid%type default null
                      ,p_ServiceName   itt_q_message_log.ServiceName%type default null
                      ,p_Receiver      itt_q_message_log.Receiver%type default null
                      ,p_ServiceGroup  itt_q_message_log.ServiceGroup%type default null
                      ,p_BTUID         itt_q_message_log.BTUID%type default null
                      ,p_Sender        itt_q_message_log.sender%type default C_C_SYSTEMNAME
                      ,p_SenderUser    itt_q_message_log.senderuser%type default null
                      ,p_MSGCode       itt_q_message_log.MSGCode%type default 0
                      ,p_MSGText       itt_q_message_log.MSGText%type default null
                      ,p_MESSBODY      itt_q_message_log.MESSBODY%type default null
                      ,p_MessMETA      xmltype default null
                      ,p_queue_num     itt_q_message_log.queue_num%type default null
                      ,p_RequestDT     itt_q_message_log.requestdt%type default null
                      ,p_ESBDT         itt_q_message_log.esbdt%type default null
                      ,p_check         boolean default true -- Можно отключить некоторые проверки если это команда или ответ об ошибке
                       ) return it_q_message_t as
    v_message  it_q_message_t;
    v_Priority itt_q_message_log.Priority%type := nvl(p_Priority, C_C_MSG_PRIORITY_N);
    v_Receiver itt_q_message_log.Receiver%type := nvl(p_Receiver, C_C_SYSTEMNAME);
    v_Sender   itt_q_message_log.sender%type := nvl(p_Sender, C_C_SYSTEMNAME);
  begin
    -- Проверки формата
    if nvl(p_message_type, chr(10)) not in (C_C_MSG_TYPE_R, C_C_MSG_TYPE_A)
       or nvl(p_delivery_type, chr(10)) not in (C_C_MSG_DELIVERY_S, C_C_MSG_DELIVERY_A)
       or nvl(v_priority, chr(10)) not in (C_C_MSG_PRIORITY_F, C_C_MSG_PRIORITY_N)
    then
      raise_application_error(-20000, 'Ошибка параметра сообщения!');
    end if;
    if nvl(p_check, true)
    then
      if p_message_type = C_C_MSG_TYPE_A
         and p_corrmsgid is null
      then
        raise_application_error(-20000, 'Для типа сообщения A(ответ) не заполнен обязательный параметр CORRMSGID');
      end if;
      if p_message_type = C_C_MSG_TYPE_R
         and p_servicename is null
      then
        raise_application_error(-20000, 'Для типа сообщения R(запрос) не заполнен обязательный параметр SERVICENAME');
      end if;
      if p_queue_num is not null
         and check_queue_num(p_queue_num) != 1
      then
        raise_application_error(-20000, 'Номер очереди не может быть = ' || p_queue_num);
      end if;
      if p_ServiceGroup is not null
         and p_queue_num is null
      then
        raise_application_error(-20000, 'При заказе потока исполнени (ServiceGroup) необходимо указать номер очереди');
      end if;
    end if;
    v_message := it_q_message_t(message_type => p_message_type
                               ,delivery_type => p_delivery_type
                               ,Priority => v_priority
                               ,msgid => get_sys_guid()
                               ,CORRmsgid => p_corrmsgid
                               ,ServiceName => p_servicename
                               ,Sender => v_Sender
                               ,Receiver => v_receiver
                               ,RequestDT => case
                                               when v_Sender = C_C_SYSTEMNAME then
                                                systimestamp
                                               else
                                                p_RequestDT
                                             end
                               ,ESBDT => case
                                           when v_receiver = C_C_SYSTEMNAME then
                                            nvl(p_ESBDT, systimestamp)
                                           else
                                            p_ESBDT
                                         end
                               ,SenderUser => case
                                                when v_Sender = C_C_SYSTEMNAME then
                                                 get_q_user()
                                                else
                                                 p_SenderUser
                                              end
                               ,ServiceGroup => p_servicegroup
                               ,BTUID => p_btuid
                               ,uteg => C_C_UTEG
                               ,MSGCode => nvl(p_msgcode, 0)
                               ,MSGText => p_msgtext
                               ,MESSBODY => p_messbody
                               ,MessMETA => p_MessMETA
                               ,queue_num => p_queue_num);
    if p_queue_num is null
       and p_delivery_type = C_C_MSG_DELIVERY_S
    then
      v_message.queue_num := find_queue_num(v_message);
    end if;
    return v_message;
  end;

  procedure msg_enqueue(p_message     it_q_message_t
                       ,p_isquery     pls_integer -- сообщение с ожиданием ответа (1/0)
                       ,p_comment     varchar2
                       ,p_queuetype   itt_q_message_log.queuetype%type
                       ,p_queue_num   itt_q_message_log.queue_num%type
                       ,p_correlation varchar2
                       ,p_delay       number default 0 --sys.dbms_aq.no_delay -- задежка появления сообщения в очереди
                       ,p_no_log      number default 0 -- 1- Не записываем в лог
                        ) as
    v_delay          number;
    v_msgid          raw(16);
    v_full_queuename varchar(128);
    v_enqdt          timestamp;
    v_queuetable     varchar2(128);
    v_LogID          number;
    v_tsenqdt        timestamp := systimestamp;
  begin
    v_delay := GREATEST(0, nvl(p_delay, 0));
    if v_delay > 0
    then
      v_tsenqdt := v_tsenqdt + numtodsinterval(v_delay, 'SECOND');
    end if;
    if p_queuetype = C_C_QUEUE_TYPE_IN
    then
      v_queuetable     := C_C_QTABLE_IN_PREFIX || p_queue_num;
      v_full_queuename := get_full_queue_name(C_C_QUEUE_IN_PREFIX || p_queue_num);
    elsif p_queuetype = C_C_QUEUE_TYPE_OUT
    then
      v_queuetable     := C_C_QTABLE_OUT_PREFIX || p_queue_num;
      v_full_queuename := get_full_queue_name(C_C_QUEUE_OUT_PREFIX || p_queue_num);
    else
      raise_application_error(-20000, 'Ошибка параметра процедуры it_q_message.msg_enqueue');
    end if;
    if p_queue_num = C_C_QUEUENUM_XX
    then
      v_LogID := its_q_log.nextval;
      execute immediate 'insert into ' || v_queuetable ||
                        ' (log_id,delay, qenqdt, correlation, msgid, corrmsgid, message_type, delivery_type, priority, ServiceName, ServiceGroup, sender, senderuser,receiver,txtmessbody)' ||
                        ' values (:v_LogID, :delay,:qenqdt,:correlation, :msgid, :corrmsgid, :message_type, :delivery_type, :priority, :ServiceName, :ServiceGroup, :sender, :senderuser, :receiver, :txtmessbody)'
        using v_LogID, v_tsenqdt, v_tsenqdt, p_correlation, p_message.msgid, p_message.CORRmsgid, p_message.message_type, p_message.delivery_type, p_message.Priority, p_message.ServiceName, p_message.ServiceGroup, p_message.sender, p_message.senderuser, p_message.receiver, dbms_lob.substr(p_message.messbody, 128, 1);
    else
      execute immediate q'[
      declare
        vr_enqueue_options sys.dbms_aq.enqueue_options_t;
        vr_msg_properties  sys.dbms_aq.message_properties_t;
      begin
        vr_msg_properties.correlation := :v_correlation;
        vr_msg_properties.delay       := :v_delay;
        sys.dbms_aq.enqueue(queue_name => :v_full_queuename
                           ,enqueue_options => vr_enqueue_options
                           ,message_properties => vr_msg_properties
                           ,payload => :p_message
                           ,msgid => :v_msgid);
      end;]'
        using in p_correlation, in v_delay, in v_full_queuename, in p_message, out v_msgid;
    end if;
    v_enqdt := systimestamp;
    if p_queue_num = C_C_QUEUENUM_XX
       or p_no_log = 0
    then
      messlog_insert(p_message => p_message
                    ,p_correlation => p_correlation
                    ,p_queuename => C_C_QUEUE_OUT_PREFIX || p_queue_num -- записываем в OUT
                    ,p_status => case
                                   when p_isquery = 0 then
                                    C_STATUS_SEND
                                   else
                                    C_STATUS_SENDQUERY
                                 end
                    ,p_enqdt => v_enqdt
                    ,p_comment => p_comment || case
                                    when v_delay != 0 then
                                     ' [Задержка ' || v_delay || 'с]'
                                    else
                                     null
                                  end
                    ,p_LogID => v_LogID);
    end if;
    if p_queue_num = C_C_QUEUENUM_XX
       and p_queuetype = C_C_QUEUE_TYPE_IN
       and (p_message.message_type = 'R' or (p_message.message_type = 'A' and p_message.delivery_type = 'A'))
       and it_q_message.check_correlation(p_correlation => p_correlation
                                         ,p_message_type => p_message.message_type
                                         ,p_delivery_type => p_message.delivery_type
                                         ,p_priority => p_message.priority
                                         ,p_msgid => p_message.msgid
                                         ,p_corrmsgid => p_message.corrmsgid
                                         ,p_qmcheck => 1) = 1
    then
      it_q_manager.XWorker_start;
    end if;
  end;

  procedure msg_dequeueXX(p_qmsgid      raw default null -- guid сообщения в очереди
                         ,p_msgid       itt_q_message_log.msgid%type default null
                         ,p_correlation itt_q_message_log.correlation%type default null
                         ,p_queuetype   itt_q_message_log.queuetype%type
                         ,p_expire      timestamp
                         ,p_toState     pls_integer
                         ,o_qmsgid      out raw
                         ,o_correlation out varchar2
                         ,o_message     out it_q_message_t) as
    pragma autonomous_transaction;
    v_tablename           varchar2(128);
    v_tmp                 integer;
    v_sel                 varchar2(32000);
    cur                   sys_refcursor;
    v_log_id              itt_q_message_log.log_id%type;
    v_delivery_type       itt_q_message_log.delivery_type%type;
    vr_log                itt_q_message_log%rowtype;
    v_delay               number;
    v_queueXX_max_retries integer;
    v_toState             pls_integer := nvl(p_toState, 1);
  begin
    if p_queuetype = C_C_QUEUE_TYPE_IN
    then
      v_tablename           := C_C_QTABLE_IN_PREFIX || C_C_QUEUENUM_XX;
      v_queueXX_max_retries := gn_queueXX_max_retries_IN;
    elsif p_queuetype = C_C_QUEUE_TYPE_OUT
    then
      v_tablename           := C_C_QTABLE_OUT_PREFIX || C_C_QUEUENUM_XX;
      v_queueXX_max_retries := gn_queueXX_max_retries_OUT;
    else
      raise_application_error(-20000, 'Ошибка параметра процедуры it_q_message.msg_dequeueXX');
    end if;
    o_qmsgid := p_qmsgid;
    if p_msgid is not null
    then
      begin
        execute immediate 'select q.qmsgid, q.correlation, q.log_id, q.delivery_type from ' || v_tablename || ' q where q.delay <= systimestamp and q.msgid = :p_msgid '
          into o_qmsgid, o_correlation, v_log_id, v_delivery_type
          using p_msgid;
      exception
        when others then
          raise_application_error(-20263, 'Сообщение MsgID=' || p_msgid || ' не найдено в табличной ' || p_queuetype || ' очереди!');
      end;
    end if;
    if o_qmsgid is not null
    then
      begin
        execute immediate 'select q.qmsgid, q.correlation, q.log_id, q.delivery_type  from ' || v_tablename ||
                          ' q where q.delay <= systimestamp and q.qmsgid = :o_qmsgid for update nowait'
          into o_qmsgid, o_correlation, v_log_id, v_delivery_type
          using o_qmsgid;
      exception
        when others then
          raise_application_error(-20263, 'Сообщение QMsgID=' || o_qmsgid || ' не найдено в табличной ' || p_queuetype || ' очереди!');
      end;
    elsif p_correlation is null
    then
      raise_application_error(-20000, 'Ошибка параметра p_correlation  процедуры it_q_message.msg_dequeueXX');
    else
      v_sel := 'select q.qmsgid, q.correlation, q.log_id, q.delivery_type  from ' || v_tablename ||
               ' q where q.state in (0,1) and q.delay <= systimestamp and q.correlation like :p_correlation order by q.qenqdt,q.log_id for update skip locked';
      while v_tmp is null
            or systimestamp < p_expire
      loop
        v_tmp := nvl(v_tmp, 0) + 1;
        if cur%isopen
        then
          close cur;
        end if;
        open cur for v_sel
          using p_correlation;
        fetch cur
          into o_qmsgid
              ,o_correlation
              ,v_log_id
              ,v_delivery_type;
        if cur%notfound
        then
          dbms_lock.sleep(gn_queueXX_dequeue_sleep);
          continue;
        end if;
        exit;
      end loop;
      if cur%isopen
      then
        close cur;
      end if;
    end if;
    if o_qmsgid is null
    then
      raise_application_error(-20228, 'Сообщение correlation=' || p_correlation || ' не получено из табличной ' || p_queuetype || ' очереди!');
    end if;
    vr_log := messlog_get(p_logid => v_log_id);
    if vr_log.log_id is not null
    then
      o_message := it_q_message_t(message_type => vr_log. message_type
                                 ,delivery_type => vr_log. delivery_type
                                 ,Priority => vr_log. Priority
                                 ,msgid => vr_log. msgid
                                 ,CORRmsgid => vr_log. CORRmsgid
                                 ,ServiceName => vr_log. ServiceName
                                 ,Sender => vr_log. Sender
                                 ,Receiver => vr_log. Receiver
                                 ,RequestDT => vr_log. RequestDT
                                 ,ESBDT => vr_log. ESBDT
                                 ,SenderUser => vr_log. SenderUser
                                 ,ServiceGroup => vr_log. ServiceGroup
                                 ,BTUID => vr_log. BTUID
                                 ,uteg => vr_log. uteg
                                 ,MSGCode => vr_log. MSGCode
                                 ,MSGText => vr_log. MSGText
                                 ,MessBODY => vr_log. MessBODY
                                 ,MessMETA => it_xml.Clob_to_xml(vr_log. MessMETA)
                                 ,queue_num => vr_log. queue_num);
    end if;
    if v_toState > 0
    then
      v_delay := case
                   when v_delivery_type = C_C_MSG_DELIVERY_S
                        and p_queuetype = C_C_QUEUE_TYPE_IN then
                    gn_queueXX_retry_delay_IN_S
                   when v_delivery_type = C_C_MSG_DELIVERY_A
                        and p_queuetype = C_C_QUEUE_TYPE_IN then
                    gn_queueXX_retry_delay_IN_A
                   when v_delivery_type = C_C_MSG_DELIVERY_S
                        and p_queuetype = C_C_QUEUE_TYPE_OUT then
                    gn_queueXX_retry_delay_OUT_S
                   when v_delivery_type = C_C_MSG_DELIVERY_A
                        and p_queuetype = C_C_QUEUE_TYPE_OUT then
                    gn_queueXX_retry_delay_OUT_A
                   else
                    gn_queueXX_retry_delay_OUT_A
                 end;
      execute immediate '
     update ' || v_tablename || ' q
       set q.delay = :delay
          ,q.expiration = q.expiration+1
          ,q.state = case when q.expiration >= :gn_queueXX_max_retries then 7 else decode(q.state,0,1,q.state) end 
          ,q.statedt = systimestamp
        where q.qmsgid = :o_qmsgid '
        using systimestamp + numtodsinterval(v_delay, 'SECOND'), v_queueXX_max_retries, o_qmsgid;
    end if;
    commit;
  end;

  procedure msg_dequeue(p_qmsgid      raw default null -- guid сообщения в очереди
                       ,p_msgid       itt_q_message_log.msgid%type default null
                       ,p_correlation itt_q_message_log.correlation%type default null
                       ,p_queuetype   itt_q_message_log.queuetype%type
                       ,p_queue_num   itt_q_message_log.queue_num%type -- Номер очереди 
                       ,p_wait        number default 0 -- ожидание в секундах
                       ,p_toState     pls_integer default 1 -- 0 - Не вычитывает 
                       ,p_errno       integer default 0
                       ,p_comment     varchar2 default null
                       ,o_qmsgid      out raw
                       ,o_correlation out varchar2
                       ,o_message     out it_q_message_t) is
    v_queuename     itt_q_message_log.queuename%type;
    v_tableXXname   varchar2(128);
    v_fullqueuename varchar2(128);
    v_wait          number := GREATEST(0, nvl(p_wait, 60));
    v_qmsgid        raw(16) := p_qmsgid;
    v_scr           varchar2(32000) := '      declare
        vr_deq_opts  sys.dbms_aq.dequeue_options_t;
        vr_msg_props sys.dbms_aq.message_properties_t;
      begin
        vr_deq_opts.navigation   := sys.dbms_aq.first_message;
        vr_deq_opts.wait         := :v_wait ;'; -- dbms_aq.FOREVER
    v_scr_f         varchar2(2000) := '
        sys.dbms_aq.dequeue(queue_name => :v_queuename
                           ,dequeue_options => vr_deq_opts
                           ,message_properties => vr_msg_props
                           ,payload => :o_message
                           ,msgid => :v_qmsgid_res);
        :o_correlation := vr_msg_props.correlation; 
      end;';
    v_expire        timestamp := systimestamp + numtodsinterval(nvl(v_wait, 0), 'SECOND');
    v_tmp           integer;
    v_toState       pls_integer := nvl(p_toState, 1);
  begin
    if (p_qmsgid is null and p_correlation is null and p_msgid is null)
       or (p_qmsgid is not null and p_correlation is not null)
       or (p_msgid is not null and p_correlation is not null)
    then
      raise_application_error(-20000, 'Ошибка параметров процедуры  it_q_message.msg_dequeue ');
    end if;
    if p_queuetype = C_C_QUEUE_TYPE_IN
    then
      v_queuename   := C_C_QUEUE_IN_PREFIX || p_queue_num;
      v_tableXXname := C_C_QTABLE_IN_PREFIX || C_C_QUEUENUM_XX;
    elsif p_queuetype = C_C_QUEUE_TYPE_OUT
    then
      v_queuename   := C_C_QUEUE_OUT_PREFIX || p_queue_num;
      v_tableXXname := C_C_QTABLE_OUT_PREFIX || C_C_QUEUENUM_XX;
    else
      raise_application_error(-20000, 'Ошибка параметра процедуры it_q_message.msg_dequeue');
    end if;
    if p_queue_num = C_C_QUEUENUM_XX
    then
      while v_tmp is null
            or (o_qmsgid is null and v_expire < systimestamp)
      loop
        v_tmp := 1;
        msg_dequeueXX(p_qmsgid => p_qmsgid
                     ,p_msgid => p_msgid
                     ,p_correlation => p_correlation
                     ,p_queuetype => p_queuetype
                     ,p_expire => v_expire
                     ,p_toState => v_toState
                     ,o_qmsgid => o_qmsgid
                     ,o_correlation => o_correlation
                     ,o_message => o_message);
        begin
          execute immediate 'select q.qmsgid  from ' || v_tableXXname || ' q where q.qmsgid = :o_qmsgid for update nowait '
            into o_qmsgid
            using o_qmsgid;
        exception
          when others then
            o_qmsgid := null;
            continue;
        end;
      end loop;
      if o_qmsgid is null
      then
        raise_application_error(-20228
                               ,'Сообщение correlation=' || p_correlation || ' не получено из табличной ' || p_queuetype || ' очереди!');
      end if;
      if v_toState = 1
      then
        execute immediate '
         delete  ' || v_tableXXname || ' q
            where q.qmsgid = :o_qmsgid '
          using o_qmsgid;
      elsif v_toState > 0
      then
        execute immediate '
         update  ' || v_tableXXname || ' q set state = :state, statedt = systimestamp
            where q.qmsgid = :o_qmsgid '
          using v_toState, o_qmsgid;
      end if;
    else
      v_fullqueuename := get_full_queue_name(v_queuename);
      if v_toState = 0
      then
        v_scr := v_scr || '
          vr_deq_opts.dequeue_mode := sys.dbms_aq.browse;';
      else
        v_scr := v_scr || '
           vr_deq_opts.dequeue_mode := sys.dbms_aq.remove;';
      end if;
      if p_correlation is not null
      then
        v_scr := v_scr || '
          vr_deq_opts.correlation  := :p_correlation;' || v_scr_f;
        execute immediate v_scr
          using in v_wait, in p_correlation, in v_fullqueuename, out o_message, out o_qmsgid, out o_correlation;
      else
        v_scr := v_scr || '
          vr_deq_opts.msgid        := :v_qmsgid;' || v_scr_f;
        execute immediate v_scr
          using in v_wait, in v_qmsgid, in v_fullqueuename, out o_message, out o_qmsgid, out o_correlation;
      end if;
    end if;
    if v_toState != 0
    then
      messlog_upd_deq(p_msgid => o_message.msgid, p_queuetype => C_C_QUEUE_TYPE_OUT, p_errno => p_errno, p_comment => p_comment);
    end if;
  end;

  procedure msg_dequeue_in(p_qmsgid      raw default null -- guid сообщения в очереди
                          ,p_msgid       itt_q_message_log.msgid%type default null
                          ,p_correlation itt_q_message_log.correlation%type default null
                          ,p_queue_num   itt_q_message_log.queue_num%type -- Номер очереди 
                          ,p_wait        number default 0 -- ожидание в секундах
                          ,p_toState     pls_integer default 1 -- 0 - Не вычитывает 
                          ,o_msgcode     out number
                          ,o_msgtext     out varchar2
                          ,o_enqdt       out timestamp
                          ,o_correlation out varchar2
                          ,o_message     out it_q_message_t) is
    v_wait       number := GREATEST(0, nvl(p_wait, 60));
    v_qmsgid     raw(16) := p_qmsgid;
    v_qmsgid_res raw(16);
    v_queuename  itt_q_message_log.queuename%type := C_C_QUEUE_IN_PREFIX || p_queue_num;
    vn_msgcode   integer;
    vc_msgtext   varchar(32000);
  begin
    msg_dequeue(p_qmsgid => v_qmsgid
               ,p_msgid => p_msgid
               ,p_correlation => p_correlation
               ,p_queuetype => C_C_QUEUE_TYPE_IN
               ,p_queue_num => p_queue_num
               ,p_wait => v_wait
               ,p_toState => p_toState
               ,o_qmsgid => v_qmsgid_res
               ,o_correlation => o_correlation
               ,o_message => o_message);
    o_enqdt             := get_enqdt_deq(p_qmsgid => v_qmsgid_res, p_queuename => v_queuename);
    o_message.msgid     := nvl(o_message.msgid, v_qmsgid_res);
    o_message.requestdt := nvl(o_message.requestdt, o_enqdt);
    o_msgcode           := 0;
    o_msgtext           := null;
    if check_atr_message(p_correlation => o_correlation, p_queuename => v_queuename, p_r_msg => o_message, o_errcode => o_msgcode, o_errmess => o_msgtext) = 1
    then
      if o_msgcode > 0
      then
        o_msgcode := it_q_manager.C_N_ERROR_OTHERS_MSGCODE;
      end if;
      if o_message.msgcode != 0
      then
        o_msgcode := abs(o_message.msgcode);
        o_msgtext := nvl(get_errtxt(o_message.msgtext), 'Ошибка № ' || o_message.msgcode);
      end if;
    else
      o_msgcode := -1; -- Не прошедшие контроль по атрибутам ОТВЕТЫ все равно грузим в мусор
    end if;
    if o_msgcode >= 0
    then
      vn_msgcode := in_check_message(p_message => o_message, o_errmess => vc_msgtext);
      if vn_msgcode < 0
         or vn_msgcode = 4061
         or (vn_msgcode > 0 and o_msgcode = 0)
      then
        o_msgcode := vn_msgcode; -- Сохраняем только для прошедших контроль
        o_msgtext := vc_msgtext;
      end if;
    end if;
  end;

  -- Процедура ожидания ответа для синхронного сообщения
  procedure dequeue_message(p_msgid     itt_q_message_log.msgid%type -- msgid исходного сообщения
                           ,p_queue_num itt_q_message_log.queue_num%type -- Номер очереди 
                           ,p_wait      number default 0 -- ожидание в секундах
                           ,o_msgid     out itt_q_message_log.msgid%type
                           ,o_msgcode   out integer -- 0 - без ошибки
                           ,o_msgtext   out varchar2 -- Сообщение
                           ,o_messbody  out clob
                           ,o_messmeta  out xmltype) is
    pragma autonomous_transaction;
    deque_time_out exception;
    pragma exception_init(deque_time_out, -25228);
    dequeXX_time_out exception;
    pragma exception_init(deque_time_out, -20228);
    v_qwait            number := get_qwait(p_timeout => p_wait);
    v_wait_correlation itt_q_message_log.correlation%type;
    v_correlation      itt_q_message_log.correlation%type;
    v_message          it_q_message_t;
    v_enqdt            timestamp;
  begin
    -- ждем ответа
    v_wait_correlation := C_C_MSG_TYPE_A || C_C_MSG_DELIVERY_S || '_' || p_msgid;
    msg_dequeue_in(p_correlation => v_wait_correlation
                  ,p_queue_num => p_queue_num
                  ,p_wait => v_qwait
                  ,o_msgcode => o_msgcode
                  ,o_msgtext => o_msgtext
                  ,o_enqdt => v_enqdt
                  ,o_correlation => v_correlation
                  ,o_message => v_message);
    -- пишем в журнал ответ
    messlog_insert_out(p_message => v_message
                      ,p_correlation => v_correlation
                      ,p_queuename => C_C_QUEUE_IN_PREFIX || p_queue_num
                      ,p_status => case
                                     when o_msgcode < 0 then
                                      C_STATUS_TRASH
                                     when o_msgcode > 0 then
                                      C_STATUS_ERROR
                                     else
                                      C_STATUS_LOAD
                                   end
                      ,p_enqdt => v_enqdt
                      ,p_comment => case
                                      when v_message.msgtext is null
                                           or o_msgtext != v_message.msgtext then
                                       o_msgtext
                                    end
                      ,p_workdt => systimestamp
                      ,p_workuser => get_q_user()
                      ,o_logid => o_msgid
                      ,o_messbody => o_messbody
                      ,o_messmeta => o_messmeta);
    -- Меняем статус у запроса
    messlog_upd_status(p_msgid => p_msgid
                      ,p_delivery_type => C_C_MSG_DELIVERY_S
                      ,p_queuetype => C_C_QUEUE_TYPE_OUT
                      ,p_status => case
                                     when o_msgcode = 0 then
                                      C_STATUS_ANSWER
                                     else
                                      C_STATUS_ERRANSWER
                                   end
                      ,p_comment => get_comment_add(p_msgcode => o_msgcode, p_add_comment => o_msgtext));
    commit;
  exception
    when deque_time_out
         or dequeXX_time_out then
      rollback; -- autonomous
      messlog_upd_status(p_msgid => p_msgid
                        ,p_delivery_type => C_C_MSG_DELIVERY_S
                        ,p_queuetype => C_C_QUEUE_TYPE_OUT
                        ,p_status => C_STATUS_TIMEOUT
                        ,p_comment => 'Превышен таймаут. Нет ответа.');
      commit; -- autonomous
      raise_application_error(-20999, 'Превышен таймаут. Нет ответа.');
    when others then
      rollback; -- autonomous
      messlog_upd_status(p_msgid => p_msgid
                        ,p_delivery_type => C_C_MSG_DELIVERY_S
                        ,p_queuetype => C_C_QUEUE_TYPE_OUT
                        ,p_status => C_STATUS_ERROR
                        ,p_comment => sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
      commit; -- autonomous
      raise_application_error(-20998, get_errtxt(sqlerrm));
  end;

  -- процедура отправки сообщения в очередь
  procedure enqueue_message(p_message     it_q_message_t
                           ,p_queue_num   itt_q_message_log.queue_num%type default null
                           ,p_delay       number default 0 --sys.dbms_aq.no_delay -- задежка появления сообщения в очереди
                           ,p_isquery     pls_integer -- сообщение с ожиданием ответа (1/0)
                           ,p_correlation varchar2 default null
                           ,p_comment     varchar2 default null) is
    v_queuetype   itt_q_message_log.queuetype%type;
    v_qnum        itt_q_message_log.queue_num%type := nvl(p_queue_num, find_queue_num(p_message));
    v_delay       number;
    v_isquery     pls_integer := nvl(p_isquery, 0);
    v_correlation itt_q_message_log.correlation%type;
  begin
    if check_queue_num(v_qnum) != 1
    then
      raise_application_error(-20000, 'Очередь ID ' || v_qnum || ' не инсталлирована в системе ');
    end if;
    -- Определяем correlation
    v_correlation := case
                       when p_message.receiver = C_C_SYSTEMNAME
                            or p_correlation is null then
                        get_correlation(p_message_type => p_message.message_type
                                       ,p_delivery_type => p_message.delivery_type
                                       ,p_priority => p_message.priority
                                       ,p_msgid => p_message.msgid
                                       ,p_corrmsgid => p_message.corrmsgid)
                       else
                        p_correlation
                     end;
    -- Определяем задержку
    v_delay := GREATEST(0, nvl(p_delay, 0));
    -- кладем в очередь
    if p_message.receiver = C_C_SYSTEMNAME -- Если сообщение к нам то на вход
    then
      v_queuetype := C_C_QUEUE_TYPE_IN;
    else
      v_queuetype := C_C_QUEUE_TYPE_OUT;
    end if;
    -- Для ответов от нас обновим статус связанного сообщения
    if p_message.Sender = C_C_SYSTEMNAME
       and p_message.message_type = C_C_MSG_TYPE_A
    then
      messlog_upd_status(p_msgid => p_message.corrmsgid
                        ,p_delivery_type => p_message.delivery_type
                        ,p_queuetype => C_C_QUEUE_TYPE_IN
                        ,p_status => case
                                       when p_message.MSGCode = 0 then
                                        C_STATUS_ANSWER
                                       else
                                        C_STATUS_ERRANSWER
                                     end);
    end if;
    msg_enqueue(p_message => p_message
               ,p_isquery => v_isquery
               ,p_comment => p_comment
               ,p_queuetype => v_queuetype
               ,p_queue_num => v_qnum
               ,p_correlation => v_correlation
               ,p_delay => v_delay);
  end;

  -- процедура отправки сообщения
  procedure send_message(io_message  in out nocopy it_q_message_t
                        ,p_queue_num itt_q_message_log.queue_num%type default null
                        ,p_delay     number default 0 --sys.dbms_aq.no_delay -- задежка появления сообщения в очереди
                        ,p_expire    date default null -- сообщение актуально до .... 
                        ,p_comment   varchar2 default null
                        ,p_isquery   pls_integer default null -- сообщение с ожиданием ответа (1/0)
                         ) is
    pragma autonomous_transaction;
    v_correlation itt_q_message_log.correlation%type;
  begin
    -- Упаковываем сообщение
    out_pack_message(io_message => io_message, p_expire => p_expire, o_correlation => v_correlation);
    enqueue_message(p_message => io_message, p_queue_num => p_queue_num, p_delay => p_delay, p_correlation => v_correlation, p_comment => p_comment, p_isquery => p_isquery);
    commit; -- autonomous
  exception
    when others then
      rollback; -- autonomous
      raise_application_error(-20998, get_errtxt(sqlerrm));
  end;

  -- Обновление статичных справочников в коллекции
  procedure refresh_spr(p_refresh boolean default false) as
  begin
    if nvl(p_refresh, false)
       or sysdate > nvl(gd_last_refresh_spr, sysdate - 1) + numtodsinterval(60 * 60 * 1, 'SECOND') -- Раз в час  
    then
      gd_last_refresh_spr := sysdate;
      refresh_table_corrsystem;
      refresh_table_queue_num;
    end if;
  end;

  -- ================= Универсальные процедуры работы с сообщениями =============================
  --
  -- универсальная процедура запуска синхронного запроса
  procedure do_s_service(p_servicename    itt_q_message_log.servicename%type -- Бизнес-процесс
                        ,p_queue_num      itt_q_message_log.queue_num%type default null -- Номер очереди
                        ,p_receiver       itt_q_message_log.receiver%type default C_C_SYSTEMNAME -- Система-получатель
                        ,p_messbody       clob default null -- сообщение
                        ,p_messmeta       xmltype default null -- Мета данные
                        ,p_priority       itt_q_message_log.priority%type default C_C_MSG_PRIORITY_N -- Очередность синхронных сообщений ( F- быстрая,N- норм)
                        ,p_corrmsgid      itt_q_message_log.corrmsgid%type default null -- GUID исходногоо сообщения 
                        ,p_comment        varchar2 default null -- коментарии в лог
                        ,p_timeout        integer default null --  таймаут в секундах
                        ,p_norise_msgcode integer default null -- если 1 - не выполняется EXCEPTION если сообщение с msgcode > 0
                        ,io_msgid         in out itt_q_message_log.msgid%type -- GUID  сообщения
                        ,o_answerid       out itt_q_message_log.msgid%type -- GUID ответа
                        ,o_answerbody     out clob -- Ответ
                        ,o_answermeta     out xmltype -- Ответ Мета данные
                         ) is
    v_message it_q_message_t;
    v_wait    integer := get_qwait(p_timeout);
    v_msgcode itt_q_message_log.msgcode%type;
    v_msgtext itt_q_message_log.msgtext%type;
  begin
    refresh_spr;
    v_message       := new_message(p_message_type => C_C_MSG_TYPE_R
                                  ,p_delivery_type => C_C_MSG_DELIVERY_S
                                  ,p_Priority => p_priority
                                  ,p_CORRmsgid => p_corrmsgid
                                  ,p_ServiceName => p_ServiceName
                                  ,p_Receiver => p_Receiver
                                  ,p_MESSBODY => p_MESSBODY
                                  ,p_MessMETA => p_MessMETA
                                  ,p_queue_num => p_queue_num);
    v_message.msgid := nvl(io_msgid, v_message.msgid);
    io_msgid        := v_message.msgid;
    ----------- Отправляем сообщение
    begin
      send_message(io_message => v_message, p_expire => sysdate + numtodsinterval(v_wait, 'SECOND'), p_comment => p_comment, p_isquery => 1);
    exception
      when others then
        raise_application_error(-20998, 'Не удалось отправить сообщение в очередь ' || get_errtxt(sqlerrm));
    end;
    ----------- Ожидаем ответ
    begin
      dequeue_message(p_msgid => v_message.msgid
                     ,p_queue_num => v_message.queue_num
                     ,p_wait => v_wait
                     ,o_msgcode => v_msgcode
                     ,o_msgtext => v_msgtext
                     ,o_msgid => o_answerid
                     ,o_messbody => o_answerbody
                     ,o_messmeta => o_answermeta);
    exception
      when others then
        raise_application_error(case when sqlcode >= -20999 and sqlcode <= -20000 then sqlcode else - 20998 end
                               ,'Не удалось получить ответ из ' || p_receiver || ' [' || p_servicename || ']:' || get_errtxt(sqlerrm));
    end;
    if v_msgcode < 0
       or (v_msgcode > 0 and nvl(p_norise_msgcode, 0) != 1)
    then
      raise_application_error(case when v_msgcode between 20000 and 20999 then - v_msgcode else - 20000 end, p_receiver || '[' || p_servicename || ']:' || v_msgtext);
    end if;
  end;

  -- универсальная процедура запуска асинхронного запроса
  procedure do_a_service(p_servicename  itt_q_message_log.servicename%type -- Бизнес-процесс
                        ,p_receiver     itt_q_message_log.receiver%type default C_C_SYSTEMNAME -- Система-получатель
                        ,p_messbody     clob default null -- сообщение
                        ,p_messmeta     xmltype default null -- Мета данные
                        ,p_servicegroup itt_q_message_log.servicegroup%type default null -- GUID Поток исполнения бизнес-процесса 
                        ,p_queue_num    itt_q_message_log.queue_num%type default null -- Номер очереди
                        ,p_corrmsgid    itt_q_message_log.corrmsgid%type default null -- GUID исходногоо сообщения 
                        ,p_comment      varchar2 default null -- коментарии в лог
                        ,p_delay        number default 0 --sys.dbms_aq.no_delay -- задежка появления сообщения в очереди
                        ,io_msgid       in out itt_q_message_log.msgid%type -- GUID сообщения 
                        ,p_isquery      pls_integer default null -- сообщение с ожиданием ответа (1/0)
                         ) is
    v_message it_q_message_t;
  begin
    refresh_spr;
    v_message       := new_message(p_message_type => C_C_MSG_TYPE_R
                                  ,p_delivery_type => C_C_MSG_DELIVERY_A
                                  ,p_CORRmsgid => p_corrmsgid
                                  ,p_ServiceName => p_ServiceName
                                  ,p_Receiver => p_Receiver
                                  ,p_ServiceGroup => p_ServiceGroup
                                  ,p_queue_num => p_queue_num
                                  ,p_MESSBODY => p_MESSBODY
                                  ,p_MessMETA => p_MessMETA);
    v_message.msgid := nvl(io_msgid, v_message.msgid);
    io_msgid        := v_message.msgid;
    ----------- Отправляем сообщение
    send_message(io_message => v_message, p_delay => p_delay, p_comment => p_comment, p_isquery => p_isquery);
  exception
    when others then
      raise_application_error(-20998, 'Не удалось отправить сообщение в очередь ' || get_errtxt(sqlerrm));
  end;

  -- Повтор сообщения
  procedure repeat_message(p_msgid   itt_q_message_log.msgid%type
                          ,p_delay   number -- задежка появления сообщения в очереди (для исключения рекурсивной генерации сообщений желательно указывать не 0 )
                          ,p_comment varchar2 default 'Повторно'
                          ,o_msgid   out varchar2) as
    pragma autonomous_transaction;
    vr_mes    itt_q_message_log%rowtype;
    v_message it_q_message_t;
  begin
    refresh_spr;
    vr_mes := messlog_get(p_msgid => p_msgid, p_queuetype => C_C_QUEUE_TYPE_OUT);
    if vr_mes.msgid is null
    then
      raise_application_error(-20000, 'Не удалось найти сообщение ' || p_msgid || ' для повторного отправления!');
    end if;
    v_message := new_message(p_message_type => vr_mes.message_type
                            ,p_delivery_type => vr_mes.delivery_type
                            ,p_Priority => vr_mes.priority
                            ,p_CORRmsgid => p_msgid
                            ,p_ServiceName => vr_mes.servicename
                            ,p_Receiver => vr_mes.receiver
                            ,p_ServiceGroup => vr_mes.servicegroup
                            ,p_BTUID => vr_mes.btuid
                            ,p_MSGCode => vr_mes.msgcode
                            ,p_MSGText => vr_mes.msgtext
                            ,p_MESSBODY => vr_mes.messbody
                            ,p_MessMETA => it_xml.Clob_to_xml(vr_mes.MessMETA)
                            ,p_queue_num => vr_mes.queue_num
                            ,p_check => false);
    o_msgid   := v_message.msgid;
    enqueue_message(p_message => v_message
                   ,p_queue_num => get_queue_num(vr_mes.queuename)
                   ,p_delay => p_delay
                   ,p_isquery => 1
                   ,p_correlation => vr_mes.correlation
                   ,p_comment => p_comment);
    commit; -- autonomous
  exception
    when others then
      rollback; -- autonomous
      raise_application_error(-20998, get_errtxt(sqlerrm));
  end;

  -- процедура размещения сообщения в очередь
  procedure load_msg(io_msgid        in out itt_q_message_log.msgid%type -- GUID сообщения
                    ,p_message_type  itt_q_message_log.message_type%type -- Тип сообщения R или A Запрос/ответ 
                    ,p_delivery_type itt_q_message_log.delivery_type%type -- Вид доставки S или A Синхронный / Асинхронный
                    ,p_Sender        itt_q_message_log.sender%type default C_C_SYSTEMNAME -- Система-источник
                    ,p_Priority      itt_q_message_log.priority%type default C_C_MSG_PRIORITY_N -- Очередность синхронных сообщений ( F- быстрая,N- норм)
                    ,p_correlation   itt_q_message_log.correlation%type default null -- Correlation для внешнего сообщения (null - назначит система)
                    ,p_CORRmsgid     itt_q_message_log.corrmsgid%type default null -- GUID связанного сообщения 
                    ,p_SenderUser    itt_q_message_log.senderuser%type default get_q_user() -- Идентификатор пользователя в Системе-отправите
                    ,p_ServiceName   itt_q_message_log.servicename%type default null -- Бизнес-процесс
                    ,p_ServiceGroup  itt_q_message_log.servicegroup%type default null -- Поток исполнения бизнес-процесса  
                    ,p_Receiver      itt_q_message_log.receiver%type default C_C_SYSTEMNAME -- Система-получатель
                    ,p_BTUID         itt_q_message_log.btuid%type default null -- ID бизнес транзакции, зарезервировано
                    ,p_MSGCode       integer default 0 -- Код результата обработки сообщения. 0 - успех
                    ,p_MSGText       itt_q_message_log.msgtext%type default null -- Текст ошибки, возникший при обработке сообщен
                    ,p_MESSBODY      clob default null -- Бизнес - составляющая сообщения
                    ,p_MessMETA      xmltype default null -- Мета данные -- XML Метаданные сообщения
                    ,p_queue_num     itt_q_message_log.queue_num%type default null -- ID очереди (null - определяется системой)
                    ,p_RequestDT     timestamp default systimestamp -- Время отправления сообщения из системы источник
                    ,p_ESBDT         timestamp default null -- Время начала обработки транспортной системой  
                    ,p_delay         number default 0 --sys.dbms_aq.no_delay -- задежка появления сообщения в очереди
                    ,p_comment       itt_q_message_log.commenttxt%type default null -- коментарии в лог 
                    ,p_isquery       pls_integer default null -- сообщение с ожиданием ответа (1/0)
                     ) as
    v_message     it_q_message_t;
    v_MessMETA    xmltype := p_MessMETA; -- it_xml.Clob_to_xml(p_MessMETA, 'p_MessMETA');
    vr_log        itt_q_message_log%rowtype;
    v_queue_num   itt_q_message_log.queue_num%type := p_queue_num;
    v_Receiver    itt_q_message_log.receiver%type := p_Receiver;
    v_ServiceName itt_q_message_log.servicename%type := p_ServiceName;
    v_correlation itt_q_message_log.correlation%type;
  begin
    refresh_spr;
    if /*p_Sender = C_C_SYSTEMNAME
       and p_message_type = C_C_MSG_TYPE_R
       and p_delivery_type = C_C_MSG_DELIVERY_S
    then
      raise_application_error(-20000
                             ,'Cообщение ' || io_msgid || ' сервиса ' || p_ServiceName || ' должно быть из другой подсистемы' || chr(10) ||
                              ' или внутренним асинхронным  или ответом на асинхронный запрос!');
    elsif*/ p_Sender = C_C_SYSTEMNAME
          and p_message_type = C_C_MSG_TYPE_A
          and p_CORRmsgid is null
    then
      raise_application_error(-20000
                             ,'Внутренний ответ ' || io_msgid || ' сервиса ' || p_ServiceName || chr(10) || ' не имеет ссылку p_CORRmsgid на запрос!');
    elsif p_Sender = C_C_SYSTEMNAME
          and p_message_type = C_C_MSG_TYPE_A
    then
      vr_log := get_msg(p_msgid => p_CORRmsgid, p_with_TRASH => true);
      if vr_log.log_id is null
         or vr_log.delivery_type != C_C_MSG_DELIVERY_A
      then
        raise_application_error(-20000
                               ,'Внутренний ответ ' || io_msgid || ' сервиса ' || p_ServiceName || chr(10) || ' может быть тольно на внутренний асинхронн !');
      end if;
      v_queue_num := nvl(vr_log.queue_num, it_q_message.get_queue_num(vr_log.queuename));
      if vr_log.message_type = C_C_MSG_TYPE_R
      then
        v_Receiver    := nvl(v_Receiver, vr_log.sender);
        v_ServiceName := nvl(v_ServiceName, vr_log.servicename);
      end if;
    end if;
    v_message       := new_message(p_message_type => p_message_type
                                  ,p_delivery_type => p_delivery_type
                                  ,p_Priority => p_Priority
                                  ,p_CORRmsgid => p_CORRmsgid
                                  ,p_ServiceName => v_ServiceName
                                  ,p_Receiver => v_Receiver
                                  ,p_ServiceGroup => p_ServiceGroup
                                  ,p_BTUID => p_BTUID
                                  ,p_Sender => p_Sender
                                  ,p_SenderUser => p_SenderUser
                                  ,p_MSGCode => p_MSGCode
                                  ,p_MSGText => p_MSGText
                                  ,p_MESSBODY => p_MESSBODY
                                  ,p_MessMETA => v_MessMETA
                                  ,p_queue_num => v_queue_num
                                  ,p_RequestDT => p_RequestDT
                                  ,p_ESBDT => p_ESBDT);
    v_message.msgid := nvl(io_msgid, v_message.msgid);
    io_msgid        := v_message.msgid;
    -- Упаковываем сообщение
    out_pack_message(io_message => v_message, o_correlation => v_correlation);
    v_correlation := nvl(p_correlation, v_correlation);
    enqueue_message(p_message => v_message, p_delay => p_delay, p_correlation => v_correlation, p_comment => p_comment, p_isquery => p_isquery);
  end;

  -- процедура размещения сообщения во входящую очередь ( для адаптеров)
  procedure load_msg_inqueue(p_msgid         itt_q_message_log.msgid%type -- GUID сообщения
                            ,p_message_type  itt_q_message_log.message_type%type -- Тип сообщения R или A Запрос/ответ 
                            ,p_delivery_type itt_q_message_log.delivery_type%type -- Вид доставки S или A Синхронный / Асинхронный
                            ,p_Sender        itt_q_message_log.sender%type -- Система-источник
                            ,p_Priority      itt_q_message_log.priority%type default C_C_MSG_PRIORITY_N -- Очередность синхронных сообщений ( F- быстрая,N- норм)
                             -- ,p_correlation   itt_q_message_log.correlation%type default null -- Correlation для внешнего сообщения (null - назначит система)
                            ,p_CORRmsgid    itt_q_message_log.corrmsgid%type -- GUID связанного сообщения 
                            ,p_SenderUser   itt_q_message_log.senderuser%type default get_q_user() -- Идентификатор пользователя в Системе-отправите
                            ,p_ServiceName  itt_q_message_log.servicename%type -- Бизнес-процесс
                            ,p_ServiceGroup itt_q_message_log.servicegroup%type -- Поток исполнения бизнес-процесса  
                             -- ,p_Receiver      itt_q_message_log.receiver%type default C_C_SYSTEMNAME -- Система-получатель
                            ,p_BTUID     itt_q_message_log.btuid%type default null -- ID бизнес транзакции, зарезервировано
                            ,p_MSGCode   integer -- Код результата обработки сообщения. 0 - успех
                            ,p_MSGText   itt_q_message_log.msgtext%type -- Текст ошибки, возникший при обработке сообщен
                            ,p_MESSBODY  clob -- Бизнес - составляющая сообщения
                            ,p_MessMETA  xmltype -- Мета данные -- XML Метаданные сообщения
                            ,p_queue_num itt_q_message_log.queue_num%type -- ID очереди 
                            ,p_RequestDT timestamp -- Время отправления сообщения из системы источник
                            ,p_ESBDT     timestamp -- Время начала обработки транспортной системой  
                             --,p_delay         binary_integer default sys.dbms_aq.no_delay -- задежка появления сообщения в очереди
                             --,p_comment       itt_q_message_log.commenttxt%type default null -- коментарии в лог 
                             --,p_isquery       pls_integer default null -- сообщение с ожиданием ответа (1/0)
                             ) as
    v_message     it_q_message_t;
    v_correlation itt_q_message_log.correlation%type;
  begin
    if p_Sender = C_C_SYSTEMNAME
    then
      raise_application_error(-20000, 'Cообщение ' || p_msgid || ' сервиса ' || p_ServiceName || ' должно быть из другой подсистемы !');
    end if;
    if check_queue_num(p_queue_num) != 1
    then
      raise_application_error(-20000, 'Очередь ID [' || p_queue_num || '] не инсталлирована в системе ');
    end if;
    v_message       := new_message(p_message_type => p_message_type
                                  ,p_delivery_type => p_delivery_type
                                  ,p_Priority => p_Priority
                                  ,p_CORRmsgid => p_CORRmsgid
                                  ,p_ServiceName => p_ServiceName
                                  ,p_Receiver => C_C_SYSTEMNAME
                                  ,p_ServiceGroup => p_ServiceGroup
                                  ,p_BTUID => p_BTUID
                                  ,p_Sender => p_Sender
                                  ,p_SenderUser => p_SenderUser
                                  ,p_MSGCode => p_MSGCode
                                  ,p_MSGText => p_MSGText
                                  ,p_MESSBODY => p_MESSBODY
                                  ,p_MessMETA => p_MessMETA
                                  ,p_queue_num => p_queue_num
                                  ,p_RequestDT => p_RequestDT
                                  ,p_ESBDT => p_ESBDT);
    v_message.msgid := nvl(p_msgid, v_message.msgid);
    -- Определяем correlation
    v_correlation := get_correlation(p_message_type => v_message.message_type
                                    ,p_delivery_type => v_message.delivery_type
                                    ,p_priority => v_message.priority
                                    ,p_msgid => v_message.msgid
                                    ,p_corrmsgid => v_message.corrmsgid);
    -- кладем в очередь
    msg_enqueue(p_message => v_message
               ,p_isquery => 0
               ,p_comment => null
               ,p_queuetype => C_C_QUEUE_TYPE_IN
               ,p_queue_num => p_queue_num
               ,p_correlation => v_correlation
               ,p_delay => 0
               ,p_no_log => 1);
  end;

  -- Процедура получения синхронного ответа если ответа нет o_msgid = null
  procedure wait_s_msg(p_msgid    itt_q_message_log.msgid%type -- msgid исходного сообщения
                      ,p_wait     number default 0.001 -- ожидание в секундах если 0 - то по настройке QMESSAGE_TIMEOUT
                      ,o_msgid    out itt_q_message_log.msgid%type -- msgid ответа 
                      ,o_msgcode  out integer
                      ,o_msgtext  out varchar2
                      ,o_messbody out clob
                      ,o_messmeta out xmltype) as
    deque_time_out exception;
    pragma exception_init(deque_time_out, -20999);
    vr_log      itt_q_message_log%rowtype;
    v_queue_num itt_q_message_log.queue_num%type;
  begin
    vr_log := get_msg(p_msgid => p_msgid);
    if vr_log.msgid is null
    then
      raise_application_error(-20000, 'Запрос ' || p_msgid || ' не найден!');
    elsif vr_log.message_type != C_C_MSG_TYPE_R
    then
      raise_application_error(-20000, 'Сообщение ' || p_msgid || ' должно быть запросом!');
    end if;
    v_queue_num := it_q_message.get_queue_num(vr_log.queuename);
    dequeue_message(p_msgid => p_msgid
                   ,p_queue_num => v_queue_num
                   ,p_wait => p_wait
                   ,o_msgid => o_msgid
                   ,o_msgcode => o_msgcode
                   ,o_msgtext => o_msgtext
                   ,o_messbody => o_messbody
                   ,o_messmeta => o_messmeta);
  exception
    when deque_time_out then
      null;
  end;

  -- функция возвращает сообщение в последнем состоянии
  function get_msg(p_msgid      itt_q_message_log.msgid%type
                  ,p_with_TRASH boolean default false) return itt_q_message_log%rowtype is
    v_with_TRASH boolean := nvl(p_with_TRASH, false);
    vr_log       itt_q_message_log%rowtype;
  begin
    if v_with_TRASH
    then
      select *
        into vr_log
        from (select *
                from itt_q_message_log l
               where msgid = p_MsgID
               order by decode(l.status, C_STATUS_TRASH, 1, 0) -- Приоритет не TRASH сообщение in
                       ,decode(l.queuetype, C_C_QUEUE_TYPE_IN, 0, 1)
                       ,l.enqdt desc)
       where rownum < 2;
    else
      select *
        into vr_log
        from (select *
                from itt_q_message_log l
               where msgid = p_MsgID
                 and l.status != C_STATUS_TRASH
               order by decode(l.queuetype, C_C_QUEUE_TYPE_IN, 0, 1)) -- Приоритет сообщение in 
       where rownum < 2;
    end if;
    return vr_log;
  exception
    when no_data_found then
      return vr_log;
  end;

  -- функция возвращает ответ на запрос 
  function get_answer_msg(p_msgid        itt_q_message_log.msgid%type
                         ,p_last_in_tree boolean default false -- Последний в дереве запросов - ответов
                          ) return itt_q_message_log%rowtype is
    vr_log         itt_q_message_log%rowtype;
    v_last_in_tree boolean := nvl(p_last_in_tree, false);
  begin
    if v_last_in_tree
    then
      select *
        into vr_log
        from itt_q_message_log
       where rowid = (select r_id
                        from (select r_id
                                from (select max(level) over() as lv_max
                                            ,level as lv
                                            ,rowid as r_id
                                            ,message_type
                                            ,enqdt
                                        from itt_q_message_log t
                                      connect by corrmsgid = prior msgid
                                             and delivery_type = prior delivery_type
                                             and queuetype = decode(prior queuetype, C_C_QUEUE_TYPE_OUT, C_C_QUEUE_TYPE_IN, C_C_QUEUE_TYPE_OUT)
                                             and status != C_STATUS_TRASH
                                       start with status != C_STATUS_TRASH
                                              and msgid = p_msgid
                                              and message_type = C_C_MSG_TYPE_R
                                              and ((queuetype = C_C_QUEUE_TYPE_IN and receiver = C_C_SYSTEMNAME) or (queuetype = C_C_QUEUE_TYPE_OUT and receiver != C_C_SYSTEMNAME)))
                               where lv_max = lv
                                 and message_type = C_C_MSG_TYPE_A
                               order by enqdt desc)
                       where rownum < 2);
    else
      select *
        into vr_log
        from itt_q_message_log
       where (corrmsgid, delivery_type) =
             (select msgid
                    ,delivery_type
                from itt_q_message_log l
               where status != C_STATUS_TRASH
                 and msgid = p_msgid
                 and message_type = C_C_MSG_TYPE_R
                 and ((queuetype = C_C_QUEUE_TYPE_IN and receiver = C_C_SYSTEMNAME) or (queuetype = C_C_QUEUE_TYPE_OUT and receiver != C_C_SYSTEMNAME)))
         and status != C_STATUS_TRASH
         and ((queuetype = C_C_QUEUE_TYPE_IN and receiver = C_C_SYSTEMNAME) or (queuetype = C_C_QUEUE_TYPE_OUT and receiver != C_C_SYSTEMNAME))
         and message_type = C_C_MSG_TYPE_A
         and rownum < 2
       order by enqdt desc;
    end if;
    return vr_log;
  exception
    when no_data_found then
      return vr_log;
  end;

  -- функция список дерево запросов-ответoв  
  function select_answer_msg(p_msgid     itt_q_message_log.msgid%type
                            ,p_queuetype itt_q_message_log.queuetype%type default null -- C_C_QUEUE_TYPE_IN,C_C_QUEUE_TYPE_OUT
                             ) return tt_q_message_log
    pipelined is
  begin
    if p_queuetype is null
       or p_queuetype = C_C_QUEUE_TYPE_IN -- Быстро возвращает список ( не все сообщения)  для анализа ошибок 
    then
      for cur in (select *
                    from itt_q_message_log t
                  connect by corrmsgid = prior msgid
                         and delivery_type = prior delivery_type
                         and queuetype = decode(receiver, C_C_SYSTEMNAME, C_C_QUEUE_TYPE_IN, C_C_QUEUE_TYPE_OUT)
                         and status != C_STATUS_TRASH
                   start with status != C_STATUS_TRASH
                          and msgid = p_msgid
                          and message_type = C_C_MSG_TYPE_R
                          and ((queuetype = C_C_QUEUE_TYPE_IN and receiver = C_C_SYSTEMNAME) or (queuetype = C_C_QUEUE_TYPE_OUT and receiver != C_C_SYSTEMNAME))
                   order by t.enqdt desc)
      loop
        pipe row(cur);
      end loop;
    elsif p_queuetype = C_C_QUEUE_TYPE_OUT -- Возвращает полный список 
    then
      for cur in (select *
                    from itt_q_message_log t
                  connect by corrmsgid = prior msgid
                         and status != C_STATUS_TRASH
                         and queuetype = (select min(queuetype)
                                            from itt_q_message_log q
                                           where q.msgid = t.msgid
                                             and q.status != C_STATUS_TRASH)
                   start with t.log_id = (select t.log_id
                                            from itt_q_message_log t
                                           where status != C_STATUS_TRASH
                                             and msgid = p_msgid
                                             and message_type = C_C_MSG_TYPE_R
                                           order by t.queuetype
                                           fetch first rows only)
                   order by t.enqdt desc)
      loop
        pipe row(cur);
      end loop;
    else
      raise_application_error(-20000, 'Ошибка параметра функции select_answer_msg  p_queuetype in (C_C_QUEUE_TYPE_IN,C_C_QUEUE_TYPE_OUT) !');
    end if;
  end;

  -- Возвращает статус процесса 
  function get_process_status(p_msgid    itt_q_message_log.msgid%type
                             ,o_dtstatus out timestamp
                             ,o_comment  out varchar2) return varchar2 as
    v_state number;
  begin
    select t_state
          ,dt_state
          ,'[' || servicename || ']: ' || nvl(it_q_message.get_errtxt(commenttxt), status)
      into v_state
          ,o_dtstatus
          ,o_comment
      from (select t_state
                  ,decode(t_state, -1, min(statusdt) over(partition by t_state), max(statusdt) over()) dt_state
                  ,commenttxt
                  ,servicename
                  ,status
                  ,statusdt
              from (select it_q_message.chk_process_state(p.status) as t_state
                          ,p.*
                      from (select *
                              from table(it_q_message.select_answer_msg(p_msgid => p_msgid, p_queuetype => C_C_QUEUE_TYPE_OUT))
                             where message_type = C_C_MSG_TYPE_R
                                or queuetype = C_C_QUEUE_TYPE_OUT) p)
             order by t_state
                     ,dt_state desc)
     where statusdt = dt_state
       and rownum < 2;
    case
      when v_state < 0 then
        return C_KIND_STATUS_ERROR;
      when v_state = 0 then
        return C_KIND_STATUS_WORK;
      else
        return C_KIND_STATUS_DONE;
    end case;
  exception
    when no_data_found then
      return null;
  end;

  -- Процедура вычитки сообщения из исходящей очереди
  procedure dequeue_outmessage(p_queuename   itt_q_message_log.queuename%type -- Имя очереди 
                              ,p_qmsgid      raw default null -- qmsgid GUID сообщения в очереди
                              ,p_correlation varchar2 default null -- correlation сообщения в очереди
                              ,p_wait        number default 60 -- ожидание в секундах
                              ,o_message     out it_q_message_t
                              ,o_errno       out integer
                              ,o_errmsg      out varchar2) is
    v_qmsgid      raw(16) := p_qmsgid;
    v_queuename itt_q_message_log.queuename%type := case
                                                      when p_queuename is null
                                                           and get_count_queue = 1 then
                                                       C_C_QUEUE_OUT_PREFIX || t_queue_num(1)
                                                      else
                                                       p_queuename
                                                    end;
    v_wait        number := GREATEST(0, nvl(p_wait, 60));
    v_correlation varchar2(128);
  begin
    if p_queuename is not null
       and get_queuetype(p_queuename => p_queuename) != C_C_QUEUE_TYPE_OUT
    then
      raise_application_error(-20000, 'Очередь ' || p_queuename || ' должна быть исходящей ');
    end if;
    o_errno := 1;
    begin
      msg_dequeue(p_qmsgid => v_qmsgid
                 ,p_correlation => p_correlation
                 ,p_queuetype => C_C_QUEUE_TYPE_OUT
                 ,p_queue_num => get_queue_num(v_queuename)
                 ,p_wait => v_wait
                 ,o_qmsgid => v_qmsgid
                 ,o_correlation => v_correlation
                 ,o_message => o_message);
      o_errno  := 0;
      o_errmsg := null;
    exception
      when others then
        o_errno  := abs(sqlcode);
        o_errno := case
                     when o_errno = 20228 then
                      25228
                     else
                      o_errno
                   end;
        o_errmsg := get_errtxt(sqlerrm);
    end;
  end;

begin
  refresh_spr;
  if t_queue_num_parallel.COUNT = 0
  then
    gn_find_queue_num_last := 1;
  else
    gn_find_queue_num_last := round(dbms_random.value(1 - 0.49, t_queue_num_parallel.COUNT + 0.49)); -- Начинаем с любой очереди
  end if;
  gn_next_queue_num_last := round(dbms_random.value(1 - 0.49, t_queue_num.COUNT + 0.49)); -- Начинаем с любой очереди
  thread_init;
end;
/
