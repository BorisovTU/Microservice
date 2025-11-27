create or replace package body it_q_manager is

  /**************************************************************************************************\
    BIQ-9225. Разработка очереди и журнала событий
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
    06.05.2025  Зыков М.В.       CCBO-11849                       Создание механизма нового типа очередей
    23.10.2023  Зыков М.В.       BOSS-1230                        BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
    15.05.2023  Зыков М.В.       CCBO-4870                        BIQ-13171. Разработка процедур работы с очередью событий
    19.09.2022  Зыков М.В.       BIQ-9225                         PManager + настройки
    13.09.2022  Зыков М.В.       BIQ-9225                         Оптимизация параллельной обработки очередей
    09.09.2022  Зыков М.В.       BIQ-9225                         +show_qsettings
    19.08.2022  Зыков М.В.       BIQ-9225                         Изменение формата сообщения об ошибке
    03.08.2022  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/
  --
  -- Менеджер обработки очередей сообщений
  --
  gс_manager_queue_num      char(2); -- Номер очереди обслужимания MANAGER 
  gn_is_pmanager             integer; -- =1 если PManager 
  gn_worker_num              integer; -- Номер Workera 
  gс_result_pipe_channel    char(2); -- Имя PIPE канала ответов воркера
  gс_userenv_current_schema varchar(100) := sys_context('userenv', 'current_schema');

  --
  gd_disassembly            date; -- Время последней очистки очереди 
  gd_last_worker_restart    date; -- Последняя команда на перезагрузку работников
  gd_last_init_qsettings    date; -- Последнее обновление настроек
  gd_last_worker_mark       date; -- Проверка работников
  gd_last_task_transfer     date; -- Перенос заданий
  gd_last_pipemanager_start date; -- Проверка на запуск PManagera
  gd_last_flush_services    timestamp; -- Обновление списка сервисов
  --
  C_WORKER_RESPONSE_FREE  constant char(1) := 'F'; --
  C_WORKER_RESPONSE_RUN   constant char(1) := 'N'; --
  C_WORKER_RESPONSE_START constant char(1) := 'S'; --
  --
  С_RESULT_PIPE_PREFIX           constant varchar2(32) := 'it_pipe_result'; --  префикс имени pipe для ответов WORKERов
  С_WORKER_PIPE_PREFIX           constant varchar2(32) := 'it_pipe_worker'; --  префикс имени pipe для команд WORKERу
  С_JOB_PMANAGER_PREFIX          constant varchar2(32) := 'IT_P_MANAGER'; -- префикс имени PIPE Manageraов
  С_JOB_WORKER_PREFIX            constant varchar2(32) := 'IT_Q_WORKER'; --  префикс имени джоба для WORKERов
  С_JOB_XWORKER_PREFIX           constant varchar2(32) := 'IT_Q_XWORKER'; --  префикс имени джоба для WORKERов табличной очереди
  С_LOCK_PROCESS_WORKERS_FLUSH   constant varchar2(32) := 'it_q_manager.workers_flush'; --
  С_LOCK_PROCESS_XQMANAGER_START constant varchar2(32) := 'it_q_manager.XQManager_start'; --
  С_LOCK_PROCESS_XWORKER_START   constant varchar2(32) := 'it_q_manager.Xworker_start';

  GN_QUEUE_WAIT             constant integer := 5; --  Время сек ожидания событий 
  GN_QMANAGER_REPEAT_WORKER constant integer := 50; --  Кол-во повторов ожиданий события . При превышении выдается exception
  С_CORRID_QR_PREFIX       constant char(3) := 'R__'; -- Префикс коррid для любого запроса
  GN_RESULT_MAXPIPESIZE     constant integer := 8 * 1024 * 1024; --
  GN_WORKER_MAXPIPESIZE     constant integer := 256 * 1024;

  --Настройки QMANAGER (переодически обновляются в процессе работы)
  gb_debug_enabled boolean := false; --отладка включена?
  --gc_debug_enabled_qset constant varchar2(100) := 'QMANAGER_DEBUG';
  gn_messpack_count integer := 100; --  Размер пакета обработки сообщений QManager ( одно N сообщение гарантировано пройдет в 1 пакет, проверка SF не чаще пакета) 
  GC_MESSPACK_COUNT_QSET constant varchar2(100) := 'QMANAGER_MESSPACK_COUNT'; -- доступно настроить для каждой очереди
  GN_MESSPACK_COUNT_MIN  constant integer := 5; --  Ограничение  
  GN_MESSPACK_COUNT_MAX  constant integer := null; -- Если нет SF (быcтрых синхрон) сообщений лучше установить большое число 
  --
  gn_norm_run_coins_apostq integer := 5000; --  Нормальная цена пост очереди асинхронных сообщений  
  GC_NORM_RUN_COINS_APOSTQ_QSET constant varchar2(100) := 'QMANAGER_NPRICE_APOSTQ'; -- доступно настроить для каждой очереди
  GN_NORM_RUN_COINS_APOSTQ_MIN  constant integer := 0; --  Ограничение - Пост очередь не образутся
  GN_NORM_RUN_COINS_APOSTQ_MAX  constant integer := null; -- 
  --
  gn_period_disassembly integer := 20; -- Проверка заданий у работников каждые сек
  GC_PERIOD_DISASSEMBLY_QSET constant varchar2(100) := 'QMANAGER_DISASSEMBLY'; --
  GN_PERIOD_DISASSEMBLY_MIN  constant integer := 5; --  Ограничение
  GN_PERIOD_DISASSEMBLY_MAX  constant integer := 60 * 60;

  gn_period_clear_log integer := 60 * 5; -- Очистка лог таблиц каждые сек
  GC_PERIOD_CLEAR_LOG_QSET constant varchar2(100) := 'QMANAGER_CLEAR_LOG'; --
  GN_PERIOD_CLEAR_LOG_MIN  constant integer := 5; --  Ограничение
  GN_PERIOD_CLEAR_LOG_MAX  constant integer := 60 * 60 * 24;

  gn_message_log_hist integer := 60; -- Срок хранения сообщений в ITT_Q_MESSAGE_LOG (дн.) 
  GC_MESSAGE_LOG_HIST_QSET constant varchar2(100) := 'QMANAGER_LOG_HIST'; --
  GN_MESSAGE_LOG_HIST_MIN  constant integer := 7; --
  GN_MESSAGE_LOG_HIST_MAX  constant integer := null;

  gn_service_max_running integer := 60 * 60 * 24; --  Максимальная продолжительность выполнения сервиса (сек) (по умолчанию)
  GC_SERVICE_MAX_RUNNING_QSET constant varchar2(100) := 'QSERVICE_MAX_RUN'; --
  GN_SERVICE_MAX_RUNNING_MIN  constant integer := 60 * 10; --  Ограничение
  GN_SERVICE_MAX_RUNNING_MAX  constant integer := 60 * 60 * 24 * 3;

  gn_mincount_free_for_s integer := 1; --  Минимальное кол-во свободных workerав оставленных для синхронных сообщений (Заполняем свободные воркеры асинхроном до ... последних)
  GC_MINCOUNT_FREE_FOR_S_QSET constant varchar2(100) := 'QWORKER_MINFREE_FOR_S'; --
  GN_MINCOUNT_FREE_FOR_S_MIN  constant integer := 0; --  Ограничение . Если синхронов нет
  GN_MINCOUNT_FREE_FOR_S_MAX  constant integer := 20;

  gn_in_as_expiration integer := 2 * 24 * 60 * 60; -- Время жизни входящих синхронных ответов (сек)
  GC_IN_AS_EXPIRATION_QSET constant varchar2(100) := 'QMESSAGE_IN_AS_EXPIRATION'; --
  GN_IN_AS_EXPIRATION_MIN  constant integer := 60; --  Ограничение
  GN_IN_AS_EXPIRATION_MAX  constant integer := 30 * 24 * 60 * 60;

  gn_out_s_expiration integer := 60 * 60; -- Время жизни исходящих синхронных сообщений (сек)
  GC_OUT_S_EXPIRATION_QSET constant varchar2(100) := 'QMESSAGE_OUT_S_EXPIRATION'; --
  GN_OUT_S_EXPIRATION_MIN  constant integer := 10; --  Ограничение
  GN_OUT_S_EXPIRATION_MAX  constant integer := 24 * 60 * 60;

  gn_out_A_expiration integer := 24 * 60 * 60; -- Время жизни исходящих асинхронных сообщений (сек)
  GC_OUT_A_EXPIRATION_QSET constant varchar2(100) := 'QMESSAGE_OUT_A_EXPIRATION'; --
  GN_OUT_A_EXPIRATION_MIN  constant integer := 10; --  Ограничение
  GN_OUT_A_EXPIRATION_MAX  constant integer := null;

  gn_worker_plus_force integer := 1; --  Коэффициент опережения . при нехватке воркеров будет поддерживаться опережающее добавление если > 1
  GC_WORKER_PLUS_FORCE_QSET constant varchar2(100) := 'QWORKER_PLUS_FORCE'; --
  GN_WORKER_PLUS_FORCE_MIN  constant integer := 1; --  Ограничение
  GN_WORKER_PLUS_FORCE_MAX  constant integer := 10;

  gn_worker_plus_interval integer := 0; --  Добавление работников не чаще (сек)
  GC_WORKER_PLUS_INTERVAL_QSET constant varchar2(100) := 'QWORKER_PLUS_INTERVAL'; --
  GN_WORKER_PLUS_INTERVAL_MIN  constant integer := 0; --  Ограничение
  GN_WORKER_PLUS_INTERVAL_MAX  constant integer := 60;

  gn_worker_minus_interval integer := 10; --  Отключение работников не чаще (сек)
  GC_WORKER_MINUS_INTERVAL_QSET constant varchar2(100) := 'QWORKER_MINUS_INTERVAL'; --
  GN_WORKER_MINUS_INTERVAL_MIN  constant integer := 1; --  Ограничение
  GN_WORKER_MINUS_INTERVAL_MAX  constant integer := null;

  gn_worker_restart_interval integer := 120; --  Рестарт работников каждые (мин)
  GC_WORKER_RESTART_INTERVAL_QSET constant varchar2(100) := 'QWORKER_RESTART_INTERVAL'; --
  GN_WORKER_RESTART_INTERVAL_MIN  constant integer := 10; --  Ограничение
  GN_WORKER_RESTART_INTERVAL_MAX  constant integer := null;

  --
  gn_worker_f_count integer := 1; --  Кол-во обработчиков для SF сообщений (gn_mincount_free_for_s > 0 and gn_worker_f_count > 0)- Включение обработки SF
  GC_WORKER_F_COUNT_QSET constant varchar2(100) := 'QWORKER_F_COUNT'; --
  GN_WORKER_F_COUNT_MIN  constant integer := 0; --  Ограничение
  GN_WORKER_F_COUNT_MAX  constant integer := 50;

  gn_worker_max_count integer := 56; --  Максимальное кол-во обработчиков  
  GC_WORKER_MAX_COUNT_QSET constant varchar2(100) := 'QWORKER_MAX_COUNT'; --
  gn_worker_min_count integer := gn_worker_f_count + gn_mincount_free_for_s + 1; -- Минимальное QWORKER_F_COUNT +QWORKER_MINFREE_FOR_S + 1
  GN_WORKER_MAX_COUNT_MAX constant integer := 99999;

  gn_service_startprice integer := 10; -- Цена прохождения до воркера 
  GC_SERVICE_STARTPRICE_QSET constant varchar2(100) := 'QSERVICE_STARTPRICE'; --
  GN_SERVICE_STARTPRICE_MIN  constant integer := 1; --
  GN_SERVICE_STARTPRICE_MAX  constant integer := 20;

  gn_pmanager_start_cnt_task integer := 10; -- Если заданий в ожидании > ... старт PMANAGERA ( если очередей = 1 )
  GC_PMANAGER_START_CNT_TASK_QSET constant varchar2(100) := 'PMANAGER_START_CNT'; -- 
  GN_PMANAGER_START_CNT_TASK_MIN  constant integer := 0; --  Ограничение. Всегда запущен <=1.
  GN_PMANAGER_START_CNT_TASK_MAX  constant integer := 999; --  Не запускать .
  --
  gn_pmanager_count integer := 1; -- Максимальное кол-во PMANAGERов ( в любом варианте не будет больше кол-ва очередей)
  GC_PMANAGER_COUNT_QSET constant varchar2(100) := 'PMANAGER_COUNT'; -- 
  GN_PMANAGER_COUNT_MIN  constant integer := 0; --  Ограничение. Совпадает с количеством очередей.
  GN_PMANAGER_COUNT_MAX  constant integer := null; --  Совпадает с количеством очередей .
  --
  gn_max_msec_flush_pipe integer := 5; -- Максимальное время вычитывания PIPE канала за одну итерацию при обновлении работников (mc)  
  GC_MAX_MSEC_FLUSH_PIPE_QSET constant varchar2(100) := 'QMANAGER_TMAX_FLUSH_PIPE'; -- доступно настроить для каждого PIPE канала ( очереди )
  GN_MAX_MSEC_FLUSH_PIPE_MIN  constant integer := 0; -- тогда по gn_min_cnt_flush_pipe
  GN_MAX_MSEC_FLUSH_PIPE_MAX  constant integer := 100;

  gn_min_cnt_flush_pipe integer := 50; -- Минимальное кол-во сообщений вычитывания PIPE канала за одну итерацию ( Для исключения переполнения PIPE из за малого значения gn_max_msec_flush_pipe ) 
  GC_MIN_CNT_FLUSH_PIPE_QSET constant varchar2(100) := 'QMANAGER_CMIN_FLUSH_PIPE'; -- доступно настроить для  каждого PIPE канала ( очереди )
  GN_MIN_CNT_FLUSH_PIPE_MIN  constant integer := 5; --
  GN_MIN_CNT_FLUSH_PIPE_MAX  constant integer := null;

  --gn_queueXX_dequeue_sleep number := 0.1 ; -- Задержка в сек между повторами опросов табличной очереди при вычитке с ожиданием  
  GC_QUEUEXX_DEQUEUE_SLEEP_QSET constant varchar2(100) := 'QUEUEXX_DEQUEUE_SLEEP'; --
  GN_QUEUEXX_DEQUEUE_SLEEP_MIN  constant number := 1 / 100; --
  GN_QUEUEXX_DEQUEUE_SLEEP_MAX  constant number := 1;

  --gn_queueXX_max_retries integer  := 99999 ; -- Кол-во попыток вычиток сообщения перед сменой статуса ( отменой)   
  GC_QUEUEXX_MAX_RETRIES_IN_QSET constant varchar2(100) := 'QUEUEXX_MAX_RETRIES_IN'; --
  GN_QUEUEXX_MAX_RETRIES_IN_MIN  constant integer := 1; --
  GN_QUEUEXX_MAX_RETRIES_IN_MAX  constant integer := 99999;

  GC_QUEUEXX_MAX_RETRIES_OUT_QSET constant varchar2(100) := 'QUEUEXX_MAX_RETRIES_OUT'; --
  GN_QUEUEXX_MAX_RETRIES_OUT_MIN  constant integer := 1; --
  GN_QUEUEXX_MAX_RETRIES_OUT_MAX  constant integer := 99999;

  --gn_queueXX_retry_delay_S number  := 2 ; -- Задержка в сек повторной вычитки S сообщения    
  GC_QUEUEXX_RETRY_DELAY_IN_S_QSET constant varchar2(100) := 'QUEUEXX_RETRY_DELAY_IN_S'; --
  GN_QUEUEXX_RETRY_DELAY_IN_S_MIN  constant integer := 1; --
  GN_QUEUEXX_RETRY_DELAY_IN_S_MAX  constant integer := 60 * 60;

  GC_QUEUEXX_RETRY_DELAY_OUT_S_QSET constant varchar2(100) := 'QUEUEXX_RETRY_DELAY_OUT_S'; --
  GN_QUEUEXX_RETRY_DELAY_OUT_S_MIN  constant integer := 1; --
  GN_QUEUEXX_RETRY_DELAY_OUT_S_MAX  constant integer := 60 * 60;

  --gn_queueXX_retry_delay_A number  := 60 ; -- Задержка в сек повторной вычитки A сообщения    
  GC_QUEUEXX_RETRY_DELAY_IN_A_QSET constant varchar2(100) := 'QUEUEXX_RETRY_DELAY_IN_A'; --
  GN_QUEUEXX_RETRY_DELAY_IN_A_MIN  constant integer := 1; --
  GN_QUEUEXX_RETRY_DELAY_IN_A_MAX  constant integer := 60 * 60;

  GC_QUEUEXX_RETRY_DELAY_OUT_A_QSET constant varchar2(100) := 'QUEUEXX_RETRY_DELAY_IN_A'; --
  GN_QUEUEXX_RETRY_DELAY_OUT_A_MIN  constant integer := 1; --
  GN_QUEUEXX_RETRY_DELAY_OUT_A_MAX  constant integer := 60 * 60;

  gn_queueXX_Qmanager_sleep number := 5 / 10; -- Задержка в сек между повторами опросов табличной очереди QMANAGERXX  
  GC_QUEUEXX_QMANAGER_SLEEP_QSET constant varchar2(100) := 'QUEUEXX_QMANAGER_SLEEP'; --
  GN_QUEUEXX_QMANAGER_SLEEP_MIN  constant number := 1 / 10; --
  GN_QUEUEXX_QMANAGER_SLEEP_MAX  constant number := 5;

  gn_queueXX_QWorker_sleep number := 1 / 10; -- Задержка в сек между повторами опросов табличной очереди XWorker  
  GC_QUEUEXX_QWORKER_SLEEP_QSET constant varchar2(100) := 'QUEUEXX_QWORKER_SLEEP'; --
  GN_QUEUEXX_QWORKER_SLEEP_MIN  constant number := 0; --
  GN_QUEUEXX_QWORKER_SLEEP_MAX  constant number := 5;

  gn_queueXX_QWorker_massstart number := 4; -- Максимальное кол-во одновременно запускаемых XWorker  
  GC_QUEUEXX_QWORKER_MASSSTART_QSET constant varchar2(100) := 'QUEUEXX_QWORKER_MASSSTART'; --
  GN_QUEUEXX_QWORKER_MASSSTART_MIN  constant number := 1; --
  GN_QUEUEXX_QWORKER_MASSSTART_MAX  constant number := 100;

  --
  -- Для приблизительной оценки цены сервиса 
  gn_service_done_price  integer := 10; -- Цена загрузки сообщения
  gn_service_error_price integer := 10; -- Цена ответа об отсутствии сервиса
  GN_SERVICE_KFAVG constant integer := 10; -- Коэффициент усреднения расчета цены 
  ---
  t_pipe_channel tt_pipe_channel := tt_pipe_channel(); -- Список pipe каналов ( менеджеров)
  --
  type t_service_tt is table of itt_q_service%rowtype index by varchar2(150); -- Список сервисов.
  gt_service t_service_tt;

  type t_lock_process_tt is table of varchar2(128) index by varchar2(128); -- Список блокировок  
  gt_lock_process t_lock_process_tt;

  --------------------------------------------------------------------------------
  -- Отправка сообщений в отладочный канал
  /*procedure debug(p_msg  varchar2
                 ,p_clob clob default null) is
    --    tmp integer;
  begin
    if gb_debug_enabled
    then
      it_information.store_info(p_info_type => 'DEBUG'
                               ,p_info_title => to_char(mod(extract(second from systimestamp), 1) * 1000000, '099999') || '-' || p_msg
                                --,p_info_content => sys.dbms_utility.format_error_backtrace
                                );
      -- it_log.log(to_char(mod(extract(second from systimestamp), 1) * 1000000, '099999') || '-' || p_msg);
    end if;
  exception
    when others then
      null;
  end;*/
  --------------------------------------------------------------------------------
  -- Возвращает значение пакетной переменной по имени
  function get_constant_str(p_constant varchar2) return varchar2 deterministic as
    v_ret varchar2(32676);
  begin
    execute immediate ' begin :1 := it_q_manager.' || p_constant || '; end;'
      using out v_ret;
    return v_ret;
  exception
    when others then
      return null;
  end;

  function get_gn_message_log_hist return integer as
  begin
    return gn_message_log_hist;
  end;

  --==============================================================================
  -- Обновление списка pipe каналов.
  procedure refresh_pipe_channel as
  begin
    select pipe_channel
      bulk collect
      into t_pipe_channel
      from (select *
              from (select distinct case
                                      when regexp_like(column_value, '^[0-9]+$') then
                                       0
                                      else
                                       1
                                    end as target
                                   ,column_value pipe_channel
                      from table(it_q_message.select_queue_num)
                     where column_value != it_q_message.C_C_QUEUENUM_XX)
             order by target
                     ,pipe_channel)
     where rownum <= decode(gn_pmanager_count, 0, rownum, gn_pmanager_count);
  end;

  -- Обновление статичных справочников в коллекции
  procedure refresh_spr(p_refresh_pipe_channel boolean default false) as
  begin
    it_q_message.refresh_spr(true);
    if p_refresh_pipe_channel
    then
      refresh_pipe_channel;
    end if;
  end;

  -- Табличная функция получения списка pipe каналов
  function select_pipe_channel return tt_pipe_channel
    pipelined as
  begin
    for i in 1 .. t_pipe_channel.COUNT
    loop
      pipe row(t_pipe_channel(i));
    end loop;
  end;

  function next_pipe_channel return itt_q_worker.pipe_channel%type as
    v_ret itt_q_worker.pipe_channel%type;
  begin
    if t_pipe_channel.count = 1
    then
      return t_pipe_channel(1);
    end if;
    select pipe_channel
      into v_ret
      from (select *
              from (select pipe_channel.column_value pipe_channel
                          ,count(w.pipe_channel) as cnt
                      from table(it_q_manager.select_pipe_channel) pipe_channel
                      left join itt_q_worker w
                        on pipe_channel.column_value = w.pipe_channel
                       and job_stoptime is null
                     group by pipe_channel.column_value)
             order by cnt
                     ,pipe_channel)
     where rownum < 2;
    return v_ret;
  end;

  --==============================================================================
  -- Инициализация режима вывода отладки
  procedure init_debug is
    tmp pls_integer;
  begin
    --    select coalesce(max(1), 0) into tmp from dual where upper(it_q_message.get_qset_char(gc_debug_enabled_qset)) in ('1', 'TRUE', 'YES', 'Y', 'ON');
    gb_debug_enabled := true; --false; --tmp = 1;
  exception
    when others then
      null;
  end;

  ---------------------------------------------------------------------------------------------------------
  -- Формирование сообщения о работе Workera
  function mess_work(p_channel      varchar2
                    ,p_command      char
                    ,p_param        varchar2 default null
                    ,p_systimestamp timestamp default systimestamp) return varchar2 as
  begin
    return p_channel || '#' || it_xml.timestamp_to_char_iso8601(p_systimestamp) || '#' || p_command || case when p_param is not null then '#' || p_param end;
  end;

  --=======================================================================================================
  -- Процедура сохраняет настройку типа varchar2
  procedure set_qset_char(p_qset_name itt_q_settings.qset_name%type
                         ,p_char      varchar2) is
    pragma autonomous_transaction;
  begin
    update itt_q_settings s set s.value_varchar = p_char where s.qset_name = p_qset_name;
    if sql%rowcount = 0
    then
      insert into itt_q_settings
        (qset_name
        ,value_varchar)
      values
        (p_qset_name
        ,p_char);
    end if;
    commit; -- AUTONOMOUS
  end;

  -- Процедура сохраняет настройку типа number
  procedure set_qset_number(p_qset_name itt_q_settings.qset_name%type
                           ,p_number    number) is
    pragma autonomous_transaction;
  begin
    update itt_q_settings s set s.value_number = p_number where s.qset_name = p_qset_name;
    if sql%rowcount = 0
    then
      insert into itt_q_settings
        (qset_name
        ,value_number)
      values
        (p_qset_name
        ,p_number);
    end if;
    commit; -- AUTONOMOUS
  end;

  ---------------------------------------------------------------------------------------------------
  -- Процедура устанавливает влаг на исходное сообщение для асинхронных ответов (для исключения ошибок дублирования ответов )
  procedure flag_corr_is_work(p_msgid   varchar2
                             ,p_set     integer -- 1 - Установить / 0 - снять(не реализовано )
                             ,o_errno   in out integer --  Ошибка если < 0
                             ,p_comment varchar2 default null) is
    pragma autonomous_transaction;
    v_i_res integer;
  begin
    if p_set = 1
    then
      begin
        update itt_q_message_log l
           set l.status = case
                            when o_errno = 0 then
                             IT_Q_MESSAGE.C_STATUS_ANSWER
                            else
                             IT_Q_MESSAGE.C_STATUS_ERRANSWER
                          end
              ,l.statusdt   = systimestamp
              ,l.commenttxt = it_q_message.get_comment_add(p_comenttxt => l.commenttxt, p_msgcode => o_errno, p_add_comment => p_comment)
         where l.message_type = IT_Q_MESSAGE.C_C_MSG_TYPE_R
           and l.delivery_type = IT_Q_MESSAGE.C_C_MSG_DELIVERY_A
           and (l.status in (select * from table(it_q_message.select_status(IT_Q_MESSAGE.C_KIND_STATUS_SEND))) or l.status = IT_Q_MESSAGE.C_STATUS_ERRANSWER)
           and l.msgid = p_msgid
           and l.queuetype = IT_Q_MESSAGE.C_C_QUEUE_TYPE_OUT;
        -- Допускаем получение асинхронных ответов на синхронные сообщения в статусе ANSWER ( рассылка подтверждений)
        if sql%rowcount = 0
        then
          begin
            select 0
              into v_i_res
              from itt_q_message_log l
             where l.message_type = IT_Q_MESSAGE.C_C_MSG_TYPE_R -- 'R'
               and l.delivery_type = IT_Q_MESSAGE.C_C_MSG_DELIVERY_S -- S
               and l.status = IT_Q_MESSAGE.C_STATUS_ANSWER
               and l.msgid = p_msgid
               and l.queuetype = IT_Q_MESSAGE.C_C_QUEUE_TYPE_OUT;
          exception
            when no_data_found then
              o_errno := -1;
          end;
        elsif sql%rowcount != 1
        then
          rollback;
          o_errno := -1;
        end if;
      exception
        when others then
          rollback;
          o_errno := -1;
      end;
    else
      raise_application_error(-20500, 'Ошибка параметров flag_corr_is_work');
    end if;
    commit;
  end;

  --------------------------------------------------------------------------------
  -- Получение списка QSETTINGS со знвчениями
  function show_qsettings return clob as
  begin
    return 'Настройки очереди ' || gс_manager_queue_num || chr(10) || chr(10) || --
    GC_MESSPACK_COUNT_QSET || ' = ' || gn_messpack_count || chr(10) || --
    GC_NORM_RUN_COINS_APOSTQ_QSET || ' = ' || gn_norm_run_coins_apostq || chr(10) || --
    GC_PERIOD_DISASSEMBLY_QSET || ' = ' || gn_period_disassembly || chr(10) || --
    GC_PERIOD_CLEAR_LOG_QSET || ' = ' || gn_period_clear_log || chr(10) || --
    GC_MESSAGE_LOG_HIST_QSET || ' = ' || gn_message_log_hist || chr(10) || --
    GC_SERVICE_MAX_RUNNING_QSET || ' = ' || gn_service_max_running || chr(10) || --
    GC_MINCOUNT_FREE_FOR_S_QSET || ' = ' || gn_mincount_free_for_s || chr(10) || --
    GC_IN_AS_EXPIRATION_QSET || ' = ' || gn_in_as_expiration || chr(10) || -- 
    GC_OUT_A_EXPIRATION_QSET || ' = ' || gn_out_a_expiration || chr(10) || -- 
    GC_OUT_S_EXPIRATION_QSET || ' = ' || gn_out_S_expiration || chr(10) || -- 
    GC_WORKER_F_COUNT_QSET || ' = ' || gn_worker_f_count || chr(10) || --  
    GC_WORKER_PLUS_FORCE_QSET || ' = ' || gn_worker_plus_force || chr(10) || --
    GC_WORKER_PLUS_INTERVAL_QSET || ' = ' || gn_worker_plus_interval || chr(10) || --
    GC_WORKER_MINUS_INTERVAL_QSET || ' = ' || gn_worker_minus_interval || chr(10) || --
    GC_WORKER_RESTART_INTERVAL_QSET || ' = ' || gn_worker_restart_interval || chr(10) || --
    GC_SERVICE_STARTPRICE_QSET || ' = ' || gn_service_startprice || chr(10) || --
    GC_PMANAGER_COUNT_QSET || ' = ' || gn_pmanager_count || chr(10) || --
    GC_PMANAGER_START_CNT_TASK_QSET || ' = ' || gn_pmanager_start_cnt_task || chr(10) || --
    GC_MAX_MSEC_FLUSH_PIPE_QSET || ' = ' || gn_max_msec_flush_pipe || chr(10) || --
    GC_MIN_CNT_FLUSH_PIPE_QSET || ' = ' || gn_min_cnt_flush_pipe || chr(10) || --
    GC_WORKER_MAX_COUNT_QSET || ' = ' || gn_worker_max_count || chr(10) || chr(10) || --
     'gn_service_done_price = ' || gn_service_done_price || chr(10) || --
     'gn_service_error_price = ' || gn_service_error_price || chr(10) || --
    GC_QUEUEXX_DEQUEUE_SLEEP_QSET || ' = ' || it_q_message.gn_queueXX_dequeue_sleep || chr(10) || -- 
    GC_QUEUEXX_MAX_RETRIES_IN_QSET || ' = ' || it_q_message.gn_queueXX_max_retries_IN || chr(10) || --   
    GC_QUEUEXX_MAX_RETRIES_OUT_QSET || ' = ' || it_q_message.gn_queueXX_max_retries_OUT || chr(10) || --   
    GC_QUEUEXX_RETRY_DELAY_IN_S_QSET || ' = ' || it_q_message.gn_queueXX_retry_delay_IN_S || chr(10) || --  
    GC_QUEUEXX_RETRY_DELAY_OUT_A_QSET || ' = ' || it_q_message.gn_queueXX_retry_delay_OUT_A || chr(10) || --   
    GC_QUEUEXX_RETRY_DELAY_IN_S_QSET || ' = ' || it_q_message.gn_queueXX_retry_delay_IN_S || chr(10) || --  
    GC_QUEUEXX_RETRY_DELAY_OUT_A_QSET || ' = ' || it_q_message.gn_queueXX_retry_delay_OUT_A || chr(10) || --   
    GC_QUEUEXX_QMANAGER_SLEEP_QSET || ' = ' || gn_queueXX_Qmanager_sleep || chr(10) || -- 
    GC_QUEUEXX_QWORKER_SLEEP_QSET || ' = ' || gn_queueXX_QWorker_sleep || chr(10) || -- 
    GC_QUEUEXX_QWORKER_MASSSTART_QSET || ' = ' || gn_queueXX_QWorker_massstart || chr(10) -- 
    ;
  end;

  --------------------------------------------------------------------------------------
  -- Создание сообщений об ошибке
  procedure send_information_error(p_sqlcode varchar2
                                  ,p_sqlerrm varchar2) is
    pragma autonomous_transaction;
    vd_last_rem date;
    vc_sid      varchar2(200);
  begin
    vc_sid      := '$$INFORMATION[' || 'ERR#' || p_sqlcode || ']';
    vd_last_rem := it_q_message.get_qset_data(p_qset_name => vc_sid);
    if vd_last_rem is null
       or sysdate > vd_last_rem + numtodsinterval(5, 'MINUTE')
    then
      it_q_message.set_qset_data(p_qset_name => vc_sid, p_date => sysdate);
      it_information.store_info(p_info_type => 'ERR#' || p_sqlcode, p_info_content => p_sqlerrm);
    end if;
    commit;
  end;

  --------------------------------------------------------------------------------------
  -- Создание информационного сообщения если пройден временной контроль 
  procedure send_information(p_key_info     varchar2
                            ,p_info_content clob default null) is
    vd_last_rem  date;
    vc_sid       varchar2(200);
    v_cl_msg     clob;
    v_info_title itt_information.info_title%type;
    pragma autonomous_transaction;
  begin
    vc_sid      := '$$INFORMATION[' || p_key_info || '-' || gс_manager_queue_num || ']';
    vd_last_rem := it_q_message.get_qset_data(p_qset_name => vc_sid);
    case
      when substr(p_key_info, 1, 16) = 'NOT_FREE_WORKER#' then
        if vd_last_rem is null
           or sysdate > vd_last_rem + numtodsinterval(5, 'MINUTE')
        then
          --debug('Manager' || gс_qmanager_queue_num || ': SEND REMINDER ');
          v_info_title := ' НЕДОСТАТОЧНО AQ-ОБРАБОТЧИКОВ для ' || substr(p_key_info, 17) || ' задания из очереди № ' || gс_manager_queue_num;
          v_cl_msg     := it_information.show_stat_qworkers(p_queue_num => gс_manager_queue_num, p_title => v_info_title);
          it_information.store_info(p_info_type => p_key_info, p_info_title => v_info_title, p_info_content => v_cl_msg);
          it_information.store_info(p_info_type => 'SHOW_QSETTINGS', p_info_title => 'Настройки QManager' || gс_manager_queue_num, p_info_content => show_qsettings);
          it_q_message.set_qset_data(p_qset_name => vc_sid, p_date => sysdate);
        end if;
      when substr(p_key_info, 1, 4) = 'ERR#' then
        if vd_last_rem is null
           or sysdate > vd_last_rem + numtodsinterval(5, 'MINUTE')
        then
          it_q_message.set_qset_data(p_qset_name => vc_sid, p_date => sysdate);
          it_information.store_info(p_info_type => p_key_info, p_info_content => p_info_content);
        end if;
      when substr(p_key_info, 1, 5) in ('STOP#', 'START#') then
        if vd_last_rem is null
           or sysdate > vd_last_rem + numtodsinterval(1, 'MINUTE')
        then
          it_q_message.set_qset_data(p_qset_name => vc_sid, p_date => sysdate);
          it_information.store_info(p_info_type => p_key_info, p_info_content => p_info_content);
        end if;
      else
        it_information.store_info(p_info_type => p_key_info, p_info_content => p_info_content);
    end case;
    commit;
  exception
    when others then
      rollback;
      --debug('SEND_INFORMATION: ' || sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
      send_information_error(p_sqlcode => 'SI' || sqlcode, p_sqlerrm => sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  --------------------------------------------------------------------------------------------
  -- Инициализация установок
  function get_qsetting(p_qset_name itt_q_settings.qset_name%type
                       ,p_valie_def number
                       ,p_valie_min number
                       ,p_valie_max number
                       ,p_valie_q   boolean default false) return number as
    v_value number;
  begin
    if nvl(p_valie_q, false)
    then
      v_value := it_q_message.get_qset_number(p_qset_name || gс_manager_queue_num);
    end if;
    if v_value is null
    then
      v_value := nvl(it_q_message.get_qset_number(p_qset_name), p_valie_def);
    end if;
    return case when v_value >= nvl(p_valie_min, v_value) and v_value <= nvl(p_valie_max, v_value) then v_value else p_valie_def end;
  end;

  procedure init_qsettings is
  begin
    if gd_last_init_qsettings is null
       or sysdate > gd_last_init_qsettings + numtodsinterval(GN_QUEUE_WAIT, 'SECOND')
    then
      gd_last_init_qsettings := sysdate;
      -- При изменении списка добавить изменения в show_qsettings
      gn_messpack_count                         := get_qsetting(GC_MESSPACK_COUNT_QSET, gn_messpack_count, GN_MESSPACK_COUNT_MIN, GN_MESSPACK_COUNT_MAX, true);
      gn_norm_run_coins_apostq                  := get_qsetting(GC_NORM_RUN_COINS_APOSTQ_QSET
                                                               ,gn_norm_run_coins_apostq
                                                               ,GN_NORM_RUN_COINS_APOSTQ_MIN
                                                               ,GN_NORM_RUN_COINS_APOSTQ_MAX
                                                               ,true);
      gn_period_disassembly                     := get_qsetting(GC_PERIOD_DISASSEMBLY_QSET, gn_period_disassembly, GN_PERIOD_DISASSEMBLY_MIN, GN_PERIOD_DISASSEMBLY_MAX);
      gn_period_clear_log                       := get_qsetting(GC_PERIOD_CLEAR_LOG_QSET, gn_period_clear_log, GN_PERIOD_CLEAR_LOG_MIN, GN_PERIOD_CLEAR_LOG_MAX);
      gn_service_max_running                    := get_qsetting(GC_SERVICE_MAX_RUNNING_QSET, gn_service_max_running, GN_SERVICE_MAX_RUNNING_MIN, GN_SERVICE_MAX_RUNNING_MAX);
      gn_mincount_free_for_s                    := get_qsetting(GC_MINCOUNT_FREE_FOR_S_QSET, gn_mincount_free_for_s, GN_MINCOUNT_FREE_FOR_S_MIN, GN_MINCOUNT_FREE_FOR_S_MAX);
      gn_in_as_expiration                       := get_qsetting(GC_IN_AS_EXPIRATION_QSET, gn_in_as_expiration, GN_IN_AS_EXPIRATION_MIN, GN_IN_AS_EXPIRATION_MAX);
      gn_out_s_expiration                       := get_qsetting(GC_OUT_S_EXPIRATION_QSET, gn_out_s_expiration, GN_OUT_S_EXPIRATION_MIN, GN_OUT_S_EXPIRATION_MAX);
      gn_out_a_expiration                       := get_qsetting(GC_OUT_A_EXPIRATION_QSET, gn_out_a_expiration, GN_OUT_A_EXPIRATION_MIN, GN_OUT_A_EXPIRATION_MAX);
      gn_worker_f_count                         := get_qsetting(GC_WORKER_F_COUNT_QSET, gn_worker_f_count, GN_WORKER_F_COUNT_MIN, GN_WORKER_F_COUNT_MAX);
      gn_worker_plus_force                      := get_qsetting(GC_WORKER_PLUS_FORCE_QSET, gn_worker_plus_FORCE, GN_WORKER_PLUS_FORCE_MIN, GN_WORKER_PLUS_FORCE_MAX);
      gn_worker_plus_interval                   := get_qsetting(GC_WORKER_PLUS_INTERVAL_QSET, gn_worker_plus_interval, GN_WORKER_PLUS_INTERVAL_MIN, GN_WORKER_PLUS_INTERVAL_MAX);
      gn_worker_minus_interval                  := get_qsetting(GC_WORKER_MINUS_INTERVAL_QSET, gn_worker_minus_interval, GN_WORKER_MINUS_INTERVAL_MIN, GN_WORKER_MINUS_INTERVAL_MAX);
      gn_worker_restart_interval                := get_qsetting(GC_WORKER_RESTART_INTERVAL_QSET
                                                               ,gn_worker_restart_interval
                                                               ,GN_WORKER_RESTART_INTERVAL_MIN
                                                               ,GN_WORKER_RESTART_INTERVAL_MAX);
      gn_service_startprice                     := get_qsetting(GC_SERVICE_STARTPRICE_QSET, gn_service_startprice, GN_SERVICE_STARTPRICE_MIN, GN_SERVICE_STARTPRICE_MAX);
      gn_worker_min_count                       := gn_worker_f_count + gn_mincount_free_for_s + 1;
      gn_worker_max_count                       := get_qsetting(GC_WORKER_MAX_COUNT_QSET, gn_worker_max_count, gn_worker_min_count, GN_WORKER_MAX_COUNT_MAX);
      gn_pmanager_count                         := get_qsetting(GC_PMANAGER_COUNT_QSET, gn_pmanager_count, GN_PMANAGER_COUNT_MIN, GN_PMANAGER_COUNT_MAX);
      gn_pmanager_start_cnt_task                := get_qsetting(GC_PMANAGER_START_CNT_TASK_QSET
                                                               ,gn_pmanager_start_cnt_task
                                                               ,GN_PMANAGER_START_CNT_TASK_MIN
                                                               ,GN_PMANAGER_START_CNT_TASK_MAX);
      gn_max_msec_flush_pipe                    := get_qsetting(GC_MAX_MSEC_FLUSH_PIPE_QSET, gn_max_msec_flush_pipe, GN_MAX_MSEC_FLUSH_PIPE_MIN, GN_MAX_MSEC_FLUSH_PIPE_MAX, true);
      gn_min_cnt_flush_pipe                     := get_qsetting(GC_MIN_CNT_FLUSH_PIPE_QSET, gn_min_cnt_flush_pipe, GN_MIN_CNT_FLUSH_PIPE_MIN, GN_MIN_CNT_FLUSH_PIPE_MAX, true);
      gn_message_log_hist                       := get_qsetting(GC_MESSAGE_LOG_HIST_QSET, gn_message_log_hist, GN_MESSAGE_LOG_HIST_MIN, GN_MESSAGE_LOG_HIST_MAX);
      it_q_message.gn_queueXX_dequeue_sleep     := get_qsetting(GC_QUEUEXX_DEQUEUE_SLEEP_QSET
                                                               ,it_q_message.gn_queueXX_dequeue_sleep
                                                               ,GN_QUEUEXX_DEQUEUE_SLEEP_MIN
                                                               ,GN_QUEUEXX_DEQUEUE_SLEEP_MAX);
      it_q_message.gn_queueXX_max_retries_IN    := get_qsetting(GC_QUEUEXX_MAX_RETRIES_IN_QSET
                                                               ,it_q_message.gn_queueXX_max_retries_IN
                                                               ,GN_QUEUEXX_MAX_RETRIES_IN_MIN
                                                               ,GN_QUEUEXX_MAX_RETRIES_IN_MAX);
      it_q_message.gn_queueXX_max_retries_OUT   := get_qsetting(GC_QUEUEXX_MAX_RETRIES_OUT_QSET
                                                               ,it_q_message.gn_queueXX_max_retries_OUT
                                                               ,GN_QUEUEXX_MAX_RETRIES_OUT_MIN
                                                               ,GN_QUEUEXX_MAX_RETRIES_OUT_MAX);
      it_q_message.gn_queueXX_retry_delay_IN_S  := get_qsetting(GC_QUEUEXX_RETRY_DELAY_IN_S_QSET
                                                               ,it_q_message.gn_queueXX_retry_delay_IN_S
                                                               ,GN_QUEUEXX_RETRY_DELAY_IN_S_MIN
                                                               ,GN_QUEUEXX_RETRY_DELAY_IN_S_MAX);
      it_q_message.gn_queueXX_retry_delay_IN_A  := get_qsetting(GC_QUEUEXX_RETRY_DELAY_IN_A_QSET
                                                               ,it_q_message.gn_queueXX_retry_delay_IN_A
                                                               ,GN_QUEUEXX_RETRY_DELAY_IN_A_MIN
                                                               ,GN_QUEUEXX_RETRY_DELAY_IN_A_MAX);
      it_q_message.gn_queueXX_retry_delay_OUT_S := get_qsetting(GC_QUEUEXX_RETRY_DELAY_OUT_S_QSET
                                                               ,it_q_message.gn_queueXX_retry_delay_OUT_S
                                                               ,GN_QUEUEXX_RETRY_DELAY_OUT_S_MIN
                                                               ,GN_QUEUEXX_RETRY_DELAY_OUT_S_MAX);
      it_q_message.gn_queueXX_retry_delay_OUT_A := get_qsetting(GC_QUEUEXX_RETRY_DELAY_OUT_A_QSET
                                                               ,it_q_message.gn_queueXX_retry_delay_OUT_A
                                                               ,GN_QUEUEXX_RETRY_DELAY_OUT_A_MIN
                                                               ,GN_QUEUEXX_RETRY_DELAY_OUT_A_MAX);
      gn_queueXX_QManager_sleep                 := get_qsetting(GC_QUEUEXX_QMANAGER_SLEEP_QSET
                                                               ,gn_queueXX_QManager_sleep
                                                               ,GN_QUEUEXX_QMANAGER_SLEEP_MIN
                                                               ,GN_QUEUEXX_QMANAGER_SLEEP_MAX);
      gn_queueXX_QWorker_sleep                  := get_qsetting(GC_QUEUEXX_QWORKER_SLEEP_QSET, gn_queueXX_QWorker_sleep, GN_QUEUEXX_QWORKER_SLEEP_MIN, GN_QUEUEXX_QWORKER_SLEEP_MAX);
      gn_queueXX_QWorker_massstart              := get_qsetting(GC_QUEUEXX_QWORKER_MASSSTART_QSET
                                                               ,gn_queueXX_QWorker_massstart
                                                               ,GN_QUEUEXX_QWORKER_MASSSTART_MIN
                                                               ,GN_QUEUEXX_QWORKER_MASSSTART_MAX);
    end if;
  exception
    when others then
      send_information_error(p_sqlcode => 'IQ' || sqlcode, p_sqlerrm => sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  function get_gt_service_key(p_message_type itt_q_service.message_type%type
                             ,p_servicename  itt_q_service.servicename%type) return varchar2 as
  begin
    return upper(p_message_type) || '*' || upper(trim(p_servicename));
  end;

  --------------------------------------------------------------------------------
  -- Получение копии таблицы itt_q_services в коллекцию и сохранение статистики 
  procedure flush_services(p_update boolean) is
    row_locked exception;
    pragma exception_init(row_locked, -54);
    pragma autonomous_transaction;
    --  vc_key    varchar2(200);
    v_service_price itt_q_service.service_price%type;
    v_start         timestamp;
    v_service_key   varchar2(150);
    v_service_key_  varchar2(150);
    v_service_id    itt_q_service.service_id%type;
  begin
    v_service_key := gt_service.first;
    if v_service_key is not null
       and p_update
    then
      while v_service_key is not null
      loop
        if gt_service(v_service_key).calc_stat_c > GN_SERVICE_KFAVG -- Были выполнения сервиса и ведется расчет статистики 
        then
          begin
            select p.service_id
              into v_service_id
              from itt_q_service p
             where p.message_type = it_xml.token_substr(v_service_key, '*', 1)
               and upper(trim(p.servicename)) = it_xml.token_substr(v_service_key, '*', 2)
               for update nowait;
          exception
            when no_data_found then
              v_service_key_ := gt_service.next(v_service_key);
              gt_service.delete(v_service_key);
              v_service_key := v_service_key_;
              continue;
            when row_locked then
              return;
          end;
          update itt_q_service p
             set p.service_price = round((p.service_price * GN_SERVICE_KFAVG + gt_service(v_service_key).calc_stat_p * (gt_service(v_service_key).calc_stat_c - GN_SERVICE_KFAVG)) / gt_service(v_service_key).calc_stat_c
                                        ,0)
           where p.message_type = it_xml.token_substr(v_service_key, '*', 1)
             and upper(trim(p.servicename)) = it_xml.token_substr(v_service_key, '*', 2)
             and p.service_price != round((p.service_price * GN_SERVICE_KFAVG + gt_service(v_service_key).calc_stat_p * (gt_service(v_service_key).calc_stat_c - GN_SERVICE_KFAVG)) / gt_service(v_service_key).calc_stat_c
                                         ,0)
          returning p.service_price into v_service_price;
          commit;
          if sql%rowcount > 0
          then
            gt_service(v_service_key).calc_stat_c := GN_SERVICE_KFAVG;
            gt_service(v_service_key).calc_stat_p := v_service_price;
          end if;
        end if;
        v_service_key := gt_service.next(v_service_key);
      end loop;
    end if;
    if gt_service.first is null
       or gd_last_flush_services is null
    then
      gd_last_flush_services := null;
      gt_service.delete;
    end if;
    v_start := systimestamp;
    for rec in (select * from itt_q_service s where s.update_time >= nvl(gd_last_flush_services, s.update_time))
    loop
      v_service_key := get_gt_service_key(rec.message_type, rec.servicename);
      if rec.close_sysdate <= sysdate
      then
        if gt_service.exists(v_service_key)
        then
          gt_service.delete(v_service_key);
        end if;
      else
        rec.calc_stat_c := GN_SERVICE_KFAVG;
        rec.calc_stat_p := rec.service_price;
        if nvl(rec.max_running, 0) <= 0
        then
          rec.max_running := null;
        end if;
        gt_service(v_service_key) := rec;
      end if;
    end loop;
    gd_last_flush_services := v_start;
  end;

  -----------------------------------------------------------------------
  function workername(p_queue_num char
                     ,work_id     integer) return varchar2 is
  begin
    if nvl(work_id, 0) > 0
    then
      if p_queue_num = it_q_message.C_C_QUEUENUM_XX
      then
        return 'X' || trim(to_char(work_id, '09999'));
      else
        return trim(to_char(work_id, '099999'));
      end if;
    elsif gс_manager_queue_num is not null
    then
      return 'QManager' || gс_manager_queue_num;
    else
      return 'UNKNOWN';
    end if;
  end;

  --==============================================================================
  -- Установка атрибута сессии
  procedure set_session_action(p_action varchar2 default 'Check') is
  begin
    dbms_application_info.set_action(p_action);
  end;

  procedure set_session_module(p_action  varchar2 default 'Initialization'
                              ,p_queueXX integer default 0) is
    v_session varchar2(50) := lower(gс_userenv_current_schema) || '.' || case
                                when gс_manager_queue_num is not null then
                                 case
                                   when gn_is_pmanager = 1 then
                                    'P'
                                   else
                                    'Q'
                                 end || 'Manager' || gс_manager_queue_num
                                when gn_worker_num is not null then
                                 'QWorker' || case
                                   when p_queueXX = 1 then
                                    'X'
                                 end || gn_worker_num
                                else
                                 '!!!Error!!!'
                              end;
  begin
    dbms_application_info.set_module(v_session, p_action);
  end;

  --=================================================================================================
  function check_pipe_name(p_pipe_prefix varchar2
                          ,p_channel     varchar2 default null) return boolean as
  begin
    return((p_pipe_prefix = С_WORKER_PIPE_PREFIX and p_channel is not null) or
           (p_pipe_prefix = С_RESULT_PIPE_PREFIX and (p_channel is null or gс_result_pipe_channel = p_channel)));
  end;

  --=================================================================================================
  function get_full_pipe_name(p_pipe_prefix varchar2
                             ,p_channel     varchar2 default null) return varchar2 as
    v_channel varchar2(10) := p_channel;
  begin
    if not check_pipe_name(p_pipe_prefix => p_pipe_prefix, p_channel => p_channel)
    then
      raise_application_error(-20000, 'Ошибка параметров функции it_q_manager.get_full_pipe_name');
    end if;
    v_channel := nvl(v_channel, gс_result_pipe_channel);
    return gс_userenv_current_schema || '#' || p_pipe_prefix || v_channel;
  end;

  --==============================================================================
  -- Отправка сообщения о в PIPE
  procedure pipe_send_mess(p_mess        varchar2
                          ,p_pipe_prefix varchar2
                          ,p_channel     varchar2 default null) as
    buffer_overflow exception;
    pragma exception_init(buffer_overflow, -06558);
    v_status integer;
  begin
    if not check_pipe_name(p_pipe_prefix => p_pipe_prefix, p_channel => p_channel)
    then
      raise_application_error(-20000, 'Ошибка параметров функции it_q_manager.pipe_send_mess');
    end if;
    begin
      sys.dbms_pipe.pack_message(p_mess);
    exception
      when buffer_overflow then
        sys.dbms_pipe.reset_buffer;
        sys.dbms_pipe.pack_message(p_mess);
    end;
    --debug('Send  channel'||p_channel||'->'||p_mess);
    v_status := sys.dbms_pipe.send_message(get_full_pipe_name(p_pipe_prefix, p_channel)
                                          ,0
                                          ,case
                                             when p_pipe_prefix = С_RESULT_PIPE_PREFIX then
                                              GN_RESULT_MAXPIPESIZE
                                             else
                                              GN_WORKER_MAXPIPESIZE
                                           end);
  end;

  -----------------------------------------------------------------------
  function lock_get_handle(p_process varchar2) return varchar2 as
    v_process varchar2(128) := gс_userenv_current_schema || '#' || upper(p_process);
    v_lock    varchar2(128);
  begin
    if not gt_lock_process.exists(v_process)
    then
      dbms_lock.allocate_unique(v_process, v_lock);
      gt_lock_process(v_process) := v_lock;
    else
      v_lock := gt_lock_process(v_process);
    end if;
    return v_lock;
  end;

  function lock_request(p_process varchar2
                       ,p_wait    pls_integer default 0) return boolean as
    v_lock   varchar2(128) := lock_get_handle(p_process);
    v_status pls_integer;
    v_wait   pls_integer := nvl(p_wait, 0);
    --v_start  timestamp := systimestamp;
  begin
    v_status := dbms_lock.request(v_lock, dbms_lock.x_mode, v_wait);
    return v_status = 0;
    /*if v_status = 0
    then
      debug('Manager' || gс_manager_queue_num || ' lock_request: LOCK ' || p_process || '(' || v_lock || ')-' ||
            it_xml.calc_interval_millisec(v_start, systimestamp));
      return true;
    else
      debug('Manager' || gс_manager_queue_num || ' lock_request: NO LOCK ' || p_process || '(' || v_lock || ')-' ||
            it_xml.calc_interval_millisec(v_start, systimestamp));
      return false;
    end if;*/
  end;

  procedure lock_release(p_process varchar2) as
    v_lock   varchar2(128) := lock_get_handle(p_process);
    v_status integer;
    --v_start  timestamp := systimestamp;
  begin
    v_status := dbms_lock.release(v_lock);
    /*debug('Manager' || gс_manager_queue_num || ' lock_release: UNLOCK status' || v_status || ' ' || p_process || '(' || v_lock || ')-' ||
    it_xml.calc_interval_millisec(v_start, systimestamp));*/
  end;

  ---------------------------------------------------------------------------
  function insert_worker(p_worker_num number default null) return number as
    v_worker_num number := p_worker_num;
    vc_process   varchar2(100) := 'it_q_manager.insert_worker' || p_worker_num;
  begin
    if lock_request(vc_process)
    then
      if v_worker_num is null
      then
        select min(wn) into v_worker_num from (select level as WN from dual connect by level <= gn_worker_max_count) where wn not in (select w.worker_num from itt_q_worker w);
      end if;
      if v_worker_num is not null
      then
        insert into itt_q_worker (worker_num) values (v_worker_num);
      end if;
    else
      v_worker_num := null;
    end if;
    lock_release(vc_process);
    return v_worker_num;
  exception
    when others then
      lock_release(vc_process);
      return null;
      send_information_error('PS' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  --==============================================================================
  function XGetCommand(p_command varchar2) return boolean as
    v_isexit     integer := 1;
    v_senderuser itt_q_message_log.senderuser%type;
    v_sender     itt_q_message_log.sender%type;
  begin
    begin
      execute immediate 'select * from ( select senderuser, sender from itv_q_taskxx t where message_type = ''R'' and servicename is null and correlation = ''' ||
                        IT_Q_MESSAGE.GC_CORR_COMMAND || ''' and t.txtmessbody =''' || p_command || ''' order by enqdt ) where  rownum < 2 '
        into  v_senderuser, v_sender;
    exception
      when no_data_found then
        v_isexit := 0;
    end;
    if nvl(v_isexit, 0) > 0
       and gс_manager_queue_num is not null
    then
      send_information('COMMAND'
                      ,'Получена команда ' || to_char(p_command) || ' для очереди ' || gс_manager_queue_num || ' от ' || v_senderuser || ' из ' || v_sender);
    end if;
    return(nvl(v_isexit, 0) > 0);
  end;

  -- Запуск обработчика табличной очереди  
  procedure XWorker_start as
    vc_jenabled varchar2(100);
    v_sqlcode   number;
    v_sqlerrm   varchar2(2000);
    v_job_name  varchar2(128) := С_JOB_MANAGER_PREFIX || it_q_message.C_C_QUEUENUM_XX;
  begin
    if not lock_request(С_LOCK_PROCESS_XQMANAGER_START)
    then
      lock_release(С_LOCK_PROCESS_XQMANAGER_START);
      return;
    end if;
    begin
      sys.dbms_scheduler.enable(v_job_name);
      lock_release(С_LOCK_PROCESS_XQMANAGER_START);
      return;
    exception
      when others then
        begin
          select enabled -- 'TRUE'
            into vc_jenabled
            from sys.user_scheduler_jobs
           where job_name = v_job_name;
        exception
          when no_data_found then
            vc_jenabled := null;
        end;
        if vc_jenabled is null
        then
          sys.dbms_scheduler.create_job(job_name => v_job_name
                                       ,job_type => 'STORED_PROCEDURE'
                                       ,job_action => 'it_q_manager.ManagerXMain'
                                       ,start_date => systimestamp
                                       ,auto_drop => true
                                       ,comments => 'Менеджер табличной очереди ' || v_job_name);
        end if;
        sys.dbms_scheduler.enable(v_job_name);
        lock_release(С_LOCK_PROCESS_XQMANAGER_START);
    end;
  exception
    when others then
      lock_release(С_LOCK_PROCESS_XQMANAGER_START);
      v_sqlcode := sqlcode;
      v_sqlerrm := substr(sqlerrm, 1, 2000);
      if v_sqlcode != -27477
      then
        send_information_error('XS' || v_sqlcode, v_sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
      end if;
  end;

  -- Запуск Qменеджера 
  procedure qmanager_start as
    v_info varchar2(32000);
  begin
    if startmanager(o_info => v_info) != 1
       or instr(v_info, ' START ') != 0
       or instr(v_info, ' Enable ') != 0
    then
      send_information_error('START QM', v_info);
    end if;
  end;

  -- Условия запуска менеджера pipe канала
  function pipemanager_quit(p_nocheck_qmng boolean default false) return boolean as
    v_ret boolean := true;
    v_cnt integer := 1;
  begin
    if not p_nocheck_qmng
    then
      select nvl(max(1), 0)
        into v_cnt
        from user_scheduler_jobs j
       where j.job_name like С_JOB_MANAGER_PREFIX || '%'
         and j.state = 'RUNNING';
    end if;
    if v_cnt > 0
    then
      v_ret := (it_q_message.get_count_queue = 1 and gn_pmanager_start_cnt_task > 1 and
               it_q_message.get_count_task(p_queue_num => gс_manager_queue_num, p_max_count => gn_pmanager_start_cnt_task) < gn_pmanager_start_cnt_task);
      if not v_ret
      then
        select count(*) into v_cnt from table(it_q_manager.select_pipe_channel) where column_value = gс_manager_queue_num;
        v_ret := (v_cnt = 0);
      end if;
    else
      v_ret := true;
    end if;
    return v_ret;
  end;

  -- Запуск менеджера pipe канала
  procedure pipemanager_start as
    pragma autonomous_transaction;
    vc_jenabled varchar2(100);
    vc_jstate   varchar2(100);
    vc_process constant varchar2(100) := С_LOCK_PROCESS_WORKERS_FLUSH || gс_manager_queue_num;
  begin
    if (it_q_message.get_count_queue = 1 and gn_pmanager_start_cnt_task >= GN_PMANAGER_START_CNT_TASK_MAX) --- выключена через настройки
       or (it_q_message.get_count_queue = 2 and it_q_message.check_queue_num(it_q_message.C_C_QUEUENUM_XX) = 1)
       or (it_q_message.get_count_queue > 1 and gd_last_pipemanager_start is not null -- Уже проверяли  
       and sysdate <= gd_last_pipemanager_start + numtodsinterval(GN_QUEUE_WAIT, 'SECOND'))
       or not lock_request(vc_process) -- Проверка на работающий PMANAGER 
    then
      --debug('Manager' || gс_manager_queue_num || ' pipemanager NOSTART ');
      lock_release(vc_process);
      return;
    end if;
    if pipemanager_quit(true)
    then
      if it_q_message.get_count_queue != 1
      then
        gd_last_pipemanager_start := sysdate;
      end if;
      lock_release(vc_process);
      --debug('Manager' || gс_manager_queue_num|| ' pipemanager QUIT ');
      return;
    end if;
    begin
      select enabled -- 'TRUE'
            ,state -- 'RUNNING'
        into vc_jenabled
            ,vc_jstate
        from sys.user_scheduler_jobs
       where job_name = С_JOB_PMANAGER_PREFIX || gс_manager_queue_num;
    exception
      when no_data_found then
        vc_jenabled := null;
    end;
    if vc_jenabled is null
       or vc_jenabled != 'TRUE'
    then
      if vc_jenabled is null
      then
        sys.dbms_scheduler.create_job(job_name => С_JOB_PMANAGER_PREFIX || gс_manager_queue_num
                                     ,job_type => 'STORED_PROCEDURE'
                                     ,job_action => 'it_q_manager.PipeManagerMain'
                                     ,number_of_arguments => 1
                                     ,start_date => sysdate
                                     ,auto_drop => true
                                     ,comments => 'Менеджер PIPE канала очереди ' || IT_Q_MESSAGE.C_C_QUEUE_IN_PREFIX || gс_manager_queue_num);
      end if;
      if vc_jenabled is null
         or vc_jenabled != 'TRUE'
      then
        sys.dbms_scheduler.set_job_argument_value(job_name => С_JOB_PMANAGER_PREFIX || gс_manager_queue_num, argument_position => 1, argument_value => gс_manager_queue_num);
        sys.dbms_scheduler.set_attribute(С_JOB_PMANAGER_PREFIX || gс_manager_queue_num, 'start_date', sysdate);
        sys.dbms_scheduler.enable(С_JOB_PMANAGER_PREFIX || gс_manager_queue_num);
      end if;
      --debug('Manager' ||gс_manager_queue_num || ' pipemanager START ');
    end if;
    lock_release(vc_process);
    gd_last_pipemanager_start := sysdate;
  exception
    when others then
      lock_release(vc_process);
      send_information_error('PS' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  --==============================================================================
  --- Количество свободных воркеров
  function get_count_worker_free(p_priority itt_q_message_log.priority%type default null) return integer deterministic as
    vn_work_cnt integer;
    pragma udf;
  begin
    select count(*)
      into vn_work_cnt
      from itt_q_worker
     where worker_priority = nvl(p_priority, worker_priority)
       and job_stoptime is null
       and worker_enabled > 0
       and worker_free > 0;
    return vn_work_cnt;
  end;

  --- Количество свободных Xворкеров
  function get_count_workerX_free(p_priority itt_q_message_log.priority%type default null) return integer deterministic as
    vn_work_cnt integer;
    pragma udf;
  begin
    select count(*)
      into vn_work_cnt
      from itt_q_workerX w
     where job_stoptime is null
       and response_lasttime >= systimestamp - numtodsinterval(gn_queueXX_QWorker_sleep * 2, 'SECOND')
       and response_last in (C_WORKER_RESPONSE_START, C_WORKER_RESPONSE_FREE);
    return vn_work_cnt;
  end;

  --- Количество работающих воркеров
  function get_count_worker_run(p_priority itt_q_message_log.priority%type default null) return integer deterministic as
    vn_work_cnt integer;
    pragma udf;
  begin
    select count(*)
      into vn_work_cnt
      from itt_q_worker
     where worker_priority = nvl(p_priority, worker_priority)
       and job_stoptime is null
       and worker_enabled > 0
       and worker_free <= 0;
    return vn_work_cnt;
  end;

  --- Количество работающих воркеров
  function get_count_workerX_run(p_priority itt_q_message_log.priority%type default null) return integer deterministic as
    vn_work_cnt integer;
    pragma udf;
  begin
    select count(*)
      into vn_work_cnt
      from itt_q_workerX w
     where job_stoptime is null
       and response_last = C_WORKER_RESPONSE_RUN;
    return vn_work_cnt;
  end;

  --- Количество стартовавших воркеровX
  function get_count_workerX(p_priority itt_q_message_log.priority%type default null) return integer deterministic as
    vn_work_cnt integer;
    pragma udf;
  begin
    select count(*)
      into vn_work_cnt
      from itt_q_workerX w
     where job_stoptime is null
        or job_stoptime > sysdate;
    return vn_work_cnt;
  end;

  --- Количество стартовавших воркеров
  function get_count_worker(p_priority itt_q_message_log.priority%type default null) return integer deterministic as
    vn_work_cnt integer;
    pragma udf;
  begin
    select count(*)
      into vn_work_cnt
      from itt_q_worker
     where worker_priority = nvl(p_priority, worker_priority)
       and (job_stoptime is null or job_stoptime < sysdate)
       and worker_enabled > 0;
    return vn_work_cnt;
  end;

  -- Старт обработчика
  function worker_start(p_worker_num number) return boolean as
    row_locked exception;
    pragma exception_init(row_locked, -54);
    v_job_name      varchar2(60) := С_JOB_WORKER_PREFIX || p_worker_num;
    vc_jenabled     sys.user_scheduler_jobs.ENABLED%type;
    vc_jstate       sys.user_scheduler_jobs.STATE%type;
    tmp             integer;
    vc_pipe_channel itt_q_worker.pipe_channel%type;
  begin
    -- Блокировка
    select w.worker_num into tmp from itt_q_worker w where w.worker_num = p_worker_num for update nowait;
    begin
      select enabled -- 'TRUE'
            ,state -- 'RUNNING'
        into vc_jenabled
            ,vc_jstate
        from sys.user_scheduler_jobs
       where job_name = v_job_name;
    exception
      when no_data_found then
        null;
    end;
    if vc_jenabled is null
    then
      sys.dbms_scheduler.create_job(job_name => v_job_name
                                   ,job_type => 'STORED_PROCEDURE'
                                   ,job_action => 'it_q_manager.WorkerMain'
                                   ,number_of_arguments => 1
                                   ,start_date => sysdate
                                   ,repeat_interval => 'Freq=Secondly;Interval=5'
                                    --,auto_drop => true
                                   ,comments => 'Обработчик №' || p_worker_num || ' входящих очередей ' || IT_Q_MESSAGE.C_C_QUEUE_IN_PREFIX || 'XX');
      --debug('Manager' || gс_manager_queue_num || 'worker_start ' || p_worker_num || ' create_job');
    end if;
    if vc_jenabled is null
       or vc_jenabled != 'TRUE'
    then
      sys.dbms_pipe.purge(get_full_pipe_name(С_WORKER_PIPE_PREFIX, p_worker_num)); -- Очищаем 
      sys.dbms_scheduler.set_job_argument_value(job_name => v_job_name, argument_position => 1, argument_value => to_char(p_worker_num));
      sys.dbms_scheduler.set_attribute(name => v_job_name, attribute => 'start_date', value => sysdate);
      sys.dbms_scheduler.enable(v_job_name);
      vc_pipe_channel := next_pipe_channel();
      update itt_q_worker w
         set w.pipe_channel   = vc_pipe_channel
            ,w.job_stoptime   = null
            ,w.job_starttime  = null
            ,w.worker_enabled = 0
            ,w.worker_free    = 0
            ,w.run_coins      = 0
            ,w.run_count      = 0
            ,w.run_lasttime   = null
       where worker_num = p_worker_num;
      --debug('Manager' || gс_manager_queue_num || 'worker_start ' || p_worker_num || ' START');
    else
      update itt_q_worker w set w.job_stoptime = null where worker_num = p_worker_num;
    end if;
    return true;
  exception
    when row_locked then
      --debug('Manager' || gс_manager_queue_num || 'worker_start ' || p_worker_num || ' NO LOCK');
      return false;
    when others then
      send_information_error('WS' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
      return false;
  end;

  -- Останов обработчика
  procedure worker_stop(p_worker_num number) as
    v_job_name varchar2(32) := С_JOB_WORKER_PREFIX || p_worker_num;
  begin
    sys.dbms_scheduler.disable(v_job_name, true);
    sys.dbms_pipe.purge(get_full_pipe_name(С_WORKER_PIPE_PREFIX, p_worker_num)); -- очищаем команды
    pipe_send_mess('EXIT', С_WORKER_PIPE_PREFIX, p_worker_num);
  exception
    when others then
      send_information_error('WP' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  -- Рестарт одного  WORKERа
  procedure worker_restart(p_last_restart date default null) is
    pragma autonomous_transaction;
    v_last_restart  date;
    vc_sid          varchar2(100) := '$$WORKERS_RESTART';
    vc_process      varchar2(100) := 'it_q_manager.worker_restart';
    vn_restart      pls_integer := 0;
    v_store_restart date;
  begin
    if lock_request(vc_process)
    then
      v_store_restart := it_q_message.get_qset_data(p_qset_name => vc_sid);
      if p_last_restart is not null
      then
        if p_last_restart > nvl(v_store_restart, p_last_restart - 1)
        then
          it_q_message.set_qset_data(vc_sid, p_last_restart);
          v_last_restart := p_last_restart;
        else
          v_last_restart := v_store_restart;
        end if;
      elsif v_store_restart is null
      then
        v_last_restart := sysdate - numtodsinterval(gn_worker_restart_interval, 'MINUTE');
        it_q_message.set_qset_data(vc_sid, v_last_restart);
      elsif v_store_restart < sysdate - numtodsinterval(gn_worker_restart_interval, 'MINUTE')
      then
        v_last_restart := sysdate - numtodsinterval(gn_worker_restart_interval - 1, 'MINUTE'); -- Шаг 1 мин.
        it_q_message.set_qset_data(vc_sid, v_last_restart);
      else
        v_last_restart := v_store_restart;
      end if;
      for worker in (select worker_num
                           ,job_starttime
                       from itt_q_worker
                      where worker_enabled > 0
                        and job_stoptime is null
                        and job_starttime < v_last_restart
                        and worker_free > 0
                      order by worker_num desc
                        for update skip locked)
      loop
        update itt_q_worker w
           set w.worker_enabled   = 0
              ,w.worker_free      = 0
              ,w.servicename      = '#Перезапуск'
              ,w.service_delivery = null
         where worker_num = worker.worker_num;
        commit;
        pipe_send_mess('EXIT', С_WORKER_PIPE_PREFIX, worker.worker_num);
        /* send_information('RESTART#' || worker.worker_num
        ,vc_sid || '=' || to_char(v_last_restart, 'DD-MM-YYYY HH24:MI:SS') || ' Предыдущий старт обработчика :' ||
         to_char(worker.job_starttime, 'DD-MM-YYYY HH24:MI:SS'));*/
        vn_restart := 1;
        exit;
      end loop;
      if vn_restart = 0
         and gd_last_worker_restart is not null
      then
        select count(*)
          into vn_restart
          from itt_q_worker
         where job_stoptime is null
           and job_starttime < gd_last_worker_restart;
        if vn_restart = 0
        then
          gd_last_worker_restart := null; -- Всех перезапустили.
        end if;
      end if;
      commit;
    end if;
    lock_release(vc_process);
  exception
    when others then
      rollback;
      lock_release(vc_process);
      send_information_error('WE' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  -- Обновление по джобам WORKERов
  procedure workers_refresh(p_force boolean default false) is
    pragma autonomous_transaction;
    vn_cnt       pls_integer;
    v_priority_f itt_q_message_log.priority%type := IT_Q_MESSAGE.C_C_MSG_PRIORITY_F;
    v_priority_n itt_q_message_log.priority%type := IT_Q_MESSAGE.C_C_MSG_PRIORITY_N;
    vc_sid       varchar2(100) := '$$WORKERS_REFRESH';
    vc_sid_force varchar2(100) := '$$WORKERS_REFRESH_FORCE';
    vd_last_dt   date;
    vc_process   varchar2(100) := 'it_q_manager.workers_refresh';
    vn           number;
    v_res        boolean := true;
    --tmp          number;
  begin
    vd_last_dt := it_q_message.get_qset_data(p_qset_name => case
                                                              when p_force then
                                                               vc_sid_force
                                                              else
                                                               vc_sid
                                                            end);
    --debug('Manager' || gс_manager_queue_num || ' workers_refresh vd_last_dt := ' || to_char(vd_last_dt,'dd.mm.yyyy hh24:mi:ss'));
    if vd_last_dt is not null
       and sysdate < vd_last_dt + numtodsinterval(GN_QUEUE_WAIT, 'SECOND')
    then
      return;
    end if;
    if lock_request(vc_process)
    then
      if p_force
      then
        -- Проверка списка 
        select count(*) into vn_cnt from itt_q_worker w;
        if vn_cnt < gn_worker_min_count -- Обшее кол-во
        then
          for n in vn_cnt + 1 .. gn_worker_min_count
          loop
            vn := insert_worker();
            exit when vn is null;
          end loop;
          commit;
        end if;
        select count(*) into vn_cnt from itt_q_worker w where w.job_stoptime is null;
        if vn_cnt < gn_worker_min_count -- Работающие
        then
          update itt_q_worker w
             set w.job_stoptime    = null
                ,w.worker_priority = v_priority_n
           where w.job_stoptime is not null
             and rownum <= gn_worker_min_count - vn_cnt;
          commit;
        end if;
        if vn_cnt > gn_worker_max_count -- Работающие
        then
          update itt_q_worker w
             set w.job_stoptime    = sysdate
                ,w.worker_priority = v_priority_n
           where w.job_stoptime is null
             and worker_priority = v_priority_n
             and rownum <= vn_cnt - gn_worker_max_count;
          commit;
        end if;
        select count(*)
          into vn_cnt
          from itt_q_worker w
         where w.worker_priority = v_priority_f
           and w.job_stoptime is null;
        if vn_cnt != gn_worker_f_count -- F
        then
          update itt_q_worker w
             set w.worker_priority = case
                                       when rownum <= gn_worker_f_count then
                                        v_priority_f
                                       else
                                        v_priority_n
                                     end
           where w.job_stoptime is null;
          commit;
        end if;
        for c in (select jw.*
                        ,w.worker_num
                        ,w.job_starttime
                        ,w.job_stoptime
                        ,w.worker_free
                        ,w.worker_enabled
                    from itt_q_worker w
                    join (select to_number(replace(j.job_name, С_JOB_WORKER_PREFIX)) channel
                               ,j.job_name
                               ,j.ENABLED
                               ,j.state -- 'RUNNING'
                           from sys.user_scheduler_jobs j
                          where j.job_name like С_JOB_WORKER_PREFIX || '%') jw
                      on jw.channel = w.worker_num
                   where w.job_stoptime is not null
                     and ((jw.enabled = 'TRUE' and w.worker_free > 0) or
                         (jw.enabled != 'TRUE' and jw.state != 'RUNNING' and w.job_stoptime < sysdate - numtodsinterval(gn_worker_minus_interval, 'SECOND'))))
        loop
          case
            when c.enabled = 'TRUE' then
              worker_stop(c.worker_num);
              -- debug('Manager' || gс_qmanager_queue_num || ' workers_refresh worker_stop ' || c.worker_num);
            when c.enabled != 'TRUE' then
              sys.dbms_scheduler.drop_job(c.job_name);
              -- debug('Manager' || gс_qmanager_queue_num || ' workers_refresh drop_job ' || c.job_name);
            else
              null;
          end case;
          commit;
        end loop;
      end if;
      for c in (select jw.*
                      ,w.worker_num
                      ,w.job_starttime
                      ,w.job_stoptime
                      ,w.worker_free
                      ,w.worker_enabled
                      ,wm.worker_num as wrk_wn
                      ,nvl(wm.wrk_cnt, 0) as wrk_cnt
                  from itt_q_worker w
                  full join (select to_number(replace(j.job_name, С_JOB_WORKER_PREFIX)) channel
                                  ,j.job_name
                                  ,j.ENABLED
                                  ,j.state -- 'RUNNING'
                              from sys.user_scheduler_jobs j
                             where j.job_name like С_JOB_WORKER_PREFIX || '%') jw
                    on jw.channel = w.worker_num
                  full join (select t.worker_num
                                  ,count(*) as wrk_cnt
                              from itt_q_work_messages t
                             where t.worker_num > 0
                             group by t.worker_num) wm
                    on wm.worker_num = coalesce(w.worker_num, jw.channel)
                 where w.worker_num is null
                    or (jw.channel is null and w.job_stoptime is null)
                    or (jw.enabled != 'TRUE' and (w.job_stoptime is null or nvl(wm.wrk_cnt, 0) > 0)))
      loop
        if c.worker_num is null
        then
          vn := nvl(c.channel, c.wrk_wn);
          if insert_worker(vn) is null
          then
            --debug('Manager' || gс_manager_queue_num || ' workers_refresh NO START ' || vn);
            v_res := false;
            rollback;
            continue;
          end if;
        else
          vn := c.worker_num;
        end if;
        if worker_start(vn)
        then
          commit;
        else
          v_res := false;
          --debug('Manager' || gс_manager_queue_num || ' workers_refresh NO START ' || vn);
          rollback;
          continue;
        end if;
      end loop;
      if v_res
      then
        it_q_message.set_qset_data(vc_sid, sysdate); -- Если все исправили
        if p_force
        then
          it_q_message.set_qset_data(vc_sid_force, sysdate);
        end if;
      end if;
    end if;
    lock_release(vc_process);
  exception
    when others then
      rollback;
      lock_release(vc_process);
      send_information_error('WRF' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  -- Добавление работника
  function worker_plus return boolean as
    pragma autonomous_transaction;
    vc_sid       varchar2(100) := '$$WORKER_PLUS';
    vd_last_dt   date;
    v_cnt        integer;
    v_start      integer;
    v_free       integer;
    v_priority_n itt_q_worker.worker_priority%type := IT_Q_MESSAGE.C_C_MSG_PRIORITY_N;
    v_worker_num integer;
    vc_process   varchar2(100) := 'it_q_manager.worker_plus';
    v_res        boolean := false;
  begin
    if gn_worker_plus_interval > 0
    then
      vd_last_dt := it_q_message.get_qset_data(p_qset_name => vc_sid);
      if vd_last_dt is not null
         and sysdate < vd_last_dt + numtodsinterval(gn_worker_plus_interval, 'SECOND')
      then
        --debug('Manager' || gс_manager_queue_num || ' worker_plus NO TIME ');
        return v_res;
      end if;
    end if;
    if lock_request(vc_process)
    then
      select count(*)
            ,sum(case
                   when w.job_starttime is null then
                    1
                   else
                    0
                 end)
            ,sum(case
                   when worker_priority = v_priority_n
                        and worker_enabled > 0
                        and worker_free > 0 then
                    1
                   else
                    0
                 end)
        into v_cnt
            ,v_start
            ,v_free
        from itt_q_worker w
       where w.job_stoptime is null;
      if v_cnt < (gn_worker_max_count - get_count_workerX)
         and v_start < IT_Q_MESSAGE.get_count_queue + gn_worker_plus_force -- Кол-во воркеров уже стартовавших, но не готовых
         and v_free < gn_mincount_free_for_s + IT_Q_MESSAGE.get_count_queue * gn_worker_plus_force -- свободных недостаточно
      then
        v_worker_num := null;
        for worker in (select worker_num from itt_q_worker where job_stoptime is not null order by worker_num for update skip locked)
        loop
          v_worker_num := worker.worker_num;
          exit;
        end loop;
        if v_worker_num is null
        then
          v_worker_num := insert_worker();
        end if;
        if v_worker_num is not null
           and worker_start(v_worker_num)
        then
          --debug('Manager' || gс_manager_queue_num || ' worker_plus ' || v_worker_num);
          v_res := true;
          commit;
        else
          rollback;
        end if;
        -- else
        --debug('Manager' || gс_manager_queue_num || ' worker_plus  v_cnt=' || v_cnt || ' v_start=' || v_start || ' v_free=' || v_free);
      end if;
      -- else
      --  debug('Manager' || gс_manager_queue_num || ' worker_plus NO LOCK ');
    end if;
    lock_release(vc_process);
    return v_res;
  exception
    when others then
      rollback;
      lock_release(vc_process);
      send_information_error('WP' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
      return false;
  end;

  --=================================================================================================
  -- Обновление свободного WORKERов
  procedure update_worker_free(p_worker_num        itt_q_worker.worker_num%type
                              ,p_response_last     itt_q_worker.response_last%type
                              ,p_response_lasttime itt_q_worker.response_lasttime%type
                              ,p_job_starttime     itt_q_worker.job_starttime%type default sysdate) as
    v_response_lasttime timestamp := nvl(p_response_lasttime, systimestamp);
    v_job_starttime     date := nvl(p_job_starttime, sysdate);
  begin
    update itt_q_worker w
       set w.response_lasttime = v_response_lasttime
          ,w.response_last     = p_response_last
          ,w.worker_free       = 1
          ,w.worker_enabled    = 1
           --,w.run_lasttime      = null
          ,w.run_starttime    = null
          ,w.run_count        = 0
          ,w.run_coins        = 0
          ,w.run_stoptime     = null
          ,w.service_delivery = null
          ,w.servicename      = null
          ,w.stop_apostq      = 0
          ,w.servicegroup     = null
          ,w.queue_num        = null
          ,w.job_starttime    = coalesce(w.job_starttime, v_job_starttime)
     where w.worker_num = p_worker_num
       and nvl(w.run_lasttime, v_response_lasttime - 1) < v_response_lasttime;
  end;

  --=================================================================================================
  -- Обновление статусов WORKERов
  function workers_flush(p_wait_lock integer default 0
                        ,p_PipeMNG   boolean default false) return boolean is
    row_locked exception;
    pragma exception_init(row_locked, -54);
    pragma autonomous_transaction;
    v_PipeMNG       boolean := nvl(p_PipeMNG, false);
    tmp             integer;
    vc_msg          varchar2(512);
    vn_work_id      pls_integer;
    vn_work_mes     varchar2(256);
    vc_r_proc       varchar2(256);
    vc_r_price      integer;
    vc_r_price_plan integer;
    type tt_qworkers is table of itt_q_worker%rowtype index by pls_integer;
    vt_qworkers          tt_qworkers;
    vn_poz               itt_q_worker.worker_num%type;
    vn_wait_lock         integer := nvl(p_wait_lock, 0);
    v_errno              integer;
    v_errmess            varchar2(2000);
    vs_response_lasttime timestamp;
    vs_run_lasttime      timestamp;
    --vs_starting          timestamp := systimestamp;
    -- v_ret1   number;
    -- v_ret2   number;
    vs_start date := sysdate();
    vc_process constant varchar2(100) := С_LOCK_PROCESS_WORKERS_FLUSH || gс_manager_queue_num;
    vs_start_pipe   timestamp;
    vn_cnt_pipe_msg integer;
    vs_pipe_name    varchar(128) := get_full_pipe_name(С_RESULT_PIPE_PREFIX, gс_manager_queue_num);
    /*vs_manager varchar2(30) := case
      when v_PipeMNG then
       'Pipe'
    end || 'Manager' || gс_manager_queue_num;*/
  begin
    --debug(vs_manager || ' workers_flush START ');
    if not lock_request(vc_process, vn_wait_lock)
    then
      lock_release(vc_process);
      --debug(vs_manager || ' workers_flush LOCKED ' || vc_process);
      return false;
    end if;
    loop
      tmp             := null;
      vs_start_pipe   := systimestamp;
      vn_cnt_pipe_msg := 0;
      loop
        exit when v_PipeMNG and vn_cnt_pipe_msg >= gn_min_cnt_flush_pipe -- Для исключения переполнения PIPE из за малого значения gn_max_msec_flush_pipe
        and it_xml.calc_interval_millisec(vs_start_pipe, systimestamp) > gn_max_msec_flush_pipe;
        --
        tmp := sys.dbms_pipe.receive_message(vs_pipe_name
                                            ,case
                                               when not v_PipeMNG
                                                    or vn_cnt_pipe_msg != 0
                                                    or vt_qworkers.count != 0 then
                                                0
                                               else
                                                GN_QUEUE_WAIT
                                             end);
        exit when tmp != 0;
        vn_cnt_pipe_msg := vn_cnt_pipe_msg + 1;
        sys.dbms_pipe.unpack_message(vc_msg);
        --debug(vs_manager || ' MESSAGE ' || vc_msg);
        begin
          vn_work_id           := to_number(it_xml.token_substr(vc_msg, '#', 1));
          vs_response_lasttime := it_xml.char_to_timestamp_iso8601(it_xml.token_substr(vc_msg, '#', 2));
        exception
          when others then
            continue;
        end;
        if vs_response_lasttime is null
           or nvl(vn_work_id, 0) <= 0
        then
          continue;
        end if;
        vt_qworkers(vn_work_id).response_lasttime := vs_response_lasttime;
        vn_work_mes := it_xml.token_substr(vc_msg, '#', 3);
        vt_qworkers(vn_work_id).response_last := vn_work_mes;
        vt_qworkers(vn_work_id).worker_free := 0;
        vt_qworkers(vn_work_id).job_starttime := LEAST(nvl(vt_qworkers(vn_work_id).job_starttime, vs_response_lasttime), vs_response_lasttime);
        case vn_work_mes
          when 'F' then
            -- Свободен 
            vt_qworkers(vn_work_id).worker_free := 1;
            vt_qworkers(vn_work_id).run_coins := 0;
            vt_qworkers(vn_work_id).run_count := 0;
            vt_qworkers(vn_work_id).run_stoptime := vt_qworkers(vn_work_id).response_lasttime; -- флаг сброса счетчика 
          when 'S' then
            -- Стартовал 
            vt_qworkers(vn_work_id).run_stoptime := vt_qworkers(vn_work_id).response_lasttime; -- флаг сброса счетчика 
          when 'R' then
            -- Рестарт (с командой для остальных) 
            gd_last_worker_restart := greatest(nvl(gd_last_worker_restart, vt_qworkers(vn_work_id).response_lasttime), vt_qworkers(vn_work_id).response_lasttime);
          when 'W' then
            -- Выполнил задание успешно        
            vc_r_proc := it_xml.token_substr(vc_msg, '#', 4);
            vc_r_price := nvl(to_number(it_xml.token_substr(vc_msg, '#', 5) default null on conversion error), gn_service_done_price);
            vc_r_price_plan := nvl(to_number(it_xml.token_substr(vc_msg, '#', 6) default null on conversion error), gn_service_done_price); -- Цена плановая
            vt_qworkers(vn_work_id).run_coins := nvl(vt_qworkers(vn_work_id).run_coins, 0) + (vc_r_price - vc_r_price_plan); -- Разница от плана
            vt_qworkers(vn_work_id).run_count := nvl(vt_qworkers(vn_work_id).run_count, 0) - 1;
            if gt_service.exists(vc_r_proc)
            then
              gt_service(vc_r_proc).calc_stat_p := ((gt_service(vc_r_proc).calc_stat_p * gt_service(vc_r_proc).calc_stat_c) + vc_r_price) / (gt_service(vc_r_proc).calc_stat_c + 1);
              gt_service(vc_r_proc).calc_stat_c := gt_service(vc_r_proc).calc_stat_c + 1;
              --debug(vs_manager || ' STORE  ' || vc_r_proc || ' calc_stat_p = ' || gt_service(vc_r_proc).calc_stat_p || ' calc_stat_c = ' || gt_service(vc_r_proc).calc_stat_c);
            else
              gn_service_done_price := round((vc_r_price + gn_service_done_price * (GN_SERVICE_KFAVG - 1)) / GN_SERVICE_KFAVG); --  Усредняем
            end if;
          when 'E' then
            -- Ошибка при выполнении задания        
            vc_r_proc := it_xml.token_substr(vc_msg, '#', 4);
            vc_r_price := nvl(to_number(it_xml.token_substr(vc_msg, '#', 5) default null on conversion error), gn_service_done_price);
            vc_r_price_plan := nvl(to_number(it_xml.token_substr(vc_msg, '#', 6) default null on conversion error), gn_service_done_price);
            vt_qworkers(vn_work_id).run_coins := nvl(vt_qworkers(vn_work_id).run_coins, 0) + (vc_r_price - vc_r_price_plan); -- Разница от плана
            vt_qworkers(vn_work_id).run_count := nvl(vt_qworkers(vn_work_id).run_count, 0) - 1;
            if not gt_service.exists(vc_r_proc)
            then
              gn_service_error_price := round((vc_r_price + gn_service_error_price * (GN_SERVICE_KFAVG - 1)) / GN_SERVICE_KFAVG); -- Усредняем
            end if;
          when 'N' then
            -- Начал выполнение заданий
            vt_qworkers(vn_work_id).run_starttime := least(nvl(vt_qworkers(vn_work_id).run_starttime, systimestamp), vt_qworkers(vn_work_id).response_lasttime);
          else
            null;
        end case;
      end loop;
      exit when vt_qworkers.count = 0 and(not v_PipeMNG or pipemanager_quit); -- Выходим если нечего обновлять
      --debug(vs_manager || ' workers_flush PIPE ' || it_xml.calc_interval_millisec(vs_start_pipe, systimestamp) || '/' || vn_cnt_pipe_msg);
      --vs_start := systimestamp;
      vn_poz := vt_qworkers.first;
      while vn_poz is not null
            and vn_poz > 0
      loop
        begin
          select w.run_lasttime
                ,w.response_lasttime
            into vs_run_lasttime
                ,vs_response_lasttime
            from itt_q_worker w
           where w.worker_num = vn_poz
             for update nowait;
        exception
          when no_data_found then
            --debug(vs_manager || ' workers_flush ' || vn_poz || ': NOT FOUND - DELETE ');
            tmp    := vn_poz;
            vn_poz := vt_qworkers.next(vn_poz);
            vt_qworkers.delete(tmp);
            continue;
          when row_locked then
            --debug(vs_manager || ' workers_flush ' || vn_poz || ': NO LOCK ');
            vn_poz := vt_qworkers.next(vn_poz);
            continue;
        end;
        if vt_qworkers(vn_poz).worker_free = 1
            and (vs_run_lasttime is null or vs_run_lasttime < vt_qworkers(vn_poz).response_lasttime) -- Только отправленные после последнего выданного задания
        then
          update_worker_free(p_worker_num => vn_poz
                            ,p_response_last => vt_qworkers(vn_poz).response_last
                            ,p_response_lasttime => vt_qworkers(vn_poz).response_lasttime
                            ,p_job_starttime => coalesce(vt_qworkers(vn_poz).job_starttime, vt_qworkers(vn_poz).run_starttime, vt_qworkers(vn_poz).response_lasttime));
        elsif vt_qworkers(vn_poz).worker_free != 1
               and (vs_response_lasttime is null or vs_response_lasttime < vt_qworkers(vn_poz).response_lasttime)
        then
          update itt_q_worker w
             set w.response_lasttime = vt_qworkers(vn_poz).response_lasttime
                ,w.response_last     = vt_qworkers(vn_poz).response_last
                ,w.worker_free       = 0
                ,w.worker_enabled = case -- Выполняет неизвестные задание или проблемы со счетчиком
                                      when w.run_count + nvl(vt_qworkers(vn_poz).run_count, 0) < decode(vt_qworkers(vn_poz).response_last, 'N', 1, 0) then
                                       0
                                      else
                                       1
                                    end
                ,w.run_starttime = case
                                     when vt_qworkers(vn_poz).run_stoptime is null then
                                      coalesce(w.run_starttime, vt_qworkers(vn_poz).run_starttime, vt_qworkers(vn_poz).response_lasttime, systimestamp)
                                     else
                                      coalesce(vt_qworkers(vn_poz).run_starttime, vt_qworkers(vn_poz).response_lasttime, systimestamp)
                                   end
                ,w.run_count         = w.run_count + nvl(vt_qworkers(vn_poz).run_count, 0)
                 -- Пока нет решения с опоздавшими дельтами исполнения
                 /*,w.run_coins         = greatest(gn_service_done_price * (w.run_count + nvl(vt_qworkers(vn_poz).run_count, 0))
                 ,w.run_coins + nvl(vt_qworkers(vn_poz).run_coins, 0))*/
                ,w.run_stoptime  = nvl2(vt_qworkers(vn_poz).run_stoptime, null, w.run_stoptime) -- Сбрасываем если был F
                ,w.job_starttime = coalesce(vt_qworkers(vn_poz).job_starttime, w.job_starttime, vt_qworkers(vn_poz).run_starttime, vt_qworkers(vn_poz).response_lasttime)
           where w.worker_num = vn_poz;
        end if;
        --debug(vs_manager || '  workers_flush W' || vn_poz || ' ' || vt_qworkers(vn_poz).response_last || '- ' || v_ret1 || '/' || v_ret2);
        commit; -- AUTONOMOUS
        tmp    := vn_poz;
        vn_poz := vt_qworkers.next(vn_poz);
        vt_qworkers.delete(tmp);
      end loop;
      --debug(vs_manager || ' workers_flush ' || it_xml.calc_interval_millisec(vs_start_pipe, systimestamp));
      -- Проверка воркеров для перезапуска по 4061 (по одному за проход)
      if gd_last_worker_restart is not null
         and vt_qworkers.count = 0
      then
        worker_restart(gd_last_worker_restart);
      end if;
      if not v_PipeMNG
      then
        exit when vt_qworkers.count = 0; -- Выходим после одного обновления если это не PIPEManager
        if sysdate > vs_start + numtodsinterval(GN_QUEUE_WAIT, 'SECOND')
        then
          vn_poz := vt_qworkers.first;
          send_information_error('WR_UPD' || vn_poz, 'Не удалось обновить WORKERа № ' || vn_poz);
          exit;
        end if;
      elsif vt_qworkers.count = 0
            and vn_cnt_pipe_msg < gn_min_cnt_flush_pipe
      then
        exit when pipemanager_quit;
        init_qsettings;
        flush_services(true);
      end if;
    end loop;
    commit;
    lock_release(vc_process);
    --debug(vs_manager || ' workers_flush FINISH unlock ' || vc_process);
    return true;
  exception
    when others then
      --debug(vs_manager || ' workers_flush ERROR ');
      v_errno   := sqlcode;
      v_errmess := sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace;
      rollback; -- AUTONOMOUS
      lock_release(vc_process);
      send_information_error('WF' || v_errno, v_errmess);
      return false;
  end;

  -- Блокировка проблемных воркеров 
  procedure worker_mark as
    pragma autonomous_transaction;
    v_errno   integer;
    v_errmess varchar2(2000);
  begin
    if gd_last_worker_mark is null
       or sysdate > gd_last_worker_mark
    then
      -- Проверка времени работы воркеров и метим для перезапуска
      for worker in (select worker_num
                       from itt_q_worker
                      where worker_enabled > 0
                        and job_stoptime is null
                        and --((response_last = 'F' and response_lasttime < sysdate - numtodsinterval(GN_QUEUE_WAIT * 3, 'SECOND')) or -- Не в процессе выполнения и нет отклика > 3 периодов
                            (response_last != 'F' and run_stoptime < sysdate)
                        for update skip locked)
      loop
        update itt_q_worker w
           set w.worker_enabled = 0
              ,w.run_stoptime   = sysdate
         where worker_num = worker.worker_num;
      end loop;
      commit;
      gd_last_worker_mark := sysdate;
    end if;
    commit;
  exception
    when others then
      v_errno   := sqlcode;
      v_errmess := sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace;
      rollback; -- AUTONOMOUS
      send_information_error('WM' || v_errno, v_errmess);
  end;

  --=================================================================================================
  --  Перезапуск зависших работников
  procedure workers_check is
    vc_jenabled sys.user_scheduler_jobs.enabled%type;
    vc_jstate   sys.user_scheduler_jobs.state%type;
    vc_job_name varchar(32);
  begin
    for w in (select *
                from itt_q_worker w
               where w.worker_enabled = 0
                 and w.run_stoptime <= systimestamp
               order by worker_num)
    loop
      vc_job_name := С_JOB_WORKER_PREFIX || w.worker_num;
      begin
        select enabled -- 'TRUE'
              ,state -- 'RUNNING'
          into vc_jenabled
              ,vc_jstate
          from sys.user_scheduler_jobs
         where job_name = vc_job_name;
      exception
        when no_data_found then
          vc_jstate := null;
      end;
      if vc_jstate = 'RUNNING'
         and w.job_stoptime is null
      then
        update itt_q_worker set job_stoptime = systimestamp where worker_num = w.worker_num;
        commit;
        worker_stop(w.worker_num);
      else
        if vc_jstate = 'RUNNING'
           and sysdate >= w.job_stoptime
        then
          send_information('STOP#' || w.worker_num
                          ,'Останов ' || vc_job_name || case when w.response_last = 'N' then
                           chr(13) || ' последний процесс ' || w.servicename || ' запущен ' || to_char(w.run_starttime, 'DD-MM-YYYY HH24:MI:SS') --
                           when w.response_last is null then chr(13) || ' Не запустился при старте ! ' --
                           else chr(13) || ' последний ответ "' || w.response_last || '" получен ' || to_char(w.response_lasttime, 'DD-MM-YYYY HH24:MI:SS') end);
          --debug('flush_child: stop ' || vc_job_name);
          sys.dbms_scheduler.disable(vc_job_name, true);
          begin
            sys.dbms_scheduler.stop_job(vc_job_name);
            sys.dbms_pipe.purge(get_full_pipe_name(С_WORKER_PIPE_PREFIX, w.worker_num));
          exception
            when others then
              send_information_error('WC' || w.worker_num
                                    ,'Ошибка перезапуска ' || vc_job_name || chr(13) || sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
              continue;
          end;
        end if;
        update itt_q_worker set run_stoptime = null where worker_num = w.worker_num;
        commit;
      end if;
    end loop;
  exception
    when others then
      rollback;
      send_information_error('WC' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  -- Попытка починить работников
  procedure workers_repair is
    vd_last    date;
    vc_process varchar2(100) := 'it_q_manager.workers_repair';
    vb_tmp     boolean;
  begin
    vd_last := it_q_message.get_qset_data('$$WORKER_REPAIR'); -- Раз в 10 мин пытаемся вылечить работников
    if vd_last is null
       or sysdate > vd_last + numtodsinterval(10, 'MINUTE')
    then
      if lock_request(vc_process)
      then
        --debug('Manager' || gс_manager_queue_num || ' workers_repair ');
        it_q_message.set_qset_data('$$WORKERS_REPAIR', sysdate);
        init_qsettings;
        vb_tmp := workers_flush;
        workers_check;
      end if;
    end if;
    lock_release(vc_process);
  exception
    when others then
      lock_release(vc_process);
      send_information_error('WR' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  --- Процент нагрузки воркеров (100% - все занято )
  function get_worker_load_percent return pls_integer as
  begin
    return round(((get_count_worker_run(IT_Q_MESSAGE.C_C_MSG_PRIORITY_N) + get_count_workerX_run) / (gn_worker_max_count - gn_worker_f_count)) * 100);
  end;

  --- Количество заданий в обработке
  function get_count_work_message(p_max_count integer default null) return integer as
    vn_msq_cnt  integer;
    v_max_count integer := p_max_count + 1;
  begin
    select count(*) into vn_msq_cnt from itt_q_work_messages where rownum <= nvl(v_max_count, rownum);
    return vn_msq_cnt;
  end;

  --- Количество заданий в очереди
  function get_count_task_message(p_queue_num itt_q_message_log.queue_num%type -- Номер очереди
                                 ,p_uniq_sg   integer default null) return integer as
    vn_msq_cnt integer;
    v_uniq_sg  integer := nvl(p_uniq_sg, 0);
    v_sql      varchar2(2000);
  begin
    if v_uniq_sg != 0
    then
      v_sql := ' count(distinct servicegroup) + nvl(sum(nvl2(servicegroup,0,1)),0) ';
    else
      v_sql := ' count(*) ';
    end if;
    execute immediate 'select ' || v_sql || ' from ' || IT_Q_MESSAGE.C_C_QVIEW_TASK_PREFIX || p_queue_num
      into vn_msq_cnt;
    return vn_msq_cnt;
  end;

  function get_run_lasttime_worker(p_priority itt_q_message_log.priority%type default null) return timestamp as
    vs_run_last timestamp;
  begin
    select min(nvl(w.run_lasttime, sysdate - 1))
      into vs_run_last
      from itt_q_worker w
     where worker_priority = nvl(p_priority, worker_priority)
       and job_stoptime is null
       and worker_enabled > 0
       and worker_free > 0;
    return vs_run_last;
  end;

  -- Определение  возможности размещения сообщения в посточередь работника
  function get_type_apostq(p_message_type  itt_q_message_log.message_type%type
                          ,p_delivery_type itt_q_message_log.delivery_type%type
                          ,p_servicename   itt_q_message_log.servicename%type
                          ,p_servicegroup  itt_q_message_log.servicegroup%type) return integer as
    v_res           integer := 0;
    v_c_key_service varchar2(2000) := get_gt_service_key(p_message_type, p_servicename);
  begin
    case
      when p_servicename is null then
        v_res := 0; -- Поиск свободного воркера
      when not gt_service.exists(v_c_key_service)
           or gt_service(v_c_key_service).service_proc is null then
        v_res := 2; -- Только без запуска сервиса размещаются в посточередь сразу 
      when p_servicegroup is not null then
        v_res := -1; -- Поиск потока исполнения
      when nvl(p_delivery_type, IT_Q_MESSAGE.C_C_MSG_DELIVERY_S) = IT_Q_MESSAGE.C_C_MSG_DELIVERY_A then
        v_res := 1; -- асинхрон может размещаться в посточередь.
      else
        v_res := 0;
    end case;
    return v_res;
  end;

  -- Определение  и блокировка свободного работника  
  function get_worker_free(p_priority      itt_q_message_log.priority%type
                          ,p_message_type  itt_q_message_log.message_type%type default IT_Q_MESSAGE.C_C_MSG_TYPE_R
                          ,p_delivery_type itt_q_message_log.delivery_type%type default IT_Q_MESSAGE.C_C_MSG_DELIVERY_S
                          ,p_servicename   itt_q_message_log.servicename%type default null
                          ,p_servicegroup  itt_q_message_log.servicegroup%type default null
                          ,p_queue_num     itt_q_message_log.queue_num%type default null
                          ,p_enqtdt        timestamp default systimestamp
                          ,p_nowait        boolean default false
                          ,p_noorder       boolean default false) return integer as
    row_locked exception;
    pragma exception_init(row_locked, -54);
    vn_work_id integer;
    vd_start   timestamp := systimestamp;
    --vn_worker_cnt    integer;
    --v_systimestamp  timestamp := systimestamp; --nvl(p_enqtdt, systimestamp);
    v_tmp           boolean := false;
    v_workers_flush boolean := false;
    v_worker_plus   boolean := false;
    v_nowait  constant boolean := nvl(p_nowait, false);
    v_noorder constant boolean := nvl(p_noorder, false);
    v_enqtdt         timestamp := nvl(p_enqtdt, systimestamp);
    v_price_prestart integer;
    v_systimestamp   timestamp := systimestamp;
    v_type_apostq    number := get_type_apostq(p_message_type => p_message_type
                                              ,p_delivery_type => p_delivery_type
                                              ,p_servicename => p_servicename
                                              ,p_servicegroup => p_servicegroup);
    v_select_a_      varchar2(2000) := 'select worker_num from itt_q_worker w
where worker_num in
  (select worker_num from 
     (select w.*, row_number() over(partition by worker_free order by worker_num) as rn_free from 
        (select w.*, count(*) over() as wrk_cnt
                , sum(case when worker_priority = ''' || IT_Q_MESSAGE.C_C_MSG_PRIORITY_N || '''
                               and worker_enabled > 0  and worker_free > 0 then 1
                           else 0 end) over() as wrk_free_cnt
         from itt_q_worker w  where job_stoptime is null) w
      where worker_enabled > 0 and worker_priority = ''' || IT_Q_MESSAGE.C_C_MSG_PRIORITY_N || '''
             and (worker_free > 0 or (worker_free = 0  and servicegroup is null and stop_apostq = 0 and run_count >= 0 )))
   where (worker_free > 0 and rn_free > ' || gn_mincount_free_for_s || ') 
          '; -- gn_mincount_free_for_s свободных оставляем под синхрон
    v_select         varchar2(4000);
    cur_a            sys_refcursor;
    --v_debug          varchar2(100) := 'Manager' || gс_manager_queue_num || 'get_worker_free MSG-' || p_message_type || p_delivery_type || p_priority || ' ';
  begin
    --debug(v_debug || ' Start');
    loop
      --debug(v_debug || '  loop');
      if v_type_apostq = -1 -- Поиск потока исполнения
      then
        begin
          select t.worker_num
            into vn_work_id
            from itt_q_worker t
           where t.servicegroup = p_servicegroup
             and t.queue_num = p_queue_num
             and rownum < 2
             for update nowait;
        exception
          when no_data_found then
            vn_work_id := get_worker_free(p_priority => IT_Q_MESSAGE.C_C_MSG_PRIORITY_N
                                         ,p_message_type => p_message_type
                                         ,p_delivery_type => p_delivery_type
                                         ,p_servicename => p_servicename
                                         ,p_enqtdt => p_enqtdt
                                         ,p_nowait => v_nowait
                                         ,p_noorder => v_noorder);
          when others then
            vn_work_id := null; -- ждем 
        end;
      else
        begin
          case
            when v_type_apostq > 0 then
              case
                when v_type_apostq = 2 then
                  -- не больше gn_norm_run_coins_apostq*10 
                  v_select := v_select_a_ ||
                              ' or worker_free = 0 )
order by case when worker_free = 0 
                   and greatest(1,( run_coins - it_xml.calc_interval_millisec(nvl(run_lasttime, :v_systimestamp), :v_systimestamp))) <= ' ||
                              (gn_norm_run_coins_apostq * 10) || ' then worker_num
              when worker_free > 0 then ' || (gn_worker_max_count + 1) || '
              else ' || (gn_worker_max_count + 2) || ' end 
        , case when worker_free = 0
                    then  greatest(1,( run_coins - it_xml.calc_interval_millisec(nvl(run_lasttime, :v_systimestamp), :v_systimestamp)))
              else 0 end';
                else
                  -- Занятые только если свободных меньше менеджеров 
                  v_select := v_select_a_ || ' or (worker_free = 0  
         and wrk_free_cnt - ' || gn_mincount_free_for_s || ' < ' || IT_Q_MESSAGE.get_count_queue || '))
order by case when worker_free = 0
              then  greatest(1,( run_coins - it_xml.calc_interval_millisec(nvl(run_lasttime, :v_systimestamp), :v_systimestamp)))
         else 0 end ';
              end case;
              v_select := v_select || '
        ,run_count
        ,worker_num ' || case
                            when v_noorder then
                             'DESC'
                          end || ' for update skip locked';
              v_price_prestart := it_xml.calc_interval_millisec(v_enqtdt, systimestamp);
              v_systimestamp   := systimestamp;
              if v_type_apostq = 2
              then
                open cur_a for v_select
                  using v_systimestamp, v_systimestamp, v_systimestamp, v_systimestamp;
              else
                open cur_a for v_select
                  using v_systimestamp, v_systimestamp;
              end if;
            when p_priority = IT_Q_MESSAGE.C_C_MSG_PRIORITY_F -- Быстрая
             then
              v_select := 'select worker_num
from itt_q_worker
where worker_enabled > 0
     and job_stoptime is null
     and worker_free > 0
order by worker_priority desc 
        ,worker_num ' || case
                            when v_noorder then
                             'DESC'
                          end || ' for update skip locked '; -- Сначала N потом F
              open cur_a for v_select;
            else
              v_select := ' select worker_num
from itt_q_worker
where worker_priority = ''' || IT_Q_MESSAGE.C_C_MSG_PRIORITY_N || '''
     and worker_enabled > 0
     and job_stoptime is null 
     and worker_free > 0
order by worker_num  ' || case
                            when v_noorder then
                             'DESC'
                          end || ' for update skip locked ';
              open cur_a for v_select;
          end case;
          fetch cur_a
            into vn_work_id;
          close cur_a;
        exception
          when no_data_found then
            if cur_a%isopen
            then
              close cur_a;
            end if;
            vn_work_id := null;
          when others then
            if cur_a%isopen
            then
              close cur_a;
            end if;
            send_information_error('WA' || sqlcode
                                  ,sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace || ' v_type_apostq = ' || v_type_apostq || utl_tcp.crlf || ' v_select =  ' ||
                                   v_select);
            vn_work_id := null;
        end;
      end if;
      /*if vn_work_id is not null
      then
        debug(v_debug || ' -' || vn_work_id || '-' || it_xml.calc_interval_millisec(v_systimestamp, systimestamp), v_select);
      else
        debug(v_debug || ' NO ' || it_xml.calc_interval_millisec(v_systimestamp, systimestamp), v_select);
      end if;*/
      --
      exit when vn_work_id is not null or v_nowait;
      --
      if v_workers_flush
      then
        v_tmp         := worker_plus();
        v_worker_plus := true;
        --debug(v_debug || ' worker_plus');
      end if;
      --
      if sysdate > vd_start + numtodsinterval((GN_QMANAGER_REPEAT_WORKER * GN_QUEUE_WAIT), 'SECOND')
      then
        -- Сообщаем о проблеме и выходим (((
        send_information('QM_STOP', 'QManager ОСТАНОВЛЕН ! Нет свободных WORKERов ' || to_char(GN_QMANAGER_REPEAT_WORKER * GN_QUEUE_WAIT) || ' сек.');
        exit;
      end if;
      --
      if sysdate > vd_start + numtodsinterval(GREATEST(gn_worker_plus_interval, GN_QUEUE_WAIT) * 2, 'SECOND')
      then
        -- пытаемся перезапустить зависших работников 
        workers_repair;
      end if;
      if sysdate > vd_start + numtodsinterval(GREATEST(gn_worker_plus_interval, GN_QUEUE_WAIT), 'SECOND')
      then
        send_information('NOT_FREE_WORKER#' || p_delivery_type || p_priority);
        workers_refresh;
      end if;
      --
      if not workers_flush
      then
        -- Обновляем инфу о обработчиках
        -- Запущен PManager  сразу добавляем воркер
        --debug(v_debug || ' workers_flush false');
        v_tmp         := worker_plus();
        v_worker_plus := true;
      end if;
      v_workers_flush := true;
    end loop;
    return vn_work_id;
  exception
    when others then
      send_information_error('WF' || sqlcode, (sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace || utl_tcp.crlf));
      return null;
  end;

  --Выключение работника
  procedure worker_minus as
    pragma autonomous_transaction;
    vc_sid     varchar2(100) := '$$WORKER_MINUS';
    vd_last_dt date;
    --v_priority_n itt_q_worker.worker_priority%type := IT_Q_MESSAGE.C_C_MSG_PRIORITY_N;
    v_worker_num integer;
    vc_process   varchar2(100) := 'it_q_manager.worker_minus';
    vb_tmp       boolean;
  begin
    vd_last_dt := it_q_message.get_qset_data(p_qset_name => vc_sid);
    if vd_last_dt is null
       or sysdate > vd_last_dt + numtodsinterval(gn_worker_minus_interval, 'SECOND')
    then
      if lock_request(vc_process)
      then
        --debug('Manager' || gс_manager_queue_num || ' worker_minus ');
        it_q_message.set_qset_data(vc_sid, sysdate);
        if vd_last_dt > sysdate - numtodsinterval(1.5 * (gn_worker_minus_interval + GN_QUEUE_WAIT), 'SECOND') -- Приступаем только если спокойный период
        then
          vb_tmp := workers_flush;
          if get_count_worker_free() > gn_worker_min_count -- Если свободных больше минимума 
             and get_run_lasttime_worker(IT_Q_MESSAGE.C_C_MSG_PRIORITY_N) < sysdate - numtodsinterval(gn_worker_minus_interval, 'SECOND') -- и есть давно не загруженные   
          then
            v_worker_num := get_worker_free(p_priority => IT_Q_MESSAGE.C_C_MSG_PRIORITY_N, p_nowait => true, p_noorder => true);
            if v_worker_num is not null
            then
              update itt_q_worker w set w.job_stoptime = sysdate where w.worker_num = v_worker_num;
              commit;
              --debug('Manager' || gс_qmanager_queue_num || ' worker_minus ' || v_worker_num);
            end if;
          end if;
        end if;
        lock_release(vc_process);
      end if;
    end if;
  exception
    when others then
      rollback;
      lock_release(vc_process);
      send_information_error('WM' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  -- Перенос зависших заданий 
  procedure task_transfer as
    pragma autonomous_transaction;
    row_locked exception;
    pragma exception_init(row_locked, -54);
    v_errno          integer;
    v_errmess        varchar2(2000);
    vn_work_new      integer;
    vn_work_old      integer;
    v_qmsgid         raw(16);
    vc_process       varchar2(100) := 'it_q_manager.task_transfer';
    v_msg_delivery_a itt_q_message_log.delivery_type%type := IT_Q_MESSAGE.C_C_MSG_DELIVERY_A;
    vb_tmp           boolean;
    v_stop_apostq    itt_q_service.stop_apostq%type;
  begin
    if (gd_last_task_transfer is null or sysdate > gd_last_task_transfer)
       and get_count_worker_free(IT_Q_MESSAGE.C_C_MSG_PRIORITY_N) > gn_mincount_free_for_s
    then
      if lock_request(vc_process)
      then
        --debug('Manager' || gс_manager_queue_num || ' task_transfer ');
        -- Проверка заданий для переноса
        for task in (select t.qmsgid
                           ,t.worker_num
                           ,t.service_delivery
                           ,t.servicename
                           ,t.service_price
                       from itt_q_work_messages t
                      where t.worker_num > 0
                        and t.work_ready is null
                        and t.servicegroup is null
                        and t.service_delivery = v_msg_delivery_a
                        and t.create_time < sysdate - numtodsinterval(1, 'SECOND')
                        and it_xml.calc_interval_millisec(t.enqdt, systimestamp) > gn_norm_run_coins_apostq -- Задержка 
                      order by t.enqdt)
        loop
          vb_tmp := workers_flush;
          if get_count_worker_free(IT_Q_MESSAGE.C_C_MSG_PRIORITY_N) > gn_mincount_free_for_s
          then
            begin
              select t.qmsgid
                into v_qmsgid
                from itt_q_work_messages t
               where t.work_ready is null
                 and t.qmsgid = task.qmsgid
                 and t.worker_num = task.worker_num
                 for update nowait;
              select w.worker_num into vn_work_old from itt_q_worker w where w.worker_num = task.worker_num for update nowait;
              vn_work_new := get_worker_free(p_priority => IT_Q_MESSAGE.C_C_MSG_PRIORITY_N, p_nowait => true);
              if vn_work_new is not null
              then
                update itt_q_work_messages m
                   set m.worker_num  = vn_work_new
                      ,m.create_time = systimestamp
                 where m.qmsgid = task.qmsgid
                   and m.worker_num = task.worker_num;
                if gt_service.exists(task.servicename)
                then
                  v_stop_apostq := gt_service(task.servicename).stop_apostq;
                else
                  v_stop_apostq := 0;
                end if;
                update itt_q_worker w
                   set worker_free      = 0
                      ,run_coins = run_coins + case
                                     when worker_num = vn_work_old then
                                      0 --  task.service_price не уменьшаем - уже просрочено 
                                     else
                                      (task.service_price + gn_service_startprice)
                                   end
                      ,run_count = run_count + case
                                     when worker_num = vn_work_old then
                                      -1
                                     else
                                      1
                                   end
                      ,service_delivery = case
                                            when worker_num = vn_work_old then
                                             service_delivery
                                            else
                                             task.service_delivery
                                          end
                      ,servicename = case
                                       when worker_num = vn_work_old then
                                        servicename
                                       else
                                        task.servicename
                                     end
                      ,stop_apostq = case
                                       when worker_num = vn_work_old then
                                        stop_apostq
                                       else
                                        v_stop_apostq
                                     end
                      ,run_starttime = case
                                         when worker_num = vn_work_old then
                                          run_starttime
                                         else
                                          systimestamp
                                       end
                      ,run_lasttime = case
                                        when worker_num = vn_work_old then
                                         run_lasttime
                                        else
                                         systimestamp
                                      end
                 where w.worker_num in (vn_work_old, vn_work_new);
                commit;
                pipe_send_mess('W', С_WORKER_PIPE_PREFIX, vn_work_new);
              else
                rollback;
                exit;
              end if;
            exception
              when no_data_found
                   or row_locked then
                rollback;
                continue;
            end;
          else
            exit;
          end if;
        end loop;
      end if;
      lock_release(vc_process);
      gd_last_task_transfer := sysdate;
    end if;
    rollback;
  exception
    when others then
      rollback; -- AUTONOMOUS
      lock_release(vc_process);
      v_errno   := sqlcode;
      v_errmess := sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace;
      send_information_error('TTR' || v_errno, v_errmess);
  end;

  function msg_task_is_work(p_conrol_result number
                           ,p_c_key_service varchar2
                           ,p_message_type  itt_q_message_log.message_type%type
                           ,p_delivery_type itt_q_message_log.delivery_type%type
                           ,p_CORRmsgid     itt_q_message_log.corrmsgid%type) return boolean as
  begin
    return(p_conrol_result = 0 or (p_conrol_result > 0 and (p_message_type = IT_Q_MESSAGE.C_C_MSG_TYPE_A or
           (p_delivery_type = IT_Q_MESSAGE.C_C_MSG_DELIVERY_A and
           p_conrol_result not in (C_N_ERROR_OTHERS_MSGCODE, C_N_ERROR_RESTART_MSGCODE) and p_CORRmsgid is not null)))) --
    and gt_service.exists(p_c_key_service) and gt_service(p_c_key_service).service_proc is not null;
  end;

  function msg_task_get_next_status(p_status        varchar2
                                   ,p_c_key_service varchar2
                                   ,p_conrol_result number
                                   ,p_message_type  itt_q_message_log.message_type%type
                                   ,p_delivery_type itt_q_message_log.delivery_type%type
                                   ,p_CORRmsgid     itt_q_message_log.corrmsgid%type) return varchar2 as
    --
  begin
    case
      when p_conrol_result < 0 then
        return IT_Q_MESSAGE.C_STATUS_TRASH;
      when p_conrol_result > 0 then
        if p_status is null
        then
          if msg_task_is_work(p_conrol_result => p_conrol_result
                             ,p_c_key_service => p_c_key_service
                             ,p_message_type => p_message_type
                             ,p_delivery_type => p_delivery_type
                             ,p_CORRmsgid => p_CORRmsgid)
          then
            return IT_Q_MESSAGE.C_STATUS_ERRWORK;
          else
            return IT_Q_MESSAGE.C_STATUS_ERRLOAD;
          end if;
        else
          return IT_Q_MESSAGE.C_STATUS_ERROR;
        end if;
      else
        if p_status is null
        then
          if msg_task_is_work(p_conrol_result => p_conrol_result
                             ,p_c_key_service => p_c_key_service
                             ,p_message_type => p_message_type
                             ,p_delivery_type => p_delivery_type
                             ,p_CORRmsgid => p_CORRmsgid)
          then
            return IT_Q_MESSAGE.C_STATUS_WORK;
          else
            return IT_Q_MESSAGE.C_STATUS_LOAD;
          end if;
        elsif p_status = IT_Q_MESSAGE.C_STATUS_ERRWORK
        then
          return IT_Q_MESSAGE.C_STATUS_ERRDONE;
        else
          return IT_Q_MESSAGE.C_STATUS_DONE;
        end if;
    end case;
  end;

  procedure msg_task_dequeue(p_qmsgid      raw default null -- guid сообщения в очереди
                            ,p_msgid       itt_q_message_log.msgid%type default null
                            ,p_correlation itt_q_message_log.correlation%type default null
                            ,p_queue_num   itt_q_message_log.queue_num%type -- Номер очереди 
                            ,p_startdt     itt_q_message_log.statusdt%type
                            ,p_commanddt   itt_q_message_log.commanddt%type
                            ,p_workdt      itt_q_message_log.workdt%type
                            ,p_workername  itt_q_message_log.workername%type
                            ,o_errno       out integer
                            ,o_errmsg      out varchar2
                            ,o_msgid       out itt_q_message_log.msgid%type
                            ,o_message     out it_q_message_t) is
    deque_time_out exception;
    pragma exception_init(deque_time_out, -25228);
    dequeXX_time_out exception;
    pragma exception_init(deque_time_out, -20228);
    v_enqdt         timestamp;
    v_correlation   varchar2(128);
    v_c_key_service varchar2(2000);
    v_msgcode       itt_q_message_log.msgcode%type;
    v_msgtext       itt_q_message_log.msgtext%type;
  begin
    o_errno := 0;
    begin
      it_q_message.msg_dequeue_in(p_qmsgid => p_qmsgid
                                 ,p_msgid => p_msgid
                                 ,p_correlation => p_correlation
                                 ,p_queue_num => p_queue_num
                                 ,p_wait => 0
                                 ,p_toState => case
                                                 when p_queue_num = it_q_message.C_C_QUEUENUM_XX then
                                                  it_q_message.C_N_QUEUEXX_STATE_RUN
                                               end
                                 ,o_msgcode => v_msgcode
                                 ,o_msgtext => v_msgtext
                                 ,o_enqdt => v_enqdt
                                 ,o_correlation => v_correlation
                                 ,o_message => o_message);
    exception
      when deque_time_out
           or dequeXX_time_out then
        o_errno := null; -- Нет сообщения # ' || p_qmsgid;
      when others then
        o_errno  := null;
        o_errmsg := sqlerrm;
    end;
    if o_errno is null
    then
      return;
    end if;
    begin
      v_c_key_service := get_gt_service_key(o_message.message_type, o_message.servicename);
      if v_msgcode = 4061
      then
        o_errno := v_msgcode;
      elsif o_message.message_type = IT_Q_MESSAGE.C_C_MSG_TYPE_A
            and o_message.delivery_type != IT_Q_MESSAGE.C_C_MSG_DELIVERY_A
      then
        o_errmsg := 'Тип сообщения должен быть "R"(запрос) или оно должно быть асинхронным!';
        o_errno  := C_N_ERROR_OTHERS_MSGCODE;
      elsif o_message.message_type = IT_Q_MESSAGE.C_C_MSG_TYPE_R
            and o_message.servicename is null
      then
        o_errmsg := 'Не указан обработчик сообщения (ServiceName) !';
        o_errno  := C_N_ERROR_OTHERS_MSGCODE;
      elsif o_message.message_type = IT_Q_MESSAGE.C_C_MSG_TYPE_R
            and not gt_service.exists(v_c_key_service)
      then
        o_errmsg := 'Обработчик сообщения (ServiceName) "' || o_message.servicename || '" не найден !';
        o_errno  := C_N_ERROR_OTHERS_MSGCODE;
      end if;
      if o_errno >= 0
         and o_message.corrmsgid is not null
      then
        if o_message.message_type = IT_Q_MESSAGE.C_C_MSG_TYPE_A
        then
          -- Установка флага на исходное сообщение для ответа 
          flag_corr_is_work(p_msgid => o_message.corrmsgid, p_set => 1, o_errno => o_errno, p_comment => o_errmsg);
          if o_errno < 0
          then
            o_errmsg := 'Система ' || IT_Q_MESSAGE.C_C_SYSTEMNAME || ' не ожидает этот асинхронный ответ !';
          end if;
        else
          -- Смена статуса для запроса с ожиданием  
          update itt_q_message_log l
             set l.status = IT_Q_MESSAGE.C_STATUS_DELIVERED
           where l.message_type = IT_Q_MESSAGE.C_C_MSG_TYPE_R
             and l.delivery_type = IT_Q_MESSAGE.C_C_MSG_DELIVERY_A
             and l.status = IT_Q_MESSAGE.C_STATUS_DELIVEREDQUERY
             and l.msgid = o_message.corrmsgid
             and l.queuetype = IT_Q_MESSAGE.C_C_QUEUE_TYPE_OUT;
        end if;
      end if;
    exception
      when others then
        o_errno  := -1;
        o_errmsg := 'Ошибка при проверке сообщения :' || substr(sqlerrm, 1, 1500);
        --debug('Processor' || p_worker_num || '  error ' || o_errmsg);
        send_information_error('CM' || sqlcode, o_errmsg || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
    end;
    begin
      it_q_message.messlog_insert_out(p_message => o_message
                                     ,p_correlation => v_correlation
                                     ,p_queuename => IT_Q_MESSAGE.C_C_QUEUE_IN_PREFIX || p_queue_num
                                     ,p_status => msg_task_get_next_status(p_status => null
                                                                          ,p_c_key_service => v_c_key_service
                                                                          ,p_conrol_result => o_errno
                                                                          ,p_message_type => o_message.message_type
                                                                          ,p_delivery_type => o_message.delivery_type
                                                                          ,p_CORRmsgid => o_message.CORRmsgid)
                                     ,p_qmsgid => p_qmsgid
                                     ,p_enqdt => v_enqdt
                                     ,p_startdt => p_startdt
                                     ,p_commanddt => p_commanddt
                                     ,p_workdt => p_workdt
                                     ,p_workername => p_workername
                                     ,p_comment => case
                                                     when o_errno != o_message.msgcode then
                                                      it_q_message.get_comment_add(p_msgcode => o_errno, p_add_comment => o_errmsg)
                                                   end
                                     ,o_logid => o_msgid
                                     ,o_messbody => o_message.messbody
                                     ,o_messmeta => o_message.messmeta);
    exception
      when others then
        o_errno  := -1;
        o_errmsg := 'Ошибка записи в worklog :' || substr(sqlerrm, 1, 1000);
        it_q_message.messlog_insert(p_message => o_message
                                   ,p_correlation => v_correlation
                                   ,p_queuename => IT_Q_MESSAGE.C_C_QUEUE_IN_PREFIX || p_queue_num
                                   ,p_status => IT_Q_MESSAGE.C_STATUS_TRASH
                                   ,p_qmsgid => p_qmsgid
                                   ,p_enqdt => v_enqdt
                                   ,p_startdt => p_startdt
                                   ,p_commanddt => p_commanddt
                                   ,p_workdt => p_workdt
                                   ,p_workername => p_workername
                                   ,p_comment => o_errmsg);
    end;
  end;

  procedure msg_task_execute(p_worklogid    itt_q_message_log.log_id%type
                            ,p_msg          it_q_message_t
                            ,p_workstartdt  timestamp
                            ,io_result      in out number
                            ,io_err         in out varchar2
                            ,o_msgid_result out varchar2
                            ,ocl_result     out clob
                            ,oxml_result    out xmltype
                            ,o_srv_price    out number) as
    vc_func             itt_q_service.servicename%type;
    v_c_comment         varchar2(2000);
    v_c_key_service     varchar2(2000);
    vd_workstartdt      timestamp;
    vn_price_start      integer;
    vc_start_status_msg varchar2(32);
    vn_exec_result      number;
  begin
    v_c_key_service     := get_gt_service_key(p_msg.message_type, p_msg.servicename);
    vc_start_status_msg := msg_task_get_next_status(p_status => null
                                                   ,p_c_key_service => v_c_key_service
                                                   ,p_conrol_result => io_result
                                                   ,p_message_type => p_msg.message_type
                                                   ,p_delivery_type => p_msg.delivery_type
                                                   ,p_CORRmsgid => p_msg.CORRmsgid);
    --
    vn_price_start := it_xml.calc_interval_millisec(p_workstartdt, systimestamp);
    if msg_task_is_work(p_conrol_result => io_result
                       ,p_c_key_service => v_c_key_service
                       ,p_message_type => p_msg.message_type
                       ,p_delivery_type => p_msg.delivery_type
                       ,p_CORRmsgid => p_msg.CORRmsgid)
    then
      set_session_action(v_c_key_service);
      vc_func        := gt_service(v_c_key_service).service_proc;
      vn_exec_result := null; -- Результат работы сервиса
      loop
        vd_workstartdt := systimestamp; --  Начало обработки
        io_err         := null;
        v_c_comment    := null;
        begin
          execute immediate 'DECLARE pragma autonomous_transaction; v_outMSGCode integer; BEGIN ' || vc_func ||
                            '(:worklogid,:messbody,:messmeta,:outmsgid,v_outMSGCode,:MSGText,:outbody,:outmeta);' ||
                            ' if nvl(v_outMSGCode,0) = 0 then commit; else rollback; end if;' || ' :outMSGCode := v_outMSGCode; END;'
            using in p_worklogid, in p_msg.messbody, p_msg.messmeta, out o_msgid_result, out io_err, out ocl_result, out oxml_result, out vn_exec_result;
          vn_exec_result := nvl(abs(vn_exec_result), 0);
          v_c_comment    := it_q_message.get_comment_add(p_msgcode => vn_exec_result, p_add_comment => io_err);
        exception
          when others then
            v_c_comment := substr(sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace, 1, 2000);
            io_err      := it_q_message.get_errtxt(sqlerrm);
            if sqlcode in (-4061, -4065, -4068, -6508)
            then
              if vn_exec_result is null -- Повторно выполним команду
              then
                vn_exec_result := C_N_ERROR_RESTART_MSGCODE;
                continue;
              else
                vn_exec_result := C_N_ERROR_RESTART_MSGCODE;
                io_err         := C_C_ERROR_OTHERS_MSGTEXT;
                -- pipe_send_mess(mess_work(p_channel => p_worker_num, p_command => 'R'), С_RESULT_PIPE_PREFIX);
                -- send_information('RESTART#' || p_worker_num, v_c_comment);
                gd_last_worker_restart := sysdate;
              end if;
            else
              vn_exec_result := abs(sqlcode);
              if not vn_exec_result between 20000 and 20999
              then
                vn_exec_result := C_N_ERROR_OTHERS_MSGCODE;
                io_err         := C_C_ERROR_OTHERS_MSGTEXT;
              end if;
            end if;
        end;
        exit;
      end loop;
      io_result := vn_exec_result;
      it_q_message.messlog_upd_status(p_msgid => p_msg.msgid
                                     ,p_delivery_type => p_msg.delivery_type
                                     ,p_queuetype => IT_Q_MESSAGE.C_C_QUEUE_TYPE_IN
                                     ,p_status => msg_task_get_next_status(p_status => vc_start_status_msg
                                                                          ,p_c_key_service => v_c_key_service
                                                                          ,p_conrol_result => io_result
                                                                          ,p_message_type => p_msg.message_type
                                                                          ,p_delivery_type => p_msg.delivery_type
                                                                          ,p_CORRmsgid => p_msg.CORRmsgid)
                                     ,p_comment => v_c_comment);
      set_session_action;
    end if;
    o_srv_price := vn_price_start + it_xml.calc_interval_millisec(vd_workstartdt, systimestamp);
  end;

  procedure msg_task_send_answ(p_start_result varchar2
                              ,pn_result      number
                              ,pc_err         varchar2
                              ,p_msgid_result varchar2
                              ,pcl_result     clob
                              ,pxml_result    xmltype
                              ,p_msg          it_q_message_t) as
    vr_outansver    it_q_message_t;
    vn_sqlcode      integer;
    v_c_comment     varchar2(2000);
    v_c_key_service varchar2(2000);
  begin
    v_c_key_service := get_gt_service_key(p_msg.message_type, p_msg.servicename);
    if pn_result >= 0
       and p_msg.message_type = IT_Q_MESSAGE.C_C_MSG_TYPE_R --  Отправляем ответы только на нетрешовые не пустые запросы и не подписки
       and p_start_result in (0, C_N_ERROR_OTHERS_MSGCODE, C_N_ERROR_RESTART_MSGCODE)
       and (not gt_service.exists(v_c_key_service) or (gt_service(v_c_key_service).subscription <= 0 and gt_service(v_c_key_service).service_proc is not null))
    then
      vr_outansver       := it_q_message.new_message(p_message_type => IT_Q_MESSAGE.C_C_MSG_TYPE_A
                                                    ,p_delivery_type => p_msg.delivery_type
                                                    ,p_Priority => p_msg.priority
                                                    ,p_CORRmsgid => p_msg.msgid
                                                    ,p_ServiceName => p_msg.servicename
                                                    ,p_Receiver => p_msg.sender
                                                    ,p_ServiceGroup => p_msg.servicegroup
                                                    ,p_BTUID => p_msg.BTUID
                                                    ,p_MSGCode => pn_result
                                                    ,p_MSGText => pc_err
                                                    ,p_MESSBODY => pcl_result
                                                    ,p_MessMETA => pxml_result
                                                    ,p_queue_num => p_msg.queue_num
                                                    ,p_check => false);
      vr_outansver.msgid := nvl(p_msgid_result, vr_outansver.msgid);
      begin
        it_q_message.send_message(io_message => vr_outansver, p_queue_num => p_msg.queue_num);
      exception
        when others then
          rollback;
          vn_sqlcode  := sqlcode;
          v_c_comment := substr('Сообщение не отправлено :' || sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace, 1, 2000);
          it_q_message.messlog_insert(p_message => vr_outansver
                                     ,p_correlation => null
                                     ,p_queuename => IT_Q_MESSAGE.C_C_QUEUE_OUT_PREFIX || p_msg.queue_num
                                     ,p_status => IT_Q_MESSAGE.C_STATUS_ERRSEND
                                     ,p_comment => v_c_comment);
          send_information_error('SA' || vn_sqlcode, v_c_comment);
      end;
    end if;
  end;

  procedure do_clear_log as
    pragma autonomous_transaction;
    vr_msg          it_q_message_t;
    vb_need_work    boolean;
    vi_cout_adult   integer := 0;
    vi_cout_error   integer := 0;
    vi_cout_correrr integer := 0;
    --tmp               integer;
    vc_erradd           varchar2(1000);
    cur_q               sys_refcursor;
    vc_cur_q_select     varchar2(2000) := 'select t.qmsgid,t.state,t.correlation
                      ,it_q_message.check_correlation(p_correlation => t.correlation
                                                     ,p_message_type => t.message_type
                                                     ,p_delivery_type => t.delivery_type
                                                     ,p_priority => t.priority
                                                     ,p_msgid => t.msgid
                                                     ,p_corrmsgid => t.corrmsgid) chk
                       ,t.enqdt
                       from ';
    vc_cur_qin_where    varchar2(2000) := ' t
                 where it_q_message.check_correlation(p_correlation => t.correlation
                                                     ,p_message_type => t.message_type
                                                     ,p_delivery_type => t.delivery_type
                                                     ,p_priority => t.priority
                                                     ,p_msgid => t.msgid
                                                     ,p_corrmsgid => t.corrmsgid) != 1 
                    or (t.state = 0 and t.message_type = :type and t.delivery_type = :delivery and t.enqdt < sysdate - numtodsinterval(:period, ''SECOND''))
                    or (t.state in (7,9))';
    vc_cur_qXXin_where  varchar2(2000) := ' t
                 where it_q_message.check_correlation(p_correlation => t.correlation
                                                     ,p_message_type => t.message_type
                                                     ,p_delivery_type => t.delivery_type
                                                     ,p_priority => t.priority
                                                     ,p_msgid => t.msgid
                                                     ,p_corrmsgid => t.corrmsgid) != 1 
                    or (t.state in (0,1) and t.delay <= systimestamp and t.message_type = :type and t.delivery_type = :delivery and t.enqdt < sysdate - numtodsinterval(:period, ''SECOND''))
                    or (t.state in (7,9))';
    vc_cur_qout_where   varchar2(2000) := ' t
                 where (t.state = 0 and t.delivery_type = :delivery and t.enqdt < sysdate - numtodsinterval(:period, ''SECOND''))
                    or (t.state in (7,9)) ';
    vc_cur_qXXout_where varchar2(2000) := ' t
                 where (t.state in (0,1) and t.delay <= systimestamp and t.delivery_type = :delivery and t.enqdt < sysdate - numtodsinterval(:period, ''SECOND''))
                    or (t.state in (7,9)) ';
    vqmsg               itt_q_work_messages.qmsgid%type;
    --vmsgid              itt_q_message_log.msgid%type;
    --vmsg_delivery       itt_q_message_log.delivery_type%type;
    vmsg_enqdt        itt_q_message_log.enqdt%type;
    vchk              number;
    vqmsg_state       integer;
    vqmsg_correlation varchar2(128);
    v_queuename       itt_q_message_log.queuename%type;
    v_chk_delivery    itt_q_message_log.delivery_type%type;
    v_chk_expiration  number;
    v_correlation     itt_q_message_log.correlation%type;
    v_qmsgid          raw(16);
    vc_sid            varchar2(100) := '$$CLEAR_LOG';
    vd_last_dt        date;
    vc_process        varchar2(100) := 'it_q_manager.do_clear_log';
    v_SQLselect       varchar2(32000);
  begin
    if lock_request(vc_process)
    then
      vd_last_dt := it_q_message.get_qset_data(p_qset_name => vc_sid);
      if vd_last_dt is not null
         and sysdate < vd_last_dt + numtodsinterval(gn_period_clear_log, 'SECOND')
      then
        lock_release(vc_process);
        return;
      end if;
      it_q_message.set_qset_data(vc_sid, sysdate);
      -- Разбор ошибочных и устаревших сообщений
      for q in (select column_value queue_num from table(it_q_message.select_queue_num)) -- По всем очередям
      loop
        vi_cout_error   := 0;
        vi_cout_adult   := 0;
        vi_cout_correrr := 0;
        -- Входящие сообщения
        v_SQLselect := vc_cur_q_select || IT_Q_MESSAGE.C_C_QVIEW_IN_PREFIX || q.queue_num || case
                         when q.queue_num = it_q_message.C_C_QUEUENUM_XX then
                          vc_cur_qXXin_where
                         else
                          vc_cur_qin_where
                       end;
        --    dbms_output.put_line(v_SQLselect);
        open cur_q for v_SQLselect
          using IT_Q_MESSAGE.C_C_MSG_TYPE_A, IT_Q_MESSAGE.C_C_MSG_DELIVERY_S, gn_in_as_expiration;
        v_queuename := IT_Q_MESSAGE.C_C_QUEUE_IN_PREFIX || q.queue_num;
        loop
          fetch cur_q
            into vqmsg
                ,vqmsg_state
                ,vqmsg_correlation
                ,vchk
                ,vmsg_enqdt;
          exit when cur_q%notfound;
          if vqmsg_state != 0
          then
            vc_erradd := 'Ошибка состояния сообщения STATE=' || vqmsg_state;
          elsif vqmsg_correlation = IT_Q_MESSAGE.GC_CORR_COMMAND
          then
            vc_erradd := 'Комманда QMANAGERу';
          elsif vchk = 1
          then
            vc_erradd := 'Устаревшее входящее сообщение';
          else
            vc_erradd := 'Ошибка CORRELATION';
          end if;
          begin
            it_q_message.msg_dequeue(p_qmsgid => vqmsg
                                    ,p_queuetype => it_q_message.C_C_QUEUE_TYPE_IN
                                    ,p_queue_num => q.queue_num
                                    ,p_wait => 0
                                    ,p_errno => -1
                                    ,p_comment => vc_erradd
                                    ,o_qmsgid => v_qmsgid
                                    ,o_correlation => v_correlation
                                    ,o_message => vr_msg);
            vb_need_work := true;
          exception
            when others then
              vb_need_work := false;
          end;
          if vb_need_work
          then
            if vqmsg_state != 0
            then
              vi_cout_correrr := vi_cout_correrr + 1;
            elsif vqmsg_correlation = IT_Q_MESSAGE.GC_CORR_COMMAND
            then
              vi_cout_correrr := vi_cout_correrr + 1;
            elsif vchk = 1
            then
              vi_cout_adult := vi_cout_adult + 1;
            else
              vi_cout_correrr := vi_cout_correrr + 1;
            end if;
            it_q_message.messlog_insert(p_message => vr_msg
                                       ,p_correlation => v_correlation
                                       ,p_queuename => v_queuename
                                       ,p_status => IT_Q_MESSAGE.C_STATUS_TRASH
                                       ,p_qmsgid => vqmsg
                                       ,p_enqdt => vmsg_enqdt
                                       ,p_workername => workername(q.queue_num, gn_worker_num)
                                       ,p_comment => vc_erradd);
            commit;
          else
            rollback;
          end if;
        end loop;
        close cur_q;
        if vi_cout_error > 0
        then
          send_information(IT_Q_MESSAGE.C_STATUS_TRASH || '_IN', 'Ошибок вычитки из очереди ' || v_queuename || ' :' || vi_cout_error);
        end if;
        if vi_cout_adult + vi_cout_correrr > 0
        then
          send_information(IT_Q_MESSAGE.C_STATUS_TRASH || '_IN'
                          ,v_queuename || ' Переведено в статус ' || IT_Q_MESSAGE.C_STATUS_TRASH || ' ' || (vi_cout_adult + vi_cout_correrr) ||
                           ' сообщ. из них устаревшиж синхронных ответов ' || vi_cout_adult);
        end if;
        commit;
        -- Исходящие сообщения
        vi_cout_error   := 0;
        vi_cout_adult   := 0;
        vi_cout_correrr := 0;
        v_queuename     := IT_Q_MESSAGE.C_C_QUEUE_OUT_PREFIX || q.queue_num;
        -- Запускаем дважды для синхронов и асинхронов
        for n in 1 .. 2
        loop
          if n = 1
          then
            v_chk_delivery   := IT_Q_MESSAGE.C_C_MSG_DELIVERY_S;
            v_chk_expiration := gn_out_s_expiration;
          else
            v_chk_delivery   := IT_Q_MESSAGE.C_C_MSG_DELIVERY_A;
            v_chk_expiration := gn_out_a_expiration;
          end if;
          v_SQLselect := vc_cur_q_select || IT_Q_MESSAGE.C_C_QVIEW_OUT_PREFIX || q.queue_num || case
                           when q.queue_num = it_q_message.C_C_QUEUENUM_XX then
                            vc_cur_qXXout_where
                           else
                            vc_cur_qout_where
                         end;
          --      dbms_output.put_line(v_SQLselect);
          open cur_q for v_SQLselect
            using v_chk_delivery, v_chk_expiration;
          loop
            fetch cur_q
              into vqmsg
                  ,vqmsg_state
                  ,vqmsg_correlation
                  ,vchk
                  ,vmsg_enqdt;
            exit when cur_q%notfound;
            if vqmsg_state != 0
            then
              vc_erradd := 'Ошибка состояния сообщения STATE=' || vqmsg_state;
            elsif vqmsg_correlation = IT_Q_MESSAGE.GC_CORR_COMMAND
            then
              vc_erradd := 'Комманда QMANAGERу';
            else
              vc_erradd := 'Устаревшее исходящее сообщение';
            end if;
            begin
              it_q_message.msg_dequeue(p_qmsgid => vqmsg
                                      ,p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT
                                      ,p_queue_num => q.queue_num
                                      ,p_wait => 0
                                      ,p_errno => -1
                                      ,p_comment => vc_erradd
                                      ,o_qmsgid => v_qmsgid
                                      ,o_correlation => v_correlation
                                      ,o_message => vr_msg);
              vb_need_work := true;
            exception
              when others then
                vb_need_work := false;
            end;
            if vb_need_work
            then
              if vqmsg_state != 0
              then
                vi_cout_correrr := vi_cout_correrr + 1;
              elsif vqmsg_correlation = IT_Q_MESSAGE.GC_CORR_COMMAND
              then
                vi_cout_correrr := vi_cout_correrr + 1;
              else
                vi_cout_adult := vi_cout_adult + 1;
              end if;
              it_q_message.messlog_insert(p_message => vr_msg
                                         ,p_correlation => v_correlation
                                         ,p_queuename => v_queuename
                                         ,p_status => IT_Q_MESSAGE.C_STATUS_TRASH
                                         ,p_qmsgid => vqmsg
                                         ,p_enqdt => vmsg_enqdt
                                         ,p_workername => workername(q.queue_num, gn_worker_num)
                                         ,p_comment => vc_erradd);
              commit;
            else
              rollback;
            end if;
          end loop;
          close cur_q;
        end loop;
        if vi_cout_error > 0
        then
          send_information(IT_Q_MESSAGE.C_STATUS_TRASH || '_OUT', 'Ошибок вычитки из очереди ' || v_queuename || ' :' || vi_cout_error);
        end if;
        if vi_cout_adult + vi_cout_correrr > 0
        then
          send_information(IT_Q_MESSAGE.C_STATUS_TRASH || '_OUT'
                          ,v_queuename || ' Переведено в статус ' || IT_Q_MESSAGE.C_STATUS_TRASH || ' ' || (vi_cout_adult + vi_cout_correrr) || ' сообщ. из них устаревшиж ' ||
                           vi_cout_adult);
        end if;
        commit;
      end loop;
      -- Очистка лога
      -- TRASH
      delete itt_q_message_log l
       where l.enqdt < sysdate - gn_message_log_hist
         and l.status = it_q_message.C_STATUS_TRASH;
      commit;
      -- Процесс устарел
      for cur in (select /*+ index(l ITI_Q_MESSAGE_LOG_ENQDT)*/
                  distinct l.msgid
                    from itt_q_message_log L
                   where l.enqdt < sysdate - gn_message_log_hist
                     and l.corrmsgid is null)
      loop
        delete from itt_q_message_log
         where rowid in (select distinct rowid
                           from itt_q_message_log l
                         connect by nocycle corrmsgid = prior msgid
                                and queuetype = IT_Q_MESSAGE.C_C_QUEUE_TYPE_IN
                          start with l.msgid = cur.msgid
                                 and l.queuetype = IT_Q_MESSAGE.C_C_QUEUE_TYPE_IN);
        commit;
         delete from itt_q_message_log where rowid in (select distinct rowid from itt_q_message_log l connect by nocycle corrmsgid = prior msgid start with l.msgid = cur.msgid);
        commit;
      end loop;
      -- Потеряшки 
      delete from itt_q_message_log
       where rowid in (select /*+ index(l ITI_Q_MESSAGE_LOG_ENQDT)*/
                        rowid
                         from itt_q_message_log l
                        where l.enqdt < sysdate - gn_message_log_hist
                          and l.corrmsgid is not null
                          and not exists (select 1 from itt_q_message_log where msgid = l.corrmsgid));
      commit;
      delete itt_information i where i.senddt < sysdate - gn_message_log_hist;
      commit;
    end if;
    lock_release(vc_process);
    if cur_q%isopen
    then
      close cur_q;
    end if;
  exception
    when others then
      rollback;
      lock_release(vc_process);
      if cur_q%isopen
      then
        close cur_q;
      end if;
      send_information_error('DC' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  --- Процедура очистки очереди и лога
  procedure do_disassemblyX is
    pragma autonomous_transaction;
    deque_time_out exception;
    pragma exception_init(deque_time_out, -20228);
    deque_no_message exception;
    pragma exception_init(deque_no_message, -20263);
    v_qmsgid itt_queue_in_xx.qmsgid%type;
    --vb_need_work  boolean;
    --vr_msg        it_q_message_t;
    vc_erradd varchar2(1000);
    --vmsgid        itt_q_message_log.msgid%type;
    --v_correlation itt_q_message_log.correlation%type;
  begin
    -- Разбор зависших  заданий 
    for c in (select *
                from itt_queue_in_xx q
               where q.state = it_q_message.C_N_QUEUEXX_STATE_RUN
                 and q.delay <= systimestamp)
    loop
      begin
        select m.qmsgid into v_qmsgid from itt_queue_in_xx m where m.qmsgid = c.qmsgid for update nowait; -- Если выполняется должно быть заблокировано 
        if sql%rowcount = 0
        then
          continue;
        end if;
        delete from itt_queue_in_xx m where m.qmsgid = c.qmsgid;
      exception
        when others then
          continue;
      end;
      vc_erradd := 'Работник остановлен аварийно' || chr(10) || ' Очередь#' || it_q_message.C_C_QUEUENUM_XX || ' MsgID:' || c.qmsgid || ' Начало обработки сообщения:' ||
                   it_xml.timestamp_to_char_iso8601(c.statedt);
      it_q_message.messlog_upd_status(p_msgid => c.msgid
                                     ,p_delivery_type => c.delivery_type
                                     ,p_queuetype => IT_Q_MESSAGE.C_C_QUEUE_TYPE_IN
                                     ,p_status => IT_Q_MESSAGE.C_STATUS_ERROR
                                     ,p_comment => 'Ошибка ' || vc_erradd);
      commit;
    end loop;
    do_clear_log;
    commit;
  exception
    when others then
      rollback;
      send_information_error('XD' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  --- Процедура очистки очереди и лога
  procedure do_disassembly is
    pragma autonomous_transaction;
    deque_time_out exception;
    pragma exception_init(deque_time_out, -25228);
    deque_no_message exception;
    pragma exception_init(deque_no_message, -25263);
    vr_msg       it_q_message_t;
    vb_need_work boolean;
    --tmp               integer;
    vc_erradd     varchar2(1000);
    cur_q         sys_refcursor;
    vmsgid        itt_q_message_log.msgid%type;
    vmsg_delivery itt_q_message_log.delivery_type%type;
    v_correlation itt_q_message_log.correlation%type;
    v_qmsgid      raw(16);
  begin
    if gn_worker_num is not null
    then
      -- debug('WORKER' || p_worker_num || ' Q_DISASSEMBLY ');
      pipe_send_mess(mess_work(gn_worker_num, 'N'), С_RESULT_PIPE_PREFIX);
    end if;
    --debug('Processor' || p_worker_num || ': do_disassembly');
    -- Разбор управляющих команд в посточереди
    for c in (select * from itt_q_work_messages m where m.worker_num = 0 order by m.create_time)
    loop
      begin
        it_q_message.msg_dequeue(p_qmsgid => c.qmsgid
                                ,p_queuetype => it_q_message.C_C_QUEUE_TYPE_IN
                                ,p_queue_num => c.queue_num
                                ,p_wait => 0
                                ,p_errno => -1
                                ,o_qmsgid => v_qmsgid
                                ,o_correlation => v_correlation
                                ,o_message => vr_msg);
        vb_need_work := true;
      exception
        when deque_no_message then
          rollback;
        when others then
          rollback;
          continue;
      end;
      delete from itt_q_work_messages m
       where m.worker_num = c.worker_num
         and m.qmsgid = c.qmsgid;
      commit;
    end loop;
    -- Разбор зависших  заданий 
    for c in (select *
                from itt_q_work_messages m
               where m.work_ready is not null
                 and m.work_ready < sysdate - numtodsinterval(GN_QUEUE_WAIT, 'SECOND'))
    loop
      begin
        select m.msgid
              ,m.service_delivery
          into vmsgid
              ,vmsg_delivery
          from itt_q_work_messages m
         where m.worker_num = c.worker_num
           and m.qmsgid = c.qmsgid
           for update nowait; -- Если выполняется должно быть заблокировано 
        if sql%rowcount = 0
        then
          continue;
        end if;
        delete from itt_q_work_messages m
         where m.worker_num = c.worker_num
           and m.qmsgid = c.qmsgid;
      exception
        when others then
          continue;
      end;
      vb_need_work := true;
      vc_erradd    := nvl(c.work_errmess, 'Работник остановлен аварийно') || chr(10) || ' Очередь#' || c.queue_num || ' MsgID:' || c.qmsgid || ' WORKER#' ||
                      workername(c.queue_num, c.worker_num) || ' Начало обработки сообщения:' || it_xml.timestamp_to_char_iso8601(c.work_ready);
      begin
        it_q_message.msg_dequeue(p_qmsgid => c.qmsgid
                                ,p_queuetype => it_q_message.C_C_QUEUE_TYPE_IN
                                ,p_queue_num => c.queue_num
                                ,p_wait => 0
                                ,p_errno => -1
                                ,p_comment => vc_erradd
                                ,o_qmsgid => v_qmsgid
                                ,o_correlation => v_correlation
                                ,o_message => vr_msg);
      exception
        when deque_time_out
             or deque_no_message then
          vb_need_work := null;
        when others then
          vb_need_work := false;
          vc_erradd    := 'Ошибка вычитки сообщения (' || sqlerrm || ')' || vc_erradd;
      end;
      if vb_need_work
      then
        it_q_message.messlog_insert(p_message => vr_msg
                                   ,p_correlation => v_correlation
                                   ,p_queuename => IT_Q_MESSAGE.C_C_QUEUE_IN_PREFIX || c.queue_num
                                   ,p_status => IT_Q_MESSAGE.C_STATUS_TRASH
                                   ,p_qmsgid => c.qmsgid
                                   ,p_enqdt => c.enqdt
                                   ,p_workdt => systimestamp
                                   ,p_workername => workername(c.queue_num, gn_worker_num)
                                   ,p_comment => 'Ошибка ' || vc_erradd);
      elsif vb_need_work is null
      then
        it_q_message.messlog_upd_status(p_msgid => vmsgid
                                       ,p_delivery_type => vmsg_delivery
                                       ,p_queuetype => IT_Q_MESSAGE.C_C_QUEUE_TYPE_IN
                                       ,p_status => IT_Q_MESSAGE.C_STATUS_ERROR
                                       ,p_comment => 'Ошибка ' || vc_erradd);
      end if;
      if vb_need_work is null -- Если сообщение вычиталось или его нет
         or vb_need_work
      then
        commit;
      else
        rollback;
      end if;
    end loop;
    do_clear_log;
  exception
    when others then
      rollback;
      if cur_q%isopen
      then
        close cur_q;
      end if;
      send_information_error('DD' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  --==============================================================================
  -- Отправка команды работнику
  procedure work_run(p_enqtdt        timestamp
                    ,p_startdt       timestamp
                    ,p_message_type  itt_q_message_log.message_type%type
                    ,p_delivery_type itt_q_message_log.delivery_type%type
                    ,p_priority      itt_q_message_log.priority%type
                    ,p_queuename     itt_q_message_log.queuename%type
                    ,p_qmsgid        raw
                    ,p_msgid         itt_q_message_log.msgid%type
                    ,p_correlation   varchar2
                    ,p_servicename   itt_q_message_log.servicename%type
                    ,p_sender        itt_q_message_log.sender%type
                    ,p_senderuser    itt_q_message_log.senderuser%type
                    ,p_servicegroup  itt_q_message_log.servicegroup%type
                    ,p_txtmessbody   varchar) is
    --tmp            integer;
    vn_work_id     integer;
    vc_key         varchar2(200);
    v_queue_num    itt_q_message_log.queue_num%type := it_q_message.get_queue_num(p_objname => p_queuename);
    v_calc_stat_p  itt_q_service.calc_stat_c%type;
    v_max_running  itt_q_service.max_running%type;
    v_stop_apostq  itt_q_service.stop_apostq%type;
    v_systimestamp timestamp;
    v_run_coins    itt_q_worker.run_coins%type;
    v_run_count    itt_q_worker.run_count%type;
    vb_tmp         boolean;
    v_type_apostq  number := get_type_apostq(p_message_type => p_message_type, p_delivery_type => p_delivery_type, p_servicename => p_servicename, p_servicegroup => p_servicegroup);
    --v_ret2 integer;
  begin
    rollback; -- Начало. 
    if p_message_type = 'R' -- Команда QMANAGERу
       and p_servicename is null
       and p_correlation = IT_Q_MESSAGE.GC_CORR_COMMAND
    then
      insert into itt_q_work_messages w
        (queue_num
        ,qmsgid
        ,enqdt
        ,startdt
        ,worker_num)
      values
        (v_queue_num
        ,p_qmsgid
        ,p_enqtdt
        ,p_startdt
        ,0);
      commit;
      if p_txtmessbody in ('EXIT', 'RESTART')
      then
        if p_txtmessbody = 'EXIT'
        then
          sys.dbms_scheduler.disable(С_JOB_MANAGER_PREFIX || gс_manager_queue_num, true); --Отключить рестарт при ручной остановке
        else
          sys.dbms_scheduler.set_attribute(С_JOB_MANAGER_PREFIX || gс_manager_queue_num, 'start_date', sysdate + numtodsinterval(2, 'SECOND'));
          sys.dbms_scheduler.enable(С_JOB_MANAGER_PREFIX || gс_manager_queue_num);
        end if;
        send_information('COMMAND'
                        ,'Получена команда ' || to_char(p_txtmessbody) || ' для очереди ' || gс_manager_queue_num || ' от ' || p_senderuser || ' из ' || p_sender);
        raise_application_error(-20500, p_txtmessbody);
      end if;
      return;
    else
      vn_work_id := get_worker_free(p_message_type => p_message_type
                                   ,p_delivery_type => p_delivery_type
                                   ,p_priority => p_priority
                                   ,p_servicename => p_servicename
                                   ,p_servicegroup => p_servicegroup
                                   ,p_queue_num => v_queue_num
                                   ,p_enqtdt => p_enqtdt);
      if vn_work_id is null
      then
        rollback;
        raise_application_error(-20500, 'Нет доступных обработчиков');
      end if;
      vc_key        := get_gt_service_key(p_message_type, p_servicename);
      v_max_running := 5;
      v_stop_apostq := 0;
      if p_servicename is not null
         and gt_service.exists(vc_key)
      then
        v_calc_stat_p := gt_service(vc_key).calc_stat_p;
        v_max_running := greatest(5, gt_service(vc_key).max_running);
        v_stop_apostq := case
                           when gt_service(vc_key).stop_apostq = 0 then
                            0
                           else
                            1
                         end;
      elsif p_message_type = IT_Q_MESSAGE.C_C_MSG_TYPE_R
      then
        v_calc_stat_p := gn_service_error_price;
      else
        v_calc_stat_p := gn_service_done_price;
      end if;
      insert into itt_q_work_messages
        (queue_num
        ,qmsgid
        ,service_delivery
        ,servicename
        ,service_price
        ,servicegroup
        ,enqdt
        ,startdt
        ,worker_num
        ,msgid)
      values
        (v_queue_num
        ,p_qmsgid
        ,p_delivery_type
        ,vc_key
        ,v_calc_stat_p
        ,p_servicegroup
        ,p_enqtdt
        ,p_startdt
        ,vn_work_id
        ,p_msgid);
      v_systimestamp := systimestamp;
      update itt_q_worker w
         set worker_free      = 0
            ,run_coins = case
                           when worker_free = 0 then
                            greatest(0, run_coins - it_xml.calc_interval_millisec(nvl(run_lasttime, v_systimestamp), v_systimestamp))
                           else
                            0
                         end + v_calc_stat_p + gn_service_startprice
            ,run_count        = run_count + 1
            ,service_delivery = p_delivery_type
            ,servicename      = vc_key
            ,stop_apostq      = v_stop_apostq
            ,servicegroup = case
                              when v_type_apostq < 0 then
                               p_servicegroup
                              else
                               servicegroup
                            end
            ,queue_num        = v_queue_num
            ,run_starttime = case
                               when worker_free = 0 then
                                run_starttime
                               else
                                v_systimestamp
                             end
            ,run_lasttime     = v_systimestamp
            ,run_stoptime = case
                              when worker_free = 0 then
                               run_stoptime
                              else
                               v_systimestamp
                            end + numtodsinterval(v_max_running, 'SECOND')
       where w.worker_num = vn_work_id
      returning run_coins, run_count into v_run_coins, v_run_count;
      commit;
      pipe_send_mess('W', С_WORKER_PIPE_PREFIX, vn_work_id);
      if (v_type_apostq = 1 and v_run_count > 1)
      -- or (v_type_apostq < 0 and v_run_count > 1 and v_run_coins > gn_norm_run_coins_apostq - (v_calc_stat_p + gn_service_startprice))
      then
        --debug('Manager' || gс_manager_queue_num || ' work_run v_run_coins - v_calc_stat_p =' || (v_run_coins - v_calc_stat_p) || ' v_run_coins=' ||
        --      v_run_coins || ' v_calc_stat_p=' || v_calc_stat_p);
        vb_tmp := workers_flush;
        vb_tmp := worker_plus;
      end if;
    end if;
  end;

  --- Передача команды обработчику для очистки очереди
  procedure q_disassembly is
    pragma autonomous_transaction;
    vn_work_id    integer;
    vn_count_work integer;
    --tmp           integer;
    cc_caption constant itt_q_worker.servicename%type := '#Очистка очереди (DISASSEMBLY)';
    vc_process varchar2(100) := 'it_q_manager.q_disassembly';
  begin
    if gd_disassembly is null
       or gd_disassembly + numtodsinterval(gn_period_disassembly, 'SECOND') < sysdate
    then
      vn_count_work := get_count_worker_free(IT_Q_MESSAGE.C_C_MSG_PRIORITY_N);
      --debug('Manager' || gс_manager_queue_num || ' q_disassembly vn_count_work = '||vn_count_work);
      if (vn_count_work >= gn_worker_min_count)
      then
        gd_disassembly := sysdate;
        if lock_request(vc_process)
        then
          select count(*)
            into vn_count_work
            from itt_q_worker w
           where w.servicename = cc_caption
             and w.worker_free = 0;
          --debug('Manager' || gс_manager_queue_num || ' q_disassembly vn_count_work = '||vn_count_work);
          if vn_count_work = 0
          then
            vn_work_id := get_worker_free(p_priority => IT_Q_MESSAGE.C_C_MSG_PRIORITY_N, p_nowait => true);
            --debug('Manager' || gс_manager_queue_num || ' q_disassembly vn_work_id = '||vn_work_id);
            if vn_work_id is not null
            then
              update itt_q_worker w
                 set w.worker_enabled = 0
                    ,worker_free      = 0
                    ,run_coins        = 999999
                    ,run_count        = 1
                    ,service_delivery = IT_Q_MESSAGE.C_C_MSG_PRIORITY_N
                    ,stop_apostq      = 1
                    ,servicename      = cc_caption
                    ,servicegroup     = null
                    ,queue_num        = null
                    ,run_starttime    = null
                     --,run_lasttime     = null
                    ,run_stoptime = null
               where w.worker_num = vn_work_id;
              commit; -- AUTONOMOUS
              --debug('Manager' || gс_manager_queue_num || ' Q_DISASSEMBLY ');
              pipe_send_mess('Q_DISASSEMBLY', С_WORKER_PIPE_PREFIX, vn_work_id);
            end if;
          end if;
        end if;
      end if;
    end if;
    commit;
    lock_release(vc_process);
  exception
    when others then
      rollback; -- AUTONOMOUS
      lock_release(vc_process);
      send_information_error('QD' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  --- Очистка очереди от управляющих коммвнд и переход в ожидание задания
  procedure q_wait(p_starting   boolean default false
                  ,p_queue_wait number default GN_QUEUE_WAIT) is
    pragma autonomous_transaction;
    numeric_overflow exception;
    pragma exception_init(numeric_overflow, -01426);
    listen_time_out exception;
    pragma exception_init(listen_time_out, -25254);
    deque_time_out exception;
    pragma exception_init(deque_time_out, -25228);
    v_queue_wait constant number := nvl(p_queue_wait, GN_QUEUE_WAIT);
    vr_msg        it_q_message_t;
    vb_tmp        boolean;
    v_correlation itt_q_message_log.correlation%type;
    v_qmsgid      raw(16);
  begin
    if p_starting
    then
      -- Очистка очереди от управляющих сообщений на случай, если напихали много EXIT, RESTART ...
      begin
        loop
          it_q_message.msg_dequeue(p_correlation => IT_Q_MESSAGE.GC_CORR_COMMAND
                                  ,p_queuetype => IT_Q_MESSAGE.C_C_QUEUE_TYPE_IN
                                  ,p_queue_num => gс_manager_queue_num
                                  ,p_wait => 0
                                  ,o_qmsgid => v_qmsgid
                                  ,o_correlation => v_correlation
                                  ,o_message => vr_msg);
          commit;
        end loop;
      exception
        when deque_time_out then
          commit;
      end;
      -- Очистка заданий от управляющих сообщений  ...
      delete itt_q_work_messages m
       where m.worker_num = 0
         and m.queue_num = gс_manager_queue_num;
      commit;
    end if;
    vb_tmp := workers_flush;
    -- Переходим в ожидание  ...
    --debug('Manager' || gс_qmanager_queue_num || ': wait ');
    declare
      vt_begin timestamp := systimestamp;
    begin
      it_q_message.msg_dequeue(p_correlation => С_CORRID_QR_PREFIX || '%'
                              ,p_queuetype => IT_Q_MESSAGE.C_C_QUEUE_TYPE_IN
                              ,p_queue_num => gс_manager_queue_num
                              ,p_wait => v_queue_wait
                              ,p_toState => 0
                              ,o_qmsgid => v_qmsgid
                              ,o_correlation => v_correlation
                              ,o_message => vr_msg);
      /*if it_xml.calc_interval_millisec(vt_begin, systimestamp) < 50
         and it_q_message.get_count_task(p_queue_num => gс_manager_queue_num, p_max_count => 0) = 0
      then -- сработало по сообщению в посточереди
        dbms_lock.sleep(0.1);
      end if;*/
    exception
      when deque_time_out then
        --debug('Manager' || gс_manager_queue_num || ': no task ');
        vb_tmp := workers_flush;
    end;
    rollback;
  end;

  --==============================================================================
  -- Основной процесс менеджера pipe канала
  procedure pipemanagermain(p_queue_num itt_q_message_log.queue_num%type) as
    vb_tmp boolean;
  begin
    if p_queue_num is null
       or p_queue_num != upper(p_queue_num)
       or it_q_message.check_queue_num(p_queue_num) != 1
    then
      raise_application_error(-20000, 'Очередь ID ' || p_queue_num || ' не инсталлирована в системе');
    end if;
    gс_manager_queue_num := p_queue_num;
    gn_is_pmanager        := 1;
    set_session_module;
    --
    --debug('PIPEmanager' || p_queue_num || ' START');
    init_debug;
    init_qsettings;
    flush_services(false);
    loop
      rollback;
      --debug('PIPEmanager' || p_queue_num || ' workers_flush ');
      set_session_action('Run');
      vb_tmp := workers_flush(p_wait_lock => GN_QUEUE_WAIT, p_PipeMNG => true);
      --debug('PIPEmanager' || p_queue_num || ' workers_flush EXIT');
      set_session_action;
      init_qsettings;
      flush_services(true);
      workers_refresh(true);
      task_transfer;
      exit when pipemanager_quit;
    end loop;
    rollback;
    --debug('PIPEmanager' || p_queue_num || ' STOP');
  exception
    when others then
      rollback;
      send_information_error('PM' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  --==============================================================================
  -- Обработчик сообщений табличной очереди
  procedure WorkerXMain(p_worker_num integer) is
    v_dtstart                 timestamp;
    v_waitsec                 number;
    vd_tmp                    date;
    vb_tmp                    boolean;
    vn_tmp                    number;
    vn_result                 number;
    vc_err                    varchar2(512);
    vb_iswork                 boolean := false;
    vcl_result                clob;
    vxml_result               xmltype;
    vc_msgid_result           varchar2(256);
    vr_msg                    it_q_message_t;
    vn_worklogid              integer;
    vd_workstartdt            timestamp;
    vn_start_result           number;
    v_srv_price               number;
    vb_state_exit             boolean;
    vc_process_disassemblyX   varchar2(100) := 'it_q_manager.do_disassemblyX';
    vc_sid_disassemblyX       varchar2(100) := '$$QUEUEXX_DISASSEMBLY';
    v_store_date_disassemblyX date;
    --v_count_workerX_free      pls_integer;
    vc_r_proc varchar2(150);
    --
    function set_workerx_param(p_response      char default null
                              ,p_job_stoptime  date default null
                              ,p_message_type  itt_q_workerx.message_type%type default null
                              ,p_servicename   itt_q_workerx.servicename%type default null
                              ,p_servicegroup  itt_q_workerx.servicegroup%type default null
                              ,p_run_starttime timestamp default null) return boolean as
    begin
      select w.worker_num into vn_tmp from itt_q_workerx w where worker_num = gn_worker_num for update nowait;
      update itt_q_workerx w
         set response_last     = nvl(p_response, response_last)
            ,response_lasttime = nvl2(p_response, systimestamp, response_lasttime)
            ,job_stoptime      = nvl2(p_job_stoptime, p_job_stoptime, job_stoptime)
            ,run_starttime     = p_run_starttime
            ,message_type      = p_message_type
            ,servicename       = p_servicename
            ,servicegroup      = p_servicegroup
       where worker_num = gn_worker_num;
      return true;
    exception
      when others then
        return false;
    end;
  
    function cnk_WorkerX_EXIT return boolean as
    begin
      if gd_last_worker_restart is not null
      then
        return true;
      end if;
      if XGetCommand('EXIT')
      then
        return true;
      end if;
      select x.job_stoptime into vd_tmp from itt_q_workerx x where x.worker_num = gn_worker_num;
      if vd_tmp <= sysdate
      then
        return true;
      elsif vd_tmp is not null
      then
        return null;
      end if;
      return false;
    end;
  
  begin
    --debug('START W=' || p_worker_num);
    if it_q_message.check_queue_num(it_q_message.C_C_QUEUENUM_XX) != 1
    then
      raise_application_error(-20000, 'Табличная очередь ' || it_q_message.C_C_QUEUENUM_XX || ' не инсталлирована в системе');
    end if;
    gn_worker_num := p_worker_num;
    set_session_module(p_queueXX => 1);
    init_qsettings;
    flush_services(false);
    init_debug();
    loop
      set_session_action;
      vb_state_exit := cnk_WorkerX_EXIT;
      exit when vb_state_exit;
      --
      if not set_workerx_param(p_response => C_WORKER_RESPONSE_FREE)
      then
        exit;
      end if;
      commit;
      v_dtstart := systimestamp;
      vb_iswork := false;
      if not vb_state_exit
      then
        if not set_workerx_param(p_response => C_WORKER_RESPONSE_RUN)
        then
          exit;
        end if;
        for work_msg in (select qmsgid
                               ,correlation
                           from (select tsk.qmsgid
                                       ,tsk.delivery_type
                                       ,tsk.priority
                                       ,tsk.servicegroup
                                       ,min(tsk.enqdt) over(partition by tsk.servicegroup) as sg_enqdt
                                       ,tsk.enqdt
                                       ,srv.service_id
                                       ,tsk.correlation
                                   from itv_q_taskxx tsk
                                   left join itt_q_workerx wx
                                     on wx.servicegroup = tsk.servicegroup
                                   left join itt_q_service srv
                                     on srv.message_type = tsk.message_type
                                    and UPPER(trim(srv.servicename)) = UPPER(trim(tsk.servicename))
                                  where wx.worker_num is null)
                          where enqdt = nvl2(servicegroup, sg_enqdt, enqdt)
                          order by nvl2(service_id, 0, 1)
                                  ,case
                                     when delivery_type = it_q_message.C_C_MSG_DELIVERY_S
                                          and priority = it_q_message.C_C_MSG_PRIORITY_F then
                                      0
                                     else
                                      1
                                   end
                                  ,enqdt)
        loop
          exit when work_msg.correlation = IT_Q_MESSAGE.GC_CORR_COMMAND;
          vd_workstartdt := systimestamp;
          msg_task_dequeue(p_qmsgid => work_msg.qmsgid
                          ,p_queue_num => it_q_message.C_C_QUEUENUM_XX
                          ,p_startdt => v_dtstart
                          ,p_commanddt => systimestamp
                          ,p_workdt => vd_workstartdt
                          ,p_workername => workername(it_q_message.C_C_QUEUENUM_XX, gn_worker_num)
                          ,o_errno => vn_result
                          ,o_errmsg => vc_err
                          ,o_msgid => vn_worklogid
                          ,o_message => vr_msg);
          exit when vb_iswork and vn_result is null;
          continue when vn_result is null;
          -------
          if vn_result = 4061
          then
            gd_last_worker_restart := sysdate;
            rollback;
            exit;
          end if;
          --
          vb_iswork := true;
          if not set_workerx_param(p_response => C_WORKER_RESPONSE_RUN
                                  ,p_message_type => vr_msg.message_type
                                  ,p_servicename => vr_msg.servicename
                                  ,p_servicegroup => vr_msg.ServiceGroup
                                  ,p_run_starttime => systimestamp)
          then
            rollback;
            exit;
          end if;
          ------
          commit;
          delete itt_queue_in_xx q where q.qmsgid = work_msg.qmsgid;
          vb_tmp          := set_workerx_param(p_response => C_WORKER_RESPONSE_RUN);
          vn_start_result := vn_result;
          msg_task_execute(p_worklogid => vn_worklogid
                          ,p_msg => vr_msg
                          ,p_workstartdt => vd_workstartdt
                          ,io_result => vn_result
                          ,io_err => vc_err
                          ,ocl_result => vcl_result
                          ,o_msgid_result => vc_msgid_result
                          ,oxml_result => vxml_result
                          ,o_srv_price => v_srv_price);
          -------
          vb_tmp := set_workerx_param(p_response => C_WORKER_RESPONSE_RUN);
          commit;
          vb_tmp := set_workerx_param(p_response => C_WORKER_RESPONSE_RUN);
          ---------
          msg_task_send_answ(p_start_result => vn_start_result
                            ,pn_result => vn_result
                            ,pc_err => vc_err
                            ,p_msgid_result => vc_msgid_result
                            ,pcl_result => vcl_result
                            ,pxml_result => vxml_result
                            ,p_msg => vr_msg);
          -------
          vb_tmp := set_workerx_param(p_response => C_WORKER_RESPONSE_RUN);
          commit;
          ---------
          if vn_result = 0
          then
            vc_r_proc := get_gt_service_key(vr_msg.message_type, vr_msg.servicename);
            if gt_service.exists(vc_r_proc)
            then
              gt_service(vc_r_proc).calc_stat_p := ((gt_service(vc_r_proc).calc_stat_p * gt_service(vc_r_proc).calc_stat_c) + (v_srv_price)) /
                                                   (gt_service(vc_r_proc).calc_stat_c + 1);
              gt_service(vc_r_proc).calc_stat_c := gt_service(vc_r_proc).calc_stat_c + 1;
            end if;
          end if;
          exit when cnk_WorkerX_EXIT;
        end loop;
      end if;
      --
      exit when cnk_WorkerX_EXIT;
      if not vb_iswork
      then
        if lock_request(vc_process_disassemblyX)
        then
          v_store_date_disassemblyX := it_q_message.get_qset_data(p_qset_name => vc_sid_disassemblyX);
          if v_store_date_disassemblyX is null
             or v_store_date_disassemblyX < sysdate - numtodsinterval(gn_period_disassembly, 'SECOND')
          then
            vb_iswork := true;
            vb_tmp    := set_workerx_param(p_response => C_WORKER_RESPONSE_RUN);
            set_session_action('#DISASSEMBLYXX');
            do_disassemblyX;
            it_q_message.set_qset_data(vc_sid_disassemblyX, sysdate);
          end if;
        end if;
        lock_release(vc_process_disassemblyX);
      end if;
      exit when cnk_WorkerX_EXIT;
      if not vb_iswork
      then
        flush_services(true);
        v_waitsec := greatest(0, gn_queueXX_QWorker_sleep * (1 + get_count_workerX_free * 0.33) - it_xml.calc_interval_millisec(v_dtstart, systimestamp) / 1000);
        if set_workerx_param(p_response => C_WORKER_RESPONSE_FREE)
           and v_waitsec > 0
        then
          commit;
          set_session_action('Wait ...');
          dbms_lock.sleep(v_waitsec);
        end if;
      end if;
    end loop;
    vb_tmp := set_workerx_param(p_job_stoptime => sysdate);
    flush_services(true);
    commit;
  exception
    when others then
      rollback;
      lock_release(С_LOCK_PROCESS_XWORKER_START);
      send_information_error('XW' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  -- Обработчик сообщений
  procedure WorkerMain(p_worker_num integer) is
    numeric_overflow exception;
    pragma exception_init(numeric_overflow, -01426);
    listen_time_out exception;
    pragma exception_init(listen_time_out, -25254);
    tmp             integer;
    vn_result       number;
    vc_msg          varchar2(256);
    vc_err          varchar2(512);
    vb_iswork       boolean := false;
    vcl_result      clob;
    vxml_result     xmltype;
    vc_msgid_result varchar2(256);
    vr_msg          it_q_message_t;
    vb_need_work    boolean;
    vc_need_messerr itt_q_work_messages.work_errmess%type;
    vn_sqlcode      integer;
    v_c_key_service varchar2(2000);
    vn_worklogid    integer;
    vd_workstartdt  timestamp;
    --vn_price_start  integer;
    vn_start_result number;
    v_srv_price     number;
  begin
    if p_worker_num is null
       or p_worker_num < 1
    then
      raise_application_error(-20500, 'Ошибка параметра p_worker_num при старте РАБОТНИКА');
    end if;
    begin
      select w.pipe_channel into gс_result_pipe_channel from itt_q_worker w where w.worker_num = p_worker_num;
    exception
      when no_data_found then
        raise_application_error(-20500, 'Ошибка параметра p_worker_num при старте РАБОТНИКА');
    end;
    if gс_result_pipe_channel is null
       or gс_result_pipe_channel != upper(gс_result_pipe_channel)
       or it_q_message.check_queue_num(gс_result_pipe_channel) != 1
    then
      raise_application_error(-20000, 'Очередь ID ' || gс_result_pipe_channel || ' не инсталлирована в системе');
    end if;
    gn_worker_num := p_worker_num;
    set_session_module;
    init_debug;
    init_qsettings;
    flush_services(false);
    --debug('Processor' || p_worker_num || ': started');
    sys.dbms_pipe.purge(get_full_pipe_name(С_WORKER_PIPE_PREFIX, p_worker_num));
    pipe_send_mess(mess_work(p_channel => p_worker_num, p_command => 'S'), С_RESULT_PIPE_PREFIX);
    ----
    update itt_q_worker w
       set w.job_starttime = sysdate
          ,w.job_stoptime  = null
     where w.worker_num = p_worker_num;
    commit;
    <<outer_loop>>
    loop
      --debug('Processor' || p_worker_num || ': new select');
      vb_iswork      := false;
      vd_workstartdt := systimestamp; --  Единая метка 
      for work_msg in (select *
                         from itt_q_work_messages m
                        where m.worker_num = p_worker_num
                          and m.work_ready is null
                        order by m.create_time)
      loop
        --debug('Processor' || p_worker_num || ': new message');
        vd_workstartdt := systimestamp; --  Начало обработки
        if not vb_iswork
        then
          vb_iswork := true;
          set_session_action;
          pipe_send_mess(mess_work(p_channel => p_worker_num, p_command => 'N'), С_RESULT_PIPE_PREFIX);
          --debug('Processor' || p_worker_num || ': PIPE N ');
        end if;
        vn_sqlcode      := 0;
        vc_need_messerr := null;
        vb_need_work    := true;
        vn_worklogid    := null;
        begin
          -- Блокируем задание
          select m.worker_num
            into tmp
            from itt_q_work_messages m
           where m.qmsgid = work_msg.qmsgid
             and m.worker_num = p_worker_num
             and m.work_ready is null
             for update nowait;
        exception
          when others then
            -- Идем дальше
            continue;
        end;
        --debug('Processor' || p_worker_num || ': LOCK ');
        update itt_q_work_messages m
           set m.work_ready = vd_workstartdt
         where m.qmsgid = work_msg.qmsgid
           and m.worker_num = p_worker_num
           and m.work_ready is null;
        --debug('Processor' || p_worker_num || ': UPDATE ');
        vb_need_work := sql%rowcount > 0;
        if vb_need_work
        then
          --!!!!!!!!!!!!!!
          msg_task_dequeue(p_qmsgid => work_msg.qmsgid
                          ,p_queue_num => work_msg.queue_num
                          ,p_startdt => work_msg.startdt
                          ,p_commanddt => work_msg.create_time
                          ,p_workdt => vd_workstartdt
                          ,p_workername => workername(work_msg.queue_num, p_worker_num)
                          ,o_errno => vn_result
                          ,o_errmsg => vc_err
                          ,o_msgid => vn_worklogid
                          ,o_message => vr_msg);
          if vn_result is null
          then
            update itt_q_work_messages m
               set m.work_errno   = 1
                  ,m.work_errmess = vc_err
             where m.qmsgid = work_msg.qmsgid
               and m.worker_num = p_worker_num;
            commit;
            continue;
          end if;
          if vn_result = 4061
          then
            pipe_send_mess(mess_work(p_channel => p_worker_num, p_command => 'R'), С_RESULT_PIPE_PREFIX);
            gd_last_worker_restart := sysdate;
            rollback;
            exit;
          end if;
          -----
          commit;
          -------
          --debug('Processor' || p_worker_num || ': START WORK message');
          vcl_result      := null;
          vxml_result     := null;
          vc_msgid_result := null;
          delete from itt_q_work_messages m
           where m.qmsgid = work_msg.qmsgid
             and m.worker_num = p_worker_num; -- Блокировка 
          --
          vn_start_result := vn_result;
          msg_task_execute(p_worklogid => vn_worklogid
                          ,p_msg => vr_msg
                          ,p_workstartdt => vd_workstartdt
                          ,io_result => vn_result
                          ,io_err => vc_err
                          ,ocl_result => vcl_result
                          ,o_msgid_result => vc_msgid_result
                          ,oxml_result => vxml_result
                          ,o_srv_price => v_srv_price);
          -------
          commit;
          ---------
          msg_task_send_answ(p_start_result => vn_start_result
                            ,pn_result => vn_result
                            ,pc_err => vc_err
                            ,p_msgid_result => vc_msgid_result
                            ,pcl_result => vcl_result
                            ,pxml_result => vxml_result
                            ,p_msg => vr_msg);
          --debug('Processor' || p_worker_num || ': DONE message');
          -------
          commit;
          ---------
          --debug('Processor' || p_worker_num || ': SEND message');
          pipe_send_mess(mess_work(p_channel => p_worker_num
                                  ,p_command => case
                                                  when vn_result = 0 then
                                                   'W'
                                                  else
                                                   'E'
                                                end
                                  ,p_param => v_c_key_service || '#' || round(v_srv_price) || '#' || work_msg.service_price)
                        ,С_RESULT_PIPE_PREFIX);
          --
          --debug('Processor' || p_worker_num || ': PUSH PIPE');
        end if;
        --debug('Processor' || p_worker_num || ': End ');
        exit outer_loop when gd_last_worker_restart is not null;
      end loop;
      if vb_iswork
      then
        --Очищаем PIPE перед заходом на цикл
        sys.dbms_pipe.purge(get_full_pipe_name(С_WORKER_PIPE_PREFIX, p_worker_num));
      else
        begin
          select 1 into tmp from itt_q_worker w where w.worker_num = p_worker_num for update nowait;
          update_worker_free(p_worker_num => p_worker_num, p_response_last => 'F', p_response_lasttime => vd_workstartdt);
          commit;
          --debug('Processor' || p_worker_num || ': FREE ');
          pipe_send_mess(mess_work(p_channel => p_worker_num, p_command => 'F', p_systimestamp => vd_workstartdt), С_RESULT_PIPE_PREFIX);
        exception
          when others then
            pipe_send_mess(mess_work(p_channel => p_worker_num, p_command => 'F'), С_RESULT_PIPE_PREFIX);
        end;
        --debug('Processor' || p_worker_num || ': WAIT ');
        set_session_action('Wait ...');
        loop
          tmp := sys.dbms_pipe.receive_message(get_full_pipe_name(С_WORKER_PIPE_PREFIX, p_worker_num), GN_QUEUE_WAIT);
          if tmp != 0
          then
            refresh_spr;
            exit;
          end if;
          begin
            sys.dbms_pipe.unpack_message(vc_msg);
          exception
            when others then
              exit;
          end;
          case vc_msg
            when 'EXIT' then
              exit outer_loop;
            when 'Q_DISASSEMBLY' then
              if not vb_iswork
              then
                set_session_action('#DISASSEMBLY');
                do_disassembly;
                set_session_action;
              end if;
              vb_iswork := true;
            else
              exit;
          end case;
        end loop;
        if tmp != 0
        then
          -- Запущен ли менеджер обработчиков?
          select nvl(max(1), 0)
            into tmp
            from user_scheduler_jobs j
           where j.job_name like С_JOB_MANAGER_PREFIX || '%'
             and j.state = 'RUNNING';
          --debug('Processor' || p_worker_num || '  Запущен ли менеджер обработчиков? -' || tmp);
          if tmp = 0
          -- and false -- !!!!!!!!!!!!!!!!!!!!!!
          then
            sys.dbms_scheduler.disable(С_JOB_WORKER_PREFIX || p_worker_num, true);
            exit;
          end if;
          flush_services(false);
        end if;
      end if;
    end loop outer_loop;
    flush_services(true);
    update itt_q_worker w set w.worker_enabled = 0 where w.worker_num = p_worker_num;
    commit;
    --debug('Processor' || p_worker_num || '  EXIT' );
  exception
    when others then
      rollback;
      send_information_error('QW' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
      update itt_q_worker w set w.worker_enabled = 0 where w.worker_num = p_worker_num;
      commit;
  end;

  --==============================================================================
  function get_count_workerx_job_stop return number as
    v_ret number;
  begin
    select count(*)
      into v_ret
      from itt_q_workerx
     where job_stoptime > sysdate
       and response_lasttime >= systimestamp - numtodsinterval(gn_queueXX_QWorker_sleep * 2, 'SECOND');
    return v_ret;
  end;

  procedure workerX_plus as
    pragma autonomous_transaction;
    v_worker_num number;
    v_job_name   varchar2(60);
    vc_jenabled  user_scheduler_jobs.ENABLED%type;
    vc_jstate    user_scheduler_jobs.STATE%type;
  begin
    if get_count_workerx_job_stop > 0
    then
      begin
        select w.worker_num
          into v_worker_num
          from itt_q_workerx w
         where job_stoptime > sysdate
           and response_lasttime >= systimestamp - numtodsinterval(gn_queueXX_QWorker_sleep * 2, 'SECOND')
           for update nowait;
      exception
        when others then
          v_worker_num := null;
      end;
      if v_worker_num is not null
      then
        update itt_q_workerx w set w.job_stoptime = null where w.worker_num = v_worker_num;
      end if;
    end if;
    if v_worker_num is null
    then
      for worker in (select worker_num
                       from itt_q_workerx
                      where response_lasttime < systimestamp - numtodsinterval(gn_queueXX_QWorker_sleep * 2, 'SECOND')
                      order by worker_num
                        for update skip locked)
      loop
        v_worker_num := worker.worker_num;
        exit;
      end loop;
      if v_worker_num is null
      then
        insert into itt_q_workerx (worker_num) values ((select nvl(max(worker_num), 0) from itt_q_workerx) + 1) returning worker_num into v_worker_num;
      else
        update itt_q_workerx w
           set job_starttime     = sysdate
              ,job_stoptime      = null
              ,message_type      = null
              ,servicename       = null
              ,servicegroup      = null
              ,run_starttime     = null
              ,run_stoptime      = null
              ,response_last     = C_WORKER_RESPONSE_START
              ,response_lasttime = systimestamp
         where worker_num = v_worker_num;
      end if;
    end if;
    v_job_name := С_JOB_XWORKER_PREFIX || v_worker_num;
    begin
      sys.dbms_scheduler.set_job_argument_value(job_name => v_job_name, argument_position => 1, argument_value => to_char(v_worker_num));
      sys.dbms_scheduler.set_attribute(name => v_job_name, attribute => 'start_date', value => systimestamp);
      sys.dbms_scheduler.enable(v_job_name);
    exception
      when others then
        begin
          select enabled -- 'TRUE'
                ,state -- 'RUNNING'
            into vc_jenabled
                ,vc_jstate
            from sys.user_scheduler_jobs
           where job_name = v_job_name;
        exception
          when no_data_found then
            null;
        end;
        if vc_jenabled is null
        then
          sys.dbms_scheduler.create_job(job_name => v_job_name
                                       ,job_type => 'STORED_PROCEDURE'
                                       ,job_action => 'it_q_manager.WorkerXMain'
                                       ,number_of_arguments => 1
                                       ,start_date => sysdate
                                       ,auto_drop => true
                                       ,comments => 'Обработчик №' || v_worker_num || ' табличной очереди ');
        end if;
        sys.dbms_scheduler.set_job_argument_value(job_name => v_job_name, argument_position => 1, argument_value => to_char(v_worker_num));
        sys.dbms_scheduler.set_attribute(name => v_job_name, attribute => 'start_date', value => systimestamp);
        sys.dbms_scheduler.enable(v_job_name);
    end;
    commit;
  end;

  procedure workerX_chk as
    pragma autonomous_transaction;
    v_count_workerX_free pls_integer;
    v_worker_num         itt_q_workerx.worker_num%type;
    --vn_tmp               number;
    v_count_workerX      pls_integer;
    v_count_task_message integer;
    v_massadd            boolean := false;
  begin
    if lock_request(С_LOCK_PROCESS_XWORKER_START)
    then
      for wrk in (select w.worker_num
                    from itt_q_workerx w
                   where w.job_stoptime is null
                     and response_lasttime < systimestamp - numtodsinterval(gn_queueXX_QWorker_sleep * 2, 'SECOND')
                     for update skip locked)
      loop
        update itt_q_workerx set job_stoptime = sysdate where worker_num = wrk.worker_num;
      end loop;
      commit;
      v_count_workerX := get_count_workerX;
      if v_count_workerX = 0
      then
        workerX_plus;
        v_count_workerX := 1;
      end if;
      v_count_workerX_free := get_count_workerX_free;
      v_count_task_message := get_count_task_message(it_q_message.C_C_QUEUENUM_XX, 1);
      --debug('get_count_worker=' || get_count_worker || ' v_count_workerX=' || v_count_workerX || ' v_count_workerX_free =' || v_count_workerX_free || ' v_count_task_message=' ||
      --   v_count_task_message);
      if v_count_workerX_free < v_count_task_message
         and (gn_worker_max_count - get_count_worker - v_count_workerX) > 0
      then
        --debug('ADD ' || least((v_count_task_message - v_count_workerX_free), (gn_worker_max_count - get_count_worker - v_count_workerX)));
        v_massadd := true;
        for n in 1 .. least(gn_queueXX_QWorker_massstart, (v_count_task_message - v_count_workerX_free), (gn_worker_max_count - get_count_worker - v_count_workerX))
        loop
          workerX_plus;
          v_count_workerX := v_count_workerX + 1;
        end loop;
      end if;
      --debug('v_count_workerX=' || v_count_workerX || ' v_count_workerX_free =' || v_count_workerX_free || ' gn_worker_plus_force=' || gn_worker_plus_force);
      if v_count_workerX_free > gn_worker_plus_force
         and not v_massadd
      then
        --debug('JOB_MINUS v_count_workerX_free =' || v_count_workerX_free || ' gn_worker_plus_force=' || gn_worker_plus_force);
        if get_count_workerx_job_stop = 0
        then
          begin
            select wr.worker_num
              into v_worker_num
              from itt_q_workerx wr
             where wr.worker_num = (select max(w.worker_num)
                                      from itt_q_workerx w
                                     where w.job_stoptime is null
                                       and response_lasttime >= systimestamp - numtodsinterval(gn_queueXX_QWorker_sleep * 2, 'SECOND')
                                       and w.response_last != C_WORKER_RESPONSE_RUN)
               for update nowait;
            update itt_q_workerx w set w.job_stoptime = sysdate + numtodsinterval(gn_worker_minus_interval, 'SECOND') where w.worker_num = v_worker_num;
          exception
            when others then
              lock_release(С_LOCK_PROCESS_XWORKER_START);
              rollback;
              return;
          end;
          commit;
          --debug('JOB_MINUS v_worker_num =' || v_worker_num);
        end if;
      elsif v_count_workerX_free < gn_worker_plus_force
            and (gn_worker_max_count - get_count_worker - v_count_workerX) > 0
      then
        --debug('get_count_worker=' || get_count_worker || ' v_count_workerX=' || v_count_workerX || ' v_count_workerX_free =' || v_count_workerX_free || ' v_count_task_message=' ||
        --   v_count_task_message);
        --debug('ADD ');
        workerX_plus;
      end if;
    end if;
    lock_release(С_LOCK_PROCESS_XWORKER_START);
    commit;
  end;

  -- Основной процесс менеджера табличной очереди
  procedure ManagerXMain is
    v_dtstart timestamp;
    v_waitsec number;
  begin
    if it_q_message.check_queue_num(it_q_message.C_C_QUEUENUM_XX) != 1
    then
      raise_application_error(-20000, 'Табличная очередь ' || it_q_message.C_C_QUEUENUM_XX || ' не инсталлирована в системе');
    end if;
    gс_manager_queue_num := it_q_message.C_C_QUEUENUM_XX;
    set_session_module;
    if not lock_request(С_LOCK_PROCESS_XQMANAGER_START, 5)
    then
      lock_release(С_LOCK_PROCESS_XQMANAGER_START);
      raise_application_error(-20000, 'Невозможно захватить блокировку для старта менеджера');
    end if;
    --
    flush_services(false);
    init_debug();
    init_qsettings;
    loop
      set_session_action;
      v_dtstart := systimestamp;
      exit when XGetCommand('EXIT');
      workerX_chk;
      v_waitsec := greatest(0, gn_queueXX_QManager_sleep - it_xml.calc_interval_millisec(v_dtstart, systimestamp) / 1000);
      if v_waitsec > 0
      then
        set_session_action('Wait ...');
        dbms_lock.sleep(v_waitsec);
      end if;
    end loop;
    lock_release(С_LOCK_PROCESS_XQMANAGER_START);
  exception
    when others then
      rollback;
      lock_release(С_LOCK_PROCESS_XQMANAGER_START);
      send_information_error('XM' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
  end;

  -- Основной процесс менеджера очереди
  procedure ManagerMain(p_queue_num itt_q_message_log.queue_num%type) is
    numeric_overflow exception;
    pragma exception_init(numeric_overflow, -01426);
    listen_time_out exception;
    pragma exception_init(listen_time_out, -25254);
    deque_time_out exception;
    pragma exception_init(deque_time_out, -25228);
    vn_count_pack     integer := 0; -- Счетчик сообщений в пакете
    vd_n_startdt      timestamp;
    vd_f_startdt      timestamp;
    vc_cur_order      varchar2(2000) := ' order by enqdt,local_order_no desc';
    vc_cur_all_select varchar2(2000) := 'select qmsgid,msgid,enqdt, correlation, message_type, delivery_type, priority, servicename, sender, senderuser, servicegroup,  txtmessbody from ' ||
                                        IT_Q_MESSAGE.C_C_QVIEW_TASK_PREFIX || p_queue_num;
    cur_n             sys_refcursor;
    vc_cur_n_select   varchar2(2000) := vc_cur_all_select || ' t where  t.priority = ''' || IT_Q_MESSAGE.C_C_MSG_PRIORITY_N || ''' or t.delivery_type = ''' ||
                                        IT_Q_MESSAGE.C_C_MSG_DELIVERY_A || ''' or t.servicegroup is not null ' || vc_cur_order;
    cur_f             sys_refcursor;
    vc_cur_f_select   varchar2(2000) := vc_cur_all_select || ' t where t.priority != ''' || IT_Q_MESSAGE.C_C_MSG_PRIORITY_N || ''' and t.delivery_type != ''' ||
                                        IT_Q_MESSAGE.C_C_MSG_DELIVERY_A || ''' and t.servicegroup is null ' || vc_cur_order;
    vc_cur_select     varchar2(2000);
    v_queuename       itt_q_message_log.queuename%type := IT_Q_MESSAGE.C_C_QUEUE_IN_PREFIX || p_queue_num;
    v_n_qmsgid        raw(16);
    v_n_msgid         itt_q_message_log.msgid%type;
    v_n_enqdt         timestamp;
    v_n_correlation   varchar2(128);
    v_n_message_type  itt_q_message_log.message_type%type;
    v_n_delivery_type itt_q_message_log.delivery_type%type;
    v_n_priority      itt_q_message_log.Priority%type;
    v_n_servicename   itt_q_message_log.ServiceName%type;
    v_n_sender        itt_q_message_log.Sender%type;
    v_n_senderuser    itt_q_message_log.SenderUser%type;
    v_n_servicegroup  itt_q_message_log.ServiceGroup%type;
    v_n_txtmessbody   varchar2(128);
    v_f_qmsgid        raw(16);
    v_f_msgid         itt_q_message_log.msgid%type;
    v_f_enqdt         timestamp;
    v_f_correlation   varchar2(128);
    v_f_message_type  itt_q_message_log.message_type%type;
    v_f_delivery_type itt_q_message_log.delivery_type%type;
    v_f_priority      itt_q_message_log.Priority%type;
    v_f_servicename   itt_q_message_log.ServiceName%type;
    v_f_sender        itt_q_message_log.Sender%type;
    v_f_senderuser    itt_q_message_log.SenderUser%type;
    v_f_servicegroup  itt_q_message_log.ServiceGroup%type;
    v_f_txtmessbody   varchar2(128);
    v_fworker_on      boolean;
    vb_tmp            boolean;
  begin
    if p_queue_num is null
       or p_queue_num != upper(p_queue_num)
       or it_q_message.check_queue_num(p_queue_num) != 1
    then
      raise_application_error(-20000, 'Очередь ID ' || p_queue_num || ' не инсталлирована в системе');
    end if;
    gс_manager_queue_num := p_queue_num;
    set_session_module;
    --
    vc_cur_all_select := vc_cur_all_select || vc_cur_order;
    flush_services(false);
    init_debug();
    init_qsettings;
    vb_tmp := workers_flush; -- разбор всего PIPE
    workers_refresh(true);
    pipemanager_start;
    do_disassembly;
    q_wait(true);
    loop
      --debug('Manager' || p_queue_num || ' managermain loop');
      vd_n_startdt := null;
      v_fworker_on := (gn_mincount_free_for_s > 0 and gn_worker_f_count > 0); -- Включена обработка SF cообщений
      if v_fworker_on
      then
        vn_count_pack := gn_messpack_count + 1;
        vc_cur_select := vc_cur_n_select;
      else
        vd_f_startdt  := null;
        vn_count_pack := 0;
        vc_cur_select := vc_cur_all_select;
      end if;
      open cur_n for vc_cur_select;
      loop
        fetch cur_n
          into v_n_qmsgid
              ,v_n_msgid
              ,v_n_enqdt
              ,v_n_correlation
              ,v_n_message_type
              ,v_n_delivery_type
              ,v_n_priority
              ,v_n_servicename
              ,v_n_sender
              ,v_n_senderuser
              ,v_n_servicegroup
              ,v_n_txtmessbody;
        exit when cur_n%notfound;
        if vd_n_startdt is null -- Первое задание
        then
          --debug('Manager' || p_queue_num || ' managermain START');
          vd_n_startdt := systimestamp;
          set_session_action('Run');
          pipemanager_start;
        end if;
        if v_fworker_on
           and vn_count_pack >= gn_messpack_count + 1
        then
          vn_count_pack := 0;
          if not cur_f%isopen
          then
            open cur_f for vc_cur_f_select;
            vd_f_startdt := null;
          end if;
          loop
            fetch cur_f
              into v_f_qmsgid
                  ,v_f_msgid
                  ,v_f_enqdt
                  ,v_f_correlation
                  ,v_f_message_type
                  ,v_f_delivery_type
                  ,v_f_priority
                  ,v_f_servicename
                  ,v_f_sender
                  ,v_f_senderuser
                  ,v_f_servicegroup
                  ,v_f_txtmessbody;
            exit when cur_f%notfound;
            vd_f_startdt := nvl(vd_f_startdt, systimestamp);
            work_run(p_enqtdt => v_f_enqdt
                    ,p_startdt => vd_f_startdt
                    ,p_message_type => v_f_message_type
                    ,p_delivery_type => v_f_delivery_type
                    ,p_priority => IT_Q_MESSAGE.C_C_MSG_PRIORITY_F
                    ,p_queuename => v_queuename
                    ,p_qmsgid => v_f_qmsgid
                    ,p_msgid => v_f_msgid
                    ,p_correlation => v_f_correlation
                    ,p_servicename => v_f_servicename
                    ,p_sender => v_f_sender
                    ,p_senderuser => v_f_senderuser
                    ,p_servicegroup => v_f_servicegroup
                    ,p_txtmessbody => v_f_txtmessbody); -- Отправка бастрой команды обработчику
            vn_count_pack := vn_count_pack + 1;
            exit when vn_count_pack >= gn_messpack_count;
          end loop;
          if cur_f%isopen
             and cur_f%notfound
          then
            close cur_f;
          end if;
        end if;
        work_run(p_enqtdt => v_n_enqdt
                ,p_startdt => vd_n_startdt
                ,p_message_type => v_n_message_type
                ,p_delivery_type => v_n_delivery_type
                ,p_priority => IT_Q_MESSAGE.C_C_MSG_PRIORITY_N
                ,p_queuename => v_queuename
                ,p_qmsgid => v_n_qmsgid
                ,p_msgid => v_n_msgid
                ,p_correlation => v_n_correlation
                ,p_servicename => v_n_servicename
                ,p_sender => v_n_sender
                ,p_senderuser => v_n_senderuser
                ,p_servicegroup => v_n_servicegroup
                ,p_txtmessbody => v_n_txtmessbody); -- Отправка обычной команды обработчику
        vn_count_pack := vn_count_pack + 1;
      end loop;
      close cur_n;
      if v_fworker_on
         and vd_n_startdt is null -- Если нет медленных проталкиваем все быстрые
      then
        vd_f_startdt := null;
        open cur_f for vc_cur_f_select;
        loop
          fetch cur_f
            into v_f_qmsgid
                ,v_f_enqdt
                ,v_f_correlation
                ,v_f_message_type
                ,v_f_delivery_type
                ,v_f_priority
                ,v_f_servicename
                ,v_f_sender
                ,v_f_senderuser
                ,v_f_servicegroup
                ,v_f_txtmessbody;
          exit when cur_f%notfound;
          if vd_f_startdt is null
          then
            pipemanager_start;
            vd_f_startdt := systimestamp;
          end if;
          work_run(p_enqtdt => v_f_enqdt
                  ,p_startdt => vd_f_startdt
                  ,p_message_type => v_f_message_type
                  ,p_delivery_type => v_f_delivery_type
                  ,p_priority => IT_Q_MESSAGE.C_C_MSG_PRIORITY_F
                  ,p_queuename => v_queuename
                  ,p_qmsgid => v_f_qmsgid
                  ,p_msgid => v_f_msgid
                  ,p_correlation => v_f_correlation
                  ,p_servicename => v_f_servicename
                  ,p_sender => v_f_sender
                  ,p_senderuser => v_f_senderuser
                  ,p_servicegroup => v_f_servicegroup
                  ,p_txtmessbody => v_f_txtmessbody); -- Отправка бастрой команды обработчику
        end loop;
        close cur_f;
      end if;
      init_qsettings;
      if vd_n_startdt is not null
         or vd_f_startdt is not null
      then
        --debug('Manager' || gс_manager_queue_num || ':end sp task ');
        worker_mark;
        workers_refresh;
      else
        set_session_action;
        --debug('Manager' || gс_manager_queue_num || ':NO task ');
        -- Если нет медленных и быстрых заданий
        vb_tmp := workers_flush;
        worker_mark;
        task_transfer;
        workers_refresh(true);
        refresh_spr(true);
        q_disassembly;
        worker_restart;
        flush_services(true);
        if it_q_message.get_count_queue > 1
           and it_q_message.get_count_task(p_max_count => 0, p_not_qXX => 1) > 0
        then
          qmanager_start;
        else
          workers_repair;
          worker_minus;
        end if;
        --exit; --!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        pipemanager_start;
        set_session_action('Wait ...');
        q_wait; -- Переходим в ожидание
      end if;
    end loop;
    flush_services(true);
    commit;
  exception
    when others then
      rollback;
      if cur_n%isopen
      then
        close cur_n;
      end if;
      if cur_f%isopen
      then
        close cur_f;
      end if;
      send_information_error('QM' || sqlcode, sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
      flush_services(true);
  end;

  -- Запуск QMANAGER (получение информации) 1-ОК  
  function startmanager(p_queue_num varchar2 default null
                       ,o_info      out varchar2) return integer is
    pragma autonomous_transaction;
    v_ret             integer;
    vc_jenabled       varchar2(100);
    vc_jstate         varchar2(100);
    vc_process        varchar2(100) := 'it_q_manager.startmanager';
    v_enqueue_enabled varchar2(100); --user_queues.ENQUEUE_ENABLED%type;
    v_dequeue_enabled varchar2(100); --user_queues.DEQUEUE_ENABLED%type;*/
    v_queue_name      varchar2(128);
    listen_time_out exception;
    pragma exception_init(listen_time_out, -20254);
    deque_time_out exception;
    pragma exception_init(deque_time_out, -20228);
    vr_msg        it_q_message_t;
    v_qmsgid      raw(16);
    v_correlation itt_q_message_log.correlation%type;
  begin
    if lock_request(vc_process)
    then
      for q in (select column_value queue_num from table(it_q_message.select_queue_num) order by 1)
      loop
        v_queue_name := upper(IT_Q_MESSAGE.C_C_QUEUE_IN_PREFIX || q.queue_num);
        if q.queue_num = it_q_message.C_C_QUEUENUM_XX
        then
          -- Очистка очереди от управляющих сообщений на случай, если напихали много EXIT, RESTART ...
          begin
            loop
              it_q_message.msg_dequeue(p_correlation => IT_Q_MESSAGE.GC_CORR_COMMAND
                                      ,p_queuetype => IT_Q_MESSAGE.C_C_QUEUE_TYPE_IN
                                      ,p_queue_num => q.queue_num
                                      ,p_wait => 0
                                      ,o_qmsgid => v_qmsgid
                                      ,o_correlation => v_correlation
                                      ,o_message => vr_msg);
              commit;
            end loop;
          exception
            when deque_time_out then
              commit;
          end;
          XWorker_start;
          o_info := o_info || С_JOB_MANAGER_PREFIX || q.queue_num || ' Running ';
        else
          execute immediate '
        select max(trim(lq.enqueue_enabled))
              ,max(trim(lq.dequeue_enabled))
          from user_queues lq
         where name = :queue_name'
            into v_enqueue_enabled, v_dequeue_enabled
            using in v_queue_name;
          if v_enqueue_enabled != 'YES'
             or v_dequeue_enabled != 'YES'
          then
            execute immediate 'call dbms_aqadm.start_queue(:queue_name)'
              using v_queue_name;
          end if;
          v_queue_name := upper(IT_Q_MESSAGE.C_C_QUEUE_OUT_PREFIX || q.queue_num);
          execute immediate '
        select max(trim(lq.enqueue_enabled))
              ,max(trim(lq.dequeue_enabled))
          from user_queues lq
         where name = :queue_name'
            into v_enqueue_enabled, v_dequeue_enabled
            using in v_queue_name;
          if v_enqueue_enabled != 'YES'
             or v_dequeue_enabled != 'YES'
          then
            execute immediate 'call dbms_aqadm.start_queue(:queue_name)'
              using v_queue_name;
          end if;
          begin
            select enabled -- 'TRUE'
                  ,state -- 'RUNNING'
              into vc_jenabled
                  ,vc_jstate
              from sys.user_scheduler_jobs
             where job_name = С_JOB_MANAGER_PREFIX || q.queue_num;
          exception
            when no_data_found then
              vc_jenabled := null;
          end;
          begin
            if vc_jenabled is null
            then
              sys.dbms_scheduler.create_job(job_name => С_JOB_MANAGER_PREFIX || q.queue_num
                                           ,job_type => 'STORED_PROCEDURE'
                                           ,job_action => 'it_q_manager.ManagerMain'
                                           ,number_of_arguments => 1
                                           ,start_date => sysdate
                                           ,repeat_interval => 'Freq=Secondly;Interval=2'
                                           ,auto_drop => false
                                           ,comments => 'Менеджер очереди ' || IT_Q_MESSAGE.C_C_QUEUE_IN_PREFIX || q.queue_num);
              sys.dbms_scheduler.set_job_argument_value(job_name => С_JOB_MANAGER_PREFIX || q.queue_num, argument_position => 1, argument_value => q.queue_num);
              sys.dbms_scheduler.enable(С_JOB_MANAGER_PREFIX || q.queue_num);
              o_info := o_info || С_JOB_MANAGER_PREFIX || q.queue_num || ' START ';
            elsif vc_jenabled != 'TRUE'
                  or vc_jstate != 'RUNNING'
            then
              sys.dbms_scheduler.set_attribute(С_JOB_MANAGER_PREFIX || q.queue_num, 'start_date', sysdate);
              sys.dbms_scheduler.enable(С_JOB_MANAGER_PREFIX || q.queue_num);
              o_info := o_info || С_JOB_MANAGER_PREFIX || q.queue_num || ' Enable ';
            else
              o_info := o_info || С_JOB_MANAGER_PREFIX || q.queue_num || ' Running ';
            end if;
            select enabled -- 'TRUE'
                  ,state -- 'RUNNING'
              into vc_jenabled
                  ,vc_jstate
              from sys.user_scheduler_jobs
             where job_name = С_JOB_MANAGER_PREFIX || q.queue_num;
            o_info := o_info || С_JOB_MANAGER_PREFIX || q.queue_num || ' Enabled=[' || vc_jenabled || '] State=[' || vc_jstate || ']' || chr(10);
          exception
            when others then
              o_info := o_info || С_JOB_MANAGER_PREFIX || q.queue_num || ' ERROR :' || sqlerrm;
              v_ret  := 0;
          end;
        end if;
      end loop;
    end if;
    lock_release(vc_process);
    return nvl(v_ret, 1);
  exception
    when others then
      rollback;
      o_info := o_info || sqlerrm;
      lock_release(vc_process);
      return 0;
  end;

  --==============================================================================
  -- Отправка команды QManagerу
  procedure cmdmanager(p_msg       varchar2
                      ,p_queue_num itt_q_message_log.queue_num%type default null -- если null - всем очередям
                       ) is
    pragma autonomous_transaction;
    vr_msg it_q_message_t;
  begin
    if p_queue_num is not null
       and it_q_message.check_queue_num(p_queue_num) != 1
    then
      raise_application_error(-20000, 'ID очереди не может быть = ' || p_queue_num);
    end if;
    for q in (select column_value queue_num from table(it_q_message.select_queue_num) where column_value = nvl(p_queue_num, column_value))
    loop
      -- создаем сообщение
      vr_msg := it_q_message.new_message(p_message_type => IT_Q_MESSAGE.C_C_MSG_TYPE_R
                                        ,p_delivery_type => IT_Q_MESSAGE.C_C_MSG_DELIVERY_A
                                        ,p_Priority => IT_Q_MESSAGE.C_C_MSG_PRIORITY_F
                                        ,p_MESSBODY => p_msg
                                        ,p_queue_num => q.queue_num
                                        ,p_check => false);
      it_q_message.msg_enqueue(p_message => vr_msg
                              ,p_isquery => 0
                              ,p_comment => null
                              ,p_queuetype => IT_Q_MESSAGE.C_C_QUEUE_TYPE_IN
                              ,p_queue_num => q.queue_num
                              ,p_correlation => IT_Q_MESSAGE.GC_CORR_COMMAND
                              ,p_no_log => 1);
      commit;
    end loop;
  end;

begin
  init_qsettings;
  refresh_spr(true);
end;
/
