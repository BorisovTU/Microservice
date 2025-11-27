create or replace package it_q_manager is

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
    09.09.2022  Зыков М.В.       BIQ-9225                         +show_qsettings
    03.08.2022  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/
  С_JOB_MANAGER_PREFIX    constant varchar2(32) := 'IT_Q_MANAGER'; --  префикс имени джоба для MANAGERов
  C_N_ERROR_OTHERS_MSGCODE constant integer := 20000;

  C_N_ERROR_RESTART_MSGCODE constant integer := 24061;

  C_C_ERROR_OTHERS_MSGTEXT constant varchar2(100) := 'Ошибка в работе сервиса';

  --------------------------------------------------------------------------------
  type tt_pipe_channel is table of itt_q_worker.pipe_channel%type;

  --------------------------------------------------------------------------------
  -- Возвращает значение пакетной переменной по имени
  function get_constant_str(p_constant varchar2) return varchar2 deterministic;

  function get_gn_message_log_hist return integer;

  -- Настройки 
  procedure init_qsettings;

  --- Процент нагрузки воркеров (100% - все занято )
  function get_worker_load_percent return pls_integer;

  --------------------------------------------------------------------------------
  -- Табличная функция получения списка pipe каналов
  function select_pipe_channel return tt_pipe_channel
    pipelined;

  function next_pipe_channel return itt_q_worker.pipe_channel%type;

  -- Обновление статусов WORKERов
  function workers_flush(p_wait_lock integer default 0
                        ,p_PipeMNG   boolean default false) return boolean;

  --- Количество свободных воркеров
  function get_count_worker_free(p_priority itt_q_message_log.priority%type default null) return integer deterministic;

  -- Получение списка QSETTINGS со знвчениями
  function show_qsettings return clob;

  -- Процедура очистки очередей
  procedure do_disassembly;

  --==============================================================================
  -- Запуск обработчика табличной очереди  
  procedure XWorker_start;

  -- Основной процесс менеджера pipe канала
  procedure pipemanagermain(p_queue_num itt_q_message_log.queue_num%type);

  -- Обработчик сообщений табличной очереди
  procedure WorkerXMain(p_worker_num integer);

  -- Обработчик сообщений
  procedure workermain(p_worker_num integer);

  -- Основной процесс менеджера табличной очереди
  procedure managerXmain;

  -- Основной процесс менеджера очередей
  procedure managermain(p_queue_num itt_q_message_log.queue_num%type -- ID Очереди
                        );

  --==============================================================================
  -- Запуск QMANAGER (получение информации) 1-ОК  
  function startmanager(p_queue_num varchar2 default null
                       ,o_info      out varchar2) return integer;

  -- Отправка команды QManagerу
  procedure cmdmanager(p_msg       varchar2
                      ,p_queue_num itt_q_message_log.queue_num%type default null -- если null - всем очередям
                       );

end;
/
