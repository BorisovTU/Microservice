create or replace package it_q_message is

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
    03.08.2022  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/
  C_C_UTEG constant varchar2(1) := 'Ю';

  C_C_SYSTEMNAME constant varchar2(32) := 'SOFR_DB';

  C_C_QUEUENUM_XX       constant char(2) := 'XX'; -- Номер табличной очереди
  C_N_QUEUEXX_STATE_RUN constant integer := 5 ;
  C_C_QTABLE_IN_PREFIX  constant varchar2(32) := 'ITT_QUEUE_IN_'; -- Префикс таблицы IN
  C_C_QTABLE_OUT_PREFIX constant varchar2(32) := 'ITT_QUEUE_OUT_'; -- Префикс таблицы OUT
  C_C_QUEUE_IN_PREFIX   constant varchar2(32) := 'ITQ_IN_'; -- Префикс входящей очереди 
  C_C_QUEUE_OUT_PREFIX  constant varchar2(32) := 'ITQ_OUT_'; -- Префикс исходящей очереди
  C_C_QVIEW_IN_PREFIX   constant varchar2(32) := 'ITV_Q_IN'; -- Префикс view просмотра входящей очереди
  C_C_QVIEW_OUT_PREFIX  constant varchar2(32) := 'ITV_Q_OUT'; -- Префикс view просмотра исходящей очереди
  C_C_QVIEW_TASK_PREFIX constant varchar2(32) := 'ITV_Q_TASK'; -- Префикс view просмотра входящих заданий (R_ / AA)
  -- 
  C_C_QUEUE_TYPE_IN  constant varchar2(3) := 'IN'; --
  C_C_QUEUE_TYPE_OUT constant varchar2(3) := 'OUT'; --
  C_C_QUEUE_TYPE_NO  constant varchar2(3) := '---'; -- Сообщение в логе не из очереди (TRASH)
  -- Статусы IN сообщения
  C_STATUS_WORK     constant varchar2(32) := 'WORK'; -- сообщение в работе у работника (процессора)
  C_STATUS_ERRWORK  constant varchar2(32) := 'ERRWORK'; -- Сообщение c ошибкой в стадии обработки
  C_STATUS_LOAD     constant varchar2(32) := 'LOAD'; -- сообщение загружено ( сохранено без выполнения сервиса)
  C_STATUS_ERRLOAD  constant varchar2(32) := 'ERRLOAD'; -- Сообщение c ошибкой загружено 
  C_STATUS_DONE     constant varchar2(32) := 'DONE'; -- сообщение  обработано (выполнен сервис)
  C_STATUS_ERRDONE  constant varchar2(32) := 'ERRDONE'; -- Сообщение c ошибкой обработано (выполнен сервис)
  C_STATUS_ERROR    constant varchar2(32) := 'ERROR'; -- Сообщение обработано (выполнен сервис) c ошибкой
  C_STATUS_ROLLBACK constant varchar2(32) := 'ROLLBACK'; -- произведен откат сообщения (зарезервировано)
  -- Статусы OUT сообщения
  C_STATUS_SEND           constant varchar2(32) := 'SEND'; -- сообщение отправлено
  C_STATUS_SENDQUERY      constant varchar2(32) := 'SENDQUERY'; -- отправлено сообщние с ожиданием ответа 
  C_STATUS_ERRSEND        constant varchar2(32) := 'ERRSEND'; -- Сообщение не отправлено (ошибка транспортной системой)
  C_STATUS_DELIVERED      constant varchar2(32) := 'DELIVERED'; -- Сообщение получено транспортной системой
  C_STATUS_DELIVEREDQUERY constant varchar2(32) := 'DELIVEREDQUERY'; -- Сообщение с ожиданием ответа получено транспортной системой  
  C_STATUS_TIMEOUT        constant varchar2(32) := 'TIMEOUT'; -- ожидание ответа синхронного запроса(RS) прервалось по таймауту (не дождались ответа)
  -- Общие 
  C_STATUS_ANSWER    constant varchar2(32) := 'ANSWER'; -- для сообщения отправлен/получен ответ
  C_STATUS_ERRANSWER constant varchar2(32) := 'ERRANSWER'; -- для сообщения отправлен/получен ответ об ошибке
  C_STATUS_TRASH     constant varchar2(32) := 'TRASH'; -- "Мусор" ( используется в индексе LOG)
  -- Группы статусов ( при добавлении править select_status)
  C_KIND_STATUS_WORK    constant varchar2(32) := 'WORK'; -- 
  C_KIND_STATUS_DONE    constant varchar2(32) := 'DONE'; -- 
  C_KIND_STATUS_ERROR   constant varchar2(32) := 'ERROR'; -- 
  C_KIND_STATUS_SEND    constant varchar2(32) := 'SEND'; -- 
  C_KIND_STATUS_ERRSEND constant varchar2(32) := 'ERRSEND'; --
  -- Списки групп статусов ( при добавлении править select_status)
  type tt_status_sp is table of varchar2(32); --
  T_STATUS_MSG_WORK  tt_status_sp := tt_status_sp(C_STATUS_WORK, C_STATUS_SEND, C_STATUS_SENDQUERY, C_STATUS_DELIVEREDQUERY); -- в работе 
  T_STATUS_MSG_DONE  tt_status_sp := tt_status_sp(C_STATUS_DONE, C_STATUS_ANSWER, C_STATUS_LOAD, C_STATUS_DELIVERED, C_STATUS_ROLLBACK); -- обработано  
  T_STATUS_MSG_ERROR tt_status_sp := tt_status_sp(C_STATUS_ERROR, C_STATUS_ERRWORK, C_STATUS_ERRDONE, C_STATUS_ERRLOAD, C_STATUS_ERRANSWER, C_STATUS_ERRSEND, C_STATUS_TIMEOUT); -- обработано с ошибкой 
  -- Статусы траспорта
  T_STATUS_MSG_SEND    tt_status_sp := tt_status_sp(C_STATUS_SEND, C_STATUS_SENDQUERY, C_STATUS_DELIVERED, C_STATUS_DELIVEREDQUERY); -- отправлено 
  T_STATUS_MSG_ERRSEND tt_status_sp := tt_status_sp(C_STATUS_ERRSEND, C_STATUS_TIMEOUT); -- отправлено с ошибкой 
  -- Атрибуты сообщения
  C_C_MSG_TYPE_A constant char(1) := 'A'; --
  C_C_MSG_TYPE_R constant char(1) := 'R';

  C_C_MSG_DELIVERY_S constant char(1) := 'S'; --
  C_C_MSG_DELIVERY_A constant char(1) := 'A';

  C_C_MSG_PRIORITY_F constant char(1) := 'F'; --
  C_C_MSG_PRIORITY_N constant char(1) := 'N';

  GC_CORR_COMMAND constant varchar(128) := 'RAF-SOFR-COMMAND'; -- correlation для команды qmanager
  C_N_THREAD_MAX  constant pls_integer := 16; -- максимольное число потоков исполнения пользователя 
  gn_queueXX_dequeue_sleep     number := 1 / 10; -- Задержка в сек между повторами опросов табличной очереди при вычитке с ожиданием  
  gn_queueXX_max_retries_IN    integer := 999; -- Кол-во попыток вычиток входящих сообщения перед сменой статуса ( отменой)   
  gn_queueXX_max_retries_OUT   integer := 99; -- Кол-во попыток вычиток исходящих сообщения перед сменой статуса ( отменой)   
  gn_queueXX_retry_delay_IN_S  number := 2; -- Задержка в сек повторной вычитки входящих S сообщения    
  gn_queueXX_retry_delay_IN_A  number := 10; -- Задержка в сек повторной вычитки входящих A сообщения    
  gn_queueXX_retry_delay_OUT_S number := 60; -- Задержка в сек повторной вычитки исходящих S сообщения    
  gn_queueXX_retry_delay_OUT_A number := 5 * 60; -- Задержка в сек повторной вычитки исходящих A сообщения    
  ------------------------------------------------------------------------------------------
  type tt_queue_num is table of itt_q_message_log.queue_num%type;

  type tt_q_message_log is table of itt_q_message_log%rowtype;

  -- Табличная функция получения списка статусов
  function select_status(p_kind_status varchar2) return tt_status_sp
    pipelined;

  -- Возвращает группу статуса
  function get_kind_status(p_status varchar2) return varchar2 deterministic;

  -- Проверка состояние процесса по статусу ( -1 - ошибка, 0- процесс идет , 1 - процесс закончен )
  function chk_process_state(p_status varchar2) return number deterministic ;

  -- Возвращает значение пакетной переменной по имени
  function get_constant_str(p_constant varchar2) return varchar2 deterministic;

  --  Наполнение коллекций для генерации потоков исполнения пользователя
  procedure thread_init(p_count_thread pls_integer default null
                       ,p_systemname   itt_q_message_log.receiver%type default C_C_SYSTEMNAME
                       ,p_queue_num    itt_q_message_log.queue_num%type default null);

  --  Переход к  потоку исполнения ( если 0 - следующему)
  procedure thread_next(p_num_thread pls_integer default null
                       ,p_systemname itt_q_message_log.receiver%type default C_C_SYSTEMNAME);

  -- Возвращает servicegroup текущего потока исполнения пользователя
  function thread_get_servicegroup(p_systemname itt_q_message_log.receiver%type default C_C_SYSTEMNAME) return itt_q_message_log.servicegroup%type;

  -- Возвращает очередь для текущего потока исполнения пользователя
  function thread_get_queue_num(p_systemname itt_q_message_log.receiver%type default C_C_SYSTEMNAME) return itt_q_message_log.queue_num%type;

  -- Балансировщик потоков. Получение следующего servicegroup для префикса потоков
  function get_next_servicegroup(p_pref_servicegroup varchar2
                                ,io_last_thread      in out pls_integer
                                ,p_max_thread        pls_integer default null
                                ,p_systemname        itt_q_message_log.receiver%type default C_C_SYSTEMNAME
                                ,p_queue_num         itt_q_message_log.queue_num%type default null) return varchar2;

  -- Генерим GUID с разделителями
  function get_sys_guid return varchar2;

  --функция, возвращает цифровое значение настройки
  function get_qset_number(p_qset_name itt_q_settings.qset_name%type) return itt_q_settings.value_number%type;

  --функция, возвращает текстовое значение настройки
  function get_qset_char(p_qset_name itt_q_settings.qset_name%type) return itt_q_settings.value_varchar%type;

  -- Процедура сохраняет настройку типа ДАТА
  procedure set_qset_data(p_qset_name itt_q_settings.qset_name%type
                         ,p_date      date);

  --функция, возвращает значение настройки типа ДАТА
  function get_qset_data(p_qset_name itt_q_settings.qset_name%type) return itt_q_settings.value_date%type;

  -- Имя пользователя отправителя сообщения
  function get_q_user return itt_q_message_log.senderuser%type;

  -- Выбор номера очереди для отправки сообщения (пока все последовательно из общих)
  function find_queue_num(p_message it_q_message_t default null) return itt_q_message_log.queue_num%type;

  --  Получение последовательно номера очереди из всего списка
  function next_queue_num return itt_q_message_log.queue_num%type;

  -- Получение типа очереди IN/OUT ; null - ошибка (ITQ_IN_01)
  function get_queuetype(p_queuename itt_q_message_log.queuename%type) return itt_q_message_log.queuetype%type deterministic;

  -- Формируем correlation сообщения
  function get_correlation(p_message_type  itt_q_message_log.message_type%type
                          ,p_delivery_type itt_q_message_log.delivery_type%type
                          ,p_priority      itt_q_message_log.priority%type
                          ,p_msgid         itt_q_message_log.msgid%type
                          ,p_corrmsgid     itt_q_message_log.corrmsgid%type default null) return varchar2;

  -- Обновление списка инсталированных очередей.
  procedure refresh_table_queue_num;

  -- Табличная функция получения списка инсталированных очередей
  function select_queue_num return tt_queue_num
    pipelined;

  -- Возвращает 1 в случае правильного queue_num
  function check_queue_num(p_queue_num itt_q_message_log.queue_num%type default null) return number;

  -- Возвращает кол-во инсталлированных очередей
  function get_count_queue return integer;

  --- Количество заданий во входящей очереди
  function get_count_task(p_queue_num itt_q_message_log.queue_num%type default null -- null во всех очередяж
                         ,p_max_count number default null -- граница подстчета 
                         ,p_not_qXX   number default 0 -- 1-   без табличной очереди 
                          ) return integer;

  --- Количество сообщений во входящей очереди
  function get_count_in_msg(p_queue_num itt_q_message_log.queue_num%type default null -- null во всех очередяж
                           ,p_max_count number default null -- граница подстчета 
                            ) return integer;

  -- Возвращает временную метку создания сообщения в очереди при чтении 
  function get_enqdt_deq(p_qmsgid    raw
                        ,p_queuename itt_q_message_log.queuename%type) return timestamp;

  -- Возвращает временную метку создания сообщения в очереди при записи 
  function get_enqdt_enq(p_qmsgid    raw
                        ,p_queuename itt_q_message_log.queuename%type) return timestamp;

  -- Получение номера очереди из имени обекта (ITQ_IN_01,ITV_Q_IN01 .....)
  function get_queue_num(p_objname varchar2) return itt_q_message_log.queue_num%type deterministic;

  -- Получение парвметра кореспонлирующей системы 
  function get_corrsystem_param(p_system_name varchar2
                               ,p_param       varchar2) return varchar2 deterministic;

  -- Получение полного имени  очереди 
  function get_full_queue_name(p_queue_name itt_q_message_log.queuename%type) return varchar2;

  -- Вставка сообщения в очередь ( техническая - не использовать). 
  procedure msg_enqueue(p_message     it_q_message_t
                       ,p_isquery     pls_integer -- сообщение с ожиданием ответа (1/0)
                       ,p_comment     varchar2
                       ,p_queuetype   itt_q_message_log.queuetype%type
                       ,p_queue_num   itt_q_message_log.queue_num%type
                       ,p_correlation varchar2
                       ,p_delay       number default 0 --sys.dbms_aq.no_delay -- задежка появления сообщения в очереди
                       ,p_no_log      number default 0 -- 1- Не записываем в лог
                        );

  -- Вычитка ( ожидание) сообщения из очереди  (техническая - не использовать)
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
                       ,o_message     out it_q_message_t);

  -- Вычитка ( ожидание) сообщения из очереди  (техническая - не использовать)
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
                          ,o_message     out it_q_message_t);

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
                          ,p_logid       number default null);

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
                              ,o_messmeta    out xmltype);

  -- Формирование строкм комментария 
  function get_comment_add(p_msgcode     integer
                          ,p_add_comment varchar2
                          ,p_comenttxt   varchar2 default null
                          ,p_len         integer default 4000) return varchar2;

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
                                ,o_logid         out integer);

  -- процедура апдейта статуса сообщения в логе
  procedure messlog_upd_status(p_msgid         itt_q_message_log.msgid%type
                              ,p_delivery_type itt_q_message_log.delivery_type%type
                              ,p_queuetype     itt_q_message_log.queuetype%type
                              ,p_status        itt_q_message_log.status%type
                              ,p_workername    itt_q_message_log.workername%type default null
                              ,p_comment       varchar2 default null);

  -- процедура апдейта информации о вычитки сообщения в логе
  procedure messlog_upd_deq(p_msgid     itt_q_message_log.msgid%type
                           ,p_queuetype itt_q_message_log.queuetype%type
                           ,p_errno     integer default 0
                           ,p_comment   varchar2 default null);

  -- функция возвращает сообщение из лога
  function messlog_get(p_logid     itt_q_message_log.log_id%type default null
                      ,p_msgid     itt_q_message_log.msgid%type default null
                      ,p_queuetype itt_q_message_log.queuetype%type default C_C_QUEUE_TYPE_IN) return itt_q_message_log%rowtype;

  -- функция возвращает сообщение из лога с блокировкой
  function messlog_get_withlock(p_logid     itt_q_message_log.log_id%type default null
                               ,p_msgid     itt_q_message_log.msgid%type default null
                               ,p_queuetype itt_q_message_log.queuetype%type default C_C_QUEUE_TYPE_IN) return itt_q_message_log%rowtype;

  -- Возвращает текст ошибки из sqlerrm
  function get_errtxt(p_sqlerrm varchar2) return varchar2;

  -- Возвращает 1 в случае правильното correlation
  function check_correlation(p_correlation   varchar2
                            ,p_message_type  itt_q_message_log.message_type%type
                            ,p_delivery_type itt_q_message_log.delivery_type%type
                            ,p_priority      itt_q_message_log.priority%type
                            ,p_msgid         itt_q_message_log.msgid%type
                            ,p_corrmsgid     itt_q_message_log.corrmsgid%type
                            ,p_qmcheck       integer default 0 -- Предпроверка в QMANAGERе
                             ) return integer;

  -- Функция проверки атрибутов сообщения 1 - если ОК; 0- EROR ;<0 - TRASH
  function check_atr_message(p_correlation varchar2
                            ,p_queuename   itt_q_message_log.queuename%type
                            ,p_r_msg       it_q_message_t
                            ,o_errcode     out integer -- код ошибки если > 0 то в ERROR если < 0 то в TRASH
                            ,o_errmess     out varchar2 -- Ошибка
                             ) return integer;

  -- Функция проверки возможности отката сообщения 1 - если ОК , 0- пропуск , < 0 - ошибка 
  function check_rollback_message(p_msgid    itt_q_message_log.msgid%type
                                 ,p_withlock integer default null) return integer deterministic;

  -- Функция проверки возможности отката сообщения 1 - если ОК , 0- пропуск , < 0 - ошибка 
  function check_rollback_message_out(p_msgid    itt_q_message_log.msgid%type
                                     ,p_withlock integer default null
                                     ,o_errmess  out varchar2) return integer;

  -- Функция проверки входящего сообщения в записимости от отправителя  < 0 в 'TRASH'; >0 - отправляем ошибку
  function in_check_message(p_message it_q_message_t
                           ,o_errmess out varchar2 -- Сообщение об ошибке
                            ) return integer;

  -- Обновление статичных справочников в коллекции
  procedure refresh_spr(p_refresh boolean default false);

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
                       ) return it_q_message_t;

  -- процедура отправки сообщения (для QManagera)
  procedure send_message(io_message  in out nocopy it_q_message_t
                        ,p_queue_num itt_q_message_log.queue_num%type default null
                        ,p_delay     number default 0 --sys.dbms_aq.no_delay -- задежка появления сообщения в очереди
                        ,p_expire    date default null -- сообщение актуально до .... 
                        ,p_comment   varchar2 default null
                        ,p_isquery   pls_integer default null -- сообщение с ожиданием ответа (1/0)
                         );

  -- ================= Универсальные процедуры работы с сообщениями =============================
  --
  -- универсальная процедура запуска синхронного запроса
  procedure do_s_service(p_servicename    itt_q_message_log.servicename%type -- Бизнес-процесс
                        ,p_queue_num      itt_q_message_log.queue_num%type default null -- Номер очереди
                        ,p_receiver       itt_q_message_log.receiver%type default C_C_SYSTEMNAME -- Система-получатель
                        ,p_messbody       clob default null -- сообщение
                        ,p_messmeta       xmltype default null -- Мета данные
                        ,p_priority       itt_q_message_log.priority%type default C_C_MSG_PRIORITY_N -- Очередность синхронных сообщений ( F- быстрая,N- норм)
                        ,p_corrmsgid      itt_q_message_log.corrmsgid%type default null -- ID исходногоо сообщения 
                        ,p_comment        varchar2 default null -- коментарии в лог
                        ,p_timeout        integer default null --  таймаут в секундах
                        ,p_norise_msgcode integer default null -- если 1 - не выполняется EXCEPTION если сообщение с msgcode > 0
                        ,io_msgid         in out itt_q_message_log.msgid%type -- ID отправленого сообщения
                        ,o_answerid       out itt_q_message_log.msgid%type -- ID ответа
                        ,o_answerbody     out clob -- Ответ
                        ,o_answermeta     out xmltype -- Ответ Мета данные
                         );

  -- универсальная процедура запуска асинхронного запроса
  procedure do_a_service(p_servicename  itt_q_message_log.servicename%type -- Бизнес-процесс
                        ,p_receiver     itt_q_message_log.receiver%type default C_C_SYSTEMNAME -- Система-получатель
                        ,p_messbody     clob default null -- сообщение
                        ,p_messmeta     xmltype default null -- Мета данные
                        ,p_servicegroup itt_q_message_log.servicegroup%type default null -- GUID Поток исполнения бизнес-процесса 
                        ,p_queue_num    itt_q_message_log.queue_num%type default null -- Номер очереди
                        ,p_corrmsgid    itt_q_message_log.corrmsgid%type default null -- ID исходногоо сообщения 
                        ,p_comment      varchar2 default null -- коментарии в лог
                        ,p_delay        number default 0 -- sys.dbms_aq.no_delay -- задежка появления сообщения в очереди
                        ,io_msgid       in out itt_q_message_log.msgid%type -- ID созданого сообщения 
                        ,p_isquery      pls_integer default null -- сообщение с ожиданием ответа (1/0)
                         );

  -- Повтор сообщения
  procedure repeat_message(p_msgid   itt_q_message_log.msgid%type
                          ,p_delay   number -- задежка появления сообщения в очереди (для исключения рекурсивной генерации сообщений желательно указывать не 0 )
                          ,p_comment varchar2 default 'Повторно'
                          ,o_msgid   out varchar2);

  -- процедура размещения сообщения в очередь
  procedure load_msg(io_msgid        in out itt_q_message_log.msgid%type -- GUID сообщения
                    ,p_message_type  itt_q_message_log.message_type%type -- Тип сообщения R или A Запрос/ответ 
                    ,p_delivery_type itt_q_message_log.delivery_type%type -- Вид доставки S или A Синхронный / Асинхронный
                    ,p_Sender        itt_q_message_log.sender%type default C_C_SYSTEMNAME -- Система-источник
                    ,p_Priority      itt_q_message_log.priority%type default C_C_MSG_PRIORITY_N -- Очередность синхронных сообщений ( F- быстрая,N- норм)
                    ,p_Correlation   itt_q_message_log.correlation%type default null -- Correlation для внешнего сообщения (null - назначит система)
                    ,p_CORRmsgid     itt_q_message_log.corrmsgid%type default null -- GUID связанного сообщения 
                    ,p_SenderUser    itt_q_message_log.senderuser%type default get_q_user() -- Идентификатор пользователя в Системе-отправите
                    ,p_ServiceName   itt_q_message_log.servicename%type default null -- Бизнес-процесс
                    ,p_ServiceGroup  itt_q_message_log.servicegroup%type default null -- Поток исполнения бизнес-процесса  
                    ,p_Receiver      itt_q_message_log.receiver%type default C_C_SYSTEMNAME -- Система-получатель
                    ,p_BTUID         itt_q_message_log.btuid%type default null -- ID бизнес транзакции, зарезервировано
                    ,p_MSGCode       integer default 0 -- Код результата обработки сообщения. 0 - успех
                    ,p_MSGText       itt_q_message_log.msgtext%type default null -- Текст ошибки, возникший при обработке сообщен
                    ,p_MESSBODY      clob default null -- Бизнес - составляющая сообщения
                    ,p_MessMETA      xmltype default null -- Мета данные
                    ,p_queue_num     itt_q_message_log.queue_num%type default null -- ID очереди (null - определяется системой)
                    ,p_RequestDT     timestamp default systimestamp -- Время отправления сообщения из системы источник
                    ,p_ESBDT         timestamp default null -- Время начала обработки транспортной системой  
                    ,p_delay         number default 0 -- sys.dbms_aq.no_delay -- задежка появления сообщения в очереди
                    ,p_comment       itt_q_message_log.commenttxt%type default null -- коментарии в лог для транзитных сообщений
                    ,p_isquery       pls_integer default null -- сообщение с ожиданием ответа (1/0)
                     );

  -- процедура размещения сообщения во входящую очередь ( для адаптеров)
  procedure load_msg_inqueue(p_msgid         itt_q_message_log.msgid%type -- GUID сообщения
                            ,p_message_type  itt_q_message_log.message_type%type -- Тип сообщения R или A Запрос/ответ 
                            ,p_delivery_type itt_q_message_log.delivery_type%type -- Вид доставки S или A Синхронный / Асинхронный
                            ,p_Sender        itt_q_message_log.sender%type -- Система-источник
                            ,p_Priority      itt_q_message_log.priority%type default C_C_MSG_PRIORITY_N -- Очередность синхронных сообщений ( F- быстрая,N- норм)
                            ,p_CORRmsgid     itt_q_message_log.corrmsgid%type -- GUID связанного сообщения 
                            ,p_SenderUser    itt_q_message_log.senderuser%type default get_q_user() -- Идентификатор пользователя в Системе-отправите
                            ,p_ServiceName   itt_q_message_log.servicename%type -- Бизнес-процесс
                            ,p_ServiceGroup  itt_q_message_log.servicegroup%type -- Поток исполнения бизнес-процесса  
                            ,p_BTUID         itt_q_message_log.btuid%type default null -- ID бизнес транзакции, зарезервировано
                            ,p_MSGCode       integer -- Код результата обработки сообщения. 0 - успех
                            ,p_MSGText       itt_q_message_log.msgtext%type -- Текст ошибки, возникший при обработке сообщен
                            ,p_MESSBODY      clob -- Бизнес - составляющая сообщения
                            ,p_MessMETA      xmltype -- Мета данные -- XML Метаданные сообщения
                            ,p_queue_num     itt_q_message_log.queue_num%type -- ID очереди 
                            ,p_RequestDT     timestamp -- Время отправления сообщения из системы источник
                            ,p_ESBDT         timestamp -- Время начала обработки транспортной системой  
                             );

  -- Процедура получения синхронного ответа на асинхронный запрос если ответа нет o_msgid = null
  procedure wait_s_msg(p_msgid    itt_q_message_log.msgid%type -- msgid исходного сообщения
                      ,p_wait     number default 0.001 -- ожидание в секундах если 0 - то по настройке QMESSAGE_TIMEOUT
                      ,o_msgid    out itt_q_message_log.msgid%type -- msgid ответа 
                      ,o_msgcode  out integer
                      ,o_msgtext  out varchar2
                      ,o_messbody out clob
                      ,o_messmeta out xmltype);

  -- функция возвращает сообщение в последнем состоянии
  function get_msg(p_msgid      itt_q_message_log.msgid%type
                  ,p_with_TRASH boolean default false) return itt_q_message_log%rowtype;

  -- функция возвращает ответ на запрос 
  function get_answer_msg(p_msgid        itt_q_message_log.msgid%type
                         ,p_last_in_tree boolean default false -- Последний в дереве запросов - ответов
                          ) return itt_q_message_log%rowtype;

  -- функция список дерево запросов-ответoв  
  function select_answer_msg(p_msgid     itt_q_message_log.msgid%type
                            ,p_queuetype itt_q_message_log.queuetype%type default null -- C_C_QUEUE_TYPE_IN,C_C_QUEUE_TYPE_OUT
                             ) return tt_q_message_log
    pipelined;

  -- Возвращает статус процесса 
  function get_process_status(p_msgid    itt_q_message_log.msgid%type
                             ,o_dtstatus out timestamp
                             ,o_comment  out varchar2) return varchar2;

  -- Процедура вычитки сообщения из мсходящей очереди
  procedure dequeue_outmessage(p_queuename   itt_q_message_log.queuename%type -- Имя очереди 
                              ,p_qmsgid      raw default null -- qmsgid GUID сообщения в очереди
                              ,p_correlation varchar2 default null -- correlation сообщения в очереди
                              ,p_wait        number default 60 -- ожидание в секундах null всегда
                              ,o_message     out it_q_message_t
                              ,o_errno       out integer
                              ,o_errmsg      out varchar2);

end;
/
