declare 
  n integer;
  v_queue_num char(2) := '01';
begin
  begin
    execute immediate 'begin it_q_manager.cmdmanager(p_msg => ''EXIT''); end;';
    dbms_lock.sleep(5);
  exception
    when others then 
    null;
  end;

  select count(*) into n from user_objects o where o.OBJECT_NAME = 'IT_Q_MESSAGE_T' and o.OBJECT_TYPE = 'TYPE' ;
  if n = 0 then
    execute immediate 'create or replace type It_q_message_t as object
    (
   -- AQ сообщение 
    message_type  char(1), -- Тип сообщения R или A Запрос/ответ 
    delivery_type char(1), -- Вид доставки S или A Синхронный / Асинхронный
    Priority      char(1), -- Очередность синхронных сообщений ( F- быстрая,N- норм)
    msgid         varchar2(128), -- GUID сообщения 
    CORRmsgid     varchar2(128), -- GUID связанного сообщения 
    ServiceName   varchar2(128), -- Бизнес-процесс
    Sender        varchar2(128), -- Система-источник 
    Receiver      varchar2(128), -- Система-получатель
    RequestDT     timestamp, -- Время создания сообщения в системе источник
    ESBDT         timestamp, -- Время начала обработки транспортной системой  
    SenderUser    varchar2(128), -- Идентификатор пользователя в Системе-отправителе
    ServiceGroup  varchar2(128), -- Поток исполнения бизнес-процесса
    BTUID         varchar2(128), -- GUID бизнес транзакции(зарезервировано)
    uteg          char(1), -- Ю
    MSGCode       integer, -- Код результата обработки сообщения. 0 - успех
    MSGText       varchar2(2000), -- Текст ошибки, возникший при обработке сообщения-запроса
    MessBODY      clob, -- Бизнес- составляющая сообщения
    MessMETA      xmltype, -- Метаданные сообщения
    queue_num     char(2) -- ID очереди (null - определяется системой)
    )';
  end if;

  select count(*) into n from user_queues q where q.name in ('ITQ_IN_' || v_queue_num, 'ITQ_OUT_' || v_queue_num);
  if n != 2
  then
    begin
      dbms_aqadm.stop_queue(queue_name => 'itq_IN_' || v_queue_num);
    exception
      when others then
        null;
    end;
    begin
      dbms_aqadm.stop_queue(queue_name => 'itq_OUT_' || v_queue_num);
    exception
      when others then
        null;
    end;
    begin
      dbms_aqadm.drop_queue(queue_name => 'itq_IN_' || v_queue_num);
    exception
      when others then
        null;
    end;
    begin
      dbms_aqadm.drop_queue(queue_name => 'itq_OUT_' || v_queue_num);
    exception
      when others then
        null;
    end;
    begin
      dbms_aqadm.drop_queue_table(queue_table => 'itt_queue_IN_' || v_queue_num);
    exception
      when others then
        null;
    end;
    begin
      dbms_aqadm.drop_queue_table(queue_table => 'itt_queue_OUT_' || v_queue_num);
    exception
      when others then
        null;
    end;
    -- создаем таблицу для очереди
    dbms_aqadm.create_queue_table(queue_table => 'itt_queue_IN_' || v_queue_num
                                 ,queue_payload_type => 'It_q_message_t'
                                 ,sort_list => 'ENQ_TIME'
                                 ,comment => 'Таблица для очереди itq_IN_' || v_queue_num);
    dbms_aqadm.create_queue_table(queue_table => 'itt_queue_OUT_' || v_queue_num
                                 ,queue_payload_type => 'It_q_message_t'
                                 ,sort_list => 'ENQ_TIME'
                                 ,comment => 'Таблица для очереди itq_OUT_' || v_queue_num);
    -- создаем очередь
    dbms_aqadm.create_queue(queue_name => 'itq_IN_' || v_queue_num, queue_table => 'itt_queue_IN_' || v_queue_num, max_retries => 999999);
    dbms_aqadm.create_queue(queue_name => 'itq_OUT_' || v_queue_num, queue_table => 'itt_queue_OUT_' || v_queue_num, max_retries => 999999);
    -- стартуем очередь
    dbms_aqadm.start_queue('itq_IN_' || v_queue_num);
    dbms_aqadm.start_queue('itq_OUT_' || v_queue_num);
  end if;
  declare
    t_name varchar2(100) := 'itt_information';
  begin
    select count(*) into n from user_tables t where t.table_name = upper(t_name);
    if n = 0
    then
      execute immediate 'create table itt_information(info_id integer not null
                                  ,info_type varchar2(128) not null
                                  ,mail_group varchar2(128) not null
                                  ,create_sysdate date default sysdate not null
                                  ,info_title varchar2(128)
                                  ,info_content clob
                                  ,senddt date)';
      execute immediate 'comment on table itt_information is ''Cобщения для обработки ''';
      execute immediate 'comment on column itt_information.info_type is ''Тип сообщения''';
      execute immediate 'comment on column itt_information.mail_group is ''Группа получателей сообщения''';
      execute immediate 'comment on column itt_information.create_sysdate is ''Время создания сообщения''';
      execute immediate 'comment on column itt_information.info_title is ''Заголовок сообщения''';
      execute immediate 'comment on column itt_information.info_content is ''Текст сообщения''';
      execute immediate 'comment on column itt_information.senddt is ''Время обработки (отправки получателям)''';
      execute immediate 'alter table itt_information add constraint ITI_information_PK primary key(info_id)  using index tablespace INDX';
    end if;
  end;
  declare
    t_name varchar2(100) := 'itt_q_service';
  begin
    select count(*) into n from user_tables t where t.table_name = upper(t_name);
    if n = 0
    then
      execute immediate 'create table itt_q_service
  (
    service_id          INTEGER not null,
    message_type        CHAR(1) not null,
    servicename         VARCHAR2(128) not null,
    subscription        INTEGER default 0 not null,  
    service_proc        VARCHAR2(64) not null,
    service_rollback    VARCHAR2(64) ,
    service_caption     VARCHAR2(200) not null,
    stop_apostq         INTEGER default 1    not null,
    service_price       INTEGER default 1000 not null,
    max_running         INTEGER,
    close_sysdate       date,
    close_info          VARCHAR2(200), 
    calc_stat_c         INTEGER default 0 not null,
    calc_stat_p         INTEGER default 0 not null,
    create_sysdate      date,
    update_time         timestamp
  )';
      execute immediate 'comment on table itt_q_service
    is ''Настройки обработчиков бизнес-процессов ''';
      execute immediate 'comment on column itt_q_service.message_type
    is ''Обработка R-запросов A-ответов''';
      execute immediate 'comment on column itt_q_service.servicename
    is ''бизнес-процесс (не использовать символы # и * )''';
      execute immediate 'comment on column itt_q_service.service_proc
    is ''Процудура наката ''';
      execute immediate 'comment on column itt_q_service.service_rollback
    is ''Процедура отката''';
      execute immediate 'comment on column itt_q_service.service_caption
    is ''Описание бизнес-процесса''';
      execute immediate 'comment on column itt_q_service.service_price
    is ''Цена функции-обработчика (среднее время исполнения в мс)''';
      execute immediate 'comment on column itt_q_service.max_running
    is ''Максимальное время (сек) выполнения после чего процесс будет принудительно остановлен''';
      execute immediate 'comment on column itt_q_service.stop_apostq
    is ''!=0 после отправки на выпонения сервиса посточередь воркера не формируется''';
      execute immediate 'comment on column itt_q_service.calc_stat_c
    is ''Служебное(для расчета статистики) ''';
      execute immediate 'comment on column itt_q_service.calc_stat_p
    is ''Служебное(для расчета статистики)''';
      execute immediate 'comment on column itt_q_service.subscription
    is ''=1 Подписка (ответ не отправляется )''';
      execute immediate 'comment on column itt_q_service.close_sysdate
    is ''Дата исключения из списка сервисов''';
      execute immediate 'comment on column itt_q_service.close_info
    is ''Описание причины закрытия''';

      execute immediate 'alter table itt_q_service add constraint itt_q_service_pk primary key (service_id) using index tablespace INDX';
      execute immediate 'alter table itt_q_service add constraint itt_q_service_msg_type_chk check (message_type in (''R'',''A''))';
      execute immediate 'alter table itt_q_service add constraint itt_q_service_subscription_chk check (subscription in (0,1))';
      execute immediate 'alter table itt_q_service add constraint itt_q_service_stop_apostq_chk check (stop_apostq in (0,1))';
      execute immediate 'alter table itt_q_service add constraint itt_q_service_price_chk check (service_price > 0)';
      execute immediate 'alter table itt_q_service add constraint itt_q_service_max_running_chk check (nvl(max_running,2) > 1)';
      execute immediate 'alter table itt_q_service add constraint itt_q_service_servicename_chk check (instr(servicename,''#'')=0 and  instr(servicename,''*'')=0)';
      execute immediate 'create unique index iti_q_service_uq on ITT_Q_SERVICE (message_type, upper(trim(servicename))) tablespace INDX ';
      execute immediate 'create index iti_q_service_update on ITT_Q_SERVICE(update_time) tablespace INDX';
    end if;
  end;
  declare
    t_name varchar2(100) := 'itt_q_settings';
  begin
    select count(*) into n from user_tables t where t.table_name = upper(t_name);
    if n = 0
    then
      execute immediate 'create table itt_q_settings
  (
    qset_name     VARCHAR2(100) not null,
    value_date    DATE,
    value_number  NUMBER,
    value_varchar VARCHAR2(4000),
    qset_comment  VARCHAR2(4000),
    create_sysdate  DATE,
    update_sysdate  DATE
  )';
      execute immediate 'comment on table itt_q_settings is ''Таблица настроек обработчика очередей''';
      execute immediate 'comment on column itt_q_settings.qset_name is ''Название настройки. $$XXXXX - Служебные (создаются QManagerом)''';
      execute immediate 'comment on column itt_q_settings.value_date is ''Значение настройки типа DATA''';
      execute immediate 'comment on column itt_q_settings.value_number is ''Числовое значение настройки''';
      execute immediate 'comment on column itt_q_settings.value_varchar is ''Текстовое значение настройки''';
      execute immediate 'comment on column itt_q_settings.qset_comment is ''Комментарий''';
      execute immediate 'alter table itt_q_settings  add constraint itt_q_settings_PK primary key (QSET_NAME) using index tablespace INDX';
    end if;
  end;
  declare
    t_name varchar2(100) := 'itt_q_worker';
  begin
    select count(*) into n from user_tables t where t.table_name = upper(t_name);
    if n = 0
    then
      execute immediate 'create table itt_q_worker(
                                worker_num integer not null
                               ,worker_priority char(1) default ''N'' not null
                               ,pipe_channel char(2) 
                               ,job_starttime date
                               ,job_stoptime date
                               ,worker_enabled integer default 0 not null
                               ,worker_free integer default 0 not null
                               ,run_coins integer default 0 not null
                               ,run_count integer default 0 not null
                               ,service_delivery char(1)
                               ,servicename varchar2(128)
                               ,stop_apostq integer default 0 not null
                               ,queue_num char(2)
                               ,servicegroup varchar2(128)
                               ,run_starttime timestamp(6)
                               ,run_lasttime timestamp(6)
                               ,response_last char(1)
                               ,response_lasttime timestamp(6)
                               ,run_stoptime timestamp(6))';
      execute immediate 'comment on table itt_q_worker is ''Информация об обработчиках заданий''';
      execute immediate 'comment on column itt_q_worker.worker_num is ''Номер обработчика''';
      execute immediate 'comment on column itt_q_worker.pipe_channel is ''Pipe канал''';
      execute immediate 'comment on column itt_q_worker.worker_priority is ''(F/N)''';
      execute immediate 'comment on column itt_q_worker.job_starttime is ''Время старта  job''';
      execute immediate 'comment on column itt_q_worker.job_stoptime is ''Время остановки  job''';
      execute immediate 'comment on column itt_q_worker.worker_enabled is ''Доступен для заданий''';
      execute immediate 'comment on column itt_q_worker.worker_free is ''Обработчик свободен ''';
      execute immediate 'comment on column itt_q_worker.run_coins is ''Оценка очереди заданий''';
      execute immediate 'comment on column itt_q_worker.run_count is ''Длина очереди заданий''';
      execute immediate 'comment on column itt_q_worker.service_delivery is ''Тип сообщения S/A последнего выданного задания''';
      execute immediate 'comment on column itt_q_worker.servicename is ''Последнее выданное задание''';
      execute immediate 'comment on column itt_q_worker.stop_apostq is ''Остановка посточереди после последнего задание''';
      execute immediate 'comment on column itt_q_worker.queue_num is ''Поток обработки в очереди''';
      execute immediate 'comment on column itt_q_worker.servicegroup is ''Поток обработки''';
      execute immediate 'comment on column itt_q_worker.run_starttime is ''Вермя старта очереди заданий''';
      execute immediate 'comment on column itt_q_worker.run_lasttime is ''Последний старт задания''';
      execute immediate 'comment on column itt_q_worker.response_last is ''Последнее сообщение обработчика''';
      execute immediate 'comment on column itt_q_worker.response_lasttime is ''Время последнего отклика обработчика''';
      execute immediate 'comment on column itt_q_worker.run_stoptime is ''Задание будет принудительно остановлено. ''';
      execute immediate 'alter table itt_q_worker add constraint iti_q_worker_pk primary key(worker_num) using index tablespace INDX';
    end if;
  end;
  declare
    t_name varchar2(100) := 'itt_q_message_log';
  begin
    select count(*) into n from user_tables t where t.table_name = upper(t_name);
    if n = 0
    then
      execute immediate 'create table itt_q_message_log
  (
    log_id        number not null,
    queuetype     varchar2(3) not null,
    msgid         varchar2(128) not null,
    message_type  char(1),
    delivery_type char(1),
    priority      char(1),
    correlation   varchar2(128),
    corrmsgid     varchar2(128),
    servicename   varchar2(128),
    sender        varchar2(128),
    senderuser    varchar2(128),
    receiver      varchar2(128),
    servicegroup  varchar2(128),
    btuid         varchar2(128),
    uteg          char(1),
    msgcode       number,
    msgtext       varchar2(2000),
    messbody      clob,
    messmeta      clob,
    queue_num     char(2),
    queuename     varchar2(32) not null,
    status        varchar2(32),
    statusdt      timestamp(6),
    requestdt     timestamp(6),
    esbdt         timestamp(6),
    enqdt         timestamp(6) not null,
    startdt       timestamp(6),
    commanddt     timestamp(6),
    workername    varchar2(32),
    workdt        timestamp(6),
    commenttxt    varchar2(4000)
  )';
      execute immediate 'comment on table itt_q_message_log is ''Логирование сообщений очередей''';
      execute immediate 'comment on column itt_q_message_log.log_id is ''ID''';
      execute immediate 'comment on column itt_q_message_log.queuename is ''Наименование очереди''';
      execute immediate 'comment on column itt_q_message_log.queuetype is ''Тип очереди IN / OUT''';
      execute immediate 'comment on column itt_q_message_log.msgid is ''GUID сообщения ''';
      execute immediate 'comment on column itt_q_message_log.message_type is ''Тип сообщения R или A Запрос/ответ''';
      execute immediate 'comment on column itt_q_message_log.delivery_type is ''Вид доставки S или A Синхронный / Асинхронный''';
      execute immediate 'comment on column itt_q_message_log.priority is ''Очередность синхронных сообщений ( F- быстрая,N- медленная)''';
      execute immediate 'comment on column itt_q_message_log.correlation is ''Correlation сообщения ''';
      execute immediate 'comment on column itt_q_message_log.corrmsgid is ''GUID связанного сообщения ''';
      execute immediate 'comment on column itt_q_message_log.servicename is ''Бизнес-процесс''';
      execute immediate 'comment on column itt_q_message_log.sender is ''Система-источник''';
      execute immediate 'comment on column itt_q_message_log.receiver is ''Система-получатель''';
      execute immediate 'comment on column itt_q_message_log.requestdt is ''Время отправления сообщения из системы источник''';
      execute immediate 'comment on column itt_q_message_log.esbdt is ''Время начала обработки транспортной системой''';
      execute immediate 'comment on column itt_q_message_log.senderuser is ''Пользователь ''';
      execute immediate 'comment on column itt_q_message_log.servicegroup is ''Поток исполнения бизнес-процесса''';
      execute immediate 'comment on column itt_q_message_log.btuid is ''ID бизнес транзакции, зарезервировано''';
      execute immediate 'comment on column itt_q_message_log.uteg is ''Ю''';
      execute immediate 'comment on column itt_q_message_log.msgcode is ''Код результата обработки сообщения. 0 - успех''';
      execute immediate 'comment on column itt_q_message_log.msgtext is ''Текст ошибки, возникший при обработке сообщения-запроса''';
      execute immediate 'comment on column itt_q_message_log.messbody is ''Бизнес-составляющая сообщения''';
      execute immediate 'comment on column itt_q_message_log.messmeta is ''XML Метаданные сообщения''';
      execute immediate 'comment on column itt_q_message_log.queue_num is ''ID очереди (null - определяется системой)''';
      execute immediate 'comment on column itt_q_message_log.status is ''Коды статусов, например: SEND,TIMEOUT и т.п.''';
      execute immediate 'comment on column itt_q_message_log.statusdt is ''Время изменения статуса''';
      execute immediate 'comment on column itt_q_message_log.enqdt is ''Время размещения сообщения в очередь''';
      execute immediate 'comment on column itt_q_message_log.startdt is ''Время обнаружения сообщения QManager''';
      execute immediate 'comment on column itt_q_message_log.Commanddt is ''Время отправки сообщения обработчику''';
      execute immediate 'comment on column itt_q_message_log.workdt is ''Время начала обработки''';
      execute immediate 'comment on column itt_q_message_log.workername is ''Имя работника''';
      execute immediate 'comment on column itt_q_message_log.commenttxt is ''Примечание к сообщению''';
      execute immediate 'create index iti_q_message_log_corrmsgid on itt_q_message_log(corrmsgid) tablespace INDX';
      execute immediate 'create index iti_q_message_log_msgid on itt_q_message_log(msgid) tablespace INDX';
      execute immediate 'create index iti_q_message_log_enqdt on itt_q_message_log(enqdt) tablespace INDX';
      execute immediate 'create unique index iti_q_message_log_uq on itt_q_message_log(case status when ''TRASH'' then null else msgid end, case status when ''TRASH'' then null else queuetype end) tablespace INDX';
      execute immediate 'create unique index iti_q_message_log_pk on itt_q_message_log(log_id) reverse tablespace INDX';
    end if;
  end;
  declare
    seq_name varchar2(100) := 'its_q_log';
  begin
    select count(*) into n from user_sequences t where t.sequence_name = upper(seq_name);
    if n = 0
    then
      execute immediate 'create sequence its_q_log minvalue 1 maxvalue 9999999999999999999999999 start with 1 increment by 1 cache 20 cycle ';
    end if;
  end;
  declare
    t_name varchar2(100) := 'itt_q_corrsystem';
  begin
    select count(*) into n from user_tables t where t.table_name = upper(t_name);
    if n = 0
    then
      execute immediate 'create table itt_q_corrsystem
  (
    system_name           varchar2(128) not null,
    system_caption        varchar2(200) not null,
    out_pack_message_proc varchar2(64),
    in_check_message_func varchar2(64),
    create_sysdate        date,
    update_sysdate        date
  )';
      execute immediate 'comment on table itt_q_corrsystem is ''Справочник систем-корреспондентов ''';
      execute immediate 'comment on column itt_q_corrsystem.system_name is ''Система-корреспондент''';
      execute immediate 'comment on column itt_q_corrsystem.system_caption is ''Наименование''';
      execute immediate 'comment on column itt_q_corrsystem.out_pack_message_proc is ''Процедура-упаковщик исходящего сообщения''';
      execute immediate 'comment on column itt_q_corrsystem.in_check_message_func  is ''функция проверки входящего сообщения''';
      execute immediate 'alter table itt_q_corrsystem add constraint iti_q_corrsystem_pk primary key (system_name) using index tablespace INDX';
    end if;
  end;
  declare
    t_name varchar2(100) := 'itt_q_work_messages';
  begin
    select count(*) into n from user_tables t where t.table_name = upper(t_name);
    if n = 0
    then
      execute immediate 'create table itt_q_work_messages
  ( queue_num     char(2) not null,
    qmsgid        raw(16) not null,
    service_delivery char(1),
    servicename varchar2(128),
    servicegroup  varchar2(128),
    service_price number default 5 not null,
    create_time   timestamp(6) default systimestamp not null,
    enqdt         timestamp(6) not null,
    startdt       timestamp(6) not null,
    work_ready    timestamp(6),
    work_errno    number default 0 ,
    work_errmess  varchar2(2000),
    worker_num    integer not null)';
      execute immediate 'comment on table itt_q_work_messages is ''Сообщения для вычитывания работниками''';
      execute immediate 'comment on column itt_q_work_messages.queue_num is ''ID очереди''';
      execute immediate 'comment on column itt_q_work_messages.qmsgid is ''msgid сообщения из очереди''';
      execute immediate 'comment on column itt_q_work_messages.service_delivery is ''Тип сообщения S/A задания''';
      execute immediate 'comment on column itt_q_work_messages.servicename is ''Бизнеспроцесс''';
      execute immediate 'comment on column itt_q_work_messages.service_price is ''Ожидаемая цена выполнения (mc)''';
      execute immediate 'comment on column itt_q_work_messages.servicegroup is ''Поток обработки''';
      execute immediate 'comment on column itt_q_work_messages.create_time is ''Время отправки работнику''';
      execute immediate 'comment on column itt_q_work_messages.enqdt is ''Время появления в очереди ''';
      execute immediate 'comment on column itt_q_work_messages.startdt is ''Время обнаружения QManager ''';
      execute immediate 'comment on column itt_q_work_messages.work_ready is ''Начало обработки сообщения''';
      execute immediate 'comment on column itt_q_work_messages.work_errno is ''Ошибка обработки сообщения''';
      execute immediate 'comment on column itt_q_work_messages.work_errmess is ''Текст ошибки обработки сообщения''';
      execute immediate 'comment on column itt_q_work_messages.worker_num is ''Номер обработчика''';

    end if;
  end;

end;
/
