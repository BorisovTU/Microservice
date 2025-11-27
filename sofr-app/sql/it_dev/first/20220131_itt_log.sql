 /**************************************************************************************************\
  Таблица для логирования для всего функционала разработки РСХБ-Интех.
  **************************************************************************************************
  Изменения:
  ---------------------------------------------------------------------------------------------------
  Дата        Автор            Jira                          Описание 
  ----------  ---------------  ---------------------------   ----------------------------------------
  31.01.2022  Зотов Ю.Н.       BIQ-6664 CCBO-506             Создание
 \**************************************************************************************************/

--drop table itt_log;

create table itt_log(id_log         number,
                     object_name    varchar2(100),
                     line           number,
                     create_sysdate date,    
                     msg            varchar2(4000),
                     msg_clob       clob,
                     msg_type       varchar2(5), --'MSG','DEBUG','ERROR'
                     call_stack     clob,
                     error_stack    clob,
                     sid            number,
                     serial         number,
                     user_name      varchar2(32),
                     tran_id        varchar2(32)
                    )
LOB (msg_clob) STORE AS  (DISABLE STORAGE IN ROW)
LOB (call_stack) STORE AS  (DISABLE STORAGE IN ROW) 
LOB (error_stack) STORE AS  (DISABLE STORAGE IN ROW)
;
                    
comment on table itt_log is 'Таблица для логов. Наполняется пакетом it_log.';

comment on column itt_log.id_log is 'Primary Key. Уникальный идентификатор строки.';

comment on column itt_log.object_name is 'Наименование объекта, из которого было вызвано логирование.';

comment on column itt_log.line is 'Номер строки кода, из которой было вызвано логирование.';

comment on column itt_log.create_sysdate is 'Дата и время логирования.';

comment on column itt_log.msg is 'Сообщение, которое было передано в лог (varchar2).';

comment on column itt_log.msg_clob is 'Сообщение, которое было передано в лог (clob).';

comment on column itt_log.msg_type is 'Тип сообщения: DEBUG, MSG, ERROR, ... (см. константы it_log.C_MSG_TYPE__*).';

comment on column itt_log.call_stack is 'Стек вызова.';

comment on column itt_log.error_stack is 'Стек ошибки.';

comment on column itt_log.sid is 'SID сессии.';

comment on column itt_log.serial is 'SERIAL сессии.';

comment on column itt_log.user_name is 'Имя пользователя.';                    

comment on column itt_log.tran_id is 'Идентификатор транзации.';                    
                                      
alter table itt_log add constraint itt_msg_log_pk primary key (id_log);      

create index iti_log__date_object_name on itt_log(create_sysdate, object_name);   

create index iti_log__sid_serial on itt_log(sid, serial);

