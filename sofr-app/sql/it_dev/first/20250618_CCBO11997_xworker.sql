declare
  n      number;
  t_name varchar2(100) := 'itt_q_workerx';
begin
  select count(*) into n from user_tables t where t.table_name = upper(t_name);
  if n > 0
  then
    execute immediate 'drop table '||t_name;
  end if;
  execute immediate 'create table '||t_name||'(
        worker_num        INTEGER not null,
        job_starttime     DATE default sysdate not null,
        job_stoptime      DATE,
        message_type      CHAR(1),
        servicename       VARCHAR2(128),
        servicegroup      VARCHAR2(128),
        run_starttime     TIMESTAMP(6),
        run_stoptime      TIMESTAMP(6),
        response_last     CHAR(1) default ''S'' not null,
        response_lasttime TIMESTAMP(6) default systimestamp not null
                             )';
  execute immediate 'comment on table '||t_name||' is ''Информация об обработчиках заданий табличной очереди''';
  execute immediate 'comment on column '||t_name||'.worker_num is ''Номер обработчика''';
  execute immediate 'comment on column '||t_name||'.job_starttime is ''Время старта  job''';
  execute immediate 'comment on column '||t_name||'.job_stoptime is ''Время остановки  job''';
  execute immediate 'comment on column '||t_name||'.message_type is ''Тип сообщения R/A последнего выданного задания''';
  execute immediate 'comment on column '||t_name||'.servicename is ''Последнее выданное задание''';
  execute immediate 'comment on column '||t_name||'.servicegroup is ''Поток обработки''';
  execute immediate 'comment on column '||t_name||'.run_starttime is ''Вермя старта очереди заданий''';
  execute immediate 'comment on column '||t_name||'.run_stoptime is ''Задание будет принудительно остановлено. ''';
  execute immediate 'comment on column '||t_name||'.response_last is ''Последнее сообщение обработчика''';
  execute immediate 'comment on column '||t_name||'.response_lasttime is ''Время последнего отклика обработчика''';
 
  execute immediate 'alter table '||t_name||' add constraint iti_q_workerx_pk primary key(worker_num) using index tablespace INDX';
end;
/
