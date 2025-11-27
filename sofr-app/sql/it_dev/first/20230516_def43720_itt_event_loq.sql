declare
  n      number;
  t_name varchar2(100) := 'itt_event_log';
begin
  select count(*) into n from user_tables t where t.table_name = upper(t_name);
  if n = 1
  then
    execute immediate 'drop table ' || t_name;
  end if;
  select count(*)
    into n
    from user_constraints t
   where t.TABLE_NAME = 'ITT_Q_MESSAGE_LOG'
     and t.CONSTRAINT_NAME = 'ITI_Q_MESSAGE_LOG_PK';
  if n = 0
  then
    select count(*) into n from user_indexes where index_name = upper('ITI_Q_MESSAGE_LOG_PK');
    if n = 1
    then
      execute immediate 'drop index ITI_Q_MESSAGE_LOG_PK';
    end if;
    execute immediate 'alter table ITT_Q_MESSAGE_LOG add constraint ITI_Q_MESSAGE_LOG_PK primary key (LOG_ID)';
  end if;
  execute immediate 'create table ' || t_name || '
(
  log_id              number not null,
  create_sysdate      date default sysdate,
  msgid               varchar2(128),
  SystemId            varchar2(128),
  LevelInfo           number,
  reg_status          varchar2(32),
  info_msgid          varchar2(128),
  constraint iti_event_fk foreign key (LOG_ID)
  references itt_q_message_log (LOG_ID) on delete cascade
)';
  execute immediate 'comment on column ITT_EVENT_LOG.log_id  is ''ID EVENT сообщения ''';
  execute immediate 'comment on column ITT_EVENT_LOG.msgid  is ''GUID EVENT сообщения ''';
  execute immediate 'comment on column ITT_EVENT_LOG.SystemId  is ''Ключ системы''';
  execute immediate 'comment on column ITT_EVENT_LOG.LevelInfo  is ''уровень критичности события от 0 - информация до 10 - АВАРИЯ ''';
  execute immediate 'comment on column ITT_EVENT_LOG.reg_status  is ''Статус EVENT сообщения при регистрации ''';
  execute immediate 'comment on column ITT_EVENT_LOG.info_msgid  is ''GUID INFO сообщения ''';
  execute immediate 'create index iti_event_info on ITT_EVENT_LOG(info_msgid) tablespace INDX';
end;
/
