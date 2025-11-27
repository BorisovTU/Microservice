declare
  n      number;
  t_name varchar2(100) := 'ITT_KAFKA_TOPIC';
begin
  select count(*) into n from user_tables t where t.table_name = upper(t_name);
  if n = 0
  then
    execute immediate 'create table itt_kafka_topic
(
  topic_id            INTEGER not null,
  queuetype           varchar2(3) not null,
  topic_name          VARCHAR2(256) not null,
  topic_caption       VARCHAR2(512) ,
  system_name         VARCHAR2(128) not null,
  servicename         VARCHAR2(128) not null,
  RootElement         VARCHAR2(128) ,
  msg_format          VARCHAR2(8) not null,
  create_sysdate      date ,
  update_time         timestamp
)';
    execute immediate 'comment on table ITT_KAFKA_TOPIC is ''Справочник Topic KAFKA''';
    execute immediate 'comment on column ITT_KAFKA_TOPIC.queuetype is ''Тип очереди IN / OUT''';
    execute immediate 'comment on column ITT_KAFKA_TOPIC.topic_name is ''Наименование топика в Кафка''';
    execute immediate 'comment on column ITT_KAFKA_TOPIC.servicename is ''бизнес-процесс в SOFR''';
    execute immediate 'comment on column ITT_KAFKA_TOPIC.system_name is ''Система-корреспондент''';
    execute immediate 'comment on column ITT_KAFKA_TOPIC.RootElement is ''Если указан то производится контроль RootElementа''';
    execute immediate 'comment on column ITT_KAFKA_TOPIC.msg_format is ''Формат сообщения XML / JSON''';

    execute immediate 'alter table ITT_KAFKA_TOPIC add constraint ITT_KAFKA_TOPIC_queuetype_chk check (queuetype in (''IN'',''OUT''))';
    execute immediate 'create unique index itt_kafka_topic_uq on itt_kafka_topic (queuetype,upper(system_name),upper(servicename)) tablespace INDX ';
  end if;
end;
/
