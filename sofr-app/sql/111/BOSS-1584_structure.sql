declare
  l_sq_start integer;

  procedure drop_table_if_exists( p_table_name varchar2) is
    l_cnt number(1);
  begin
    select count(*) into l_cnt from user_tables WHERE upper(table_name) = upper(p_table_name);
    if l_cnt = 1 then
      execute immediate 'DROP TABLE ' || p_table_name;
      it_log.log_handle(p_object => 'drop_table_if_exists',
                        p_msg    => 'table ' || p_table_name || ' dropped');
    end if;
  end drop_table_if_exists;

  procedure create_sq_if_not_exists (
    p_sq_name varchar2,
    p_start   integer
  ) is
    l_cnt number(1);
  begin
    select count(1)
      into l_cnt
      from user_sequences s
     where s.SEQUENCE_NAME = upper(p_sq_name);

    if l_cnt = 0 then
      execute immediate 'create sequence ' || p_sq_name || ' nocache start with ' || to_char(p_start);
      it_log.log_handle(p_object => 'create_sq_if_not_exists',
                        p_msg    => 'sequence ' || p_sq_name || ' created');
    end if;
  end create_sq_if_not_exists;
begin
  select max(t_id) + 10000
    into l_sq_start
    from dnptxop_dbt;

  drop_table_if_exists(p_table_name => 'quik_sent_order_messages');
  create_sq_if_not_exists(p_sq_name => 'quik_sent_order_mess_seq', p_start => l_sq_start);
end;
/

begin
  execute immediate '
    create table quik_sent_order_messages (
      msg_id        number(10),
      order_type    number(2),
      operation_id  number(10),
      msg_guid      varchar2(128),
      create_time   timestamp,
      request_time  timestamp,
      response_time timestamp,
      error_descr   varchar2(4000)
    )';

  execute immediate 'alter table quik_sent_order_messages add constraint pk_qsom_msg_id primary key (msg_id) using index tablespace INDX';

  execute immediate 'create index ind_qsom_operation_id on quik_sent_order_messages (operation_id) tablespace INDX';
  execute immediate 'create index ind_qsom_msg_guid     on quik_sent_order_messages (msg_guid) tablespace INDX';
  
  execute immediate 'comment on table quik_sent_order_messages is ''Неторговые поручения для отправки в QUIK''';
  execute immediate 'comment on column quik_sent_order_messages.order_type is ''0 - списание, 1 - зачисление, 2 - НДФЛ''';
  execute immediate 'comment on column quik_sent_order_messages.msg_guid is ''msgid из таблицы itt_q_message_log''';
end;
/

