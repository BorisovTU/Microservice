create table quik_sent_order_messages (
  msg_id        number(10),
  order_type    number(2),
  operation_id  number(10),
  msg_guid      varchar2(128),
  create_time   timestamp,
  request_time  timestamp,
  response_time timestamp,
  error_descr   varchar2(4000)
);

alter table quik_sent_order_messages add constraint pk_qsom_msg_id primary key (msg_id) using index tablespace INDX;

create index ind_qsom_operation_id on quik_sent_order_messages (operation_id) tablespace INDX;
create index ind_qsom_msg_guid     on quik_sent_order_messages (msg_guid) tablespace INDX;

comment on table quik_sent_order_messages is 'Неторговые поручения для отправки в QUIK';
comment on column quik_sent_order_messages.order_type is '0 - списание, 1 - зачисление, 2 - НДФЛ';
comment on column quik_sent_order_messages.msg_guid is 'msgid из таблицы itt_q_message_log';
