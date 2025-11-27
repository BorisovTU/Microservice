declare
  n      number;
  t_name varchar2(100) := 'ITT_KAFKA_TOPIC';
  c_name varchar2(100) := 'msg_param';
begin
   select count(*) into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
   if n = 0 then
     execute immediate 'alter table ITT_KAFKA_TOPIC add msg_param VARCHAR2(256)';
     execute immediate 'comment on column itt_q_message_log.workuser  is ''Параметры сообщения (Для XML пространство имен)''';
   end if;
end;
