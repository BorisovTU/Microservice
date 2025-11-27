declare
  n      number;
  t_name varchar2(100) := 'itt_q_message_log';
  c_name varchar2(100) := 'workuser';
begin
   select count(*) into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
   if n = 0 then
     execute immediate 'alter table itt_q_message_log add workuser varchar2(128)';
     execute immediate 'comment on column itt_q_message_log.workuser  is ''?ользователь обработавший сообщение''';
   end if;
end;
