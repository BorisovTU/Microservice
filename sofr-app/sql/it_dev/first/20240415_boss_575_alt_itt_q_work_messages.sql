declare
  n      number;
  t_name varchar2(100) := 'ITT_Q_WORK_MESSAGES';
  c_name varchar2(100) ;
begin
  c_name := 'msgid';
  select count(*) into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = upper(c_name);
  if n = 0 then
    execute immediate 'alter table ITT_Q_WORK_MESSAGES add msgid varchar2(128)';
    execute immediate 'comment on column itt_q_work_messages.msgid  is ''id сообщения ''';
  end if;
end;
