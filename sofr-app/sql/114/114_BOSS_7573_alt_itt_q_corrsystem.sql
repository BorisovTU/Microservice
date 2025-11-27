declare
  n      number;
  t_name varchar2(100) := 'ITT_Q_CORRSYSTEM';
begin
   select count(*)  into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = 'SYSTEM_PARAM';
   if n = 0 then
     execute immediate 'alter table itt_q_corrsystem add system_param varchar2(4000)';
     execute immediate 'comment on column ITT_Q_CORRSYSTEM.system_param is ''Справочные параметры системы ''' ;
   end if;
 end;
/
