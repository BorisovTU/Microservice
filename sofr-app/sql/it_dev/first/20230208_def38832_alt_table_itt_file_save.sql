-- Добавление поля в ITT_FILE_SAVE
declare
cnt integer;
begin
select count(*) into cnt from user_tab_columns c where c.TABLE_NAME ='ITT_FILE_SAVE' and c.COLUMN_NAME = 'INTEGRATION_ID' ;
if cnt = 0 then
  execute immediate 'alter table ITT_FILE_SAVE add integration_id VARCHAR2(250)';
  execute immediate 'comment on column ITT_FILE_SAVE.integration_id  is ''╚фхэЄшЇшърЄюЁ ёюс√Єш  IPS''';
  execute immediate 'create index ITI_FILE_SAVE_integration_id on ITT_FILE_SAVE (integration_id)';
end if;
end;
/
