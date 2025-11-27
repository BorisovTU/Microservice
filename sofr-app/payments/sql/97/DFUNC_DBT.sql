DECLARE
    e_field_not_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT( e_field_not_exists, -904);
begin    
  execute immediate 'alter table dfunc_dbt drop (t_version)';
exception
    WHEN e_field_not_exists THEN NULL;
end;
/

declare
  e_field_exists exception;
  pragma exception_init( e_field_exists, -1430);
begin
  execute immediate 'alter table dfunc_dbt add versionrec INTEGER';
  execute immediate 'comment on column dfunc_dbt.versionrec is ''Версия записи''';
exception
  when e_field_exists then null;
end;
/
