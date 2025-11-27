/*DEF-37221 расширение длины столбца*/
begin
    execute immediate 'alter table UQIACCREDITATION_STEP_DBT modify t_comment varchar2(2000)';
end;