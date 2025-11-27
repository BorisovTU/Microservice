-- Добавить колонку t_Technical в таблицы DNPTXOBJ_DBT и DNPTXOBJBC_DBT
declare
   column_already_exist EXCEPTION;
   pragma exception_init(column_already_exist, -1430); -- Ошибка ORA-001430
begin
   begin
      execute immediate 'alter table dnptxobj_dbt add t_Technical CHAR(1)';
   exception
      when column_already_exist
      then NULL;
   end;
   
   begin
      execute immediate 'alter table dnptxobj_tmp add t_Technical CHAR(1)';
   exception
      when column_already_exist
      then NULL;
   end;
   
   execute immediate 'alter table dnptxobjbc_dbt add t_Technical CHAR(1)';
   commit; 
exception
   when column_already_exist
   then NULL;
end;