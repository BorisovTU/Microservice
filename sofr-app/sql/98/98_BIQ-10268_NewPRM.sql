declare
   column_already_exist EXCEPTION;
   pragma exception_init(column_already_exist, -1430);
begin
   begin
      execute immediate 'ALTER TABLE DPTDUPPRM_DBT ADD (T_KIO CHAR(1))';
   exception
      when column_already_exist
      then NULL;
   end;
   
   begin
      execute immediate 'ALTER TABLE DPTDUPPRM_DBT ADD (T_OKOPF CHAR(1))';
   exception
      when column_already_exist
      then NULL;
   end;
   
   execute immediate 'ALTER TABLE DPTDUPPRM_DBT ADD (T_OKPO CHAR(1))';
   commit; 
exception
   when column_already_exist
   then NULL;
end;