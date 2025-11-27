--  Изменение поля 
declare

  column_already_exist EXCEPTION;
  pragma exception_init(column_already_exist, -1430); -- Ошибка ORA-001430

begin

   begin
      execute immediate 'alter table PKO_WRITEOFF add step1_waitstatus integer';
   exception
      when column_already_exist
      then 
        it_log.log('column step1_waitstatus already exist');
   end;

   begin
      execute immediate 'alter table PKO_WRITEOFF add step2_reject integer';
   exception
      when column_already_exist
      then 
        it_log.log('column step2_reject already exist');
   end;

   begin
      execute immediate 'alter table PKO_WRITEOFF add step3_writeoff integer';
   exception
      when column_already_exist
      then 
        it_log.log('column step3_writeoff already exist');
   end;

end;
/
