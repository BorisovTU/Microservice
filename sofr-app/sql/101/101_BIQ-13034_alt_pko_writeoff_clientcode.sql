--  Изменение поля 
declare
begin
     execute immediate 'alter table PKO_WRITEOFF modify ClientCode VARCHAR2(35)';
end;
/
