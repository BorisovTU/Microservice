declare
  e_field_exists exception;
  pragma exception_init( e_field_exists, -1430);
begin
 
execute immediate 'alter table DQUEUE_LOG_DBT add T_JMSMESSAGEGUID VARCHAR2(256)';
 
EXCEPTION
    WHEN e_field_exists THEN NULL;                       
end;
/