-- dnptxop_req_dbt => nontrading_orders_buffer
declare
  e_table_not_exists exception;
  pragma exception_init(e_table_not_exists, -942);
begin
  execute immediate 'alter table dnptxop_req_dbt rename to nontrading_orders_buffer';
exception
  when e_table_not_exists then null;
end;
/
