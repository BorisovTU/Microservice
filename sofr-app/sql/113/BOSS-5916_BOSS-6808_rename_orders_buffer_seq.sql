declare
  e_noexists_object exception;
  pragma exception_init( e_noexists_object, -4043);
begin
  execute immediate 'rename dnptxop_req_seq to nontrading_orders_buffer_seq';
exception
  when e_noexists_object then null;
end;
/
