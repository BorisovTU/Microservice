begin
  execute immediate 'alter table d_otcdealtmp_dbt add error_msg varchar2(2000)';
exception
  when others then
    it_log.log_error(p_object => 'alter table d_otcdealtmp_dbt add column error_msg');
end;
/
