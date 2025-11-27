begin
  execute immediate 'alter table d_otcdealtmp_dbt add t_depo_acc_num_dias varchar2(2000)';
exception
  when others then
    it_log.log_error(p_object => 'alter table d_otcdealtmp_dbt add column t_depo_acc_num_dias');
end;
/

begin
  execute immediate 'alter table otc_deals_tmp add t_depo_acc_num_dias varchar2(2000)';
exception
  when others then
    it_log.log_error(p_object => 'alter table otc_deals_tmp add column t_depo_acc_num_dias');
end;
/

begin
  execute immediate 'drop index D_OTCDEALTMP_IDX7';
exception
  when others then
    it_log.log_error(p_object => 'drop index D_OTCDEALTMP_IDX7');
end;
/

begin
  execute immediate 'create unique index D_OTCDEALTMP_IDX7 on d_otcdealtmp_dbt (t_isin, t_depo_acc_num_person, t_depo_acc_num_dias)';
exception
  when others then
    it_log.log_error(p_object => 'create index D_OTCDEALTMP_IDX7');
end;
/
