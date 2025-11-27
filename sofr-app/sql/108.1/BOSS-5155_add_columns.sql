begin
  execute immediate 'alter table D_OTCDEALTMP_DBT add T_PA_QTY_FACT   number(32, 12) default 0';
  execute immediate 'alter table D_OTCDEALTMP_DBT add T_PA_VALUE_FACT number(32, 12) default 0';
  execute immediate 'alter table otc_deals_tmp add T_PA_QTY_FACT number(32, 12)';
  execute immediate 'alter table otc_deals_tmp add T_PA_VALUE_FACT number(32, 12)';
end;
/

declare
  procedure drop_ind_if_exists (
    p_name varchar2
  ) is
    e_ind_not_exists exception;
    pragma exception_init( e_ind_not_exists, -01418);
  begin
    execute immediate 'drop index ' || p_name;
    it_log.log_handle(p_object => 'install_script',
                      p_msg    => 'index ' || p_name || ' dropped');
  exception
    when e_ind_not_exists then null;
  end drop_ind_if_exists;
begin
  drop_ind_if_exists(p_name => 'D_OTCDEALTMP_IDX7');
end;
/