declare
  procedure drop_package_if_exists (
    p_name varchar2
  ) is
    e_pkg_not_exists exception;
    pragma exception_init( e_pkg_not_exists, -04043);
  begin
    execute immediate 'drop package ' || p_name;
    it_log.log_handle(p_object => 'install_script',
                      p_msg    => 'package ' || p_name || ' dropped');
  exception
    when e_pkg_not_exists then null;
  end drop_package_if_exists;
begin
  drop_package_if_exists(p_name => 'nptx_money_ui');
end;
/
