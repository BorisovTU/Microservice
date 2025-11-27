declare
  procedure drop_table_if_exists( p_table_name varchar2) is
    l_cnt number(1);
  begin
    select count(*) into l_cnt from user_tables WHERE upper(table_name) = upper(p_table_name);
    if l_cnt = 1 then
      execute immediate 'DROP TABLE ' || p_table_name;
      it_log.log_handle(p_object => 'create_table',
                        p_msg    => 'table ' || p_table_name || ' dropped');
    end if;
  end drop_table_if_exists;
begin
  drop_table_if_exists(p_table_name => 'd724_metall_details');
end;
/

begin
  execute immediate '
    create global temporary table d724_metall_details (
     t_contr_groupid      varchar2(100),
     t_client_groupid     varchar2(100),
     t_clientcode         varchar2(35),
     t_name               varchar2(320),
     t_parent_sf_id       number(10),
     t_sf_id              number(10),
     t_account            varchar2(20),
     t_rest               number(32, 12),
     t_fiid               number(10),
     t_rest_rub           number(32, 12)
    ) on commit preserve rows';
end;
/