--delete funcobj
begin
  delete from dfuncobj_dbt f where f.t_funcid = 11500;
  delete from dfunc_dbt f where f.t_funcid = 11500;

  delete from dllvalues_dbt v where v.t_list = 5002 and v.t_code = 'run_otc_deal_844';
  delete from dfuncobj_dbt f where f.t_funcid = 11502;
  delete from dfunc_dbt f where f.t_funcid = 11502;
end;
/

declare
  procedure drop_index_if_exists (
    p_name varchar2
  ) is
    e_noexists_index exception;
    pragma exception_init( e_noexists_index, -1418);
  begin
    execute immediate 'drop index ' || p_name;
  exception
    when e_noexists_index then null;
  end drop_index_if_exists;
  
  procedure drop_trigger_if_exists (
    p_name varchar2
  ) is
    e_noexists_trigger exception;
    pragma exception_init( e_noexists_trigger, -4080);
  begin
    execute immediate 'drop trigger ' || p_name;
  exception
    when e_noexists_trigger then null;
  end drop_trigger_if_exists;
begin
  drop_index_if_exists(p_name => 'D_OTCDEALTMP_IDX1');
  drop_index_if_exists(p_name => 'D_OTCDEALTMP_IDX2');
  drop_index_if_exists(p_name => 'D_OTCDEALTMP_IDX3');
  drop_index_if_exists(p_name => 'D_OTCDEALTMP_IDX4');
  drop_index_if_exists(p_name => 'D_OTCDEALTMP_IDX5');
  drop_index_if_exists(p_name => 'D_OTCDEALTMP_IDX6');
  
  drop_trigger_if_exists(p_name => 'D_OTCDEALTMP_DBT_T0_AINC');
end;
/

declare
  e_noexists_object exception;
  pragma exception_init( e_noexists_object, -4043);
begin
  execute immediate 'drop package rshb_844';
exception
  when e_noexists_object then null;
end;
/

begin
  it_rs_interface.release_parm_path(p_parm_path => '‘•\„ˆ…Š’ˆˆ\IMPORT_844_DEALS');
  it_rs_interface.release_parm_path(p_parm_path => 'SECUR\BLOCKED_OTC_DEALS_IMPORT');
end;
/

declare
  procedure drop_table_if_exists (
    p_name varchar2
  ) is
    l_cnt integer;
  begin
    select count(1)
      into l_cnt
      from all_tables t
     where t.TABLE_NAME = upper(p_name);

    if l_cnt = 1 then
      execute immediate 'drop table ' || p_name;
      it_log.log_handle(p_object   => 'DEF-77733_delete_all.drop_table_if_exists',
                        p_msg      => 'table ' || p_name || ' dropped');
    end if;
  end drop_table_if_exists;
begin
  drop_table_if_exists(p_name => 'otc_deals_tmp');
end;
/

--drop sequense
declare
  procedure drop_seq_if_exists (
    p_name varchar2
  ) is
    l_cnt number(1);
  begin
    select count(1)
      into l_cnt
      from user_sequences s
     where s.SEQUENCE_NAME = upper(p_name);

    if l_cnt > 0 then
      execute immediate 'drop sequence '|| p_name;
      it_log.log_handle(p_object   => 'DEF-77733_delete_all.drop_seq_if_exists',
                        p_msg      => 'sequence ' || p_name || ' dropped');
    end if;
  end drop_seq_if_exists;
begin
  drop_seq_if_exists(p_name => 'dl_tick_internal_code_sq'); 

end;
/

--delete menu
declare
  procedure delete_menu_by_macfile (
    p_mac_file   varchar2,
    p_module     char
  ) is
    l_item_id ditemuser_dbt.t_icaseitem%type;
  begin
    l_item_id := it_rs_interface.get_iusermodule(p_file_mac => p_mac_file, p_cidentprogram => p_module);
    
    if l_item_id > 0 then 
      delete from dmenuitem_dbt i
      where i.t_iidentprogram = ascii(p_module)
        and i.t_icaseitem = l_item_id;

      it_rs_interface.release_usermodule(p_iusermodule => l_item_id, p_cidentprogram => p_module);
    end if;
  exception
    when others then
      null;
  end delete_menu_by_macfile;
  
  procedure delete_menu_by_path (
    p_path          varchar2,
    p_inumberfather number
  ) is
  
  begin
    for i in (
              with all_menu as (
                 select substr(sys_connect_by_path(regexp_replace(i.t_sznameitem, '~|^ ', null), '\'), 2) as path
                       ,i.*
                   from dmenuitem_dbt i
                  start with i.t_iprogitem = p_inumberfather
                 connect by prior i.t_inumberpoint = i.t_inumberfather
                        and prior i.t_objectid = i.t_objectid
                        and prior i.t_iprogitem = i.t_iprogitem
                       )
                   select m.*
                     from all_menu m
                    where m.path = p_path)
    loop
      delete dmenuitem_dbt m
       where m.t_objectid = i.t_objectid
         and m.t_iprogitem = i.t_iprogitem
         and m.t_inumberpoint = i.t_inumberpoint;
    end loop;
  end delete_menu_by_path;
begin
  delete_menu_by_macfile(p_mac_file => '844_create_deals_ui.mac',      p_module   => 'S');
  delete_menu_by_macfile(p_mac_file => '844_run_otc_deals_ui.mac',     p_module   => 'S');
  delete_menu_by_macfile(p_mac_file => '844_report.mac',               p_module   => 'S');
  delete_menu_by_macfile(p_mac_file => 'xls_imp844.mac',               p_module   => 'S');
  delete_menu_by_macfile(p_mac_file => '844_match_deals_w_orders.mac', p_module   => 'S');
  delete_menu_by_macfile(p_mac_file => '844_reject_reqs.mac',          p_module   => 'S');

  delete_menu_by_path(p_path => '‘¤¥«ª¨\“ª § ü844\“¯à ¢«¥­¨¥ § ï¢ª ¬¨', p_inumberfather => 83);
  delete_menu_by_path(p_path => '‘¤¥«ª¨\“ª § ü844', p_inumberfather => 83);
end;
/