declare
  l_object      it_rs_interface.tt_object;

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
begin
  delete_menu_by_macfile(p_mac_file => 'nptx_money_load_gurnal.mac',
                         p_module   => 'S');

  commit;
end;
/