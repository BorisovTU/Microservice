declare
  l_object      it_rs_interface.tt_object;


  procedure delete_menu_by_macfile (
    p_mac_file   varchar2,
    p_module     char
  ) is
    l_item_id ditemuser_dbt.t_icaseitem%type;
  begin
    l_item_id := it_rs_interface.get_iusermodule(p_file_mac => p_mac_file, p_cidentprogram => p_module);
    
    delete from dmenuitem_dbt i
    where i.t_iidentprogram = ascii(p_module)
      and i.t_icaseitem = l_item_id;

    it_rs_interface.release_usermodule(p_iusermodule => l_item_id, p_cidentprogram => p_module);
  end delete_menu_by_macfile;
begin
  delete_menu_by_macfile(p_mac_file => '844_run_otc_deals.mac',
                         p_module   => 'S');

  l_object(10014) := '[14] Специалист БО';
  it_rs_interface.add_menu_item_oper (
         p_cidentprogram   => 'S'
       , p_menu_path       => 'Сделки\Указ №844'
       , p_menu_item       => 'Исполнение сделок (асинхронно)'
       , p_menu_nameprompt => 'Исполнение сделок (асинхронно)'
       , p_usermodule_name => 'Исполнение сделок (асинхронно)'
       , p_usermodule_file => '844_run_otc_deals_ui.mac'
       , pt_objectid       => l_object
       , p_inumberline     => 40
    );

  commit;
end;
/