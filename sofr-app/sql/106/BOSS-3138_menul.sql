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
  delete_menu_by_macfile(p_mac_file => 'nptx_money_load_from_file_ui.mac',
                         p_module   => 'S');

  l_object(10013) := '[13] Специалист БУ БО';
  l_object(10014) := '[14] Специалист БО';

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => 'Договоры\Договоры брокерского обслуживания'
       ,p_menu_item       => 'Выводы/Переводы ДС'
       ,p_menu_nameprompt => 'Выводы/Переводы ДС'
       ,p_iusermodule     => 0
       ,pt_objectid       => l_object
       ,p_inumberline     => null
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => 'Договоры\Договоры брокерского обслуживания\Выводы/Переводы ДС'
       ,p_menu_item       => 'Загрузка выводов ДС с биржевых счетов'
       ,p_menu_nameprompt => 'Загрузка выводов ДС с биржевых счетов'
       ,p_usermodule_name => 'Загрузка выводов ДС с биржевых счетов'
       ,p_usermodule_file => 'nptx_money_load_exchange_ui.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 10
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => 'Договоры\Договоры брокерского обслуживания\Выводы/Переводы ДС'
       ,p_menu_item       => 'Загрузка выводов ДС с внебиржевых счетов'
       ,p_menu_nameprompt => 'Загрузка выводов ДС с внебиржевых счетов'
       ,p_usermodule_name => 'Загрузка выводов ДС с внебиржевых счетов'
       ,p_usermodule_file => 'nptx_money_load_otc_ui.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 20
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => 'Договоры\Договоры брокерского обслуживания\Выводы/Переводы ДС'
       ,p_menu_item       => 'Загрузка переводов ДС'
       ,p_menu_nameprompt => 'Загрузка переводов ДС'
       ,p_usermodule_name => 'Загрузка переводов ДС'
       ,p_usermodule_file => 'nptx_money_load_transfers_ui.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 30
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => 'Договоры\Договоры брокерского обслуживания\Выводы/Переводы ДС'
       ,p_menu_item       => 'Создание отложенных операций'
       ,p_menu_nameprompt => 'Создание отложенных операций'
       ,p_usermodule_name => 'Создание отложенных операций'
       ,p_usermodule_file => 'nptx_money_create_operations_ui.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 100
    );

  commit;
end;
/