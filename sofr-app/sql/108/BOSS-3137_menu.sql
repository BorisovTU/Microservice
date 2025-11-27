declare
  l_object      it_rs_interface.tt_object;
begin

  l_object(10013) := '[13] Специалист БУ БО';
  l_object(10014) := '[14] Специалист БО';
  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => 'Договоры\Договоры брокерского обслуживания\Выводы/Переводы ДС'
       ,p_menu_item       => 'Буфер неторговых поручений'
       ,p_menu_nameprompt => 'Буфер неторговых порученийв'
       ,p_usermodule_name => 'Буфер неторговых поручений'
       ,p_usermodule_file => 'nptx_money_scroll_ui.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 50
    );
  commit;
end;
/