declare
  l_object      it_rs_interface.tt_object;
begin

  l_object(10010) := '[10] à¨ª« ¤­®©  ¤¬¨­¨áâà â®à';
  --l_object(10013) := '[13] ‘¯¥æ¨ «¨áâ “ ';
  --l_object(10014) := '[14] ‘¯¥æ¨ «¨áâ ';
  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => '‘¥à¢¨á­ë¥ ®¯¥à æ¨¨\®£ è¥­¨ï'
       ,p_menu_item       => 'Š®à¯®à â¨¢­ë¥ ¤¥©áâ¢¨ï'
       ,p_menu_nameprompt => 'Š®à¯®à â¨¢­ë¥ ¤¥©áâ¢¨ï'
       ,p_usermodule_name => 'Š®à¯®à â¨¢­ë¥ ¤¥©áâ¢¨ï'
       ,p_usermodule_file => 'SecurRedemptionsUI.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 150
    );
end;
/