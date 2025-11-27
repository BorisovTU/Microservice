declare
  l_object      it_rs_interface.tt_object;
begin
  l_object(10010) := '[10] à¨ª« ¤­®©  ¤¬¨­¨áâà â®à';
  l_object(10013) := '[13] ‘¯¥æ¨ «¨áâ “ ';
  l_object(10014) := '[14] ‘¯¥æ¨ «¨áâ ';

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => '‘¤¥«ª¨\‚­¥¡¨à¦¥¢ë¥ á¤¥«ª¨'
       ,p_menu_item       => 'Š¢¨â®¢ª  á¤¥«®ª á § ï¢ª ¬¨-¯®àãç¥­¨ï¬¨'
       ,p_menu_nameprompt => 'Š¢¨â®¢ª  á¤¥«®ª á § ï¢ª ¬¨-¯®àãç¥­¨ï¬¨'
       ,p_usermodule_name => 'Š¢¨â®¢ª  á¤¥«®ª á § ï¢ª ¬¨-¯®àãç¥­¨ï¬¨'
       ,p_usermodule_file => 'SecurOTCMatchRequestsWDealsUI.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 40
    );
end;