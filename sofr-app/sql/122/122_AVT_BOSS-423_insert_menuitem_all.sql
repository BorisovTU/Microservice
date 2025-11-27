declare
  l_object      it_rs_interface.tt_object;
begin

  l_object(10010) := '[10] à¨ª« ¤­®©  ¤¬¨­¨áâà â®à';
  l_object(10011) := '[11] ‘¯¥æ¨ «¨áâ “ –';
  l_object(10012) := '[12] ‘¯¥æ¨ «¨áâ –';
  l_object(10013) := '[13] ‘¯¥æ¨ «¨áâ “ ';
  l_object(10014) := '[14] ‘¯¥æ¨ «¨áâ ';
  l_object(10021) := '[21] €¤¬¨­¨áâà â®à ¨­ä®à¬ æ¨®­­®© ¡¥§®¯ á­®áâ¨';
  l_object(10026) := '[26] ‘¯¥æ¨ «¨áâ ¯® ä®à¬¨à®¢ ­¨î ®âç¥â­®áâ¨ (æ¥­­ë¥ ¡ã¬ £¨)';
  l_object(10027) := '[27] Š®­âà®«ñà „ŠŠ';
  l_object(10027) := '[30] €ã¤¨â®à';
  l_object(10032) := '[32]  ¡®â­¨ª ®â¤¥«  ¯®á«¥¤ãîé¥£® ª®­âà®«ï';
  
  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => ' «®£®¢ë© ãç¥â\„”‹'
       ,p_menu_item       => ' áâà®©ª¨'
       ,p_menu_nameprompt => ' áâà®©ª¨'
       ,p_iusermodule     => 0
       ,pt_objectid       => l_object
       ,p_inumberline     => 210
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => ' «®£®¢ë© ãç¥â\„”‹\ áâà®©ª¨'
       ,p_menu_item       => '˜ ¡«®­ë ª«¨¥­âáª¨å ã¢¥¤®¬«¥­¨©'
       ,p_menu_nameprompt => '˜ ¡«®­ë ª«¨¥­âáª¨å ã¢¥¤®¬«¥­¨©'
       ,p_usermodule_name => '˜ ¡«®­ë ª«¨¥­âáª¨å ã¢¥¤®¬«¥­¨©'
       ,p_usermodule_file => 'scrollnptxmessage.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 10
    );
end;
/