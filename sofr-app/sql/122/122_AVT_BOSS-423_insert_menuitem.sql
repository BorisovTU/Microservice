declare
  l_object      it_rs_interface.tt_object;
begin
  l_object(10036) := '[36] â¢¥âáâ¢¥­­ë© á®âàã¤­¨ª „”‹';
  
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