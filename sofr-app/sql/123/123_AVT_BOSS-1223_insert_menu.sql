declare
  l_object      it_rs_interface.tt_object;
begin
  l_object(10013) := '[13] ‘¯¥æ¨ «¨áâ “ ';
  l_object(10014) := '[14] ‘¯¥æ¨ «¨áâ ';
  l_object(10036) := '[36] â¢¥âáâ¢¥­­ë© á®âàã¤­¨ª „”‹';

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => ' «®£®¢ë© ãç¥â\„”‹'
       ,p_menu_item       => '‡ £àã§ª  â¥ªãé¨å áç¥â®¢ ¤«ï ¢®§¢à â  ­ «®£ '
       ,p_menu_nameprompt => '‡ £àã§ª  â¥ªãé¨å áç¥â®¢ ¤«ï ¢®§¢à â  ­ «®£ '
       ,p_usermodule_name => '‡ £àã§ª  â¥ªãé¨å áç¥â®¢ ¤«ï ¢®§¢à â  ­ «®£ '
       ,p_usermodule_file => 'ParseCftClientSPI.mac '
       ,pt_objectid       => l_object
       ,p_inumberline     => 250
    );
end;
/