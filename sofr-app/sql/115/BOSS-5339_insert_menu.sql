declare
  l_object      it_rs_interface.tt_object;
begin
  l_object(10036) := '[36] â¢¥âáâ¢¥­­ë© á®âàã¤­¨ª „”‹';

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => ' «®£®¢ë© ãç¥â'
       ,p_menu_item       => '‘¯à ¢®ç­¨ª¨'
       ,p_menu_nameprompt => '‘¯à ¢®ç­¨ª¨'
       ,p_iusermodule     => 0
       ,pt_objectid       => l_object
       ,p_inumberline     => null
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => ' «®£®¢ë© ãç¥â\‘¯à ¢®ç­¨ª¨'
       ,p_menu_item       => '„ âë à¥§¥à¢. áã¬¬ „”‹ ª ¯¥à¥ç¨á«.'
       ,p_menu_nameprompt => '„ âë à¥§¥à¢¨à®¢ ­¨ï áã¬¬ „”‹ ª ¯¥à¥ç¨á«¥­¨î'
       ,p_usermodule_name => '„ âë à¥§¥à¢¨à®¢ ­¨ï áã¬¬ „”‹ ª ¯¥à¥ç¨á«¥­¨î'
       ,p_usermodule_file => 'nptxresdates_menurun.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 50
    );
end;