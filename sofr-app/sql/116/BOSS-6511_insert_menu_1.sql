declare
  l_object      it_rs_interface.tt_object;
begin
  l_object(10036) := '[36] â¢¥âáâ¢¥­­ë© á®âàã¤­¨ª „”‹';

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => ' «®£®¢ë© ãç¥â\„”‹'
       ,p_menu_item       => '‘¥à¢¨á­ë¥ ®¯¥à æ¨¨ ãà¥£ã«¨à®¢ ­¨ï ®áâ âª®¢'
       ,p_menu_nameprompt => '‘¥à¢¨á­ë¥ ®¯¥à æ¨¨ ãà¥£ã«¨à®¢ ­¨ï ®áâ âª®¢'
       ,p_iusermodule     => 0
       ,pt_objectid       => l_object
       ,p_inumberline     => null
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => ' «®£®¢ë© ãç¥â\„”‹\‘¥à¢¨á­ë¥ ®¯¥à æ¨¨ ãà¥£ã«¨à®¢ ­¨ï ®áâ âª®¢'
       ,p_menu_item       => '“à¥£. ®áâ âª®¢ ­  áç¥â å „”‹ ª ¯¥à¥ç¨á«¥­¨î/¢®§¢à âã'
       ,p_menu_nameprompt => '“à¥£ã«¨à®¢ ­¨¥ ®áâ âª®¢ ­  áç¥â å „”‹ ª ¯¥à¥ç¨á«¥­¨î/¢®§¢à âã'
       ,p_usermodule_name => '“à¥£. ®áâ âª®¢ ­  áç¥â å „”‹ ª ¯¥à¥ç¨á«¥­¨î/¢®§¢à âã'
       ,p_usermodule_file => 'nptxadjeven_munerun.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 20
    );
end;