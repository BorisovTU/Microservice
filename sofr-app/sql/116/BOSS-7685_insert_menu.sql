declare
  l_object      it_rs_interface.tt_object;
begin
  l_object(10036) := '[36] â¢¥âáâ¢¥­­ë© á®âàã¤­¨ª „”‹';

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => ' «®£®¢ë© ãç¥â\„”‹'
       ,p_menu_item       => 'ˆ§¬. ª â. "ˆáª«. ¨§  ¢â®¬ â. ã¤¥à¦. „”‹ ¯® ¨â®£ ¬ £®¤ "'
       ,p_menu_nameprompt => 'ˆ§¬¥­¥­¨¥ ª â¥£®à¨¨ "ˆáª«îç¨âì ¨§  ¢â®¬ â¨ç¥áª®£® ã¤¥à¦ ­¨ï „”‹ ¯® ¨â®£ ¬ £®¤ "'
       ,p_usermodule_name => 'ˆ§¬. ª â. "ˆáª«. ¨§  ¢â®¬ â. ã¤¥à¦. „”‹ ¯® ¨â®£ ¬ £®¤ "'
       ,p_usermodule_file => 'nptxskipcat_munerun.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 105
    );
end;

