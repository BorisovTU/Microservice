declare
  l_object      it_rs_interface.tt_object;
begin
  l_object(10010) := '[10] à¨ª« ¤­®©  ¤¬¨­¨áâà â®à';
  l_object(10013) := '[13] ‘¯¥æ¨ «¨áâ “ ';
  l_object(10014) := '[14] ‘¯¥æ¨ «¨áâ ';

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => '‘¤¥«ª¨'
       ,p_menu_item       => '‚­¥¡¨à¦¥¢ë¥ á¤¥«ª¨'
       ,p_menu_nameprompt => '‚­¥¡¨à¦¥¢ë¥ á¤¥«ª¨'
       ,p_iusermodule     => 0
       ,pt_objectid       => l_object
       ,p_inumberline     => null
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => '‘¤¥«ª¨\‚­¥¡¨à¦¥¢ë¥ á¤¥«ª¨'
       ,p_menu_item       => '‡ £àã§ª  § ï¢®ª ¯® § ¬¥é¥­¨î/¢ëªã¯ã –'
       ,p_menu_nameprompt => '‡ £àã§ª  § ï¢®ª ¯® § ¬¥é¥­¨î/¢ëªã¯ã –'
       ,p_usermodule_name => '‡ £àã§ª  § ï¢®ª ¯® § ¬¥é¥­¨î/¢ëªã¯ã –'
       ,p_usermodule_file => 'SecurOTCRequestFileRequestCreatorUI.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 10
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => '‘¤¥«ª¨\‚­¥¡¨à¦¥¢ë¥ á¤¥«ª¨'
       ,p_menu_item       => '‡ £àã§ª  á¤¥«®ª ¯® § ¬¥é¥­¨î/¢ëªã¯ã –'
       ,p_menu_nameprompt => '‡ £àã§ª  á¤¥«®ª ¯® § ¬¥é¥­¨î/¢ëªã¯ã –'
       ,p_usermodule_name => '‡ £àã§ª  á¤¥«®ª ¯® § ¬¥é¥­¨î/¢ëªã¯ã –'
       ,p_usermodule_file => 'SecurOTCRequestFileSaveDealBufferUI.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 20
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => '‘¤¥«ª¨\‚­¥¡¨à¦¥¢ë¥ á¤¥«ª¨'
       ,p_menu_item       => '‘®§¤ ­¨¥ ®â«®¦¥­­ëå á¤¥«®ª'
       ,p_menu_nameprompt => '‘®§¤ ­¨¥ ®â«®¦¥­­ëå á¤¥«®ª'
       ,p_usermodule_name => '‘®§¤ ­¨¥ ®â«®¦¥­­ëå á¤¥«®ª'
       ,p_usermodule_file => 'SecurOTCCreateDealsUI.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 30
    );

end;