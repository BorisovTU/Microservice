declare
  l_object      it_rs_interface.tt_object;
begin

  l_object(10013) := '[13] ‘¯¥æ¨ «¨áâ “ ';
  l_object(10014) := '[14] ‘¯¥æ¨ «¨áâ ';

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => '‘¤¥«ª¨'
       ,p_menu_item       => '“ª § ü844'
       ,p_menu_nameprompt => '“ª § ü844'
       ,p_iusermodule     => 0
       ,pt_objectid       => l_object
       ,p_inumberline     => null
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => '‘¤¥«ª¨\“ª § ü844'
       ,p_menu_item       => '“¯à ¢«¥­¨¥ § ï¢ª ¬¨'
       ,p_menu_nameprompt => '“¯à ¢«¥­¨¥ § ï¢ª ¬¨'
       ,p_iusermodule     => 0
       ,pt_objectid       => l_object
       ,p_inumberline     => null
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => '‘¤¥«ª¨\“ª § ü844\“¯à ¢«¥­¨¥ § ï¢ª ¬¨'
       ,p_menu_item       => 'â¬¥­¨âì § ï¢ª¨ ª«¨¥­â '
       ,p_menu_nameprompt => 'â¬¥­¨âì § ï¢ª¨ ª«¨¥­â '
       ,p_usermodule_name => 'â¬¥­¨âì § ï¢ª¨ ª«¨¥­â '
       ,p_usermodule_file => '844_reject_reqs.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 10
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => '‘¤¥«ª¨\“ª § ü844'
       ,p_menu_item       => '‡ £àã§ª  ä ©«  á® á¤¥«ª ¬¨'
       ,p_menu_nameprompt => '‡ £àã§ª  ä ©«  á® á¤¥«ª ¬¨'
       ,p_usermodule_name => '‡ £àã§ª  ä ©«  á® á¤¥«ª ¬¨'
       ,p_usermodule_file => 'xls_imp844.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 10
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => '‘¤¥«ª¨\“ª § ü844'
       ,p_menu_item       => '‘®§¤ ­¨¥ ®â«®¦¥­­ëå á¤¥«®ª ( á¨­åà®­­®)'
       ,p_menu_nameprompt => '‘®§¤ ­¨¥ ®â«®¦¥­­ëå á¤¥«®ª ( á¨­åà®­­®)'
       ,p_usermodule_name => '‘®§¤ ­¨¥ ®â«®¦¥­­ëå á¤¥«®ª ( á¨­åà®­­®)'
       ,p_usermodule_file => '844_create_deals_ui.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 20
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => '‘¤¥«ª¨\“ª § ü844'
       ,p_menu_item       => 'Š¢¨â®¢ª  á¤¥«®ª á § ï¢ª ¬¨-¯®àãç¥­¨ï¬¨'
       ,p_menu_nameprompt => 'Š¢¨â®¢ª  á¤¥«®ª á § ï¢ª ¬¨-¯®àãç¥­¨ï¬¨'
       ,p_usermodule_name => 'Š¢¨â®¢ª  á¤¥«®ª á § ï¢ª ¬¨-¯®àãç¥­¨ï¬¨'
       ,p_usermodule_file => '844_match_deals_w_orders.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 30
    );

  it_rs_interface.add_menu_item_oper (
         p_cidentprogram   => 'S'
       , p_menu_path       => '‘¤¥«ª¨\“ª § ü844'
       , p_menu_item       => 'ˆá¯®«­¥­¨¥ á¤¥«®ª ( á¨­åà®­­®)'
       , p_menu_nameprompt => 'ˆá¯®«­¥­¨¥ á¤¥«®ª ( á¨­åà®­­®)'
       , p_usermodule_name => 'ˆá¯®«­¥­¨¥ á¤¥«®ª ( á¨­åà®­­®)'
       , p_usermodule_file => '844_run_otc_deals_ui.mac'
       , pt_objectid       => l_object
       , p_inumberline     => 40
    );

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => '‘¤¥«ª¨\“ª § ü844'
       ,p_menu_item       => '†ãà­ « § £àã§ª¨'
       ,p_menu_nameprompt => '†ãà­ « § £àã§ª¨'
       ,p_usermodule_name => '†ãà­ « § £àã§ª¨'
       ,p_usermodule_file => '844_report.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 50
    );
end;