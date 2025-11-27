declare
  l_object      it_rs_interface.tt_object;
begin

  l_object(10010) := '[10] à¨ª« ¤­®©  ¤¬¨­¨áâà â®à';
--  l_object(10008) := '[08]  «®£®¢ë© ¬¥­¥¤¦¥à';
  l_object(10026) := '[26] ‘¯¥æ¨ «¨áâ ¯® ä®à¬¨à®¢ ­¨î ®âç¥â­®áâ¨ (æ¥­­ë¥ ¡ã¬ £¨)';

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'							-- Š®¤ ¯®¤á¨áâ¥¬ë (S –¥­­ë¥ ¡ã¬ £¨) 
       ,p_menu_path       => '‚­ãâà¥­­¨© ãç¥â\âç¥âë\€ªâë á¢¥àª¨'
       ,p_menu_item       => ' áâà®¥ç­ ï â ¡«¨æ  ¬¥áâ åà ­¥­¨ï áç¥â®¢ „…'
       ,p_menu_nameprompt => ' áâà®¥ç­ ï â ¡«¨æ  ¬¥áâ åà ­¥­¨ï áç¥â®¢ „…'
       ,p_usermodule_name => ' áâà®¥ç­ ï â ¡«¨æ  ¬¥áâ åà ­¥­¨ï áç¥â®¢ „…'
       ,p_usermodule_file => 'u_depoacc_tradeplace.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 150
    );
end;
/