declare
  l_object      it_rs_interface.tt_object;
begin
  l_object(10010) := '[10] à¨ª« ¤­®©  ¤¬¨­¨áâà â®à';
  l_object(10002) := '[02] ‘¯¥æ¨ «¨áâ „Š ¯® ®¯¥à æ¨ï¬ á ¢¥ªá¥«ï¬¨';
  l_object(10003) := '[03] ‘¯¥æ¨ «¨áâ ¯® ®ä®à¬«¥­¨î ®¯¥à æ¨© á ¢¥ªá¥«ï¬¨';
  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'Y'
       ,p_menu_path       => '‘¯à ¢®ç­¨ª¨'
       ,p_menu_item       => 'ˆáâ®à¨ï ¨§¬¥­¥­¨ï ª®¤®¢'
       ,p_menu_nameprompt => 'ˆáâ®à¨ï ¨§¬¥­¥­¨ï ª®¤®¢'
       ,p_usermodule_name => 'ˆáâ®à¨ï ¨§¬¥­¥­¨ï ª®¤®¢'
       ,p_usermodule_file => 'ChBranchHistScroll.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 100
    );
end;
/