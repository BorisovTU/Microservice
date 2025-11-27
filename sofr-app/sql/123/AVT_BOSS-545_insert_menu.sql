
-- ‘®§¤ ­¨¥ ¯ã­ªâ  ¬¥­î ¤«ï ¯®«ì§®¢ â¥«¥© ¯® à®«ï¬
declare
  t_object      it_rs_interface.tt_object;
  cidentprogram char(1) := 'S'; 
  menu_path       varchar2(256) := 'âç¥âë\ ¥£« ¬¥­â¨à®¢ ­­ë¥\ ”®à¬ë ®âçñâ®¢, ¯à¥¤áâ ¢«ï¥¬ë¥ ¢  ­ª ®áá¨¨'; -- ¯ãâì ª ¯ã­ªâã ¬¥­î c à §¤¥«¨â¥«¥¬ \
  menu_item       varchar2(128) := '‘¢¥¤¥­¨ï ® ¬ à¦¨­ «ì­ëå á¤¥«ª å ª«¨¥­â®¢ (ä.725)'; -- ˆ¬ï ¯ã­ªâ .
  menu_nameprompt varchar2(256) := '‘¢¥¤¥­¨ï ® ¬ à¦¨­ «ì­ëå á¤¥«ª å ª«¨¥­â®¢ (ä.725)'; -- ¯¨á ­¨¥.
  usermodule_name varchar2(128) := '‘¢¥¤¥­¨ï ® ¬ à¦¨­ «ì­ëå á¤¥«ª å ª«¨¥­â®¢ (ä.725)'; --  ¨¬¥­®¢ ­¨¥ ¯®«ì§®¢ â¥«ìáª®£® ¬®¤ã«ï 
  usermodule_file varchar2(128) := 'dl_report725.mac'; --  ¨¬¥­®¢ ­¨¥ ¬ ªà®á  
begin

 
  t_object(10012) := '[12] ‘¯¥æ¨ «¨áâ –';
  t_object(10014) := '[14] ‘¯¥æ¨ «¨áâ ';

  it_rs_interface.add_menu_item_oper(p_cidentprogram => cidentprogram
                                    ,p_menu_path => menu_path
                                    ,p_menu_item => menu_item
                                    ,p_menu_nameprompt => menu_nameprompt
                                    ,p_usermodule_name => usermodule_name
                                    ,p_usermodule_file => usermodule_file
                                    ,pt_objectid => t_object
                                    ,p_inumberline => 100);

end;
/