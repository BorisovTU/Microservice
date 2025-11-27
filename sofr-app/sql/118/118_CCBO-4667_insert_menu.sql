-- ‘®§¤ ­¨¥ ¯ã­ªâ  ¬¥­î ¤«ï ¯®«ì§®¢ â¥«¥© ¯® à®«ï¬
declare
  t_object      it_rs_interface.tt_object;
  cidentprogram char(1) := 'S'; 
  menu_path       varchar2(256) := '‘¯à ¢®ç­¨ª¨\–¥­­ë¥ ¡ã¬ £¨'; -- ¯ãâì ª ¯ã­ªâã ¬¥­î c à §¤¥«¨â¥«¥¬ \
  menu_item       varchar2(128) := 'ˆ¬¯®àâ ˆ ¨§ RuData'; -- ˆ¬ï ¯ã­ªâ .
  menu_nameprompt varchar2(256) := 'ˆ¬¯®àâ ˆ ¨§ RuData'; -- ¯¨á ­¨¥.
  usermodule_name varchar2(128) := 'ˆ¬¯®àâ ˆ ¨§ RuData'; --  ¨¬¥­®¢ ­¨¥ ¯®«ì§®¢ â¥«ìáª®£® ¬®¤ã«ï 
  usermodule_file varchar2(128) := 'FaceValueImportAllRuData.mac'; --  ¨¬¥­®¢ ­¨¥ ¬ ªà®á  
begin

 
  t_object(10010) := '[10] à¨ª« ¤­®©  ¤¬¨­¨áâà â®à';
  t_object(10011) := '[11] ‘¯¥æ¨ «¨áâ “ –';
  t_object(10013) := '[13] ‘¯¥æ¨ «¨áâ “ ';
  t_object(10014) := '[14] ‘¯¥æ¨ «¨áâ ';
  t_object(10026) := '[26] ‘¯¥æ¨ «¨áâ ¯® ä®à¬¨à®¢ ­¨î ®âç¥â­®áâ¨ (æ¥­­ë¥ ¡ã¬ £¨)';
  it_rs_interface.add_menu_item_oper(p_cidentprogram => cidentprogram
                                    ,p_menu_path => menu_path
                                    ,p_menu_item => menu_item
                                    ,p_menu_nameprompt => menu_nameprompt
                                    ,p_usermodule_name => usermodule_name
                                    ,p_usermodule_file => usermodule_file
                                    ,pt_objectid => t_object
                                    ,p_inumberline => 121);

end;
/