
-- ‘®§¤ ­¨¥ ¯ã­ªâ  ¬¥­î ¤«ï ¯®«ì§®¢ â¥«¥© ¯® à®«ï¬
declare
  t_object      it_rs_interface.tt_object;
  cidentprogram char(1) := 'S'; 
  menu_path       varchar2(256) := '„®£®¢®àë \„®£®¢®àë ¡à®ª¥àáª®£® ®¡á«ã¦¨¢ ­¨ï'; -- ¯ãâì ª ¯ã­ªâã ¬¥­î c à §¤¥«¨â¥«¥¬ \
  menu_item       varchar2(128) := '…¦¥­¥¤¥«ì­ ï  ­ «¨â¨ª  ¯® à¥£¨áâà æ¨¨ ¤®£®¢®à®¢'; -- ˆ¬ï ¯ã­ªâ .
  menu_nameprompt varchar2(256) := '…¦¥­¥¤¥«ì­ ï  ­ «¨â¨ª  ¯® à¥£¨áâà æ¨¨ ¤®£®¢®à®¢'; -- ¯¨á ­¨¥.
  usermodule_name varchar2(128) := '…¦¥­¥¤¥«ì­ ï  ­ «¨â¨ª  ¯® à¥£¨áâà æ¨¨ ¤®£®¢®à®¢'; --  ¨¬¥­®¢ ­¨¥ ¯®«ì§®¢ â¥«ìáª®£® ¬®¤ã«ï 
  usermodule_file varchar2(128) := 'weekly_number_clients_Report.mac'; --  ¨¬¥­®¢ ­¨¥ ¬ ªà®á  
begin

  t_object(10010) := '[10] à¨ª« ¤­®©  ¤¬¨­¨áâà â®à';

  it_rs_interface.add_menu_item_oper(p_cidentprogram => cidentprogram
                                    ,p_menu_path => menu_path
                                    ,p_menu_item => menu_item
                                    ,p_menu_nameprompt => menu_nameprompt
                                    ,p_usermodule_name => usermodule_name
                                    ,p_usermodule_file => usermodule_file
                                    ,pt_objectid => t_object
                                    ,p_inumberline => 121);

  t_object(10014) := '[14] ‘¯¥æ¨ «¨áâ ';

  it_rs_interface.add_menu_item_oper(p_cidentprogram => cidentprogram
                                    ,p_menu_path => menu_path
                                    ,p_menu_item => menu_item
                                    ,p_menu_nameprompt => menu_nameprompt
                                    ,p_usermodule_name => usermodule_name
                                    ,p_usermodule_file => usermodule_file
                                    ,pt_objectid => t_object
                                    ,p_inumberline => 121);

  t_object(10034) := '[34] ‘¯¥æ¨ «¨áâ „Š ¯® ª®­áã«ìâ¨à®¢ ­¨î ª«¨¥­â®¢';

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