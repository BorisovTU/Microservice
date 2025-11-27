-- ‘®§¤ ­¨¥ ¯ã­ªâ  ¬¥­î ¤«ï ¯®«ì§®¢ â¥«¥© ¯® à®«ï¬
declare
  t_object      it_rs_interface.tt_object;
  cidentprogram char(1) := 'Y'; --  Š®¤ ¯®¤á¨áâ¥¬ë Y ‚¥ªá¥«ï ¡ ­ª 

  menu_path       varchar2(256) := 'âç¥âë'; -- ¯ãâì ª ¯ã­ªâã ¬¥­î c à §¤¥«¨â¥«¥¬ \
  menu_item       varchar2(128) := 'âç¥â ¤«ï à áç¥â  —„'; -- ˆ¬ï ¯ã­ªâ .
  menu_nameprompt varchar2(256) := '‚¥ªá¥«ì­ë© ®âç¥â ¤«ï à áç¥â  —„'; -- ¯¨á ­¨¥.
  usermodule_name varchar2(128) := '‚¥ªá¥«ì­ë© ®âç¥â ¤«ï à áç¥â  —„'; --  ¨¬¥­®¢ ­¨¥ ¯®«ì§®¢ â¥«ìáª®£® ¬®¤ã«ï 
  usermodule_file varchar2(128) := 'ChodVekselRep.mac'; --  ¨¬¥­®¢ ­¨¥ ¬ ªà®á  
  -- iusermodule  number :=    ;  -- ID ¯®«ì§®¢ â¥«ìáª®£® ¬®¤ã«ï 
begin
  t_object(10002) := '[02] ‘¯¥æ¨ «¨áâ „Š ¯® ®¯¥à æ¨ï¬ á ¢¥ªá¥«ï¬¨';
  
  it_rs_interface.add_menu_item_oper(p_cidentprogram => cidentprogram
                                    ,p_menu_path => menu_path
                                    ,p_menu_item => menu_item
                                    ,p_menu_nameprompt => menu_nameprompt
                                    ,p_usermodule_name => usermodule_name -- “¡à âì ¥á«¨ ¥áâì ID ¯®«ì§®¢ â¥«ìáª®£® ¬®¤ã«ï
                                    ,p_usermodule_file => usermodule_file -- “¡à âì ¥á«¨ ¥áâì ID ¯®«ì§®¢ â¥«ìáª®£® ¬®¤ã«ï
                                     --                                   ,p_iusermodule  => iusermodule    -- ID ¯®«ì§®¢ â¥«ìáª®£® ¬®¤ã«ï
                                    ,pt_objectid => t_object
                                    ,p_inumberline     => 90);
end;
/