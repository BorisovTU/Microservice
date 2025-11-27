declare
  iusermodule number;
  t_object      it_rs_interface.tt_object;
  cidentprogram char(1) := 'Г'; 
  menu_path       varchar2(256) := 'РСХБ\Интеграция'; 
  menu_item       varchar2(128) := 'Пересозд операций ЦБ pkowriteoff'; 
  menu_nameprompt varchar2(256) := 'Пересоздание списаний зачислений ЦБ pkowriteoff';

begin
  iusermodule := IT_RS_INTERFACE.add_usermodule(p_file_mac => 'pkowritescroll.mac',
                                            p_name => 'Пересозд спис зачисл ЦБ pkowriteoff',
                                            p_cidentprogram => 'Г');
   t_object(10010) := '[10] Прикладной администратор';
  it_rs_interface.add_menu_item_oper(p_cidentprogram => cidentprogram
                                    ,p_menu_path => menu_path
                                    ,p_menu_item => menu_item
                                    ,p_menu_nameprompt => menu_nameprompt
                                    ,p_iusermodule  => iusermodule  
                                    ,pt_objectid => t_object);

end;
