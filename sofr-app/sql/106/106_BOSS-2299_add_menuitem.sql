declare
  iusermodule number;
  inumberpoint number;
  t_object      it_rs_interface.tt_object;
  cidentprogram char(1) := 'Г'; 
  menu_path       varchar2(256) := 'РСХБ'; 
  menu_item       varchar2(128) := 'Кафка. Перевыгрузка всех клиентов в Свои инвестиции'; 
  menu_nameprompt varchar2(256) := 'Кафка. Перевыгрузка всех клиентов в Свои инвестиции';
begin
  iusermodule := IT_RS_INTERFACE.get_iusermodule(p_file_mac => 'MassUnloadInvestments.mac' -- Наименование макроса  
                          ,p_cidentprogram => 'Г' 
                           );
  dbms_output.put_line('iusermodule='||to_char(iusermodule));

  if ( iusermodule = 0) then
    iusermodule := IT_RS_INTERFACE.add_usermodule(p_file_mac => 'MassUnloadInvestments.mac',
                                            p_name => menu_item,
                                            p_cidentprogram => 'Г');
  end if;
       
  dbms_output.put_line('iusermodule='||to_char(iusermodule));
  
  delete from dmenuitem_dbt m where 
    m.t_icaseitem = iusermodule
    and m.t_iidentprogram = ascii('Г') 
    and m.t_sznameitem like '%MassUnloadInvestments%';

  t_object(10010) := '[10] Прикладной администратор';

  inumberpoint := it_rs_interface.get_inumberpoint_menu_path(
                    p_menu_path => 'РСХБ\' || menu_item
                    ,p_cidentprogram => 'Г');
  
  dbms_output.put_line('inumberpoint='||to_char(inumberpoint));

  if (inumberpoint = 0) then
    begin 
      it_rs_interface.add_menu_item_oper(p_cidentprogram => cidentprogram
                                    ,p_menu_path => menu_path
                                    ,p_menu_item => menu_item
                                    ,p_menu_nameprompt => menu_nameprompt
                                    ,p_iusermodule  => iusermodule  
                                    ,pt_objectid => t_object);
      commit;
    end;

  end if;

EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE(sqlerrm);
    DBMS_OUTPUT.PUT_LINE(sqlcode);
end;
/
