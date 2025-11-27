
-- Создание пункта меню для пользователей по ролям
declare
  t_object      it_rs_interface.tt_object;
  cidentprogram char(1) := 'S'; 
  menu_path       varchar2(256) := 'Договоры \Договоры брокерского обслуживания'; -- путь к пункту меню c разделителем \
  menu_item       varchar2(128) := 'Еженедельная аналитика по регистрации договоров'; -- Имя пункта.
  menu_nameprompt varchar2(256) := 'Еженедельная аналитика по регистрации договоров'; -- Описание.
  usermodule_name varchar2(128) := 'Еженедельная аналитика по регистрации договоров'; -- Наименование пользовательского модуля 
  usermodule_file varchar2(128) := 'weekly_number_clients_Report.mac'; -- Наименование макроса 
begin

 
  t_object(10010) := '[10] Прикладной администратор';
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