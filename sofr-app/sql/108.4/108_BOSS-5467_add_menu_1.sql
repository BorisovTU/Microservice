-- Изменения по BOSS-5467_BOSS-5508
-- Создание пункта меню 'Сделки/Заявки Указ 677'
DECLARE
  logID VARCHAR2(32) := 'BOSS-5467_BOSS-5508';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Добавление настройки
  PROCEDURE AddMenu( 
    p_IdentProg IN VARCHAR2		-- Код подсистемы
    , p_MenuPath IN VARCHAR2		-- путь к пункту меню c разделителем \
    , p_MenuItem IN VARCHAR2		-- Имя пункта.
    , p_MenuDesc IN VARCHAR2		-- Описание.
    , p_ModuleName IN VARCHAR2 		-- Наименование пользовательского модуля 
    , p_MacrosName IN VARCHAR2          -- Наименование макроса 
    , p_Line IN NUMBER 			-- порядковый номер строки меню ( если не указан - последний ) 
  )
  AS
    t_object      it_rs_interface.tt_object;
  BEGIN
    LogIt('Добавление меню '''||p_IdentProg||''', '||p_MenuItem);
    t_object(10014) := '[14] Специалист БО';
    it_rs_interface.add_menu_item_oper (
       p_cidentprogram => p_IdentProg
       , p_menu_path => p_MenuPath
       , p_menu_item => p_MenuItem
       , p_menu_nameprompt => p_MenuDesc
       , p_usermodule_name => p_ModuleName -- Убрать если есть ID пользовательского модуля
       , p_usermodule_file => p_MacrosName -- Убрать если есть ID пользовательского модуля
       , pt_objectid => t_object
       , p_inumberline => p_Line
    );
    LogIt('Добавлено меню '''||p_IdentProg||''', '||p_MenuItem);
  EXCEPTION
    WHEN OTHERS THEN
       LogIt('Ошибка добавления меню '''||p_IdentProg||''', '||p_MenuItem);
  END;
BEGIN
  -- Добавление меню
  AddMenu(
    'S'             						-- Код подсистемы (S Ценные бумаги)	
    , 'Сделки'    	         				-- путь к пункту меню c разделителем \
    , 'Заявки Указ 677' 					-- Имя пункта.
    , 'Заявки Указ 677' 					-- Описание.
    , 'Заявки Указ 677' 					-- Наименование пользовательского модуля 
    , '677_load_reqs.mac'					-- Наименование макроса 
    , 90 							-- порядковый номер строки меню ( если не указан - последний ) 
  );
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
