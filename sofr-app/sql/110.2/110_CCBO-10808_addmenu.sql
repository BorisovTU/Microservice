-- Изменения по CCBO-10808
-- Создание пункта меню 'Отчет о субъектах, блокированных для обмена с CDI'
-- Реализован для ролей
-- '[11] Специалист БУ ЦБ'
-- '[13] Специалист БУ БО'
-- '[14] Специалист БО'
DECLARE
  logID VARCHAR2(32) := 'CCBO-10808';
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
    t_object(10011) := '[11] Специалист БУ ЦБ';
    t_object(10013) := '[13] Специалист БУ БО';
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
  END;
BEGIN
  -- Добавление меню
  AddMenu(
    'Г'             						-- Код подсистемы (Г Главная книга)	
    , 'РСХБ'    						-- путь к пункту меню c разделителем \
    , 'Отчет о субъектах, блокированных для обмена с CDI' 	-- Имя пункта.
    , 'Отчет о субъектах, блокированных для обмена с CDI' 	-- Описание.
    , 'Отчет о субъектах, блокированных для обмена с CDI' 	-- Наименование пользовательского модуля 
    , 'biq10268_BlockReport.mac' 				-- Наименование макроса 
    , 120 							-- порядковый номер строки меню ( если не указан - последний ) 
  );
END;
/
