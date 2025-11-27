-- Изменения по DEF-76794
-- Создание пункта меню 'Сверка проводок 24'
-- Расширен список ролей
-- Кроме роли '[28] Аналитик', добавлены роли
-- '[13] Специалист БУ БО'
-- '[14] Специалист БО'
DECLARE
  logID VARCHAR2(32) := 'DEF-76794';
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
    t_object(10013) := '[13] Специалист БУ БО';
    t_object(10014) := '[14] Специалист БО';
    t_object(10028) := '[28] Аналитик';
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
    'Г'             		-- Код подсистемы (Г Главная книга)	
    , 'РСХБ\Интеграция'    	-- путь к пункту меню c разделителем \
    , 'Сверка проводок 24' 	-- Имя пункта.
    , 'Сверка проводок 24' 	-- Описание.
    , 'Сверка проводок 24' 	-- Наименование пользовательского модуля 
    , 'uentcompare_dlg24.mac' 	-- Наименование макроса 
    , 185 			-- порядковый номер строки меню ( если не указан - последний ) 
  );
END;
/
