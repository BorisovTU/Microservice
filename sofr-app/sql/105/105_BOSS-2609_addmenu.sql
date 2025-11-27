-- Изменения по BOSS-2609_BOSS-2849
-- Создание пунктов меню 
-- 'Загрузка сделок в рамках Указа №844'
-- 'Исполнение сделок в рамках Указа №844'
DECLARE
  logID VARCHAR2(32) := 'BOSS-2609_BOSS-2849';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Добавление под-меню
  PROCEDURE AddSubMenu( 
    p_IdentProg IN VARCHAR2     -- Код подсистемы
    , p_MenuPath IN VARCHAR2    -- путь к пункту меню c разделителем \
    , p_MenuItem IN VARCHAR2    -- Имя пункта.
    , p_MenuDesc IN VARCHAR2    -- Описание.
    , p_Line IN NUMBER          -- порядковый номер строки меню ( если не указан - последний ) 
  )
  AS
    t_object      it_rs_interface.tt_object;
  BEGIN
    t_object(10014) := '[14] Специалист БО';
    LogIt('Добавление под-меню '''||p_IdentProg||''', '||p_MenuItem);
    it_rs_interface.add_menu_item_oper(
       p_cidentprogram => p_IdentProg
       , p_menu_path => p_MenuPath
       , p_menu_item => p_MenuItem
       , p_menu_nameprompt => p_MenuDesc
       , p_iusermodule => 0
       , pt_objectid => t_object
       , p_inumberline => p_Line
    );
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Добавлено под-меню '''||p_IdentProg||''', '||p_MenuItem);
  EXCEPTION
    WHEN OTHERS THEN
       LogIt('Ошибка добавления под-меню '''||p_IdentProg||''', '||p_MenuItem);
       LogIt('SQLERRM: '||SQLERRM);
       EXECUTE IMMEDIATE 'ROLLBACK';
  END;
  -- Добавление меню
  PROCEDURE AddMenu( 
    p_IdentProg IN VARCHAR2   -- Код подсистемы
    , p_MenuPath IN VARCHAR2    -- путь к пункту меню c разделителем \
    , p_MenuItem IN VARCHAR2    -- Имя пункта.
    , p_MenuDesc IN VARCHAR2    -- Описание.
    , p_ModuleName IN VARCHAR2    -- Наименование пользовательского модуля 
    , p_MacrosName IN VARCHAR2          -- Наименование макроса 
    , p_Line IN NUMBER      -- порядковый номер строки меню ( если не указан - последний ) 
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
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Добавлено меню '''||p_IdentProg||''', '||p_MenuItem);
  EXCEPTION
    WHEN OTHERS THEN
       LogIt('Ошибка добавления меню '''||p_IdentProg||''', '||p_MenuItem);
       LogIt('SQLERRM: '||SQLERRM);
       EXECUTE IMMEDIATE 'ROLLBACK';
  END;
BEGIN
  AddSubMenu(
    'S'                       -- Код подсистемы (S Ценные бумаги) 
    , 'Сделки'              -- путь к пункту меню c разделителем \
    , 'Указ №844'           -- Имя пункта.
    , 'Указ №844'           -- Описание.
    , 80
  );
  AddMenu(
    'S'                       -- Код подсистемы (S Ценные бумаги) 
    , 'Сделки\Указ №844'            -- путь к пункту меню c разделителем \
    , 'Загрузка файла со сделками'     -- Имя пункта.
    , 'Загрузка файла со сделками'     -- Описание.
    , 'Загрузка файла со сделками'     -- Наименование пользовательского модуля 
    , 'xls_imp844.mac'          -- Наименование макроса 
    , 10                -- порядковый номер строки меню ( если не указан - последний ) 
  );

  AddMenu(
    'S'                       -- Код подсистемы (S Ценные бумаги) 
    , 'Сделки\Указ №844'            -- путь к пункту меню c разделителем \
    , 'Создание отложенных сделок (асинхронно)'   -- Имя пункта.
    , 'Создание отложенных сделок (асинхронно)'   -- Описание.
    , 'Создание отложенных сделок (асинхронно)'   -- Наименование пользовательского модуля 
    , '844_create_deals_ui.mac'            -- Наименование макроса 
    , 20                -- порядковый номер строки меню ( если не указан - последний ) 
  );
  
  AddMenu(
    'S'                       -- Код подсистемы (S Ценные бумаги) 
    , 'Сделки\Указ №844'            -- путь к пункту меню c разделителем \
    , 'Квитовка сделок с заявками-поручениями'   -- Имя пункта.
    , 'Квитовка сделок с заявками-поручениями'   -- Описание.
    , 'Квитовка сделок с заявками-поручениями'   -- Наименование пользовательского модуля 
    , '844_match_deals_w_orders.mac'            -- Наименование макроса 
    , 30                -- порядковый номер строки меню ( если не указан - последний ) 
  );
  
  AddMenu(
    'S'                       -- Код подсистемы (S Ценные бумаги) 
    , 'Сделки\Указ №844'            -- путь к пункту меню c разделителем \
    , 'Исполнение сделок'   -- Имя пункта.
    , 'Исполнение сделок'   -- Описание.
    , 'Исполнение сделок'   -- Наименование пользовательского модуля 
    , '844_run_otc_deals.mac'            -- Наименование макроса 
    , 40                -- порядковый номер строки меню ( если не указан - последний ) 
  );
  
  AddMenu(
    'S'                       -- Код подсистемы (S Ценные бумаги) 
    , 'Сделки\Указ №844'            -- путь к пункту меню c разделителем \
    , 'Журнал загрузки'   -- Имя пункта.
    , 'Журнал загрузки'   -- Описание.
    , 'Журнал загрузки'   -- Наименование пользовательского модуля 
    , '844_report.mac'            -- Наименование макроса 
    , 50                -- порядковый номер строки меню ( если не указан - последний ) 
  );

--  AddMenu(
--    'S'                       -- Код подсистемы (S Ценные бумаги) 
--    , 'Сделки\Указ №844'            -- путь к пункту меню c разделителем \
--    , 'Исполнение сделок в рамках Указа №844'   -- Имя пункта.
--    , 'Исполнение сделок в рамках Указа №844'   -- Описание.
--    , 'Исполнение сделок в рамках Указа №844'   -- Наименование пользовательского модуля 
--    , 'run844.mac'            -- Наименование макроса 
--    , 20                -- порядковый номер строки меню ( если не указан - последний ) 
--  );
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/