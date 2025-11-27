--добавление нового пункта меню в модуль "Ценные бумаги". Путь: Ценные бумаги => Сервисные операции => Скроллинг операций "Списания ц/б ПКО"
declare
    l_object  it_rs_interface.tt_object; --список ролей которые будут иметь доступ к пункт меню
    cidentprogram char(1) := 'S'; --  Код модуля пользователя в СОФР - S Ценные бумаги

    menu_path varchar2(255) := 'Сервисные операции'; -- путь к пункту меню c разделителем \
    --вариант для обновленного меню ЦБ: menu_path       varchar2(255) := 'Неторговые операции\Неторговые операции с ЦБ; -- путь к пункту меню c разделителем \
    menu_item varchar2(100) := 'Скроллинг операций "Списания ц/б ПКО"'; -- Название пункта
    menu_nameprompt varchar2(255) := 'Скроллинг операций "Списания ц/б ПКО"'; -- Описание.
    usermodule_name varchar2(100) := 'Скроллинг операций "Списания ц/б ПКО"'; -- Наименование пользовательского модуля 
    usermodule_file varchar2(100) := 'nontrading_secur_pkowriteoff_scroll_UI.mac'; -- Наименование макроса который будет вызываться при выборе нового пункта меню

begin
   -- l_object(1) := '[01] Технический пользователь';
    l_object(10010) := '[10] Прикладной администратор';
    l_object(10013) := '[13] Специалист БУ БО';
    l_object(10014) := '[14] Специалист БО';

     it_rs_interface.add_menu_item_oper(p_cidentprogram => cidentprogram
                                    ,p_menu_path => menu_path
                                    ,p_menu_item => menu_item
                                    ,p_menu_nameprompt => menu_nameprompt
                                    ,p_usermodule_name => usermodule_name 
                                    ,p_usermodule_file => usermodule_file 
                                    ,pt_objectid => l_object);
                                    --,p_inumberline     => 90); --порядковый номер строчки где будет размещен пункт, по умолчанию на последней
end;