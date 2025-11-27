create or replace package IT_RS_INTERFACE is

  /**
   @file IT_RS_INTERFACE.spc
   @brief Работа с интерфейсом RSBank BIQ-11358
     
   # changeLog
   |date       |author      |tasks                                                     |note                                                        
   |-----------|------------|----------------------------------------------------------|-------------------------------------------------------------
   |2023.08.23 |Зыков М.В.  |CCBO-7119                                                 | СОФР.Автоматизация меню и настроек.               
   |2022.05.12 |Зыков М.В.  |BIQ-11358                                                 | Создание                  
    
  */
  type tt_object is table of varchar2(256) index by pls_integer;
  
  reg_type_int     constant dregparm_dbt.t_type%type := 0;
  reg_type_double  constant dregparm_dbt.t_type%type := 1;
  reg_type_varchar constant dregparm_dbt.t_type%type := 2;
  reg_type_bool    constant dregparm_dbt.t_type%type := 4;

  function varchar_to_blob(p_str varchar2) return blob deterministic;

  function blob_to_varchar(p_blob blob) return varchar2 deterministic;

  ---!!!!! Меню 
  -- Возвращает T_INUMBERPOINT по пути меню. Если путь не найден то 0
  function get_inumberpoint_menu_path(p_menu_path     varchar2 -- Путь пункта меню с разделителем '\'
                                     ,p_cidentprogram char default chr(131) -- Идентификатор приложения (Г - Главная книга)
                                     ,p_objectid      integer default 1 -- Пользователь
                                     ,p_iusermodule   number default 0 -- если 0 - подпункт меню != 0 пункт  
                                     ,p_istemplate    char default null) return dmenuitem_dbt.t_inumberpoint%type;

  -- Вощвращает ID модуля по имени MACфайла если не найден - 0
  function get_iusermodule(p_file_mac      varchar2 -- Наименование макроса  
                          ,p_cidentprogram char default chr(131) -- Символьный мдентификатор приложения (Г - Главная книга) 
                           ) return number;

  -- Регистрирует программный модуль и возвращает ID модуля
  function add_usermodule(p_file_mac      varchar2 -- Наименование макроса 
                         ,p_name          varchar2 -- Наименование модуля 
                         ,p_cidentprogram char default chr(131) -- Символьный мдентификатор приложения (Г - Главная книга) 
                          ) return number;

  -- Удаляет программный модуль 
  procedure release_usermodule(p_iusermodule   number -- ID модуля 
                              ,p_cidentprogram char default chr(131) -- Символьный мдентификатор приложения (Г - Главная книга) 
                               );

  /*
    -- Создание пункта меню в шаблоне для ролей 
  
  objectid  
  1002  [02] Специалист ДРРК по операциям с векселями
  1003  [03] Специалист по оформлению операций с векселями
  1004  [04] Специалист по сопровождению и учёту операций с векселями
  1006  [06] Специалист по оформлению залогов
  1007  [07] Специалист по отчётности Репозитария
  1008  [08] Налоговый менеджер
  1009  [09] Специалист сопровождения для расчета налога ЮЛН
  1010  [10] Прикладной администратор
  1011  [11] Специалист БУ ЦБ
  1012  [12] Специалист ЦБ
  1013  [13] Специалист БУ БО
  1014  [14] Специалист БО
  1015  [15] Специалист БУ ПФИ
  1016  [16] Специалист ПФИ
  1017  [17] Специалист МБК
  1019  [19] Администратор операционного дня по операциям с ценными бум
  1020  [20] Администратор операционного дня по операциям вал.рын.и ПФИ
  1021  [21] Администратор информационной безопасности
  1025  [25] Специалист по формированию отчетности ДО
  1026  [26] Специалист по формированию отчетности (ценные бумаги)
  1027  [27] Контролёр ДКК
  1028  [28] Аналитик
  1029  [29] Просмотр МБК
  1027  [30] Аудитор
  1005  [31] Экономист РФ
  1027  [32] Работник отдела последующего контроля
  1033  [33] Специалист ДРРК по брокерским операциям
  1034  [34] Специалист ДРРК по консультированию клиентов
  1035  [35] Курсы валют для ДПОФР
  */
  procedure add_menu_item_template(p_cidentprogram   char -- Символьный идентификатор приложения (Г - Главная книга и т.д.)
                                  ,p_menu_path       varchar2 -- Путь пункта меню с разделителем '\'
                                  ,p_menu_item       varchar2 -- Название
                                  ,p_menu_nameprompt varchar2 -- Описание 
                                  ,p_usermodule_name varchar2 -- Наименование модуля
                                  ,p_usermodule_file varchar2 -- Наименование макроса
                                  ,pt_objectid       tt_object -- список ролей
                                  ,p_inumberline     number default null -- порядковый номер строки меню ( если не указан - последний ) 
                                   );

  procedure add_menu_item_template(p_cidentprogram   char -- Символьный идентификатор приложения (Г - Главная книга и т.д.)
                                  ,p_menu_path       varchar2 -- Путь пункта меню с разделителем '\'
                                  ,p_menu_item       varchar2 -- Название
                                  ,p_menu_nameprompt varchar2 -- Описание 
                                  ,p_iusermodule     number -- ID пользовательского модуля
                                  ,pt_objectid       tt_object -- список ролей
                                  ,p_inumberline     number default null -- порядковый номер строки меню ( если не указан - последний ) 
                                   );

  /*
    -- Создание пункта меню для оперраторов по ролям 
  
  objectid  
  -- t_object(10002) := '[02] Специалист ДРРК по операциям с векселями';
  -- t_object(10003) := '[03] Специалист по оформлению операций с векселями';
  -- t_object(10004) := '[04] Специалист по сопровождению и учёту операций с векселями';
  -- t_object(10006) := '[06] Специалист по оформлению залогов';
  -- t_object(10007) := '[07] Специалист по отчётности Репозитария';
  -- t_object(10008) := '[08] Налоговый менеджер';
  -- t_object(10009) := '[09] Специалист сопровождения для расчета налога ЮЛН';
  -- t_object(10010) := '[10] Прикладной администратор';
  -- t_object(10011) := '[11] Специалист БУ ЦБ';
  -- t_object(10012) := '[12] Специалист ЦБ';
  -- t_object(10013) := '[13] Специалист БУ БО';
  -- t_object(10014) := '[14] Специалист БО';
  -- t_object(10015) := '[15] Специалист БУ ПФИ';
  -- t_object(10016) := '[16] Специалист ПФИ';
  -- t_object(10017) := '[17] Специалист МБК';
  -- t_object(10019) := '[19] Администратор операционного дня по операциям с ценными бум';
  -- t_object(10020) := '[20] Администратор операционного дня по операциям вал.рын.и ПФИ';
  -- t_object(10021) := '[21] Администратор информационной безопасности';
  -- t_object(10025) := '[25] Специалист по формированию отчетности ДО';
  -- t_object(10026) := '[26] Специалист по формированию отчетности (ценные бумаги)';
  -- t_object(10027) := '[27] Контролёр ДКК';
  -- t_object(10028) := '[28] Аналитик';
  -- t_object(10029) := '[29] Просмотр МБК';
  -- t_object(10027) := '[30] Аудитор';
  -- t_object(10005) := '[31] Экономист РФ';
  -- t_object(10032) := '[32] Работник отдела последующего контроля';
  -- t_object(10033) := '[33] Специалист ДРРК по брокерским операциям';
  -- t_object(10034) := '[34] Специалист ДРРК по консультированию клиентов';
  -- t_object(10035) := '[35] Курсы валют для ДПОФР';
  */
  procedure add_menu_item_oper(p_cidentprogram   char -- Символьный идентификатор приложения (Г - Главная книга и т.д.)
                              ,p_menu_path       varchar2 -- Путь пункта меню с разделителем '\'
                              ,p_menu_item       varchar2 -- Название
                              ,p_menu_nameprompt varchar2 -- Описание 
                              ,p_usermodule_name varchar2 -- Наименование модуля
                              ,p_usermodule_file varchar2 -- Наименование макроса
                              ,pt_objectid       tt_object -- список ролей
                              ,p_inumberline     number default null -- порядковый номер строки меню ( если не указан - последний ) 
                               );

  procedure add_menu_item_oper(p_cidentprogram   char -- Символьный идентификатор приложения (Г - Главная книга и т.д.)
                              ,p_menu_path       varchar2 -- Путь пункта меню с разделителем '\'
                              ,p_menu_item       varchar2 -- Название
                              ,p_menu_nameprompt varchar2 -- Описание 
                              ,p_iusermodule     number -- ID пользовательского модуля
                              ,pt_objectid       tt_object -- список ролей
                              ,p_inumberline     number default null -- порядковый номер строки меню ( если не указан - последний ) 
                               );

  ---!!!!! Настройки  
  -- Возвращает T_KEYID по пути настроек. Если путь не найден то 0
  function get_keyid_parm_path(p_parm_path varchar2 -- Путь настройки с разделителем '\
                               ) return dregparm_dbt.t_keyid%type;

  -- Добавление(изменение типа) параметра по пути . Возвращает KEYID. Если путь не найден то создается .
  function add_parm_path(p_parm_path   varchar2 -- Путь настройки с разделителем '\
                        ,p_type        dregparm_dbt.t_type%type -- 0 - integer/ 1-Double/ 2- String /4 - FLAG
                        ,p_description dregparm_dbt.t_description%type default null -- Примечание
                         ) return dregparm_dbt.t_keyid%type;

  -- Удаление параметра
  procedure release_parm_path(p_parm_path varchar2 -- Путь настройки с разделителем '\
                              );

  -- Удаление параметра
  procedure release_parm(p_keyid dregparm_dbt.t_keyid%type);

  -- Запись значения параметра
  procedure set_parm(p_keyid dregparm_dbt.t_keyid%type
                    ,p_parm  number);

  procedure set_parm(p_keyid dregparm_dbt.t_keyid%type
                    ,p_parm  varchar2);

  -- Считывание параметра типа number 
  function get_parm_number_path(p_parm_path varchar2 -- Путь настройки с разделителем '\
                                ) return number;

  function get_parm_number(p_keyid dregparm_dbt.t_keyid%type) return number;

  -- Считывание параметра типа varchar 
  function get_parm_varchar_path(p_parm_path varchar2 -- Путь настройки с разделителем '\
                                 ) return varchar2;

  function get_parm_varchar(p_keyid dregparm_dbt.t_keyid%type) return varchar2;

end IT_RS_INTERFACE;
/
