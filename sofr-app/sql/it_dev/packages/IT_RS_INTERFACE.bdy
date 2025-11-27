create or replace package body IT_RS_INTERFACE is

  /**
  @file IT_RS_INTERFACE.bdy
  @brief Работа с интерфейсом RSBank BIQ-11358
    
  # changeLog
  |date       |author         |tasks                    |note                                                        
  |-----------|---------------|-------------------------|-------------------------------------------------------------
  |01.10.2024 |Велигжанин.А.В.|BOSS-5467_BOSS-5508      | та же правка, что и по BOSS-1266_BOSS-5396, но нужна 
  |           |               |                         |  в release-108.4
  |24.09.2024 |Велигжанин.А.В.|BOSS-1266_BOSS-5396      | add_usermodule(), при определении номера модуля ищется 
  |           |               |                         | 'дырка' в t_icaseitem.ditemuser_dbt
  |17.10.2023 | Зыков М.В.    |BOSS-358                 |BIQ-13699.2. СОФР. Этап 2 - добавление файла ограничений по срочному рынку в обработку IPS
  |2023.08.23 |Зыков М.В.     |CCBO-7119                | СОФР.Автоматизация меню и настроек.               
  |2022.05.12 |Зыков М.В.     |BIQ-11358                | Создание                  
   
  */
  c_delm_menu_path constant char(1) := '\';

  c_delm_parm_path constant char(1) := '\';

  function varchar_to_blob(p_str varchar2) return blob deterministic as
  begin
    return utl_raw.cast_to_raw(p_str || chr(0));
  end;

  function blob_to_varchar(p_blob blob) return varchar2 deterministic as
  begin
    return translate(utl_raw.cast_to_varchar2(p_blob), 'A' || chr(0), 'A');
  end;

  ---!!!!! Меню 
  -- Проверка идентификатора приложения
  procedure chk_cidentprogram(p_cidentprogram char
                             ,p_objectid      number default null) as
    v_ret integer;
  begin
    if nvl(length(p_cidentprogram), 0) = 0
    then
      raise_application_error(-20001, 'Укажите символьный Идентификатор приложения ');
    end if;
    if nvl(length(p_cidentprogram), 0) != 1
    then
      raise_application_error(-20001, 'Идентификатор приложения должен быть 1 символ');
    end if;
    select nvl(max(1), 0)
      into v_ret
      from dtypeac_dbt t
     where t.t_inumtype = 22
       and t.t_type_account = p_cidentprogram
       and rownum < 2;
    if v_ret = 0
    then
      raise_application_error(-20002
                             ,'Идентификатор приложения [' || p_cidentprogram || '] не зерегистрирован в системе');
    end if;
    select nvl(max(1), 0)
      into v_ret
      from DMENUTPL_DBT
     where T_MENUID = 1
       and T_IDENTPROGRAM like '%' || p_cidentprogram || '%';
    if v_ret = 0
    then
      raise_application_error(-20002
                             ,'Для идентификатора приложения [' || p_cidentprogram || '] не доступно меню ');
    end if;
    /*   if p_objectid is not null
        then
          select nvl(max(1), 0)
            into v_ret
            from DMENUTPL_DBT
           where T_MENUID = p_objectid
             and T_IDENTPROGRAM like '%' || p_cidentprogram || '%';
          if v_ret = 0
          then
            raise_application_error(-20002
                                   ,'Идентификатор приложения [' || p_cidentprogram || '] не доступен для роли ' || p_objectid);
          end if;
        end if;
    */
  end;

  -- Возвращает T_INUMBERPOINT по пути меню. Если путь не найден то 0
  function get_inumberpoint_menu_path(p_menu_path     varchar2 -- Путь пункта меню с разделителем '\'
                                     ,p_cidentprogram char default chr(131) -- Идентификатор приложения (Г - Главная книга)
                                     ,p_objectid      integer default 1 -- Пользователь
                                     ,p_iusermodule   number default 0 -- если 0 - подпункт меню != 0 пункт  
                                     ,p_istemplate    char default null) return dmenuitem_dbt.t_inumberpoint%type as
    pos            integer;
    v_menu_path    varchar2(32000) := trim(c_delm_menu_path from p_menu_path);
    v_item_menu    varchar2(200);
    v_inumberpoint dmenuitem_dbt.t_inumberpoint%type := 0;
    v_istemplate constant char(1) := case
                                       when trim(nvl(p_istemplate, chr(0))) = chr(88) then
                                        chr(88)
                                       else
                                        chr(0)
                                     end;
  begin
    chk_cidentprogram(p_cidentprogram);
    loop
      pos := instr(v_menu_path, c_delm_menu_path);
      if pos > 0
      then
        v_item_menu := substr(v_menu_path, 1, pos - 1);
        v_menu_path := substr(v_menu_path, pos + 1);
      else
        v_item_menu := v_menu_path;
      end if;
      continue when v_item_menu is null and pos > 0;
      select max(t.t_inumberpoint)
        into v_inumberpoint
        from dmenuitem_dbt t
       where t.t_iidentprogram = ascii(p_cidentprogram)
         and t.t_icaseitem = case
               when pos > 0
                    or p_iusermodule = 0 then
                0
               else
                t.t_icaseitem
             end
         and t.t_objectid = p_objectid
         and T_ISTEMPLATE = v_istemplate
         and trim(translate(t.t_sznameitem, 'A~', 'A')) = trim(v_item_menu)
         and t.t_inumberfather = v_inumberpoint;
      exit when pos = 0 or v_inumberpoint is null;
    end loop;
    return nvl(v_inumberpoint, 0);
  end;

  -- Вощвращает ID модуля по имени MACфайла если не найден - 0
  function get_iusermodule(p_file_mac      varchar2 -- Наименование макроса
                          ,p_cidentprogram char default chr(131) -- Символьный мдентификатор приложения (Г - Главная книга) 
                           ) return number as
    v_itemparm ditemuser_dbt.t_parm%type := utl_raw.cast_to_raw(rpad(lpad(chr(0), 259, chr(0)) || lower(p_file_mac), 400, chr(0)));
    v_ret      ditemuser_dbt.t_icaseitem%type;
  begin
    chk_cidentprogram(p_cidentprogram);
    select nvl(max(i.t_icaseitem), 0)
      into v_ret
      from ditemuser_dbt i
     where i.t_parm = v_itemparm
       and i.t_cidentprogram = p_cidentprogram
       and rownum < 2;
    return v_ret;
  end;

  -- Регистрирует программный модуль и возвращает ID модуля
  function add_usermodule(p_file_mac      varchar2 -- Наименование макроса  
                         ,p_name          varchar2 -- Наименование модуля 
                         ,p_cidentprogram char default chr(131) -- Символьный мдентификатор приложения (Г - Главная книга) 
                          ) return number as
    v_itemparm  ditemuser_dbt.t_parm%type := utl_raw.cast_to_raw(rpad(lpad(chr(0), 259, chr(0)) || lower(p_file_mac), 400, chr(0)));
    v_icaseitem ditemuser_dbt.t_icaseitem%type;
    v_maxitem ditemuser_dbt.t_icaseitem%type;
  begin
    if p_file_mac is null
    then
      raise_application_error(-20003, 'Наименование MAC файла не указано ');
    end if;
    chk_cidentprogram(p_cidentprogram);
    if get_iusermodule(p_file_mac, p_cidentprogram) != 0
    then
      raise_application_error(-20004
                             ,'Для идентификатора приложения [' || p_cidentprogram || '] уже зарегистрирован программный модуль ' || p_file_mac);
    end if;
    -- BOSS-5467_BOSS-5508 
    -- BOSS-1266_BOSS-5396 Поправка: при определении номера ищем дырку в пользовательском диапазоне
    select nvl(max(i.t_icaseitem), 0)+1 into v_maxitem from ditemuser_dbt i where i.t_cidentprogram = p_cidentprogram;
    SELECT NVL(MIN(a.a),v_maxitem) AS a into v_icaseitem 
      FROM (SELECT level AS a FROM DUAL CONNECT BY LEVEL <= v_maxitem) a
      WHERE a.a NOT IN (select t_icaseitem AS a from ditemuser_dbt i where i.t_cidentprogram = p_cidentprogram)
    ;
    insert into ditemuser_dbt
      (t_cidentprogram
      ,t_icaseitem
      ,t_ikindmethod
      ,t_ikindprogram
      ,t_ihelp
      ,t_reserve
      ,t_sznameitem
      ,t_parm)
    values
      (p_cidentprogram
      ,v_icaseitem
      ,1
      ,2
      ,0
      ,chr(1)
      ,p_name
      ,v_itemparm);
    return v_icaseitem;
  end;

  -- Удаляет программный модуль 
  procedure release_usermodule(p_iusermodule   number -- ID модуля 
                              ,p_cidentprogram char default chr(131) -- Символьный мдентификатор приложения (Г - Главная книга) 
                               ) as
  begin
    chk_cidentprogram(p_cidentprogram);
    delete from ditemuser_dbt
     where t_cidentprogram = p_cidentprogram
       and t_icaseitem = p_iusermodule;
  end;

  -- Создание пункта меню 
  procedure add_menu_item(p_menu_path       varchar2 -- Путь пункта меню с разделителем '\'
                         ,p_menu_item       varchar2 -- Название
                         ,p_menu_nameprompt varchar2 -- Описание 
                         ,p_iusermodule     number -- ID модуля если 0 - подпункт меню 
                         ,p_cidentprogram   char default chr(131) -- Символьный идентификатор приложения (Г - Главная книга) 
                         ,p_objectid        number default 1 -- Полное меню
                         ,p_inumberline     number default null
                         ,p_istemplate      char default null) as
    v_inumberpoint integer;
    v_parent_id    integer;
    v_inumberline  integer;
    v_chk          integer;
    v_objectid   constant number := nvl(p_objectid, 1);
    v_istemplate constant char(1) := case
                                       when trim(nvl(p_istemplate, chr(0))) = chr(88) then
                                        chr(88)
                                       else
                                        chr(0)
                                     end;
    v_iusermodule number := nvl(p_iusermodule, 0);
  begin
    if p_menu_path is null
    then
      raise_application_error(-20005, 'Не указан путь пункта меню с разделителем ''\''');
    end if;
    if p_menu_item is null
    then
      raise_application_error(-20006, 'Не указано название пункта меню');
    end if;
    chk_cidentprogram(p_cidentprogram, p_objectid);
    v_parent_id := get_inumberpoint_menu_path(p_menu_path => p_menu_path
                                             ,p_cidentprogram => p_cidentprogram
                                             ,p_objectid => p_objectid
                                             ,p_iusermodule => 0
                                             ,p_istemplate => p_istemplate);
    if v_parent_id = 0
    then
      raise_application_error(-20007
                             ,'Путь меню [' || p_menu_path || ']' || chr(10) || ' не найден для идентификатора приложения [' || p_cidentprogram ||
                              '] и пользователя ' || p_objectid);
    end if;
    if get_inumberpoint_menu_path(p_menu_path => p_menu_path || c_delm_menu_path || p_menu_item
                                 ,p_cidentprogram => p_cidentprogram
                                 ,p_objectid => p_objectid
                                 ,p_iusermodule => 1) > 0
    then
      raise_application_error(-20009
                             ,'Меню [' || p_menu_path || c_delm_menu_path || p_menu_item || ']' || chr(10) || ' для идентификатора приложения [' ||
                              p_cidentprogram || '] и пользователя ' || p_objectid || ' уже зарегистрировано !');
    end if;
    if v_iusermodule != 0
    then
      select nvl(max(1), 0)
        into v_chk
        from ditemuser_dbt i
       where i.t_cidentprogram = p_cidentprogram
         and i.t_icaseitem = v_iusermodule
         and rownum < 2;
      if v_chk = 0
      then
        raise_application_error(-20008
                               ,'ID модуля ' || v_iusermodule || ' не зарегистрирован в подсистеме [' || p_cidentprogram || ']');
      end if;
    end if;
    select nvl(max(i.T_INUMBERPOINT), 0) + 1
      into v_inumberpoint
      from dmenuitem_dbt i
     where T_OBJECTID = p_objectid
       and i.t_istemplate = v_istemplate
       and T_IIDENTPROGRAM = ascii(p_cidentprogram);
    if p_inumberline is null
    then
      select nvl(max(i.T_INUMBERLINE), 0) + 10
        into v_inumberline
        from dmenuitem_dbt i
       where T_OBJECTID = p_objectid
         and i.t_istemplate = v_istemplate
         and T_IIDENTPROGRAM = ascii(p_cidentprogram)
         and t_inumberfather = v_parent_id;
    else
      v_inumberline := p_inumberline;
    end if;
    insert into dmenuitem_dbt
      (t_objectid
      ,t_istemplate
      ,t_iidentprogram
      ,t_inumberpoint
      ,t_inumberfather
      ,t_inumberline
      ,t_icaseitem
      ,t_csystemitem
      ,t_sznameitem
      ,t_sznameprompt
      ,t_ihelp
      ,t_iprogitem)
    values
      (p_objectid
      ,v_istemplate -- chr(0) --chr(88)/*x*/
      ,ascii(p_cidentprogram)
      ,v_inumberpoint
      ,v_parent_id
      ,v_inumberline
      ,v_iusermodule --id модуля
      ,chr(0)
      ,chr(32) || trim(p_menu_item)
      ,p_menu_nameprompt
      ,0
      ,ascii(p_cidentprogram));
  end;

  -- Создание пункта меню если его нет
  procedure add_menu_item_chk(p_cidentprogram   char -- Символьный идентификатор приложения (Г - Главная книга и т.д.)
                             ,p_menu_path       varchar2 -- Путь пункта меню с разделителем '\'
                             ,p_menu_item       varchar2 -- Название
                             ,p_menu_nameprompt varchar2 -- Описание 
                             ,p_iusermodule     number
                             ,p_objectid        number -- operID
                             ,p_inumberline     number default null -- порядковый номер строки меню ( если не указан - последний ) 
                             ,p_istemplate      char default null) as
  begin
    if get_inumberpoint_menu_path(p_menu_path => p_menu_path || c_delm_menu_path || p_menu_item
                                 ,p_cidentprogram => p_cidentprogram
                                 ,p_objectid => p_objectid
                                 ,p_iusermodule => 1) = 0
    then
      add_menu_item(p_menu_path => p_menu_path
                   ,p_menu_item => p_menu_item
                   ,p_menu_nameprompt => p_menu_nameprompt
                   ,p_iusermodule => p_iusermodule
                   ,p_cidentprogram => p_cidentprogram
                   ,p_objectid => p_objectid
                   ,p_inumberline => p_inumberline
                   ,p_istemplate => p_istemplate);
    end if;
  end;

  -- Создание пункта меню в шаблоне для роли 
  procedure add_menu_item_template(p_cidentprogram   char -- Символьный идентификатор приложения (Г - Главная книга и т.д.)
                                  ,p_menu_path       varchar2 -- Путь пункта меню с разделителем '\'
                                  ,p_menu_item       varchar2 -- Название
                                  ,p_menu_nameprompt varchar2 -- Описание 
                                  ,p_usermodule_name varchar2 -- Наименование модуля
                                  ,p_usermodule_file varchar2 -- Наименование макроса
                                  ,pt_objectid       tt_object -- список ролей
                                  ,p_inumberline     number default null -- порядковый номер строки меню ( если не указан - последний ) 
                                   ) as
    v_iusermodule number;
  begin
    chk_cidentprogram(p_cidentprogram);
    v_iusermodule := get_iusermodule(p_file_mac => p_usermodule_file, p_cidentprogram => p_cidentprogram);
    if v_iusermodule = 0
    then
      v_iusermodule := add_usermodule(p_file_mac => p_usermodule_file, p_name => p_usermodule_name, p_cidentprogram => p_cidentprogram);
    end if;
    add_menu_item_template(p_cidentprogram => p_cidentprogram
                          ,p_menu_path => p_menu_path
                          ,p_menu_item => p_menu_item
                          ,p_menu_nameprompt => p_menu_nameprompt
                          ,p_iusermodule => v_iusermodule
                          ,pt_objectid => pt_objectid
                          ,p_inumberline => p_inumberline);
  end;

  -- Создание пункта меню в шаблоне для роли 
  procedure add_menu_item_template(p_cidentprogram   char -- Символьный идентификатор приложения (Г - Главная книга и т.д.)
                                  ,p_menu_path       varchar2 -- Путь пункта меню с разделителем '\'
                                  ,p_menu_item       varchar2 -- Название
                                  ,p_menu_nameprompt varchar2 -- Описание 
                                  ,p_iusermodule     number -- ID пользовательского модуля
                                  ,pt_objectid       tt_object -- список ролей
                                  ,p_inumberline     number default null -- порядковый номер строки меню ( если не указан - последний ) 
                                   ) as
    v_objectid  number;
    vt_objectid tt_object := pt_objectid;
  begin
    chk_cidentprogram(p_cidentprogram);
    vt_objectid(1) := 'Все меню';
    v_objectid := vt_objectid.first;
    loop
      add_menu_item_chk(p_cidentprogram => p_cidentprogram
                       ,p_menu_path => p_menu_path
                       ,p_menu_item => p_menu_item
                       ,p_menu_nameprompt => p_menu_nameprompt
                       ,p_iusermodule => p_iusermodule
                       ,p_objectid => v_objectid
                       ,p_inumberline => p_inumberline
                       ,p_istemplate => case
                                          when v_objectid != 1 then
                                           chr(88)
                                        end);
      v_objectid := vt_objectid.next(v_objectid);
      exit when v_objectid is null;
    end loop;
  end;

  -- Создание пункта меню для операторов по ролям 
  procedure add_menu_item_oper(p_cidentprogram   char -- Символьный идентификатор приложения (Г - Главная книга и т.д.)
                              ,p_menu_path       varchar2 -- Путь пункта меню с разделителем '\'
                              ,p_menu_item       varchar2 -- Название
                              ,p_menu_nameprompt varchar2 -- Описание 
                              ,p_usermodule_name varchar2 -- Наименование модуля
                              ,p_usermodule_file varchar2 -- Наименование макроса
                              ,pt_objectid       tt_object -- список ролей
                              ,p_inumberline     number default null -- порядковый номер строки меню ( если не указан - последний ) 
                               ) as
    v_iusermodule number;
  begin
    chk_cidentprogram(p_cidentprogram);
    v_iusermodule := get_iusermodule(p_file_mac => p_usermodule_file, p_cidentprogram => p_cidentprogram);
    if v_iusermodule = 0
    then
      v_iusermodule := add_usermodule(p_file_mac => p_usermodule_file, p_name => p_usermodule_name, p_cidentprogram => p_cidentprogram);
    end if;
    add_menu_item_oper(p_cidentprogram => p_cidentprogram
                      ,p_menu_path => p_menu_path
                      ,p_menu_item => p_menu_item
                      ,p_menu_nameprompt => p_menu_nameprompt
                      ,p_iusermodule => v_iusermodule
                      ,pt_objectid => pt_objectid
                      ,p_inumberline => p_inumberline);
  end;

  -- Создание пункта меню для операторов по ролям 
  procedure add_menu_item_oper(p_cidentprogram   char -- Символьный идентификатор приложения (Г - Главная книга и т.д.)
                              ,p_menu_path       varchar2 -- Путь пункта меню с разделителем '\'
                              ,p_menu_item       varchar2 -- Название
                              ,p_menu_nameprompt varchar2 -- Описание 
                              ,p_iusermodule     number -- ID пользовательского модуля
                              ,pt_objectid       tt_object -- список ролей
                              ,p_inumberline     number default null -- порядковый номер строки меню ( если не указан - последний ) 
                               ) as
    v_objectid  number;
    vt_objectid tt_object := pt_objectid;
    v_menuid    number;
  begin
    chk_cidentprogram(p_cidentprogram);
    add_menu_item_chk(p_cidentprogram => p_cidentprogram
                     ,p_menu_path => p_menu_path
                     ,p_menu_item => p_menu_item
                     ,p_menu_nameprompt => p_menu_nameprompt
                     ,p_iusermodule => p_iusermodule
                     ,p_objectid => 1
                     ,p_inumberline => p_inumberline);
    v_objectid := vt_objectid.first;
    loop
      exit when v_objectid is null;
      select min(m.t_menuid) into v_menuid from DACSROLETREE_DBT m where m.t_roleid = v_objectid;
      if v_menuid is null
      then
        raise_application_error(-20009, 'Для роли ' || v_objectid || ' не зарегистрировано меню шаблона ');
      end if;
      add_menu_item_chk(p_cidentprogram => p_cidentprogram
                       ,p_menu_path => p_menu_path
                       ,p_menu_item => p_menu_item
                       ,p_menu_nameprompt => p_menu_nameprompt
                       ,p_iusermodule => p_iusermodule
                       ,p_objectid => v_menuid
                       ,p_inumberline => p_inumberline
                       ,p_istemplate => chr(88));
      for cur in (select distinct p.t_oper
                    from DACSOPROLE_DBT rol
                    join dperson_dbt p
                      on p.t_oper = rol.t_oper
                   where ROL.T_ROLEID = v_objectid
                     and p.t_userclosed != chr(88))
      loop
        add_menu_item_chk(p_cidentprogram => p_cidentprogram
                         ,p_menu_path => p_menu_path
                         ,p_menu_item => p_menu_item
                         ,p_menu_nameprompt => p_menu_nameprompt
                         ,p_iusermodule => p_iusermodule
                         ,p_objectid => cur.t_oper
                         ,p_inumberline => p_inumberline);
      end loop;
      v_objectid := vt_objectid.next(v_objectid);
    end loop;
  end;

  /*-- Удаление пункта меню 
  procedure release_menu_item(p_menu_path     varchar2 -- Путь пункта меню с разделителем '\'
                             ,p_cidentprogram char default chr(131) -- Символьный идентификатор приложения (Г - Главная книга) 
                             ,p_objectid      integer default 1 -- 
                              ) as
    v_inumberpoint integer;
    v_inumberline  integer;
    v_chk          integer;
    v_objectid   constant number := nvl(p_objectid, 1);
    v_istemplate constant char(1) := case
                                       when v_objectid = 1 then
                                        chr(0)
                                       else
                                        chr(88)
                                     end;
  begin
    if p_menu_path is null
    then
      raise_application_error(-20005, 'Не указан путь пункта меню с разделителем ''\''');
    end if;
    chk_cidentprogram(p_cidentprogram);
    v_inumberpoint := get_inumberpoint_menu_path(p_menu_path => p_menu_path
                                                ,p_cidentprogram => p_cidentprogram
                                                ,p_objectid => v_objectid
                                                ,p_iusermodule => 1);
    if v_inumberpoint = 0
    then
      raise_application_error(-20007
                             ,'Путь меню [' || p_menu_path || ']' || chr(10) || ' не найден для идентификатора приложения [' || p_cidentprogram ||
                              '] и пользователя ' || v_objectid);
    end if;
    delete from dmenuitem_dbt m
     where t_objectid = v_objectid
       and t_iidentprogram = ascii(p_cidentprogram)
       and m.t_istemplate = v_istemplate
       and t_inumberpoint in (select t_inumberpoint
                                from dmenuitem_dbt
                              connect by nocycle prior t_inumberpoint = t_inumberfather
                               start with t_inumberpoint = v_inumberpoint);
  end;*/
  ---!!!!! Настройки  
  -- Возвращает T_KEYID по пути настроек. Если путь не найден то 0
  function get_keyid_parm_path(p_parm_path varchar2 -- Путь настройки с разделителем '\
                               ) return dregparm_dbt.t_keyid%type as
    pos         integer;
    v_parm_path varchar2(32000) := upper(trim(c_delm_parm_path from p_parm_path));
    v_item_parm dregparm_dbt.t_name%type;
    v_keyid     dregparm_dbt.t_keyid%type := 0;
  begin
    loop
      pos := instr(v_parm_path, c_delm_parm_path);
      if pos > 0
      then
        v_item_parm := substr(v_parm_path, 1, pos - 1);
        v_parm_path := substr(v_parm_path, pos + 1);
      else
        v_item_parm := v_parm_path;
      end if;
      continue when v_item_parm is null and pos > 0;
      select max(t_keyid)
        into v_keyid
        from dregparm_dbt t
       where trim(t.t_name) = trim(v_item_parm)
         and t.t_parentid = v_keyid;
      exit when pos = 0 or v_keyid is null;
    end loop;
    return nvl(v_keyid, 0);
  end;

  -- Добавление(изменение типа) параметра по пути . Возвращает KEYID. Если путь не найден то создается .
  function add_parm_path(p_parm_path   varchar2 -- Путь настройки с разделителем '\'
                        ,p_type        dregparm_dbt.t_type%type -- 0 - integer/ 1-Double/ 2- String /4 - FLAG
                        ,p_description dregparm_dbt.t_description%type default null -- Примечание
                         ) return dregparm_dbt.t_keyid%type as
    pos           integer;
    v_parm_path   varchar2(32000) := upper(trim(c_delm_parm_path from p_parm_path));
    v_item_parm   varchar2(32000);
    v_keyid       dregparm_dbt.t_keyid%type := 0;
    v_keyid_      dregparm_dbt.t_keyid%type := 0;
    v_description dregparm_dbt.t_description%type := chr(0);
    v_type        dregparm_dbt.t_type%type := 0;
  begin
    if v_parm_path is null
    then
      raise_application_error(-20022, 'Укажите путь к настройке');
    end if;
    if nvl(p_type, -1) not in (0, 1, 2, 4)
    then
      raise_application_error(-20021, 'Тип параметра =' || p_type || ' не подерживается');
    end if;
    loop
      pos := instr(v_parm_path, c_delm_parm_path);
      if pos > 0
      then
        v_item_parm := substr(v_parm_path, 1, pos - 1);
        v_parm_path := substr(v_parm_path, pos + 1);
      else
        v_item_parm   := v_parm_path;
        v_description := p_description;
        v_type        := p_type;
      end if;
      continue when v_item_parm is null and pos > 0;
      select max(t_keyid)
        into v_keyid_
        from dregparm_dbt t
       where trim(t.t_name) = trim(v_item_parm)
         and t.t_parentid = v_keyid;
      if v_keyid_ is null
      then
        v_keyid_ := dregparm_dbt_seq.nextval();
        insert into dregparm_dbt
          (t_keyid
          ,t_parentid
          ,t_name
          ,t_type
          ,t_global
          ,t_description
          ,t_security
          ,t_isbranch
          ,t_template)
        values
          (v_keyid_
          ,v_keyid
          ,trim(v_item_parm)
          ,v_type
          ,chr(0)
          ,v_description
          ,chr(0)
          ,chr(0)
          ,chr(1));
        insert into dregval_dbt
          (T_KEYID
          ,T_REGKIND
          ,T_OBJECTID
          ,T_BLOCKUSERVALUE
          ,T_EXPDEP
          ,T_LINTVALUE
          ,T_LDOUBLEVALUE)
        values
          (v_keyid_
          ,0
          ,0
          ,chr(0)
          ,0
          ,0
          ,0);
      elsif pos = 0
            and v_keyid_ is not null
      then
        update dregparm_dbt t
           set t.t_type        = p_type
              ,t.t_description = nvl(v_description, t.t_description)
         where t.t_keyid = v_keyid_;
      end if;
      v_keyid := v_keyid_;
      exit when pos = 0;
    end loop;
    return nvl(v_keyid, 0);
  end;

  -- Удаление параметра
  procedure release_parm(p_keyid dregparm_dbt.t_keyid%type) as
    v_type dregparm_dbt.t_type%type;
  begin
    delete from dregparm_dbt t
     where t.t_keyid in (select t_keyid from dregparm_dbt s connect by nocycle s.t_parentid = prior s.t_keyid start with s.t_keyid = p_keyid);
    if sql%rowcount = 0
    then
      raise_application_error(-20025, 'Параметр keyid =' || p_keyid || ' не найден ');
    end if;
  end;

  procedure release_parm_path(p_parm_path varchar2 -- Путь настройки с разделителем '\
                              ) as
    v_keyid integer;
  begin
    v_keyid := get_keyid_parm_path(p_parm_path);
    if v_keyid != 0
    then
      release_parm(v_keyid);
    end if;
  end;

  -- Запись значения параметра
  procedure set_parm(p_keyid dregparm_dbt.t_keyid%type
                    ,p_parm  number) as
    v_type dregparm_dbt.t_type%type;
  begin
    if p_parm is null
    then
      raise_application_error(-20022, 'Укажите значение в параметре keyid =' || p_keyid);
    end if;
    select max(t.t_type) into v_type from dregparm_dbt t where t.t_keyid = p_keyid;
    case v_type
      when 0 then
        if trunc(p_parm) != p_parm
        then
          raise_application_error(-20023
                                 ,'Невозможно записать значение ' || p_parm || ' в параметр keyid =' || p_keyid || ' (INTEGER) без потери данных');
        end if;
        merge into dregval_dbt p
        using (select p_keyid keyid
                     ,p_parm  parm
                 from dual) z
        on (p.t_keyid = z.keyid)
        when matched then
          update set p.t_lintvalue = z.parm
        when not matched then
          insert
            (t_keyid
            ,t_regkind
            ,t_objectid
            ,t_blockuservalue
            ,t_expdep
            ,t_lintvalue
            ,t_ldoublevalue)
          values
            (z.keyid
            ,0
            ,0
            ,chr(0)
            ,0
            ,z.parm
            ,0);
      when 1 then
        merge into dregval_dbt p
        using (select p_keyid keyid
                     ,p_parm  parm
                 from dual) z
        on (p.t_keyid = z.keyid)
        when matched then
          update set p.t_ldoublevalue = z.parm
        when not matched then
          insert
            (t_keyid
            ,t_regkind
            ,t_objectid
            ,t_blockuservalue
            ,t_expdep
            ,t_lintvalue
            ,t_ldoublevalue)
          values
            (z.keyid
            ,0
            ,0
            ,chr(0)
            ,0
            ,0
            ,z.parm);
      when 2 then
        raise_application_error(-20024
                               ,'Несовпадение типа значения ' || p_parm || ' и типа параметра keyid =' || p_keyid || ' (STRING)');
      when 4 then
        merge into dregval_dbt p
        using (select p_keyid keyid
                     ,decode(p_parm, 0, 0, 88) parm
                 from dual) z
        on (p.t_keyid = z.keyid)
        when matched then
          update set p.t_lintvalue = z.parm
        when not matched then
          insert
            (t_keyid
            ,t_regkind
            ,t_objectid
            ,t_blockuservalue
            ,t_expdep
            ,t_lintvalue
            ,t_ldoublevalue)
          values
            (z.keyid
            ,0
            ,0
            ,chr(0)
            ,0
            ,z.parm
            ,0);
      else
        raise_application_error(-20025, 'Параметр keyid =' || p_keyid || ' не найден ');
    end case;
  end;

  procedure set_parm(p_keyid dregparm_dbt.t_keyid%type
                    ,p_parm  varchar2) as
    v_type dregparm_dbt.t_type%type;
  begin
    select max(t.t_type) into v_type from dregparm_dbt t where t.t_keyid = p_keyid;
    case v_type
      when 0 then
        raise_application_error(-20024
                               ,'Несовпадение типа значения ' || p_parm || ' и типа параметра keyid =' || p_keyid || ' (INTEGER)');
      when 1 then
        raise_application_error(-20024
                               ,'Несовпадение типа значения ' || p_parm || ' и типа параметра keyid =' || p_keyid || ' (DOUBLE)');
      when 2 then
        merge into dregval_dbt p
        using (select p_keyid keyid
                     ,it_rs_interface.varchar_to_blob(p_parm) parm
                 from dual) z
        on (p.t_keyid = z.keyid)
        when matched then
          update set p.t_fmtblobdata_xxxx = z.parm
        when not matched then
          insert
            (t_keyid
            ,t_regkind
            ,t_objectid
            ,t_blockuservalue
            ,t_expdep
            ,t_lintvalue
            ,t_ldoublevalue
            ,t_fmtblobdata_xxxx)
          values
            (z.keyid
            ,0
            ,0
            ,chr(0)
            ,0
            ,0
            ,0
            ,z.parm);
      when 4 then
        if nvl(p_parm, '-') not in (chr(0), chr(88))
        then
          raise_application_error(-20024
                                 ,'Несовпадение типа значения ' || p_parm || ' и типа параметра keyid =' || p_keyid || ' (FLAG)');
        end if;
        merge into dregval_dbt p
        using (select p_keyid keyid
                     ,decode(p_parm, chr(0), 0, 88) parm
                 from dual) z
        on (p.t_keyid = z.keyid)
        when matched then
          update set p.t_lintvalue = z.parm
        when not matched then
          insert
            (t_keyid
            ,t_regkind
            ,t_objectid
            ,t_blockuservalue
            ,t_expdep
            ,t_lintvalue
            ,t_ldoublevalue)
          values
            (z.keyid
            ,0
            ,0
            ,chr(0)
            ,0
            ,z.parm
            ,0);
      else
        raise_application_error(-20025, 'Параметр keyid =' || p_keyid || ' не найден ');
    end case;
  end;

  -- Считывание параметра типа number 
  function get_parm_number_path(p_parm_path varchar2 -- Путь настройки с разделителем '\
                                ) return number as
    v_keyid integer;
  begin
    v_keyid := get_keyid_parm_path(p_parm_path);
    if v_keyid = 0
    then
      return null;
    else
      return get_parm_number(v_keyid);
    end if;
  end;

  function get_parm_number(p_keyid dregparm_dbt.t_keyid%type) return number as
    v_type dregparm_dbt.t_type%type;
    v_ret  number;
  begin
    select max(t.t_type) into v_type from dregparm_dbt t where t.t_keyid = p_keyid;
    case v_type
      when 0 then
        select max(p.t_lintvalue) into v_ret from dregval_dbt p where p.t_keyid = p_keyid;
      when 1 then
        select max(p.t_ldoublevalue) into v_ret from dregval_dbt p where p.t_keyid = p_keyid;
      when 2 then
        raise_application_error(-20026, 'Несовпадение типа параметра keyid =' || p_keyid || ' (STRING)');
      when 4 then
        select max(p.t_lintvalue) into v_ret from dregval_dbt p where p.t_keyid = p_keyid;
      else
        raise_application_error(-20025, 'Параметр keyid =' || p_keyid || ' не найден ');
    end case;
    return v_ret;
  end;

  -- Считывание параметра типа varchar 
  function get_parm_varchar_path(p_parm_path varchar2 -- Путь настройки с разделителем '\
                                 ) return varchar2 as
    v_keyid integer;
  begin
    v_keyid := get_keyid_parm_path(p_parm_path);
    if v_keyid = 0
    then
      return null;
    else
      return get_parm_varchar(v_keyid);
    end if;
  end;

  function get_parm_varchar(p_keyid dregparm_dbt.t_keyid%type) return varchar2 as
    v_type dregparm_dbt.t_type%type;
    v_ret  varchar2(32767);
  begin
    select max(t.t_type) into v_type from dregparm_dbt t where t.t_keyid = p_keyid;
    case v_type
      when 0 then
        raise_application_error(-20026, 'Несовпадение типа параметра keyid =' || p_keyid || ' (INTEGER)');
      when 1 then
        raise_application_error(-20026, 'Несовпадение типа параметра keyid =' || p_keyid || ' (DOUBLE)');
      when 2 then
        begin
          select it_rs_interface.blob_to_varchar(p.t_fmtblobdata_xxxx) into v_ret from dregval_dbt p where p.t_keyid = p_keyid;
        exception
          when no_data_found then
            v_ret := null;
        end;
      when 4 then
        select decode(nvl(max(p.t_lintvalue), 0), 0, chr(0), chr(88)) into v_ret from dregval_dbt p where p.t_keyid = p_keyid;
      else
        raise_application_error(-20025, 'Параметр keyid =' || p_keyid || ' не найден ');
    end case;
    return v_ret;
  end;

end IT_RS_INTERFACE;
/
