create or replace package body it_file is

  /**************************************************************************************************\
   Обмен файлами для всего функционала разработки РСХБ-Интех
   **************************************************************************************************
   Изменения:
   ---------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание 
   ----------  ---------------  ---------------------------   ----------------------------------------
   13.11.2023  Зыков М.В.       DEF-53020                     НЕ формируется отчет МСФО ОД_3_сделки РЕПО по состоянию на 01/10/2023  
   27.04.2022  Мелихова О.С.    BIQ-11358                     Добавлен параметр p_file_code (код файла)
   04.02.2022  Мелихова О.С.    BIQ-6664 CCBO-506             Создание
  \**************************************************************************************************/
  -- Возвращает значение пакетной переменной по имени
  function get_constant_str(p_constant varchar2) return varchar2 deterministic as
    v_ret varchar2(32676);
  begin
    execute immediate ' begin :1 := it_file.' || p_constant || '; end;'
      using out v_ret;
    return v_ret;
  exception
    when others then
      return null;
  end;

  --вставка записи в таблицу
  function insert_file(p_file_dir    in varchar2 default null
                      ,p_file_name   in varchar2 default null
                      ,p_file_clob   in clob default null
                      ,p_file_blob   in blob default null
                      ,p_from_system in varchar2 default null
                      ,p_from_module in varchar2 default null
                      ,p_to_system   in varchar2 default null
                      ,p_to_module   in varchar2 default null
                      ,p_create_user in varchar2 default null
                      ,p_note        in varchar2 default null
                      ,p_file_code   in varchar2 default null
                      ,p_part_no     in number default null
                      ,p_sessionid   in number default null) return number as
    v_id_file  number;
    v_msg_clob clob;
  begin
    v_id_file := its_main.nextval();
    insert into itt_file
      (id_file
      ,file_dir
      ,file_name
      ,file_clob
      ,file_blob
      ,from_system
      ,from_module
      ,to_system
      ,to_module
      ,create_sysdate
      ,create_user
      ,note
      ,file_code
      ,part_no
      ,sessionid)
    values
      (v_id_file
      ,p_file_dir
      ,p_file_name
      ,p_file_clob
      ,p_file_blob
      ,p_from_system
      ,p_from_module
      ,p_to_system
      ,p_to_module
      ,sysdate
      ,p_create_user
      ,p_note
      ,p_file_code
      ,p_part_no 
      ,nvl(p_sessionid, sys_context('USERENV', 'SESSIONID')));
    return v_id_file;
  exception
    when others then
      it_error.put_error_in_stack;
      v_msg_clob := 'ERROR:' || ' v_id_file: ' || v_id_file || chr(13) || chr(10) || ' p_file_dir: ' || p_file_dir || chr(13) || chr(10) ||
                    ' p_file_name: ' || p_file_name || chr(13) || chr(10) || ' p_from_system: ' || p_from_system || chr(13) || chr(10) ||
                    ' p_from_module: ' || p_from_module || chr(13) || chr(10) || ' p_to_system: ' || p_to_system || chr(13) || chr(10) ||
                    ' p_create_user: ' || p_create_user || chr(13) || chr(10) || ' p_note: ' || p_note || chr(13) || chr(10) || ';';
      v_msg_clob := v_msg_clob || ' ' || sqlerrm;
      it_log.log(p_msg_clob => v_msg_clob, p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

end;
/
