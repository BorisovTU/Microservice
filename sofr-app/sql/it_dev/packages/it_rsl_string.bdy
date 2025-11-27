create or replace package body it_rsl_string is

  /**************************************************************************************************\
   BIQ-6664 / Работа с CLOB переменными 
   **************************************************************************************************
   Изменения:
   ---------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                             Описание 
   ----------  ---------------  ------------------------------   -------------------------------------
   04.04.2023  Зыков М.В.       DEF-30097                        Добавление функций генерации полей CSV
   04.04.2023  Сенников И.В.    DEF-34187                        Добавление функции по вставке строки в начало буфера
   26.07.2022  Зыков М.В.       BIQ-11358                        Правка set_clob
   17.05.2022  Зыков М.В.                                        Уход от использования коллекции для буферизации данных
   21.02.2022  Мелихова О.С.    BIQ-6664 CCBO-506                Создание
  \**************************************************************************************************/
  g_clob_buffer clob;

  --Добавить clob в буфер
  procedure append_clob(p_clob in clob) is
  begin
    if dbms_lob.getlength(lob_loc =>  p_clob) > 0
    then
      dbms_lob.append(dest_lob => g_clob_buffer, src_lob => p_clob);
    end if;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  --Добавить запись в буфер
  procedure append_varchar(p_str in varchar2) is
  begin
    if length(p_str) > 0
    then
      dbms_lob.writeappend(lob_loc => g_clob_buffer, amount => length(p_str), buffer => p_str);
    end if;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  --Добавить запись в начало буфера
  procedure insert_before_varchar(p_str in varchar2) is
  begin
    if length(p_str) > 0
    then
      dbms_lob.copy(dest_lob => g_clob_buffer, src_lob => g_clob_buffer, amount => dbms_lob.getlength(lob_loc => g_clob_buffer), dest_offset => length(p_str) + 1, src_offset => 1);
      dbms_lob.write(lob_loc => g_clob_buffer, amount => length(p_str), offset => 1, buffer => p_str);
    end if;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  --очистить буфер
  procedure clear is
  begin
    dbms_lob.trim(g_clob_buffer, 0);
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  --получить clob из буфера
  function get_clob return clob is
  begin
    return g_clob_buffer;
  end;

  --записать clob в буфер и возвращает кол-во строк по p_len_str символов в строке 
  function set_clob(p_clob    clob
                   ,p_len_str integer default c_SIZE) return number is
    v_clob_length number;
  begin
    v_clob_length := dbms_lob.getlength(lob_loc => p_clob);
    if v_clob_length > 0
    then
      dbms_lob.copy(g_clob_buffer, p_clob, v_clob_length);
    else
      clear;
    end if;
    return ceil(v_clob_length / p_len_str);
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  --записать clob в буфер
  procedure set_clob(p_Clob clob) is
    v_res number;
  begin
    v_res := set_clob(p_clob);
  end;

  -- возвращаем строку из буфера
  function get_varchar(p_index   in number
                      ,p_len_str integer default c_SIZE) return varchar2 is
    --v_res varchar2(c_SIZE);
    v_res         varchar2(32767); --!!! временно! синтаксис TOAD не принимает константу как число.
    v_amount      integer;
    v_offset      integer;
    v_clob_length number;
  begin
    v_clob_length := dbms_lob.getlength(lob_loc => g_clob_buffer);
    v_offset      := ((p_index - 1) * p_len_str) + 1;
    v_amount      := least(p_len_str, (v_clob_length - v_offset + 1));
    v_res         := dbms_lob.substr(lob_loc => g_clob_buffer, amount => v_amount, offset => v_offset);
    return v_res;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  function GetCell(p_value number
                  ,p_last  boolean default false) return varchar2 as
    v_str varchar2(100) := rtrim(rtrim(trim(to_char(p_value, '99999999999999999999999999999990d999999999999999999999999999999', 'NLS_NUMERIC_CHARACTERS = '', ''')), '0'), ',');
  begin
    return v_str || case when p_last then chr(13) || chr(10) else ';' end;
  end;

  function GetCell(p_value varchar2
                  ,p_last  boolean default false) return varchar2 as
    v_str varchar2(32676) := replace(p_value, '"', '""');
  begin
    v_str := translate(v_str, c_NOTEXT_CHAR, ' ');
    if instr(v_str, '"') > 0
       or instr(v_str, ';') > 0
    then
      v_str := '"' || v_str || '"';
      /*elsif length(trim(v_str)) <= 255
            and not REGEXP_LIKE(trim(v_str), '[^,0-9]')
      then
        v_str := '="' || v_str || '"';*/
    end if;
    return v_str ||(case when p_last then chr(13) || chr(10) else ';' end);
  end;

  function GetCell(p_value date
                  ,p_last  boolean default false) return varchar2 as
    v_str varchar2(50);
  begin
    if p_value = trunc(p_value)
    then
      v_str := to_char(p_value, 'yyyy-mm-dd');
    else
      v_str := to_char(p_value, 'yyyy-mm-dd hh24:mi:ss');
    end if;
    return v_str || case when p_last then chr(13) || chr(10) else ';' end;
  end;

  procedure CSVTemplate(p_clob_csv in out clob) as
    v_clob_length integer;
  begin
    v_clob_length := dbms_lob.getlength(lob_loc => p_clob_csv);
    if v_clob_length > 2
       and dbms_lob.substr(lob_loc => p_clob_csv, amount => 2, offset => v_clob_length - 1) = chr(13) || chr(10)
    then
      dbms_lob.trim(p_clob_csv, v_clob_length - 2);
    end if;
  end;

  procedure AddCell(p_num  number
                   ,p_last boolean default false) as
  begin
    it_rsl_string.append_varchar(rtrim(rtrim(trim(to_char(p_num, '9999999999999999999999990d9999', 'NLS_NUMERIC_CHARACTERS = '', ''')), '0'), ',') || case when
                                 p_last then chr(13) || chr(10) else ';' end);
  end;

  procedure AddCell(p_str  varchar2
                   ,p_last boolean default false) as
    v_str varchar2(32676) := replace(p_str, '"', '""');
  begin
    v_str := translate(v_str, chr(0) || chr(13), ' ');
    if instr(v_str, '"') > 0
       or instr(v_str, ';') > 0
    then
      v_str := '"' || v_str || '"';
    end if;
    it_rsl_string.append_varchar(v_str || case when p_last then chr(13) || chr(10) else ';' end);
  end;

begin
  dbms_lob.createtemporary(lob_loc => g_clob_buffer, cache => true);
end;
/
