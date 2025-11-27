create or replace package it_rsl_string is

  /**************************************************************************************************\
    BIQ-6664 / Работа с CLOB переменными 
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
    04.04.2023  Зыков М.В.       DEF-30097                        Создание полей CSV
    04.04.2023  Сенников И.В.    DEF-34187                        Добавление функции по вставке строки в начало буфера
    17.05.2022  Зыков М.В.       BIQ-11358                        Уход от использования коллекции для буферизации данных
    21.02.2022  Мелихова О.С.    BIQ-6664 CCBO-506                Создание
  \**************************************************************************************************/
  c_SIZE constant number := 32767;

  c_NOTEXT_CHAR constant varchar2(100) := chr(0) || chr(1) || chr(2) || chr(3) || chr(4) || chr(5) || chr(6) || chr(7) || chr(8) || chr(9) || chr(11) ||
                                          chr(12) || chr(13);

  --Добавить запись в буфер
  procedure append_varchar(p_str in varchar2);

  --Добавить clob в буфер
  procedure append_clob(p_clob in clob);

  --Добавить запись в начало буфера
  procedure insert_before_varchar(p_str in varchar2);

  --получить clob из буфера
  function get_clob return clob;

  --очистить буфер
  procedure clear;

  --записать clob в буфер и возвращает кол-во строк по p_len_str символов в строке 
  function set_clob(p_clob    clob
                   ,p_len_str integer default c_SIZE) return number;

  --записать clob в буфер
  procedure set_clob(p_Clob clob);

  -- возвращаем строку из буфера
  function get_varchar(p_index   in number
                      ,p_len_str integer default c_SIZE) return varchar2;

  --Создание полей CSV
  function GetCell(p_value number
                  ,p_last  boolean default false) return varchar2;

  function GetCell(p_value varchar2
                  ,p_last  boolean default false) return varchar2;

  function GetCell(p_value date
                  ,p_last  boolean default false) return varchar2;

  procedure CSVTemplate(p_clob_csv in out clob);

  procedure AddCell(p_num  number
                   ,p_last boolean default false);

  procedure AddCell(p_str  varchar2
                   ,p_last boolean default false);

end;
/
