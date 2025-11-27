create or replace package it_xml is

  /**************************************************************************************************\
    BIQ-9225. Разработка очереди и журнала событий
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
    23.10.2023  Зыков М.В.       BOSS-1230                        BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
    15.05.2023  Зыков М.В.       CCBO-4870                        BIQ-13171. Разработка процедур работы с очередью событий
    18.08.2022  Зыков М.В.       BIQ-9225                         Добавление функции Clob_to_xml 
    03.08.2022  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/
  -- Переводим в NUMBER то что пришло
  function char_to_number(p_snumber varchar2
                         ,p_decim   integer default null
                         ,p_messerr varchar2 default null) return number deterministic;

  -- Переводим в Varchar2 из NUMBER
  function number_to_char(p_number  number
                         ,p_decim   integer default 2 -- Если < 0 '0' справа удаляются  
                         ,p_messerr varchar2 default null) return varchar2 deterministic;

  -- Переводим дату в строку
  function date_to_char(p_date date) return varchar2 deterministic;

  -- Переводим дату в формат ISO 8601
  function date_to_char_iso8601(p_date date) return varchar2 deterministic;

  -- Переводим строку  в дату
  function char_to_date(p_sdate varchar2) return date deterministic;

  -- Переводим строку формата ISO 8601 в дату
  function char_to_date_iso8601(p_sdate varchar2) return date deterministic;

  -- Переводим TIMESTAMP в формат ISO 8601
  function timestamp_to_char_iso8601(p_timestamp timestamp) return varchar2 deterministic;

  -- Переводим строку  в TIMESTAMP
  function char_to_timestamp(p_stimestamp varchar2) return timestamp deterministic;

  -- Переводим строку формата ISO 8601 в TIMESTAMP
  function char_to_timestamp_iso8601(p_stimestamp varchar2) return timestamp deterministic;

  -- Вычисление кол-ва миллисекунд между двумя метками
  function calc_interval_millisec(p_ts_start timestamp
                                 ,p_ts_stop  timestamp) return integer deterministic;

  -- Преобразование CLOB в XLMType
  function Clob_to_xml(p_clob     clob
                      ,p_errparam varchar2 default null) return xmltype;

  /*-- Преобразование CLOB в XLMType игнорировать namesapce
  function Clob_to_xml_delns(p_clob     clob
                            ,p_errparam varchar2 default null) return xmltype;*/

  -- Преобразование XLMType в CLOB 
  function xml_to_Clob(p_xml xmltype) return clob;

  -- Выделение из CLOB в Varchar2
  function Clob_to_str(p_clob clob
                      ,p_len  integer default 4000) return varchar2;

  -- Преобразование CLOB в Varchar2
  function Clob_to_varchar2(p_clob    clob
                           ,p_messerr varchar2 default null) return varchar2;

  -- Получение куска из строки с разделителями
  function token_substr(p_source varchar2 -- где
                       ,p_delim  char -- разделитель
                       ,p_num    pls_integer -- № части
                        ) return varchar2 deterministic;

  -- Кодирование спецсимволов
  function encode_spec_chr(p_source varchar2) return varchar2 deterministic;
  function decode_spec_chr(p_source varchar2) return varchar2 deterministic;
end it_xml;
/
