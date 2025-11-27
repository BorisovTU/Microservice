create or replace package it_file is

  /**************************************************************************************************\
   Обмен файлами для всего функционала разработки РСХБ-Интех
   **************************************************************************************************
   Изменения:
   ---------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание 
   ----------  ---------------  ---------------------------   ----------------------------------------
   13.11.2023  Зыков М.В.       DEF-53020                     НЕ формируется отчет МСФО ОД_3_сделки РЕПО по состоянию на 01/10/2023  
   17.10.2023  Зыков М.В.       BOSS-358                      BIQ-13699.2. СОФР. Этап 2 - добавление файла ограничений по срочному рынку в обработку IPS
   14.12.2022  Зыков М.В.       DEF-36224                     Добавление константы C_FILE_CODE_QUIK_FUTUR,..._SECUR
   26.07.2022  Зыков М.В.       BIQ-11358                     Добавление константы C_FILE_CODE_LIMIT_GLOBAL
   25.04.2022  Мелихова О.С.    BIQ-11358                     Добавление константы C_OUIK
                                                              Добавлен параметр p_file_code (код файла) 
                                                              Добавлена процедура get_last_file_by_code
   04.02.2022  Мелихова О.С.    BIQ-6664 CCBO-506             Создание  
  \**************************************************************************************************/
  
  C_SOFR_RSBANK       constant varchar2(250) := 'SOFR_RSBANK';
  C_SOFR_DB           constant varchar2(250) := 'SOFR_DB'; 
  C_QUIK              constant varchar2(250) := 'QUIK_DB';
 
  C_FILE_CODE_QUIK             constant varchar2(250) := 'QUIK_LIMITS';
  C_FILE_CODE_LIMIT_GLOBAL     constant varchar2(250) := 'QUIK_LIMITS_GLOBAL';
  C_FILE_CODE_LIMIT_FORTS      constant varchar2(250) := 'QUIK_LIMITS_FORTS';
  C_FILE_CODE_QUIK_FUTUR     constant varchar2(250) := 'QUIK_FILE_FUTUR';
  C_FILE_CODE_QUIK_SECUR     constant varchar2(250) := 'QUIK_FILE_SECUR';
   ------ Отчеты 
  C_FILE_CODE_REP_OD3 constant varchar2(250) := 'REP_MCFO_OD3'; -- Реестр всех сделок РЕПО с 01 января отчетного года и сделок, открытых по состоянию на отчетную дату
  C_FILE_CODE_REP_NREG constant varchar2(250) := 'REP_NREGISTERIA'; -- Внутренний учет, Реестр ВУ
  ------
  -- Возвращает значение пакетной переменной по имени
  function get_constant_str(p_constant varchar2) return varchar2 deterministic;

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
                      ,p_sessionid   in number default null) return number;

end;
/
