create or replace package it_information is

  /**************************************************************************************************\
    BIQ-9225. Разработка очереди и журнала событий
              Работа с информационным журналом
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
    08.11.2023  Зыков М.В.       DEF-54476                        BIQ-13171. Доработка механизма отправки сообщений из SiteScope
    15.05.2023  Зыков М.В.       CCBO-4870                        BIQ-13171. Разработка процедур работы с очередью событий
    03.08.2022  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/
  C_C_MAIL_GROUP_DEF constant itt_information.mail_group%type := 'SUPPORT';

  function get_MAIL_GROUP_DEF return varchar2;

  -- Запись сообщения в журнал 
  procedure store_info(p_info_type    itt_information.info_type%type
                      ,p_mail_group   itt_information.mail_group%type default C_C_MAIL_GROUP_DEF
                      ,p_info_title   itt_information.info_title%type default null
                      ,p_info_content clob default null);

  -- Вывод инфы о WORKERах в HTML
  function show_stat_qworkers(p_title     varchar2 default null
                             ,p_queue_num itt_q_message_log.queue_num%type) return clob;

  -- Возвращает новый набор сообщений для почтовой группы 
  function get_list_info(p_mail_group itt_information.mail_group%type
                        ,p_format     integer default 0 -- зарезервировано 0 - текст 1 - HTML
                        ,p_len        integer default 4000
                        ,p_str_begin  varchar2 default null
                        ,p_str_end    varchar2 default chr(13) || chr(10)) return varchar2;

  -- Возвращает новое сообщение для почтовой группы 
  function get_mess_info(p_mail_group itt_information.mail_group%type
                        ,o_Title      out itt_information.info_title%type) return varchar2;

end it_information;
/
