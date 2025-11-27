create or replace package it_q_service is

  /**************************************************************************************************\
    BIQ-9225. Разработка очереди и журнала событий
              Пакет с AQ сервисами
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
    26.09.2024  Зыков М.В.       BOSS-1585                        Сервис ExecuteCode
    23.10.2023  Зыков М.В.       BOSS-1230                        BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
    03.09.2022  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/
  -- Тестовый обработчик № 1
  procedure test1(p_worklogid integer
                 ,p_messbody  clob
                 ,p_messmeta  xmltype
                 ,o_msgid     out varchar2
                 ,o_MSGCode   out integer
                 ,o_MSGText   out varchar2
                 ,o_messbody  out clob
                 ,o_messmeta  out xmltype);

  -- обработчик для очистки дублирующих сообщений об ошибках в мониторинге
  procedure MONITOR_Erase_SPAMError(p_worklogid integer
                                   ,p_messbody  clob
                                   ,p_messmeta  xmltype
                                   ,o_msgid     out varchar2
                                   ,o_MSGCode   out integer
                                   ,o_MSGText   out varchar2
                                   ,o_messbody  out clob
                                   ,o_messmeta  out xmltype);

  -- Сервис для выполнения кода воркерами. Код должен иметь 2 параметра p_worklogid ,p_messmeta ;
  procedure ExecuteCode(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype);

end it_q_service;
/
