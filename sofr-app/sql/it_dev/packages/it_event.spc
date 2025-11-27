create or replace package it_event is

  /**************************************************************************************************\
    BIQ-9225. Разработка очереди и журнала событий
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
     21.01.2025  Зыков М.В.       BOSS-7457                        BOSS-7453 Разработка. Рефакторинг процедуры мониторинга 
     23.10.2023  Зыков М.В.       BOSS-1230                        BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
     15.05.2023  Зыков М.В.       CCBO-4870                        BIQ-13171. Разработка процедур работы с очередью событий
     02.09.2022  Зыков М.В.       BIQ-9225                         Перевод на универсальную процедуру отправки сообщения
     18.08.2022  Зыков М.В.       BIQ-9225                         Изменение параметров
     03.08.2022  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/
  C_C_INFO_Mail_TO constant varchar2(2000) := 'SOFR_Monitoring@rshb.ru';

  C_C_INFO_Message_TO constant varchar2(100) := '----';

  --  
  type tr_monitoring_info is record(
     SystemId     itt_event_log.systemid%type -- SYSTEMID ключ внешней системы 
    ,Info_msgid   itt_event_log.info_msgid%type -- GUID INFO сообщения 
    ,Info_enqdt   date -- Сообщение сформировано 
    ,Info_txt     varchar2(2000) -- Текст сообщения 
    ,MaxLevel     number -- Максимальный уровень критичности событий
    ,MaxLevel_dt  date -- Время  события
    ,MaxLevel_txt varchar2(200) -- Текст события  
    ,Info_META    varchar2(4000) -- XML c мета данными проверок 
    );

  type tt_monitoring_info is table of tr_monitoring_info;

  --  
  type tr_MessageSend is record(
     Info_msgid   itt_event_log.info_msgid%type -- GUID INFO сообщения 
    ,Info_enqdt   date -- Сообщение сформировано 
    ,Message_type char(1) -- M - почта / T - telegramm 
    ,Message_TO   varchar2(2000) -- Адресат сообщения 
    ,Subject      varchar2(200) -- Тема сообщения 
    ,Page         number -- Номер страницы сообщения 
    ,Text         varchar2(4000) -- Текст сообщения 
    );

  type tt_MessageSend is table of tr_MessageSend;

  --регистрирует событие 
  procedure RegisterEvent(p_EventID     varchar2 default null --ключ события внешней системы  , если пустой СОФР сам присваивает GUID
                         ,p_SystemId    varchar2 -- SYSTEMID ключ внешней системы , EventID+ SYSTEMID = суррогатный уникальный ключ который позволяет идентифицировать событие во внешней системе. если пустой, дефалтим в UNKNOWNSYS
                         ,p_ServiceName varchar2 default null -- ключ внешнего сервиса , у одного SYSTEMID м.б. несколько сервисов, . если пустой дефалтим в UNDEFINEDSERVICE
                         ,p_MsgBODY     clob default null -- Текст описания события  
                         ,p_MsgMETA     clob default null -- метаданные события  , XML с единственным узлом и  парами ключ-значение в атрибутах вида <XML param1="value1" param2="value2" .. paramN="valuen"></XML>
                          -- LevelInfo = "0" - уровень критичности события от 0 - информация до 10 - АВАРИЯ ,<0 - бизнес ошибки . Не отправляются в поддержку
                         ,o_errtxt out varchar2 --- Текст ошибки регистрации 
                         ,o_MsgID  out itt_q_message_log.msgid%type);

  --Информация о событии из журнала событий
  function GetEvent(p_MsgID       varchar2 -- ключ который был получен CALLBACKID RegisterEvent
                   ,p_contentType clob default null -- переключатель структуры выхлопа выходного CLOBa. на первой итерации констатнта - XML , по умолчанию он же . CLOB сериализуем в XML.
                    ) return clob;

  --Список новых сообщений для мониторинга
  function GetNewInfo(p_SystemId itt_event_log.systemid%type default null) return tt_monitoring_info
    pipelined;

  --Список архивных сообщений мониторинга
  function GetInfo(p_dBegin date
                  ,p_dEnd   date
                  ,p_all    number default 0) return tt_monitoring_info
    pipelined;

  --Новые сообщения для рассылки 
  function GetNewMessageSend(p_wait         number default null -- ожидание в секундах null до появления в очереди
                            ,p_Message_type char default null) return tt_MessageSend
    pipelined;

  --Парсинг Сообщения для рассылки 
  function GetMessageSend(p_Info_msgid itt_event_log.info_msgid%type -- GUID INFO сообщения 
                          ) return tt_MessageSend
    pipelined;

  -- Регистрация ОШИБКИ как события мониторинга
  procedure RegisterError(p_SystemId    varchar2 -- SYSTEMID ключ системы 
                         ,p_ServiceName varchar2 -- ключ внешнего сервиса , у одного SYSTEMID м.б. несколько сервисов,
                         ,p_ErrorCode   integer
                         ,p_ErrorDesc   varchar2
                         ,p_LevelInfo   integer -- >= 0 для службы поддержки 0 - сообщение 10 - авария , <0 - бизнес ошибки . Не отправляются в поддержку
                         ,p_backtrace   varchar2 default null
                         ,p_MsgBODY     clob default null -- Развернутый текст описания ошибки 
                         ,p_MsgMETA     clob default null -- XML будет добавлено в RootElement <XML> 
                          );

  --  Запись ОШИБКИ в формате события в ITT_LOG .
  procedure AddErrorITLog(p_SystemId    varchar2 -- SYSTEMID ключ системы 
                         ,p_ServiceName varchar2 -- ключ внешнего сервиса , у одного SYSTEMID м.б. несколько сервисов,
                         ,p_ErrorCode   integer
                         ,p_ErrorDesc   varchar2
                         ,p_LevelInfo   integer -- >= 0 для службы поддержки 0 - сообщение 10 - авария , <0 - бизнес ошибки . Не отправляются в поддержку
                         ,p_backtrace   varchar2 default null
                         ,p_MsgBODY     clob default null -- Развернутый текст описания ошибки 
                         ,p_MsgMETA     clob default null -- XML будет добавлено в RootElement <XML> 
                          );

 -- Запись ОШИБКИ в формате события в ITT_LOG и регистрация события мониторинга (RegisterError) с защитой от спама по p_SystemId,p_ServiceName,p_ErrorCode за период p_period) .
  procedure AddErrorITLogMonitoring(p_SystemId    varchar2 -- SYSTEMID ключ системы 
                                   ,p_ServiceName varchar2 -- ключ внешнего сервиса , у одного SYSTEMID м.б. несколько сервисов,
                                   ,p_ErrorCode   integer
                                   ,p_ErrorDesc   varchar2
                                   ,p_LevelInfo   integer -- >= 0 для службы поддержки 0 - сообщение 10 - авария , <0 - бизнес ошибки . Не отправляются в поддержку
                                   ,p_backtrace   varchar2 default null
                                   ,p_MsgBODY     clob default null -- Развернутый текст описания ошибки 
                                   ,p_MsgMETA     clob default null -- XML будет добавлено в RootElement <XML> 
                                   ,p_period   integer default null
                                    );

end it_event;
/
