create or replace package it_event_utils is

  /**
   @file it_event_utils.spc
   @brief Сервисы для формирования сообщений Мониторинта БО СОФР BIQ-13171
     
   # changeLog
   |date       |author      |tasks           |note                                                        
   |-----------|------------|----------------|-------------------------------------------------------------
   |22.01.2025 |Зыков М.В.  |CCBO-10701      | Реализация мониторинга по итогу СОР, по мотивам дефекта DEF-74069
   |08.11.2023 |Зыков М.В.  |DEF-54476       | BIQ-13171. Доработка механизма отправки сообщений из SiteScope
   |23.10.2023 |Зыков М.В.  |BOSS-1230       | BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
   |2023.09.20 |Зыков М.В.  |CCBO-7636       | BIQ-13171 Story CCBO-4826 - разработка. Настройка параметров проверок                
   |2023.05.15 |Зыков М.В.  |CCBO-4870       | Создание BIQ-13171. Разработка процедур работы с очередью событий                 
    
  */
  C_C_QUEUE_NUM          constant varchar2(32) := '01'; -- Номер исходящей очереди ожидания рассылки сообщений .
  C_C_MONITORINGSYSTEM   constant varchar2(32) := '##SOFR_MONITORING'; -- Суррагатное имя системы для выделения сообщений из исходящей очереди .
  C_C_REGISTER_SN_PREFIX constant varchar2(100) := 'EVENT.REGISTER'; -- Префикс имени сервиса выполняемого при регистрации события
  C_C_INFO_SN_PREFIX     constant varchar2(100) := 'EVENT.GETINFO'; -- Префикс имени сервиса получения ссобщений для C_C_MONITORINGSYSTEM
  C_C_MESSAGESEND_SN     constant varchar2(128) := 'EVENT##MESSAGESEND'; -- Суррагатное имя сервиса получения ссобщений для рассылки
  C_C_MESSAGESEND_CORR   constant varchar(128) := '##SOFR_MONITORING##MESSAGESEND#'; -- correlation ссобщений для рассылки
  C_C_QSET_PREFIX        constant varchar2(100) := '$$EVENT'; -- Префикс имени сохраняемого параметра 
  -- Настройки путей настроек
  GC_PARAM_LI         constant varchar2(1000) := '\LEVELINFO'; --
  GC_PARAM_MONITORING constant varchar2(1000) := 'РСХБ\МОНИТОРИНГ SOFR'; --
  GC_PARAM_ANTI_SPAM  constant varchar2(1000) := GC_PARAM_monitoring || '\ЗАЩИТА ОТ SPAM';

  GC_PARAM_FORMAT_MSG          constant varchar2(1000) := GC_PARAM_monitoring || '\ФОРМАТ EMAIL СООБЩЕНИЯ'; --
  GC_PARAM_FORMAT_MSG_T        constant varchar2(1000) := GC_PARAM_monitoring || '\ФОРМАТ "T" СООБЩЕНИЯ'; --
  GC_PARAM_FORMAT_MSG_T_PERIOD constant varchar2(1000) := GC_PARAM_FORMAT_MSG_T || '\PERIOD'; --
  GC_PARAM_FORMAT_MSG_T_LOWLI  constant varchar2(1000) := GC_PARAM_FORMAT_MSG_T || '\LEVELINFO'; --
  --
  GC_PARAM_LOAD_CUR        constant varchar2(1000) := GC_PARAM_monitoring || '\ЗАГРУЗКА ПО ВАЛЮТНОМУ РЫНКУ'; --
  GC_PARAM_LOAD_CUR_START  constant varchar2(1000) := GC_PARAM_load_cur || '\TIME_START'; --
  GC_PARAM_LOAD_CUR_STOP   constant varchar2(1000) := GC_PARAM_load_cur || '\TIME_STOP'; --
  GC_PARAM_LOAD_CUR_PERIOD constant varchar2(1000) := GC_PARAM_load_cur || '\PERIOD'; --
  --
  GC_PARAM_PLAN1        constant varchar2(1000) := GC_PARAM_monitoring || '\ПЛАНИРОВЩИК-1'; --
  GC_PARAM_PLAN1_PERIOD constant varchar2(1000) := GC_PARAM_PLAN1 || '\PERIOD'; --
  --
  GC_PARAM_PLAN2        constant varchar2(1000) := GC_PARAM_monitoring || '\ПЛАНИРОВЩИК-2'; --
  GC_PARAM_PLAN2_PERIOD constant varchar2(1000) := GC_PARAM_PLAN2 || '\PERIOD'; --
  --
  GC_PARAM_OPER        constant varchar2(1000) := GC_PARAM_monitoring || '\ОТКАТ ОПЕРАЦИЙ'; --
  GC_PARAM_OPER_PERIOD constant varchar2(1000) := GC_PARAM_oper || '\PERIOD'; --
  GC_PARAM_OPER_SP     constant varchar2(1000) := GC_PARAM_oper || '\OPERATIONS'; --
  --
  GC_PARAM_ASTS_BRIDGE       constant varchar2(1000) := GC_PARAM_monitoring || '\ОБРАБОТКА ASTS BRIDGE'; --
  GC_PARAM_ASTS_BRIDGE_START constant varchar2(1000) := GC_PARAM_ASTS_BRIDGE || '\TIME_START'; --
  GC_PARAM_ASTS_BRIDGE_STOP  constant varchar2(1000) := GC_PARAM_ASTS_BRIDGE || '\TIME_STOP'; --
  GC_PARAM_ASTS_BRIDGE_DELAY constant varchar2(1000) := GC_PARAM_ASTS_BRIDGE || '\DELAY'; --
  --
  GC_PARAM_CFT10002        constant varchar2(1000) := GC_PARAM_monitoring || '\СЧЕТА И ПРОВОДКИ В ЦФТ'; --
  GC_PARAM_CFT10002_PERIOD constant varchar2(1000) := GC_PARAM_CFT10002 || '\PERIOD'; --
  --
  type tr_LevelInfo_number is record(
     Param number);

  type tt_LevelInfo_number is table of tr_LevelInfo_number index by pls_integer;

  --
  type tr_SystemID_monitiring is record(
     service_id  itt_q_service.service_id%type
    ,SystemId    itt_event_log.systemid%type -- SYSTEMID ключ внешней системы 
    ,ServiceName itt_q_service.servicename%type -- Сервис формирование данных для мониторинга
    );

  type tt_SystemID_monitiring is table of tr_SystemID_monitiring;

  -- Возвращает значение пакетной переменной по имени
  function get_constant_str(p_constant varchar2) return varchar2 deterministic;

  --Список сервисов мониторинга
  function sel_SystemID_monitiring return tt_SystemID_monitiring
    pipelined;

  -- Сохранение сообщения об ошибке
  function store_error_message(p_SystemId  itt_event_log.systemid%type
                              ,p_LevelInfo number
                              ,p_Info_dt   date
                              ,p_Info_txt  varchar2
                              ,px_SendInfo xmltype default null
                              ,p_comment   varchar2 default null) return it_event.tr_monitoring_info;

  -- Создание сообщений об ошибке
  procedure send_information_error(p_sqlcode varchar2
                                  ,p_sqlerrm varchar2);

  -- Проверка QManager
  function check_qmanager(p_SystemId itt_event_log.systemid%type) return it_event.tr_monitoring_info;

  -- Разбор  body сообщение для системы мониторинга .
  procedure parsing_Info_BODY(p_BODY         clob
                             ,o_Info_txt     out varchar2
                             ,o_MaxLevel     out number
                             ,o_MaxLevel_dt  out date
                             ,o_MaxLevel_txt out varchar2);

  --Сервис формирования сообщений для мониторинга QMANAGERа
  procedure GetINFO_QMANAGER(p_worklogid integer
                            ,p_messbody  clob
                            ,p_messmeta  xmltype
                            ,o_msgid     out varchar2
                            ,o_MSGCode   out integer
                            ,o_MSGText   out varchar2
                            ,o_messbody  out clob
                            ,o_messmeta  out xmltype);

  --Сервис формирования сообщений для мониторинга SOFRа
  procedure GetINFO_SOFR(p_worklogid integer
                        ,p_messbody  clob
                        ,p_messmeta  xmltype
                        ,o_msgid     out varchar2
                        ,o_MSGCode   out integer
                        ,o_MSGText   out varchar2
                        ,o_messbody  out clob
                        ,o_messmeta  out xmltype);

  --Сервис формирования сообщений для мониторинга систем не вошедших в список
  procedure GetINFO_OTHERS(p_worklogid integer
                          ,p_messbody  clob
                          ,p_messmeta  xmltype
                          ,o_msgid     out varchar2
                          ,o_MSGCode   out integer
                          ,o_MSGText   out varchar2
                          ,o_messbody  out clob
                          ,o_messmeta  out xmltype);

end it_event_utils;
/
