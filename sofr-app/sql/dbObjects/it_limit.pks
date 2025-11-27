create or replace package it_limit is

  /**
   @file it_limit.spc
   @brief Пакет запуска расчета лимитов с использованием QManagera
   
    
   # tag
   - functional_block:Лимиты
   - code_type:API 
    
   # link
   - https://sdlc.go.rshbank.ru/confluence/pages/viewpage.action?pageId=287923421)
    
   # changeLog
   |date       |author      |tasks                                                     |note                                                        
   |-----------|------------|----------------------------------------------------------|-------------------------------------------------------------
   |2025.02.12 |Зыков М.В.  |BOSS-5710                                                 | Убрать из расчета лимитов нулевые лимиты
   |2024.06.15 |Зыков М.В.  |BOSS-2461 / BIQ-16667                                     | Перевод процедуры расчета лимитов на обработчик сервисов QManager                     
    
    
  */
  GC_PARAM_LIMIT constant varchar2(1000) := 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\РАСЧЕТ_ЛИМИТОВ_QUIK';

  GC_PARAM_QMANAGER constant varchar2(1000) := GC_PARAM_LIMIT || '\ИСПОЛЬЗОВАТЬ QMANAGER';

  GC_PARAM_QFORCE constant varchar2(1000) := GC_PARAM_QMANAGER || '\FORCE-РЕЖИМ РАСЧЕТА';

  GC_PARAM_QDAYCHK constant varchar2(1000) := GC_PARAM_LIMIT || '\КОНТРОЛЬ ЗАПУСКОВ';

  g_qserv_LimitCleaner varchar2(128) := 'Limit.Cleaner';

  type tr_Market is record(
     NPP        number(10)
    ,MarketID   number(10)
    ,MarketCode varchar2(64)
    ,ByStock    number(10)
    ,ByCurr     number(10)
    ,ByDeriv    number(10)
    ,ByEDP      number(10)
    ,MsgID      varchar2(128));

  type tt_Market is table of tr_Market;

  type tr_parallel_sid is record(
     num             number
    ,calc_panelcontr varchar2(128)
    ,addparam        varchar2(4000));

  type tt_parallel_sid is table of tr_parallel_sid;

  type tr_calc_log is record(
     t_label        dcalclimitlog_dbt.t_label%type
    ,isinfo         number
    ,tme            char(8)
    ,t_excepsqlcode dcalclimitlog_dbt.t_excepsqlcode%type
    ,t_start        dcalclimitlog_dbt.t_start%type
    ,t_end          dcalclimitlog_dbt.t_end%type
    ,t_action       dcalclimitlog_dbt.t_action%type);

  type tt_calc_log is table of tr_calc_log;

  -- Возвращает значение пакетной переменной по имени
  function get_constant_str(p_constant varchar2) return varchar2 deterministic;

  -- Установка SID расчета
  procedure set_calc_sid(p_calc_direct varchar2);

  -- Возвращает SID panelcontr расчета
  function get_calc_panelcontr(p_calc_direct varchar2 default null) return varchar2 deterministic;

  -- Сохранение статуса и протокола расчета
  procedure set_calc_status(p_calc_direct in varchar2
                           ,p_ErrorCode   in number -- != 0 ошиб
                           ,p_ErrorDesc   in varchar2 default null
                           ,p_log         clob default null);

  --  Лог  расчета
  function select_calc_log(p_calc_direct in varchar2
                          ,p_log_king    pls_integer default 0 --(0- протокол расчета,1-(ошибки,предупреждения,информационные) )
                           ) return tt_calc_log
    pipelined;

  -- Получение СИД  нового расчета 
  function get_calc_direct(p_CalcDate       in date
                          ,p_ByStockMB      in number default 0
                          ,p_ByStockSPB     in number default 0
                          ,p_ByCurMB        in number default 0
                          ,p_ByFortsMB      in number default 0
                          ,p_ByEDP          in number default 0
                          ,o_ErrorCode      out number -- != 0 ошиб
                          ,o_ErrorDesc      out varchar2
                          ,p_MarketID       in number default -1 --
                          ,p_UseListClients in number default 0
                          ,p_useQMng        in number default 0) return varchar2;

  -- Старт расчета без QManager по площадке 
  procedure CreateLimits_market(p_calc_direct    in varchar2
                               ,p_CalcDate       in date
                               ,p_ByStockMB      in number default 0
                               ,p_ByStockSPB     in number default 0
                               ,p_ByCurMB        in number default 0
                               ,p_ByFortsMB      in number default 0
                               ,p_ByEDP          in number default 0
                               ,p_MarketID       in number
                               ,o_ErrorCode      out number -- != 0 ошибка создания сообщения  o_ErrorDesc
                               ,o_ErrorDesc      out varchar2
                               ,p_UseListClients in number default 0);

  -- Старт расчета через QManager
  procedure CreateLimits(p_CalcDate       in date
                        ,p_ByStockMB      in number default 0
                        ,p_ByStockSPB     in number default 0
                        ,p_ByCurMB        in number default 0
                        ,p_ByFortsMB      in number default 0
                        ,p_ByEDP          in number default 0
                        ,o_calc_direct    out varchar2
                        ,o_ErrorCode      out number -- != 0 ошибка создания сообщения  o_ErrorDesc
                        ,o_ErrorDesc      out varchar2
                        ,p_MarketID       in number default -1 -- если -1 все площадки
                        ,p_UseListClients in number default 0);

  --------------------------------------------------------------
  -- !!!!!!!!Функции мспользуемые при работе сервисов!!!!!!!!!!!
  --------------------------------------------------------------
  function GetCalcSID return varchar2;

  -- Список площадок расчета 
  function select_Market(p_messmeta xmltype) return tt_Market
    pipelined;

  -- Список SID panelcontr для параллельнрго расчета 
  function select_parallel_sid(p_messmeta xmltype) return tt_parallel_sid
    pipelined;

  -- Вставка записи в DCALCLIMITLOG_DBT 
  procedure CALCLIMITLOG(p_calc_direct  varchar2
                        ,p_CalcDate     date
                        ,p_action       pls_integer
                        ,p_label        varchar2
                        ,p_NPPmarket    pls_integer default 0
                        ,p_MarketCode   varchar2 default null
                        ,p_group        pls_integer default 1000
                        ,p_dtstart      timestamp default null
                        ,p_dtend        timestamp default SYSTIMESTAMP
                        ,p_EXCEPSQLCODE pls_integer default null);

  -- Проверка окончания расчета (0- не окончен 1 - Ok )
  function chk_finish_calc_direct(p_calc_direct in varchar2) return number;

  -- Возвращает кол-во сервисов расчета с ошибкой. 
  function get_sevice_calc_error(p_msgid     itt_q_message_log.msgid%type default null
                                ,o_messerror out itt_q_message_log.commenttxt%type -- Текст первой ошибки
                                 ) return pls_integer;

  -- !!!!!!!! СЕРВИСЫ !!!!!!!!!!!
  -- BIQ-16667 Сервис старта расчета лимитов
  procedure Limit_Start(p_worklogid integer
                       ,p_messbody  clob
                       ,p_messmeta  xmltype
                       ,o_msgid     out varchar2
                       ,o_MSGCode   out integer
                       ,o_MSGText   out varchar2
                       ,o_messbody  out clob
                       ,o_messmeta  out xmltype);

  -- BIQ-16667 Сервис старта расчета лимитов по площадке
  procedure Limit_Begin(p_worklogid integer
                       ,p_messbody  clob
                       ,p_messmeta  xmltype
                       ,o_msgid     out varchar2
                       ,o_MSGCode   out integer
                       ,o_MSGText   out varchar2
                       ,o_messbody  out clob
                       ,o_messmeta  out xmltype);

  --BIQ-16667 Сервис формирование списка договоров по площадке
  procedure Limit_FCTnotDeriv_parallel(p_worklogid integer
                                      ,p_messbody  clob
                                      ,p_messmeta  xmltype
                                      ,o_msgid     out varchar2
                                      ,o_MSGCode   out integer
                                      ,o_MSGText   out varchar2
                                      ,o_messbody  out clob
                                      ,o_messmeta  out xmltype);

  --BIQ-16667 Сервис формирование списка договоров (Deriv) по площадке 
  procedure Limit_FCTbyDeriv(p_worklogid integer
                            ,p_messbody  clob
                            ,p_messmeta  xmltype
                            ,o_msgid     out varchar2
                            ,o_MSGCode   out integer
                            ,o_MSGText   out varchar2
                            ,o_messbody  out clob
                            ,o_messmeta  out xmltype);

  --Сервис расчета лимитов. Старт расчета остатков
  procedure Limit_FCTAccStart(p_worklogid integer
                             ,p_messbody  clob
                             ,p_messmeta  xmltype
                             ,o_msgid     out varchar2
                             ,o_MSGCode   out integer
                             ,o_MSGText   out varchar2
                             ,o_messbody  out clob
                             ,o_messmeta  out xmltype);

  --Сервис расчета лимитов. Старт расчета остатков
  procedure Limit_FCTAcc_parallel(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype);

  --IQ-16667 Сервис расчета лимитов.Расчет сумм неоплаченных комиссий
  procedure Limit_FCTCOM(p_worklogid integer
                        ,p_messbody  clob
                        ,p_messmeta  xmltype
                        ,o_msgid     out varchar2
                        ,o_MSGCode   out integer
                        ,o_MSGText   out varchar2
                        ,o_messbody  out clob
                        ,o_messmeta  out xmltype);

  -- Сервис Запуска Контроль данных по договорам
  procedure Limit_CheckContrTableStart(p_worklogid integer
                                      ,p_messbody  clob
                                      ,p_messmeta  xmltype
                                      ,o_msgid     out varchar2
                                      ,o_MSGCode   out integer
                                      ,o_MSGText   out varchar2
                                      ,o_messbody  out clob
                                      ,o_messmeta  out xmltype);

  -- Сервис Контроль данных по договорам
  procedure Limit_CheckContrTable_parallel(p_worklogid integer
                                          ,p_messbody  clob
                                          ,p_messmeta  xmltype
                                          ,o_msgid     out varchar2
                                          ,o_MSGCode   out integer
                                          ,o_MSGText   out varchar2
                                          ,o_messbody  out clob
                                          ,o_messmeta  out xmltype);

  -- Сервис Старт отбора лотов и сделок по площадке
  procedure Limit_SetStart(p_worklogid integer
                          ,p_messbody  clob
                          ,p_messmeta  xmltype
                          ,o_msgid     out varchar2
                          ,o_MSGCode   out integer
                          ,o_MSGText   out varchar2
                          ,o_messbody  out clob
                          ,o_messmeta  out xmltype);

  -- Сервис параллельгного старта отбора лотов
  procedure Limit_LotTmpStart(p_worklogid integer
                             ,p_messbody  clob
                             ,p_messmeta  xmltype
                             ,o_msgid     out varchar2
                             ,o_MSGCode   out integer
                             ,o_MSGText   out varchar2
                             ,o_messbody  out clob
                             ,o_messmeta  out xmltype);

  -- Сервис расчета лимитов по площадке.Отбор сделок
  procedure Limit_TickTmp(p_worklogid integer
                         ,p_messbody  clob
                         ,p_messmeta  xmltype
                         ,o_msgid     out varchar2
                         ,o_MSGCode   out integer
                         ,o_MSGText   out varchar2
                         ,o_messbody  out clob
                         ,o_messmeta  out xmltype);

  -- Сервис расчета лимитов по площадке.Отбор требований и обязательств
  procedure Limit_CollectPlanSumCur(p_worklogid integer
                                   ,p_messbody  clob
                                   ,p_messmeta  xmltype
                                   ,o_msgid     out varchar2
                                   ,o_MSGCode   out integer
                                   ,o_MSGText   out varchar2
                                   ,o_messbody  out clob
                                   ,o_messmeta  out xmltype);


  -- Сервис расчета лимитов по площадке.Отбор лотов
  procedure Limit_LotTmp_parallel(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype);

  -- Сервис Запуск расчета димитов по площадке
  procedure Limit_Create(p_worklogid integer
                        ,p_messbody  clob
                        ,p_messmeta  xmltype
                        ,o_msgid     out varchar2
                        ,o_MSGCode   out integer
                        ,o_MSGText   out varchar2
                        ,o_messbody  out clob
                        ,o_messmeta  out xmltype);

  -- Сервис Расчет лимитов MONEY фондовый рынок
  procedure Limit_CashStockLimits(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype);

  -- Сервис Расчет лимитов MONEY фондовый рынок
  procedure Limit_CashStockLimByKind(p_worklogid integer
                                    ,p_messbody  clob
                                    ,p_messmeta  xmltype
                                    ,o_msgid     out varchar2
                                    ,o_MSGCode   out integer
                                    ,o_MSGText   out varchar2
                                    ,o_messbody  out clob
                                    ,o_messmeta  out xmltype);

  -- Сервис окончания Расчет лимитов MONEY фондовый рынок
  procedure Limit_CashStockLimByKindFINISH(p_worklogid integer
                                          ,p_messbody  clob
                                          ,p_messmeta  xmltype
                                          ,o_msgid     out varchar2
                                          ,o_MSGCode   out integer
                                          ,o_MSGText   out varchar2
                                          ,o_messbody  out clob
                                          ,o_messmeta  out xmltype);

  -- Сервис Очистка расчетов лимитов DEPO фондовый рынок
  procedure Limit_ClearSecurLimits(p_worklogid integer
                                  ,p_messbody  clob
                                  ,p_messmeta  xmltype
                                  ,o_msgid     out varchar2
                                  ,o_MSGCode   out integer
                                  ,o_MSGText   out varchar2
                                  ,o_messbody  out clob
                                  ,o_messmeta  out xmltype);

  -- Сервис Старт расчет лимитов DEPO фондовый рынок
  procedure Limit_SecurLimits(p_worklogid integer
                             ,p_messbody  clob
                             ,p_messmeta  xmltype
                             ,o_msgid     out varchar2
                             ,o_MSGCode   out integer
                             ,o_MSGText   out varchar2
                             ,o_messbody  out clob
                             ,o_messmeta  out xmltype);

  -- Сервис Расчет лимитов DEPO фондовый рынок
  procedure Limit_SecurLimByKind(p_worklogid integer
                                ,p_messbody  clob
                                ,p_messmeta  xmltype
                                ,o_msgid     out varchar2
                                ,o_MSGCode   out integer
                                ,o_MSGText   out varchar2
                                ,o_messbody  out clob
                                ,o_messmeta  out xmltype);

  -- Сервис окончания Расчет лимитов DEPO фондовый рынок
  procedure Limit_SecurLimByKindFINISH(p_worklogid integer
                                      ,p_messbody  clob
                                      ,p_messmeta  xmltype
                                      ,o_msgid     out varchar2
                                      ,o_MSGCode   out integer
                                      ,o_MSGText   out varchar2
                                      ,o_messbody  out clob
                                      ,o_messmeta  out xmltype);

  -- Сервис расчет цен приобритения
  procedure Limit_WAPositionPrice_parallel(p_worklogid integer
                                          ,p_messbody  clob
                                          ,p_messmeta  xmltype
                                          ,o_msgid     out varchar2
                                          ,o_MSGCode   out integer
                                          ,o_MSGText   out varchar2
                                          ,o_messbody  out clob
                                          ,o_messmeta  out xmltype);

  -- Завершение расчета цен приобритения  
  procedure Limit_SecurLimitsFINISH(p_worklogid integer
                                   ,p_messbody  clob
                                   ,p_messmeta  xmltype
                                   ,o_msgid     out varchar2
                                   ,o_MSGCode   out integer
                                   ,o_MSGText   out varchar2
                                   ,o_messbody  out clob
                                   ,o_messmeta  out xmltype);

  -- Сервис Расчет лимитов Расчет лимитов валютный рынок
  procedure Limit_CashStockLimitsCur(p_worklogid integer
                                    ,p_messbody  clob
                                    ,p_messmeta  xmltype
                                    ,o_msgid     out varchar2
                                    ,o_MSGCode   out integer
                                    ,o_MSGText   out varchar2
                                    ,o_messbody  out clob
                                    ,o_messmeta  out xmltype);

  -- Сервис Расчет лимитов Расчет лимитов валютный рынок
  procedure Limit_CashStockLimCurByKind(p_worklogid integer
                                       ,p_messbody  clob
                                       ,p_messmeta  xmltype
                                       ,o_msgid     out varchar2
                                       ,o_MSGCode   out integer
                                       ,o_MSGText   out varchar2
                                       ,o_messbody  out clob
                                       ,o_messmeta  out xmltype);

  -- Сервис окончания Расчет лимитов Расчет лимитов валютный рынок
  procedure Limit_CashStockLimCurByKindFINISH(p_worklogid integer
                                             ,p_messbody  clob
                                             ,p_messmeta  xmltype
                                             ,o_msgid     out varchar2
                                             ,o_MSGCode   out integer
                                             ,o_MSGText   out varchar2
                                             ,o_messbody  out clob
                                             ,o_messmeta  out xmltype);

  -- Сервис Старт расчета по срочному рынку
  procedure Limit_FutureMarkLimits(p_worklogid integer
                                  ,p_messbody  clob
                                  ,p_messmeta  xmltype
                                  ,o_msgid     out varchar2
                                  ,o_MSGCode   out integer
                                  ,o_MSGText   out varchar2
                                  ,o_messbody  out clob
                                  ,o_messmeta  out xmltype);

  -- Сервис окончания расчета по срочному рынку
  procedure Limit_FutureMarkLimitsFINISH(p_worklogid integer
                                        ,p_messbody  clob
                                        ,p_messmeta  xmltype
                                        ,o_msgid     out varchar2
                                        ,o_MSGCode   out integer
                                        ,o_MSGText   out varchar2
                                        ,o_messbody  out clob
                                        ,o_messmeta  out xmltype);

  -- Сервис очистки лимитов ЕДП
  procedure Limit_ClearLimitsEDP(p_worklogid integer
                                ,p_messbody  clob
                                ,p_messmeta  xmltype
                                ,o_msgid     out varchar2
                                ,o_MSGCode   out integer
                                ,o_MSGText   out varchar2
                                ,o_messbody  out clob
                                ,o_messmeta  out xmltype);

  -- Сервис Расчет лимитов MONEY EDP
  procedure Limit_CashEDPLimits(p_worklogid integer
                               ,p_messbody  clob
                               ,p_messmeta  xmltype
                               ,o_msgid     out varchar2
                               ,o_MSGCode   out integer
                               ,o_MSGText   out varchar2
                               ,o_messbody  out clob
                               ,o_messmeta  out xmltype);

  -- Сервис Расчет лимитов MONEY EDP
  procedure Limit_CashEDPLimByKind(p_worklogid integer
                                  ,p_messbody  clob
                                  ,p_messmeta  xmltype
                                  ,o_msgid     out varchar2
                                  ,o_MSGCode   out integer
                                  ,o_MSGText   out varchar2
                                  ,o_messbody  out clob
                                  ,o_messmeta  out xmltype);

  -- Сервис окончания Расчет лимитов MONEY EDP
  procedure Limit_CashEDPLimByKindFINISH(p_worklogid integer
                                        ,p_messbody  clob
                                        ,p_messmeta  xmltype
                                        ,o_msgid     out varchar2
                                        ,o_MSGCode   out integer
                                        ,o_MSGText   out varchar2
                                        ,o_messbody  out clob
                                        ,o_messmeta  out xmltype);

  -- Сервис Расчет лимитов валютный рынок ЕДП
  procedure Limit_CashEDPLimitsCur(p_worklogid integer
                                  ,p_messbody  clob
                                  ,p_messmeta  xmltype
                                  ,o_msgid     out varchar2
                                  ,o_MSGCode   out integer
                                  ,o_MSGText   out varchar2
                                  ,o_messbody  out clob
                                  ,o_messmeta  out xmltype);

  -- Сервис Расчет лимитов Расчет лимитов валютный рынок ЕДП
  procedure Limit_CashEDPLimCurByKind(p_worklogid integer
                                     ,p_messbody  clob
                                     ,p_messmeta  xmltype
                                     ,o_msgid     out varchar2
                                     ,o_MSGCode   out integer
                                     ,o_MSGText   out varchar2
                                     ,o_messbody  out clob
                                     ,o_messmeta  out xmltype);

  -- Сервис окончания Расчет лимитов  валютный рынок ЕДП
  procedure Limit_CashEDPLimCurByKindFINISH(p_worklogid integer
                                           ,p_messbody  clob
                                           ,p_messmeta  xmltype
                                           ,o_msgid     out varchar2
                                           ,o_MSGCode   out integer
                                           ,o_MSGText   out varchar2
                                           ,o_messbody  out clob
                                           ,o_messmeta  out xmltype);

  -- Сервис Завершения расчета Cash лимитов
  procedure Limit_CashFINISH(p_worklogid integer
                            ,p_messbody  clob
                            ,p_messmeta  xmltype
                            ,o_msgid     out varchar2
                            ,o_MSGCode   out integer
                            ,o_MSGText   out varchar2
                            ,o_messbody  out clob
                            ,o_messmeta  out xmltype);

  -- Сервис Завершения расчета
  procedure Limit_FINISH(p_worklogid integer
                        ,p_messbody  clob
                        ,p_messmeta  xmltype
                        ,o_msgid     out varchar2
                        ,o_MSGCode   out integer
                        ,o_MSGText   out varchar2
                        ,o_messbody  out clob
                        ,o_messmeta  out xmltype);

  -- Сервис Очистки промежуточных данных
  procedure Limit_Cleaner(p_worklogid integer
                         ,p_messbody  clob
                         ,p_messmeta  xmltype
                         ,o_msgid     out varchar2
                         ,o_MSGCode   out integer
                         ,o_MSGText   out varchar2
                         ,o_messbody  out clob
                         ,o_messmeta  out xmltype);

end it_limit;
/
