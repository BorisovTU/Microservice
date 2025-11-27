create or replace package it_diasoft is

  /*********************************************************************************************************************************************************\
    Пакет для обмена сообщениями СОФР Диасофт через KAFKA
   *********************************************************************************************************************************************************
    Изменения:
   ---------------------------------------------------------------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание 
   ----------  ---------------  ---------------------------   ----------------------------------------------------------------------------------------------
   24.10.2023  Зыков М.В.       BOSS-1231                     BIQ-13034 BOSS-770 Доработка QManager для получения сообщений из Кафку и запуска процедур СОФР
  \*********************************************************************************************************************************************************/
  C_C_SYSTEM_NAME constant varchar2(128) := 'DIASOFT';

  C_C_SYSTEM_NAME_TAXES constant varchar2(128) := C_C_SYSTEM_NAME; -- DEF-76759 'DIASOFT_TAXES';
  /*Коды критичных ошибок*/
  ERROR_IN_THE_SERVICE      constant number(5) := 20000; /*Ошибка в работе сервиса*/
  ERROR_UNEXPECTED_GET_DATA constant number(5) := 5; /*Непредвиденная ошибка получения данных в СОФР*/
  ERROR_CLIENT_NOTFOUND     constant number(5) := 1; /*Не найден клиент*/
  ERROR_CONTRACT_NOTFOUND   constant number(5) := 2; /*Не найден договор*/
  ERROR_AVOIR_NOTFOUND      constant number(5) := 3; /*Не найдена ценная бумага*/
  ERROR_DOUBLE              constant number(5) := 4; /*Дублирование запроса*/
  --Статусы записи DNPTXNKDREQDIAS_DBT (DNAMEALG_DBT.T_ITYPEALG = 7342)
  NPTXNKDREQDIAS_STATUS_NEW              constant number := 0; --Новая запись
  NPTXNKDREQDIAS_STATUS_WAITCONFIRM      constant number := 1; --Ожидает подтверждения
  NPTXNKDREQDIAS_STATUS_PROCESSED        constant number := 2; --Обработано
  NPTXNKDREQDIAS_STATUS_VALIDATION_ERROR constant number := 3; --Ошибка валидации
  NPTXNKDREQDIAS_STATUS_PROCESSING_ERROR constant number := 4; --Ошибка обработки
  NPTXNKDREQDIAS_STATUS_NOTCONFIRMED     constant number := 5; --Не подтверждено
  PTCK_CFT                               constant number(5) := 101; /*Код ЦФТ*/
  C_MARKETPLACE_SPB                      constant varchar2(5) := 'SPB';

  C_MARKETPLACE_NRD constant varchar2(5) := 'NRD';

  OBJECTTYPE_CONTRACT   constant number(5) := 207; /* тип субъекта договор*/
  INTEGRATION_OPER      constant number(5) := 9997; /*номер операциониста под которым выполняются интеграионные действия*/
  CODEKIND_SPB          constant number(5) := 76; /*Код СПБ биржи*/
  CODEKIND_REGISTER_SPB constant number(5) := 2; /*Регистрационный код участника торгов СПБ*/
  OBJECTTYPE_CLIENT     constant number(5) := 3; /*Тип субъекта клиент*/
  /*примечания на ДБО*/
  NOTEKIND_DEPONUMBER_K             constant number(5) := 102; /*Номер договора Депо*/
  NOTEKIND_DEPONUMBER_T             constant number(5) := 104; /*Торговый счет Депо*/
  NOTEKIND_DEPOACCOUNT_K            constant number(5) := 101; /*Счет Депо Владельца*/
  NOTEKIND_DEPOSTARTDATE            constant number(5) := 103; /* Дата заключения договора Депо*/
  NOTEKIND_DEPO_TRADING_ACCOUNT     constant number(5) := 106; /*Раздел Торгового счета Депо*/
  NOTEKIND_DEPO_TRADING_ACCOUNT_SPB constant number(5) := 115; /*Раздел торгового счёта депо СПБ*/
  /*категории договора*/
  CONTRACT_CATEGORY_STATUS constant number(5) := 101; /* статус договора БО */
  /*значение категории "статус договора*/
  CATEGORY_STATUS_UNDEFINED    constant number(5) := 0; /* не определен */
  CATEGORY_STATUS_DEPO_ACCEPT  constant number(5) := 1; /* Получено подтверждение Депозитария */
  CATEGORY_STATUS_ASOA_ACCEPT  constant number(5) := 2; /* Получено подтверждение АСОА - не используется */
  CATEGORY_STATUS_FINISHED     constant number(5) := 3; /* Обработка завершена */
  CATEGORY_STATUS_NEW_CONTRACT constant number(5) := 4; /* Новый */
  CATEGORY_STATUS_MOEX_ACCEPT  constant number(5) := 5; /* Получено подтвеждение ММВБ */
  /*тип объекта (dfunc_dbt)*/
  OBJECTTYPE_NOTIFICATION constant number(5) := 5207; /*Уведомление пользователю*/
  OBJECTTYPE_UPLOADDOC    constant number(5) := 6207; /*Отправка уведомления в АСОА*/
  -- Упаковщик исходящх сообшений в DIASIFT
  procedure out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype);

  -- Упаковщик исходящх сообшений в DIASIFT через KAFKA ДЛЯ ТВИС 16598 - https://sdlc.go.rshbank.ru/confluence/pages/viewpage.action?pageId=268501101
  procedure out_pack_message_second(p_message     it_q_message_t
                                   ,p_expire      date
                                   ,o_correlation out varchar2
                                   ,o_messbody    out clob
                                   ,o_messmeta    out xmltype);

  --  Обработчик SendPkoInfoReq
  procedure SendPkoInfo(p_worklogid integer
                       ,p_messbody  clob
                       ,p_messmeta  xmltype
                       ,o_msgid     out varchar2
                       ,o_MSGCode   out integer
                       ,o_MSGText   out varchar2
                       ,o_messbody  out clob
                       ,o_messmeta  out xmltype);

  --  Обработчик SendPkoStatusResult
  procedure SendPkoStatusResult(p_worklogid integer
                               ,p_messbody  clob
                               ,p_messmeta  xmltype
                               ,o_msgid     out varchar2
                               ,o_MSGCode   out integer
                               ,o_MSGText   out varchar2
                               ,o_messbody  out clob
                               ,o_messmeta  out xmltype);

  --  Формирование ответа на  SendPkoInfoReq BIQ-1304 
  procedure SendPkoInfoResp(p_GUIDReq         in varchar2 -- GUID из входящего сообщения  SendPkoInfoReq,
                           ,p_CustodyOrderId  in varchar2 -- Id поручения в Диасофт, из CustodyOrderId во входящем xml
                           ,p_SofrOperationId in varchar2 -- Id свежесозданной операции в СОФР (ddl_tick_dbt.t_dealid). Не заполняется, если операцию не удалось создать.
                           ,o_ErrorCode       out number -- != 0 ошибка создания сообщения  o_ErrorDesc
                           ,o_ErrorDesc       out varchar2
                           ,p_MSGCode         in number default 0 -- Код ошибки обработки SendPkoInfoReq
                           ,p_MSGText         in varchar2 default null
                           ,p_comment         in varchar2 default null);

  -- BIQ-16598 Ответ на запрос УНКД из Диасофта 
  procedure GetAllocatedCouponInfoResp(p_GUIDReq   in varchar2 -- GUID из входящего сообщения GetAllocatedCouponInfoReq 
                                      ,p_GUIDResp  in varchar2 -- GUID ответного сообщения 
                                      ,p_MSGCode   in number default 0 -- глобальный код ошибки 
                                      ,p_MSGText   in varchar2 default 'Успешно' -- глобальный текст ошибки 
                                      ,o_messbody  out clob -- сформированный ответ 
                                      ,o_ErrorCode out number -- != 0 ошибка создания сообщения 
                                      ,o_ErrorDesc out varchar2 -- описание ошибки 
                                       );

  --  BIQ-16598 Запрос УНКД из Диасофта 
  procedure GetAllocatedCouponInfoReq(p_worklogid integer
                                     ,p_messbody  clob
                                     ,p_messmeta  xmltype
                                     ,o_msgid     out varchar2
                                     ,o_MSGCode   out integer
                                     ,o_MSGText   out varchar2
                                     ,o_messbody  out clob
                                     ,o_messmeta  out xmltype);

  -- BIQ-16598 Ответ на ответ на запрос УНКД из Диасофта                            
  procedure SendAllocatedCouponResultReq(p_worklogid integer
                                        ,p_messbody  clob
                                        ,p_messmeta  xmltype
                                        ,o_msgid     out varchar2
                                        ,o_MSGCode   out integer
                                        ,o_MSGText   out varchar2
                                        ,o_messbody  out clob
                                        ,o_messmeta  out xmltype);

  -- BOSS-1984 СОФР. Мониторинг ошибок по BIQ-13034 Отправка события .
  procedure SendErrorEvent(p_ErrorCode  in integer
                          ,p_Head       in varchar2
                          ,p_Text       in clob
                          ,p_monitoring in boolean default false);

  /*BIQ-1304  
  Отбирает записи PKO_WriteOff, где ExpirationDate < Текущей календарной даты и не IsLimitCorrected и не IsCanceled и не IsCompleted,
  проставляет признак IsCanceled и CancelationTimestamp = текущая дата/время
  Добавляет записи с id операции в очередь для вызова макроса diasoft_Pko_CancelExpiredOrders */
  procedure MarkExpiredOrders(p_CalcDate date default trunc(sysdate));

  procedure start_Pko_NoSecurities(p_WriteOffid  number
                                  ,p_operationid number
                                  ,p_Dealid      number
                                  ,o_ErrorCode   out number
                                  ,o_ErrorDesc   out varchar2
                                  ,p_send_notify number default 0 -- 0- не отправлять  
                                  ,p_Head       varchar2 default null
                                  ,p_Text       clob default null);

  procedure start_Pko_blockSecurities_Open(p_WriteOffid  number
                                          ,p_operationid number
                                          ,p_Dealid      number
                                          ,o_ErrorCode   out number
                                          ,o_ErrorDesc   out varchar2);

  -- Формирование ответа при нажатии ctrl-z и выбора "достаточно"/"недостаточно" лимитов из операции списания ЦБ
  procedure SendLimitMessage(p_SofrOperationId         in number -- Id операции в СОФР (ddl_tick_dbt.t_dealid)
                            ,p_DiasoftId               in varchar2 default null -- Id операции в Диасофт 
                            ,p_LimitCheckStatus        number -- статус проверки лимитов в СОФР 
                            ,p_LimitCheckStatusComment varchar2 -- комментарий статуса проверки лимитов в СОФР 
                            ,o_ErrorCode               out number -- != 0 ошибка создания сообщения  o_ErrorDesc
                            ,o_ErrorDesc               out varchar2
                            ,p_MSGCode                 in number default 0 -- Код ошибки обработки 
                            ,p_MSGText                 in varchar2 default null
                            ,p_comment                 in varchar2 default null);

  /*BIQ-1304  Реквизиты сделки  0- Не найдена  */
  function get_DealRecv(p_deailid       number
                       ,o_dealtype      out number 
                       ,o_clientid      out number
                       ,o_clientcontrid out number
                       ,o_pfi           out number
                       ,o_PKO_opertype  out number
                       ,o_principal     out number
                       ,o_id_operation  out number
                       ,o_marketid      out number
                       ,o_ClientCode    out varchar2
                       ,o_DlContrID     out number) return number;

  /*BIQ-1304  фактический остаток   */
  function get_RestFI(p_pfi           number
                     ,p_clientid      number
                     ,p_clientcontrid number
                     ,p_dayCalc       date) return number;

  /*BIQ-1304  Т/О o_for_plan1  все, o_for_plan2 за сегодня   */
  procedure Get_forPLAN(p_pfi           number
                       ,p_clientid      number
                       ,p_clientcontrid number
                       ,p_dayCalc       date
                       ,o_for_plan1     out number
                       ,o_for_plan2     out number);

  /*BIQ-1304  скорректирование лимиты за сегодня   */
  function Get_PKO_limitcorrected(p_pfi           number
                                 ,p_clientid      number
                                 ,p_clientcontrid number
                                 ,p_dayCalc       date) return number;

  /*BIQ-1304  Установка даты мсполнения   */
  procedure Set_DealDate(p_deailid      number
                        ,p_finalstatusdate date
                        ,o_ErrorCode       out number
                        ,o_ErrorDesc       out varchar2) ;
  
  /*BIQ-1304  Установка даты  графиков СОБУ   */
  procedure Set_GRSOBUDate(p_deailid      number
                           ,p_GRSOBUDate date) ;
  
  /*BIQ-1304 Рассчитывается плановый остаток = фактический остаток + плановые требования - плановые обязательства.
  Добавляет записи с id операции в очередь для вызова макроса diasoft_Pko_blockSecurities_Open   или diasoft_Pko_NoSecurities по условию
  Добавлено в спецификацию для вызова из Diasoft_SendPkoInfo(diasoft_CreatePKO_part2)
  */
  procedure CheckSecuritiesOTC(p_WriteOffid number
                              ,o_ErrorCode  out number
                              ,o_ErrorDesc  out varchar2
                              ,p_send_notify number default 0 -- 0- не отправлять 1- вариант утренняя проверка
                               ,p_Head       varchar2 default null
                               ,p_Text       clob default null);

  -- BIQ-1304 Вызывается в конце процедуры расчета лимитов
  procedure PKO_CheckAndCorrectSecuritiesLimits(p_MarketID       number
                                               ,p_CalcDate       date
                                               ,p_UseListClients number);

  --to_do описание                                               
  procedure SendBrokerContractDepo(p_DBOID number);

  -- to_do описание
  procedure GetBrokerContractDepo(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype);

  --Получение идентификатора клиента по виду кода "Код ЦФТ"  
  function GetPartyIDByCFT(p_PartyCode in varchar2) return number;

  --Получение внутреннего идентификатора ценной бумаги в СОФР по коду ISIN/LSIN 
  function GetAvoirFIID(p_ISINLSIN in varchar2) return number;

  --Проверка наличия договора БО для переданного клиента и номера договора
  function CheckDBO(p_PartyID   in number
                   ,p_Number    in varchar2
                   ,p_StartDate in date
                   ,p_EndDate   in date) return number;

  procedure process_corp_action_redempt (
      p_worklogid     integer
     ,p_messbody      clob
     ,p_messmeta      xmltype
     ,o_msgid     out varchar2
     ,o_MSGCode   out integer
     ,o_MSGText   out varchar2
     ,o_messbody  out clob
     ,o_messmeta  out xmltype
  );
end it_diasoft;
/
