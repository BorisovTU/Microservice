CREATE OR REPLACE package it_ips_dfactory is

  C_C_SYSTEM_NAME constant varchar2(128) := 'IPS_DFACTORY';
  
  NOTIFICATIONTYPE_RECOGNITION     CONSTANT NUMBER(3) := 501;
  NOTIFICATIONTYPE_REJECTION       CONSTANT NUMBER(3) := 509;
  NOTIFICATIONTYPE_EXCLUSION       CONSTANT NUMBER(3) := 513;
  NOTIFICATIONTYPE_CONFIRMATIONREQ CONSTANT NUMBER(3) := 511;
  
  /* "x-template-params" Динамически изменяемые параметры (факсимиле)
  Обязательно с экранированием  Да  Передается объектом с указателем на требуемый динамический параметр. 
  Пример: 
  "x-template-params": [
    {
     "paramName": "df_facsimile",
     "paramType": "FILE",
     "paramValue": "FileName.png"
    }
  ]
  */
  type tr_templateparams is record (paramName varchar2(128),paramType varchar2(128),paramValue varchar2(128)) ;
  type tt_templateparams is table of tr_templateparams ;

  type tr_addparams is record (paramName varchar2(128), paramValue varchar2(128)) ;
  type tt_addparams is table of tr_addparams;


  /**
  * Упаковщик исходящих сообшений в Фабрику Документов IPS через KAFKA
  * @since RSHB 110
  * @qtest NO
  */
  procedure out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype);
  
  -- BOSS-319 Cбор данных о клиенте, упаковки и отправки в Kafka сообщения с json содержащим данные для генерации уведомлениях по КИ
  PROCEDURE SendInvestorStatus_json(p_worklogid INTEGER
                                   ,p_messbody  CLOB
                                   ,p_messmeta  XMLType
                                   ,o_msgid     OUT VARCHAR2
                                   ,o_MSGCode   OUT INTEGER
                                   ,o_MSGText   OUT VARCHAR2
                                   ,o_messbody  OUT CLOB
                                   ,o_messmeta  OUT XMLType);

  -- BOSS-319 Cбор данных о клиенте, упаковки и отправки в Kafka сообщения с json содержащим данные для генерации уведомлениях по КИ - адаптер для RSL
  PROCEDURE SendInvestorStatus(p_ClientID         NUMBER,
                               p_NotificationType NUMBER,
                               p_NotificationDate DATE,
                               p_SignerID         NUMBER,
                               p_Reason           VARCHAR2,
                               p_ClientNameMod    VARCHAR2,
                               o_ErrorCode        OUT NUMBER,
                               o_ErrorDesc        OUT VARCHAR2);

  -- Формирование Header для KAFKA IPS и упаковка его в MessMETA                        
  function add_KafkaHeader_Xmessmeta(px_messmeta          xmltype default null
                                    ,p_List_dllvalues_dbt number -- Справочник статических header-ов
                                    ,p_traceid            varchar2 --  Уникальный идентификатор для трассировки, заполняется значением из GUID входящего запроса   
                                    ,p_requestid          varchar2 -- Уникальный идентификатор СОФР Да  GUID
                                    ,p_requesttime        timestamp default systimestamp -- Дата и время запроса  Да  
                                    ,p_templateparams     tt_templateparams -- Динамически изменяемые параметры (факсимиле)
                                    ,p_addparams          tt_addparams default null -- дополнительные параметры заголовка
                                    ,p_outputfilename     varchar2 -- Наименование файла Свободный формат без расширения. Недопустимо использование слеш-знаков (/ \ |). Ограничение наименования: 128 с.  Да  Пример: "Отчет брокера за период с 28.08.2024 по 29.08.2024"
                                    ,p_datafilename       varchar2 default null --Если null будет p_requestid.  Наименование файла - источника данных Передается в случае, если источник данных - s3 Значение может быть длиной не более 128 символов и содержать заглавные и строчные латинские буквы, цифры, дефисы, точки, подчеркивания  Условно нет 
                                    ) return xmltype ;
                               
  function add_templateparams(p_templateparams tt_templateparams default null
                            ,p_paramName      varchar2
                            ,p_paramType      varchar2 default 'FILE'
                            ,p_paramValue     varchar2) return tt_templateparams ;
                            
  function add_addparams(p_addparams tt_addparams default null
                        ,p_paramName      varchar2
                        ,p_paramValue     varchar2) return tt_addparams;

end it_ips_dfactory;
/