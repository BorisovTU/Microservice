create or replace package it_kafka is

  /***************************************************************************************************\
    Пакет для работы QManagera c KAFKA
   **************************************************************************************************
    Изменения:
   ---------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание 
   ----------  ---------------  ---------------------------   ----------------------------------------
   04.02.2025  Зыков М.В.       BOSS-7625                     Разработка нового формата входных сообщений из IPS
   27.01.2025  Зыков М.В.       BOSS-7573                     Доработки QMessage в части взаимодействия с адаптером для S3
   05.09.2024  Зыков М.В.       BOSS-5212                     BOSS-1574.9 Доработка СОФР для передачи в sofr_qmngr_mq_adapter параметра кластера платформенной Kafka
   25.03.2024  Зыков М.В.       BOSS-2688                     BOSS-575 СОФР. Доработка формирования отчетной формы "Справка 5798-У" (справка для гос. служащего). Доработка Q-Manager 
   23.10.2023  Зыков М.В.       BOSS-1230                     BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
  \**************************************************************************************************/
  -----
  C_C_MSG_FORMAT_XML            constant itt_kafka_topic.msg_format%type := 'XML'; -- XML с rootelement из itt_kafka_topic.rootelement и тегами /GUID ,/GUIDReq и т.д.
  C_C_MSG_FORMAT_XDIASOFT_TAXES constant itt_kafka_topic.msg_format%type := 'XDIASOFT_TAXES'; -- XML по DEF-76759
  C_C_MSG_FORMAT_JSON           constant itt_kafka_topic.msg_format%type := 'JSON'; -- JSON с rootelement из itt_kafka_topic.rootelement и тегами /GUID ,/GUIDReq и т.д.
  C_C_MSG_FORMAT_SUBSCR_TR      constant itt_kafka_topic.msg_format%type := 'SUBSCR'; -- просто сообщение без ответа в один поток исполнения
  C_C_MSG_FORMAT_SUBSCR_PL      constant itt_kafka_topic.msg_format%type := 'SUBSCR+'; -- просто сообщение без ответа в параллельное исполнения
  C_C_MSG_FORMAT_JIPS           constant itt_kafka_topic.msg_format%type := 'JIPS'; -- JSON из IPS параметры в Heder.
  ----- 
  -- Возвращает значение пакетной переменной по имени
  function get_constant_str(p_constant varchar2) return varchar2 deterministic;

  -- Загрузка сообщения в QManager
  procedure qmanager_load_msg(p_kafka_topic varchar2
                             ,p_GUID        varchar2
                             ,p_ESBDT       timestamp
                             ,pcl_header    clob default null -- Header 
                             ,pcl_message   clob -- Body
                             ,o_ErrorCode   out number
                             ,o_ErrorDesc   out varchar2);

  -- Выгрузка сообщения из QManager
  procedure qmanager_read_msg(p_wait_msg      in number --  Ожидание сообщения (сек)
                             ,p_QUEUENAME     varchar2 default null
                             ,p_QMSGID        in raw default null -- GUID Сообщениz в очереди 
                             ,o_kafka_cluster out varchar2 -- cluster
                             ,o_kafka_topic   out varchar2
                             ,o_msgID         out varchar2 -- GUID Сообщения
                             ,ocl_header      out clob -- Header 
                             ,ocl_message     out clob -- Body
                             ,o_ErrorCode     out number -- = 0 ОК
                             ,o_ErrorDesc     out varchar2);

  -- Выгрузка больщих сообщений из QManager через IPS S3
  procedure qmanager_read_msg_S3(p_wait_msg        in number --  Ожидание сообщения (сек)
                                ,p_QUEUENAME       varchar2 default null
                                ,p_QMSGID          in raw default null -- GUID Сообщениz в очереди 
                                ,o_kafka_cluster   out varchar2 -- cluster
                                ,o_kafka_topic     out varchar2
                                ,o_S3xdatafilename out varchar2
                                ,o_S3_point        out varchar2 -- точка интеграции S3
                                ,o_msgID           out varchar2 -- GUID Сообщения
                                ,ocl_header        out clob -- Header 
                                ,ocl_message       out clob -- Body
                                ,o_ErrorCode       out number -- = 0 ОК
                                ,o_ErrorDesc       out varchar2);

  -- Зарегистрировать ошибку сохранения сообщения  в транспортной системе
  procedure qmanager_read_msg_error(p_kafka_topic in varchar2
                                   ,p_msgID       in varchar2 -- GUID Сообщения
                                   ,p_ErrorCode   in number -- Код ошибки p_ErrorDesc 
                                   ,p_ErrorDesc   in varchar2);

  -- процедура размещения сообщения в очередь для отправки через KAFKA
  procedure load_msg(io_msgid       in out itt_q_message_log.msgid%type -- GUID сообщения
                    ,p_message_type itt_q_message_log.message_type%type -- Тип сообщения R или A Запрос/ответ 
                    ,p_ServiceName  itt_q_message_log.servicename%type -- Бизнес-процесс
                    ,p_Receiver     itt_q_message_log.receiver%type -- Система-получатель
                    ,p_MESSBODY     clob -- Бизнес - составляющая сообщения
                    ,o_ErrorCode    out number -- != 0 ошибка o_ErrorDesc
                    ,o_ErrorDesc    out varchar2
                    ,p_CORRmsgid    itt_q_message_log.corrmsgid%type default null -- GUID связанного сообщения 
                    ,p_MSGCode      integer default 0 -- Код результата обработки сообщения. 0 - успех
                    ,p_MSGText      itt_q_message_log.msgtext%type default null -- Текст ошибки, возникший при обработке сообщен
                    ,p_comment      itt_q_message_log.commenttxt%type default null -- коментарии в лог 
                    ,p_MessMETA     xmltype default null -- XML Метаданные сообщения
                    ,p_isquery      pls_integer default null -- сообщение с ожиданием ответа (1/0
                     );

  function get_namespace(p_system_name itt_kafka_topic.system_name%type
                        ,p_rootelement itt_kafka_topic.rootelement%type) return varchar2 deterministic;

  -- Возвращает MessMeta c встроеным KafkaHeader
  function add_Header_Xmessmeta(p_Header    varchar2
                               ,px_messmeta xmltype default null) return xmltype;

  -- Возвращает MessMeta c встроенным S3 x-data-file-name
  function add_S3xdatafilename_Xmessmeta(p_S3xdatafilename varchar2
                                        ,px_messmeta       xmltype default null) return xmltype;

  -- Из MessMeta  KafkaHeader S3x-data-file-name 
  procedure get_info_Xmessmeta(px_messmeta       xmltype
                              ,ocl_Нeader       out clob
                              ,o_S3xdatafilename out varchar2);

  -- Проверка маршрута через KAFKA S3 1- OK ( Если 0 и o_ErrorCode = 0 то KAFKA без S3) 
  -- 20901 - Отправка сообщения >= 10 MБ через KAFKA невозможна !
  -- 20902 - Отправка сообщения > 1MБ в ?????? через KAFKA невозможна Необходимо настроить IPS S3 для ??????
  function chk_from_KAFKAS3(p_Receiver     itt_q_message_log.receiver%type
                           ,p_len_MessBODY number -- dbms_lob.getlength(lob_loc => MessBODY)
                           ,o_ErrorCode    out integer
                           ,o_ErrorDesc    out varchar2) return pls_integer;

  function get_correlation(p_Receiver     itt_q_message_log.receiver%type
                          ,p_len_MessBODY number) return varchar2;

  -- процедура размещения сообщения в очередь для отправки через S3 в KAFKA
  procedure load_msg_S3(p_msgid        itt_q_message_log.msgid%type -- GUID сообщения
                       ,p_message_type itt_q_message_log.message_type%type -- Тип сообщения R или A Запрос/ответ 
                       ,p_ServiceName  itt_q_message_log.servicename%type -- Бизнес-процесс
                       ,p_Receiver     itt_q_message_log.receiver%type -- Система-получатель
                       ,p_MESSBODY     clob -- Бизнес - составляющая сообщения
                       ,p_MessMETA     xmltype -- XML Метаданные сообщения
                       ,o_ErrorCode    out number -- != 0 ошибка o_ErrorDesc
                       ,o_ErrorDesc    out varchar2
                       ,p_CORRmsgid    itt_q_message_log.corrmsgid%type default null -- GUID связанного сообщения 
                       ,p_MSGCode      integer default 0 -- Код результата обработки сообщения. 0 - успех
                       ,p_MSGText      itt_q_message_log.msgtext%type default null -- Текст ошибки, возникший при обработке сообщен
                       ,p_isquery      pls_integer default null -- сообщение с ожиданием ответа (1/0
                       ,p_comment      itt_q_message_log.commenttxt%type default null -- коментарии в лог 
                        );

  -- процедура дублирования сообщения в очередь для отправки header в KAFKA после выгрузки боди в S3 ( костыль так как адаптер S3 не хочет отправлять хедеры в KAFKA )
  procedure S3_load_header_msg_KAFKA(p_msgid     itt_q_message_log.msgid%type -- GUID сообщения
                                   );

end it_kafka;
/
