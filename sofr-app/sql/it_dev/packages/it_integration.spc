create or replace package it_integration is

  /***************************************************************************************************\
   Интеграционные решения ИНТЕХ
   **************************************************************************************************
    Изменения:
   ---------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание 
   ----------  ---------------  ---------------------------   ----------------------------------------
   27.01.2025  Зыков М.В.       BOSS-7573                     Доработки QMessage в части взаимодействия с адаптером для S3
   05.09.2024  Зыков М.В.       BOSS-5212                     BOSS-1574.9 Доработка СОФР для передачи в sofr_qmngr_mq_adapter параметра кластера платформенной Kafka
   15.05.2024  Топорков Д.В.    BIQ-16474                     CCBO-9309. Добавление процедуры Condor_GetLastSOFRSequenceDeal
   25.03.2024  Зыков М.В.       BOSS-2688                     BOSS-575 СОФР. Доработка формирования отчетной формы "Справка 5798-У" (справка для гос. служащего). Доработка Q-Manager 
   23.10.2023  Зыков М.В.       BOSS-1230                     BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
   17.10.2023  Зыков М.В.       BOSS-358                      BIQ-13699.2. СОФР. Этап 2 - добавление файла ограничений по срочному рынку в обработку IPS
   05.09.2022  Зыков М.В.       BIQ-11358                     PRJ-2146 BIQ-11358 Добавление параметра LimitCount
   11.05.2022  Мелихова О.С.    BIQ-11358                     Создание
  \**************************************************************************************************/
  type t_rec_limit is record(
     id        number --ИД файла
    ,body_clob clob --содержимое  в формате clob
    ,xml_param clob -- Параметры 
    );

  type t_tab_limit is table of t_rec_limit;

  type t_rec_qmsg_kafka is record(
     QUEUENAME   varchar(128)
    ,QMSGID      raw(16) -- GUID сообщения в очереди
    ,CORRELATION varchar(128));

  type t_tab_qmsg_kafka is table of t_rec_qmsg_kafka;

  --------------------------------- ИНТЕГРАЦИЯ QUIK  -------------------------------------
  --Получение последнего файла лимита за указанную дату
  function get_file_limit(p_string in varchar2 -- Если дата   в формате 'dd.mm.yyyy'  "QUIK_LIMITS"
                          -- '<XML p_date="22.09.2023" p_file_code="QUIK_LIMITS_FORTS"/>
                          ) return t_tab_limit
    pipelined;

  --Сохранение файла лимита за указанную дату
  function ins_file_save(p_xml in clob) return t_tab_limit
    pipelined;

  --------------------------------- ИНТЕГРАЦИЯ КАФКА -------------------------------------
  --Получение списка сообщений для вычитки из QManager
  function qmanager_select_msg return t_tab_qmsg_kafka
    pipelined;

  -- Выгрузка сообщения из QManager УНИВЕРСАЛЬНАЯ 
  procedure qmanager_read_msg(p_wait_msg      in number --  Ожидание сообщения (сек)
                             ,p_QUEUENAME     in varchar2
                             ,p_QMSGID        in raw default null -- GUID Сообщениz в очереди 
                             ,o_kafka_cluster out varchar2 -- cluster
                             ,o_kafka_topic   out varchar2 -- Topc
                             ,o_msgID         out varchar2 -- GUID Сообщения
                             ,ocl_header      out clob -- Header 
                             ,ocl_message     out clob -- Body
                             ,o_ErrorCode     out number -- != 0 o_ErrorDesc или нет сообщений
                             ,o_ErrorDesc     out varchar2);

  -- Зарегистрировать ошибку сохранения сообщения  в транспортной системе
  procedure qmanager_read_msg_error(p_kafka_topic in varchar2
                                   ,p_msgID       in varchar2 -- GUID Сообщения
                                   ,p_ErrorCode   in number -- Код ошибки p_ErrorDesc 
                                   ,p_ErrorDesc   in varchar2);

  -- Загрузка сообщения  в QManager c Header
  procedure qmanager_load_msg(p_kafka_topic varchar2
                             ,p_GUID        varchar2 -- GUID сообшения в КАФКА
                             ,p_ESBDT       timestamp -- Врема записи сообщения в КАФКА
                             ,pcl_header    clob -- Header 
                             ,pcl_message   clob -- Body
                             ,o_ErrorCode   out number -- != 0 - Ошибка o_ErrorDesc
                             ,o_ErrorDesc   out varchar2);

  -- Выгрузка сообщения из QManager c Header
  procedure qmanager_read_msg(p_wait_msg    in number --  Ожидание сообщения (сек)
                             ,o_kafka_topic out varchar2 -- Topc
                             ,o_msgID       out varchar2 -- GUID Сообщения
                             ,ocl_header    out clob -- Header 
                             ,ocl_message   out clob -- Body
                             ,o_ErrorCode   out number -- != 0 o_ErrorDesc или нет сообщений
                             ,o_ErrorDesc   out varchar2);

  -- Выгрузка сообщения из QManager c Header и с Cluster
  procedure qmanager_read_msg(p_wait_msg      in number --  Ожидание сообщения (сек)
                             ,o_kafka_cluster out varchar2 -- cluster
                             ,o_kafka_topic   out varchar2 -- Topc
                             ,o_msgID         out varchar2 -- GUID Сообщения
                             ,ocl_header      out clob -- Header 
                             ,ocl_message     out clob -- Body
                             ,o_ErrorCode     out number -- != 0 o_ErrorDesc или нет сообщений
                             ,o_ErrorDesc     out varchar2);

  -- Выгрузка больщих сообщений из QManager через IPS S3 ( унмверсальная )
  procedure qmanager_read_msg_S3(p_wait_msg        in number --  Ожидание сообщения (сек)
                                ,p_QUEUENAME       varchar2
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

  -- Выгрузка больщих сообщений из QManager через IPS S3 
  procedure qmanager_read_msg_S3(p_wait_msg        in number --  Ожидание сообщения (сек)
                                ,o_kafka_cluster   out varchar2 -- cluster
                                ,o_kafka_topic     out varchar2
                                ,o_S3xdatafilename out varchar2
                                ,o_S3_point        out varchar2 -- точка интеграции S3
                                ,o_msgID           out varchar2 -- GUID Сообщения
                                ,ocl_header        out clob -- Header 
                                ,ocl_message       out clob -- Body
                                ,o_ErrorCode       out number -- = 0 ОК
                                ,o_ErrorDesc       out varchar2);

  -- процедура дублирования сообщения в очередь для отправки header в KAFKA после выгрузки боди в S3 ( костыль так как адаптер S3 не хочет отправлять хедеры в KAFKA )
  procedure S3_load_header_msg_KAFKA(p_msgid     itt_q_message_log.msgid%type -- GUID сообщения
                                   );

  /**
  Процедура получения номера следующего ID в таблице сделок из sequence, определяемого по i_SeqType:
   - "deal" для ddl_tick_dbt_seq
   - "ndeal" для ddvndeal_dbt_seq
  */
  PROCEDURE Condor_GetLastSOFRSequenceDeal(
                            p_ReqType      IN VARCHAR2,  -- Тип запроса (N - создание, U - изменение, D - удаление)
                            p_SecType      IN VARCHAR2,  -- Тип sequence
                            p_DealCode     IN VARCHAR2,  -- Код сделки. Поле входящей XML IFX->ProcessDeals_req->DealList->DealParm->IdentityDeal
                            p_RequestID    IN VARCHAR2,  -- ID запроса. Поле входящей XML IFX->ProcessDeals_req->?
                            o_SeqID        OUT NUMBER,   -- Значение последовательности для выбранного sequence
                            o_ErrorMessage OUT VARCHAR2);-- Текст возможной ошибки

end;
/
