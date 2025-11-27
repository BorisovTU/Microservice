begin
  delete from itt_q_corrsystem s where lower(s.system_name) = lower('FRONTSYSTEMS');
  insert into itt_q_corrsystem
    (system_name
    ,system_caption
    ,out_pack_message_proc)
  values
    ('FRONTSYSTEMS'
    ,'Фронтальные системы (ДБО ФЛ, ЕФР, ДБО ЮЛ) (обмен сообщений через KAFKA)'
    ,'it_frontsystems.out_pack_message');

  delete itt_kafka_topic t where t.servicename in ('FrontSystems.SendNonTradingOrder', 'FrontSystems.SendNonTradingOrderStatus');
  insert into itt_kafka_topic
    (queuetype
    ,topic_name
    ,topic_caption
    ,system_name
    ,servicename
    ,rootelement
    ,msg_format)
  values
    ('IN'
    ,'sofr.send-nontrading-order-request'
    ,'BIQ-13121 Сервис загрузки неторгового поручения клиента на вывод/перевод дс'
    ,'FRONTSYSTEMS'
    ,'FrontSystems.SendNonTradingOrder'
    ,'SendNonTradingOrderReq'
    ,'JSON');

  insert into itt_kafka_topic
    (queuetype
    ,topic_name
    ,topic_caption
    ,system_name
    ,servicename
    ,rootelement
    ,msg_format)
  values
    ('OUT'
    ,'sofr.send-nontrading-order-answer'
    ,'BIQ-13121 Результат загрузки неторгового поручения клиента на вывод/перевод дс'
    ,'FRONTSYSTEMS'
    ,'FrontSystems.SendNonTradingOrder'
    ,'SendNonTradingOrderResp'
    ,'JSON');

  insert into itt_kafka_topic
    (QUEUETYPE
    ,TOPIC_NAME
    ,TOPIC_CAPTION
    ,SYSTEM_NAME
    ,SERVICENAME
    ,ROOTELEMENT
    ,MSG_FORMAT)
  values
    ('OUT'
    ,'sofr.send-nontrading-order-status-request'
    ,'BIQ-13121 Отправка статуса обработки неторгового поручения клиента на вывод/перевод дс'
    ,'FRONTSYSTEMS'
    ,'FrontSystems.SendNonTradingOrderStatus'
    ,'SendNonTradingOrderStatusReq'
    ,'JSON');

  insert into itt_kafka_topic
    (QUEUETYPE
    ,TOPIC_NAME
    ,TOPIC_CAPTION
    ,SYSTEM_NAME
    ,SERVICENAME
    ,ROOTELEMENT
    ,MSG_FORMAT)
  values
    ('IN'
    ,'sofr.send-nontrading-order-status-answer'
    ,'BIQ-13121 Ответ о получении статуса обработки неторгового поручения клиента на вывод/перевод дс'
    ,'FRONTSYSTEMS'
    ,'FrontSystems.SendNonTradingOrderStatus'
    ,'SendNonTradingOrderStatusResp'
    ,'JSON');
 
 delete itt_q_service t where t.servicename in ('FrontSystems.SendNonTradingOrder', 'FrontSystems.SendNonTradingOrderStatus','ExecuteCode');

  insert into itt_q_service (message_type,
                             servicename,
                             subscription,
                             service_proc,
                             service_caption,
                             stop_apostq)
  values
    ('R'
    ,'FrontSystems.SendNonTradingOrder'
    ,0
    ,'it_frontsystems.save_req_from_json'
    ,'BIQ-13121 Сервис загрузки неторгового поручения клиента на вывод/перевод дс'
    ,0);

 insert into itt_q_service (message_type,
                             servicename,
                             subscription,
                             service_proc,
                             service_caption,
                             stop_apostq)
  values
    ('A'
    ,'FrontSystems.SendNonTradingOrderStatus'
    ,0
    ,'it_frontsystems.resp_status_message_json'
    ,'BIQ-13121 Обработка ответа о получении статуса обработки неторгового поручения клиента на вывод/перевод дс'
    ,0);

  insert into itt_q_service (message_type,
                             servicename,
                             subscription,
                             service_proc,
                             service_caption,
                             stop_apostq)
  values
    ('R'
    ,'ExecuteCode'
    ,1
    ,'it_q_service.ExecuteCode'
    ,'Сервис запуска PL/SQL кода QWorkerом '
    ,1);


end;
/