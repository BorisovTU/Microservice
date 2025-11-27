begin
  delete itt_kafka_topic t where t.servicename in ('Diasoft.SendPkoInfo', 'Diasoft.SendPkoStatus', 'Diasoft.SendPkoStatusResult');
  insert into itt_kafka_topic
    (QUEUETYPE
    ,TOPIC_NAME
    ,TOPIC_CAPTION
    ,SYSTEM_NAME
    ,SERVICENAME
    ,ROOTELEMENT
    ,MSG_FORMAT
    ,msg_param)
  values
    ('IN'
    ,'diasoft.sofr.pko-info.req'
    ,'BIQ-13034 Сервис загрузки информации по поручению кастодиальной операции на списание и зачисление ценных бумаг'
    ,'DIASOFT'
    ,'Diasoft.SendPkoInfo'
    ,'SendPkoInfoReq'
    ,'XML'
    ,'http://www.rshb.ru/csm/sofr/send_pko_info/202310/req');
  insert into itt_kafka_topic
    (QUEUETYPE
    ,TOPIC_NAME
    ,TOPIC_CAPTION
    ,SYSTEM_NAME
    ,SERVICENAME
    ,ROOTELEMENT
    ,MSG_FORMAT
    ,msg_param)
  values
    ('OUT'
    ,'sofr.diasoft.pko-info.resp'
    ,'BIQ-13034 Сервис загрузки информации по поручению кастодиальной операции на списание и зачисление ценных бумаг'
    ,'DIASOFT'
    ,'Diasoft.SendPkoInfo'
    ,'SendPkoInfoResp'
    ,'XML'
    ,'http://www.rshb.ru/csm/diasoft/send_pko_info/202310/resp');
  insert into itt_kafka_topic
    (QUEUETYPE
    ,TOPIC_NAME
    ,TOPIC_CAPTION
    ,SYSTEM_NAME
    ,SERVICENAME
    ,ROOTELEMENT
    ,MSG_FORMAT
    ,msg_param)
  values
    ('OUT'
    ,'sofr.diasoft.pko-info-status.req'
    ,'BIQ-13034 Сервис получения статуса достаточности лимитов для проведения кастодиальной операции по списанию цб'
    ,'DIASOFT'
    ,'Diasoft.SendPkoStatus'
    ,'SendPkoStatusReq'
    ,'XML'
    ,'http://www.rshb.ru/csm/diasoft/send_pko_status/202310/req');
  insert into itt_kafka_topic
    (QUEUETYPE
    ,TOPIC_NAME
    ,TOPIC_CAPTION
    ,SYSTEM_NAME
    ,SERVICENAME
    ,ROOTELEMENT
    ,MSG_FORMAT
    ,msg_param)
  values
    ('IN'
    ,'diasoft.sofr.pko-info-status-result.req'
    ,'BIQ-13034 Сервис изменения статуса поручения кастодиальной операции по списанию ценных бумаг'
    ,'DIASOFT'
    ,'Diasoft.SendPkoStatusResult'
    ,'SendPkoStatusResultReq'
    ,'XML'
    ,'http://www.rshb.ru/csm/sofr/send_pko_status_result/202310/req');
  insert into itt_kafka_topic
    (QUEUETYPE
    ,TOPIC_NAME
    ,TOPIC_CAPTION
    ,SYSTEM_NAME
    ,SERVICENAME
    ,ROOTELEMENT
    ,MSG_FORMAT
    ,msg_param)
  values
    ('OUT'
    ,'sofr.diasoft.pko-info-status-result.resp'
    ,'BIQ-13034 Сервис изменения статуса поручения кастодиальной операции по списанию ценных бумаг'
    ,'DIASOFT'
    ,'Diasoft.SendPkoStatusResult'
    ,'SendPkoStatusResultResp'
    ,'XML'
    ,'http://www.rshb.ru/csm/diasoft/send_pko_status_result/202310/resp');
  commit;
end;
