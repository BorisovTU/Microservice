begin
  delete itt_kafka_topic t where t.servicename in ('ndbole.SOFRSendBillDeal');
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
    ,'sofr.send-bill-deal-request'
    ,'BIQ-18679 Новая интеграция СОФР-ДБО ЮЛ по выгрузке параметров сделок с собственными векселями'
    ,'NDBOLE'
    ,'ndbole.SOFRSendBillDeal'
    ,'SOFRSendBillDealReq'
    ,'JSON');

  delete itt_kafka_topic t where t.servicename in ('ndbole.SOFRSendBill');
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
    ,'sofr.send-bill-request'
    ,'BIQ-18679 Новая интеграция СОФР-ДБО ЮЛ по выгрузке параметров сделок с собственными векселями'
    ,'NDBOLE'
    ,'ndbole.SOFRSendBill'
    ,'SOFRSendBillReq'
    ,'JSON');
end;
/