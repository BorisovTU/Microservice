begin
  delete itt_kafka_topic t
   where t.servicename in
         ('QUIK.LimitsNewInstrMon');
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
    ,'sofr.limits.NewInstMon'
    ,'BIQ-15498 Отправка поручения на ввод денежных средств  для автоматизации процесса обработки поручения и  корректировки лимитов по денежным средствам '
    ,'QUIK'
    ,'QUIK.LimitsNewInstrMon'
    ,'UpdateQuikLimitsNewInstrMonReq'
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
    ('IN'
    ,'quik.limits.NewInstMon'
    ,'BIQ-15498 Ответ  поручения на ввод денежных средств  для автоматизации процесса обработки поручения и  корректировки лимитов по денежным средствам '
    ,'QUIK'
    ,'QUIK.LimitsNewInstrMon'
    ,'UpdateQuikLimitsNewInstrMonResp'
    ,'JSON');
  commit;
end;
