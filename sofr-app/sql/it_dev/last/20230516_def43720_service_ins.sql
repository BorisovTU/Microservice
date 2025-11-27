begin
  delete from itt_q_service t
   where t.message_type = 'R'
     and t.servicename like 'EVENT.GETINFO(%)';
  insert into itt_q_service
    (message_type
    ,servicename
    ,service_proc
    ,service_caption
    ,service_price
    ,stop_apostq)
  values
    ('R'
    ,'EVENT.GETINFO(QMANAGER)'
    ,'it_event_utils.GetINFO_QMANAGER'
    ,'Формирования сообщений для мониторинга QMANAGERа'
    ,50
    ,0);
  insert into itt_q_service
    (message_type
    ,servicename
    ,service_proc
    ,service_caption
    ,service_price
    ,stop_apostq)
  values
    ('R'
    ,'EVENT.GETINFO(SOFR)'
    ,'it_event_utils.GetINFO_SOFR'
    ,'Формирования сообщений для мониторинга SOFRа'
    ,50
    ,0);
  insert into itt_q_service
    (message_type
    ,servicename
    ,service_proc
    ,service_caption
    ,service_price
    ,stop_apostq)
  values
    ('R'
    ,'EVENT.GETINFO(OTHERS)'
    ,'it_event_utils.GetINFO_OTHERS'
    ,'Формирования сообщений для мониторинга систем не вошедших в список'
    ,50
    ,0);
  commit;
end;
/
