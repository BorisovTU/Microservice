begin
  delete ITT_Q_SERVICE s
   where s.servicename in ('QUIK.LimitsNewInstMon'
                          ,'EVENT.REGISTER(KAFKA.QManager_load_msg)'
                          ,'EVENT.REGISTER(KAFKA.QManager_read_msg)');
  insert into itt_q_service
    (message_type
    ,servicename
    ,subscription
    ,service_proc
    ,service_caption
    ,stop_apostq
    ,service_price)
  values
    ('A'
    ,'QUIK.LimitsNewInstrMon'
    ,0
    ,'it_quik.LimitsNewInstrMonResp'
    ,'BIQ-15498 Обработка ответа поручения на ввод денежных средств  для автоматизации процесса обработки поручения и  корректировки лимитов по денежным средствам '
    ,0
    ,100);
  insert into itt_q_service
    (message_type
    ,servicename
    ,subscription
    ,service_proc
    ,service_caption
    ,stop_apostq
    ,service_price)
  values
    ('R'
    ,'EVENT.REGISTER(KAFKA.QManager_load_msg)'
    ,1
    ,'it_q_service.MONITOR_Erase_SPAMError'
    ,'Очистка оповещений мониторинга от дублирующей информации'
    ,0
    ,20);
  insert into itt_q_service
    (message_type
    ,servicename
    ,subscription
    ,service_proc
    ,service_caption
    ,stop_apostq
    ,service_price)
  values
    ('R'
    ,'EVENT.REGISTER(KAFKA.QManager_read_msg)'
    ,1
    ,'it_q_service.MONITOR_Erase_SPAMError'
    ,'Очистка оповещений мониторинга от дублирующей информации'
    ,0
    ,20);
  commit;
end;
