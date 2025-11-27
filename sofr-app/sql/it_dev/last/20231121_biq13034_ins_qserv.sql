begin
  delete ITT_Q_SERVICE s where s.servicename in ('Diasoft.SendPkoInfo','Diasoft.SendPkoStatusResult');
  insert into itt_q_service
    (message_type,
     servicename,
     subscription,
     service_proc,
     service_caption,
     stop_apostq,
     service_price)
  values
    ('R',
     'Diasoft.SendPkoInfo',
     1,
     'it_Diasoft.SendPkoInfo',
     'BIQ-13034 Загрузка информации по поручению кастодиальной операции на списание и зачисление ценных бумаг',
     0,
     100);

  insert into itt_q_service
    (message_type,
     servicename,
     subscription,
     service_proc,
     service_caption,
     stop_apostq,
     service_price)
  values
    ('R',
     'Diasoft.SendPkoStatusResult',
     0,
     'it_Diasoft.SendPkoStatusResult',
     'BIQ-13034 BOSS-1473 Сервис получения сообщения по упрощенной технологии',
     0,
     100);
  commit;
end;
