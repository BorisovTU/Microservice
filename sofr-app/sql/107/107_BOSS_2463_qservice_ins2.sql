begin
delete from itt_q_service t where upper(t.servicename) like 'LIMIT.WAPOSITIONPRICE';
insert into itt_q_service
  (message_type
  ,servicename
  ,service_proc
  ,service_caption
  ,service_price
  ,subscription
  ,stop_apostq)
values
  ('R'
  ,'Limit.WAPositionPrice'
  ,'it_limit.Limit_WAPositionPrice_parallel'
  ,'BIQ-16667 Сервис расчет цен приобритения'
  ,500
  ,0
  ,1);
insert into itt_q_service
  (message_type
  ,servicename
  ,service_proc
  ,service_caption
  ,service_price
  ,subscription
  ,stop_apostq)
values
  ('A'
  ,'Limit.WAPositionPrice'
  ,'it_limit.Limit_SecurLimitsFINISH'
  ,'BIQ-16667 Завершение расчета цен приобритения'
  ,50
  ,0
  ,0);
commit;
end;
/
