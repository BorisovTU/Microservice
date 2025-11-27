delete from itt_q_service t where upper(t.servicename) in ('LIMIT.COLLECTPLANSUMCUR')
/
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
  ,'Limit.CollectPlanSumCur'
  ,'it_limit.Limit_CollectPlanSumCur'
  ,'BOSS-5710 Сервис отбора требований и обязательств валютный рынок'
  ,1000
  ,1
  ,1)
/
