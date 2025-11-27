begin
  delete from itt_q_service t where upper(t.servicename) like 'LIMIT.%';
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
    ,'Limit.Start'
    ,'it_limit.Limit_Start'
    ,'BIQ-16667 Сервис старта расчета лимитов'
    ,50
    ,1
    ,0);
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
    ,'Limit.Begin'
    ,'it_limit.Limit_Begin'
    ,'BIQ-16667 Сервис начала расчета лимитов по площадке'
    ,5000
    ,1
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
    ('R'
    ,'Limit.FillContrTablenotDeriv'
    ,'it_limit.Limit_FCTnotDeriv_parallel'
    ,'BIQ-16667 Сервис формирование списка договоров по площадке'
    ,5000
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
    ('R'
    ,'Limit.FillContrTablebyDeriv'
    ,'it_limit.Limit_FCTbyDeriv'
    ,'BIQ-16667 Сервис формирование списка договоров (Deriv) по площадке'
    ,5000
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
    ,'Limit.FillContrTablenotDeriv'
    ,'it_limit.Limit_FCTAccStart'
    ,'BIQ-16667 Сервис расчета лимитов. Старт расчета данных по счетам'
    ,5000
    ,0
    ,0);
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
    ,'Limit.FillContrTablebyDeriv'
    ,'it_limit.Limit_FCTAccStart'
    ,'BIQ-16667 Сервис расчета лимитов. Старт расчета данных по счетам'
    ,5000
    ,0
    ,0);
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
    ,'Limit.FillContrTableAcc'
    ,'it_limit.Limit_FCTAcc_parallel'
    ,'BIQ-16667 Сервис расчета лимитов.Заполнение прочей информации '
    ,5000
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
    ('R'
    ,'Limit.FillContrTableCOM'
    ,'it_limit.Limit_FCTCOM'
    ,'BIQ-16667 Сервис расчета лимитов.Расчет сумм неоплаченных комиссий'
    ,5000
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
    ,'Limit.FillContrTableAcc'
    ,'it_limit.Limit_CheckContrTableStart'
    ,'BIQ-16667 Сервис расчета лимитов .Старт контроля данных по договорам'
    ,5000
    ,0
    ,0);
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
    ,'Limit.FillContrTableCOM'
    ,'it_limit.Limit_CheckContrTableStart'
    ,'BIQ-16667 Сервис расчета лимитов .Старт контроля данных по договорам'
    ,5000
    ,0
    ,0);
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
    ,'Limit.CheckContrTable'
    ,'it_limit.Limit_CheckContrTable_parallel'
    ,'BIQ-16667 Сервис расчета лимитов .Контроль данных по договорам'
    ,5000
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
    ,'Limit.CheckContrTable'
    ,'it_limit.Limit_SetStart'
    ,'BIQ-16667 Сервис Старт отбора лотов и сделок по площадке'
    ,5000
    ,0
    ,0);
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
    ,'Limit.LotTmpStart'
    ,'it_limit.Limit_LotTmpStart'
    ,'BIQ-16667 Сервис параллельгного старта отбора лотов'
    ,5000
    ,1
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
    ('R'
    ,'Limit.TickTmp'
    ,'it_limit.Limit_TickTmp'
    ,'BIQ-16667 Сервис расчета лимитов по площадке.Отбор сделок '
    ,5000
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
    ('R'
    ,'Limit.LotTmp'
    ,'it_limit.Limit_LotTmp_parallel'
    ,'BIQ-16667 ССервис расчета лимитов по площадке.Отбор лотов '
    ,5000
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
    ,'Limit.TickTmp'
    ,'it_limit.Limit_Create'
    ,'BIQ-16667 Сервис Запуск расчета димитов по площадке '
    ,5000
    ,0
    ,0);
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
    ,'Limit.LotTmp'
    ,'it_limit.Limit_Create'
    ,'BIQ-16667 Сервис Запуск расчета димитов по площадке'
    ,5000
    ,0
    ,0);
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
    ,'Limit.CashStockLimits'
    ,'it_limit.Limit_CashStockLimits'
    ,'BIQ-16667 Сервис Старт расчета лимитов MONEY фондовый рынок'
    ,5000
    ,1
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
    ('R'
    ,'Limit.CashStockLimByKind'
    ,'it_limit.Limit_CashStockLimByKind'
    ,'BIQ-16667 Сервис Расчет лимитов MONEY фондовый рынок'
    ,5000
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
    ,'Limit.CashStockLimByKind'
    ,'it_limit.Limit_CashStockLimByKindFINISH'
    ,'BIQ-16667 Сервис Окончание расчета лимитов MONEY фондовый рынок'
    ,5000
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
    ('R'
    ,'Limit.ClearSecurLimits'
    ,'it_limit.Limit_ClearSecurLimits'
    ,'BIQ-16667 Сервис Очистка расчетов лимитов DEPO фондовый рынок'
    ,5000
    ,1
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
    ('R'
    ,'Limit.SecurLimits'
    ,'it_limit.Limit_SecurLimits'
    ,'BIQ-16667 Сервис Старт расчет лимитов DEPO фондовый рынок'
    ,5000
    ,1
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
    ('R'
    ,'Limit.SecurLimByKind'
    ,'it_limit.Limit_SecurLimByKind'
    ,'BIQ-16667 Сервис Расчет лимитов DEPO фондовый рынок'
    ,5000
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
    ,'Limit.SecurLimByKind'
    ,'it_limit.Limit_SecurLimByKindFINISH'
    ,'BIQ-16667 Сервис Окончание Расчета лимитов DEPO фондовый рынок'
    ,5000
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
    ('R'
    ,'Limit.CashStockLimitsCur'
    ,'it_limit.Limit_CashStockLimitsCur'
    ,'BIQ-16667 Сервис Старт Расчет лимитов валютный рынок'
    ,5000
    ,1
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
    ('R'
    ,'Limit.CashStockLimitsCurByKind'
    ,'it_limit.Limit_CashStockLimCurByKind'
    ,'BIQ-16667 Сервис Расчет лимитов валютный рынок'
    ,5000
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
    ,'Limit.CashStockLimitsCurByKind'
    ,'it_limit.Limit_CashStockLimCurByKindFINISH'
    ,'BIQ-16667 Сервис Окончание Расчет лимитов валютный рынок'
    ,5000
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
    ('R'
    ,'Limit.FutureMarkLimits'
    ,'it_limit.Limit_FutureMarkLimits'
    ,'BIQ-16667 Сервис Старт расчета по срочному рынку'
    ,5000
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
    ,'Limit.FutureMarkLimits'
    ,'it_limit.Limit_FutureMarkLimitsFINISH'
    ,'BIQ-16667 Сервис Окончание расчета по срочному рынку'
    ,5000
    ,0
    ,0);
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
    ,'Limit.ClearLimitsEDP'
    ,'it_limit.Limit_ClearLimitsEDP'
    ,'BIQ-16667 Сервис очистки лимитов ЕДП'
    ,5000
    ,1
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
    ('R'
    ,'Limit.CashEDPLimits'
    ,'it_limit.Limit_CashEDPLimits'
    ,'BIQ-16667 Сервис Старт расчета лимитов MONEY ЕДП'
    ,5000
    ,1
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
    ('R'
    ,'Limit.CashEDPLimByKind'
    ,'it_limit.Limit_CashEDPLimByKind'
    ,'BIQ-16667 Сервис Расчет лимитов MONEY ЕДП'
    ,5000
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
    ,'Limit.CashEDPLimByKind'
    ,'it_limit.Limit_CashEDPLimByKindFINISH'
    ,'BIQ-16667 Сервис Окончание расчета лимитов MONEY ЕДП'
    ,5000
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
    ('R'
    ,'Limit.CashEDPLimitsCur'
    ,'it_limit.Limit_CashEDPLimitsCur'
    ,'BIQ-16667 Сервис Старт Расчет лимитов валютный рынок ЕДП'
    ,5000
    ,1
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
    ('R'
    ,'Limit.CashEDPLimitsCurByKind'
    ,'it_limit.Limit_CashEDPLimCurByKind'
    ,'BIQ-16667 Сервис Расчет лимитов валютный рынок ЕДП'
    ,5000
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
    ,'Limit.CashEDPLimitsCurByKind'
    ,'it_limit.Limit_CashEDPLimCurByKindFINISH'
    ,'BIQ-16667 Сервис Окончание Расчет лимитов валютный рынок ЕДП '
    ,5000
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
    ('R'
    ,'Limit.CashFINISH'
    ,'it_limit.Limit_CashFINISH'
    ,'BIQ-16667 Сервис Завершения расчета Cash лимитов'
    ,5000
    ,1
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
    ('R'
    ,'Limit.FINISH'
    ,'it_limit.Limit_FINISH'
    ,'BIQ-16667 Сервис Завершения расчета '
    ,5000
    ,1
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
    ('R'
    ,'Limit.Cleaner'
    ,'it_limit.Limit_Cleaner'
    ,'BIQ-16667 Сервис очистки промежуточных расчетов '
    ,5000
    ,1
    ,1);
  commit;
end;
/
