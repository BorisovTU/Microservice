-- Наполнение таблиц

declare
    vcnt number;
begin
   select count(*) into vcnt from ITT_KAFKA_TOPIC where upper(SYSTEM_NAME) = 'DIASOFT' and upper(SERVICENAME) = 'DIASOFT.SENDDEPOPAYMENTINFOREQ'
                                                    and QUEUETYPE = 'IN';
   if vcnt = 0 then
      Insert into ITT_KAFKA_TOPIC
        (QUEUETYPE, TOPIC_NAME, TOPIC_CAPTION, SYSTEM_NAME, SERVICENAME, ROOTELEMENT, MSG_FORMAT, MSG_PARAM)
      Values
        ('IN', 'ips.dias-sofr.ndfl', 'BIQ-13198 Сервис загрузки депозитарных данных из диасофт', 'DIASOFT', 
         'test', 'SendDepoPaymentInfoReq', 'XML', 'http://www.rshb.ru/csm/sofr/send_depo_payment_info/202308/req');
      Insert into ITT_Q_SERVICE
         (MESSAGE_TYPE, SERVICENAME, SUBSCRIPTION, SERVICE_PROC, SERVICE_CAPTION, STOP_APOSTQ, SERVICE_PRICE, CALC_STAT_C, CALC_STAT_P)
       Values
         ('R', 'Diasoft.SendDepoPaymentInfoReq', 1, 'RSB_DIASOFT.SendDepoPaymentInfoReq', 
          'Сервис загрузки депозитарных данных из диасофт', 0, 23, 0, 0);
   end if;
end;
/