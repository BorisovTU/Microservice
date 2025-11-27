declare 
   v_cnt number(10);
   v_rec_topic ITT_KAFKA_TOPIC%rowtype;
   v_rec_service ITT_Q_SERVICE%rowtype;
begin 
   v_rec_topic.TOPIC_ID       := 0;
   v_rec_topic.QUEUETYPE      := 'IN';
   v_rec_topic.TOPIC_NAME     := 'dias.sofr.allocated-coupon';
   v_rec_topic.TOPIC_CAPTION  := 'BIQ-16598 Запрос УНКД из Диасофта';
   v_rec_topic.SYSTEM_NAME    := 'DIASOFT';
   v_rec_topic.SERVICENAME    := 'Diasoft.GetAllocatedCouponInfo';
   v_rec_topic.ROOTELEMENT    := 'GetAllocatedCouponInfoReq';
   v_rec_topic.MSG_FORMAT     := 'XML';
   v_rec_topic.CREATE_SYSDATE := sysdate;
   v_rec_topic.UPDATE_TIME    := sysdate;
   v_rec_topic.MSG_PARAM      := 'http://www.rshb.ru/csm/sofr/get_allocated_coupon_info/202404/req';
   
   select count(*) into v_cnt
     from ITT_KAFKA_TOPIC
   where upper(QUEUETYPE) = upper(v_rec_topic.QUEUETYPE)
     and upper(SYSTEM_NAME) = upper(v_rec_topic.SYSTEM_NAME)
     and upper(SERVICENAME) = upper(v_rec_topic.SERVICENAME);
     
   if v_cnt = 0 then
      insert into ITT_KAFKA_TOPIC values v_rec_topic;
   end if;
   
   v_rec_service.SERVICE_ID      := 0;
   v_rec_service.MESSAGE_TYPE    := 'R';
   v_rec_service.SERVICENAME     := v_rec_topic.SERVICENAME;
   v_rec_service.SUBSCRIPTION    := 1;
   v_rec_service.SERVICE_PROC    := 'it_Diasoft.GetAllocatedCouponInfoReq';
   v_rec_service.SERVICE_CAPTION := v_rec_topic.TOPIC_CAPTION;
   v_rec_service.STOP_APOSTQ     := 1;
   v_rec_service.SERVICE_PRICE   := 1000;
   v_rec_service.CALC_STAT_C     := 0;
   v_rec_service.CALC_STAT_P     := 0;
   
   select count(*) into v_cnt
     from ITT_Q_SERVICE
    where upper(MESSAGE_TYPE) = v_rec_service.MESSAGE_TYPE and upper(SERVICENAME) = upper(v_rec_topic.SERVICENAME);
    
   if v_cnt = 0 then
      Insert into ITT_Q_SERVICE values v_rec_service;
   end if;
   
   v_rec_topic.QUEUETYPE          := 'OUT';
   v_rec_topic.TOPIC_NAME     := 'ips.sofr-dias.allocated-coupon';
   v_rec_topic.TOPIC_CAPTION  := 'BIQ-16598 Передача УНКД по запросу из Диасофта';
   v_rec_topic.ROOTELEMENT    := 'GetAllocatedCouponInfoResp';
   v_rec_topic.MSG_PARAM      := 'http://www.rshb.ru/csm/sofr/get_allocated_coupon_info/202404/resp';
   
   select count(*) into v_cnt
     from ITT_KAFKA_TOPIC
   where upper(QUEUETYPE) = upper(v_rec_topic.QUEUETYPE)
     and upper(SYSTEM_NAME) = upper(v_rec_topic.SYSTEM_NAME)
     and upper(SERVICENAME) = upper(v_rec_topic.SERVICENAME);
     
   if v_cnt = 0 then
      insert into ITT_KAFKA_TOPIC values v_rec_topic;
   end if;
   
   commit;
exception
   when others then 
      it_error.put_error_in_stack;
      it_log.log( p_msg => 'exception'
                  , p_msg_type => it_log.C_MSG_TYPE__ERROR
                  );
      it_error.clear_error_stack;
end;