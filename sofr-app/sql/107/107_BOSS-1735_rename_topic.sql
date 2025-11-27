begin
    update ITT_KAFKA_TOPIC
        set topic_name = 'ips.dias-sofr.allocated-coupon'
    where  queuetype = 'IN' and system_name = 'DIASOFT' and servicename = 'Diasoft.GetAllocatedCouponInfo';

end;