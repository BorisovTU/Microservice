begin
    update ITT_KAFKA_TOPIC
        set topic_caption = 'BIQ-16598 Запрос УНКД из Диасофта'
    where  queuetype = 'IN' and system_name = 'DIASOFT' and servicename = 'Diasoft.GetAllocatedCouponInfo';

    update ITT_KAFKA_TOPIC
        set topic_caption = 'BIQ-16598 Передача УНКД по запросу из Диасофта'
    where  queuetype = 'OUT' and system_name = 'DIASOFT' and servicename = 'Diasoft.GetAllocatedCouponInfo';

end;