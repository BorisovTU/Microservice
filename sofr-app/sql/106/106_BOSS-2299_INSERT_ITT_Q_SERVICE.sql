
Insert into ITT_Q_SERVICE
   (SERVICE_ID, MESSAGE_TYPE, SERVICENAME, SUBSCRIPTION, SERVICE_PROC, SERVICE_CAPTION, STOP_APOSTQ, SERVICE_PRICE, CALC_STAT_C, CALC_STAT_P)
 Values
   (0, 'R', 'CreateUpdateClientInfo', 0, 'IT_broker.ClientInfo', 
    'Boss-1642: Создание и обновление клиентов из СОФР в Брокер 2.0', 1, 1000, 0, 0)
/

                            
Insert into ITT_KAFKA_TOPIC
   (TOPIC_ID, QUEUETYPE, TOPIC_NAME, TOPIC_CAPTION, SYSTEM_NAME, SERVICENAME, ROOTELEMENT, MSG_FORMAT)
 Values
   (0, 'OUT', 'sofr.mq-adapter.client-info', 'Boss-1642: Создание и обновление клиентов из СОФР в Брокер 2.0', 'SINV', 
    'CreateUpdateClientInfo', 'CreateUpdateClientInfoReq', 'JSON')
/

                  