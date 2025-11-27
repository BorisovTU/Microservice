Insert into ITT_Q_SERVICE
   (SERVICE_ID, MESSAGE_TYPE, SERVICENAME, SUBSCRIPTION, SERVICE_PROC, SERVICE_CAPTION, STOP_APOSTQ, SERVICE_PRICE, CALC_STAT_C, CALC_STAT_P)
 Values
   (0, 'R', 'EFR.SendStateOfficerSertSofr', 0, 'IT_StateOfficerSert.GetReferenceFromEFR', 
    'BIQ-18375 Автоматизация отчетной формы "Справка 5798-У" (справка для гос. служащего)', 1, 1000, 0, 0)
/

Insert into ITT_KAFKA_TOPIC
   (TOPIC_ID, QUEUETYPE, TOPIC_NAME, TOPIC_CAPTION, SYSTEM_NAME, SERVICENAME, ROOTELEMENT, MSG_FORMAT, MSG_PARAM)
 Values
   (0, 'IN', 'ips.efr-sofr.state-officer-sert', 'BIQ-18375 Автоматизация отчетной формы "Справка 5798-У" (справка для гос. служащего)_запрос', 'EFR', 
    'EFR.SendStateOfficerSertSofr', 'SendStateOfficerSertSofrReq', 'XML', 'http://www.rshb.ru/csm/sofr/send_state_officer_sert_sofr/202403/req')
/

Insert into ITT_KAFKA_TOPIC
   (TOPIC_ID, QUEUETYPE, TOPIC_NAME, TOPIC_CAPTION, SYSTEM_NAME, SERVICENAME, ROOTELEMENT, MSG_FORMAT, MSG_PARAM)
 Values
   (0, 'OUT', 'ips.sofr-efr.state-officer-sert', 'BIQ-18375 Автоматизация отчетной формы "Справка 5798-У" (справка для гос. служащего)_ответ', 'EFR', 
    'EFR.SendStateOfficerSertSofr', 'ResendStateOfficerSertEfrReq', 'XML', 'http://www.rshb.ru/csm/efr/resend_state_officer_sert_efr/202403/req')
/

Insert into ITT_Q_CORRSYSTEM
   (SYSTEM_NAME, SYSTEM_CAPTION, OUT_PACK_MESSAGE_PROC)
 Values
   ('EFR', 'ЕФР (обмен сообщений через KAFKA)', 'it_efr.out_pack_message')
/

 
 
 