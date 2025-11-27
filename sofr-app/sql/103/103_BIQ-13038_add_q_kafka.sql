BEGIN
    INSERT INTO ITT_KAFKA_TOPIC (QUEUETYPE,
                                 TOPIC_NAME,
                                 TOPIC_CAPTION,
                                 SYSTEM_NAME,
                                 SERVICENAME,
                                 ROOTELEMENT,
                                 MSG_FORMAT)
             VALUES (
                        'IN',
                        'maks.transfer-rates.universal-json',
                        'BIQ-13038 Получение трансферных процентных ставок',
                        'MAKS',
                        'MAKS.TransferRates',
                        '',
                        'SUBSCR');

    INSERT INTO ITT_Q_SERVICE (MESSAGE_TYPE,
                               SERVICENAME,
                               SUBSCRIPTION,
                               SERVICE_PROC,
                               SERVICE_CAPTION)
             VALUES (
                        'R',
                        'MAKS.TransferRates',
                        1,
                        'IT_TransferRates.SetRates',
                        'BIQ-13038. Загрузка данных о трансферных процентных ставках');
END;
/