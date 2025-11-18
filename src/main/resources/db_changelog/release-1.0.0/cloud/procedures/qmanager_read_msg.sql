-- Процедура для чтения исходящих сообщений (БД → Kafka)
CREATE OR REPLACE FUNCTION test_kafka_adapter.qmanager_read_msg(
    p_wait_seconds INTEGER DEFAULT 10,
    OUT p_cluster VARCHAR,
    OUT p_topic VARCHAR,
    OUT p_message_id VARCHAR,
    OUT p_headers VARCHAR,
    OUT p_message VARCHAR,
    OUT p_error_code INTEGER,
    OUT p_error_description VARCHAR
)
LANGUAGE plpgsql
AS $$
DECLARE
v_message_record RECORD;
BEGIN
    p_error_code := 0;
    p_error_description := 'OK';

    -- Ищем сообщение для отправки (статус PENDING или ERROR с повтором)
SELECT cluster, topic, message_id, headers, payload
INTO v_message_record
FROM test_kafka_adapter.outgoing_messages
WHERE status = 'PENDING'
   OR (status = 'ERROR' AND attempts < max_attempts AND next_retry_at <= NOW())
ORDER BY
    CASE status
        WHEN 'PENDING' THEN 1
        WHEN 'ERROR' THEN 2
        END,
    created_at
    LIMIT 1;

IF v_message_record IS NULL THEN
        -- Нет сообщений для обработки
        p_error_code := 25228;
        p_error_description := 'No messages available in queue';
        RETURN;
END IF;

    -- Обновляем статус сообщения на "обрабатывается"
UPDATE test_kafka_adapter.outgoing_messages
SET
    status = 'PROCESSING',
    attempts = attempts + 1,
    next_retry_at = NOW() + INTERVAL '5 minutes'
WHERE message_id = v_message_record.message_id;

-- Возвращаем данные сообщения
p_cluster := v_message_record.cluster;
    p_topic := v_message_record.topic;
    p_message_id := v_message_record.message_id;
    p_headers := COALESCE(v_message_record.headers::TEXT, '{}');
    p_message := v_message_record.payload;

    -- Логируем успешное извлечение
    RAISE NOTICE 'Message retrieved: topic=%, message_id=%, cluster=%',
        p_topic, p_message_id, p_cluster;

EXCEPTION
    WHEN OTHERS THEN
        p_error_code := -1;
        p_error_description := 'Error reading message: ' || SQLERRM;
        RAISE EXCEPTION 'Error in qmanager_read_msg: %', SQLERRM;
END;
$$;