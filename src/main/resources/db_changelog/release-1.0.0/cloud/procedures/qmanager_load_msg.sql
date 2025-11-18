-- Процедура для сохранения входящих сообщений (Kafka → БД)
CREATE OR REPLACE FUNCTION test_kafka_adapter.qmanager_load_msg(
    p_topic VARCHAR,
    p_message_id VARCHAR,
    p_timestamp TIMESTAMP,
    p_payload VARCHAR,
    OUT p_error_code INTEGER,
    OUT p_error_description VARCHAR
)
LANGUAGE plpgsql
AS $$
BEGIN
    p_error_code := 0;
    p_error_description := 'OK';

BEGIN
        -- Вставляем сообщение в таблицу входящих сообщений
INSERT INTO test_kafka_adapter.incoming_messages (
    topic, message_id, timestamp, payload
) VALUES (
             p_topic, p_message_id, p_timestamp, p_payload
         );

-- Логируем успешное сохранение
RAISE NOTICE 'Message saved: topic=%, message_id=%, timestamp=%',
            p_topic, p_message_id, p_timestamp;

EXCEPTION
        WHEN unique_violation THEN
            p_error_code := 1;
            p_error_description := 'Duplicate message_id: ' || p_message_id;
            RAISE WARNING 'Duplicate message: %', p_message_id;

WHEN OTHERS THEN
            p_error_code := -1;
            p_error_description := 'Database error: ' || SQLERRM;
            RAISE EXCEPTION 'Error saving message: %', SQLERRM;
END;
END;
$$;