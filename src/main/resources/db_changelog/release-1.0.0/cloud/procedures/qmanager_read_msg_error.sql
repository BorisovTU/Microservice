-- Процедура для сохранения информации об ошибках
CREATE OR REPLACE FUNCTION test_kafka_adapter.qmanager_read_msg_error(
    p_topic VARCHAR,
    p_message_id VARCHAR,
    p_error_code INTEGER,
    p_error_description VARCHAR
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    -- Сохраняем ошибку в таблицу ошибок
INSERT INTO test_kafka_adapter.message_errors (
    topic, message_id, error_code, error_description, source
) VALUES (
             p_topic, p_message_id, p_error_code, p_error_description, 'PROCESSOR'
         );

-- Обновляем статус сообщения на ERROR
UPDATE test_kafka_adapter.outgoing_messages
SET
    status = 'ERROR',
    error_message = p_error_description,
    next_retry_at = NOW() + INTERVAL '5 minutes'
WHERE message_id = p_message_id;

-- Логируем сохранение ошибки
RAISE NOTICE 'Error saved: topic=%, message_id=%, error=%',
        p_topic, p_message_id, p_error_description;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error saving error information: %', SQLERRM;
END;
$$;