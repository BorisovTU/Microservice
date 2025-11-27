begin
execute immediate 'alter table itt_kafka_topic modify msg_param VARCHAR2(4000)';
execute immediate 'alter table ITT_KAFKA_TOPIC modify msg_format VARCHAR2(32)';
end;
/