DECLARE 
e_object_exists EXCEPTION;
PRAGMA EXCEPTION_INIT(e_object_exists, -24001); 
BEGIN
DBMS_AQADM.CREATE_QUEUE_TABLE (Queue_table => 'ws_inc_sync_qtt_bytes', 
Queue_payload_type => 'SYS.AQ$_JMS_BYTES_MESSAGE', compatible => '8.1.0');

DBMS_AQADM.CREATE_QUEUE (Queue_name => 'aq_incoming_sync_ws', 
Queue_table => 'ws_inc_sync_qtt_bytes');

DBMS_AQADM.START_QUEUE (Queue_name => 'aq_incoming_sync_ws');
EXCEPTION 
WHEN e_object_exists THEN NULL; 
END;
/