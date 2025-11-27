-- Изменение параметров очереди
begin
  sys.dbms_aqadm.alter_queue(queue_name => 'ITQ_OUT_01', max_retries => 15000, retry_delay => 5);
end;
/
