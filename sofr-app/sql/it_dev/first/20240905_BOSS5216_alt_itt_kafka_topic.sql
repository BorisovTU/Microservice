declare
  n      number;
  t_name varchar2(100) := 'ITT_KAFKA_TOPIC';
begin
   select count(*)  into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = 'TOPIC_CLUSTER';
   if n = 0 then
     execute immediate 'alter table ITT_KAFKA_TOPIC add topic_cluster varchar2(128)';
     execute immediate 'comment on column ITT_KAFKA_TOPIC.topic_cluster is ''Š« áâ¥à â®¯¨ª  ¢ Š äª ''' ;
   end if;
   select count(*)  into n from user_tab_columns c where c.TABLE_NAME = upper(t_name) and c.COLUMN_NAME = 'SG_COUNT';
   if n = 0 then
     execute immediate 'alter table ITT_KAFKA_TOPIC add sg_count number';
     execute immediate 'comment on column ITT_KAFKA_TOPIC.sg_count is ''Œ ªá¨¬ «ì­®¥ ª®«-¢® ¯®â®ª®¢ ServeceGroup''' ;
   end if;
   sys.dbms_aqadm.alter_queue(queue_name => 'ITQ_OUT_01', max_retries => (4 * 60 * 24), retry_delay => 15);
end;
/
