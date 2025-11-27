/*************************************************
-- CDC на триггерах для BOSS-1642
-- Томилин Е.Е.
--схема: таблица(триггер) => очередь => подписка => хранимая процедура
*/

/********************************************************************************* 
   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.1       12.07.2024   Shishkin-EV       Адаптация CDC под 1642 

******************************************************************************/
  
-- добавляем таблицу для очереди
DECLARE
    E_TABLE_EXISTS EXCEPTION;
    PRAGMA EXCEPTION_INIT(E_TABLE_EXISTS, -24001); -- ORA-24001 cannot create QUEUE_TABLE, string already exists
BEGIN
DBMS_AQADM.CREATE_QUEUE_TABLE (
   queue_table          => 'CDC_QUEUE_TAB',     -- имя таблицы для очереди 
   queue_payload_type   => 'SYS.ANYDATA',
   storage_clause       => NULL ,
   sort_list            => '',
   multiple_consumers   => TRUE,                              -- под несколько консьюмеров, если потребуется в будущем
   message_grouping     => DBMS_AQADM.NONE,
   comment              => 'таблица для очереди CDC', -- коммент
   auto_commit          => TRUE,
   primary_instance     => '0' ,
   secondary_instance   => '0' ,
   compatible           => '8.1', 
   secure               => FALSE ,                                -- не шифруем
   replication_mode     => DBMS_AQADM.NONE )
; COMMIT ;
EXCEPTION
    WHEN E_TABLE_EXISTS THEN NULL;
END;
/

-- [02] добавляем очередь для CDC
DECLARE
    E_QUEUE_EXISTS EXCEPTION;
    PRAGMA EXCEPTION_INIT(E_QUEUE_EXISTS, -24006);
BEGIN
DBMS_AQADM.create_queue(
        queue_name     => 'CDC_TRIG_QUEUE',   -- название очереди 
        queue_table    => 'CDC_QUEUE_TAB',   -- ссылка на таблицу с предыдущего шага 
        queue_type     => DBMS_AQADM.NORMAL_QUEUE,
        max_retries    => NULL,
        retry_delay    => 0,              
        retention_time => 3600,  -- сколько хранить в секундах , выставил 1 час           
        dependency_tracking => FALSE,
        comment        => 'очередь CDC',
        auto_commit    => TRUE )
;COMMIT;                       -- на всякий случай 
EXCEPTION
    WHEN E_QUEUE_EXISTS THEN NULL;
END; 
/

-- запустить очередь [02]
BEGIN DBMS_AQADM.START_QUEUE (queue_name => 'CDC_TRIG_QUEUE'); END;
/

-- [03] добавляем подписчика (агент)
DECLARE
    E_SUBSCRIBER_EXISTS EXCEPTION;
    PRAGMA EXCEPTION_INIT(E_SUBSCRIBER_EXISTS, -24034);
begin
   dbms_aqadm.add_subscriber(
   queue_name => 'CDC_TRIG_QUEUE' ,
   subscriber => sys.aq$_agent(name => 'CDC_TRIG_SUBSCR',address => NULL ,protocol => NULL),
   rule => NULL,
   transformation => NULL,
   queue_to_queue => FALSE,
   delivery_mode =>  dbms_aqadm.BUFFERED);
EXCEPTION
    WHEN E_SUBSCRIBER_EXISTS THEN NULL;
end;
/

-- [04] привязать подписчику процедуру-обработчик (callback)
BEGIN
   dbms_aq.register
      (sys.aq$_reg_info_list                      
         (sys.aq$_reg_info 
            ('CDC_TRIG_QUEUE:CDC_TRIG_SUBSCR'      -- name=> reference string literal <shema>.<queue>:<consumer> , will not work without consumer for multi cons. queue
            ,DBMS_AQ.NAMESPACE_AQ                                -- namespace => always DBMS_AQ.NAMESPACE_AQ for interbase queues, for mail and java must be DBMS_AQ.NAMESPACE_ANONYMOUS
            ,'plsql://CDC_BY_TRIGGER.CDC_callback?PR=0'                         -- callback => reference string to callback proc , PR=1 for XML load ; see https://docs.oracle.com/en/database/oracle/oracle-database/12.2/arpls/advanced-queuing-AQ-types.html
            ,NULL)                                               -- anyctx=> anytype context 
         ),
      1                                                          -- array items counter for  sys.aq$_reg_info_list
      );
END;
/

-- [05] создать триггера на таблицах в целевой схеме (см константы в пакете CDC_BY_TRIGGER, пересобирать под окружение)
declare v_ret varchar(32676);
begin
v_ret:= CDC_BY_TRIGGER.setup_CDC;
end;
/