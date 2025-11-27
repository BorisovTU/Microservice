CREATE OR REPLACE TRIGGER "TR_DSCQINV_DBT_CDC"
  FOR update or delete or insert
   OF t_regdate,t_sysdate,t_systime,t_changedate,t_state
   ON DSCQINV_DBT
  compound trigger
  after each row is

  msgkey RAW(32);
  eqopts dbms_aq.enqueue_options_t;
  msgprops dbms_aq.message_properties_t;
  msgdata sys.anydata ;
  msgmeta varchar2(128)  ;
  reclist DBMS_AQ.aq$_recipient_list_t;

BEGIN

   IF :NEW.T_PARTYID IS NOT NULL THEN
      IF :OLD.T_PARTYID IS NOT NULL THEN
         msgmeta := '<XML action = "UPD" tab = "DSCQINV_DBT" pkcol = "T_PARTYID" pk = "' || CAST(:NEW.T_PARTYID AS VARchar2) || '"/>';
      ELSE
         msgmeta := '<XML action = "INS" tab = "DSCQINV_DBT" pkcol = "T_PARTYID" pk = "' || CAST(:NEW.T_PARTYID AS VARchar2) || '"/>';
      END IF;
   ELSE  msgmeta := '<XML action = "DEL" tab = "DSCQINV_DBT" pkcol = "T_PARTYID" pk = "' || CAST(:OLD.T_PARTYID AS VARchar2) || '"/>';
   END IF;

   reclist(1):= SYS.AQ$_AGENT('CDC_TRIG_SUBSCR', NULL, NULL);

   eqopts.visibility     := SYS.DBMS_AQ.IMMEDIATE;
   eqopts.relative_msgid    := NULL;
   eqopts.sequence_deviation  :=  NULL;
   eqopts.transformation   := NULL;
   eqopts.delivery_mode  := SYS.DBMS_AQ.BUFFERED ;

   msgprops.priority    := 0;
   msgprops.delay       := SYS.DBMS_AQ.NO_DELAY;
   msgprops.expiration  :=  3600;
   msgprops.correlation :=  msgmeta;
   msgprops.recipient_list := reclist;

   msgdata :=  anydata.ConvertVarchar2(msgmeta) ;

  DBMS_AQ.ENQUEUE (
   queue_name=> 'CDC_TRIG_QUEUE',
   enqueue_options     => eqopts,
   message_properties  => msgprops,
   payload             => msgdata ,
   msgid               => msgkey );

EXCEPTION
  WHEN OTHERS THEN 
 CDC_BY_TRIGGER.ExHandler(msg => sqlerrm,msg_type => 'ERROR',msg_clob => NULL);
end after each row;
end TR_DSCQINV_DBT_CDC;
/
