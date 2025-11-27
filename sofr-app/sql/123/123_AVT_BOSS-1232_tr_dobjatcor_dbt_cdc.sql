CREATE OR REPLACE TRIGGER "TR_DOBJATCOR_DBT_CDC"
  FOR UPDATE OR DELETE OR INSERT
   OF T_ATTRID
   ON DOBJATCOR_DBT
  COMPOUND TRIGGER
  AFTER EACH ROW IS

  msgkey RAW(32);
  eqopts dbms_aq.enqueue_options_t;
  msgprops dbms_aq.message_properties_t;
  msgdata sys.anydata ;
  msgmeta VARCHAR2(128)  ;
  reclist DBMS_AQ.aq$_recipient_list_t;

BEGIN
 IF( (:NEW.T_OBJECTTYPE = 207 AND :NEW.T_GROUPID = 101) OR (:OLD.T_OBJECTTYPE = 207 AND :OLD.T_GROUPID = 101) ) THEN
     IF :NEW.T_ID IS NOT NULL THEN
        IF :OLD.T_ID IS NOT NULL THEN
           msgmeta := '<XML action = "UPD" tab = "DOBJATCOR_DBT" pkcol = "T_ID" pk = "' || :NEW.T_OBJECT || '"/>';
        ELSE
           msgmeta := '<XML action = "INS" tab = "DOBJATCOR_DBT" pkcol = "T_ID" pk = "' || :NEW.T_OBJECT || '"/>';
        END IF;
     ELSE  msgmeta := '<XML action = "DEL" tab = "DOBJATCOR_DBT" pkcol = "T_ID" pk = "' || :OLD.T_OBJECT || '"/>';
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
 END IF;
EXCEPTION
  WHEN OTHERS THEN
 CDC_BY_TRIGGER.ExHandler(msg => sqlerrm,msg_type => 'ERROR',msg_clob => NULL);
END AFTER EACH ROW;
END TR_DOBJATCOR_DBT_CDC;
/