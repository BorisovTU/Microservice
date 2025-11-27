CREATE OR REPLACE TRIGGER DNOTETEXT_DBT_HIST_AIUD AFTER INSERT OR UPDATE OR DELETE ON dnotetext_dbt
BEGIN
  --Created by RsbPartyTrig.sql
  RSI_RSBPARTY.SavePtprmhistArray;
END;

CREATE OR REPLACE TRIGGER DNOTETEXT_DBT_HIST_AIUDR AFTER INSERT OR UPDATE OR DELETE ON dnotetext_dbt
FOR EACH ROW
DECLARE
  m_old dnotetext_dbt%ROWTYPE;
  m_new dnotetext_dbt%ROWTYPE;
BEGIN
  --Created by RsbPartyTrig.sql
  IF (   :OLD.t_objecttype = 3
      OR :NEW.t_objecttype = 3
      OR :OLD.t_objecttype = 8
      OR :NEW.t_objecttype = 8) THEN
    IF NOT INSERTING THEN
      m_new.t_objecttype := :OLD.t_objecttype;
      m_new.t_notekind   := :OLD.t_notekind  ;
      m_new.t_documentid := :OLD.t_documentid;

      m_old.t_objecttype := :OLD.t_objecttype;
      m_old.t_notekind   := :OLD.t_notekind  ;
      m_old.t_documentid := :OLD.t_documentid;
      m_old.t_text       := :OLD.t_text      ;
      m_old.t_date       := :OLD.t_date      ;
    END IF;
    IF NOT DELETING THEN
      m_new.t_objecttype := :NEW.t_objecttype;
      m_new.t_notekind   := :NEW.t_notekind  ;
      m_new.t_documentid := :NEW.t_documentid;
      m_new.t_text       := :NEW.t_text      ;
      m_new.t_date       := :NEW.t_date      ;

      m_old.t_objecttype := :NEW.t_objecttype;
      m_old.t_notekind   := :NEW.t_notekind  ;
      m_old.t_documentid := :NEW.t_documentid;
    END IF;
    IF INSERTING AND RSI_RSBPARTY.IsExistsPartyIDArray(TO_NUMBER(m_new.t_documentid)) THEN
      RETURN;
    END IF;
    RSI_RSBPARTY.NotetextTIUD
    (
       m_old
      ,m_new
    );
  END IF;
END;

CREATE OR REPLACE TRIGGER DNOTETEXT_DBT_T1_DBO_LEGAL FOR INSERT ON dnotetext_dbt
COMPOUND TRIGGER     
   NeedSendBill BOOLEAN := false; 
   FicertID NUMBER := 0; 
   DocumentID NUMBER := 0;   
    
   AFTER EACH ROW IS    
   BEGIN  
      IF (   :OLD.t_objecttype = 24
      OR :NEW.t_objecttype = 24) and 
     (   :OLD.t_notekind = 23
      OR :NEW.t_notekind = 23) THEN
          FicertID := LTRIM (:NEW.t_documentid, '0');
          NeedSendBill := true;
      END IF;
   END AFTER EACH ROW;    
    
   AFTER STATEMENT IS      
   BEGIN
      IF NeedSendBill = true THEN
          BEGIN  
          SELECT T_CERTID INTO DocumentID FROM DFICERT_DBT WHERE T_FICERTID = FicertID;
          EXCEPTION
          WHEN NO_DATA_FOUND THEN DocumentID := 0; END;
      
          IF party_read.is_legal_entity_from_bcid(DocumentID) != 0 THEN
              IT_NDBOLE.send_bill_json(DocumentID);
          END IF;
      END IF;   
   END AFTER STATEMENT;    
END DNOTETEXT_DBT_T1_DBO_LEGAL;

CREATE OR REPLACE TRIGGER DNOTETEXT_DBT_T5_AINC BEFORE INSERT OR UPDATE OF t_id ON dnotetext_dbt FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.t_id = 0 OR :new.t_id IS NULL) THEN
 SELECT dnotetext_dbt_seq.nextval INTO :new.t_id FROM dual;
 ELSE
 select last_number into v_id from user_sequences where sequence_name = upper ('dnotetext_dbt_SEQ');
 IF :new.t_id >= v_id THEN
 RAISE DUP_VAL_ON_INDEX;
 END IF;
 END IF;
END;

CREATE OR REPLACE TRIGGER DNOTETEXT_DBT_TRGGT
  AFTER DELETE OR INSERT OR UPDATE OF t_text, t_validtodate
  ON dnotetext_dbt
  FOR EACH ROW
DECLARE
  --created by cre_reg_gt_trg.sql
  v_obj_gtkobj   dobj_gtkobj_dbt%ROWTYPE;
  v_objcoders    VARCHAR2(100);
  v_objecttype   dnotetext_dbt.t_objecttype%TYPE;
  v_objectid     dnotetext_dbt.t_documentid%TYPE;
  v_gtobjectcode dgtcode_dbt.t_objectcode%TYPE;
  v_gtobjectname dgtobject_dbt.t_name%TYPE;
  v_actionid     NUMBER(5);
  v_stmt         VARCHAR2(1000);

BEGIN

  v_actionid := RSI_RSB_GATE.CONST_ACTIONID_UPD;

  IF INSERTING OR UPDATING THEN
    v_objecttype := :NEW.t_objecttype;
    v_objectid   := :NEW.t_documentid;

  ELSIF DELETING THEN
    v_objecttype := :OLD.t_objecttype;
    v_objectid   := :OLD.t_documentid;

  END IF;

  BEGIN
    SELECT * INTO v_obj_gtkobj FROM dobj_gtkobj_dbt WHERE t_objecttype = v_objecttype;
    IF v_obj_gtkobj.t_forNoteText = CHR(0) THEN RAISE NO_DATA_FOUND; END IF;
    v_stmt := 'BEGIN                                                        ' ||
              '  ' || v_obj_gtkobj.t_getobjcoders || '(:1, :2, :3, :4, :5); ' ||
              'END;                                                         ' ;
    EXECUTE IMMEDIATE v_stmt USING v_objecttype, v_objectid, RSI_RSB_GTPREP.CNST_CALL_NOTETEXT, OUT v_gtobjectcode, OUT v_gtobjectname;

    v_stmt := 'DECLARE                                                                             ' ||
              '  v_objectchange RSI_RSB_GATE.ObjectChange_cur;                                         ' ||
              'BEGIN                                                                               ' ||
              '  OPEN v_objectchange FOR SELECT '   || TO_CHAR(v_obj_gtkobj.t_gtobjectkind) ||   ',' ||
              '                                 ''' || v_gtobjectcode                       || ''',' ||
              '                                 ''' || v_gtobjectname                       || ''',' ||
              '                                 '   || TO_CHAR(v_actionid)                  ||   ',' ||
              '                                 NULL                                              ,' ||
              '                                 NULL                                               ' ||
              '                            FROM dual;                                              ' ||
              '  RSI_RSB_GATE.Al_RegistryObject(v_objectchange);                                       ' ||
              'END;                                                                                ' ;
    RSB_TOOLS.dynamic_autonomous(v_stmt);
  EXCEPTION WHEN OTHERS THEN v_actionid:=0;
  END;

END DNOTETEXT_DBT_TRGGT;

CREATE OR REPLACE TRIGGER TR_DNOTETEXT_DBT_CDC FOR update or delete or insert
   OF t_date,t_text
   ON DNOTETEXT_DBT
  compound trigger
  after each row is

  msgkey RAW(32);
  eqopts dbms_aq.enqueue_options_t;
  msgprops dbms_aq.message_properties_t;
  msgdata sys.anydata ;
  msgmeta varchar2(128)  ;
  reclist DBMS_AQ.aq$_recipient_list_t;

BEGIN

   IF :NEW.T_ID IS NOT NULL THEN
      IF :OLD.T_ID IS NOT NULL THEN
         msgmeta := '<XML action = "UPD" tab = "DNOTETEXT_DBT" pkcol = "T_ID" pk = "' || CAST(:NEW.T_ID AS VARchar2) || '"/>';
      ELSE
         msgmeta := '<XML action = "INS" tab = "DNOTETEXT_DBT" pkcol = "T_ID" pk = "' || CAST(:NEW.T_ID AS VARchar2) || '"/>';
      END IF;
   ELSE  msgmeta := '<XML action = "DEL" tab = "DNOTETEXT_DBT" pkcol = "T_ID" pk = "' || CAST(:OLD.T_ID AS VARchar2) || '"/>';
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
end TR_DNOTETEXT_DBT_CDC;