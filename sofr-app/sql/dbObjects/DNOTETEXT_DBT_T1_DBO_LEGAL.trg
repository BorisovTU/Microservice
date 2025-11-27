CREATE OR REPLACE TRIGGER DNOTETEXT_DBT_T1_DBO_LEGAL    
FOR INSERT ON dnotetext_dbt
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
/