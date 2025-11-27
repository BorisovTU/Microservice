CREATE OR REPLACE TRIGGER DVSBANNER_DBT_T1_DBO_LEGAL    
FOR UPDATE ON DVSBANNER_DBT    
COMPOUND TRIGGER     
   NeedSendBill BOOLEAN := false; 
   BCID NUMBER := 0;   
    
   AFTER EACH ROW IS    
   BEGIN  
      IF (   :OLD.t_bcstatus != :NEW.t_bcstatus 
      OR  :OLD.t_bcstate !=  :NEW.t_bcstate ) THEN
          BCID := :NEW.t_BCID;
          NeedSendBill := true;
      END IF;
   END AFTER EACH ROW;    
    
   AFTER STATEMENT IS      
   BEGIN      
      IF ( NeedSendBill = true and party_read.is_legal_entity_from_bcid(BCID) != 0) THEN
          IT_NDBOLE.send_bill_json(BCID);
      END IF;   
   END AFTER STATEMENT;    
END DVSBANNER_DBT_T1_DBO_LEGAL; 
/