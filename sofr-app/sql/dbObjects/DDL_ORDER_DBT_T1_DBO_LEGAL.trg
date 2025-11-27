CREATE OR REPLACE TRIGGER DDL_ORDER_DBT_T1_DBO_LEGAL    
FOR UPDATE ON DDL_ORDER_DBT    
COMPOUND TRIGGER     
   NeedSendBill BOOLEAN := false;
   NeedSendDeal BOOLEAN := false; 
   ContractID NUMBER := 0;    
   
   TYPE row_bcid_t IS TABLE OF DVSBANNER_DBT.T_BCID%TYPE INDEX BY PLS_INTEGER;    

   p_row_bcid   row_bcid_t; 
    
   AFTER EACH ROW IS    
   BEGIN 
      IF (:OLD.T_CONTRACTSTATUS = 0 AND :NEW.T_CONTRACTSTATUS = 10 AND :OLD.T_DOCKIND = 110) THEN
        NeedSendDeal := true;
        ContractID := :OLD.T_CONTRACTID;
      END IF; 
      IF (:OLD.T_CONTRACTSTATUS = 10 AND :NEW.T_CONTRACTSTATUS >= 10 AND :OLD.T_DOCKIND IN (109, 110) ) THEN
          NeedSendBill := true;
          ContractID := :OLD.T_CONTRACTID;
          FOR c IN (SELECT lnk.T_BCID BCID FROM dvsordlnk_dbt lnk WHERE lnk.T_CONTRACTID = :OLD.T_CONTRACTID AND lnk.T_DOCKIND = :OLD.T_DOCKIND )
          LOOP
              p_row_bcid (p_row_bcid.COUNT + 1) := c.BCID;    
          END LOOP;
      END IF;
   END AFTER EACH ROW;    
    
   AFTER STATEMENT IS      
   BEGIN 
      IF ( NeedSendDeal = true and party_read.is_legal_entity(ContractID) = 1) THEN
          IT_NDBOLE.send_bill_deal_json(ContractID, 10);  
      END IF;     
      IF ( NeedSendBill = true and party_read.is_legal_entity(ContractID) = 1) THEN
          FOR indx IN 1 .. p_row_bcid.COUNT    
          LOOP                                      
            IT_NDBOLE.send_bill_json(p_row_bcid (indx));  
          END LOOP;  
      END IF;   
   END AFTER STATEMENT;    
END DDL_ORDER_DBT_T1_DBO_LEGAL; 
/