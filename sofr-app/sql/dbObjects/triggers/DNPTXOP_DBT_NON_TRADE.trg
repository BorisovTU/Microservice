CREATE OR REPLACE TRIGGER "DNPTXOP_DBT_NON_TRADE" 
AFTER  UPDATE  ON DNPTXOP_DBT REFERENCING NEW AS NEW OLD AS OLD FOR EACH ROW
DECLARE

  IsSuitableClient  number(10) := 0;
  NameOperation     varchar2(100);
 
BEGIN

  IF :NEW.T_ID IS NOT NULL AND :NEW.t_status in (1, 2) AND :NEW.t_dockind = 4607
  THEN
    if it_broker.IsRegval_NonTrade_On = 'X' then
   
      if it_broker.IsRegval_NonTrade_OnlyFL_On = 'X' then
        IsSuitableClient := party_read.is_person_entity_clear(:NEW.t_client);
      else 
        IsSuitableClient := 1;
      end if;

      if IsSuitableClient > 0 then 
        NameOperation := it_broker.GetOperationName(:NEW.t_subkind_operation);
        if NameOperation <> '---' then 
          it_broker.non_trade(:NEW.t_id,
                              :NEW.t_dockind,
                              :NEW.t_client, 
                              :NEW.t_kind_operation,
                              :NEW.t_subkind_operation,
                              :NEW.t_contract,
                              :NEW.t_currency,
                              :NEW.t_outsum,
                              :NEW.t_tax,
                              :NEW.t_status,
                              :NEW.t_account);
        end if;
      end if;
      
    end if;
  END IF;

EXCEPTION

  WHEN OTHERS
  THEN 
    it_error.put_error_in_stack; 
    it_log.log(p_msg => 'Error nptxopid = '||:NEW.t_id, p_msg_type => it_log.c_msg_type__error); 
    it_error.clear_error_stack; 

END;
/