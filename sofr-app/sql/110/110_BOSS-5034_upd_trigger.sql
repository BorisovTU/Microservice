CREATE OR REPLACE TRIGGER "NPTXOBJ_DBT_TIUD"
   BEFORE INSERT OR UPDATE OR DELETE 
   ON DNPTXOBJ_DBT
   FOR EACH ROW
DECLARE
   v_IIS CHAR;
   v_IIS_NEW CHAR;
   v_IIS_OLD CHAR;
BEGIN

   IF INSERTING THEN
       IF (RSI_NPTO.CheckObjIIS(:NEW.t_AnaliticKind6, :NEW.t_Analitic6) = 1) THEN
         v_IIS := CHR(88);
       ELSE
         v_IIS := CHR(0);
       END IF; 

      if (:NEW.t_Date <= RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CLOSE, :NEW.T_CLIENT, v_IIS )) then
         --Попытка вставки объекта НДР в закрытом налоговом периоде
        if ((v_IIS <> 'X') and (RSI_NPTO.IsAdmin(RsbSessionData.Oper) = false)) then
          RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20636,''); 
        end if;
      end if;

      IF(:NEW.t_Level = 8 AND :NEW.t_AnaliticKind2 <= 0) THEN
        IF(:NEW.T_KIND IN (1143, 870, 1144, 875)) THEN
          :NEW.t_AnaliticKind2 := RSI_NPTXC.TXOBJ_KIND2040;
          :NEW.t_Analitic2 := 201;
        ELSIF(:NEW.T_KIND IN (1150, 1161, 1162, 1163, 1151)) THEN
          :NEW.t_AnaliticKind2 := RSI_NPTXC.TXOBJ_KIND2040;
          :NEW.t_Analitic2 := 208;
        END IF;
      END IF;

      IF(:NEW.T_TAXPERIOD IS NULL) THEN
        :NEW.T_TAXPERIOD := 0;
      END IF;
      
      /*BOSS-3884 Сумма для всех объектов НДР с 1 по 7 уровень округляется до 2 знаков после запятой*/
      IF( :NEW.t_level > 0 and :NEW.t_level < 8 ) THEN 
        :NEW.t_sum  := round(:NEW.t_sum, 2);
        :NEW.t_sum0 := round(:NEW.t_sum0, 2);
      END IF;
   ELSIF UPDATING THEN
       IF (RSI_NPTO.CheckObjIIS(:NEW.t_AnaliticKind6, :NEW.t_Analitic6) = 1) THEN
         v_IIS_NEW := CHR(88);
       ELSE
         v_IIS_NEW := CHR(0);
       END IF; 

       IF (RSI_NPTO.CheckObjIIS(:OLD.t_AnaliticKind6, :OLD.t_Analitic6) = 1) THEN
         v_IIS_OLD := CHR(88);
       ELSE
         v_IIS_OLD := CHR(0);
       END IF; 

      if ( (:NEW.t_Date <= RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CLOSE, :NEW.T_CLIENT, v_IIS_NEW ) ) OR
           (:OLD.t_Date <= RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CLOSE, :OLD.T_CLIENT, v_IIS_OLD ) )
         ) then
        if ((v_IIS_OLD <> 'X')  and (RSI_NPTO.IsAdmin(RsbSessionData.Oper) = false)) then
         --Попытка изменения объекта НДР в закрытом налоговом периоде
         RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20637,''); 
        end if;
      end if;

      IF(:NEW.T_TAXPERIOD IS NULL) THEN
        :NEW.T_TAXPERIOD := 0;
      END IF;

      /*BOSS-3884 Сумма для всех объектов НДР с 1 по 7 уровень округляется до 2 знаков после запятой*/
      IF(:NEW.t_level > 0 and :NEW.t_level < 8 ) THEN 
        :NEW.t_sum  := round(:NEW.t_sum, 2);
        :NEW.t_sum0 := round(:NEW.t_sum0, 2);
      END IF;
   ELSIF DELETING THEN

       IF (RSI_NPTO.CheckObjIIS(:OLD.t_AnaliticKind6, :OLD.t_Analitic6) = 1) THEN
         v_IIS_OLD := CHR(88);
       ELSE
         v_IIS_OLD := CHR(0);
       END IF; 

      if (:OLD.t_Date <= RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CLOSE, :OLD.T_CLIENT, v_IIS_OLD )) then
        if ((v_IIS_OLD <> 'X')  and (RSI_NPTO.IsAdmin(RsbSessionData.Oper) = false)) then
         --Попытка удаления объекта НДР в закрытом налоговом периоде
         RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20638,''); 
        end if;
      end if;

      DELETE FROM DNPTXOBDC_DBT WHERE t_ObjID = :OLD.t_ObjID;
   END IF;

END NPTXOBJ_DBT_TIUD;
/