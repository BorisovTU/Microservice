create or replace TRIGGER DFININSTR_DBT_TDUR_SC
 AFTER INSERT OR UPDATE OF T_DRAWINGDATE
 ON DFININSTR_DBT FOR EACH ROW
DECLARE
   NRecRetire     NUMBER := 0;  -- наличие операции погашения или распределения
BEGIN

  IF (Rsb_Common.GetRegBoolValue('SECUR\РЕЖИМ ХРАНИЛИЩА ДАННЫХ ДЛЯ НУ') = FALSE) THEN
    IF( :NEW.t_fi_kind = RSI_RSB_FIInstr.FIKIND_AVOIRISS ) THEN

       -- получаем количество операций
       BEGIN
         SELECT count(1) INTO NRecRetire
           FROM DDL_TICK_DBT tick, DDL_LEG_DBT leg
          WHERE     rsb_secur.IsRet_Issue(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tick.t_DealType,tick.t_BofficeKind))) = 1
                AND tick.t_DealID = leg.t_DealID
                and tick.t_bofficekind != 130
                AND leg.t_PFI = :NEW.T_FIID
                AND leg.t_LegKind = 0
                AND leg.t_LegID = 0;
        EXCEPTION
           WHEN NO_DATA_FOUND THEN NRecRetire:=0;
        END;

        --1.  Если измененялось T_DRAWINGDATE и существует операция погашения выпуска :NEW.T_FIID - ошибка: "Попытка изменить дату погашения в операции погашения"
        IF( (:OLD.T_DRAWINGDATE <> :NEW.T_DRAWINGDATE) AND
            ( NRecRetire > 0 )
          ) THEN
           RAISE_APPLICATION_ERROR( -20205,'');
        END IF;
    END IF;
  END IF;
END DFININSTR_DBT_TDUR_SC;