--Обновить записи о стоимости для данных НУ зачислений, если она не равна цене * количество
DECLARE
BEGIN
  
   --Восстановим ранее сохраненные записи, если обновление выполняют повторно из-за исправления скрипта
   UPDATE ddlsum_dbt dls
      SET dls.t_Sum = (SELECT dls_cost.t_Sum
                         FROM ddlsum_dbt dls_cost
                        WHERE dls_cost.t_DocKind = dls.t_DocKind
                          AND dls_cost.t_DocID = dls.t_DocID
                          AND dls_cost.t_Kind = -1230)
    WHERE (dls.t_DocKind, dls.t_DocID, dls.t_Kind) IN (SELECT dls_cost.t_DocKind, dls_cost.t_DocID, -1*dls_cost.t_Kind
                                                         FROM ddlsum_dbt dls_cost
                                                        WHERE dls_cost.t_DocKind = 127
                                                          AND dls_cost.t_Kind = -1230
                                                      );

   DELETE FROM ddlsum_dbt dls_cost WHERE dls_cost.t_DocKind = 127 AND dls_cost.t_Kind = -1230;

   --Сначала на всякий случай сохраним прежние записи, сделав отрицательный t_Kind
   INSERT INTO ddlsum_dbt ( T_DLSUMID,
                            T_DOCKIND,
                            T_DOCID,
                            T_KIND,
                            T_DATE,
                            T_SUM,
                            T_NDS,
                            T_CURRENCY,
                            T_IMMATERIAL,
                            T_INSTANCE,
                            T_GRPID,
                            T_MARKETID,
                            T_FIID
                          )
   SELECT 0,                     
          dls_cost.T_DOCKIND,    
          dls_cost.T_DOCID,      
          -1*dls_cost.T_KIND,       
          dls_cost.T_DATE,       
          dls_cost.T_SUM,        
          dls_cost.T_NDS,        
          dls_cost.T_CURRENCY,   
          dls_cost.T_IMMATERIAL, 
          dls_cost.T_INSTANCE,   
          dls_cost.T_GRPID,      
          dls_cost.T_MARKETID,   
          dls_cost.T_FIID        
     FROM (SELECT t_Kind_Operation, t_DocKind
             FROM doprkoper_dbt 
            WHERE t_DocKind = 127
              AND Rsb_Secur.IsAvrWrtIn(rsb_secur.get_OperationGroup(t_SysTypes)) = 1) Opr,
          ddl_tick_dbt tk, ddl_leg_dbt leg, ddlsum_dbt dls_cost, ddlsum_dbt dls_price
    WHERE tk.t_BOfficeKind = opr.t_DocKind
      AND tk.t_DealType = opr.t_Kind_Operation
      AND leg.t_DealID = tk.t_DealID
      AND leg.t_LegID = 0
      AND leg.t_LegKind = 0
      AND leg.t_Principal > 0
      AND leg.t_RelativePrice <> 'X'
      AND dls_cost.t_DocKind = tk.t_BOfficeKind
      AND dls_cost.t_DocID = tk.t_DealID
      AND dls_cost.t_Kind = 1230
      AND dls_price.t_DocKind = tk.t_BOfficeKind
      AND dls_price.t_DocID = tk.t_DealID
      AND dls_price.t_Kind = 1220
      AND dls_price.t_Sum > 0
      AND ABS(ROUND(dls_cost.t_Sum, 2) - ROUND(dls_price.t_Sum*leg.t_Principal, 2)) > 1;

   --Обновим записи, посчитав стоимость как цена * количество
   UPDATE ddlsum_dbt dls
      SET dls.t_Sum = (SELECT ROUND(dls_price.t_Sum*leg.t_Principal, 2)  
                         FROM ddl_leg_dbt leg, ddlsum_dbt dls_price
                        WHERE leg.t_DealID = dls.t_DocID
                          AND leg.t_LegID = 0
                          AND leg.t_LegKind = 0
                          AND dls_price.t_DocKind = dls.t_DocKind
                          AND dls_price.t_DocID = dls.t_DocID
                          AND dls_price.t_Kind = 1220
                       )
    WHERE (dls.t_DocKind, dls.t_DocID, dls.t_Kind)  IN (SELECT dls_cost.t_DocKind, dls_cost.t_DocID, dls_cost.t_Kind  
                                                          FROM (SELECT t_Kind_Operation, t_DocKind
                                                                  FROM doprkoper_dbt 
                                                                 WHERE t_DocKind = 127
                                                                   AND Rsb_Secur.IsAvrWrtIn(rsb_secur.get_OperationGroup(t_SysTypes)) = 1) Opr,
                                                               ddl_tick_dbt tk, ddl_leg_dbt leg, ddlsum_dbt dls_cost, ddlsum_dbt dls_price
                                                         WHERE tk.t_BOfficeKind = opr.t_DocKind
                                                           AND tk.t_DealType = opr.t_Kind_Operation
                                                           AND leg.t_DealID = tk.t_DealID
                                                           AND leg.t_LegID = 0
                                                           AND leg.t_LegKind = 0
                                                           AND leg.t_Principal > 0
                                                           AND leg.t_RelativePrice <> 'X'
                                                           AND dls_cost.t_DocKind = tk.t_BOfficeKind
                                                           AND dls_cost.t_DocID = tk.t_DealID
                                                           AND dls_cost.t_Kind = 1230
                                                           AND dls_price.t_DocKind = tk.t_BOfficeKind
                                                           AND dls_price.t_DocID = tk.t_DealID
                                                           AND dls_price.t_Kind = 1220
                                                           AND dls_price.t_Sum > 0
                                                           AND ABS(ROUND(dls_cost.t_Sum, 2) - ROUND(dls_price.t_Sum*leg.t_Principal, 2)) > 1
                                                        );

EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

BEGIN
 EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/