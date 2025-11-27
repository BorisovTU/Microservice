/*Обновить поле таблицы*/
DECLARE
BEGIN
  UPDATE DDL_COMM_DBT comm
     SET comm.t_ConsiderFirstBuyDate = 'X'
   WHERE comm.T_DOCKIND IN (135)               --(Глобальная операция с ц/б)
     AND comm.T_COMMDATE >= TO_DATE ('01.01.2024', 'DD.MM.YYYY')
     AND comm.T_CommStatus = 2  --Закрыта
     AND comm.t_OperSubKind = 1 --Конвертация
     AND EXISTS(SELECT 1 
                  FROM dfininstr_dbt fin, DSCDLFI_DBT dlfi, dfininstr_dbt newfin
                 WHERE fin.t_FIID = comm.t_FIID
                   AND RSI_RSB_FIINSTR.FI_AvrKindsGetRoot (2, fin.t_AvoirKind) = 10
                   AND dlfi.t_DealKind = comm.t_DocKInd
                   AND dlfi.t_dealID = comm.t_DocumentID
                   AND newfin.t_FIID = dlfi.t_NewFIID
                   AND RSI_RSB_FIINSTR.FI_AvrKindsGetRoot (2, newfin.t_AvoirKind) = 20
               );
END;
/