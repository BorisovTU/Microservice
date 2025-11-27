-- Изменение типа столбца таблицы Kondor_SOFR_Buffer_dbt
DECLARE
BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE Kondor_SOFR_Buffer_dbt';
  EXECUTE IMMEDIATE 'ALTER TABLE Kondor_SOFR_Buffer_dbt MODIFY t_requestID VARCHAR2(30)';
END;
/