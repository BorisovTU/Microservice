/*Обновление DNPTXOBJBC_DBT*/
DECLARE
BEGIN

UPDATE dnptxobjbc_dbt
SET t_Holding_Period = 0
WHERE t_Holding_Period IS NULL;
 
END;
/