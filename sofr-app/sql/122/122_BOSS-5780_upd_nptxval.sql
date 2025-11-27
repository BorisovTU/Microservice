/*Обновление записей*/
DECLARE
BEGIN
   UPDATE DNPTXVAL_DBT
      SET T_KIND = 33
    WHERE T_DOCKIND = 4605
      AND T_KIND = 23;

END;
/