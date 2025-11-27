/*Обновление истории объектов НДР*/
DECLARE

BEGIN
   UPDATE DNPTXOBJBC_DBT
      SET T_KIND = 402,
          T_HOLDING_PERIOD = (CASE WHEN T_KIND = 403 THEN 3
                                   WHEN T_KIND = 404 THEN 4
                                   WHEN T_KIND = 405 THEN 5
                                   WHEN T_KIND = 406 THEN 6
                                   ELSE 0 END
                             )
    WHERE T_KIND IN (403, 404, 405, 406)
      AND T_HOLDING_PERIOD = 0 OR T_HOLDING_PERIOD IS NULL;

END;
/