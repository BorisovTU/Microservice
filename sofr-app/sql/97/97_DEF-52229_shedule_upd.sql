--Обновление заданий планировщика
DECLARE
BEGIN
  UPDATE DSHEDULE_DBT
     SET T_ONEXACTTIME = CHR(0)
   WHERE T_COMMENT = 'Пересчет СНОБ(планировщик)'
     AND T_PERIODTYPE < 4;

END;
/

DECLARE
BEGIN
  UPDATE DSHEDULE_DBT
     SET T_ONEXACTTIME = CHR(0)
   WHERE T_COMMENT = 'Повторная отправка событий СНОБ(планировщик)'
     AND T_PERIODTYPE < 4;

END;
/

BEGIN
 EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/