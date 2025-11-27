--Обновление типа записи в таблице сверки СНОБ
DECLARE
BEGIN
   UPDATE DNPTXSNOBVER_DBT 
      SET T_CHECKTYPE = 1
    WHERE T_CHECKTYPE = 0;
END;
/
