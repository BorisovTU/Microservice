--Добавление новых полей в таблицу событий СНОБ
DECLARE
   e_exist_field EXCEPTION;

   PRAGMA EXCEPTION_INIT( e_exist_field, -1430);

BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE DNPTXTOTALBASE_DBT ADD T_SYMBOL CHAR(1) DEFAULT CHR(0)';
   EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXTOTALBASE_DBT.T_SYMBOL IS ''Некий символ для пометки записи''';
   EXCEPTION WHEN e_exist_field THEN NULL;
END;
/

