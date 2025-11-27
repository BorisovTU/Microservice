--Добавление новых полей в таблицу событий СНОБ
DECLARE
   e_exist_field EXCEPTION;

   PRAGMA EXCEPTION_INIT( e_exist_field, -1430);

BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE DNPTXTOTALBASEBC_DBT ADD T_INITIAL_DOCKIND NUMBER(5) DEFAULT 0';
   EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXTOTALBASEBC_DBT.T_INITIAL_DOCKIND IS ''Вид родительского первичного документа, к которому относится запись''';
   EXCEPTION WHEN e_exist_field THEN NULL;
END;
/

DECLARE
   e_exist_field EXCEPTION;

   PRAGMA EXCEPTION_INIT( e_exist_field, -1430);

BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE DNPTXTOTALBASEBC_DBT ADD T_INITIAL_DOCID NUMBER(10) DEFAULT 0';
   EXECUTE IMMEDIATE 'COMMENT ON COLUMN DNPTXTOTALBASEBC_DBT.T_INITIAL_DOCID IS ''Идентификатор родительского первичного документа, к которому относится запись''';
   EXCEPTION WHEN e_exist_field THEN NULL;
END;
/

