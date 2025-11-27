DECLARE
   table_exists_exception EXCEPTION;
   PRAGMA EXCEPTION_INIT (table_exists_exception, -955);
   table_not_exists_exception EXCEPTION;
   PRAGMA EXCEPTION_INIT (table_not_exists_exception, -942);
BEGIN
   
   BEGIN
      EXECUTE IMMEDIATE ('DROP TABLE "DNPTXOP2OPLNK_TMP"');
   EXCEPTION
      WHEN table_not_exists_exception
      THEN NULL;
   END;
   
   EXECUTE IMMEDIATE ('CREATE GLOBAL TEMPORARY TABLE "DNPTXOP2OPLNK_TMP" ' ||
                     '( ' ||
                     '   t_DocID NUMBER(10), ' ||
                     '   t_Parent_DocID NUMBER(10), ' ||
                     '   t_PartyID NUMBER(10) ' ||
                     ') ' ||
                     'ON COMMIT PRESERVE ROWS');
   EXECUTE IMMEDIATE ('COMMENT ON TABLE DNPTXOP2OPLNK_TMP IS ''Связи между операциями списания д/с и удержания НДФЛ с операциями расчета НОБ в 6-НДФЛ до 01.01.2021''');
   EXECUTE IMMEDIATE ('COMMENT ON COLUMN DNPTXOP2OPLNK_TMP.T_DOCID IS ''t_ID операции расчета НОБ''');
   EXECUTE IMMEDIATE ('COMMENT ON COLUMN DNPTXOP2OPLNK_TMP.T_PARENT_DOCID IS ''t_ID операций списания д/с или удержания НДФЛ''');
   EXECUTE IMMEDIATE ('COMMENT ON COLUMN DNPTXOP2OPLNK_TMP.T_PARTYID IS ''ID клиента''');
   
   COMMIT;
END;
/