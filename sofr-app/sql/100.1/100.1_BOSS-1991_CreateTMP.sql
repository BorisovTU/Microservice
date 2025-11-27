DECLARE
   table_already_exists EXCEPTION;
   pragma exception_init(table_already_exists, -955);
BEGIN
   execute immediate 'CREATE GLOBAL TEMPORARY TABLE DNPTX6CLIENT_TMP ( ' ||
                     '                                                  T_PARTYID NUMBER(10) ' ||
                     '                                               ) ON COMMIT PRESERVE ROWS';

   execute immediate 'COMMENT ON TABLE DNPTX6CLIENT_TMP IS ''Клиенты для 6-НДФЛ с расшифровками''';
   execute immediate 'COMMENT ON COLUMN DNPTX6CLIENT_TMP.T_PARTYID IS ''ID клиента''';
EXCEPTION
   WHEN table_already_exists
   THEN IT_LOG.LOG('BOSS-1991. Таблица DNPTX6CLIENT_TMP уже существует');
END;
/