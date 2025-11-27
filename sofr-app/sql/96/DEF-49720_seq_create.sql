--создание последовательности

DECLARE
   vcnt   NUMBER;
BEGIN
   SELECT COUNT (*)
     INTO vcnt
     FROM user_sequences
    WHERE UPPER (sequence_name) = 'DLIMIT_REVISE_SEQ';

   IF vcnt = 0
   THEN
      EXECUTE IMMEDIATE 'CREATE SEQUENCE DLIMIT_REVISE_SEQ
                                START WITH 1
                                MAXVALUE 9999999999999999999999999999
                                MINVALUE 1
                                NOCYCLE
                                NOCACHE
                                NOORDER';
      EXECUTE IMMEDIATE 'truncate table DLIMIT_MONEY_REVISE_DBT';
      EXECUTE IMMEDIATE 'truncate table DLIMIT_SECUR_REVISE_DBT';
   END IF;
END;