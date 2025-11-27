DECLARE
   vcnt   NUMBER;
BEGIN
   SELECT COUNT (*)
     INTO vcnt
     FROM user_sequences
    WHERE UPPER (sequence_name) = 'BGEXECUTER_SEQ';

   IF vcnt = 0
   THEN
      EXECUTE IMMEDIATE 'CREATE SEQUENCE BGEXECUTER_SEQ
                                START WITH 1
                                MAXVALUE 9999999999999999999999999999
                                MINVALUE 1
                                NOCYCLE
                                NOCACHE
                                NOORDER';
   END IF;
END;
/