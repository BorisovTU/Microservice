--добавление операции зачисления к календарным операциям
DECLARE
BEGIN
  INSERT INTO DDLCALENOPRS_DBT (T_IDENTPROGRAM, T_OBJTYPE, T_NAME, T_ISSYSTEM) VALUES (158,3,'Операция списания и зачисления денежных средств',CHR(0));
  INSERT INTO DDLCALENOPRS_DBT (T_IDENTPROGRAM, T_OBJTYPE, T_NAME, T_ISSYSTEM) VALUES (83,3,'Операция списания и зачисления денежных средств',CHR(0));

EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;

END;
/

