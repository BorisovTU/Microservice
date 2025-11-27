--Обновление механизма генерации кода ц/б
DECLARE
  V_SEQID NUMBER := 0;
BEGIN
  SELECT T_SEQID INTO V_SEQID FROM DREFER_DBT WHERE T_REFID = 47;

  IF V_SEQID != 363 THEN
    UPDATE DREFER_DBT 
       SET T_SEQID = 363,
           T_CALCTYPE = 2,
           T_MACROFILE = 'rshb_fi_codeinacc.mac',
           T_MACROPROC = 'GetCodeInAcc'
     WHERE T_REFID = 47;
   END IF;
                      
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/