/*Обновить референс*/
DECLARE
  v_MaxNum NUMBER := 0;
BEGIN
  BEGIN
    WITH mnum AS( SELECT TO_NUMBER(T_CODE) as NumCode
                    FROM DNPTXOP_DBT 
                   WHERE T_DOCKIND = 4644 
                     AND regexp_like(T_CODE, '^\d+(\.\d+)?$')
                )
    SELECT NVL(MAX(TO_NUMBER(NumCode) ),0)
      INTO v_MaxNum            
      FROM mnum
     WHERE TO_NUMBER(NumCode) < 4000000;

    EXCEPTION 
         WHEN OTHERS THEN NULL;
  END;

  IF v_MaxNum > 0 THEN
    UPDATE DMETER_DBT
       SET T_LASTVALUE = v_MaxNum
     WHERE T_SEQID = 356
       AND T_CLOSEDATE = TO_DATE('01.01.0001','DD.MM.YYYY');
  END IF;
END;
/

BEGIN
 EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/