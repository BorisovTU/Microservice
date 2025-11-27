/* обновление справочников */
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26046,0,'Откатываемая операция не является последней для события СНОБ');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/

BEGIN
 EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/
