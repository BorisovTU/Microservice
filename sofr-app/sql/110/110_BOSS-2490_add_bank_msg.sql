/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26059,0,'При расчете возникли ошибки. Смотрите протокол по F7');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
