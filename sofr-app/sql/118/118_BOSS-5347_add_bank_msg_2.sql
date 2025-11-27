/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26079,0,'По договору выполнен расчет НОБ с типом "Закрытие ИИС"');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
