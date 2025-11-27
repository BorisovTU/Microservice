/*Добавить сообщение*/
BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26066,0,'Введенные параметры налогового периода для типа расчета НОБ "Обычный расчет" некорректны. Необходимо выбрать текущий налоговый период');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
