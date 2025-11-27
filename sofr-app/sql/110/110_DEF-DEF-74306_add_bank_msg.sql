/*Добавить сообщение*/
BEGIN
   UPDATE DBANK_MSG 
      SET T_CONTENTS = 'Введенные параметры налогового периода для типа расчета НОБ "Обычный расчет" некорректны.|Необходимо выбрать текущий налоговый период'
    WHERE T_NUMBER = 26066;
END;
/

BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26067,0,'Поле "Дата операции" заполнено некорректно. Запуск операции невозможен.');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/

BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS )
   VALUES (26068,0,'Дата ЗА ПЕРИОД ДО не должна превышать текущую');
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
