BEGIN
  INSERT INTO DBANK_MSG (T_NUMBER,T_PAGE,T_CONTENTS)
                 VALUES (28137,0,'Услуга не подключена');

  INSERT INTO DBANK_MSG (T_NUMBER,T_PAGE,T_CONTENTS)
                 VALUES (28138,0,'Дата подключения услуги не может быть меньше даты заключения ДИК');
END;
/


