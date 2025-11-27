BEGIN
  INSERT INTO DBANK_MSG (T_NUMBER,T_PAGE,T_CONTENTS)
                 VALUES (28130,0,'Дата подключения услуги не может быть меньше даты заключения ДБО');

  INSERT INTO DBANK_MSG (T_NUMBER,T_PAGE,T_CONTENTS)
                 VALUES (28131,0,'Дата отключения услуги не может быть меньше даты подключения услуги');

  INSERT INTO DBANK_MSG (T_NUMBER,T_PAGE,T_CONTENTS)
                 VALUES (28132,0,'Дата отключения услуги не может быть меньше текущего операционного дня');

  INSERT INTO DBANK_MSG (T_NUMBER,T_PAGE,T_CONTENTS)
                 VALUES (28133,0,'Услуга уже подключена');

  INSERT INTO DBANK_MSG (T_NUMBER,T_PAGE,T_CONTENTS)
                 VALUES (28134,0,'Услуга "Инвестиционное консультирование" не может быть подключена для договора ИИС');

  INSERT INTO DBANK_MSG (T_NUMBER,T_PAGE,T_CONTENTS)
                 VALUES (28135,0,'Услуга "Инвестиционное консультирование" может быть подключена только для ДБО с тарифным планом "Инвестор"');

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

