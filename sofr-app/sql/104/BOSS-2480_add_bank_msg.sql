BEGIN
  INSERT INTO DBANK_MSG (T_NUMBER,T_PAGE,T_CONTENTS)
                 VALUES (26060,0,'Не может быть старше, чем %d дней назад от даты "Период по"');
END;
/