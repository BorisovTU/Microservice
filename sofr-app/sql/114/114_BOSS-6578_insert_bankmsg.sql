-- Сообщения
BEGIN
  INSERT INTO dBank_msg (t_Number, t_Page, t_Contents)
                 VALUES (28145, 0, 'Операция не применима для бессрочных договоров');
END;
/