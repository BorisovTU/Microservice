-- Добавление сообщения в dbank_msg
DECLARE
BEGIN
  INSERT INTO dbank_msg (t_number, t_page, t_contents) VALUES (20453, 0, 'Ошибка ввода: дублирование записи (%s)');
END;
/