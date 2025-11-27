-- Добавление сообщения в dbank_msg

DECLARE
  v_cnt NUMBER := 0;
BEGIN
  SELECT COUNT(1) INTO v_cnt
    FROM dbank_msg
   WHERE t_Number = 28074;
   
   IF (v_cnt = 0) THEN
     INSERT INTO dbank_msg
       (t_Number, t_Page, t_Contents)
     VALUES
       (28074, 0, 'Для юридического лица должен быть заполнен параметр TRD_RESTR');
   END IF;
   
   COMMIT;

EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/