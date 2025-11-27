DECLARE
  v_cnt NUMBER;
BEGIN
  SELECT COUNT(1) INTO v_cnt
    FROM dbank_msg
   WHERE t_number = 31979;
  IF v_cnt = 0 THEN
    INSERT 
      INTO dbank_msg 
        (t_number, t_page, t_contents)
    VALUES 
        (31979, 0, 'Запрещено сохранение купона при выставленном|признаке "Отказ от выплаты" и датой погашения|менее текущего опердня');
  END IF;
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