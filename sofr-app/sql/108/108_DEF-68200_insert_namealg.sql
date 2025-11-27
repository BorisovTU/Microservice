-- Добавление записей по статусам заявок в dNameAlg_dbt
DECLARE
  ALG_DL_REQ_STATUS NUMBER := 3167;
  ALG_DL_REQ_ORDERSTATUS NUMBER := 3166;
  v_cnt NUMBER;
BEGIN
  SELECT COUNT(1) INTO v_cnt
    FROM dNameAlg_dbt
   WHERE t_iTypeAlg = ALG_DL_REQ_STATUS
     AND t_iNumberAlg = 6;
     
  IF v_cnt = 0 THEN
    INSERT INTO dNameAlg_dbt
            (t_iTypeAlg, t_iNumberAlg, t_szNameAlg, t_iLenName, t_iQuantAlg, t_Reserve)
     VALUES (ALG_DL_REQ_STATUS, 6, 'ожидает активации', 24, 7, chr(1));
  END IF;
  
  SELECT COUNT(1) INTO v_cnt
    FROM dNameAlg_dbt
   WHERE t_iTypeAlg = ALG_DL_REQ_STATUS
     AND t_iNumberAlg = 7;
     
  IF v_cnt = 0 THEN
    INSERT INTO dNameAlg_dbt
            (t_iTypeAlg, t_iNumberAlg, t_szNameAlg, t_iLenName, t_iQuantAlg, t_Reserve)
     VALUES (ALG_DL_REQ_STATUS, 7, 'активна', 24, 7, chr(1));
    
    UPDATE dNameAlg_dbt
       SET t_iQuantAlg = 7
     WHERE t_iTypeAlg = ALG_DL_REQ_STATUS;
  END IF;
  
  SELECT COUNT(1) INTO v_cnt
    FROM dNameAlg_dbt
   WHERE t_iTypeAlg = ALG_DL_REQ_ORDERSTATUS
     AND t_iNumberAlg = 4;
     
  IF v_cnt = 0 THEN
    INSERT INTO dNameAlg_dbt
            (t_iTypeAlg, t_iNumberAlg, t_szNameAlg, t_iLenName, t_iQuantAlg, t_Reserve)
     VALUES (ALG_DL_REQ_ORDERSTATUS, 4, 'Пока не исполнено', 19, 4, chr(1));
    
    UPDATE dNameAlg_dbt
       SET t_iQuantAlg = 4
     WHERE t_iTypeAlg = ALG_DL_REQ_ORDERSTATUS;
  END IF;
END;
/