-- Добавление нового типа в dimgtype_dbt
DECLARE
  v_cnt NUMBER;
  logID VARCHAR2(50) := 'BOSS-194-1 Add ImgType';
BEGIN
  SELECT COUNT(1) INTO v_cnt
    FROM dImgType_dbt
   WHERE t_objectType = 3 AND t_imageType = 4;
  
  IF v_cnt = 0 THEN
    INSERT INTO dImgType_dbt
      (t_imageType,
       t_name,
       t_isDefault,
       t_objectType,
       t_subSystems)
    VALUES
      (4,
       'Подпись сотрудника с печатью банка',
       CHR(0),
       3,
       CHR(1));

     COMMIT;
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/