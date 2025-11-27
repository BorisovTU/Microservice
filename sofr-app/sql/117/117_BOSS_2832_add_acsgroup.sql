-- Добавление в СУД новой группы 
DECLARE
  v_cnt NUMBER;
  logID VARCHAR2(50) := 'BOSS-2832 Add AcsGroup';
BEGIN
  SELECT COUNT(1) INTO v_cnt
    FROM dAcsGroup_dbt
   WHERE t_name = 'Подписанты налоговой отчетности';
  
  IF v_cnt = 0 THEN
    INSERT INTO dacsgroup_dbt
      (t_groupID,
       t_name, 
       t_comment, 
       t_isNotLocal, 
       t_department, 
       t_isOuterSystem)
    VALUES 
      ((SELECT MAX(t_groupID) FROM dAcsGroup_dbt) + 1,
       'Подписанты налоговой отчетности',
       CHR(1),
       CHR(0),
       1,
       CHR(0));

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