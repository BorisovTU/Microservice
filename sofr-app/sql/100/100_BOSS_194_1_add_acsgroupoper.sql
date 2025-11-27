-- Добавление пользователей в группу СУД
DECLARE
  groupID NUMBER;
  logID VARCHAR2(50) := 'BOSS-194-1 Add AcsGroupOper';
  PROCEDURE AddGroupUser(p_groupID IN NUMBER, p_user IN NUMBER)
  IS
    v_cnt NUMBER;
  BEGIN
    SELECT COUNT(1) INTO v_cnt 
      FROM dAcsGroupOper_dbt
     WHERE t_groupID = p_groupID
       AND t_oper = p_user;
    IF v_cnt = 0 THEN
      INSERT INTO Dacsgroupoper_Dbt
        (t_groupID, t_Oper, t_isInherited, t_createMode)
      VALUES
        (p_groupID, p_user, CHR(0), 1);
    END IF;
  END;
BEGIN
  BEGIN
    SELECT t_groupID INTO groupID
      FROM dAcsGroup_dbt
     WHERE t_name = 'Подписанты отчетности по КИ';
  EXCEPTION WHEN OTHERS THEN
    groupID := 0;
  END;   
  
  IF groupID > 0 THEN
    AddGroupUser(groupID, 1559);
    AddGroupUser(groupID, 1566);
    AddGroupUser(groupID, 1906);
  END IF;
  
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/