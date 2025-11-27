/* Linux. Регресс. Замедление более чем в 5 раз Биржевого автомата ММВБ. Загрузка в БО сделок/заявок/клиринга. высокая Загрузка CPU */

DECLARE
 v_ParentID NUMBER(10);
 v_KeyID    NUMBER(10);
BEGIN
  -- COMMON\FUNCOBJ\USE_FUNCOBJ_TASK
  SELECT t_KeyID INTO v_ParentID
  FROM dregparm_dbt 
  WHERE t_Name = 'COMMON' AND t_ParentID = 0;

  SELECT t_KeyID INTO v_ParentID
  FROM dregparm_dbt 
  WHERE t_Name = 'FUNCOBJ' AND t_ParentID = v_ParentID;

  BEGIN
    INSERT INTO dregparm_dbt(T_KEYID, T_PARENTID, T_NAME, T_TYPE, T_GLOBAL, T_DESCRIPTION, T_SECURITY, T_ISBRANCH, T_TEMPLATE)
       VALUES(0, v_ParentID, 'USE_FUNCOBJ_TASK', 2, chr(0), 'Номера варианта запуска с использованием FuncObj, через запятую', chr(0), chr(0), chr(1))
     RETURNING T_KEYID INTO v_KeyID;

    INSERT INTO dregval_dbt(T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
       VALUES(v_KeyID, 0, 0, chr(0), 0, 0, 0, '');

  EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN NULL;
  END;

EXCEPTION
  WHEN NO_DATA_FOUND THEN NULL;
END;
/