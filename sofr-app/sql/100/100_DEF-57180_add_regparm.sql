--
-- Добавление новых настроек реестра
--
-- T_TYPE:
-- INTEGER = 0;
-- DOUBLE = 1;
-- STRING = 2;
-- BINARY = 3;
-- FLAG = 4;

--
-- BANK_INI\\ОБЩИЕ ПАРАМЕТРЫ\\LICMESSAGE\\LIC_REGCOCTS
-- BANK_INI\\ОБЩИЕ ПАРАМЕТРЫ\\LICMESSAGE\\LIC_COCTSINFO
 
DECLARE
 v_ParentID NUMBER(10);
 v_KeyID    NUMBER(10);
BEGIN

  SELECT t_KeyID INTO v_ParentID
  FROM dregparm_dbt 
  WHERE t_Name = 'BANK_INI' AND t_ParentID = 0;

  SELECT t_KeyID INTO v_ParentID
  FROM dregparm_dbt 
  WHERE t_Name = 'ОБЩИЕ ПАРАМЕТРЫ' AND t_ParentID = v_ParentID;
  
  SELECT t_KeyID INTO v_ParentID
  FROM dregparm_dbt 
  WHERE t_Name = 'LICMESSAGE' AND t_ParentID = v_ParentID;

  BEGIN

    INSERT INTO dregparm_dbt(T_KEYID, T_PARENTID, T_NAME, T_TYPE, T_GLOBAL, T_DESCRIPTION, T_SECURITY, T_ISBRANCH, T_TEMPLATE)
       VALUES(0, v_ParentID, 'LIC_REGCOCTS', 4, 'X', 'При обращении к лицензируемому функционалу в проектах регистрировать расход. YES - регистрировать; NO - не регистрировать', chr(0), chr(0), chr(1))
     RETURNING T_KEYID INTO v_KeyID;

    INSERT INTO dregval_dbt(T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
       VALUES(v_KeyID, 0, 0, chr(0), 0, 0, 0, '');

  EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN NULL;
  END;
  
  BEGIN
  
  INSERT INTO dregparm_dbt(T_KEYID, T_PARENTID, T_NAME, T_TYPE, T_GLOBAL, T_DESCRIPTION, T_SECURITY, T_ISBRANCH, T_TEMPLATE)
        VALUES(0, v_ParentID, 'LIC_COCTSINFO', 4, 'X', 'При входе в панель "Информация о лицензиях" пересчитывать расход. YES - пересчитывать; NO - не пересчитывать', chr(0), chr(0), chr(1))
      RETURNING T_KEYID INTO v_KeyID;

    INSERT INTO dregval_dbt(T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
      VALUES(v_KeyID, 0, 0, chr(0), 0, 88, 0, '');
      
  EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN NULL;
  END;
  

EXCEPTION
  WHEN NO_DATA_FOUND THEN NULL;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/