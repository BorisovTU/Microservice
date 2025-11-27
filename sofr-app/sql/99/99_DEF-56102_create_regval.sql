-- Изменения по DEF-56102, добавление настройки 
DECLARE
  logID VARCHAR2(9) := 'DEF-56102';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Возвращает числовое значения для флаговой настройки
  FUNCTION GetLIntValue( p_Val IN VARCHAR2 )
    RETURN NUMBER
  IS
    x_LIntValue NUMBER;
  BEGIN
     IF nvl(p_Val, '') = 'X' THEN
        x_LIntValue := 88;
     ELSE
        x_LIntValue := 0;
     END IF;
     RETURN x_LIntValue;
  END;
  -- Поиск ID родительской настройки
  FUNCTION GetParentKey(p_PathParent IN VARCHAR2)
    RETURN NUMBER
  IS
    x_PrnID NUMBER := 0;
  BEGIN
     BEGIN
       SELECT T_KEYID INTO x_PrnID
         FROM( SELECT p.t_keyid, SYS_CONNECT_BY_PATH(p.t_name, '/') Path
                FROM dregparm_dbt p, dregval_dbt v
               WHERE v.t_keyid = p.t_keyid and v.t_objectid = 0
             CONNECT BY PRIOR p.t_keyid = p.t_parentid 
             START WITH p.t_parentid = 0
        )
        WHERE Path = '/'||p_PathParent;
     EXCEPTION
        WHEN no_data_found THEN x_PrnID := 0;
     END; 
     RETURN x_PrnID;
  END;
  -- Добавление настройки
  PROCEDURE AddReg( p_PathParent IN VARCHAR2, p_KeyName IN VARCHAR2, p_KeyDesc IN VARCHAR2, p_Val IN VARCHAR2 )
  AS
    x_KeyId number;
    x_ParentId number;
    x_LIntValue number;
  BEGIN
    x_LIntValue := GetLIntValue( p_Val );
    x_ParentId := GetParentKey( p_PathParent );
    LogIt('Добавление настройки '''||p_KeyName||'''');
    INSERT INTO dregparm_dbt (
       t_keyid, t_parentid, t_type, t_name
       , t_global, t_description, t_security, t_isbranch
    ) VALUES (
       0, x_ParentId, 4, p_KeyName, 'X', p_KeyDesc, chr(0), chr(0)
    )
    RETURNING t_KeyId INTO x_KeyId;
    INSERT INTO dregval_dbt (
       T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP
       , T_LINTVALUE, T_LDOUBLEVALUE
    ) VALUES (
       x_KeyId, 0, 0, chr(0), 0, x_LIntValue, 0
    );
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Добавлена настройка '''||p_KeyName||'''');
  EXCEPTION
    WHEN OTHERS THEN
       LogIt('Ошибка добавления настройки '''||p_KeyName||'''');
       EXECUTE IMMEDIATE 'ROLLBACK';
  END;
BEGIN
  -- Добавление настройки
  AddReg( 'РСХБ/ИНТЕГРАЦИЯ', 'ЖЕСТКИЙ_ЗАПРЕТ_ОТКАТА', 'жесткий запрет отката операции', 'X' );
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
