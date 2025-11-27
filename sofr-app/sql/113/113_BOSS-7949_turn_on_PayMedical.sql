-- Изменения по BOSS-6400 BOSS-7949
-- Включение настройки 'РСХБ/БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ/ОПЛАТА_ДОРОГО_ЛЕЧЕНИЯ/'
DECLARE
  logID VARCHAR2(32) := 'BOSS-7949';
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
  -- Возвращает 1, если есть значение настройки в dregparm_dbt
  FUNCTION GetRegVal(p_ParentId IN number, p_KeyName IN VARCHAR2, p_KeyID OUT NUMBER )
    RETURN NUMBER
  IS
    x_Count number;
  BEGIN
    SELECT t_keyid 
      INTO p_KeyId 
      FROM dregparm_dbt r 
      WHERE r.t_parentid = p_ParentId AND r.t_name = p_KeyName AND rownum < 2;
    SELECT nvl(count(*),0) 
      INTO x_Count 
      FROM dregval_dbt 
      WHERE t_keyID = p_KeyID;
    RETURN x_Count;
  EXCEPTION
     WHEN no_data_found THEN 
       p_KeyID := -1;
       RETURN 0;
  END;
  -- Добавление новой настройки
  PROCEDURE AddNewReg( p_ParentId IN number, p_KeyName IN VARCHAR2, p_KeyDesc IN VARCHAR2, p_LIntValue IN NUMBER )
  AS
    x_KeyId number;
  BEGIN
    LogIt('Добавление настройки '''||p_KeyName||'''');
    INSERT INTO dregparm_dbt (
       t_keyid, t_parentid, t_type, t_name
       , t_global, t_description, t_security, t_isbranch
    ) VALUES (
       0, p_ParentId, 4, p_KeyName, 'X', p_KeyDesc, chr(0), chr(0)
    )
    RETURNING t_KeyId INTO x_KeyId;
    INSERT INTO dregval_dbt (
       T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP
       , T_LINTVALUE, T_LDOUBLEVALUE, t_fmtblobdata_xxxx
    ) VALUES (
       x_KeyId, 0, 0, chr(0), 0, p_LIntValue, 0, ''
    );
    LogIt('Добавлена настройка '''||p_KeyName||'''');
  EXCEPTION
    WHEN OTHERS THEN
       LogIt('Ошибка добавления настройки '''||p_KeyName||'''');
  END;
  -- Изменение настройки
  PROCEDURE UpdateReg( p_KeyID IN number, p_LIntValue IN number )
  AS
  BEGIN
    LogIt('Изменение настройки '''||p_KeyID||'''');
    UPDATE dregval_dbt SET T_LINTVALUE = p_LIntValue WHERE T_KEYID = p_KeyId;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Изменена настройка '''||p_KeyID||'''');
  EXCEPTION
    WHEN OTHERS THEN
       LogIt('Ошибка изменения настройки '''||p_KeyID||'''');
       EXECUTE IMMEDIATE 'ROLLBACK';
  END;
  -- Добавление (или изменение) настройки
  PROCEDURE AddReg( p_PathParent IN VARCHAR2, p_KeyName IN VARCHAR2, p_KeyDesc IN VARCHAR2, p_Val IN VARCHAR2 )
  AS
    x_KeyId number;
    x_ParentId number;
    x_LIntValue number;
    x_Count number;
  BEGIN
    x_LIntValue := GetLIntValue( p_Val );
    x_ParentId := GetParentKey( p_PathParent );
    x_Count := GetRegVal( x_ParentId, p_KeyName, x_KeyId );
    IF(x_Count > 0) THEN
      -- настройка есть, нужно обновить
      UpdateReg( x_KeyId, x_LIntValue );
    ELSE
      -- настройки нет
      AddNewReg( x_ParentId, p_KeyName, p_KeyDesc, x_LIntValue );
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
       LogIt('Ошибка добавления настройки '''||p_KeyName||'''');
       EXECUTE IMMEDIATE 'ROLLBACK';
  END;
BEGIN
  -- Включение настройки (настройка добавлена ранее в ВЫКЛЮЧЕННОМ состоянии)
  AddReg( 'РСХБ/БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ', 'ОПЛАТА_ДОРОГО_ЛЕЧЕНИЯ', '', 'X' );
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
