DECLARE
  v_ObjType NUMBER := 4180;
  v_Counter NUMBER := 1;

  PROCEDURE InsertValue(p_Key IN VARCHAR2, p_Value IN VARCHAR2)
  IS
  BEGIN
    INSERT INTO DLLVALUES_DBT(T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
    VALUES(v_ObjType, v_Counter, v_Counter, p_Key, v_Counter, p_Value, CHR(1));

    v_Counter := v_Counter + 1;
  END; 
BEGIN
  INSERT INTO DOBJECTS_DBT(T_OBJECTTYPE, T_NAME, T_CODE, T_USERNUMBER, T_PARENTOBJECTTYPE, T_SERVICEMACRO, T_MODULE)
  VALUES(v_ObjType, 'Справочник статических header-ов для обмена с ЕФР', 'ЕФРHeader', 0, 0, CHR(1), CHR(0));

  InsertValue('x-source-system',  'SOFR');
  InsertValue('x-source-service', 'ResendStateOfficerSertEfr');
END;
/
