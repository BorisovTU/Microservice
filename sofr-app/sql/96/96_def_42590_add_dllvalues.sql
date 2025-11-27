DECLARE
 V_OBJECTTYPE NUMBER (10);
BEGIN
SELECT MAX(T_OBJECTTYPE)+1 INTO V_OBJECTTYPE FROM dobjects_dbt;
  INSERT INTO dobjects_dbt (T_OBJECTTYPE,T_NAME,T_CODE,T_USERNUMBER,T_PARENTOBJECTTYPE,T_SERVICEMACRO,T_MODULE) VALUES (V_OBJECTTYPE,'Техн. спр-к ошибок-исключений OpenAccounts','OA_SkipEr',0,0,null,0);                         
  INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE) VALUES (V_OBJECTTYPE,1,001,'Клиент не найден',1,'Клиент не найден',null);         
  INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE) VALUES (V_OBJECTTYPE,5,005,'Такой счет уже существует',5,'Такой счет уже существует',null);            
  INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE) VALUES (V_OBJECTTYPE,99,099,'Счет не найден',99,'Счет не найден',null) ;            
  COMMIT;
END;/   