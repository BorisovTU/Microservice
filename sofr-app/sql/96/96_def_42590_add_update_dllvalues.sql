DECLARE
 V_OBJECTTYPE NUMBER (10) ;
 V_OBJECTTYPE_NEW NUMBER (10);
BEGIN
 
 BEGIN   
   SELECT T_OBJECTTYPE INTO V_OBJECTTYPE FROM dobjects_dbt where t_code = 'OA_SkipEr';
     EXCEPTION WHEN NO_DATA_FOUND THEN
        V_OBJECTTYPE := 0;
 END;
            
 IF V_OBJECTTYPE = 0 THEN
   SELECT MAX(T_OBJECTTYPE)+1 INTO V_OBJECTTYPE_NEW FROM dobjects_dbt;
   INSERT INTO dobjects_dbt (T_OBJECTTYPE,T_NAME,T_CODE,T_USERNUMBER,T_PARENTOBJECTTYPE,T_SERVICEMACRO,T_MODULE) VALUES (V_OBJECTTYPE_NEW,'Техн. спр-к ошибок-исключений OpenAccounts','OA_SkipEr',0,0,null,0);                         
   INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE) VALUES (V_OBJECTTYPE_NEW,1,'001','Клиент не найден',1,'Клиент не найден',null);         
   INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE) VALUES (V_OBJECTTYPE_NEW,5,'005','Такой счет уже существует',5,'Такой счет уже существует',null);            
   INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE) VALUES (V_OBJECTTYPE_NEW,99,'099','Счет не найден',99,'Счет не найден',null) ;            
 ELSE
   update dllvalues_dbt set t_code = '001' where t_list = V_OBJECTTYPE and t_name = 'Клиент не найден';
   update dllvalues_dbt set t_code = '005' where t_list = V_OBJECTTYPE and t_name = 'Такой счет уже существует';
   update dllvalues_dbt set t_code = '099' where t_list = V_OBJECTTYPE and t_name = 'Счет не найден';       
 END IF;
 
  COMMIT;
END;/   