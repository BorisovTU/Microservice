--Добавить новый справочник
BEGIN
  INSERT INTO DOBJECTS_DBT (T_OBJECTTYPE, T_NAME, T_CODE, T_USERNUMBER, T_PARENTOBJECTTYPE, T_SERVICEMACRO, T_MODULE)
     VALUES(4164,'Особые условия льгот НДФЛ','ОсобУсЛг',-1,0,CHR(1),CHR(0));

  INSERT INTO DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     VALUES(4164,1,'1','Количество реализованных ФЛ акций не превышает 1 процента',1,'В налоговом периоде количество реализованных ФЛ акций не превышает 1 процента общего количества акций этой организации эмитента',CHR(1));
    
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
     
END;
/



