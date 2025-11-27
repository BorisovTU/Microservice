--Добавить новыые справочники
BEGIN
  INSERT INTO DOBJECTS_DBT (T_OBJECTTYPE, T_NAME, T_CODE, T_USERNUMBER, T_PARENTOBJECTTYPE, T_SERVICEMACRO, T_MODULE)
     VALUES(4158,'КУ для перечисления НДФЛ','КУпрчНДФЛ',0,0,CHR(1),CHR(0));

  INSERT INTO DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     VALUES(4158,1,'13%','НДФЛ к перечислению, FLR',13,'Налог на доходы физических лиц к перечислению по ставке 13% (для резидентов)/ 30% (для нерезидентов)',CHR(1));

  INSERT INTO DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     VALUES(4158,2,'30%','НДФЛ к перечислению, FLR',30,'Налог на доходы физических лиц к перечислению по ставке 13% (для резидентов)/ 30% (для нерезидентов)',CHR(1));

  INSERT INTO DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     VALUES(4158,3,'15%','НДФЛ к перечислению 15%',15,'Налог на доходы физических лиц к перечислению, рассчитанный  по ставке 15%',CHR(1));

  INSERT INTO DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     VALUES(4158,4,'18%','НДФЛ к перечислению 18%',18,'Налог на доходы физических лиц к перечислению, рассчитанный  по ставке 18%',CHR(1));

  INSERT INTO DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     VALUES(4158,5,'20%','НДФЛ к перечислению 20%',20,'Налог на доходы физических лиц к перечислению, рассчитанный  по ставке 20%',CHR(1));

  INSERT INTO DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     VALUES(4158,6,'22%','НДФЛ к перечислению 22%',22,'Налог на доходы физических лиц к перечислению, рассчитанный  по ставке 22%',CHR(1));

EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
     
END;
/

BEGIN
  INSERT INTO DOBJECTS_DBT (T_OBJECTTYPE, T_NAME, T_CODE, T_USERNUMBER, T_PARENTOBJECTTYPE, T_SERVICEMACRO, T_MODULE)
     VALUES(4159,'КУ для возврата НДФЛ','КУвзрНДФЛ',0,0,CHR(1),CHR(0));

  INSERT INTO DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     VALUES(4159,1,'13%','НДФЛ к возврату',13,'Налог на доходы физических лиц излишне уплаченный, подлежащий возврату по ставке 13% (для резидентов)/ 30% (для нерезидентов)',CHR(1));

  INSERT INTO DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     VALUES(4159,2,'30%','НДФЛ к возврату',30,'Налог на доходы физических лиц излишне уплаченный, подлежащий возврату по ставке 13% (для резидентов)/ 30% (для нерезидентов)',CHR(1));

  INSERT INTO DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     VALUES(4159,3,'15%','НДФЛ к возврату_15%',15,'Налог на доходы физических лиц излишне уплаченный, подлежащий возврату по ставке 15% (372-ФЗ)',CHR(1));

  INSERT INTO DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     VALUES(4159,4,'18%','НДФЛ к возврату_18%',18,'Налог на доходы физических лиц излишне уплаченный, подлежащий возврату по ставке 18% (372-ФЗ)',CHR(1));

  INSERT INTO DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     VALUES(4159,5,'20%','НДФЛ к возврату_20%',20,'Налог на доходы физических лиц излишне уплаченный, подлежащий возврату по ставке 20% (372-ФЗ)',CHR(1));

  INSERT INTO DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     VALUES(4159,6,'22%','НДФЛ к возврату_22%',22,'Налог на доходы физических лиц излишне уплаченный, подлежащий возврату по ставке 22% (372-ФЗ)',CHR(1));

EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
     
END;
/

