DECLARE
  cnt NUMBER;
BEGIN
  SELECT COUNT (1)
    INTO cnt
    FROM DOBJECTS_DBT
   WHERE T_OBJECTTYPE = 2549;

  IF CNT = 0
  THEN
  INSERT INTO DOBJECTS_DBT (
     T_CODE, T_MODULE, T_NAME, 
     T_OBJECTTYPE, T_PARENTOBJECTTYPE, T_SERVICEMACRO, 
     T_USERNUMBER) 
  VALUES ( 'ТипСУБCDI',
   chr(0),
   'Тип субъекта экономики CDI',
   2549,
   0,
   chr(1),
  -1);

  INSERT INTO DLLVALUES_DBT (
     T_LIST, T_ELEMENT, T_FLAG, 
     T_CODE, T_NAME, T_RESERVE, 
     T_NOTE) 
  VALUES ( 2549, 1, 0, 'ЮЛ', 'Юридическое лицо', chr(1), 'Юридическое лицо' );

  INSERT INTO DLLVALUES_DBT (
     T_LIST, T_ELEMENT, T_FLAG, 
     T_CODE, T_NAME, T_RESERVE, 
     T_NOTE) 
  VALUES ( 2549, 2, 0, 'ФЛ', 'Физическое лицо', chr(1), 'Физическое лицо' );

  INSERT INTO DLLVALUES_DBT (
     T_LIST, T_ELEMENT, T_FLAG, 
     T_CODE, T_NAME, T_RESERVE, 
     T_NOTE) 
  VALUES ( 2549, 3, 0, 'ФП', 'Физическое лицо - предприниматель', chr(1), 'Физическое лицо - предприниматель' );

  INSERT INTO DLLVALUES_DBT (
     T_LIST, T_ELEMENT, T_FLAG, 
     T_CODE, T_NAME, T_RESERVE, 
     T_NOTE) 
  VALUES ( 2549, 4, 0, 'ЮЛ Рез', 'Юридическое лицо резидент', chr(1), 'Юридическое лицо резидент' );

  INSERT INTO DLLVALUES_DBT (
     T_LIST, T_ELEMENT, T_FLAG, 
     T_CODE, T_NAME, T_RESERVE, 
     T_NOTE) 
  VALUES ( 2549, 5, 0, 'ЮЛ НеРез', 'Юридическое лицо нерезидент', chr(1), 'Юридическое лицо нерезидент' );

  INSERT INTO DLLVALUES_DBT (
     T_LIST, T_ELEMENT, T_FLAG, 
     T_CODE, T_NAME, T_RESERVE, 
     T_NOTE) 
  VALUES ( 2549, 6, 0, 'ИП', 'Индивидуальный предприниматель', chr(1), 'Индивидуальный предприниматель' );

  INSERT INTO DLLVALUES_DBT (
     T_LIST, T_ELEMENT, T_FLAG, 
     T_CODE, T_NAME, T_RESERVE, 
     T_NOTE) 
  VALUES ( 2549, 7, 0, 'ФЛЧП', 'Физическое лицо, занимающееся частной практикой', chr(1), 'Физическое лицо, занимающееся частной практикой' );

  COMMIT;
  END IF;
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

BEGIN
  update dptdupprm_dbt set t_partytype = 4 where t_paramsetid = 1228;

  update dptdupprm_dbt set t_partytype = 5 where t_paramsetid in (1227,1229);

  update dptdupprm_dbt set t_partytype = 6 where t_paramsetid = 1231;

  update dptdupprm_dbt set t_partytype = 7 where t_paramsetid = 1230;
  
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/