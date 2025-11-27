DECLARE
    cnt   NUMBER;
BEGIN
  SELECT COUNT (*)
    INTO cnt
    FROM DFUNC_DBT
   WHERE T_NAME = 'Сервис создания карточек клиентов ЮЛ в АС CDI';

  IF CNT = 0
  THEN
    INSERT INTO dfunc_dbt(T_FUNCID, T_CODE, T_NAME, T_TYPE, T_FILENAME, T_FUNCTIONNAME, T_INTERVAL, T_VERSION)
      VALUES(5056, 'CdiCreateOrg', 'Сервис создания карточек клиентов ЮЛ в АС CDI', 1, 'ws_synch_SOFR', 'ws_CdiCreateOrg', 0, 0);
  
    INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE)
      VALUES (5002,5056,'5056','Сервис создания карточек клиентов ЮЛ в АС CDI',5056,'',CHR (1)) ;   

  COMMIT;
  END IF;
END;