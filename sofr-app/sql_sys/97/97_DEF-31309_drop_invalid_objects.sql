-- Удаление невалидных объектов
DECLARE
  PROCEDURE drop_object(p_objecttype VARCHAR2, p_user VARCHAR2, p_objectname VARCHAR2)
  AS
  BEGIN
    EXECUTE IMMEDIATE 'DROP ' || p_objecttype || ' ' || p_user || '.' || p_objectname;
    
    EXCEPTION 
        WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE('Ошибка: '||SQLCODE||' - '||SQLERRM);
  END;
BEGIN
  drop_object('VIEW', 'RSHB_SOFR', 'V1003GOOD');
  drop_object('VIEW', 'RSHB_SOFR', 'V1003BEFORE');
  drop_object('VIEW', 'RSHB_SOFR', 'V1003ОN_PERIOD');
  drop_object('TRIGGER', 'RSHB_SOFR', 'DRSHB_CB_DBT_TRG');
  drop_object('VIEW', 'SHCHERBININSV', 'DWLDLFX_DBT');
  drop_object('PACKAGE', 'SHCHERBININSV', 'RSHB_RSI_SCLIMIT');
  drop_object('PACKAGE', 'SHCHERBININSV', 'RSHB_LIMIT_UTIL');
END;
/