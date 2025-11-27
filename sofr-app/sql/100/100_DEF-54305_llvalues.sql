--Добавление записи в справочник
BEGIN
  INSERT INTO DLLVALUES_DBT(T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
              VALUES(5003, 4645, 'PmOpr4645', 'Соответствие DocKind pmpaym и oproper', 450, 'Соответствие DocKind pmpaym и oproper', CHR(1));
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/


BEGIN
 EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/