DECLARE
   v_Count NUMBER := 0;
BEGIN
   DELETE FROM DNAMEALG_DBT WHERE T_ITYPEALG = 3183;

   INSERT ALL
      INTO DNAMEALG_DBT (t_ITypeAlg, t_INumberAlg, t_szNameAlg, t_ILenName, t_IQuantAlg, t_Reserve) VALUES (3183, 0, 'Отложена', 24, 4, '')
      INTO DNAMEALG_DBT (t_ITypeAlg, t_INumberAlg, t_szNameAlg, t_ILenName, t_IQuantAlg, t_Reserve) VALUES (3183, 1, 'Выполняется', 24, 4, '')
      INTO DNAMEALG_DBT (t_ITypeAlg, t_INumberAlg, t_szNameAlg, t_ILenName, t_IQuantAlg, t_Reserve) VALUES (3183, 2, 'Ожидает получения ответа', 24, 4, '')
      INTO DNAMEALG_DBT (t_ITypeAlg, t_INumberAlg, t_szNameAlg, t_ILenName, t_IQuantAlg, t_Reserve) VALUES (3183, 3, 'Выполнена', 24, 3, '')
   SELECT * FROM DUAL;
END;
/