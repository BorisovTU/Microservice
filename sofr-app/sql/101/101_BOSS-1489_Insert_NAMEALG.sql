DECLARE
   v_Count NUMBER := 0;
BEGIN
   SELECT COUNT(1) INTO v_Count
   FROM DNAMEALG_DBT
   WHERE t_ITypeAlg = 3183;

   IF v_Count = 0
   THEN
      INSERT ALL
         INTO DNAMEALG_DBT (t_ITypeAlg, t_INumberAlg, t_szNameAlg, t_ILenName, t_IQuantAlg, t_Reserve) VALUES (3183, 0, 'Отложена', 11, 3, '')
         INTO DNAMEALG_DBT (t_ITypeAlg, t_INumberAlg, t_szNameAlg, t_ILenName, t_IQuantAlg, t_Reserve) VALUES (3183, 1, 'Ожидает получения ответа', 11, 3, '')
         INTO DNAMEALG_DBT (t_ITypeAlg, t_INumberAlg, t_szNameAlg, t_ILenName, t_IQuantAlg, t_Reserve) VALUES (3183, 2, 'Выполнена', 11, 3, '')
      SELECT * FROM DUAL;
   ELSE
      IT_LOG.LOG('BOSS-1489. Таблица DNAMEALG_DBT уже содержит значения с t_ITypeAlg = 3183');
   END IF;
END;
/