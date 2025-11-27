--Не отделять заблокированные ЦБ из тарификации
BEGIN
  INSERT INTO DOBJATCOR_DBT (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_ISAUTO)
  (SELECT 57, 104, 1, LPAD(T_SFPLANID, 10, '0'), CHR(88), TO_DATE('01/01/0001', 'DD/MM/YYYY'), 1, TO_DATE('31/12/9999', 'DD/MM/YYYY'), CHR(88)
     FROM DSFPLAN_DBT --До запуска макроса установим категорию на все ТП, а там уже выборочно ее снимем
  );
END;
/