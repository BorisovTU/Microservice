-- Изменение времени работы планировшика
DECLARE
    cnt    NUMBER;
BEGIN
SELECT COUNT (*)
  INTO cnt
  FROM DLLVALUES_DBT
 WHERE T_LIST = 4142 AND T_CODE = 'SHPTWDAY';
 IF (cnt = 0)
 THEN
    INSERT INTO DLLVALUES_DBT (T_CODE,
                               T_ELEMENT,
                               T_FLAG,
                               T_LIST,
                               T_NAME,
                               T_NOTE,
                               T_RESERVE)
         VALUES ('SHPTWDAY',
                 4,
                 4,
                 4142,
                 'День',
                 CHR (1),
                 CHR (1));
  END IF;

UPDATE DSS_SHEDULER_DBT
   SET T_STARTTIME = TO_DATE ('01.01.0001 02:53:01', 'dd.mm.yyyy hh24:mi:ss'),
       T_WORKENDTIME =
           TO_DATE ('01.01.0001 05:53:01', 'dd.mm.yyyy hh24:mi:ss'),
       T_WORKSTARTTIME =
           TO_DATE ('01.01.0001 02:53:01', 'dd.mm.yyyy hh24:mi:ss')
 WHERE T_NAME = 'Обмен данными с AC CDI - КонтрID';

UPDATE DSS_SHEDULER_DBT
   SET T_STARTTIME = TO_DATE ('01.01.0001 03:05:01', 'dd.mm.yyyy hh24:mi:ss'),
       T_WORKENDTIME =
           TO_DATE ('01.01.0001 23:59:01', 'dd.mm.yyyy hh24:mi:ss'),
       T_WORKSTARTTIME =
           TO_DATE ('01.01.0001 03:05:01', 'dd.mm.yyyy hh24:mi:ss'),
       T_PERIOD = 3
 WHERE T_NAME = 'Обмен данными с AC CDI';

COMMIT;
END;
/