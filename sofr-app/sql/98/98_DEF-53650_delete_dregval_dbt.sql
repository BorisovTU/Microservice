--удаление сохраненных параметров скроллинга Записей репликации (Шлюз)
DECLARE
   v_cnt NUMBER := 0;
   v_parmName VARCHAR2(30) := '1001_-DIR:1 -_S_GTREC';
BEGIN
   SELECT COUNT(1) INTO v_cnt
     FROM dregparm_dbt parm
     WHERE parm.t_Name = v_parmName;
   
   IF (v_cnt = 1) THEN
     DELETE 
        FROM dregval_dbt val
        WHERE val.t_KeyID = (SELECT parm.t_KeyID
                               FROM dregparm_dbt parm 
                               WHERE parm.t_Name = v_parmName)
          AND t_regkind = 2;
   END IF;
   
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