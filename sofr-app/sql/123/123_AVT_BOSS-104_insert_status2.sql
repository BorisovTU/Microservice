
DECLARE
t_count integer;
BEGIN
  
SELECT count(*) INTO t_count from DOPRSTVAL_DBT where T_STATUSKINDID = 484131 AND T_NUMVALUE = 4;

if t_count = 0 then
  INSERT INTO DOPRSTVAL_DBT(T_STATUSKINDID,T_NUMVALUE,T_NAME,T_ELIMINATED)
  VALUES(484131,4,'Закрытие',CHR(0));
end if;

INSERT INTO doprsblck_dbt (t_blockid, t_statuskindid, t_numvalue, t_default)
SELECT 203713, 484131, 4, chr(0)
FROM dual 
WHERE NOT EXISTS ( SELECT 1 FROM doprsblck_dbt WHERE t_blockid = 203713 AND t_statuskindid = 484131 AND t_numvalue = 4);

INSERT INTO doprsblck_dbt (t_blockid, t_statuskindid, t_numvalue, t_default)
SELECT 203714, 484131, 4, chr(0)
FROM dual 
WHERE NOT EXISTS ( SELECT 1 FROM doprsblck_dbt WHERE t_blockid = 203714 AND t_statuskindid = 484131 AND t_numvalue = 4);

END;
/
