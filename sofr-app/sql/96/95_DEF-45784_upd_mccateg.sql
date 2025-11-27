BEGIN
  UPDATE DMCCATEG_DBT
    SET T_CURTYPE = 2,
        T_OPTIONAL = CHR(0)
   WHERE T_CODE IN ('ОЭБ, к исполнению','ОЭБ, к погашению','ОЭБ%, к исполнению','ОЭБ%, к погашению','ОЭБ купон, к погашению','ОЭБ купон, к исполнению');
    
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