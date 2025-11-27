-- ‚αβ Άª  §­ η¥­¨© ¤«ο α¨¬Ά®«®Ά '#' ¨ 'V'
DECLARE
   v_CatID NUMBER;
BEGIN
   SELECT t_ID INTO v_CatID
   FROM DMCCATEG_DBT categ
   WHERE upper(categ.t_Code) like ' “,  ƒ€…';
   
   EXECUTE IMMEDIATE 'INSERT ALL ' ||
                     'INTO DMCTPLELM_DBT (T_CATID, T_IDELEMENT, T_SYMBOL) VALUES (' || v_CatID || ', 11, ''V'') ' ||
                     'INTO DMCTPLELM_DBT (T_CATID, T_IDELEMENT, T_SYMBOL) VALUES (' || v_CatID || ', 31, ''#'') ' ||
                     'SELECT * FROM DUAL ';
                     
   COMMIT;
EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN NULL;
END;