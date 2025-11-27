-- Вставка значений для символов '#' и 'V'
BEGIN
   EXECUTE IMMEDIATE 'INSERT ALL ' ||
                     'INTO DMCTPLELM_DBT (T_CATID, T_IDELEMENT, T_SYMBOL) VALUES (866, 11, ''V'') ' ||
                     'INTO DMCTPLELM_DBT (T_CATID, T_IDELEMENT, T_SYMBOL) VALUES (866, 31, ''#'') ' ||
                     'SELECT * FROM DUAL ';
EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN NULL;
END;