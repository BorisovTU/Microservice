-- добавление поля в таблицу dDlContrMp_tmp
DECLARE
  v_cnt NUMBER;
BEGIN
  SELECT COUNT(1) INTO v_cnt
    FROM user_tab_columns
   WHERE UPPER(table_name) = 'DDLCONTRMP_TMP' 
     AND UPPER(column_name) = 'T_RCODETKS';

  IF v_cnt = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE dDlContrMp_tmp ADD t_RCodeTKS VARCHAR2(64)';
  END IF;
END;
/