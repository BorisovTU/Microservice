/*Удаление новых колонок*/
BEGIN 
  execute immediate 'ALTER TABLE ddvdeal_dbt DROP COLUMN t_ExecType'; 
  execute immediate 'ALTER TABLE ddvdeal_dbt DROP COLUMN t_ExecOneDay';  
END;
/
