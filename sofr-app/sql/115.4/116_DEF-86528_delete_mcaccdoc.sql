--Удаление счета
DECLARE

BEGIN
  delete from dmcaccdoc_dbt where t_account='47407810999003020066' and t_iscommon=chr(88) and t_templnum=1;
END;
/