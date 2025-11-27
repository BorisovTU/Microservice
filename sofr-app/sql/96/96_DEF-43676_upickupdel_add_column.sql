/*DEF-43676 добавление столбца с информацией о пользователе*/
DECLARE
   e_exist_field EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_exist_field, -1430);

BEGIN
   EXECUTE IMMEDIATE 'alter table upickupdel_dbt add t_deletesource varchar2(500 byte) default chr(1)';
   EXCEPTION WHEN e_exist_field THEN NULL;
END;
