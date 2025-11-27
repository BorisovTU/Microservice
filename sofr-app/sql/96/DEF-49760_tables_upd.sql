--Добавление поля
DECLARE
   e_exist_field EXCEPTION;

   PRAGMA EXCEPTION_INIT( e_exist_field, -1430);

BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE DLIMIT_MONEY_REVISE_DBT ADD t_sofr_broker_comis_for_rev NUMBER (32,12)';
   EXCEPTION WHEN e_exist_field THEN NULL;
END;
/