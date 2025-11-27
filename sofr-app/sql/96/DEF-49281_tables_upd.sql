--обновление таблиц
declare
    vcnt number;
begin
   select count(*) into vcnt from user_tables where upper(table_name) = 'DLIMIT_MONEY_REVISE_DBT';
   if vcnt = 1 then
      execute immediate  
       'ALTER TABLE DLIMIT_MONEY_REVISE_DBT
          ADD (T_SOFR_FIXCOMIS  NUMBER(32,12))';

   end if;

   select count(*) into vcnt from user_tables where upper(table_name) = 'DLIMIT_SECUR_REVISE_DBT';
   if vcnt = 1 then
      execute immediate  
       'ALTER TABLE DLIMIT_SECUR_REVISE_DBT
          ADD (T_SOFR_AVRWRT  NUMBER(32,12))';

   end if;
end;
/