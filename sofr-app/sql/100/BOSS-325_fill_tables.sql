-- Наполнение таблиц

declare
    vcnt number;
begin
   select count(*) into vcnt from DLLVALUES_DBT where upper(T_CODE) = 'НК' and T_LIST = 3519;
   if vcnt = 0 then
      Insert into DLLVALUES_DBT (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG) Values (3519, 2030, 'НК', 'Номер купона', 2030);
      Insert into DLLCLSVAL_DBT (T_CLASSIFICATOR, T_ELEMENT, T_LIST, T_ISSYS, T_SORT) Values (1755, 2030, 3519, 'X', 0);
   end if;
end;
/