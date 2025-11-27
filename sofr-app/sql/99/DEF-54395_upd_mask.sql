-- Наполнение таблиц


declare
   vcnt number;
begin
   select count(*) into vcnt from DMCSTPELM_DBT where T_IDELEMENT = 1008;
   if vcnt = 0 then
      Insert into DMCSTPELM_DBT (T_IDELEMENT, T_NAME, T_DEFAULTSYMBOL) Values (1008, 'Торговая площадка по ДО', '&');
      delete from DMCTPLELM_DBT where T_CATID = (select T_ID from DMCCATEG_DBT WHERE T_CODE = 'ЦБ Клиента, ВУ') and T_SYMBOL = 'М';
      Insert into DMCTPLELM_DBT (T_CATID, T_IDELEMENT, T_SYMBOL) Values 
                  ((select T_ID from DMCCATEG_DBT WHERE T_CODE = 'ЦБ Клиента, ВУ'), 1008, '&');
      Insert into DMCTPLELM_DBT (T_CATID, T_IDELEMENT, T_SYMBOL) Values 
                  ((select T_ID from DMCCATEG_DBT WHERE T_CODE = 'ЦБ, Расч. с клиентом, ВУ'), 1008, '&');
   end if;
end;
/

BEGIN
  UPDATE DMCCATEG_DBT SET T_MASK = REPLACE(T_MASK, 'ММММ','&&&&') WHERE T_CODE = 'ЦБ Клиента, ВУ';
  UPDATE DMCCATEG_DBT SET T_MASK = REPLACE(T_MASK, '0000','&&&&') WHERE T_CODE = 'ЦБ, Расч. с клиентом, ВУ';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/