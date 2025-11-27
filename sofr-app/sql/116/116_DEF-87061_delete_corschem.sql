--Удаление новых схем расчетов
DECLARE

BEGIN
  delete from dcorschem_dbt where t_corrid = 4 and t_number = 39 and t_fiid = 0;
  delete from dbnkschem_dbt where t_fiid = 0 and t_schem = 39 and t_bankid = 4;
END;
/