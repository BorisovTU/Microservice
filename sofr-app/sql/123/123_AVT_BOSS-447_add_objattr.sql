/*Добавление в справочник*/
BEGIN
insert into dobjattr_dbt
  (t_objecttype, t_groupid, t_attrid, t_parentid, t_codelist, t_numinlist, t_nameobject, t_chattr, t_longattr, t_intattr, t_name, t_fullname, t_opendate, t_closedate, t_classificator, t_corractype, t_balance, t_isobject)
values
  (3, 101, 1, 0, chr(1), 1, 1, chr(0), 0, 0, 'Да', 'Да', to_date('01.01.0001','DD.MM.YYYY'), to_date('01.01.0001','DD.MM.YYYY'), 0, chr(1), chr(1), chr(0));
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/


