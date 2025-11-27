/*Добавление в справочник*/
BEGIN
  insert into dobjgroup_dbt
  (t_objecttype, t_groupid, t_type, t_name, t_system, t_order, t_macroname, t_keepoldvalues, t_updateflag, t_successopflag, t_attrobjecttype, t_attrgroupid, t_ishidden, t_fullnameisbasic, t_syshidden, t_notusefielduse, t_ismeanparentnode, t_ismanualfirst, t_comment)
values
  (3, 101, chr(88), 'Является экономически значимой организацией', chr(0), 101, chr(1), chr(88), 0, chr(0), 0, 0, chr(0), chr(0), chr(0), chr(0), chr(0), chr(0), chr(1));

EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/


