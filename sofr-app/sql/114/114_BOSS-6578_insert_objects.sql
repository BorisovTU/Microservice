-- Виды объектов
BEGIN
  insert into dobjects_dbt (t_objecttype, t_name, t_code, t_usernumber, t_parentobjecttype, t_servicemacro, t_module)
                    values (475, 'Договоры об общих условиях', 'ДОУC', 0, 0, chr(1), chr(0));
END;
/