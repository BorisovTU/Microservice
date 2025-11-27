-- Виды первичных документов
BEGIN
  insert into doprkdoc_dbt (t_dockind, t_primary, t_mode, t_name, t_dbfile, 
                            t_macroname, t_classmacroname, t_classname, t_parentdockind, t_maxphase, t_program, t_origin)
                    values (4661, chr(88), 0, 'Договор об общих условиях', 'dl_genagr.dbt', 
                            chr(1), chr(1), chr(1), 0, 0, chr(0), 0);
END;
/