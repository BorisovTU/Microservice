DECLARE
BEGIN
  execute immediate 'alter table dscsrvrep_dbt add t_autosendkind NUMBER (5)';
  execute immediate 'alter table dscsrvrep_dbt add t_excludeuk CHAR(1)';
END;
/