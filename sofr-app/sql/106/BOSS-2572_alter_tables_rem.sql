DECLARE
BEGIN
  execute immediate 'alter table dscsrvrep_dbt drop column t_autosend';
END;
/