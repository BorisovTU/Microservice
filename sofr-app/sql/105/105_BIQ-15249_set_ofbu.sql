BEGIN
  UPDATE ddl_tick_dbt
     SET t_ofbu = CHR (88)
   WHERE t_dealtype = 32742;
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/