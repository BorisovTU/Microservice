-- обновление для TRD_RESTR
BEGIN
  UPDATE dobjgroup_dbt
     SET t_name = 'TRD_RESTR'
   WHERE t_objecttype = 207
     AND t_groupid = 125 
     AND t_name = 'TRD_REST';
     
     COMMIT;
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/