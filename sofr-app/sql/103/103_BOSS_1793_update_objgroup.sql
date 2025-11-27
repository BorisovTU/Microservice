-- Обновление записи в dObjGroup_dbt
DECLARE
BEGIN
  UPDATE dObjGroup_dbt
     SET t_macroName = 'catEditDisable_check.mac'
   WHERE t_objectType = 207
     AND t_groupID = 126;
END;
/