begin
  UPDATE DREGPARM_DBT
     SET t_global = CHR (88)
   WHERE t_parentid =
            (SELECT t_keyid
               FROM DREGPARM_DBT
              WHERE t_name =
                       'ВЫГРУЗКА РЕКОНСИЛ СДЕЛОК ДОФР');

  update DSS_SHEDULER_DBT
     SET T_NAME = 'Экспорт данных для реконсиляции сделок ДОФР', T_DESCRIPTION = 'Экспорт данных для реконсиляции сделок ДОФР'
   where T_NAME = 'Выгрузка данных ДОФР';

  update DSIMPLESERVICE_DBT
     SET T_NAME = 'Экспорт данных для реконсиляции сделок ДОФР', T_DESCRIPTION = 'Экспорт данных для реконсиляции сделок ДОФР'
   where T_NAME = 'Выгрузка данных ДОФР';

  update DSS_FUNC_DBT
     SET T_NAME = 'Экспорт данных для реконсиляции сделок ДОФР'
   where T_NAME = 'Выгрузка данных ДОФР';
end; 
/
