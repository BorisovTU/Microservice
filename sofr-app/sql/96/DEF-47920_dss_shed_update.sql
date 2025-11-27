begin
   UPDATE DSS_SHEDULER_DBT
      SET t_shedulertype = 0
    WHERE t_name =
             'Загрузка сделок из РСХБ-Брокер для лимитов';
end;
/