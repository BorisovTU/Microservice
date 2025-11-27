begin
   update dmenuitem_dbt set t_sznameitem = ' Начальная выгрузка в QUIK'
   where t_objectid = 1 and t_iidentprogram = 131 and t_inumberpoint = 1114 and t_istemplate = chr(0);

   update dmenuitem_dbt set t_sznameitem = ' Начальная выгрузка в QUIK'
   where t_objectid = 1010 and t_iidentprogram = 131 and t_inumberpoint = 1114 and t_istemplate = chr(88);
   commit;
end;