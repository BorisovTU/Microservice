--Обновление справочника финансовых инструментов
BEGIN
  --Обновляем фьючерсы с привязкой к ошибочным базовым активам
  update dfininstr_dbt fin 
     set fin.t_facevaluefi = (select fin_cur.t_fiid 
                                from dfininstr_dbt fin_cur 
                               where fin_cur.t_ccy = (select fin_base.t_fi_code 
                                                        from dfininstr_dbt fin_base 
                                                       where fin_base.t_fiid = fin.t_facevaluefi 
                                                         and fin_base.t_fi_code IN('BYN', 'KZT', 'AED', 'HKD', 'INR', 'TRY')))  
  where fin.t_facevaluefi IN (select fin_base_global.t_fiid 
                                from dfininstr_dbt fin_base_global 
                               where fin_base_global.t_fi_code IN('BYN', 'KZT', 'AED', 'HKD', 'INR', 'TRY'));

  --Привязываем коды с ошибочно созданных индексов к активам-валютам
  update dobjcode_dbt code 
     set code.t_objectid = (select currency.t_fiid 
                              from dfininstr_dbt currency 
                             where currency.t_ccy = (select fi_index.t_fi_code 
                                                       from dfininstr_dbt fi_index 
                                                      where fi_index.t_fiid = code.t_objectid)) 
   where code.t_objecttype = 9 
     and code.t_objectid in (select t_fiid 
                               from dfininstr_dbt 
                              where t_fi_code IN('BYN', 'KZT', 'AED', 'HKD', 'INR', 'TRY'));

  --удаляем ошибочные активы-индексы
  delete from dfininstr_dbt fin where fin.t_fi_code IN ('BYN', 'KZT', 'AED', 'HKD', 'INR', 'TRY'); 
END;
/

