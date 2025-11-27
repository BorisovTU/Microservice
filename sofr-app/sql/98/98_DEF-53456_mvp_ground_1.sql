declare
  cnt integer;
begin
  select 
    count(*) into cnt
    from dmultymtd_dbt
   where T_METHODID = 100
     and t_name = 'Доходы и расходы по депо комиссиям';
  if(cnt > 0) then  
    delete dmultymtd_dbt
      where T_METHODID = 100
        and t_name = 'Доходы и расходы по депо комиссиям';
    delete dmultymtdcat_dbt where T_METHODID = 100 and T_CHAPTER = 1
      and T_CATPLUS = 1001 and T_CATMINUS = 1002;
  end if;
  INSERT INTO dmultymtd_dbt
    (T_METHODID,
     T_MACRONAME,
     T_CLASSNAME,
     T_NAME,
     T_SHORT_NAME,
     T_TYPEUSERMETHOD,
     T_PARENTMETHODID,
     T_GROUNDPOSITIVE,
     T_GROUNDNEGATIVE)
  VALUES
    (100,
     chr(1),
     chr(1),
     'Доходы и расходы по депо комиссиям',
     'Депо.комисс',
     0,
     0,
     'Реализованная курсовая разница к сумме {ValSum} {ValFiCode}. Исходный документ Мем.ор. N ''{Numb}''  от {Date}, валюта {ValFiCode}, курс операции {Rate}, текущий курс {Rate_CB}.',
     'Реализованная курсовая разница к сумме {ValSum} {ValFiCode}. Исходный документ Мем.ор. N ''{Numb}''  от {Date}, валюта {ValFiCode}, курс операции {Rate}, текущий курс {Rate_CB}.');
   INSERT INTO dmultymtdcat_dbt (T_METHODID,T_CHAPTER,T_CATPLUS,T_CATMINUS) VALUES (100,1,1001,1002);
  commit;
exception when others then
  it_log.log('Ошибка при добавлении основания МВП');
  dbms_output.put_line('Ошибка при добавлении основания МВП');
end;
