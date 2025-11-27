declare

begin
  delete from dfunc_dbt where t_funcid = 8209;
  -- создание типа задания для обработки результата обработки неторгового поручения в QUIK
  INSERT INTO dfunc_dbt (T_FUNCID,T_CODE,T_NAME,T_TYPE,T_FILENAME,T_FUNCTIONNAME,T_INTERVAL,T_VERSION) 
  VALUES (8209,'ResQuikEnroll','Результат обработки неторгового поручения в QUIK',1,'quik_writequikresultenroll_funcobj.mac',
    'writeQuikResultToEnroll',0,0);
  -- внесение в справочник Общий для 5002 справочника
  INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE) 
  VALUES (5002,8209,8209,'Результат обработки неторгового поручения в QUIK',
    8209,'Результат обработки неторгового поручения в QUIK','');
  commit;
exception when others then
  it_log.log('Ошибка при создании типа задания для обработки результата обработки неторгового поручения в QUIK');
end;
