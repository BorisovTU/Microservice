declare
  logID VARCHAR2(9) := 'BIQ-13034';

begin
  -- создания типа задания для создания операции списания ЦБ
  INSERT INTO dfunc_dbt (T_FUNCID,T_CODE,T_NAME,T_TYPE,T_FILENAME,T_FUNCTIONNAME,T_INTERVAL,T_VERSION) 
  VALUES (8208,'SendPKOInfo','Создание списания ЦБ по поручению Диасофт',1,'diasoft_createPKO_funcobj.mac',
    'createEnroll',0,0);
  -- внесение в справочник Общий для 5002 справочника
  INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE) 
  VALUES (5002,8208,8208,'Создание операции списания ЦБ по запросу ДИАСОФТ',
    8208,'Создание операции списания ЦБ по запросу ДИАСОФТ','');
  commit;
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
end;
