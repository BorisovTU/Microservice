declare
  logID VARCHAR2(9) := 'BIQ-13034';

begin

  -- создания типа задания для создания операции списания ЦБ
  INSERT INTO dfunc_dbt (T_FUNCID,T_CODE,T_NAME,T_TYPE,T_FILENAME,T_FUNCTIONNAME,T_INTERVAL,T_VERSION) 
  VALUES (8210,'diasoft_Pko_CancelExpiredOrders','Отказ операции списания ЦБ по поручению Диасофт',1,
    'diasoft_Pko_CancelExpiredOrders.mac',
    'main',0,0);
  -- внесение в справочник Общий для 5002 справочника
  INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE) 
  VALUES (5002,8210,8210,'Отказ операции списания ЦБ по поручению Диасофт',
    8210,'Отказ операции списания ЦБ по поручению Диасофт','');

  -- создания типа задания для создания операции списания ЦБ
  INSERT INTO dfunc_dbt (T_FUNCID,T_CODE,T_NAME,T_TYPE,T_FILENAME,T_FUNCTIONNAME,T_INTERVAL,T_VERSION) 
  VALUES (8211,'diasoft_Pko_blockSecurities_Open','Открытие операции списания ЦБ по поручению Диасофт',1,
    'diasoft_Pko_blockSecurities_Open.mac',
    'main',0,0);
  -- внесение в справочник Общий для 5002 справочника
  INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE) 
  VALUES (5002,8211,8211,'Открытие операции списания ЦБ по поручению Диасофт',
    8211,'Открытие операции списания ЦБ по поручению Диасофт','');

  -- создания типа задания для создания операции списания ЦБ
  INSERT INTO dfunc_dbt (T_FUNCID,T_CODE,T_NAME,T_TYPE,T_FILENAME,T_FUNCTIONNAME,T_INTERVAL,T_VERSION) 
  VALUES (8212,'diasoft_Pko_NoSecurities','Сообщение в Диасофт о недостаточном количестве ц/б',1,
    'diasoft_Pko_NoSecurities.mac',
    'main',0,0);
  -- внесение в справочник Общий для 5002 справочника
  INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE) 
  VALUES (5002,8212,8212,'Сообщение в Диасофт о недостаточном количестве ц/б',
    8212,'Сообщение в Диасофт о недостаточном количестве ц/б','');

  -- создания типа задания для создания операции списания ЦБ
  INSERT INTO dfunc_dbt (T_FUNCID,T_CODE,T_NAME,T_TYPE,T_FILENAME,T_FUNCTIONNAME,T_INTERVAL,T_VERSION) 
  VALUES (8213,'Diasoft_FinalStatus_Close','Подготовка операции списания ЦБ по поручению Диасофт',1,
    'Diasoft_FinalStatus_Close.mac',
    'main',0,0);
  -- внесение в справочник Общий для 5002 справочника
  INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE) 
  VALUES (5002,8213,8213,'Подготовка операции списания ЦБ по поручению Диасофт',
    8213,'Подготовка операции списания ЦБ по поручению Диасофт','');
  
  commit;
  
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
end;
