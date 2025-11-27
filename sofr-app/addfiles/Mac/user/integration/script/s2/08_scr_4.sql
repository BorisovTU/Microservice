--задание в очередь TomCat uTableProcessOut_dbt
DECLARE

    v_FuncId            NUMBER(10);

BEGIN

  v_FuncId := 5002;

  INSERT INTO dfunc_dbt(T_FUNCID, T_CODE, T_NAME, T_TYPE, T_FILENAME, T_FUNCTIONNAME, T_INTERVAL, T_VERSION)
  VALUES(v_FuncId, 'StartLMTBuf', 'Лимиты в буферные таблицы', 1, 'ws_lmt_to_buf', 'ws_lmt_to_buf', 0, 0);

  v_FuncId := 5004;

  INSERT INTO dfunc_dbt(T_FUNCID, T_CODE, T_NAME, T_TYPE, T_FILENAME, T_FUNCTIONNAME, T_INTERVAL, T_VERSION)
  VALUES(v_FuncId, 'ChangeLMTBuf', 'Корректировки лимитов в буферные таблицы', 1, 'ws_lmt_to_buf', 'ws_lmtchange_to_buf', 0, 0);


  v_FuncId := 5005;

  INSERT INTO dfunc_dbt(T_FUNCID, T_CODE, T_NAME, T_TYPE, T_FILENAME, T_FUNCTIONNAME, T_INTERVAL, T_VERSION)
  VALUES(v_FuncId, 'SOFR_OpenAcc', 'Передача из СОФР данных по счетам бух. учета', 1, 'ws_synch_SOFR', 'ws_SOFR_OpenAccount', 0, 0);

  v_FuncId := 5006;

  INSERT INTO dfunc_dbt(T_FUNCID, T_CODE, T_NAME, T_TYPE, T_FILENAME, T_FUNCTIONNAME, T_INTERVAL, T_VERSION)
  VALUES(v_FuncId, 'SOFR_addAccountingEntries', 'Передача из СОФР данных по проводкам', 1, 'ws_synch_SOFR', 'ws_SOFR_addAccountingEntries', 0, 0);


END;
