/*Добавление в справочник*/
BEGIN
   INSERT INTO DFUNC_DBT (T_FUNCID, T_CODE, T_NAME, T_TYPE, T_FILENAME, T_FUNCTIONNAME, T_INTERVAL, T_VERSION, T_MODULE)
   VALUES(5800, 'ExecOprCalcExch', 'Обработка расчетов с биржей по клиентам', 1, 'execopr_funcobj.mac', 'ExecOprCalcExch', 0, 0, NULL);
EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN 
   	it_log.log('DEF-78031. В таблице DFUNC_DBT уже существует запись с t_FuncID = 5800');
END;
/
