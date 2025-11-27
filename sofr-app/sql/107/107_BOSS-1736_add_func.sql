/*Добавление в справочник*/
BEGIN
   INSERT INTO DFUNC_DBT (T_FUNCID, T_CODE, T_NAME, T_TYPE, T_FILENAME, T_FUNCTIONNAME, T_INTERVAL, T_MODULE, T_VERSION)
   VALUES(5300,'ExecNKDReqDias','Выполнить обработку запроса УНКД из Диасофта',1,'nptxnkdreqdias_funcobj.mac','ExecNKDReqDias_Funcobj',0,NULL,0);

EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
