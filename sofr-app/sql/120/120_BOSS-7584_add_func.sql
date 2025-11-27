/*Добавление в справочник*/
BEGIN
   INSERT INTO DFUNC_DBT (T_FUNCID, T_CODE, T_NAME, T_TYPE, T_FILENAME, T_FUNCTIONNAME, T_INTERVAL, T_MODULE, T_VERSION)
   VALUES(5301,'CreateNptxShort2025Data','Выполнить подготовку данных для отчета Краткая справка НДФЛ',1,'nptxshort_2025_funcobj.mac','CreateNptxShort2025Data_Funcobj',0,NULL,0);

EXCEPTION 
   WHEN DUP_VAL_ON_INDEX THEN NULL;
END;
/
