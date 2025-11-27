/* Linux. Регресс. Замедление более чем в 5 раз Биржевого автомата ММВБ. Загрузка в БО сделок/заявок/клиринга. высокая Загрузка CPU */

BEGIN
   -- T_FUNCID  T_CODE  T_NAME  T_TYPE  T_FILENAME  T_FUNCTIONNAME  T_INTERVAL  T_MODULE  T_VERSION
   -- 110 CNVCOM  Запуск функций конвейера  1 cnv_funcobj.mac cnvStartFunc  0   0

   execute immediate 'ALTER TRIGGER DFUNC_SYS_TRG DISABLE';
  
   INSERT INTO DFUNC_DBT (
                            t_FuncID,
                            t_Code,
                            t_Name,
                            t_Type,
                            t_FileName,
                            t_FunctionName,
                            t_Interval,
                            t_Version,
                            t_Module
                         )
                  VALUES (
                            110,
                            'CNVCOM',
                            'Запуск функций конвейера',
                            1,
                            'cnv_funcobj.mac',
                            'cnvStartFunc',
                            0,
                            0,
                            NULL
                         );
   execute immediate 'ALTER TRIGGER DFUNC_SYS_TRG ENABLE';
EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN it_log.log('DEF-78031. В таблице DFUNC_DBT уже существует запись с t_FuncID = 110');
END;
/

