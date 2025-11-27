BEGIN
   BEGIN
      INSERT INTO DLLVALUES_DBT (
                                   t_List,
                                   t_Element,
                                   t_Code,
                                   t_Name,
                                   t_Flag,
                                   t_Note,
                                   t_Reserve
                                )
                         VALUES (
                                   5002,
                                   6030,
                                   '6030',
                                   'Выполнение процедуры запроса к хранилищу СНОБ',
                                   6030,
                                   CHR(1),
                                   NULL
                                );
   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN it_log.log('BOSS-1819. Значение вида справочника 5002 с кодом 6030 уже существует');
   END;
   
   COMMIT;
   
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
                            6030,
                            'RequstSNOBStor',
                            'Выполнение процедуры запроса к хранилищу СНОБ',
                            1,
                            'RequestSnobStor_funcobj.mac',
                            'Exec_RequestSnobStor',
                            0,
                            0,
                            NULL
                         );
EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN it_log.log('BOSS-1819. В таблице DFUNC_DBT уже существует запись с t_FuncID = 6030');
END;
/

BEGIN
   BEGIN
      INSERT INTO DLLVALUES_DBT (
                                   t_List,
                                   t_Element,
                                   t_Code,
                                   t_Name,
                                   t_Flag,
                                   t_Note,
                                   t_Reserve
                                )
                         VALUES (
                                   5002,
                                   6031,
                                   '6031',
                                   'Контроль выполнения запроса к хранилищу СНОБ',
                                   6031,
                                   CHR(1),
                                   NULL
                                );
   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN it_log.log('BOSS-1819. Значение вида справочника 5002 с кодом 6031 уже существует');
   END;
   
   COMMIT;
   
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
                            6031,
                            'CheckRequstSNOBStor',
                            'Контроль выполнения запроса к хранилищу СНОБ',
                            1,
                            'ChekRequestSnobStor_funcobj.mac',
                            'Exec_CheckRequestSnobStor',
                            0,
                            0,
                            NULL
                         );
EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN it_log.log('BOSS-1819. В таблице DFUNC_DBT уже существует запись с t_FuncID = 6031');
END;
/