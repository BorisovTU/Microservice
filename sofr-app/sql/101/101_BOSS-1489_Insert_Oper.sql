DECLARE
   v_DocKindName VARCHAR2(4000);
   v_KindOperName VARCHAR2(4000);
BEGIN
   BEGIN
      INSERT INTO DOPRKDOC_DBT (
                                  t_DocKind,
                                  t_Primary,
                                  t_Mode,
                                  t_Name,
                                  t_DBFile,
                                  t_MacroName,
                                  t_ClassMacroName,
                                  t_ClassName,
                                  t_ParentDocKind,
                                  t_MaxPhase,
                                  t_Program,
                                  t_Origin
                               )
                        VALUES (
                                  4650,
                                  'X',
                                  0,
                                  'Техническая сверка СНОБ',
                                  'nptxsnobver.dbt',
                                  CHR(1),
                                  CHR(1),
                                  CHR(1),
                                  0,
                                  0,
                                  CHR(2),
                                  0
                               );
      v_DocKindName := 'Техническая сверка СНОБ';
   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN
         SELECT t_Name INTO v_DocKindName
         FROM DOPRKDOC_DBT
         WHERE t_DocKind = 4650;
         
         IT_LOG.LOG('BOSS-1489. Уже существует запись с t_DocKind = 4650, t_Name = ''' || v_DocKindName || ''' в таблице DOPRKDOC_DBT');
   END;
   
   IF v_DocKindName = 'Техническая сверка СНОБ'
   THEN
      BEGIN
         INSERT INTO DOPRKOPER_DBT (
                                      t_Kind_Operation,
                                      t_DocKind,
                                      t_NotInUse,
                                      t_InitMacro,
                                      t_SysTypes,
                                      t_Name,
                                      t_ShortName,
                                      t_Upgrade,
                                      T_ISMAINTANANCE,
                                      t_Parent,
                                      t_Version,
                                      T_MASSOPRSTART
                                   )
                            VALUES (
                                      2047,
                                      4650,
                                      CHR(2),
                                      CHR(1),
                                      CHR(1),
                                      'Техническая сверка СНОБ',
                                      'ТЕХСВЕРКА_СНОБ',
                                      CHR(2),
                                      CHR(2),
                                      0,
                                      CHR(1),
                                      CHR(2)
                                   );
         v_KindOperName := 'Техническая сверка СНОБ';
      EXCEPTION
         WHEN DUP_VAL_ON_INDEX
         THEN
            SELECT t_Name INTO v_KindOperName
            FROM DOPRKOPER_DBT
            WHERE t_Kind_Operation = 2047;

            IT_LOG.LOG('BOSS-1489. Уже существует запись с t_Kind_Operation = 2047, t_Name = ''' || v_KindOperName || ''' в таблице DOPRKOPER_DBT');
      END;
      
      IF v_KindOperName = 'Техническая сверка СНОБ'
      THEN
         BEGIN
            INSERT ALL
               INTO DOPRBLOCK_DBT (t_BlockID, t_Name,          t_DocKind, t_Parent, t_Upgrade, t_Version, t_VersionWEB)
                           VALUES (204701,    'Инициализация', 4650,      0,        CHR(2),    CHR(1),    1)
               INTO DOPRBLOCK_DBT (t_BlockID, t_Name,                          t_DocKind, t_Parent, t_Upgrade, t_Version, t_VersionWEB)
                           VALUES (204702,    'Формирование и отправка файла', 4650,      0,        CHR(2),    CHR(1),    1)
               INTO DOPRBLOCK_DBT (t_BlockID, t_Name,      t_DocKind, t_Parent, t_Upgrade, t_Version, t_VersionWEB)
                           VALUES (204703,    'Закрытие',  4650,      0,        CHR(2),    CHR(1),    0)
            SELECT * FROM DUAL;
         EXCEPTION
            WHEN DUP_VAL_ON_INDEX
            THEN IT_LOG.LOG('BOSS-1489. Уже существуют записи с t_DocKind = 4650 в таблице DOPRBLOCK_DBT');
         END;
         
         INSERT ALL
            INTO DOPROSTEP_DBT (t_BlockID, t_Number_Step, t_Kind_Action, t_DayOffset, t_Scale, t_DayFlag, t_CalendarID, t_Symbol, T_PREVIOUS_STEP, T_MODIFICATION, T_CARRY_MACRO,        T_PRINT_MACRO, T_POST_MACRO, T_NOTINUSE, T_FIRSTSTEP, T_NAME,         T_DATEKINDID, T_REV,  T_AUTOEXECUTESTEP, T_ONLYHANDCARRY, T_ISALLOWFOROPER, T_OPERORGROUP, T_RESTRICTEARLYEXECUTION, T_USERTYPES, T_INITDATEKINDID, T_ASKFORDATE, T_BACKOUT, T_ISBACKOUTGROUP, T_MASSEXECUTEMODE, T_ISCASE, T_ISDISTAFFEXECUTE, T_SKIPINITAFTERPLANDATE, T_MASSPACKSIZE)
                        VALUES (204701,    5,             1,             0,           0,       CHR(2),    0,            'И',      0,               0,              'nptxsnobver005.mac', CHR(1),        CHR(1),       CHR(2),     NULL,       'Инициализация',465000000,    CHR(2), 'X',               CHR(2),          0,                CHR(2),        'X',                      CHR(1),      0,                CHR(2),       0,         CHR(2),           0,                 CHR(2),   CHR(2),             'X',                     0)
            INTO DOPROSTEP_DBT (t_BlockID, t_Number_Step, t_Kind_Action, t_DayOffset, t_Scale, t_DayFlag, t_CalendarID, t_Symbol, T_PREVIOUS_STEP, T_MODIFICATION, T_CARRY_MACRO,        T_PRINT_MACRO, T_POST_MACRO, T_NOTINUSE, T_FIRSTSTEP, T_NAME,                        T_DATEKINDID, T_REV,  T_AUTOEXECUTESTEP, T_ONLYHANDCARRY, T_ISALLOWFOROPER, T_OPERORGROUP, T_RESTRICTEARLYEXECUTION, T_USERTYPES, T_INITDATEKINDID, T_ASKFORDATE, T_BACKOUT, T_ISBACKOUTGROUP, T_MASSEXECUTEMODE, T_ISCASE, T_ISDISTAFFEXECUTE, T_SKIPINITAFTERPLANDATE, T_MASSPACKSIZE)
                        VALUES (204702,    10,            1,             0,           0,       CHR(2),    0,            'О',      0,               0,              'nptxsnobver010.mac', CHR(1),        CHR(1),       CHR(2),     NULL,       'Отбор записей хранилища СОФР',465000000,    CHR(2), 'X',               CHR(2),          0,                CHR(2),        'X',                      CHR(1),      0,                CHR(2),       0,         CHR(2),           0,                 CHR(2),   CHR(2),             'X',                     0)
            INTO DOPROSTEP_DBT (t_BlockID, t_Number_Step, t_Kind_Action, t_DayOffset, t_Scale, t_DayFlag, t_CalendarID, t_Symbol, T_PREVIOUS_STEP, T_MODIFICATION, T_CARRY_MACRO,        T_PRINT_MACRO, T_POST_MACRO, T_NOTINUSE, T_FIRSTSTEP, T_NAME,                         T_DATEKINDID, T_REV,  T_AUTOEXECUTESTEP, T_ONLYHANDCARRY, T_ISALLOWFOROPER, T_OPERORGROUP, T_RESTRICTEARLYEXECUTION, T_USERTYPES, T_INITDATEKINDID, T_ASKFORDATE, T_BACKOUT, T_ISBACKOUTGROUP, T_MASSEXECUTEMODE, T_ISCASE, T_ISDISTAFFEXECUTE, T_SKIPINITAFTERPLANDATE, T_MASSPACKSIZE)
                        VALUES (204702,    15,            1,             0,           0,       CHR(2),    0,            'Ф',      0,               0,              'nptxsnobver015.mac', CHR(1),        CHR(1),       CHR(2),     NULL,       'Формирование и отправка файла',465000000,    CHR(2), 'X',               CHR(2),          0,                CHR(2),        'X',                      CHR(1),      0,                CHR(2),       0,         CHR(2),           0,                 CHR(2),   CHR(2),             'X',                     0)
            INTO DOPROSTEP_DBT (t_BlockID, t_Number_Step, t_Kind_Action, t_DayOffset, t_Scale, t_DayFlag, t_CalendarID, t_Symbol, T_PREVIOUS_STEP, T_MODIFICATION, T_CARRY_MACRO,        T_PRINT_MACRO, T_POST_MACRO, T_NOTINUSE, T_FIRSTSTEP, T_NAME,                              T_DATEKINDID, T_REV,  T_AUTOEXECUTESTEP, T_ONLYHANDCARRY, T_ISALLOWFOROPER, T_OPERORGROUP, T_RESTRICTEARLYEXECUTION, T_USERTYPES, T_INITDATEKINDID, T_ASKFORDATE, T_BACKOUT, T_ISBACKOUTGROUP, T_MASSEXECUTEMODE, T_ISCASE, T_ISDISTAFFEXECUTE, T_SKIPINITAFTERPLANDATE, T_MASSPACKSIZE)
                        VALUES (204702,    20,            1,             0,           0,       CHR(2),    0,            'П',      0,               0,              'nptxsnobver020.mac', CHR(1),        CHR(1),       CHR(2),     NULL,       'Получение ответа от хранилища СНОБ',465000000,    CHR(2), 'X',               CHR(2),          0,                CHR(2),        'X',                      CHR(1),      0,                CHR(2),       0,         CHR(2),           0,                 CHR(2),   CHR(2),             'X',                     0)
           INTO DOPROSTEP_DBT (t_BlockID, t_Number_Step, t_Kind_Action, t_DayOffset, t_Scale, t_DayFlag, t_CalendarID, t_Symbol, T_PREVIOUS_STEP, T_MODIFICATION, T_CARRY_MACRO,        T_PRINT_MACRO, T_POST_MACRO, T_NOTINUSE,  T_FIRSTSTEP, T_NAME,         T_DATEKINDID, T_REV,  T_AUTOEXECUTESTEP, T_ONLYHANDCARRY, T_ISALLOWFOROPER, T_OPERORGROUP, T_RESTRICTEARLYEXECUTION, T_USERTYPES, T_INITDATEKINDID, T_ASKFORDATE, T_BACKOUT, T_ISBACKOUTGROUP, T_MASSEXECUTEMODE, T_ISCASE, T_ISDISTAFFEXECUTE, T_SKIPINITAFTERPLANDATE, T_MASSPACKSIZE)
                        VALUES (204703,    25,             1,             0,           0,       CHR(2),    0,            'И',      0,               0,              'nptxsnobver025.mac', CHR(1),        CHR(1),       CHR(2),    NULL,       'Закрытие',465000000,    CHR(2), 'X',               CHR(2),          0,                CHR(2),        'X',                      CHR(1),      0,                CHR(2),       0,         CHR(2),           0,                 CHR(2),   CHR(2),             'X',                     0)
         SELECT * FROM DUAL;
         
         INSERT INTO DOPRSTKND_DBT (
                                      t_StatusKindID,
                                      t_Code,
                                      t_Name,
                                      t_DocKind,
                                      t_Sort,
                                      T_ELIMINATED
                                   )
                            VALUES (
                                      46501,
                                      'ДО',
                                      'Документооборот',
                                      4650,
                                      0,
                                      CHR(2)
                                   );

         INSERT ALL
            INTO DOPRSTVAL_DBT (t_StatusKindID, t_NumValue, t_Name,   T_ELIMINATED)
                        VALUES (46501,          1,          'Открыт', CHR(2))
            INTO DOPRSTVAL_DBT (t_StatusKindID, t_NumValue, t_Name,         T_ELIMINATED)
                        VALUES (46501,          2,          'Отбор записей',CHR(2))
            INTO DOPRSTVAL_DBT (t_StatusKindID, t_NumValue, t_Name,         T_ELIMINATED)
                        VALUES (46501,          3,          'Закрытие',     CHR(2))
            INTO DOPRSTVAL_DBT (t_StatusKindID, t_NumValue, t_Name,         T_ELIMINATED)
                        VALUES (46501,          4,          'Закрыт',       CHR(2))
         SELECT * FROM DUAl;
         
         INSERT ALL
            INTO DOPROBLCK_DBT (t_OperBlockID, t_Kind_Operation, t_BlockID, t_Sort, t_NotInUse, t_NoInsert, t_NoReplace, T_NOCLOSEINSERT, t_IsManual, t_SymbolsForInsertion, t_Symbol)
                        VALUES (3426,          2047,             204701,    1,      CHR(2),     CHR(2),     CHR(2),      'X',              CHR(2),     CHR(1),                'И')
            INTO DOPROBLCK_DBT (t_OperBlockID, t_Kind_Operation, t_BlockID, t_Sort, t_NotInUse, t_NoInsert, t_NoReplace, T_NOCLOSEINSERT, t_IsManual, t_SymbolsForInsertion, t_Symbol)
                        VALUES (3427,          2047,             204702,    2,      CHR(2),     CHR(2),     CHR(2),      'X',              CHR(2),     CHR(1),                CHR(2))
            INTO DOPROBLCK_DBT (t_OperBlockID, t_Kind_Operation, t_BlockID, t_Sort, t_NotInUse, t_NoInsert, t_NoReplace, T_NOCLOSEINSERT, t_IsManual, t_SymbolsForInsertion, t_Symbol)
                        VALUES (3428,          2047,             204703,    3,      CHR(2),     CHR(2),     CHR(2),      'X',              CHR(2),     CHR(1),                CHR(2))
         SELECT * FROM DUAL;
         
         INSERT INTO DOPRKSTEP_DBT (
                                      t_DocKind,
                                      t_Kind_Action,
                                      t_Name,
                                      T_ISALLOWFOROPER,
                                      T_OPERORGROUP,
                                      t_Reserve
                                   )
                            VALUES (
                                      4650,
                                      1,
                                      'Пользовательский шаг',
                                      0,
                                      CHR(2),
                                      NULL
                                   );
         
         INSERT INTO DOPRKDATE_DBT (
                                      t_DateKindID,
                                      t_DocKind,
                                      t_NumberDate,
                                      t_NameDate,
                                      T_ELIMINATED
                                   )
                            VALUES (
                                      465000000,
                                      4650,
                                      0,
                                      'Дата операции',
                                      CHR(2)
                                   );

         INSERT ALL
            INTO DOPRSBLCK_DBT (t_BlockID, t_StatusKindID, t_NumValue, t_Default)
                        VALUES (204701,    46501,          1,          CHR(2))
            INTO DOPRSBLCK_DBT (t_BlockID, t_StatusKindID, t_NumValue, t_Default)
                        VALUES (204701,    46501,          2,          CHR(2))
            INTO DOPRSBLCK_DBT (t_BlockID, t_StatusKindID, t_NumValue, t_Default)
                        VALUES (204701,    46501,          3,          CHR(2))
            INTO DOPRSBLCK_DBT (t_BlockID, t_StatusKindID, t_NumValue, t_Default)
                        VALUES (204702,    46501,          3,          CHR(2))
            INTO DOPRSBLCK_DBT (t_BlockID, t_StatusKindID, t_NumValue, t_Default)
                        VALUES (204703,    46501,          4,          CHR(2))
         SELECT * FROM DUAL;
         
         INSERT INTO DOPRINIST_DBT (
                                      t_Kind_Operation,
                                      t_StatusKindID,
                                      t_NumValue
                                   )
                            VALUES (
                                      2047,
                                      46501,
                                      1
                                   );
                                   
         INSERT ALL
            INTO DOPRCBLCK_DBT (t_OperBlockID, t_StatusKindID, t_NumValue, t_Condition)
                        VALUES (3426,          46501,          1,          0)
            INTO DOPRCBLCK_DBT (t_OperBlockID, t_StatusKindID, t_NumValue, t_Condition)
                        VALUES (3427,          46501,          2,          0)
            INTO DOPRCBLCK_DBT (t_OperBlockID, t_StatusKindID, t_NumValue, t_Condition)
                        VALUES (3428,          46501,          3,          0)
         SELECT * FROM DUAL;
      END IF;
   END IF;
END;
/