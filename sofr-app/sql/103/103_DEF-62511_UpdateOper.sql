begin
   begin
      update doprkdoc_dbt
      set t_Name = 'Техническая сверка СНОБ',
          t_MacroName = chr(0),
          t_ClassMacroName = chr(0),
          t_ClassName = chr(0),
          t_Program = chr(0)
      where t_DocKind = 4650;
      
   it_log.log('DEF-62511. Таблица doprkdoc_dbt успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_DocKind = 4650 в таблице doprkdoc_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprkoper_dbt
      set t_NotInUse = chr(0),
          t_InitMacro = chr(0),
          t_SysTypes = chr(0),
          t_Name = 'Техническая сверка СНОБ',
          t_ShortName = 'ТЕХСВЕРКА_СНОБ',
          t_Upgrade = chr(0),
          t_IsMainTanance = chr(0),
          t_Version = chr(0),
          t_MassOprStart = chr(0)
      where t_DocKind = 4650;
      
   it_log.log('DEF-62511. Таблица doprkoper_dbt успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_DocKind = 4650 в таблице doprkoper_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprblock_dbt
      set t_Name = 'Инициализация',
          t_Upgrade = chr(0),
          t_Version = chr(0)
      where t_BlockID = 204701;
      
   it_log.log('DEF-62511. Таблица doprblock_dbt (204701) успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_BlockID = 204701 в таблице doprblock_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprblock_dbt
      set t_Name = 'Формирование и отправка файла',
          t_Upgrade = chr(0),
          t_Version = chr(0)
      where t_BlockID = 204702;
      
   it_log.log('DEF-62511. Таблица doprblock_dbt (204702) успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_BlockID = 204702 в таблице doprblock_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprblock_dbt
      set t_Name = 'Закрытие',
          t_Upgrade = chr(0),
          t_Version = chr(0)
      where t_BlockID = 204703;
      
   it_log.log('DEF-62511. Таблица doprblock_dbt (204703) успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_BlockID = 204703 в таблице doprblock_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprostep_dbt
      set t_DayFlag = chr(0),
          t_Symbol = 'И',
          t_Print_Macro = chr(0),
          t_Post_Macro = chr(0),
          t_NotInUse = chr(0),
          t_Name = 'Инициализация',
          t_Rev = chr(0),
          t_OnlyHandCarry = chr(0),
          t_OperOrGroup = chr(0),
          t_UserTypes = chr(0),
          t_AskForDate = chr(0),
          t_IsBackoutGroup = chr(0),
          t_IsCase = chr(0),
          T_ISDISTAFFEXECUTE = chr(0)
      where t_BlockID = 204701
        and t_Number_Step = 5;
      
   it_log.log('DEF-62511. Таблица doprostep_dbt (t_BlockID = 204701, t_Number_Step = 5) успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_BlockID = 204701, t_Number_Step = 5 в таблице doprostep_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprostep_dbt
      set t_DayFlag = chr(0),
          t_Symbol = 'О',
          t_Print_Macro = chr(0),
          t_Post_Macro = chr(0),
          t_NotInUse = chr(0),
          t_Name = 'Отбор записей хранилища СОФР',
          t_Rev = chr(0),
          t_OnlyHandCarry = chr(0),
          t_OperOrGroup = chr(0),
          t_UserTypes = chr(0),
          t_AskForDate = chr(0),
          t_IsBackoutGroup = chr(0),
          t_IsCase = chr(0),
          T_ISDISTAFFEXECUTE = chr(0)
      where t_BlockID = 204702
        and t_Number_Step = 10;
      
   it_log.log('DEF-62511. Таблица doprostep_dbt (t_BlockID = 204702, t_Number_Step = 10) успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_BlockID = 204702, t_Number_Step = 10 в таблице doprostep_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprostep_dbt
      set t_DayFlag = chr(0),
          t_Symbol = 'Ф',
          t_Print_Macro = chr(0),
          t_Post_Macro = chr(0),
          t_NotInUse = chr(0),
          t_Name = 'Формирование и отправка файла',
          t_Rev = chr(0),
          t_OnlyHandCarry = chr(0),
          t_OperOrGroup = chr(0),
          t_UserTypes = chr(0),
          t_AskForDate = chr(0),
          t_IsBackoutGroup = chr(0),
          t_IsCase = chr(0),
          T_ISDISTAFFEXECUTE = chr(0)
      where t_BlockID = 204702
        and t_Number_Step = 15;
      
   it_log.log('DEF-62511. Таблица doprostep_dbt (t_BlockID = 204702, t_Number_Step = 15) успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_BlockID = 204702, t_Number_Step = 15 в таблице doprostep_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprostep_dbt
      set t_DayFlag = chr(0),
          t_Symbol = 'П',
          t_Print_Macro = chr(0),
          t_Post_Macro = chr(0),
          t_NotInUse = chr(0),
          t_Name = 'Получение ответа от хранилища СНОБ',
          t_Rev = chr(0),
          t_OnlyHandCarry = chr(0),
          t_OperOrGroup = chr(0),
          t_UserTypes = chr(0),
          t_AskForDate = chr(0),
          t_IsBackoutGroup = chr(0),
          t_IsCase = chr(0),
          T_ISDISTAFFEXECUTE = chr(0)
      where t_BlockID = 204702
        and t_Number_Step = 20;
      
   it_log.log('DEF-62511. Таблица doprostep_dbt (t_BlockID = 204702, t_Number_Step = 20) успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_BlockID = 204702, t_Number_Step = 20 в таблице doprostep_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprostep_dbt
      set t_DayFlag = chr(0),
          t_Symbol = 'И',
          t_Print_Macro = chr(0),
          t_Post_Macro = chr(0),
          t_NotInUse = chr(0),
          t_Name = 'Закрытие',
          t_Rev = chr(0),
          t_OnlyHandCarry = chr(0),
          t_OperOrGroup = chr(0),
          t_UserTypes = chr(0),
          t_AskForDate = chr(0),
          t_IsBackoutGroup = chr(0),
          t_IsCase = chr(0),
          T_ISDISTAFFEXECUTE = chr(0)
      where t_BlockID = 204703
        and t_Number_Step = 25;
      
   it_log.log('DEF-62511. Таблица doprostep_dbt (t_BlockID = 204703, t_Number_Step = 25) успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_BlockID = 204703, t_Number_Step = 25 в таблице doprostep_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprstknd_dbt
      set t_Name = 'Документооборот',
          t_Code = 'ДО',
          T_ELIMINATED = chr(0)
      where t_StatusKindID = 46501;
      
   it_log.log('DEF-62511. Таблица doprstknd_dbt (t_StatusKindID = 46501) успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_StatusKindID = 46501 в таблице doprstknd_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprstval_dbt
      set t_Name = 'Открыт',
          T_ELIMINATED = chr(0)
      where t_StatusKindID = 46501
        and t_NumValue = 1;
      
   it_log.log('DEF-62511. Таблица doprstval_dbt (t_StatusKindID = 46501, t_NumValue = 1) успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_StatusKindID = 46501, t_NumValue = 1 в таблице doprstval_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprstval_dbt
      set t_Name = 'Отбор записей',
          T_ELIMINATED = chr(0)
      where t_StatusKindID = 46501
        and t_NumValue = 2;
      
   it_log.log('DEF-62511. Таблица doprstval_dbt (t_StatusKindID = 46501, t_NumValue = 2) успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_StatusKindID = 46501, t_NumValue = 2 в таблице doprstval_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprstval_dbt
      set t_Name = 'Закрытие',
          T_ELIMINATED = chr(0)
      where t_StatusKindID = 46501
        and t_NumValue = 3;
      
   it_log.log('DEF-62511. Таблица doprstval_dbt (t_StatusKindID = 46501, t_NumValue = 3) успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_StatusKindID = 46501, t_NumValue = 3 в таблице doprstval_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprstval_dbt
      set t_Name = 'Закрыт',
          T_ELIMINATED = chr(0)
      where t_StatusKindID = 46501
        and t_NumValue = 4;
      
   it_log.log('DEF-62511. Таблица doprstval_dbt (t_StatusKindID = 46501, t_NumValue = 4) успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись с t_StatusKindID = 46501, t_NumValue = 4 в таблице doprstval_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doproblck_dbt
      set t_NotInUse = chr(0),
          t_NoInsert = chr(0),
          t_NoReplace = chr(0),
          t_IsManual = chr(0),
          t_SymbolsForInsertion = chr(0),
          t_Symbol = chr(0)
      where t_Kind_Operation = 2047;
      
   it_log.log('DEF-62511. Таблица doproblck_dbt успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Записи в таблице doproblck_dbt не найдены');
   end;
   
   COMMIT;
   
   begin
      update doprkstep_dbt
      set t_Name = 'Пользовательский шаг',
          T_OPERORGROUP = chr(0)
      where t_DocKind = 4650;
      
   it_log.log('DEF-62511. Таблица doprkstep_dbt успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись в таблице doprkstep_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update doprkdate_dbt
      set t_NameDate = 'Дата операции',
          T_ELIMINATED = chr(0)
      where t_DocKind = 4650;
      
   it_log.log('DEF-62511. Таблица doprkdate_dbt успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись в таблице doprkdate_dbt не найдена');
   end;
   
   COMMIT;
   
   begin
      update DOPRSBLCK_DBT
      set t_Default = chr(0)
      where t_StatusKindID = 46501;
      
   it_log.log('DEF-62511. Таблица doprsblck_dbt успешно обновлена');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-62511. Запись в таблице doprsblck_dbt не найдена');
   end;
end;
/