-- Вид операций
BEGIN
  insert into doprkoper_dbt (t_kind_operation, t_dockind, t_notinuse, t_initmacro, t_systypes, t_name, t_shortname, t_upgrade, t_ismaintanance, t_parent, t_version, t_massoprstart)
                     values (1095, 4661, CHR(0), 'vsdousinit.mac', CHR(1), 'Сопровождение Договоров об общих условиях', 'СОПР_ДОУС', CHR(0), CHR(0), 0, CHR(1), CHR(0));
END;
/

-- Блоки операций
BEGIN
  INSERT ALL
    INTO doprblock_dbt (t_BlockID, t_Name, t_DocKind, t_Parent, t_Upgrade, t_Version, t_VersionWeb)
                VALUES (109500, 'Основная ветка', 4661, 0, CHR(0), CHR(1), 0)
    INTO doprblock_dbt (t_BlockID, t_Name, t_DocKind, t_Parent, t_Upgrade, t_Version, t_VersionWeb) 
                VALUES (109501, 'Пролонгация ДОУС', 4661, 0, CHR(0), CHR(1), 0)
  SELECT 1 FROM DUAL;
END;
/

-- Виды шагов
BEGIN
  INSERT INTO dOprKStep_dbt (t_DocKind, t_Kind_Action, t_Name, t_IsAllowForOper, t_OperOrGroup, t_Reserve) 
                     VALUES (4661, 1, 'Пользовательский шаг', 0, CHR(0), CHR(1));
END;
/

-- Виды дат операции
BEGIN
  INSERT ALL
     INTO dOprKDate_dbt (t_DateKindID, t_DocKind, t_NumberDate, t_NameDate, t_Eliminated)
                 VALUES (46619999, 4661, -1, 'Дата цепочки', CHR(0))
     INTO dOprKDate_dbt (t_DateKindID, t_DocKind, t_NumberDate, t_NameDate, t_Eliminated)
                 VALUES (46610000, 4661, 0, 'Начало операции', CHR(0))
     INTO dOprKDate_dbt (t_DateKindID, t_DocKind, t_NumberDate, t_NameDate, t_Eliminated)
                 VALUES (46610001, 4661, 1, 'Закрытие', CHR(0))
  SELECT 1 FROM DUAL;
END;
/

-- Шаги
BEGIN
  INSERT ALL
  INTO doprostep_dbt (t_BlockID, t_Number_Step, t_Kind_Action, t_DayOffset, t_Scale,
                      t_DayFlag, t_CalendarID, t_Symbol, t_Previous_Step, t_Modification,
                      t_Carry_Macro, t_Print_Macro, t_Post_Macro, t_NotInUse, t_FirstStep,
                      t_Name, t_DateKindID, t_Rev, t_AutoExecuteStep, t_OnlyHandCarry,
                      t_IsAllowForOper, t_OperOrGroup, t_RestrictEarlyExecution, t_UserTypes, t_InitDateKindID,
                      t_AskForDate, t_Backout, t_IsBackoutGroup, t_MassExecuteMode, t_IsCase,
                      t_IsDistaffExecute, t_SkipInitAfterPlanDate, t_MassPackSize) 
              VALUES (109500, 10, 1, 0, 0,
                      CHR(88), 0, 'о', 0, 0,
                      'vsdous10.mac', CHR(1), CHR(1), CHR(0), CHR(0),
                      'Открытие Договора об общих условиях', 46610000, CHR(0), CHR(88), CHR(0),
                      0, CHR(0), CHR(88), CHR(1), 0,
                      CHR(0), 0, CHR(0), 0, CHR(0),
                      CHR(0), CHR(0), 0)
  INTO doprostep_dbt (t_BlockID, t_Number_Step, t_Kind_Action, t_DayOffset, t_Scale,
                      t_DayFlag, t_CalendarID, t_Symbol, t_Previous_Step, t_Modification,
                      t_Carry_Macro, t_Print_Macro, t_Post_Macro, t_NotInUse, t_FirstStep,
                      t_Name, t_DateKindID, t_Rev, t_AutoExecuteStep, t_OnlyHandCarry,
                      t_IsAllowForOper, t_OperOrGroup, t_RestrictEarlyExecution, t_UserTypes, t_InitDateKindID,
                      t_AskForDate, t_Backout, t_IsBackoutGroup, t_MassExecuteMode, t_IsCase,
                      t_IsDistaffExecute, t_SkipInitAfterPlanDate, t_MassPackSize) 
              VALUES (109500, 15, 1, 0, 0,
                      CHR(88), 0, 'з', 10, 0,
                      'vsdous15.mac', CHR(1), CHR(1), CHR(0), CHR(0),
                      'Закрытие Договора об общих условиях', 46610001, CHR(0), CHR(0), CHR(0),
                      0, CHR(0), CHR(88), CHR(1), 0,
                      CHR(0), 0, CHR(0), 0, CHR(0),
                      CHR(0), CHR(0), 0)
  INTO doprostep_dbt (t_BlockID, t_Number_Step, t_Kind_Action, t_DayOffset, t_Scale,
                      t_DayFlag, t_CalendarID, t_Symbol, t_Previous_Step, t_Modification,
                      t_Carry_Macro, t_Print_Macro, t_Post_Macro, t_NotInUse, t_FirstStep,
                      t_Name, t_DateKindID, t_Rev, t_AutoExecuteStep, t_OnlyHandCarry,
                      t_IsAllowForOper, t_OperOrGroup, t_RestrictEarlyExecution, t_UserTypes, t_InitDateKindID,
                      t_AskForDate, t_Backout, t_IsBackoutGroup, t_MassExecuteMode, t_IsCase,
                      t_IsDistaffExecute, t_SkipInitAfterPlanDate, t_MassPackSize) 
              VALUES (109501, 20, 1, 0, 0,
                      CHR(88), 0, 'п', 0, 0,
                      'vsdous20.mac', CHR(1), CHR(1), CHR(0), CHR(0),
                      'Пролонгация Договора об общих условиях', 0, CHR(0), CHR(88), CHR(0),
                      0, CHR(0), CHR(88), CHR(1), 0,
                      CHR(0), 0, CHR(0), 0, CHR(0),
                      CHR(0), CHR(0), 0)
  SELECT 1 FROM DUAL;
END;
/

-- Сегменты статуса
BEGIN
  INSERT INTO dOprStKnd_dbt (t_StatusKindID, t_Code, t_Name, t_DocKind, t_Sort, t_Eliminated)
                     VALUES (46611, 'ДО', 'Документооборот', 4661, 0, CHR(0));
                   
  INSERT ALL
  INTO dOprStVal_dbt (t_StatusKindID, t_NumValue, t_Name, t_Eliminated)
              VALUES (46611, 1, 'Открыт', CHR(0))
  INTO dOprStVal_dbt (t_StatusKindID, t_NumValue, t_Name, t_Eliminated)
              VALUES (46611, 2, 'Закрыт', CHR(0))
  INTO dOprStVal_dbt (t_StatusKindID, t_NumValue, t_Name, t_Eliminated)
              VALUES (46611, 3, 'Отвергнут', CHR(0))
  SELECT 1 FROM DUAL;
END;
/

-- Настройка блока для вида операции
DECLARE
  v_OperBlockID NUMBER := 0;
BEGIN
  INSERT INTO dOprOBlck_dbt (t_OperBlockID, t_Kind_Operation, t_BlockID, t_Sort, t_NotInUse, t_NoInsert, t_NoReplace, t_NoCloseInsert, t_IsManual, t_SymbolsForInsertion, t_Symbol)
                     VALUES (0, 1095, 109500, 1, CHR(0), CHR(0), CHR(0), CHR(88), CHR(0), CHR(1), CHR(0))
  RETURNING t_OperBlockID INTO v_OperBlockID;
  
  INSERT INTO dOprCBlck_dbt (t_OperBlockID, t_StatusKindID, t_NumValue, t_Condition)
                     VALUES (v_OperBlockID, 46611, 1, 0);
                     
  INSERT INTO dOprOBlck_dbt (t_OperBlockID, t_Kind_Operation, t_BlockID, t_Sort, t_NotInUse, t_NoInsert, t_NoReplace, t_NoCloseInsert, t_IsManual, t_SymbolsForInsertion, t_Symbol)
                     VALUES (0, 1095, 109501, 2, CHR(0), CHR(0), CHR(0), CHR(88), CHR(0), 'з', 'Р');
                     
  INSERT INTO dOprInist_dbt (t_Kind_Operation, t_StatusKindID, t_NumValue)
                     VALUES (1095, 46611, 1);
END;
/

-- Статусы при выходе из блока
BEGIN
  INSERT ALL
  INTO dOprSBlck_dbt (t_BlockID, t_StatusKindID, t_NumValue, t_Default)
              VALUES (109500, 46611, 2, CHR(88))
  INTO dOprSBlck_dbt (t_BlockID, t_StatusKindID, t_NumValue, t_Default)
              VALUES (109501, 46611, 1, CHR(88))
  SELECT 1 FROM DUAL;
END;
/