-- Настройка конвейера "Исполнение открытых внебиржевых сделок"
DECLARE
  v_ConvTypeID number := 1006; -- конвейер "Исполнение открытых внебиржевых сделок"
  v_TaskID number := 1006; -- вариант запуска конвейера "Исполнение открытых внебиржевых сделок"
  v_OperationID number := 1006; -- операция "Исполнение открытых внебиржевых сделок"
  v_Macro varchar2(32) := 'KondorRunDealCnv.mac';
  v_Service varchar2(64) := 'http://sgo-ap605/RSBankWS/RSBankWS.asmx';
  
  -- Функция создания конвейера
  FUNCTION addConv(aConvTypeID number, aConvName varchar2) RETURN number 
  IS
    v_Conv number;
    v_Name dcnvtype_dbt.t_name%TYPE;
    v_NeedToCreate boolean := false; -- нужно создавать конвейер ?
    v_NeedToGenerate boolean := false; -- нужно генерировать номер конвейера ? да, если конвейер с номером существует, но не тот, который нужен
  BEGIN
    BEGIN
      SELECT t_name INTO v_Name FROM dcnvtype_dbt r WHERE r.t_convTypeID = aConvTypeID;
      IF v_Name <> aConvName THEN
        v_NeedToCreate := true;
        v_NeedToGenerate := true;
      END IF;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN 
        v_NeedToCreate := true;
    END;
    IF v_NeedToCreate = true THEN
      IF v_NeedToGenerate = false THEN
        v_Conv := aConvTypeID; 
      ELSE
        BEGIN
          SELECT nvl(max(t_convTypeID), 0)+1 INTO v_Conv FROM dcnvtype_dbt;
        EXCEPTION
          WHEN others THEN 
            v_Conv := -1;
        END;
      END IF;
      IF v_Conv > 0 THEN
        BEGIN
          INSERT INTO dcnvtype_dbt ( 
            t_convTypeID, t_Name, t_packetsize
          ) VALUES (
            v_Conv, aConvName, 1000
          );
        EXCEPTION
          WHEN others THEN 
            v_Conv := -1;
        END;
      END IF;
    END IF;
    
    RETURN(v_Conv);
  END;
  -- Функция создания общего варианта запуска конвейера
  FUNCTION addTask(aTaskID number, aTaskName varchar2) RETURN number 
  IS
    v_Task number;
    v_Name dcnvtask_dbt.t_name%TYPE;
    v_NeedToCreate boolean := false; -- нужно создавать вариант запуска ?
    v_NeedToGenerate boolean := false; -- нужно генерировать номер варианта запуска ? да, если вариант запуска с номером существует, но не тот, который нужен
  BEGIN
    BEGIN
      SELECT t_name INTO v_Name FROM dcnvtask_dbt r WHERE r.t_taskID = aTaskID;
      IF v_Name <> aTaskName THEN
        v_NeedToCreate := true;
        v_NeedToGenerate := true;
      END IF;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN 
        v_NeedToCreate := true;
    END;
    
    IF v_NeedToCreate = true THEN
      IF v_NeedToGenerate = false THEN
        v_Task := aTaskID; 
      ELSE
        BEGIN
          SELECT nvl(max(t_taskID), 0)+1 INTO v_Task FROM dcnvtask_dbt;
        EXCEPTION
          WHEN others THEN 
            v_Task := -1;
        END;
      END IF;
      IF v_Task > 0 THEN
        BEGIN
          INSERT INTO dcnvtask_dbt ( 
            t_taskID, t_Name
          ) VALUES (
            v_Task, aTaskName
          );
        EXCEPTION
          WHEN others THEN 
            v_Task := -1;
        END;
      END IF;
    END IF;
    
    RETURN(v_Task);
  END;
  
  -- Процедура создания варианта запуска конвейера
  PROCEDURE addTaskCnv(aTaskID number, aConvTypeID number, aSelectProc varchar2, aSelectMacro varchar2)
  IS
  BEGIN
    INSERT INTO dcnvtaskcnv_dbt ( 
      t_taskID, t_convtypeID, t_selectproc, t_selectmacro, t_panelclass, t_panelmacro, t_notused
      , t_packetSize, t_afterexecproc, t_afterexecmacro
    ) VALUES (
      aTaskID, aConvTypeID, aSelectProc, aSelectMacro, chr(1), chr(1), chr(0)
      , 1000, chr(1), chr(1) 
    );
  END;
  
  -- Процедура создания вида операции
  PROCEDURE addOprType(aOperationID number, aName varchar2, aShortName varchar2, aProc varchar2, aMacro varchar2, aService varchar2)
  IS
  BEGIN
    INSERT INTO dcnvoprtype_dbt ( 
      t_operationid, t_name, t_shortname, t_proc, t_macro, t_panelclass, t_panelmacro
      , t_statein, t_stateout, t_service, t_threads
    ) VALUES (
      aOperationID, aName, aShortName, aProc, aMacro, chr(1), chr(1)
      , aOperationID*2-1, aOperationID*2, aService, 1
    );
  END;
  
  -- Процедура создания операции конвейера
  PROCEDURE addCnvOpr( aConvTypeID number, aOperationID number )
  IS
  BEGIN
    INSERT INTO dcnvopr_dbt ( 
      t_convtypeid, t_operationid, t_order, t_notused, t_service, t_threads
    ) VALUES (
      aConvTypeID, aOperationID, 1, chr(0), chr(1), 0
    );
  END;
  
  -- Процедура добавления макроса, вызываемого удаленно
  PROCEDURE addXrMacro( aMacro varchar2 )
  IS
  BEGIN
    INSERT INTO dxr_macro_dbt ( 
      t_macroid, t_macro, t_ispublic, t_issystem
    ) VALUES (
      0, aMacro, 'X', 'X'
    );
  END;
BEGIN
  -- 1. Создание конвейера (или проверка, что нужный конвейер есть)
  v_ConvTypeID := addConv(v_ConvTypeID, 'Исполнение открытых внебиржевых сделок');
  -- 2. Создание общего варианта запуска (или проверка, что нужный вариант запуска есть)
  v_TaskID := addTask(v_TaskID, 'Запуск конвейера по исполнению открытых внебиржевых сделок');
  -- 3. Создание варианта запуска
  addTaskCnv(v_TaskID, v_ConvTypeID, 'SelectKondorDealsCnv', v_Macro);
  -- 4. Создание операции
  addOprType(v_OperationID, 'Пакетная операция исполнения открытых внебиржевых сделок', 'Исполнение открытых внебиржевых сделок', 'ExecKondorDealsCnv', v_Macro, v_Service);
  -- 5. создание операции конвейера
  addCnvOpr(v_ConvTypeID, v_OperationID);
  -- 6. реестр макросов, вызываемых удаленно
  -- addXrMacro(v_Macro);
END;
/