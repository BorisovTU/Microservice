CREATE OR REPLACE PACKAGE BODY OPRDOCS_utl IS

  --Encoding: Win 866

  /**
   @file 		OPRDOCS_utl.pks
   @brief 		Утилиты для трансформации данных таблицы OPRDOCS_DBT (CCBO-11001_CCBO-11002)
     
# changelog
 |date       |autor          |tasks                                           |note
 |-----------|---------------|------------------------------------------------|-------------------------------------------------------------
 |12.03.2025 |Велигжанин А.В.|BOSS-7698_BOSS-8204                             |fillDTransferControlDbt()
 |20.12.2024 |Велигжанин А.В.|CCBO-11001_CCBO-11002                           |Создание                  
     
  */

  g_SourceTable VARCHAR2(64) := 'DOPRDOCS_DBT';  	-- исходная таблица
  g_DestTable VARCHAR2(64) := 'DOPRDOCS1_DBT';  	-- таблица назначения (партиционированная)
  g_CompressFlag NUMBER := 0;  				-- нужна компрессия?

  /**
   @brief    Функция для журналирования времени выполнения.
  */
  FUNCTION ElapsedTime ( p_time IN pls_integer ) return varchar2 
  IS
  BEGIN
    RETURN to_char((dbms_utility.get_time - p_time) / 100, 'fm9999999990D00');
  END ElapsedTime;

  /**
   @brief    Функция для получения имени партиции для обработки
  */
  FUNCTION GetReadyPartition RETURN varchar2 
  IS
    x_PartitionName VARCHAR2(64);
    x_Ret NUMBER := 0;
    pragma autonomous_transaction;
  BEGIN
    UPDATE dtransfercontrol_dbt r 
      SET r.t_status = 'P', r.t_started = SYSDATE 
      WHERE r.t_status = 'R' AND r.t_partitionname = 'P2017_12' AND rownum = 1 
      RETURNING r.t_partitionname
      INTO x_PartitionName
    ;
    IF( SQL%ROWCOUNT <> 1) THEN
      UPDATE dtransfercontrol_dbt r 
        SET r.t_status = 'P', r.t_started = SYSDATE 
        WHERE r.t_highvalue = (SELECT max(t_highvalue) FROM dtransfercontrol_dbt WHERE t_status = 'R')
        RETURNING r.t_partitionname
        INTO x_PartitionName
      ;
      IF( SQL%ROWCOUNT <> 1) THEN
        x_PartitionName := NULL;
      END IF;
    END IF;
    COMMIT;
    RETURN x_PartitionName;
  EXCEPTION
    WHEN others THEN
      ROLLBACK;
      RETURN NULL;
  END;

  /**
   @brief    отметка о завершении обработки кусочка партиции
  */
  PROCEDURE EndChunk( p_RowID IN NUMBER, p_Status IN VARCHAR2, p_ParaID IN NUMBER, p_Count IN NUMBER )
  IS
    pragma autonomous_transaction;
  BEGIN
    UPDATE itt_parallel_exec 
       SET str02 = p_Status, dat02 = SYSDATE, num03 = p_ParaID, num04 = p_Count 
     WHERE row_id = p_RowID;
    COMMIT;
  EXCEPTION
    WHEN others THEN
      ROLLBACK;
  END;

  /**
   @brief    Копирование данных для одной партиции для диапазона t_id_operation c p_StartID по p_EndID
   @param[in]  	p_ParaID     		номер параллельного процесса
   @param[in]  	p_PartitionName 	наименование партиции таблицы OPROPER для обработки
   @param[in]  	p_StartID     		начальный t_id_operation
   @param[in]  	p_EndID     		конечный t_id_operation
   @param[in]  	p_RowID     		ID задания ( itt_parallel_exec.row_id )
   @param[in]  	p_SrcTable     		имя таблицы-источника
   @param[in]  	p_DstTable     		имя таблицы-приемника
  */
  PROCEDURE CopyOnePartition ( 
    p_ParaID IN NUMBER
    , p_PartitionName IN VARCHAR2
    , p_StartID IN NUMBER
    , p_EndID IN NUMBER
    , p_RowID IN NUMBER 
    , p_SrcTable IN VARCHAR2
    , p_DstTable IN VARCHAR2
  )
  IS
    x_Prefix VARCHAR2(64) := to_char(p_ParaID)||': '||p_PartitionName||' ('||p_StartID||', '||p_EndID||')';
    x_StartTime pls_integer;
    x_Sql CLOB;
    x_Rows NUMBER := 0;
  BEGIN
    x_StartTime := dbms_utility.get_time;
    x_Sql := 'INSERT /*+ APPEND */ INTO '||p_DstTable||' SELECT d.* FROM '||p_SrcTable||' d '
             ||' WHERE d.t_id_operation IN ('
             ||' SELECT t_id_operation FROM doproper_dbt PARTITION ('||p_PartitionName||') WHERE t_id_operation BETWEEN :1 AND :2)'
    ;
    EXECUTE IMMEDIATE x_Sql USING p_StartID, p_EndID;
    x_Rows := SQL%ROWCOUNT;
    COMMIT;
    EndChunk(p_RowID, 'S', p_ParaID, x_Rows);

    -- Сообщение о завершении процедуры
    it_log.log(
      p_msg => x_Prefix||', End: '||ElapsedTime(x_StartTime)||', rows: '||to_char(x_Rows)
      , p_msg_type => it_log.c_msg_type__debug
    );

  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log( p_msg => x_Prefix||', Err: '||SQLERRM, p_msg_type => it_log.c_msg_type__error );
      it_error.clear_error_stack;
      EndChunk(p_RowID, 'E', p_ParaID, x_Rows);
      RAISE;
  END CopyOnePartition;

  /**
   @brief    отметка о завершении обработки партиции
  */
  PROCEDURE EndProcess ( p_PartitionName IN varchar2, p_CompressFlag IN NUMBER )
  IS
    pragma autonomous_transaction;
  BEGIN
    UPDATE dtransfercontrol_dbt r 
      SET r.t_status = 'S', r.t_ended = SYSDATE, r.t_CompressFlag = p_CompressFlag
      WHERE r.t_partitionname  = p_PartitionName
    ;
    COMMIT;
  EXCEPTION
    WHEN others THEN
      ROLLBACK;
  END;

  /**
   @brief    Возвращает 1, если есть необработанный кусочек партиции
   @param[in]  	p_CalcID     		ID расчета (itt_parallel_exec.calc_id)
   @param[in]  	p_PartitionName 	наименование партиции таблицы OPROPER для обработки
   @param[out]	p_StartID     		начальный t_id_operation
   @param[out] 	p_EndID     		конечный t_id_operation
   @param[out] 	p_RowID     		ID задания ( itt_parallel_exec.row_id )
   @param[out] 	p_SrcTable     		имя таблицы-источника
   @param[out]  p_DstTable     		имя таблицы-приемника
  */
  FUNCTION GetChunk ( 
    p_CalcID IN varchar2
    , p_PartitionName IN VARCHAR2
    , p_StartID OUT NUMBER
    , p_EndID OUT NUMBER
    , p_RowID OUT NUMBER 
    , p_SrcTable OUT VARCHAR2
    , p_DstTable OUT VARCHAR2
  ) RETURN number
  IS
    x_Ret NUMBER := 0;
    x_Sql CLOB;
    pragma autonomous_transaction;
  BEGIN
    x_Sql := 'UPDATE itt_parallel_exec partition (p'||p_CalcID||') r '
             ||' SET r.str02 = ''P'', r.dat01 = SYSDATE '
             ||' WHERE r.str02 is null AND r.str01 = '''||p_PartitionName||''' AND rownum = 1 '
             ||' RETURNING r.num01, r.num02, r.row_id, r.str03, r.str04 '
             ||' INTO :p_StartID, :p_EndID, :p_RowID, :p_SrcTable, :p_DstTable '
    ;
    EXECUTE IMMEDIATE x_Sql RETURNING INTO p_StartID, p_EndID, p_RowID, p_SrcTable, p_DstTable;
    IF( SQL%ROWCOUNT = 1) THEN
      x_Ret := 1;
    END IF;
    COMMIT;
    RETURN x_Ret;
  EXCEPTION
    WHEN others THEN
      ROLLBACK;
      RETURN x_Ret;
  END GetChunk;

  /**
   @brief    Нитка параллельного выполнения для копирования данных.
   @param[in]  p_ParaID     	номер параллельного процесса
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
  */
  PROCEDURE CopyParallelProc ( p_ParaID IN NUMBER, p_CalcID IN varchar2 )
  IS
    x_Prefix VARCHAR2(64) := '('||to_char(p_ParaID)||', SessionID: '||TO_CHAR (USERENV ('sessionid'))||')';
    x_StartTime pls_integer;
    x_PartitionName VARCHAR2(64);
    x_CompressFlag NUMBER := 0;
    x_StartID number; 
    x_EndID number; 
    x_RowID number;
    x_Count number := 0;
    x_Limit number := 0;
    x_SrcTable varchar2(64);
    x_DstTable varchar2(64);
    x_Rows number;
  BEGIN
    x_StartTime := dbms_utility.get_time;

    -- Сообщение о начале процедуры
    it_log.log( p_msg => x_Prefix||', Start ', p_msg_type => it_log.c_msg_type__debug );

    -- Цикл, пока есть даты для обработки
    LOOP
       x_PartitionName := GetReadyPartition();     		-- получаем партицию
       it_log.log( 
         p_msg => x_Prefix||', x_PartitionName: '||x_PartitionName
         , p_msg_type => it_log.c_msg_type__debug 
       );
       -- завершаем обработку, если нет партиции
       EXIT WHEN x_PartitionName IS NULL;          		
       -- копируем, пока не закончатся кусочки партиции
       WHILE( GetChunk(p_CalcID, x_PartitionName, x_StartID, x_EndID, x_RowID, x_SrcTable, x_DstTable) = 1 ) LOOP
          CopyOnePartition ( p_ParaID, x_PartitionName, x_StartID, x_EndID, x_RowID, x_SrcTable, x_DstTable );
       END LOOP;
       EndProcess ( x_PartitionName, x_CompressFlag ); 		-- отметка о завершении обработки партиции
       x_Count := x_Count + 1;
       IF(x_Limit <> 0 AND x_Count > x_Limit) THEN
         EXIT;
       END IF;
    END LOOP;

    -- Сообщение о завершении процедуры
    it_log.log( p_msg => x_Prefix||', End: '||ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );

  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log( p_msg => x_Prefix||', Err: '||SQLERRM, p_msg_type => it_log.c_msg_type__error );
      it_error.clear_error_stack;
      RAISE;

  END CopyParallelProc;

  /**
   @brief    Процедура создания суррогатных партиций в таблице-приемнике.
   @param[in]  p_SrcTable     	имя таблицы-источника
   @param[in]  p_DstTable     	имя таблицы-приемника
  */
  PROCEDURE CreateSurrogates ( p_SrcTable IN varchar2, p_DstTable IN varchar2 )
  IS
    x_Sql CLOB;
    x_SqlIns CLOB;
    x_Rec VARCHAR2(64);
    x_Cursor SYS_REFCURSOR;
  BEGIN
    x_Sql := 'SELECT t_partitionname FROM dtransfercontrol_dbt r  '
       ||' WHERE r.t_partitionname not in ( '
       ||' SELECT partition_name FROM user_tab_partitions WHERE table_name = '''||p_DstTable||''')';
    OPEN x_Cursor FOR x_Sql;
    LOOP
      FETCH x_Cursor INTO x_Rec;
      EXIT WHEN x_Cursor%NOTFOUND;
      x_SqlIns := 'INSERT INTO '||p_DstTable
         ||' SELECT d.* FROM '||p_SrcTable||' d, DOPROPER_DBT partition ('||x_Rec||') o '
         ||' WHERE d.t_id_operation = o.t_id_operation AND rownum = 1';
      EXECUTE IMMEDIATE x_SqlIns;
      EXECUTE IMMEDIATE 'ROLLBACK';
    END LOOP;
    CLOSE x_Cursor;
  END CreateSurrogates;

  /**
   @brief    Очистка данных перед запуском копирования.
  */
  PROCEDURE CleanEnv ( p_CalcID IN varchar2 )
  IS
    x_Prefix VARCHAR2(64) := p_CalcID;
    x_StartTime pls_integer;
  BEGIN
    x_StartTime := dbms_utility.get_time;
    EXECUTE IMMEDIATE 'truncate table doprdocs1_dbt';
    it_log.log( p_msg => x_Prefix||'DELETED FROM doprdocs1_dbt, Time: ' || ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );

    x_StartTime := dbms_utility.get_time;
    update dtransfercontrol_dbt r set r.t_status = 'R', r.t_started = null, r.t_ended = null;
    it_log.log( p_msg => x_Prefix||'dtransfercontrol_dbt is ready, Time: ' || ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );

    x_StartTime := dbms_utility.get_time;
    EXECUTE IMMEDIATE 'UPDATE itt_parallel_exec partition (P'||p_CalcID||') SET STR02 = null, dat01 = null, dat02 = null';
    it_log.log( p_msg => x_Prefix||' itt_parallel_exec is clean, Time: ' || ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );
    commit;

    x_StartTime := dbms_utility.get_time;
    CreateSurrogates('DOPRDOCS_DBT', 'DOPRDOCS1_DBT');
    it_log.log( p_msg => x_Prefix||' Surrogates created, Time: ' || ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );

  END CleanEnv;


  /**
   @brief    Заполнение управляющей таблицы DTRANSFERCONTROL_DBT (см. BOSS-7698_BOSS-8204)
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
  */
  PROCEDURE fillDTransferControlDbt ( p_CalcID IN NUMBER )
  IS
    x_HighValue DATE;
    x_TableName VARCHAR2(32) := 'DOPROPER_DBT';
    x_Rows NUMBER := 0;
  BEGIN
    it_log.log( p_msg => 'заполнение таблицы DTRANSFERCONTROL_DBT ('||p_CalcID||')', p_msg_type => it_log.c_msg_type__debug );
    EXECUTE IMMEDIATE 'DELETE FROM dtransfercontrol_dbt '; 
    FOR i IN (SELECT PARTITION_NAME, HIGH_VALUE FROM USER_TAB_PARTITIONS WHERE TABLE_NAME = x_TableName) LOOP
      EXECUTE IMMEDIATE 'BEGIN :ret := ' || i.HIGH_VALUE || '; END;' USING OUT x_HighValue;
      EXECUTE IMMEDIATE 'INSERT INTO dtransfercontrol_dbt (t_partitionname, t_highvalue) VALUES (:partition_name, :x_HighValue) ' 
        USING i.partition_name, x_HighValue;
      x_Rows := x_Rows + 1;
    END LOOP;
    it_log.log( p_msg => 'Произведено заполнение таблицы DTRANSFERCONTROL_DBT, x_Rows: '||x_Rows, p_msg_type => it_log.c_msg_type__debug );
  END fillDTransferControlDbt;

  /**
   @brief    Генерация чанков для полученной партиции для itt_parallel_exec.
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
   @param[in]  p_PartitionName 	наименование партиции (исходной таблицы и doproper_dbt)
   @param[in]  p_ChunkSize     	размер порции
   @param[in]  p_SrcTable     	имя таблицы-источника
   @param[in]  p_DstTable     	имя таблицы-приемника
  */
  PROCEDURE GenPartitionChunks ( p_CalcID IN NUMBER, p_PartitionName IN VARCHAR2, p_ChunkSize IN NUMBER, p_SrcTable IN varchar2, p_DstTable IN varchar2 )
  IS
    x_Start NUMBER := -1;
    x_End NUMBER;
    x_Counts NUMBER := 0;
    x_Rec NUMBER;
    x_Rows NUMBER;
    x_Cursor SYS_REFCURSOR;
    x_SqlCount VARCHAR2(1000);
    x_SqlIns VARCHAR2(1000);
    x_SqlLoop VARCHAR2(1000);
  BEGIN
    x_SqlCount := 'SELECT count(*) FROM '||p_SrcTable||' WHERE t_id_operation = :1';
    x_SqlLoop := 'SELECT o.t_id_operation FROM doproper_dbt partition ('||p_PartitionName||') o ORDER BY o.t_id_operation';
    x_SqlIns := 'INSERT INTO itt_parallel_exec ( calc_id, str01, num01, num02, num05, str03, str04 ) VALUES ( :1, :2, :3, :4, :5, :6, :7 )';
    OPEN x_Cursor FOR x_SqlLoop;
    LOOP
      FETCH x_Cursor INTO x_Rec;
      EXIT WHEN x_Cursor%NOTFOUND;
      EXECUTE IMMEDIATE x_SqlCount INTO x_Rows USING x_Rec;
      IF(x_Start = -1) THEN
        x_Start := x_Rec;
      END IF;
      x_Counts := x_Counts + x_Rows;
      x_End := x_Rec;
      IF(x_Counts > p_ChunkSize) THEN
        -- начинаем новую группу
        it_log.log( p_msg => p_PartitionName||': '||x_Start||'..'||x_End||'('||x_Counts||')', p_msg_type => it_log.c_msg_type__debug );
        EXECUTE IMMEDIATE x_SqlIns USING p_CalcID, p_PartitionName, x_Start, x_End, x_Counts, p_SrcTable, p_DstTable;
        x_Start := -1;
        x_Counts := 0;
      END IF;
    END LOOP;
    CLOSE x_Cursor;

    IF(x_Start <> -1) THEN
      it_log.log( p_msg => p_PartitionName||': '||x_Start||'..'||x_End||'('||x_Counts||')', p_msg_type => it_log.c_msg_type__debug );
      EXECUTE IMMEDIATE x_SqlIns USING p_CalcID, p_PartitionName, x_Start, x_End, x_Counts, p_SrcTable, p_DstTable;
    END IF;
  END GenPartitionChunks;

  /**
   @brief    Перенос данных из таблицы DOPRDOCS_DBT в партиционированную таблицу.
             Так как таблица DOPRDOCS_DBT -- большого размера (~140 Gb на 23-12-2024),
             перенос данных осуществляется параллельно несколькими потоками.
  */
  PROCEDURE CopyParallel ( p_CalcID IN varchar2, p_ParaLevel IN NUMBER, p_Limit IN NUMBER )
  IS
    x_ChunkSql VARCHAR2(2000);
    x_SqlStmt VARCHAR2(2000);
    x_StartTime pls_integer;
  BEGIN
    x_StartTime := dbms_utility.get_time;

    -- Сообщение о начале процедуры
    it_log.log( p_msg => 'p_CalcID: '||p_CalcID||', p_ParaLevel: '||p_ParaLevel||', p_Limit: '||p_Limit, p_msg_type => it_log.c_msg_type__debug );

    -- так как при формировании чанков, используется управляющая таблица с партициями, то нужно предварительно
    -- выполнить очистку
    UPDATE dtransfercontrol_dbt r SET r.t_status = 'R', r.t_started = null, r.t_ended = null;
    COMMIT;

    -- выражение для определения чанков
    x_ChunkSql := 'SELECT level, level FROM DUAL CONNECT BY LEVEL <= '||p_ParaLevel;

    -- выражение для процедуры параллельного исполнения
    x_SqlStmt := q'[
       DECLARE
         x_StartID number := :start_id ; x_EndID number := :end_id; 
       BEGIN
         OPRDOCS_utl.CopyParallelProc( x_StartID, ']'||p_CalcID||q'[' );
       EXCEPTION
           when others then
             it_error.put_error_in_stack;
             it_log.log( p_msg => 'Err: '||SQLERRM, p_msg_type => it_log.c_msg_type__error );
             it_error.clear_error_stack;
             RAISE;
       END;
    ]';

    -- запуск параллельного процесса
    it_parallel_exec.run_task_chunks_by_sql ( 
       p_parallel_level => p_ParaLevel
       , p_chunk_sql => x_ChunkSql
       , p_sql_stmt => x_SqlStmt 
    );

    -- Сообщение о завершении процедуры
    it_log.log( p_msg => 'End: '||ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );

  END CopyParallel;


  /**
   @brief    Нитка параллельного выполнения для пересоздания индекса.
   @param[in]  p_ParaID     	номер параллельного процесса
   @param[in]  p_TableName     	наименование таблицы
  */
  PROCEDURE RebuildOneIndex ( p_ParaID IN NUMBER, p_TableName IN varchar2 )
  IS
    x_Prefix VARCHAR2(64) := '('||to_char(p_ParaID)||', SessionID: '||TO_CHAR (USERENV ('sessionid'))||')';
    x_StartTime pls_integer;
    x_Sql CLOB;
    x_IndexName VARCHAR2(64);
  BEGIN
    x_StartTime := dbms_utility.get_time;

    -- в зависимости от номера потока, работаем с разными индексами, определяем имя индекса
    IF(p_ParaID = 1) THEN
      x_IndexName := p_TableName||'_IDX2';
    ELSIF(p_ParaID = 2) THEN
      x_IndexName := p_TableName||'_IDX5';
    END IF;

    -- Определяем выражение для пересоздания индекса
    IF(x_IndexName IS NOT NULL) THEN
      x_Sql := 'ALTER INDEX '||x_IndexName||' REBUILD ONLINE NOLOGGING';
    END IF;

    -- Сообщение о начале процедуры, логгируем выражение для пересоздания индекса
    it_log.log( p_msg => x_Prefix||', Start '||x_IndexName, p_msg_type => it_log.c_msg_type__debug, p_msg_clob => x_Sql );

    -- Сообщение о начале процедуры, логгируем выражение для пересоздания индекса
    IF(x_Sql IS NOT NULL) THEN
      EXECUTE IMMEDIATE x_Sql;
    END IF;

    -- Сообщение о завершении процедуры
    it_log.log( p_msg => x_Prefix||', End: '||ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );

  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log( p_msg => x_Prefix||', Err: '||SQLERRM, p_msg_type => it_log.c_msg_type__error );
      it_error.clear_error_stack;
      RAISE;

  END RebuildOneIndex;

  /**
   @brief    Параллельный REBUILD индексов.
             Нужно пересоздать два индекса, поэтому запускается два потока.
   @param[in]  p_TableName     	имя таблицы
  */
  PROCEDURE IndexParallel ( p_TableName IN varchar2 )
  IS
    x_ParaLevel NUMBER := 2;
    x_ChunkSql VARCHAR2(2000);
    x_SqlStmt VARCHAR2(2000);
    x_StartTime pls_integer;
  BEGIN
    x_StartTime := dbms_utility.get_time;

    -- Сообщение о начале процедуры
    it_log.log( p_msg => 'p_TableName: '||p_TableName||', p_ParaLevel: '||x_ParaLevel, p_msg_type => it_log.c_msg_type__debug );

    -- выражение для определения чанков
    x_ChunkSql := 'SELECT level, level FROM DUAL CONNECT BY LEVEL <= '||x_ParaLevel;

    -- выражение для процедуры параллельного исполнения
    x_SqlStmt := q'[
       DECLARE
         x_StartID number := :start_id ; x_EndID number := :end_id; 
       BEGIN
         OPRDOCS_utl.RebuildOneIndex( x_StartID, ']'||p_TableName||q'[' );
       EXCEPTION
           when others then
             it_error.put_error_in_stack;
             it_log.log( p_msg => 'Err: '||SQLERRM, p_msg_type => it_log.c_msg_type__error );
             it_error.clear_error_stack;
             RAISE;
       END;
    ]';

    -- запуск параллельного процесса
    it_parallel_exec.run_task_chunks_by_sql ( 
       p_parallel_level => x_ParaLevel
       , p_chunk_sql => x_ChunkSql
       , p_sql_stmt => x_SqlStmt 
    );

    -- Сообщение о завершении процедуры
    it_log.log( p_msg => 'End: '||ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );
  END IndexParallel;

  /**
   @brief    Нитка параллельной генерации заданий для последующего параллельного копирования.
   @param[in]  p_ParaID     	номер параллельного процесса
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
   @param[in]  p_ChunkSize     	размер порции
   @param[in]  p_SrcTable     	имя таблицы-источника
   @param[in]  p_DstTable     	имя таблицы-приемника
  */
  PROCEDURE GenParallelProc ( p_ParaID IN NUMBER, p_CalcID IN VARCHAR2, p_ChunkSize IN NUMBER, p_SrcTable IN varchar2, p_DstTable IN varchar2 )
  IS
    x_Prefix VARCHAR2(64) := '('||to_char(p_ParaID)||', SessionID: '||TO_CHAR (USERENV ('sessionid'))||')';
    x_StartTime pls_integer;
    x_PartitionName VARCHAR2(64);
    x_CompressFlag NUMBER := 0;
  BEGIN
    x_StartTime := dbms_utility.get_time;

    it_log.log( p_msg => x_Prefix||', CalcID: '||p_CalcID||', p_ChunkSize: '||p_ChunkSize, p_msg_type => it_log.c_msg_type__debug );

    LOOP
       x_PartitionName := OPRDOCS_utl.GetReadyPartition();        		-- получаем партицию
       EXIT WHEN x_PartitionName IS NULL;          		            	-- завершаем обработку, если нет партиции
       it_log.log( p_msg => to_char(p_ParaID)||': x_PartitionName: '||x_PartitionName, p_msg_type => it_log.c_msg_type__debug  );
       GenPartitionChunks ( p_CalcID, x_PartitionName, p_ChunkSize, p_SrcTable, p_DstTable );
       EndProcess ( x_PartitionName, x_CompressFlag ); 				-- отметка о завершении обработки партиции
       COMMIT;
    END LOOP;

    -- Сообщение о завершении процедуры
    it_log.log( p_msg => x_Prefix||', CalcID: '||p_CalcID||', End: '||ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );

  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log( p_msg => x_Prefix||', Err: '||SQLERRM, p_msg_type => it_log.c_msg_type__error );
      it_error.clear_error_stack;
      RAISE;

  END GenParallelProc;

  /**
   @brief    Генератор заданий для параллельного копирования.
             Возвращает itt_parallel_exec.calc_id
             Задания также генерятся несколькими потоками.

   @param[in]  p_CalcID     	количество параллельных процессов
   @param[in]  p_ParaLevel     	количество параллельных процессов
   @param[in]  p_ChunkSize     	размер порции
   @param[in]  p_SrcTable     	имя таблицы-источника
   @param[in]  p_DstTable     	имя таблицы-приемника
  */
  PROCEDURE GenParallel ( p_CalcID IN varchar2, p_ParaLevel IN NUMBER, p_ChunkSize IN number, p_SrcTable IN varchar2, p_DstTable IN varchar2 )
  IS
    x_ChunkSql VARCHAR2(2000);
    x_SqlStmt VARCHAR2(2000);
    x_StartTime pls_integer;
  BEGIN
    x_StartTime := dbms_utility.get_time;

    -- Сообщение о начале процедуры
    it_log.log( p_msg => 'p_CalcID: '||p_CalcID||', p_ParaLevel: '||p_ParaLevel||', p_ChunkSize: '||p_ChunkSize, p_msg_type => it_log.c_msg_type__debug );

    -- так как при формировании чанков, используется управляющая таблица с партициями, то нужно предварительно
    -- выполнить очистку
    UPDATE dtransfercontrol_dbt r SET r.t_status = 'R', r.t_started = null, r.t_ended = null;
    COMMIT;

    -- выражение для определения чанков
    x_ChunkSql := 'SELECT level, level FROM DUAL CONNECT BY LEVEL <= '||p_ParaLevel;

    -- выражение для процедуры параллельного исполнения
    x_SqlStmt := q'[
       DECLARE
         x_StartID number := :start_id ; x_EndID number := :end_id;
       BEGIN
         OPRDOCS_utl.GenParallelProc( x_StartID, ']'||p_CalcID||q'[', ]'||p_ChunkSize||q'[, ']'||p_SrcTable||q'[', ']'||p_DstTable||q'[' );
       EXCEPTION
           when others then
             it_error.put_error_in_stack;
             it_log.log( p_msg => 'Err: '||SQLERRM, p_msg_type => it_log.c_msg_type__error );
             it_error.clear_error_stack;
             RAISE;
       END;
    ]';

    -- запуск параллельного процесса
    it_parallel_exec.run_task_chunks_by_sql ( 
       p_parallel_level => p_ParaLevel
       , p_chunk_sql => x_ChunkSql
       , p_sql_stmt => x_SqlStmt 
    );

    -- Сообщение о завершении процедуры
    it_log.log( p_msg => 'End: '||ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );

  END GenParallel;

  /**
   @brief    Завершение calc_id
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
  */
  PROCEDURE EndParallelExec ( p_CalcID IN varchar2 )
  IS
  BEGIN
    it_parallel_exec.clear_calc(p_id => p_CalcID);
  END EndParallelExec;

  /**
   @brief    Процедура переименования таблицы
   @param[in]  p_TableName     	имя исходной таблицы
   @param[in]  p_NewName     	новое имя таблицы
  */
  PROCEDURE RenameTable ( p_TableName IN varchar2, p_NewName IN varchar2 )
  IS
  BEGIN
      it_log.log(
         p_msg => 'Переименование таблицы '||p_TableName||' в '||p_NewName
         , p_msg_type => it_log.c_msg_type__debug
      );
      EXECUTE IMMEDIATE 'ALTER TABLE '||p_TableName||' RENAME TO '||p_NewName;
  EXCEPTION
    WHEN others THEN
      it_log.log( p_msg => 'Ошибка переименования', p_msg_type => it_log.c_msg_type__debug );
  END RenameTable;

  /**
   @brief    Подготовка тестовых данных
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
  */
  PROCEDURE PrepareTest ( p_CalcID IN varchar2, p_Limit IN number )
  IS
  BEGIN
    it_log.log( p_msg => 'Подготовка тестовых данных', p_msg_type => it_log.c_msg_type__debug );
    IF( p_Limit > 0 ) THEN
       -- переведем все задания в статус 'F' (обработано)
       EXECUTE IMMEDIATE 'UPDATE itt_parallel_exec partition (p'||p_CalcID||') r SET str02 = ''F'' ';
       -- вернем небольшую часть заданий в статус готовности к обработке
       EXECUTE IMMEDIATE 'UPDATE itt_parallel_exec partition (p'||p_CalcID||') r SET str02 = null WHERE rownum < :1 AND NUM05 < 300000' USING p_Limit;
       COMMIT;
    END IF;
  END PrepareTest;

  /**
   @brief    Проверка размера
   @param[in]  	p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
   @param[in]  	p_Limit     	кол-во заданий для обработки
   @param[out]	p_Expected   	Кол-во ожидаемых записей
   @param[out]	p_Processed   	Кол-во обработанных записей
  */
  FUNCTION CheckSize ( 
    p_CalcID IN varchar2
    , p_Limit IN number 
    , p_Expected OUT number
    , p_Processed OUT number
  ) 
    RETURN number
  IS
    x_Ret NUMBER;
    x_Sql VARCHAR2(2000);
  BEGIN
    x_Sql := 'SELECT sum(num04), sum(num05) FROM itt_parallel_exec partition (P'||p_CalcID||')';
    IF( p_Limit > 0 ) THEN
       -- частичная обработка заданий
       x_Sql := x_Sql||' WHERE str02 = ''S'' ';
    END IF;
    EXECUTE IMMEDIATE x_Sql INTO p_Processed, p_Expected;
    IF(p_Processed <> p_Expected) THEN
      x_Ret := 3; -- размер не сходится
    ELSIF( p_Limit > 0 ) THEN
      x_Ret := 0; -- размер сошелся при частичной обработке
    ELSIF( p_Expected < 300000000 ) THEN
      x_Ret := 4; -- слишком маленький размер
    ELSE
      x_Ret := 0; -- размер сошелся при полной обработке
    END IF;
    RETURN x_Ret;
  EXCEPTION
    WHEN others THEN
      RETURN 2; -- ошибка определения размера
  END CheckSize;

  /**
   @brief    Процедура удаления таблицы
   @param[in]  p_TableName     	наименование таблицы
  */
  PROCEDURE DropTable ( p_TableName IN varchar2 )
  IS
  BEGIN
      it_log.log(
         p_msg => 'Удаление таблицы '||p_TableName
         , p_msg_type => it_log.c_msg_type__debug
      );
      EXECUTE IMMEDIATE 'DROP TABLE '||p_TableName||' CASCADE CONSTRAINTS';
      it_log.log(
         p_msg => 'Удалена таблица '||p_TableName
         , p_msg_type => it_log.c_msg_type__debug
      );
  EXCEPTION
    WHEN others THEN
      it_log.log(
         p_msg => 'Ошибка удаления таблицы '||p_TableName
         , p_msg_type => it_log.c_msg_type__debug
      );
  END DropTable;

  /**
   @brief    Процедура фиксации результатов трансформации
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
  */
  PROCEDURE FixProcess ( p_CalcID IN varchar2, p_OldTable IN varchar2 )
  IS
    x_Sql VARCHAR2(2000);
    x_OldTable VARCHAR2(64);
  BEGIN
    it_log.log( p_msg => 'Фиксация результатов calc_id = '||p_CalcID, p_msg_type => it_log.c_msg_type__debug );
  
    -- Удаление старой таблицы
    DropTable( UPPER(p_OldTable) );
  
    -- Удаление заданий параллельного процесса
    it_parallel_exec.clear_calc( p_CalcID );

  END FixProcess;

  /**
   @brief    Процедура отката процесса трансформации
   @param[in]  	p_SwitchFlag    флаг переключения контекста (1 - до копирования, 2 - после копирования), 
                                если задан, то производит переименование таблицы p_SrcTable в таблицу '_OLD'
   @param[in]  	p_SrcTable     	имя исходной таблицы
   @param[in]  	p_DstTable     	имя таблицы-результата
  */
  PROCEDURE RollbackProcess ( p_SwitchFlag IN number, p_SrcTable IN varchar2, p_DstTable IN varchar2 )
  IS
    x_Sql VARCHAR2(2000);
    x_OldTable VARCHAR2(64) := p_SrcTable||'_OLD';
  BEGIN
    it_log.log( p_msg => 'Откат процесса', p_msg_type => it_log.c_msg_type__debug );

    -- если было переключение контекста, выполняется возврат контекста
    IF( p_SwitchFlag > 0 ) THEN
      RenameTable ( p_SrcTable, p_DstTable );  -- исходная таблица переименовывается в таблицу-результат
      RenameTable ( x_OldTable, p_SrcTable );  -- таблица_OLD переименовывается в исходную таблицу
    END IF;

    -- Очистка данных в таблице-приемнике
    it_log.log( p_msg => 'Очистка '||p_DstTable, p_msg_type => it_log.c_msg_type__debug );
    EXECUTE IMMEDIATE 'TRUNCATE TABLE '||p_DstTable;
  
  END RollbackProcess;

  /**
   @brief    Инициализация параллельного процесса.
             Параллельный процесс используется как для генерации заданий, так и для копирования данных
  */
  FUNCTION InitProcess RETURN varchar2
  IS
  BEGIN
    RETURN to_char( it_parallel_exec.init_calc() );
  END InitProcess;


  /**
   @brief    Процедура отключения индексов
   @param[in]  p_TableName     	Имя таблицы, у которой отключаются индексы 
  */
  PROCEDURE DisableIndexes( p_TableName IN varchar2 )
  IS
    x_IndexName VARCHAR2(64);
  BEGIN
    x_IndexName := p_TableName||'_IDX2';
    it_log.log( p_msg => 'Отключение индекса '||x_IndexName, p_msg_type => it_log.c_msg_type__debug );
    EXECUTE IMMEDIATE 'ALTER INDEX '||x_IndexName||' UNUSABLE';

    x_IndexName := p_TableName||'_IDX5';
    it_log.log( p_msg => 'Отключение индекса '||x_IndexName, p_msg_type => it_log.c_msg_type__debug );
    EXECUTE IMMEDIATE 'ALTER INDEX '||x_IndexName||' UNUSABLE';
  END DisableIndexes;

  /**
   @brief    Главная процедура трансформации данных OPRDOCS.
             Возвращает код результата: если 0 -- ok, если > 0 -- ошибка
   @param[in]   p_CalcID     	ID расчета (itt_parallel_exec.calc_id) для заданий параллельного копирования,
                                получается посредством вызова InitProcess()
   @param[in]  	p_SrcTable     	имя исходной таблицы
   @param[in]  	p_DstTable     	имя таблицы-результата
   @param[in]  	p_Limit     	кол-во заданий для обработки
   @param[in]  	p_SwitchFlag    флаг переключения контекста (1 - до копирования, 2 - после копирования), 
                                если задан, то производит переименование таблицы p_SrcTable в таблицу '_OLD'
   @param[in]  	p_FixFlag     	флаг фиксации результата
   @param[out]	p_Expected   	Кол-во ожидаемых записей
   @param[out]	p_Processed   	Кол-во обработанных записей
  */
  FUNCTION ExecProcess ( 
    p_CalcID IN VARCHAR2 
    , p_SrcTable IN varchar2
    , p_DstTable IN varchar2
    , p_Limit IN number
    , p_SwitchFlag IN number
    , p_FixFlag IN number
    , p_Expected OUT number
    , p_Processed OUT number
  ) 
    RETURN number
  IS
    x_StartTime pls_integer;
    x_SrcSave VARCHAR2(64) := p_SrcTable;  			-- запоминает имя таблицы-источника
    x_DstSave VARCHAR2(64) := p_DstTable;  			-- запоминает имя таблицы-приемника
    x_OldTable VARCHAR2(64) := p_SrcTable||'_OLD'; 
    x_SrcTable VARCHAR2(64) := p_SrcTable;  			-- таблица-источник
    x_DstTable VARCHAR2(64) := p_DstTable;  			-- таблица-приемник
    x_ParaLevel NUMBER := 12;
    x_ChunkSize NUMBER := 300000;
    x_FixFlag NUMBER := p_FixFlag;
    x_Ret NUMBER;
  BEGIN
    p_Expected := 0;
    p_Processed := 0;

    -- Сообщение о начале процедуры
    it_log.log( p_msg => 'p_SrcTable: '||p_SrcTable||', p_DstTable: '||p_DstTable, p_msg_type => it_log.c_msg_type__debug );

    -- BOSS-7698_BOSS-8204 Заполнять управляющую таблицу необходимо перед началом процесса
    fillDTransferControlDbt( p_CalcID );

    -- Переключение контекста 1
    IF( p_SwitchFlag = 1 ) THEN
      RenameTable ( p_SrcTable, x_OldTable );  -- исходная таблица переименовывается в таблицу с суффиксом '_OLD'
      RenameTable ( p_DstTable, p_SrcTable );  -- таблица-результат переименовывается в исходную таблицу
      -- Определение таблиц источника и приемника
      x_SrcTable := x_OldTable;	-- таблица-источник -- это бывшая исходная таблица (нынешняя '_OLD')
      x_DstTable := p_SrcTable;   -- таблица-приемник -- это таблица-результата (нынешний 'OPRDOCS')
    END IF;

    -- Отключение индексов
    DisableIndexes( x_DstSave );

    -- Отключение триггеров
    it_log.log( p_msg => 'Отключение триггеров таблицы '||x_DstSave, p_msg_type => it_log.c_msg_type__debug );
    EXECUTE IMMEDIATE 'ALTER TABLE '||x_DstSave||' DISABLE ALL TRIGGERS';

    -- Генерация заданий
    GenParallel(p_CalcID, x_ParaLevel, x_ChunkSize, x_SrcTable, x_DstTable);

    -- Создание суррогатов
    CreateSurrogates( x_SrcTable, x_DstTable );

    -- Подготовка тестовых данных
    IF( p_Limit > 0 ) THEN
      x_FixFlag := 0; -- для частичного копирования фиксация не производится.
      PrepareTest( p_CalcID, p_Limit );
    END IF;

    -- Копирование 
    CopyParallel( p_CalcID, x_ParaLevel, 0 );

    -- Проверка размера
    x_Ret := CheckSize( p_CalcID, p_Limit, p_Expected, p_Processed );

    -- Индексация
    IndexParallel ( x_DstSave );

    -- Включение триггеров
    it_log.log( p_msg => 'Включение триггеров таблицы '||x_DstSave, p_msg_type => it_log.c_msg_type__debug );
    EXECUTE IMMEDIATE 'ALTER TABLE '||x_DstSave||' ENABLE ALL TRIGGERS';

    -- Переключение контекста 2
    IF( p_SwitchFlag = 2 ) THEN
      RenameTable ( x_SrcTable, x_OldTable );  -- исходная таблица переименовывается в таблицу с суффиксом '_OLD'
      RenameTable ( x_DstTable, p_SrcTable );  -- таблица-результат переименовывается в исходную таблицу
    END IF;

    -- Фиксация результата
    IF((x_Ret = 0) AND (x_FixFlag = 1)) THEN
       FixProcess ( p_CalcID, x_OldTable );
    END IF;

    -- Если процесс неудачный, выполняем откат
    IF(x_Ret > 0) THEN
       RollbackProcess ( p_SwitchFlag, x_SrcSave, x_DstSave );
    END IF;

    -- Сообщение о завершении процедуры
    it_log.log( p_msg => 'End: '||ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );

    RETURN x_Ret;
  EXCEPTION
    WHEN others THEN
      it_log.log(
         p_msg => 'ERR: '||SQLERRM
         , p_msg_type => it_log.c_msg_type__debug
      );
      RETURN 1;
  END ExecProcess;

END OPRDOCS_utl;
/
