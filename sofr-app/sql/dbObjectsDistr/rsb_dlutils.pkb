/* Formatted on 20.03.2024 11:02:04 (QP5 v5.149.1003.31008) */
CREATE OR REPLACE PACKAGE BODY RSB_DLUTILS
IS
   FUNCTION GETSESSIONID
      RETURN INTEGER
   AS
      T_SESSIONID   INTEGER;
   BEGIN
      SELECT SYS_CONTEXT ('USERENV', 'SESSIONID') INTO T_SESSIONID FROM DUAL;

      RETURN T_SESSIONID;
   END;

   PROCEDURE ParallelUpdateExecute (
      p_tableName        IN VARCHAR2,
      p_sql_main_stmt    IN VARCHAR2,
      p_sql_where_stmt   IN VARCHAR2,
      p_parallel_level   IN NUMBER,
      p_chunk_size       IN NUMBER DEFAULT C_CHUNK_SIZE)
   IS
      l_task            VARCHAR2 (128) := '';
      l_sql_stmt        VARCHAR2 (32767);
      v_schema          VARCHAR2 (32767);
      l_try             NUMBER;
      l_status          NUMBER;
      v_chunk_size      NUMBER (10) := p_chunk_size;
      v_Cnt             NUMBER (10) := 0;
      p_error_code      NUMBER (10);
      p_error_message   VARCHAR2 (32767);
   BEGIN
      l_sql_stmt :=
            'select count(*) from '
         || p_tableName
         || ' where '
         || p_sql_where_stmt;

      EXECUTE IMMEDIATE l_sql_stmt INTO v_Cnt;

      IF v_Cnt = 0
      THEN
         RETURN;
      END IF;

      IF (v_chunk_size IS NULL)
      THEN
         v_chunk_size := CEIL (v_Cnt / p_parallel_level);
      END IF;

      -- генерация уникального имени задания
      SELECT DBMS_PARALLEL_EXECUTE.generate_task_name INTO l_task FROM DUAL;

      -- получаем имя текущей схемы
      SELECT username INTO v_schema FROM user_users;

      -- создание задания
      DBMS_PARALLEL_EXECUTE.CREATE_TASK (l_task);

      -- генерируем способ отбора записей по p_chunk_size на поток
      DBMS_PARALLEL_EXECUTE.
       create_chunks_by_rowid (task_name     => l_task,
                               table_owner   => v_schema,
                               table_name    => p_tableName,
                               by_row        => TRUE,
                               chunk_size    => v_chunk_size);

      -- выполняем DML в многопотоке
      l_sql_stmt :=
            p_sql_main_stmt
         || ' WHERE '
         || p_sql_where_stmt
         || ' AND ROWID BETWEEN :start_id AND :end_id';

      DBMS_PARALLEL_EXECUTE.RUN_TASK (task_name        => l_task,
                                      sql_stmt         => l_sql_stmt,
                                      language_flag    => DBMS_SQL.NATIVE,
                                      parallel_level   => p_parallel_level);

      L_try := 0;
      L_status := DBMS_PARALLEL_EXECUTE.TASK_STATUS (l_task);

      WHILE (l_try < 2 AND L_status != DBMS_PARALLEL_EXECUTE.FINISHED)
      LOOP
         L_try := l_try + 1;
         DBMS_PARALLEL_EXECUTE.RESUME_TASK (l_task);
         L_status := DBMS_PARALLEL_EXECUTE.TASK_STATUS (l_task);
      END LOOP;

      BEGIN
         SELECT ERROR_CODE, ERROR_MESSAGE
           INTO p_error_code, p_error_message
           FROM user_parallel_execute_chunks
          WHERE     task_name = l_task
                AND ERROR_CODE IS NOT NULL
                AND ERROR_CODE <> 0
                AND ROWNUM = 1;

         raise_application_error (
            -20000,
            'One of chunk throw error: ' || p_error_message);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            NULL;
      END;

      -- завершена обработка, удаляем задачу
      DBMS_PARALLEL_EXECUTE.DROP_TASK (l_task);
   END ParallelUpdateExecute;

   PROCEDURE RunFunctionInParallel (p_ExecStr           IN VARCHAR2,
                                    p_chunksStatement   IN VARCHAR2)
   AS
      l_task_name       VARCHAR2 (30);
      l_try             NUMBER;
      l_status          NUMBER;
      l_stmt            CLOB;
      p_error_code      NUMBER (10);
      p_error_message   VARCHAR2 (32767);
   BEGIN
      l_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;
      DBMS_PARALLEL_EXECUTE.create_task (task_name => l_task_name);

      l_stmt := p_chunksStatement;
      DBMS_PARALLEL_EXECUTE.
       create_chunks_by_sql (task_name   => l_task_name,
                             sql_stmt    => l_stmt,
                             by_rowid    => FALSE);

      DBMS_PARALLEL_EXECUTE.
       run_task (task_name        => l_task_name,
                 sql_stmt         => p_ExecStr,
                 language_flag    => DBMS_SQL.NATIVE,
                 parallel_level   => RSB_DLUTILS.PARALLEL_LEVEL);

      l_try := 0;
      l_status := DBMS_PARALLEL_EXECUTE.task_status (l_task_name);

      WHILE (l_try < 2 AND l_status != DBMS_PARALLEL_EXECUTE.FINISHED)
      LOOP
         l_try := l_try + 1;
         DBMS_PARALLEL_EXECUTE.resume_task (l_task_name);
         l_status := DBMS_PARALLEL_EXECUTE.task_status (l_task_name);
      END LOOP;

      BEGIN
         SELECT ERROR_CODE, ERROR_MESSAGE
           INTO p_error_code, p_error_message
           FROM user_parallel_execute_chunks
          WHERE     task_name = l_task_name
                AND ERROR_CODE IS NOT NULL
                AND ERROR_CODE <> 0
                AND ROWNUM = 1;

         raise_application_error (
            -20000,
            'One of chunk throw error: ' || p_error_message);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            NULL;
      END;

      DBMS_PARALLEL_EXECUTE.drop_task (l_task_name);
   END;
END;
/