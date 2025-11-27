CREATE OR REPLACE PACKAGE RSB_DLUTILS
IS
    C_CHUNK_SIZE   NUMBER (10) := NULL; --по умолчанию количество записей на поток

    PARALLEL_LEVEL NUMBER (10) := 8; --по умолчанию количество потоков

    FUNCTION GETSESSIONID RETURN INTEGER;

    PROCEDURE ParallelUpdateExecute (
        p_tableName        IN VARCHAR2,
        p_sql_main_stmt    IN VARCHAR2,
        p_sql_where_stmt   IN VARCHAR2,
        p_parallel_level   IN NUMBER,
        p_chunk_size       IN NUMBER DEFAULT C_CHUNK_SIZE);
        
    PROCEDURE RunFunctionInParallel(p_ExecStr IN VARCHAR2, p_chunksStatement IN VARCHAR2);
END;
/