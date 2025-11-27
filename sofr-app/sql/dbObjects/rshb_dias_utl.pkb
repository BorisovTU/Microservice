CREATE OR REPLACE PACKAGE BODY rshb_dias_utl IS

  --Encoding: Win 866

  /**
   @file    rshb_dias_utl.pkb
   @brief     Утилиты для трансформации данных таблицы SOFR_DIASDEPORESTFULL (по DEF-61499)
     
   # changeLog
   |date       |author         |tasks                                                     |note                                                        
   |-----------|---------------|----------------------------------------------------------|-------------------------------------------------------------
   |2024.02.15 |Велигжанин А.В.|DEF-61499                                                 | Создание                  
    
  */


  /**
   @brief    Функция для журналирования времени выполнения.
  */
  FUNCTION ElapsedTime ( p_time IN pls_integer ) return varchar2 
  IS
  BEGIN
    RETURN to_char((dbms_utility.get_time - p_time) / 100, 'fm9999999990D00');
  END ElapsedTime;

  /**
   @brief    Функция для получения даты для обработки
  */
  FUNCTION GetReadyDate RETURN date 
  IS
    x_Date DATE;
    x_Ret NUMBER := 0;
    pragma autonomous_transaction;
  BEGIN
    UPDATE DDIASREPDATES_DBT r 
      SET r.t_status = 'P', r.t_started = SYSDATE 
      WHERE r.t_status = 'R' AND rownum = 1 
      RETURNING r.reportdate 
      INTO x_Date
    ;
    IF( SQL%ROWCOUNT <> 1) THEN
      x_Date := NULL;
    END IF;
    COMMIT;
    RETURN x_Date;
  EXCEPTION
    WHEN others THEN
      ROLLBACK;
      RETURN NULL;
  END;

  /**
   @brief    Процедура переноса данных для одного дня
  */
  PROCEDURE MoveOneDate ( p_ChunkID IN NUMBER, p_Date IN date )
  IS
    x_Prefix VARCHAR2(64) := '('||to_char(p_ChunkID)||', '||to_char (p_Date, 'DD.MM.YYYY')||')';
    x_StartTime pls_integer;
    x_Count NUMBER;
  BEGIN
    x_StartTime := dbms_utility.get_time;

    -- Сообщение о начале процедуры
    it_log.log(
      p_msg => x_Prefix||', Start'      
      , p_msg_type => it_log.c_msg_type__debug
    );

    EXECUTE IMMEDIATE 'INSERT INTO DDIASRESTDEPO_DBT (
      recid, accdepoid, reportdate, isin, value, t_timestamp
      )
    SELECT
      a.recid, a.accdepoid, a.reportdate, b.t_id AS isin, a.value, a.t_timestamp 
    FROM (
       SELECT
         r.recid, r.accdepoid, r.reportdate, r.isin, r.value, r.t_timestamp 
       FROM (
       SELECT
          last_value(r.recid) IGNORE NULLS over (partition by r.t_sofraccid, r.isin
                                    order by r.t_timestamp
                                    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) as recid
          , r.t_sofraccid AS accdepoid
          , r.reportdate
          , r.isin
          , last_value(r.value) IGNORE NULLS over (partition by r.t_sofraccid, r.isin
                                    order by r.t_timestamp
                                    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) as value
          , last_value(r.t_timestamp) IGNORE NULLS over (partition by r.t_sofraccid, r.isin
                                    order by r.t_timestamp
                                    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) as t_timestamp
          FROM (
                SELECT r.recid, r.accdepoid, r.reportdate, r.isin, r.value, r.t_timestamp, m.t_sofraccid
                FROM sofr_diasdeporestfull r
                JOIN DDIASACCMAP_DBT m ON (m.t_diasaccid = r.accdepoid)
                WHERE r.reportdate = :1
               ) r
          ) r 
          GROUP BY r.recid, r.accdepoid, r.reportdate, r.isin, r.value, r.t_timestamp) a
          JOIN DDIASISIN_DBT b ON (b.t_isin = a.isin)
       '
       USING p_Date
    ;
    x_Count := SQL%ROWCOUNT;
    COMMIT;

    -- Сообщение о завершении процедуры
    it_log.log(
      p_msg => x_Prefix||', End: '||ElapsedTime(x_StartTime)||', rows: '||to_char(x_Count)
      , p_msg_type => it_log.c_msg_type__debug
    );

  END;

  /**
   @brief    отметка о завершении обработки даты
  */
  PROCEDURE EndProcess ( p_Date IN DATE )
  IS
    pragma autonomous_transaction;
  BEGIN
    UPDATE DDIASREPDATES_DBT r 
      SET r.t_status = 'S', r.t_ended = SYSDATE 
      WHERE r.reportdate  = p_Date
    ;
    COMMIT;
  EXCEPTION
    WHEN others THEN
      ROLLBACK;
  END;

  /**
   @brief    Нитка параллельного выполнения.
  */
  PROCEDURE MoveDiasRestChunk ( p_StartID IN NUMBER, p_EndID IN NUMBER )
  IS
    x_Prefix VARCHAR2(64) := '('||to_char(p_StartID)||')';
    x_StartTime pls_integer;
    x_Date DATE;
  BEGIN
    x_StartTime := dbms_utility.get_time;

    -- Сообщение о начале процедуры
    it_log.log(
      p_msg => x_Prefix||', Start '||', SessionID: '||TO_CHAR (USERENV ('sessionid'))
      , p_msg_type => it_log.c_msg_type__debug
    );

    -- Цикл, пока есть даты для обработки
    LOOP
       x_Date := GetReadyDate();                   -- получаем дату
       EXIT WHEN x_Date IS NULL;                   -- завершаем обработку, если даты нет
       MoveOneDate ( p_StartID, x_Date );          -- переносим данные за дату
       EndProcess ( x_Date );                      -- отметка о завершении обработки даты
    END LOOP;

    -- Сообщение о завершении процедуры
    it_log.log(
      p_msg => x_Prefix||', End: '||ElapsedTime(x_StartTime)
      , p_msg_type => it_log.c_msg_type__debug
    );

  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'Error', p_msg_type => it_log.c_msg_type__error);
      it_error.clear_error_stack;
      RAISE;

  END MoveDiasRestChunk;

  /**
   @brief    Процедура запуска параллельных процессов.
  */
  PROCEDURE ExecParallel (
     p_ExecStr IN VARCHAR2
     , p_parallel in number default 4
  )
  AS
     x_TaskName VARCHAR2 (30);
     x_Try NUMBER;
     x_Status NUMBER;
     x_Stmt CLOB;
  BEGIN
     x_TaskName := DBMS_PARALLEL_EXECUTE.generate_task_name;
     DBMS_PARALLEL_EXECUTE.create_task (task_name => x_TaskName);

     x_Stmt := 'SELECT level, level FROM DUAL CONNECT BY LEVEL <= '||p_parallel;
     DBMS_PARALLEL_EXECUTE.create_chunks_by_sql (task_name => x_TaskName, sql_stmt => x_Stmt, by_rowid => FALSE);

     DBMS_PARALLEL_EXECUTE.run_task (
        task_name => x_TaskName
        , sql_stmt => p_ExecStr
        , language_flag => DBMS_SQL.NATIVE
        ,  parallel_level => p_parallel
     );

     x_Try := 0;
     x_Status := DBMS_PARALLEL_EXECUTE.task_status (x_TaskName);

     WHILE (x_Try < 2 AND x_Status != DBMS_PARALLEL_EXECUTE.FINISHED) LOOP
        x_Try := x_Try + 1;
        DBMS_PARALLEL_EXECUTE.resume_task (x_TaskName);
        x_Status := DBMS_PARALLEL_EXECUTE.task_status (x_TaskName);
     END LOOP;

     DBMS_PARALLEL_EXECUTE.drop_task (x_TaskName);

  END ExecParallel;

  /**
   @brief    Перенос данных из таблицы SOFR_DIASDEPORESTFULL в DDIASRESTDEPO_DBT.
             В исходной таблице (SOFR_DIASDEPORESTFULL) содержатся избыточные данные.
             При переносе данных, производится перенос только последних значений за дату.
             Так как таблица SOFR_DIASDEPORESTFULL -- большого размера (~14 Gb на 13-02-2023),
             перенос данных осуществляется параллельно несколькими потоками.
  */
  PROCEDURE MoveDiasRest ( p_Parallel IN NUMBER DEFAULT 4 )
  IS
    x_StartTime pls_integer;
  BEGIN
    x_StartTime := dbms_utility.get_time;

    -- Сообщение о начале процедуры
    it_log.log(
       p_msg => 'Start'
       , p_msg_type => it_log.c_msg_type__debug
    );

    -- Запуск параллельных процессов
    ExecParallel (
       'BEGIN rshb_dias_utl.MoveDiasRestChunk(:start_id, :end_id); END;'
       , p_Parallel
    );

    -- Сообщение о завершении процедуры
    it_log.log(
       p_msg => 'End. Total time: ' || ElapsedTime(x_StartTime)
       , p_msg_type => it_log.c_msg_type__debug
    );

  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'Error', p_msg_type => it_log.c_msg_type__error);
      it_error.clear_error_stack;
      RAISE;

  END MoveDiasRest;

  /**
   @brief    Процедура переименования таблицы
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
      it_log.log(
         p_msg => 'Ошибка переименования'
         , p_msg_type => it_log.c_msg_type__debug
      );
  END;

  /**
   @brief    Процедура создания представления
  */
  PROCEDURE CreateView ( p_ViewName IN varchar2, p_ViewText IN varchar2 )
  IS
  BEGIN
    it_log.log(
       p_msg => 'Создание представления '||p_ViewName
       , p_msg_type => it_log.c_msg_type__debug
    );
    EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW '||p_ViewName||' '||p_ViewText;
  EXCEPTION
    WHEN others THEN
      it_log.log(
         p_msg => 'Ошибка создания представления'
         , p_msg_type => it_log.c_msg_type__debug
      );
  END;

  /**
   @brief    Процедура удаления представления
  */
  PROCEDURE DropView ( p_ViewName IN varchar2 )
  IS
  BEGIN
      it_log.log(
         p_msg => 'Удаление представления '||p_ViewName
         , p_msg_type => it_log.c_msg_type__debug
      );
      EXECUTE IMMEDIATE 'DROP VIEW '||p_ViewName;
      it_log.log(
         p_msg => 'Удалено представление '||p_ViewName
         , p_msg_type => it_log.c_msg_type__debug
      );
  EXCEPTION
    WHEN others THEN
      it_log.log(
         p_msg => 'Ошибка удаления представления '||p_ViewName
         , p_msg_type => it_log.c_msg_type__debug
      );
  END;

  /**
   @brief    Процедура удаления таблицы
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
   @brief    Процедура изменения триггера
  */
  PROCEDURE ReplaceTrigger ( p_TrgName IN varchar2, p_TrgText IN varchar2 )
  IS
  BEGIN
      it_log.log( p_msg => 'Изменение триггера '||p_TrgName, p_msg_type => it_log.c_msg_type__debug );
      EXECUTE IMMEDIATE 'CREATE OR REPLACE TRIGGER '||p_TrgName||' '||p_TrgText;
      it_log.log( p_msg => 'Создан тригер '||p_TrgName, p_msg_type => it_log.c_msg_type__debug );
  EXCEPTION
    WHEN others THEN
      it_log.log( p_msg => 'Ошибка изменения триггера '||p_TrgName, p_msg_type => it_log.c_msg_type__debug );
  END;

  /**
   @brief    Процедура переключения представлений.
   @param    p_Type Вид представления: 0 -- старый, 1 -- новый
  */
  PROCEDURE SwitchView ( p_Type IN NUMBER )
  IS
    x_ObjectType VARCHAR2(32);
    x_Str VARCHAR2(32000);
    cr VARCHAR2(2) := CHR(10);  -- перевод строки
    ct VARCHAR2(2) := CHR(9); -- табуляция
  BEGIN
    SELECT r.OBJECT_TYPE INTO x_ObjectType FROM user_objects r WHERE r.OBJECT_NAME = 'SOFR_DIASDEPORESTFULL' AND ROWNUM = 1;
    IF ( p_Type = 0 ) THEN
      -- переключить на старый набор представлений ( таблицы SOFR_DIASDEPORESTFULL и SOFR_DIASACCDEPOFULL )
      it_log.log(
         p_msg => 'Переключение на старый набор представлений '
         , p_msg_type => it_log.c_msg_type__debug
      );
      IF(x_ObjectType = 'TABLE') THEN
        -- уже используется старый набор представлений.
        -- ничего больше не делаем
        it_log.log(
           p_msg => 'Уже используется старый набор представлений '
           , p_msg_type => it_log.c_msg_type__debug
        );
        RETURN ; 
      END IF;

      DropView ( 'SOFR_DIASACCDEPOFULL' );
      DropView ( 'SOFR_DIASDEPORESTFULL' );
      RenameTable( 'SOFR_DIASACCDEPOFULL_OLD', 'SOFR_DIASACCDEPOFULL' );
      RenameTable( 'SOFR_DIASDEPORESTFULL_OLD', 'SOFR_DIASDEPORESTFULL' );

      x_Str := cr||'AFTER INSERT ON SOFR_SVERKAACCDEPOIN '
  ||cr||'REFERENCING NEW AS New OLD AS Old '
  ||cr||'FOR EACH ROW '
  ||cr||'DECLARE '
  ||cr||'BEGIN '
  ||cr||ct||'insert into SOFR_DIASACCDEPOFULL (RECID,  DEPONUMBER ,  ACCDEPONUMBER ,  SECTIONCODE ,  CONTRACTNUMBER ,  T_TIMESTAMP ) '
  ||cr||ct||'values (:new.RECID,  :new.DEPONUMBER ,  :new.ACCDEPONUMBER ,  :new.SECTIONCODE ,  :new.CONTRACTNUMBER ,  SYSTIMESTAMP); '
  ||cr||'EXCEPTION '
      ||cr||ct||'WHEN OTHERS THEN '
        ||cr||ct||ct||'it_error.put_error_in_stack; '
        ||cr||ct||ct||'it_log.log(p_msg => ''Error'', p_msg_type => it_log.c_msg_type__error); '
        ||cr||ct||ct||'it_error.clear_error_stack; '
        ||cr||ct||ct||'RAISE; '
        ||cr||'END;';
      ReplaceTrigger ( 'SOFR_SVERKAACCDEPOIN_HIST_AIR', x_Str);

      x_Str := cr||'AFTER INSERT ON SOFR_SVERKARESTDEPOIN '
  ||cr||'REFERENCING NEW AS New OLD AS Old '
  ||cr||'FOR EACH ROW '
  ||cr||'DECLARE '
  ||cr||'BEGIN '
  ||cr||ct||'insert into SOFR_DIASDEPORESTFULL (RECID ,ACCDEPOID, REPORTDATE, ISIN, VALUE, T_TIMESTAMP ) '
  ||cr||ct||'values (:new.RECID,  :new.ACCDEPOID, :new.REPORTDATE, :new.ISIN, :new.VALUE,  SYSTIMESTAMP); '
  ||cr||'EXCEPTION '
      ||cr||ct||'WHEN OTHERS THEN '
        ||cr||ct||ct||'it_error.put_error_in_stack; '
        ||cr||ct||ct||'it_log.log(p_msg => ''Error'', p_msg_type => it_log.c_msg_type__error); '
        ||cr||ct||ct||'it_error.clear_error_stack; '
        ||cr||ct||ct||'RAISE; '
        ||cr||'END;';
      ReplaceTrigger ( 'SOFR_SVERKARESTDEPOIN_HIST_AIR', x_Str);

      it_log.log(
         p_msg => 'Произведено переключение на старый набор представлений '
         , p_msg_type => it_log.c_msg_type__debug
      );

    ELSE

      -- переключить на новый набор представлений ( вьюхи SOFR_DIASDEPORESTFULL и SOFR_DIASACCDEPOFULL )
      it_log.log(
         p_msg => 'Переключение на новый набор представлений '
         , p_msg_type => it_log.c_msg_type__debug
      );
      IF(x_ObjectType = 'VIEW') THEN
        -- уже используется новый набор представлений.
        -- ничего больше не делаем
        it_log.log(
           p_msg => 'Уже используется новый набор представлений '
           , p_msg_type => it_log.c_msg_type__debug
        );
        RETURN ; 
      END IF;

      RenameTable('SOFR_DIASACCDEPOFULL', 'SOFR_DIASACCDEPOFULL_OLD');
      RenameTable('SOFR_DIASDEPORESTFULL', 'SOFR_DIASDEPORESTFULL_OLD');
      CreateView('SOFR_DIASACCDEPOFULL'
         , '(
           RECID, DEPONUMBER, ACCDEPONUMBER, SECTIONCODE, CONTRACTNUMBER, T_TIMESTAMP
         )
         AS
         SELECT 
           a.t_sofraccid AS recid, a.t_deponumber AS deponumber, a.t_accdeponumber AS accdeponumber
           , a.t_sectioncode AS sectioncode, a.t_contractnumber AS contractnumber, a.t_timestamp
         FROM DDIASACCDEPO_DBT a'
      );
      CreateView('SOFR_DIASDEPORESTFULL'
         , '(
           RECID, ACCDEPOID, REPORTDATE, ISIN, VALUE, T_TIMESTAMP
         )
         AS
         SELECT 
           t.recid, t.accdepoid, t.reportdate, i.t_isin AS isin, t.value, t.t_timestamp
         FROM DDIASRESTDEPO_DBT t
         JOIN DDIASISIN_DBT i ON (i.t_id = t.isin)'
      );

      x_Str := cr||'AFTER INSERT ON SOFR_SVERKAACCDEPOIN '
  ||cr||'REFERENCING NEW AS New OLD AS Old '
  ||cr||'FOR EACH ROW '
  ||cr||'DECLARE '
    ||cr||ct||'x_TimeStamp DATE; '
    ||cr||ct||'x_SofrAccID NUMBER; '
    ||cr||ct||'x_DiasAccID NUMBER := :new.RECID; '
    ||cr||ct||'x_DepoNumber DDIASACCDEPO_DBT.t_deponumber%type := trim(:new.DEPONUMBER); '
    ||cr||ct||'x_AccDpoNumber DDIASACCDEPO_DBT.t_accdeponumber%type := trim(:new.ACCDEPONUMBER); '
    ||cr||ct||'x_SectionCode DDIASACCDEPO_DBT.t_sectioncode%type := trim(:new.SECTIONCODE); '
    ||cr||ct||'x_ContractNumber DDIASACCDEPO_DBT.t_contractnumber%type := trim(:new.CONTRACTNUMBER); '
  ||cr||'BEGIN '
    ||cr||ct||'SELECT SYSTIMESTAMP INTO x_TimeStamp FROM dual; '
    ||cr||ct||'-- пытаемся обновить timestamp таблицы счетов '
    ||cr||ct||'UPDATE DDIASACCDEPO_DBT r '
      ||cr||ct||ct||'SET r.t_timestamp = x_TimeStamp, r.t_lastid = x_DiasAccID '
      ||cr||ct||ct||'WHERE r.t_contractnumber = trim(x_ContractNumber) AND r.t_sectioncode = trim(x_SectionCode) '
      ||cr||ct||ct||'AND rownum = 1 '
      ||cr||ct||ct||'RETURNING r.t_sofraccid '
      ||cr||ct||ct||'INTO x_SofrAccID; '
    ||cr||ct||'-- если счета нет, добавляем '
    ||cr||ct||'IF( SQL%ROWCOUNT <> 1) THEN '
      ||cr||ct||ct||'INSERT INTO DDIASACCDEPO_DBT r ( '
        ||cr||ct||ct||ct||'r.t_deponumber, r.t_accdeponumber, r.t_sectioncode, r.t_contractnumber, r.t_timestamp, r.t_lastid '
      ||cr||ct||ct||') VALUES ( '
        ||cr||ct||ct||ct||'x_DepoNumber, x_AccDpoNumber, trim(x_SectionCode), trim(x_ContractNumber), x_TimeStamp, 0 '
      ||cr||ct||ct||') '
      ||cr||ct||ct||'RETURNING r.t_sofraccid '
      ||cr||ct||ct||'INTO x_SofrAccID; '
    ||cr||ct||'END IF; '
    ||cr||ct||'-- добавляем значение в таблицу маппинга '
    ||cr||ct||'INSERT INTO DDIASACCMAP_DBT r ( '
    ||cr||ct||ct||'r.t_sofraccid, r.t_diasaccid '
    ||cr||ct||') VALUES ( '
      ||cr||ct||ct||'x_SofrAccID, x_DiasAccID '
    ||cr||ct||'); '
  ||cr||'EXCEPTION '
      ||cr||ct||'WHEN OTHERS THEN '
        ||cr||ct||ct||'it_error.put_error_in_stack; '
        ||cr||ct||ct||'it_log.log(p_msg => ''Error'', p_msg_type => it_log.c_msg_type__error); '
        ||cr||ct||ct||'it_error.clear_error_stack; '
        ||cr||ct||ct||'RAISE; '
        ||cr||'END;';
      ReplaceTrigger ( 'SOFR_SVERKAACCDEPOIN_HIST_AIR', x_Str);

      x_Str := cr||'AFTER INSERT ON SOFR_SVERKARESTDEPOIN '
  ||cr||'REFERENCING NEW AS New OLD AS Old '
  ||cr||'FOR EACH ROW '
  ||cr||'DECLARE '
    ||cr||ct||'x_TimeStamp DATE; '
    ||cr||ct||'x_IsinID DDIASISIN_DBT.t_id%type; '
    ||cr||ct||'x_Isin DDIASISIN_DBT.t_isin%type := trim(:new.ISIN); '
    ||cr||ct||'x_DiasAccID DDIASACCMAP_DBT.t_Diasaccid%type := :new.ACCDEPOID; '
    ||cr||ct||'x_SofrAccID DDIASACCMAP_DBT.T_SofrAccID%type := -1; '
    ||cr||ct||'x_RecID DDIASRESTDEPO_DBT.recID%type := :new.RECID; '
    ||cr||ct||'x_Value DDIASRESTDEPO_DBT.value%type := :new.VALUE; '
    ||cr||ct||'x_ReportDate DDIASRESTDEPO_DBT.reportdate%type := :new.REPORTDATE; '
  ||cr||'BEGIN '
    ||cr||ct||'SELECT SYSTIMESTAMP INTO x_TimeStamp FROM dual; '
    ||cr||ct||'-- Анализируется ISIN. Полученное значение заменяется идентификатором. '
    ||cr||ct||'-- Если такого ISIN нет, производится добавление новой записи в таблицу. '
    ||cr||ct||'BEGIN '
      ||cr||ct||ct||'SELECT r.t_ID INTO x_IsinID FROM DDIASISIN_DBT r where r.t_isin = x_Isin; '
    ||cr||ct||'EXCEPTION '
      ||cr||ct||ct||'WHEN OTHERS THEN '
        ||cr||ct||ct||ct||'INSERT INTO DDIASISIN_DBT r ( t_isin ) VALUES ( x_Isin ) '
        ||cr||ct||ct||ct||'RETURNING t_ID INTO x_IsinID; '
    ||cr||ct||'END;'
    ||cr||ct||'-- Производится поиск ACCDEPOID в таблице маппинга. '
    ||cr||ct||'BEGIN '
      ||cr||ct||ct||'SELECT r.t_sofraccid INTO x_SofrAccID '
      ||cr||ct||ct||'FROM DDIASACCMAP_DBT r where r.t_diasaccid = x_DiasAccID; '
    ||cr||ct||'EXCEPTION '
      ||cr||ct||ct||'WHEN OTHERS THEN '
        ||cr||ct||ct||ct||'-- этого не может быть '
        ||cr||ct||ct||ct||'it_log.log( '
        ||cr||ct||ct||ct||ct||'p_msg => ''Ошибка маппинга, x_DiasAccID: '' || to_char(x_DiasAccID) '
        ||cr||ct||ct||ct||ct||', p_msg_type => it_log.c_msg_type__debug '
        ||cr||ct||ct||ct||'); '
    ||cr||ct||'END; '
    ||cr||ct||'IF( x_SofrAccID = -1 ) THEN '
      ||cr||ct||ct||'RETURN ; '
    ||cr||ct||'END IF; '
    ||cr||ct||'-- Производится попытка изменения данных в таблице DDIASRESTDEPO_DBT '
    ||cr||ct||'UPDATE DDIASRESTDEPO_DBT r '
      ||cr||ct||ct||'SET r.t_timestamp = x_TimeStamp, r.recID = x_RecID, r.value = x_Value '
      ||cr||ct||ct||'WHERE r.reportdate = x_ReportDate AND r.accdepoid = x_SofrAccID AND r.isin = x_IsinID '
      ||cr||ct||ct||'AND rownum = 1 '
    ||cr||ct||'; '
    ||cr||ct||'-- Неудачная попытка изменения данных означает то, что данные являются новыми (для счета, reportdate и ISIN), '
    ||cr||ct||'-- поэтому (при неудачном изменении) производится вставка записи в таблице остатков. '
    ||cr||ct||'-- если счета нет, добавляем '
    ||cr||ct||'IF( SQL%ROWCOUNT <> 1) THEN '
      ||cr||ct||ct||'INSERT INTO DDIASRESTDEPO_DBT r ( '
        ||cr||ct||ct||ct||'r.recid, r.accdepoid, r.reportdate, r.isin, r.value, r.t_timestamp '
      ||cr||ct||ct||') VALUES ( '
        ||cr||ct||ct||ct||'x_RecID, x_SofrAccID, x_ReportDate, x_IsinID, x_Value, x_TimeStamp '
      ||cr||ct||ct||'); '
    ||cr||ct||'END IF; '
  ||cr||'EXCEPTION '
      ||cr||ct||'WHEN OTHERS THEN '
        ||cr||ct||ct||'it_error.put_error_in_stack; '
        ||cr||ct||ct||'it_log.log(p_msg => ''Error'', p_msg_type => it_log.c_msg_type__error); '
        ||cr||ct||ct||'it_error.clear_error_stack; '
        ||cr||ct||ct||'RAISE; '
        ||cr||'END;';
      ReplaceTrigger ( 'SOFR_SVERKARESTDEPOIN_HIST_AIR', x_Str);

      it_log.log(
         p_msg => 'Произведено переключение на новый набор представлений '
         , p_msg_type => it_log.c_msg_type__debug
      );
    END IF;

  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'Error', p_msg_type => it_log.c_msg_type__error);
      it_error.clear_error_stack;
      RAISE;

  END SwitchView;

  /**
   @brief    Фиксация доработки. Выполняет переключение на новые структуры данных и удаляет старые таблицы.
  */
  PROCEDURE FixView (p_Ret IN OUT NUMBER)
  IS
    x_ObjectType VARCHAR2(32);
    x_CountDates NUMBER;
    x_SuccessDates NUMBER;
  BEGIN
    -- если дойдем до конца, изменим статус на 0
    p_Ret := 1; 
    it_log.log( p_msg => 'Фиксация доработки. Проверка копирования данных', p_msg_type => it_log.c_msg_type__debug );
    -- Проверка, что обработаны все дни, которые нужно
    SELECT count(*) INTO x_CountDates FROM ddiasrepdates_dbt;
    SELECT count(*) INTO x_SuccessDates FROM ddiasrepdates_dbt r WHERE r.t_status = 'S';
    IF (x_CountDates < 400) THEN
       it_log.log( p_msg => 'Меньше 400 дней для обработки', p_msg_type => it_log.c_msg_type__debug);
       p_Ret := 2;
       RETURN ;
    ELSIF (x_CountDates <> x_SuccessDates) THEN 
       it_log.log( p_msg => 'Обработаны не все данные', p_msg_type => it_log.c_msg_type__debug);
       p_Ret := 3;
       RETURN ;
    END IF;

    -- Проверка пройдена, можно переключаться
    it_log.log( p_msg => 'Проверка пройдена. Прежние структуры будут удалены', p_msg_type => it_log.c_msg_type__debug );
    SELECT r.OBJECT_TYPE INTO x_ObjectType FROM user_objects r WHERE r.OBJECT_NAME = 'SOFR_DIASDEPORESTFULL' AND ROWNUM = 1;
    IF(x_ObjectType = 'TABLE') THEN
      -- используется старый набор представлений.
      -- производим переключение
      SwitchView(1);
    END IF;
    DropTable('SOFR_DIASDEPORESTFULL_OLD');
    DropTable('SOFR_DIASACCDEPOFULL_OLD');
    it_log.log( p_msg => 'Произведена фиксация доработки. Таблицы SOFR_DIASDEPORESTFULL удалены', p_msg_type => it_log.c_msg_type__debug );
    p_Ret := 0;
  EXCEPTION
    WHEN others THEN
      p_Ret := -1;
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'Error', p_msg_type => it_log.c_msg_type__error);
      it_error.clear_error_stack;
      RAISE;

  END FixView;

END rshb_dias_utl;
/
