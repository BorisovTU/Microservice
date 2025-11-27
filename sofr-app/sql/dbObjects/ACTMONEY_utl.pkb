CREATE OR REPLACE PACKAGE BODY ACTMONEY_utl IS

  --Encoding: Win 866

  /**
   @file 		ACTMONEY_utl.pkb
   @brief 		Утилиты для отчета-сверки ДС между БУ и ВУ
     
# changelog
 |date       |autor          |tasks                                           |note
 |-----------|---------------|------------------------------------------------|-------------------------------------------------------------
 |07.03.2025 |Велигжанин А.В.|DEF-84078                                       |закрытые счета в отчет не выводить
 |06.03.2025 |Велигжанин А.В.|DEF-83777                                       |доработка GetSfEdp(), для поиска суб-договора ЕДП 
 |           |               |                                                |сначала ищется по фондовому рынку, потом по валютному, потом по срочному
 |06.03.2025 |Велигжанин А.В.|DEF-83901                                       |ProcessSfContr(), исправление ошибки сбора данных
 |           |               |                                                |неверно инициализировалась дата, из-за чего
 |           |               |                                                |перебирались не все валюты
 |           |               |                                                |для ФЛ, нужно также собрать данные по вне-бирже
 |05.03.2025 |Велигжанин А.В.|DEF-83777                                       |доработка ProcessTask (), для ЕДП данные собираются только
 |           |               |                                                |по суб-договора фондового рынка
 |13.02.2025 |Велигжанин А.В.|BOSS-5882_BOSS-7917                             |CreateData(), формирование данных для отчета-сверки
 |06.02.2025 |Велигжанин А.В.|BOSS-5882_BOSS-7711                             |Создание                  

  */

  g_ParallelLimit NUMBER := 8;  -- макс. число параллельных потоков

  /**
   @brief    Функция для журналирования времени выполнения.
  */
  FUNCTION ElapsedTime ( p_time IN pls_integer ) return varchar2 
  IS
  BEGIN
    RETURN to_char((dbms_utility.get_time - p_time) / 100, 'fm9999999990D00');
  END ElapsedTime;

  /**
   @brief    Возвращает массив валют для договора
   @param[in]  	p_SfContrID    		ID договора
   @param[in]  	p_FIID 			ID финансового инструмента
  */
  FUNCTION GetCurArray ( p_SfContrID IN NUMBER, p_FIID IN NUMBER ) RETURN tt_Currency
  IS
    x_CurArray tt_Currency := tt_Currency();
  BEGIN
    SELECT t_currency 
      BULK COLLECT into x_CurArray 
      FROM dmcaccdoc_dbt mc 
      WHERE mc.t_clientcontrid = p_SfContrID and mc.t_catid = 70 
      and mc.t_currency = case when (p_FIID = -1) then mc.t_currency else p_FIID end 
      GROUP BY t_currency ORDER BY 1
    ;
    RETURN x_CurArray;
  END GetCurArray;

  /**
   @brief    Возвращает счет внутреннего учета (ВУ)
   @param[in]  	p_SfContrID    		ID договора
   @param[in]  	p_FIID 			ID финансового инструмента
  */
  FUNCTION GetInnerAcc ( 
    p_SfContrID IN NUMBER
    , p_FIID IN NUMBER
  ) 
  RETURN varchar2
  IS
    x_Acc varchar2(25);
  BEGIN
    SELECT mc.t_account INTO x_Acc
       FROM dsfcontr_dbt sf, dmcaccdoc_dbt mc 
       WHERE
         sf.t_id = p_SfContrID
       and mc.t_catID = 349 and mc.t_owner = sf.t_partyid and mc.t_clientcontrid = sf.t_id  -- индекс
       and mc.t_templnum = case when RSB_SECUR.GetGeneralMainObjAttr (659,LPAD (sf.t_id, 10, '0'), 102, to_date('31122999','ddmmyyyy')) = 1 then 6
                                when sf.t_servkind = 1 then 1 
                                when sf.t_servkind = 15 then 3 
                                when sf.t_servkind = 21 then 5 else mc.t_templnum end
       and mc.t_currency = p_FIID
       and mc.t_disablingdate = to_date('1-1-1', 'dd-mm-yyyy')
       and rownum = 1
    ;
    RETURN x_Acc;
  EXCEPTION
    WHEN others THEN
      RETURN NULL;
  END GetInnerAcc;

  /**
   @brief    Возвращает счет бухгалтерского учета (БУ)
   @param[in]  	p_SfContrID    		ID договора
   @param[in]  	p_FIID 			ID финансового инструмента
  */
  FUNCTION GetGbAcc ( 
    p_SfContrID IN NUMBER
    , p_FIID IN NUMBER
  ) 
  RETURN varchar2
  IS
    x_Acc varchar2(25);
  BEGIN
    SELECT mc.t_account INTO x_Acc
       FROM dsfcontr_dbt sf, dmcaccdoc_dbt mc 
       WHERE
         sf.t_id = p_SfContrID
       and mc.t_catID = 70 and mc.t_owner = sf.t_partyid and mc.t_clientcontrid = sf.t_id  -- индекс
       and mc.t_currency = p_FIID
       and mc.t_disablingdate = to_date('1-1-1', 'dd-mm-yyyy')
       and mc.t_iscommon = 'X'
       and rownum = 1
    ;
    RETURN x_Acc;
  EXCEPTION
    WHEN others THEN
      RETURN NULL;
  END GetGbAcc;

  /**
   @brief    Запуск параллельного процесса получения данных для отчета.
  */
  PROCEDURE ProcessParallel ( p_CalcID IN varchar2, p_ParaLevel IN NUMBER )
  IS
    x_ChunkSql VARCHAR2(2000);
    x_SqlStmt VARCHAR2(2000);
    x_StartTime pls_integer;
  BEGIN
    x_StartTime := dbms_utility.get_time;

    -- Сообщение о начале процедуры
    it_log.log( p_msg => 'p_CalcID: '||p_CalcID||', p_ParaLevel: '||p_ParaLevel, p_msg_type => it_log.c_msg_type__debug );

    -- выражение для определения чанков
    x_ChunkSql := 'SELECT level, level FROM DUAL CONNECT BY LEVEL <= '||p_ParaLevel;

    -- выражение для процедуры параллельного исполнения
    x_SqlStmt := q'[
       DECLARE
         x_StartID number := :start_id ; x_EndID number := :end_id; 
       BEGIN
         ACTMONEY_utl.ParallelProc( x_StartID, ']'||p_CalcID||q'[' );
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

  END ProcessParallel;

  /**
   @brief    Возвращает ID задания, пригодного для обработки (itt_parallel_exec.row_id)
             Также возвращаются реквизиты задания
   @param[in]  	p_CalcID     		ID расчета (itt_parallel_exec.calc_id)
   @param[out] 	p_BegDate       	начальная дата отчета
   @param[out] 	p_EndDate     		конечная дата отчета
   @param[out] 	p_PartyID     		ID клиента
   @param[out] 	p_SfContrID    		ID договора
   @param[out] 	p_FIID     		ID финансового инструмента
   @param[out] 	p_DiffFlag     		если 'X', то отчет показывает только различия, иначе всё
  */
  FUNCTION GetReadyTask ( 
    p_CalcID IN varchar2
    , p_BegDate OUT DATE
    , p_EndDate OUT DATE
    , p_PartyID OUT number
    , p_SfContrID OUT number
    , p_FIID OUT number
    , p_DiffFlag OUT varchar2 
  ) 
    RETURN number
  IS
    x_RowID NUMBER := 0;
    x_Sql CLOB;
    pragma autonomous_transaction;
  BEGIN
    x_Sql := 'UPDATE itt_parallel_exec partition (p'||p_CalcID||') r '
             ||' SET r.str02 = ''P'', r.dat03 = SYSDATE '
             ||' WHERE r.str02 is null AND rownum = 1 '
             ||' RETURNING r.row_id, r.dat01, r.dat02, r.num01, r.num02, r.num03, r.str01 '
             ||' INTO :x_RowID, :p_BegDate, :p_EndDate, :p_PartyID, :p_SfContrID, :p_FIID, :p_DiffFlag '
    ;
    EXECUTE IMMEDIATE x_Sql RETURNING INTO x_RowID, p_BegDate, p_EndDate, p_PartyID, p_SfContrID, p_FIID, p_DiffFlag;
    IF( SQL%ROWCOUNT <> 1) THEN
      x_RowID := NULL;
    END IF;
    COMMIT;
    RETURN x_RowID;
  EXCEPTION
    WHEN others THEN
      ROLLBACK;
      RETURN NULL;
  END GetReadyTask;

  /**
   @brief    отметка о завершении обработки задания
   @param[in]  p_RowID     	ID задания
   @param[in]  p_Status     	статус задания
  */
  PROCEDURE EndTask( p_RowID IN NUMBER, p_Status IN VARCHAR2 )
  IS
    pragma autonomous_transaction;
  BEGIN
    UPDATE itt_parallel_exec 
       SET str02 = p_Status, dat04 = SYSDATE
     WHERE row_id = p_RowID;
    COMMIT;
  EXCEPTION
    WHEN others THEN
      ROLLBACK;
  END EndTask;


  /**
   @brief    Возвращает 1, если счет p_Acc является действующим на дату p_RestDate
   @param[in]  	p_Acc     		счет
   @param[in] 	p_RestDate       	дата расчета остатков
  */
  FUNCTION IsOpenAcc ( p_Acc IN varchar2, p_RestDate IN DATE ) 
    RETURN number
  IS
    x_Ret NUMBER := 0;
    x_OpenDate DATE;
    x_CloseDate DATE;
  BEGIN
    SELECT r.t_open_date, r.t_close_date INTO x_OpenDate, x_CloseDate FROM daccount_dbt r WHERE r.t_account = p_Acc;
    IF( (x_CloseDate = to_date('1-1-0001', 'dd-mm-yyyy')) OR (p_RestDate BETWEEN x_OpenDate AND x_CloseDate) ) THEN
      x_Ret := 1;
    END IF;
    RETURN x_Ret;
  EXCEPTION
    WHEN others THEN
      RETURN x_Ret;
  END IsOpenAcc;

  /**
   @brief    Добавление строки в отчет
   @param[in]  	p_RepID     		ID отчета (dlactmoney5882_dbt.t_repid он же itt_parallel_exec.calc_id)
   @param[in] 	p_PartyID     		ID клиента
   @param[in] 	p_SfContrID    		ID договора
   @param[in] 	p_InnerAcc    		Счет ВУ
   @param[in] 	p_GbAcc    		Счет БУ
   @param[in] 	p_FIID     		ID финансового инструмента
   @param[in] 	p_RestDate       	дата расчета остатков
   @param[in] 	p_DiffFlag     		если 'X', то отчет показывает только различия, иначе всё
  */
  PROCEDURE AddRepRow ( 
    p_RepID IN NUMBER, p_PartyID IN NUMBER, p_SfContrID IN NUMBER, p_InnerAcc IN varchar2, p_GbAcc varchar2
    , p_FIID IN NUMBER, p_RestDate IN DATE, p_DiffFlag IN varchar2 
  )
  IS
    x_InnerRest NUMBER := rsb_account.restall(p_InnerAcc, 21, p_FIID, p_RestDate - 1);
    x_GbRest NUMBER := rsb_account.restall(p_GbAcc, 1, p_FIID, p_RestDate - 1);
  BEGIN
    IF((IsOpenAcc(p_InnerAcc, p_RestDate) = 1) AND (IsOpenAcc(p_GbAcc, p_RestDate) = 1)) THEN
      IF ((p_DiffFlag IS null) OR (x_InnerRest <> x_GbRest)) THEN
        INSERT INTO dlactmoney5882_dbt r (
           r.t_repid, r.t_partyid, r.t_sfcontrid, r.t_fiid
           , r.t_inneraccount, r.t_gbaccount, r.t_restdate
           , r.t_innerrest, r.t_gbrest
        ) VALUES (
           p_RepID, p_PartyID, p_SfContrID, p_FIID
           , p_InnerAcc, p_GbAcc, p_RestDate
           , x_InnerRest, x_GbRest
        );
      END IF;
    END IF;
  END AddRepRow;

  /**
   @brief    Запуск процесса получения данных по суб-договору.
   @param[in]  	p_CalcID     		ID расчета (itt_parallel_exec.calc_id)
   @param[in] 	p_BegDate       	начальная дата отчета
   @param[in] 	p_EndDate     		конечная дата отчета
   @param[in] 	p_PartyID     		ID клиента
   @param[in] 	p_SfContrID    		ID договора
   @param[in] 	p_FIID     		ID финансового инструмента
   @param[in] 	p_DiffFlag     		если 'X', то отчет показывает только различия, иначе всё
  */
  PROCEDURE ProcessSfContr ( 
    p_CalcID IN varchar2
    , p_BegDate IN DATE
    , p_EndDate IN DATE
    , p_PartyID IN number
    , p_SfContrID IN number
    , p_FIID IN number
    , p_DiffFlag IN varchar2 
  )
  IS
    x_RestDate DATE;
    x_SfContrID NUMBER;
    x_CurArray tt_Currency := tt_Currency();
    x_FIID number;
    x_InnerAcc varchar2(25);
    x_GbAcc varchar2(25);
  BEGIN
    x_SfContrID := p_SfContrID; 
    x_CurArray := GetCurArray (x_SfContrID, p_FIID);
    IF x_CurArray.COUNT > 0 THEN
      FOR j IN x_CurArray.FIRST..x_CurArray.LAST LOOP
        x_FIID := x_CurArray(j);
        x_InnerAcc := ACTMONEY_utl.GetInnerAcc(x_SfContrID, x_FIID);
        x_GbAcc := ACTMONEY_utl.GetGbAcc(x_SfContrID, x_FIID);
        x_RestDate := p_BegDate; -- DEF-83901, инициализировать начальную дату нужно здесь
        WHILE (x_RestDate <= p_EndDate) LOOP
          AddRepRow( p_CalcID, p_PartyID, x_SfContrID, x_InnerAcc, x_GbAcc, x_FIID, x_RestDate, p_DiffFlag );
          x_RestDate := x_RestDate + 1;
        END LOOP; -- x_RestDate
      END LOOP; -- j
    END IF;
  END ProcessSfContr;

  /**
   @brief    Функция возвращает ID суб-договора фондового рынка для ЕДП. Иначе -- NULL.
   @param[in] 	p_DlContrID    		ID договора
   @param[in] 	p_Market    		суффикс, определяющий вид рынка ('ф')
  */
  FUNCTION GetSfEdpMarket( p_DlContr IN NUMBER, p_Market IN VARCHAR2 )
     RETURN number
  IS
    x_SfContr NUMBER;
  BEGIN
      select MIN(sf.t_id) INTO x_SfContr from ddlcontr_dbt dl 
      join ddlcontrmp_dbt mp on (mp.t_dlcontrid = dl.t_dlcontrid)
      join dsfcontr_dbt sf on (sf.t_id = mp.t_sfcontrid)
      where dl.t_sfcontrid = p_DlContr
      and substr(sf.t_number, length(sf.t_number)) = p_Market;
    IF(RSB_SECUR.GetGeneralMainObjAttr (659,LPAD (x_SfContr, 10, '0'),102, to_date('31122999','ddmmyyyy')) <> 1) THEN
       RETURN NULL;
    END IF;
    return x_SfContr; 
  EXCEPTION
    WHEN others THEN
       return NULL; 
  END GetSfEdpMarket;

  /**
   @brief    Функция возвращает ID суб-договора для ЕДП. Иначе -- NULL.
             Сначала пытается найти суб-договор фондового рынка, затем валютного, затем срочного.
   @param[in] 	p_DlContrID    		ID договора
  */
  FUNCTION GetSfEdp(p_DlContr IN NUMBER)
     RETURN number
  IS
    x_SfContr NUMBER;
  BEGIN
    x_SfContr := GetSfEdpMarket(p_DlContr, 'ф');
    IF(x_SfContr IS NULL) THEN
      x_SfContr := GetSfEdpMarket(p_DlContr, 'v');
    END IF;
    IF(x_SfContr IS NULL) THEN
      x_SfContr := GetSfEdpMarket(p_DlContr, 'с');
    END IF;
    return x_SfContr; 
  EXCEPTION
    WHEN others THEN
       return NULL; 
  END GetSfEdp;

  /**
   @brief    Функция возвращает 2, если клиент является физ.лицом
   @param[in] 	p_PartyID    		ID клиента
  */
  FUNCTION GetLegalForm( p_PartyID IN NUMBER )
     RETURN number
  IS
    x_LegalForm NUMBER := -1;
  BEGIN
    select r.t_legalform into x_LegalForm from dparty_dbt r where r.t_partyid = p_PartyID;
    return x_LegalForm; 
  EXCEPTION
    WHEN others THEN
       return -1; 
  END GetLegalForm;


  /**
   @brief    Функция возвращает ID суб-договора по вне-бирже
   @param[in] 	p_PartyID    		ID клиента
  */
  FUNCTION GetSfOut( p_PartyID IN NUMBER )
     RETURN number
  IS
    x_SfContr NUMBER;
  BEGIN
    select sf.t_id into x_SfContr from dsfcontr_dbt sf 
      where sf.t_partyID = p_PartyID and sf.t_servkind = 1 and sf.t_servkindsub = 9;
    return x_SfContr; 
  EXCEPTION
    WHEN others THEN
       return NULL; 
  END GetSfOut;


  /**
   @brief    Запуск процесса получения данных для отчета.
   @param[in]  	p_CalcID     		ID расчета (itt_parallel_exec.calc_id)
   @param[in] 	p_BegDate       	начальная дата отчета
   @param[in] 	p_EndDate     		конечная дата отчета
   @param[in] 	p_PartyID     		ID клиента
   @param[in] 	p_SfContrID    		ID договора
   @param[in] 	p_FIID     		ID финансового инструмента
   @param[in] 	p_DiffFlag     		если 'X', то отчет показывает только различия, иначе всё
  */
  PROCEDURE ProcessTask ( 
    p_CalcID IN varchar2
    , p_BegDate IN DATE
    , p_EndDate IN DATE
    , p_PartyID IN number
    , p_SfContrID IN number
    , p_FIID IN number
    , p_DiffFlag IN varchar2 
  )
  IS
    x_SfContr NUMBER := GetSfEdp( p_SfContrID );
    x_LegalForm NUMBER;
  BEGIN
    -- Если не ЕДП, перебираем все суб-договора
    IF (x_SfContr IS NULL) THEN
      FOR i IN (
        select sf.t_id from ddlcontr_dbt dl 
        join ddlcontrmp_dbt mp on (mp.t_dlcontrid = dl.t_dlcontrid)
        join dsfcontr_dbt sf on (sf.t_id = mp.t_sfcontrid)
        where dl.t_sfcontrid = p_SfContrID
      ) LOOP
        ProcessSfContr ( p_CalcID, p_BegDate, p_EndDate, p_PartyID, i.t_id, p_FIID, p_DiffFlag );
      END LOOP;  -- i
    ELSE
      -- Если ЕДП, то собираем данные по суб-договору
      ProcessSfContr ( p_CalcID, p_BegDate, p_EndDate, p_PartyID, x_SfContr, p_FIID, p_DiffFlag );
      -- DEF-83901 Для ФЛ, нужно ещё собрать данные по вне-бирже
      IF(GetLegalForm( p_PartyID ) = 2) THEN 
        x_SfContr := GetSfOut( p_PartyID );
        IF(x_SfContr IS NOT NULL) THEN 
          ProcessSfContr ( p_CalcID, p_BegDate, p_EndDate, p_PartyID, x_SfContr, p_FIID, p_DiffFlag );
        END IF;
      END IF;
    END IF;
    COMMIT;
  END ProcessTask;


  /**
   @brief    Запуск процесса получения данных для отчета.
   @param[in]  p_ProcessNo     	Номер параллельного процесса
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
  */
  PROCEDURE ReportProcess ( p_ProcessNo IN NUMBER, p_CalcID IN varchar2 )
  IS
    x_Prefix VARCHAR2(64) := 'p_CalcID: '||p_CalcID||' ('||p_ProcessNo||')';
    x_StartTime pls_integer;
    x_TaskID number;
    x_BegDate DATE;
    x_EndDate DATE;
    x_PartyID number;
    x_SfContrID number;
    x_FIID number;
    x_DiffFlag varchar2(32);
  BEGIN
    x_StartTime := dbms_utility.get_time;

    -- Сообщение о начале процедуры
    it_log.log( p_msg => x_Prefix, p_msg_type => it_log.c_msg_type__debug );

    -- в цикле получаем задания, пригодные для обработки, и обрабатываем их
    -- продолжаем, пока задания не закончатся
    LOOP
       x_TaskID := ACTMONEY_utl.GetReadyTask( 					-- получаем задание, пригодное для обработки
         p_CalcID, x_BegDate, x_EndDate, x_PartyID, x_SfContrID
         , x_FIID, x_DiffFlag
       ); 			
       EXIT WHEN x_TaskID IS NULL;          		            		-- завершаем обработку, если нет заданий
       ProcessTask ( 
         p_CalcID, x_BegDate, x_EndDate, x_PartyID, x_SfContrID
         , x_FIID, x_DiffFlag
       );
       EndTask ( x_TaskID, 'S' ); 					        -- отметка о завершении обработки задания
       COMMIT;
    END LOOP;

    -- Сообщение о завершении процедуры
    it_log.log( p_msg => x_Prefix||'End: '||ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );

  END ReportProcess;


  /**
   @brief    Запуск процесса получения данных для отчета. Запускается сервисом ExecuteCode через QManager
   @param[in]  p_worklogid     	ID задания (itt_q_message_log.msgid)
   @param[in]  p_messmeta     	мета-данные задания
  */
  PROCEDURE CallProcess ( p_worklogid integer, p_messmeta  xmltype )
  IS
    x_CalcID varchar2(25);
    x_ProcessNo NUMBER;
  BEGIN
    -- считывание параметров
    WITH meta AS 
      (select p_messmeta as x from dual)
      SELECT 
        EXTRACTVALUE(meta.x, '/XML/@CalcID')
        , to_number(EXTRACTVALUE(meta.x, '/XML/@ProcessNo'))
      INTO x_CalcID, x_ProcessNo
      FROM meta
    ;
    -- запуск процесса
    IF(x_CalcID is not null) THEN
      ReportProcess(x_ProcessNo, x_CalcID);
    END IF;
  END CallProcess;


  /**
   @brief    Запуск сервиса ExecuteCode через QManager
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
   @param[in]  p_ParallelCnt   	Количество параллельных процессов
  */
  PROCEDURE CallExecuteCodeInQManager( p_CalcID IN varchar2, p_ParallelCnt IN NUMBER )
  IS
    x_MessMETA xmltype;
    x_msgID itt_q_message_log.msgid%type;
    x_ParallelCnt number := p_ParallelCnt;
  BEGIN
    IF(x_ParallelCnt > g_ParallelLimit) THEN
      x_ParallelCnt := g_ParallelLimit;
    END IF;
    FOR i IN 1..x_ParallelCnt LOOP
      x_msgID := null;
      SELECT xmlelement("XML", xmlattributes(p_CalcID as "CalcID", i AS "ProcessNo")) INTO x_MessMETA FROM dual;
      it_q_message.load_msg(
         io_msgid        => x_msgID
         , p_message_type  => it_q_message.C_C_MSG_TYPE_R
         , p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
         , p_ServiceName   => 'ExecuteCode'
         , p_MESSBODY      => 'call ACTMONEY_utl.CallProcess(:1, :2)'
         , p_MessMETA      => x_MessMETA
      );  
    END LOOP;
  END CallExecuteCodeInQManager;


  /**
   @brief    Проверка существования партиции отчета dlactmoney5882_dbt.t_repID
   @param[in]  p_RepID     	ID отчета (dlactmoney5882_dbt.t_repID)
  */
  PROCEDURE CheckPartition( p_RepID IN varchar2 )
  IS
    x_IsPartition number := 0;
  BEGIN
    BEGIN
      SELECT 1 AS is_partition INTO x_IsPartition 
        FROM user_tab_partitions 
        WHERE table_name = UPPER('dlactmoney5882_dbt') and partition_name = UPPER('p'||p_RepID);
    EXCEPTION
      WHEN no_data_found THEN
        NULL;
    END;
    IF(x_IsPartition = 0) THEN
      EXECUTE IMMEDIATE 'alter table dlactmoney5882_dbt add partition p' || p_RepID || ' values (' || p_RepID || ')';
    END IF;
    -- очистка данных в партиции отчета
    EXECUTE IMMEDIATE 'delete from dlactmoney5882_dbt partition (p' || p_RepID || ')';
    COMMIT;
  END CheckPartition;


  /**
   @brief    Процедура очистки данных отчета-сверки
   @param[in]	p_RepID   	ID отчета
  */
  PROCEDURE ClearRep(p_RepID IN varchar2)
  IS
  BEGIN
    it_parallel_exec.clear_calc(p_RepID);
    for cur_par in (select p.partition_name
                      from user_tab_partitions p
                     where p.table_name = UPPER('dlactmoney5882_dbt')
                       and p.partition_name = UPPER('p'||p_RepID)
                     order by p.partition_name)
    loop
      execute immediate 'alter table dlactmoney5882_dbt drop partition ' || cur_par.partition_name;
    end loop;
  END ClearRep;


  /**
   @brief    Процедура формирования данных для отчета-сверки
   @param[in]  	p_BegDate       начальная дата отчета
   @param[in]  	p_EndDate     	конечная дата отчета
   @param[in]  	p_FIID     	ID финансового инструмента
   @param[in]  	p_DiffFlag     	если 'X', то отчет показывает только различия, иначе всё
   @param[out]	p_RepID   	Возвращает ID отчета
   @param[out]	p_Cnt   	Количество заданий для параллельного расчета
  */
  PROCEDURE CreateData ( 
    p_BegDate IN DATE
    , p_EndDate IN DATE
    , p_FIID IN number
    , p_DiffFlag IN varchar2 
    , p_RepID OUT varchar2 
    , p_Cnt OUT NUMBER
  )
  IS
    x_SqlIns VARCHAR2(1000);
    x_CalcID NUMBER;
    x_ParallelCnt NUMBER := 6;
  BEGIN
    -- формируем задания для параллельной обработки
    x_CalcID := it_parallel_exec.init_calc();
    p_RepID := to_char( x_CalcID );
    x_SqlIns := 'INSERT INTO itt_parallel_exec ( calc_id, str01, num01, num02, num03, dat01, dat02 ) VALUES ( :1, :2, :3, :4, :5, :6, :7 )';
    p_Cnt := 0;
    FOR i IN (SELECT r.t_clientid, r.t_contrid FROM dset_sfc_u_tmp_ r WHERE r.t_setflag = 'X') LOOP
      EXECUTE IMMEDIATE x_SqlIns USING x_CalcID, p_DiffFlag, i.t_clientid, i.t_contrid, p_FIID, p_BegDate, p_EndDate;
      p_Cnt := p_Cnt + 1;
    END LOOP;
    COMMIT;

    -- Проверка партиции dlactmoney5882_dbt 
    CheckPartition( p_RepID );
    
    -- вызов сервиса ExecuteCode через QManager
    CallExecuteCodeInQManager( x_CalcID, 4 );

    -- Процесс запущен, в вызывающем коде можно организовать опрос таблицы itt_parallel_exec по x_CalcID
    -- для отображения прогресса и ожидать завершения
  EXCEPTION
    WHEN others THEN
      ROLLBACK;
  END CreateData;

END ACTMONEY_utl;
/
