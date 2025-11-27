-- Изменения по BOSS-2609_BOSS-2893
-- Создание таблицы "D_OTCDEALTMP_DBT", для буферизации данных по загрузке сделок OTC в рамках Указа №844
DECLARE
  logID VARCHAR2(32) := 'BOSS-2609_BOSS-2893'; 
  x_Cnt NUMBER;
  x_Stat NUMBER := 0;
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  procedure create_otc_deals_tmp(p_Stat IN OUT number) as
    x_Cnt number;
  begin
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;
    
    --delete if exists
    select count(1) into x_cnt from user_tables where upper(table_name) = 'OTC_DEALS_TMP';
    IF( x_Cnt = 1 ) THEN
      LogIt('Удаление таблицы OTC_DEALS_TMP.');
      EXECUTE IMMEDIATE 'DROP TABLE OTC_DEALS_TMP';
      LogIt('Удалена таблица OTC_DEALS_TMP.');
    END IF;
    
    execute immediate 'create global temporary table otc_deals_tmp on commit preserve rows as 
                        select t_last_name,
                              t_first_name,
                              t_middle_name,
                              t_doc_type,
                              t_doc_series,
                              t_doc_number,
                              t_doc_date,
                              t_birth_date,
                              t_inn,
                              t_isin,
                              t_fi_ndc_code,
                              t_section_code,
                              t_depo_acc_num,
                              t_depo_acc_num_person,
                              t_qty,
                              t_dep_name,
                              t_dep_code,
                              t_price,
                              t_value,
                              t_trust_date,
                              t_trust_no,
                              t_qty_fact,
                              t_price_end,
                              t_value_fact,
                              t_shareholding_formula,
                              t_utstmp,
                              t_f0_file_name,
                              t_f1_file_name
                          from d_otcdealtmp_dbt where 1 = 0';
    LogIt('Создана таблица otc_deals_tmp');
  EXCEPTION
    WHEN OTHERS THEN
      LogIt('Ошибка создания таблицы otc_deals_tmp: ' || sqlerrm);
      p_Stat := 1;
      raise;
  end create_otc_deals_tmp;
  
  -- создание индекса
  PROCEDURE createIndex ( 
     p_Stat IN OUT number
     , p_Unique IN varchar2
     , p_TableName IN varchar2
     , p_IndexName IN varchar2
     , p_Fields IN varchar2 
     , p_TableSpace IN varchar2 DEFAULT 'USERS'  -- в INDX нет места
     , p_Local IN varchar2 DEFAULT ''  -- для партиционированной таблицы можно указать LOCAL
  )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Проверка индекса '||p_IndexName);
    SELECT count(*) INTO x_Cnt FROM user_indexes i WHERE i.INDEX_NAME = p_IndexName ;
    IF (x_Cnt = 1) THEN
       LogIt('Индекс '||p_IndexName||' существует');
       LogIt('Удаление индекса '||p_IndexName);
       EXECUTE IMMEDIATE 'DROP INDEX '||p_IndexName;
       LogIt('Удален индекс: '||p_IndexName);
    END IF;
    LogIt('Создание индекса '||p_IndexName);
    EXECUTE IMMEDIATE 'CREATE '||p_Unique||' INDEX '||p_IndexName
       ||' ON '||p_TableName||' ('||p_Fields||') '
       ||p_Local||' TABLESPACE '||p_TableSpace
    ;
    LogIt('Создан индекс: '||p_IndexName);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания индекса: '||p_IndexName);
      p_Stat := 1;
  END;
  -- создание индексов для D_OTCDEALTMP_DBT
  PROCEDURE CreateIdx_OTCDEALTMP ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    createIndex ( p_Stat, 'unique', 'D_OTCDEALTMP_DBT', 'D_OTCDEALTMP_IDX7', 't_isin, t_depo_acc_num_person', 'USERS' );
  EXCEPTION
   WHEN OTHERS THEN 
      p_Stat := 1;
  END;
BEGIN
  create_otc_deals_tmp( x_Stat );    -- 1.5 ?
  CreateIdx_OTCDEALTMP( x_Stat );    -- 3) создание индексов для D_OTCDEALTMP_DBT
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
    raise;
END;
/
