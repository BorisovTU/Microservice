-- Изменения по DEF-56180, добавление индекса по reportdate для таблицы SOFR_DIASDEPORESTFULL
DECLARE
  logID VARCHAR2(9) := 'DEF-56180';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Добавление индекса
  PROCEDURE AddIndex(p_table VARCHAR2, p_index VARCHAR2, p_columns VARCHAR2)
  AS
    x_Cnt number;
  BEGIN
    LogIt('Добавление индекса '''||p_index||''' для таблицы '''||p_table||'''');
    SELECT count(*) INTO x_Cnt FROM user_indexes i WHERE i.TABLE_NAME=p_table AND i.INDEX_NAME=p_index ;
    IF x_Cnt =1 THEN
      execute immediate 'drop index '||p_index;
    END IF;
    execute immediate 'CREATE INDEX '||p_index||' ON '||p_table||' ('||p_columns||') tablespace indx ';
    LogIt('Добавлен индекс '''||p_index||''' для таблицы '''||p_table||'''');
  END;
BEGIN
  -- Добавление индекса в таблицу SOFR_DIASDEPORESTFULL (по полю REPORTDATE)
  AddIndex('SOFR_DIASDEPORESTFULL', 'SOFR_DIASDEPORESTFULL_IDXREP', 'REPORTDATE');
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
