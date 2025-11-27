-- Изменения по DEF-64505 
DECLARE
  logID VARCHAR2(32) := 'DEF-61499_DEF-64505';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- изменение триггера SOFR_SVERKARESTDEPOIN_HIST_AIR
  PROCEDURE replaceTriggerRest
  IS
    x_Str VARCHAR2(32000);
    x_TrgName VARCHAR2(32) := 'SOFR_SVERKARESTDEPOIN_HIST_AIR';
    cr VARCHAR2(2) := CHR(10);  -- перевод строки
    ct VARCHAR2(2) := CHR(9); -- табуляция
  BEGIN
    LogIt('Создание тригера '||x_TrgName);
    x_Str := 'CREATE OR REPLACE TRIGGER '||x_TrgName
        ||cr||'AFTER INSERT ON SOFR_SVERKARESTDEPOIN '
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
    ||cr||ct||'UPDATE /*+ index (r, DDIASRESTDEPO_DBT_IDX2) */ DDIASRESTDEPO_DBT r '
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
    EXECUTE IMMEDIATE x_Str;
    LogIt('Создан тригер '||x_TrgName);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка создания тригера '||x_TrgName);
  END;
BEGIN
  replaceTriggerRest;                 -- изменение триггера SOFR_SVERKAACCDEPOIN_HIST_AIR   
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;

