CREATE OR REPLACE TRIGGER "SOFR_SVERKARESTDEPOIN_HIST_AIR"
AFTER INSERT ON SOFR_SVERKARESTDEPOIN
REFERENCING NEW AS New OLD AS Old
FOR EACH ROW
DECLARE
  x_TimeStamp DATE;
  x_IsinID DDIASISIN_DBT.t_id%type;
  x_Isin DDIASISIN_DBT.t_isin%type := trim(:new.ISIN);
  x_DiasAccID DDIASACCMAP_DBT.t_Diasaccid%type := :new.ACCDEPOID;
  x_SofrAccID DDIASACCMAP_DBT.T_SofrAccID%type := -1;
  x_RecID DDIASRESTDEPO_DBT.recID%type := :new.RECID;
  x_Value DDIASRESTDEPO_DBT.value%type := :new.VALUE;
  x_ReportDate DDIASRESTDEPO_DBT.reportdate%type := :new.REPORTDATE;
  update_rowcount number(10,0) :=0;
BEGIN
  SELECT SYSTIMESTAMP INTO x_TimeStamp FROM dual;
  -- Анализируется ISIN. Полученное значение заменяется идентификатором.
  -- Если такого ISIN нет, производится добавление новой записи в таблицу.
  BEGIN
    SELECT r.t_ID INTO x_IsinID FROM DDIASISIN_DBT r where r.t_isin = x_Isin;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      INSERT INTO DDIASISIN_DBT r ( t_isin ) VALUES ( x_Isin )
      RETURNING t_ID INTO x_IsinID;
  END;
  -- Производится поиск ACCDEPOID в таблице маппинга.
  BEGIN
    SELECT r.t_sofraccid INTO x_SofrAccID
    FROM DDIASACCMAP_DBT r where r.t_diasaccid = x_DiasAccID;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      -- этого не может быть
      it_log.log(
        p_msg => 'Ошибка маппинга, x_DiasAccID: ' || to_char(x_DiasAccID)
        , p_msg_type => it_log.c_msg_type__debug
      );
  END;
  IF( x_SofrAccID = -1 ) THEN
    RETURN ;
  END IF;
  -- Производится попытка изменения данных в таблице DDIASRESTDEPO_DBT
  /*
    DEF-63390 если совпали reportdate,isin,accdepoid и timestamp (значит обе записи пришли в
    одной выборке), тогда увеличиваем остаток исходной записи, если timestamp различается -
    значит заново выгрузили запись, обновляем её как сейчас,
    если всё различается - вставляем новую запис
  */
  UPDATE DDIASRESTDEPO_DBT r
    SET r.t_timestamp = x_TimeStamp, r.recID = x_RecID, r.value = x_Value
    WHERE r.reportdate = x_ReportDate AND r.accdepoid = x_SofrAccID
      AND r.isin = x_IsinID AND r.t_timestamp <> x_TimeStamp
    AND rownum = 1
  ;
  update_rowcount := SQL%ROWCOUNT;
  UPDATE DDIASRESTDEPO_DBT r
    SET r.recID = x_RecID, r.value = r.value + x_Value
    WHERE r.reportdate = x_ReportDate AND r.accdepoid = x_SofrAccID
      AND r.isin = x_IsinID AND r.t_timestamp = x_TimeStamp
    AND rownum = 1
  ;
  /* Неудачная попытка изменения данных означает то, что данные являются новыми
   (для счета, reportdate и ISIN),
   поэтому (при неудачном изменении) производится вставка записи в таблице остатков.
   если счета нет, добавляем  
   Уникальный индекс по REPORTDATE, ACCDEPOID, ISIN - поэтому сравниваем с 1 */
  IF( SQL%ROWCOUNT <> 1 and update_rowcount<>1) THEN
    INSERT INTO DDIASRESTDEPO_DBT r (
      r.recid, r.accdepoid, r.reportdate, r.isin, r.value, r.t_timestamp
    ) VALUES (
      x_RecID, x_SofrAccID, x_ReportDate, x_IsinID, x_Value, x_TimeStamp
    );
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'Error', p_msg_type => it_log.c_msg_type__error);
    it_error.clear_error_stack;
    RAISE;
END;
/
