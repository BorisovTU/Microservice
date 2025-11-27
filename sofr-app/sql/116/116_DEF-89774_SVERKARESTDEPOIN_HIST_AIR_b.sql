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
  x_UpdateTimestamp number(10,0) :=0;
  x_UpdateValue number(10,0) :=0;
  x_RestTimeStamp DATE;
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

  -- Поиск данных по reportdate, accdepoid, isin
  BEGIN
    SELECT r.t_timestamp INTO x_RestTimeStamp FROM DDIASRESTDEPO_DBT r
     WHERE r.reportdate = x_ReportDate and r.accdepoid = x_SofrAccID and r.isin = x_IsinID;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      -- нет данных, добавляем
      INSERT INTO DDIASRESTDEPO_DBT r (
        r.recid, r.accdepoid, r.reportdate, r.isin, r.value, r.t_timestamp
      ) VALUES (
        x_RecID, x_SofrAccID, x_ReportDate, x_IsinID, x_Value, x_TimeStamp
      );
      RETURN ;
  END;

  -- Данные есть.
  -- Если это не проблемная бумага, то просто обновляем остаток
  IF( x_Isin NOT IN ('GB00B10RZP78') ) THEN
    UPDATE DDIASRESTDEPO_DBT r
      SET r.t_timestamp = x_TimeStamp, r.recID = x_RecID, r.value = x_Value
      WHERE r.reportdate = x_ReportDate AND r.accdepoid = x_SofrAccID AND r.isin = x_IsinID
    ;
    RETURN ;
  END IF;

  -- Проблемная бумага, но если временная метка другая, то тоже обновляем остаток
  IF ( x_RestTimeStamp <> x_TimeStamp ) THEN
    UPDATE DDIASRESTDEPO_DBT r
      SET r.t_timestamp = x_TimeStamp, r.recID = x_RecID, r.value = x_Value
      WHERE r.reportdate = x_ReportDate AND r.accdepoid = x_SofrAccID AND r.isin = x_IsinID
    ;
    RETURN ;
  END IF;

  -- Остался случай, когда несколько записей пришли в одной выборке (DEF-63390),
  -- остаток увеличиваем
  UPDATE DDIASRESTDEPO_DBT r
    SET r.recID = x_RecID, r.value = r.value + x_Value
    WHERE r.reportdate = x_ReportDate AND r.accdepoid = x_SofrAccID AND r.isin = x_IsinID
  ;

EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'Error', p_msg_type => it_log.c_msg_type__error);
    it_error.clear_error_stack;
    RAISE;
END;
/
