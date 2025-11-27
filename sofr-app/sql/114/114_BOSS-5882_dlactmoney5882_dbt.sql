-- Изменения по BOSS-5882 (Создание таблицы DLACTMONEY5882_DBT)
-- Данные для отчета-сверки ДС между счетами ВУ и БУ
DECLARE
  logID VARCHAR2(32) := 'BOSS-5882';
  x_Cnt NUMBER;
  x_Stat NUMBER := 0;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- создание таблицы DLACTMONEY5882_DBT
  PROCEDURE createDLACTMONEY5882_DBT ( p_Stat IN OUT number )
  IS
  BEGIN
    IF( p_Stat = 1 ) THEN
      RETURN;
    END IF;

    LogIt('Создание таблицы DLACTMONEY5882_DBT');
    SELECT count(*) INTO x_Cnt FROM user_tables WHERE table_name = upper('DLACTMONEY5882_DBT');
    IF (x_Cnt = 1) THEN
       LogIt('Существует таблица DLACTMONEY5882_DBT');
    ELSE
       EXECUTE IMMEDIATE q'[
         CREATE TABLE dlactmoney5882_dbt
            (  t_RepID NUMBER NOT NULL ENABLE
              , t_ID NUMBER GENERATED ALWAYS AS IDENTITY MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1 
              , t_PartyID NUMBER
              , t_SfContrID NUMBER
              , t_FIID NUMBER
              , t_InnerAccount VARCHAR2(25)
              , t_GbAccount VARCHAR2(25)
              , t_RestDate DATE
              , t_InnerRest NUMBER
              , t_GbRest NUMBER
              , t_Details VARCHAR2(2000)
            ) 
           PARTITION BY LIST ( t_RepID ) 
          (PARTITION "P999999990"  VALUES (0))
       ]';
       EXECUTE IMMEDIATE q'[CREATE INDEX dlactmoney5882_dbt_idx0 ON dlactmoney5882_dbt (t_ID) LOCAL]';
       EXECUTE IMMEDIATE q'[COMMENT ON TABLE dlactmoney5882_dbt IS 'Данные для отчета-сверки ДС между счетами ВУ и БУ']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN dlactmoney5882_dbt.t_RepID IS 'Номер отчета']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN dlactmoney5882_dbt.t_ID IS 'ID записи']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN dlactmoney5882_dbt.t_PartyID IS 'ID клиента']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN dlactmoney5882_dbt.t_SfContrID IS 'ID договора']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN dlactmoney5882_dbt.t_FIID IS 'ID финансового инструмента']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN dlactmoney5882_dbt.t_InnerAccount IS 'счет ВУ']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN dlactmoney5882_dbt.t_GbAccount IS 'счет БУ']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN dlactmoney5882_dbt.t_RestDate IS 'дата расчета остатка']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN dlactmoney5882_dbt.t_InnerRest IS 'остаток по счету ВУ']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN dlactmoney5882_dbt.t_GbRest IS 'остаток по счету БУ']';
       EXECUTE IMMEDIATE q'[COMMENT ON COLUMN dlactmoney5882_dbt.t_Details IS 'причина расхождения']';
       LogIt('Создана таблица DLACTMONEY5882_DBT');
    END IF;
  END;
BEGIN
  createDLACTMONEY5882_DBT ( x_Stat );		-- 1) создание таблицы DLACTMONEY5882_DBT
END;
/
