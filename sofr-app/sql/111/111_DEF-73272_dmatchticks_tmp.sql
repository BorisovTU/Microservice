-- Таблица "dmatchticks_tmp"

CREATE GLOBAL TEMPORARY TABLE dmatchticks_tmp (
  -- инфа о сделке продажи
    t_SaleDealDate		DATE			-- Дата сделки продажи
  , t_SaleDealTime		DATE			-- Время сделки продажи
  , t_SaleDealID            	NUMBER(10,0)		-- ID сделки продажи
  -- инфа о сделке покупки
  , t_BuySumID			NUMBER			-- ID лота покупки
  , t_BuyDealDate		DATE			-- Дата сделки покупки
  , t_BuyDealTime		DATE			-- Время сделки покупки
  , t_BuyDealID            	NUMBER(10,0)		-- ID сделки покупки
  , t_Amount                	NUMBER(32,12)		-- Объем сделки-покупки, соответствующий сделке-продаже
  , t_IsNdfl                	NUMBER			-- Если 1, -- значит 'Зачисление НДФЛ' (поле соответствует полю ddl_tick_dbt.t_flag3='X')
  , t_IsDepo                	NUMBER			-- Если 1, -- значит 'Зачисление ДЕПО'
  , t_NKD                	NUMBER(32,12)		-- НКД
) ON COMMIT PRESERVE ROWS
/

COMMENT ON TABLE dmatchticks_tmp IS 'Таблица входных данных для отчета Суммы подтвержденных расходов'/

COMMENT ON COLUMN dmatchticks_tmp.t_SaleDealDate IS 'Дата сделки продажи'/
COMMENT ON COLUMN dmatchticks_tmp.t_SaleDealTime IS 'Время сделки продажи'/
COMMENT ON COLUMN dmatchticks_tmp.t_SaleDealID IS 'ID сделки продажи'/

COMMENT ON COLUMN dmatchticks_tmp.t_BuySumID IS 'ID лота покупки'/
COMMENT ON COLUMN dmatchticks_tmp.t_BuyDealDate IS 'Дата сделки покупки'/
COMMENT ON COLUMN dmatchticks_tmp.t_BuyDealTime IS 'Время сделки покупки'/
COMMENT ON COLUMN dmatchticks_tmp.t_BuyDealID IS 'ID сделки покупки'/
COMMENT ON COLUMN dmatchticks_tmp.t_Amount IS 'Объем сделки-покупки, соответствующий сделке-продаже'/
COMMENT ON COLUMN dmatchticks_tmp.t_IsNdfl IS 'Если 1, -- значит ''Зачисление НДФЛ'''/
COMMENT ON COLUMN dmatchticks_tmp.t_IsDepo IS 'Если 1, -- значит ''Зачисление ДЕПО'''/
COMMENT ON COLUMN dmatchticks_tmp.t_NKD IS 'НКД'/

