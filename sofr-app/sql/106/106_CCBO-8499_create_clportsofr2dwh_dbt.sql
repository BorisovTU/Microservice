begin
  execute immediate q'[
create table dclportsofr2dwh_dbt (
   t_reportdate DATE,
   t_contrnumber  VARCHAR2(20),
   t_contrdatebegin DATE, 
   t_clientname	VARCHAR2(320),
   t_clientcode	VARCHAR2(12),
   t_fininstr	VARCHAR2(50),
   t_fininstrtype	VARCHAR2(50),
   t_qty	NUMBER(32,12),
   t_nkd	NUMBER(32,12),
   t_price	NUMBER(32,12),
   t_fininstrccy	VARCHAR2(3),
   t_facevalue	NUMBER(32,12),
   t_open_balance	NUMBER(32,12),
   t_open_balance_rub	NUMBER(32,12),
   t_inputcash	NUMBER(32,12),
   t_inputsec	NUMBER(32,12),
   t_outputcash	NUMBER(32,12),
   t_outputsec	NUMBER(32,12),
   t_redemption	NUMBER(32,12),
   t_amortization	NUMBER(32,12)	,
   t_div	NUMBER(32,12),
   t_profitaccount	CHAR(1),
   t_coupon	NUMBER(32,12),
   t_uploadtime	TIMESTAMP,
   t_partyid	NUMBER(10)
  )]';  
   
end;
/
   
COMMENT ON TABLE dclportsofr2dwh_dbt IS 'Выгрузка в КХД данных по остаткам ДС И ЦБ  клиентов ФЛ'
/ 
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_reportdate IS 'Дата, за которую выгружаются данные по клиенту'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_contrnumber IS '№ договора об оказании услуг брокерского обслуживания на рынке ценных бумаг'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_contrdatebegin IS 'Дата открытия договора'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_clientname IS 'ФИО Клиента'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_clientcode IS 'Уникальный номер клиента'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_fininstr IS 'Инструмент'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_fininstrtype IS 'Тип инструмента'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_qty IS 'Плановый остаток по инструментам / денежным средставм, (таблица с лимитами)'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_nkd IS 'Накопленный купонный доход на 1 бумагу, на дату'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_price IS 'Рыночная цена инструмента (рыночная цена 3)'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_fininstrccy IS 'Валюта инструмента'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_facevalue IS 'Текущий номинал ЦБ'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_open_balance IS 'Ликвидационная стоимость в валюте инструмента'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_open_balance_rub IS 'Уникальный номер клиента'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_inputcash IS 'Input_ДС в руб. экв.'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_inputsec IS 'Input_ЦБ в руб. экв.'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_outputcash IS 'Output_ДС в руб. экв.'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_outputsec IS 'Output_ЦБ в руб. экв.'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_redemption IS 'Погашение (вкл. Оферту) в руб. экв.'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_amortization IS 'Амортизация в руб. экв.'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_div IS 'Дивиденды в руб. экв.'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_profitaccount IS 'Признак выплаты купона на текущий/брокерский счет'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_coupon IS 'Полученный купон в руб. экв.'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_uploadtime IS '	Дата и время заполнения буферной таблицы'
/
COMMENT ON COLUMN dclportsofr2dwh_dbt.t_partyid IS 'Идентификатор клиента владельца договора ДБО'
/




