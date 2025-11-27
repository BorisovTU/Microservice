-- Изменения по DEF-65751
-- 1) Корректировка типа счета (для активных общесистемных счетов, добавленных по DEF-65751 )
DECLARE
  logID VARCHAR2(32) := 'DEF-65751';
  x_NvlDate DATE := to_date('1-1-0001', 'dd-mm-yyyy');
  x_OnDate DATE := to_date('27-7-2023', 'dd-mm-yyyy');
  x_Cnt NUMBER;
  x_810 NUMBER := 0;  -- рубль
  x_840 NUMBER := 7;  -- доллар
  x_978 NUMBER := 8;  -- евро
  x_156 NUMBER := 11;  -- юань

  x_CurrencyOffice NUMBER;
  x_DerivativesOffice NUMBER;
  x_StockMarket NUMBER;
  x_CurrencyMarket NUMBER;
  x_DerivativesMarket NUMBER;
  x_SettleCode25834 NUMBER;         -- идентификатор расчетного кода банка '25834'

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- добавление общесистемного счета
  PROCEDURE addCommonMcAccDoc( 
    p_CatCode IN varchar2                   -- категория
    , p_KindAccount IN varchar2                 -- сторона баланса счета
    , p_TemplNum IN number                  -- шаблон
    , p_Currency IN number      -- валюта
    , p_MarketPlaceID IN number     -- торговая площадка
    , p_MpOfficeCode IN varchar2    -- Сектор
    , p_Acc IN varchar2                         -- Счет
  )
  IS
    x_CatId NUMBER;
    x_CatNum NUMBER;
    x_OfficeID NUMBER;
  BEGIN
    LogIt('Добавление общесистемного счета '||p_Acc||' по категории '||p_CatCode);
    SELECT count(*) INTO x_Cnt FROM dmcaccdoc_dbt r WHERE r.t_account = p_Acc and r.t_iscommon = 'X';
    IF (x_Cnt = 1) THEN
       LogIt('Существует общесистемный счет '||p_Acc);
    ELSE
       SELECT r.t_officeid INTO x_OfficeID
         FROM dptoffice_dbt r 
         WHERE r.t_partyid = p_MarketPlaceID and r.t_officecode = p_MpOfficeCode AND rownum < 2;
       SELECT r.t_id, r.t_number INTO x_CatId, x_CatNum FROM dmccateg_dbt r 
         WHERE r.t_leveltype = 1 and r.t_code = p_CatCode;
       INSERT INTO dmcaccdoc_dbt r (
         r.t_id, r.t_iscommon, r.t_dockind, r.t_docid, r.t_catid, r.t_catnum
         , r.t_chapter, r.t_account, r.t_currency, r.t_templnum, r.t_groupnum
         , r.t_periodid, r.t_activatedate, r.t_disablingdate, r.t_isusable
         , r.t_fiid, r.t_owner, r.t_place, r.t_issuer, r.t_kind_account
         , r.t_centr, r.t_centroffice, r.t_actiondate, r.t_fiid2, r.t_clientcontrid
         , r.t_bankcontrid, r.t_marketplaceid, r.t_marketplaceofficeid, r.t_firole
         , r.t_indexdate, r.t_departmentid, r.t_contractor, r.t_branch, r.t_corrdepartmentid
         , r.t_currencyeq, r.t_currencyeq_ratetype, r.t_currencyeq_ratedate
         , r.t_currencyeq_rateextra, r.t_mcbranch
       ) VALUES (
         0, 'X', 0, 0, x_CatId, x_CatNum
         , 1, p_Acc, p_Currency, p_TemplNum, 0
         , 0, x_OnDate, x_NvlDate, 'X'
         , -1, -1, -1, -1, p_KindAccount
         , -1, -1, x_OnDate, -1, -1
         , -1, p_MarketPlaceID, x_OfficeID, 0
         , -1, 1, -1, 1, 0
         , -1, 0, 0
         , 0, 1 
       );
       EXECUTE IMMEDIATE 'COMMIT';
       LogIt('Добавлен общесистемный счет '||p_Acc||' по категории '||p_CatCode);
    END IF;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка добавления общесистемного счета '||p_Acc||' по категории '||p_CatCode);
      EXECUTE IMMEDIATE 'ROLLBACK';
  END;
  -- корректировка типа счета
  PROCEDURE correctMcAccDoc( 
    p_CatCode IN varchar2                   -- категория
    , p_KindAccount IN varchar2                 -- сторона баланса счета
    , p_Acc IN varchar2                         -- Счет
  )
  IS
    x_CatId NUMBER;
  BEGIN
    LogIt('корректировка типа счета '||p_Acc||' по категории '||p_CatCode);
    SELECT r.t_id INTO x_CatId FROM dmccateg_dbt r 
      WHERE r.t_leveltype = 1 and r.t_code = p_CatCode;
    UPDATE dmcaccdoc_dbt r 
      SET r.t_kind_account = p_KindAccount
      WHERE r.t_catid = x_CatId and r.t_account = p_Acc;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Скорректирован типа счета '||p_Acc||' по категории '||p_CatCode);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка корректировки типа счета '||p_Acc||' по категории '||p_CatCode);
      EXECUTE IMMEDIATE 'ROLLBACK';
  END;
BEGIN
  -- +Биржа, на площадке 28 (фондовый рынок)
  correctMcAccDoc('+Биржа', 'А', '47404810499003202617'); -- в рублях
  correctMcAccDoc('+Биржа', 'А', '47404840799003202617'); -- в долларах
  correctMcAccDoc('+Биржа', 'А', '47404978399003202617'); -- в евро
  correctMcAccDoc('+Биржа', 'А', '47404156399003202617'); -- в юанях
  -- +Биржа, на площадке 29 (валютный рынок)
  correctMcAccDoc('+Биржа', 'А', '47404810099003202619'); -- в рублях
  correctMcAccDoc('+Биржа', 'А', '47404840399003202619'); -- в долларах
  correctMcAccDoc('+Биржа', 'А', '47404978999003202619'); -- в евро
  correctMcAccDoc('+Биржа', 'А', '47404156999003202619'); -- в юанях
  -- +Биржа, на площадке 30 (срочный рынок)
  correctMcAccDoc('+Биржа', 'А', '47403810499003202621'); -- в рублях
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
