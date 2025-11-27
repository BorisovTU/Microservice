-- Изменения по BOSS-773, BOSS-1662
-- 1) Регистрация новых секторов биржи 
-- 2) Настройка категорий учета
-- 3) Создание новых схем расчетов для расчетного кода единого пула обеспечения. 
-- 4) Создание новых схем расчетов для валютного и срочного рынков, привязанных к  единому пулу обеспечения. 
-- 5) Ввод нового расчетного кода и привязка к нему новой схемы расчетов  
DECLARE
  logID VARCHAR2(32) := 'BOSS-771, BOSS-1410';
  x_NvlDate DATE := to_date('1-1-0001', 'dd-mm-yyyy');
  x_OnDate DATE := to_date('27-7-2023', 'dd-mm-yyyy');
  x_Cnt NUMBER;
  x_StockOffice NUMBER;
  x_CurrencyOffice NUMBER;
  x_DerivativesOffice NUMBER;
  x_StockMarket NUMBER;
  x_CurrencyMarket NUMBER;
  x_DerivativesMarket NUMBER;
  x_SettleCode25834 NUMBER;					-- идентификатор расчетного кода банка '25834'

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- добавление записи в структуру подразделений (dptoffice_dbt и dptoffisu_dbt)
  -- Если подразделение существует, вернется его идентификатор (поэтому функцию можно безопасно вызывать несколько раз)
  FUNCTION addPtOffice( p_PartyID IN number, p_AfterCode IN varchar2, p_Code IN varchar2, p_Name IN varchar2, p_Date IN date ) RETURN NUMBER
  IS
    x_OfficeID NUMBER;
    x_Sort NUMBER;
  BEGIN
    LogIt('Добавление подразделения '||p_PartyID||', '||p_Code);
    SELECT count(*) INTO x_Cnt FROM dptoffice_dbt r WHERE r.t_partyid = p_PartyID and r.t_officecode = p_Code;
    IF (x_Cnt = 1) THEN
       LogIt('Существует подразделение '||p_PartyID||', '||p_Code);
       SELECT r.t_officeid INTO x_OfficeID
         FROM dptoffice_dbt r 
         WHERE r.t_partyid = p_PartyID and r.t_officecode = p_Code AND rownum < 2;
    ELSE
       LogIt('Не существует подразделения '||p_PartyID||', '||p_Code);
       SELECT NVL(max(t_officeid),0)+1 INTO x_OfficeID FROM dptoffice_dbt r WHERE r.t_partyid = p_PartyID;
       INSERT INTO dptoffice_dbt r (
         r.t_partyid, r.t_officeid, r.t_officecode, r.t_officename, r.t_description, r.t_kpp
         , r.t_regnumber, r.t_regdate, r.t_dockindid, r.t_series, r.t_number, r.t_maincashier, r.t_dateclose
         , r.t_postaddress, r.t_statusbranch, r.t_noauto, r.t_stringforodbacc, r.t_dbaccess, r.t_placement
         , r.t_mainrate, r.t_phone, r.t_owncurrrate, r.t_ownexpoperset, r.t_statupregime, r.t_bankid
         , r.t_depid, r.t_client51, r.t_reserve
       ) VALUES (
         p_PartyID, x_OfficeID, p_Code, p_Name, chr(1), chr(1)
         , chr(1), x_NvlDate, 0, chr(1), chr(1), chr(1), x_NvlDate
         , chr(1), 0, chr(0), chr(1), 0, 0
         , 0, chr(1), chr(0), chr(0), chr(0), 0
         , 0, chr(0), chr(1)
       );
       SELECT NVL(max(t_sort),0)+1 INTO x_Sort FROM dptoffisu_dbt r WHERE r.t_partyid = p_PartyID;
       INSERT INTO dptoffisu_dbt r (
         r.t_partyid, r.t_officeid, r.t_datebegin, r.t_superiorid, r.t_sort, r.t_reserv
       ) VALUES (
         p_PartyID, x_OfficeID, p_Date, 0, x_Sort, chr(1)
       );
       EXECUTE IMMEDIATE 'COMMIT';
       LogIt('Добавлено подразделение '||p_PartyID||', '||p_Code);
    END IF;
    RETURN x_OfficeID;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка добавления подразделения '||p_PartyID||', '||p_Code);
      EXECUTE IMMEDIATE 'ROLLBACK';
      RETURN -1;
  END;
  -- добавление общесистемного счета
  PROCEDURE addCommonMcAccDoc( p_CatCode IN varchar2, p_Acc IN varchar2, p_Currency IN number )
  IS
    x_CatId NUMBER;
    x_CatNum NUMBER;
  BEGIN
    LogIt('Добавление общесистемного счета '||p_Acc||' по категории '||p_CatCode);
    SELECT count(*) INTO x_Cnt FROM dmcaccdoc_dbt r WHERE r.t_account = p_Acc and r.t_iscommon = 'X';
    IF (x_Cnt = 1) THEN
       LogIt('Существует общесистемный счет '||p_Acc);
    ELSE
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
         , 1, p_Acc, p_Currency, 2, 0
         , 0, x_OnDate, x_NvlDate, 'X'
         , -1, -1, -1, -1, 'A'
         , -1, -1, x_OnDate, -1, -1
         , -1, 4, 23, 0
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
  -- добавление схемы расчетов с биржей
  -- Если схема расчетов существует, вернется ее идентификатор (поэтому функцию можно безопасно вызывать несколько раз)
  FUNCTION addMarket( p_MarketCode IN varchar2, p_FiKind IN number, p_CentrOffice IN number, p_Dep IN number ) RETURN number
  IS
    x_MarketID number := -1;
  BEGIN
    LogIt('Добавление схемы расчетов с биржей '||p_MarketCode);
    SELECT count(*) INTO x_Cnt FROM ddlmarket_dbt r WHERE r.t_code = p_MarketCode;
    IF ( x_Cnt > 0 ) THEN
       LogIt('Существует схема расчетов с биржей '||p_MarketCode);
       SELECT r.t_id INTO x_MarketID
         FROM ddlmarket_dbt r 
         WHERE r.t_code = p_MarketCode AND rownum < 2;
    ELSE
       INSERT INTO ddlmarket_dbt r (
         r.t_id, r.t_istrust, r.t_fi_kind, r.t_code, r.t_market, r.t_orcb
         , r.t_centr, r.t_centroffice, r.t_balance, r.t_consolidate, r.t_depsetid
       ) VALUES (
         0, chr(0)
         , p_FiKind                                     -- Код ФИ (4 -- Производные инструменты) 
         , p_MarketCode
         , 2                                            -- 'Биржа' = ММВБ
         , 'X'
         , 4 						-- 'Расчетный центр' = НКО АО НРД
         , p_CentrOffice      				-- Сектор
         , '304'                                        -- Балансовый счет
         , 1                                            -- 'Сводные проводки' = 'Списание и зачисление'
         , p_Dep
       )           
       RETURNING t_id INTO x_MarketID
       ;
       EXECUTE IMMEDIATE 'COMMIT';
       LogIt('Добавлена схема расчетов с биржей '||p_MarketCode||', код: '||x_MarketID);
    END IF;
    RETURN x_MarketID;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка добавления схемы расчетов с биржей '||p_MarketCode);
      EXECUTE IMMEDIATE 'ROLLBACK';
      RETURN -1;
  END;
  -- добавление счета, открытого на бирже
  PROCEDURE addMarketAcc( p_MarketID IN number, p_Acc IN varchar2, p_Currency IN number )
  IS
    x_CatId NUMBER;
    x_CatNum NUMBER;
  BEGIN
    LogIt('Добавление счета, открытого на бирже '||p_Acc);
    SELECT count(*) INTO x_Cnt FROM ddlmarketacc_dbt r WHERE r.t_marketid = p_MarketID and r.t_account = p_Acc;
    IF (x_Cnt = 1) THEN
       LogIt('Существует счет, открытый на бирже '||p_Acc);
    ELSE
       INSERT INTO ddlmarketacc_dbt r (
         r.t_marketid, r.t_code_currency, r.t_account, r.t_typeown
       ) VALUES (
         p_MarketID, p_Currency, p_Acc, 2
       );
       EXECUTE IMMEDIATE 'COMMIT';
       LogIt('Добавлен счет '||p_Acc||' на бирже');
    END IF;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка добавления счета '||p_Acc||' на бирже');
      EXECUTE IMMEDIATE 'ROLLBACK';
  END;
  -- добавление счетов для схемы расчетов
  PROCEDURE addMarketAccounts( p_MarketID IN number )
  IS
  BEGIN
    addMarketAcc(p_MarketID, '30424810499003202617', 0);
    addMarketAcc(p_MarketID, '47403840999003001347', 7); -- в долларах
    addMarketAcc(p_MarketID, '47403978599003001347', 8); -- в евро
    addMarketAcc(p_MarketID, '47403156599003001347', 11); -- в юанях
    addMarketAcc(p_MarketID, '47404840299003001347', 7); -- в долларах
    addMarketAcc(p_MarketID, '47404978899003001347', 8); -- в евро
    addMarketAcc(p_MarketID, '47404156899003001347', 11); -- в юанях
  END;
  -- добавление расчетного кода банка
  FUNCTION addExtSettleCode( p_SettleCode IN varchar2, p_CentrOffice IN number, p_MarketSchemeID IN number ) RETURN number
  IS
    x_SettleID NUMBER := -1;
  BEGIN
    LogIt('Добавление расчетного кода банка '||p_SettleCode);
    SELECT count(*) INTO x_Cnt FROM ddl_extsettlecode_dbt r WHERE r.t_settlecode = p_SettleCode;
    IF (x_Cnt > 0) THEN
       SELECT r.t_id INTO x_SettleID FROM ddl_extsettlecode_dbt r WHERE r.t_settlecode = p_SettleCode AND rownum < 2;
       LogIt('Существует расчетный код банка '||p_SettleCode||', ID: '||x_SettleID);
    ELSE
       INSERT INTO ddl_extsettlecode_dbt r (
         r.t_id, r.t_settlecode, r.t_marketkind, r.t_codetype, r.t_inpool, r.t_marketid
         , r.t_centrid, r.t_centrofficeid, r.t_marketschemeid, r.t_parentid, r.t_parentsettlecode
       ) VALUES (
         0, p_SettleCode
         , 5
         , 2
         , 'X'                                          -- Единый пул
         , 2                                            -- биржа = ММВБ
         , 4 						-- 'Расчетный центр' = НКО АО НРД
         , p_CentrOffice      				-- Сектор
         , p_MarketSchemeID                             -- Схема расчетов = 'КлФондовыйТ+_ЕП_обособленные'
         , 0
         , chr(1)
       )
       RETURNING t_id INTO x_SettleID
       ;
       EXECUTE IMMEDIATE 'COMMIT';
       LogIt('Добавлен расчетный код банка '||p_SettleCode||', ID: '||x_SettleID);
    END IF;
    RETURN x_SettleID;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка добавления расчетный код банка '||p_SettleCode);
      EXECUTE IMMEDIATE 'ROLLBACK';
      RETURN -1;
  END;
  -- добавление доп. аналитики
  PROCEDURE addExtAnalytics( p_CodeID IN number, p_MarketKind IN number, p_CentrOfficeID IN number, p_MarketSchemeID IN number )
  IS
    x_CatId NUMBER;
    x_CatNum NUMBER;
  BEGIN
    LogIt('Добавление доп. аналитики для CodeID: '||p_CodeID||', MarketKind: '||p_MarketKind);
    SELECT count(*) INTO x_Cnt FROM ddl_extscanalytics_dbt r WHERE r.t_codeid = p_CodeID and r.t_marketkind = p_MarketKind;
    IF (x_Cnt = 1) THEN
       LogIt('Существует доп. аналитика для CodeID: '||p_CodeID||', MarketKind: '||p_MarketKind);
    ELSE
       INSERT INTO ddl_extscanalytics_dbt r (
         r.t_id, r.t_codeid, r.t_marketkind, r.t_centrofficeid, r.t_marketschemeid
       ) VALUES (
         0, p_CodeID, p_MarketKind, p_CentrOfficeID, p_MarketSchemeID
       );
       EXECUTE IMMEDIATE 'COMMIT';
       LogIt('Добавлена доп. аналитика для CodeID: '||p_CodeID||', MarketKind: '||p_MarketKind);
    END IF;
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка добавления доп. аналитики для CodeID: '||p_CodeID||', MarketKind: '||p_MarketKind);
      EXECUTE IMMEDIATE 'ROLLBACK';
  END;
BEGIN
  -- 1) Регистрация новых секторов биржи 
  x_StockOffice := addPtOffice(4, '27', '28', 'КлФондовыйТ+_ЕП_обособленные', to_date('15/11/2023', 'dd-mm-yyyy'));
  x_CurrencyOffice := addPtOffice(4, '28', '29', 'КлВалютный_ЕП_Обособленный', to_date('15/11/2023', 'dd-mm-yyyy'));
  x_DerivativesOffice := addPtOffice(4, '29', '30', 'КлСрочный_ЕП_Обособленный', to_date('15/11/2023', 'dd-mm-yyyy'));
  -- 2) Настройка категорий учета (добавление общесистемного счета)
  addCommonMcAccDoc('Клиринговый счет', '30424810499003202617', 0);
  addCommonMcAccDoc('-Обеспечение', '47403840999003001347', 7); -- в долларах
  addCommonMcAccDoc('-Обеспечение', '47403978599003001347', 8); -- в евро
  addCommonMcAccDoc('-Обеспечение', '47403156599003001347', 11); -- в юанях
  addCommonMcAccDoc('+Обеспечение', '47404840299003001347', 7); -- в долларах
  addCommonMcAccDoc('+Обеспечение', '47404978899003001347', 8); -- в евро
  addCommonMcAccDoc('+Обеспечение', '47404156899003001347', 11); -- в юанях 
  -- 3) Создание новых схем расчетов для расчетного кода единого пула обеспечения. 
  x_StockMarket := addMarket('КлФондовыйТ+_ЕП_обособлен', 2, x_StockOffice, 10);
  addMarketAccounts( x_StockMarket );
  -- 4) Создание новых схем расчетов для валютного и срочного рынков, привязанных к  единому пулу обеспечения. 
  x_CurrencyMarket := addMarket('КлВалютный_ЕП_Обособлен', 4, x_CurrencyOffice, null);
  addMarketAccounts( x_CurrencyMarket );
  x_DerivativesMarket := addMarket('КлСрочный_ЕП_Обособлен', 4, x_DerivativesOffice, null);
  addMarketAccounts( x_DerivativesMarket );
  -- 5) Ввод нового расчетного кода и привязка к нему новой схемы расчетов  
  x_SettleCode25834 := addExtSettleCode( '25834', x_StockOffice, x_StockMarket );
  addExtAnalytics( x_SettleCode25834, 2, x_CurrencyOffice, x_CurrencyMarket );  -- доп.аналитика для валютного рынка (см. ALG_DL_MARKETKIND_SETTLE = 7052)
  addExtAnalytics( x_SettleCode25834, 4, x_DerivativesOffice, x_DerivativesMarket );  -- доп.аналитика для срочного рынка 
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
