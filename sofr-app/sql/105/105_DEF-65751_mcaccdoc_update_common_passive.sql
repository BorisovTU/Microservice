-- Изменения по DEF-65751
-- 1) Корректировка типа счета (для общесистемных счетов, добавленных по DEF-65751 )
-- 2) поправлено добавление счета в общесистемные для Категории Биржа
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
  -- добавление группы тестов
  PROCEDURE addTestGroup ( 
     p_id IN number
     , p_descr IN varchar2
     , p_module IN varchar2 DEFAULT ''
     , p_setup IN varchar2 DEFAULT ''
     , p_teardown IN varchar2 DEFAULT ''
  )
  IS
    x_Str VARCHAR2(32000);
    cr VARCHAR2(2) := CHR(10);
  BEGIN
    LogIt('Добавление группы авто-тестов '||p_descr);
    x_Str := 'INSERT INTO dautotestgroups_dbt r ('
        ||'r.t_id, r.t_module, r.t_setup, r.t_teardown, r.t_descr '
        ||') VALUES ( '
        ||':p_id, :p_module, :p_setup, :p_teardown, :p_descr '
        ||')'
    ;
    EXECUTE IMMEDIATE x_Str USING p_id, p_module, p_setup, p_teardown, p_descr;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Добавлена группа авто-тестов '||p_descr);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка добавления группы авто-тестов '||p_descr);
      LogIt('SQLERRM '||SQLERRM);
  END;
  -- добавление теста
  PROCEDURE addTest ( 
     p_module IN varchar2
     , p_procname IN varchar2
     , p_testname IN varchar2
     , p_descr IN varchar2
     , p_group IN number
  )
  IS
    x_Str VARCHAR2(32000);
    cr VARCHAR2(2) := CHR(10);
  BEGIN
    LogIt('Добавление авто-теста '||p_testname);
    x_Str := 'INSERT INTO dautotests_dbt r ('
        ||'r.t_module, r.t_procname, r.t_testname, r.t_descr, r.t_group '
        ||') VALUES ( '
        ||':p_module, :p_procname, :p_testname, :p_descr, :p_group '
        ||')'
    ;
    EXECUTE IMMEDIATE x_Str USING p_module, p_procname, p_testname, p_descr, p_group;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Добавлен авто-тест '||p_testname);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка добавления авто-теста '||p_testname);
      LogIt('SQLERRM '||SQLERRM);
  END;
BEGIN
  -- Корректировки
  correctMcAccDoc('-Биржа', 'П', '47403810199003202617'); -- в рублях
  correctMcAccDoc('-Биржа', 'П', '47403840499003202617'); -- в долларах
  correctMcAccDoc('-Биржа', 'П', '47403978099003202617'); -- в евро
  correctMcAccDoc('-Биржа', 'П', '47403156099003202617'); -- в юанях
  correctMcAccDoc('-Биржа', 'П', '47403810799003202619'); -- в рублях
  correctMcAccDoc('-Биржа', 'П', '47403840099003202619'); -- в долларах
  correctMcAccDoc('-Биржа', 'П', '47403978699003202619'); -- в евро
  correctMcAccDoc('-Биржа', 'П', '47403156699003202619'); -- в юанях
  correctMcAccDoc('-Биржа', 'П', '47403810499003202621'); -- в рублях
  -- Группа авто-тестов по виду
  -- Активные
  addTest ( 'acc_tests.mac', 'AccCat_47404810499003202617', 'AccCat_47404810499003202617', 'Должен быть обще-системный счет 47404810499003202617, вид Активный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47404840799003202617', 'AccCat_47404840799003202617', 'Должен быть обще-системный счет 47404840799003202617, вид Активный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47404978399003202617', 'AccCat_47404978399003202617', 'Должен быть обще-системный счет 47404978399003202617, вид Активный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47404156399003202617', 'AccCat_47404156399003202617', 'Должен быть обще-системный счет 47404156399003202617, вид Активный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47404810099003202619', 'AccCat_47404810099003202619', 'Должен быть обще-системный счет 47404810099003202619, вид Активный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47404840399003202619', 'AccCat_47404840399003202619', 'Должен быть обще-системный счет 47404840399003202619, вид Активный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47404978999003202619', 'AccCat_47404978999003202619', 'Должен быть обще-системный счет 47404978999003202619, вид Активный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47404156999003202619', 'AccCat_47404156999003202619', 'Должен быть обще-системный счет 47404156999003202619, вид Активный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47404810799003202621', 'AccCat_47404810799003202621', 'Должен быть обще-системный счет 47404810799003202621, вид Активный', 4);
  -- Пассивные
  addTest ( 'acc_tests.mac', 'AccCat_47403810199003202617', 'AccCat_47403810199003202617', 'Должен быть обще-системный счет 47403810199003202617, вид Пассивный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47403840499003202617', 'AccCat_47403840499003202617', 'Должен быть обще-системный счет 47403840499003202617, вид Пассивный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47403978099003202617', 'AccCat_47403978099003202617', 'Должен быть обще-системный счет 47403978099003202617, вид Пассивный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47403156099003202617', 'AccCat_47403156099003202617', 'Должен быть обще-системный счет 47403156099003202617, вид Пассивный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47403810799003202619', 'AccCat_47403810799003202619', 'Должен быть обще-системный счет 47403810799003202619, вид Пассивный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47403840099003202619', 'AccCat_47403840099003202619', 'Должен быть обще-системный счет 47403840099003202619, вид Пассивный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47403978699003202619', 'AccCat_47403978699003202619', 'Должен быть обще-системный счет 47403978699003202619, вид Пассивный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47403156699003202619', 'AccCat_47403156699003202619', 'Должен быть обще-системный счет 47403156699003202619, вид Пассивный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47403810499003202621', 'AccCat_47403810499003202621', 'Должен быть обще-системный счет 47403810499003202621, вид Пассивный', 4);
  -- Всего добавляется 18 авто-тестов
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
