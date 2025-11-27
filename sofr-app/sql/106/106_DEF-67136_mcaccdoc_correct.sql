-- Изменения по DEF-67136
-- Корректировка типа счета (для активных общесистемных счетов по категориям '+Обеспечение' и 'Клиринговый счет' )
-- Были открыты с неверным t_kind_account (вместо А русской была заведена A английская)
DECLARE
  logID VARCHAR2(32) := 'DEF-67136';
  x_Cnt NUMBER;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
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
  -- 'Клиринговый счет'
  correctMcAccDoc('Клиринговый счет', 'А', '30424810499003202617');
  -- '+Обеспечение'
  correctMcAccDoc('+Обеспечение', 'А', '47404840299003001347'); -- в долларах
  correctMcAccDoc('+Обеспечение', 'А', '47404978899003001347'); -- в евро
  correctMcAccDoc('+Обеспечение', 'А', '47404156899003001347'); -- в юанях 
  -- Группа авто-тестов по виду
  addTest ( 'acc_tests.mac', 'AccCat_30424810499003202617', 'AccCat_30424810499003202617', 'Должен быть обще-системный счет 30424810499003202617, вид Активный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47404840299003001347', 'AccCat_47404840299003001347', 'Должен быть обще-системный счет 47404840299003001347, вид Активный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47404978899003001347', 'AccCat_47404978899003001347', 'Должен быть обще-системный счет 47404978899003001347, вид Активный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47404156899003001347', 'AccCat_47404156899003001347', 'Должен быть обще-системный счет 47404156899003001347, вид Активный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47403840999003001347', 'AccCat_47403840999003001347', 'Должен быть обще-системный счет 47403840999003001347, вид Пассивный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47403978599003001347', 'AccCat_47403978599003001347', 'Должен быть обще-системный счет 47403978599003001347, вид Пассивный', 4);
  addTest ( 'acc_tests.mac', 'AccCat_47403156599003001347', 'AccCat_47403156599003001347', 'Должен быть обще-системный счет 47403156599003001347, вид Пассивный', 4);
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
