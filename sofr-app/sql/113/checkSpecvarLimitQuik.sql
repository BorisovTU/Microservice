create or replace package checkSpecvarLimitQuik as
  /*
    @brief BIQ-15498 Спецпеременная для стопа на шаге 107 "Получение результата обработки неторгового поручения в QUIK"
    Операция списание ДС
  */
  MAKE_WHOLE_STEP constant number(2) default -1; -- выполнить тело шага
  PASS_STEP_WITHOUT_BODY constant number(2) default 1; -- пометить шаг выполненным, не выполняя тела шага, операция продолжает выполнение
  STOP_ON_STEP constant number(2) default 0; -- не выполнять шаг и встать на нем
  /* TODO можно добавить шаг SINGLE_STOP_ON_STEP - при считывании данной переменной один раз, если она равна SINGLESTOP, 
                                          она установится в MAKESTEP */
  makeStepInstrSendEnrollQuik107 integer :=MAKE_WHOLE_STEP ; -- не выполнять шаг 107 в операции зачисление д/с по-умолчанию
  function getI return integer; -- получить значение
  procedure setI(v_i integer);  -- установить значение
end checkSpecvarLimitQuik;
/
create or replace package body checkSpecvarLimitQuik as
  function getI return integer is
    begin
      return makeStepInstrSendEnrollQuik107;
    end;
  procedure setI(v_i integer) is
    begin
      makeStepInstrSendEnrollQuik107 := v_i;
    end;
end checkSpecvarLimitQuik;
/
