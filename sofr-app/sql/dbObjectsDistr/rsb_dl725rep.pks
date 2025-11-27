CREATE OR REPLACE PACKAGE RSB_DL725REP
IS
  --Очистка промежуточных таблиц отчёта
  PROCEDURE ClearTables(pSessionId IN NUMBER, pPart IN NUMBER);

  -- Заполнение промежуточных таблиц раздела 1
  PROCEDURE FillTables_Part1(pSessionId IN NUMBER, pBegDate IN DATE, pEndDate IN DATE);
                        
END RSB_DL725REP;
/
