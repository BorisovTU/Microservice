CREATE OR REPLACE PACKAGE IT_Moex IS

  /*Загрузка из JSON-структуры данных НКД и номинала на конец месяца, приходящийся на выходной день*/
  PROCEDURE SaveNKD( p_Body IN CLOB, p_Date IN DATE, p_ErrorMessage OUT VARCHAR2 );

END IT_Moex;