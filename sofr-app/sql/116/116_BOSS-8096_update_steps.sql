begin
  -- включить операцию 32743
  UPDATE doprkoper_dbt SET T_NOTINUSE=chr(0) WHERE T_KIND_OPERATION=32743 AND T_DOCKIND=127 AND T_INITMACRO='sp_doc.mac';

  -- планируемая дата шага 23 - Начало операции
  UPDATE doprostep_dbt SET T_DATEKINDID=12700000 WHERE T_BLOCKID=40269072 AND T_NUMBER_STEP=23; 

  -- планируемая дата шага 31 - Начало операции, а также "рабочие дни" отключены
  UPDATE doprostep_dbt SET T_DAYFLAG=chr(0),T_DATEKINDID=12700000 WHERE T_BLOCKID=40269073 AND T_NUMBER_STEP=31;

  -- планируемая дата шага 41 - Начало операции, а также "рабочие дни" отключены, а также отключить массовое исполнение
  UPDATE doprostep_dbt SET T_DAYFLAG=chr(0),T_DATEKINDID=12700000,T_MASSEXECUTEMODE=0 WHERE T_BLOCKID=40269073 AND T_NUMBER_STEP=41;

  -- планируемая дата шага 29 - Начало операции, а также "рабочие дни" отключены
  UPDATE doprostep_dbt SET T_DAYFLAG=chr(0),T_DATEKINDID=12700000 WHERE T_BLOCKID=40269074 AND T_NUMBER_STEP=29;

end;
