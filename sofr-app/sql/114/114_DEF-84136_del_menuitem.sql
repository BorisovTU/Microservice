/*Удалить пункты меню*/
DECLARE

BEGIN
  DELETE FROM DMENUITEM_DBT
   WHERE T_IPROGITEM = 83
     AND INSTR(t_sznameitem, 'Краткая справка по расчету НДФЛ') > 0;

  DELETE FROM DMENUITEM_DBT
   WHERE T_IPROGITEM = 83
     AND INSTR(t_sznameitem, 'Отчет с 01.01.2021') > 0;

END;
/