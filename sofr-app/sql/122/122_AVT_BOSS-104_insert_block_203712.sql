/*
Добавление нового блока 203712 Проверки для зачислений по индивидуальным РК ЮЛ
*/

DECLARE 
      OPERBLOCKID integer;
BEGIN 
  INSERT INTO doprblock_dbt
    (T_BLOCKID,
     T_NAME,
     T_DOCKIND,
     T_PARENT,
     T_UPGRADE,
     T_VERSION,
     T_VERSIONWEB)
  SELECT 203712, 'Проверки для зачислений по индивидуальным РК ЮЛ', 4607, 0, chr(0), chr(0), 0
  FROM dual
  WHERE NOT EXISTS (SELECT 1 FROM doprblock_dbt WHERE T_BLOCKID = 203712);
    
  INSERT INTO doprostep_dbt (
    t_blockid, t_number_step, t_kind_action, t_dayoffset, t_scale, t_dayflag, t_calendarid,
    t_symbol, t_previous_step, t_modification,
    t_carry_macro, t_print_macro, t_post_macro,
    t_notinuse, t_firststep, t_name,
    t_datekindid, t_rev, t_autoexecutestep, t_onlyhandcarry,
    t_isallowforoper, t_operorgroup, t_restrictearlyexecution, t_usertypes,
    t_initdatekindid, t_askfordate, t_backout, t_isbackoutgroup,
    t_massexecutemode, t_iscase, t_isdistaffexecute, t_skipinitafterplandate, t_masspacksize
)
  SELECT    203712,   150,   1,   0,  0,  chr(0),  0, chr(0), 0,0,  'nptxwrt150.mac',  chr(0), chr(0),   
  chr(0),  chr(0),  'Проверка суммы зачисления',  0,  chr(0), 'X',  chr(0), 0, chr(0), 'X', chr(0), 0, chr(0), 0, chr(0), 0, chr(0),  chr(0),'X', 0 
  FROM dual
  WHERE NOT EXISTS ( SELECT 1 FROM doprostep_dbt WHERE t_blockid = 203712 AND t_number_step = 150);

  INSERT INTO doprostep_dbt (
    t_blockid, t_number_step, t_kind_action, t_dayoffset, t_scale, t_dayflag, t_calendarid,
    t_symbol, t_previous_step, t_modification,
    t_carry_macro, t_print_macro, t_post_macro,
    t_notinuse, t_firststep, t_name,
    t_datekindid, t_rev, t_autoexecutestep, t_onlyhandcarry,
    t_isallowforoper, t_operorgroup, t_restrictearlyexecution, t_usertypes,
    t_initdatekindid, t_askfordate, t_backout, t_isbackoutgroup,
    t_massexecutemode, t_iscase, t_isdistaffexecute, t_skipinitafterplandate, t_masspacksize
)
SELECT   203712,   160,   1,   0,  0,  chr(0),  0, chr(0), 150,0,  'nptxwrt160.mac',  chr(0), chr(0),   
chr(0),  chr(0),  'Отправка подкрепления в ЦФТ',  0,  chr(0), 'X',  chr(0), 0, chr(0), 'X', chr(0), 0, chr(0), 0, chr(0), 0, chr(0),  chr(0),'X',0
FROM dual
WHERE NOT EXISTS ( SELECT 1 FROM doprostep_dbt WHERE t_blockid = 203712 AND t_number_step = 160);


INSERT INTO doprostep_dbt (
    t_blockid, t_number_step, t_kind_action, t_dayoffset, t_scale, t_dayflag, t_calendarid,
    t_symbol, t_previous_step, t_modification,
    t_carry_macro, t_print_macro, t_post_macro,
    t_notinuse, t_firststep, t_name,
    t_datekindid, t_rev, t_autoexecutestep, t_onlyhandcarry,
    t_isallowforoper, t_operorgroup, t_restrictearlyexecution, t_usertypes,
    t_initdatekindid, t_askfordate, t_backout, t_isbackoutgroup,
    t_massexecutemode, t_iscase, t_isdistaffexecute, t_skipinitafterplandate, t_masspacksize
)
SELECT   203712,   170,   1,   0,  0,  chr(0),  0, chr(0), 160,0,  'nptxwrt170.mac',  chr(0), chr(0),   
chr(0),  chr(0),  'Ожидание ответа из ЦФТ',  0,  chr(0), 'X',  chr(0), 0, chr(0), 'X', chr(0), 0, chr(0), 0, chr(0), 0, chr(0),  chr(0),'X',0
FROM dual
WHERE NOT EXISTS ( SELECT 1 FROM doprostep_dbt WHERE t_blockid = 203712 AND t_number_step = 170);

  INSERT INTO doproblck_dbt
    (T_OPERBLOCKID,
     T_KIND_OPERATION,
     T_BLOCKID,
     T_SORT,
     T_NOTINUSE,
     T_NOINSERT,
     T_NOREPLACE,
     T_NOCLOSEINSERT,
     T_ISMANUAL,
     T_SYMBOLSFORINSERTION,
     T_SYMBOL)
  SELECT 0, 2037, 203712, 1, chr(0), chr(0), chr(0),'X', chr(0),chr(0), chr(0)
  FROM dual
  WHERE NOT EXISTS ( SELECT 1 FROM doproblck_dbt WHERE T_KIND_OPERATION = 2037 AND T_BLOCKID = 203712);
   
  select T_OPERBLOCKID into  OPERBLOCKID from doproblck_dbt where t_blockid = 203712;

  INSERT INTO doprcblck_dbt (T_OPERBLOCKID,T_STATUSKINDID,T_NUMVALUE,T_CONDITION) SELECT OPERBLOCKID,484131,1,0
  FROM dual
  WHERE NOT EXISTS ( SELECT 1 FROM doprcblck_dbt WHERE T_OPERBLOCKID = OPERBLOCKID AND T_STATUSKINDID = 484131 and T_NUMVALUE = 1);

  
INSERT INTO doprsblck_dbt (t_blockid, t_statuskindid, t_numvalue, t_default)
SELECT 203700, 484131, 1, chr(0)
FROM dual 
WHERE NOT EXISTS ( SELECT 1 FROM doprsblck_dbt WHERE t_blockid = 203700 AND t_statuskindid = 484131 AND t_numvalue = 1);
    
INSERT INTO doprsblck_dbt (t_blockid, t_statuskindid, t_numvalue, t_default)
SELECT 203700, 484131, 2, chr(0)
FROM dual 
WHERE NOT EXISTS ( SELECT 1 FROM doprsblck_dbt WHERE t_blockid = 203700 AND t_statuskindid = 484131 AND t_numvalue = 2);

INSERT INTO doprsblck_dbt (t_blockid, t_statuskindid, t_numvalue, t_default)
SELECT 203712, 484131, 2, chr(0)
FROM dual 
WHERE NOT EXISTS ( SELECT 1 FROM doprsblck_dbt WHERE t_blockid = 203712 AND t_statuskindid = 484131 AND t_numvalue = 2);
    
INSERT INTO doprsblck_dbt (t_blockid, t_statuskindid, t_numvalue, t_default)
SELECT 203712, 484131, 3, chr(0)
FROM dual 
WHERE NOT EXISTS ( SELECT 1 FROM doprsblck_dbt WHERE t_blockid = 203712 AND t_statuskindid = 484131 AND t_numvalue = 3);

END;
/