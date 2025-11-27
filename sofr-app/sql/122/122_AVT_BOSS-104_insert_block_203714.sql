/*
Добавление нового блока 203714 Отказ от исполнения проводок
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
  SELECT 
    203714,'Отказ от исполнения проводок', 4607, 0, chr(0), chr(0), 0
    FROM dual
WHERE NOT EXISTS ( SELECT 1 FROM doprblock_dbt WHERE t_blockid = 203714);
    
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
SELECT 203714,   190,   1,   0,  0,  chr(0),  0, chr(0), 0,0,  'nptxwrt190.mac',  chr(0), chr(0),   
chr(0),  chr(0),  'Отказ от исполнения',  0,  chr(0), 'X',  chr(0), 0, chr(0), 'X', chr(0), 0, chr(0), 0, chr(0), 0, chr(0),  chr(0),'X',0
FROM dual
WHERE NOT EXISTS ( SELECT 1 FROM doprostep_dbt WHERE t_blockid = 203714 AND t_number_step = 190);


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
  SELECT
    0, 2037, 203714, 1, chr(0), chr(0), chr(0),'X', chr(0),chr(0), chr(0)
    FROM dual
WHERE NOT EXISTS ( SELECT 1 FROM doproblck_dbt WHERE    T_KIND_OPERATION = 2037 AND T_BLOCKID = 203714);
   
  select T_OPERBLOCKID into  OPERBLOCKID from doproblck_dbt where t_blockid = 203714;
  INSERT INTO doprcblck_dbt (T_OPERBLOCKID,T_STATUSKINDID,T_NUMVALUE,T_CONDITION) SELECT OPERBLOCKID,484131,3,0
  FROM dual
WHERE NOT EXISTS ( SELECT 1 FROM doprcblck_dbt WHERE T_STATUSKINDID = 484131 AND T_OPERBLOCKID = OPERBLOCKID and T_NUMVALUE = 3);

END;
/