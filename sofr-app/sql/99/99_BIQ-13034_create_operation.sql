declare
  l_blockid integer;
  l_newoperid integer := 32743; -- 32743 
  stat integer;
  logID VARCHAR2(9) := 'BIQ-13034';

begin
  -- копирование новой операции из 2010 операции списания ЦБ
  RSI_RsbCopyOperation.CopySystemOperationFirstStep( 2010, l_newoperid, 1, stat, 0 );
  select t.t_blockid into l_blockid from doproblck_dbt t where t.t_kind_operation = l_newoperid;
  -- обновление шага Списания 
  UPDATE doprostep_dbt SET T_CARRY_MACRO='wrtavr30PKO.mac',
    T_POST_MACRO='wrtavr30PKO.mac',T_DATEKINDID=0,
    T_AUTOEXECUTESTEP=chr(0) WHERE T_BLOCKID=l_blockid AND T_NUMBER_STEP=30;
  -- обновление шага Закрытия  
  UPDATE doprostep_dbt SET T_CARRY_MACRO='wrtavr40PKO.mac',
    T_POST_MACRO='wrtavr40PKO.mac',T_DATEKINDID=0,
    T_AUTOEXECUTESTEP=chr(0) WHERE T_BLOCKID=l_blockid AND T_NUMBER_STEP=40;
  -- вставка шага Ожидания статуса обработки  
  INSERT INTO doprostep_dbt (T_BLOCKID,T_NUMBER_STEP,T_KIND_ACTION,T_DAYOFFSET,T_SCALE,T_DAYFLAG,T_CALENDARID,T_SYMBOL,T_PREVIOUS_STEP,T_MODIFICATION,T_CARRY_MACRO,T_PRINT_MACRO,T_POST_MACRO,T_NOTINUSE,
    T_FIRSTSTEP,T_NAME,T_DATEKINDID,T_REV,T_AUTOEXECUTESTEP,T_ONLYHANDCARRY,T_ISALLOWFOROPER,T_OPERORGROUP,T_RESTRICTEARLYEXECUTION,T_USERTYPES,T_INITDATEKINDID,T_ASKFORDATE,T_BACKOUT,T_ISBACKOUTGROUP,
    T_MASSEXECUTEMODE,T_ISCASE,T_ISDISTAFFEXECUTE,T_SKIPINITAFTERPLANDATE,T_MASSPACKSIZE) 
  VALUES (l_blockid,23,1,0,0,chr(0),0,chr(0),0,0,'wrtavr23PKO.mac','','wrtavr23PKO.mac',chr(0),chr(0),
    'Ожидание статуса обработки',0,chr(0),chr(88),chr(0),0,chr(0),chr(88),'',0,chr(0),0,chr(0),0,chr(0),chr(0),chr(88),0);
  -- вставка шага Отказа  
  INSERT INTO doprostep_dbt (T_BLOCKID,T_NUMBER_STEP,T_KIND_ACTION,T_DAYOFFSET,T_SCALE,T_DAYFLAG,T_CALENDARID,T_SYMBOL,T_PREVIOUS_STEP,T_MODIFICATION,T_CARRY_MACRO,T_PRINT_MACRO,T_POST_MACRO,T_NOTINUSE,
    T_FIRSTSTEP,T_NAME,T_DATEKINDID,T_REV,T_AUTOEXECUTESTEP,T_ONLYHANDCARRY,T_ISALLOWFOROPER,T_OPERORGROUP,T_RESTRICTEARLYEXECUTION,T_USERTYPES,T_INITDATEKINDID,T_ASKFORDATE,T_BACKOUT,T_ISBACKOUTGROUP,
    T_MASSEXECUTEMODE,T_ISCASE,T_ISDISTAFFEXECUTE,T_SKIPINITAFTERPLANDATE,T_MASSPACKSIZE) 
  VALUES (l_blockid,27,1,0,0,chr(0),0,chr(0),0,0,'wrtavr27PKO.mac','','wrtavr27PKO.mac',chr(0),chr(0),
    'Отказ',0,chr(0),chr(0),chr(0),0,chr(0),chr(88),'',0,chr(0),0,chr(0),0,chr(0),chr(0),chr(88),0);
  -- обновление имени операции  
  UPDATE doprkoper_dbt SET T_NAME='Списание ценных бумаг ПКО' WHERE T_KIND_OPERATION=l_newoperid AND T_DOCKIND=127;
  commit;
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
end;  
