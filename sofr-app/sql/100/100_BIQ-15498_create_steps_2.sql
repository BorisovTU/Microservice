--- создание шага операции списания ДС
DECLARE
BEGIN
  begin
    delete from doprostep_dbt t where t.t_blockid = 203702 
      and  t.t_number_step = 105 and t.t_kind_action = 1;
  exception 
    when others then 
      it_log.log('Ошибка обновления шага 105');
  end;

  INSERT INTO doprostep_dbt (
  T_BLOCKID,T_NUMBER_STEP,T_KIND_ACTION,T_DAYOFFSET,T_SCALE,T_DAYFLAG,
  T_CALENDARID,T_SYMBOL,T_PREVIOUS_STEP,T_MODIFICATION,T_CARRY_MACRO,T_PRINT_MACRO,
  T_POST_MACRO,T_NOTINUSE,T_FIRSTSTEP,T_NAME,T_DATEKINDID,T_REV,T_AUTOEXECUTESTEP,
  T_ONLYHANDCARRY,T_ISALLOWFOROPER,T_OPERORGROUP,T_RESTRICTEARLYEXECUTION,
  T_USERTYPES,T_INITDATEKINDID,T_ASKFORDATE,T_BACKOUT,T_ISBACKOUTGROUP,
  T_MASSEXECUTEMODE,T_ISCASE,T_ISDISTAFFEXECUTE,T_SKIPINITAFTERPLANDATE,T_MASSPACKSIZE) 
  VALUES(203702,105,1,0,0,CHR(0),0,chr(0),100,0,'nptxwrt105_203702.mac',chr(1),'nptxwrt105_203702.mac',
  CHR(0),CHR(0),'Отправка неторгового поручения в QUIK',460700000,CHR(0),CHR(88),CHR(0),0,
  CHR(0),CHR(88),chr(1),0,CHR(0),0,CHR(0),0,CHR(0),CHR(0),chr(88),0);
  COMMIT;
  
EXCEPTION
   WHEN OTHERS THEN it_log.log('Ошибка вставки шага Отправка неторгового поручения в QUIK');
END;
/

DECLARE
BEGIN
  UPDATE doprostep_dbt SET T_PREVIOUS_STEP = 107 where T_BLOCKID = 203702 and T_NUMBER_STEP = 110;
  COMMIT;
EXCEPTION
   WHEN OTHERS THEN  it_log.log('Ошибка вставки установки предыдущего шага блока 203702');
END; 
/

DECLARE
BEGIN
  begin
    delete from doprostep_dbt t where t.t_blockid = 203702 
      and  t.t_number_step = 107 and t.t_kind_action = 1;
  exception 
    when others then 
      it_log.log('Ошибка обновления шага 107');
  end;
    
  INSERT INTO doprostep_dbt ( 
  T_BLOCKID,T_NUMBER_STEP,T_KIND_ACTION,T_DAYOFFSET,T_SCALE,T_DAYFLAG,
  T_CALENDARID,T_SYMBOL,T_PREVIOUS_STEP,T_MODIFICATION,T_CARRY_MACRO,T_PRINT_MACRO,
  T_POST_MACRO,T_NOTINUSE,T_FIRSTSTEP,T_NAME,T_DATEKINDID,T_REV,T_AUTOEXECUTESTEP,
  T_ONLYHANDCARRY,T_ISALLOWFOROPER,T_OPERORGROUP,T_RESTRICTEARLYEXECUTION,
  T_USERTYPES,T_INITDATEKINDID,T_ASKFORDATE,T_BACKOUT,T_ISBACKOUTGROUP,
  T_MASSEXECUTEMODE,T_ISCASE,T_ISDISTAFFEXECUTE,T_SKIPINITAFTERPLANDATE,T_MASSPACKSIZE) 
  VALUES(203702,107,1,0,0,CHR(0),0,chr(0),105,0,'nptxwrt107_203702.mac',CHR(1),'nptxwrt107_203702.mac',
  CHR(0),CHR(0),'Получение результата обработки неторгового поручения в QUIK',460700000,CHR(0),chr(88),
  CHR(0),0,CHR(0),CHR(88),CHR(1),0,CHR(0),0,CHR(0),0,CHR(0),CHR(0),CHR(88),0);
  
  COMMIT;
  
EXCEPTION
   WHEN OTHERS THEN  it_log.log('Ошибка вставки шага Получение результата обработки неторгового поручения в QUIK');
END;
/
