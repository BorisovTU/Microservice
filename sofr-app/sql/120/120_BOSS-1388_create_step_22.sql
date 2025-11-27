--Операция: Списание ценных бумаг ПКО
--Создание Шага: Отправка сообщения в Diasoft 
DECLARE
BEGIN
  begin
    delete from doprostep_dbt t where t.t_blockid = 40269072 
      and  t.t_number_step = 22 and t.t_kind_action = 1;
  exception 
    when others then 
      it_log.log('Ошибка обновления шага 22');
  end;

  INSERT INTO doprostep_dbt (
  T_BLOCKID,T_NUMBER_STEP,T_KIND_ACTION,T_DAYOFFSET,T_SCALE,T_DAYFLAG,
  T_CALENDARID,T_SYMBOL,T_PREVIOUS_STEP,T_MODIFICATION,T_CARRY_MACRO,T_PRINT_MACRO,
  T_POST_MACRO,T_NOTINUSE,T_FIRSTSTEP,T_NAME,T_DATEKINDID,T_REV,T_AUTOEXECUTESTEP,
  T_ONLYHANDCARRY,T_ISALLOWFOROPER,T_OPERORGROUP,T_RESTRICTEARLYEXECUTION,
  T_USERTYPES,T_INITDATEKINDID,T_ASKFORDATE,T_BACKOUT,T_ISBACKOUTGROUP,
  T_MASSEXECUTEMODE,T_ISCASE,T_ISDISTAFFEXECUTE,T_SKIPINITAFTERPLANDATE,T_MASSPACKSIZE) 
  VALUES(40269072,22,1,0,0,CHR(0),0,chr(0),21,0,'wrtavr22PKO.mac',chr(1),chr(1),
  CHR(0),CHR(0),'Отправка сообщения в Diasoft',12700000,CHR(0),CHR(88),CHR(0),0,
  CHR(0),CHR(88),chr(1),0,CHR(0),0,CHR(0),0,CHR(0),CHR(0),chr(88),0);
  COMMIT;
  
EXCEPTION
   WHEN OTHERS THEN it_log.log('Ошибка вставки шага Отправка сообщения в Diasoft');
END;
/

DECLARE
BEGIN
  UPDATE doprostep_dbt SET T_PREVIOUS_STEP = 22 where T_BLOCKID = 40269072 and T_NUMBER_STEP = 23;
  COMMIT;
EXCEPTION
   WHEN OTHERS THEN  it_log.log('Ошибка вставки установки предыдущего шага блока 40269072');
END; 
/

