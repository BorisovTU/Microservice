--DEF-50327 BIQ-7294. Отсутствуют задания планировщика "Повторная отправка" и "Пересчет"
--Создание заданий планировщика для системных модулей 20205.Пересчет СНОБ через планировщик и 20206.Повторная отправка событий СНОБ через планировщик 
declare
  nCount number := 0;
begin  
-- Задание планировщика: 20205.'Пересчет СНОБ(планировщик)'  
  begin
    select count(1) into nCount from DSHEDULE_DBT where t_comment like 'Пересчет СНОБ(планировщик)';

    if nCount = 0 then  
      INSERT INTO DSHEDULE_DBT (
         T_ID, T_CIDENTPROGRAM, T_EVENTTYPE, 
         T_PERIODICALEVENT, T_STARTDATE, T_STARTTIME, 
         T_ENDDATE, T_ENDTIME, T_NEXTDATE, 
         T_NEXTTIME, T_PERIODTYPE, T_PERIODLENGTH, 
         T_WORKDAYS, T_DAYSOFWEEK, T_DAYSOFMONTH, 
         T_STATUS, T_ACTION, T_PARMS, 
         T_PAUSED, T_COMMENT, T_SYSEVENTS, 
         T_DEPARTMENT, T_PRIORITY, T_OPENPHASE, 
         T_USEREVENTCODE, T_USEDAYEVENT, T_EVENTDAYORDER, 
         T_EVENTDAYKIND, T_EVENTPERIODTYPE, T_NOTIFYOFERROR, 
         T_NOTIFYOFCOMPLETION, T_ONEXACTTIME, T_EXACTTIME, 
         T_NOTIFYOFACTCOMPLETE, T_RUNACTIONSCHAIN) 
      SELECT 
      0,chr(131),chr(0),chr(88),
     to_date('01.11.2019 00:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     to_date('01.01.0001 12:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     to_date('01.01.0001 00:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     to_date('01.01.0001 00:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     to_date('01.11.2019 00:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     to_date('01.01.0001 06:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     3,1,chr(0),0,0,0,1,
     '-exec:20205',chr(88),'Пересчет СНОБ(планировщик)',
     0,1,0,0,chr(1),chr(0),0,0,0,chr(0),chr(0),chr(88),
     to_date('01.01.0001 06:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     chr(0),chr(0)
      FROM dual;
      commit;
      dbms_output.put_line('Ok. Задание планировщика ''Пересчет СНОБ(планировщик)'' создано');
    else
      dbms_output.put_line('Ok. Задание планировщика ''Пересчет СНОБ(планировщик)'' уже существует');
    end if;    
  exception when others then
    dbms_output.put_line('Ошибка при создании задания ''Пересчет СНОБ(планировщик)''');
  end;

-- Задание планировщика: 20206."Повторная отправка событий СНОБ(планировщик)"'
  begin
    select count(1) into nCount from DSHEDULE_DBT where t_comment like 'Повторная отправка событий СНОБ(планировщик)';

    if nCount = 0 then  
      INSERT INTO DSHEDULE_DBT (
         T_ID, T_CIDENTPROGRAM, T_EVENTTYPE, 
         T_PERIODICALEVENT, T_STARTDATE, T_STARTTIME, 
         T_ENDDATE, T_ENDTIME, T_NEXTDATE, 
         T_NEXTTIME, T_PERIODTYPE, T_PERIODLENGTH, 
         T_WORKDAYS, T_DAYSOFWEEK, T_DAYSOFMONTH, 
         T_STATUS, T_ACTION, T_PARMS, 
         T_PAUSED, T_COMMENT, T_SYSEVENTS, 
         T_DEPARTMENT, T_PRIORITY, T_OPENPHASE, 
         T_USEREVENTCODE, T_USEDAYEVENT, T_EVENTDAYORDER, 
         T_EVENTDAYKIND, T_EVENTPERIODTYPE, T_NOTIFYOFERROR, 
         T_NOTIFYOFCOMPLETION, T_ONEXACTTIME, T_EXACTTIME, 
         T_NOTIFYOFACTCOMPLETE, T_RUNACTIONSCHAIN) 
      SELECT 
      0,chr(131),chr(0),chr(88),
     to_date('01.11.2019 00:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     to_date('01.01.0001 12:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     to_date('01.01.0001 00:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     to_date('01.01.0001 00:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     to_date('01.11.2019 00:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     to_date('01.01.0001 06:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     3,1,chr(0),0,0,0,1,
     '-exec:20206',chr(88),'Повторная отправка событий СНОБ(планировщик)',
     0,1,0,0,chr(1),chr(0),0,0,0,chr(0),chr(0),chr(88),
     to_date('01.01.0001 06:00:00', 'DD.MM.YYYY HH24:MI:SS'),
     chr(0),chr(0)
      FROM dual;
      commit;
      dbms_output.put_line('Ok. Задание планировщика ''Повторная отправка событий СНОБ(планировщик)'' создано');
    else
      dbms_output.put_line('Ok. Задание планировщика ''Повторная отправка событий СНОБ(планировщик)'' уже существует');
    end if;    
  exception when others then
    dbms_output.put_line('Ошибка при создании задания ''Повторная отправка событий СНОБ(планировщик)''');
  end;
end;
/

