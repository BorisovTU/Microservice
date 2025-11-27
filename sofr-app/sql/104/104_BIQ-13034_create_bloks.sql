declare
  --Донастройка новой операции списания ЦБ 32743
 P_OPERBLOCKID_REJECT number;  --код нового блока "Отказ" в составе блоков новой операции списания
 P_OPERBLOCKID_GO number;      --код нового блока "Списание ЦБ" в составе блоков новой операции списания
begin

  --возможные статус на первичный документ 127 Списание/Зачисление ЦБ
  INSERT INTO doprstknd_dbt (T_STATUSKINDID,T_CODE,T_NAME,T_DOCKIND,T_SORT,T_ELIMINATED) 
   VALUES (484111,'СП','Списание',127,0,0);
  INSERT INTO doprstknd_dbt (T_STATUSKINDID,T_CODE,T_NAME,T_DOCKIND,T_SORT,T_ELIMINATED) 
   VALUES (484121,'ОТ','Отказ',127,0,0);
  --значения для новых статусов первичного документа 127
  INSERT INTO doprstval_dbt (T_STATUSKINDID,T_NUMVALUE,T_NAME,T_ELIMINATED) VALUES (484111,1,'Открыт',0);
  INSERT INTO doprstval_dbt (T_STATUSKINDID,T_NUMVALUE,T_NAME,T_ELIMINATED) VALUES (484111,2,'Закрыт',0);
  INSERT INTO doprstval_dbt (T_STATUSKINDID,T_NUMVALUE,T_NAME,T_ELIMINATED) VALUES (484111,3,'Отвергнут',0);
  INSERT INTO doprstval_dbt (T_STATUSKINDID,T_NUMVALUE,T_NAME,T_ELIMINATED) VALUES (484121,1,'Открыт',0);
  INSERT INTO doprstval_dbt (T_STATUSKINDID,T_NUMVALUE,T_NAME,T_ELIMINATED) VALUES (484121,2,'Закрыт',0);
  INSERT INTO doprstval_dbt (T_STATUSKINDID,T_NUMVALUE,T_NAME,T_ELIMINATED) VALUES (484121,3,'Отвергнут',0);

  --вставка блока Списание ЦБ в Блоки шагов
  INSERT INTO doprblock_dbt
    (T_BLOCKID,
     T_NAME,
     T_DOCKIND,
     T_PARENT,
     T_UPGRADE,
     T_VERSION,
     T_VERSIONWEB)
  VALUES
    (40269073, 'Списание ЦБ', 127, 0, chr(0), chr(1), 0) ;

  --вставка блока Отказ в Блоки шагов
  INSERT INTO doprblock_dbt
    (T_BLOCKID,
     T_NAME,
     T_DOCKIND,
     T_PARENT,
     T_UPGRADE,
     T_VERSION,
     T_VERSIONWEB)
  VALUES
    (40269074, 'Отказ операции списания ЦБ ', 127, 0, chr(0), chr(1), 0) ;
   
  --вставка блока Списание в состав блоков новой операции
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
  VALUES
    (0,         
    32743,     
    40269073,  
    2,         
    chr(0),         
    chr(0),         
    chr(0),         
    chr(88),        
    chr(0),         
    chr(1),         
    chr(105)
    ) returning T_OPERBLOCKID into P_OPERBLOCKID_GO ;
  --настройка блока Списание - сегменты
  INSERT INTO doprcblck_dbt (T_OPERBLOCKID,T_STATUSKINDID,T_NUMVALUE,T_CONDITION)
    VALUES (P_OPERBLOCKID_GO,484111,1,0);
  INSERT INTO doprcblck_dbt (T_OPERBLOCKID,T_STATUSKINDID,T_NUMVALUE,T_CONDITION)
    VALUES (P_OPERBLOCKID_GO,1271,2,0);

  --вставка блока Отказ  в состав блоков новой операции
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
  VALUES
    (0,         
    32743,     
    40269074,  
    3,         
    chr(0),         
    chr(0),         
    chr(0),         
    chr(88),        
    chr(0),         
    chr(1),         
    chr(120)
    ) returning T_OPERBLOCKID into P_OPERBLOCKID_REJECT ;
  --настройка блока Отказ - сегменты   
  INSERT INTO doprcblck_dbt (T_OPERBLOCKID,T_STATUSKINDID,T_NUMVALUE,T_CONDITION)
    VALUES (P_OPERBLOCKID_REJECT,484121,1,0);
  INSERT INTO doprcblck_dbt (T_OPERBLOCKID,T_STATUSKINDID,T_NUMVALUE,T_CONDITION)
    VALUES (P_OPERBLOCKID_REJECT,1271,2,0);

  --добавление шагов в новый блок Списание
  --шаг Закрытие
  INSERT INTO doprostep_dbt
    (T_BLOCKID,   T_NUMBER_STEP,   T_KIND_ACTION,   T_DAYOFFSET,   T_SCALE,   T_DAYFLAG,
     T_CALENDARID,   T_SYMBOL,   T_PREVIOUS_STEP,   T_MODIFICATION,   T_CARRY_MACRO,   T_PRINT_MACRO,
     T_POST_MACRO,   T_NOTINUSE,   T_FIRSTSTEP,   T_NAME,   T_DATEKINDID,   T_REV,   T_AUTOEXECUTESTEP,
     T_ONLYHANDCARRY,   T_ISALLOWFOROPER,   T_OPERORGROUP,   T_RESTRICTEARLYEXECUTION,   T_USERTYPES,
     T_INITDATEKINDID,   T_ASKFORDATE,   T_BACKOUT,   T_ISBACKOUTGROUP,   T_MASSEXECUTEMODE,   T_ISCASE,
     T_ISDISTAFFEXECUTE,   T_SKIPINITAFTERPLANDATE,   T_MASSPACKSIZE)
  VALUES
    (40269073   ,41   ,1   ,0   ,0   ,chr(88)                        
     ,0   ,chr(135)   ,0   ,0   ,'wrtavr40PKO.mac'     ,chr(1)                      
     ,'wrtavr40PKO.mac'   ,chr(0)   ,chr(0)   ,'Закрытие операции'       
     ,0   ,chr(0)   ,chr(88)   ,chr(0)   ,0                        
     ,chr(0)   ,chr(88)    ,chr(1)                         
     ,0   ,chr(0)   ,0   ,chr(0)   ,2   ,chr(0)   ,chr(0)   ,chr(88)  ,0                        
  );
  --шаг Списание ЦБ
  INSERT INTO doprostep_dbt
    (T_BLOCKID,   T_NUMBER_STEP,   T_KIND_ACTION,   T_DAYOFFSET,   T_SCALE,   T_DAYFLAG,
     T_CALENDARID,   T_SYMBOL,   T_PREVIOUS_STEP,   T_MODIFICATION,   T_CARRY_MACRO,   T_PRINT_MACRO,
     T_POST_MACRO,   T_NOTINUSE,   T_FIRSTSTEP,   T_NAME,   T_DATEKINDID,   T_REV,   T_AUTOEXECUTESTEP,
     T_ONLYHANDCARRY,   T_ISALLOWFOROPER,   T_OPERORGROUP,   T_RESTRICTEARLYEXECUTION,   T_USERTYPES,
     T_INITDATEKINDID,   T_ASKFORDATE,   T_BACKOUT,   T_ISBACKOUTGROUP,   T_MASSEXECUTEMODE,   T_ISCASE,
     T_ISDISTAFFEXECUTE,   T_SKIPINITAFTERPLANDATE,   T_MASSPACKSIZE)
  VALUES
    (40269073   ,31   ,1   ,0   ,0   ,chr(88)                        
     ,0   ,chr(0)   ,0   ,0   ,'wrtavr30PKO.mac'     ,chr(1)                      
     ,'wrtavr30PKO.mac'   ,chr(0)   ,chr(0)   ,'Списание ЦБ'       
     ,0   ,chr(0)   ,chr(88)   ,chr(0)   ,0                        
     ,chr(0)   ,chr(88)    ,chr(1)                         
     ,0   ,chr(0)   ,0   ,chr(0)   ,2   ,chr(0)   ,chr(0)   ,chr(88)  ,0                        
  );
  --добавление шагов в новый блок Отказ
  --шаг Отказ
  INSERT INTO doprostep_dbt
    (T_BLOCKID,   T_NUMBER_STEP,   T_KIND_ACTION,   T_DAYOFFSET,   T_SCALE,   T_DAYFLAG,
     T_CALENDARID,   T_SYMBOL,   T_PREVIOUS_STEP,   T_MODIFICATION,   T_CARRY_MACRO,   T_PRINT_MACRO,
     T_POST_MACRO,   T_NOTINUSE,   T_FIRSTSTEP,   T_NAME,   T_DATEKINDID,   T_REV,   T_AUTOEXECUTESTEP,
     T_ONLYHANDCARRY,   T_ISALLOWFOROPER,   T_OPERORGROUP,   T_RESTRICTEARLYEXECUTION,   T_USERTYPES,
     T_INITDATEKINDID,   T_ASKFORDATE,   T_BACKOUT,   T_ISBACKOUTGROUP,   T_MASSEXECUTEMODE,   T_ISCASE,
     T_ISDISTAFFEXECUTE,   T_SKIPINITAFTERPLANDATE,   T_MASSPACKSIZE)
  VALUES
    (40269074   ,29   ,1   ,0   ,0   ,chr(88)                        
     ,0   ,chr(0)   ,0   ,0   ,'wrtavr27PKO.mac'     ,chr(1)                      
     ,'wrtavr27PKO.mac'   ,chr(0)   ,chr(0)   ,'Отказ'       
     ,0   ,chr(0)   ,chr(88)   ,chr(0)   ,0                        
     ,chr(0)   ,chr(88)    ,chr(1)                         
     ,0   ,chr(0)   ,0   ,chr(0)   ,0   ,chr(0)   ,chr(0)   ,chr(88)  ,0                        
  );

  --Добавить возможные статусы основного блока новой операции списания ЦБ 32743
  --сегмент Отказ основного блока
  INSERT INTO doprsblck_dbt (T_BLOCKID,T_STATUSKINDID,T_NUMVALUE,T_DEFAULT)
    VALUES (40269072,484121,3,0);   
  INSERT INTO doprsblck_dbt (T_BLOCKID,T_STATUSKINDID,T_NUMVALUE,T_DEFAULT)
   VALUES (40269072,484121,2,0);   
  INSERT INTO doprsblck_dbt (T_BLOCKID,T_STATUSKINDID,T_NUMVALUE,T_DEFAULT)
   VALUES (40269072,484121,1,0);   
  --сегмент Списание ЦБ (основного блока)
  INSERT INTO doprsblck_dbt (T_BLOCKID,T_STATUSKINDID,T_NUMVALUE,T_DEFAULT)
   VALUES (40269072,484111,3,0);   
  INSERT INTO doprsblck_dbt (T_BLOCKID,T_STATUSKINDID,T_NUMVALUE,T_DEFAULT)
   VALUES (40269072,484111,2,0);   
  INSERT INTO doprsblck_dbt (T_BLOCKID,T_STATUSKINDID,T_NUMVALUE,T_DEFAULT)
   VALUES (40269072,484111,1,0);   

  --Возможный статус "Списание" блока "Списание ЦБ" новой операции списания ЦБ 32743
  INSERT INTO doprsblck_dbt (T_BLOCKID,T_STATUSKINDID,T_NUMVALUE,T_DEFAULT)
   VALUES (40269073,484111,2,chr(88));   
  --Возможный статус "Отказ" блока "Отказ" новой операции списания ЦБ 32743
  INSERT INTO doprsblck_dbt (T_BLOCKID,T_STATUSKINDID,T_NUMVALUE,T_DEFAULT)
   VALUES (40269074,484121,2,chr(88));   

  --Отключение ранее введенных новых шагов в основном блоке (перенесены в другие блоки) - TODO лучше удалить
  UPDATE doprostep_dbt SET T_NOTINUSE=chr(88) WHERE T_BLOCKID=40269072 AND T_NUMBER_STEP=27;
  --Отключение старого шага в основном блоке (перенесен в другой блоки)  - TODO лучше удалить
  UPDATE doprostep_dbt SET T_NOTINUSE=chr(88) WHERE T_BLOCKID=40269072 AND T_NUMBER_STEP=30;
  --Отключение старого шага в основном блоке (перенесен в другой блоки)  - TODO лучше удалить
  UPDATE doprostep_dbt SET T_NOTINUSE=chr(88) WHERE T_BLOCKID=40269072 AND T_NUMBER_STEP=40; 
  --Включение автоматичности для нового шага "Ожидание статуса обработки" 
  UPDATE doprostep_dbt SET T_AUTOEXECUTESTEP=chr(88) WHERE T_BLOCKID=40269072 AND T_NUMBER_STEP=23;

  COMMIT;
end;
