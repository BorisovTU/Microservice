  /**************************************************************************************************\
  Скрипт перекомпиляции всех инвалидных пакетов IT_% в схеме RSHB_SOFR%
  **************************************************************************************************
  Изменения:
  ---------------------------------------------------------------------------------------------------
  Дата        Автор            Jira                          Описание 
  ----------  ---------------  ---------------------------   ----------------------------------------
  03.08.2022  Зотов Ю.Н.                                     Создание
              
 */

declare 
  -- Local variables here
  v_ddl varchar2(1000);
begin
  -- Test statements here
  for c1 in (select o.OWNER, o.OBJECT_NAME
               from all_objects o
              where o.OWNER like 'RSHB_SOFR%'
                and o.OBJECT_TYPE = 'PACKAGE'
                and o.status = 'INVALID'
                and o.OBJECT_NAME like 'IT_%'
            )
  loop
    begin
      v_ddl := 'alter package '||c1.owner||'.'||c1.object_name||' compile';
      --dbms_output.put_line(v_ddl);
      execute immediate v_ddl;
    exception
      when others then 
        null;
    end;    
  end loop;

  for c1 in (select * 
               from all_objects o
              where o.OWNER like 'RSHB_SOFR%'
                and o.OBJECT_TYPE = 'VIEW'
                and o.status = 'INVALID'
                and o.OBJECT_NAME like 'ITV_%'
            )
  loop
    begin
      v_ddl := 'alter view '||c1.owner||'.'||c1.object_name||' compile';
      --dbms_output.put_line(v_ddl);
      execute immediate v_ddl;
    exception
      when others then 
        null;
    end;    
  end loop;

  for c1 in (select * 
               from all_objects o
              where o.OWNER like 'RSHB_SOFR%'
                and o.OBJECT_TYPE = 'TRIGGER'
                and o.status = 'INVALID'
                and o.OBJECT_NAME like 'ITT_%'
            )
  loop
    begin
      v_ddl := 'alter trigger '||c1.owner||'.'||c1.object_name||' compile';
      --dbms_output.put_line(v_ddl);
      execute immediate v_ddl;
    exception
      when others then 
        null;
    end;    
  end loop;

  for c1 in (select * 
               from all_objects o
              where o.OWNER like 'RSHB_SOFR%'
                and o.OBJECT_TYPE = 'PACKAGE BODY'
                and o.status = 'INVALID'
                and o.OBJECT_NAME like 'IT_%'
            )
  loop
    begin
      v_ddl := 'alter package '||c1.owner||'.'||c1.object_name||' compile body';
      --dbms_output.put_line(v_ddl);
      execute immediate v_ddl;
    exception
      when others then 
        null;
    end;    
  end loop;



end;
/