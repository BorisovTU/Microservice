  /************************************************\
    Скрипт перекомпиляции всех инвалидных обьектов
  \************************************************/

declare 
  v_cmd varchar2(1000);
begin
  for c1 in (select o.OBJECT_NAME
               from user_objects o
              where (1=1) 
                and o.OBJECT_TYPE = 'PACKAGE'
                and o.status = 'INVALID')
  loop
    begin
      v_cmd := 'alter package '||c1.object_name||' compile';
      execute immediate v_cmd;
    exception
      when others then 
        null;
    end;    
  end loop;

  for c1 in (select  o.OBJECT_NAME
               from user_objects o
              where (1=1) 
                and o.OBJECT_TYPE = 'FUNCTION'
                and o.status = 'INVALID' )
  loop
    begin
      v_cmd := 'alter function '||c1.object_name||' compile ';
      execute immediate v_cmd;
    exception
      when others then 
        null;
    end;    
  end loop;

  for c1 in (select  o.OBJECT_NAME
               from user_objects o
              where (1=1) 
                and o.OBJECT_TYPE = 'PROCEDURE'
                and o.status = 'INVALID' )
  loop
    begin
      v_cmd := 'alter procedure '||c1.object_name||' compile ';
      execute immediate v_cmd;
    exception
      when others then 
        null;
    end;    
  end loop;

  for c1 in (select  o.OBJECT_NAME
               from user_objects o
              where (1=1) 
                and o.OBJECT_TYPE = 'VIEW'
                and o.status = 'INVALID' )
  loop
    begin
      v_cmd := 'alter view '||c1.object_name||' compile ';
      execute immediate v_cmd;
    exception
      when others then 
        null;
    end;    
  end loop;

  for c1 in (select o.OBJECT_NAME
               from user_objects o
              where (1=1) 
                and o.OBJECT_TYPE = 'PACKAGE BODY'
                and o.status = 'INVALID' 
            )
  loop
    begin
      v_cmd := 'alter package '||c1.object_name||' compile body';
      execute immediate v_cmd;
    exception
      when others then 
        null;
    end;    
  end loop;

  for c1 in (select  o.OBJECT_NAME
               from user_objects o
              where (1=1) 
                and o.OBJECT_TYPE = 'TRIGGER'
                and o.status = 'INVALID' )
  loop
    begin
      v_cmd := 'alter trigger '||c1.object_name||' compile ';
      execute immediate v_cmd;
    exception
      when others then 
        null;
    end;    
  end loop;


 DBMS_UTILITY.compile_schema(sys_context( 'userenv', 'current_schema' ),false);

end;
/
