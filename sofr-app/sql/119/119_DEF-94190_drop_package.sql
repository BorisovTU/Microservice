  /************************************************\
    Скрипт удаления ненужного пакета RSHB_F303_2CHD
  \************************************************/

declare 
  v_cmd varchar2(1000);
begin
  for c1 in (select o.OBJECT_NAME from user_objects o
              where (1=1)
                and o.object_name = 'RSHB_F303_2CHD' 
                and o.OBJECT_TYPE = 'PACKAGE'
                )
  loop
    begin
      v_cmd := 'drop package '||c1.object_name;
      execute immediate v_cmd;
    exception
      when others then 
        null;
    end;    
  end loop;
end;
/
