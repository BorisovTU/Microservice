--Установка значения UNSET_CHAR признака "Технический" на тех объектах НДР, у которых он был NULL
begin
   execute immediate 'ALTER TRIGGER NPTXOBJ_DBT_TIUD DISABLE';
   begin
      update dnptxobj_dbt
      set t_Technical = chr(0)
      where t_Technical is NULL;

      it_log.log('DEF-5369: обновлены записи в таблице DNPTXOBJ_DBT');
   exception
      when NO_DATA_FOUND
      then it_log.log('DEF-53691: в табл. DNPTXOBJ_DBT не найдены записи с t_Technical = NULL');
   end;
   execute immediate 'ALTER TRIGGER NPTXOBJ_DBT_TIUD ENABLE';

   update dnptxobjbc_dbt
   set t_Technical = chr(0)
   where t_Technical is NULL;

   it_log.log('DEF-5369: обновлены записи в таблице DNPTXOBJBC_DBT');

   commit;
exception
   when NO_DATA_FOUND
   then it_log.log('DEF-53691: в табл. DNPTXOBJBC_DBT не найдены записи с t_Technical = NULL');
end;
/