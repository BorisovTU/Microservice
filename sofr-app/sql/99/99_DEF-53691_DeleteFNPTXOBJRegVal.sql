--Удаление значения настройки "COMMON/FILTERS/S/16823__FNPTXOBJ" для корректного отображения фильтра скроллинга объектов НДР.
declare
   v_KeyID NUMBER := 0;
begin
   v_KeyID := RSB_COMMON.GETREGPARM('COMMON/FILTERS/S/16823__FNPTXOBJ');
   if v_KeyID > 0
   then
      begin
         delete from dregval_dbt where t_KeyID = v_KeyID;
         IT_LOG.LOG('DEF-53691 nptxobj filter regval was deleted');
      exception
         when NO_DATA_FOUND
         then IT_LOG.LOG('DEF-53691 nptxobj filter regval not found');
      end;
   end if; 
   
   COMMIT;
end;
/