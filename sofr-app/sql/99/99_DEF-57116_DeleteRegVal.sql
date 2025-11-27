/*Удалить сохраненный фильтр*/
declare
   v_ICaseItem NUMBER := 20212;
   v_KeyID NUMBER := 0;
begin
     
   DBMS_OUTPUT.PUT_LINE('v_ICaseItem = ' || v_ICaseItem);
     
   v_KeyID := RSB_COMMON.GETREGPARM('COMMON/FILTERS/S/' || v_ICaseItem || '__NPTXTEFL');
   DBMS_OUTPUT.PUT_LINE('v_KeyID = ' || v_KeyID);
   IF v_KeyID > 0
   THEN
      delete from dregval_dbt where t_KeyID = v_KeyID;
   END IF;
   
   COMMIT;
exception
   when NO_DATA_FOUND
   then DBMS_OUTPUT.PUT_LINE('NO_DATA_FOUND');    
end;
/