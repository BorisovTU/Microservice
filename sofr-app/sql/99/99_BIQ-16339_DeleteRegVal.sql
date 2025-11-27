declare
   v_ICaseItem NUMBER := 0;
   v_KeyID NUMBER := 0;
begin
   select t_ICaseItem into v_ICaseItem
   from ditemsyst_dbt itemsyst
   where itemsyst.t_SZNameItem = 'Îïåðàöèè ðàñ÷åòà ÍÎÁ äëÿ ÍÄÔË'
     and t_CIdentProgram = 'S'
     and rownum = 1;
     
   DBMS_OUTPUT.PUT_LINE('v_ICaseItem = ' || v_ICaseItem);
     
   v_KeyID := RSB_COMMON.GETREGPARM('COMMON/FILTERS/S/' || v_ICaseItem || '__NPTXOFLT');
   DBMS_OUTPUT.PUT_LINE('v_KeyID = ' || v_KeyID);
   IF v_KeyID > 0
   THEN
      execute immediate 'delete from dregval_dbt where t_KeyID = ' || v_KeyID;
   END IF;
   
   COMMIT;
exception
   when NO_DATA_FOUND
   then DBMS_OUTPUT.PUT_LINE('NO_DATA_FOUND');
end;