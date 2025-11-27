-- исправление записей

declare
begin
  update DCNVOPRTYPE_DBT set T_PANELCLASS = chr(1) where T_PANELCLASS = chr(0);
  update DCNVOPRTYPE_DBT set T_PANELMACRO = chr(1) where T_PANELMACRO = chr(0);
  update DCNVOPRTYPE_DBT set T_SERVICE = chr(1) where T_SERVICE = chr(0);
end;
/