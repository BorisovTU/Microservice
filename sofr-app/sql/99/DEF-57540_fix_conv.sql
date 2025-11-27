-- исправление записей

declare
begin
  update DCNVOPRTYPE_DBT set T_PANELCLASS = chr(1), T_PANELMACRO = chr(1) where T_PANELCLASS = chr(0) or T_PANELMACRO = chr(0);
end;
/