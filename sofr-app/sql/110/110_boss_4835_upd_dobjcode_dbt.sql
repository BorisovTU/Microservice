begin
  -- Трейдеры 
  update dobjcode_dbt set t_code = replace(t_code,'tr') where t_codekind = 104 and t_objecttype = 3 ;
end;
/