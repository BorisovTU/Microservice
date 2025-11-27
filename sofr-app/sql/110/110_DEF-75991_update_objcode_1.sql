begin
  update dobjcode_dbt n
     set n.t_bankdate = n.t_bankdate + 1 
   where n.t_codekind = 104 
     and n.t_objecttype = 3
     and n.t_code in ('1','3','4')
     and n.t_bankdate = (select max(n1.t_bankdate)
                           from dobjcode_dbt n1
                          where n1.t_codekind = n.t_codekind
                            and n1.t_objecttype = n.t_objecttype
                            and n1.t_code = n.t_code);
end;
/

begin
  update dobjcode_dbt 
     set t_bankclosedate = t_bankclosedate + 1 
   where t_codekind = 104 
     and t_objecttype = 3
     and t_bankclosedate <> to_date('01.01.0001','dd.mm.yyyy');
end;
/

begin
  update dobjcode_dbt n
     set n.t_bankclosedate = to_date('22.11.2023','dd.mm.yyyy')
   where n.t_codekind = 104 
     and n.t_objecttype = 3
     and n.t_code = '8'
     and n.t_bankdate = (select max(n1.t_bankdate)
                           from dobjcode_dbt n1
                          where n1.t_codekind = n.t_codekind
                            and n1.t_objecttype = n.t_objecttype
                            and n1.t_code = n.t_code);
end;
/

begin
  update dobjcode_dbt 
     set t_bankdate = decode(t_code, '13', to_date('09.07.2024','dd.mm.yyyy'), to_date('22.11.2023','dd.mm.yyyy')) 
   where t_codekind = 104 
     and t_objecttype = 3
     and t_code in ('11','13');
end;
/