begin
  for rec in (select r1.t_rateid t_rateid_Rub
                    ,r2.t_rateid t_rateid_InVal
                from dratedef_dbt  r1
                    ,dratedef_dbt  r2
                    ,dfininstr_dbt fin
               where r1.t_Type in (12) -- CC
                 and r1.t_Type = r2.t_Type -- Одинаковый вид курса
                 and r1.t_FIID != r2.t_FIID -- Разный котируемый Фи
                 and r1.t_fiid = 0
                 and (((r1.t_rate / power(10, r1.t_Point)) - (r2.t_rate / power(10, r2.t_Point))) / (r2.t_rate / power(10, r2.t_Point)) < 1) -- Если разница в курсе меньше 100%       
                 and r1.t_OtherFI = r2.t_OtherFI -- Одна бумага   
                 and fin.t_FIID = r1.t_OtherFI
              --FETCH FIRST 1 ROWS ONLY
              )
  loop
    update dratehist_dbt h
       set h.t_rateid = rec.t_rateid_InVal
     where h.t_rateid = rec.t_rateid_Rub
       and not exists (select 1
              from DRATEHIST_DBT H1
             where H1.t_rateid = REC.t_rateid_InVal
               and H1.T_SINCEDATE = H.T_SINCEDATE);
    delete from dratehist_dbt where t_rateid = rec.t_rateid_Rub;
    delete from dratedef_dbt where t_rateid = rec.t_rateid_Rub;
  end loop;
end;
/
