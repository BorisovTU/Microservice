create or replace function GetRate(pFromFI in number
                                  ,pToFI   in number
                                  ,pbdate  in date) return number deterministic is
  v_Rate     number := 0;
  v_RateID   number;
  v_RateDate date;
  v_RateFiid number;
begin
  if pToFI != 0
  then
    v_Rate := RSI_RSB_FIInstr.FI_GetRate(pFromFI, pToFI, 12, pbdate, pbdate - to_date('01.01.0001', 'DD.MM.YYYY'), 0, v_RateID, v_RateDate);
  end if;
  if (v_Rate <= 0)
  then
    v_Rate := RSI_RSB_FIInstr.FI_GetRate(pFromFI, -1, 12, pbdate, pbdate - to_date('01.01.0001', 'DD.MM.YYYY'), 0, v_RateID, v_RateDate);
  end if;
  if (v_RateID > 0)
  then
    begin
      select t_rate / power(10, t_point) / t_scale
            ,t_fiid
        into v_Rate
            ,v_RateFiid
        from dratedef_dbt
       where t_rateid = v_RateID
         and t_isinverse != chr(88)
         and t_sincedate = v_RateDate;
    exception
      when others then
        begin
          select t.t_rate / power(10, t.t_point) / t.t_scale
                ,defrate.t_fiid
            into v_Rate
                ,v_RateFiid
            from dratehist_dbt t
                ,dratedef_dbt  defrate
           where t.t_rateid = v_RateID
             and t.t_isinverse != chr(88)
             and
                --MZ             t.t_sincedate = (select max(v_RateDate) from dratehist_dbt where t_rateid = t.t_rateid and t_sincedate <= v_ratedate) and
                 t.t_sincedate = v_ratedate
             and t.t_rateid = defrate.t_rateid;
        exception
          when others then
            null;
        end;
    end;
  end if;
  if pToFI != v_RateFiid
  then
    v_Rate := RSB_FIInstr.ConvSum(v_Rate, v_RateFiid, pToFI, pbdate);
  end if;
  return v_Rate;
exception
  when others then
    return 0.0;
end;
/
