begin
  merge into ddlmarketacc_dbt a
  using (
        with  accounts as (
                  select 25 /*КлФондовыйТ+_ЕП*/ as market_id,           'SLV' as cur_code, '30411A99860000006263' as acc from dual union all
                  select 25 /*КлФондовыйТ+_ЕП*/ as market_id,           'GLD' as cur_code, '30411A98560000006263' as acc from dual union all
                  select 34 /*КлФондовыйТ+_ЕП_обособлен*/ as market_id, 'SLV' as cur_code, '30411A99160000025834' as acc from dual union all
                  select 34 /*КлФондовыйТ+_ЕП_обособлен*/ as market_id, 'GLD' as cur_code, '30411A98860000025834' as acc from dual
                  )
           select a.market_id,
                  f.t_fiid,
                  a.acc
             from accounts a
             join dfininstr_dbt f on f.t_ccy = a.cur_code and f.t_fi_kind = 1
       ) t
  on (a.t_marketid = t.market_id and
      a.t_code_currency = t.t_fiid and
      a.t_account = t.acc)
  when not matched then
  insert (t_marketid,
          t_code_currency,
          t_account,
          t_typeown)
  values (t.market_id,
          t.t_fiid,
          t.acc,
          2);
end;
/
