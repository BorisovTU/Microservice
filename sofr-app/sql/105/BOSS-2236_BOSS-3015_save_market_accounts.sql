begin
  merge into ddlMarketAcc_dbt a
using (
        with  accounts as (
                  select 25 as market_id, 'AED' as cur_code, '47405784190000006263' as acc from dual union all
                  select 25 as market_id, 'BYN' as cur_code, '47405933190000006263' as acc from dual union all
                  select 25 as market_id, 'HKD' as cur_code, '47405344190000006263' as acc from dual union all
                  select 25 as market_id, 'KZT' as cur_code, '47405398890000006263' as acc from dual union all
                  select 25 as market_id, 'TRY' as cur_code, '47405949090000006263' as acc from dual union all
                  select 34 as market_id, 'AED' as cur_code, '47405784490000025834' as acc from dual union all
                  select 34 as market_id, 'BYN' as cur_code, '47405933490000025834' as acc from dual union all
                  select 34 as market_id, 'HKD' as cur_code, '47405344490000025834' as acc from dual union all
                  select 34 as market_id, 'KZT' as cur_code, '47405398190000025834' as acc from dual union all
                  select 34 as market_id, 'TRY' as cur_code, '47405949390000025834' as acc from dual
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
  values(t.market_id,
         t.t_fiid,
         t.acc,
         2);
  commit;
end;
/
