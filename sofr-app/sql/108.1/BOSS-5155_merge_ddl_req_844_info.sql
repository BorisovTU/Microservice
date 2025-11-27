begin
  merge into ddl_req_844_info i
  using (select r.t_id,
               case when r.t_status = 'W'
                    then -1
                 else 1
               end ready_for_deals,
               case when r.t_fiid = 1580  then 86698.64
                    when r.t_fiid = 54353 then 10520.91
                    when r.t_fiid = 55355 then 28746.30
                    when r.t_fiid = 54412 then 11418.34
                  else r.t_price
               end price_beg,
               case when r.t_fiid = 1580  then r.t_amount/10
                    when r.t_fiid = 54353 then r.t_amount/2
                    when r.t_fiid = 55355 then r.t_amount/2
                    when r.t_fiid = 54412 then r.t_amount/4
                  else r.t_amount
               end qty_beg
          from ddl_req_dbt r
         where r.t_codeTS like 'BlockedOTC%'
           and r.t_kind = 350) r
  on (i.req_id = r.t_id)
  when not matched then insert (req_id,
                                ready_for_deals,
                                price_beg,
                                qty_beg)
  values (r.t_id,
          r.ready_for_deals,
          r.price_beg,
          r.qty_beg);

  update d_otcdealtmp_dbt d
     set d.t_value_fact = d.t_price_end*d.t_qty_fact
   where d.t_value_fact != d.t_price_end*d.t_qty_fact;


  update d_otcdealtmp_dbt d
      set d.t_error = 100008
    where d.t_dealid = -1
      and d.t_qty_fact = 0;
end;