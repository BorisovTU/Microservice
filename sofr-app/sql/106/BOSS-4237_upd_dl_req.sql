declare
  l_proc_name varchar2(50) := 'upd_req_boss_4237';

  function get_fiid (
    p_isin varchar2
  ) return number is
    l_fiid number;
  begin
    select av.t_fiid
      into l_fiid
      from davoiriss_dbt av
     where av.t_isin = p_isin;

    return l_fiid;
  end get_fiid;

  procedure upd_req (
    p_isin varchar2,
    p_koef number
  ) is
    l_fiid number(10);
  begin
    l_fiid := get_fiid(p_isin => p_isin);
    
    update ddl_req_dbt r
       set r.t_amount = r.t_amount * p_koef,
           r.t_price = round(r.t_price / p_koef, 2)
     where r.t_kind = 350
       and r.t_code like 'BlockedOTC%'
       and r.t_fiid = l_fiid;

    it_log.log_handle(p_object => l_proc_name,
                      p_msg    => 'isin: ' || p_isin || '; koef: ' || p_koef || '; updated: ' || sql%rowcount);
  end upd_req;
begin
  upd_req(p_isin => 'US67066G1040', p_koef => 10);
  upd_req(p_isin => 'US0320951017', p_koef => 2);
  upd_req(p_isin => 'US0684631080', p_koef => 4);

  commit;
exception
  when others then
    it_log.log_error(p_object => l_proc_name,
                     p_msg    => sqlerrm);
    rollback;
    raise;
end;
/
