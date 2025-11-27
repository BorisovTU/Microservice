declare
  l_start_date date := to_date('01.01.2025', 'dd.mm.yyyy');
  l_end_date date := to_date('30.06.2025', 'dd.mm.yyyy');
  l_note_text varchar2(100) := '„ ”‹';

  l_cnt number(10) := 0;
begin
    for reqs in (
              select req.t_id,
                     req.t_date
                from ddl_req_dbt req
                join dparty_dbt party on req.t_client = party.t_partyid
                join davoiriss_dbt fi on req.t_fiid = fi.t_fiid
                join ddl_tick_dbt tick on tick.t_bofficekind = req.t_sourcekind and tick.t_dealid = req.t_sourceid
                join dsfcontr_dbt sf on sf.t_id =  req.t_clientcontr and sf.t_partyid = req.t_client
                join ddlcontrmp_dbt mp on mp.t_sfcontrid = req.t_clientcontr 
               where req.t_kind = 350 
                 and tick.t_portfolioid = 0
                 and TICK.T_MARKETID = -1
                 and req.t_code != CHR(0)
                 and req.t_date between l_start_date and l_end_date
               order by req.t_date, req.t_time, TO_NUMBER(REGEXP_SUBSTR(req.t_code, '[[:digit:]]{2,4}', 3))
    ) loop
        l_cnt := l_cnt + 1;
        note_utils.save_note(p_object_type => 149,
                             p_note_kind => 104,
                             p_document_id => lpad(reqs.t_id, 34, '0'),
                             p_note => l_note_text,
                             p_date => reqs.t_date
        );
    end loop;

    it_log.log_handle(p_object => 'DEF-98334_save_new_notes',
                      p_msg    => 'updated ' || l_cnt || ' reqeusts');
end;
/