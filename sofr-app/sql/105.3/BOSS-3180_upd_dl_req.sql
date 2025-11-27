begin
  update ddl_req_dbt r
     set r.t_status = 'W'
    where r.t_id in (with contrs as (
                              select s.t_id as sfcontrid, s.t_partyid
                                from ddlobjcode_dbt c
                                join ddlcontrmp_dbt m on m.t_dlcontrid = c.t_objectid
                                join dsfcontr_dbt s on s.t_id = m.t_sfcontrid
                               where c.t_objecttype = 207
                                 and c.t_codekind = 1
                                 and c.t_code in ('468781','434780','446217','438287','464347','4100710','491999',
                                 '435460','460100','422117','462475','482136','452043','466634','472187','439138',
                                 '480069','4108371','455330','439590','453766','445201','446880','490428','438493',
                                 '443324','435484','479590','436757','465660','421765','448633','4202885','451034',
                                 '4107899','465928','485711','4102835','495924','465966','4203861','493708','460676')
                                )
                             select case
                                      when r.t_date = min(r.t_date) over(partition by r.t_client, r.t_clientcontr)
                                        then r.t_id
                                    end id_to_update
                               from ddl_req_dbt r
                               join contrs c on r.t_clientcontr = c.sfcontrid
                              where r.t_codets like 'BlockedOTC%'
                                and r.t_kind = 350
                                )
    and (r.t_status is null or r.t_status != 'W');

  it_log.log_handle(p_object   => 'update ddl_req_dbt status',
                    p_msg      => 'rowcount: ' || sql%rowcount,
                    p_msg_type => it_log.C_MSG_TYPE__MSG);

  commit;
end;
/

begin
  merge into ddl_req_dbt r
    using (
        with all_codes as (
              select t.column_value as codets
                from table(t_string_list('BlockedOTC-3783215-595','BlockedOTC-3783215-592','BlockedOTC-3783562-346',
                     'BlockedOTC-3783562-347','BlockedOTC-3783875-2022','BlockedOTC-3783875-2023','BlockedOTC-3783875-2021',
                     'BlockedOTC-3783296-26','BlockedOTC-3783270-51','BlockedOTC-3783270-49','BlockedOTC-3783270-50','BlockedOTC-3783296-127',
                     'BlockedOTC-3783296-126','BlockedOTC-3783875-1985','BlockedOTC-3783875-1982','BlockedOTC-3783563-312',
                     'BlockedOTC-3783563-313','BlockedOTC-3783563-314','BlockedOTC-3783737-1482','BlockedOTC-3783737-1481',
                     'BlockedOTC-3783963-3169','BlockedOTC-3783963-3173','BlockedOTC-3783963-3168','BlockedOTC-3783963-3170',
                     'BlockedOTC-3783963-3171','BlockedOTC-3783963-3172','BlockedOTC-3783242-613','BlockedOTC-3783124-3691',
                     'BlockedOTC-3783420-238','BlockedOTC-3783684-2398','BlockedOTC-3783270-741','BlockedOTC-3783270-742',
                     'BlockedOTC-3783270-744','BlockedOTC-3783270-745','BlockedOTC-3783270-743','BlockedOTC-3783215-1841',
                     'BlockedOTC-3783215-1842','BlockedOTC-3783215-1843','BlockedOTC-3783215-1840','BlockedOTC-3783736-857',
                     'BlockedOTC-3783736-856','BlockedOTC-3783188-1336','BlockedOTC-3783625-437','BlockedOTC-3783124-287',
                     'BlockedOTC-3783124-9751','BlockedOTC-3783124-9758','BlockedOTC-3783875-3217','BlockedOTC-3783736-2336',
                     'BlockedOTC-3783124-9754', 'BlockedOTC-3783124-9756','BlockedOTC-3783875-1983')) t
                )
             ,all_reqs as (
              select r.*
                from ddl_req_dbt r
                join dsfcontr_dbt s on s.t_id = r.t_clientcontr
                join all_codes c on c.codets = r.t_codets
               where r.t_kind = 350
              )
             ,all_clients as (
              select distinct r.t_client
                from all_reqs r
              )
             ,client_contr_pair as (
              select sc.t_partyid, sc.t_id clientcontr
                from all_clients c
                join dsfcontr_dbt s on s.t_partyid = c.t_client
                join ddlcontr_dbt dc on dc.t_sfcontrid = s.t_id
                join ddlcontrmp_dbt m on m.t_dlcontrid = dc.t_dlcontrid
                join dsfcontr_dbt sc on sc.t_id = m.t_sfcontrid
               where dc.t_iis = 'X'
                 and sc.t_servkind = 1
                 and sc.t_servkindsub = 9
              )
              select r.t_id
                    ,p.clientcontr as new_clientcontr
                from all_reqs r
                join client_contr_pair p on p.t_partyid = r.t_client) t
    on (r.t_id = t.t_id)
  when matched then update
    set r.t_clientcontr = t.new_clientcontr
   where r.t_clientcontr != t.new_clientcontr;

  it_log.log_handle(p_object   => 'update ddl_req_dbt contract iis',
                    p_msg      => 'rowcount: ' || sql%rowcount,
                    p_msg_type => it_log.C_MSG_TYPE__MSG);

  commit;
end;
/

begin
  merge into ddl_req_dbt r
    using (
        with all_codes as (
              select t.column_value as codets
                from table(t_string_list('BlockedOTC-3783510-211','BlockedOTC-3783510-212','BlockedOTC-3783510-213',
                     'BlockedOTC-3783510-214', 'BlockedOTC-3783185-135','BlockedOTC-3783185-136','BlockedOTC-3783185-137',
                     'BlockedOTC-3783185-138','BlockedOTC-3783185-139','BlockedOTC-3783185-600',
                     'BlockedOTC-3783242-925','BlockedOTC-3783242-926','BlockedOTC-3783242-927','BlockedOTC-3783242-928','BlockedOTC-3783242-929','BlockedOTC-3783242-930')) t
                )
              select r.t_id req_id, s.t_id contrid
                from ddl_req_dbt r
                join all_codes c on c.codets = r.t_codets
                join ddlcontrmp_dbt m on m.t_sfcontrid = r.t_clientcontr
                join ddlcontrmp_dbt dm on dm.t_dlcontrid = m.t_dlcontrid
                join dsfcontr_dbt s on s.t_id = dm.t_sfcontrid
               where r.t_kind = 350
                 and s.t_servkind = 1
                 and s.t_servkindsub = 9) t
    on (r.t_id = t.req_id)
  when matched then update
    set r.t_clientcontr = t.contrid
   where r.t_clientcontr != t.contrid; 

  it_log.log_handle(p_object   => 'update ddl_req_dbt contract otc',
                    p_msg      => 'rowcount: ' || sql%rowcount,
                    p_msg_type => it_log.C_MSG_TYPE__MSG);

  commit;
end;
/

declare

  procedure link_req_spground (
    p_req_id      number,
    p_spground_id number
  ) is
  begin
    insert into dspgrdoc_dbt (t_sourcedockind,
                              t_sourcedocid,
                              t_spgroundid,
                              t_status,
                              t_version)
    values (350,
            p_req_id,
            p_spground_id,
            chr(1),
            0);
  end link_req_spground;

  function insert_spground (
    p_src_spground_id number
  ) return dspground_dbt.t_spgroundid%type is
    l_spground_row dspground_dbt%rowtype;
  begin
    select *
      into l_spground_row
      from dspground_dbt g
     where g.t_spgroundid = p_src_spground_id;

    l_spground_row.t_spgroundid := dspground_dbt_seq.nextval;
    
    insert into dspground_dbt values l_spground_row;
    
    return l_spground_row.t_spgroundid;
  end insert_spground;

  function get_spground_id (
    p_req_id number
  ) return dspground_dbt.t_spgroundid%type is
    l_spground_id dspground_dbt.t_spgroundid%type;
  begin
    select d.t_spgroundid
      into l_spground_id
      from dspgrdoc_dbt d
     where d.t_sourcedockind = 350
       and d.t_sourcedocid = p_req_id;

    return l_spground_id;
  exception
    when no_data_found then
      return null;
  end get_spground_id;

  function get_sf_contr_id (
    p_client_id number,
    p_iis       number
  ) return dsfcontr_dbt.t_id%type is
    l_sfcontr_id dsfcontr_dbt.t_id%type;
  begin
    select s.t_id
      into l_sfcontr_id
      from dsfcontr_dbt s
      join ddlcontrmp_dbt m on m.t_sfcontrid = s.t_id
      join ddlcontr_dbt c on c.t_dlcontrid = m.t_dlcontrid
     where s.t_partyid = p_client_id
       and s.t_servkind = 1
       and s.t_servkindsub = 9
       and c.t_iis = case when p_iis = 1 then chr(88) else chr(0) end;

    return l_sfcontr_id;
  end get_sf_contr_id;

  function insert_req (
    p_src_code  varchar2,
    p_tgt_code  varchar2,
    p_doc_type  number, --0 - copy; 1 - no_iis; 2 - iis
    p_amount    number default null
  ) return ddl_req_dbt.t_id%type is
    l_req_row ddl_req_dbt%rowtype;
  begin
    select r.*
      into l_req_row
      from ddl_req_dbt r
     where r.t_kind = 350
       and r.t_codets = p_src_code;

    l_req_row.t_id      := ddl_req_dbt_seq.nextval;
    l_req_row.t_code    := p_tgt_code;
    l_req_row.t_codets  := p_tgt_code;
    l_req_row.t_amount  := nvl(p_amount, l_req_row.t_amount);
    
    if p_doc_type > 0 then
      l_req_row.t_clientcontr := get_sf_contr_id(p_client_id => l_req_row.t_client,
                                                 p_iis       => case when p_doc_type = 2 then 1 else 0 end);
    end if;
    
    insert into ddl_req_dbt values l_req_row;

    return l_req_row.t_id;
  end insert_req;

  function get_req_id (
    p_code varchar2
  ) return ddl_req_dbt.t_id%type is
    l_req_id ddl_req_dbt.t_id%type;
  begin
    select r.t_id
      into l_req_id
      from ddl_req_dbt r
     where r.t_kind = 350
       and r.t_code = p_code;
  
    return l_req_id;
  exception
    when no_data_found then
      return null;
  end get_req_id;

  procedure new_req (
    p_src_code  varchar2,
    p_tgt_code  varchar2,
    p_doc_type  number, --0 - copy; 1 - no_iis; 2 - iis
    p_amount    number default null
  ) is
    l_req_id      ddl_req_dbt.t_id%type;
    l_spground_id dspground_dbt.t_spgroundid%type;
  begin
    l_req_id := get_req_id(p_code => p_tgt_code);
    
    if l_req_id is null then
      l_req_id := insert_req(p_src_code => p_src_code,
                             p_tgt_code => p_tgt_code,
                             p_doc_type => p_doc_type,
                             p_amount   => p_amount);

      it_log.log_handle(p_object => 'upd_dl_req',
                        p_msg    => 'new dl req id = ' || l_req_id);
    end if;
    l_spground_id := get_spground_id(p_req_id => l_req_id);
    if l_spground_id is null then
      l_spground_id := insert_spground(p_src_spground_id => get_spground_id(p_req_id => get_req_id(p_code => p_src_code)));

      link_req_spground(p_req_id      => l_req_id,
                        p_spground_id => l_spground_id);
    end if;
  exception
    when others then
      it_log.log_error(p_object => 'upd_dl_req',
                       p_msg    => 'src: ' || p_src_code || '; tgt: ' || p_tgt_code || '; err: ' || sqlerrm);
  end new_req;
begin
  new_req(p_src_code => 'BlockedOTC-3783124-9754',
          p_tgt_code => 'BlockedOTC-3783124-9755',
          p_doc_type => 1,
          p_amount   => 3);

  new_req(p_src_code => 'BlockedOTC-3783124-9756',
          p_tgt_code => 'BlockedOTC-3783124-9757',
          p_doc_type => 1,
          p_amount   => 3410);

  new_req(p_src_code => 'BlockedOTC-3783242-611',
          p_tgt_code => 'BlockedOTC-3783242-615',
          p_doc_type => 2);

  new_req(p_src_code => 'BlockedOTC-3783242-612',
          p_tgt_code => 'BlockedOTC-3783242-616',
          p_doc_type => 2);

  new_req(p_src_code => 'BlockedOTC-3783875-1983',
          p_tgt_code => 'BlockedOTC-3783875-1984',
          p_doc_type => 1,
          p_amount   => 26);

  new_req(p_src_code => 'BlockedOTC-3783185-572',
          p_tgt_code => 'BlockedOTC-3783185-571',
          p_doc_type => 2,
          p_amount   => 1);

  new_req(p_src_code => 'BlockedOTC-3783745-1375',
          p_tgt_code => 'BlockedOTC-3783745-1376',
          p_doc_type => 1,
          p_amount   => 100);

  new_req(p_src_code => 'BlockedOTC-3783562-1126',
          p_tgt_code => 'BlockedOTC-3783562-1128',
          p_doc_type => 1,
          p_amount   => 1);

  new_req(p_src_code => 'BlockedOTC-3783625-658',
          p_tgt_code => 'BlockedOTC-3783625-659',
          p_doc_type => 1,
          p_amount   => 12);

  new_req(p_src_code => 'BlockedOTC-3783098-6683',
          p_tgt_code => 'BlockedOTC-3783098-6688',
          p_doc_type => 1,
          p_amount   => 1609);

  new_req(p_src_code => 'BlockedOTC-3783296-26',
          p_tgt_code => 'BlockedOTC-3783296-29',
          p_doc_type => 1,
          p_amount   => 390);

  new_req(p_src_code => 'BlockedOTC-3783296-27',
          p_tgt_code => 'BlockedOTC-3783296-28',
          p_doc_type => 2,
          p_amount   => 305);

  commit;
end;
/

begin
  update ddl_req_dbt r set t_amount = 14 where t_kind = 350 and t_codets = 'BlockedOTC-3783745-1375';
  update ddl_req_dbt r set t_amount = 1  where t_kind = 350 and t_codets = 'BlockedOTC-3783562-1126';
  update ddl_req_dbt r set t_amount = 1  where t_kind = 350 and t_codets = 'BlockedOTC-3783625-658';
  commit;
end;
/
