create or replace package body dlreq_utils as

  g_object_type constant number(3) := 149;

  function dlreq_objecttype
    return number deterministic is
  begin
    return g_object_type;
  end dlreq_objecttype;

  procedure insert_dlreq (
    po_id      out ddl_req_dbt.t_id%type,
    p_kind         ddl_req_dbt.t_kind%type,
    p_code         ddl_req_dbt.t_code%type,
    p_codets       ddl_req_dbt.t_codets%type,
    p_date         ddl_req_dbt.t_date%type,
    p_time         ddl_req_dbt.t_time%type,
    p_party        ddl_req_dbt.t_party%type,
    p_client       ddl_req_dbt.t_client%type,
    p_fiid         ddl_req_dbt.t_fiid%type,
    p_amount       ddl_req_dbt.t_amount%type,
    p_price        ddl_req_dbt.t_price%type,
    p_pricefiid    ddl_req_dbt.t_pricefiid%type,
    p_sourcekind   ddl_req_dbt.t_sourcekind%type,
    p_sourceid     ddl_req_dbt.t_sourceid%type,
    p_contract     ddl_req_dbt.t_clientcontr%type,
    p_status       ddl_req_dbt.t_status%type,
    p_direction    ddl_req_dbt.t_direction%type
  ) is
  begin
    insert into ddl_req_dbt (t_kind,
                             t_code,
                             t_codets,
                             t_date,
                             t_time,
                             t_party,
                             t_client,
                             t_fiid,
                             t_amount,
                             t_price,
                             t_pricefiid,
                             t_reporate,
                             t_status,
                             t_sourcekind,
                             t_sourceid,
                             t_methodapplic,
                             t_direction,
                             t_clientcontr)
    values (p_kind,
            p_code,
            p_codets,
            p_date,
            p_time,
            p_party,
            p_client,
            p_fiid,
            p_amount,
            p_price,
            p_pricefiid,
            0,
            p_status,
            p_sourcekind,
            p_sourceid,
            1, --todo: convert to constant. select * from dnamealg_dbt where t_itypealg = 3172
            p_direction,
            p_contract
           )
    returning t_id into po_id;
  end insert_dlreq;
  
  procedure update_dlreq (
    p_id           ddl_req_dbt.t_id%type,
    p_code         ddl_req_dbt.t_code%type,
    p_codets       ddl_req_dbt.t_codets%type,
    p_sourcekind   ddl_req_dbt.t_sourcekind%type,
    p_sourceid     ddl_req_dbt.t_sourceid%type,
    p_status       ddl_req_dbt.t_status%type
  ) is
  begin
    update ddl_req_dbt r
       set r.t_code       = p_code,
           r.t_codets     = p_codets,
           r.t_sourcekind = p_sourcekind,
           r.t_sourceid   = p_sourceid,
           r.t_status     = p_status
    where r.t_id = p_id;
  end update_dlreq;

  procedure save_dlreq (
    pio_id  in out ddl_req_dbt.t_id%type,
    p_kind         ddl_req_dbt.t_kind%type,
    p_code         ddl_req_dbt.t_code%type,
    p_codets       ddl_req_dbt.t_codets%type,
    p_date         ddl_req_dbt.t_date%type,
    p_time         ddl_req_dbt.t_time%type,
    p_party        ddl_req_dbt.t_party%type,
    p_client       ddl_req_dbt.t_client%type,
    p_fiid         ddl_req_dbt.t_fiid%type,
    p_amount       ddl_req_dbt.t_amount%type,
    p_price        ddl_req_dbt.t_price%type,
    p_pricefiid    ddl_req_dbt.t_pricefiid%type,
    p_sourcekind   ddl_req_dbt.t_sourcekind%type,
    p_sourceid     ddl_req_dbt.t_sourceid%type,
    p_contract     ddl_req_dbt.t_clientcontr%type,
    p_status       ddl_req_dbt.t_status%type,
    p_direction    ddl_req_dbt.t_direction%type
  ) is
  begin
    if pio_id is null then
      insert_dlreq(po_id        => pio_id,
                   p_kind       => p_kind,
                   p_code       => p_code,
                   p_codets     => p_codets,
                   p_date       => p_date,
                   p_time       => p_time,
                   p_party      => p_party,
                   p_client     => p_client,
                   p_fiid       => p_fiid,
                   p_amount     => p_amount,
                   p_price      => p_price,
                   p_pricefiid  => p_pricefiid,
                   p_sourcekind => p_sourcekind,
                   p_sourceid   => p_sourceid,
                   p_contract   => p_contract,
                   p_status     => p_status,
                   p_direction  => p_direction);
    else
      update_dlreq(p_id         => pio_id,
                   p_code       => p_code,
                   p_codets     => p_codets,
                   p_sourcekind => p_sourcekind,
                   p_sourceid   => p_sourceid,
                   p_status     => p_status);
    end if;
  
  exception
    when others then
      it_log.log_error(p_object => 'dlreq_utils.save_dlreq',
                       p_msg    => 'Error ' || sqlerrm);
      raise_application_error(-20000, 'Error ' || sqlerrm, true);
  end save_dlreq;
  
  --возвращает признак, что заявка по замещению ценных бумаг
  function is_substitution_secur (
    p_id ddl_req_dbt.t_id%type
  ) return number is
  begin
    return nvl(categ_read.get_attr_id(p_object_type => g_object_type,
                                      p_group_id    => 2,
                                      p_object      => lpad(p_id, 34, '0'),
                                      p_date        => sysdate), 0);
  end is_substitution_secur;

end dlreq_utils;
/
