create or replace package body sec_rovu_read as

  function get_second_sfcontr (p_dl_acc_id ddl_acc_dbt.t_id%type)
    return dsfcontr_dbt.t_id%type is
    l_sfcontr_id dsfcontr_dbt.t_id%type;
  begin
    select sfc.t_id
      into l_sfcontr_id
      from ddl_acc_dbt a
      join ddlcontrmp_dbt m on m.t_sfcontrid = a.t_clientcontr
      join ddlcontrmp_dbt c on c.t_dlcontrid = m.t_dlcontrid
      join dsfcontr_dbt sfc on sfc.t_id = c.t_sfcontrid
      join dl_acc_place_servkind_lnk l on l.place = a.t_newplace and
                                          l.place_kind = a.t_newplacekind and
                                          l.market_place = case when a.t_newplace<>1 then a.t_newcentroffice else l.market_place end and
                                          l.serv_kind = sfc.t_servkind and
                                          l.serv_kind_sub = sfc.t_servkindsub
     where a.t_id = p_dl_acc_id
       and a.t_clientcontr != sfc.t_id and c.t_marketid=2;
    return l_sfcontr_id;
  exception
    when no_data_found then
      return null;
  end get_second_sfcontr;

  procedure get_expected_params (
    p_serv_kind         dl_acc_place_servkind_lnk.serv_kind%type,
    p_serv_kind_sub     dl_acc_place_servkind_lnk.serv_kind_sub%type,
    po_place_kind   out dl_acc_place_servkind_lnk.place_kind%type,
    po_place        out dl_acc_place_servkind_lnk.place%type,
    po_market_place out dl_acc_place_servkind_lnk.market_place%type
  ) is
  begin
    select l.place,
           l.place_kind,
           l.market_place
      into po_place,
           po_place_kind,
           po_market_place
      from dl_acc_place_servkind_lnk l
     where l.serv_kind = p_serv_kind
       and l.serv_kind_sub = p_serv_kind_sub;
  exception
    when no_data_found then
      null;
  end get_expected_params;

end sec_rovu_read;
