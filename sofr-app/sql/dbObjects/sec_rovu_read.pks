create or replace package sec_rovu_read as

  function get_second_sfcontr (p_dl_acc_id ddl_acc_dbt.t_id%type)
    return dsfcontr_dbt.t_id%type;

  procedure get_expected_params (
    p_serv_kind         dl_acc_place_servkind_lnk.serv_kind%type,
    p_serv_kind_sub     dl_acc_place_servkind_lnk.serv_kind_sub%type,
    po_place_kind   out dl_acc_place_servkind_lnk.place_kind%type,
    po_place        out dl_acc_place_servkind_lnk.place%type,
    po_market_place out dl_acc_place_servkind_lnk.market_place%type
  );
end sec_rovu_read;
/
