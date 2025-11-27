create or replace package body nontrading_orders_read as

  g_buf_status_ready     constant number(1) := 0;
  g_buf_status_wait      constant number(1) := 1;
  g_buf_status_done      constant number(1) := 2;
  g_buf_status_error     constant number(1) := 3;
  g_buf_status_reject    constant number(1) := 4;
  g_buf_status_executing constant number(1) := 5;
  g_buf_status_creating  constant number(1) := 6;
  g_buf_status_deleted   constant number(1) := 7;
  
  g_buf_kind_out_exchange constant number(1) := 0;
  g_buf_kind_out_otc      constant number(1) := 1;
  g_buf_kind_transfer     constant number(1) := 2;

  g_buf_servkind_currency constant number(2) := 21;
  
  g_buf_otc_market      constant number(1) := 0;
  g_buf_exchange_market constant number(1) := 1;
  g_buf_currency_market constant number(1) := 2;
  g_buf_forts_market    constant number(1) := 3;
  g_buf_spb_market      constant number(1) := 4;
  
  g_buf_err_no_error                constant number(1) := 0;
  g_buf_err_duplicate               constant number(1) := 1;
  g_buf_err_client_not_found        constant number(1) := 2;
  g_buf_err_cntr_not_found          constant number(1) := 3;
  g_buf_err_ekk_not_found           constant number(1) := 4;
  g_buf_err_cur_not_found           constant number(1) := 5;
  g_buf_err_acc_wo_not_found        constant number(1) := 6;
  g_buf_err_acc_enrl_not_found      constant number(1) := 8;
  g_buf_err_cntr_enrl_not_found     constant number(1) := 9;
  g_buf_err_cntr_is_edp             constant number(2) := 10;
  g_buf_err_clnt_not_matched_w_cntr constant number(2) := 11;
  g_buf_err_department_not_found    constant number(2) := 12;
  g_buf_err_internal_error          constant number(3) := 999;
  
  g_subkind_out      constant number(2) := 20;
  g_subkind_transfer constant number(2) := 30;
  
  g_dockind constant number(4) := 4607;
  
  g_order_object_type constant number(3) := 131;
  
  g_notekind_last_sent_status constant number(3) := 107;
  
  g_categ_group_src constant number(3) := 102;
  
  --operation_constants
  g_otc_place           constant number(1) := 1; --RSHB
  g_main_sector         constant number(1) := 2;
  g_forts_sector        constant number(1) := 8;
  g_cur_sector          constant number(1) := 9;
  
  g_rc_place_kind       constant number(1) := 5; --RC - raschetniy center
  g_bank_place_kind     constant number(1) := 2; --bank
  g_nko_nrd_place       constant number(1) := 4; --NKO NRD
  
  g_note_enroll_allowed constant varchar2(50) := 'Разрешено зачисление';
  
  type t_src_attr is table of number(2) index by varchar2(200); --ёт ч№ ёшёЄхь√-шёЄюўэшър ш attr_id ърЄхуюЁшш 102
  g_src_attr t_src_attr;
  
  function buf_status_ready
    return number deterministic is
  begin
    return g_buf_status_ready;
  end buf_status_ready;
  
  function buf_status_wait
    return number deterministic is
  begin
    return g_buf_status_wait;
  end buf_status_wait;
  
  function buf_status_done
    return number deterministic is
  begin
    return g_buf_status_done;
  end buf_status_done;
  
  function buf_status_error
    return number deterministic is
  begin
    return g_buf_status_error;
  end buf_status_error;
  
  function buf_status_reject
    return number deterministic is
  begin
    return g_buf_status_reject;
  end buf_status_reject;
  
  function buf_status_executing
    return number deterministic is
  begin
    return g_buf_status_executing;
  end buf_status_executing;
  
  function buf_status_creating
    return number deterministic is
  begin
    return g_buf_status_creating;
  end buf_status_creating;
  
  function buf_status_deleted
    return number deterministic is
  begin
    return g_buf_status_deleted;
  end buf_status_deleted;

  function buf_kind_out_exchange
    return number deterministic is
  begin
    return g_buf_kind_out_exchange;
  end buf_kind_out_exchange;
  
  function buf_kind_out_otc
    return number deterministic is
  begin
    return g_buf_kind_out_otc;
  end buf_kind_out_otc;
  
  function buf_kind_transfer
    return number deterministic is
  begin
    return g_buf_kind_transfer;
  end buf_kind_transfer;
  
  function buf_servkind_stock
    return number deterministic is
  begin
    return sfcontr_read.servkind_stock;
  end buf_servkind_stock; 
     
  function buf_servkind_forts
    return number deterministic is
  begin
    return sfcontr_read.servkind_forts;
  end buf_servkind_forts; 
  
  function buf_servkind_currency
    return number deterministic is
  begin
    return g_buf_servkind_currency;
  end buf_servkind_currency; 
  
  function buf_otc_market
    return number deterministic is
  begin
    return g_buf_otc_market;
  end buf_otc_market;
  
  function buf_exchange_market
    return number deterministic is
  begin
    return g_buf_exchange_market;
  end buf_exchange_market;
  
  function buf_currency_market
    return number deterministic is
  begin
    return g_buf_currency_market;
  end buf_currency_market;
  
  function buf_forts_market
    return number deterministic is
  begin
    return g_buf_forts_market;
  end buf_forts_market;
  
  function buf_spb_market
    return number deterministic is
  begin
    return g_buf_spb_market;
  end buf_spb_market;
  
  function buf_err_no_error
    return number deterministic is
  begin
    return g_buf_err_no_error;
  end buf_err_no_error;
  
  function buf_err_duplicate
    return number deterministic is
  begin
    return g_buf_err_duplicate;
  end buf_err_duplicate;
  
  function buf_err_client_not_found
    return number deterministic is
  begin
    return g_buf_err_client_not_found;
  end buf_err_client_not_found;
  
  function buf_err_cntr_not_found
    return number deterministic is
  begin
    return g_buf_err_cntr_not_found;
  end buf_err_cntr_not_found;
  
  function buf_err_ekk_not_found
    return number deterministic is
  begin
    return g_buf_err_ekk_not_found;
  end buf_err_ekk_not_found;
  
  function buf_err_cur_not_found
    return number deterministic is
  begin
    return g_buf_err_cur_not_found;
  end buf_err_cur_not_found;
  
  function buf_err_acc_enrl_not_found
    return number deterministic is
  begin
    return g_buf_err_acc_enrl_not_found;
  end buf_err_acc_enrl_not_found;
  
  function buf_err_acc_wo_not_found
    return number deterministic is
  begin
    return g_buf_err_acc_wo_not_found;
  end buf_err_acc_wo_not_found;
  
  function buf_err_cntr_enrl_not_found
    return number deterministic is
  begin
    return g_buf_err_cntr_enrl_not_found;
  end buf_err_cntr_enrl_not_found;
  
  function buf_err_cntr_is_edp
    return number deterministic is
  begin
    return g_buf_err_cntr_is_edp;
  end buf_err_cntr_is_edp;
  
  function buf_err_clnt_not_matched_w_cntr
    return number deterministic is
  begin
    return g_buf_err_clnt_not_matched_w_cntr;
  end buf_err_clnt_not_matched_w_cntr;
  
  function buf_err_department_not_found
    return number deterministic is
  begin
    return g_buf_err_department_not_found;
  end buf_err_department_not_found;

  function buf_err_internal_error
    return number deterministic is
  begin
    return g_buf_err_internal_error;
  end buf_err_internal_error;
  
  function subkind_out
    return number deterministic is
  begin
    return g_subkind_out;
  end subkind_out;
  
  function subkind_transfer
    return number deterministic is
  begin
    return g_subkind_transfer;
  end subkind_transfer;
  
  function dockind
    return number deterministic is
  begin
    return g_dockind;
  end dockind;
  
  function order_object_type
    return number deterministic is
  begin
    return g_order_object_type;
  end order_object_type;
  
  function notekind_last_sent_status
    return number deterministic is
  begin
    return g_notekind_last_sent_status;
  end notekind_last_sent_status;
  
  function categ_group_src
    return number deterministic is
  begin
    return g_categ_group_src;
  end categ_group_src;
  
  function rc_place_kind
    return number deterministic is
  begin
    return g_rc_place_kind;
  end rc_place_kind;
  
  function bank_place_kind
    return number deterministic is
  begin
    return g_bank_place_kind;
  end bank_place_kind;
  
  function nko_nrd_place
    return number deterministic is
  begin
    return g_nko_nrd_place;
  end nko_nrd_place;
  
  function otc_place
    return number deterministic is
  begin
    return g_otc_place;
  end otc_place;
  
  function main_sector
    return number deterministic is
  begin
    return g_main_sector;
  end main_sector;
  
  function forts_sector
    return number deterministic is
  begin
    return g_forts_sector;
  end forts_sector;
  
  function cur_sector
    return number deterministic is
  begin
    return g_cur_sector;
  end cur_sector;
  
  function note_enroll_allowed
    return varchar2 deterministic is
  begin
    return g_note_enroll_allowed;
  end note_enroll_allowed;
  
  procedure z___________func is
  begin
    null;
  end;
  
  function get_active_req (
    p_src     nontrading_orders_buffer.src%type,
    p_ext_id  nontrading_orders_buffer.external_id%type,
    p_kind    nontrading_orders_buffer.kind%type,
    p_client  nontrading_orders_buffer.client_cft_id%type
  ) return nontrading_orders_buffer.req_id%type is
    l_req_id nontrading_orders_buffer.req_id%type;
  begin
    select r.req_id
      into l_req_id
      from nontrading_orders_buffer r
     where r.src = p_src
       and r.external_id = p_ext_id
       and r.kind =  case when p_src = 'ЕФР' then p_kind else r.kind end
       and r.error_id = nontrading_orders_read.buf_err_no_error
       and r.client_cft_id = p_client;

    return l_req_id;
  exception
    when no_data_found then
      return null;
  end get_active_req;

  --нет обработки too_many_rows специально. Если такая ошибка случается - что-то пошло не так в заполнении таблицы
  function get_src_buf_name_by_opername (
    p_opercode_name nontrading_orders_source.opercode_name%type
  ) return nontrading_orders_source.buffer_name%type is
    l_buf_name nontrading_orders_source.buffer_name%type;
  begin
    select distinct s.buffer_name
      into l_buf_name
      from nontrading_orders_source s
     where s.opercode_name = p_opercode_name;

    return l_buf_name;
  exception
    when no_data_found then
      return null;
  end get_src_buf_name_by_opername;

  function get_src_opername_by_buf (
    p_buf_name nontrading_orders_source.buffer_name%type
  ) return nontrading_orders_source.opercode_name%type is
    l_opercode_name nontrading_orders_source.opercode_name%type;
  begin
    select distinct s.opercode_name
      into l_opercode_name
      from nontrading_orders_source s
     where s.buffer_name = p_buf_name;

    return l_opercode_name;
  exception
    when no_data_found then
      return null;
  end get_src_opername_by_buf;
  
  function get_buf_by_incoming_src_name (
    p_incoming_name nontrading_orders_source.incoming_name%type
  ) return nontrading_orders_source.buffer_name%type is
    l_buf_name nontrading_orders_source.buffer_name%type;
  begin
    select s.buffer_name
      into l_buf_name
      from nontrading_orders_source s
     where s.incoming_name = p_incoming_name;

    return l_buf_name;
  exception
    when no_data_found then
      return null;
  end get_buf_by_incoming_src_name;
  
  function get_sfcontr_by_name (
    p_name dsfcontr_dbt.t_number%type
  ) return dsfcontr_dbt.t_id%type is
    l_id dsfcontr_dbt.t_id%type;
  begin
    select c.t_id
      into l_id
      from dsfcontr_dbt c
     where c.t_number = p_name;

    return l_id;
  exception
    when others then
      return null;
  end get_sfcontr_by_name;

  procedure get_sfcontr_data (
    p_name              dsfcontr_dbt.t_number%type,
    p_serv_kind         dsfcontr_dbt.t_servkind%type,
    p_serv_kind_sub     dsfcontr_dbt.t_servkindsub%type,
    p_market_id         ddlcontrmp_dbt.t_marketid%type,
    p_date              dsfcontr_dbt.t_dateclose%type,
    po_sfcontr_id   out dsfcontr_dbt.t_id%type,
    po_iis          out number
  ) is
  begin
    select sf_srv.t_id,
           case when d.t_iis = 'X' then 1 else 0 end
      into po_sfcontr_id
          ,po_iis
      from dsfcontr_dbt sf
      join ddlcontr_dbt d on d.t_sfcontrid = sf.t_id
      join ddlcontrmp_dbt m on m.t_dlcontrid = d.t_dlcontrid
      join dsfcontr_dbt sf_srv on sf_srv.t_id = m.t_sfcontrid
     where sf.t_number = p_name
       and sf_srv.t_servkind = p_serv_kind
       and sf_srv.t_servkindsub = p_serv_kind_sub
       and (   sf_srv.t_dateclose = to_date('01.01.0001', 'dd.mm.yyyy')
            or sf_srv.t_dateclose >= trunc(p_date) )
       and m.t_marketid = nvl(p_market_id, m.t_marketid);

  exception
    when others then
      null;
  end get_sfcontr_data;

  function get_sfcontr_account (
    p_sfcontr_id dsfcontr_dbt.t_id%type,
    p_fiid       ddlcontracc_dbt.t_fiid%type,
    p_date       daccount_dbt.t_close_date%type
  ) return dmcaccdoc_dbt.t_account%type is
    l_account dmcaccdoc_dbt.t_account%type;
  begin
    select /*+ use_nl(mc)*/ mc.t_account
      into l_account
      from dmcaccdoc_dbt mc
      join daccount_dbt a on a.t_account = mc.t_account
     where rownum = 1
       and mc.t_catid = 70
       and mc.t_currency = p_fiid
       and mc.t_dockind = 3001
       and mc.t_docid = p_sfcontr_id
       and (   a.t_close_date = to_date('01.01.0001', 'dd.mm.yyyy')
            or a.t_close_date >= trunc(p_date));

    return l_account;
  exception
    when no_data_found then
      return null;
  end get_sfcontr_account;
  
  function get_fiid_by_ccy (
    p_ccy dfininstr_dbt.t_ccy%type
  ) return dfininstr_dbt.t_fiid%type deterministic is
    l_fiid dfininstr_dbt.t_fiid%type;
  begin
    select t_fiid
      into l_fiid
      from dfininstr_dbt f
     where f.t_ccy = case when p_ccy = 'RUR' then 'RUB' else p_ccy end
       and f.t_fi_kind = 1;

    return l_fiid;
  exception
    when no_data_found then
      return null;
  end get_fiid_by_ccy;
  
  function get_account_id (
    p_account   daccount_dbt.t_account%type
  ) return daccount_dbt.t_accountid%type is
    l_account_id daccount_dbt.t_accountid%type;
  begin
    select t_accountid
      into l_account_id
      from daccount_dbt
     where t_account = p_account;
  
    return l_account_id;
  exception
    when others then
      return null;
  end get_account_id;
  
  function get_rest (
    p_account       daccount_dbt.t_account%type,
    p_date          drestdate_dbt.t_restdate%type,
    p_rest_currency drestdate_dbt.t_restcurrency%type
  ) return drestdate_dbt.t_rest%type is
    l_rest       drestdate_dbt.t_rest%type;
    l_account_id daccount_dbt.t_accountid%type;
  begin
    l_account_id := get_account_id(p_account => p_account);

    select r.t_rest
      into l_rest
      from drestdate_dbt r
     where r.t_accountid = l_account_id
       and r.t_restcurrency = p_rest_currency
       and r.t_restdate = (select max(t_restdate)
                             from drestdate_dbt
                            where t_accountid = r.t_accountid
                              and t_restcurrency = r.t_restcurrency
                              and t_restdate <= p_date);

    return l_rest;
  exception
    when no_data_found then
      return null;
  end get_rest;

  function get_micex_id
    return number deterministic is
    l_micex_id number(10);
  begin
    select t_objectid
      into l_micex_id
      from dobjcode_dbt
     where t_objecttype = 3
       and t_codekind = 1
       and t_state = 0
       and t_code = RSB_Common.GetRegStrValue('SECUR\MICEX_CODE');
  
    return l_micex_id;
  exception
    when no_data_found then
      return null;
  end get_micex_id;
  
  function get_spbex_id
    return number deterministic is
    l_spbex_id number(10);
  begin
    select t_objectid
      into l_spbex_id
      from dobjcode_dbt
     where t_objecttype = 3
       and t_codekind = 1
       and t_state = 0
       and t_code = RSB_Common.GetRegStrValue('SECUR\SPBEX_CODE');
  
    return l_spbex_id;
  exception
    when no_data_found then
      return null;
  end get_spbex_id;
  
  function get_nrd_id
    return number deterministic is
    l_nrd_id number(10);
  begin
    select t_objectid
      into l_nrd_id
      from dobjcode_dbt
     where t_objecttype = 3
       and t_codekind = 1
       and t_state = 0
       and t_code = RSB_Common.GetRegStrValue('SECUR\NRD_CODE');
  
    return l_nrd_id;
  exception
    when no_data_found then
      return null;
  end get_nrd_id;
  
  function get_servkind(
    p_marketplace in nontrading_orders_buffer.marketplace_withdrawal%type
  ) return number is
    v_servkind number;
  begin
    v_servkind := case p_marketplace
                    when buf_otc_market      then buf_servkind_stock
                    when buf_exchange_market then buf_servkind_stock
                    when buf_currency_market then buf_servkind_currency
                    when buf_forts_market    then buf_servkind_forts
                    when buf_spb_market      then buf_servkind_stock
                    else 0
                  end;
    return v_servkind;
  end; 
  
  function get_marketid(
    p_kind        in nontrading_orders_buffer.kind%type,
    p_marketplace in nontrading_orders_buffer.marketplace_withdrawal%type
  ) return number is
    v_market_id number;
  begin  
    v_market_id := case 
                    when p_kind = buf_kind_out_otc then -1
                    when p_kind <> buf_kind_out_otc and p_marketplace = buf_spb_market      then get_spbex_id()
                    when p_kind <> buf_kind_out_otc and p_marketplace = buf_exchange_market then get_micex_id()
                    else null
                   end; 
    return v_market_id;
  end;
  
  function is_edp_subcontr (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return boolean is
    v_cnt number;
    v_res boolean;
    
    C_OBJECTTYPE_SUBCONTRACT constant number := 659;
    C_GROUPID_EDP constant number := 102;
    C_EDP_YES constant number := 1;
  begin
  
    select count(*)
      into v_cnt
      from  dobjatcor_dbt o,
            dsfcontr_dbt sf
     where sf.t_id = p_sfcontr_id
       and o.t_objecttype = C_OBJECTTYPE_SUBCONTRACT and o.t_groupid = C_GROUPID_EDP
       and o.t_object = lpad(sf.t_id,10,'0') and o.t_validtodate = to_date('31129999','ddmmyyyy')
       and o.t_attrid = C_EDP_YES;
       
    v_res := case v_cnt 
               when 0 then false 
               else true
             end;
    return v_res;           
  end is_edp_subcontr;

  function check_party_sfcontr (
    p_sfcontr_id dsfcontr_dbt.t_id%type,
    p_party_id   dsfcontr_dbt.t_partyid%type
  ) return boolean is
    l_cnt number(5);
  begin
    select count(1)
      into l_cnt
      from dsfcontr_dbt s
     where s.t_id = p_sfcontr_id
       and s.t_partyid = p_party_id;

    return l_cnt > 0;
  end check_party_sfcontr;
  
  function get_party_code (
    p_party_id   dparty_dbt.t_partyid%type,
    p_code_kind  dobjcode_dbt.t_codekind%type
  ) return dobjcode_dbt.t_code%type is
    l_code dobjcode_dbt.t_code%type;
  begin
    select c.t_code
      into l_code
      from dobjcode_dbt c
     where c.t_objecttype = 3
       and c.t_codekind = p_code_kind
       and c.t_objectid = p_party_id
       and c.t_bankclosedate = to_date('01.01.0001', 'dd.mm.yyyy');
  
    return l_code;
  exception
    when no_data_found then
      return null;
  end get_party_code;
  
  function get_party_name (
    p_party_id   dparty_dbt.t_partyid%type
  ) return dparty_dbt.t_name%type is
    l_name dparty_dbt.t_name%type;
  begin
    select t_name
      into l_name
      from dparty_dbt p
     where p.t_partyid = p_party_id;
  
    return l_name;
  exception
    when no_data_found then
      return null;
  end get_party_name;

  function get_req_row (
    p_req_id nontrading_orders_buffer.req_id%type default null,
    p_oper_id nontrading_orders_buffer.operation_id%type default null
  ) return nontrading_orders_buffer%rowtype is
    l_row  nontrading_orders_buffer%rowtype;
  begin
    if p_req_id is not null then 
      select *
        into l_row
        from nontrading_orders_buffer r
       where r.req_id = p_req_id;
    elsif p_oper_id is not null then 
      select *
        into l_row
        from nontrading_orders_buffer r
       where r.operation_id = p_oper_id;
    end if;
    return l_row;
  exception
    when no_data_found then
      return null;
  end get_req_row;

  procedure get_buf_data_transfer (
    p_req_row              nontrading_orders_buffer%rowtype,
    po_contract        out dnptxop_dbt.t_contract%type,
    po_contract_tgt    out dnptxop_dbt.t_contract%type,
    po_currency        out dnptxop_dbt.t_currency%type,
    po_account         out dnptxop_dbt.t_account%type,
    po_account_tgt     out dnptxop_dbt.t_account%type,
    po_outsum          out dnptxop_dbt.t_outsum%type,
    po_iis             out number
  ) is
    l_iis               number(1);
    l_contract_internal dnptxop_dbt.t_contract%type;
  begin
    po_contract := get_sfcontr_by_name(p_name => p_req_row.contract);
    get_sfcontr_data(p_name          => p_req_row.contract,
                     p_serv_kind     => get_servkind(p_req_row.marketplace_withdrawal),
                     p_serv_kind_sub => case p_req_row.marketplace_withdrawal when buf_otc_market then 9 else 8 end,
                     p_market_id     => get_marketid(p_req_row.kind, p_req_row.marketplace_withdrawal),
                     p_date          => trunc(sysdate),
                     po_sfcontr_id   => l_contract_internal,
                     po_iis          => po_iis);

    get_sfcontr_data(p_name          => p_req_row.contract,
                     p_serv_kind     => get_servkind(p_req_row.marketplace_enroll),
                     p_serv_kind_sub => case p_req_row.marketplace_enroll when buf_otc_market then 9 else 8 end,
                     p_market_id     => get_marketid(p_req_row.kind, p_req_row.marketplace_enroll),
                     p_date          => trunc(sysdate),
                     po_sfcontr_id   => po_contract_tgt,
                     po_iis          => l_iis);

    po_currency    := nontrading_orders_read.get_fiid_by_ccy(p_ccy => p_req_row.currency);
    po_account     := get_sfcontr_account(p_sfcontr_id => l_contract_internal, p_fiid => po_currency, p_date => trunc(sysdate));
    po_account_tgt := get_sfcontr_account(p_sfcontr_id => po_contract_tgt, p_fiid => po_currency, p_date => trunc(sysdate));

    if p_req_row.is_full_rest = 0
    then
      po_outsum := p_req_row.amount;
    elsif p_req_row.marketplace_withdrawal = buf_exchange_market
    then
      po_outsum := 0;
    else
      po_outsum := nvl(nontrading_orders_read.get_rest(p_account       => po_account,
                                                       p_date          => trunc(sysdate),
                                                       p_rest_currency => po_currency)
                       ,0);
    end if;
  end get_buf_data_transfer;

  procedure get_buf_data_out (
    p_req_row              nontrading_orders_buffer%rowtype,
    po_contract        out dnptxop_dbt.t_contract%type,
    po_currency        out dnptxop_dbt.t_currency%type,
    po_account         out dnptxop_dbt.t_account%type,
    po_outsum          out dnptxop_dbt.t_outsum%type,
    po_iis             out number
  ) is
  begin
    get_sfcontr_data(p_name          => p_req_row.contract,
                     p_serv_kind     => get_servkind(p_req_row.marketplace_withdrawal), 
                     p_serv_kind_sub => case when p_req_row.kind = buf_kind_out_otc then 9 else 8 end,
                     p_market_id     => get_marketid(p_req_row.kind, p_req_row.marketplace_withdrawal),
                     p_date          => trunc(sysdate),
                     po_sfcontr_id   => po_contract,
                     po_iis          => po_iis);

    po_currency := nontrading_orders_read.get_fiid_by_ccy(p_ccy => p_req_row.currency);
    po_account  := get_sfcontr_account(p_sfcontr_id => po_contract, p_fiid => po_currency, p_date => trunc(sysdate));

    if p_req_row.is_full_rest = 0
    then
      po_outsum := p_req_row.amount;
    elsif p_req_row.kind = buf_kind_out_exchange or po_iis = 1
    then
      po_outsum := 0;
    else
      po_outsum := nvl(nontrading_orders_read.get_rest(p_account       => po_account,
                                                p_date          => trunc(sysdate),
                                                p_rest_currency => po_currency)
                       ,0);
    end if;
    
  end get_buf_data_out;
  
  function get_dep_id_by_code (
    p_code varchar2
  ) return ddp_dep_dbt.t_partyid%type is
    l_dep_id ddp_dep_dbt.t_partyid%type;
  begin
    select d.t_partyid
      into l_dep_id
      from ddp_dep_dbt d
     where d.t_name = p_code;

    return l_dep_id;
  exception
    when others then 
      return null;
  end get_dep_id_by_code;
  
  function get_funcobj_code
    return varchar2 deterministic is
  begin
    return 'run_nptx_money';
  end get_funcobj_code;
  
  function get_email_grp_kafka_err
    return number deterministic is
    l_grp_id number(10);
  begin
    select v.t_element
      into l_grp_id
      from dllvalues_dbt v
     where v.t_list = 5009
       and v.t_name = 'NonTradingOrderLoadingError';

    return l_grp_id;
  exception
    when others then
      return null;
  end get_email_grp_kafka_err;

  function get_unallowed_cur_setting
    return varchar2 is 
  begin
    return it_rs_interface.get_parm_varchar_path(p_parm_path => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\СПИСОК_ВАЛЮТ_ЗАПРЕТ_СПИС_ЗАЧ');
  end get_unallowed_cur_setting;
  
  function check_if_cur_allowed (
    p_fiid dfininstr_dbt.t_fiid%type
  ) return number is
    l_unallowed_curs_str varchar2(4000);
    l_cnt number(9);
  begin
    l_unallowed_curs_str := get_unallowed_cur_setting();
    
    select /*+ cardinality(t 5)*/ count(*)
      into l_cnt
      from table(string_utils.str_to_list(p_str   => l_unallowed_curs_str,
                                          p_delim => ',')) t
      join dfininstr_dbt f on f.t_ccy = t.column_value
     where f.t_fiid = p_fiid;
    
    return case when l_cnt > 0 then 0 else 1 end;
  end check_if_cur_allowed;

  function is_allowed_autorun (
    p_system_name          nontrading_autorun_config.src%type,
    p_exchange_type        nontrading_autorun_config.exchange_type%type,
    p_exchange_type_target nontrading_autorun_config.exchange_type_target%type,
    p_is_full_rest         nontrading_autorun_config.is_full_rest%type,
    p_currency             nontrading_autorun_config.currency%type,
    p_iis                  number
  ) return nontrading_autorun_config.is_allowed%type is
    l_is_allowed nontrading_autorun_config.is_allowed%type;
    l_currency   nontrading_autorun_config.currency%type := case when p_currency = 'RUB' then 'RUR' else p_currency end;
  begin
    select c.is_allowed
      into l_is_allowed
      from nontrading_autorun_config c
     where c.src = p_system_name
       and c.exchange_type = p_exchange_type
       and (c.exchange_type_target = p_exchange_type_target or
            c.exchange_type_target is null and p_exchange_type_target is null)
       and c.is_full_rest = p_is_full_rest
       and c.currency = l_currency
       and c.is_iis = p_iis;

    return l_is_allowed;
  exception
    when others then
      return 0;
  end is_allowed_autorun;
  
  function is_allowed_autorun_by_nptxop (
    p_operation_id dnptxop_dbt.t_id%type
  ) return nontrading_autorun_config.is_allowed%type is
    l_req_row    nontrading_orders_buffer%rowtype;
  begin
    l_req_row := get_req_row(p_oper_id => p_operation_id);
    return is_allowed_autorun(p_system_name          => l_req_row.src,
                              p_exchange_type        => l_req_row.marketplace_withdrawal,
                              p_exchange_type_target => l_req_row.marketplace_enroll,
                              p_is_full_rest         => l_req_row.is_full_rest,
                              p_currency             => l_req_row.currency,
                              p_iis                  => sfcontr_read.is_subcontr_iis(p_sfcontr_id => get_sfcontr_by_name(p_name => l_req_row.contract)));
  exception
    when others then
      return 0;
  end is_allowed_autorun_by_nptxop;
  
  function is_client_operation_in_funcobj (
    p_client_id dnptxop_dbt.t_client%type
  ) return number is
    l_cnt       number(9);
  begin
    select count(1)
      into l_cnt
      from dfuncobj_dbt f
      join dnptxop_dbt o on o.t_id = f.t_objectid
     where f.t_funcid = funcobj_utils.get_func_id(p_code => get_funcobj_code)
       and f.t_param = to_char(p_client_id)
       and f.t_objecttype = 0
       and f.t_state = 0
       and o.t_subkind_operation in (20, 30)
       and rownum = 1;

    return case when l_cnt > 0 then 0 else 1 end;
  end is_client_operation_in_funcobj;
  
  function is_client_allowed_autorun (
    p_client_id         dnptxop_dbt.t_client%type,
    p_exclude_oper_list t_number_list
  ) return number is
    l_cnt       number(9);
  begin
    select /*+ index (o DNPTXOP_DBT_IDX3) */ count(1)
      into l_cnt
      from dnptxop_dbt o
     where o.t_dockind = 4607
       and o.t_client = p_client_id
       and o.t_subkind_operation in (20, 30)
       and o.t_status in (0, 1)
       and o.t_id not in (select /*+ cardinality(t 2)*/ t.column_value from table(p_exclude_oper_list) t)
       and rownum = 1;

    if l_cnt > 0 then
      return 0;
    end if;

    return is_client_operation_in_funcobj(p_client_id => p_client_id);
  end is_client_allowed_autorun;
  
  function is_allowed_system_kafka (
    p_system_name varchar2
  ) return number is
    l_prefix varchar2(100) := GC_PARAM_LOAD||'\';
    l_key_id number(10);
    l_value  varchar2(2);
  begin
    l_key_id := it_rs_interface.get_keyid_parm_path(p_parm_path => l_prefix || p_system_name);
    l_value := it_rs_interface.get_parm_varchar(p_keyid => l_key_id);
    
    return case when l_value = chr(88) then 1 else 0 end;
  exception
    when others then
      return 0;
  end is_allowed_system_kafka;
  
  function is_allowed_send_status (
    p_system_name varchar2
  ) return number is
    l_prefix varchar2(100) := GC_PARAM_STATUS||'\';
    l_value  varchar2(2);
  begin
    l_value := it_rs_interface.get_parm_varchar_path(p_parm_path => l_prefix || p_system_name) ;
   return case when l_value = chr(88) then 1 else 0 end;

  exception
    when others then
      return 0;
  end is_allowed_send_status;

  procedure get_allowed_period (
    p_date        date,
    po_dbegin out date,
    po_dend   out date
  ) is
    l_start_parm varchar2(50);
    l_end_parm   varchar2(50);
  begin
    l_start_parm := it_rs_interface.get_parm_varchar_path(p_parm_path => GC_PARAM_TIME_START);
    l_end_parm   := it_rs_interface.get_parm_varchar_path(p_parm_path => GC_PARAM_TIME_STOP);

    po_dbegin := trunc(p_date) + substr(l_start_parm, 1, 2)/24 + substr(l_start_parm, 3)/24/60;
    po_dend   := trunc(p_date)   + substr(l_end_parm, 1, 2)/24 + substr(l_end_parm, 3)/24/60;
  exception
    when others then
      po_dbegin := trunc(p_date);
      po_dend   := trunc(p_date);
  end get_allowed_period;
  
  function get_error_text (
    p_error_id nontrading_orders_error.error_id%type
  ) return nontrading_orders_error.error_name%type is
    l_error_name nontrading_orders_error.error_name%type;
  begin
    select e.error_name
      into l_error_name
      from nontrading_orders_error e
     where e.error_id = p_error_id;

    return l_error_name;
  exception
    when no_data_found then
      return null;
  end get_error_text;
  
  function get_exchange(
    p_trading_platform in varchar2
  ) return nontrading_orders_buffer.marketplace_withdrawal%type is 
    l_marketplace nontrading_orders_buffer.marketplace_withdrawal%type;
  begin
    l_marketplace := case p_trading_platform
                        when 'Внебиржевой рынок' then buf_otc_market
                        when 'Биржевой рынок' then buf_exchange_market
                        when 'Фондовый рынок' then buf_exchange_market
                        when 'Валютный рынок' then buf_currency_market
                        when 'Срочный рынок' then buf_forts_market
                        when 'СПБ' then buf_spb_market
                      end;
    return l_marketplace;
  end;
  
  function get_trade_name(
    p_marketplace in nontrading_orders_buffer.marketplace_withdrawal%type
  ) return varchar2 is 
    v_trade_name  varchar2(200);
  begin
    v_trade_name := case p_marketplace
                      when buf_otc_market      then 'Внебиржевой рынок'
                      when buf_exchange_market then 'Биржевой рынок'
                      when buf_currency_market then 'Валютный рынок'
                      when buf_forts_market    then 'Срочный рынок'
                      when buf_spb_market      then 'СПБ'
                      else '-'
                    end;
    return v_trade_name;
  end;
  
  function get_src_attr_by_name (
    p_src_buf_name nontrading_orders_source.buffer_name%type
  ) return dobjatcor_dbt.t_attrid%type is
  begin
    return g_src_attr(p_src_buf_name);
  end get_src_attr_by_name;

  procedure z___________ui is
  begin
    null;
  end;

  function check_status_changed (
    p_req_id         nontrading_orders_buffer.req_id%type,
    p_status_changed date
  ) return number is
    l_status_changed date;
  begin
    select cast(r.status_changed as date)
      into l_status_changed
      from nontrading_orders_buffer r
     where r.req_id = p_req_id;

    if p_status_changed < l_status_changed
    then
      return 1;
    end if;

    return 0;
  end check_status_changed;

  function get_reqs (
    p_dbegin       date,
    p_dend         date,
    p_out_exchange number,
    p_out_otc      number,
    p_transfer     number,
    p_status_id    number,
    p_error_id     number,
    p_src          varchar2,
    p_contract     varchar2,
    p_currency     varchar2
  )
    return sys_refcursor is
    l_cur sys_refcursor;
    l_kind_list    t_number_list := t_number_list();
    l_null_id      number(1) := -1;
    l_null_varchar varchar2(4) := 'null';
  begin
    if p_out_exchange = 1 then
      l_kind_list.extend();
      l_kind_list(l_kind_list.count) := 0;
    end if;

    if p_out_otc = 1 then
      l_kind_list.extend();
      l_kind_list(l_kind_list.count) := 1;
    end if;

    if p_transfer = 1 then
      l_kind_list.extend();
      l_kind_list(l_kind_list.count) := 2;
    end if;

    open l_cur for
    select --+ cardinality (kl, 3)
           r.req_id,
           k.kind_name,
           trunc(r.import_time) import_date, -- to_char(r.import_time, 'dd.mm.yyyy hh24:mi:ss') import_time,
           to_char(r.import_time, 'hh24:mi:ss') import_time, -- to_char(r.import_time, 'dd.mm.yyyy hh24:mi:ss') import_time,
           r.req_date,
           to_char(r.req_time, 'hh24:mi:ss') req_time,
           r.status_id,
           s.status_name,
           e.error_name,
           r.src,
           r.external_id,
           nvl(n.t_code, '-') t_code,
           r.operation_id,
           r.client_cft_id,
           r.contract,
           r.currency,
           r.amount,
           nontrading_orders_read.get_trade_name(r.marketplace_withdrawal) market_place,
           nontrading_orders_read.get_trade_name(r.marketplace_enroll) market_place_tgt,
           r.status_changed
      from nontrading_orders_buffer r
      join nontrading_orders_status s on s.status_id = r.status_id
      join nontrading_orders_error e on e.error_id = r.error_id
      join nontrading_orders_kind k on k.kind_id = r.kind
      join table(l_kind_list) kl on kl.column_value = r.kind
      left join dnptxop_dbt n on n.t_id = r.operation_id
     where r.import_time >= p_dbegin
       and trunc(r.import_time) <= p_dend
       and (p_status_id = l_null_id or r.status_id = p_status_id)
       and (p_error_id  = l_null_id or r.error_id  = p_error_id)
       and (p_src = l_null_varchar or r.src = p_src)
       and (p_contract = l_null_varchar or r.contract = p_contract)
       and (p_currency = l_null_varchar or r.currency = p_currency)
     order by r.import_time desc;

    return l_cur;
  end get_reqs;
  
  function get_status_name(p_status_id nontrading_orders_status.status_id%type) return nontrading_orders_status.status_name%type deterministic as
    v_name nontrading_orders_status.status_name%type;
  begin
    select s.status_name into v_name from nontrading_orders_status s where s.status_id = p_status_id;
    return v_name;
  exception
    when no_data_found then
      return null;
  end get_status_name;

begin
  g_src_attr('ЕФР')    := 1;
  g_src_attr('ДБО ФЛ') := 3;
  g_src_attr('ДБО ЮЛ') := 4;
  g_src_attr('Свои Инвестиции') := 5;
end nontrading_orders_read;
/
