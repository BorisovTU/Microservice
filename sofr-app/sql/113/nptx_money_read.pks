create or replace package nptx_money_read as
  
  GC_PARAM_L1 constant varchar2(128) := '‘•\ˆ’…ƒ€–ˆŸ\…’ƒ‚›… “—…ˆŸ';
    GC_PARAM_STATUS constant varchar2(128) := GC_PARAM_L1||'\‚›ƒ“‡Š€ ‘’€’“‘‚';
      GC_PARAM_STATUS_NOTIFICATION_OD constant varchar2(128) := GC_PARAM_STATUS||'\‚…™…ˆ… „';
      GC_PARAM_STATUS_NOTIFICATION_SUPPORT constant varchar2(128) := GC_PARAM_STATUS||'\‚…™…ˆ…  ‘‚†„…ˆŸ';

    GC_PARAM_LOAD constant varchar2(128) := GC_PARAM_L1||'\‡€ƒ“‡Š€';
      GC_PARAM_LOAD_NOTIFICATION_OD constant varchar2(128) := GC_PARAM_LOAD||'\‚…™…ˆ… „';
      GC_PARAM_LOAD_NOTIFICATION_SUPPORT constant varchar2(128) := GC_PARAM_LOAD||'\‚…™…ˆ…  ‘‚†„…ˆŸ';

    GC_PARAM_TIME constant varchar2(128) := GC_PARAM_L1||'\ƒ€”ˆŠ €’Šˆ';
      GC_PARAM_TIME_START constant varchar2(128) := GC_PARAM_TIME||'\€—€‹ …ˆ„€';
      GC_PARAM_TIME_STOP constant varchar2(128) := GC_PARAM_TIME||'\Š—€ˆ… …ˆ„€';

  function buf_status_ready
    return number deterministic;
  
  function buf_status_wait
    return number deterministic;
  
  function buf_status_done
    return number deterministic;

  function buf_status_error
    return number deterministic;
  
  function buf_status_reject
    return number deterministic;
  
  function buf_status_executing
    return number deterministic;
  
  function buf_status_creating
    return number deterministic;
  
  function buf_status_deleted
    return number deterministic;
  
  function buf_kind_out_exchange
    return number deterministic;
  
  function buf_kind_out_otc
    return number deterministic;

  function buf_kind_transfer
    return number deterministic;
    
  function buf_servkind_stock
    return number deterministic;
    
  function buf_servkind_forts
    return number deterministic;
    
  function buf_servkind_currency
    return number deterministic; 

  function buf_err_no_error
    return number deterministic;

  function buf_err_duplicate
    return number deterministic;
  
  function subkind_out
    return number deterministic;
  
  function subkind_transfer
    return number deterministic;
  
  function buf_err_client_not_found
    return number deterministic;
  
  function buf_err_cntr_not_found
    return number deterministic;
  
  function buf_err_ekk_not_found
    return number deterministic;
  
  function buf_err_cur_not_found
    return number deterministic;
  
  function buf_err_acc_enrl_not_found
    return number deterministic;
  
  function buf_err_acc_wo_not_found
    return number deterministic;
  
  function buf_err_cntr_enrl_not_found
    return number deterministic;
    
  function buf_err_cntr_is_edp
    return number deterministic;
  
  function buf_err_clnt_not_matched_w_cntr
    return number deterministic;
  
  function buf_err_internal_error
    return number deterministic;
  
  function dockind
    return number deterministic;
  
  function rc_place_kind
    return number deterministic;
  
  function bank_place_kind
    return number deterministic;
  
  function nko_nrd_place
    return number deterministic;
  
  function otc_place
    return number deterministic;
  
  function main_sector
    return number deterministic;
    
  function forts_sector
    return number deterministic;
    
  function cur_sector
    return number deterministic;
  
  function note_enroll_allowed
    return varchar2 deterministic;

  procedure z___________func;

  function get_active_req (
    p_src     dnptxop_req_dbt.src%type,
    p_ext_id  dnptxop_req_dbt.external_id%type,
    p_kind    dnptxop_req_dbt.kind%type,
    p_client  dnptxop_req_dbt.client_cft_id%type
  ) return dnptxop_req_dbt.req_id%type;
  
  function get_src_name_rus (
    p_src_name varchar2
  ) return varchar2;
  
  function get_src_name_rus_opcode (
    p_src_name varchar2
  ) return varchar2;  
  
  function get_src_name (
    p_src_name varchar2
  ) return varchar2;
  
  function get_src_ext_name (
    p_src_name varchar2
  ) return varchar2;
  
  function get_sfcontr_account (
    p_sfcontr_id dsfcontr_dbt.t_id%type,
    p_fiid       ddlcontracc_dbt.t_fiid%type,
    p_date       daccount_dbt.t_close_date%type
  ) return dmcaccdoc_dbt.t_account%type;

  function get_fiid_by_ccy (
    p_ccy dfininstr_dbt.t_ccy%type
  ) return dfininstr_dbt.t_fiid%type deterministic;
  
  function get_rest (
    p_account       daccount_dbt.t_account%type,
    p_date          drestdate_dbt.t_restdate%type,
    p_rest_currency drestdate_dbt.t_restcurrency%type
  ) return drestdate_dbt.t_rest%type;

  function get_micex_id
    return number deterministic;

  function get_spbex_id
    return number deterministic;

  function get_nrd_id
    return number deterministic;
  
  function get_servkind(
    p_isexchange in dnptxop_req_dbt.is_exchange%type
  ) return number;

  function check_ekk_sfcontr (
    p_sfcontr_id dsfcontr_dbt.t_id%type,
    p_ekk        ddlobjcode_dbt.t_code%type
  ) return boolean;
  
  function get_party_code (
    p_party_id   dparty_dbt.t_partyid%type,
    p_code_kind  dobjcode_dbt.t_codekind%type
  ) return dobjcode_dbt.t_code%type;
  
  function get_party_name (
    p_party_id   dparty_dbt.t_partyid%type
  ) return dparty_dbt.t_name%type;

  function get_req_row (
    p_req_id dnptxop_req_dbt.req_id%type default null,
    p_oper_id dnptxop_req_dbt.operation_id%type default null
  ) return dnptxop_req_dbt%rowtype;

  procedure get_buf_data_transfer (
    p_req_row              dnptxop_req_dbt%rowtype,
    po_contract        out dnptxop_dbt.t_contract%type,
    po_contract_tgt    out dnptxop_dbt.t_contract%type,
    po_currency        out dnptxop_dbt.t_currency%type,
    po_account         out dnptxop_dbt.t_account%type,
    po_account_tgt     out dnptxop_dbt.t_account%type,
    po_outsum          out dnptxop_dbt.t_outsum%type,
    po_iis             out number
  );

  procedure get_buf_data_out (
    p_req_row              dnptxop_req_dbt%rowtype,
    po_contract        out dnptxop_dbt.t_contract%type,
    po_currency        out dnptxop_dbt.t_currency%type,
    po_account         out dnptxop_dbt.t_account%type,
    po_outsum          out dnptxop_dbt.t_outsum%type,
    po_iis             out number
  );
  
  function get_dep_id_by_code (
    p_code varchar2
  ) return ddp_dep_dbt.t_partyid%type;
  
  function get_func_run_deal_id
    return varchar2 deterministic;

  function get_email_grp_kafka_err
    return number deterministic;

  function is_allowed_autorun (
    p_system_name          nontrading_autorun_config.src%type,
    p_exchange_type        nontrading_autorun_config.exchange_type%type,
    p_exchange_type_target nontrading_autorun_config.exchange_type_target%type,
    p_is_full_rest         nontrading_autorun_config.is_full_rest%type
  ) return nontrading_autorun_config.is_allowed%type;
  
  function is_allowed_autorun_by_nptxop (
    p_operation_id dnptxop_dbt.t_id%type
  ) return nontrading_autorun_config.is_allowed%type;
  
  function is_client_allowed_autorun (
    p_operation_id dnptxop_dbt.t_id%type,
    p_client_id    dnptxop_dbt.t_client%type
  ) return number;
  
  function is_allowed_system_kafka (
    p_system_name varchar2
  ) return number;
  
  function is_allowed_send_status (
    p_system_name varchar2
  ) return number;
  
  procedure get_allowed_period (
    p_date        date,
    po_dbegin out date,
    po_dend   out date
  );
  
  function get_error_text (
    p_error_id dnptxop_err_dbt.error_id%type
  ) return dnptxop_err_dbt.error_name%type ;
  
  function get_exchange(
    p_scr_system in varchar2,
    p_trading_platform in varchar2
  ) return dnptxop_req_dbt.is_exchange%type;
  
  function get_trade_name(
    p_is_exchange in dnptxop_req_dbt.is_exchange%type
  ) return varchar2;
  
  function is_edp_subcontr (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return boolean;

  function check_party_sfcontr (
    p_sfcontr_id dsfcontr_dbt.t_id%type,
    p_party_id   dsfcontr_dbt.t_partyid%type
  ) return boolean;
 
  procedure z___________ui;
                                   
  function check_status_changed (
    p_req_id         dnptxop_req_dbt.req_id%type,
    p_status_changed date
  ) return number;

  function get_reqs (
    p_dbegin       date,
    p_dend         date,
    p_out_exchange number,
    p_out_otc      number,
    p_transfer     number,
    p_status_id    number,
    p_error_id     number,
    p_src          varchar2,
    p_client_code  varchar2,
    p_contract     varchar2,
    p_currency     varchar2
  )
    return sys_refcursor;
 
   function get_status_name(p_status_id dnptxop_status_dbt.status_id%type) return dnptxop_status_dbt.status_name%type  deterministic;
   
   function get_src_SystemOrigin( p_SystemOrigin varchar2) return varchar2;
end nptx_money_read;
/
