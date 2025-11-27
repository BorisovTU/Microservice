create or replace package payment_utils as

  function get_department_param(
    p_dep_code     ddp_dep_dbt.t_code%type,
    o_bankname out dparty_dbt.t_name%type,
    o_coracc   out dbankdprt_dbt.t_coracc%type
  ) return ddp_dep_dbt.t_partyid%type;
  
  function get_schem_param( 
    p_bank_id       in dbnkschem_dbt.t_bankid%type,
    p_code_currency in dbnkschem_dbt.t_fiid%type,
    o_schem_corrac  out  dcorschem_dbt.t_account%type
  ) return dcorschem_dbt.t_number%type;

  function save_pmpaym (
    p_documentid      dpmpaym_dbt.t_documentid%type,
    p_dockind         dpmpaym_dbt.t_dockind%type,
    p_purpose         dpmpaym_dbt.t_purpose%type,
    p_subpurpose      dpmpaym_dbt.t_subpurpose%type,
    p_currency        dpmpaym_dbt.t_fiid%type,
    p_payer           dpmpaym_dbt.t_payer%type,
    p_payeraccount    dpmpaym_dbt.t_payeraccount%type,
    p_payerbankid     dpmpaym_dbt.t_payerbankid%type,
    p_receiver        dpmpaym_dbt.t_receiver%type,
    p_receiveraccount dpmpaym_dbt.t_receiveraccount%type,
    p_receiverbankid  dpmpaym_dbt.t_receiverbankid%type,
    p_amount          dpmpaym_dbt.t_amount%type,
    p_valuedate       dpmpaym_dbt.t_valuedate%type,
    p_numberpack      dpmpaym_dbt.t_numberpack%type,
    p_futurepayeraccount    dpmpaym_dbt.t_futurepayeraccount%type,
    p_futurereceiveraccount dpmpaym_dbt.t_futurereceiveraccount%type
  ) return dpmpaym_dbt.t_paymentid%type;

  procedure save_pmprop (
    p_paymentid   dpmprop_dbt.t_paymentid%type,
    p_bank_id     number,
    p_debetcredit dpmprop_dbt.t_debetcredit%type,
    p_corracc     dpmprop_dbt.t_ourcorracc%type,
    p_schem_number  dpmprop_dbt.t_corschem%type
  );

  procedure save_pmrmprop (
    p_paymentid         dpmrmprop_dbt.t_paymentid%type,
    p_number            dpmrmprop_dbt.t_number%type,
    p_date              dpmrmprop_dbt.t_date%type,
    p_payername         dpmrmprop_dbt.t_payername%type,
    p_payerbankname     dpmrmprop_dbt.t_payerbankname%type,
    p_payercorracc      dpmrmprop_dbt.t_payercorraccnostro%type,
    p_payerinn          dpmrmprop_dbt.t_payerinn%type,
    p_receivername      dpmrmprop_dbt.t_receiverinn%type,
    p_receiverbankname  dpmrmprop_dbt.t_receiverbankname%type,
    p_receiverinn       dpmrmprop_dbt.t_payerinn%type,
    p_receivercorracc   dpmrmprop_dbt.t_receivercorraccnostro%type,
    p_paydate           dpmrmprop_dbt.t_paydate%type,
    p_ground            dpmrmprop_dbt.t_ground%type
  );

end payment_utils;
/
