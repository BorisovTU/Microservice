create or replace package secur_redemption_utils is
  
  function message_type_creation     return varchar2 deterministic;
  function message_type_completion   return varchar2 deterministic;
  function message_type_cancellation return varchar2 deterministic;
  
  function status_created return number deterministic;

  procedure save_status (
    p_redemption_id secur_redemptions.redemption_id%type,
    p_status_id     secur_redemptions.status_id%type
  );

  procedure save_redemption (
    p_message_type    varchar2,
    p_fix_date        secur_redemptions.fix_date%type,
    p_complete_date   secur_redemptions.complete_date%type,
    p_source_number   secur_redemptions.source_number%type,
    p_source_id       secur_redemptions.source_id%type,
    p_action_type     secur_redemptions.action_type%type,
    p_owner_date      secur_redemptions.list_owners_date%type,
    p_depositary_name varchar2,
    p_isin            secur_redemptions.isin%type,
    p_reg_number      secur_redemptions.reg_number%type,
    p_nrd_code        secur_redemptions.nrd_code%type,
    p_quantity        secur_redemptions.quantity%type
  );
  
  procedure send_redemption_to_funcobj (
    p_id secur_redemptions.redemption_id%type
  );

end secur_redemption_utils;
/
