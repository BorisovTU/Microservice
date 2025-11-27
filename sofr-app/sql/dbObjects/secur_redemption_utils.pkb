create or replace package body secur_redemption_utils is

  g_message_type_creation     constant varchar2(20) := 'Creation CA';
  g_message_type_completion   constant varchar2(20) := 'Completion CA';
  g_message_type_cancellation constant varchar2(20) := 'Cancellation CA';

  g_status_created   constant number(1) := 1;
  g_status_cancelled constant number(1) := 8;
  g_status_cancel_request constant number(2) := 11;

  type t_string_number_map is table of number(10) index by varchar2(100);
  g_depositary_map  t_string_number_map;
  
  procedure z_______________read is
  begin
    null;
  end;
  
  function message_type_creation return varchar2 deterministic is
  begin
    return g_message_type_creation;
  end message_type_creation;
  
  function message_type_completion return varchar2 deterministic is
  begin
    return g_message_type_completion;
  end message_type_completion;
  
  function message_type_cancellation return varchar2 deterministic is
  begin
    return g_message_type_cancellation;
  end message_type_cancellation;
  
  function status_created return number deterministic is
  begin
    return g_status_created;
  end status_created;
  
  function status_cancelled return number deterministic is
  begin
    return g_status_cancelled;
  end status_cancelled;
  
  function status_cancel_request return number deterministic is
  begin
    return g_status_cancel_request;
  end status_cancel_request;
  
  function get_redemption_by_source_id (
    p_source_id secur_redemptions.source_id%type
  ) return secur_redemptions%rowtype is
    l_redemption secur_redemptions%rowtype;
  begin
    select *
      into l_redemption
      from secur_redemptions r
     where r.source_id = p_source_id;

    return l_redemption;
  exception
    when others then
      return null;
  end get_redemption_by_source_id;

  function get_redemption_by_id (
    p_redemption_id secur_redemptions.redemption_id%type
  ) return secur_redemptions%rowtype is
    l_redemption secur_redemptions%rowtype;
  begin
    select *
      into l_redemption
      from secur_redemptions r
     where r.redemption_id = p_redemption_id;

    return l_redemption;
  exception
    when others then
      return null;
  end get_redemption_by_id;

  function is_allowed_processing
  return number is
    l_key_id number(10);
    l_value  varchar2(2);
  begin
    l_key_id := it_rs_interface.get_keyid_parm_path(p_parm_path => '‘•\Š€’ˆ‚›… „…‰‘’‚ˆŸ\‡€ƒ“‡Š€\ƒ€˜…ˆŸ –');
    l_value := it_rs_interface.get_parm_varchar(p_keyid => l_key_id);
    
    return case when l_value = chr(88) then 1 else 0 end;
  exception
    when others then
      return 0;
  end is_allowed_processing;
  
  procedure z_______________utils is
  begin
    null;
  end;

  procedure save_status_history (
    p_redemption_id secur_redemption_status_hist.redemption_id%type,
    p_old_status    secur_redemption_status_hist.old_status%type,
    p_new_status    secur_redemption_status_hist.new_status%type
  ) is
  begin
    insert into secur_redemption_status_hist (id,
                                              redemption_id,
                                              old_status,
                                              new_status)
    values (sq_sec_red_status_hist.nextval,
            p_redemption_id,
            p_old_status,
            p_new_status);
  end save_status_history;

  procedure save_status (
    p_redemption_id secur_redemptions.redemption_id%type,
    p_status_id     secur_redemptions.status_id%type
  ) is
    l_redemption    secur_redemptions%rowtype;
  begin
    l_redemption := get_redemption_by_id(p_redemption_id => p_redemption_id);

    save_status_history(p_redemption_id => p_redemption_id,
                        p_old_status    => l_redemption.status_id,
                        p_new_status    => p_status_id);

    update secur_redemptions
       set status_id = p_status_id,
           updated_time = systimestamp
     where redemption_id = p_redemption_id;
  end save_status;
  
  procedure create_redemption (
    po_id            out secur_redemptions.redemption_id%type,
    p_fix_date           secur_redemptions.fix_date%type,
    p_complete_date      secur_redemptions.complete_date%type,
    p_source_number      secur_redemptions.source_number%type,
    p_source_id          secur_redemptions.source_id%type,
    p_action_type        secur_redemptions.action_type%type,
    p_owner_date         secur_redemptions.list_owners_date%type,
    p_depositary_id      secur_redemptions.depositary_id%type,
    p_isin               secur_redemptions.isin%type,
    p_reg_number         secur_redemptions.reg_number%type,
    p_nrd_code           secur_redemptions.nrd_code%type,
    p_quantity           secur_redemptions.quantity%type,
    p_status_id          secur_redemptions.status_id%type
  ) is
  begin
    insert into secur_redemptions(redemption_id,
                                  fix_date,
                                  complete_date,
                                  source_number,
                                  source_id,
                                  action_type,
                                  list_owners_date,
                                  depositary_id,
                                  isin,
                                  reg_number,
                                  nrd_code,
                                  quantity,
                                  deal_id,
                                  status_id,
                                  created_time,
                                  updated_time)
    values (sq_secur_redemptions.nextval,
            p_fix_date,
            p_complete_date,
            p_source_number,
            p_source_id,
            p_action_type,
            p_owner_date,
            p_depositary_id,
            p_isin,
            p_reg_number,
            p_nrd_code,
            p_quantity,
            -1,
            p_status_id,
            systimestamp,
            systimestamp
           )
    returning redemption_id into po_id;
  end create_redemption;

  procedure update_redemption (
    p_id                 secur_redemptions.redemption_id%type,
    p_complete_date      secur_redemptions.complete_date%type,
    p_owner_date         secur_redemptions.list_owners_date%type,
    p_quantity           secur_redemptions.quantity%type
  ) is
  begin
    update secur_redemptions
       set complete_date = p_complete_date,
           list_owners_date = p_owner_date,
           quantity = p_quantity,
           updated_time = systimestamp
    where redemption_id = p_id;
  end update_redemption;

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
  ) is
    l_redemption    secur_redemptions%rowtype;
    l_depositary_id secur_redemptions.depositary_id%type;
    l_complete_date secur_redemptions.complete_date%type;
    l_owner_date    secur_redemptions.list_owners_date%type;
    l_status_id     secur_redemptions.status_id%type;
    l_null_date     date := to_date('01.01.0001', 'dd.mm.yyyy');
  begin
    if is_allowed_processing() = 0 then
      raise_application_error(-20000, 'processing is not allowed');
    end if;

    l_owner_date := nvl(p_owner_date, l_null_date);
    if p_message_type = message_type_creation() then
      l_complete_date := l_null_date;
    else
      l_complete_date := nvl(p_complete_date, l_null_date);
    end if;
    l_redemption := get_redemption_by_source_id(p_source_id => p_source_id);
    
    if l_redemption.redemption_id is null then
      if not g_depositary_map.exists(p_depositary_name) then
        raise_application_error(-20000, 'Unexpected depositary name: ' || p_depositary_name);
      end if;

      l_depositary_id := g_depositary_map(p_depositary_name);
      l_status_id := case when p_message_type = message_type_cancellation()
                       then status_cancel_request()
                      else status_created()
                     end;

      create_redemption(po_id            => l_redemption.redemption_id,
                        p_fix_date       => p_fix_date,
                        p_complete_date  => l_complete_date,
                        p_source_number  => p_source_number,
                        p_source_id      => p_source_id,
                        p_action_type    => p_action_type,
                        p_owner_date     => l_owner_date,
                        p_depositary_id  => l_depositary_id,
                        p_isin           => p_isin,
                        p_reg_number     => p_reg_number,
                        p_nrd_code       => p_nrd_code,
                        p_quantity       => p_quantity,
                        p_status_id      => l_status_id
      );

      save_status_history(p_redemption_id => l_redemption.redemption_id,
                          p_old_status    => null,
                          p_new_status    => l_status_id);
    else
      update_redemption (p_id            => l_redemption.redemption_id,
                         p_complete_date => l_complete_date,
                         p_owner_date    => l_owner_date,
                         p_quantity      => p_quantity
      );
    end if;

    send_redemption_to_funcobj(p_id => l_redemption.redemption_id);
  end save_redemption;
  
  procedure send_redemption_to_funcobj (
    p_id secur_redemptions.redemption_id%type
  ) is
    l_func_code varchar2(20) := 'SecRedemptGlobal';
  begin
    funcobj_utils.save_task(p_objectid => p_id,
                            p_funcid   => funcobj_utils.get_func_id(p_code => l_func_code),
                            p_param    => null,
                            p_priority => funcobj_utils.get_priority_from_reserve(p_code => l_func_code));
  end send_redemption_to_funcobj;

begin
  g_depositary_map('Š € „') := 4;
  g_depositary_map('€ ‘') := 1450;
end secur_redemption_utils;
/
