create or replace package body nontrading_secur_orders_utils as

    function get_pko_row_by_dealid (
        p_deal_id pko_writeoff.dealid%type
    ) return pko_writeoff%rowtype is
        l_pko_row pko_writeoff%rowtype;
    begin
        select *
          into l_pko_row
          from pko_writeoff w
         where w.dealid = p_deal_id;
        
        return l_pko_row;
    exception
        when others then
            return null;
    end get_pko_row_by_dealid;

    function get_pko_row_by_id (
        p_id pko_writeoff.id%type
    ) return pko_writeoff%rowtype is
        l_pko_row pko_writeoff%rowtype;
    begin
        select *
          into l_pko_row
          from pko_writeoff w
         where w.id = p_id;
        
        return l_pko_row;
    exception
        when others then
            return null;
    end get_pko_row_by_id;

    function is_voluntary_redemption_by_row (
        p_pko_row in out nocopy pko_writeoff%rowtype
    ) return number is
    begin
        return case when p_pko_row.OperType = '102' then 1 else 0 end;
    end is_voluntary_redemption_by_row;

    procedure set_is_enough_quantity (
        p_deal_id number,
        p_is_enough_quantity number
    ) is
        l_attr_id integer;
        l_note_text varchar2(30);
    begin
        if p_is_enough_quantity = 1
        then
            l_attr_id := 1; --"да"
            l_note_text := 'Успешно';
        else
            l_attr_id := 2; --"нет"
            l_note_text := 'Ц/Б Недостаточно';
        end if;

        Rsb_Secur.SetDealAttrID(p_deal_id, sysdate, l_attr_id, 215);

        note_utils.save_note(p_object_type => 101,
                      p_note_kind   => 410,
                      p_document_id => lpad(p_deal_id, 34, '0'),
                      p_note        => l_note_text,
                      p_date        => trunc(sysdate));
    end set_is_enough_quantity;

    procedure set_is_limit_corrected (
        p_pko_id pko_writeoff.id%type
    ) is
    begin
        update pko_writeoff w
           set w.islimitcorrected = 'X'
              ,w.limitcorrectiontimestamp = systimestamp
        where w.id = p_pko_id
          and nvl(w.islimitcorrected, chr(0)) != 'X';
    end set_is_limit_corrected;

    procedure push_to_cancel (
        p_pko_id integer
    ) is
    begin
        funcobj_utils.save_task(
            p_objectid => p_pko_id,
            p_funcid => 8210,
            p_param => null
        );
    end push_to_cancel;

    procedure push_to_execute_deal (
        p_deal_id integer
    ) is
    begin
        funcobj_utils.save_task(
            p_objectid => p_deal_id,
            p_funcid => 8213,
            p_param => null
        );
    end push_to_execute_deal;

    procedure set_is_cancelled_categ (
        p_deal_id integer,
        p_is_cancelled integer
    ) is
    begin
        Rsb_Secur.SetDealAttrID(p_deal_id, sysdate, case when p_is_cancelled = 1 then 2 else 1 end, 213);
    end set_is_cancelled_categ;

    procedure set_wait_status (
        p_pko_id pko_writeoff.id%type,
        p_is_wait integer
    ) is
    begin
        update pko_writeoff
           set step1_waitstatus = case when p_is_wait = 1 then 0 else 1 end
         where id = p_pko_id;
    end set_wait_status;

    function get_deal_id_by_code_ts (
        p_code_ts ddl_tick_dbt.t_dealcodets%type
    ) return ddl_tick_dbt.t_dealid%type is
        l_dealid ddl_tick_dbt.t_dealid%type;
    begin
        select t_dealid
          into l_dealid
          from ddl_tick_dbt
         where t_bofficekind = 127
           and t_dealcodets = p_code_ts;
        
        return l_dealid;
    exception
        when others then
            return null;
    end get_deal_id_by_code_ts;

end nontrading_secur_orders_utils;
/