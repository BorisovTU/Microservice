
create or replace package nontrading_secur_orders_utils as

    function get_pko_row_by_dealid (
        p_deal_id pko_writeoff.dealid%type
    ) return pko_writeoff%rowtype;

    function get_pko_row_by_id (
        p_id pko_writeoff.id%type
    ) return pko_writeoff%rowtype;

    function is_voluntary_redemption_by_row (
        p_pko_row in out nocopy pko_writeoff%rowtype
    ) return number;

    procedure set_is_enough_quantity (
        p_deal_id number,
        p_is_enough_quantity number
    );

    procedure set_is_limit_corrected (
        p_pko_id pko_writeoff.id%type
    );

    procedure push_to_cancel (
        p_pko_id integer
    );

    procedure push_to_execute_deal (
        p_deal_id integer
    );

    procedure set_is_cancelled_categ (
        p_deal_id integer,
        p_is_cancelled integer
    );

    procedure set_wait_status (
        p_pko_id pko_writeoff.id%type,
        p_is_wait integer
    );

    function get_deal_id_by_code_ts (
        p_code_ts ddl_tick_dbt.t_dealcodets%type
    ) return ddl_tick_dbt.t_dealid%type;

end nontrading_secur_orders_utils;
/