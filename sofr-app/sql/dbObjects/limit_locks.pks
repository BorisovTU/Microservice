create or replace package limit_locks as

    function get_secur_limit_lock_row (
        p_source_type secur_limit_locks.source_type%type,
        p_source_id   secur_limit_locks.source_id%type,
        p_fiid        secur_limit_locks.fiid%type,
        p_market_id   secur_limit_locks.market_id%type,
        p_contract_id secur_limit_locks.contract_id%type
    ) return secur_limit_locks%rowtype;

    procedure save_secur_limit_lock (
        p_source_type secur_limit_locks.source_type%type,
        p_source_id   secur_limit_locks.source_id%type,
        p_fiid        secur_limit_locks.fiid%type,
        p_market_id   secur_limit_locks.market_id%type,
        p_contract_id secur_limit_locks.contract_id%type,
        p_quantity    secur_limit_locks.quantity%type,
        p_start_date  secur_limit_locks.start_date%type,
        p_end_date    secur_limit_locks.end_date%type
    );

    procedure set_secur_lock (
        p_MarketID       number
       ,p_CalcDate       date
       ,p_UseListClients number
    );

end limit_locks;
/