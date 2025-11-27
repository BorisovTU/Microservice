create or replace package body limit_locks as

    type t_number_string_map is table of varchar2(100) index by pls_integer;
    g_contract_ekk_cache t_number_string_map;

    function get_secur_limit_lock_row (
        p_source_type secur_limit_locks.source_type%type,
        p_source_id   secur_limit_locks.source_id%type,
        p_fiid        secur_limit_locks.fiid%type,
        p_market_id   secur_limit_locks.market_id%type,
        p_contract_id secur_limit_locks.contract_id%type
    ) return secur_limit_locks%rowtype as
        l_limit_lock_row secur_limit_locks%rowtype;
    begin
        select *
          into l_limit_lock_row
          from secur_limit_locks l
         where l.source_type = p_source_type
           and l.source_id = p_source_id
           and l.fiid = p_fiid
           and (l.market_id = p_market_id or l.market_id is null and p_market_id is null)
           and (l.contract_id = p_contract_id or l.contract_id is null and p_contract_id is null);
   
        return l_limit_lock_row;
    exception
        when no_data_found then
            return null;
    end get_secur_limit_lock_row;

    procedure create_secur_limit_lock (
        p_source_type secur_limit_locks.source_type%type,
        p_source_id   secur_limit_locks.source_id%type,
        p_fiid        secur_limit_locks.fiid%type,
        p_market_id   secur_limit_locks.market_id%type,
        p_contract_id secur_limit_locks.contract_id%type,
        p_quantity    secur_limit_locks.quantity%type,
        p_start_date  secur_limit_locks.start_date%type,
        p_end_date    secur_limit_locks.end_date%type
    ) is
    begin
        insert into secur_limit_locks (
            lock_id,
            source_type,
            source_id,
            fiid,
            market_id,
            contract_id,
            quantity,
            start_date,
            end_date
        )
        values (
            sq_secur_limit_locks.nextval,
            p_source_type,
            p_source_id,
            p_fiid,
            p_market_id,
            p_contract_id,
            p_quantity,
            p_start_date,
            p_end_date
        );
    end create_secur_limit_lock;

    procedure update_secur_limit_lock (
        p_lock_id     secur_limit_locks.lock_id%type,
        p_quantity    secur_limit_locks.quantity%type,
        p_start_date  secur_limit_locks.start_date%type,
        p_end_date    secur_limit_locks.end_date%type
    ) is
    begin
        update secur_limit_locks
           set quantity = p_quantity,
               start_date = p_start_date,
               end_date = p_end_date,
               updated_time = systimestamp
         where lock_id = p_lock_id;
    end update_secur_limit_lock;

    procedure save_secur_limit_lock (
        p_source_type secur_limit_locks.source_type%type,
        p_source_id   secur_limit_locks.source_id%type,
        p_fiid        secur_limit_locks.fiid%type,
        p_market_id   secur_limit_locks.market_id%type,
        p_contract_id secur_limit_locks.contract_id%type,
        p_quantity    secur_limit_locks.quantity%type,
        p_start_date  secur_limit_locks.start_date%type,
        p_end_date    secur_limit_locks.end_date%type
    ) as
        l_limit_lock_row secur_limit_locks%rowtype;
        l_end_date secur_limit_locks.end_date%type;
    begin
        l_end_date := nvl(p_end_date, to_date('31.12.9999', 'dd.mm.yyyy'));

        l_limit_lock_row := get_secur_limit_lock_row(
            p_source_type => p_source_type,
            p_source_id => p_source_id,
            p_fiid => p_fiid,
            p_market_id => p_market_id,
            p_contract_id => p_contract_id
        );

        if l_limit_lock_row.lock_id is null
        then
            create_secur_limit_lock(
                p_source_type => p_source_type,
                p_source_id => p_source_id,
                p_fiid => p_fiid,
                p_market_id => p_market_id,
                p_contract_id => p_contract_id,
                p_quantity => p_quantity,
                p_start_date => p_start_date,
                p_end_date => l_end_date
            );
        else
            update_secur_limit_lock(
                p_lock_id => l_limit_lock_row.lock_id,
                p_quantity => p_quantity,
                p_start_date => p_start_date,
                p_end_date => l_end_date
            );
        end if;

    exception
        when others then
            it_log.log_error(p_object => 'limit_locks.save_secur_limit_lock',
                             p_msg    => 'Error ' || sqlerrm);
            raise_application_error(-20000, 'Error ' || sqlerrm, true);
    end save_secur_limit_lock;

    procedure set_secur_lock (
        p_MarketID       number
       ,p_CalcDate       date
       ,p_UseListClients number
    ) as
        l_limit_kind_list t_number_list;
        l_calc_date date; --дата расчёта с учётом limit_kind
        l_ekk varchar2(100);
        l_filtered_ekk_list t_string_list := t_string_list();
    begin
        l_limit_kind_list := t_number_list(0, 1, 2, 365);

        if p_UseListClients = 1
        then
            select t_clientcode
              bulk collect into l_filtered_ekk_list
              from ddl_panelcontr_dbt p
             where t_calc_sid = rshb_rsi_sclimit.g_calc_panelcontr
               and p.t_setflag = chr(88);
        end if;

        --посчитать отдельно для каждого limit_kind
        for idx in l_limit_kind_list.first .. l_limit_kind_list.last 
        loop
            l_calc_date := trunc(p_CalcDate) + l_limit_kind_list(idx);

            --найти все подходящие блокировки на дату l_calc_date
            for locks in (
                    with filtered_locks as (
                        select /*+ materialize*/ l.fiid,
                               l.market_id,
                               l.contract_id,
                               l.quantity
                          from secur_limit_locks l
                         where l.start_date <= l_calc_date
                           and l.end_date > l_calc_date
                           and (l.market_id = p_MarketID or l.market_id is null)
                    )
                    ,fiid_locks as (
                        select fiid from filtered_locks where market_id is null and contract_id is null and quantity is null
                    )
                    ,market_locks as (
                        select fiid, market_id from filtered_locks where contract_id is null and quantity is null
                    )
                    ,contract_locks as (
                        select fiid, market_id, contract_id from filtered_locks where quantity is null
                    )
                    select ll.fiid,
                           case when fl.fiid is not null then null else ll.market_id end as market_id,
                           case when coalesce(fl.fiid, ml.fiid) is not null then null else ll.contract_id end as contract_id,
                           case when coalesce(fl.fiid, ml.fiid, cl.fiid) is not null then null else sum(ll.quantity) end quantity
                      from filtered_locks ll
                      left join fiid_locks fl on fl.fiid = ll.fiid
                      left join market_locks ml on ml.fiid = ll.fiid
                                                and ml.market_id = ll.market_id
                      left join contract_locks cl on cl.fiid = ll.fiid
                                                and cl.market_id = ll.market_id
                                                and cl.contract_id = ll.contract_id
                     group by ll.fiid,
                             case when fl.fiid is not null then null else ll.market_id end,
                             case when coalesce(fl.fiid, ml.fiid) is not null then null else ll.contract_id end,
                             coalesce(fl.fiid, ml.fiid, cl.fiid)
            )  
            loop
                l_ekk := null;
                if locks.contract_id is not null
                then
                    if not g_contract_ekk_cache.exists(locks.contract_id)
                    then
                        g_contract_ekk_cache(locks.contract_id) := sfcontr_read.get_ekk_subcontr(p_sfcontr_id => locks.contract_id);    
                    end if;
                    l_ekk := g_contract_ekk_cache(locks.contract_id);
                end if;

                update ddl_limitsecurites_dbt l
                   set l.t_open_balance = l.t_open_balance - nvl(locks.quantity, l.t_open_balance)
                 where l.t_date = p_CalcDate
                   and l.t_market = p_MarketID
                   and l.t_client_code = nvl(l_ekk, l.t_client_code)
                   and l.t_security = locks.fiid
                   and l.t_limit_kind = l_limit_kind_list(idx)
                   and (p_UseListClients = 0 or l.t_client_code in (select column_value from table(l_filtered_ekk_list)));
            end loop;
        end loop;
    end set_secur_lock;

end limit_locks;
/