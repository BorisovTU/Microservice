update DDL_LIMITPRM_DBT t
   set t.t_poscode = case
                       when t.t_marketkind = 3 then
                        'RTOD'
                       when t.t_marketkind = 1
                            and t.t_MarketID = 2 then
                        'EQTV'
                       when t.t_marketkind = 1
                            and t.t_MarketID = 151337 then
                        'ZICB'
                       else
                        t.t_poscode
                     end
 where t.t_marketkind in (1, 3)
   and t.t_MarketID in (2, 151337)
/