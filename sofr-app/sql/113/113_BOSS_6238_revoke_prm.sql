update DDL_LIMITPRM_DBT t
   set t.t_poscode = 'EQTV'
 where t.t_marketkind in (1, 3)
   and t.t_MarketID in (2, 151337)
/
