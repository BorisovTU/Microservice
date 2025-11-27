CREATE OR REPLACE PACKAGE trgpckg_dacctrn_dbt AS
  TYPE t_utableprocessevent IS TABLE OF uTableProcessEvent_dbt%rowtype INDEX BY binary_integer;
  TYPE t_upickupdel IS TABLE OF uPickupDel_dbt%rowtype INDEX BY binary_integer;
  
  -- „«ï dacctrn_dbt_pickupdel
  v_upickupdel t_upickupdel;
  
  -- „«ï dacctrn_dbt_delete
  v_tableprocessevent t_utableprocessevent;
  
  -- „«ï dacctrn_dbt_synh
  v_tableprocessevent_upd t_utableprocessevent;
  v_tableprocessevent_ins t_utableprocessevent;
END trgpckg_dacctrn_dbt;
/
