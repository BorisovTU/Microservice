create or replace package categ_read as

  function get_categ_row (
    p_object_type dobjatcor_dbt.t_objecttype%type,
    p_group_id    dobjatcor_dbt.t_groupid%type,
    p_object      dobjatcor_dbt.t_object%type,
    p_date        dobjatcor_dbt.t_validfromdate%type
  ) return dobjatcor_dbt%rowtype;
  
  function get_attr_id (
    p_object_type dobjatcor_dbt.t_objecttype%type,
    p_group_id    dobjatcor_dbt.t_groupid%type,
    p_object      dobjatcor_dbt.t_object%type,
    p_date        dobjatcor_dbt.t_validfromdate%type
  ) return dobjatcor_dbt.t_attrid%type;

  function get_last_attr_id (
    p_objecttype dobjattr_dbt.t_objecttype%type,
    p_groupid    dobjattr_dbt.t_groupid%type
  ) return dobjattr_dbt.t_attrid%type;
  
end categ_read;
/
