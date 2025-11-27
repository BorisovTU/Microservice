create or replace package categ_utils as

  procedure create_attr (
    pio_attrid   in out dobjattr_dbt.t_attrid%type,
    p_objecttype        dobjattr_dbt.t_objecttype%type,
    p_groupid           dobjattr_dbt.t_groupid%type,
    p_name              dobjattr_dbt.t_name%type,
    p_fullname          dobjattr_dbt.t_fullname%type
  );

  procedure add_new (
    p_object_type dobjatcor_dbt.t_objecttype%type,
    p_object      dobjatcor_dbt.t_object%type,
    p_group_id    dobjatcor_dbt.t_groupid%type,
    p_attr_id     dobjatcor_dbt.t_attrid%type,
    p_date        dobjatcor_dbt.t_validfromdate%type default null
  );

  procedure save_categ (
    p_object_type dobjatcor_dbt.t_objecttype%type,
    p_group_id    dobjatcor_dbt.t_groupid%type,
    p_object      dobjatcor_dbt.t_object%type,
    p_attr_id     dobjatcor_dbt.t_attrid%type,
    p_date        dobjatcor_dbt.t_validfromdate%type
  );
  
  function get_attr_id(p_ObjectType dobjatcor_dbt.t_ObjectType%type
                      ,p_Object     dobjatcor_dbt.t_Object%type
                      ,p_GroupID    dobjatcor_dbt.t_GroupID%type
                      ,p_Date       dobjatcor_dbt.t_ValidFromDate%type default null)
  return dobjattr_dbt.t_AttrID%type deterministic;
end categ_utils;
/
