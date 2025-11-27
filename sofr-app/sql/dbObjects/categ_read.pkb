create or replace package body categ_read as

  /* Не обработана ситуация, когда у объекта может быть несколько атрибутов одной категории
     Для этого следует сделать отдельную функцию get_categ_row_list, чтобы и обрабатывать результат явно иначе
  */
  function get_categ_row (
    p_object_type dobjatcor_dbt.t_objecttype%type,
    p_group_id    dobjatcor_dbt.t_groupid%type,
    p_object      dobjatcor_dbt.t_object%type,
    p_date        dobjatcor_dbt.t_validfromdate%type
  ) return dobjatcor_dbt%rowtype is
    l_categ_row dobjatcor_dbt%rowtype;
  begin
    select *
      into l_categ_row
      from dobjatcor_dbt c
     where c.t_objecttype = p_object_type
       and c.t_groupid = p_group_id
       and c.t_object = p_object
       and c.t_validfromdate <= p_date
       and (   p_date is null and c.t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy')
            or c.t_validtodate > p_date);

    return l_categ_row;
  exception
    when others then
      return null;
  end get_categ_row;

  function get_attr_id (
    p_object_type dobjatcor_dbt.t_objecttype%type,
    p_group_id    dobjatcor_dbt.t_groupid%type,
    p_object      dobjatcor_dbt.t_object%type,
    p_date        dobjatcor_dbt.t_validfromdate%type
  ) return dobjatcor_dbt.t_attrid%type is
    l_attr_id dobjatcor_dbt.t_attrid%type;
  begin
    select c.t_attrid
      into l_attr_id
      from dobjatcor_dbt c
     where c.t_objecttype = p_object_type
       and c.t_groupid = p_group_id
       and c.t_object = p_object
       and (p_date is null or c.t_validfromdate <= p_date)
       and (   p_date is null and c.t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy')
            or c.t_validtodate > p_date);

    return l_attr_id;
  exception
    when others then
      return null;
  end get_attr_id;

  function get_last_attr_id (
    p_objecttype dobjattr_dbt.t_objecttype%type,
    p_groupid    dobjattr_dbt.t_groupid%type
  ) return dobjattr_dbt.t_attrid%type is
    l_attr_id dobjattr_dbt.t_attrid%type;
  begin
    select max(t_attrid)
      into l_attr_id
      from dobjattr_dbt a
     where a.t_objecttype = p_objecttype
       and a.t_groupid = p_groupid;

    return l_attr_id;
  end get_last_attr_id;
end categ_read;
/
