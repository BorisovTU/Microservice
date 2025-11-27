create or replace package body categ_utils as

  procedure create_attr (
    pio_attrid   in out dobjattr_dbt.t_attrid%type,
    p_objecttype        dobjattr_dbt.t_objecttype%type,
    p_groupid           dobjattr_dbt.t_groupid%type,
    p_name              dobjattr_dbt.t_name%type,
    p_fullname          dobjattr_dbt.t_fullname%type
  ) is
  begin
    if pio_attrid is null then
      pio_attrid := nvl(categ_read.get_last_attr_id(p_objecttype => p_objecttype, p_groupid => p_groupid), 0) + 1;
    end if;

    insert into dobjattr_dbt (t_objecttype,
                              t_groupid,
                              t_attrid,
                              t_parentid,
                              t_codelist,
                              t_numinlist,
                              t_nameobject,
                              t_chattr,
                              t_longattr,
                              t_intattr,
                              t_name,
                              t_fullname,
                              t_opendate,
                              t_closedate,
                              t_classificator,
                              t_corractype,
                              t_balance,
                              t_isobject)
    values (p_objecttype,
            p_groupid,
            pio_attrid,
            0,
            chr(1),
            to_char(pio_attrid),
            to_char(pio_attrid),
            chr(0),
            0,
            0,
            p_name,
            p_fullname,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            0,
            chr(1),
            chr(1),
            chr(0));
  end create_attr;

  --Just new categs, without any check, pls.
  --if you need to check smth, write new function/procedure with checks which finally calls this procedure
  procedure add_new (
    p_object_type dobjatcor_dbt.t_objecttype%type,
    p_object      dobjatcor_dbt.t_object%type,
    p_group_id    dobjatcor_dbt.t_groupid%type,
    p_attr_id     dobjatcor_dbt.t_attrid%type,
    p_date        dobjatcor_dbt.t_validfromdate%type default null
  ) is
    l_date dobjatcor_dbt.t_validfromdate%type;
  begin
    l_date := case
                when p_date is not null
                  then p_date
                when RsbSessionData.Curdate != to_date('01.01.0001', 'dd.mm.yyyy')
                  then RsbSessionData.Curdate
                else trunc(sysdate)
              end;
    insert into dobjatcor_dbt (t_objecttype,
                               t_groupid,
                               t_attrid,
                               t_object,
                               t_general,
                               t_validfromdate,
                               t_oper,
                               t_validtodate,
                               t_isauto)
    values (p_object_type,
            p_group_id,
            p_attr_id,
            p_object,
            'X',
            l_date,
            nvl(RsbSessionData.Oper, 9997),
            to_date('31.12.9999', 'dd.mm.yyyy'),
            'X');
  end add_new;
  
  procedure update_categ_by_id (
    p_id          dobjatcor_dbt.t_id%type,
    p_attr_id     dobjatcor_dbt.t_attrid%type,
    p_validtodate dobjatcor_dbt.t_validtodate%type
  ) is
  begin
    update dobjatcor_dbt c
       set c.t_attrid = p_attr_id,
           c.t_validtodate = p_validtodate
     where c.t_id = p_id;
  end update_categ_by_id;
  
  procedure close_categ_by_id (
    p_id   dobjatcor_dbt.t_id%type,
    p_date dobjatcor_dbt.t_validtodate%type
  ) is
  begin
    update dobjatcor_dbt c
       set c.t_validtodate = p_date
     where c.t_id = p_id;
  end close_categ_by_id;

  /*
    сохранение категории.
    Обработанные случаи:
    - аналогичной категории на объекте ещё не было
      просто инсерт в таблицу
    - аналогичная категория на объекте есть и была добавлена в дату p_date
      просто апдейт категории
    - аналогичная категория на объекте есть и была добавлена раньше, чем p_date
      необходимо закрыть старую и инсертить новую
    - на объекте существует аналогичная категория с датой начала позже, чем p_date.
      в данной процедуре эта ситуация не обрабатывается
    
    !Не обработан случай, когда у объекта может быть несколько атрибутов одной категории
  */
  procedure save_categ (
    p_object_type dobjatcor_dbt.t_objecttype%type,
    p_group_id    dobjatcor_dbt.t_groupid%type,
    p_object      dobjatcor_dbt.t_object%type,
    p_attr_id     dobjatcor_dbt.t_attrid%type,
    p_date        dobjatcor_dbt.t_validfromdate%type
  ) is
    l_categ_row dobjatcor_dbt%rowtype;
    l_date      dobjatcor_dbt.t_validfromdate%type := trunc(p_date);
  begin
    l_categ_row := categ_read.get_categ_row(p_object_type => p_object_type,
                                            p_group_id    => p_group_id,
                                            p_object      => p_object,
                                            p_date        => l_date);

    if l_categ_row.t_id is null
    then
      add_new(p_object_type => p_object_type,
              p_object      => p_object,
              p_group_id    => p_group_id,
              p_attr_id     => p_attr_id,
              p_date        => l_date);
    elsif l_categ_row.t_validfromdate = l_date
    then
      update_categ_by_id(p_id          => l_categ_row.t_id,
                         p_attr_id     => p_attr_id,
                         p_validtodate => to_date('31.12.9999', 'dd.mm.yyyy'));
    elsif l_categ_row.t_validfromdate < l_date
    then
      close_categ_by_id(p_id   => l_categ_row.t_id,
                        p_date => l_date - 1);
      add_new(p_object_type => p_object_type,
              p_object      => p_object,
              p_group_id    => p_group_id,
              p_attr_id     => p_attr_id,
              p_date        => l_date);
    end if;
  end save_categ;

  function get_attr_id(p_ObjectType dobjatcor_dbt.t_ObjectType%type
                      ,p_Object     dobjatcor_dbt.t_Object%type
                      ,p_GroupID    dobjatcor_dbt.t_GroupID%type
                      ,p_Date       dobjatcor_dbt.t_ValidFromDate%type default null)
  return dobjattr_dbt.t_AttrID%type deterministic is
    p_AttrID dobjattr_dbt.t_AttrID%type;
    l_date   dobjatcor_dbt.t_ValidFromDate%type;
  begin
    l_date := case
                when p_Date is not null
                  then p_Date
                when RsbSessionData.Curdate != to_date('01.01.0001', 'dd.mm.yyyy')
                  then RsbSessionData.Curdate
                else trunc(sysdate)
              end;

    begin
      select AtCor.t_AttrID
        into p_AttrID
        from dobjatcor_dbt AtCor
       where AtCor.t_ObjectType = p_ObjectType
         and AtCor.t_GroupID = p_GroupID
         and AtCor.t_Object = p_Object
         and AtCor.t_ValidToDate >= l_date
         and AtCor.t_ValidFromDate = (select max(t.t_ValidFromDate)
                                        from dobjatcor_dbt t
                                       where t.t_ObjectType = p_ObjectType
                                         and t.t_GroupID = p_GroupID
                                         and t.t_Object = p_Object
                                         and t.t_ValidFromDate <= l_date
                                         and t.t_ValidToDate >= l_date);
    exception
      when NO_DATA_FOUND then
        p_AttrID := 0;
    end;
    return p_AttrID;

  end get_attr_id;
end categ_utils;
/
