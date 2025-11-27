declare
  g_source_curr_id number(3) := 7;
  g_begin_date     date      := to_date('01.01.2024', 'dd.mm.yyyy');
  l_curr_id        number(5);
  
  type t_string_list is table of varchar2(5);
  
  l_target_curr_list t_string_list := t_string_list('KZT', 'BYN', 'AED', 'HKD', 'TRY');
--  l_target_curr_list t_string_list := t_string_list('AED', 'HKD', 'TRY');
  
  function get_curr_name(p_curr_id dfininstr_dbt.t_fiid%type)
    return dfininstr_dbt.t_ccy%type is
    l_curr_name dfininstr_dbt.t_ccy%type;
  begin
    select f.t_ccy
      into l_curr_name
      from dfininstr_dbt f
     where f.t_fiid = p_curr_id;
    
    return l_curr_name;
  end get_curr_name;
  
  function get_curr_id(p_curr_name dfininstr_dbt.t_ccy%type)
    return dfininstr_dbt.t_fiid%type is
    l_fiid dfininstr_dbt.t_fiid%type;
  begin
    select f.t_fiid
      into l_fiid
      from dfininstr_dbt f
     where f.t_ccy = p_curr_name;
    
    return l_fiid;
  end get_curr_id;
  
  function get_tarsclid (
    p_feetype     dsftarscl_dbt.t_feetype%type,
    p_commnumber  dsftarscl_dbt.t_commnumber%type
  ) return dsftarscl_dbt.t_id%type is
    l_id dsftarscl_dbt.t_id%type;
  begin
    select t_id
      into l_id
      from dsftarscl_dbt t
     where t.t_feetype = p_feetype
       and t.t_commnumber = p_commnumber
       and t.t_concomid = 0;

    return l_id;
  exception
    when no_data_found then
      return 0;
  end get_tarsclid;
  
  procedure insert_sf_commis(
    p_source_curr_id     dsfcomiss_dbt.t_fiid_comm%type,
    p_source_com_id      dsfcomiss_dbt.t_comissid%type,
    p_target_curr_id     dsfcomiss_dbt.t_fiid_comm%type
  ) is
    l_source_curr_name varchar2(4);
    l_target_curr_name varchar2(4);
    l_feetype          dsfcomiss_dbt.t_feetype%type;
    l_comnumber        dsfcomiss_dbt.t_number%type;
    l_com_tarsclid     dsftarscl_dbt.t_id%type;
    l_parent_tarsclid  dsftarscl_dbt.t_id%type;
    l_parent_number    dsfcomiss_dbt.t_number%type;
    l_check            number(1);
  begin
    l_source_curr_name := get_curr_name(p_curr_id => p_source_curr_id);
    l_target_curr_name := get_curr_name(p_curr_id => p_target_curr_id);
    
    --check
    begin
      select sign(count(1))
        into l_check
        from dsfcomiss_dbt sc
        join dsfcomiss_dbt tc on tc.t_parentcomissid = sc.t_parentcomissid and
                                 tc.t_fiid_comm = p_target_curr_id
       where sc.t_comissid = p_source_com_id;
    end;

    if l_check > 0 then
      return;
    end if;
    
    select sc.t_number, sc.t_feetype, (select max(t_number) + 1 from dsfcomiss_dbt)
      into l_parent_number, l_feetype, l_comnumber
      from dsfcomiss_dbt sc
     where sc.t_comissid = p_source_com_id;

    insert into dsfcomiss_dbt(t_feetype,
                              t_number,
                              t_code,
                              t_name,
                              t_calcperiodtype,
                              t_calcperiodnum,
                              t_date,
                              t_paynds,
                              t_fiid_comm,
                              t_getsummin,
                              t_summin,
                              t_summax,
                              t_ratetype,
                              t_receiverid,
                              t_incfeetype,
                              t_inccommnumber,
                              t_formalg,
                              t_servicekind,
                              t_servicesubkind,
                              t_calccomisssumalg,
                              t_setaccsearchalg,
                              t_fiid_paysum,
                              t_datebegin,
                              t_dateend,
                              t_instantpayment,
                              t_productid,
                              t_ndscateg,
                              t_isfreeperiod,
                              t_comment,
                              t_comissid,
                              t_parentcomissid,
                              t_isbankexpenses,
                              t_iscompensationcom)
    select  s.t_feetype,
            l_comnumber,
            replace(s.t_code, l_source_curr_name, l_target_curr_name),
            s.t_name,
            s.t_calcperiodtype,
            s.t_calcperiodnum,
            s.t_date,
            s.t_paynds,
            p_target_curr_id,
            s.t_getsummin,
            s.t_summin,
            s.t_summax,
            s.t_ratetype,
            s.t_receiverid,
            s.t_incfeetype,
            s.t_inccommnumber,
            s.t_formalg,
            s.t_servicekind,
            s.t_servicesubkind,
            s.t_calccomisssumalg,
            s.t_setaccsearchalg,
            case s.t_fiid_paysum
              when p_source_curr_id then
                p_target_curr_id
              else
                s.t_fiid_paysum
            end,
            g_begin_date,
            s.t_dateend,
            s.t_instantpayment,
            s.t_productid,
            s.t_ndscateg,
            s.t_isfreeperiod,
            s.t_comment,
            0,
            s.t_parentcomissid,
            s.t_isbankexpenses,
            s.t_iscompensationcom
       from dsfcomiss_dbt s
      where s.t_comissid = p_source_com_id;

    INSERT INTO DSFCALCAL_DBT (T_FEETYPE,T_COMMNUMBER,T_KIND,T_NUMBER,T_FILTERTYPE,T_FILTERMACRO,T_SCALETYPE,T_SCALEMACRO,T_SCALEMACRORET,
                               T_CALCMETHOD,T_FIID_TARSCL,T_SUMMIN,T_SUMMAX,T_DESCRIPTION,T_BINDTOOBJECTS,T_ISALLOCATE,T_CONCOMID,T_ISBATCHMODE,T_FMTBLOBDATA_XXXX)
    SELECT T_FEETYPE, l_comnumber, T_KIND, T_NUMBER, T_FILTERTYPE, T_FILTERMACRO, T_SCALETYPE, T_SCALEMACRO, T_SCALEMACRORET,
           T_CALCMETHOD,
           case T_FIID_TARSCL
             when p_source_curr_id then
               p_target_curr_id
             else
              T_FIID_TARSCL
            end,
           T_SUMMIN, T_SUMMAX, T_DESCRIPTION, T_BINDTOOBJECTS, T_ISALLOCATE, T_CONCOMID, T_ISBATCHMODE, T_FMTBLOBDATA_XXXX
      FROM DSFCALCAL_DBT alg
     where (alg.t_feetype, alg.t_commnumber) in (select c.t_feetype, c.t_number
                                                   from dsfcomiss_dbt c
                                                  where c.t_comissid = p_source_com_id)
       and alg.t_concomid = 0;

 
    INSERT INTO DSFTARSCL_DBT (T_FEETYPE, T_COMMNUMBER, T_ALGKIND, T_ALGNUMBER, T_BEGINDATE, T_ISBLOCKED, T_ID, T_ENDDATE, T_CONCOMID)
    values (l_feetype,
            l_comnumber,
            8,
            1,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            chr(0),
            0,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            0);
      
    l_com_tarsclid    := get_tarsclid(p_feetype => l_feetype, p_commnumber => l_comnumber);
    l_parent_tarsclid := get_tarsclid(p_feetype => l_feetype, p_commnumber => l_parent_number);
 
    IF l_com_tarsclid > 0 AND l_parent_tarsclid > 0 THEN
      INSERT INTO DSFTARIF_DBT (T_ID, T_TARSCLID, T_SIGN, T_BASETYPE, T_BASESUM, T_TARIFTYPE, T_TARIFSUM, T_MINVALUE, T_MAXVALUE, T_SORT)
      SELECT 0, l_com_tarsclid, T_SIGN, T_BASETYPE, T_BASESUM, T_TARIFTYPE, T_TARIFSUM,T_MINVALUE,T_MAXVALUE,T_SORT
        FROM DSFTARIF_DBT
       WHERE T_TARSCLID = l_parent_tarsclid;
    END IF;

    INSERT INTO DOBJATCOR_DBT (T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_ISAUTO)
    SELECT T_OBJECTTYPE, T_GROUPID, T_ATTRID, '000010' || l_comnumber, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_ISAUTO
      FROM DOBJATCOR_DBT
     WHERE T_OBJECTTYPE = 650
       AND T_OBJECT = '000010' || l_parent_number;
  end insert_sf_commis;

  procedure create_comis_template (
    p_servicekind number,
    p_curr_id     number
  ) is
  begin
    for src_com in (select c.t_comissid
                      from dsfcomiss_dbt c
                     where c.t_servicekind = p_servicekind
                       and c.t_fiid_comm = g_source_curr_id) loop
      insert_sf_commis(p_source_curr_id => g_source_curr_id,
                       p_source_com_id  => src_com.t_comissid,
                       p_target_curr_id => p_curr_id);
    end loop;
    it_log.log(p_msg      => 'create_comis_template with cur = '  || p_curr_id,
               p_msg_type => it_log.C_MSG_TYPE__DEBUG);
  end create_comis_template;
  
begin
  for i in 1..l_target_curr_list.count() loop
    l_curr_id := get_curr_id(p_curr_name => l_target_curr_list(i));
    create_comis_template(p_servicekind => 21,
                          p_curr_id     => l_curr_id);
    commit;
  end loop;
exception
  when others then
    it_error.put_error_in_stack;
    it_log.log(p_msg      => 'Save new commis. error',
               p_msg_type => it_log.C_MSG_TYPE__ERROR);
    it_error.clear_error_stack;
    raise;
end;
/