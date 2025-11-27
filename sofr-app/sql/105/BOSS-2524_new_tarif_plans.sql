DECLARE 

  l_curr_id        number(5);

  type t_string_list is table of varchar2(5);
  
  l_target_curr_list t_string_list := t_string_list('KZT', 'BYN', 'AED', 'HKD', 'TRY');
--  l_target_curr_list t_string_list := t_string_list('AED', 'HKD', 'TRY');

  procedure log(p_text varchar2) is
  begin
--    dbms_output.put_line(p_text);
    it_log.log(p_msg      => 'new_tarif_plans. ' || p_text,
               p_msg_type => it_log.C_MSG_TYPE__DEBUG);
  end;
  
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

  PROCEDURE CopySFTarif(p_planID in number, p_com_number in number, p_parent_number in number, p_feetype number, p_parent_com_id number)
  IS
    com_tarsclID number(10);
    parent_tarsclID number(10);
  BEGIN
    IF p_com_number > 0 THEN
      BEGIN
         SELECT tarscl.T_ID
           INTO com_tarsclID
           FROM DSFCONCOM_DBT concom, DSFTARSCL_DBT tarscl
          WHERE concom.t_feetype = p_feetype 
            AND concom.t_commnumber = p_com_number 
            AND concom.t_objectid = p_planID 
            AND concom.t_objecttype = 57 
            AND tarscl.t_concomID = concom.t_ID;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            com_tarsclID := 0;
            if p_parent_com_id != 5943 then --just magic. No comments
              log('К ТП '||p_planID || ' не привязана/привязана с ошибкой комиссия ' || p_com_number || '!');
            end if;
      END;

      BEGIN
         SELECT tarscl.T_ID
           INTO parent_tarsclID
           FROM DSFCONCOM_DBT concom, DSFTARSCL_DBT tarscl
          WHERE concom.t_feetype = p_feetype 
            AND concom.t_commnumber = p_parent_number 
            AND concom.t_objectid = p_planID 
            AND concom.t_objecttype = 57 
            AND tarscl.t_concomID = concom.t_ID;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            parent_tarsclID := 0;
            log('На ТП ' || p_planID || ' нет соответствующей комиссии в USD для копирования тарифной сетки в ' || p_com_number || '!');
      END;

      IF com_tarsclID > 0 THEN
        DELETE FROM DSFTARIF_DBT WHERE T_TARSCLID = com_tarsclID;

        INSERT INTO DSFTARIF_DBT (T_ID, T_TARSCLID, T_SIGN, T_BASETYPE, T_BASESUM, T_TARIFTYPE, T_TARIFSUM, T_MINVALUE, T_MAXVALUE, T_SORT)
        SELECT 0,
               com_tarsclID,
               T_SIGN,
               T_BASETYPE,
               T_BASESUM,
               T_TARIFTYPE,
               T_TARIFSUM,
               T_MINVALUE,
               T_MAXVALUE,
               T_SORT
         FROM DSFTARIF_DBT
        WHERE T_TARSCLID = parent_tarsclID;
      END IF;
    END IF; 
  END CopySFTarif;

BEGIN
  for i in 1..l_target_curr_list.count() loop
    l_curr_id := get_curr_id(p_curr_name => l_target_curr_list(i));

    for rec in (select lnk.t_objectid as plan_id,
                       pc.t_feetype,
                       pc.t_number as parent_com_number,
                       cc.t_number as com_number,
                       pc.t_parentcomissid
                  from dsfcomiss_dbt pc
                  join dsfcomiss_dbt cc on cc.t_parentcomissid = pc.t_parentcomissid and
                                           cc.t_fiid_comm = l_curr_id
                  join dsfconcom_dbt lnk on lnk.t_feetype = pc.t_feetype and
                                            lnk.t_commnumber = pc.t_number
                 where pc.t_servicekind = 21
                   and pc.t_fiid_comm = 7
                   and lnk.t_objecttype = 57
                   and lnk.t_objectid in (select t_sfplanid From dsfplan_dbt)
                   )
    loop
      CopySFTarif(p_planID        => rec.plan_id,
                  p_com_number    => rec.com_number,
                  p_parent_number => rec.parent_com_number,
                  p_feetype       => rec.t_feetype,
                  p_parent_com_id => rec.t_parentcomissid);
      log('End. fiid = ' || l_curr_id || '; com_number = ' || rec.com_number || '; plan_id = ' || rec.plan_id);
    end loop;
    commit;
  end loop;
exception
  when others then
    log('Error: ' || sqlerrm);
    raise;
END;
/