declare
  l_dt date := sysdate - 1;
begin
   delete from dobjcode_dbt c
   where c.t_objecttype = 9
     and c.t_codekind = 11
     and c.t_code in ('KZT', 'BYN', 'AED', 'HKD', 'TRY')
     and not exists (select 1
                       from dfininstr_dbt f
                      where f.t_fiid = c.t_objectid
                        and f.t_fi_kind = 1
                        and f.t_ccy = c.t_code);

  insert into dobjcode_dbt (t_objecttype,
                            t_codekind,
                            t_objectid,
                            t_code,
                            t_state,
                            t_bankdate,
                            t_sysdate,
                            t_systime,
                            t_userid,
                            t_unique,
                            t_autokey,
                            t_bankclosedate,
                            t_normcode)
  select 9, 11, f.t_fiid, f.t_ccy, 0, l_dt, l_dt, l_dt, 1, chr(0), 0, to_date('01.01.0001', 'dd.mm.yyyy'), chr(0)
    from dfininstr_dbt f
   where f.t_ccy in ('KZT', 'BYN', 'AED', 'HKD', 'TRY')
     and not exists (select 1
                       from dobjcode_dbt c
                     where c.t_objecttype = 9
                       and c.t_codekind = 11
                       and c.t_code = f.t_ccy);

  commit;
end;
/
