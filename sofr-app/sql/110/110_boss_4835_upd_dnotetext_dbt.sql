begin
-- Коды трейдеров
update dnotetext_dbt n
   set n.t_text = rpad(utl_raw.cast_to_raw(c => case
                                                  when n.t_date <= date '2022-04-18' then
                                                   'p2'
                                                  when n.t_date <= date '2023-07-31' then
                                                   'p8'
                                                  else
                                                   'p1'
                                                end)
                      ,3000
                      ,0)
 where n.t_id in (select t_id
                    from (select t.t_id
                                ,trim(chr(0) from RSB_STRUCT.getString(t.T_TEXT)) txt
                            from dnotetext_dbt t
                           where t.t_objecttype = 149
                             and t.t_notekind = 102) r
                   where lower(txt) in ('peper', 'paper', 'paer', 'paepr')) ;
end;
/