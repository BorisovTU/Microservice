begin
  execute immediate 'drop table dl_acc_place_servkind_lnk';
exception
  when others then
    it_log.log('error while drop table dl_acc_place_servkind_lnk');
end;
/
DECLARE
  E_OBJECT_EXISTS EXCEPTION;
  PRAGMA EXCEPTION_INIT( E_OBJECT_EXISTS, -955);
BEGIN
  EXECUTE IMMEDIATE 'create table dl_acc_place_servkind_lnk (
  serv_kind     number(10),
  serv_kind_sub number(10),
  place         number(10),
  place_kind    number(10),
  market_place  number(10)
  )';
EXCEPTION
    WHEN E_OBJECT_EXISTS THEN NULL;
END;
/
begin
    merge into dl_acc_place_servkind_lnk l
    using (select 1 as place, 2  as place_kind, 0 as market_place, 1  as serv_kind, 9 as serv_kind_sub from dual union all
           select 4 as place, 46 as place_kind, 2 as market_place, 1  as serv_kind, 8 as serv_kind_sub from dual union all
           select 4 as place, 46 as place_kind, 9 as market_place, 21 as serv_kind, 8 as serv_kind_sub from dual union all
           select 4 as place, 46 as place_kind, 8 as market_place, 15 as serv_kind, 8 as serv_kind_sub from dual
           ) t
    on (l.serv_kind = t.serv_kind and l.serv_kind_sub = t.serv_kind_sub)
    when not matched then insert (place,
                                   place_kind,
                                   market_place,
                                   serv_kind,
                                   serv_kind_sub)
    values (t.place,
            t.place_kind,
            t.market_place,
            t.serv_kind,
            t.serv_kind_sub);
  commit;
end;
/
