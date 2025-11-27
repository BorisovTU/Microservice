begin
  update dnptxop_req_dbt r
     set r.status_id = 4
   where r.operation_id is not null
     and r.status_id != 4
     and exists (select 1
                   from dnotetext_dbt n
                  where n.t_objecttype = 131
                    and n.t_notekind = 103
                    and n.t_documentid = lpad(r.operation_id, 34, '0'));

  update dnptxop_req_dbt r
     set r.status_id = 7
   where r.operation_id is not null
     and r.status_id != 7
     and not exists (select 1
                       from dnptxop_dbt o
                      where o.t_id = r.operation_id);

  commit;
end;
/
