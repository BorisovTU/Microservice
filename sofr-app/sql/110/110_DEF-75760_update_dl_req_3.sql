begin
  update ddl_req_dbt
     set t_MethodApplic = 2 /*Бумажный документ*/
   where t_MethodApplic <> 2
     and t_ID in (select ReqID
                    from (select to_number(t.t_documentid) as ReqID
                                ,trim(chr(0) from RSB_STRUCT.getString(t.t_text)) txt
                            from dnotetext_dbt t
                           where t.t_objecttype = 149
                             and t.t_notekind = 102)
                   where (SubStr(lower(txt), 1, 1) = 'p' AND REGEXP_LIKE(TRIM(SubStr(lower(txt), 2)), '^[[:digit:]]+$')) OR INSTR(lower(txt), 'paper') > 0 
                 );
end;
/