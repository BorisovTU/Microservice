declare
  v_trader_code VARCHAR(64);
begin
  for one_rec in (select t_id, txt
                    from (select t.t_id
                                ,trim(chr(0) from RSB_STRUCT.getString(t.t_text)) txt
                            from dnotetext_dbt t
                           where t.t_objecttype in (145, 148)
                             and t.t_notekind = 150)
                   where instr(txt, '/') > 0) 
  loop
    v_trader_code := NVL(SUBSTR(one_rec.txt, INSTR(one_rec.txt, '/') + 1), CHR(1));

    if instr(v_trader_code, '/') > 0 then
      v_trader_code := NVL(SUBSTR(v_trader_code, INSTR(v_trader_code, '/') + 1), CHR(1));
    end if;
    
    if v_trader_code <> CHR(1) and v_trader_code <> '/' then
      update dnotetext_dbt
         set t_text = rpad(utl_raw.cast_to_raw(c => v_trader_code), 3000, 0)
       where t_id = one_rec.t_id;
    else
      delete from dnotetext_dbt
       where t_id = one_rec.t_id;
    end if;
  end loop;
end;
/