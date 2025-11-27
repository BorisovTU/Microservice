create or replace function NtpxMessageRender(p_text in clob, p_values_json in clob) return clob is
    v_pos     pls_integer := 1;
    v_start   pls_integer;
    v_end     pls_integer;
    v_out     clob;
    v_name    varchar2(4000);
    v_val     clob;
    j         json_object_t;
  begin
    dbms_lob.createtemporary(v_out, true);
    j := json_object_t.parse(p_values_json);

    loop

      v_start := instr(p_text, '\{', v_pos);
      exit when v_start = 0;

      if v_start > 1 and substr(p_text, v_start-1, 1) = '\' then
        dbms_lob.append(v_out, substr(p_text, v_pos, v_start - v_pos)); -- до первого слеша
        dbms_lob.append(v_out, '\{');                                   -- буквальный \{
        v_pos := v_start + 2;                                           -- после "\{"
        continue;
      end if;

      dbms_lob.append(v_out, substr(p_text, v_pos, v_start - v_pos));

      v_end := instr(p_text, '}', v_start+2);
      if v_end = 0 then
        dbms_lob.append(v_out, substr(p_text, v_start));
        return v_out;
      end if;

      v_name := substr(p_text, v_start+2, v_end - (v_start+2));

      begin
        v_val := j.get_string(v_name);
      exception when others then
        v_val := '\{'||v_name||'}';
      end;
      
      if v_val is null then
        v_val := '\{'||v_name||'}';
      end if;

      dbms_lob.append(v_out, v_val);
      v_pos := v_end + 1;  -- продолжить после }
    end loop;

    dbms_lob.append(v_out, substr(p_text, v_pos));

    v_out := replace(v_out, CHR(10), '<br>');

    return v_out;
  end;
/