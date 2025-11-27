create or replace package body string_utils as 

  procedure list_append(p_list in out nocopy t_string_list, p_val in varchar2)
  is
  begin
    p_list.extend;
    p_list(p_list.count) := p_val;
  end list_append;

  function str_to_list(
    p_str in varchar2,
    p_delim in varchar2
  ) return t_string_list deterministic
  is
    i pls_integer;
    j pls_integer := 1;
    str varchar2(32767);
    res t_string_list := t_string_list();
  begin
    if p_str is not null then
      if p_delim is null then
        list_append(res, p_str);
      else
        str := p_str || p_delim;
        i := instr(str, p_delim);
        while i > 0 loop
          list_append(res, substr(str, j, i-j));
          j := i + length(p_delim);
          i := instr(str, p_delim, j);
        end loop;
      end if;
    end if;
    return res;
  end str_to_list;

end;
/
