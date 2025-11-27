create or replace package string_utils as 

  function str_to_list(
    p_str in varchar2,
    p_delim in varchar2
  ) return t_string_list deterministic;

end;
/
