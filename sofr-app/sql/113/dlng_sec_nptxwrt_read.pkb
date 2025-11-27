create or replace package body dlng_sec_nptxwrt_read as

  function get_unallowed_cur_setting
    return varchar2 is 
  begin
    return it_rs_interface.get_parm_varchar_path(p_parm_path => '‘•\ŽŠ…‘ŠŽ… Ž‘‹“†ˆ‚€ˆ…\‘ˆ‘ŽŠ_‚€‹ž’_‡€…’_‘ˆ‘_‡€—');
  end get_unallowed_cur_setting;
  
  function check_if_cur_allowed (
    p_fiid dfininstr_dbt.t_fiid%type
  ) return number is
    l_unallowed_curs_str varchar2(4000);
    l_cnt number(9);
  begin
    l_unallowed_curs_str := get_unallowed_cur_setting();
    
    select /*+ cardinality(t 5)*/ count(*)
      into l_cnt
      from table(string_utils.str_to_list(p_str   => l_unallowed_curs_str,
                                          p_delim => ',')) t
      join dfininstr_dbt f on f.t_ccy = t.column_value
     where f.t_fiid = p_fiid;
    
    return case when l_cnt > 0 then 0 else 1 end;
  end check_if_cur_allowed;

end dlng_sec_nptxwrt_read;
