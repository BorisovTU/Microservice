create or replace package dlng_sec_nptxwrt_read as

  function check_if_cur_allowed (
    p_fiid dfininstr_dbt.t_fiid%type
  ) return number;

end dlng_sec_nptxwrt_read;
/
