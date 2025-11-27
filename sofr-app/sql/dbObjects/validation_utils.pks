create or replace package validation_utils is

  procedure not_null(p_value varchar2, p_name varchar2);
  procedure not_null(p_value date,     p_name varchar2);
  procedure not_null(p_value number,   p_name varchar2);

end validation_utils;
/
