create or replace package body validation_utils is

  procedure not_null (p_value varchar2, p_name varchar2) is
  begin
    if p_value is null then
      raise_application_error(-20000, p_name || ' required');
    end if;
  end not_null;

  procedure not_null (p_value date, p_name varchar2) is
  begin
    if p_value is null then
      raise_application_error(-20000, p_name || ' required');
    end if;
  end not_null;

  procedure not_null (p_value number, p_name varchar2) is
  begin
    if p_value is null then
      raise_application_error(-20000, p_name || ' required');
    end if;
  end not_null;

end validation_utils;
/
