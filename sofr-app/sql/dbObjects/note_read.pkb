create or replace package body note_read as

  g_note_text_char varchar2(9) := chr(0)||chr(1)||chr(2)||chr(3)||chr(4)||chr(5)||chr(6)||chr(7)||chr(8);

  function cast_to_raw (
    p_value varchar2
  ) return dnotetext_dbt.t_text%type is
  begin
    return rpad(utl_raw.cast_to_raw(c => p_value), 3000, 0);
  end cast_to_raw;
  
  function cast_to_varchar2 (
    p_raw dnotetext_dbt.t_text%type
  ) return varchar2 is
  begin
    return translate(rsb_struct.getString(p_Value => p_raw), 'A' || g_note_text_char, 'A');
  end cast_to_varchar2;
  
  function get_note (
    p_object_type dnotetext_dbt.t_objecttype%type,
    p_note_kind   dnotetext_dbt.t_notekind%type,
    p_document_id dnotetext_dbt.t_documentid%type
  ) return varchar2 is
    l_note dnotetext_dbt.t_text%type;
  begin
    select n.t_text
      into l_note
      from dnotetext_dbt n
     where n.t_objecttype = p_object_type
       and n.t_notekind = p_note_kind
       and n.t_documentid = p_document_id
       and n.t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy');

    return cast_to_varchar2(p_raw => l_note);
  exception
    when others then
      return null;
  end get_note;
  
  function get_note_money (
    p_object_type dnotetext_dbt.t_objecttype%type,
    p_note_kind   dnotetext_dbt.t_notekind%type,
    p_document_id dnotetext_dbt.t_documentid%type
  ) return number is
    l_note dnotetext_dbt.t_text%type;
  begin
    select n.t_text
      into l_note
      from dnotetext_dbt n
     where n.t_objecttype = p_object_type
       and n.t_notekind = p_note_kind
       and n.t_documentid = p_document_id
       and n.t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy');

    return rsb_struct.getMoney(l_note);
  exception
    when others then
      return null;
  end get_note_money;

  function get_note_row (
    p_object_type dnotetext_dbt.t_objecttype%type,
    p_note_kind   dnotetext_dbt.t_notekind%type,
    p_document_id dnotetext_dbt.t_documentid%type
  ) return dnotetext_dbt%rowtype is
    l_note_row dnotetext_dbt%rowtype;
  begin
    select *
      into l_note_row
      from dnotetext_dbt n
     where n.t_objecttype = p_object_type
       and n.t_notekind = p_note_kind
       and n.t_documentid = p_document_id
       and n.t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy');

    return l_note_row;
  exception
    when others then
      return null;
  end get_note_row;

end note_read;
/
