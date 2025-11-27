create or replace package note_read as

  function cast_to_raw (
    p_value varchar2
  ) return dnotetext_dbt.t_text%type;
  
  function cast_to_varchar2 (
    p_raw dnotetext_dbt.t_text%type
  ) return varchar2;
  
  function get_note (
    p_object_type dnotetext_dbt.t_objecttype%type,
    p_note_kind   dnotetext_dbt.t_notekind%type,
    p_document_id dnotetext_dbt.t_documentid%type
  ) return varchar2;
  
  function get_note_money (
    p_object_type dnotetext_dbt.t_objecttype%type,
    p_note_kind   dnotetext_dbt.t_notekind%type,
    p_document_id dnotetext_dbt.t_documentid%type
  ) return number;

  function get_note_row (
    p_object_type dnotetext_dbt.t_objecttype%type,
    p_note_kind   dnotetext_dbt.t_notekind%type,
    p_document_id dnotetext_dbt.t_documentid%type
  ) return dnotetext_dbt%rowtype;

end note_read;
/
