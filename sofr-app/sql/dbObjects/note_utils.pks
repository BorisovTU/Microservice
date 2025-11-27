create or replace package note_utils as

  procedure add_new (
    p_object_type dnotetext_dbt.t_objecttype%type,
    p_document_id dnotetext_dbt.t_documentid%type,
    p_note_kind   dnotetext_dbt.t_notekind%type,
    p_value       varchar2,
    p_date        dnotetext_dbt.t_date%type default null
  );


  procedure save_note (
    p_object_type dnotetext_dbt.t_objecttype%type,
    p_note_kind   dnotetext_dbt.t_notekind%type,
    p_document_id dnotetext_dbt.t_documentid%type,
    p_note        varchar2,
    p_date        dnotetext_dbt.t_date%type
  );

  -- Примечание 
 function GetTextID34(p_object_type dnotetext_dbt.t_objecttype%type
                      ,p_id          number
                      ,p_note_kind   dnotetext_dbt.t_notekind%type
                      ,p_date date default null) return varchar deterministic ;

end note_utils;
/
