create or replace package body note_utils as
  c_notTextChar varchar2(9) :=chr(0)||chr(1)||chr(2)||chr(3)||chr(4)||chr(5)||chr(6)||chr(7)||chr(8) ;
  --Just new notes, without any check, pls.
  --if you need to check smth, write new function/procedure with checks which finally calls this procedure
  procedure add_new (
    p_object_type dnotetext_dbt.t_objecttype%type,
    p_document_id dnotetext_dbt.t_documentid%type,
    p_note_kind   dnotetext_dbt.t_notekind%type,
    p_value       varchar2,
    p_date        dnotetext_dbt.t_date%type default null
  ) is
    l_date dnotetext_dbt.t_date%type;
  begin
    l_date := case
                when p_date is not null
                  then p_date
                when RsbSessionData.Curdate != to_date('01.01.0001', 'dd.mm.yyyy')
                  then RsbSessionData.Curdate
                else trunc(sysdate)
              end;
    insert into dnotetext_dbt (t_objecttype,
                               t_documentid,
                               t_notekind,
                               t_oper,
                               t_date,
                               t_time,
                               t_text,
                               t_validtodate,
                               t_branch,
                               t_numsession)
    VALUES   (p_object_type,
              p_document_id,
              p_note_kind,
              nvl(RsbSessionData.Oper, 9997),
              l_date,
              to_date('01010001'||to_char(sysdate,'hhmiss'),'DDMMYYYYhhmiss'),
              note_read.cast_to_raw(p_value => p_value),
              to_date('31129999','ddmmyyyy'),
              1,
              0);
  end add_new;
  
  procedure update_note_by_id (
    p_id          dnotetext_dbt.t_id%type,
    p_value       varchar2,
    p_validtodate dnotetext_dbt.t_validtodate%type
  ) is
  begin
    update dnotetext_dbt n
       set n.t_text = note_read.cast_to_raw(p_value => p_value),
           n.t_validtodate = p_validtodate
     where n.t_id = p_id;
  end update_note_by_id;
  
  procedure close_note_by_id (
    p_id    dnotetext_dbt.t_id%type,
    p_date  dnotetext_dbt.t_validtodate%type
  ) is
  begin
    update dnotetext_dbt n
       set n.t_validtodate = p_date
     where n.t_id = p_id;
  end close_note_by_id;
  
  function set_lock (
    p_object_type dnotetext_dbt.t_objecttype%type,
    p_note_kind   dnotetext_dbt.t_notekind%type,
    p_document_id dnotetext_dbt.t_documentid%type
  ) return boolean is
  begin
    return lock_utils.set_lock(p_lockname          => 'save_note_obj' || to_char(p_object_type) || '_kind' || to_char(p_note_kind) || '_doc' || p_document_id,
                               p_release_on_commit => true,
                               p_exclusive         => true,
                               p_timeout           => 1); --1 sec
  end set_lock;
    

  /*
    сохранение примечания.
    Обработанные случаи:
    - аналогичного примечания на объекте ещё не было
      просто инсерт в таблицу
    - аналогичное примечание на объекте есть и было добавлено в дату p_date
      просто апдейт примечания
    - аналогичное примечание на объекте есть и было добавлен раньше, чем p_date
      необходимо закрыть старое и инсертить новое
    - на объекте существует аналогичное примечание с датой начала позже, чем p_date.
      в данной процедуре эта ситуация не обрабатывается
  */
  procedure save_note (
    p_object_type dnotetext_dbt.t_objecttype%type,
    p_note_kind   dnotetext_dbt.t_notekind%type,
    p_document_id dnotetext_dbt.t_documentid%type,
    p_note        varchar2,
    p_date        dnotetext_dbt.t_date%type
  ) is
    l_note_row dnotetext_dbt%rowtype;
    l_date     dnotetext_dbt.t_date%type := trunc(p_date);
  begin
    if not set_lock(p_object_type => p_object_type,
                    p_note_kind   => p_note_kind,
                    p_document_id => p_document_id)
    then
      it_log.log_error(p_object   => 'note_utils.save_note',
                       p_msg      => 'couldnt set lock. obj=' || to_char(p_object_type) || '; kind=' || to_char(p_note_kind) || '; doc=' || p_document_id);
      return;
    end if;

    l_note_row := note_read.get_note_row(p_object_type => p_object_type,
                                         p_note_kind   => p_note_kind,
                                         p_document_id => p_document_id);

    if l_note_row.t_id is null
    then
      add_new(p_object_type => p_object_type,
              p_note_kind   => p_note_kind,
              p_document_id => p_document_id,
              p_value       => p_note,
              p_date        => l_date);
    elsif l_note_row.t_date = l_date
    then
      update_note_by_id(p_id          => l_note_row.t_id,
                        p_value       => p_note,
                        p_validtodate => to_date('31.12.9999', 'dd.mm.yyyy'));
    elsif l_note_row.t_date < l_date
    then
      close_note_by_id(p_id   => l_note_row.t_id,
                       p_date => l_date - 1);

      add_new(p_object_type => p_object_type,
              p_note_kind   => p_note_kind,
              p_document_id => p_document_id,
              p_value       => p_note,
              p_date        => l_date);
    end if;
  end save_note;

  function GetTextID34(p_object_type dnotetext_dbt.t_objecttype%type
                      ,p_id          number
                      ,p_note_kind   dnotetext_dbt.t_notekind%type
                      ,p_date        date default null) return varchar deterministic as
    v_date date := nvl(p_date, to_date('31129999', 'ddmmyyyy'));
  begin
    return TRANSLATE(RSB_STRUCT.getString(rsi_rsb_kernel.GetNote(ObjType => p_object_type, ObjId => lpad(p_id, 34, '0'), NoteKind => p_note_kind, Dat => v_date))
                    ,'A' || c_notTextChar
                    ,'A');
  end;
 
end note_utils;
/
