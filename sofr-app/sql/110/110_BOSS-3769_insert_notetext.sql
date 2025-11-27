declare
  PROCEDURE InsertNoteText_Email(p_PartyID IN NUMBER, p_Email IN VARCHAR2)
  IS
  BEGIN
    INSERT INTO dnotetext_dbt (t_objecttype,
                               t_documentid,
                               t_notekind,
                               t_oper,
                               t_date,
                               t_time,
                               t_text,
                               t_validtodate,
                               t_branch,
                               t_numsession)
      VALUES (3,
              LPAD(p_PartyID, 10, 0),
              109, 
              1,
              to_date('01012021','ddmmyyyy'), --Вставим примечание прошлой датой, иначе не сможем выпускать отчет в архив
              to_date('01010001'||to_char(sysdate,'hh24miss'),'DDMMYYYYhh24miss'),
              rpad(utl_raw.cast_to_raw(c => p_Email), 3000, 0),
              to_date('31129999','ddmmyyyy'),
              1,
              0);                
  END;
begin
  InsertNoteText_Email(132303, 'TitovaTN@rshb.ru');
  InsertNoteText_Email(150175, 'SuharevaGV@rshb.ru');
  InsertNoteText_Email(149477, 'FedorovaEN@rshb.ru');
  InsertNoteText_Email(319208, 'ShopovaIV@rshb.ru');
  InsertNoteText_Email(152119, 'KreysikEA@rshb.ru');
end;
/


