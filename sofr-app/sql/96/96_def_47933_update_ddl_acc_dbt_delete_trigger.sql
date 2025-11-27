CREATE OR REPLACE TRIGGER "DDL_ACC_DBT_DELETE"
  BEFORE DELETE ON ddl_acc_dbt
  FOR EACH ROW
DECLARE
  OBJECT_TYPE          CONSTANT uTableProcessEvent_dbt.T_OBJECTTYPE%TYPE := 5073; -- вид объекта Проводка РОВУ - коды придумываем кто во что горазд
  OBJECT_OPER_TYPE_DEL CONSTANT uTableProcessEvent_dbt.T_TYPE%TYPE := 3; -- тип: Удаление
  OBJECT_STATUS_READY  CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := 1;
  CFT_trnid integer; -- id проводки ЦФТ
BEGIN
  SELECT trunc(to_number(replace(UTL_RAW.CAST_TO_VARCHAR2(nt.t_Text),
                                 CHR(0),
                                 '')))
    into CFT_trnid
    FROM dnotetext_dbt nt
   WHERE nt.t_ObjectType = 159
     AND nt.t_DocumentID = LPAD(:old.t_id, 10, '0')  -- дистрибутивная функция GetNoteTextStr не подходит, так как добавляет до 34 знаков
     AND nt.t_NoteKind = 102
     AND nt.t_Date <= sysdate;

  INSERT INTO uTableProcessEvent_dbt
    (T_TIMESTAMP, T_OBJECTTYPE, T_OBJECTID, T_TYPE, T_STATUS,T_NOTE)
  VALUES
    (SYSDATE,
     OBJECT_TYPE,
     :old.t_id,
     OBJECT_OPER_TYPE_DEL,
     OBJECT_STATUS_READY,
     CFT_trnid);

  --dbms_output.put_line(CFT_trnid);

EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE(sqlerrm);
END;
/