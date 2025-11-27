--Заполнение категории "Счет Депо в Спецдепозитарии" по текущим ДБО
BEGIN
  INSERT INTO dobjatcor_dbt (t_ObjectType, t_GroupID, t_AttrID, t_Object, t_General, t_ValidFromDate, t_Oper, t_ValidToDate, t_IsAuto)
  (SELECT 207, 105, 1, LPAD(mp.t_DlContrID, 34, '0'), CHR(88), sf_root.t_DateBegin, 1, TO_DATE('31/12/9999', 'DD/MM/YYYY'), CHR(88)
     FROM dnotetext_dbt note, dsfcontr_dbt sf, ddlcontrmp_dbt mp, ddlcontr_dbt dl, dsfcontr_dbt sf_root
    WHERE note.t_ObjectType = 659 
      AND note.t_NoteKind = 5
      AND INSTR(REPLACE(UTL_RAW.cast_to_varchar2(note.t_Text), chr(0), null), 'Y') <> 0
      AND sf.t_ID = TO_NUMBER(note.t_DocumentID) 
      AND sf.t_ServKind = 1
      AND sf.t_ServKindSub = 8
      AND mp.t_SfContrID = sf.t_ID
      AND mp.t_MarketID = 2 /*ММВБ*/
      AND mp.t_DlContrID = dl.t_DlContrID
      AND sf_root.t_ID = dl.t_SfContrID
  );
END;
/