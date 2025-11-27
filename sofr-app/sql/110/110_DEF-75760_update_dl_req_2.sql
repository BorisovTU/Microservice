DECLARE
  v_CurReqID NUMBER(10) := 0;
  v_MethodApplic NUMBER(5);

  FUNCTION SetMethodApplic(p_TraderName IN VARCHAR2) RETURN NUMBER
  IS
    v_TraderName VARCHAR2(64);
  BEGIN
    IF p_TraderName <> chr(1) AND p_TraderName <> ' ' THEN
      v_TraderName := LOWER(p_TraderName) ;

      IF (SubStr(v_TraderName, 1, 1) = 'p' AND REGEXP_LIKE(TRIM(SubStr(v_TraderName, 2)), '^[[:digit:]]+$')) OR INSTR(v_TraderName, 'paper') > 0 THEN
        RETURN 2 /*Бумажный документ*/;
      ELSIF SubStr(v_TraderName, 1, 2) = 'tr' AND REGEXP_LIKE(TRIM(SubStr(v_TraderName, 3)), '^[[:digit:]]+$') THEN
        RETURN 3 /*Голосовое сообщение*/;
      END IF;
    END IF;

    RETURN 1 /*Электронный документ*/;
  END;
BEGIN
  FOR one_rec IN (SELECT req.t_ID as ReqID, req.t_MethodApplic as ReqMethodApplic, tk.t_ID as DealID, tk.t_MethodApplic as DealMethodApplic, note.t_date, note.t_time, note.t_text, trim(chr(0) from RSB_STRUCT.getString(note.t_text)) txt         
                    FROM ddl_req_dbt req, dspgrdoc_dbt reqdoc, dspground_dbt ground, dspgrdoc_dbt dealdoc, ddvndeal_dbt tk, dnotetext_dbt note
                   WHERE req.t_SourceKind IN (199, 4813)
                     AND reqdoc.t_sourcedocid = req.t_ID
                     AND reqdoc.t_sourcedockind = req.t_kind
                     AND ground.t_spgroundid = reqdoc.t_spgroundid
                     AND ground.t_spgroundid = dealdoc.t_spgroundid
                     AND dealdoc.t_sourcedocid = tk.t_ID
                     AND dealdoc.t_sourcedockind = tk.t_DocKind
                     AND note.t_objecttype in (145, 148)
                     AND note.t_notekind = 150
                     AND note.t_documentid = LPAD(tk.t_ID, 34, 0)
                     AND note.t_validtodate = to_date('31129999','ddmmyyyy')
                     AND NOT EXISTS (SELECT 1 
                                       FROM dnotetext_dbt note2                                         
                                      WHERE note2.t_objecttype = 149
                                        AND note2.t_notekind = 102
                                        AND note2.t_documentid = LPAD(req.t_ID, 34, 0)
                                    )
                   ORDER BY req.t_ID
                 )
  LOOP
    v_MethodApplic := SetMethodApplic(one_rec.txt);

    IF v_CurReqID <> one_rec.ReqID THEN
      v_CurReqID := one_rec.ReqID;

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
        VALUES (149,
                LPAD(one_rec.ReqID, 34, 0),
                102, 
                1,
                one_rec.t_date, 
                one_rec.t_time,
                one_rec.t_text,
                to_date('31129999','ddmmyyyy'),
                1,
                0);

      IF v_MethodApplic <> one_rec.ReqMethodApplic THEN
        UPDATE ddl_req_dbt
           SET t_MethodApplic = v_MethodApplic
         WHERE t_ID = one_rec.ReqID; 
      END IF; 
    END IF;

    IF v_MethodApplic <> one_rec.DealMethodApplic THEN
      UPDATE ddvndeal_dbt
         SET t_MethodApplic = v_MethodApplic
       WHERE t_ID = one_rec.DealID; 
    END IF; 
  END LOOP;
END;
/