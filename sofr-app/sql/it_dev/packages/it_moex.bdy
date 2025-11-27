CREATE OR REPLACE PACKAGE BODY IT_Moex IS                                         

  /*Загрузка из JSON-структуры данных НКД и номинала на конец месяца, приходящийся на выходной день*/
  PROCEDURE SaveNKD( p_Body IN CLOB, p_Date IN DATE, p_ErrorMessage OUT VARCHAR2 )
  IS
    v_FIID INTEGER := 0;
    v_FaceValueFI INTEGER := 0;
    v_Point INTEGER := 0;
    v_Error VARCHAR2(100);
    v_isJSON BOOLEAN := false;
    v_err integer := 0;
  BEGIN
    p_ErrorMessage := NULL;
    FOR JSONRec IN (select js.* FROM JSON_TABLE(p_Body, '$.monthend_accints[*]' 
      COLUMNS
      (TradeDate DATE PATH '$.tradedate',
       SecID VARCHAR2(50) PATH '$.secid',
       SecName VARCHAR2(50) PATH '$.name',
       ShortName VARCHAR2(50) PATH '$.shortname',
       RegNumber VARCHAR2(50) PATH '$.regnumber',
       AccInt NUMBER(32,12) PATH '$.accint'
      )) as js WHERE AccInt IS NOT NULL  )
    LOOP
      v_FIID := 0;
      v_Error := chr(0);
      v_isJSON := true;
      BEGIN
        with objc as (select distinct T_OBJECTID, T_CODE FROM DOBJCODE_DBT 
                                WHERE T_CODEKIND = 11 
                                  AND T_OBJECTTYPE = 9 ) 
        SELECT fin.t_FIID, fin.t_FaceValueFi INTO v_FIID, v_FaceValueFI
          FROM dfininstr_dbt fin, davoiriss_dbt avoir
            WHERE fin.t_fiid = avoir.t_fiid
              AND (avoir.t_indexnom = chr(88) OR avoir.t_floatingrate = chr(88))
              AND (fin.t_FIID IN (Select objc.T_OBJECTID FROM objc WHERE objc.t_Code = JSONRec.SecID) OR avoir.t_LSIN = JSONRec.RegNumber);                                   
             EXCEPTION WHEN NO_DATA_FOUND THEN
              v_FIID := 0;
              v_FaceValueFI  := 0;
      END;

      IF (v_FIID <> 0) THEN
        begin
          select max(t_Point) INTO v_Point from dratedef_dbt where t_type = 15 and t_otherfi = v_FIID;
            EXCEPTION WHEN NO_DATA_FOUND THEN v_Point := 4;
        end;
        v_err := USR_PKG_IMPORT_SOFR.ImportOneCourse_g(v_FaceValueFI, v_FIID, 15, JSONRec.TradeDate, 0, 0, JSONRec.AccInt*1*Power(10,v_Point), 1, v_Point, chr(0), chr(0), chr(0), 1, v_Error);
      END IF;

      IF(v_Error != chr(0)) THEN
        p_ErrorMessage := 'Oracle IT_Moex.SaveNKD Ошибка сохранения НКД. Ошибка обработки курса ц/б '||JSONRec.SecID||' '||v_Error;          
          END IF;
    END LOOP;

    IF (v_isJSON = false) THEN
      p_ErrorMessage := 'Oracle IT_Moex.SaveNKD Ошибка сохранения НКД. Не началась обработка сообщения JSON, проверьте формат входящих данных';  
    END IF;

    IF(p_ErrorMessage IS NOT NULL) THEN
      rsb_payments_api.InsertEmailNotify(50, 'Ошибка при загрузке данных НКД и номинала на конец месяца, приходящийся на выходной день', p_ErrorMessage);
      it_log.log_error(p_object   => 'IT_Moex.SaveNKD',
                       p_msg      => p_ErrorMessage);
    END IF;

  END;
END IT_Moex;