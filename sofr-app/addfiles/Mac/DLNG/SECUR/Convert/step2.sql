CREATE OR REPLACE PACKAGE SC_CONVERT
IS

--  FUNCTION FindObjCodeByCode( p_ObjType NUMBER, p_CodeKind NUMBER, p_Code VARCHAR2 ) RETURN NUMBER;

--  FUNCTION InsertPartyOwn( p_OldPartyID NUMBER, p_NewPartyID NUMBER ) RETURN NUMBER;

--  FUNCTION InsertAdress( p_OldPartyID NUMBER, p_NewPartyID NUMBER )  RETURN NUMBER;
  FUNCTION FindObjCode( p_ObjType NUMBER, p_CodeKind NUMBER, p_ObjectID NUMBER ) RETURN VARCHAR2;

  FUNCTION FindPartyName( p_PartyID NUMBER ) RETURN VARCHAR2;

  FUNCTION MakeDocumentID( DocID NUMBER ) RETURN VARCHAR2;

  PROCEDURE ConvertMain;

END SC_CONVERT;
/

CREATE OR REPLACE PACKAGE BODY SC_CONVERT
IS

  FUNCTION FindObjCode( p_ObjType NUMBER, p_CodeKind NUMBER, p_ObjectID NUMBER ) RETURN VARCHAR2
   IS
    v_ObjCode VARCHAR2(35);
  BEGIN

    SELECT t_Code INTO v_ObjCode
      FROM DOBJCODE_DBT 
     WHERE t_ObjectType = p_ObjType  AND
           t_CodeKind   = p_CodeKind AND
           t_ObjectID   = p_ObjectID;

    RETURN v_ObjCode;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN SCIDMAP.SetError('INFO','FindObjCode','Субьект с ID = '||p_ObjectID||' не найден.'); RETURN '';
    WHEN OTHERS        THEN RETURN '';
  END;


  FUNCTION FindPartyName( p_PartyID NUMBER ) RETURN VARCHAR2
   IS
    v_Name VARCHAR2(320);
  BEGIN

    SELECT t_Name INTO v_Name
      FROM DPARTY_DBT 
     WHERE t_PartyID = p_PartyID;

    RETURN v_Name;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN SCIDMAP.SetError('INFO','FindPartyName','Субьект с ID = '||p_PartyID||' не найден.'); RETURN '';
    WHEN OTHERS        THEN RETURN '';
  END;
  
  FUNCTION FindObjCodeByCode( p_ObjType NUMBER, p_CodeKind NUMBER, p_Code VARCHAR2 ) RETURN NUMBER
   IS
    v_ObjID NUMBER;
  BEGIN

    SELECT t_ObjectID INTO v_ObjID 
      FROM DOBJCODE_DBT 
     WHERE t_ObjectType = p_ObjType  AND
           t_CodeKind   = p_CodeKind AND
           t_Code       = p_Code;

    RETURN v_ObjID;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN -2000;
    WHEN OTHERS        THEN RETURN -2001;
  END;

  ---Вставим что возможно
  FUNCTION InsertPartyOwn( p_OldPartyID NUMBER, p_NewPartyID NUMBER )  RETURN NUMBER
   IS

    CURSOR c_PartyOwn IS ( SELECT * FROM Ivanov2028_86.DPARTYOWN_DBT WHERE t_PartyID = p_OldPartyID );

  BEGIN

    FOR v_PartyOwn IN c_PartyOwn LOOP

       BEGIN
         v_PartyOwn.t_PartyID := p_NewPartyID;

         INSERT INTO DPARTYOWN_DBT VALUES v_PartyOwn;

       EXCEPTION
         WHEN DUP_VAL_ON_INDEX THEN SCIDMAP.SetError('INFO','InsertPartyOwn','Вставка записи DPARTYOWN_DBT для субьекта с ID = '||p_OldPartyID||' ненужна, запись есть.');
         WHEN OTHERS      THEN SCIDMAP.SetError('ERROR','InsertPartyOwn','Ошибка при вставке DPARTYOWN_DBT для субьекта с ID = '||p_OldPartyID);
       END;

    END LOOP;

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertPartyOwn','Ошибка при копировании содержимого DPARTYOWN_DBT для субьекта с ID = '||p_OldPartyID); RETURN 1;
  END;

  ---Вставим что возможно
  FUNCTION InsertAdress( p_OldPartyID NUMBER, p_NewPartyID NUMBER )  RETURN NUMBER
   IS

    CURSOR c_Adress IS ( SELECT * FROM Ivanov2028_86.DADRESS_DBT WHERE t_PartyID = p_OldPartyID );

  BEGIN

    FOR v_Adress IN c_Adress LOOP

       BEGIN
         v_Adress.t_PartyID := p_NewPartyID;

         INSERT INTO DADRESS_DBT ( T_PARTYID, T_TYPE, T_ADRESS, T_COUNTRY, T_POSTINDEX, T_REGION,
                                   T_CODEPROVINCE, T_PROVINCE, T_CODEDISTRICT, T_DISTRICT, T_CODEPLACE, T_PLACE, T_CODESTREET,
                                   T_STREET, T_HOUSE, T_NUMCORPS, T_FLAT, T_PHONENUMBER, T_PHONENUMBER2, T_FAX, T_TELEGRAPH,
                                   T_TELEXNUMBER, T_E_MAIL, T_RS_MAIL_COUNTRY, T_RS_MAIL_REGION, T_RS_MAIL_NODE, T_TERRITORY, T_KLADR,
                                   T_BRANCH, T_NUMSESSION, T_CODEREGION
                                 ) VALUES 
                                 ( v_Adress.T_PARTYID, v_Adress.T_TYPE, v_Adress.T_ADRESS, v_Adress.T_COUNTRY, v_Adress.T_POSTINDEX, v_Adress.T_REGION,
                                   v_Adress.T_CODEPROVINCE, v_Adress.T_PROVINCE, v_Adress.T_CODEDISTRICT, v_Adress.T_DISTRICT, v_Adress.T_CODEPLACE, v_Adress.T_PLACE, v_Adress.T_CODESTREET,
                                   v_Adress.T_STREET, v_Adress.T_HOUSE, v_Adress.T_NUMCORPS, v_Adress.T_FLAT, v_Adress.T_PHONENUMBER, v_Adress.T_PHONENUMBER2, v_Adress.T_FAX, v_Adress.T_TELEGRAPH,
                                   v_Adress.T_TELEXNUMBER, v_Adress.T_E_MAIL, v_Adress.T_RS_MAIL_COUNTRY, v_Adress.T_RS_MAIL_REGION, v_Adress.T_RS_MAIL_NODE, v_Adress.T_TERRITORY, v_Adress.T_KLADR,
                                   v_Adress.T_BRANCH, v_Adress.T_NUMSESSION, v_Adress.T_CODEREGION );

       EXCEPTION
         WHEN DUP_VAL_ON_INDEX THEN SCIDMAP.SetError('INFO','InsertAdress','Вставка записи DADRESS_DBT для субьекта с ID = '||p_OldPartyID||' ненужна, запись есть.');
         WHEN OTHERS      THEN SCIDMAP.SetError('ERROR','InsertAdress','Ошибка при вставке DADRESS_DBT для субьекта с ID = '||p_OldPartyID);
       END;

    END LOOP;

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertAdress','Ошибка при копировании содержимого DADRESS_DBT для субьекта с ID = '||p_OldPartyID); RETURN 1;
  END;

  ---Вставим что возможно
  FUNCTION InsertClient( p_OldPartyID NUMBER, p_NewPartyID NUMBER )  RETURN NUMBER
   IS

    CURSOR c_Client IS ( SELECT * FROM Ivanov2028_86.DCLIENT_DBT WHERE t_PartyID = p_OldPartyID );

  BEGIN

    FOR v_Client IN c_Client LOOP

       BEGIN
         v_Client.t_PartyID := p_NewPartyID;

         INSERT INTO DCLIENT_DBT VALUES v_Client;

       EXCEPTION
         WHEN DUP_VAL_ON_INDEX THEN SCIDMAP.SetError('INFO','InsertClient','Вставка записи DCLIENT_DBT для субьекта с ID = '||p_OldPartyID||' ненужна, запись есть.');
         WHEN OTHERS      THEN SCIDMAP.SetError('ERROR','InsertClient','Ошибка при вставке DCLIENT_DBT для субьекта с ID = '||p_OldPartyID);
       END;

    END LOOP;

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertClient','Ошибка при копировании содержимого DCLIENT_DBT для субьекта с ID = '||p_OldPartyID); RETURN 1;
  END;

  FUNCTION InsertPtSvDp( p_OldPartyID NUMBER, p_NewPartyID NUMBER )  RETURN NUMBER
   IS

    CURSOR c_PtSvDp IS ( SELECT * FROM Ivanov2028_86.DPTSVDP_DBT WHERE t_PartyID = p_OldPartyID );

  BEGIN

    FOR v_PtSvDp IN c_PtSvDp LOOP

       BEGIN
         v_PtSvDp.t_PartyID := p_NewPartyID;

         INSERT INTO DPTSVDP_DBT VALUES v_PtSvDp;

       EXCEPTION
         WHEN DUP_VAL_ON_INDEX THEN SCIDMAP.SetError('INFO','InsertPtSvDp','Вставка записи DPTSVDP_DBT для субьекта с ID = '||p_OldPartyID||' ненужна, запись есть.');
         WHEN OTHERS      THEN SCIDMAP.SetError('ERROR','InsertPtSvDp','Ошибка при вставке DPTSVDP_DBT для субьекта с ID = '||p_OldPartyID);
       END;

    END LOOP;

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertPtSvDp','Ошибка при копировании содержимого DPTSVDP_DBT для субьекта с ID = '||p_OldPartyID); RETURN 1;
  END;

  FUNCTION InsertInstitut( p_OldPartyID NUMBER, p_NewPartyID NUMBER )  RETURN NUMBER
   IS

    CURSOR c_Institut IS ( SELECT * FROM Ivanov2028_86.DINSTITUT_DBT WHERE t_PartyID = p_OldPartyID );

  BEGIN

    FOR v_Institut IN c_Institut LOOP

       BEGIN
         v_Institut.t_PartyID := p_NewPartyID;

         INSERT INTO DINSTITUT_DBT VALUES v_Institut;

       EXCEPTION
         WHEN DUP_VAL_ON_INDEX THEN SCIDMAP.SetError('INFO','InsertInstitut','Вставка записи DINSTITUT_DBT для субьекта с ID = '||p_OldPartyID||' ненужна, запись есть.');
         WHEN OTHERS      THEN SCIDMAP.SetError('ERROR','InsertInstitut','Ошибка при вставке DINSTITUT_DBT для субьекта с ID = '||p_OldPartyID);
       END;

    END LOOP;

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertInstitut','Ошибка при копировании содержимого DINSTITUT_DBT для субьекта с ID = '||p_OldPartyID); RETURN 1;
  END;

  FUNCTION InsertObjCode( p_OldPartyID NUMBER, p_NewPartyID NUMBER, p_ObjectType NUMBER )  RETURN NUMBER
   IS

    CURSOR c_ObjCode IS ( SELECT * FROM Ivanov2028_86.DOBJCODE_DBT WHERE t_ObjectID = p_OldPartyID AND t_ObjectType = p_ObjectType );

    v_AutoKey NUMBER;

  BEGIN

    SELECT MAX(t_AutoKey) INTO v_AutoKey FROM DOBJCODE_DBT;

    FOR v_ObjCode IN c_ObjCode LOOP

--       v_AutoKey := v_AutoKey + 1;

       v_ObjCode.t_ObjectID := p_NewPartyID;
--       v_ObjCode.t_AutoKey  := v_AutoKey;
       v_ObjCode.t_AutoKey  := 0;
       
       BEGIN

         INSERT INTO DOBJCODE_DBT VALUES v_ObjCode;

       EXCEPTION
         WHEN DUP_VAL_ON_INDEX THEN SCIDMAP.SetError('INFO','InsertObjCode','Вставка записи DOBJCODE_DBT для субьекта с ID = '||p_OldPartyID||' ненужна, запись есть.');
         WHEN OTHERS      THEN SCIDMAP.SetError('ERROR','InsertObjCode','Ошибка при вставке DOBJCODE_DBT для субьекта с ID = '||p_OldPartyID);
       END;
 
       IF( v_ObjCode.t_CodeKind = 1 ) THEN
--          v_AutoKey := v_AutoKey + 1;
--          v_ObjCode.t_AutoKey  := v_AutoKey;
          v_ObjCode.t_AutoKey  := 0;
          v_ObjCode.t_CodeKind := 400;

          BEGIN

            INSERT INTO DOBJCODE_DBT VALUES v_ObjCode;

          EXCEPTION
            WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertObjCode','Ошибка при вставке кода ИБТ DOBJCODE_DBT для субьекта с ID = '||p_OldPartyID);
          END;

       END IF;

    END LOOP;

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertObjCode','Ошибка при копировании содержимого DOBJCODE_DBT для субьекта с ID = '||p_OldPartyID); RETURN 1;
  END;

  FUNCTION InsertObjRGDoc( p_OldPartyID NUMBER )  RETURN NUMBER
   IS

    CURSOR c_ObjRGDoc IS ( SELECT * FROM Ivanov2028_86.DOBJRGDOC_DBT WHERE t_ObjectID = p_OldPartyID AND t_ObjectType = 3 );

  BEGIN

    FOR v_ObjRGDoc IN c_ObjRGDoc LOOP

       BEGIN
         v_ObjRGDoc.t_ObjectID := SCIDMAP.GetObjMappedID( 1, v_ObjRGDoc.t_ObjectID, 1 );
         v_ObjRGDoc.t_RegPartyID := SCIDMAP.GetObjMappedID( 1, v_ObjRGDoc.t_RegPartyID, 1 );
         v_ObjRGDoc.t_DocumentID := 0;

         INSERT INTO DOBJRGDOC_DBT VALUES v_ObjRGDoc;

       EXCEPTION
         WHEN DUP_VAL_ON_INDEX THEN SCIDMAP.SetError('INFO','InsertObjRGDoc','Вставка записи DOBJRGDOC_DBT для субьекта с ID = '||p_OldPartyID||' ненужна, запись есть.');
         WHEN OTHERS      THEN SCIDMAP.SetError('ERROR','InsertObjRGDoc','Ошибка при вставке DOBJRGDOC_DBT для субьекта с ID = '||p_OldPartyID);
       END;

    END LOOP;

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertObjRGDoc','Ошибка при копировании содержимого DOBJRGDOC_DBT для субьекта с ID = '||p_OldPartyID); RETURN 1;
  END;


  FUNCTION InsertSettAcc( p_OldPartyID NUMBER )  RETURN NUMBER
   IS

    CURSOR c_SettAcc IS ( SELECT * FROM Ivanov2028_86.DSETTACC_DBT WHERE t_PartyID = p_OldPartyID );

    v_NewID NUMBER;
    v_OldID NUMBER;
    v_stat  NUMBER;

  BEGIN

    FOR v_SettAcc IN c_SettAcc LOOP

       BEGIN
         v_SettAcc.t_PartyID := SCIDMAP.GetObjMappedID( 1, v_SettAcc.t_PartyID, 1 );
         v_SettAcc.t_BankID := SCIDMAP.GetObjMappedID( 1, v_SettAcc.t_BankID, 1 );
         v_SettAcc.t_BankCorrID := SCIDMAP.GetObjMappedID( 1, v_SettAcc.t_BankCorrID, 1 );
         v_SettAcc.t_BeneficiaryID := SCIDMAP.GetObjMappedID( 1, v_SettAcc.t_BeneficiaryID, 1 );
         v_OldID := v_SettAcc.t_SettAccID;
         v_SettAcc.t_SettAccID := 0;

         INSERT INTO DSETTACC_DBT VALUES v_SettAcc RETURNING t_SettAccID INTO v_NewID;

       EXCEPTION
         WHEN DUP_VAL_ON_INDEX THEN SCIDMAP.SetError('INFO','InsertSettAcc','Вставка записи DSETTACC_DBT для субьекта с ID = '||p_OldPartyID||' ненужна, запись есть.');
         WHEN OTHERS      THEN SCIDMAP.SetError('ERROR','InsertSettAcc','Ошибка при вставке DSETTACC_DBT для субьекта с ID = '||p_OldPartyID);
       END;

       v_stat := SCIDMAP.PutObjMappedID( 3, v_OldID, v_NewID, CHR(88) );

    END LOOP;

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertSettAcc','Ошибка при копировании содержимого DSETTACC_DBT для субьекта с ID = '||p_OldPartyID); RETURN 1;
  END;

  FUNCTION InsertPMAutoAc( p_OldPartyID NUMBER )  RETURN NUMBER
   IS

    CURSOR c_PMAutoAC IS ( SELECT * FROM Ivanov2028_86.DPMAUTOAC_DBT WHERE t_PartyID = p_OldPartyID );

  BEGIN

    FOR v_PMAutoAC IN c_PMAutoAC LOOP

       BEGIN
         v_PMAutoAC.t_PartyID := SCIDMAP.GetObjMappedID( 1, v_PMAutoAC.t_PartyID, 1 );

         INSERT INTO DPMAUTOAC_DBT VALUES v_PMAutoAC;

       EXCEPTION
         WHEN DUP_VAL_ON_INDEX THEN SCIDMAP.SetError('INFO','InsertPMAutoAc','Вставка записи DPMAUTOAC_DBT для субьекта с ID = '||p_OldPartyID||' ненужна, запись есть.');
         WHEN OTHERS           THEN SCIDMAP.SetError('ERROR','InsertPMAutoAc','Ошибка при вставке DPMAUTOAC_DBT для субьекта с ID = '||p_OldPartyID);
       END;

    END LOOP;

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertPMAutoAc','Ошибка при копировании содержимого DPMAUTOAC_DBT для субьекта с ID = '||p_OldPartyID); RETURN 1;
  END;

  FUNCTION InsertNoteText( p_OldID VARCHAR2, p_NewID VARCHAR2, p_ObjectType NUMBER )  RETURN NUMBER
   IS

    CURSOR c_NoteText IS ( SELECT * FROM Ivanov2028_86.DNOTETEXT_DBT WHERE t_DocumentID = p_OldID AND t_ObjectType = p_ObjectType );

  BEGIN

    FOR v_NoteText IN c_NoteText LOOP

       BEGIN
         v_NoteText.t_DocumentID := p_NewID;
         v_NoteText.t_ID := 0;

         INSERT INTO DNOTETEXT_DBT ( T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT,
                                     T_VALIDTODATE, T_BRANCH, T_NUMSESSION ) 
                                   VALUES 
                                   ( v_NoteText.T_OBJECTTYPE, v_NoteText.T_DOCUMENTID, v_NoteText.T_NOTEKIND, v_NoteText.T_OPER, v_NoteText.T_DATE, v_NoteText.T_TIME, v_NoteText.T_TEXT,
                                     v_NoteText.T_VALIDTODATE, v_NoteText.T_BRANCH, v_NoteText.T_NUMSESSION );


       EXCEPTION
         WHEN DUP_VAL_ON_INDEX THEN SCIDMAP.SetError('INFO','InsertNoteText','Вставка записи DNOTETEXT_DBT для субьекта с ID = '||p_OldID||' ненужна, запись есть.');
         WHEN OTHERS           THEN SCIDMAP.SetError('ERROR','InsertNoteText','Ошибка при вставке DNOTETEXT_DBT для субьекта с ID = '||p_OldID);
       END;

    END LOOP;

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertNoteText','Ошибка при копировании содержимого DNOTETEXT_DBT для субьекта с ID = '||p_OldID); RETURN 1;
  END;

  FUNCTION InsertFiWarnts( p_OldFIID NUMBER, p_NewFIID NUMBER ) RETURN NUMBER
   IS

    CURSOR c_Warnts IS ( SELECT * FROM Ivanov2028_86.DFIWARNTS_DBT WHERE t_FIID = p_OldFIID );

  BEGIN

    FOR v_Warnts IN c_Warnts LOOP

       BEGIN
         v_Warnts.t_FIID := p_NewFIID;

         INSERT INTO DFIWARNTS_DBT ( T_FIID, T_NUMBER, T_NAME, T_DEFINITION, T_DRAWINGDATE, T_RELATIVEINCOME,
                                     T_INCOMERATE, T_INCOMEVOLUME, T_ISCLOSED, T_FIRSTDATE, T_ISPARTIAL, T_INCOMESCALE, T_INCOMEPOINT,
                                     T_RESERVE
                                   ) VALUES 
                                   ( v_Warnts.T_FIID, v_Warnts.T_NUMBER, v_Warnts.T_NAME, v_Warnts.T_DEFINITION, v_Warnts.T_DRAWINGDATE, v_Warnts.T_RELATIVEINCOME,
                                     v_Warnts.T_INCOMERATE, v_Warnts.T_INCOMEVOLUME, v_Warnts.T_ISCLOSED, v_Warnts.T_FIRSTDATE, v_Warnts.T_ISPARTIAL, v_Warnts.T_INCOMESCALE, v_Warnts.T_INCOMEPOINT,
                                     v_Warnts.T_RESERVE );

       EXCEPTION
         WHEN DUP_VAL_ON_INDEX THEN SCIDMAP.SetError('INFO','InsertFiWarnts','Вставка записи DFIWARNTS_DBT для ценной бумаги FIID = '||p_OldFIID||' ненужна, запись есть.');
         WHEN OTHERS      THEN SCIDMAP.SetError('ERROR','InsertFiWarnts','Ошибка при вставке DFIWARNTS_DBT для ценной бумаги FIID = '||p_OldFIID);
       END;

    END LOOP;

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertFiWarnts','Ошибка при копировании содержимого DFIWARNTS_DBT для субьекта с ID = '||p_OldFIID); RETURN 1;
  END;

  FUNCTION InsertObjAtCor( p_OldID VARCHAR2, p_NewID VARCHAR2, p_ObjectType NUMBER )  RETURN NUMBER
   IS

    CURSOR c_ObjAtCor IS ( SELECT * FROM Ivanov2028_86.DOBJATCOR_DBT WHERE t_Object = p_OldID AND t_ObjectType = p_ObjectType );

  BEGIN

    FOR v_ObjAtCor IN c_ObjAtCor LOOP

       BEGIN
         v_ObjAtCor.t_Object := p_NewID;
         SCIDMAP.SetError('INFO','InsertObjAtCor','Вставка записи DOBJATCOR_DBT для субьекта с ID = '||p_NewID);

         INSERT INTO DOBJATCOR_DBT VALUES v_ObjAtCor;

       EXCEPTION
         WHEN DUP_VAL_ON_INDEX THEN SCIDMAP.SetError('INFO','InsertObjAtCor','Вставка записи DOBJATCOR_DBT для субьекта с ID = '||p_OldID||' ненужна, запись есть.');
         WHEN OTHERS           THEN SCIDMAP.SetError('ERROR','InsertObjAtCor','Ошибка при вставке DOBJATCOR_DBT для субьекта с ID = '||p_OldID);
       END;

    END LOOP;

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertObjAtCor','Ошибка при копировании содержимого DOBJATCOR_DBT для субьекта с ID = '||p_OldID); RETURN 1;
  END;


  FUNCTION InsertRate( p_OldFIID NUMBER, p_NewFIID NUMBER )  RETURN NUMBER
   IS

    CURSOR c_RateDef IS ( SELECT * FROM Ivanov2028_86.DRATEDEF_DBT WHERE t_OtherFI = p_OldFIID );

    v_NewRateID NUMBER;
    v_OldRateID NUMBER;

  BEGIN

    BEGIN
      DELETE FROM DRATEHIST_DBT WHERE t_RateID in ( SELECT t_RateID FROM DRATEDEF_DBT WHERE t_OtherFI = p_NewFIID );
      DELETE FROM DRATEDEF_DBT WHERE t_OtherFI = p_NewFIID;
    EXCEPTION
      WHEN OTHERS THEN SCIDMAP.SetError('WARNING','InsertRate','Немогу удалить записи DRATEDEF_DBT и DRATEHIST_DBT для FIID = '||p_NewFIID);
    END;

    FOR v_RateDef IN c_RateDef LOOP

       BEGIN
         v_OldRateID := v_RateDef.t_RateID;
         v_RateDef.t_RateID := 0;
         v_RateDef.t_Informator := SCIDMAP.GetObjMappedID( 1, v_RateDef.t_Informator, 0 );
         v_RateDef.t_Market_Place := SCIDMAP.GetObjMappedID( 1, v_RateDef.t_Market_Place, 0 );

         INSERT INTO DRATEDEF_DBT VALUES v_RateDef RETURNING t_RateID INTO v_NewRateID;

         BEGIN
           INSERT INTO DRATEHIST_DBT (SELECT v_NewRateID, t_IsInverse, t_Rate, t_Scale, t_Point, t_InputDate, t_InputTime, t_Oper, t_SinceDate 
                                        FROM Ivanov2028_86.DRATEHIST_DBT 
                                       WHERE t_RateID = v_OldRateID);
         EXCEPTION
           WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertRate','Ошибка при вставке DRATEHIST_DBT для FIID = '||p_OldFIID);
         END;

       EXCEPTION
         WHEN DUP_VAL_ON_INDEX THEN SCIDMAP.SetError('INFO','InsertRate','Вставка записи DRATEDEF_DBT для FIID = '||p_OldFIID||' ненужна, запись есть.');
         WHEN OTHERS           THEN SCIDMAP.SetError('ERROR','InsertRate','Ошибка при вставке DRATEDEF_DBT для FIID = '||p_OldFIID);
       END;

    END LOOP;

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SCIDMAP.SetError('ERROR','InsertRate','Ошибка при копировании содержимого DRATEDEF_DBT для FIID = '||p_OldFIID); RETURN 1;
  END;


  FUNCTION MakeDocumentID( DocID NUMBER ) RETURN VARCHAR2
   IS
  BEGIN

    RETURN LPAD(DocID, 34, '0');

  END;

  
  PROCEDURE CopyOperation( p_DocKind NUMBER, p_Old_DocID NUMBER, p_New_DocID NUMBER )
   IS
    v_OprOper          DOPROPER_DBT%ROWTYPE;
    v_stat             NUMBER;
    v_Old_ID_Operation NUMBER;
    v_New_ID_Operation NUMBER;
  BEGIN
    v_stat := 0;

    BEGIN
      SELECT * INTO v_OprOper FROM Ivanov2028_86.DOPROPER_DBT WHERE t_DocKind = p_DocKind AND t_DocumentID = MakeDocumentID( p_Old_DocID );
    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN SCIDMAP.SetError('ERROR','CopyOperation','Не найдена операция для сделки DocKind = '||p_DocKind||' DocID = '||p_Old_DocID); v_stat := 1;
      WHEN OTHERS        THEN SCIDMAP.SetError('ERROR','CopyOperation','Ошибка в структуре таблицы DOPROPER_DBT'); v_stat := 1;
    END;

    BEGIN
      v_Old_ID_Operation       := v_OprOper.t_ID_Operation;
      v_OprOper.t_ID_Operation := 0;
      v_OprOper.t_DocumentID   := MakeDocumentID( p_New_DocID );
      
      INSERT INTO DOPROPER_DBT VALUES v_OprOper RETURNING t_ID_Operation INTO v_New_ID_Operation;

      v_stat := SCIDMAP.PutObjMappedID( 8, v_Old_ID_Operation, v_New_ID_Operation, CHR(88) );

    EXCEPTION
      WHEN OTHERS THEN SCIDMAP.SetError('ERROR','CopyOperation','Ошибка при вставке DOPROPER_DBT для DocumentID = '||v_OprOper.t_DocumentID);
    END;

    IF( v_stat = 0) THEN
       BEGIN

         INSERT INTO DOPRSTEP_DBT (T_ID_OPERATION, T_ID_STEP, T_KIND_OPERATION, T_NUMBER_STEP, T_PLAN_DATE,
                                   T_FACT_DATE, T_SYST_DATE, T_SYST_TIME, T_OPER, T_ISEXECUTE, T_KIND_ACTION, T_SYMBOL,
                                   T_PREVIOUS_STEP, T_DOCKIND, T_COUNTNUM, T_ISREMOVESTEP, T_NUMREMOVEDSTEP, T_OPERORGROUP,
                                   T_SERVDOCKIND, T_SERVDOCID, T_ISREMOVECOM, T_ISREADYFORAUTOEXEC, T_SERVDOCORDER, T_BLOCKID,
                                   T_LASTSTEP, T_REMOVEBLOCKID, T_LASTBRANCHSTEP, T_HASCHILDDOC)
                                  ( SELECT v_New_ID_Operation, T_ID_STEP, T_KIND_OPERATION, T_NUMBER_STEP, T_PLAN_DATE,
                                           T_FACT_DATE, T_SYST_DATE, T_SYST_TIME, T_OPER, T_ISEXECUTE, T_KIND_ACTION, T_SYMBOL,
                                           T_PREVIOUS_STEP, T_DOCKIND, T_COUNTNUM, T_ISREMOVESTEP, T_NUMREMOVEDSTEP, T_OPERORGROUP,
                                           T_SERVDOCKIND, T_SERVDOCID, T_ISREMOVECOM, T_ISREADYFORAUTOEXEC, T_SERVDOCORDER, T_BLOCKID,
                                           T_LASTSTEP, T_REMOVEBLOCKID, T_LASTBRANCHSTEP, T_HASCHILDDOC
                                      FROM Ivanov2028_86.DOPRSTEP_DBT 
                                     WHERE T_ID_OPERATION = v_Old_ID_Operation );    

       EXCEPTION
         WHEN OTHERS THEN SCIDMAP.SetError('ERROR','CopyOperation','Ошибка при вставке DOPRSTEP_DBT для DocumentID = '||v_OprOper.t_DocumentID);
       END;
    END IF;

    IF( v_stat = 0) THEN
       BEGIN

         INSERT INTO DOPRCURST_DBT ( SELECT v_New_ID_Operation, T_STATUSKINDID, T_NUMVALUE, T_DOCKIND
                                       FROM Ivanov2028_86.DOPRCURST_DBT
                                      WHERE T_ID_OPERATION = v_Old_ID_Operation );    

       EXCEPTION
         WHEN OTHERS THEN SCIDMAP.SetError('ERROR','CopyOperation','Ошибка при вставке DOPRCURST_DBT для DocumentID = '||v_OprOper.t_DocumentID);
       END;
    END IF;

    IF( v_stat = 0) THEN
       BEGIN

         INSERT INTO DOPRDATES_DBT ( T_ID_OPERATION, T_DATE, T_DATEKINDID )
                                   ( SELECT v_New_ID_Operation, T_DATE, T_DATEKINDID
                                       FROM Ivanov2028_86.DOPRDATES_DBT
                                      WHERE T_ID_OPERATION = v_Old_ID_Operation );    

       EXCEPTION
         WHEN OTHERS THEN SCIDMAP.SetError('ERROR','CopyOperation','Ошибка при вставке DOPRCURST_DBT для DocumentID = '||v_OprOper.t_DocumentID);
       END;
    END IF;

  END;


  PROCEDURE CopyParty
   IS

    CURSOR c_PartyCode IS ( SELECT * FROM Ivanov2028_86.DOBJCODE_DBT WHERE t_ObjectType = 3 AND t_CodeKind = 1 );

    v_NewPartyID NUMBER;
    v_stat       NUMBER;
    v_Party      DPARTY_DBT%ROWTYPE;
    v_Persn      DPERSN_DBT%ROWTYPE;
    v_Bank       DBANKDPRT_DBT%ROWTYPE;
    v_Swift      DPTBICDIR_DBT%ROWTYPE;
    v_Count      NUMBER;

  BEGIN

    SELECT MAX(t_PartyID) INTO v_Count FROM DPARTY_DBT;

    -- перенос субьектов

    SCIDMAP.SetError('INFO','CopyParty','Запуск копирования Субьектов');

    FOR v_PartyCode IN c_PartyCode LOOP
       v_stat := 0;

       SCIDMAP.SetError('INFO','CopyParty','Копирования субьекта '||v_PartyCode.t_Code);

       --1.1
       v_NewPartyID := FindObjCodeByCode( v_PartyCode.t_ObjectType, v_PartyCode.t_CodeKind, v_PartyCode.t_Code );

       --1.2
       IF( v_NewPartyID > 0 ) THEN
       --1.2.1
       --1.2.2
         v_stat := SCIDMAP.PutObjMappedID( 1, v_PartyCode.t_ObjectID, v_NewPartyID, CHR(0) );
       --1.2.3
         IF( v_stat = 0 ) THEN
            v_stat := InsertPartyOwn( v_PartyCode.t_ObjectID, v_NewPartyID );
         END IF;
         IF( v_stat = 0 ) THEN
            v_stat := InsertAdress( v_PartyCode.t_ObjectID, v_NewPartyID );
         END IF;
         IF( v_stat = 0 ) THEN
            v_stat := InsertClient( v_PartyCode.t_ObjectID, v_NewPartyID );
         END IF;
         IF( v_stat = 0 ) THEN
            v_stat := InsertPtSvDp( v_PartyCode.t_ObjectID, v_NewPartyID );
         END IF;
         IF( v_stat = 0 ) THEN
            v_stat := InsertInstitut( v_PartyCode.t_ObjectID, v_NewPartyID );
         END IF;
         IF( v_stat = 0 ) THEN
            v_stat := InsertNoteText( LPAD(v_PartyCode.t_ObjectID, 10, '0'), LPAD(v_NewPartyID, 10, '0'), 3 );
         END IF;
         IF( v_stat = 0 ) THEN
            v_stat := InsertObjAtCor( LPAD(v_PartyCode.t_ObjectID, 10, '0'), LPAD(v_NewPartyID, 10, '0'), 3 );
         END IF;


       ELSIF( v_NewPartyID = -2000) THEN

         BEGIN

           SELECT * INTO v_Party FROM Ivanov2028_86.DPARTY_DBT WHERE t_PartyID = v_PartyCode.t_ObjectID;

           v_Count := v_Count + 1;
           v_Party.t_PartyID := v_Count;

           INSERT INTO DPARTY_DBT VALUES v_Party RETURNING t_PartyID INTO v_NewPartyID;
         EXCEPTION
           WHEN OTHERS THEN NULL;
         END;

         v_stat := SCIDMAP.PutObjMappedID( 1, v_PartyCode.t_ObjectID, v_NewPartyID, CHR(88) );

         BEGIN

           SELECT * INTO v_Persn FROM Ivanov2028_86.DPERSN_DBT WHERE t_PersonID  = v_PartyCode.t_ObjectID;

           v_Persn.t_PersonID := v_NewPartyID;

           INSERT INTO DPERSN_DBT VALUES v_Persn;
         EXCEPTION
           WHEN OTHERS THEN NULL;
         END;

         BEGIN

           SELECT * INTO v_Bank FROM Ivanov2028_86.DBANKDPRT_DBT WHERE t_PartyID  = v_PartyCode.t_ObjectID;

           v_Bank.t_PartyID := v_NewPartyID;

           INSERT INTO DBANKDPRT_DBT VALUES v_Bank;
         EXCEPTION
           WHEN OTHERS THEN NULL;
         END;

         BEGIN

           SELECT * INTO v_Swift FROM Ivanov2028_86.DPTBICDIR_DBT WHERE t_PartyID  = v_PartyCode.t_ObjectID;

           v_Swift.t_PartyID := v_NewPartyID;

           INSERT INTO DPTBICDIR_DBT VALUES v_Swift;
         EXCEPTION
           WHEN OTHERS THEN NULL;
         END;
         
         IF( v_stat = 0 ) THEN
            v_stat := InsertObjCode( v_PartyCode.t_ObjectID, v_NewPartyID, 3 );
         END IF;
         IF( v_stat = 0 ) THEN
            v_stat := InsertPartyOwn( v_PartyCode.t_ObjectID, v_NewPartyID );
         END IF;
         IF( v_stat = 0 ) THEN
            v_stat := InsertAdress( v_PartyCode.t_ObjectID, v_NewPartyID );
         END IF;
         IF( v_stat = 0 ) THEN
            v_stat := InsertClient( v_PartyCode.t_ObjectID, v_NewPartyID );
         END IF;
         IF( v_stat = 0 ) THEN
            v_stat := InsertPtSvDp( v_PartyCode.t_ObjectID, v_NewPartyID );
         END IF;
         IF( v_stat = 0 ) THEN
            v_stat := InsertInstitut( v_PartyCode.t_ObjectID, v_NewPartyID );
         END IF;
         IF( v_stat = 0 ) THEN
            v_stat := InsertNoteText( LPAD(v_PartyCode.t_ObjectID, 10, '0'), LPAD(v_NewPartyID, 10, '0'), 3 );
         END IF;

       END IF;

    END LOOP;

  END;

  PROCEDURE UpdateParty
   IS
    CURSOR c_SCIDMap IS ( SELECT scmap.* 
                            FROM DSCIDMAP_TMP scmap, DPARTY_DBT party
                           WHERE scmap.t_Table = 1 AND 
                                 scmap.t_IsMove = chr(88) AND 
                                 party.t_PartyID = scmap.t_NewID AND
                                 party.t_MainPartyID not in (-1, 0) );

    v_NewPartyID NUMBER;
    v_stat       NUMBER;

  
  BEGIN

    FOR v_SCIDMap IN c_SCIDMap LOOP

       BEGIN

         UPDATE DPARTY_DBT
            SET t_MainPartyID = SCIDMAP.GetObjMappedID( 1, t_MainPartyID, 1 )
          WHERE t_PartyID = v_SCIDMap.t_NewID;

       EXCEPTION
         WHEN OTHERS THEN NULL;
       END;

       v_stat := InsertObjRGDoc( v_SCIDMap.t_OldID );
       IF( v_stat = 0 ) THEN
          v_stat := InsertSettAcc( v_SCIDMap.t_OldID );
       END IF;
       IF( v_stat = 0 ) THEN
          v_stat := InsertPMAutoAc( v_SCIDMap.t_OldID );
       END IF;

    END LOOP;

  END;


  PROCEDURE CopyContr
   IS
    CURSOR c_Contract IS ( SELECT * FROM Ivanov2028_86.DSFCONTR_DBT WHERE t_ServKind = 1 ); --PTSK_STOCKDL
    v_stat         NUMBER;
    v_FindContract NUMBER;
    v_NewID        NUMBER;
    v_OldID        NUMBER;

  BEGIN

    SCIDMAP.SetError('INFO','----------','Запуск копирования Договоров обслуживания');

    FOR v_Contract IN c_Contract LOOP
       v_stat := 0;

       SCIDMAP.SetError('INFO','CopyContr',' Копирования договора обслуживания '||v_Contract.t_Number);

       BEGIN
         SELECT t_ID INTO v_FindContract FROM DSFCONTR_DBT WHERE t_Number = v_Contract.t_Number AND t_AccCode = v_Contract.t_AccCode;
       EXCEPTION
         WHEN OTHERS THEN v_FindContract := 0;
       END;

       IF( v_FindContract > 0 ) THEN
          v_stat := SCIDMAP.PutObjMappedID( 4, v_Contract.t_ID, v_FindContract, CHR(0) );
       ELSE

          BEGIN
            v_OldID := v_Contract.t_ID;
            v_Contract.t_ID := 0;
            v_Contract.t_PartyID := SCIDMAP.GetObjMappedID( 1, v_Contract.t_PartyID, 1 );
            v_Contract.t_ContractorID := SCIDMAP.GetObjMappedID( 1, v_Contract.t_ContractorID, 1 );
--            v_Contract.t_Number := CONCAT('IBT',v_Contract.t_Number);
--Я не совсем поняла, зачем вообще добавляют префикс в номер договора? 
--Клиентских договоров в НБТ нет, а если и будут, то только по тем клиентам, которые будут в НБТ. При этом в любом случае бэк-офис вводить реальный номер договора и ест-но, он уникальный. Так что не вижу смысла его дополнять буквами IBT...

            INSERT INTO DSFCONTR_DBT VALUES v_Contract RETURNING t_ID INTO v_NewID;

          EXCEPTION
            WHEN DUP_VAL_ON_INDEX THEN v_stat := 1; SCIDMAP.SetError('INFO','CopyContr','Вставка записи DSFCONTR_DBT для договора '||v_Contract.t_Number||' ненужна, запись есть.');
            WHEN OTHERS           THEN SCIDMAP.SetError('ERROR','CopyContr','Ошибка при вставке записи DSFCONTR_DBT для договора '||v_Contract.t_Number);
          END;

          IF( v_stat = 0 ) THEN          
             v_stat := SCIDMAP.PutObjMappedID( 4, v_OldID, v_NewID, CHR(88) );
          END IF;

          IF( v_stat = 0 ) THEN
             v_stat := InsertNoteText( LPAD(v_OldID, 10, '0'), LPAD(v_NewID, 10, '0'), 659 );
          END IF;

       END IF;

    END LOOP;

  END;

  PROCEDURE CopyAvoiriss 
   IS
    CURSOR c_Avoiriss IS ( SELECT * FROM Ivanov2028_86.DAVOIRISS_DBT );

    v_stat         NUMBER;
    v_NewFIID      NUMBER;
    v_OldFIID      NUMBER;
    v_FinInstr     DFININSTR_DBT%ROWTYPE;
    v_FindFIID     NUMBER;
    v_FindFIKind   NUMBER;
    v_NewAvr       DAVOIRISS_DBT%ROWTYPE;
    v_NextFIID     NUMBER;


  BEGIN

    SCIDMAP.SetError('INFO','----------','Запуск копирования Ценных бумаг');

    SELECT MAX(t_FIID) INTO v_NextFIID FROM DFININSTR_DBT;

    FOR v_Avoiriss IN c_Avoiriss LOOP
       v_stat := 0;

       BEGIN 
         SELECT * INTO v_FinInstr FROM Ivanov2028_86.DFININSTR_DBT WHERE T_FIID = v_Avoiriss.T_FIID;
       EXCEPTION
         WHEN OTHERS THEN v_stat := 1; SCIDMAP.SetError('ERROR','CopyContr','Ошибка при поиске записи в исходной таблице DFININSTR_DBT с FIID = '||v_Avoiriss.T_FIID);
       END;

       IF( v_stat = 0 ) THEN
          SCIDMAP.SetError('INFO','CopyAvoiriss',' Копирования ценной бумаги с кодом '||v_FinInstr.t_FI_Code);

          BEGIN 
            SELECT T_FIID, T_AVOIRKIND INTO v_FindFIID, v_FindFIKind FROM DFININSTR_DBT WHERE T_FI_Code = v_FinInstr.t_FI_Code;

            SELECT * INTO v_NewAvr FROM DAVOIRISS_DBT WHERE T_FIID = v_FindFIID;

            SCIDMAP.SetError('INFO','CopyAvoiriss','Ценная бумага с кодом '||v_FinInstr.t_FI_Code||' найдена.');
          EXCEPTION
            WHEN OTHERS THEN v_FindFIID := -1; SCIDMAP.SetError('INFO','CopyAvoiriss','Ценная бумага с кодом '||v_FinInstr.t_FI_Code||' не найдена. Производим перенос параметров.');
          END;

          IF( v_FindFIID > 0 ) THEN

             IF( (v_Avoiriss.t_ISIN <> v_NewAvr.t_ISIN) OR
                 (v_Avoiriss.t_LSIN <> v_NewAvr.t_LSIN) OR
                 (v_FinInstr.t_AvoirKind <> v_FindFIKind)
               ) THEN
                SCIDMAP.SetError('WARNING','CopyAvoiriss','Для ценной бумаги с кодом '||v_FinInstr.t_FI_Code||' LSIN или ISIN или вид ц/б (подвид ФИ) в ИБТ и НБТ не совпадают.');
             END IF;

             v_stat := SCIDMAP.PutObjMappedID( 2, v_FinInstr.t_FIID, v_FindFIID, CHR(0) );

          ELSE

             BEGIN
               v_OldFIID := v_FinInstr.t_FIID;
               v_NextFIID := v_NextFIID + 1;
               v_FinInstr.t_FIID := v_NextFIID;
               v_FinInstr.t_Issuer := SCIDMAP.GetObjMappedID( 1, v_FinInstr.t_Issuer, 1 );
               v_FinInstr.t_FaceValueFI := SCIDMAP.GetObjMappedID( 2, v_FinInstr.t_FaceValueFI, 0 );

               INSERT INTO DFININSTR_DBT VALUES v_FinInstr;

               v_NewFIID := v_FinInstr.t_FIID;
               v_Avoiriss.t_FIID := v_NewFIID;

               INSERT INTO DAVOIRISS_DBT VALUES v_Avoiriss;

             EXCEPTION
               WHEN DUP_VAL_ON_INDEX THEN v_stat := 1; SCIDMAP.SetError('INFO','CopyAvoiriss','Вставка записи DFININSTR_DBT для ценной бумаги '||v_FinInstr.t_FI_Code||' ненужна, запись есть.');
               WHEN OTHERS           THEN v_stat := 1; SCIDMAP.SetError('ERROR','CopyContr','Ошибка при вставке записи DFININSTR_DBT для ценной бумаги '||v_FinInstr.t_FI_Code);
             END;

             IF( v_stat = 0 ) THEN 
                v_stat := SCIDMAP.PutObjMappedID( 2, v_OldFIID, v_NewFIID, CHR(88) );
             END IF;


             IF( v_stat = 0 ) THEN 
                v_stat := InsertObjCode( v_OldFIID, v_NewFIID, 9 );
             END IF;

             IF( v_stat = 0 ) THEN 
                v_stat := InsertFiWarnts( v_OldFIID, v_NewFIID );
             END IF;

             IF( v_stat = 0 ) THEN
                v_stat := InsertNoteText( LPAD(v_OldFIID, 10, '0'), LPAD(v_NewFIID, 10, '0'), 12 );
             END IF;

             IF( v_stat = 0 ) THEN
                v_stat := InsertObjAtCor( LPAD(v_OldFIID, 10, '0'), LPAD(v_NewFIID, 10, '0'), 12 );
             END IF;

             IF( v_stat = 0 ) THEN
                v_stat := InsertRate(v_OldFIID, v_NewFIID);
             END IF;

          END IF;

          IF( v_stat <> 0 ) THEN 
             SCIDMAP.SetError('ERROR','CopyAvoiriss','Ошибка при копировании ценной бумаги '||v_FinInstr.t_FI_Code||'. Вероятно данные скопированы частично.');
          END IF;

       END IF;

    END LOOP;

  END;

  PROCEDURE CopyCurrency 
   IS
    CURSOR c_FinInstr IS ( SELECT * FROM Ivanov2028_86.DFININSTR_DBT WHERE t_FI_Kind = 1 );

    v_stat         NUMBER;
    v_NewFIID      NUMBER;
    v_OldFIID      NUMBER;
    v_FinInstr     DFININSTR_DBT%ROWTYPE;
    v_FindFIID     NUMBER;
    v_FindFIKind   NUMBER;
    v_NewAvr       DAVOIRISS_DBT%ROWTYPE;
    v_NextFIID     NUMBER;


  BEGIN

    SCIDMAP.SetError('INFO','----------','Запуск копирования Валют');

    SELECT MAX(t_FIID) INTO v_NextFIID FROM DFININSTR_DBT;

    FOR v_FinInstr IN c_FinInstr LOOP
       v_stat := 0;

       SCIDMAP.SetError('INFO','CopyCurrency','копируется валюта '||v_FinInstr.t_FI_Code);

       BEGIN 
         SELECT T_FIID INTO v_FindFIID FROM DFININSTR_DBT WHERE T_FI_Code = v_FinInstr.t_FI_Code;

         SCIDMAP.SetError('INFO','CopyCurrency','Валюта с кодом '||v_FinInstr.t_FI_Code||' найдена.');
       EXCEPTION
         WHEN OTHERS THEN v_FindFIID := -1; SCIDMAP.SetError('INFO','CopyCurrency','Валюта с кодом '||v_FinInstr.t_FI_Code||' не найдена. Производим перенос параметров.');
       END;
       
       if( v_FindFIID > 0 ) THEN
          v_stat := SCIDMAP.PutObjMappedID( 2, v_FinInstr.t_FIID, v_FindFIID, CHR(0) );
       ELSE
          BEGIN
            v_OldFIID := v_FinInstr.t_FIID;
            v_NextFIID := v_NextFIID + 1;
            v_FinInstr.t_FIID := v_NextFIID;

            INSERT INTO DFININSTR_DBT VALUES v_FinInstr;

            v_NewFIID := v_FinInstr.t_FIID;

          EXCEPTION
            WHEN DUP_VAL_ON_INDEX THEN v_stat := 1; SCIDMAP.SetError('INFO','CopyCurrency','Вставка записи DFININSTR_DBT для валюты '||v_FinInstr.t_FI_Code||' ненужна, запись есть.');
            WHEN OTHERS           THEN v_stat := 1; SCIDMAP.SetError('ERROR','CopyCurrency','Ошибка при вставке записи DFININSTR_DBT для валюты '||v_FinInstr.t_FI_Code);
          END;

          IF( v_stat = 0 ) THEN 
             v_stat := SCIDMAP.PutObjMappedID( 2, v_OldFIID, v_NewFIID, CHR(88) );
          END IF;

       END IF;

       IF( v_stat <> 0 ) THEN 
          SCIDMAP.SetError('ERROR','CopyCurrency','Ошибка при копировании валюты '||v_FinInstr.t_FI_Code||'. Вероятно данные скопированы частично.');
       END IF;

    END LOOP;

  END;

  PROCEDURE CopyDeal
   IS
    CURSOR c_Ticket IS ( SELECT * FROM Ivanov2028_86.DDL_TICK_DBT WHERE T_BOFFICEKIND IN (101,117,127) AND T_DEALSTATUS > 0 );
    CURSOR c_DlLeg  ( p_DealID NUMBER ) IS ( SELECT * FROM Ivanov2028_86.DDL_LEG_DBT WHERE t_DealID = p_DealID );
    CURSOR c_SPTKChng  ( p_DealID NUMBER ) IS ( SELECT * FROM Ivanov2028_86.DSPTKCHNG_DBT WHERE t_DealID = p_DealID );
    CURSOR c_SPGround IS ( SELECT gr.* FROM Ivanov2028_86.DSPGROUND_DBT gr WHERE (SELECT COUNT(1) FROM Ivanov2028_86.DSPGRDOC_DBT t WHERE t.T_SPGROUNDID = gr.T_SPGROUNDID AND t.T_SOURCEDOCKIND IN (101,117) ) > 0 );

    v_stat          NUMBER;
    v_OldDealID     NUMBER;
    v_NewDealID     NUMBER;
    v_OldDlLegID    NUMBER;
    v_NewDlLegID    NUMBER;
    v_OldSPGroundID NUMBER;
    v_NewSPGroundID NUMBER;
    v_tick         DDL_TICK_DBT%ROWTYPE;
    v_size          NUMBER;
  BEGIN

    FOR v_Ticket IN c_Ticket LOOP
       v_stat := 0;

       BEGIN
         v_OldDealID := v_Ticket.t_DealID;
         v_Ticket.t_DealID := 0;
         v_Ticket.t_PartyID := SCIDMAP.GetObjMappedID( 1, v_Ticket.t_PartyID, 0 );
         v_Ticket.t_BrokerID := SCIDMAP.GetObjMappedID( 1, v_Ticket.t_BrokerID, 0 );
         v_Ticket.t_ClientID := SCIDMAP.GetObjMappedID( 1, v_Ticket.t_ClientID, 0 );
         v_Ticket.t_TraderID := SCIDMAP.GetObjMappedID( 1, v_Ticket.t_TraderID, 0 );
         v_Ticket.t_MarketID := SCIDMAP.GetObjMappedID( 1, v_Ticket.t_MarketID, 0 );

         v_Ticket.t_ClientContrID := SCIDMAP.GetObjMappedID( 4, v_Ticket.t_ClientContrID, 0 );
         v_Ticket.t_BrokerContrID := SCIDMAP.GetObjMappedID( 4, v_Ticket.t_BrokerContrID, 0 );

         v_size := LENGTH(v_Ticket.t_DealCode);
         IF( v_size < 8 ) THEN
            v_Ticket.t_DealCode := CONCAT( SUBSTR('00000000', 0, 8-v_size), v_Ticket.t_DealCode);
         END IF;

         v_Ticket.t_DealCode := CONCAT('IBT-',v_Ticket.t_DealCode);

         SCIDMAP.SetError('INFO','CopyDeal','Копирование сделки '|| v_Ticket.t_DealCode);       

         INSERT INTO DDL_TICK_DBT ( T_DEALID, T_BOFFICEKIND, T_DEALTYPE, T_DEALGROUP, T_TRADESYSTEM,
                                    T_DEALCODE, T_DEALCODETS, T_TYPEDOC, T_USERTYPEDOC, T_PARTYID, T_BROKERID, T_CLIENTID, T_TRADERID,
                                    T_DEPOSITID, T_MARKETID, T_INDOCID, T_DEALDATE, T_REGDATE, T_DEALSTATUS, T_NUMBERPACK, T_DEPARTMENT,
                                    T_OPER, T_ORIGINID, T_EXTERNID, T_FLAG1, T_FLAG2, T_FLAG3, T_FLAG4, T_FLAG5, T_USERFIELD1,
                                    T_USERFIELD2, T_USERFIELD3, T_USERFIELD4, T_COMMENT, T_CLOSEDATE, T_SHIELD, T_SHIELDSIZE,
                                    T_ISPERCENT, T_SCALE, T_POINTS, T_REVRATE, T_COLLATERAL, T_DEALTIME, T_PORTFOLIOID, T_BUNDLE,
                                    T_CBRISKGROUP, T_RISKGROUP, T_ATTRIBUTES, T_PRODUCT, T_NETTING, T_DEALCODEPS, T_CONFTPID,
                                    T_LINKCHANNEL, T_NUMBER_COUPON, T_MARKETOFFICEID, T_CLIENTCONTRID, T_BROKERCONTRID, T_INDOCCODE,
                                    T_PREOUTLAY, T_GROUNDID, T_BUYGOAL, T_COMMDATE, T_PAYMENTSMETHOD, T_FIXSUM, T_NUMBER_PARTLY,
                                    T_CHANGEDATE, T_INSTANCE, T_CHANGEKIND, T_PORTFOLIOID_2, T_ISPARTYCLIENT, T_PARTYCONTRID, T_BRANCH,
                                    T_AVOIRKIND, T_OFBU, T_COSTCORRELATION, T_MARKETSCHEMEID, T_DEPSETID, T_PREOUTLAYFIID,
                                    T_RETURNINCOMEKIND, T_ISAUTO 
                                  ) VALUES 
                                  ( v_Ticket.T_DEALID, v_Ticket.T_BOFFICEKIND, v_Ticket.T_DEALTYPE, v_Ticket.T_DEALGROUP, v_Ticket.T_TRADESYSTEM,
                                    v_Ticket.T_DEALCODE, v_Ticket.T_DEALCODETS, v_Ticket.T_TYPEDOC, v_Ticket.T_USERTYPEDOC, v_Ticket.T_PARTYID, v_Ticket.T_BROKERID, v_Ticket.T_CLIENTID, v_Ticket.T_TRADERID,
                                    v_Ticket.T_DEPOSITID, v_Ticket.T_MARKETID, v_Ticket.T_INDOCID, v_Ticket.T_DEALDATE, v_Ticket.T_REGDATE, v_Ticket.T_DEALSTATUS, v_Ticket.T_NUMBERPACK, v_Ticket.T_DEPARTMENT,
                                    v_Ticket.T_OPER, v_Ticket.T_ORIGINID, v_Ticket.T_EXTERNID, v_Ticket.T_FLAG1, v_Ticket.T_FLAG2, v_Ticket.T_FLAG3, v_Ticket.T_FLAG4, v_Ticket.T_FLAG5, v_Ticket.T_USERFIELD1,
                                    v_Ticket.T_USERFIELD2, v_Ticket.T_USERFIELD3, v_Ticket.T_USERFIELD4, v_Ticket.T_COMMENT, v_Ticket.T_CLOSEDATE, v_Ticket.T_SHIELD, v_Ticket.T_SHIELDSIZE,
                                    v_Ticket.T_ISPERCENT, v_Ticket.T_SCALE, v_Ticket.T_POINTS, v_Ticket.T_REVRATE, v_Ticket.T_COLLATERAL, v_Ticket.T_DEALTIME, v_Ticket.T_PORTFOLIOID, v_Ticket.T_BUNDLE,
                                    v_Ticket.T_CBRISKGROUP, v_Ticket.T_RISKGROUP, v_Ticket.T_ATTRIBUTES, v_Ticket.T_PRODUCT, v_Ticket.T_NETTING, v_Ticket.T_DEALCODEPS, v_Ticket.T_CONFTPID,
                                    v_Ticket.T_LINKCHANNEL, v_Ticket.T_NUMBER_COUPON, v_Ticket.T_MARKETOFFICEID, v_Ticket.T_CLIENTCONTRID, v_Ticket.T_BROKERCONTRID, v_Ticket.T_INDOCCODE,
                                    v_Ticket.T_PREOUTLAY, v_Ticket.T_GROUNDID, v_Ticket.T_BUYGOAL, v_Ticket.T_COMMDATE, v_Ticket.T_PAYMENTSMETHOD, v_Ticket.T_FIXSUM, v_Ticket.T_NUMBER_PARTLY,
                                    v_Ticket.T_CHANGEDATE, v_Ticket.T_INSTANCE, v_Ticket.T_CHANGEKIND, v_Ticket.T_PORTFOLIOID_2, v_Ticket.T_ISPARTYCLIENT, v_Ticket.T_PARTYCONTRID, v_Ticket.T_BRANCH,
                                    v_Ticket.T_AVOIRKIND, v_Ticket.T_OFBU, v_Ticket.T_COSTCORRELATION, v_Ticket.T_MARKETSCHEMEID, v_Ticket.T_DEPSETID, v_Ticket.T_PREOUTLAYFIID,
                                    v_Ticket.T_RETURNINCOMEKIND, v_Ticket.T_ISAUTO ) RETURNING t_DealID INTO v_NewDealID;    

          v_stat := SCIDMAP.PutObjMappedID( 5, v_OldDealID, v_NewDealID, CHR(88) );
       EXCEPTION
         WHEN OTHERS THEN v_stat := 1; SCIDMAP.SetError('ERROR','CopyDeal','Ошибка при вставке DDL_TICK_DBT для сделки '|| v_Ticket.t_DealCode);
       END;

       IF( v_stat = 0 ) THEN

          FOR v_DlLeg IN c_DlLeg( v_OldDealID ) LOOP

             BEGIN
               v_OldDlLegID := v_DlLeg.t_ID;

               INSERT INTO DDL_LEG_DBT ( T_DEALID, T_LEGID, T_PFI, T_CFI, T_START, T_MATURITY, T_EXPIRY,
                                         T_PRINCIPAL, T_PRICE, T_BASIS, T_DURATION, T_PITCH, T_COST, T_MODE, T_CLOSED, T_REFRATE, T_FACTOR,
                                         T_FORMULA, T_VERSION, T_RESERVE0, T_PERIODNUMBER, T_PERIODTYPE, T_DIFF, T_PAYDAY, T_LEGKIND,
                                         T_SCALE, T_POINT, T_ISCALCUSED, T_LEGNUMBER, T_RELATIVEPRICE, T_NKD, T_TOTALCOST,
                                         T_MATURITYISPRINCIPAL, T_REGISTRAR, T_INCOMERATE, T_INCOMESCALE, T_INCOMEPOINT, T_INTERESTSTART,
                                         T_RECEIPTAMOUNT, T_REGISTRARCONTRID, T_PRINCIPALBASE, T_PRINCIPALDIFF, T_STARTBASE, T_STARTDIFF,
                                         T_BASE, T_PAYREGTAX, T_RETURNINCOME, T_REJECTDATE, T_DELIVERINGFIID, T_BITMASK, T_OPERSTATE,
                                         T_SUPPLYTIME 
                                       ) VALUES
                                       ( v_NewDealID, v_DlLeg.T_LEGID, SCIDMAP.GetObjMappedID(2, v_DlLeg.T_PFI, 0), SCIDMAP.GetObjMappedID(2, v_DlLeg.T_CFI, 0), v_DlLeg.T_START, v_DlLeg.T_MATURITY, v_DlLeg.T_EXPIRY,
                                         v_DlLeg.T_PRINCIPAL, v_DlLeg.T_PRICE, v_DlLeg.T_BASIS, v_DlLeg.T_DURATION, v_DlLeg.T_PITCH, v_DlLeg.T_COST, v_DlLeg.T_MODE, v_DlLeg.T_CLOSED, v_DlLeg.T_REFRATE, v_DlLeg.T_FACTOR,
                                         v_DlLeg.T_FORMULA, v_DlLeg.T_VERSION, v_DlLeg.T_RESERVE0, v_DlLeg.T_PERIODNUMBER, v_DlLeg.T_PERIODTYPE, v_DlLeg.T_DIFF, v_DlLeg.T_PAYDAY, v_DlLeg.T_LEGKIND,
                                         v_DlLeg.T_SCALE, v_DlLeg.T_POINT, v_DlLeg.T_ISCALCUSED, v_DlLeg.T_LEGNUMBER, v_DlLeg.T_RELATIVEPRICE, v_DlLeg.T_NKD, v_DlLeg.T_TOTALCOST,
                                         v_DlLeg.T_MATURITYISPRINCIPAL, SCIDMAP.GetObjMappedID(1, v_DlLeg.T_REGISTRAR, 0), v_DlLeg.T_INCOMERATE, v_DlLeg.T_INCOMESCALE, v_DlLeg.T_INCOMEPOINT, v_DlLeg.T_INTERESTSTART,
                                         v_DlLeg.T_RECEIPTAMOUNT, v_DlLeg.T_REGISTRARCONTRID, v_DlLeg.T_PRINCIPALBASE, v_DlLeg.T_PRINCIPALDIFF, v_DlLeg.T_STARTBASE, v_DlLeg.T_STARTDIFF,
                                         v_DlLeg.T_BASE, v_DlLeg.T_PAYREGTAX, v_DlLeg.T_RETURNINCOME, v_DlLeg.T_REJECTDATE, SCIDMAP.GetObjMappedID(2, v_DlLeg.T_DELIVERINGFIID, 0), v_DlLeg.T_BITMASK, v_DlLeg.T_OPERSTATE,
                                         v_DlLeg.T_SUPPLYTIME ) RETURNING t_ID INTO v_NewDlLegID;    

               v_stat := SCIDMAP.PutObjMappedID( 6, v_OldDlLegID, v_NewDlLegID, CHR(88) );

             EXCEPTION
               WHEN OTHERS THEN v_stat := 1; SCIDMAP.SetError('ERROR','CopyDeal','Ошибка при вставке DDL_LEG_DBT для сделки '|| v_Ticket.t_DealCode);       
             END;

          END LOOP;

       END IF;

       IF( v_stat = 0 ) THEN

          FOR v_SPTKChng IN c_SPTKChng( v_OldDealID ) LOOP

             BEGIN
               INSERT INTO DSPTKCHNG_DBT ( T_DEALID, T_OLDINSTANCE, T_OLDCHANGEDATE, T_OLDCHANGEKIND,
                                           T_OLDCFI1, T_OLDCFI2, T_OLDPRICE1, T_OLDPOINT1, T_OLDPRICE2, T_OLDPOINT2, T_OLDINCOMERATE,
                                           T_OLDCOST1, T_OLDCOST2, T_OLDTOTALCOST1, T_OLDTOTALCOST2, T_OLDNKD1, T_OLDNKD2, T_OLDFIXSUM,
                                           T_OLDCOSTCORRELATION, T_OLDPFI1, T_OLDPFI2, T_OLDPRINCIPAL, T_OLDFORMULA1, T_OLDFORMULA2,
                                           T_OLDPAYFIID1, T_OLDPAYFIID2, T_OLDPARTYID, T_OLDADVANCE1, T_OLDADVANCE2, T_OLDSTART1, T_OLDSTART2,
                                           T_OLDMATURITY1, T_OLDMATURITY2, T_OLDEXPIRY1, T_OLDEXPIRY2, T_OLDMATURITYISPRINCIPAL1,
                                           T_OLDMATURITYISPRINCIPAL2, T_OLDPREOUTLAY, T_OLDPAYREGTAX1, T_OLDPAYREGTAX2, T_OLDREGISTRAR1,
                                           T_OLDREGISTRAR2, T_OLDDEALTIME, T_OLDBASIS, T_OLDREJECTDATE1, T_OLDREJECTDATE2, T_SUM,
                                           T_ID_OPERATION, T_ID_STEP, T_OLDRETURNINCOME, T_FLAG3 
                                         ) VALUES
                                         ( v_NewDealID, v_SPTKChng.T_OLDINSTANCE, v_SPTKChng.T_OLDCHANGEDATE, v_SPTKChng.T_OLDCHANGEKIND,
                                           v_SPTKChng.T_OLDCFI1, v_SPTKChng.T_OLDCFI2, v_SPTKChng.T_OLDPRICE1, v_SPTKChng.T_OLDPOINT1, v_SPTKChng.T_OLDPRICE2, v_SPTKChng.T_OLDPOINT2, v_SPTKChng.T_OLDINCOMERATE,
                                           v_SPTKChng.T_OLDCOST1, v_SPTKChng.T_OLDCOST2, v_SPTKChng.T_OLDTOTALCOST1, v_SPTKChng.T_OLDTOTALCOST2, v_SPTKChng.T_OLDNKD1, v_SPTKChng.T_OLDNKD2, v_SPTKChng.T_OLDFIXSUM,
                                           v_SPTKChng.T_OLDCOSTCORRELATION, SCIDMAP.GetObjMappedID(2, v_SPTKChng.T_OLDPFI1, 0), SCIDMAP.GetObjMappedID(2, v_SPTKChng.T_OLDPFI2, 0), v_SPTKChng.T_OLDPRINCIPAL, v_SPTKChng.T_OLDFORMULA1, v_SPTKChng.T_OLDFORMULA2,
                                           v_SPTKChng.T_OLDPAYFIID1, v_SPTKChng.T_OLDPAYFIID2, SCIDMAP.GetObjMappedID(1, v_SPTKChng.T_OLDPARTYID, 0), v_SPTKChng.T_OLDADVANCE1, v_SPTKChng.T_OLDADVANCE2, v_SPTKChng.T_OLDSTART1, v_SPTKChng.T_OLDSTART2,
                                           v_SPTKChng.T_OLDMATURITY1, v_SPTKChng.T_OLDMATURITY2, v_SPTKChng.T_OLDEXPIRY1, v_SPTKChng.T_OLDEXPIRY2, v_SPTKChng.T_OLDMATURITYISPRINCIPAL1,
                                           v_SPTKChng.T_OLDMATURITYISPRINCIPAL2, v_SPTKChng.T_OLDPREOUTLAY, v_SPTKChng.T_OLDPAYREGTAX1, v_SPTKChng.T_OLDPAYREGTAX2, SCIDMAP.GetObjMappedID(1, v_SPTKChng.T_OLDREGISTRAR1, 0),
                                           SCIDMAP.GetObjMappedID(1, v_SPTKChng.T_OLDREGISTRAR2, 0), v_SPTKChng.T_OLDDEALTIME, v_SPTKChng.T_OLDBASIS, v_SPTKChng.T_OLDREJECTDATE1, v_SPTKChng.T_OLDREJECTDATE2, v_SPTKChng.T_SUM,
                                           0, 0, v_SPTKChng.T_OLDRETURNINCOME, v_SPTKChng.T_FLAG3 );    

             EXCEPTION
               WHEN OTHERS THEN v_stat := 1; SCIDMAP.SetError('ERROR','CopyDeal','Ошибка при вставке DSPTKCHNG_DBT для сделки '|| v_Ticket.t_DealCode);       
             END;

          END LOOP;

       END IF;

       IF( v_stat = 0 ) THEN
          v_stat := InsertNoteText( LPAD(v_OldDealID, 10, '0'), LPAD(v_NewDealID, 10, '0'), v_Ticket.T_BOFFICEKIND );
       END IF;

       IF( v_stat = 0 ) THEN
          v_stat := InsertObjAtCor( LPAD(v_OldDealID, 10, '0'), LPAD(v_NewDealID, 10, '0'), v_Ticket.T_BOFFICEKIND );
       END IF;

       IF( (v_stat = 0) AND (v_Ticket.T_DEALSTATUS = 10) ) THEN
          CopyOperation( v_Ticket.T_BOFFICEKIND, v_OldDealID, v_NewDealID );
       END IF;
      
       IF( v_stat <> 0 ) THEN 
          SCIDMAP.SetError('ERROR','CopyDeal','Ошибка при копровани сделки с номером '||v_Ticket.t_DealCode||'. Вероятно данные скопированы частично.');
       END IF;
    
    END LOOP;

    IF( v_stat = 0 ) THEN
       FOR v_SPGround IN c_SPGround LOOP
       
          BEGIN
            v_OldSPGroundID := v_SPGround.t_SPGroundID;
            v_SPGround.t_SPGroundID := 0;
            v_SPGround.t_Proxy := SCIDMAP.GetObjMappedID(1, v_SPGround.t_Proxy, 0);
            v_SPGround.t_Party := SCIDMAP.GetObjMappedID(1, v_SPGround.t_Party, 0);

            INSERT INTO DSPGROUND_DBT VALUES v_SPGround RETURNING t_SPGroundID INTO v_NewSPGroundID;

            INSERT INTO DSPGRDOC_DBT (SELECT T_SOURCEDOCKIND, SCIDMAP.GetObjMappedID(5, T_SOURCEDOCID, 0), v_NewSPGroundID, T_ORDER, T_DEBITCREDIT FROM Ivanov2028_86.DSPGRDOC_DBT WHERE T_SPGROUNDID = v_OldSPGroundID AND T_SOURCEDOCKIND IN (101,117) );

          EXCEPTION
            WHEN OTHERS THEN SCIDMAP.SetError('ERROR','CopyDeal','Ошибка при вставке DSPGROUND_DBT для сделок ');       
          END;

       END LOOP;
    END IF;

    IF( v_stat = 0 ) THEN
       BEGIN

         INSERT INTO DDLSUM_DBT ( SELECT 0, T_DOCKIND, SCIDMAP.GetObjMappedID( DECODE(T_DOCKIND, 176, 6, 5), T_DOCID, 0 ), T_KIND, T_DATE, T_SUM, T_NDS, T_CURRENCY 
                                    FROM Ivanov2028_86.DDLSUM_DBT );

       EXCEPTION
         WHEN OTHERS THEN SCIDMAP.SetError('ERROR','CopyDeal','Ошибка при вставке DDLSUM_DBT для сделок ');       
       END;
    END IF;

  END;

  PROCEDURE CopyPaym
   IS
    CURSOR c_Paym IS ( SELECT * FROM Ivanov2028_86.DPMPAYM_DBT WHERE t_DocKind in (101, 117, 127) );
    v_NewPaymID     NUMBER;
    v_OldPaymID     NUMBER;
    v_stat          NUMBER;
  BEGIN

    SCIDMAP.SetError('INFO','CopyPaym','Копирую платежи по сделкам');       
    FOR v_Paym IN c_Paym LOOP

       BEGIN
         v_OldPaymID := v_Paym.t_PaymentID;
         v_Paym.t_PaymentID := 0;
         v_Paym.t_DocumentID := SCIDMAP.GetObjMappedID(5, v_Paym.t_DocumentID, 0);

         v_Paym.T_FIID := SCIDMAP.GetObjMappedID(2, v_Paym.T_FIID, 0);
         v_Paym.T_PAYFIID := SCIDMAP.GetObjMappedID(2, v_Paym.T_PAYFIID, 0);
         v_Paym.T_BASEFIID := SCIDMAP.GetObjMappedID(2, v_Paym.T_BASEFIID, 0);
         v_Paym.T_FIID_FUTUREPAYACC := SCIDMAP.GetObjMappedID(2, v_Paym.T_FIID_FUTUREPAYACC, 0);
         v_Paym.T_FIID_FUTURERECACC := SCIDMAP.GetObjMappedID(2, v_Paym.T_FIID_FUTURERECACC, 0);

         v_Paym.T_PAYER := SCIDMAP.GetObjMappedID(1, v_Paym.T_PAYER, 0);
         v_Paym.T_PAYERBANKID := SCIDMAP.GetObjMappedID(1, v_Paym.T_PAYERBANKID, 0);
         v_Paym.T_PAYERMESBANKID := SCIDMAP.GetObjMappedID(1, v_Paym.T_PAYERMESBANKID, 0);
         v_Paym.T_RECEIVER := SCIDMAP.GetObjMappedID(1, v_Paym.T_RECEIVER, 0);
         v_Paym.T_RECEIVERBANKID := SCIDMAP.GetObjMappedID(1, v_Paym.T_RECEIVERBANKID, 0);
         v_Paym.T_RECEIVERMESBANKID := SCIDMAP.GetObjMappedID(1, v_Paym.T_RECEIVERMESBANKID, 0);

         INSERT INTO DPMPAYM_DBT ( T_DOCKIND, T_DOCUMENTID, T_PURPOSE, T_SUBPURPOSE, T_FIID,
                                   T_AMOUNT, T_PAYFIID, T_PAYER, T_PAYERBANKID, T_PAYERMESBANKID, T_PAYERACCOUNT, T_RECEIVER,
                                   T_RECEIVERBANKID, T_RECEIVERMESBANKID, T_RECEIVERACCOUNT, T_VALUEDATE, T_PAYMSTATUS, T_DELIVERYKIND,
                                   T_NETTING, T_DEPARTMENT, T_PROCKIND, T_PLANPAYMID, T_FACTPAYMID, T_NUMBERPACK, T_SUBSPLITTEDPAYMENT,
                                   T_CREATEDINSS, T_LASTSPLITSESSION, T_TOBACKOFFICE, T_LEGID, T_INDOORSTORAGE, T_AMOUNTNDS,
                                   T_ACCOUNTNDS, T_ISPLANPAYM, T_ISFACTPAYM, T_RESERV1, T_RECALLAMOUNT, T_FUTUREPAYERACCOUNT,
                                   T_FUTURERECEIVERACCOUNT, T_PAYAMOUNT, T_RATETYPE, T_ISINVERSE, T_SCALE, T_POINT, T_ISFIXAMOUNT,
                                   T_FEETYPE, T_DEFCOMID, T_PAYERCODEKIND, T_PAYERCODE, T_RECEIVERCODEKIND, T_RECEIVERCODE, T_RATE,
                                   T_FIID_FUTUREPAYACC, T_FIID_FUTURERECACC, T_BASEAMOUNT, T_BASEFIID, T_RATEDATE, T_BASERATETYPE,
                                   T_BASERATE, T_BASEPOINT, T_BASESCALE, T_ISBASEINVERSE, T_BASERATEDATE, T_FUTUREPAYERAMOUNT,
                                   T_FUTURERECEIVERAMOUNT, T_SUBKIND, T_PAYERDPNODE, T_RECEIVERDPNODE, T_PAYERDPBLOCK,
                                   T_RECEIVERDPBLOCK, T_I2PLACEDATE, T_PAYERBANKMARKDATE, T_RECEIVERBANKMARKDATE, T_PAYERBANKENTERDATE,
                                   T_PARTPAYMNUMBER, T_PARTPAYMSHIFRMAIN, T_PARTPAYMNUMMAIN, T_PARTPAYMDATEMAIN,
                                   T_PARTPAYMRESTAMOUNTMAIN, T_KINDOPERATION, T_CLAIMID, T_OPER, T_ORIGIN, T_STARTDEPARTMENT,
                                   T_ENDDEPARTMENT, T_DBFLAG, T_ISPURPOSE, T_NOTFORBACKOFFICE, T_PLACETOINDEX, T_PRIMDOCKIND,
                                   T_CONVERTED, T_PAYERDPPARTITION, T_RECEIVERDPPARTITION, T_LINKAMOUNTKIND, T_COMISSFIID,
                                   T_COMISSACCOUNT, T_CONTRNVERSION, T_BOPROCESSKIND, T_BENEFIT, T_FUTURERATETYPE, T_FUTURERATE,
                                   T_FUTUREPOINT, T_FUTURESCALE, T_FUTUREISINVERSE, T_FUTURERATEDATE, T_FUTURERATEDEPARTMENT,
                                   T_CHAPTER, T_FUTUREBASEAMOUNT
                                 ) VALUES
                                 ( v_Paym.T_DOCKIND, v_Paym.T_DOCUMENTID, v_Paym.T_PURPOSE, v_Paym.T_SUBPURPOSE, v_Paym.T_FIID,
                                   v_Paym.T_AMOUNT, v_Paym.T_PAYFIID, v_Paym.T_PAYER, v_Paym.T_PAYERBANKID, v_Paym.T_PAYERMESBANKID, v_Paym.T_PAYERACCOUNT, v_Paym.T_RECEIVER,
                                   v_Paym.T_RECEIVERBANKID, v_Paym.T_RECEIVERMESBANKID, v_Paym.T_RECEIVERACCOUNT, v_Paym.T_VALUEDATE, v_Paym.T_PAYMSTATUS, v_Paym.T_DELIVERYKIND,
                                   v_Paym.T_NETTING, v_Paym.T_DEPARTMENT, v_Paym.T_PROCKIND, v_Paym.T_PLANPAYMID, v_Paym.T_FACTPAYMID, v_Paym.T_NUMBERPACK, v_Paym.T_SUBSPLITTEDPAYMENT,
                                   v_Paym.T_CREATEDINSS, v_Paym.T_LASTSPLITSESSION, v_Paym.T_TOBACKOFFICE, v_Paym.T_LEGID, v_Paym.T_INDOORSTORAGE, v_Paym.T_AMOUNTNDS,
                                   v_Paym.T_ACCOUNTNDS, v_Paym.T_ISPLANPAYM, v_Paym.T_ISFACTPAYM, v_Paym.T_RESERV1, v_Paym.T_RECALLAMOUNT, v_Paym.T_FUTUREPAYERACCOUNT,
                                   v_Paym.T_FUTURERECEIVERACCOUNT, v_Paym.T_PAYAMOUNT, v_Paym.T_RATETYPE, v_Paym.T_ISINVERSE, v_Paym.T_SCALE, v_Paym.T_POINT, v_Paym.T_ISFIXAMOUNT,
                                   v_Paym.T_FEETYPE, v_Paym.T_DEFCOMID, v_Paym.T_PAYERCODEKIND, v_Paym.T_PAYERCODE, v_Paym.T_RECEIVERCODEKIND, v_Paym.T_RECEIVERCODE, v_Paym.T_RATE,
                                   v_Paym.T_FIID_FUTUREPAYACC, v_Paym.T_FIID_FUTURERECACC, v_Paym.T_BASEAMOUNT, v_Paym.T_BASEFIID, v_Paym.T_RATEDATE, v_Paym.T_BASERATETYPE,
                                   v_Paym.T_BASERATE, v_Paym.T_BASEPOINT, v_Paym.T_BASESCALE, v_Paym.T_ISBASEINVERSE, v_Paym.T_BASERATEDATE, v_Paym.T_FUTUREPAYERAMOUNT,
                                   v_Paym.T_FUTURERECEIVERAMOUNT, v_Paym.T_SUBKIND, v_Paym.T_PAYERDPNODE, v_Paym.T_RECEIVERDPNODE, v_Paym.T_PAYERDPBLOCK,
                                   v_Paym.T_RECEIVERDPBLOCK, v_Paym.T_I2PLACEDATE, v_Paym.T_PAYERBANKMARKDATE, v_Paym.T_RECEIVERBANKMARKDATE, v_Paym.T_PAYERBANKENTERDATE,
                                   v_Paym.T_PARTPAYMNUMBER, v_Paym.T_PARTPAYMSHIFRMAIN, v_Paym.T_PARTPAYMNUMMAIN, v_Paym.T_PARTPAYMDATEMAIN,
                                   v_Paym.T_PARTPAYMRESTAMOUNTMAIN, v_Paym.T_KINDOPERATION, v_Paym.T_CLAIMID, v_Paym.T_OPER, v_Paym.T_ORIGIN, v_Paym.T_STARTDEPARTMENT,
                                   v_Paym.T_ENDDEPARTMENT, v_Paym.T_DBFLAG, v_Paym.T_ISPURPOSE, v_Paym.T_NOTFORBACKOFFICE, v_Paym.T_PLACETOINDEX, v_Paym.T_PRIMDOCKIND,
                                   v_Paym.T_CONVERTED, v_Paym.T_PAYERDPPARTITION, v_Paym.T_RECEIVERDPPARTITION, v_Paym.T_LINKAMOUNTKIND, v_Paym.T_COMISSFIID,
                                   v_Paym.T_COMISSACCOUNT, v_Paym.T_CONTRNVERSION, v_Paym.T_BOPROCESSKIND, v_Paym.T_BENEFIT, v_Paym.T_FUTURERATETYPE, v_Paym.T_FUTURERATE,
                                   v_Paym.T_FUTUREPOINT, v_Paym.T_FUTURESCALE, v_Paym.T_FUTUREISINVERSE, v_Paym.T_FUTURERATEDATE, v_Paym.T_FUTURERATEDEPARTMENT,
                                   v_Paym.T_CHAPTER, v_Paym.T_FUTUREBASEAMOUNT ) RETURNING t_PaymentID INTO v_NewPaymID;

         v_stat := SCIDMAP.PutObjMappedID( 7, v_OldPaymID, v_NewPaymID, CHR(88) );


         INSERT INTO DPMPROP_DBT ( SELECT v_NewPaymID, T_DEBETCREDIT, T_CODEKIND, T_CODENAME, SC_CONVERT.FindObjCode(3, T_CODEKIND, v_Paym.T_PAYERBANKID), SCIDMAP.GetObjMappedID(2, T_PAYFIID, 0),
                                          T_CORSCHEM, T_ISSENDER, T_PROPSTATUS, T_TPID, T_TRANSFERDATE, T_CORRACC, T_CORRCODEKIND,
                                          T_CORRCODENAME, T_CORRCODE, T_SORTKEY, T_CORRPOSTYPE, T_INSTRUCTIONABONENT, T_SETTLEMENTSYSTEMCODE,
                                          SCIDMAP.GetObjMappedID(1, T_CORRID, 0), SCIDMAP.GetObjMappedID(1, T_OURCORRID, 0), T_OURCORRCODEKIND, T_OURCORRCODE, T_OURCORRACC, T_INOURBALANCE, T_GROUP,
                                          T_CONTRNVERSION, T_TPSCHEMID, T_RLSFORMID, T_RESERVE 
                                     FROM Ivanov2028_86.DPMPROP_DBT
                                    WHERE T_PAYMENTID = v_OldPaymID AND T_DEBETCREDIT = 0);

         INSERT INTO DPMPROP_DBT ( SELECT v_NewPaymID, T_DEBETCREDIT, T_CODEKIND, T_CODENAME, SC_CONVERT.FindObjCode(3, T_CODEKIND, v_Paym.T_RECEIVERBANKID), SCIDMAP.GetObjMappedID(2, T_PAYFIID, 0),
                                          T_CORSCHEM, T_ISSENDER, T_PROPSTATUS, T_TPID, T_TRANSFERDATE, T_CORRACC, T_CORRCODEKIND,
                                          T_CORRCODENAME, T_CORRCODE, T_SORTKEY, T_CORRPOSTYPE, T_INSTRUCTIONABONENT, T_SETTLEMENTSYSTEMCODE,
                                          SCIDMAP.GetObjMappedID(1, T_CORRID, 0), SCIDMAP.GetObjMappedID(1, T_OURCORRID, 0), T_OURCORRCODEKIND, T_OURCORRCODE, T_OURCORRACC, T_INOURBALANCE, T_GROUP,
                                          T_CONTRNVERSION, T_TPSCHEMID, T_RLSFORMID, T_RESERVE 
                                     FROM Ivanov2028_86.DPMPROP_DBT
                                    WHERE T_PAYMENTID = v_OldPaymID AND T_DEBETCREDIT = 1);

         INSERT INTO DPMRMPROP_DBT ( SELECT v_NewPaymID, T_NUMBER, T_REFERENCE, T_DATE, T_PAYMENTKIND,
                                            T_PAYERCORRACCNOSTRO, SC_CONVERT.FindPartyName(v_Paym.T_PAYERBANKID), SC_CONVERT.FindPartyName(v_Paym.T_PAYER), T_PAYERINN, T_RECEIVERCORRACCNOSTRO,
                                            SC_CONVERT.FindPartyName(v_Paym.T_RECEIVERBANKID), SC_CONVERT.FindPartyName(v_Paym.T_RECEIVER), T_RECEIVERINN, T_SHIFROPER, T_PRIORITY, T_PAYDATE, T_GROUND,
                                            T_CLIENTDATE, T_PROCESSKIND, T_MESSAGETYPE, T_PARTYINFO, T_PAYERCORRBANKNAME,
                                            T_RECEIVERCORRBANKNAME, T_ISSHORTFORMAT, T_KINDPAYCURRENCY, T_OURPAYERCORRNAME,
                                            T_OURRECEIVERCORRNAME, T_PAYERCHARGEOFFDATE, T_TAXAUTHORSTATE, T_BTTTICODE, T_OKATOCODE,
                                            T_TAXPMGROUND, T_TAXPMPERIOD, T_TAXPMNUMBER, T_TAXPMDATE, T_TAXPMTYPE, T_SYMBNOTBALDEBET,
                                            T_INSTANCY, T_DOCDISPATCHDATE, T_CASHSYMBOLDEBET, T_CASHSYMBOLCREDIT, T_SYMBNOTBALCREDIT,
                                            T_ISOPTIMBYTIME, T_COMISSCHARGES, T_INSTRUCTIONCODE, T_ADDITIONALINFO, T_CONTRNVERSION,
                                            T_BENEFITNOTE
                                       FROM Ivanov2028_86.DPMRMPROP_DBT
                                      WHERE T_PAYMENTID = v_OldPaymID );

         INSERT INTO DPMHIST_DBT ( SELECT 0, v_NewPaymID, T_STATUSIDTO, T_STATUSIDFROM, T_OPER, T_DATE,
                                          T_SYSDATE, T_SYSTIME, T_OLDVALUEDATE, T_NEWVALUEDATE, T_ISPLANPAYM, T_ISFACTPAYM,
                                          T_IAPPLICATIONKIND, T_APPLICATIONKEY, T_CONTRNVERSION, T_RESERVE
                                     FROM Ivanov2028_86.DPMHIST_DBT
                                    WHERE T_PAYMENTID = v_OldPaymID );

       EXCEPTION
         WHEN OTHERS THEN SCIDMAP.SetError('ERROR','CopyDeal','Ошибка при вставке DPMPAYM_DBT PaymID = '||v_OldPaymID);       
       END;
    END LOOP;

  END;

  PROCEDURE CopyLots
   IS
    CURSOR c_pmwrtsum IS SELECT * FROM Ivanov2028_86.DPMWRTSUM_DBT WHERE T_DOCKIND IN (29,117) ORDER BY T_SUMID;
    CURSOR c_pmwrtsum_new IS SELECT * FROM DPMWRTSUM_DBT WHERE (t_Parent > 0) OR (t_Source > 0) ORDER BY T_SUMID;
    CURSOR c_pmwrtlnk IS SELECT * FROM Ivanov2028_86.DPMWRTLNK_DBT ORDER BY T_LNKID;
    CURSOR c_pmwrtbc IS ( SELECT * FROM Ivanov2028_86.DPMWRTBC_DBT );
    v_stat         NUMBER;
    v_OldSumID     NUMBER;
    v_NewSumID     NUMBER;
    v_size         NUMBER;
  BEGIN

    SCIDMAP.SetError('INFO','CopyLots','Копирование лотов.');       

    FOR v_pmwrtsum IN c_pmwrtsum LOOP
       BEGIN
         SCIDMAP.SetError('INFO','CopyLots','Insert lot '||v_pmwrtsum.t_SumID);       
         v_OldSumID := v_pmwrtsum.t_SumID;
         v_pmwrtsum.t_SumID := 0;
         BEGIN 
           v_pmwrtsum.t_ID_Operation := SCIDMAP.GetObjMappedID( 8, v_pmwrtsum.t_ID_Operation, 0 );
         EXCEPTION
           WHEN OTHERS THEN SCIDMAP.SetError('WARNING','CopyLots','Нет операции, ID операции зануляем.');       
              v_pmwrtsum.t_ID_Operation := 0;
              v_pmwrtsum.t_ID_Step := 0;
         END;
         v_pmwrtsum.t_DocID := SCIDMAP.GetObjMappedID( 7, v_pmwrtsum.t_DocID, 0 );
         v_pmwrtsum.t_Party := SCIDMAP.GetObjMappedID( 1, v_pmwrtsum.t_Party, 0 );
         v_pmwrtsum.t_FIID := SCIDMAP.GetObjMappedID( 2, v_pmwrtsum.t_FIID, 0 );
         v_pmwrtsum.t_DealID := SCIDMAP.GetObjMappedID( 5, v_pmwrtsum.t_DealID, 0 );

         v_size := LENGTH(v_pmwrtsum.t_DealCode);
         IF( v_size < 8 ) THEN
            v_pmwrtsum.t_DealCode := CONCAT( SUBSTR('00000000', 0, 8-v_size), v_pmwrtsum.t_DealCode);
         END IF;
         v_pmwrtsum.t_DealCode := CONCAT('IBT-',v_pmwrtsum.t_DealCode);
/*
         v_pmwrtsum.t_Parent := SCIDMAP.GetObjMappedID( 9, v_pmwrtsum.t_Parent, 0 );
         v_pmwrtsum.t_Source := SCIDMAP.GetObjMappedID( 9, v_pmwrtsum.t_Source, 0 );
*/
         INSERT INTO DPMWRTSUM_DBT ( T_SUMID, T_DOCKIND, T_DOCID, T_PARTNUM, T_PARTY, T_CONTRACT,
                                     T_PORTFOLIO, T_FIID, T_GROUPID, T_BUY_SALE, T_KIND, T_DATE, T_TIME, T_AMOUNT, T_AMOUNTBD, T_SUM,
                                     T_CURRENCY, T_COST, T_BALANCECOST, T_BALANCECOSTBD, T_NKDAMOUNT, T_INTERESTINCOME,
                                     T_NOTCARRYINTEREST, T_INTERESTDATE, T_BEGDISCOUNT, T_DISCOUNTINCOME, T_NOTCARRYDISCOUNT,
                                     T_DISCOUNTDATE, T_OUTLAY, T_RESERVAMOUNT, T_RESERVDATE, T_OVERAMOUNT, T_OVERAMOUNTBD, T_OVERDATE,
                                     T_COUPON, T_PARTLY, T_DEPARTMENT, T_DEALID, T_DEALDATE, T_DEALCODE, T_STATE, T_ENTERDATE,
                                     T_STATEDATE, T_INSTANCE, T_CHANGEDATE, T_ACTION, T_ID_OPERATION, T_ID_STEP, T_ISFREE, T_TRUST,
                                     T_PARENT, T_SOURCE, T_BEGDATE
                                   ) VALUES 
                                   ( v_pmwrtsum.T_SUMID, v_pmwrtsum.T_DOCKIND, v_pmwrtsum.T_DOCID, v_pmwrtsum.T_PARTNUM, v_pmwrtsum.T_PARTY, v_pmwrtsum.T_CONTRACT,
                                     v_pmwrtsum.T_PORTFOLIO, v_pmwrtsum.T_FIID, v_pmwrtsum.T_GROUPID, v_pmwrtsum.T_BUY_SALE, v_pmwrtsum.T_KIND, v_pmwrtsum.T_DATE, v_pmwrtsum.T_TIME, v_pmwrtsum.T_AMOUNT, v_pmwrtsum.T_AMOUNTBD, v_pmwrtsum.T_SUM,
                                     v_pmwrtsum.T_CURRENCY, v_pmwrtsum.T_COST, v_pmwrtsum.T_BALANCECOST, v_pmwrtsum.T_BALANCECOSTBD, v_pmwrtsum.T_NKDAMOUNT, v_pmwrtsum.T_INTERESTINCOME,
                                     v_pmwrtsum.T_NOTCARRYINTEREST, v_pmwrtsum.T_INTERESTDATE, v_pmwrtsum.T_BEGDISCOUNT, v_pmwrtsum.T_DISCOUNTINCOME, v_pmwrtsum.T_NOTCARRYDISCOUNT,
                                     v_pmwrtsum.T_DISCOUNTDATE, v_pmwrtsum.T_OUTLAY, v_pmwrtsum.T_RESERVAMOUNT, v_pmwrtsum.T_RESERVDATE, v_pmwrtsum.T_OVERAMOUNT, v_pmwrtsum.T_OVERAMOUNTBD, v_pmwrtsum.T_OVERDATE,
                                     v_pmwrtsum.T_COUPON, v_pmwrtsum.T_PARTLY, v_pmwrtsum.T_DEPARTMENT, v_pmwrtsum.T_DEALID, v_pmwrtsum.T_DEALDATE, v_pmwrtsum.T_DEALCODE, v_pmwrtsum.T_STATE, v_pmwrtsum.T_ENTERDATE,
                                     v_pmwrtsum.T_STATEDATE, v_pmwrtsum.T_INSTANCE, v_pmwrtsum.T_CHANGEDATE, v_pmwrtsum.T_ACTION, v_pmwrtsum.T_ID_OPERATION, v_pmwrtsum.T_ID_STEP, v_pmwrtsum.T_ISFREE, v_pmwrtsum.T_TRUST,
                                     v_pmwrtsum.T_PARENT, v_pmwrtsum.T_SOURCE, v_pmwrtsum.T_BEGDATE ) RETURNING t_SumID INTO v_NewSumID;

         v_stat := SCIDMAP.PutObjMappedID( 9, v_OldSumID, v_NewSumID, CHR(88) );


       EXCEPTION
         WHEN OTHERS THEN SCIDMAP.SetError('ERROR','CopyLots','Ошибка при вставке DPMWRTSUM_DBT для лота по сделке '||v_pmwrtsum.t_DealCode);       
       END;
    END LOOP;

    SCIDMAP.SetError('INFO','CopyLots','Копирование связей.');       

    FOR v_pmwrtlnk IN c_pmwrtlnk LOOP
       BEGIN
         v_pmwrtlnk.t_LnkID := 0;
         BEGIN 
           v_pmwrtlnk.t_ID_Operation := SCIDMAP.GetObjMappedID( 8, v_pmwrtlnk.t_ID_Operation, 0 );
         EXCEPTION
           WHEN OTHERS THEN SCIDMAP.SetError('WARNING','CopyLots','Нет операции, ID операции зануляем.');       
              v_pmwrtlnk.t_ID_Operation := 0;
              v_pmwrtlnk.t_ID_Step := 0;
         END;
         v_pmwrtlnk.t_SaleID := SCIDMAP.GetObjMappedID( 9, v_pmwrtlnk.t_SaleID, 0 );
         v_pmwrtlnk.t_BuyID := SCIDMAP.GetObjMappedID( 9, v_pmwrtlnk.t_BuyID, 0 );

         INSERT INTO DPMWRTLNK_DBT ( T_LNKID, T_SALEID, T_BUYID, T_KIND, T_AMOUNT, T_SUMSALE, T_SUMBUY,
                                     T_COSTSALE, T_COSTBUY, T_BALANCECOSTSALE, T_BALANCECOSTBUY, T_BALANCECOSTBD, T_NKDSALEAMOUNT,
                                     T_NKDBUYAMOUNT, T_INTERESTINCOMEBUY, T_INTERESTINCOMEADD, T_NOTCARRYINTERESTBUY,
                                     T_NOTCARRYINTERESTADD, T_BEGDISCOUNTCHANGE, T_DISCOUNTINCOMEBUY, T_DISCOUNTINCOMEADD,
                                     T_NOTCARRYDISCOUNTBUY, T_NOTCARRYDISCOUNTADD, T_OUTLAYSALE, T_OUTLAYBUY, T_RESERVCHANGE,
                                     T_OVERCHANGE, T_COUPON, T_PARTLY, T_ID_OPERATION, T_ID_STEP, T_ACTION,
                                     T_CREATEDATE
                                   ) VALUES 
                                   ( v_pmwrtlnk.T_LNKID, v_pmwrtlnk.T_SALEID, v_pmwrtlnk.T_BUYID, v_pmwrtlnk.T_KIND, v_pmwrtlnk.T_AMOUNT, v_pmwrtlnk.T_SUMSALE, v_pmwrtlnk.T_SUMBUY,
                                     v_pmwrtlnk.T_COSTSALE, v_pmwrtlnk.T_COSTBUY, v_pmwrtlnk.T_BALANCECOSTSALE, v_pmwrtlnk.T_BALANCECOSTBUY, v_pmwrtlnk.T_BALANCECOSTBD, v_pmwrtlnk.T_NKDSALEAMOUNT,
                                     v_pmwrtlnk.T_NKDBUYAMOUNT, v_pmwrtlnk.T_INTERESTINCOMEBUY, v_pmwrtlnk.T_INTERESTINCOMEADD, v_pmwrtlnk.T_NOTCARRYINTERESTBUY,
                                     v_pmwrtlnk.T_NOTCARRYINTERESTADD, v_pmwrtlnk.T_BEGDISCOUNTCHANGE, v_pmwrtlnk.T_DISCOUNTINCOMEBUY, v_pmwrtlnk.T_DISCOUNTINCOMEADD,
                                     v_pmwrtlnk.T_NOTCARRYDISCOUNTBUY, v_pmwrtlnk.T_NOTCARRYDISCOUNTADD, v_pmwrtlnk.T_OUTLAYSALE, v_pmwrtlnk.T_OUTLAYBUY, v_pmwrtlnk.T_RESERVCHANGE,
                                     v_pmwrtlnk.T_OVERCHANGE, v_pmwrtlnk.T_COUPON, v_pmwrtlnk.T_PARTLY, v_pmwrtlnk.T_ID_OPERATION, v_pmwrtlnk.T_ID_STEP, v_pmwrtlnk.T_ACTION,
                                     v_pmwrtlnk.T_CREATEDATE );

       EXCEPTION
         WHEN OTHERS THEN SCIDMAP.SetError('ERROR','CopyLots','Ошибка при вставке DPMWRTLNK_DBT для лотов BuyID='||v_pmwrtlnk.t_BuyID||' SaleID='||v_pmwrtlnk.t_SaleID);       
       END;
    END LOOP;

    SCIDMAP.SetError('INFO','CopyLots','Копирование истории изменений лота.');       

    FOR v_pmwrtbc IN c_pmwrtbc LOOP
       BEGIN
         v_pmwrtbc.t_BCID := 0;
         v_pmwrtbc.t_ID_Operation := SCIDMAP.GetObjMappedID( 8, v_pmwrtbc.t_ID_Operation, 0 );
         v_pmwrtbc.t_SumID := SCIDMAP.GetObjMappedID( 9, v_pmwrtbc.t_SumID, 0 );
         v_pmwrtbc.t_FIID := SCIDMAP.GetObjMappedID( 2, v_pmwrtbc.t_FIID, 0 );

         INSERT INTO DPMWRTBC_DBT ( T_BCID, T_SUMID, T_ID_OPERATION, T_ID_STEP, T_ACTION, T_INSTANCE,
                                    T_CHANGEDATE, T_FIID, T_PORTFOLIO, T_GROUPID, T_DATE, T_TIME, T_AMOUNT, T_AMOUNTBD, T_SUM,
                                    T_CURRENCY, T_COST, T_BALANCECOST, T_BALANCECOSTBD, T_NKDAMOUNT, T_INTERESTINCOME,
                                    T_NOTCARRYINTEREST, T_INTERESTDATE, T_BEGDISCOUNT, T_DISCOUNTINCOME, T_NOTCARRYDISCOUNT,
                                    T_DISCOUNTDATE, T_OUTLAY, T_RESERVAMOUNT, T_RESERVDATE, T_OVERAMOUNT, T_OVERAMOUNTBD, T_OVERDATE,
                                    T_STATE, T_STATEDATE, T_LNKREF, T_BEGDATE
                                  ) VALUES 
                                  ( v_pmwrtbc.T_BCID, v_pmwrtbc.T_SUMID, v_pmwrtbc.T_ID_OPERATION, v_pmwrtbc.T_ID_STEP, v_pmwrtbc.T_ACTION, v_pmwrtbc.T_INSTANCE,
                                    v_pmwrtbc.T_CHANGEDATE, v_pmwrtbc.T_FIID, v_pmwrtbc.T_PORTFOLIO, v_pmwrtbc.T_GROUPID, v_pmwrtbc.T_DATE, v_pmwrtbc.T_TIME, v_pmwrtbc.T_AMOUNT, v_pmwrtbc.T_AMOUNTBD, v_pmwrtbc.T_SUM,
                                    v_pmwrtbc.T_CURRENCY, v_pmwrtbc.T_COST, v_pmwrtbc.T_BALANCECOST, v_pmwrtbc.T_BALANCECOSTBD, v_pmwrtbc.T_NKDAMOUNT, v_pmwrtbc.T_INTERESTINCOME,
                                    v_pmwrtbc.T_NOTCARRYINTEREST, v_pmwrtbc.T_INTERESTDATE, v_pmwrtbc.T_BEGDISCOUNT, v_pmwrtbc.T_DISCOUNTINCOME, v_pmwrtbc.T_NOTCARRYDISCOUNT,
                                    v_pmwrtbc.T_DISCOUNTDATE, v_pmwrtbc.T_OUTLAY, v_pmwrtbc.T_RESERVAMOUNT, v_pmwrtbc.T_RESERVDATE, v_pmwrtbc.T_OVERAMOUNT, v_pmwrtbc.T_OVERAMOUNTBD, v_pmwrtbc.T_OVERDATE,
                                    v_pmwrtbc.T_STATE, v_pmwrtbc.T_STATEDATE, v_pmwrtbc.T_LNKREF, v_pmwrtbc.T_BEGDATE );

       EXCEPTION
         WHEN OTHERS THEN NULL;       
       END;
    END LOOP;

    SCIDMAP.SetError('INFO','CopyLots','Обновлении T_PARENT и T_SOURCE в DPMWRTSUM_DBT');       

    FOR v_pmwrtsum IN c_pmwrtsum_new LOOP
       BEGIN
         IF( v_pmwrtsum.t_Parent > 0 ) THEN
            UPDATE DPMWRTSUM_DBT
               SET t_Parent = SCIDMAP.GetObjMappedID( 9, v_pmwrtsum.t_Parent, 0 )
             WHERE t_SumID = v_pmwrtsum.t_SumID;
         END IF;

         IF( v_pmwrtsum.t_Source > 0 ) THEN
            UPDATE DPMWRTSUM_DBT
               SET t_Source = SCIDMAP.GetObjMappedID( 9, v_pmwrtsum.t_Source, 0 )
             WHERE t_SumID = v_pmwrtsum.t_SumID;
         END IF;

       EXCEPTION
         WHEN OTHERS THEN SCIDMAP.SetError('ERROR','CopyLots','Ошибка при обновлении T_PARENT и T_SOURCE в DPMWRTSUM_DBT для лота '||v_pmwrtsum.t_SumID);       
       END;
    END LOOP;

  END;

  PROCEDURE ConvertMain
   IS
  BEGIN

    SCIDMAP.GlobalFun := 'CopyParty';

    CopyParty;

    SCIDMAP.GlobalFun := 'UpdateParty';

    UpdateParty;

    SCIDMAP.GlobalFun := 'CopyContr';

    CopyContr;

    SCIDMAP.GlobalFun := 'CopyCurrency';

    CopyCurrency;

    SCIDMAP.GlobalFun := 'CopyAvoiriss';

    CopyAvoiriss;

    SCIDMAP.GlobalFun := 'CopyDeal';

    CopyDeal;

    SCIDMAP.GlobalFun := 'CopyPaym';

    CopyPaym;

    SCIDMAP.GlobalFun := 'CopyLots';

    BEGIN
      EXECUTE IMMEDIATE('ALTER TRIGGER DPMWRTLNK_DBT_TBI DISABLE');
    END;

    CopyLots;

    BEGIN
      EXECUTE IMMEDIATE('ALTER TRIGGER DPMWRTLNK_DBT_TBI ENABLE');
    END;

    SCIDMAP.SetError('INFO','ConvertMain','Перенос данных завершен.');

  END;

END SC_CONVERT;
/
