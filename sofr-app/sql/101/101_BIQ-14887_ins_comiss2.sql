DECLARE
  v_OldTarSclID NUMBER(10);
  v_OldCommNum  NUMBER(5) := 0;
  v_OldCommCode VARCHAR2(30) := '╚этхёЄ╤ютхЄэшъ';
BEGIN
  BEGIN
    SELECT t_Number INTO v_OldCommNum FROM dsfcomiss_dbt WHERE t_Code like (v_OldCommCode);
  EXCEPTION
    WHEN NO_DATA_FOUND
    THEN v_OldCommNum := 0;
  END;
  
  IF v_OldCommNum > 0 THEN
    DELETE FROM DSFTARIF_DBT WHERE T_TARSCLID IN (SELECT T_ID FROM DSFTARSCL_DBT WHERE t_FeeType = 1 AND t_CommNumber = v_OldCommNum); 

    DELETE FROM DSFTARSCL_DBT WHERE T_FEETYPE = 1 AND T_COMMNUMBER = v_OldCommNum;

    DELETE FROM DSFCALCAL_DBT WHERE T_FEETYPE = 1 AND T_COMMNUMBER = v_OldCommNum;

    DELETE FROM DSFCOMISS_DBT WHERE T_FEETYPE = 1 AND T_NUMBER = v_OldCommNum;
   
    COMMIT;
  END IF;
END;
/

DECLARE
  v_TarSclID NUMBER(10);
  v_RecCount NUMBER(5) := 0;
  v_CommNum  NUMBER(5) := 0;
  v_CommCode VARCHAR2(30) := 'ИнвестСоветник';
  v_NewBlob  BLOB;
  v_AlgSize  NUMBER(5) := 81;
  v_AlgName  VARCHAR2(81) := 'Инвест';
  v_AlgMacro VARCHAR2(81) := 'Invest_com.mac';
  v_AlgRetType NUMBER := 1;
BEGIN
  SELECT count(1) INTO v_RecCount FROM dsfcomiss_dbt WHERE t_Code like (v_CommCode);
  IF v_RecCount = 0 THEN
    SELECT max(t_Number)+1 into v_CommNum from dsfcomiss_dbt;

    INSERT INTO DSFCOMISS_DBT (T_FEETYPE,
                               T_NUMBER,
                               T_CODE,
                               T_NAME,
                               T_CALCPERIODTYPE,
                               T_CALCPERIODNUM,
                               T_DATE,
                               T_PAYNDS,
                               T_FIID_COMM,
                               T_GETSUMMIN,
                               T_SUMMIN,
                               T_SUMMAX,
                               T_RATETYPE,
                               T_RECEIVERID,
                               T_INCFEETYPE,
                               T_INCCOMMNUMBER,
                               T_FORMALG,
                               T_SERVICEKIND,
                               T_SERVICESUBKIND,
                               T_CALCCOMISSSUMALG,
                               T_SETACCSEARCHALG,
                               T_FIID_PAYSUM,
                               T_DATEBEGIN,
                               T_DATEEND,
                               T_INSTANTPAYMENT,
                               T_PRODUCTID,
                               T_NDSCATEG,
                               T_ISFREEPERIOD,
                               T_COMMENT,
                               T_COMISSID,
                               T_PARENTCOMISSID,
                               T_ISBANKEXPENSES,
                               T_ISCOMPENSATIONCOM)
         VALUES (1,
                 v_CommNum,
                 v_CommCode,
                 'Комиссия за Инвест консультирование',
                 2,
                 1,
                 TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
                 1,
                 0,
                 CHR(0),
                 0,
                 0,
                 0,
                 1,
                 0,
                 0,
                 1,
                 1,
                 8,
                 1,
                 1,
                 0,
                 TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
                 TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
                 'X',
                 0,
                 0,
                 'X',
                 'Комиссия за Инвест консультирование',
                 0,
                 0,
                 CHR(0),
                 CHR(0));

    v_NewBlob := empty_blob();

    v_NewBlob := utl_raw.concat(utl_raw.cast_to_raw(v_AlgName), utl_raw.copies(utl_raw.cast_to_raw(chr(0)), v_AlgSize-length(v_AlgName)));

    dbms_lob.Append(v_NewBlob, utl_raw.concat(utl_raw.cast_to_raw(v_AlgMacro),utl_raw.copies(utl_raw.cast_to_raw(chr(0)), v_AlgSize-length(v_AlgMacro))));

    dbms_lob.Append(v_NewBlob, dbms_lob.substr(utl_raw.cast_from_binary_integer(v_AlgRetType, utl_raw.little_endian), 2, 1));

    INSERT INTO DSFCALCAL_DBT (T_FEETYPE,
                               T_COMMNUMBER,
                               T_KIND,
                               T_NUMBER,
                               T_FILTERTYPE,
                               T_FILTERMACRO,
                               T_SCALETYPE,
                               T_SCALEMACRO,
                               T_SCALEMACRORET,
                               T_CALCMETHOD,
                               T_FIID_TARSCL,
                               T_SUMMIN,
                               T_SUMMAX,
                               T_DESCRIPTION,
                               T_BINDTOOBJECTS,
                               T_ISALLOCATE,
                               T_ID,
                               T_ISBATCHMODE,
                               T_CONCOMID,
                               T_FMTBLOBDATA_XXXX)
         VALUES (1,
                 v_CommNum,
                 8,
                 3,
                 2,
                 CHR(1),
                 1,
                 CHR(1),
                 0,
                 0,
                 0,
                 0,
                 0,
                 CHR(1),
                 CHR(0),
                 CHR(0),
                 0,
                 'X',
                 0,
                 v_NewBlob);

    INSERT INTO DSFTARSCL_DBT (T_FEETYPE,
                               T_COMMNUMBER,
                               T_ALGKIND,
                               T_ALGNUMBER,
                               T_BEGINDATE,
                               T_ISBLOCKED,
                               T_ID,
                               T_ENDDATE,
                               T_CONCOMID)
         VALUES (1,
                 v_CommNum,
                 8,
                 3,
                 TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
                 CHR(0),
                 0,
                 TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
                 0);

    BEGIN
      SELECT t_id
        INTO v_TarSclID
        FROM DSFTARSCL_DBT
       WHERE t_FeeType = 1 AND t_CommNumber = v_CommNum;
    EXCEPTION
      WHEN NO_DATA_FOUND
      THEN v_TarSclID := 0;
    END;

    IF v_TarSclID > 0 THEN
      INSERT INTO DSFTARIF_DBT (T_ID,
                                T_TARSCLID,
                                T_SIGN,
                                T_BASETYPE,
                                T_BASESUM,
                                T_TARIFTYPE,
                                T_TARIFSUM,
                                T_MINVALUE,
                                T_MAXVALUE,
                                T_SORT)
           VALUES (0,
                   v_TarSclID,
                   2,
                   1,
                   0,
                   2,
                   10000,
                   0,
                   0,
                   5);                      
    ELSE
      ROLLBACK;
    END IF;
   
    COMMIT;
  END IF;
END;
/