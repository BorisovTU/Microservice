CREATE OR REPLACE PACKAGE BODY USR_PKG_IMPORT_SOFR
AS



 --добавим LOB запись для выгрузки в файл 0 - успешно, 1 - ошибка
 FUNCTION AddLOBToTMP( p_Mode IN INTEGER, p_IsDel IN INTEGER, p_FileName IN VARCHAR2 ) RETURN INTEGER
 IS
  v_state           INTEGER;
  v_Str_CLOB        CLOB;
 BEGIN

  BEGIN

   v_state := 0;

   IF p_Mode = 0 THEN

    IF p_IsDel = 1 THEN

     DELETE ulob_txt_tmp
     WHERE T_FILENAME = p_FileName || '.lim';

    END IF;


    --.lim
    FOR file_cursor IN ( SELECT T_LIMIT_TYPE, T_FIRM_ID, NULL AS T_SECCODE, T_TAG, T_CURR_CODE, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_LIMIT,
                          T_LEVERAGE, NULL AS T_TRDACCID, NULL AS T_WA_POSITION_PRICE, T_LIMIT_KIND    
                         FROM udl_lmtcashstock_exch_dbt
                         WHERE 1=1
                         UNION ALL
                         SELECT T_LIMIT_TYPE, T_FIRM_ID, T_SECCODE, NULL AS T_TAG, NULL AS T_CURR_CODE, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_LIMIT,
                          NULL AS  T_LEVERAGE, T_TRDACCID, T_WA_POSITION_PRICE, T_LIMIT_KIND
                         FROM udl_lmtsecuritest_exch_dbt
                         ORDER BY T_LIMIT_TYPE
    )
    LOOP

     IF v_state = 0 THEN
  
      INSERT INTO ulob_txt_tmp
      VALUES( p_FileName || '.lim', EMPTY_CLOB())
      RETURNING T_FILE INTO v_Str_CLOB;

      v_state := 1;

     END IF;

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB( (CASE WHEN file_cursor.T_LIMIT_TYPE IS NOT NULL THEN file_cursor.T_LIMIT_TYPE || ':' 
                                            ELSE NULL END) || 
                                           (CASE WHEN file_cursor.T_FIRM_ID IS NOT NULL THEN 'FIRM_ID=' || file_cursor.T_FIRM_ID || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_SECCODE IS NOT NULL THEN 'SECCODE=' || file_cursor.T_SECCODE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_TAG IS NOT NULL THEN 'TAG=' || file_cursor.T_TAG || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_CURR_CODE IS NOT NULL THEN 'CURR_CODE=' || file_cursor.T_CURR_CODE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_CLIENT_CODE IS NOT NULL THEN 'CLIENT_CODE=' || file_cursor.T_CLIENT_CODE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_CLIENT_CODE IS NOT NULL THEN 'CLIENT_CODE=' || file_cursor.T_CLIENT_CODE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_OPEN_BALANCE IS NOT NULL THEN 'OPEN_BALANCE=' || file_cursor.T_OPEN_BALANCE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_OPEN_LIMIT IS NOT NULL THEN 'OPEN_LIMIT=' || file_cursor.T_OPEN_LIMIT || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_CURRENT_LIMIT IS NOT NULL THEN 'CURRENT_LIMIT=' || file_cursor.T_CURRENT_LIMIT || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_LEVERAGE IS NOT NULL THEN 'LEVERAGE=' || file_cursor.T_LEVERAGE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_TRDACCID IS NOT NULL THEN 'TRDACCID=' || file_cursor.T_TRDACCID || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_WA_POSITION_PRICE IS NOT NULL THEN 'WA_POSITION_PRICE=' || file_cursor.T_WA_POSITION_PRICE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_LIMIT_KIND IS NOT NULL THEN 'LIMIT_KIND=' || file_cursor.T_LIMIT_KIND || ';' 
                                            ELSE NULL END) || CHR(10) ) );
    END LOOP;

    IF v_state = 1 THEN

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB(p_FileName || '.lim' || CHR(10))); --боремся с рудиментами

     UPDATE ulob_txt_tmp
     SET T_FILE = v_Str_CLOB
     WHERE T_FILENAME = p_FileName || '.lim';

     v_state := 0;

    END IF;



    --.fli
    IF p_IsDel = 1 THEN

     DELETE ulob_txt_tmp
     WHERE T_FILENAME = p_FileName || '.fli';

    END IF;

    FOR file_cursor IN ( SELECT T_CLASS_CODE, T_ACCOUNT, T_VOLUMEMN, T_VOLUMEPL, T_KFL, T_KGO, T_USE_KGO, T_FIRM_ID, T_SECCODE
                         FROM udl_lmtfuturmark_exch_dbt
                         WHERE 1=1
    )
    LOOP

     IF v_state = 0 THEN

      INSERT INTO ulob_txt_tmp
      VALUES( p_FileName || '.fli', EMPTY_CLOB())
      RETURNING T_FILE INTO v_Str_CLOB;

      v_state := 1;

     END IF;

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB( (CASE WHEN file_cursor.T_CLASS_CODE IS NOT NULL THEN 'CLASS_CODE=' || file_cursor.T_CLASS_CODE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_ACCOUNT IS NOT NULL THEN 'ACCOUNT=' || file_cursor.T_ACCOUNT || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_VOLUMEMN IS NOT NULL THEN 'VOLUMEMN=' || file_cursor.T_VOLUMEMN || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_VOLUMEPL IS NOT NULL THEN 'VOLUMEPL=' || file_cursor.T_VOLUMEPL || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_KFL IS NOT NULL THEN 'KFL=' || file_cursor.T_KFL || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_KGO IS NOT NULL THEN 'KGO=' || file_cursor.T_KGO || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_USE_KGO IS NOT NULL THEN 'USE_KGO=' || file_cursor.T_USE_KGO || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_FIRM_ID IS NOT NULL THEN 'FIRM_ID=' || file_cursor.T_FIRM_ID || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_SECCODE IS NOT NULL THEN 'SECCODE=' || file_cursor.T_SECCODE || ';' 
                                            ELSE NULL END) || CHR(10) ) );
    END LOOP;

    IF v_state = 1 THEN

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB(p_FileName || '.fli' || CHR(10))); --боремся с рудиментами

     UPDATE ulob_txt_tmp
     SET T_FILE = v_Str_CLOB
     WHERE T_FILENAME = p_FileName || '.fli';

     v_state := 0;

    END IF;

   ELSIF p_Mode = 1 THEN

    --.lci
    IF p_IsDel = 1 THEN

     DELETE ulob_txt_tmp
     WHERE T_FILENAME = p_FileName || '.lci';

    END IF;

    FOR file_cursor IN ( SELECT T_LIMIT_TYPE, T_LIMIT_ID, T_FIRM_ID, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_BALANCE, T_CURRENT_LIMIT, 
                          T_LIMIT_OPERATION, T_TRDACCID, T_SECCODE, T_TAG, T_CURR_CODE, T_LIMIT_KIND, T_LEVERAGE, T_WA_POSITION_PRICE
                         FROM udl_dl_lmtadjust_exch_dbt
                         WHERE 1=1
                         ORDER BY T_LIMIT_TYPE
    )
    LOOP

     IF v_state = 0 THEN

      INSERT INTO ulob_txt_tmp
      VALUES( p_FileName || '.lci', EMPTY_CLOB())
      RETURNING T_FILE INTO v_Str_CLOB;

      v_state := 1;

     END IF;

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB( (CASE WHEN file_cursor.T_LIMIT_TYPE IS NOT NULL THEN 'LIMIT_TYPE=' || file_cursor.T_LIMIT_TYPE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_LIMIT_ID IS NOT NULL THEN 'LIMIT_ID=' || file_cursor.T_LIMIT_ID || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_FIRM_ID IS NOT NULL THEN 'FIRM_ID=' || file_cursor.T_FIRM_ID || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_CLIENT_CODE IS NOT NULL THEN 'CLIENT_CODE=' || file_cursor.T_CLIENT_CODE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_OPEN_BALANCE IS NOT NULL THEN 'OPEN_BALANCE=' || file_cursor.T_OPEN_BALANCE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_OPEN_LIMIT IS NOT NULL THEN 'OPEN_LIMIT=' || file_cursor.T_OPEN_LIMIT || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_CURRENT_BALANCE IS NOT NULL THEN 'CURRENT_BALANCE=' || file_cursor.T_CURRENT_BALANCE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_CURRENT_LIMIT IS NOT NULL THEN 'CURRENT_LIMIT=' || file_cursor.T_CURRENT_LIMIT || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_LIMIT_OPERATION IS NOT NULL THEN 'LIMIT_OPERATION=' || file_cursor.T_LIMIT_OPERATION || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_TRDACCID IS NOT NULL THEN 'TRDACCID=' || file_cursor.T_TRDACCID || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_SECCODE IS NOT NULL THEN 'SECCODE=' || file_cursor.T_SECCODE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_TAG IS NOT NULL THEN 'TAG=' || file_cursor.T_TAG || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_CURR_CODE IS NOT NULL THEN 'CURR_CODE=' || file_cursor.T_CURR_CODE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_LIMIT_KIND IS NOT NULL THEN 'LIMIT_KIND=' || file_cursor.T_LIMIT_KIND || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_LEVERAGE IS NOT NULL THEN 'LEVERAGE=' || file_cursor.T_LEVERAGE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_WA_POSITION_PRICE IS NOT NULL THEN 'WA_POSITION_PRICE=' || file_cursor.T_WA_POSITION_PRICE || ';' 
                                            ELSE NULL END) || CHR(10) ) );
    END LOOP;

    IF v_state = 1 THEN

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB(p_FileName || '.lci' || CHR(10))); --боремся с рудиментами

     UPDATE ulob_txt_tmp
     SET T_FILE = v_Str_CLOB
     WHERE T_FILENAME = p_FileName || '.lci';

     v_state := 0;

    END IF;

/*!!!!!!!!!!!!!!пока не используем до выяснения - это входящий файл
    --.lco       !!!!!!!!!!!!!!!!!!!!!!этот файл не понятно откуда выгружать и что это
    IF p_IsDel = 1 THEN

     DELETE ulob_txt_tmp
     WHERE T_FILENAME = p_FileName || '.lco';

    END IF;

    FOR file_cursor IN ( SELECT T_CLASS_CODE, T_ACCOUNT, T_VOLUMEMN, T_VOLUMEPL, T_KFL, T_KGO, T_USE_KGO, T_FIRM_ID, T_SECCODE
                         FROM udl_lmtfuturmark_exch_dbt
                         WHERE 1=1
    )
    LOOP

     IF v_state = 0 THEN

      INSERT INTO ulob_txt_tmp
      VALUES( p_FileName || '.lco', EMPTY_CLOB())
      RETURNING T_FILE INTO v_Str_CLOB;

      v_state := 1;

     END IF;

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB( (CASE WHEN file_cursor.T_CLASS_CODE IS NOT NULL THEN 'CLASS_CODE=' || file_cursor.T_CLASS_CODE || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_ACCOUNT IS NOT NULL THEN 'ACCOUNT=' || file_cursor.T_ACCOUNT || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_VOLUMEMN IS NOT NULL THEN 'VOLUMEMN=' || file_cursor.T_VOLUMEMN || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_VOLUMEPL IS NOT NULL THEN 'VOLUMEPL=' || file_cursor.T_VOLUMEPL || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_KFL IS NOT NULL THEN 'KFL=' || file_cursor.T_KFL || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_KGO IS NOT NULL THEN 'KGO=' || file_cursor.T_KGO || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_USE_KGO IS NOT NULL THEN 'USE_KGO=' || file_cursor.T_USE_KGO || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_FIRM_ID IS NOT NULL THEN 'FIRM_ID=' || file_cursor.T_FIRM_ID || ';' 
                                            ELSE NULL END) ||
                                           (CASE WHEN file_cursor.T_SECCODE IS NOT NULL THEN 'SECCODE=' || file_cursor.T_SECCODE || ';' 
                                            ELSE NULL END) || CHR(10) ) );
    END LOOP;

    IF v_state = 1 THEN

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB(p_FileName || '.lco' || CHR(10))); --боремся с рудиментами

     UPDATE ulob_txt_tmp
     SET T_FILE = v_Str_CLOB
     WHERE T_FILENAME = p_FileName || '.lco';

     v_state := 0;

    END IF;
*/
   END IF;

   RETURN v_state;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;



 --добавим записи в буферную таблицу udl_lmtcashstock_exch_dbt 0 - успешно, 1 - ошибка
 FUNCTION AddInudl_lmtcashstock_exch RETURN INTEGER
 IS
  v_state           INTEGER;
 BEGIN

  BEGIN

   v_state := 0;

   DELETE udl_lmtcashstock_exch_dbt;

   INSERT INTO udl_lmtcashstock_exch_dbt( T_LIMIT_TYPE, T_FIRM_ID, T_TAG, T_CURR_CODE, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_LIMIT,
     T_LEVERAGE, T_LIMIT_KIND )
    SELECT 'MONEY',T_FIRM_ID, T_TAG, T_CURR_CODE, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_LIMIT, T_LEVERAGE, T_LIMIT_KIND
    FROM DDL_LIMITCASHSTOCK_DBT
    WHERE 1 = 1; -- могут добавиться условия!!!!!!!


   RETURN v_state;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;



 --добавим записи в буферную таблицу udl_lmtsecuritest_exch_dbt 0 - успешно, 1 - ошибка
 FUNCTION AddInudl_lmtsecuritest_exch RETURN INTEGER
 IS
  v_state           INTEGER;
 BEGIN

  BEGIN

   v_state := 0;

   DELETE udl_lmtsecuritest_exch_dbt;

   INSERT INTO udl_lmtsecuritest_exch_dbt( T_LIMIT_TYPE, T_FIRM_ID, T_SECCODE, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_LIMIT,
     T_TRDACCID, T_WA_POSITION_PRICE, T_LIMIT_KIND )
    SELECT 'DEPO',T_FIRM_ID, T_SECCODE, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_LIMIT, T_TRDACCID, T_WA_POSITION_PRICE, T_LIMIT_KIND
    FROM DDL_LIMITSECURITES_DBT
    WHERE 1 = 1; -- могут добавиться условия!!!!!!!


   RETURN v_state;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;



 --добавим записи в буферную таблицу udl_lmtfuturmark_exch_dbt 0 - успешно, 1 - ошибка
 FUNCTION AddInudl_lmtfuturmark_exch RETURN INTEGER
 IS
  v_state           INTEGER;
 BEGIN

  BEGIN

   v_state := 0;

   DELETE udl_lmtfuturmark_exch_dbt;

   INSERT INTO udl_lmtfuturmark_exch_dbt( T_CLASS_CODE, T_ACCOUNT, T_VOLUMEMN, T_VOLUMEPL, T_KFL, T_KGO, T_USE_KGO, T_FIRM_ID, T_SECCODE )
    SELECT T_CLASS_CODE, T_ACCOUNT, T_VOLUMEMN, T_VOLUMEPL, T_KFL, T_KGO, T_USE_KGO, T_FIRM_ID, T_SECCODE
    FROM DDL_LIMITFUTURMARK_DBT
    WHERE 1 = 1; -- могут добавиться условия!!!!!!!


   RETURN v_state;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;



 --добавим записи в буферную таблицу udl_dl_lmtadjust_exch_dbt 0 - успешно, 1 - ошибка
 FUNCTION AddInudl_dl_lmtadjust_exch RETURN INTEGER
 IS
  v_state           INTEGER;
 BEGIN

  BEGIN

   v_state := 0;

   DELETE udl_dl_lmtadjust_exch_dbt;

   INSERT INTO udl_dl_lmtadjust_exch_dbt( T_LIMIT_TYPE, T_LIMIT_ID, T_FIRM_ID, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, /*T_CURRENT_BALANCE,*/ 
    T_CURRENT_LIMIT, T_LIMIT_OPERATION, T_TRDACCID, T_SECCODE, T_TAG, T_CURR_CODE, T_LIMIT_KIND, T_LEVERAGE /*, T_WA_POSITION_PRICE*/ )
     SELECT T_LIMIT_TYPE, T_LIMITID, T_FIRM_ID, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, /*,???!!!Соответствие отсутствует*/
      T_CURRENT_LIMIT, T_LIMIT_OPERATION, T_TRDACCID, T_SECCODE, T_TAG, T_CURR_CODE, T_LIMIT_KIND, T_LEVERAGE /*,???!!!Соответствие отсутствует*/
     FROM DDL_LIMITADJUST_DBT
     WHERE 1 = 1; -- могут добавиться условия!!!!!!!
     
      
   RETURN v_state;
   
  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;





 --добавим СПИ субъекта 0 - успешно, 1 - ошибка
 FUNCTION AddSfSiForParty( p_PartyId IN NUMBER, p_ServiceKind IN NUMBER, p_KindOper IN NUMBER, p_FiKind IN NUMBER, p_FiCode IN VARCHAR,
  p_Account IN VARCHAR, p_BankId IN NUMBER, p_BankDate IN DATE ) RETURN INTEGER
 IS
  v_CodeKindBank           dobjcode_dbt.T_CODEKIND%TYPE;
  v_CodeKindClient         dobjcode_dbt.T_CODEKIND%TYPE;
  v_ObjectType             dobjcode_dbt.T_OBJECTTYPE%TYPE;
  v_SetAccId               dsettacc_dbt.T_SETTACCID%TYPE;
 BEGIN

  BEGIN

   v_CodeKindBank := 3;
   v_CodeKindClient := 1;
   v_ObjectType := 3;

   INSERT INTO dsettacc_dbt( T_PARTYID, T_BANKID, T_FIID, T_CHAPTER, T_ACCOUNT, T_RECNAME, T_BANKCODEKIND, T_BANKCODE, T_BANKNAME, T_BANKCORRID,
    T_BANKCORRCODEKIND, T_BANKCORRCODE, T_BANKCORRNAME, T_FIKIND, T_BENEFICIARYID, T_CODEKIND, T_CODE, T_ORDER, T_SHORTNAME )
   VALUES( p_PartyId, p_BankId,
     ( SELECT T_FIID FROM dfininstr_dbt
       WHERE T_FI_KIND = p_FiKind
        AND T_FI_CODE = p_FiCode ),
     ( SELECT T_CHAPTER FROM dbalance_dbt B
       WHERE T_INUMPLAN = 0
        AND T_BALANCE = SUBSTR( p_Account, 1, 5 ) ),
     p_Account,
     ( SELECT T_NAME FROM dparty_dbt
       WHERE T_PARTYID = p_PartyId ),
     v_CodeKindBank,
     ( SELECT T_CODE FROM dobjcode_dbt
       WHERE  T_OBJECTTYPE = v_ObjectType
        AND T_CODEKIND = v_CodeKindBank
        AND T_OBJECTID = p_BankId
        AND T_STATE = 0
        AND T_BANKDATE <= p_BankDate ),
     ( SELECT T_NAME FROM dparty_dbt
       WHERE T_PARTYID = p_BankId ),
     ( SELECT T_OBJECTID FROM dobjcode_dbt
       WHERE T_OBJECTTYPE = v_ObjectType
        AND T_CODEKIND = v_CodeKindBank
        AND T_CODE = ( SELECT T_BIC_RCC FROM dbankdprt_dbt
                       WHERE T_PARTYID = p_BankId )
        AND T_STATE = 0 ),
     v_CodeKindBank,
     ( SELECT T_BIC_RCC FROM dbankdprt_dbt
       WHERE T_PARTYID = p_BankId ),
     ( SELECT A.T_NAME FROM dparty_dbt A
       WHERE A.T_PARTYID = ( SELECT B.T_OBJECTID FROM dobjcode_dbt B
                             WHERE B.T_OBJECTTYPE = v_ObjectType
                              AND B.T_CODEKIND = v_CodeKindBank
                              AND B.T_CODE = ( SELECT C.T_BIC_RCC FROM dbankdprt_dbt C
                                             WHERE C.T_PARTYID = p_BankId )
                              AND B.T_STATE = 0 ) ),
     p_FiKind, p_PartyId, v_CodeKindClient,
     ( SELECT T_CODE FROM dobjcode_dbt
       WHERE  T_OBJECTTYPE = v_ObjectType
        AND T_CODEKIND = v_CodeKindClient
        AND T_OBJECTID = p_PartyId
        AND T_STATE = 0
        AND T_BANKDATE <= p_BankDate ),
     '1', 'СПИ операции ' || TO_CHAR( p_KindOper ) )
    RETURNING T_SETTACCID INTO v_SetAccId;
    
   INSERT INTO dpmautoac_dbt( T_PARTYID, T_FIID, T_KINDOPER, T_PURPOSE, T_SETTACCID, T_FIKIND, T_SERVICEKIND, T_ORDER, T_ACCOUNT )
    SELECT p_PartyId,
     ( SELECT T_FIID FROM dfininstr_dbt
       WHERE T_FI_KIND = p_FiKind
        AND T_FI_CODE = p_FiCode ),
     p_KindOper, 0, v_SetAccId, p_FiKind, p_ServiceKind, '1', CHR(1)
    FROM DUAL;

   RETURN 0;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    IF v_SetAccId > 0 THEN

     ROLLBACK;

    END IF;

    RETURN 1;

   END;
  END;


 END;



 --получим счет из СПИ субъекта и PartyId банка из СПИ субъекта 0 - успешно, 1 - ошибка
 FUNCTION GetSfSiAccountAndBankPartyId( p_PartyId IN NUMBER, p_ServiceKind IN NUMBER, p_KindOper IN NUMBER, p_FiKind IN NUMBER, p_FiCode IN VARCHAR, p_Account IN VARCHAR, p_AccountResult OUT VARCHAR,
    p_BankId OUT NUMBER ) RETURN INTEGER
 IS
  v_PartyId          dparty_dbt.T_PARTYID%TYPE;
 BEGIN

  BEGIN

   SELECT T_ACCOUNT, T_BANKID INTO p_AccountResult, p_BankId FROM dsettacc_dbt
   WHERE T_SETTACCID IN( SELECT A.T_SETTACCID FROM
                          (SELECT T_SETTACCID FROM dpmautoac_dbt
                          WHERE T_PARTYID = p_PartyId
                           AND T_SERVICEKIND = p_ServiceKind
                           AND T_KINDOPER = p_KindOper
                           AND T_FIID = (SELECT T_FIID FROM dfininstr_dbt B
                                         WHERE B.T_FI_KIND = p_FiKind
                                          AND T_FI_CODE = p_FiCode)
                           ORDER BY  T_ORDER) A)
    AND T_ACCOUNT = p_Account;

   RETURN 0;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    p_AccountResult := '';
    p_BankId := -1;
    RETURN 1;

   END;
  END;


 END;


 --получим Id субъект и счет ДО по номеру открытого ДО 0 - успешно, 1 - ошибка
 FUNCTION GetPropFromContrNum( p_LegalForm IN NUMBER, p_ContrNumber IN VARCHAR, p_ObjectType IN NUMBER, p_Account IN VARCHAR, p_ObjType IN NUMBER,
  p_FiKind IN NUMBER, p_FiCode IN VARCHAR, p_PartyId OUT NUMBER, p_ContrId OUT NUMBER, p_AccountContr OUT VARCHAR, p_ContrAccountId OUT NUMBER  ) RETURN INTEGER
 IS
  v_PartyId          dparty_dbt.T_PARTYID%TYPE;
 BEGIN

  BEGIN

   IF p_LegalForm = 1 THEN  --для юр.лиц

/*

    SELECT T_PARTYID, T_OBJECT, T_ID INTO p_PartyId, p_AccountContr, p_ContrId FROM dsfcontr_dbt A, ddlcontr_dbt B
    WHERE A.T_NUMBER = p_ContrNumber
     AND A.T_OBJECTTYPE = p_ObjectType
     AND A.T_DATECLOSE = TO_DATE('01010001','ddmmyyyy')
     AND B.T_SFCONTRID = A.T_ID
     AND ROWNUM = 1;
*/

    SELECT A.T_PARTYID, A.T_OBJECT, A.T_ID, A.T_ACCOUNTID INTO p_PartyId, p_AccountContr, p_ContrId, p_ContrAccountId
    FROM ( SELECT A.T_PARTYID, A.T_OBJECT, D.T_ID, F.T_ACCOUNTID
           FROM dsfcontr_dbt A, ddlcontr_dbt B, ddlcontrmp_dbt C, dsfcontr_dbt D, dsettacc_dbt E, daccount_dbt F, dsfssi_dbt G 
           WHERE A.T_NUMBER = p_ContrNumber
            AND A.T_OBJECTTYPE = p_ObjectType
            AND A.T_DATECLOSE = TO_DATE('01010001','ddmmyyyy')
            AND B.T_SFCONTRID = A.T_ID
            AND C.T_DLCONTRID = B.T_DLCONTRID
            AND D.T_ID = C.T_SFCONTRID
            AND E.T_ACCOUNT = p_Account
            AND E.T_SETTACCID = G.T_SETACCID
            AND G.T_OBJECTID = LPAD( D.T_ID, 10, '0')
            AND G.T_OBJECTTYPE = p_ObjType
            AND G.T_FIKIND = p_FiKind
            AND G.T_FIID = (SELECT H.T_FIID FROM dfininstr_dbt H    
                                     WHERE H.T_FI_KIND = G.T_FIKIND         
                                      AND H.T_FI_CODE = p_FiCode)             
            AND F.T_ACCOUNT = E.T_ACCOUNT                                                
            AND F.T_CHAPTER = E.T_CHAPTER                                                
            AND F.T_CODE_CURRENCY = E.T_FIID
            ORDER BY E.T_ORDER ) A
    WHERE ROWNUM = 1; 

   ELSE --для физ.лиц

    SELECT A.T_PARTYID, A.T_OBJECT, D.T_ID, F.T_ACCOUNTID INTO p_PartyId, p_AccountContr, p_ContrId, p_ContrAccountId
    FROM dbrokacc_dbt H, dsfcontr_dbt A, ddlcontr_dbt B, ddlcontrmp_dbt C, dsfcontr_dbt D, dsfssi_dbt G, dsettacc_dbt E, daccount_dbt F  
    WHERE H.T_ACCOUNT = p_Account
     AND A.T_NUMBER = p_ContrNumber
     AND A.T_OBJECTTYPE = p_ObjectType
     AND A.T_DATECLOSE = TO_DATE('01010001','ddmmyyyy')
     AND B.T_SFCONTRID = A.T_ID
     AND C.T_DLCONTRID = B.T_DLCONTRID
     AND D.T_ID = C.T_SFCONTRID
     AND D.T_SERVKIND = H.T_SERVKIND
     AND D.T_SERVKINDSUB = H.T_SERVKINDSUB
     AND G.T_OBJECTID = LPAD( D.T_ID, 10, '0')
     AND G.T_OBJECTTYPE = p_ObjType
     AND G.T_FIKIND = p_FiKind
     AND G.T_FIID = H.T_CURRENCY
     AND E.T_SETTACCID = G.T_SETACCID
     AND F.T_ACCOUNT = E.T_ACCOUNT                                                
     AND F.T_CHAPTER = E.T_CHAPTER                                                
     AND F.T_CODE_CURRENCY = E.T_FIID;

   END IF;
 
   RETURN 0;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;



 --вставка CLOB-XML в лог 0 - успешно, 1 - ошибка
 FUNCTION AddRecXMLToLOG( p_Cnum IN NUMBER ) RETURN INTEGER
 IS
  v_Cnt          INTEGER;

 BEGIN

  BEGIN

   v_Cnt := 0;

   SELECT COUNT(1) INTO v_Cnt FROM uclientRegMB_Log_dbt A
   WHERE A.T_SESSIONID = p_Cnum
    AND A.T_FILENAME = ( SELECT B.T_FILENAME
                         FROM uclientRegMB_LogTmp_dbt B
                         WHERE B.T_SESSIONID = p_Cnum
                          AND ROWNUM = 1 );

   IF v_Cnt > 0 THEN

    DELETE uclientRegMB_Log_dbt A
    WHERE A.T_SESSIONID = p_Cnum
     AND A.T_FILENAME = ( SELECT B.T_FILENAME
                          FROM uclientRegMB_LogTmp_dbt B
                          WHERE B.T_SESSIONID = p_Cnum
                           AND ROWNUM = 1 );

   END IF;


   INSERT INTO uclientRegMB_Log_dbt( T_SESSIONID, T_FILENAME, T_STATUS, T_OPER, T_XML_MESS)
    (SELECT p_Cnum AS T_SESSIONID, T_FILENAME, 0 AS T_STATUS,  T_OPER, T_XML_MESS
     FROM uclientRegMB_LogTmp_dbt
     WHERE T_SESSIONID = p_Cnum
      AND ROWNUM = 1);

   DELETE uclientRegMB_LogTmp_dbt WHERE T_SESSIONID = p_Cnum;

   RETURN 0;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;



 --обработать загруженный CLOB-XML регистрация клиентов на МБ 0 - успешно, 1 - ошибка
 FUNCTION ProcwssObjAttrib(p_ObjectType IN NUMBER, p_CodeKind IN NUMBER, p_GroupId IN NUMBER, p_SessionId IN NUMBER,
  p_FileName IN VARCHAR, p_Oper IN NUMBER) RETURN INTEGER
 IS

  v_Cnt          INTEGER;
  v_CntValid     INTEGER;
  v_RecID        uclientRegMB_Log_dbt.T_RECID%TYPE;
  v_Status       uclientRegMB_Log_dbt.T_STATUS%TYPE;
  v_ErrCode      uclientRegMB_Log_dbt.T_ERRCODE%TYPE;
  v_ErrText      uclientRegMB_Log_dbt.T_ERRTEXT%TYPE;

 BEGIN

  BEGIN

   v_Cnt := 0;
   v_CntValid := 0;
   v_RecID := -1;
   v_Status := 0;
   v_ErrCode := 0;
   v_ErrText := '';

   FOR clnt_regMB_XML_rec IN (
    SELECT A.CLIENT_CODE, A.MARKETID, A.RESCODE, A.RESMESSAGE, 
     (SELECT B.T_OBJECTID FROM dobjcode_dbt B
      WHERE B.T_OBJECTTYPE  = p_ObjectType
       AND B.T_CODEKIND = p_CodeKind
       AND B.T_CODE = A.CLIENT_CODE 
       AND T_STATE = 0
       AND  ROWNUM = 1 ) AS PARTYID,
     A.T_RECID
    FROM
     (WITH XML_MESS AS ( SELECT T_RECID, XMLTYPE(T_XML_MESS) AS MESS 
                        FROM  uclientRegMB_Log_dbt
                        WHERE T_SESSIONID = p_SessionId
                         AND T_FILENAME = p_FileName
                         AND T_STATUS = 0)
      SELECT MEMBER.COLUMN_VALUE.EXTRACT ('//CLIENT_CODE/CLIENT_MARKETS/@ClientCode').getStringVal() AS CLIENT_CODE,
       MEMBER.COLUMN_VALUE.EXTRACT ('//CLIENT_CODE/CLIENT_MARKETS/@MarketId').getStringVal() AS MARKETID,
       MEMBER.COLUMN_VALUE.EXTRACT ('//CLIENT_CODE/CLIENT_MARKETS/@ResCode').getStringVal() AS RESCODE,
       MEMBER.COLUMN_VALUE.EXTRACT ('//CLIENT_CODE/CLIENT_MARKETS/@ResMessage').getStringVal() AS RESMESSAGE,
       T_RECID
      FROM XML_MESS XML_MESS_TBL, TABLE( XMLSEQUENCE( XML_MESS_TBL.MESS.EXTRACT( 'MICEX_DOC/CLIENTS/CLIENT' ) ) ) MEMBER) A )
   LOOP

    BEGIN
     v_RecID := clnt_regMB_XML_rec.T_RECID;

     IF clnt_regMB_XML_rec.PARTYID IS NOT NULL THEN
   
      RSI_RSB_CATEGORY.SetPartyAtCor( clnt_regMB_XML_rec.PARTYID, p_GroupId, ( CASE WHEN clnt_regMB_XML_rec.RESCODE = '0' THEN 1 ELSE 2 END ), p_Oper );

      IF clnt_regMB_XML_rec.RESCODE <> '0' THEN
       --субъект не зарегистрирован с clnt_regMB_XML_rec.CLIENT_CODE в ErrText код возврата -3
       --накапливаем данные, по которым не установлена категория, сваливаем все это в ErrText
 
       v_ErrText := v_ErrText || '/n' || 'Файл ' || p_FileName || ' Отрицательный результат регистрации субъекта с кодом ' ||
        clnt_regMB_XML_rec.CLIENT_CODE || ', Id ' || clnt_regMB_XML_rec.PARTYID;
       v_ErrCode := -3; --отрицательный результат регистрации субъекта

      END IF;

      v_CntValid := v_CntValid + 1;

     ELSE
      --не найден субъект с clnt_regMB_XML_rec.CLIENT_CODE в ErrText код возврата -1
      --накапливаем данные, по которым не установлена категория, сваливаем все это в ErrText
      v_ErrText := v_ErrText || '/n' || 'Файл ' || p_FileName || ' Не найден субъект с кодом ' || clnt_regMB_XML_rec.CLIENT_CODE;
      v_ErrCode := -1; --не все записи файла обработаны
      
     END IF;


    EXCEPTION
     WHEN OTHERS THEN
     BEGIN
      --не установлена категория субъекта с кодом clnt_regMB_XML_rec.CLIENT_CODE, clnt_regMB_XML_rec.PARTYID в ErrText код возврата -1
      --накапливаем данные, по которым не установлена категория, сваливаем все это в ErrText
      v_ErrText := v_ErrText || '/n' || 'Файл ' || p_FileName || ' Не установлена категория субъекта с кодом ' || clnt_regMB_XML_rec.CLIENT_CODE ||
         ', Id ' || clnt_regMB_XML_rec.PARTYID;
      v_ErrCode := -1; --не все записи файла обработаны

     END;
    END;

    v_Cnt := v_Cnt + 1;

   END LOOP;

   v_Status := 1;


   IF v_Cnt = 0 THEN

    v_ErrText := v_ErrText || '/n' || 'Файл ' || p_FileName || ' Не обработано ни одной записи';
    v_ErrCode := -2; --записи файла не обработаны
    v_Status := 2;

   ELSE

    v_ErrText := v_ErrText || '/n' || 'Файл ' || p_FileName || ' Записей ' || TO_CHAR(v_Cnt) || ' Обработано записей успешно ' || TO_CHAR(v_CntValid) ||
     ' Обработано записей с ошибкой ' || TO_CHAR(v_Cnt - v_CntValid);

   END IF;


   --пишем ErrText!!!!!!!!!!!!
   UPDATE uclientRegMB_Log_dbt
   SET T_ERRTEXT = v_ErrText, T_STATUS = v_Status, T_ERRCODE = v_ErrCode
   WHERE T_RECID = v_RecID;


   RETURN 0;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    UPDATE uclientRegMB_Log_dbt
    SET T_ERRTEXT = v_ErrText, T_STATUS = 2, T_ERRCODE = -1000
    WHERE T_RECID = v_RecID;

    RETURN 1;

   END;
  END;


 END;


END USR_PKG_IMPORT_SOFR;
