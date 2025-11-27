--„®΅ Ά«¥­¨¥ ­ αβΰ®©ª¨ ¤«ο ―®«μ§®Ά β¥«μαª¨ε ¤¥©αβΆ¨© ―ΰ¨ ΰ ΅®β¥ α ª β¥£®ΰ¨ο¬¨ ®΅κ¥ªβ®Ά

DECLARE
    /*v_ParentID NUMBER(10);
    v_ProcFnsKey number := 0;
    v_ProcKey number := 0;*/
    v_KeyID number := 0;
    
    function GetKeyID(pPath in varchar2) return varchar2
    AS
      m_KeyID number := 0;
    BEGIN
        SELECT P.T_KEYID into m_KeyID
        FROM 
        (
          SELECT 
            T_KEYID, UPPER(SUBSTR(SYS_CONNECT_BY_PATH( T_NAME, '\'), 2)) PATH
          FROM DREGPARM_DBT
          START WITH T_PARENTID = 0
          CONNECT BY NOCYCLE T_PARENTID = PRIOR T_KEYID
        ) P
        WHERE P.PATH IN (pPath);

        return m_KeyID;
    exception when no_data_found then
      return 0;
    end;

    function AddRegKey(pParent in number, pName in varchar2, pType in number, pGlobal in char, pDesc in varchar2, pSec in char, pBranch in char)
    return number
    as
      v_KeyID number := 0;
    begin
      INSERT INTO dregparm_dbt(T_KEYID, T_PARENTID, T_NAME, T_TYPE, T_GLOBAL, T_DESCRIPTION, T_SECURITY, T_ISBRANCH)
      VALUES(0, pParent, pName, pType, pGlobal, pDesc, pSec, pBranch)
      RETURNING T_KEYID INTO v_KeyID;

      return v_KeyID;
    end;

    function CreatePath(pPath in varchar2, pType in number, pGlobal in char, pDesc in varchar2, pSec in char, pBranch in char) return number
    as
      m_MaxLevel number := 0;
      m_TmpKeyID number := 0;
      m_ParentKey number := 0;
    begin
      SELECT
        max(level) into m_MaxLevel
      FROM dual
      CONNECT BY REGEXP_SUBSTR(pPath, '[^\]+', 1, level) IS NOT NULL;

      for rec in 
      (
        SELECT
        REGEXP_SUBSTR(pPath, '[^\]+', 1, level) AS t_keyname,
        ltrim(SYS_CONNECT_BY_PATH(REGEXP_SUBSTR(pPath, '[^\]+', 1, level), '\'), '\') t_path,
        level t_level
        FROM dual
        CONNECT BY REGEXP_SUBSTR(pPath, '[^\]+', 1, level) IS NOT NULL
      )
      loop
        m_TmpKeyID := GetKeyID(rec.t_path);

        if m_TmpKeyID = 0 then
          if rec.t_level = m_MaxLevel then
            m_TmpKeyID := AddRegKey(m_ParentKey, rec.t_keyname, pType, pGlobal, pDesc, pSec, pBranch);
          else
            m_TmpKeyID := AddRegKey(m_ParentKey, rec.t_keyname, 0, chr(0), chr(1), chr(0), chr(0));
          end if;
        end if;

        m_ParentKey := m_TmpKeyID;
      end loop;

      return m_TmpKeyID;
    end;

    procedure AddValue
    (
      pKeyId in number, 
      pRegKind in number, 
      pObject in number, 
      pBlockUsrVal in char, 
      pExpDep in number, 
      pInt in number, 
      pDouble in number,
      pParam in blob default EMPTY_BLOB()
    )
    as
    begin
      Insert into DREGVAL_DBT
      (
        T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, 
        T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX
      )
      Values
      (pKeyId, pRegKind, pObject, pBlockUsrVal, pExpDep, pInt, pDouble, pParam);
    exception when dup_val_on_index then null;
    end;
BEGIN
  v_KeyID := CreatePath('BANK_INI\™… €€…’›\’—_„‘’“€\€’…ƒ', 4, chr(88), ' αβΰ®©ª  ¤«ο γª § ­¨ο ¨¬¥­¨ ¬ ªΰ®α  α ®―¨α ­¨¥¬ ―®«μ§®Ά β¥«μαª¨ε ¤¥©αβΆ¨© ¤«ο ª β¥£®ΰ¨© ®΅κ¥ªβ ', chr(88), chr(0));
  AddValue(v_KeyID, 0, 0, chr(0), 0, 88, 0);
  DBMS_OUTPUT.PUT_LINE('BANK_INI\™… €€…’›\’—_„‘’“€\€’…ƒ = ' || v_KeyID);


  v_KeyID := CreatePath('BANK_INI\™… €€…’›\’—_„‘’“€\€’…ƒ\’ƒ_003', 2, chr(88), ' ªΰ®α ª β¥£®ΰ¨¨ 003', chr(0), chr(0));
  AddValue(v_KeyID, 0, 0, chr(0), 0, 88, 0, UTL_RAW.CAST_TO_RAW(CHR(0)));
  DBMS_OUTPUT.PUT_LINE('BANK_INI\™… €€…’›\’—_„‘’“€\€’…ƒ = ' || v_KeyID);
END;
/
