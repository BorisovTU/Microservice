CREATE OR REPLACE PACKAGE BODY RSI_RSBPARTY IS

  FUNCTION SetEnablePARTYTRG(p_IsEnable INTEGER) RETURN INTEGER
  IS
    m_WasEnable INTEGER;
  BEGIN
    m_WasEnable := GetEnablePARTYTRG();
    m_IsEnablePARTYTRG := p_IsEnable;
    RETURN m_WasEnable;
  END;

  FUNCTION SetEnablePARTYNAMETRG(p_IsEnable INTEGER) RETURN INTEGER
  IS
    m_WasEnable INTEGER;
  BEGIN
    m_WasEnable := GetEnablePARTYNAMETRG();
    m_IsEnablePARTYNAMETRG := p_IsEnable;
    RETURN m_WasEnable;
  END;

  FUNCTION SetEnablePERSNPLACEWORK(p_IsEnable INTEGER) RETURN INTEGER
  IS
    m_WasEnable INTEGER;
  BEGIN
    m_WasEnable := GetEnablePERSNPLACEWORK();
    m_IsEnablePERSNPLACEWORK := p_IsEnable;
    RETURN m_WasEnable;
  END;

  FUNCTION SetEnablePTRESIDENTHIST(p_IsEnable INTEGER) RETURN INTEGER
  IS
    m_WasEnable INTEGER;
  BEGIN
    m_WasEnable := GetEnablePTRESIDENTHIST();
    m_IsEnablePTRESIDENTHIST := p_IsEnable;
    RETURN m_WasEnable;
  END;

  FUNCTION GetEnablePARTYTRG     RETURN INTEGER IS BEGIN RETURN m_IsEnablePARTYTRG;     END;
  FUNCTION GetEnablePARTYNAMETRG RETURN INTEGER IS BEGIN RETURN m_IsEnablePARTYNAMETRG; END;
FUNCTION GetEnablePERSNPLACEWORK RETURN INTEGER IS BEGIN RETURN m_IsEnablePERSNPLACEWORK; END;
  FUNCTION GetEnablePTRESIDENTHIST RETURN INTEGER IS BEGIN RETURN m_IsEnablePTRESIDENTHIST; END;

  PROCEDURE ContactTIU
  (
     p_old dcontact_dbt%ROWTYPE
    ,p_new dcontact_dbt%ROWTYPE
  )
  AS
    m_oldname dptprmhist_dbt.t_valuebefore%TYPE;
    m_newname dptprmhist_dbt.t_valueafter%TYPE;
    m_PartyID NUMBER;
  BEGIN
    IF    ConvStringValue(p_old.t_value) != ConvStringValue(p_new.t_value)
       OR ConvIntValue(p_old.T_CONTACTKIND) != ConvIntValue(p_new.T_CONTACTKIND)
    THEN

      m_oldname := RSI_RSB_SCRHLP.GetContactKindName(p_old.T_CONTACTKIND);
      m_newname := RSI_RSB_SCRHLP.GetContactKindName(p_new.T_CONTACTKIND);
      IF m_oldname != CHR(1) THEN
        m_oldname := m_oldname || ': ';
        m_oldname := CASE WHEN ConvStringValue(p_old.t_value) != CHR(1) THEN m_oldname || ConvStringValue(p_old.t_value) ELSE m_oldname END;
      END IF;
      IF m_newname != CHR(1) THEN
        m_newname := m_newname || ': ';
        m_newname := CASE WHEN ConvStringValue(p_new.t_value) != CHR(1) THEN m_newname || ConvStringValue(p_new.t_value) ELSE m_newname END;
      END IF;

      m_PartyID := CASE WHEN p_new.t_partyid IS NULL THEN p_old.t_partyid ELSE p_new.t_partyid END;

      AddItemToPtprmhistArray
      (
         m_PartyID
        ,PARTY_CONTACT_VALUE
        ,m_oldname
        ,m_newname
      );
    END IF;

  END;

  --Добавить наименование субъекта в PARTYNAME.DBT
  PROCEDURE AddPartyName(
    p_PartyID INTEGER,
    p_NameTypeID INTEGER,
    p_Name VARCHAR2
  ) IS
    m_IsMult CHAR(1);
  BEGIN
    BEGIN
      SELECT t_ismult INTO m_IsMult FROM dpartykname_dbt WHERE t_nametypeid = p_nametypeid;
      IF m_IsMult = SET_CHAR THEN
        INSERT INTO dpartyname_dbt (t_partynameid, t_partyid, t_nametypeid, t_name, t_freq)
                            VALUES (0, p_PartyID, p_NameTypeID, p_Name, 1);
      ELSE
        IF p_Name = CHR(1) THEN
          DELETE dpartyname_dbt WHERE t_partyid = p_PartyID AND t_nametypeid = p_NameTypeID;
        ELSE
          IF p_NameTypeID = PTKN_SHORTNAME THEN
            UPDATE dpartyname_dbt SET t_name = p_Name, t_freq = 1
             WHERE t_partyid = p_PartyID AND t_nametypeid = p_NameTypeID
               AND INSTR(t_name, p_Name) <> 1;
            --проверим что такое вообще есть, если такого нет (SQL%ROWCOUNT = 0), то дальше вставим
            UPDATE dpartyname_dbt SET t_name = t_name
             WHERE t_partyid = p_PartyID AND t_nametypeid = p_NameTypeID;
          ELSE
            UPDATE dpartyname_dbt SET t_name = p_Name, t_freq = 1
             WHERE t_partyid = p_PartyID AND t_nametypeid = p_NameTypeID;
          END IF;
          IF SQL%ROWCOUNT = 0 THEN
            INSERT INTO dpartyname_dbt (t_partynameid, t_partyid, t_nametypeid, t_name, t_freq)
                                VALUES (0, p_PartyID, p_NameTypeID, p_Name, 1);
          END IF;
        END IF;
      END IF;
      IF p_NameTypeID = PTKN_FULLNAME THEN
        UPDATE dsettacc_dbt SET t_RecName = p_Name WHERE t_partyid = p_PartyID;
      END IF;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
  END;
  --Установить наименование субъекта в PARTY.DBT
  PROCEDURE SetPartyName(
    p_PartyID INTEGER,
    p_NameTypeID INTEGER,
    p_Name VARCHAR2
  ) IS
    m_Name dparty_dbt.t_name%TYPE;
  BEGIN
    IF p_NameTypeID = PTKN_FULLNAME OR
       p_NameTypeID = PTKN_FOREIGNNAME OR
       p_NameTypeID = PTKN_ADDNAME THEN
      SELECT CAST(p_Name AS VARCHAR2(320)) INTO m_Name FROM dual;
    ELSE
      IF p_NameTypeID = PTKN_SHORTNAME THEN
        SELECT CAST(p_Name AS VARCHAR2(60)) INTO m_Name FROM dual;
      ELSE
        RETURN;
      END IF;
    END IF;
    UPDATE dparty_dbt SET
      t_name      = CASE WHEN p_NameTypeID = PTKN_FULLNAME    THEN m_Name ELSE t_name      END,
      t_shortname = CASE WHEN p_NameTypeID = PTKN_SHORTNAME   THEN m_Name ELSE t_shortname END,
      t_latname   = CASE WHEN p_NameTypeID = PTKN_FOREIGNNAME THEN m_Name ELSE t_latname   END,
      t_addname   = CASE WHEN p_NameTypeID = PTKN_ADDNAME     THEN m_Name ELSE t_addname   END
    WHERE t_partyid = p_PartyID;
    IF p_NameTypeID = PTKN_FULLNAME THEN
      UPDATE dsettacc_dbt SET t_RecName = m_Name WHERE t_partyid = p_PartyID;
    END IF;
  END;

  function GetPartyID(p_Branch IN INTEGER ) return INTEGER is
    DefinePartyID INTEGER;
  begin
    select max(t_PartyID) into DefinePartyID from dparty_dbt;

    return DefinePartyID;
  end;


  procedure OurBankChange(Code IN INTEGER, IsHeader IN INTEGER, oldPartyID IN INTEGER, newPartyID IN INTEGER) as
  begin
    update DSFCOMISS_DBT set T_RECEIVERID = newPartyID where T_RECEIVERID = oldPartyID;

  end;

    -- Найти код субъекта по ID с учетом подчиненности субъектов
  function PT_GetPartyCodeEx( p_PartyID in number, p_CodeKind in number, p_Code out varchar2, p_CodeOwnerID out number ) return BINARY_INTEGER
  as
    v_count number(5);
  begin

    select count(*) into v_count
    from ddp_dep_dbt dp
    where dp.t_PartyID = p_PartyID;

    if v_count = 0 then

      -- иерархия dparty_dbt

      select /*+FIRST_ROWS*/ pt3.t_PartyID, pt3.t_Code into p_CodeOwnerID, p_Code
      from ( select pt2.t_PartyID, oc.t_Code
             from ( select pt.t_PartyID, level pt_level
                    from dparty_dbt pt
                    connect by pt.t_PartyID = prior pt.T_SUPERIOR
                    start with pt.t_PartyID = p_PartyID ) pt2
             inner join dobjcode_dbt oc on oc.t_ObjectType = 3 and oc.t_CodeKind = p_CodeKind and oc.t_ObjectID = pt2.t_PartyID and oc.t_State = 0
             order by pt2.pt_level ) pt3
      where rownum = 1;

    else

      -- иерархия ddp_dep_dbt

      select /*+FIRST_ROWS*/ dp3.t_PartyID, dp3.t_Code into p_CodeOwnerID, p_Code
      from ( select dp2.t_PartyID, oc.t_Code
             from ( select dp.t_PartyID, level dp_level
                    from ddp_dep_dbt dp
                    connect by dp.t_Code = prior dp.t_ParentCode
                    start with dp.t_PartyID = p_PartyID ) dp2
             inner join dobjcode_dbt oc on oc.t_ObjectType = 3 and oc.t_CodeKind = p_CodeKind and oc.t_ObjectID = dp2.t_PartyID and oc.t_State = 0
             order by dp2.dp_level ) dp3
      where rownum = 1;

    end if;

    return 0;

    exception
      when others then
        p_CodeOwnerID := -1;
        p_Code        := chr(1);
        return 4;

  end PT_GetPartyCodeEx;

     -- Найти код субъекта по PartyID
  function PT_GetPartyCode( p_PartyID in number, p_CodeKind in number) return VARCHAR2
  as
    v_count number(5);
    v_Code varchar2(200);
  begin

    select count(*) into v_count
    from ddp_dep_dbt dp
    where dp.t_PartyID = p_PartyID;

    if v_count = 0 then

      -- шхЁрЁїш  dparty_dbt

      select /*+FIRST_ROWS*/ pt3.t_Code into v_Code
      from ( select pt2.t_PartyID, oc.t_Code
             from ( select pt.t_PartyID, level pt_level
                    from dparty_dbt pt
                    connect by pt.t_PartyID = prior pt.T_SUPERIOR
                    start with pt.t_PartyID = p_PartyID ) pt2
             inner join dobjcode_dbt oc on oc.t_ObjectType = 3 and oc.t_CodeKind = p_CodeKind and oc.t_ObjectID = pt2.t_PartyID and oc.t_State = 0
             order by pt2.pt_level ) pt3
      where rownum = 1;

    else

      -- шхЁрЁїш  ddp_dep_dbt

      select /*+FIRST_ROWS*/ dp3.t_Code into v_Code
      from ( select dp2.t_PartyID, oc.t_Code
             from ( select dp.t_PartyID, level dp_level
                    from ddp_dep_dbt dp
                    connect by dp.t_Code = prior dp.t_ParentCode
                    start with dp.t_PartyID = p_PartyID ) dp2
             inner join dobjcode_dbt oc on oc.t_ObjectType = 3 and oc.t_CodeKind = p_CodeKind and oc.t_ObjectID = dp2.t_PartyID and oc.t_State = 0
             order by dp2.dp_level ) dp3
      where rownum = 1;

    end if;

    return v_Code;

    exception
      when others then
       return chr(1);

  end PT_GetPartyCode;

     -- Найти адрес субъекта по PartyID
  function PT_GetAdress( p_PartyID in number ) return varchar2
  as
    v_count number(5);
    v_Adress varchar2(512);

    PTADDR_LEGAL CONSTANT number(5) := 1;
    PTADDR_REAL CONSTANT number(5) := 2;
  begin

    begin
      select ad.t_adress into v_Adress
        from dadress_dbt ad
       where ad.t_PartyId = p_PartyId
         and ad.t_type = PTADDR_LEGAL;
    exception
       when NO_DATA_FOUND then

      select ad.t_adress into v_Adress
        from dadress_dbt ad
       where ad.t_PartyId = p_PartyId
         and ad.t_type = PTADDR_REAL;
    end;

    return v_Adress;
  exception
    when others then
    v_Adress := chr(1);
    return v_Adress;

  end PT_GetAdress;

  FUNCTION GetFullBankName (p_PartyID NUMBER) RETURN VARCHAR2 AS
    v_Name      dparty_dbt.t_name%TYPE;
    v_PlaceName dbankdprt_dbt.t_PlaceName%TYPE;
    v_Place     dbankdprt_dbt.t_Place%TYPE;
    v_RetVal    dparty_dbt.t_name%TYPE;
  BEGIN

    BEGIN
      SELECT pt.t_Name, bd.t_PlaceName, bd.t_Place
        INTO v_Name, v_PlaceName, v_Place
        FROM dparty_dbt pt, dbankdprt_dbt bd
       WHERE pt.t_PartyID = p_PartyID
         AND pt.t_PartyID = bd.t_PartyID;
    EXCEPTION WHEN OTHERS THEN
      SELECT pt.t_Name, '', ''
        INTO v_Name, v_PlaceName, v_Place
        FROM dparty_dbt pt
       WHERE pt.t_PartyID = p_PartyID;
    END;

    v_RetVal := REPLACE( REPLACE( REPLACE( TRANSLATE( TRANSLATE( UPPER(v_Name), CHR(9)||CHR(10)||CHR(13),'   '), 'AaBCcEeHKkMmOoPpTuXxYy', '?????????????????????'), ' ', ' _' ), '_ ' ), '_' );

    IF (
        v_PlaceName = CHR(1)   OR
        TRIM(v_PlaceName) = '' OR
        v_RetVal LIKE '% ? %'  OR
        v_RetVal LIKE '%)? %'  OR
        v_RetVal LIKE '%,? %'  OR
        v_RetVal LIKE '%"? %'  OR
        v_RetVal LIKE ('%.' || UPPER(v_PlaceName) || '%') OR
       (v_RetVal LIKE ('%' || REPLACE(UPPER(v_Place),'.','') || '.%') AND INSTR(v_RetVal,'.',-1) <> LENGTH(v_RetVal)) OR
        REPLACE(v_RetVal,'-',' ') LIKE ('%'||REPLACE(UPPER(v_PlaceName),'-',' ')||'.%') OR
        REPLACE(v_RetVal,'-',' ') LIKE ('%'||REPLACE(UPPER(v_PlaceName),'-',' ')||',%') OR
        REPLACE(v_RetVal,'-',' ') LIKE ('%'||REPLACE(UPPER(v_PlaceName),'-',' ')||')%')) THEN
      RETURN TRIM(v_RetVal);
    END IF;

    RETURN TRIM(REPLACE(v_RetVal || ' ' || v_Place || ' ' || v_PlaceName, CHR(1), ''));

  END GetFullBankName;

  --Процедура вставки ид.субъекта в массив вставленных в тек.транзакции
  PROCEDURE AddPartyIDArray(p_partyid dptprmhist_dbt.t_partyid%TYPE)
  AS
    m_loctrnid VARCHAR2(100);
    m_nument BINARY_INTEGER;
    m_partyids PARTYID_TYPE;
  BEGIN
    SELECT DBMS_TRANSACTION.LOCAL_TRANSACTION_ID INTO m_loctrnid FROM dual;
    IF NOT g_trn_inserted_partyid.EXISTS(m_loctrnid) THEN
      g_trn_inserted_partyid(m_loctrnid) := m_partyids;
    END IF;
    g_trn_inserted_partyid(m_loctrnid)(p_partyid) := p_partyid;
  END;
  --Процедура проверки вставки ид.субъекта в массив вставленных в тек.транзакции
  FUNCTION IsExistsPartyIDArray(p_partyid dptprmhist_dbt.t_partyid%TYPE) RETURN BOOLEAN DETERMINISTIC
  AS
    m_loctrnid VARCHAR2(100);
  BEGIN
    SELECT DBMS_TRANSACTION.LOCAL_TRANSACTION_ID INTO m_loctrnid FROM dual;
    IF g_trn_inserted_partyid.EXISTS(m_loctrnid) THEN
      IF g_trn_inserted_partyid(m_loctrnid).EXISTS(p_partyid) THEN
        RETURN TRUE;
      END IF;
    END IF;
    RETURN FALSE;
  END;
  --Функция конвертации символа в Rs-строку
  FUNCTION ConvCharValue(p_value IN CHAR) RETURN VARCHAR2
  AS
  BEGIN
    IF p_value IS NULL THEN RETURN CHR(1); END IF;
    RETURN CASE WHEN p_value = CHR(0) THEN CHR(1) ELSE p_value END;
  END;
  --Функция конвертации даты в Rs-строку
  FUNCTION ConvDateValue(p_value IN DATE) RETURN VARCHAR2
  AS
  BEGIN
    IF p_value IS NULL THEN RETURN CHR(1); END IF;
    IF p_value = TO_DATE('01.01.0001', 'DD.MM.YYYY') THEN RETURN '00.00.0000'; END IF;
    RETURN TRIM(TO_CHAR(p_value, 'DD.MM.YYYY'));
  END;
  --Функция конвертации числа в Rs-строку со знаками после точки
  FUNCTION ConvDoubleValue(p_value IN NUMBER) RETURN VARCHAR2
  AS
  BEGIN
    IF p_value IS NULL THEN RETURN CHR(1); END IF;
    RETURN TRIM(TO_CHAR(p_value, '999999999999999999999999999999.99'));
  END;
  --Функция конвертации числа в Rs-строку
  FUNCTION ConvIntValue(p_value IN NUMBER) RETURN VARCHAR2
  AS
  BEGIN
    IF p_value IS NULL THEN RETURN CHR(1); END IF;
    RETURN TRIM(TO_CHAR(p_value));
  END;
  --Функция конвертации строки в Rs-строку
  FUNCTION ConvStringValue(p_value IN VARCHAR2) RETURN VARCHAR2
  AS
  BEGIN
    IF p_value IS NULL THEN RETURN CHR(1); END IF;
    RETURN p_value;
  END;
  --Функция получения кода субъекта
  FUNCTION GetPartyCode(p_partyid IN NUMBER, p_codekind IN NUMBER) RETURN VARCHAR2
  AS
    m_code dobjcode_dbt.t_code%TYPE;
  BEGIN
    m_code := chr(1);
    BEGIN
      SELECT t_code INTO m_code
        FROM dpartcode_dbt
       WHERE t_partyid = p_partyid
         AND t_codekind = p_codekind;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
    RETURN m_code;
  END;
  --Функция получения наименования страны
  FUNCTION GetCountryName(p_countryid IN NUMBER) RETURN VARCHAR2
  AS
    m_name dcountry_dbt.t_name%TYPE;
  BEGIN
    m_name := chr(1);
    BEGIN
      SELECT t_name INTO m_name
        FROM dcountry_dbt
       WHERE t_countryid = p_countryid;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
    RETURN m_name;
  END;
  --Процедура вставки в массив историзации
  PROCEDURE AddItemToPtprmhistArray
  (
     p_partyid      dptprmhist_dbt.t_partyid%TYPE
    ,p_paramkindid  dptprmhist_dbt.t_paramkindid%TYPE
    ,p_valuebefore  dptprmhist_dbt.t_valuebefore%TYPE
    ,p_valueafter   dptprmhist_dbt.t_valueafter%TYPE
  )
  AS
  BEGIN
    g_nument                := g_nument + 1 ;
    g_partyid(g_nument)     := p_partyid    ;
    g_paramkindid(g_nument) := p_paramkindid;
    g_valuebefore(g_nument) := p_valuebefore;
    g_valueafter(g_nument)  := p_valueafter ;
  END;
  --Процедура вставки в массив историзации
  PROCEDURE SavePtprmhistArray
  AS
    m_curdate DATE;
    m_oper    INTEGER;
  BEGIN
    IF g_nument > 0 THEN
      m_curdate := RSBSESSIONDATA.curdate;
      m_oper    := RSBSESSIONDATA.oper   ;
      FOR i IN 1 .. g_nument LOOP
        BEGIN
          INSERT INTO dptprmhist_dbt(
             t_partyid
            ,t_sysdate
            ,t_systime
            ,t_bankdate
            ,t_oper
            ,t_paramkindid
            ,t_valuebefore
            ,t_valueafter
            )
          VALUES(
             g_partyid(i)
            ,TRUNC(SYSDATE)
            ,TO_DATE('01-01-0001:' || TO_CHAR(SYSDATE,'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS')
            ,m_curdate
            ,m_oper
            ,g_paramkindid(i)
            ,g_valuebefore(i)
            ,g_valueafter(i)
            );
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
      END LOOP;
    END IF;
    g_nument := 0;
  END;
  --Процедура вставки в массив историзации из триггера dparty_dbt
  PROCEDURE PartyTU
  (
     p_old dparty_dbt%ROWTYPE
    ,p_new dparty_dbt%ROWTYPE
  )
  AS
  BEGIN
    --party.dbt / locked       PARTY_LOCKED
    IF ConvCharValue(p_old.t_locked) !=
       ConvCharValue(p_new.t_locked) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_LOCKED
        ,ConvCharValue(p_old.t_locked)
        ,ConvCharValue(p_new.t_locked)
      );
    END IF;
    --party.dbt / shortname    PARTY_SHORTNAME
    IF ConvStringValue(p_old.t_shortname) !=
       ConvStringValue(p_new.t_shortname) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_SHORTNAME
        ,ConvStringValue(p_old.t_shortname)
        ,ConvStringValue(p_new.t_shortname)
      );
    END IF;
    --party.dbt / name         PARTY_NAME
    IF ConvStringValue(p_old.t_name) !=
       ConvStringValue(p_new.t_name) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_NAME
        ,ConvStringValue(p_old.t_name)
        ,ConvStringValue(p_new.t_name)
      );
    END IF;
    --party.dbt / addname      PARTY_ADDNAME
    --Обрабатывается в триггере по dpartyname_dbt
    --IF ConvStringValue(p_old.t_addname) !=
    --   ConvStringValue(p_new.t_addname) THEN
    --  AddItemToPtprmhistArray
    --  (
    --     p_new.t_partyid
    --    ,PARTY_ADDNAME
    --    ,ConvStringValue(p_old.t_addname)
    --    ,ConvStringValue(p_new.t_addname)
    --  );
    --END IF;
    --party.dbt / superior     PARTY_SUPERIOR
    IF ConvIntValue(p_old.t_superior) !=
       ConvIntValue(p_new.t_superior) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_SUPERIOR
        ,GetPartyCode(p_old.t_superior, 1)
        ,GetPartyCode(p_new.t_superior, 1)
      );
    END IF;
    --party.dbt / nrcountry    PARTY_NRCOUNTRY
    IF ConvStringValue(p_old.t_nrcountry) !=
       ConvStringValue(p_new.t_nrcountry) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_NRCOUNTRY
        ,ConvStringValue(p_old.t_nrcountry)
        ,ConvStringValue(p_new.t_nrcountry)
      );
    END IF;
    --party.dbt / notresident  PARTY_NOTRESIDENT
    IF ConvCharValue(p_old.t_notresident) !=
       ConvCharValue(p_new.t_notresident) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_NOTRESIDENT
        ,ConvCharValue(p_old.t_notresident)
        ,ConvCharValue(p_new.t_notresident)
      );
    END IF;
    --party.dbt / ofshorezone  PARTY_OFFSHOREZONE
    IF ConvIntValue(p_old.t_ofshorezone) !=
       ConvIntValue(p_new.t_ofshorezone) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_OFFSHOREZONE
        ,GetCountryName(p_old.t_ofshorezone)
        ,GetCountryName(p_new.t_ofshorezone)
      );
    END IF;
    --party.dbt / okpo         PARTY_OKPO
    IF ConvStringValue(p_old.t_okpo) !=
       ConvStringValue(p_new.t_okpo) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_OKPO
        ,ConvStringValue(p_old.t_okpo)
        ,ConvStringValue(p_new.t_okpo)
      );
    END IF;
    --party.dbt / userfield1   PARTY_USERFIELD1
    IF ConvStringValue(p_old.t_userfield1) !=
       ConvStringValue(p_new.t_userfield1) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_USERFIELD1
        ,ConvStringValue(p_old.t_userfield1)
        ,ConvStringValue(p_new.t_userfield1)
      );
    END IF;
    --party.dbt / userfield2   PARTY_USERFIELD2
    IF ConvStringValue(p_old.t_userfield2) !=
       ConvStringValue(p_new.t_userfield2) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_USERFIELD2
        ,ConvStringValue(p_old.t_userfield2)
        ,ConvStringValue(p_new.t_userfield2)
      );
    END IF;
    --party.dbt / userfield3   PARTY_USERFIELD3
    IF ConvStringValue(p_old.t_userfield3) !=
       ConvStringValue(p_new.t_userfield3) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_USERFIELD3
        ,ConvStringValue(p_old.t_userfield3)
        ,ConvStringValue(p_new.t_userfield3)
      );
    END IF;
    --party.dbt / userfield4   PARTY_USERFIELD4
    IF ConvStringValue(p_old.t_userfield4) !=
       ConvStringValue(p_new.t_userfield4) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_USERFIELD4
        ,p_old.t_userfield4
        ,p_new.t_userfield4
      );
    END IF;
    --party.dbt / isdoubler    PARTY_ISDOUBLER
    IF ConvCharValue(p_old.t_isdoubler) !=
       ConvCharValue(p_new.t_isdoubler) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_ISDOUBLER
        ,ConvCharValue(p_old.t_isdoubler)
        ,ConvCharValue(p_new.t_isdoubler)
      );
    END IF;
    --party.dbt / mainpartyid  PARTY_MAINPARTYID
    IF ConvIntValue(p_old.t_mainpartyid) !=
       ConvIntValue(p_new.t_mainpartyid) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,PARTY_MAINPARTYID
        ,GetPartyCode(p_old.t_mainpartyid, 1)
        ,GetPartyCode(p_new.t_mainpartyid, 1)
      );
    END IF;
  END;
  --Процедура вставки в массив историзации из триггера dpartyname_dbt
  PROCEDURE PartynameTIU
  (
     p_old dpartyname_dbt%ROWTYPE
    ,p_new dpartyname_dbt%ROWTYPE
  )
  AS
    m_oldname dptprmhist_dbt.t_valuebefore%TYPE;
    m_newname dptprmhist_dbt.t_valueafter%TYPE;
    m_PartyID NUMBER;
  BEGIN
    --partyname.dbt / name     PARTY_ADDNAME
    IF ConvStringValue(p_old.t_name) !=
       ConvStringValue(p_new.t_name) THEN
      m_oldname := RSI_RSB_SCRHLP.GetPartyKNameName(p_old.t_nametypeid);
      m_newname := RSI_RSB_SCRHLP.GetPartyKNameName(p_new.t_nametypeid);
      IF m_oldname != CHR(1) THEN
        m_oldname := m_oldname || ': ';
        m_oldname := CASE WHEN ConvStringValue(p_old.t_name) != CHR(1) THEN m_oldname || ConvStringValue(p_old.t_name) ELSE m_oldname END;
      END IF;
      IF m_newname != CHR(1) THEN
        m_newname := m_newname || ': ';
        m_newname := CASE WHEN ConvStringValue(p_new.t_name) != CHR(1) THEN m_newname || ConvStringValue(p_new.t_name) ELSE m_newname END;
      END IF;

      m_PartyID := CASE WHEN p_new.t_partyid IS NULL THEN p_old.t_partyid ELSE p_new.t_partyid END;

      AddItemToPtprmhistArray
      (
         m_PartyID
        ,PARTY_ADDNAME
        ,m_oldname
        ,m_newname
      );
    END IF;
  END;
  --Процедура вставки в массив историзации из триггера dadress_dbt
  PROCEDURE AdressTIU
  (
     p_old dadress_dbt%ROWTYPE
    ,p_new dadress_dbt%ROWTYPE
  )
  AS
    m_tmp dadress_dbt%ROWTYPE;
  BEGIN
    --adress.dbt / adress ADRESS_ADRESS_URD
    --adress.dbt / adress ADRESS_ADRESS_FACT
    --adress.dbt / adress ADRESS_ADRESS_MAIL
    IF p_new.t_type IN (1, 2, 3) THEN
      IF p_old.t_type != p_new.t_type THEN
        m_tmp.t_partyid := p_old.t_partyid;
        m_tmp.t_type    := p_old.t_type   ;
        AdressTIU
        (
           p_old
          ,m_tmp
        );
        m_tmp.t_partyid := p_new.t_partyid;
        m_tmp.t_type    := p_new.t_type   ;
        AdressTIU
        (
           m_tmp
          ,p_new
        );
      ELSE
        IF ConvStringValue(p_old.t_adress) !=
           ConvStringValue(p_new.t_adress) THEN
          AddItemToPtprmhistArray
          (
             p_new.t_partyid
            ,CASE p_new.t_type WHEN 1 THEN ADRESS_ADRESS_URD WHEN 2 THEN ADRESS_ADRESS_FACT ELSE ADRESS_ADRESS_MAIL END
            ,ConvStringValue(p_old.t_adress)
            ,ConvStringValue(p_new.t_adress)
          );
        END IF;
      END IF;
    END IF;
  END;
  --Процедура вставки в массив историзации из триггера dinstitut_dbt
  PROCEDURE InstitutTIU
  (
     p_old dinstitut_dbt%ROWTYPE
    ,p_new dinstitut_dbt%ROWTYPE
  )
  AS
  BEGIN
    --institut.dbt / charterdate    INSTITUT_CHARTERDATE
    IF ConvDateValue(p_old.t_charterdate) !=
       ConvDateValue(p_new.t_charterdate) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,INSTITUT_CHARTERDATE
        ,ConvDateValue(p_old.t_charterdate)
        ,ConvDateValue(p_new.t_charterdate)
      );
    END IF;
    --institut.dbt / capitalfi      INSTITUT_CAPITALFI
    IF ConvIntValue(p_old.t_capitalfi) !=
       ConvIntValue(p_new.t_capitalfi) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,INSTITUT_CAPITALFI
        ,RSI_RSB_SCRHLP.GetFICode(p_old.t_capitalfi)
        ,RSI_RSB_SCRHLP.GetFICode(p_new.t_capitalfi)
      );
    END IF;
    --institut.dbt / declarecapital INSTITUT_DECLARECAPITAL
    IF ConvDoubleValue(p_old.t_declarecapital) !=
       ConvDoubleValue(p_new.t_declarecapital) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,INSTITUT_DECLARECAPITAL
        ,ConvDoubleValue(p_old.t_declarecapital)
        ,ConvDoubleValue(p_new.t_declarecapital)
      );
    END IF;
    --institut.dbt / realcapital    INSTITUT_REALCAPITAL
    IF ConvDoubleValue(p_old.t_realcapital) !=
       ConvDoubleValue(p_new.t_realcapital) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_partyid
        ,INSTITUT_REALCAPITAL
        ,ConvDoubleValue(p_old.t_realcapital)
        ,ConvDoubleValue(p_new.t_realcapital)
      );
    END IF;
  END;
  --Процедура вставки в массив историзации из триггера dpersn_dbt
  PROCEDURE PersnTU
  (
     p_old dpersn_dbt%ROWTYPE
    ,p_new dpersn_dbt%ROWTYPE
  )
  AS
  BEGIN
    --persn.dbt / name1            PERSN_NAME1
    IF ConvStringValue(p_old.t_name1) !=
       ConvStringValue(p_new.t_name1) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_NAME1
        ,ConvStringValue(p_old.t_name1)
        ,ConvStringValue(p_new.t_name1)
      );
    END IF;
    --persn.dbt / name2            PERSN_NAME2
    IF ConvStringValue(p_old.t_name2) !=
       ConvStringValue(p_new.t_name2) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_NAME2
        ,ConvStringValue(p_old.t_name2)
        ,ConvStringValue(p_new.t_name2)
      );
    END IF;
    --persn.dbt / name3            PERSN_NAME3
    IF ConvStringValue(p_old.t_name3) !=
       ConvStringValue(p_new.t_name3) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_NAME3
        ,ConvStringValue(p_old.t_name3)
        ,ConvStringValue(p_new.t_name3)
      );
    END IF;
    --persn.dbt / ismale           PERSN_ISMALE
    IF ConvCharValue(p_old.t_ismale) !=
       ConvCharValue(p_new.t_ismale) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_ISMALE
        ,CASE WHEN ConvCharValue(p_old.t_ismale) = CHR(88) THEN PTHIST_PARAM_MAN ELSE PTHIST_PARAM_WOMAN END
        ,CASE WHEN ConvCharValue(p_new.t_ismale) = CHR(88) THEN PTHIST_PARAM_MAN ELSE PTHIST_PARAM_WOMAN END
      );
    END IF;
    --persn.dbt / ethnos           PERSN_ETHNOS
    IF ConvStringValue(p_old.t_ethnos) !=
       ConvStringValue(p_new.t_ethnos) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_ETHNOS
        ,ConvStringValue(p_old.t_ethnos)
        ,ConvStringValue(p_new.t_ethnos)
      );
    END IF;
    --persn.dbt / birsplase        PERSN_BIRSPLASE
    IF ConvStringValue(p_old.t_birsplase) !=
       ConvStringValue(p_new.t_birsplase) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_BIRSPLASE
        ,ConvStringValue(p_old.t_birsplase)
        ,ConvStringValue(p_new.t_birsplase)
      );
    END IF;
    --persn.dbt / regionborn       PERSN_REGIONBORN
    IF ConvStringValue(p_old.t_regionborn) !=
       ConvStringValue(p_new.t_regionborn) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_REGIONBORN
        ,ConvStringValue(p_old.t_regionborn)
        ,ConvStringValue(p_new.t_regionborn)
      );
    END IF;
    --persn.dbt / raionborn        PERSN_RAIONBORN
    IF ConvStringValue(p_old.t_raionborn) !=
       ConvStringValue(p_new.t_raionborn) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_RAIONBORN
        ,ConvStringValue(p_old.t_raionborn)
        ,ConvStringValue(p_new.t_raionborn)
      );
    END IF;
    --persn.dbt / placeborn        PERSN_PLACEBORN
    IF ConvStringValue(p_old.t_placeborn) !=
       ConvStringValue(p_new.t_placeborn) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_PLACEBORN
        ,ConvStringValue(p_old.t_placeborn)
        ,ConvStringValue(p_new.t_placeborn)
      );
    END IF;
    --persn.dbt / born             PERSN_BORN
    IF ConvDateValue(p_old.t_born) !=
       ConvDateValue(p_new.t_born) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_BORN
        ,ConvDateValue(p_old.t_born)
        ,ConvDateValue(p_new.t_born)
      );
    END IF;
    --persn.dbt / death            PERSN_DEATH
    IF ConvDateValue(p_old.t_death) !=
       ConvDateValue(p_new.t_death) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_DEATH
        ,ConvDateValue(p_old.t_death)
        ,ConvDateValue(p_new.t_death)
      );
    END IF;
    --persn.dbt / placework        PERSN_PLACEWORK
    IF ConvStringValue(p_old.t_placework) !=
       ConvStringValue(p_new.t_placework) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_PLACEWORK
        ,ConvStringValue(p_old.t_placework)
        ,ConvStringValue(p_new.t_placework)
      );
    END IF;
    --persn.dbt / isemployer       PERSN_ISEMPLOYER
    IF ConvCharValue(p_old.t_isemployer) !=
       ConvCharValue(p_new.t_isemployer) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_ISEMPLOYER
        ,ConvCharValue(p_old.t_isemployer)
        ,ConvCharValue(p_new.t_isemployer)
      );
    END IF;
    --persn.dbt / licencenumber    PERSN_LICENCENUMBER
    IF ConvStringValue(p_old.t_licencenumber) !=
       ConvStringValue(p_new.t_licencenumber) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_LICENCENUMBER
        ,ConvStringValue(p_old.t_licencenumber)
        ,ConvStringValue(p_new.t_licencenumber)
      );
    END IF;
    --persn.dbt / licencedate      PERSN_LICENCEDATE
    IF ConvDateValue(p_old.t_licencedate) !=
       ConvDateValue(p_new.t_licencedate) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_LICENCEDATE
        ,ConvDateValue(p_old.t_licencedate)
        ,ConvDateValue(p_new.t_licencedate)
      );
    END IF;
    --persn.dbt / kategor          PERSN_KATEGOR
    IF ConvIntValue(p_old.t_kategor) !=
       ConvIntValue(p_new.t_kategor) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_KATEGOR
        ,ConvIntValue(p_old.t_kategor)
        ,ConvIntValue(p_new.t_kategor)
      );
    END IF;
    --persn.dbt / groupauthor      PERSN_GROUPAUTHOR
    IF ConvCharValue(p_old.t_groupauthor) !=
       ConvCharValue(p_new.t_groupauthor) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_GROUPAUTHOR
        ,ConvCharValue(p_old.t_groupauthor)
        ,ConvCharValue(p_new.t_groupauthor)
      );
    END IF;
    --persn.dbt / isliterate       PERSN_ISLITERATE
    IF ConvCharValue(p_old.t_isliterate) !=
       ConvCharValue(p_new.t_isliterate) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_ISLITERATE
        ,ConvCharValue(p_old.t_isliterate)
        ,ConvCharValue(p_new.t_isliterate)
      );
    END IF;
    --persn.dbt / groupwil         PERSN_GROUPWIL
    IF ConvCharValue(p_old.t_groupwil) !=
       ConvCharValue(p_new.t_groupwil) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_GROUPWIL
        ,ConvCharValue(p_old.t_groupwil)
        ,ConvCharValue(p_new.t_groupwil)
      );
    END IF;
    --persn.dbt / specialaccess    PERSN_SPECIALACCESS
    IF ConvCharValue(p_old.t_specialaccess) !=
       ConvCharValue(p_new.t_specialaccess) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_SPECIALACCESS
        ,ConvCharValue(p_old.t_specialaccess)
        ,ConvCharValue(p_new.t_specialaccess)
      );
    END IF;
    --persn.dbt / vzaimosvyazanniy PERSN_VZAIMOSVYAZANNIY
    IF ConvCharValue(p_old.t_vzaimosvyazanniy) !=
       ConvCharValue(p_new.t_vzaimosvyazanniy) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_VZAIMOSVYAZANNIY
        ,ConvCharValue(p_old.t_vzaimosvyazanniy)
        ,ConvCharValue(p_new.t_vzaimosvyazanniy)
      );
    END IF;
    --persn.dbt / offshoerresident PERSN_OFFSHORERESIDENT
    IF ConvCharValue(p_old.t_offshoreresident) !=
       ConvCharValue(p_new.t_offshoreresident) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_OFFSHOERRESIDENT
        ,ConvCharValue(p_old.t_offshoreresident)
        ,ConvCharValue(p_new.t_offshoreresident)
      );
    END IF;
    --persn.dbt / penscardnumber   PERSN_PENSCARDNUMBER
    IF ConvStringValue(p_old.t_penscardnumber) !=
       ConvStringValue(p_new.t_penscardnumber) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_PENSCARDNUMBER
        ,ConvStringValue(p_old.t_penscardnumber)
        ,ConvStringValue(p_new.t_penscardnumber)
      );
    END IF;
    --persn.dbt / penscarddate     PERSN_PENSCARDDATE
    IF ConvDateValue(p_old.t_penscarddate) !=
       ConvDateValue(p_new.t_penscarddate) THEN
      AddItemToPtprmhistArray
      (
         p_new.t_personid
        ,PERSN_PENSCARDDATE
        ,ConvDateValue(p_old.t_penscarddate)
        ,ConvDateValue(p_new.t_penscarddate)
      );
    END IF;
  END;
  --Процедура вставки в массив историзации из триггера dbankdprt_dbt
  PROCEDURE BankdprtTIUD
  (
     p_old dbankdprt_dbt%ROWTYPE
    ,p_new dbankdprt_dbt%ROWTYPE
  )
  AS
    m_partyid NUMBER;
  BEGIN
    m_partyid := CASE WHEN p_new.t_partyid IS NOT NULL THEN p_new.t_partyid ELSE p_old.t_partyid END;
    IF m_partyid IS NOT NULL THEN
      --bankdprt.dbt / bic_rcc     BANKDPRT_BIC_RCC
      IF ConvStringValue(p_old.t_bic_rcc) !=
         ConvStringValue(p_new.t_bic_rcc) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_BIC_RCC
          ,ConvStringValue(p_old.t_bic_rcc)
          ,ConvStringValue(p_new.t_bic_rcc)
        );
      END IF;
      --bankdprt.dbt / region      BANKDPRT_REGION
      IF ConvStringValue(p_old.t_region) !=
         ConvStringValue(p_new.t_region) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_REGION
          ,ConvStringValue(p_old.t_region)
          ,ConvStringValue(p_new.t_region)
        );
      END IF;
      --bankdprt.dbt / place       BANKDPRT_PLACE
      IF ConvStringValue(p_old.t_place) !=
         ConvStringValue(p_new.t_place) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_PLACE
          ,ConvStringValue(p_old.t_place)
          ,ConvStringValue(p_new.t_place)
        );
      END IF;
      --bankdprt.dbt / checkalg    BANKDPRT_CHECKALG
      IF ConvIntValue(p_old.t_checkalg) !=
         ConvIntValue(p_new.t_checkalg) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_CHECKALG
          ,ConvIntValue(p_old.t_checkalg)
          ,ConvIntValue(p_new.t_checkalg)
        );
      END IF;
      --bankdprt.dbt / checkdata   BANKDPRT_CHECKDATA
      IF ConvStringValue(p_old.t_checkdata) !=
         ConvStringValue(p_new.t_checkdata) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_CHECKDATA
          ,ConvStringValue(p_old.t_checkdata)
          ,ConvStringValue(p_new.t_checkdata)
        );
      END IF;
      --bankdprt.dbt / blnc_rcc    BANKDPRT_BLNC_RCC
      IF ConvStringValue(p_old.t_blnc_rcc) !=
         ConvStringValue(p_new.t_blnc_rcc) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_BLNC_RCC
          ,ConvStringValue(p_old.t_blnc_rcc)
          ,ConvStringValue(p_new.t_blnc_rcc)
        );
      END IF;
      --bankdprt.dbt / lock        BANKDPRT_LOCK
      IF ConvCharValue(p_old.t_lock) !=
         ConvCharValue(p_new.t_lock) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_LOCK
          ,ConvCharValue(p_old.t_lock)
          ,ConvCharValue(p_new.t_lock)
        );
      END IF;
      --bankdprt.dbt / coracc      BANKDPRT_CORACC
      IF ConvStringValue(p_old.t_coracc) !=
         ConvStringValue(p_new.t_coracc) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_CORACC
          ,ConvStringValue(p_old.t_coracc)
          ,ConvStringValue(p_new.t_coracc)
        );
      END IF;
      --bankdprt.dbt / rept        BANKDPRT_REPT
      IF ConvCharValue(p_old.t_rept) !=
         ConvCharValue(p_new.t_rept) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_REPT
          ,ConvCharValue(p_old.t_rept)
          ,ConvCharValue(p_new.t_rept)
        );
      END IF;
      --bankdprt.dbt / vkey        BANKDPRT_VKEY
      IF ConvStringValue(p_old.t_vkey) !=
         ConvStringValue(p_new.t_vkey) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_VKEY
          ,ConvStringValue(p_old.t_vkey)
          ,ConvStringValue(p_new.t_vkey)
        );
      END IF;
      --bankdprt.dbt / notbank     BANKDPRT_NOTBANK
      IF ConvCharValue(p_old.t_notbank) !=
         ConvCharValue(p_new.t_notbank) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_NOTBANK
          ,ConvCharValue(p_old.t_notbank)
          ,ConvCharValue(p_new.t_notbank)
        );
      END IF;
      --bankdprt.dbt / paymentkind BANKDPRT_PAYMENTKIND
      IF ConvCharValue(p_old.t_paymentkind) !=
         ConvCharValue(p_new.t_paymentkind) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_PAYMENTKIND
          ,ConvCharValue(p_old.t_paymentkind)
          ,ConvCharValue(p_new.t_paymentkind)
        );
      END IF;
      --bankdprt.dbt / uertype     BANKDPRT_UERTYPE
      IF ConvIntValue(p_old.t_uertype) !=
         ConvIntValue(p_new.t_uertype) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_UERTYPE
          ,ConvIntValue(p_old.t_uertype)
          ,ConvIntValue(p_new.t_uertype)
        );
      END IF;
      --bankdprt.dbt / uer         BANKDPRT_UER
      IF ConvIntValue(p_old.t_uer) !=
         ConvIntValue(p_new.t_uer) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_UER
          ,ConvIntValue(p_old.t_uer)
          ,ConvIntValue(p_new.t_uer)
        );
      END IF;
      --bankdprt.dbt / real        BANKDPRT_REAL
      IF ConvStringValue(p_old.t_real) !=
         ConvStringValue(p_new.t_real) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_REAL
          ,ConvStringValue(p_old.t_real)
          ,ConvStringValue(p_new.t_real)
        );
      END IF;
      --bankdprt.dbt / controldate BANKDPRT_CONTROLDATE
      IF ConvDateValue(p_old.t_controldate) !=
         ConvDateValue(p_new.t_controldate) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,BANKDPRT_CONTROLDATE
          ,ConvDateValue(p_old.t_controldate)
          ,ConvDateValue(p_new.t_controldate)
        );
      END IF;
    END IF;
  END;
  --Процедура вставки в массив историзации из триггера dptbicdir_dbt
  PROCEDURE PtbicdirTIUD
  (
     p_old dptbicdir_dbt%ROWTYPE
    ,p_new dptbicdir_dbt%ROWTYPE
  )
  AS
    m_partyid NUMBER;
  BEGIN
    m_partyid := CASE WHEN p_new.t_partyid IS NOT NULL THEN p_new.t_partyid ELSE p_old.t_partyid END;
    IF m_partyid IS NOT NULL THEN
      --ptbicdir.dbt / institutionname      PTBICDIR_INSTITUTIONNAME
      IF ConvStringValue(p_old.t_institutionname) !=
         ConvStringValue(p_new.t_institutionname) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_INSTITUTIONNAME
          ,ConvStringValue(p_old.t_institutionname)
          ,ConvStringValue(p_new.t_institutionname)
        );
      END IF;
      --ptbicdir.dbt / branchinformation    PTBICDIR_BRANCHINFORMATION
      IF ConvStringValue(p_old.t_branchinformation) !=
         ConvStringValue(p_new.t_branchinformation) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_BRANCHINFORMATION
          ,ConvStringValue(p_old.t_branchinformation)
          ,ConvStringValue(p_new.t_branchinformation)
        );
      END IF;
      --ptbicdir.dbt / subtypeindication    PTBICDIR_SUBTYPEINDICATION
      IF ConvStringValue(p_old.t_subtypeindication) !=
         ConvStringValue(p_new.t_subtypeindication) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_SUBTYPEINDICATION
          ,ConvStringValue(p_old.t_subtypeindication)
          ,ConvStringValue(p_new.t_subtypeindication)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices1  PTBICDIR_VALUEADDEDSERVICES1
      IF ConvStringValue(p_old.t_valueaddedservices1) !=
         ConvStringValue(p_new.t_valueaddedservices1) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES1
          ,ConvStringValue(p_old.t_valueaddedservices1)
          ,ConvStringValue(p_new.t_valueaddedservices1)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices2  PTBICDIR_VALUEADDEDSERVICES2
      IF ConvStringValue(p_old.t_valueaddedservices2) !=
         ConvStringValue(p_new.t_valueaddedservices2) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES2
          ,ConvStringValue(p_old.t_valueaddedservices2)
          ,ConvStringValue(p_new.t_valueaddedservices2)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices3  PTBICDIR_VALUEADDEDSERVICES3
      IF ConvStringValue(p_old.t_valueaddedservices3) !=
         ConvStringValue(p_new.t_valueaddedservices3) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES3
          ,ConvStringValue(p_old.t_valueaddedservices3)
          ,ConvStringValue(p_new.t_valueaddedservices3)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices4  PTBICDIR_VALUEADDEDSERVICES4
      IF ConvStringValue(p_old.t_valueaddedservices4) !=
         ConvStringValue(p_new.t_valueaddedservices4) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES4
          ,ConvStringValue(p_old.t_valueaddedservices4)
          ,ConvStringValue(p_new.t_valueaddedservices4)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices5  PTBICDIR_VALUEADDEDSERVICES5
      IF ConvStringValue(p_old.t_valueaddedservices5) !=
         ConvStringValue(p_new.t_valueaddedservices5) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES5
          ,ConvStringValue(p_old.t_valueaddedservices5)
          ,ConvStringValue(p_new.t_valueaddedservices5)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices6  PTBICDIR_VALUEADDEDSERVICES6
      IF ConvStringValue(p_old.t_valueaddedservices6) !=
         ConvStringValue(p_new.t_valueaddedservices6) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES6
          ,ConvStringValue(p_old.t_valueaddedservices6)
          ,ConvStringValue(p_new.t_valueaddedservices6)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices7  PTBICDIR_VALUEADDEDSERVICES7
      IF ConvStringValue(p_old.t_valueaddedservices7) !=
         ConvStringValue(p_new.t_valueaddedservices7) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES7
          ,ConvStringValue(p_old.t_valueaddedservices7)
          ,ConvStringValue(p_new.t_valueaddedservices7)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices8  PTBICDIR_VALUEADDEDSERVICES8
      IF ConvStringValue(p_old.t_valueaddedservices8) !=
         ConvStringValue(p_new.t_valueaddedservices8) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES8
          ,ConvStringValue(p_old.t_valueaddedservices8)
          ,ConvStringValue(p_new.t_valueaddedservices8)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices9  PTBICDIR_VALUEADDEDSERVICES9
      IF ConvStringValue(p_old.t_valueaddedservices9) !=
         ConvStringValue(p_new.t_valueaddedservices9) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES9
          ,ConvStringValue(p_old.t_valueaddedservices9)
          ,ConvStringValue(p_new.t_valueaddedservices9)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices10 PTBICDIR_VALUEADDEDSERVICES10
      IF ConvStringValue(p_old.t_valueaddedservices10) !=
         ConvStringValue(p_new.t_valueaddedservices10) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES10
          ,ConvStringValue(p_old.t_valueaddedservices10)
          ,ConvStringValue(p_new.t_valueaddedservices10)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices11 PTBICDIR_VALUEADDEDSERVICES11
      IF ConvStringValue(p_old.t_valueaddedservices11) !=
         ConvStringValue(p_new.t_valueaddedservices11) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES11
          ,ConvStringValue(p_old.t_valueaddedservices11)
          ,ConvStringValue(p_new.t_valueaddedservices11)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices12 PTBICDIR_VALUEADDEDSERVICES12
      IF ConvStringValue(p_old.t_valueaddedservices12) !=
         ConvStringValue(p_new.t_valueaddedservices12) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES12
          ,ConvStringValue(p_old.t_valueaddedservices12)
          ,ConvStringValue(p_new.t_valueaddedservices12)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices13 PTBICDIR_VALUEADDEDSERVICES13
      IF ConvStringValue(p_old.t_valueaddedservices13) !=
         ConvStringValue(p_new.t_valueaddedservices13) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES13
          ,ConvStringValue(p_old.t_valueaddedservices13)
          ,ConvStringValue(p_new.t_valueaddedservices13)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices14 PTBICDIR_VALUEADDEDSERVICES14
      IF ConvStringValue(p_old.t_valueaddedservices14) !=
         ConvStringValue(p_new.t_valueaddedservices14) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES14
          ,ConvStringValue(p_old.t_valueaddedservices14)
          ,ConvStringValue(p_new.t_valueaddedservices14)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices15 PTBICDIR_VALUEADDEDSERVICES15
      IF ConvStringValue(p_old.t_valueaddedservices15) !=
         ConvStringValue(p_new.t_valueaddedservices15) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES15
          ,ConvStringValue(p_old.t_valueaddedservices15)
          ,ConvStringValue(p_new.t_valueaddedservices15)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices16 PTBICDIR_VALUEADDEDSERVICES16
      IF ConvStringValue(p_old.t_valueaddedservices16) !=
         ConvStringValue(p_new.t_valueaddedservices16) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES16
          ,ConvStringValue(p_old.t_valueaddedservices16)
          ,ConvStringValue(p_new.t_valueaddedservices16)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices17 PTBICDIR_VALUEADDEDSERVICES17
      IF ConvStringValue(p_old.t_valueaddedservices17) !=
         ConvStringValue(p_new.t_valueaddedservices17) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES17
          ,ConvStringValue(p_old.t_valueaddedservices17)
          ,ConvStringValue(p_new.t_valueaddedservices17)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices18 PTBICDIR_VALUEADDEDSERVICES18
      IF ConvStringValue(p_old.t_valueaddedservices18) !=
         ConvStringValue(p_new.t_valueaddedservices18) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES18
          ,ConvStringValue(p_old.t_valueaddedservices18)
          ,ConvStringValue(p_new.t_valueaddedservices18)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices19 PTBICDIR_VALUEADDEDSERVICES19
      IF ConvStringValue(p_old.t_valueaddedservices19) !=
         ConvStringValue(p_new.t_valueaddedservices19) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES19
          ,ConvStringValue(p_old.t_valueaddedservices19)
          ,ConvStringValue(p_new.t_valueaddedservices19)
        );
      END IF;
      --ptbicdir.dbt / valueaddedservices20 PTBICDIR_VALUEADDEDSERVICES20
      IF ConvStringValue(p_old.t_valueaddedservices20) !=
         ConvStringValue(p_new.t_valueaddedservices20) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_VALUEADDEDSERVICES20
          ,ConvStringValue(p_old.t_valueaddedservices20)
          ,ConvStringValue(p_new.t_valueaddedservices20)
        );
      END IF;
      --ptbicdir.dbt / extrainformation     PTBICDIR_EXTRAINFORMATION
      IF ConvStringValue(p_old.t_extrainformation) !=
         ConvStringValue(p_new.t_extrainformation) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_EXTRAINFORMATION
          ,ConvStringValue(p_old.t_extrainformation)
          ,ConvStringValue(p_new.t_extrainformation)
        );
      END IF;
      --ptbicdir.dbt / updatedate           PTBICDIR_UPDATEDATE
      IF ConvDateValue(p_old.t_updatedate) !=
         ConvDateValue(p_new.t_updatedate) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_UPDATEDATE
          ,ConvDateValue(p_old.t_updatedate)
          ,ConvDateValue(p_new.t_updatedate)
        );
      END IF;
      --ptbicdir.dbt / physicaladdress      PTBICDIR_PHYSICALADDRESS
      IF ConvStringValue(p_old.t_physicaladdress) !=
         ConvStringValue(p_new.t_physicaladdress) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_PHYSICALADDRESS
          ,ConvStringValue(p_old.t_physicaladdress)
          ,ConvStringValue(p_new.t_physicaladdress)
        );
      END IF;
      --ptbicdir.dbt / location             PTBICDIR_LOCATION
      IF ConvStringValue(p_old.t_location) !=
         ConvStringValue(p_new.t_location) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_LOCATION
          ,ConvStringValue(p_old.t_location)
          ,ConvStringValue(p_new.t_location)
        );
      END IF;
      --ptbicdir.dbt / zipcode              PTBICDIR_ZIPCODE
      IF ConvStringValue(p_old.t_zipcode) !=
         ConvStringValue(p_new.t_zipcode) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_ZIPCODE
          ,ConvStringValue(p_old.t_zipcode)
          ,ConvStringValue(p_new.t_zipcode)
        );
      END IF;
      --ptbicdir.dbt / city                 PTBICDIR_CITY
      IF ConvStringValue(p_old.t_city) !=
         ConvStringValue(p_new.t_city) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_CITY
          ,ConvStringValue(p_old.t_city)
          ,ConvStringValue(p_new.t_city)
        );
      END IF;
      --ptbicdir.dbt / country              PTBICDIR_COUNTRY
      IF ConvStringValue(p_old.t_country) !=
         ConvStringValue(p_new.t_country) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_COUNTRY
          ,ConvStringValue(p_old.t_country)
          ,ConvStringValue(p_new.t_country)
        );
      END IF;
      --ptbicdir.dbt / pob_location         PTBICDIR_POB_LOCATION
      IF ConvStringValue(p_old.t_pob_location) !=
         ConvStringValue(p_new.t_pob_location) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_POB_LOCATION
          ,ConvStringValue(p_old.t_pob_location)
          ,ConvStringValue(p_new.t_pob_location)
        );
      END IF;
      --ptbicdir.dbt / pob_zipcode          PTBICDIR_POB_ZIPCODE
      IF ConvStringValue(p_old.t_pob_zipcode) !=
         ConvStringValue(p_new.t_pob_zipcode) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_POB_ZIPCODE
          ,ConvStringValue(p_old.t_pob_zipcode)
          ,ConvStringValue(p_new.t_pob_zipcode)
        );
      END IF;
      --ptbicdir.dbt / pob_number           PTBICDIR_POB_NUMBER
      IF ConvStringValue(p_old.t_pob_number) !=
         ConvStringValue(p_new.t_pob_number) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_POB_NUMBER
          ,ConvStringValue(p_old.t_pob_number)
          ,ConvStringValue(p_new.t_pob_number)
        );
      END IF;
      --ptbicdir.dbt / pob_country          PTBICDIR_POB_COUNTRY
      IF ConvStringValue(p_old.t_pob_country) !=
         ConvStringValue(p_new.t_pob_country) THEN
        AddItemToPtprmhistArray
        (
           m_partyid
          ,PTBICDIR_POB_COUNTRY
          ,ConvStringValue(p_old.t_pob_country)
          ,ConvStringValue(p_new.t_pob_country)
        );
      END IF;
    END IF;
  END;
  --Процедура вставки в массив историзации из триггера dobjatcor_dbt
  PROCEDURE ObjatcorTIUD
  (
     p_old dobjatcor_dbt%ROWTYPE
    ,p_new dobjatcor_dbt%ROWTYPE
  )
  AS
    m_partyid NUMBER;
    m_valuebefore VARCHAR2(1023);
    m_valueafter  VARCHAR2(1023);
  BEGIN
    BEGIN
      m_partyid := TO_NUMBER(p_new.t_object);
      IF m_partyid IS NOT NULL THEN
        --PARTY_CATEGORY
        m_valuebefore := RSI_RSB_SCRHLP.GetObjGroupName(p_old.t_objecttype, p_old.t_groupid);
        m_valueafter  := RSI_RSB_SCRHLP.GetObjGroupName(p_new.t_objecttype, p_new.t_groupid);

        IF RSI_RSB_SCRHLP.GetObjGroupType(p_old.t_objecttype, p_old.t_groupid) = CHR(0) THEN
          m_valuebefore := m_valuebefore || ' (неисключительная)';
        END IF;

        IF RSI_RSB_SCRHLP.GetObjGroupType(p_new.t_objecttype, p_new.t_groupid) = CHR(0) THEN
          m_valueafter := m_valueafter || ' (неисключительная)';
        END IF;

        IF p_old.t_attrid IS NOT NULL THEN
          m_valuebefore := m_valuebefore || ': ' || TO_CHAR(p_old.t_attrid) || ' (' || TO_CHAR(p_old.t_validfromdate, 'DD.MM.YYYY') || ')';
        END IF;

        IF p_new.t_attrid IS NOT NULL THEN
          m_valueafter  := m_valueafter  || ': ' || TO_CHAR(p_new.t_attrid) || ' (' || TO_CHAR(p_new.t_validfromdate, 'DD.MM.YYYY') || ')';
        END IF;

        AddItemToPtprmhistArray
        (
           m_partyid
          ,PARTY_CATEGORY
          ,m_valuebefore
          ,m_valueafter
        );
      END IF;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
  END;
  --Процедура вставки в массив историзации из триггера dobjlink_dbt
  PROCEDURE ObjlinkTIUD
  (
     p_old dobjlink_dbt%ROWTYPE
    ,p_new dobjlink_dbt%ROWTYPE
  )
  AS
    m_partyid NUMBER;
    m_valuebefore VARCHAR2(1023);
    m_valueafter  VARCHAR2(1023);
  BEGIN
    BEGIN
      m_partyid := TO_NUMBER(p_new.t_objectid);
      IF m_partyid IS NOT NULL THEN
        --PARTY_LINK
        m_valuebefore := RSI_RSB_SCRHLP.GetObjRoleNameAttr(p_old.t_objecttype, p_old.t_groupid);
        m_valueafter  := RSI_RSB_SCRHLP.GetObjRoleNameAttr(p_new.t_objecttype, p_new.t_groupid);

        IF p_old.t_attrid IS NOT NULL THEN
          m_valuebefore := m_valuebefore || ': ' || p_old.t_attrid || ' (' || TO_CHAR(p_old.t_validfromdate, 'DD.MM.YYYY') || ')';
        END IF;

        IF p_new.t_attrid IS NOT NULL THEN
          m_valueafter  := m_valueafter  || ': ' || p_new.t_attrid || ' (' || TO_CHAR(p_new.t_validfromdate, 'DD.MM.YYYY') || ')';
        END IF;

        AddItemToPtprmhistArray
        (
           m_partyid
          ,PARTY_LINK
          ,m_valuebefore
          ,m_valueafter
        );
      END IF;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
  END;
  --Процедура вставки в массив историзации из триггера dnotetext_dbt
  PROCEDURE NotetextTIUD
  (
     p_old dnotetext_dbt%ROWTYPE
    ,p_new dnotetext_dbt%ROWTYPE
  )
  AS
    m_partyid NUMBER;
    m_objecttype NUMBER;
    m_valuebefore VARCHAR2(1023);
    m_valueafter  VARCHAR2(1023);
    m_notetype NUMBER;
    m_stringbefore VARCHAR2(32767);
    m_stringafter  VARCHAR2(32767);
  BEGIN
    BEGIN
      m_partyid    := TO_NUMBER(p_new.t_documentid);
      m_objecttype := TO_NUMBER(p_new.t_objecttype);
      IF m_partyid IS NOT NULL THEN
        --PARTY_NOTE
        --PERSN_NOTE

        m_valuebefore := RSI_RSB_SCRHLP.GetNoteKindName(p_old.t_objecttype, p_old.t_notekind);
        m_valueafter  := RSI_RSB_SCRHLP.GetNoteKindName(p_new.t_objecttype, p_new.t_notekind);
        m_notetype    := RSI_RSB_SCRHLP.GetNoteKindNoteType(p_new.t_objecttype, p_new.t_notekind);

        IF p_old.t_date IS NOT NULL THEN
          m_valuebefore := m_valuebefore || ': ';

          IF m_notetype != 7 THEN
            m_valuebefore := m_valuebefore || CASE m_notetype
                                              WHEN 0  THEN TO_CHAR(RSB_STRUCT.getInt   (TO_BLOB(p_old.t_text)))
                                              WHEN 1  THEN TO_CHAR(RSB_STRUCT.getInt   (TO_BLOB(p_old.t_text)))
                                              WHEN 4  THEN TO_CHAR(RSB_STRUCT.getDouble(TO_BLOB(p_old.t_text)))
                                              WHEN 9  THEN TO_CHAR(RSB_STRUCT.getDate  (TO_BLOB(p_old.t_text)), 'DD.MM.YYYY')
                                              WHEN 10 THEN TO_CHAR(RSB_STRUCT.getTime  (TO_BLOB(p_old.t_text)), 'HH24:MI:SS')
                                              WHEN 25 THEN TO_CHAR(RSB_STRUCT.getMoney (TO_BLOB(p_old.t_text)))
                                              ELSE         TO_CHAR(RSB_STRUCT.getChar  (TO_BLOB(p_old.t_text)))
                                              END;
          ELSE
            m_stringbefore := RSB_STRUCT.getString(TO_BLOB(p_old.t_text));
            m_stringbefore := SUBSTR(m_stringbefore, 1, INSTR(m_stringbefore, CHR(0)) - 1);
            m_valuebefore := m_valuebefore || m_stringbefore;
          END IF;

          m_valuebefore := m_valuebefore || ' (' || TO_CHAR(p_old.t_date, 'DD.MM.YYYY') || ')';

        END IF;

        IF p_new.t_date IS NOT NULL THEN
          m_valueafter := m_valueafter || ': ';

          IF m_notetype != 7 THEN
            m_valueafter := m_valueafter || CASE m_notetype
                                            WHEN 0  THEN TO_CHAR(RSB_STRUCT.getInt   (TO_BLOB(p_new.t_text)))
                                            WHEN 1  THEN TO_CHAR(RSB_STRUCT.getInt   (TO_BLOB(p_new.t_text)))
                                            WHEN 4  THEN TO_CHAR(RSB_STRUCT.getDouble(TO_BLOB(p_new.t_text)))
                                            WHEN 9  THEN TO_CHAR(RSB_STRUCT.getDate  (TO_BLOB(p_new.t_text)), 'DD.MM.YYYY')
                                            WHEN 10 THEN TO_CHAR(RSB_STRUCT.getTime  (TO_BLOB(p_new.t_text)), 'HH24:MI:SS')
                                            WHEN 25 THEN TO_CHAR(RSB_STRUCT.getMoney (TO_BLOB(p_new.t_text)))
                                            ELSE         TO_CHAR(RSB_STRUCT.getChar  (TO_BLOB(p_new.t_text)))
                                            END;
          ELSE
            m_stringafter := RSB_STRUCT.getString(TO_BLOB(p_new.t_text));
            m_stringafter := SUBSTR(m_stringafter, 1, INSTR(m_stringafter, CHR(0)) - 1);
            m_valueafter := m_valueafter || m_stringafter;
          END IF;

          m_valueafter := m_valueafter || ' (' || TO_CHAR(p_new.t_date, 'DD.MM.YYYY') || ')';
        END IF;

        AddItemToPtprmhistArray
        (
           m_partyid
          ,CASE m_objecttype WHEN 3 THEN PARTY_NOTE ELSE PERSN_NOTE END
          ,m_valuebefore
          ,m_valueafter
        );
      END IF;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
  END;
  /*Процедура подготовки временной таблицы для функции получения
  параметров субъкта на заданную дату*/
  PROCEDURE PreparePtPrmHistTmp
  (
     p_PartyID NUMBER
    ,p_Date DATE
  )
  AS
  BEGIN
    DELETE FROM dptprmhist_tmp;
    INSERT INTO dptprmhist_tmp
    (
      t_paramkindid
     ,t_valuebefore
    )
    SELECT
      h.t_paramkindid
     ,h.t_valuebefore
      FROM dptprmhist_dbt h
     WHERE h.t_autokey in (SELECT MIN(p.t_autokey)
                             FROM dptprmhist_dbt p
                             WHERE p.t_partyid = p_PartyID
                                   AND p.t_bankdate > p_Date
                              group by p.t_paramkindid );
  END;
  --Функция получения параметра субъекта типа число
  FUNCTION GetPtParamNumber
  (
     p_ParamKindID NUMBER
    ,p_RetValue OUT NUMBER
  ) RETURN NUMBER
  AS
  BEGIN
    SELECT
      CASE t_valuebefore
        WHEN CHR(1) THEN 0
        ELSE TO_NUMBER(t_valuebefore)
      END INTO p_RetValue
      FROM dptprmhist_tmp
     WHERE t_paramkindid = p_ParamKindID;
    RETURN 0;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN -1;
    WHEN OTHERS THEN RETURN p_ParamKindID;
  END;
  --Функция получения параметра субъекта типа строка
  FUNCTION GetPtParamVarchar2
  (
     p_ParamKindID NUMBER
    ,p_RetValue OUT VARCHAR2
  ) RETURN NUMBER
  AS
  BEGIN
    SELECT
      t_valuebefore INTO p_RetValue
      FROM dptprmhist_tmp
     WHERE t_paramkindid = p_ParamKindID;
    RETURN 0;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN -1;
    WHEN OTHERS THEN RETURN p_ParamKindID;
  END;
  --Функция получения параметра субъекта типа символ
  FUNCTION GetPtParamChar
  (
     p_ParamKindID NUMBER
    ,p_RetValue OUT CHAR
  ) RETURN NUMBER
  AS
  BEGIN
    SELECT
      CASE t_paramkindid
        WHEN PERSN_ISMALE
          THEN CASE WHEN t_valuebefore = PTHIST_PARAM_MAN THEN CHR(88) ELSE CHR(0) END
        ELSE CASE WHEN t_valuebefore = CHR(1) THEN CHR(0) ELSE CHR(88) END
      END INTO p_RetValue
      FROM dptprmhist_tmp
     WHERE t_paramkindid = p_ParamKindID;
    RETURN 0;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN -1;
    WHEN OTHERS THEN RETURN p_ParamKindID;
  END;
  --Функция получения параметра субъекта типа дата
  FUNCTION GetPtParamDate
  (
     p_ParamKindID NUMBER
    ,p_RetValue OUT DATE
  ) RETURN NUMBER
  AS
  BEGIN
    SELECT
      CASE t_valuebefore
        WHEN '00.00.0000' THEN TO_DATE('01.01.0001', 'DD.MM.YYYY')
        WHEN CHR(1) THEN TO_DATE('01.01.0001', 'DD.MM.YYYY')
        ELSE TO_DATE(t_valuebefore, 'DD.MM.YYYY')
      END INTO p_RetValue
      FROM dptprmhist_tmp
     WHERE t_paramkindid = p_ParamKindID;
    RETURN 0;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN -1;
    WHEN OTHERS THEN RETURN p_ParamKindID;
  END;
  --Функция получения параметров субъекта
  PROCEDURE GetPartyBuff
  (
    p_PartyID   IN NUMBER,
    p_Party    OUT dparty_dbt%ROWTYPE
  )
  AS
  BEGIN
    SELECT * INTO p_Party FROM dparty_dbt WHERE t_PartyID = p_PartyID;
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  PROCEDURE GetBankDprtBuff
  (
    p_PartyID   IN NUMBER,
    p_BankDprt OUT dbankdprt_dbt%ROWTYPE
  )
  AS
  BEGIN
    SELECT * INTO p_BankDprt FROM dbankdprt_dbt WHERE t_PartyID = p_PartyID;
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  PROCEDURE GetPtBicDirBuff
  (
    p_PartyID   IN NUMBER,
    p_PtBicDir OUT dptbicdir_dbt%ROWTYPE
  )
  AS
  BEGIN
    SELECT * INTO p_PtBicDir FROM dptbicdir_dbt WHERE t_PartyID = p_PartyID;
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  PROCEDURE GetPersnBuff
  (
    p_PartyID   IN NUMBER,
    p_Persn    OUT dpersn_dbt%ROWTYPE
  )
  AS
  BEGIN
    SELECT * INTO p_Persn FROM dpersn_dbt WHERE t_PersonID = p_PartyID;
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  --Функция получения параметров субъекта на заданную дата
  FUNCTION GetPartyOnDate
  (
    p_PartyID   IN NUMBER
   ,p_Date      IN DATE DEFAULT NULL
   ,p_Party    OUT dparty_dbt%ROWTYPE
   ,p_BankDprt OUT dbankdprt_dbt%ROWTYPE
   ,p_PtBicDir OUT dptbicdir_dbt%ROWTYPE
   ,p_Persn    OUT dpersn_dbt%ROWTYPE
  ) RETURN NUMBER
  AS
    m_Error NUMBER;
  BEGIN
    PreparePtPrmHistTmp(p_PartyID, p_Date);
    GetPartyBuff(p_PartyID, p_Party);
    GetBankDprtBuff(p_PartyID, p_BankDprt);
    GetPtBicDirBuff(p_PartyID, p_PtBicDir);
    GetPersnBuff(p_PartyID, p_Persn);
    IF p_Date IS NULL OR p_Date = TO_DATE('01.01.0001', 'DD.MM.YYYY') THEN
      RETURN 0;
    END IF;
    DECLARE
      CURSOR c_ptprmhist IS
        SELECT * FROM dptprmhist_tmp;
    BEGIN
      FOR v_ptprmhist IN c_ptprmhist LOOP
        m_Error :=
        CASE v_ptprmhist.t_paramkindid
          WHEN PARTY_LOCKED                  THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_Party.t_Locked)    -- Признак ?Субъект закрыт?;FT_CHAR;1
          WHEN PARTY_SHORTNAME               THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Party.t_ShortName)    -- Сокращенное наименование;FT_STRING;20
          WHEN PARTY_NAME                    THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Party.t_Name)    -- Полное наименование;FT_STRING;320
          WHEN PARTY_ADDNAME                 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Party.t_AddName)    -- Дополнительное наименование (для электронных документов);FT_STRING;320
          WHEN PARTY_SUPERIOR                THEN GetPtParamNumber  (v_ptprmhist.t_paramkindid, p_Party.t_Superior)    -- Вышестоящая или объемлющая организация;FT_INT32;0
          WHEN PARTY_NRCOUNTRY               THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Party.t_NRCountry)    -- Страна;FT_STRING;3
          WHEN PARTY_NOTRESIDENT             THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_Party.t_NotResident)    -- Признак нерезидента;FT_CHAR;1
          WHEN PARTY_OFFSHOREZONE            THEN GetPtParamNumber  (v_ptprmhist.t_paramkindid, p_Party.t_OfshoreZone)    -- Оффшорная зона;FT_INT32;0
          WHEN PARTY_OKPO                    THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Party.t_OKPO)    -- Код ОКПО;FT_STRING;15
          --WHEN ADRESS_ADRESS_URD           -- Юридический адрес;FT_STRING;512,
          --WHEN ADRESS_ADRESS_FACT          -- Фактический адрес;FT_STRING;512
          --WHEN ADRESS_ADRESS_MAIL          -- Почтовый адрес;FT_STRING;512
          WHEN PARTY_USERFIELD1              THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Party.t_UserField1)    -- Пользовательское поле 1;FT_STRING;120
          WHEN PARTY_USERFIELD2              THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Party.t_UserField2)    -- Пользовательское поле 2;FT_STRING;40
          WHEN PARTY_USERFIELD3              THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Party.t_UserField3)    -- Пользовательское поле 3;FT_STRING;80
          WHEN PARTY_USERFIELD4              THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Party.t_UserField4)    -- Пользовательское поле 4;FT_STRING;80
          --WHEN INSTITUT_CHARTERDATE        -- Дата регистрации устава;FT_DATE;0
          --WHEN INSTITUT_CAPITALFI          -- Валюта уставного капитала;FT_INT32;0
          --WHEN INSTITUT_DECLARECAPITAL     -- Объявленный уставной капитал;FT_MONEY;0
          --WHEN INSTITUT_REALCAPITAL        -- Фактический уставной капитал;FT_MONEY;0
          WHEN PARTY_ISDOUBLER               THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_Party.t_IsDoubler)    -- Признак фактического дублера;FT_CHAR;1
          WHEN PARTY_MAINPARTYID             THEN GetPtParamNumber  (v_ptprmhist.t_paramkindid, p_Party.t_MainPartyID)    -- Идентификатор субъекта-оригинала;FT_INT32;0
          WHEN PERSN_NAME1                   THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Persn.t_Name1)    -- Фамилия;FT_STRING;24
          WHEN PERSN_NAME2                   THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Persn.t_Name2)    -- Имя;FT_STRING;24
          WHEN PERSN_NAME3                   THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Persn.t_Name3)    -- Отчество;FT_STRING;24
          WHEN PERSN_ISMALE                  THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_Persn.t_IsMale)    -- Пол;FT_CHAR;1
          WHEN PERSN_ETHNOS                  THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Persn.t_Ethnos)    -- Национальность;FT_STRING;24
          WHEN PERSN_BIRSPLASE               THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Persn.t_Birsplase)    -- Место рождения;FT_STRING;80
          WHEN PERSN_REGIONBORN              THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Persn.t_RegionBorn)    -- Код региона места рождения;FT_STRING;11
          WHEN PERSN_RAIONBORN               THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Persn.t_RaionBorn)    -- Район места рождения;FT_STRING;24
          WHEN PERSN_PLACEBORN               THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Persn.t_PlaceBorn)    -- Населенный пункт места рождения;FT_STRING;24
          WHEN PERSN_BORN                    THEN GetPtParamDate    (v_ptprmhist.t_paramkindid, p_Persn.t_Born)    -- Дата рождения;FT_DATE;0
          WHEN PERSN_DEATH                   THEN GetPtParamDate    (v_ptprmhist.t_paramkindid, p_Persn.t_Death)    -- Дата смерти;FT_DATE;0
          WHEN PERSN_PLACEWORK               THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Persn.t_PlaceWork)    -- Место работы и должность;FT_STRING;52
          WHEN PERSN_ISEMPLOYER              THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_Persn.t_IsEmployer)    -- Признак ?Является предпринимателем?;FT_CHAR;1
          WHEN PERSN_LICENCENUMBER           THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Persn.t_LicenceNumber)    -- Номер лицензии;FT_STRING;20
          WHEN PERSN_LICENCEDATE             THEN GetPtParamDate    (v_ptprmhist.t_paramkindid, p_Persn.t_LicenceDate)    -- Дата лицензии;FT_DATE;0
          WHEN PERSN_KATEGOR                 THEN GetPtParamNumber  (v_ptprmhist.t_paramkindid, p_Persn.t_Kategor)    -- Категория населения;FT_INT16;0
          WHEN PERSN_GROUPAUTHOR             THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_Persn.t_GroupAuthor)    -- Признак наличия генеральной доверенности;FT_CHAR;1
          WHEN PERSN_ISLITERATE              THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_Persn.t_IsLiterate)    -- Признак неграмотности;FT_CHAR;1
          WHEN PERSN_GROUPWIL                THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_Persn.t_GroupWil)    -- Признак наличия группового завещания;FT_CHAR;1
          WHEN PERSN_SPECIALACCESS           THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_Persn.t_SpecialAccess)    -- Признак специального доступа к клиенту;FT_CHAR;1
          WHEN PERSN_VZAIMOSVYAZANNIY        THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_Persn.t_Vzaimosvyazanniy)    -- Признак взаимосвязанного клиента;FT_CHAR;1
          WHEN PERSN_OFFSHOERRESIDENT        THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_Persn.t_Offshoreresident)    -- Признак резидента оффшорной зоны;FT_CHAR;1
          WHEN PERSN_PENSCARDNUMBER          THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_Persn.t_PensCardNumber)    -- Номер пенсионного удостоверения;FT_STRING;15
          WHEN PERSN_PENSCARDDATE            THEN GetPtParamDate    (v_ptprmhist.t_paramkindid, p_Persn.t_PensCardDate)    -- Дата выдачи пенсионного удостоверения;FT_DATE;0
          WHEN BANKDPRT_BIC_RCC              THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_BankDprt.t_BIC_RCC)    -- Банк: БИК РКЦ;FT_STRING;9
          WHEN BANKDPRT_REGION               THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_BankDprt.t_Region)    -- Банк: Регион по классификации ЦБ;FT_STRING;5
          WHEN BANKDPRT_PLACE                THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_BankDprt.t_Place)    -- Банк: Вид населенного пункта по классификации ЦБ;FT_STRING;5
          WHEN BANKDPRT_CHECKALG             THEN GetPtParamNumber  (v_ptprmhist.t_paramkindid, p_BankDprt.t_CheckAlg)    -- Банк: Алгоритм проверки счета;FT_INT16;0
          WHEN BANKDPRT_CHECKDATA            THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_BankDprt.t_CheckData)    -- Банк: Данные проверки счета;FT_STRING;12
          WHEN BANKDPRT_BLNC_RCC             THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_BankDprt.t_Blnc_RCC)    -- Банк: Балансовый счет в РКЦ;FT_STRING;3
          WHEN BANKDPRT_LOCK                 THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_BankDprt.t_Lock)    -- Банк: Признак блокировки;FT_CHAR;1
          WHEN BANKDPRT_CORACC               THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_BankDprt.t_CorAcc)    -- Банк: Номер корреспондентского счета;FT_STRING;25
          WHEN BANKDPRT_REPT                 THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_BankDprt.t_Rept)    -- Банк: Признак отдельного перечня;FT_CHAR;1
          WHEN BANKDPRT_VKEY                 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_BankDprt.t_VKey)    -- Банк: Уникальный ключ по ЦБ;FT_STRING;8
          WHEN BANKDPRT_NOTBANK              THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_BankDprt.t_NotBank)    -- Банк: Признак небанковской кредитной организации;FT_CHAR;1
          WHEN BANKDPRT_PAYMENTKIND          THEN GetPtParamChar    (v_ptprmhist.t_paramkindid, p_BankDprt.t_PaymentKind)    -- Банк: Вид платежа;FT_CHAR;1
          WHEN BANKDPRT_UERTYPE              THEN GetPtParamNumber  (v_ptprmhist.t_paramkindid, p_BankDprt.t_UERType)    -- Банк: Тип банка;FT_INT16;0
          WHEN BANKDPRT_UER                  THEN GetPtParamNumber  (v_ptprmhist.t_paramkindid, p_BankDprt.t_UER)    -- Банк: Участник/пользователь системы расчетов;FT_INT16;0
          WHEN BANKDPRT_REAL                 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_BankDprt.t_Real)    -- Банк: Ограничение участия в расчетах;FT_STRING;4
          WHEN BANKDPRT_CONTROLDATE          THEN GetPtParamDate    (v_ptprmhist.t_paramkindid, p_BankDprt.t_ControlDate)    -- Банк: Дата контроля;FT_DATE;0
          WHEN PTBICDIR_INSTITUTIONNAME      THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_InstitutionName)    -- SWIFT: Наименование финансового института;FT_STRING;105
          WHEN PTBICDIR_BRANCHINFORMATION    THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_BranchInformation)    -- SWIFT: Наименование отделения;FT_STRING;70
          WHEN PTBICDIR_SUBTYPEINDICATION    THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_SubtypeIndication)    -- SWIFT: Код типа участника;FT_STRING;4
          WHEN PTBICDIR_VALUEADDEDSERVICES1  THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices1)    -- SWIFT: Дополнительный сервис 1;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES2  THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices2)    -- SWIFT: Дополнительный сервис 2;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES3  THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices3)    -- SWIFT: Дополнительный сервис 3;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES4  THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices4)    -- SWIFT: Дополнительный сервис 4;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES5  THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices5)    -- SWIFT: Дополнительный сервис 5;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES6  THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices6)    -- SWIFT: Дополнительный сервис 6;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES7  THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices7)    -- SWIFT: Дополнительный сервис 7;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES8  THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices8)    -- SWIFT: Дополнительный сервис 8;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES9  THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices9)    -- SWIFT: Дополнительный сервис 9;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES10 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices10)    -- SWIFT: Дополнительный сервис 10;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES11 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices11)    -- SWIFT: Дополнительный сервис 11;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES12 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices12)    -- SWIFT: Дополнительный сервис 12;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES13 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices13)    -- SWIFT: Дополнительный сервис 13;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES14 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices14)    -- SWIFT: Дополнительный сервис 14;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES15 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices15)    -- SWIFT: Дополнительный сервис 15;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES16 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices16)    -- SWIFT: Дополнительный сервис 16;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES17 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices17)    -- SWIFT: Дополнительный сервис 17;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES18 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices18)    -- SWIFT: Дополнительный сервис 18;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES19 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices19)    -- SWIFT: Дополнительный сервис 19;FT_STRING;3
          WHEN PTBICDIR_VALUEADDEDSERVICES20 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ValueAddedServices20)    -- SWIFT: Дополнительный сервис 20;FT_STRING;3
          WHEN PTBICDIR_EXTRAINFORMATION     THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ExtraInformation)    -- SWIFT: Дополнительная информация;FT_STRING;35
          WHEN PTBICDIR_UPDATEDATE           THEN GetPtParamDate    (v_ptprmhist.t_paramkindid, p_PtBicDir.t_UpdateDate)    -- SWIFT: Дата обновления;FT_DATE;0
          WHEN PTBICDIR_PHYSICALADDRESS      THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_PhysicalAddress)    -- SWIFT: Физический адрес;FT_STRING;140
          WHEN PTBICDIR_LOCATION             THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_Location)    -- SWIFT: Адрес;FT_STRING;90
          WHEN PTBICDIR_ZIPCODE              THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_ZipCode)    -- SWIFT: Индекс;FT_STRING;15
          WHEN PTBICDIR_CITY                 THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_City)    -- SWIFT: Город;FT_STRING;35
          WHEN PTBICDIR_COUNTRY              THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_Country)    -- SWIFT: Страна;FT_STRING;70
          WHEN PTBICDIR_POB_LOCATION         THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_Pob_Location)    -- SWIFT: Адрес почтового ящика;FT_STRING;90
          WHEN PTBICDIR_POB_ZIPCODE          THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_Pob_ZipCode)    -- SWIFT: Индекс почтового ящика;FT_STRING;15
          WHEN PTBICDIR_POB_NUMBER           THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_Pob_Number)    -- SWIFT: Номер почтового ящика;FT_STRING;35
          WHEN PTBICDIR_POB_COUNTRY          THEN GetPtParamVarchar2(v_ptprmhist.t_paramkindid, p_PtBicDir.t_Pob_Country)    -- SWIFT: Страна почтового ящика;FT_STRING;70
          ELSE 0
        END;
        IF m_Error > 0 THEN RETURN m_Error; END IF;
      END LOOP;
    END;
    RETURN 0;
  END;
  --Функция получения кода субъекта на заданную дату
  FUNCTION GetPartyCodeOnDate
  (
    p_PartyID  IN NUMBER
   ,p_CodeKind IN NUMBER
   ,p_Date     IN DATE DEFAULT NULL
   ,p_Owner    OUT NUMBER
  )
  RETURN VARCHAR2
  AS
    m_Code dobjcode_dbt.t_code%TYPE;
    m_ParentCode ddp_dep_dbt.t_parentcode%TYPE;
  BEGIN
    p_Owner := 0;
    IF p_Date IS NULL OR p_Date = TO_DATE('01.01.0001', 'DD.MM.YYYY') THEN
      BEGIN
        SELECT t_code INTO m_Code
          FROM dobjcode_dbt
         WHERE t_objectid = p_PartyID
           AND t_objecttype = 3
           AND t_codekind = p_CodeKind
           AND t_bankclosedate = TO_DATE('01.01.0001', 'DD.MM.YYYY')
           AND ROWNUM = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
      END;
    ELSE
      BEGIN
        SELECT t_code INTO m_Code
          FROM dobjcode_dbt
         WHERE t_objectid = p_PartyID
           AND t_objecttype = 3
           AND t_codekind = p_CodeKind
           AND p_Date >= t_bankdate
           AND p_Date < DECODE(t_bankclosedate, TO_DATE('01.01.0001', 'DD.MM.YYYY'), TO_DATE('31.12.9999', 'DD.MM.YYYY'), t_bankclosedate)
           AND ROWNUM = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
      END;
    END IF;
    IF m_Code IS NULL THEN
      BEGIN
        SELECT t_parentcode INTO m_ParentCode
          FROM ddp_dep_dbt
         WHERE t_partyid = p_PartyID;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
      END;
      IF m_ParentCode IS NULL THEN
        BEGIN
          SELECT t_superior INTO p_Owner
            FROM dparty_dbt
           WHERE t_partyid = p_PartyID;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN NULL;
        END;
      ELSIF m_ParentCode != 0 THEN
        BEGIN
          SELECT t_partyid INTO p_Owner
            FROM ddp_dep_dbt
           WHERE t_code = m_ParentCode;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN NULL;
        END;
      END IF;
      IF p_Owner != 0 THEN
        m_Code := GetPartyCodeOnDate
        (
          p_Owner
         ,p_CodeKind
         ,p_Date
         ,p_Owner
        );
      END IF;
    ELSE
      p_Owner := p_PartyID;
    END IF;
    RETURN m_Code;
  END;

  PROCEDURE CalcOwnPrcNode
  (
    p_PtBeneOwnerRecID IN NUMBER
   ,p_ID IN NUMBER
   ,p_OwnWay IN NUMBER
   ,p_CapitalPrc IN FLOAT
   ,p_Bene3dPartyID IN NUMBER
  )
  IS
    CURSOR c_ptbenownstr IS
      SELECT t_ID, t_ParentID, t_OwnWay, t_CapitalPrc, t_PtBeneOwnerRecID, t_Bene3dPartyID
        FROM dptbenownstr_tmp
       WHERE t_ParentID = p_ID AND t_PtBeneOwnerRecID = p_PtBeneOwnerRecID;

    m_CurrOwnPrc FLOAT;
  BEGIN

    FOR v_ptbenownstr IN c_ptbenownstr LOOP
      CalcOwnPrcNode(v_ptbenownstr.t_PtBeneOwnerRecID, v_ptbenownstr.t_ID, v_ptbenownstr.t_OwnWay, v_ptbenownstr.t_CapitalPrc, v_ptbenownstr.t_Bene3dPartyID);
    END LOOP;

    IF p_Bene3dPartyID != -1 THEN
      BEGIN
        SELECT NVL(SUM(t_OwnPrc), 0) INTO m_CurrOwnPrc
          FROM dptbenownstr_tmp
         WHERE t_ParentID = p_ID AND t_PtBeneOwnerRecID = p_PtBeneOwnerRecID;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN m_CurrOwnPrc := 0;
      END;
      m_CurrOwnPrc := CASE WHEN p_OwnWay = 0 /*контроль*/ THEN m_CurrOwnPrc ELSE m_CurrOwnPrc * p_CapitalPrc / 100 END;
    ELSE
      m_CurrOwnPrc := CASE WHEN p_OwnWay = 0 /*контроль*/ THEN 100 ELSE p_CapitalPrc END;
    END IF;

    IF m_CurrOwnPrc > 100 THEN
      m_CurrOwnPrc := 100;
    END IF;

    UPDATE dptbenownstr_tmp SET t_OwnPrc = m_CurrOwnPrc WHERE t_ID = p_ID AND t_PtBeneOwnerRecID = p_PtBeneOwnerRecID;
  END;

  --Процедура вычисления % владения субъекта БВ в объекте БВ
  PROCEDURE CalcOwnPrc
  IS
    CURSOR c_ptbenownstr IS
      SELECT t_ID, t_ParentID, t_OwnWay, t_CapitalPrc, t_PtBeneOwnerRecID, t_Bene3dPartyID
        FROM dptbenownstr_tmp
       WHERE t_ParentID = -1;

    m_PartyID NUMBER;
    m_BeneOwnerID NUMBER;
    m_PrntOwnPrc FLOAT;
    m_CurrOwnPrc FLOAT;
  BEGIN

    FOR v_ptbenownstr IN c_ptbenownstr LOOP
      CalcOwnPrcNode(v_ptbenownstr.t_PtBeneOwnerRecID, v_ptbenownstr.t_ID, v_ptbenownstr.t_OwnWay, v_ptbenownstr.t_CapitalPrc, v_ptbenownstr.t_Bene3dPartyID);

      BEGIN
        SELECT t_OwnPrc INTO m_CurrOwnPrc
          FROM dptbenownstr_tmp
         WHERE t_PtBeneOwnerRecID = v_ptbenownstr.t_PtBeneOwnerRecID
           AND t_ID = v_ptbenownstr.t_ID;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN m_CurrOwnPrc := 0;
      END;

      BEGIN
        SELECT t_PartyID, t_BeneOwnerID INTO m_PartyID, m_BeneOwnerID
          FROM dptbeneowner_dbt
         WHERE t_ID = v_ptbenownstr.t_PtBeneOwnerRecID;

        UPDATE dptbeneowner_dbt
           SET t_OwnPrc = m_CurrOwnPrc
         WHERE t_PartyID = m_PartyID
           AND t_BeneOwnerID = m_BeneOwnerID;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
      END;
    END LOOP;

  END;

  FUNCTION GetPartyBankruptStatus
  (
    PartyID   IN NUMBER,
    LegalForm IN NUMBER, -- уже не используется
    OnDate    IN DATE
  ) RETURN NUMBER
  AS
    d DATE;
  BEGIN
    RETURN GetPartyBankruptStatus_Ex(PartyID, OnDate, d);
  END;

  FUNCTION GetPartyBankruptStatus_Ex
  (
    PartyID   IN NUMBER,
    OnDate    IN DATE,
    EventDate OUT DATE
  ) RETURN NUMBER
  AS
    Status    NUMBER;
    v_EventDate DATE;
  begin
    Status := 0;
    v_EventDate := TO_DATE('01.01.0001', 'DD.MM.YYYY');

    BEGIN
        SELECT T_BANKRUPTSTATUS, T_EVENTDATE INTO Status, v_EventDate
          FROM (  SELECT BH.T_BANKRUPTSTATUS, BH.T_EVENTDATE
                    FROM DPTBANKRUPTHIST_DBT BH, DLLVALUES_DBT LV2
                   WHERE     BH.T_PARTYID = PartyID
                         AND BH.T_EVENTDATE =
                                (SELECT MAX (BH2.T_EVENTDATE)
                                   FROM DPTBANKRUPTHIST_DBT BH2
                                  WHERE     BH2.T_PARTYID = BH.T_PARTYID
                                        AND BH2.T_EVENTDATE <= OnDate)
                         AND LV2.T_ELEMENT = BH.T_BANKRUPTSTATUS
                         AND LV2.T_LIST = 3564
                ORDER BY LV2.T_FLAG DESC)
                WHERE ROWNUM = 1;

    EventDate := v_EventDate;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN Status := 10;
    END;

    RETURN Status;
  END;

    --Функция исключения организационно-правовой формы (ОПФ)
  FUNCTION RemoveOPF(p_value IN VARCHAR2) RETURN VARCHAR2
  AS
    OrigName    VARCHAR2(320);
    Str         VARCHAR2(320);
    i           NUMBER;
    FirstWord   VARCHAR2(320);
    Flag        BOOLEAN;
  BEGIN
    Flag := FALSE;

    OrigName := p_value;
    Str := Trim(OrigName);

    i := INSTRB(Str, ' ');

    IF i > 0 THEN

      FirstWord := SUBSTR(Str, 1, i - 1);

      FOR k IN OPFList.FIRST .. OPFList.LAST
      LOOP
        IF UPPER(OPFList(K)) = UPPER(FirstWord) THEN
          Flag := TRUE;
        END IF;
      END LOOP;

      IF Flag = TRUE THEN

        OrigName := SUBSTR(Str, i + 1);

      END IF;

    END IF;

    RETURN OrigName;
  END;

  FUNCTION bitor(x NUMBER, y NUMBER) RETURN NUMBER DETERMINISTIC
   IS
   BEGIN
    RETURN x - bitand(x, y) + y;
   END;

  FUNCTION PtCertainCategory(PartyID IN NUMBER, pFlags OUT NUMBER)
  RETURN NUMBER
  AS
    Flag      NUMBER := 0;
    LegalForm NUMBER := 0;
    Cnt       NUMBER := 0;
  BEGIN
    SELECT T_LEGALFORM INTO LegalForm FROM DPARTY_DBT WHERE T_PARTYID = PartyID;

    IF LegalForm <> 2 THEN
        RETURN 12536;
    END IF;

    SELECT COUNT(*) INTO Cnt FROM (SELECT T_PARTYID FROM DOFFCERCAT_DBT WHERE T_PARTYID = PartyID UNION
        SELECT T_PARTYID FROM DDROFFICERCAT_DBT WHERE T_PARTYID = PartyID OR T_OFFICIALID = PartyID);

    IF Cnt = 0 THEN
        Flag := PT_CC_NOT_APPLICABLE;
    END IF;

    IF Flag = 0 THEN
        SELECT COUNT(T_PARTYID) INTO Cnt FROM DOFFCERCAT_DBT
            WHERE T_PARTYID = PartyID AND T_CATEGORY = '1';

        IF Cnt <> 0 THEN
            Flag := bitor(Flag, PT_CC_FOREIGN_PUBLIC);
        END IF;

        SELECT COUNT(T_ID) INTO Cnt
          FROM DDROFFICERCAT_DBT t1
         WHERE     T_PARTYID = PartyID
               AND (   (    T_PARTYID = T_OFFICIALID
                        AND T_COMMENT LIKE '%ИПДЛ%')
                    OR EXISTS
                          (SELECT 1
                             FROM DOFFCERCAT_DBT
                            WHERE    T_PARTYID = t1.T_PARTYID AND T_CATEGORY = '1'));
        IF Cnt <> 0 THEN
            Flag := bitor(Flag, PT_CC_REF_PUBLIC);
        END IF;

        SELECT COUNT(T_PARTYID) INTO Cnt FROM DOFFCERCAT_DBT WHERE T_PARTYID = PartyID AND T_CATEGORY <> '1';

        IF Cnt <> 0 THEN
            Flag := bitor(Flag, PT_CC_CERTAIN_CATEGORY);
        END IF;

        SELECT COUNT (t1.T_PARTYID) INTO Cnt
          FROM  DDROFFICERCAT_DBT t1
         WHERE     t1.T_PARTYID = PartyID
            AND t1.T_ID NOT IN (SELECT COUNT(T_ID)
          FROM DDROFFICERCAT_DBT t2
         WHERE     t2.T_PARTYID = PartyID
               AND (   (    t2.T_PARTYID = t2.T_OFFICIALID
                        AND t2.T_COMMENT LIKE '%ИПДЛ%')
                    OR EXISTS
                          (SELECT 1
                             FROM DOFFCERCAT_DBT
                            WHERE    T_PARTYID = t2.T_PARTYID AND T_CATEGORY = '1')));

        IF Cnt <> 0 THEN
            Flag := bitor(Flag, PT_CC_REF_OFFICIAL_CERT);
        END IF;
    END IF;

    pFlags := Flag;
    RETURN 0;
  END;

  FUNCTION FilterPtCertainCategory(PartyID IN NUMBER,
    HasForeignPublic IN CHAR,
    RefPublic IN CHAR,
    CertainCategory IN CHAR,
    RefOfficialCert IN CHAR) RETURN CHAR
  IS
    Flags NUMBER := 0;
    Stat CHAR := CHR(0);
  BEGIN
    IF PtCertainCategory(PartyID, Flags) = 0 THEN
        IF (HasForeignPublic = CHR(88))
            AND (bitand(Flags, PT_CC_FOREIGN_PUBLIC) = PT_CC_FOREIGN_PUBLIC) THEN
            Stat := CHR(88);
        END IF;

        IF (RefPublic = CHR(88))
            AND (bitand(Flags, PT_CC_REF_PUBLIC) = PT_CC_REF_PUBLIC) THEN
            Stat := CHR(88);
        END IF;

        IF (CertainCategory = CHR(88))
            AND (bitand(Flags, PT_CC_CERTAIN_CATEGORY) = PT_CC_CERTAIN_CATEGORY) THEN
            Stat := CHR(88);
        END IF;

        IF (RefOfficialCert = CHR(88))
            AND (bitand(Flags, PT_CC_REF_OFFICIAL_CERT) = PT_CC_REF_OFFICIAL_CERT) THEN
            Stat := CHR(88);
        END IF;
    END IF;

    RETURN Stat;
  END;

  PROCEDURE InsertFmChkPtHist( p_PartyID IN NUMBER, p_LastCheckTerrDate IN DATE)
  IS
     v_count NUMBER := 0;
     --v_SysTime DATE;
  BEGIN
     --v_SysTime := TO_DATE('01.01.0001 ' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD.MM.YYYY HH24:MI:SS');


    SELECT COUNT(1) INTO v_count
    FROM DFMPARTYCHKHIST_DBT
    WHERE T_PARTYID = p_PartyID
      AND T_DATE = p_LastCheckTerrDate
      --AND T_TIME = v_SysTime
      AND T_CHECKLIST = 1;

    IF v_count <> 0 THEN
      DELETE FROM DFMPARTYCHKHIST_DBT
      WHERE T_PARTYID = p_PartyID
        AND T_DATE = p_LastCheckTerrDate
        --AND T_TIME = v_SysTime
        AND T_CHECKLIST = 1;

      DELETE FROM DFMKFMCHKHIST_DBT
      WHERE T_PARTYID = p_PartyID
        AND T_DATE = p_LastCheckTerrDate;
        --AND T_TIME = v_SysTime;
    END IF;

    INSERT INTO DFMPARTYCHKHIST_DBT (T_PARTYID, T_DATE, /*T_TIME,*/ T_CHECKLIST, T_LISTNUMBER, T_LISTDATE, T_PROCVALUE)
    SELECT ptc.T_PARTYID, p_LastCheckTerrDate, /*v_SysTime,*/  ptc.T_CHECKLIST, prm.T_LISTNUMBER, prm.T_LISTDATE,  ptc.T_PROCVALUE
    FROM DFMPARTYCHK_DBT ptc, DFMIMPKFMPRM_DBT prm
    WHERE ptc.t_PartyID = p_PartyID;

    SELECT COUNT(1) INTO v_count
    FROM DFMKFMCHK_DBT
    WHERE T_PARTYID = p_PartyID;

    IF v_count <> 0 THEN

      INSERT INTO DFMKFMCHKHIST_DBT
       (T_ID,
        T_PARTYID,
        T_DATE,
        --T_TIME,
        T_TERRORISTID,
        T_TERRNUMBER,
        T_TYPE,
        T_TERRORTYPE,
        T_TERRNAME,
        T_PROCVALUE,
        T_HASREGNUM,
        T_HASDATEREG,
        T_HASINN,
        T_HASOKPO,
        T_HASDOCSERIES,
        T_HASDOCNUMBER,
        T_HASDOCDATE,
        T_HASBIRTHDATE,
        T_HASREGCOUNTRY,
        T_HASPLACECOUNTRY)
      SELECT
        0,
        CHK.T_PARTYID,
        p_LastCheckTerrDate,
        --v_SysTime,
        CHK.T_TERRORISTID,
        KFM.T_NUMBER,
        KFM.T_TYPE,
        KFM.T_TERRORTYPE,
        (SELECT T_NAME FROM DTERR_KFM_AKA_DBT AKA WHERE AKA.T_TERRORISTID = KFM.T_TERRORISTID AND ROWNUM <= 1),
        CHK.T_PROCVALUE,
        T_HASREGNUM,
        T_HASDATEREG,
        T_HASINN,
        T_HASOKPO,
        T_HASDOCSERIES,
        T_HASDOCNUMBER,
        T_HASDOCDATE,
        T_HASBIRTHDATE,
        T_HASREGCOUNTRY,
        T_HASPLACECOUNTRY
      FROM DFMKFMCHK_DBT CHK, DTERR_KFM_DBT KFM
      WHERE CHK.T_PARTYID = p_PartyID
        AND KFM.T_TERRORISTID =CHK.T_TERRORISTID;

    END IF;

  END;


  PROCEDURE InsertFmKfmChkHist( p_PartyID IN NUMBER, p_LastCheckTerrDate IN DATE, p_fmkfmchk dfmkfmchk_dbt%ROWTYPE)
  IS
     v_count NUMBER := 0;
     --v_SysTime DATE;
  BEGIN
     --v_SysTime := TO_DATE('01.01.0001 ' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD.MM.YYYY HH24:MI:SS');

     INSERT INTO DFMKFMCHKHIST_DBT
      (T_ID,
       T_PARTYID,
       T_DATE,
       --T_TIME,
       T_TERRORISTID,
       T_TERRNUMBER,
       T_TYPE,
       T_TERRORTYPE,
       T_TERRNAME,
       T_PROCVALUE,
       T_HASREGNUM,
       T_HASDATEREG,
       T_HASINN,
       T_HASOKPO,
       T_HASDOCSERIES,
       T_HASDOCNUMBER,
       T_HASDOCDATE,
       T_HASBIRTHDATE,
       T_HASREGCOUNTRY,
       T_HASPLACECOUNTRY)
     SELECT
       0,
       p_fmkfmchk.T_PARTYID,
       p_LastCheckTerrDate,
       /*v_SysTime,*/
       p_fmkfmchk.T_TERRORISTID,
       KFM.T_NUMBER,
       KFM.T_TYPE,
       KFM.T_TERRORTYPE,
       (SELECT T_NAME FROM DTERR_KFM_AKA_DBT AKA WHERE AKA.T_TERRORISTID = KFM.T_TERRORISTID AND ROWNUM <= 1),
       p_fmkfmchk.T_PROCVALUE,
       p_fmkfmchk.T_HASREGNUM,
       p_fmkfmchk.T_HASDATEREG,
       p_fmkfmchk.T_HASINN,
       p_fmkfmchk.T_HASOKPO,
       p_fmkfmchk.T_HASDOCSERIES,
       p_fmkfmchk.T_HASDOCNUMBER,
       p_fmkfmchk.T_HASDOCDATE,
       p_fmkfmchk.T_HASBIRTHDATE,
       p_fmkfmchk.T_HASREGCOUNTRY,
       p_fmkfmchk.T_HASPLACECOUNTRY
     FROM DTERR_KFM_DBT KFM
     WHERE KFM.T_TERRORISTID = p_fmkfmchk.T_TERRORISTID;
  END;


  PROCEDURE InsertFmPartyChkHist( p_PartyID IN NUMBER, p_LastCheckTerrDate IN DATE, p_fmpartychk dfmpartychk_dbt%ROWTYPE)
  IS
     v_count NUMBER := 0;
     --v_SysTime DATE;
  BEGIN
     --v_SysTime := TO_DATE('01.01.0001 ' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD.MM.YYYY HH24:MI:SS');

     INSERT INTO DFMPARTYCHKHIST_DBT (T_PARTYID, T_DATE, /*T_TIME,*/ T_CHECKLIST, T_LISTNUMBER, T_LISTDATE, T_PROCVALUE)
     SELECT p_fmpartychk.T_PARTYID, p_LastCheckTerrDate, /*v_SysTime,*/  p_fmpartychk.T_CHECKLIST, prm.T_LISTNUMBER, prm.T_LISTDATE,  p_fmpartychk.T_PROCVALUE
     FROM DFMIMPKFMPRM_DBT prm;


  END;

  FUNCTION XCOMPL_CheckBirthDate(p_BirthDate IN VARCHAR, p_PersonBorn IN DATE)
  RETURN NUMBER
  IS
    m_Stat NUMBER := 1;
    BirthDay NUMBER := 0;
    BirthMonth NUMBER := 0;
    BirthYear NUMBER := 0;
    p_BirthDay VARCHAR2(2) := CHR(1);
    p_BirthMonth VARCHAR2(2) := CHR(1);
    p_BirthYear VARCHAR2(4) := CHR(1);
  BEGIN
    IF TRIM(p_PersonBorn) = TRIM(TO_DATE('0001/01/01', 'YYYY/MM/DD')) THEN
      m_Stat := 0;
    ELSE
      IF LENGTH(p_BirthDate) = 10 THEN -- dd.MM.yyyy
        p_BirthDay := substr(p_BirthDate, 1, 2);
        p_BirthMonth := substr(p_BirthDate, 4, 2);
        p_BirthYear := substr(p_BirthDate, 7);
      ELSIF LENGTH(p_BirthDate) = 7 THEN -- MM.yyyy
        p_BirthMonth := substr(p_BirthDate, 1, 2);
        p_BirthYear := substr(p_BirthDate, 4);
      ELSE
        p_BirthYear := substr(p_BirthDate, 1);
      END IF;

      IF p_BirthDay <> CHR(1) AND p_BirthDay <> '00' THEN
        BirthDay := TO_NUMBER(p_BirthDay);
      END IF;

      IF p_BirthMonth <> CHR(1) AND p_BirthMonth <> '00' THEN
        BirthMonth := TO_NUMBER(p_BirthMonth);
      END IF;

      IF p_BirthYear <> CHR(1) AND p_BirthMonth <> '0000' THEN
        BirthYear := TO_NUMBER(p_BirthYear);
      END IF;

      IF (BirthDay <> 0) AND (BirthMonth <> 0) AND (BirthYear <> 0) THEN
        IF TRIM(p_PersonBorn) = TRIM(TO_DATE(BirthDay || '.' || BirthMonth || '.' || BirthYear, 'dd.MM.yyyy')) THEN
          m_Stat := 0;
        END IF;
      ELSIF (BirthDay = 0) AND (BirthMonth <> 0) AND (BirthYear <> 0) THEN
        IF (EXTRACT (MONTH FROM p_PersonBorn) = BirthMonth) AND (EXTRACT (YEAR FROM p_PersonBorn) = BirthYear) THEN
          m_Stat := 0;
        END IF;
      ELSIF (BirthDay = 0) AND (BirthMonth = 0) AND (BirthYear <> 0) THEN
        IF EXTRACT (YEAR FROM p_PersonBorn) = BirthYear THEN
          m_Stat := 0;
        END IF;
      ELSIF (BirthDay = 0) AND (BirthMonth = 0) AND (BirthYear = 0) THEN
        m_Stat := 0;
      END IF;
    END IF;

    RETURN m_Stat;
  EXCEPTION WHEN OTHERS THEN
    RETURN 1;
  END;

  FUNCTION XCOMPL_NormalizeString(p_Str IN VARCHAR2, p_CaseInsensetive IN NUMBER)
  RETURN VARCHAR2
  IS
    m_Tmp VARCHAR2(200);
  BEGIN
    m_Tmp := p_Str;
    IF p_CaseInsensetive = 1 THEN
      m_Tmp := UPPER(p_Str);
    END IF;

    RETURN TRIM(regexp_replace(m_Tmp, '[[:space:]]+', chr(32)));
  END;
END RSI_RSBPARTY;
/
