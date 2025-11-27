CREATE OR REPLACE PACKAGE BODY rshb_reserve_or
IS

  --определить тип эмитента/контрагента
  FUNCTION GetIssuerType (pPartyID in integer) return varchar2
  is
    v_partyid integer := 0;
    v_legalform integer := -1;
  BEGIN
  
    --Центральный банк
    begin
      select t_partyid into v_partyid from dpartyown_dbt where t_partykind = 29 and t_partyid = pPartyID;
    exception
      when no_data_found then
        null;
      when others then
        return chr(0);
    end;
  
    if (v_partyid > 0) then return CONTRACTOR_SOVEREIGN; end if;
  
    --Банк
    begin
      select t_partyid into v_partyid from dpartyown_dbt where t_partykind = 2 and t_partyid = pPartyID;
    exception
      when no_data_found then
        null;
      when others then
        return chr(0);
    end;
    
    if (v_partyid > 0) then return CONTRACTOR_BANK; end if;

    --орган гос. власти
    begin
      select t_partyid into v_partyid from dpartyown_dbt where t_partykind = 15 and t_partyid = pPartyID;
    exception
      when no_data_found then
        null;
      when others then
        return chr(0);
    end;
    
    if (v_partyid > 0) then return CONTRACTOR_SOVEREIGN; end if;
    
    --Орган гос.власти субъекта
    begin
      select t_partyid into v_partyid from dpartyown_dbt where t_partykind = 16 and t_partyid = pPartyID;
    exception
      when no_data_found then
        null;
      when others then
        return chr(0);
    end;
    
    if (v_partyid > 0) then return CONTRACTOR_SUBSOVEREIGN; end if;
    
    --Орган местной власти
    begin
      select t_partyid into v_partyid from dpartyown_dbt where t_partykind = 17 and t_partyid = pPartyID;
    exception
      when no_data_found then
        null;
      when others then
        return chr(0);
    end;
    
    if (v_partyid > 0) then return CONTRACTOR_SUBSOVEREIGN; end if;
    
    begin
      select t_legalform into v_legalform from dparty_dbt where t_partyid = pPartyID;
    exception
      when others then
        return chr(0);
    end;

    if (v_legalform = 1) then return CONTRACTOR_CORPORATE; else return chr(0); end if;

  exception
    when others then
      return chr(0);       
  end GetIssuerType;
    
  --
  FUNCTION GetSubjectRating (p_partyid      IN INTEGER,
                             p_date         IN DATE,
                             p_ratingid    OUT INTEGER,
                             p_ratingname  OUT VARCHAR2,
                             p_agencyid    OUT INTEGER,
                             p_agencyname  OUT VARCHAR2,
                             p_entrynumber OUT INTEGER
                            ) return INTEGER
  is
  BEGIN

/*
      select mainattr.t_attrid, mainattr.t_name, parentattr.t_attrid, parentattr.t_name, mainattr.t_intattr
      into p_ratingid, p_ratingname, p_agencyid, p_agencyname, p_entrynumber
      from dobjatcor_dbt atcor
      join dobjattr_dbt mainattr on atcor.t_objecttype = mainattr.t_objecttype and
                                    atcor.t_groupid = mainattr.t_groupid and
                                    atcor.t_attrid = mainattr.t_attrid and
                                    mainattr.t_parentid in (110, 210, 310)
      join dobjattr_dbt parentattr on mainattr.t_objecttype = parentattr.t_objecttype and
                                      mainattr.t_groupid = parentattr.t_groupid and
                                      mainattr.t_parentid = parentattr.t_attrid
      where atcor.t_objecttype = 3 and
            atcor.t_groupid = 19 and
            p_date between atcor.t_validfromdate and atcor.t_validtodate and
            atcor.t_object = lpad(p_partyid, 10, 0) and
            rownum = 1
      order by atcor.t_validfromdate desc;
*/

select  t_attrid, t_name, attrnum, attrname, Levelid 
 into p_ratingid, p_ratingname, p_agencyid, p_agencyname, p_entrynumber
 from (
      select   mainattr.t_attrid, mainattr.t_name, parentattr.t_attrid attrnum, parentattr.t_name attrname, 
      (case when mainattr.t_parentid  = 210 Then 
      (select NVL(max(levelid), 0) from Usr_RatingScale where moody = mainattr.t_name)
      ELSE 
      (select NVL(max(levelid),0) from Usr_RatingScale where spfitch = mainattr.t_name) END) as Levelid
      from dobjatcor_dbt atcor
      join dobjattr_dbt mainattr on atcor.t_objecttype = mainattr.t_objecttype and
                                    atcor.t_groupid = mainattr.t_groupid and
                                    atcor.t_attrid = mainattr.t_attrid and
                                    mainattr.t_parentid in (110, 210, 310)
      join dobjattr_dbt parentattr on mainattr.t_objecttype = parentattr.t_objecttype and
                                      mainattr.t_groupid = parentattr.t_groupid and
                                      mainattr.t_parentid = parentattr.t_attrid
      where atcor.t_objecttype = 3 and
            atcor.t_groupid = 19 and
           p_date between atcor.t_validfromdate and atcor.t_validtodate and
/*            mainattr.t_name != 'Снят' and*/
            atcor.t_object = lpad(p_partyid, 10, 0) 
            and atcor.t_validfromdate = ( select max(cor.t_validfromdate) from dobjatcor_dbt cor,  dobjattr_dbt tt where cor.t_groupid = atcor.t_groupid and cor.t_object = atcor.t_object and cor.t_objecttype = atcor.t_objecttype and  p_date between cor.t_validfromdate and cor.t_validtodate
            and tt.t_attrid  = cor.t_attrid and tt.t_groupid = cor.t_groupid and tt.t_objecttype = cor.t_objecttype and tt.t_parentid = mainattr.t_parentid)
  order by levelid desc, atcor.t_validfromdate desc, attrname asc
) where /* levelid is not null and*/  rownum =1;

      return 1;
  EXCEPTION
   WHEN others THEN
     return 0;
  END GetSubjectRating;
  
  FUNCTION getRatingEntryNumber(p_partyid IN INTEGER, p_date IN DATE) return INTEGER
  IS
    v_result INTEGER;
    v_ratingid INTEGER;
    v_ratingname VARCHAR2(255 CHAR);
    v_agencyid INTEGER;
    v_agencyname VARCHAR2(255 CHAR);
    v_entrynumber INTEGER := 0;
  BEGIN
    v_result := getSubjectRating(p_partyid,
                                 p_date,
                                 v_ratingid,
                                 v_ratingname, 
                                 v_agencyid,
                                 v_agencyname,
                                 v_entrynumber);
    return v_entrynumber;
  END getRatingEntryNumber;
 
  --Определение срока до погашения фин. инструмента
  FUNCTION GetTermValue (p_days in integer) return integer
  is
    v_term integer := -1;
  BEGIN

    if (p_days = 0) then v_term := 0;
    elsif ((p_days > 0) and (p_days <= 30)) then v_term := 1;
    elsif ((p_days > 30) and (p_days <= 183)) then v_term := 2;
    elsif ((p_days > 183) and (p_days <= 365)) then v_term := 3;
    else v_term := 4;
    end if;

    return v_term;
  EXCEPTION
    WHEN others THEN
      return -1;
  END GetTermValue;

/**********************************Примечание************************************************************************/

  /*добавить примечание типа double объекту
   * 0 - успешно добавлено
   * 1 - ошибка при выполнении
   */
  FUNCTION addnoteforobject (p_ObjectType in integer,
                             p_NoteKInd in integer,
                             p_DocumentID in VARCHAR2,
                             p_value in number,
                             p_date in date) return integer
  is
    id_note integer := 0;

  BEGIN
    RSB_Struct.readStruct('dnotetext_dbt');

    --проверка на наличие примечания за более позднюю дату
    begin 
      select t_id into id_note from dnotetext_dbt
      where     t_objecttype = p_ObjectType and t_notekind = p_NoteKInd
            and t_documentid = p_DocumentID and t_date > p_date
            and t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy');
      exception
        when no_data_found then
          null;
    end;

    if (id_note > 0) then return 0; end if;
    
    --проверка на наличие примечания за дату pDate
    begin 
      select t_id into id_note from dnotetext_dbt
      where     t_objecttype = p_ObjectType and t_notekind = p_NoteKInd
            and t_documentid = p_DocumentID and t_date = p_date
            and t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy');
      exception
        when no_data_found then
          null;
    end;
    
    if (id_note > 0) then
      update dnotetext_dbt set t_text = RSB_Struct.PutDouble('t_text', rpad('0',3000, '0'), p_value, (-1)*53) where t_id = id_note;
      return 0;
    end if;
    
    --проверка на наличие примечания за предыдущие даты
    begin 
      select t_id into id_note from dnotetext_dbt
      where     t_objecttype = p_ObjectType and t_notekind = p_NoteKInd
            and t_documentid = p_DocumentID and t_date < p_date
            and t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy');
      exception
        when no_data_found then
          null;
    end;
    
    if (id_note > 0) then
      update dnotetext_dbt set t_validtodate = p_date-1 where t_id = id_note;
    end if;
    
    INSERT INTO DNOTETEXT_DBT  ( T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE, T_BRANCH, T_NUMSESSION )
    VALUES   (p_ObjectType,
              p_DocumentID,
              p_NoteKInd,
              RsbSessionData.m_oper,
              p_date,
              to_date('01010001'||to_char(sysdate,'hhmiss'),'DDMMYYYYhhmiss'),
              RSB_Struct.PutDouble('t_text', rpad('0',3000, '0'), p_value, (-1)*53),
              to_date('31129999','DDMMYYYY'),
              1,
              0 );
    
    return 0;

  EXCEPTION
    WHEN no_data_found THEN
      null;
    WHEN others THEN
      return 1;
  END addnoteforobject;
  
  
/******/
/**********************************Примечание************************************************************************/

  /*добавить примечание типа int объекту
   * 0 - успешно добавлено
   * 1 - ошибка при выполнении
   */
  FUNCTION addnoteforobjectint (p_ObjectType in integer,
                             p_NoteKInd in integer,
                             p_DocumentID in VARCHAR2,
                             p_value in number,
                             p_date in date) return integer
  is
    id_note integer := 0;

  BEGIN
    RSB_Struct.readStruct('dnotetext_dbt');

    --проверка на наличие примечания за более позднюю дату
    begin 
      select t_id into id_note from dnotetext_dbt
      where     t_objecttype = p_ObjectType and t_notekind = p_NoteKInd
            and t_documentid = p_DocumentID and t_date > p_date
            and t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy');
      exception
        when no_data_found then
          null;
    end;

    if (id_note > 0) then return 0; end if;
    
    --проверка на наличие примечания за дату pDate
    begin 
      select t_id into id_note from dnotetext_dbt
      where     t_objecttype = p_ObjectType and t_notekind = p_NoteKInd
            and t_documentid = p_DocumentID and t_date = p_date
            and t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy');
      exception
        when no_data_found then
          null;
    end;
    
    if (id_note > 0) then
      update dnotetext_dbt set t_text = RSB_Struct.PutInt('t_text', rpad('0',3000, '0'), p_value, (-1)*53) where t_id = id_note;
      return 0;
    end if;
    
    --проверка на наличие примечания за предыдущие даты
    begin 
      select t_id into id_note from dnotetext_dbt
      where     t_objecttype = p_ObjectType and t_notekind = p_NoteKInd
            and t_documentid = p_DocumentID and t_date < p_date
            and t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy');
      exception
        when no_data_found then
          null;
    end;
    
    if (id_note > 0) then
      update dnotetext_dbt set t_validtodate = p_date-1 where t_id = id_note;
    end if;
    
    INSERT INTO DNOTETEXT_DBT  ( T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE, T_BRANCH, T_NUMSESSION )
    VALUES   (p_ObjectType,
              p_DocumentID,
              p_NoteKInd,
              RsbSessionData.m_oper,
              p_date,
              to_date('01010001'||to_char(sysdate,'hhmiss'),'DDMMYYYYhhmiss'),
              RSB_Struct.Putint('t_text', rpad('0',3000, '0'), p_value, (-1)*53),
              to_date('31129999','DDMMYYYY'),
              1,
              0 );
    
    return 0;

  EXCEPTION
    WHEN no_data_found THEN
      null;
    WHEN others THEN
      return 1;
  END addnoteforobjectint;  
  
/**********************************Категории************************************************************************/

    --Добавление категории к объекту. Работает с историзируемыми категориями
    PROCEDURE ConnectAttr (p_ObjType IN NUMBER,
                           p_GroupId IN NUMBER,
                           p_ObjId   IN VARCHAR2,
                           p_AttrId  IN NUMBER,
                           p_ValidFromDate IN DATE )-- RETURN NUMBER
    IS
        v_id INTEGER := 0;
        v_ValidToDate  dobjatcor_dbt.T_VALIDTODATE%TYPE := to_date('31.12.9999', 'dd.mm.yyyy');
    BEGIN
        
      --проверка на наличие категории за более позднюю дату
      begin
        select t_id into v_id
        from dobjatcor_dbt
        where     t_objecttype = p_ObjType
              and t_groupid = p_GroupId
              and t_object = p_ObjId
              and t_validfromdate > p_ValidFromDate
              and t_validtodate = v_ValidToDate;
      exception
        when no_data_found then
          null;
      end;

      --проверка на наличие категории за дату p_ValidFromDate
      IF v_id = 0 THEN
        begin
          select t_id into v_id
          from dobjatcor_dbt
          where     t_objecttype = p_ObjType
                and t_groupid = p_GroupId
                and t_object = p_ObjId
                and t_validfromdate = p_ValidFromDate
                and t_validtodate = v_ValidToDate;
        exception
          when no_data_found then
            null;
        end;

        if (v_id > 0) then
          update dobjatcor_dbt set t_attrid = p_AttrId where t_id = v_id;
        end if;
      END IF;

      IF v_id = 0 THEN
        --проверка на наличие категории за предыдущие даты
        begin 
          select t_id into v_id from dobjatcor_dbt
        where     t_objecttype = p_ObjType
              and t_groupid = p_GroupId
              and t_object = p_ObjId
              and t_validfromdate < p_ValidFromDate
              and t_validtodate = v_ValidToDate;
          exception
            when no_data_found then
              null;
        end;
        
        if (v_id > 0) then
          update dobjatcor_dbt set t_validtodate = p_ValidFromDate-1 where t_id = v_id;
        end if;

        INSERT INTO dobjatcor_dbt( T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_ISAUTO )
        VALUES( p_ObjType, p_GroupId, p_AttrId, p_ObjId, CHR(88), p_ValidFromDate, RSBSESSIONDATA.OPER, v_ValidToDate, CHR(88) );

      END IF;

    END ConnectAttr;


    PROCEDURE DisconnectAttr (p_ObjType IN NUMBER,
                              p_GroupId IN NUMBER,
                              p_ObjId   IN VARCHAR2,
                              p_ValidToDate IN DATE )
    IS
    BEGIN

      UPDATE DOBJATCOR_DBT SET T_VALIDTODATE = p_ValidToDate WHERE T_OBJECTTYPE = p_ObjType AND T_GROUPID = p_GroupId AND T_OBJECT = p_ObjId and T_VALIDTODATE = to_date('31.12.9999', 'dd.mm.yyyy');
    EXCEPTION
      WHEN others THEN
        NULL;
    END DisconnectAttr;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------
  FUNCTION get_va_objtype         return integer is BEGIN return VA_OBJECTTYPE; END;
  FUNCTION get_va_notekind_or     return integer is BEGIN return VA_NOTEKIND_OR; END;
  FUNCTION get_va_dockind         return integer is BEGIN return VA_DOCKIND; END;
  FUNCTION get_va_attr_method     return integer is BEGIN return VA_ATTRGROUP_METHOD; END;
  FUNCTION get_va_attr_stage      return integer is BEGIN return VA_ATTRGROUP_STAGE; END;
  FUNCTION get_va_attr_stage_sofr return integer is BEGIN return VA_ATTRGROUP_STAGE_SOFR; END;
  FUNCTION get_va_attr_recovery   return integer is BEGIN return VA_ATTRGROUP_RECOVERY; END;

  FUNCTION get_avr_objtype         return integer is BEGIN return AVR_OBJECTTYPE; END;
  FUNCTION get_avr_notekind_or     return integer is BEGIN return AVR_NOTEKIND_OR; END;
  FUNCTION get_avr_attr_method     return integer is BEGIN return AVR_ATTRGROUP_METHOD; END;
  FUNCTION get_avr_attr_stage      return integer is BEGIN return AVR_ATTRGROUP_STAGE; END;
  FUNCTION get_avr_attr_stage_sofr return integer is BEGIN return AVR_ATTRGROUP_STAGE_SOFR; END;
  FUNCTION get_avr_attr_recovery   return integer is BEGIN return AVR_ATTRGROUP_RECOVERY; END;

  FUNCTION get_mbk_dockind         return integer is BEGIN return MBK_DOCKIND; END;
  FUNCTION get_mbk_objtype         return integer is BEGIN return MBK_OBJECTTYPE; END;
  FUNCTION get_mbk_attr_recovery   return integer is BEGIN return MBK_ATTRGROUP_RECOVERY; END;
  FUNCTION get_mbk_attr_stage      return integer is BEGIN return MBK_ATTRGROUP_STAGE; END;
  FUNCTION get_mbk_attr_stage_sofr return integer is BEGIN return MBK_ATTRGROUP_STAGE_SOFR; END;

  FUNCTION get_deal_objtype         return integer is BEGIN return DEAL_OBJECTTYPE; END;
  FUNCTION get_deal_notekind_or     return integer is BEGIN return DEAL_NOTEKIND_OR; END;
  FUNCTION get_deal_attr_method     return integer is BEGIN return DEAL_ATTRGROUP_METHOD; END;
  FUNCTION get_deal_attr_stage      return integer is BEGIN return DEAL_ATTRGROUP_STAGE; END;
  FUNCTION get_deal_attr_stage_sofr return integer is BEGIN return DEAL_ATTRGROUP_STAGE_SOFR; END;
  FUNCTION get_deal_attr_recovery   return integer is BEGIN return DEAL_ATTRGROUP_RECOVERY; END;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------

  FUNCTION calcStage(p_default_stage IN INTEGER, p_delaydays IN INTEGER, p_recovery IN INTEGER) RETURN INTEGER
  IS
    v_calcstage integer;
  BEGIN
    v_calcstage := p_default_stage;

    IF (p_recovery = 1) AND (p_delaydays > 30) THEN
      v_calcstage := 3;
    ELSIF p_recovery <> 1 THEN
      IF (p_delaydays >= 10) AND (v_calcstage <> 3) THEN
        v_calcstage := 3;
      ELSIF (p_delaydays >= 5) and (p_delaydays < 10) and (v_calcstage < 2) THEN
        v_calcstage := 2;
      END IF;
    END IF;

    RETURN v_calcstage;
  END calcStage;

/**********************************Векселя************************************************************************/

  FUNCTION GetBnrPlanRepayDate (p_bcid IN INTEGER) RETURN DATE
  IS
    v_stat INTEGER;
    v_date DATE;
    v_legid INTEGER;
  BEGIN
  
    SELECT t_id INTO v_legid
    FROM DDL_LEG_DBT
    WHERE t_DealId = p_bcid AND t_LegKind = 1;
  
    v_stat := rsb_bill.GetBnrPlanRepayDate(v_legid, v_date);
    
    return v_date;
    
  EXCEPTION
    WHEN OTHERS THEN
      return to_date('01.01.0001', 'dd.mm.yyyy');
  END GetBnrPlanRepayDate;

  --Ставка ОР векселя рассчетным методом
  FUNCTION Load_rates_or_va_method_1 (pBCID IN integer, pDate in DATE) return integer
  is
    v_rate number(32,12) := 0.0;
    v_valuedate date;
    v_stage_default integer;
    v_recovery integer;
    v_delaydays integer := 0;
    v_stagesofr integer;
  BEGIN

    --Определить количество дней просрочки векселя
    BEGIN
      SELECT pDate - RSB_BILL.GETBNRNOPLANREPAYDATE(t_id) INTO v_delaydays
      FROM ddl_leg_dbt WHERE t_Dealid = pBCID AND t_legkind = 1 and t_legid = 0;
    EXCEPTION
      WHEN others THEN
        NULL;
    END;

    SELECT msfo.rate, msfo.valuedate, msfo.stagefact, msfo.stage, nvl(msfo.recovery_flg, -1)
    into v_rate, v_valuedate, v_stage_default, v_stagesofr, v_recovery
    FROM dmcaccdoc_dbt mcacc, USER_MSFORESERV msfo
    WHERE     mcacc.t_dockind  = VA_DOCKIND
          AND mcacc.t_iscommon = chr(0)
          AND msfo.method_id   = METHOD_CALC_ID
          AND MSFO.OBJECTTYPE  = OR_OBJECTTYPE_VA
          AND msfo.stage       = calcStage(msfo.stagefact, v_delaydays, msfo.recovery_flg)
          AND msfo.object      = mcacc.t_account
          AND mcacc.t_docid    = pBCID
          AND msfo.valuedate   >= pDate
          AND msfo.valuedate = (SELECT MAX(valuedate) FROM user_msforeserv
                                WHERE     method_id = msfo.method_id
                                      AND objecttype = msfo.objecttype
                                      AND stagefact = msfo.stagefact
                                      AND object = msfo.object
                                      AND stage = msfo.stage);

    ConnectAttr(VA_OBJECTTYPE, VA_ATTRGROUP_METHOD, lpad(pBCID, 10, '0'), METHOD_CALC_ID, v_valuedate);
    ConnectAttr(VA_OBJECTTYPE, VA_ATTRGROUP_STAGE, lpad(pBCID, 10, '0'), v_stage_default, v_valuedate);
    ConnectAttr(VA_OBJECTTYPE, VA_ATTRGROUP_STAGE_SOFR, lpad(pBCID, 10, '0'), v_stagesofr, v_valuedate);

    IF v_recovery <> -1 THEN
      ConnectAttr(VA_OBJECTTYPE, VA_ATTRGROUP_RECOVERY, lpad(pBCID, 10, '0'), v_recovery+1, v_valuedate);
    ELSE
      DisconnectAttr(VA_OBJECTTYPE, VA_ATTRGROUP_RECOVERY, lpad(pBCID, 10, '0'), v_valuedate-1);
    END IF;
    
    return addnoteforobject(VA_OBJECTTYPE, VA_NOTEKIND_OR, lpad(pBCID, 10, '0'), v_rate, v_valuedate);

  EXCEPTION
    WHEN others then
      return 1;
  END Load_rates_or_va_method_1;
  
  --Ставка ОР векселя по типу эмитента.
  --Пока нет ясности, подходит ли предоставленный файл под этот метод. временно пропусти и приступим к другим методам
  FUNCTION Load_rates_or_va_method_2 (pBCID IN integer, pDate in DATE) return integer
  is
    v_stat integer := 1;
  BEGIN
    
    return v_stat;

  END Load_rates_or_va_method_2;

  --Ставка ОР векселя по рейтингу эмитента.
  FUNCTION Load_rates_or_va_method_3 (pBCID IN integer, pIssuer in integer, pDate in date) return integer
  is
    v_IssuerType varchar2(20 char) := '';
    v_IssuerRating integer := 0;
    v_IssuerKind integer := 0;
    v_rate number(32, 12) := 0.0;
    v_valuedate date;
  BEGIN
    v_IssuerType := GetIssuerType(pIssuer);
    v_IssuerRating := getRatingEntryNumber(pIssuer, pDate);
    
    if ((v_IssuerType = '') or (v_IssuerRating = 0) ) then
      return 1;
    end if;
    
    select msfo.rate, msfo.valuedate
    into v_rate, v_valuedate
    from USER_MSFORESERV msfo
    where method_id = METHOD_RATING_ID
          and object = v_IssuerType
          and stagefact = v_IssuerRating
          and msfo.valuedate >= pDate
          and valuedate = (select max(valuedate) from user_msforeserv
                           where     method_id = msfo.method_id
                                 and object = msfo.object
                                 and stagefact = msfo.stagefact);

    ConnectAttr(VA_OBJECTTYPE, VA_ATTRGROUP_METHOD, lpad(pBCID, 10, '0'), METHOD_RATING_ID, v_valuedate);
/*BIQ-6665*/
    ConnectAttr(VA_OBJECTTYPE, VA_ATTRGROUP_RATING, lpad(pBCID, 10, '0'), v_IssuerRating, v_valuedate);
   
    IF(v_IssuerType = CONTRACTOR_BANK) Then
        v_IssuerKind := 1;
    ELSIF(v_IssuerType = CONTRACTOR_SOVEREIGN) Then 
        v_IssuerKind := 2;
    ELSIF(v_IssuerType = CONTRACTOR_SUBSOVEREIGN) Then 
        v_IssuerKind := 3;
    ELSIF(v_IssuerType = CONTRACTOR_CORPORATE) Then 
        v_IssuerKind := 4;
    END IF;    
    
    
   ConnectAttr(VA_OBJECTTYPE, VA_ATTRGROUP_ISSUERKIND, lpad(pBCID, 10, '0'), v_IssuerKind, v_valuedate); 


    return addnoteforobject(VA_OBJECTTYPE, VA_NOTEKIND_OR, lpad(pBCID, 10, '0'), v_rate, v_valuedate);

  EXCEPTION
    WHEN others THEN
      return 1;
  END Load_rates_or_va_method_3;

  --Загрузка ставок оценочных резервов для учтенных векселей
  PROCEDURE Load_rates_or_va (pDate in DATE)
  is
    v_stat integer := 1;
  BEGIN
     for rec in (select bnr.*
                from dvsbanner_dbt bnr
                where bnr.t_issuer not in (select t_partyid from ddp_dep_dbt)
                      and bnr.t_issuedate <= pDate
                      and (bnr.t_repaymentdate > pDate or bnr.t_repaymentdate = to_date('01.01.0001', 'dd.mm.yyyy'))
                      and not exists (select 1 from dvsordlnk_dbt lnk, ddl_tick_dbt tick
                                      where     lnk.t_bcid = bnr.t_bcid
                                            and lnk.t_linkkind = VA_LINKKIND_OUT
                                            and lnk.t_interestchargedate > pDate
                                            and tick.t_dealid = lnk.t_contractid
                                            and tick.t_dealstatus = 20) --закрыт
                      and not exists (select 1 from dnotetext_dbt note
                                      where t_objecttype = VA_OBJECTTYPE and t_notekind = VA_NOTEKIND_OR
                                            and t_documentid = lpad(bnr.t_bcid, 10, 0) and t_date > pDate))
      loop
          v_stat := Load_rates_or_va_method_1(rec.t_bcid, pDate);

          if (v_stat = 1) then
            v_stat := Load_rates_or_va_method_2(rec.t_bcid, pDate);
          end if;

          if (v_stat = 1) then
            v_stat := Load_rates_or_va_method_3(rec.t_bcid, rec.t_issuer, pDate);
          end if;

      end loop;

      commit;
  EXCEPTION
    WHEN others then
      return;
  END Load_rates_or_va;

/**********************************Облигации************************************************************************/
  --Ставка ОР облигации рассчетным методом
  FUNCTION Load_rates_or_avr_method_1 (p_fiid in integer, p_isin in varchar2, p_Date in DATE) return integer
  is
    v_rate number(32,12) := 0.0;
    v_valuedate date;
    v_stage_default integer;
    v_recovery integer;
    v_delaydays integer := 0;
    v_stagesofr integer;
    v_termless varchar(1 char) := chr(0);
  BEGIN

    --Определить количество дней просрочки
    BEGIN
        SELECT t_Termless INTO v_termless FROM davoiriss_dbt WHERE t_fiid = p_fiid;

        SELECT p_Date - nvl(min(to_Date(t_drawingdate, 'dd.mm.yyyy')), RSI_RSB_FIInstr.FI_GetNominalDrawingDate(p_fiid, v_termless))
        INTO v_delaydays
        FROM dfiwarnts_dbt WHERE t_FIID = p_fiid and t_SPIsClosed <> CHR(88);
    EXCEPTION
      WHEN others THEN
        NULL;
    END;

    SELECT msfo.rate, msfo.valuedate, msfo.stagefact, msfo.stage, nvl(msfo.recovery_flg, -1)
    into v_rate, v_valuedate, v_stage_default, v_stagesofr, v_recovery
    FROM USER_MSFORESERV msfo
    WHERE     msfo.method_id  = METHOD_CALC_ID
          AND MSFO.OBJECTTYPE = OR_OBJECTTYPE_AVR
          AND msfo.stage      = calcStage(msfo.stagefact, v_delaydays, msfo.recovery_flg)
          AND msfo.object     = p_isin
          AND msfo.valuedate >= p_Date
          AND msfo.valuedate = (SELECT max(valuedate) FROM user_msforeserv
                                WHERE     method_id = msfo.method_id
                                      AND objecttype = msfo.objecttype
                                      AND stagefact = msfo.stagefact
                                      AND object = msfo.object
                                      AND stage = msfo.stage);
    
    ConnectAttr(AVR_OBJECTTYPE, AVR_ATTRGROUP_METHOD, lpad(p_fiid, 10, '0'), METHOD_CALC_ID, v_valuedate);
    ConnectAttr(AVR_OBJECTTYPE, AVR_ATTRGROUP_STAGE, lpad(p_fiid, 10, '0'), v_stage_default, v_valuedate);
    ConnectAttr(AVR_OBJECTTYPE, AVR_ATTRGROUP_STAGE_SOFR, lpad(p_fiid, 10, '0'), v_stagesofr, v_valuedate);

    IF v_recovery <> -1 THEN
      ConnectAttr(AVR_OBJECTTYPE, AVR_ATTRGROUP_RECOVERY, lpad(p_fiid, 10, '0'), v_recovery+1, v_valuedate);
    ELSE
      DisconnectAttr(AVR_OBJECTTYPE, AVR_ATTRGROUP_RECOVERY, lpad(p_fiid, 10, '0'), v_valuedate-1);
    END IF;

    return addnoteforobject(AVR_OBJECTTYPE, AVR_NOTEKIND_OR, lpad(p_fiid, 10, '0'), v_rate, v_valuedate);

  EXCEPTION
    WHEN no_data_found THEN
      return 1;
    WHEN others THEN
      return 1;
  END;
  
  --Ставки ОР облигаций, метод 2
  FUNCTION Load_rates_or_avr_method_2 (p_fiid in integer, p_isin in varchar2, p_Date in DATE) return integer
  is
    v_rate number(32,12) := 0.0;
    v_valuedate date;
  BEGIN
    select msfo.rate, msfo.valuedate
    into v_rate, v_valuedate
    from USER_MSFORESERV msfo
    where     msfo.method_id = METHOD_ISSUER_ID
          and MSFO.OBJECTTYPE = OR_OBJECTTYPE_AVR
          and msfo.object = p_isin
          and msfo.valuedate >= p_Date
          and msfo.valuedate = (select max(valuedate) from user_msforeserv
                                where     method_id = msfo.method_id
                                      and objecttype = msfo.objecttype
                                      and stagefact = msfo.stagefact
                                      and object = msfo.object
                                      and stage = msfo.stage);
    
    ConnectAttr(AVR_OBJECTTYPE, AVR_ATTRGROUP_METHOD, lpad(p_fiid, 10, '0'), METHOD_ISSUER_ID, v_valuedate);

    return addnoteforobject(AVR_OBJECTTYPE, AVR_NOTEKIND_OR, lpad(p_fiid, 10, '0'), v_rate, v_valuedate);

  EXCEPTION
    WHEN no_data_found THEN
      return 1;
    WHEN others THEN
      return 1;                               
  END Load_rates_or_avr_method_2;

  --Ставки ОР облигаций на основании рейтинга эмитента
  FUNCTION Load_rates_or_avr_method_3 (p_fiid in integer, p_issuer in integer, p_date in date) return integer
  IS
    v_IssuerType varchar2(20 char) := '';
    v_IssuerRating integer := 0;
    v_IssuerKind integer := 0;
    v_rate number(32, 12) := 0.0;
    v_valuedate date;
    err integer :=0;
  BEGIN
    v_IssuerType := GetIssuerType(p_issuer);
    v_IssuerRating := getRatingEntryNumber(p_issuer, p_date);
    
    if ((v_IssuerType = '') or (v_IssuerRating = 0) ) then
      return 1;
    end if;
    
    select msfo.rate, msfo.valuedate
    into v_rate, v_valuedate
    from USER_MSFORESERV msfo
    where method_id = METHOD_RATING_ID
          and object = v_IssuerType
          and stagefact = v_IssuerRating
          and msfo.valuedate >= p_Date
          and valuedate = (select max(valuedate) from user_msforeserv
                           where     method_id = msfo.method_id
                                 and object = msfo.object
                                 and stagefact = msfo.stagefact);

    ConnectAttr(AVR_OBJECTTYPE, AVR_ATTRGROUP_METHOD, lpad(p_fiid, 10, '0'), METHOD_RATING_ID, v_valuedate);
    
    ConnectAttr(AVR_OBJECTTYPE, AVR_ATTRGROUP_RATING, lpad(p_fiid, 10, '0'), v_IssuerRating, v_valuedate);
   
    IF(v_IssuerType = CONTRACTOR_BANK) Then
        v_IssuerKind := 1;
    ELSIF(v_IssuerType = CONTRACTOR_SOVEREIGN) Then 
        v_IssuerKind := 2;
    ELSIF(v_IssuerType = CONTRACTOR_SUBSOVEREIGN) Then 
        v_IssuerKind := 3;
    ELSIF(v_IssuerType = CONTRACTOR_CORPORATE) Then 
        v_IssuerKind := 4;
    END IF;    
    
    
   ConnectAttr(AVR_OBJECTTYPE, AVR_ATTRGROUP_ISSUERKIND, lpad(p_fiid, 10, '0'), v_IssuerKind, v_valuedate); 
    
    
--    err:=addnoteforobjectint(AVR_OBJECTTYPE, AVR_NOTEKIND_RATING, lpad(p_fiid, 10, '0'), v_IssuerRating, v_valuedate);

    return addnoteforobject(AVR_OBJECTTYPE, AVR_NOTEKIND_OR, lpad(p_fiid, 10, '0'), v_rate, v_valuedate);

  EXCEPTION
    WHEN no_data_found THEN
      return 1;
    WHEN others THEN
      return 1;
  END Load_rates_or_avr_method_3;
  
  --Ставки ОР облигаций по эмитенту и сроку
  FUNCTION Load_rates_or_avr_method_4 (p_fiid in integer, p_issuer in integer, p_days in integer, p_Date in DATE) return integer
  IS
    v_IssuerType varchar2(20 char) := '';
    v_term integer := -1;
    v_rate number(32, 12) := 0.0;
    v_valuedate date;
  BEGIN
     v_IssuerType := GetIssuerType(p_issuer);
     v_term := GetTermValue(p_days);
     
     select msfo.rate, msfo.valuedate
     into v_rate, v_valuedate
     from USER_MSFORESERV msfo
     where     method_id = METHOD_TTM_ID
           and objecttype = OR_OBJECTTYPE_AVR
           and object = v_IssuerType
           and stagefact = v_term
           and msfo.valuedate >= p_Date
           and valuedate = (select max(valuedate) from user_msforeserv
                            where     method_id = msfo.method_id
                                  and object = msfo.object
                                  and stagefact = msfo.stagefact);

     ConnectAttr(AVR_OBJECTTYPE, AVR_ATTRGROUP_METHOD, lpad(p_fiid, 10, '0'), METHOD_TTM_ID, v_valuedate);
     
     return addnoteforobject(AVR_OBJECTTYPE, AVR_NOTEKIND_OR, lpad(p_fiid, 10, '0'), v_rate, v_valuedate);

  EXCEPTION
    WHEN no_data_found THEN
      return 1;
    WHEN others THEN
      return 1;
  END Load_rates_or_avr_method_4;

  PROCEDURE Load_rates_or_avr (p_Date in DATE)
  is
    v_stat integer;
  BEGIN
     for rec in (select fin.t_fiid fiid, fin.t_issuer issuer, fin.t_avoirkind avrkind, avr.t_isin isin,
                        case when t_drawingdate = to_date('01.01.0001', 'dd.mm.yyyy') then 1000
                             else t_drawingdate - p_Date
                        end days
                 from dfininstr_dbt fin, davoiriss_dbt avr
                 where     fin.t_fiid = avr.t_fiid
                       and fin.t_fi_kind = 2
                       and RSI_RSB_FIInstr.FI_AvrKindsGetRoot( fin.t_fi_kind, fin.t_AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_BOND
                       and (avr.t_spisclosed = chr(0) or t_drawingdate > p_Date or t_drawingdate = to_date('01.01.0001', 'dd.mm.yyyy'))
                       and fin.t_issuer not in (select t_partyid from ddp_dep_dbt)
                       and not exists (select 1 from dnotetext_dbt note
                                       where     t_objecttype = AVR_OBJECTTYPE and t_notekind = AVR_NOTEKIND_OR
                                             and t_documentid = lpad(fin.t_fiid, 10, 0) and t_date > p_Date)
                  )
      loop
          v_stat := Load_rates_or_avr_method_1(rec.fiid, rec.isin, p_date);

          if (v_stat = 1) then
            v_stat := Load_rates_or_avr_method_2(rec.fiid, rec.isin, p_date);
          end if;

          if (v_stat = 1) then
            v_stat := Load_rates_or_avr_method_3(rec.fiid, rec.issuer, p_date);
          end if;

          if (v_stat = 1) then
            v_stat := Load_rates_or_avr_method_4(rec.fiid, rec.issuer, rec.days, p_date);
          end if;

          commit;
          
      end loop;

  EXCEPTION
    WHEN others then
      return;
  END Load_rates_or_avr;

/**********************************Покупки ценных бумаг************************************************************************/
  PROCEDURE Load_rates_or_deals (p_Date in DATE)
  is
    v_fiid integer;
    v_stat integer;
  BEGIN
    for rec in (select *
                from user_msforeserv msfo
                where     msfo.start_dt > to_date('01.01.0001', 'dd.mm.yyyy')
                      and msfo.method_id = METHOD_CALC_ID
                      and MSFO.OBJECTTYPE = OR_OBJECTTYPE_AVR
                      and msfo.stagefact = msfo.stage
                      and msfo.valuedate >= p_Date
                      and msfo.valuedate = (select max(valuedate) from user_msforeserv
                                            where     method_id = msfo.method_id
                                                  and objecttype = msfo.objecttype
                                                  and stagefact = msfo.stagefact
                                                  and object = msfo.object
                                                  and stage = msfo.stage
                                                  and start_dt = msfo.start_dt))
    loop
      BEGIN
        SELECT t_fiid INTO v_fiid FROM davoiriss_dbt WHERE t_isin = rec.object;
      EXCEPTION
        WHEN TOO_MANY_ROWS THEN
          v_fiid := -2;
        WHEN OTHERS THEN
          RETURN;
      END;

      for deal in (select tick.t_dealid
                   from ddl_tick_dbt tick
                   where     rsb_secur.IsRepo(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(TICK.t_DealType, TICK.t_BofficeKind))) = 0
                         and RSB_SECUR.IsBuy(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(TICK.t_DealType, TICK.t_BofficeKind))) = 1
                         and tick.t_dealdate = rec.start_dt
                         and tick.t_bofficekind = DEAL_OBJECTTYPE
                         and tick.t_pfi = v_fiid
                         and not exists (select 1 from dnotetext_dbt note
                                         where     t_objecttype = DEAL_OBJECTTYPE and t_notekind = DEAL_NOTEKIND_OR
                                               and t_documentid = lpad(tick.t_dealid, 34, 0) and t_date >= p_Date))
      loop
        ConnectAttr(DEAL_OBJECTTYPE, DEAL_ATTRGROUP_METHOD, lpad(deal.t_dealid, 34, '0'), METHOD_CALC_ID, rec.valuedate);
        ConnectAttr(DEAL_OBJECTTYPE, DEAL_ATTRGROUP_STAGE,  lpad(deal.t_dealid, 34, '0'), rec.stage, rec.valuedate);
        
        IF rec.recovery_flg IS NOT NULL THEN
          ConnectAttr(DEAL_OBJECTTYPE, DEAL_ATTRGROUP_RECOVERY, lpad(deal.t_dealid, 34, '0'), rec.recovery_flg+1, rec.valuedate);
        ELSE
          DisconnectAttr(DEAL_OBJECTTYPE, DEAL_ATTRGROUP_RECOVERY, lpad(deal.t_dealid, 34, '0'), rec.valuedate-1);
        END IF;

        v_stat := addnoteforobject(DEAL_OBJECTTYPE, DEAL_NOTEKIND_OR, lpad(deal.t_dealid, 34, '0'), rec.rate, rec.valuedate);
      end loop;

    end loop;

    commit;
  EXCEPTION
    WHEN others then
      return;
  END Load_rates_or_deals;
  
  
/**********************************МБК************************************************************************/
 --вставка ставки ОР в сделку МБК
  FUNCTION AddMbkRate (p_DealID in integer,
                       p_rate in number,
                       p_Date in date,
                       p_impgrade in integer default 0,
                       p_comment in varchar2 default chr(1)) return integer
  IS
    v_id integer := 0;
    v_PartyID integer := 0;
  BEGIN
    begin
      select t_partyid into v_PartyID from ddl_tick_dbt where t_dealid = p_DealID;
    
    exception
      when others then
        return 1;
    end;
    
    if (GetIssuerType (v_PartyID) =  CONTRACTOR_SOVEREIGN) then --центральный банк
      return 0;
    end if;
    
    begin
      select t_id into v_id from dmmhstimp_dbt where t_dealid = p_DealID and t_date = p_date;
    
    exception
      when no_data_found then
        null;
      when others then
        return 1;
    end;
    
    if (v_id > 0) then
      update dmmhstimp_dbt set t_percloss = p_rate, t_impgrade = p_impgrade, t_comment = p_comment where t_id = v_id;
      return 0;
    end if;
    
    insert into dmmhstimp_dbt (t_id, t_dealid, t_date, t_impgrade, t_percloss, t_comment)
    values (dmmhstimp_dbt_seq.nextval, p_DealID, p_Date, p_impgrade, p_rate, p_comment);
    
    return 0;
    
  EXCEPTION
    WHEN others THEN
      return 1;  
  END AddMbkRate;

  FUNCTION Get_methodName(methodID in INTEGER) return VARCHAR2
  IS
    v_name VARCHAR2 (100 char) := chr(1);
  BEGIN
    select t_name into v_name from dllvalues_dbt where t_list = 5026 and t_element = methodID;
    return v_name;
  EXCEPTION
    WHEN others THEN
      return chr(1);
  END Get_methodName;

  --поиск ставки ОР МБК рассчетным методом
  FUNCTION Load_rates_or_mbk_method_1 (p_DealID in integer, p_date IN DATE) return integer
  IS
    v_rate number(32, 12) := 0.0;
    v_valuedate date;
    v_method_name VARCHAR2 (100 char);
    v_delaydays integer := 0;
    v_stage_default integer;
    v_recovery integer;
    v_stagesofr integer;
  BEGIN

    --Определить количество дней просрочки
    BEGIN
      SELECT p_date - t_maturity INTO v_delaydays
      FROM ddl_Leg_dbt WHERE t_Dealid = p_DealID AND t_legkind = 0;
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;

    SELECT msfo.rate, msfo.valuedate, msfo.stagefact, msfo.stage, nvl(msfo.recovery_flg, -1)
    INTO v_rate, v_valuedate, v_stage_default, v_stagesofr, v_recovery
    FROM dmcaccdoc_dbt mcacc, USER_MSFORESERV msfo
    WHERE     mcacc.t_dockind  = MBK_DOCKIND
          AND mcacc.t_iscommon = chr(0)
          and mcacc.t_isusable = chr(88)
          AND msfo.method_id   = METHOD_CALC_ID
          AND MSFO.OBJECTTYPE  = OR_OBJECTTYPE_MBK
          AND msfo.stage       = calcStage(msfo.stagefact, v_delaydays, msfo.recovery_flg)
          AND msfo.object      = mcacc.t_account
          AND mcacc.t_docid    = p_DealID
          AND msfo.valuedate  >= p_date
          AND msfo.valuedate = (SELECT max(valuedate) FROM user_msforeserv
                                WHERE     method_id = msfo.method_id
                                      AND objecttype = msfo.objecttype
                                      AND stagefact = msfo.stagefact
                                      AND object = msfo.object
                                      AND stage = msfo.stage);

    ConnectAttr(MBK_OBJECTTYPE, MBK_ATTRGROUP_STAGE, lpad(p_DealID, 34, '0'), v_stage_default, v_valuedate);
    ConnectAttr(MBK_OBJECTTYPE, MBK_ATTRGROUP_STAGE_SOFR, lpad(p_DealID, 34, '0'), v_stagesofr, v_valuedate);

    IF v_recovery <> -1 THEN
      ConnectAttr(MBK_OBJECTTYPE, MBK_ATTRGROUP_RECOVERY, lpad(p_DealID, 34, '0'), v_recovery+1, v_valuedate);
    ELSE
      DisconnectAttr(MBK_OBJECTTYPE, MBK_ATTRGROUP_RECOVERY, lpad(p_DealID, 34, '0'), v_valuedate-1);
    END IF;

    v_method_name := Get_methodName(METHOD_CALC_ID);
    return AddMbkRate(p_DealID, v_rate, v_valuedate, v_stagesofr-1, v_method_name);

  EXCEPTION
    WHEN others THEN
      return 1;
  END Load_rates_or_mbk_method_1;

  FUNCTION Load_rates_or_mbk_method_2 (p_DealID in integer, p_date in date) return integer
  IS
    v_rate number(32, 12) := 0.0;
    v_valuedate date;
    v_method_name VARCHAR2 (100 char) := chr(1);
  BEGIN
    select msfo.rate, msfo.valuedate
    into v_rate, v_valuedate
    from dmcaccdoc_dbt mcacc, USER_MSFORESERV msfo
    where     mcacc.t_dockind = MBK_DOCKIND
          and mcacc.t_iscommon = chr(0)
          and mcacc.t_isusable = chr(88)
          and msfo.method_id = METHOD_ISSUER_ID
          and MSFO.OBJECTTYPE = OR_OBJECTTYPE_MBK
          and msfo.object = mcacc.t_account
          and mcacc.t_docid = p_DealID
          and msfo.valuedate >= p_date
          and msfo.valuedate = (select max(valuedate) from user_msforeserv
                                where     method_id = msfo.method_id
                                      and objecttype = msfo.objecttype
                                      and stagefact = msfo.stagefact
                                      and object = msfo.object
                                      and stage = msfo.stage);

    v_method_name := Get_methodName(METHOD_ISSUER_ID);
    return AddMbkRate(p_DealID, v_rate, v_valuedate, 0, v_method_name);

  EXCEPTION
    WHEN others THEN
      return 1;
  END Load_rates_or_mbk_method_2;

  --Ставки ОР МБК на основании рейтинга контрагента
  FUNCTION Load_rates_or_mbk_method_3 (p_DealID in integer, p_contractor in integer, p_date in date) return integer
  IS
    v_ContractorType varchar2(20 char) := '';
    v_ContractorRating integer := 0;
    v_IssuerKind integer := 0;
    v_rate number(32, 12) := 0.0;
    v_valuedate date;
    v_method_name VARCHAR2 (100 char) := chr(1);
  BEGIN

    v_ContractorType := GetIssuerType(p_contractor);
    v_ContractorRating := getRatingEntryNumber(p_contractor, p_date);

    if ((v_ContractorType = '') or (v_ContractorRating = 0) ) then
      return 1;
    end if;

    select msfo.rate, msfo.valuedate
    into v_rate, v_valuedate
    from USER_MSFORESERV msfo
    where method_id = METHOD_RATING_ID
          and object = v_ContractorType
          and stagefact = v_ContractorRating
          and valuedate >= p_date
          and valuedate = (select max(valuedate) from user_msforeserv
                           where     method_id = msfo.method_id
                                 and object = msfo.object
                                 and stagefact = msfo.stagefact);

    v_method_name := Get_methodName(METHOD_RATING_ID);

  ConnectAttr(MBK_OBJECTTYPE, MBK_ATTRGROUP_RATING, lpad(p_DealID, 34, '0'), v_ContractorRating, v_valuedate);


    IF(v_ContractorType = CONTRACTOR_BANK) Then
        v_IssuerKind := 1;
    ELSIF(v_ContractorType = CONTRACTOR_SOVEREIGN) Then
        v_IssuerKind := 2;
    ELSIF(v_ContractorType = CONTRACTOR_SUBSOVEREIGN) Then
        v_IssuerKind := 3;
    ELSIF(v_ContractorType = CONTRACTOR_CORPORATE) Then
        v_IssuerKind := 4;
    END IF;

  ConnectAttr(MBK_OBJECTTYPE, MBK_ATTRGROUP_ISSUERKIND, lpad(p_DealID, 34, '0'), v_IssuerKind, v_valuedate);


    return AddMbkRate(p_DealID, v_rate, v_valuedate, 0, v_method_name);

  EXCEPTION
    WHEN no_data_found THEN
      return 1;
    WHEN others THEN
      return 1;
  END Load_rates_or_mbk_method_3;

  FUNCTION Load_rates_or_mbk_method_4 (p_DealID in integer, p_Contractor in integer, p_days in integer) return integer
  IS
    v_ContractorType varchar2(20 char) := '';
    v_term integer := -1;
    v_rate number(32, 12) := 0.0;
    v_valuedate date;
    v_method_name VARCHAR2 (100 char) := chr(1);
  BEGIN
    v_ContractorType := GetIssuerType(p_Contractor);
    v_term := GetTermValue(p_days);

    select msfo.rate, msfo.valuedate
    into v_rate, v_valuedate
    from USER_MSFORESERV msfo
    where     method_id = METHOD_TTM_ID
          and objecttype = OR_OBJECTTYPE_MBK
          and object = v_ContractorType
          and stagefact = v_term
          and valuedate = (select max(valuedate) from user_msforeserv
                           where     method_id = msfo.method_id
                                 and object = msfo.object
                                 and stagefact = msfo.stagefact);

    v_method_name := Get_methodName(METHOD_TTM_ID);
    return AddMbkRate(p_DealID, v_rate, v_valuedate, 0, v_method_name);

    EXCEPTION
    WHEN others THEN
      return 1;
  END Load_rates_or_mbk_method_4;

  PROCEDURE Load_rates_or_mbk (p_Date in DATE)
  IS
    v_stat integer;
  BEGIN
    for rec in (select tick.t_dealid, tick.t_partyid, leg.t_maturity - p_date as t_duration
                from ddl_tick_dbt tick, ddl_leg_dbt leg
                where     tick.t_bofficekind = 102
                      and tick.t_dealtype in (12305, 12306, 12335, 32305)
                      and tick.t_dealdate <= p_Date
                      and tick.t_dealid = leg.t_dealid
                      and leg.t_legkind = 0
                      and leg.t_legid = 1
                      and (t_closedate = to_date('01.01.0001', 'dd.mm.yyyy') or t_closedate > p_Date)
                      and not exists (select 1 from dmmhstimp_dbt where t_dealid = tick.t_dealid and t_date > p_Date)
                      and not (leg.t_maturity < p_Date and exists(select 1 from doprdocs_dbt a, doproper_dbt b 
                                      where a.t_id_operation=b.t_id_operation 
                                      and a.t_dockind=102 and b.t_dockind=102 
                                      and b.t_DocumentID=lpad(tick.t_dealid,34,'0')))
                )
      loop
          
          v_stat := Load_rates_or_mbk_method_1(rec.t_Dealid, p_Date);

          if (v_stat = 1) then
             v_stat := Load_rates_or_mbk_method_2(rec.t_Dealid, p_Date);
          end if;

          if (v_stat = 1) then
            v_stat := Load_rates_or_mbk_method_3(rec.t_Dealid, rec.t_partyid, p_date);
          end if;

          if (v_stat = 1) then
            v_stat := Load_rates_or_mbk_method_4(rec.t_Dealid, rec.t_partyid, rec.t_duration);
          end if;

          commit;

      end loop;

  END Load_rates_or_mbk;

/**********************************Сделки РЕПО************************************************************************/

  FUNCTION Load_rates_repo_method_3 (p_DealID in integer, p_contractor in integer, p_date in date) return integer
  IS
    v_ContractorType varchar2(20 char) := '';
    v_ContractorRating integer := 0;
    v_IssuerKind integer := 0;
    v_rate number(32, 12) := 0.0;
    v_valuedate date;
  BEGIN
    
    v_ContractorType := GetIssuerType(p_contractor);
    v_ContractorRating := getRatingEntryNumber(p_contractor, p_date);
 
    if ((v_ContractorType = '') or (v_ContractorRating = 0) ) then
      return 1;
    end if;
   
    select msfo.rate, msfo.valuedate
    into v_rate, v_valuedate
    from USER_MSFORESERV msfo
    where method_id = METHOD_RATING_ID
          and object = v_ContractorType
          and stagefact = v_ContractorRating
          and valuedate = (select max(valuedate) from user_msforeserv
                           where     method_id = msfo.method_id
                                 and object = msfo.object
                                 and stagefact = msfo.stagefact);

    ConnectAttr(DEAL_OBJECTTYPE, DEAL_ATTRGROUP_METHOD, lpad(p_DealID, 34, '0'), METHOD_RATING_ID, v_valuedate);

  ConnectAttr(DEAL_OBJECTTYPE, DEAL_ATTRGROUP_RATING, lpad(p_DealID, 34, '0'), v_ContractorRating, v_valuedate);

   
    IF(v_ContractorType = CONTRACTOR_BANK) Then
        v_IssuerKind := 1;
    ELSIF(v_ContractorType = CONTRACTOR_SOVEREIGN) Then 
        v_IssuerKind := 2;
    ELSIF(v_ContractorType = CONTRACTOR_SUBSOVEREIGN) Then 
        v_IssuerKind := 3;
    ELSIF(v_ContractorType = CONTRACTOR_CORPORATE) Then 
        v_IssuerKind := 4;
    END IF;  

  ConnectAttr(DEAL_OBJECTTYPE, DEAL_ATTRGROUP_ISSUERKIND, lpad(p_DealID, 34, '0'), v_IssuerKind, v_valuedate);

    return addnoteforobject(DEAL_OBJECTTYPE, DEAL_NOTEKIND_OR, lpad(p_DealID, 34, '0'), v_rate, v_valuedate);

  EXCEPTION
    WHEN no_data_found THEN
      return 1;
    WHEN others THEN
      return 1;
  END Load_rates_repo_method_3;
  
  FUNCTION Load_rates_or_repo_method_4 (p_DealID in integer, p_Contractor in integer, p_days in integer) return integer
  IS
    v_ContractorType varchar2(20 char) := '';
    v_term integer := -1;
    v_rate number(32, 12) := 0.0;
    v_valuedate date;
  BEGIN
    v_ContractorType := GetIssuerType(p_Contractor);
    v_term := GetTermValue(p_days);
     
    select msfo.rate, msfo.valuedate
    into v_rate, v_valuedate
    from USER_MSFORESERV msfo
    where     method_id = METHOD_TTM_ID
          and objecttype = OR_OBJECTTYPE_REPO
          and object = v_ContractorType
          and stagefact = v_term
          and valuedate = (select max(valuedate) from user_msforeserv
                           where     method_id = msfo.method_id
                                 and object = msfo.object
                                 and stagefact = msfo.stagefact);

    ConnectAttr(DEAL_OBJECTTYPE, DEAL_ATTRGROUP_METHOD, lpad(p_DealID, 34, '0'), METHOD_TTM_ID, v_valuedate);

    return addnoteforobject(DEAL_OBJECTTYPE, DEAL_NOTEKIND_OR, lpad(p_DealID, 34, '0'), v_rate, v_valuedate);
    
    EXCEPTION
    WHEN others THEN
      return 1;
  END Load_rates_or_repo_method_4;

  --Для сделок РЕПО не реализованы методы 1 и 2, т.к. не было нормальных изначальных данных для теста, и пока не понятно, по какому принципу эти сделки искать
  PROCEDURE Load_rates_reserve_REPO (p_Date in DATE)
  IS
    v_stat INTEGER;
  BEGIN
    for rec in (select tick.t_dealid, tick.t_partyid,
                       leg.t_maturity - p_Date as duration
                from ddl_tick_dbt tick, ddl_leg_dbt leg
                where     rsb_secur.IsRepo(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(TICK.t_DealType, TICK.t_BofficeKind))) = 1
                      and RSB_SECUR.IsBuy(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(TICK.t_DealType, TICK.t_BofficeKind))) = 1
                      and tick.t_dealdate <= p_Date
                      and tick.t_dealid = leg.t_Dealid
                      and leg.t_legkind = 2
                      and (greatest(leg.t_maturity, leg.t_expiry) > p_Date or tick.t_closedate = to_Date('01.01.0001', 'dd.mm.yyyy'))
                      and not exists (select 1 from dnotetext_dbt note
                                      where     t_objecttype = DEAL_OBJECTTYPE and t_notekind = DEAL_NOTEKIND_OR
                                            and t_documentid = lpad(tick.t_dealid, 34, 0) and t_date > p_Date)
               )
    loop
       v_stat := Load_rates_repo_method_3(rec.t_Dealid, rec.t_partyid, p_date);

       if (v_stat = 1) then
          v_stat := Load_rates_or_repo_method_4(rec.t_Dealid, rec.t_partyid, rec.duration);
       end if;
          
    end loop;
  END Load_rates_reserve_REPO;

  --Загрузка ставок оценочных резервов
  PROCEDURE Load_rates_or (p_Date in DATE)
  is

  BEGIN 
     Load_rates_or_va(p_Date);
     Load_rates_or_avr(p_Date);
     Load_rates_or_deals(p_Date);
     Load_rates_or_mbk(p_Date);
     Load_rates_reserve_REPO(p_Date);
  END Load_rates_or;

END rshb_reserve_or;
/
