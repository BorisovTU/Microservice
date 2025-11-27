CREATE OR REPLACE PACKAGE BODY it_ndbole AS
/******************************************************************************
   NAME:       it_ndbole
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        05.09.2024      Geraskina-TV       1. Created this package body.
******************************************************************************/
 
  TYPE QualifiedInvestor_rec IS RECORD (
    IsQualifiedInvestor BOOLEAN, 
    QualifiedInvestorState NUMBER,
    QualifiedStartDate  DATE, 
    QualifiedEndDate    DATE
   );

  g_cur varchar2(5);

  TYPE Contract_rec IS RECORD (
    DlcontrID   number(10), 
    SfcontrID   number(10),
    PartyID     number(10),
    ContractDate date
   );

  /**
  * Упаковщик исходящх сообшений через KAFKA
  * @since RSHB 110
  * @qtest NO
  * @param p_message Исходное сообщение
  * @param p_expire 
  * @param o_correlation
  * @param o_messbody Упакованное сообщение
  * @param o_messmeta 
  */
  PROCEDURE out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype) as
    v_rootElement   itt_kafka_topic.rootelement%type;
    v_msg_format    itt_kafka_topic.msg_format%type;
    vj_in_messbody  clob;
    vj_out_messbody clob;
    vz_GUID         itt_q_message_log.msgid%type;
    vz_GUIDReq      itt_q_message_log.corrmsgid%type;
    vz_ErrorCode    itt_q_message_log.msgcode%type;
    vz_ErrorDesc    itt_q_message_log.msgtext%type;
    v_select        varchar2(2000);
  BEGIN
    o_messmeta    := p_message.MessMETA;
    --
    begin
      select t.rootelement
            ,t.msg_format
        into v_rootElement
            ,v_msg_format
        from itt_kafka_topic t
       where t.system_name = C_C_SYSTEM_NAME
         and t.servicename = p_message.ServiceName
         and t.queuetype = it_q_message.C_C_QUEUE_TYPE_OUT;
    exception
      when no_data_found then
        raise_application_error(-20000
                               ,'Сервис ' || p_message.ServiceName || ' не зарегистрирован как ' || it_q_message.C_C_QUEUE_TYPE_OUT || ' для KAFKA/' ||
                                C_C_SYSTEM_NAME);
    end;
    
    if p_message.message_type = it_q_message.C_C_MSG_TYPE_R
       and upper(v_rootElement) not like '%REQ'
    then
      raise_application_error(-20001
                             ,'Сообщение для KAFKA/' || C_C_SYSTEM_NAME || ' с   RootElement ' || v_rootElement || chr(10) ||
                              ' должно быть ответом от сервиса ' || p_message.ServiceName || ' сформирован запрос на отправку! ');
    elsif p_message.message_type = it_q_message.C_C_MSG_TYPE_A
          and upper(v_rootElement) not like '%RESP'
    then
      raise_application_error(-20001
                             ,'Сообщение для KAFKA/' || C_C_SYSTEM_NAME || ' с   RootElement ' || v_rootElement || chr(10) ||
                              ' должно быть запросом к сервису ' || p_message.ServiceName || ' сформирован  ответ на отправку! ');
    end if;
    
    vj_in_messbody := p_message.Messbody;
    
    if vj_in_messbody is not null
    then
      vj_out_messbody := vj_in_messbody;
      
      if v_msg_format = 'JSON'
      then
        v_select        := 'select json_value(:1 ,''$."' || v_rootElement || '".GUID'') from dual';
        execute immediate v_select
          into vz_GUID
          using vj_out_messbody;
        
        if vz_GUID is null
        then
          raise_application_error(-20000
                                 ,'Для сервиса ' || p_message.ServiceName || chr(10) || ' зарегистрированого как ' || it_q_message.C_C_QUEUE_TYPE_OUT ||
                                  ' для KAFKA/' || C_C_SYSTEM_NAME || chr(10) || ' ожидался RootElement ' || v_rootElement || ' (ошибка формата JSON)');
        elsif p_message.msgid != vz_GUID
        then
          raise_application_error(-20001, 'Значение Msgid != GUID ! ');
        end if;
      
        if p_message.message_type = it_q_message.C_C_MSG_TYPE_A
        then
          v_select := 'select json_value(sJSON ,''$."' || v_rootElement || '".GUIDReq'' )
                   ,json_value(sJSON ,''$."' || v_rootElement || '".ErrorList.Error[0].ErrorCode'')
                   ,json_value(sJSON ,''$."' || v_rootElement || '".ErrorList.Error[0].ErrorDesc'')
            from (select :1 as sJSON from dual)';
          execute immediate v_select
            into vz_GUIDReq, vz_ErrorCode, vz_ErrorDesc
            using vj_out_messbody;
          if vz_GUIDReq is null
          then
            raise_application_error(-20001, 'Отсутствует обязательный елемент GUIDReq ! ');
          end if;
          if vz_ErrorCode is null
          then
            raise_application_error(-20001, 'Отсутствует обязательный елемент ErrorCode ! ');
          end if;
          if p_message.CORRmsgid != vz_GUIDReq
          then
            raise_application_error(-20001, 'Значение CORRmsgid != GUIDReq ! ');
          end if;
          if p_message.MSGCode != vz_ErrorCode
          then
            raise_application_error(-20001, 'Значение MSGCode != ErrorCode ! ');
          end if;
          if p_message.MSGCode != 0
             and (vz_ErrorDesc is null or vz_ErrorDesc != p_message.MSGText)
          then
            raise_application_error(-20001, 'Значение ErrorDesc должно быть "' || p_message.MSGText || '"! ');
          end if;
        end if;
      end if;
    else
    
      if p_message.message_type = it_q_message.C_C_MSG_TYPE_A
         and p_message.MSGCode != 0
      then
        select json_object(v_rootElement value
                           json_object('GUID' value p_message.msgid
                                      ,'GUIDReq' value p_message.CORRmsgid
                                      ,'RequestTime' value p_message.RequestDT
                                      ,'ErrorList' value json_object('Error' value json_array(json_object('ErrorCode' value to_char(p_message.MSGCode))
                                                             ,json_object('ErrorDesc' value p_message.MSGText)))) FORMAT JSON)
          into vj_out_messbody
          from dual;
      else
        raise_application_error(-20001, 'Отправляемое сообщение не должно быть пустое ! ');
      end if;
    end if;
    
    o_messbody := vj_out_messbody;
    o_correlation := it_kafka.get_correlation(p_Receiver => C_C_SYSTEM_NAME , p_len_MessBODY => dbms_lob.getlength(o_messbody));
  END;


  /**
  * Получение идентификатора клиента по виду кода "Код ЦФТ"  
  * @since RSHB 110
  * @qtest NO
  * @param p_PartyCode Код клиента
  * @return идентификатор клиента
  */
  FUNCTION GetPartyIDByCFT(p_PartyCode IN VARCHAR2) 
    RETURN NUMBER
  IS
    v_PartyID dparty_dbt.t_PartyID%TYPE;
  BEGIN
    SELECT t_ObjectID INTO v_PartyID
      FROM dobjcode_dbt 
     WHERE t_ObjectType = PM_COMMON.OBJTYPE_PARTY
       AND t_CodeKind = PTCK_CFT
       AND t_Code = p_PartyCode 
       AND t_State = 0;
    RETURN v_PartyID;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN -1;
  END;
  
  
  /**
  * Получение ЕКК для договора
  * @since RSHB 110
  * @qtest NO
  * @param  p_DlcontrID  Идентификатор договора
  * @return ЕКК
  */
  FUNCTION GetEKK(p_DlcontrID IN NUMBER) 
    RETURN varchar2
  IS
    v_code ddlobjcode_dbt.t_code%TYPE;
  BEGIN
    SELECT t_code INTO v_code
      FROM ddlobjcode_dbt 
     WHERE t_ObjectType = OBJECTTYPE_CONTRACT
       AND t_CodeKind = DLCK_EKK
       AND t_objectid = p_DlcontrID 
       AND t_bankclosedate = to_date('01010001','ddmmyyyy');
    RETURN v_code;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN -1;
  END;

  
/**
  * Проверка наличия договоров БО для переданного клиента 
  * @since RSHB 110
  * @qtest NO
  * @param p_PartyID   Идентификатор клиента
  * @return количество открытых на текущую дату договоров в статусе "обработка завершена"
  */
  FUNCTION CheckDBO(p_PartyID IN NUMBER) 
    RETURN NUMBER
  IS
    v_ret number(10) := 0;
  BEGIN
    SELECT count(1) INTO v_ret
      FROM dsfcontr_dbt sf, 
           ddlcontr_dbt dl 
     WHERE sf.t_id = dl.t_sfcontrid 
       AND sf.t_PartyID = p_PartyID
       AND sf.t_DateBegin <= trunc(sysdate) 
       AND (t_DateClose = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR t_DateClose >= trunc(sysdate))
       AND exists (select 1 from dobjatcor_dbt o where o.t_objecttype = OBJECTTYPE_CONTRACT and o.t_groupid = CONTRACT_CATEGORY_STATUS 
                      and o.t_attrid = STATUS_CONTRACT_FINISHED and (o.t_validtodate >= trunc(sysdate) or o.t_validtodate = to_date('01.01.0001','dd.mm.yyyy')) 
                      and lpad(dl.t_dlcontrid,34,'0') = o.t_object );
    RETURN v_ret;
  END;
  
  
/**
  * Получение идентификатора договора по номеру договора
  * @since RSHB 115
  * @qtest NO
  * @param p_ContractNumber Номер договора
  * @param p_ContractNumber Номер договора
  * @param p_ContractNumber Номер договора
  * @return идентификаторы договора БО
  */
  FUNCTION GetDlContrIDByContractNumber(p_ContractNumber IN varchar2, 
                                        p_ReportStartDate IN date, 
                                        p_ReportEndDate IN date) 
    RETURN Contract_rec
  IS
    v_ret Contract_rec;
  BEGIN
    v_ret.DlcontrID := -1;
    v_ret.SfcontrID := -1;
    v_ret.PartyID := -1;
    v_ret.ContractDate := to_date('01010001','ddmmyyyy');
    
    BEGIN
      SELECT dl.t_dlcontrid, sf.t_id, sf.t_partyid, sf.t_datebegin
        INTO v_ret.DlcontrID, v_ret.SfcontrID, v_ret.PartyID, v_ret.ContractDate
        FROM dsfcontr_dbt sf, 
            ddlcontr_dbt dl 
       WHERE sf.t_id = dl.t_sfcontrid 
         AND sf.t_DateBegin <= p_ReportEndDate 
         AND (t_DateClose = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR t_DateClose >= p_ReportStartDate)
         AND exists (select 1 from dobjatcor_dbt o where o.t_objecttype = OBJECTTYPE_CONTRACT and o.t_groupid = CONTRACT_CATEGORY_STATUS 
                      and o.t_attrid = STATUS_CONTRACT_FINISHED and (o.t_validtodate >= p_ReportEndDate or o.t_validtodate = to_date('01.01.0001','dd.mm.yyyy')) 
                      and lpad(dl.t_dlcontrid,34,'0') = o.t_object )
         AND sf.t_number = p_ContractNumber;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN 
        v_ret.DlcontrID := -1;
        v_ret.SfcontrID := -1;
        v_ret.PartyID := -1;
    END;
    
    return v_ret;
    
  END;
  
  
/**
  * Признак выбора необеспеченных сделок
  * @since RSHB 110
  * @qtest NO
  * @param p_PartyID   Идентификатор клиента
  * @return true/false в виде строки
  
  Для использования внутри селекта необходимо строковое значение, преобразование в boolean происходит через format json
  */
  FUNCTION GetIsSelectImperfectTransactions(p_partyid IN number) 
    RETURN varchar2
  IS 
    res varchar2(5) := 'false';
    vAttrid dobjatcor_dbt.t_attrid%type;
  BEGIN
    select t_attrid 
      into vAttrid
      from dobjatcor_dbt 
     where t_objecttype = PM_COMMON.OBJTYPE_PARTY 
       and t_groupid = CATEGORY_IMPERFECT_TRANSACTION
       and t_object = lpad(p_partyid,10,'0') 
       and t_validtodate = to_date ('31129999','ddmmyyyy');
    
    if vAttrid = CATEGORY_VALUE_YES then
      res := 'true';
    end if;

    return res;
  EXCEPTION
    when no_data_found then 
      return 'false';
  END;


/**  
  * Получить примечание договора
  * @since RSHB 110
  * @qtest NO
  * @param p_DlcontrID  Идентификатор договора
  * @param p_Notekind   Номер примечания
  * @return Значение примечания
  */
  FUNCTION GetNoteKindFromContract(p_DlcontrID in NUMBER, p_Notekind in NUMBER) 
    RETURN varchar2 
  IS
    res varchar2(1500);
    v_notetext dnotetext_dbt.t_text%type;
  BEGIN
    res := null;
    select n.t_text 
      into v_notetext
      from  dnotetext_dbt n
     where n.t_objecttype = OBJECTTYPE_CONTRACT
       and n.t_documentid = lpad(p_DlcontrID,34,'0')
       and n.t_notekind = p_Notekind
       and n.t_validtodate = to_date('31/12/9999','dd/mm/yyyy');
       
    res:= case p_Notekind 
            when CONTRACT_NOTEKIND_DEPO_CONTRACTNUMBER then replace(rsb_struct.getString(v_notetext),chr(0),'')
            when CONTRACT_NOTEKIND_DEPO_ACCOUNT then replace(rsb_struct.getString(v_notetext),chr(0),'')
            when CONTRACT_NOTEKIND_DEPO_TRADEACCOUNT then replace(rsb_struct.getString(v_notetext),chr(0),'')
            when CONTRACT_NOTEKIND_TARIFFID_REPO then replace(rsb_struct.getString(v_notetext),chr(0),'')
            when CONTRACT_NOTEKIND_TARIFFPLAN_REPO then replace(rsb_struct.getString(v_notetext),chr(0),'')
            when CONTRACT_NOTEKIND_DEPO_CONTRACTDATE then to_char(rsb_struct.getdate(v_notetext), 'yyyy-mm-dd')
            else null
          end;
  
    return res;
  EXCEPTION
    when NO_DATA_FOUND then
      return null;
  END;


/**  
  * Получить признак субдоговора 
  * @since RSHB 110
  * @qtest NO
  * @param p_DlcontrID  Идентификатор договора
  * @param p_Groupid    Номер категории
  * @param p_Value_yes  Значение "да"
  * @return 1 - 'true', 2 - 'false'
  
  Строковое значение для использования в select 
  */
  FUNCTION GetBrokerCategoryValue(p_DlcontrID in NUMBER, p_Groupid in number, p_Value_yes in number) 
    RETURN varchar2 
  IS
    res varchar2(5) := 'false';
    v_cnt number(10) := 0;
  BEGIN

    select count(*)
      into v_cnt
      from  dobjatcor_dbt o,
            dsfcontr_dbt sf,
            ddlcontrmp_dbt mp
     where sf.t_id = mp.t_sfcontrid
       and o.t_objecttype = OBJECTTYPE_SUBCONTRACT and o.t_groupid = p_Groupid
       and o.t_object = lpad(sf.t_id,10,'0') and o.t_validtodate = to_date('31129999','ddmmyyyy')
       and o.t_attrid = p_Value_yes
       and mp.t_dlcontrid = p_DlcontrID;

    if p_Groupid = SUBCONTRACT_CATEGORY_RIGHTUSESTOCK then
      res:= case v_cnt 
              when 0 then 'false'
              else 'true'
            end;
    elsif p_Groupid = SUBCONTRACT_CATEGORY_EDP then
      res:= case v_cnt 
        when 0 then 'false'
        else 'true'
      end;
    else 
      res := 'false';
    end if;

    return res;
  END;


/**  
  * Получить счета по субдоговору
  * @since RSHB 110
  * @qtest NO
  * @param p_DlcontrID  Идентификатор договора
  * @param p_ShowRest  Отображать остаток, для ЕДП остаток только для ММВБ и внебиржи
  * @return Список счетов
  */
  FUNCTION GetBrokerAccountList(p_sfcontrID in NUMBER, p_ShowRest in number) 
    RETURN clob
  IS 
    res clob;
  BEGIN
    
    with acc as (
      SELECT  ac.t_code_currency,
              ac.t_account, 
              RSI_RSB_ACCOUNT.restall( ac.t_account, 1, ac.t_code_currency, trunc(sysdate)) restacc,
              fin.t_ccy
        FROM  ddlcontrmp_dbt mp,
              dsfcontr_dbt sf,
              dmcaccdoc_dbt mcacc,
              daccount_dbt ac,
              dfininstr_dbt fin
       WHERE mcacc.t_clientcontrid = sf.t_id
         AND mcacc.t_owner = sf.t_partyid
         AND mcacc.t_iscommon = CHR(88)
         AND mcacc.t_catid = 70
         AND mcacc.t_chapter = ac.t_chapter
         AND mcacc.t_currency = ac.t_code_currency
         AND mcacc.t_account = ac.t_account
         AND sf.t_id = mp.t_sfcontrid
         AND sf.T_ID = p_sfcontrID
         AND fin.t_fiid = ac.t_code_currency
         AND fin.t_fi_kind = 1
     )
      SELECT JSON_OBJECT ( key 'BrokerAccount'  value
                 json_arrayagg( JSON_OBJECT ( key 'AccountNumber' value acc.t_account,
                                              key 'Saldo'         value to_char(case p_ShowRest 
                                                                                  when 0 then 0
                                                                                  else acc.restacc
                                                                                end,
                                                                                'FM99999999999999990D000000',
                                                                                'NLS_NUMERIC_CHARACTERS = ''.,'''
                                                                                ),
                                              key 'CurrencyCode'  value acc.t_ccy
                                                returning clob
                                    ) 
                                absent on null returning clob )
                          format json returning clob)
        INTO res
        FROM acc;

    return res;
  EXCEPTION
    when NO_DATA_FOUND then return null;
  END;


/**  
  * Получить стоимость ценной бумаги
  * @since RSHB 110
  * @qtest NO
  * @param p_fiid  Идентификатор фин.инструмента
  * @param p_facevaluefi  Валюта номинала
  * @param p_daterate  Дата курса
  * @param p_mp_mmvb  ID ММВБ
  * @param p_mp_spb  ID СПБ
  * @return Стоимость бумаги в валюте курса или 0, если курсы не найдены
  
    ISO-код валюты курса сохраняется в глобальную переменную, так как для селекта нельзя использовать параметры OUT
  */
  FUNCTION GetCost(p_fiid in number, p_facevaluefi in number, p_daterate in date, p_mp_mmvb in number, p_mp_spb in number) 
    RETURN number
  IS 
    v_rate number;
    v_toFi number;
    v_ccy varchar2(3);
  BEGIN
    v_rate := GetRateFin(p_fiid, p_daterate, p_mp_mmvb, p_mp_spb, v_toFi); 
    if v_rate = 0 then 
      v_toFi := p_facevaluefi;
    end if;
    
    begin 
      select t_ccy into v_ccy 
        from dfininstr_dbt 
       where t_fiid = v_toFi;
    exception
       when no_data_found then v_ccy := '---';
    end;
    
    g_cur := v_ccy;
    return v_rate;
  
  END;
  
  
/**  
  * Получить значение глобальной переменной
  * @since RSHB 110
  * @qtest NO
  * @return Значение
  
    ISO-код валюты курса сохраняется в глобальную переменную, так как для селекта нельзя использовать параметры OUT
  */
  FUNCTION GetGCur
    RETURN varchar2 
  is 
  BEGIN
    return g_cur;
  END;


/**  
  * Получить депо-счета и остатки бумаг по субдоговору
  * @since RSHB 110
  * @qtest NO
  * @param p_DlcontrID  Идентификатор договора
  * @param p_sfcontrID  Идентификатор субдоговора
  * @param p_servkind  Наименование площадки
  * @param pmarketid_moex  ID ММВБ
  * @param pmarketid_spb  ID СПБ
  * @return Список депо-счетов 
  */
  FUNCTION GetDepoAccountList(p_DlcontrID in NUMBER, p_sfcontrID in NUMBER, p_servkind in varchar2, pmarketid_moex in number, pmarketid_spb in number) 
    RETURN clob
  IS 
    res clob;
    
    vnotekind_depo number(10);
    vdepoAccount_note varchar2(1500);
    
  BEGIN
  
    vnotekind_depo := case p_servkind
                        when 'ММВБ' then CONTRACT_NOTEKIND_DEPO_TRADEACCOUNT
                        when 'Внебиржа' then CONTRACT_NOTEKIND_DEPO_ACCOUNT
                        else 0
                      end;
      
    if vnotekind_depo > 0 then
      vdepoAccount_note := GetNoteKindFromContract(p_DlcontrID, vnotekind_depo);
      if vdepoAccount_note is not null then -- счет есть

        WITH depoacc as (
          SELECT a.*  
            FROM (SELECT sfcontr.t_number, sfcontr.t_id, sfcontr.t_servkindsub, 
                          -1 * rsb_account.restac(accd.t_Account, 
                                                  accd.t_Currency, 
                                                  trunc(SYSDATE), 
                                                  accd.t_Chapter, 
                                                  NULL) AS restcb, 
                          a.t_isin,
                          f.t_name, f.t_fiid, f.t_facevaluefi
                    FROM dsfcontr_dbt sfcontr, 
                         dmcaccdoc_dbt accd, 
                         dmccateg_dbt cat, 
                         davoiriss_dbt a, 
                         dfininstr_dbt f 
                   WHERE accd.t_Chapter = 22 
                     AND accd.t_ClientContrID = sfcontr.t_id 
                     AND accd.t_iscommon = chr(88) 
                     AND accd.t_owner = sfcontr.t_partyid 
                     AND cat.t_Id = accd.t_CatID 
                     AND cat.t_LevelType = 1 
                     AND cat.t_Code = 'ЦБ Клиента, ВУ' 
                     AND a.t_fiid = accd.t_currency 
                     AND f.t_fi_kind = 2 
                     AND a.t_fiid = f.t_fiid 
                     AND sfcontr.t_id = p_sfcontrID
                  ) a 
          )
        SELECT json_object ( key 'DepoAccount'  value
                   nvl( json_arrayagg( json_object (  key 'DepoAccountNumber' value vdepoAccount_note,
                                                      key 'Securities' value 
                                                        json_object ( key 'SecuritiesName' value depoacc.t_name,
                                                                      key 'SecuritiesISIN' value  case depoacc.t_isin 
                                                                                                    when chr(1) then ' '
                                                                                                    else depoacc.t_isin
                                                                                                  end,
                                                                      key 'SecuritiesAmount' value  to_char(round(depoacc.restCB,6),
                                                                                                            'FM99999999999999990D000000',
                                                                                                            'NLS_NUMERIC_CHARACTERS = ''.,'''),
                                                                      key 'SecuritiesCost' value to_char( round(depoacc.restCB*GetCost(depoacc.t_fiid,depoacc.t_facevaluefi, 
                                                                                                                        trunc(sysdate), pmarketid_moex, pmarketid_spb),6),
                                                                                                          'FM99999999999999990D000000',
                                                                                                          'NLS_NUMERIC_CHARACTERS = ''.,'''),
                                                                      key 'SecuritiesCurrencyCode' value GetGCur
                                                                        returning clob
                                                                     )
                                                        returning clob
                                                     )
                                          returning clob
                                 ), json_array( json_object( key 'DepoAccountNumber' value vdepoAccount_note,
                                                             key 'Securities' value json_object() 
                                                             returning clob
                                                           )
                                                returning clob
                                              )
                      )
                            format json absent on null returning clob )
          INTO res
          FROM depoacc where restCB <> 0;
      end if;
    end if;

    return res;
  EXCEPTION
    when NO_DATA_FOUND then return null;
  END;


/**  
  * Получить инструменты срочного рынка
  * @since RSHB 110
  * @qtest NO
  * @param p_DlcontrID  Идентификатор договора
  * @return Список инструментов 
  */
  FUNCTION GetFutureList(p_DlcontrID in NUMBER) 
    RETURN clob
  IS 
    res clob;
    
    vsf_id number(10);
    
    C_SERVKIND_FORTS constant number := 15;
    C_OBJECTYPE_FININSTR constant number := 9;
    C_CODEKIND_FININSTR constant number := 11;
  BEGIN
  
    SELECT cntr.t_id
      INTO vsf_id
      FROM dsfcontr_dbt cntr, 
           ddlcontrmp_dbt mp 
     WHERE cntr.t_id = mp.t_sfcontrid 
       AND mp.t_dlcontrid = p_DlcontrID
       AND mp.t_mpclosedate = to_date('01.01.0001','dd.mm.yyyy') 
       AND cntr.t_dateclose = to_date('01.01.0001','dd.mm.yyyy') 
       AND cntr.t_servkind = C_SERVKIND_FORTS;
       
    WITH forts AS (
      SELECT avr.t_name as t_type, obj.t_code, fi.t_name, (dft.t_LongPosition - dft.t_ShortPosition) as Rest,
             (dft.t_longpositioncost - dft.t_shortpositioncost ) as OutCost 
        FROM ddvfipos_dbt t 
   LEFT JOIN dfininstr_dbt fi ON t.t_fiid = fi.t_fiid 
   LEFT JOIN dobjcode_dbt obj ON obj.t_objecttype = C_OBJECTYPE_FININSTR AND obj.t_codekind = C_CODEKIND_FININSTR AND obj.t_objectid = fi.t_fiid 
   LEFT JOIN davrkinds_dbt avr ON fi.t_avoirkind = avr.t_avoirkind and fi.t_fi_kind = avr.t_fi_kind 
   LEFT JOIN ddvfiturn_dbt dft ON dft.t_fiid = t.t_fiid AND dft.t_Department = t.t_Department AND dft.t_Broker = t.t_Broker 
                                  AND dft.t_ClientContr = t.t_ClientContr AND dft.t_GenAgrID = t.t_GenAgrID 
                                  AND (dft.t_Date is null OR dft.t_Date =  
                                             NVL ((SELECT MAX (tableDVF.t_Date) t_TurnDate 
                                                     FROM ddvfiturn_dbt tableDVF 
                                                    WHERE tableDVF.t_FIID = dft.t_FIID AND tableDVF.t_Department = dft.t_Department 
                                                      AND tableDVF.t_Broker = dft.t_Broker AND tableDVF.t_ClientContr = dft.t_ClientContr 
                                                      AND tableDVF.t_GenAgrID = dft.t_GenAgrID), trunc(sysdate)))
       WHERE t.t_clientcontr = vsf_id and t.t_state = 1)
    SELECT json_object ( key 'Futures'  value
                          nvl( json_arrayagg( json_object ( key 'FuturesNumber' value forts.t_code,
                                                            key 'FuturesName' value forts.t_name,
                                                            key 'FuturesQuantity' value to_char( round(forts.Rest,6),
                                                                                                 'FM99999999999999990D000000',
                                                                                                 'NLS_NUMERIC_CHARACTERS = ''.,'''),
                                                            key 'FuturesType' value forts.t_type,
                                                            key 'OutCostRest' value forts.OutCost
                                                              absent on null returning clob
                                                       )
                                          returning clob
                                         ), '[]') 
                            format json returning clob )
      INTO res
      FROM forts where Rest is not null;

    return res;
  EXCEPTION
    when NO_DATA_FOUND then return null;
  END;


/**  
  * Получить торговые площадки по договору
  * @since RSHB 110
  * @qtest NO
  * @param p_DlcontrID  Идентификатор договора
  * @return Список площадок
  */
  FUNCTION GetTradingPlatformList(p_DlcontrID in NUMBER) 
    RETURN clob
  IS
    res clob;
    
    vDateEDP_str  varchar2(10);
    vDateEDP      date;
    vIsEDP        number := 1;
    vEDPOn        boolean := true;
    
    vmarketid_spb   number(10);
    vmarket_spb     varchar2(255);
    vmarketid_moex  number(10);
    vmarket_moex    varchar2(255);
    
    vEKK          ddlobjcode_dbt.t_code%type;
  BEGIN
  
    vmarket_moex := RSB_Common.GetRegStrValue('SECUR\MICEX_CODE');
    if vmarket_moex is not null then
      BEGIN
        SELECT t_partyid INTO vmarketid_moex
          FROM dpartcode_dbt
         WHERE t_code = vmarket_moex
           AND t_codekind = 1;
      EXCEPTION WHEN OTHERS 
        THEN vmarketid_moex := 0;
      END;
    else 
      vmarketid_moex := 0;
    end if;

    vmarket_spb := RSB_Common.GetRegStrValue('SECUR\SPBEX_CODE');
    if vmarket_spb is not null then
      BEGIN
        SELECT t_partyid INTO vmarketid_spb
          FROM dpartcode_dbt
         WHERE t_code = vmarket_spb
           AND t_codekind = 1;
      EXCEPTION WHEN OTHERS 
        THEN vmarketid_spb := 0;
      END;
    else 
      vmarketid_spb := 0;
    end if;
    
    vDateEDP_str := nvl(RSB_Common.GetRegStrValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ДАТА ВКЛЮЧЕНИЯ ЕДП'),'00.00.0000');
    if vDateEDP_str != '00.00.0000' then
      vDateEDP := to_date(vDateEDP_str,'dd.mm.yyyy');
      if trunc(sysdate) >= vDateEDP then
        vEDPOn := true;
      end if;
    else 
      vEDPOn := false;
    end if;
    
    if vEDPOn then 
        vIsEDP := case GetBrokerCategoryValue(p_DlcontrID, SUBCONTRACT_CATEGORY_EDP, SUBCONTRACT_CATEGORY_EDP_YES)
                    when 'true' then 1
                    else 0
                  end;
    else 
        vIsEDP := 0;
    end if;
    vEKK   := GetEKK(p_DlcontrID);
    
    with subcontr as (
      SELECT 
          CASE 
            when cntr.t_servkind = 1 and t_servkindsub = 8 and t_marketid = vmarketid_moex then 'ММВБ'
            when cntr.t_servkind = 1 and t_servkindsub = 8 and t_marketid = vmarketid_spb then 'СПБ'
            when cntr.t_servkind = 1 and t_servkindsub = 9 then 'Внебиржа' 
            when cntr.t_servkind = 21 then 'MMVB' 
            when cntr.t_servkind = 15 then 'FORTS' 
            else '-' 
          END servkind,
          mp.t_mpcode, mp.t_marketid,
          cntr.t_id
        FROM  dsfcontr_dbt cntr, 
              ddlcontrmp_dbt mp 
       WHERE cntr.t_id = mp.t_sfcontrid 
         AND mp.t_dlcontrid = p_DlcontrID
         AND mp.t_mpclosedate = to_date('01.01.0001','dd.mm.yyyy') 
         AND cntr.t_dateclose = to_date('01.01.0001','dd.mm.yyyy') 
    ORDER BY t_servkind )
      SELECT  JSON_OBJECT ( key 'TradingPlatform'  value
                 json_arrayagg( JSON_OBJECT (
                                  key 'TradingPlatformName' value subcontr.servkind,
                                  key 'ClientCode'          value case subcontr.servkind
                                                                    when 'СПБ' then subcontr.t_mpcode
                                                                    else vEKK
                                                                  end,
                                  key 'BrokerAccountList' value it_ndbole.GetBrokerAccountList(subcontr.t_id,
                                                                                                case 
                                                                                                  when vIsEDP = 1 and subcontr.servkind = 'ММВБ' then 1
                                                                                                  when vIsEDP = 1 and subcontr.servkind = 'Внебиржа' then 1
                                                                                                  when vIsEDP = 0 then 1
                                                                                                  else 0
                                                                                                end
                                                                                                ) format json,
                                  key 'DepoAccountList' value it_ndbole.GetDepoAccountList(p_DlcontrID, subcontr.t_id, subcontr.servkind, vmarketid_moex, vmarketid_spb) format json
                                    absent on null returning clob
                                  ) returning clob 
                              ) returning clob
                 ) 
        INTO res
        FROM subcontr;

    return res;
  EXCEPTION
    when NO_DATA_FOUND then return null;
  END;


/**  
  * Получить актуальные тарифы по договору
  * @since RSHB 110
  * @qtest NO
  * @param p_sfcontrID  Идентификатор dsfcontr_dbt
  * @param p_DlcontrID  Идентификатор договора
  * @return Список тарифов
  
  */
  FUNCTION GetTariffList(p_sfcontrID in NUMBER, p_DlcontrID in NUMBER) 
    RETURN clob
  IS
    res clob;
    tarif clob;
    
    tarif_rec clob;
    vbinddate date;
    vunbinddate date;
    vtarif_id varchar2(1500);
    vtarif_plan varchar2(1500);
    vindividual varchar2(5);
    
    TYPE Ind_t IS TABLE OF varchar2(5) INDEX BY PLS_INTEGER;
    vtarif_ind Ind_t;

    TYPE unbinddate_t IS TABLE OF date INDEX BY PLS_INTEGER;
    vtarif_unbinddate unbinddate_t;
    
    ai PLS_INTEGER;
    
  BEGIN
    
    ai := 0;
    for sfplan_brok in (
      select sfplan.t_id, sfplan.t_sfcontrid, sfplan.t_sfplanid, 
             sfplan.t_begin as BindDate, sfplan.t_end as UnbindDate, 
             tp.t_num as TariffId, tp.t_name as TariffPlan, 
             tp.t_begin as BeginDate, tp.t_end as EndDate, case tp.t_comment when chr(1) then ' ' else tp.t_comment end t_comment, 
             case tp.t_hidden when chr(88) then 'true' else 'false' end as t_hidden, 
             case nvl((select 1 from dobjatcor_dbt where t_objecttype = 57 and t_groupid = 101 
                         and t_attrid = 1 and t_object = lpad(tp.t_sfplanid,10,'0') and t_validtodate > trunc(sysdate)),0)
                  when 1 then 'true' else 'false' end as ind 
        from dsfcontrplan_dbt sfplan, 
             dsfplan_dbt tp, 
             dsfcontr_dbt sf 
       where sfplan.t_sfcontrid = sf.t_id 
         and sfplan.t_sfplanid = tp.t_sfplanid 
         and ( sfplan.t_end = to_date('01010001','ddmmyyyy') or sfplan.t_end >= trunc(sysdate) and sfplan.t_begin < trunc(sysdate)) 
         and sf.t_id = p_sfcontrID
         order by sfplan.t_begin ) loop
              
      SELECT   json_object (
                          key 'IsIndividual'  value sfplan_brok.ind format json,
                          key 'Comment'       value sfplan_brok.t_comment,
                          key 'IsHidden'      value sfplan_brok.t_hidden format json, 
                          key 'BeginDate'     value to_char(sfplan_brok.BeginDate,'yyyy-mm-dd'),
                          key 'EndDate'       value case sfplan_brok.EndDate
                                                      when to_date('01010001','ddmmyyyy') then null
                                                      else to_char(sfplan_brok.EndDate,'yyyy-mm-dd')
                                                    end,
                          key 'BindDate'      value to_char(sfplan_brok.BindDate,'yyyy-mm-dd'),
                          key 'UnbindDate'    value case sfplan_brok.UnbindDate
                                                      when to_date('01010001','ddmmyyyy') then null
                                                      else to_char(sfplan_brok.UnbindDate,'yyyy-mm-dd')
                                                    end, 
                          key 'TariffId'      value json_object ( key 'ObjectId' value to_char(sfplan_brok.TariffId),
                                                                  key 'SystemId' value 'SOFR',
                                                                  key 'SystemNodeId' value '0000'
                                                                 ),
                          key 'TariffKind'    value 'BROKERAGE',
                          key 'TariffPlan'    value sfplan_brok.TariffPlan
                            absent on null returning clob
                          )
      into tarif_rec
      from dual;
      
      if tarif is null then 
        tarif := tarif_rec;
      else 
        tarif := tarif||','||tarif_rec;
      end if;
      
      vtarif_ind(ai) := sfplan_brok.ind;
      vtarif_unbinddate(ai) := case sfplan_brok.UnbindDate
                                  when to_date('01010001','ddmmyyyy') then to_date('31129999','ddmmyyyy')
                                  else sfplan_brok.UnbindDate
                               end;
      ai := ai + 1;
      exit; -- возвращаем только первый действующий тариф (текущий) без будущих тарифов
      
    end loop;
    
    SELECT  json_object ( key 'Tariff'  value json_array( tarif format json) 
                            format json returning clob
                          ) 
      INTO res
      FROM dual;

    return res;
  EXCEPTION
    when NO_DATA_FOUND then return null;
  END;


/**  
  * Параметры квал.инвестора
  * @since RSHB 110
  * @qtest NO
  * @param p_PartyID   Идентификатор клиента
  * @return record с параметрами
  */
  FUNCTION GetQualifiedInvestor(p_PartyID in NUMBER) 
    RETURN QualifiedInvestor_rec 
  IS
    res QualifiedInvestor_rec;
  BEGIN
    res := null;
    select t_state, t_regdate, t_changedate
      into res.QualifiedInvestorState, res.QualifiedStartDate, res.QualifiedEndDate
      from dscqinv_dbt 
     where t_partyid = p_PartyID
       and rownum = 1;
    
    res.IsQualifiedInvestor := 
      case res.QualifiedInvestorState
        when 0 then false
        when 1 then true
        else null
      end;
    
    if res.QualifiedInvestorState <> 0 then -- НЕ исключен
      res.QualifiedEndDate := null;
    end if;

    return res;
  EXCEPTION
    when NO_DATA_FOUND then
      res.IsQualifiedInvestor := null;
    return res;
  END;

/**  
  * Отправка письма в автономной транзакции
  * @since RSHB 111
  * @qtest NO
  * @param p_text   Текст ошибки
  */
  procedure SendEmailError(p_text in varchar2) 
  is 
    C_POST_EMAIL constant varchar2(20) := 'broker@rshb.ru';
    
    pragma autonomous_transaction;
  begin
    INSERT INTO DEMAIL_NOTIFY_DBT ( T_HEAD,T_DATEADD, T_EMAIL,  T_TEXT ) 
    VALUES ('Ошибка сервиса GetBrokerageAgreementInfo',SYSDATE, C_POST_EMAIL, p_text);
    commit;
  end;

/**  
  * Фиксирование ошибки
  * @since RSHB 110
  * @qtest NO
  * @param p_text   Текст ошибки
  */
  procedure FixError(p_text in varchar2) 
  is 
  begin
    
    it_event.RegisterError( p_SystemId => C_C_SYSTEM_NAME,
                            p_ServiceName => 'GetBrokerageAgreementInfo',
                            p_ErrorCode => -1,
                            p_ErrorDesc => p_text,
                            p_LevelInfo => 8);
    SendEmailError(p_text);
  end;


/**  
  * Ответ json на запрос данных по клиенту 
  * @since RSHB 110
  * @qtest NO
  */
  procedure GetBrokerageAgreementInfo_json(p_worklogid integer
                                           ,p_messbody  clob
                                           ,p_messmeta  xmltype
                                           ,o_msgid     out varchar2
                                           ,o_MSGCode   out integer
                                           ,o_MSGText   out varchar2
                                           ,o_messbody  out clob
                                           ,o_messmeta  out xmltype) is 
                                     
    vMsgID      itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
    vGUIDReq    varchar2(2000);
    vRequestDate varchar2(100);
    
    vClientID   varchar2(2000);
    vPartyID    dparty_dbt.t_partyid%type;
    vCountDBO   number(10) := 0;
    
    v_json_req  json_object_t;
    
    C_NAME_REGVAL_ONOFF  varchar2(104) := 'РСХБ\ИНТЕГРАЦИЯ\ОБРАБОТКА ЗАПРОСА ОТ ДБО ЮЛ';
    vTurnOn char(1);
    
    v_json_resp  clob;
    v_reportdate varchar2(25);
    v_QualifiedInvestor QualifiedInvestor_rec;
    
    v_json_BrokerContractList clob;
    v_json_QualifiedInvestor clob;
    v_json_DepoContract clob;

  begin

    o_MSGCode := 0;
    o_MSGText := 'Успешно';
    o_msgid   := vMsgID;
    
    vTurnOn :=  nvl(RSB_Common.GetRegFlagValue(C_NAME_REGVAL_ONOFF),'0');
    if vTurnOn <> 'X' then
      o_MSGCode := -1;
      o_MSGText := 'Настройка '||C_NAME_REGVAL_ONOFF||' отключена';
      FixError(o_MSGText); 
      return;
    end if;
    
    if p_messbody is null then
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText:= 'Не передан входящий параметр p_messbody';
      FixError(o_MSGText);
      return;
    end if;

   /* select ClientId 
      into vClientID
      from json_table (p_messbody, '$.GetBrokerageAgreementInfoReq' columns ( GUID varchar2(2000) path '$.GUID',
                                              RequestTime varchar2(2000) path '$.RequestTime',
                                              ClientId varchar2(2000) path '$.ClientId.ObjectId'
                                             )
                       );*/
    /*в версиях Toad есть проблема с отображение json_table, поэтому через объект*/
    v_json_req := JSON_OBJECT_T(p_messbody);
    vClientID:= v_json_req.get_object('GetBrokerageAgreementInfoReq').get_Object('ClientId').get_string('ObjectId');
    vGUIDReq := v_json_req.get_object('GetBrokerageAgreementInfoReq').get_string('GUID');
    vRequestDate := v_json_req.get_object('GetBrokerageAgreementInfoReq').get_string('RequestTime');
    
    if vClientID is null then
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText:= 'Не удалось разобрать xml по причине: не удалось получить тег GetBrokerageAgreementInfoReq.ClientId.ObjectId';
      FixError(o_MSGText);
      return;
    end if;

    vPartyID := GetPartyIDByCFT(vClientID);
    if vPartyID = -1 then 
      o_MSGCode := ERROR_CLIENT_NOTFOUND;
      o_MSGText:= 'Не удалось найти клиента по идентификатору '||vClientID;
      return;
    end if;

    vCountDBO := CheckDBO(vPartyID);
    if vCountDBO <= 0 then
      o_MSGCode := ERROR_CONTRACT_NOTFOUND;
      o_MSGText:= 'Не удалось найти открытые договора в статусе "Обработка завершена" для клиента '||vClientID;
      return;
    end if;
    
    v_reportdate := it_xml.timestamp_to_char_iso8601(systimestamp);
    
    WITH contr as (
      SELECT dl.t_dlcontrid, dl.t_sfcontrid, sf.t_number, sf.t_datebegin, sf.t_partyid,
             dl.t_iis
        FROM dsfcontr_dbt sf, 
             ddlcontr_dbt dl 
       WHERE sf.t_id = dl.t_sfcontrid  
         AND sf.t_partyid = vPartyID
         AND (sf.t_dateclose = TO_DATE('01.01.0001', 'dd.mm.yyyy') or sf.t_dateclose > trunc(sysdate))
         AND sf.t_datebegin <= trunc(sysdate) 
         AND exists (select 1 from dobjatcor_dbt o 
                      where o.t_objecttype = OBJECTTYPE_CONTRACT and o.t_groupid = CONTRACT_CATEGORY_STATUS 
                        and o.t_attrid = STATUS_CONTRACT_FINISHED and (o.t_validtodate >= trunc(sysdate) or o.t_validtodate = to_date('01.01.0001','dd.mm.yyyy')) 
                        and lpad(dl.t_dlcontrid,34,'0') = o.t_object )
                  )
    SELECT  JSON_OBJECT( key 'BrokerContract'  value 
                            JSON_ARRAYAGG (
                              JSON_OBJECT (
                                key 'BrokerContractNumber'  value contr.t_number,
                                key 'BrokerContractDate'    value to_char(contr.t_datebegin,'yyyy-mm-dd'),
                                key 'BrokerContractStatus'  value upper('действующий'),
                                key 'IsBrokerRightUseSecurities'   value it_ndbole.GetBrokerCategoryValue(contr.t_dlcontrid, SUBCONTRACT_CATEGORY_RIGHTUSESTOCK, SUBCONTRACT_CATEGORY_RIGHTUSESTOCK_YES) format json,
                                key 'IsImperfectTransactionSelection' value it_ndbole.GetIsSelectImperfectTransactions(vPartyID) format json,
                                key 'TradingPlatformList'   value it_ndbole.GetTradingPlatformList(contr.t_dlcontrid) format json,
                                key 'TariffList'            value it_ndbole.GetTariffList(contr.t_sfcontrid, contr.t_dlcontrid) format json,
                                key 'FuturesList'           value it_ndbole.GetFutureList(contr.t_dlcontrid) format json
                                  absent on null returning clob
                                ) returning clob
                            ) returning clob
               ) 
      INTO v_json_BrokerContractList
      FROM contr;
    
    WITH DepoContract as (
      SELECT trim(it_ndbole.GetNoteKindFromContract(dl.t_dlcontrid, CONTRACT_NOTEKIND_DEPO_CONTRACTNUMBER)) DepoContractNumber,
             it_ndbole.GetNoteKindFromContract(dl.t_dlcontrid, CONTRACT_NOTEKIND_DEPO_CONTRACTDATE) DepoContractDate
        FROM ddlcontr_dbt dl, 
             dsfcontr_dbt sf
       WHERE dl.t_sfcontrid = sf.t_id 
         AND sf.t_partyid = vPartyID
         AND (sf.t_dateclose = TO_DATE('01.01.0001', 'dd.mm.yyyy') or sf.t_dateclose > trunc(sysdate))
         AND sf.t_datebegin <= trunc(sysdate)        
         AND exists (select 1 from dobjatcor_dbt o 
                      where o.t_objecttype = OBJECTTYPE_CONTRACT and o.t_groupid = CONTRACT_CATEGORY_STATUS 
                        and o.t_attrid = STATUS_CONTRACT_FINISHED and (o.t_validtodate >= trunc(sysdate) or o.t_validtodate = to_date('01.01.0001','dd.mm.yyyy')) 
                        and lpad(dl.t_dlcontrid,34,'0') = o.t_object )        
         AND EXISTS (SELECT 1 FROM dnotetext_dbt n 
                      WHERE n.t_objecttype = OBJECTTYPE_CONTRACT 
                        AND n.t_documentid = lpad(dl.t_dlcontrid, 34,0) 
                        AND n.t_notekind = CONTRACT_NOTEKIND_DEPO_CONTRACTNUMBER)
                          )
    SELECT JSON_OBJECT(key 'DepoContract'  value 
                          json_arrayagg( 
                              JSON_OBJECT(  key 'DepoContractNumber'  value DepoContract.DepoContractNumber,
                                            key 'DepoContractDate'    value DepoContract.DepoContractDate
                                          returning clob
                                ) returning clob
                              ) format json returning clob)
      INTO v_json_DepoContract 
      FROM DepoContract;
    
    v_QualifiedInvestor := GetQualifiedInvestor(vPartyID);
    
    if v_QualifiedInvestor.IsQualifiedInvestor is not null then
      v_json_QualifiedInvestor := json_object ( key 'IsQualifiedInvestor' value v_QualifiedInvestor.IsQualifiedInvestor,
                                                key 'QualifiedStatusAssignmentDate'  value to_char(v_QualifiedInvestor.QualifiedStartDate,'yyyy-mm-dd'),
                                                key 'QualifiedStatusEndDate'    value to_char(v_QualifiedInvestor.QualifiedEndDate,'yyyy-mm-dd') 
                                                  absent on null);
    else 
      v_json_QualifiedInvestor := null;
    end if;
    
    SELECT  json_object(  key 'ReportDate'  value v_reportdate,
                          key 'ClientInfo'  value 
                            json_object( key 'FilialId' value json_object (key 'ObjectId' value '0000'),
                                         key 'ClientId' value json_object (key 'ObjectId' value vClientID),
                                         key 'ClientLogin' value to_char(vPartyID),
                                         key 'BrokerContractList' value v_json_BrokerContractList format json,
                                         key 'DepoContractList' value v_json_DepoContract format json,
                                         key 'QualifiedInvestorInfo' value v_json_QualifiedInvestor format json absent on null
                                          returning clob) 
                            format json returning clob )
      INTO v_json_resp
      FROM dual;

    SELECT json_object (key 'GetBrokerageAgreementInfoResp' value 
                          json_object ( key 'GUID' value vMsgID,
                                        key 'GUIDReq' value vGUIDReq,
                                        key 'RequestTime' value vRequestDate,
                                        key 'GetBrokerContractInfo' value v_json_resp format json,
                                        key 'ErrorList' value 
                                          json_object (key 'Error' value
                                                        json_arrayagg( 
                                                          json_object( key 'ErrorCode' value to_char(o_MSGCode),
                                                                       key 'ErrorDesc' value o_MSGText
                                                                        returning clob )
                                                          returning clob
                                                          ) 
                                                        returning clob
                                                        )
                                          returning clob
                                        ) 
                          format json returning clob)
      INTO o_messbody
      FROM dual;
  END;

  function get_rate_on_date (
    p_from_fi dratedef_dbt.t_otherfi%type,
    p_to_fi   dratedef_dbt.t_fiid%type,
    p_date    dratedef_dbt.t_sincedate%type
  ) return number is
    l_error number;
    l_since_date date;
    l_rate number(32, 12);
    l_rate_type number(1) := 7;
  begin
    if p_from_fi = p_to_fi then
      return 1;
    end if;

    l_rate := rsb_sprepfun.GetRateOnDate(p_ADate        => p_date,
                                         p_FromFI       => p_from_fi,
                                         p_ToFI         => p_to_fi,
                                         p_NeedSayError => 0,
                                         p_RateType     => l_rate_type,
                                         p_Error        => l_error,
                                         p_SinceDate    => l_since_date);

    if l_rate = 0 then
      l_rate := 1 / rsb_sprepfun.GetRateOnDate(p_ADate        => p_date,
                                               p_FromFI       => p_to_fi,
                                               p_ToFI         => p_from_fi,
                                               p_NeedSayError => 0,
                                               p_RateType     => l_rate_type,
                                               p_Error        => l_error,
                                               p_SinceDate    => l_since_date);
    end if;
    
    return l_rate;
  exception
    when others then
      return null;
  end get_rate_on_date;

  function get_bill_info_json (
    p_bcid dvsbanner_dbt.t_bcid%type
  ) return clob is
    l_bill_info_json clob;
    l_guid           itt_q_message_log.msgid%type;
    l_note_storage_text dnotetext_dbt.t_text%type;
  begin
    l_note_storage_text := rpad(utl_raw.cast_to_raw(c => 'В хранилище Банка'), 3000, 0);
    l_guid := it_q_message.get_sys_guid;

    select json_object('SOFRSendBillReq' value
                       json_object('GUID' value l_guid,
                                   'RequestTime' value systimestamp,
                                   'BillInformation' value
                                   json_object('BillSeries'              value vsb.t_bcseries,
                                               'BillNumber'              value ltrim(vsb.t_bcnumber),
                                               'BillIssueDate'           value to_char(vsb.t_issuedate, 'yyyy-mm-dd'),
                                               'BillNominal'             value to_char(leg.t_principal),
                                               'BillNominalCurrencyCode' value fi.t_ccy,
                                               'InterestRate'            value to_char(leg.t_price/power(10, leg.t_point)),
                                               'BillPaymentDueDate'      value rsb_bill.payment_due_date(p_bctermformula => vsb.t_bctermformula,
                                                                                                         p_maturity      => leg.t_maturity,
                                                                                                         p_expiry        => leg.t_expiry),
                                               'BillRepaymentDate'       value to_char(rsb_bill_rshb.vs_min_repay_date(p_bcid => vsb.t_bcid), 'yyyy-mm-dd'),
                                               'BillSignStorage'         value case when l_note_storage_text = n.t_text then 'В хранилище Банка' end,
                                               'BillState'               value nullif(vsb.t_bcstate, chr(1)),
                                               'BillStatus'              value to_char(vsb.t_bcstatus),
                                               'BillId'                  value to_char(vsb.t_bcid)
                                               absent on null
                                               returning clob
                                               )
                                   returning clob)
                       returning clob)
      into l_bill_info_json
      from dvsbanner_dbt vsb
      join ddl_leg_dbt leg on leg.t_dealid = vsb.t_bcid
                          and leg.t_legkind = 1
                          and leg.t_legid = 0
      join dfininstr_dbt fi on fi.t_fiid = leg.t_pfi
      join dficert_dbt fc on fc.t_certid = vsb.t_bcid
      left join dnotetext_dbt n on n.t_objecttype = 24
                               and n.t_notekind = 23
                               and n.t_documentid = lpad(fc.t_ficertid, 10, '0')
      left join dvsordlnk_dbt lnk on lnk.t_bcid = vsb.t_bcid and lnk.t_dockind = 109
      left join ddl_order_dbt ord on lnk.T_CONTRACTID = ord.T_CONTRACTID
     where vsb.t_bcid = p_bcid
       and ((vsb.t_holder != -1 and party_read.is_legal_entity(p_party_id => vsb.t_holder) = 1)
          or (ord.T_CONTRACTID is not NULL and party_read.is_legal_entity(p_party_id => ord.T_CONTRACTOR) = 1));
    
    return l_bill_info_json;
  exception
    when no_data_found then
      return null;
  end get_bill_info_json;

  function get_bill_info_issue_json (
    p_bcid dvsbanner_dbt.t_bcid%type
  ) return clob is
    l_bill_info_json clob;
  begin
    select json_object('BillNominal'             value to_char(leg.t_principal),
                       'BillNominalCurrencyCode' value fi.t_ccy,
                       'BillPrice'               value to_char(lnk.t_bccost),
                       'PriceCurrencyCode'       value price_fi.t_ccy,
                       'InterestRate'            value to_char(leg.t_price/power(10, leg.t_point)),
                       'СonversionRate'          value it_ndbole.get_rate_on_date(p_from_fi => fi.t_fiid,
                                                                                  p_to_fi   => price_fi.t_fiid,
                                                                                  p_date    => sysdate),
                       'BillPaymentDueDate'      value rsb_bill.payment_due_date(p_bctermformula => vsb.t_bctermformula,
                                                                                 p_maturity      => leg.t_maturity,
                                                                                 p_expiry        => leg.t_expiry),
                       'AccountNumber'           value rsb_bill_rshb.VS_GetBannerAccount(p_bcid => vsb.t_bcid, p_date => sysdate),
                       'BillId'                  value to_char(vsb.t_bcid)
                       absent on null
                       returning clob
                       )
      into l_bill_info_json
      from dvsbanner_dbt vsb
      join ddl_leg_dbt leg on leg.t_dealid = vsb.t_bcid
                          and leg.t_legkind = 1
                          and leg.t_legid = 0
      join dfininstr_dbt fi on fi.t_fiid = leg.t_pfi
      join dvsordlnk_dbt lnk on lnk.t_bcid = vsb.t_bcid
      join dfininstr_dbt price_fi on price_fi.t_fiid = lnk.t_bccfi
     where vsb.t_bcid = p_bcid
       and lnk.t_dockind = 109
       and (vsb.t_holder = -1 or party_read.is_legal_entity(p_party_id => vsb.t_holder) = 1);

    return l_bill_info_json;
  exception
    when no_data_found then
      return null;
  end get_bill_info_issue_json;
  
  function get_bill_info_redempt_json (
    p_bcid dvsbanner_dbt.t_bcid%type
  ) return clob is
    l_bill_info_json clob;
  begin
    select json_object('BillSeries'              value vsb.t_bcseries,
                       'BillNumber'              value ltrim(vsb.t_bcnumber),
                       'BillNominal'             value to_char(leg.t_principal),
                       'BillNominalCurrencyCode' value fi.t_ccy,
                       'BillPrice'               value to_char(lnk.t_bccost),
                       'PriceCurrencyCode'       value price_fi.t_ccy,
                       'InterestRate'            value to_char(leg.t_price/power(10, leg.t_point)),
                       'СonversionRate'          value it_ndbole.get_rate_on_date(p_from_fi => fi.t_fiid,
                                                                                  p_to_fi   => price_fi.t_fiid,
                                                                                  p_date    => sysdate),
                       'BillPaymentDueDate'      value rsb_bill.payment_due_date(p_bctermformula => vsb.t_bctermformula,
                                                                                 p_maturity      => leg.t_maturity,
                                                                                 p_expiry        => leg.t_expiry),
                       'BillId'                  value to_char(vsb.t_bcid)
                       absent on null
                       returning clob
                       )
      into l_bill_info_json
      from dvsbanner_dbt vsb
      join ddl_leg_dbt leg on leg.t_dealid = vsb.t_bcid
                          and leg.t_legkind = 1
      join dfininstr_dbt fi on fi.t_fiid = leg.t_pfi
      join dvsordlnk_dbt lnk on lnk.t_bcid = vsb.t_bcid
      join dfininstr_dbt price_fi on price_fi.t_fiid = lnk.t_bccfi
     where 1 = 1
       and vsb.t_bcid = p_bcid
       and lnk.t_dockind = 110
       and (vsb.t_holder = -1 or party_read.is_legal_entity(p_party_id => vsb.t_holder) = 1);

    return l_bill_info_json;
  exception
    when no_data_found then
      return null;
  end get_bill_info_redempt_json;

  function get_connected_bills_json (
    p_contractid  ddl_order_dbt.t_contractid%type
  ) return clob is
    l_bill_array clob;
  begin
    select json_arrayagg(json_object('BillConnectedDeal' value it_ndbole.get_bill_info_issue_json(p_bcid => l.t_bcid) format json)
                         returning clob
                        )
      into l_bill_array
      from dvsordlnk_dbt l
     where l.t_contractid = p_contractid
       and l.t_dockind = 109;

    return l_bill_array;
  exception
    when no_data_found then
      return null;
  end get_connected_bills_json;

  function get_redeemed_bills_json (
    p_contractid  ddl_order_dbt.t_contractid%type
  ) return clob is
    l_bill_array clob;
  begin
    select json_arrayagg(json_object('BillConnectedRedemption' value it_ndbole.get_bill_info_redempt_json(p_bcid => l.t_bcid) format json)
                         returning clob
                        )
      into l_bill_array
      from dvsordlnk_dbt l
     where l.t_contractid = p_contractid
       and l.t_dockind = 110;

    return l_bill_array;
  exception
    when no_data_found then
      return null;
  end get_redeemed_bills_json;

  function get_bill_deal_detail_json (
    p_contractid  ddl_order_dbt.t_contractid%type,
    p_deal_status number default null
  ) return clob is
    l_deal_json clob;
  begin
    select json_object('AgreementNumber'   value o.t_ordernumber,
                       'AgreementSignDate' value to_char(o.t_signdate, 'yyyy-mm-dd'),
                       'AgreementStatus'   value to_char(nvl(p_deal_status, o.t_contractstatus)),
                       'IssuePlace'        value max(b.t_issueplace),
                       'BillPaymentPlace'  value max(b.t_issueplace),
                       'BranchCode'        value rpad(to_char(o.t_department), 4, '0'),
                       'GeneralAgreementNumber'      value ga.t_code,
                       'GeneralAgreementDate'        value to_char(max(ga.t_date_genagr), 'yyyy-mm-dd'),
                       'BillAgreementId'   value to_char(o.t_contractid),
                       'ClientId'          value json_object('ObjectId' value c.t_code),
                       'BillConnectedDealList'       value it_ndbole.get_connected_bills_json(p_contractid => p_contractid) format json,
                       'BillConnectedRedemptionList' value it_ndbole.get_redeemed_bills_json(p_contractid => p_contractid) format json
                       absent on null
                       returning clob
                       )
      into l_deal_json
      from ddl_order_dbt o
      join dvsordlnk_dbt lnk on lnk.t_contractid = lpad(o.t_contractid, 10, '0')
      join dvsbanner_dbt b on b.t_bcid = lnk.t_bcid
      left join ddl_genagr_dbt ga on ga.t_genagrid = o.t_gencondcontrid
      join dobjcode_dbt c on c.t_objectid = o.t_contractor
                         and c.t_objecttype = 3
                         and c.t_codekind = 101
                         and c.t_state = 0
     where o.t_contractid = p_contractid
     group by o.t_ordernumber,
              o.t_signdate,
              o.t_contractstatus,
              o.t_department,
              ga.t_code,
              o.t_contractid,
              c.t_code;

    return l_deal_json;
  end get_bill_deal_detail_json;
  
  function get_bill_deal_json (
    p_contractid  ddl_order_dbt.t_contractid%type,
    p_deal_status number default null
  ) return clob is
    l_deal_json clob;
    l_deal_detail clob;
    l_guid         itt_q_message_log.msgid%type;
  begin
    l_deal_detail := get_bill_deal_detail_json(p_contractid => p_contractid, p_deal_status => p_deal_status);
    l_guid := it_q_message.get_sys_guid;

    select json_object('SOFRSendBillDealReq' value
                       json_object('GUID'              value l_guid,
                                   'RequestTime'       value systimestamp,
                                   'BillIssuanceExchange' value case when o.t_dockind = 109 then l_deal_detail end format json,
                                   'BillRedemptionDetail' value case when o.t_dockind = 110 then l_deal_detail end format json,
                                   'BillHolderDetail'     value case when o.t_dockind = 110 then
                                                                        json_object('AccountNumber' value o.t_contractoraccount,
                                                                                    'BankBIC'       value o.t_contractorbankcode,
                                                                                    'BankName'      value o.t_contractorbankname,
                                                                                    'CorrespondentAccountNumber' value replace(o.t_contractorbankcorracc, chr(1)),
                                                                                    'DomicileCode'               value replace(o.t_correspbankcode, chr(1)),
                                                                                    'DomicileName'               value replace(o.t_correspbankname, chr(1))) end
                                   absent on null
                                   returning clob
                                   )
                       returning clob
                      )
      into l_deal_json
      from ddl_order_dbt o
     where o.t_contractid = p_contractid;
    
    l_deal_json := replace(l_deal_json, to_clob(':null'), to_clob(':""'));
    
    return l_deal_json;
  end get_bill_deal_json;

  /* p_deal_status - вручную прокидывается статус операции с векселем.
     Этот параметр нужен, потому что могут отправляться статусы, которые в софр не предусмотрены изначально. Например, "Оплачено"
     Если параметр null, то будет подставляться текущий статус сделки t_contractstatus.
     Какие могут быть статусы на данный момент:
     0 - отложенный
     10 - открытый
     20 - закрытый
     30 - оплачено
  */
  procedure send_bill_deal_json (
    p_contractid  ddl_order_dbt.t_contractid%type,
    p_deal_status number default null
  ) is
    l_message_body clob;
    l_error_code   integer := 0;
    l_error_descr  varchar2(4000);
    l_guid         itt_q_message_log.msgid%type;
    v_UseServiceDeal BOOLEAN := Rsb_Common.GetRegBoolValue('РСХБ\ИНТЕГРАЦИЯ\РУБИЛЬНИК-BOSS-5356.1', 0);
  begin
    IF v_UseServiceDeal THEN
        l_message_body := get_bill_deal_json(p_contractid  => p_contractid,
                                             p_deal_status => p_deal_status);

        select GUID
          into l_guid
          from json_table(l_message_body
                         ,'$.SOFRSendBillDealReq' columns GUID varchar2(128) path '$.GUID');

        it_kafka.load_msg(io_msgid       => l_guid
                         ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                         ,p_ServiceName  => 'ndbole.SOFRSendBillDeal'
                         ,p_Receiver     => C_C_SYSTEM_NAME
                         ,p_MESSBODY     => l_message_body
                         ,o_ErrorCode    => l_error_code
                         ,o_ErrorDesc    => l_error_descr);

        if l_error_descr is not null then
          raise_application_error(-20000, l_error_descr);
        end if;
    END IF;
  exception
    when others then
      it_event.RegisterError(p_SystemId    => C_C_SYSTEM_NAME
                            ,p_ServiceName => 'SOFRSendBillDeal'
                            ,p_ErrorCode   => l_error_code
                            ,p_ErrorDesc   => 'Сервис SOFRSendBillDeal: ' || dbms_utility.format_error_stack || ' ' || dbms_utility.format_error_backtrace
                            ,p_LevelInfo   => 7);
  end send_bill_deal_json;

  procedure send_bill_json (
    p_bcid  dvsbanner_dbt.t_bcid%type
  ) is
    l_message_body clob;
    l_error_code   integer := 0;
    l_error_descr  varchar2(4000);
    l_guid         itt_q_message_log.msgid%type;
    v_UseServiceBill BOOLEAN := Rsb_Common.GetRegBoolValue('РСХБ\ИНТЕГРАЦИЯ\РУБИЛЬНИК-BOSS-5356.2', 0);
  begin
    if v_UseServiceBill THEN
        l_message_body := get_bill_info_json(p_bcid => p_bcid);

        select GUID
          into l_guid
          from json_table(l_message_body
                         ,'$.SOFRSendBillReq' columns GUID varchar2(128) path '$.GUID');

        it_kafka.load_msg(io_msgid       => l_guid
                         ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                         ,p_ServiceName  => 'ndbole.SOFRSendBill'
                         ,p_Receiver     => C_C_SYSTEM_NAME
                         ,p_MESSBODY     => l_message_body
                         ,o_ErrorCode    => l_error_code
                         ,o_ErrorDesc    => l_error_descr);

        if l_error_descr is not null then
          raise_application_error(-20000, l_error_descr);
        end if;
    END IF;
  exception
    when others then
      it_event.RegisterError(p_SystemId    => C_C_SYSTEM_NAME
                            ,p_ServiceName => 'SOFRSendBill'
                            ,p_ErrorCode   => l_error_code
                            ,p_ErrorDesc   => 'Сервис SOFRSendBill: ' || dbms_utility.format_error_stack || ' ' || dbms_utility.format_error_backtrace
                            ,p_LevelInfo   => 7);

  end send_bill_json;


/**  
  * Занесение ошибки в буферную таблицу
  * @since RSHB 115
  * @qtest NO
  */
  function FixErrorRunReportLE( perror_text in varchar2, 
                                pid in duserlebrokrepreq_dbt.T_ID%type )
  return varchar2 is 
    pragma autonomous_transaction;
  begin
    update duserlebrokrepreq_dbt
       set t_ErrorText = perror_text,
           t_status = 'N'
     where t_id = pid;
    commit; 
    return 'Сервис GetBrokerageReportInfoReq: '||perror_text;
  end;
  
/**  
  * Вставка в буферную таблицу
  * @since RSHB 115
  * @qtest NO
  */
  function InsertReportLE( p_buf_rec in duserlebrokrepreq_dbt%rowtype )
  return varchar2 is 
    res_ID duserlebrokrepreq_dbt.T_ID%type;
    pragma autonomous_transaction;
  begin
    insert into duserlebrokrepreq_dbt
    values p_buf_rec
    returning T_ID into res_ID;
    commit;
    return res_ID;
  end;
  
/**  
  * Обновление буферной таблицы
  * @since RSHB 115
  * @qtest NO
  */
  procedure UpdateReportLE( p_buf_rec in duserlebrokrepreq_dbt%rowtype )
  is 
    err_txt varchar2(2000);
    pragma autonomous_transaction;  
  begin
    update duserlebrokrepreq_dbt
       set T_GUIDREQ    = p_buf_rec.T_GUIDREQ,
           T_STARTDATE  = p_buf_rec.T_STARTDATE,
           T_ENDDATE    = p_buf_rec.T_ENDDATE,
           T_REQREGDATE = p_buf_rec.T_REQREGDATE,
           T_CLIENTCODE = p_buf_rec.T_CLIENTCODE,
           T_CONTRNUMBER= p_buf_rec.T_CONTRNUMBER,
           T_PARTYID    = p_buf_rec.T_PARTYID,
           T_DLCONTRID  = p_buf_rec.T_DLCONTRID,
           T_JSONSYSDATE= p_buf_rec.T_JSONSYSDATE,
           T_REPNAME    = p_buf_rec.T_REPNAME,
           T_FACSIMILE  = p_buf_rec.T_FACSIMILE,
           T_ERRORTEXT  = p_buf_rec.T_ERRORTEXT,
           T_STATUS     = p_buf_rec.T_STATUS,
           T_READYTIME  = p_buf_rec.T_READYTIME
     where T_ID = p_buf_rec.T_ID;
    commit;
  exception
    when others then 
      rollback;
      err_txt := FixErrorRunReportLE( substr(sqlerrm,1,300), 
                                      p_buf_rec.T_ID );
      raise; 
  end;

/**  
  * Отправка ответа GetLegalEntityReportResp 
  * @since RSHB 115
  * @qtest NO
  */
  procedure SendGetReportResp(p_buf_rec in duserlebrokrepreq_dbt%rowtype, 
                              p_MsgID in itt_q_message_log.msgid%type, 
                              p_header in CLOB default null,
                              p_IsSuccess in varchar2, 
                              p_ErrorCode in number, 
                              p_ErrorDesc in varchar2)
  is 
    v_json_resp   CLOB;
    v_MessMETA    XMLType;
    v_MessMETA_XLSX XMLType;
    v_MsgID       itt_q_message_log.msgid%type;
    vMsgID_XLSX   itt_q_message_log.msgid%type;
    traceid_XLSX   itt_q_message_log.msgid%type;
    v_MSGCode     itt_q_message_log.msgcode%type;
    v_MSGText     itt_q_message_log.msgtext%type;
    
    v_json_req  JSON_OBJECT_T;
    v_bucket_name   varchar2(200);
    v_document_path varchar2(200);
    v_addparams it_ips_dfactory.tt_addparams;
    v_MSGCode_xlsx INTEGER;
    v_MSGText_xlsx VARCHAR2(4000);
    v_MSGCode_pdf  INTEGER;
    v_MSGText_pdf  VARCHAR2(4000);
    
    
    pragma autonomous_transaction;
  begin
  
    v_MSGCode_xlsx  := 0;
    v_MSGText_xlsx  := 'Успешно';
    v_MSGCode_pdf   := 0;
    v_MSGText_pdf   := 'Успешно';

    if p_header is not null then 
      v_json_req := JSON_OBJECT_T(p_header);
      v_bucket_name:= v_json_req.get_string('x-bucket-name');
      v_document_path := v_json_req.get_string('x-document-path');
    else 
      v_bucket_name := '';
      v_document_path := '';
    end if;
  
    SELECT  JSON_OBJECT('GetLegalEntityReportResp' IS
              JSON_OBJECT('GUID' IS p_MsgID
                         ,'GUIDReq' IS p_buf_rec.t_guidreq
                         ,'RequestTime' IS p_buf_rec.T_REQREGDATE
                         ,'IsSuccess' IS p_IsSuccess format json
                         ,'ErrorList' IS 
                              JSON_OBJECT ('Error' IS
                                            json_arrayagg( 
                                              json_object('ErrorCode' IS p_ErrorCode,
                                                          'ErrorDesc' IS p_ErrorDesc
                                                            returning clob )
                                              returning clob
                                              ) 
                                            returning clob
                                            )   
              RETURNING CLOB)
            RETURNING CLOB)
      INTO v_json_resp
      FROM dual;
      
    v_addparams := null;
    v_addparams := it_ips_dfactory.add_addparams(p_addparams => v_addparams
                                                ,p_paramName      => 'x-client-id'
                                                ,p_paramValue     => p_buf_rec.t_ClientCode);
    v_addparams := it_ips_dfactory.add_addparams(p_addparams => v_addparams
                                                ,p_paramName      => 'x-bucket-name'
                                                ,p_paramValue     => v_bucket_name);
    v_addparams := it_ips_dfactory.add_addparams(p_addparams => v_addparams
                                                ,p_paramName      => 'x-document-path'
                                                ,p_paramValue     => v_document_path);

 IF lower(substr(v_document_path, instr(v_document_path, '.', -1) + 1)) = 'xlsx' THEN 
    v_MessMETA := it_ips_dfactory.add_KafkaHeader_Xmessmeta( p_List_dllvalues_dbt => 4199
                                                            ,p_traceid => p_buf_rec.T_GUIDREQ 
                                                            ,p_requestid => p_MsgID
                                                            ,p_templateparams => null
                                                            ,p_addparams => v_addparams
                                                            ,p_outputfilename => p_buf_rec.t_repname);
ELSE 
    v_MessMETA := it_ips_dfactory.add_KafkaHeader_Xmessmeta( p_List_dllvalues_dbt => 4192
                                                            ,p_traceid => p_buf_rec.T_GUIDREQ 
                                                            ,p_requestid => p_MsgID
                                                            ,p_templateparams => null
                                                            ,p_addparams => v_addparams
                                                            ,p_outputfilename => p_buf_rec.t_repname);
END IF;     
    v_MsgID := p_MsgID;
    
    it_kafka.load_msg(io_msgid => v_MsgID
                     ,p_message_type => it_q_message.C_C_MSG_TYPE_A
                     ,p_ServiceName => 'NDBOLE.GetReportLEFromNDBOLE' -- Имя сервиса 
                     ,p_Receiver => it_ndbole.C_C_SYSTEM_NAME
                     ,p_MESSBODY => v_json_resp  -- JSON c данными
                     ,p_MessMETA => v_MessMETA
                     ,p_CORRmsgid => p_buf_rec.t_guidreq
                     ,p_MSGCode => p_ErrorCode
                     ,p_MSGText => p_ErrorDesc
                     ,o_ErrorCode => v_MSGCode_pdf
                     ,o_ErrorDesc => v_MSGText_pdf); 


    if RSB_Common.GetRegFlagValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ИНТЕГРАЦИЯ КАФКА\ВЫГРУЗКА ОТЧЕТА БРОКЕРА В XLSX') = 'X' then
       if NVL(v_MSGCode_xlsx,0) <> 0 then
          v_MSGCode := v_MSGCode_xlsx;
          v_MSGText := v_MSGText_xlsx;
       elsif NVL(v_MSGCode_pdf,0) <> 0 then
          v_MSGCode := v_MSGCode_pdf;
          v_MSGText := v_MSGText_pdf;
       else
          v_MSGCode := 0;
          v_MSGText := 'Успешно';
       end if;
    else
       v_MSGCode := NVL(v_MSGCode_pdf,0);
       v_MSGText := v_MSGText_pdf;
       if v_MSGCode = 0 then
          v_MSGText := 'Успешно';
       end if;
    end if;

    if v_MSGCode <> 0 then 
        it_log.log('SendGetReportResp. Ошибка: '||v_MSGText,it_log.C_MSG_TYPE__ERROR,v_json_resp);
    end if; 
    
    commit;
    
  exception
    when others then 
        rollback;
        it_log.log(p_msg => 'GetLegalEntityReportResp. Непредвиденная ошибка '||SQLERRM);
  end;

/**  
  * Cообщение с запросом Отчета Брокера
  * @since RSHB 115
  * @qtest NO
  */
  procedure GetReportLEFromNDBOLE(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype) is 
                                     
    vMsgID      itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
    vMsgID_resp itt_q_message_log.msgid%type := lower(it_q_message.get_sys_guid);
    vMsgID_XLSX itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
    
    v_buf_rec     duserlebrokrepreq_dbt%rowtype;
    vContract_rec Contract_rec;
    v_SignerParty dparty_dbt.t_PartyID%TYPE;
    v_RequestTime varchar2(200);
    v_MessMETA    XMLType;
    v_MessMETA_XLSX    XMLType;
    v_templateparams it_ips_dfactory.tt_templateparams;
    v_MSGCode_xlsx INTEGER;
    v_MSGText_xlsx VARCHAR2(4000);
    v_MSGCode_pdf  INTEGER;
    v_MSGText_pdf  VARCHAR2(4000);
    vTurnOn char(1);
    
    v_json_stock  clob;
    v_json_otc  clob;
    v_json_resp clob;
    
  begin

    o_MSGCode := 0;
    o_MSGText := 'Успешно';
    o_msgid   := vMsgID;

    v_MSGCode_xlsx  := 0;
    v_MSGText_xlsx  := 'Успешно';
    v_MSGCode_pdf   := 0;
    v_MSGText_pdf   := 'Успешно';
    
    vTurnOn :=  nvl(RSB_Common.GetRegFlagValue(C_NAME_REGVAL_NDBOLE_BROKERREPORT_ONOFF),'0');
    if vTurnOn <> 'X' then
      o_MSGCode := -1;
      o_MSGText := 'Настройка '||C_NAME_REGVAL_NDBOLE_BROKERREPORT_ONOFF||' отключена';
      return;
    end if;
    
    if p_messbody is null then
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText:= 'Не передан входящий параметр p_messbody';
      return;
    end if;

    v_buf_rec.T_ID           := 0;
    v_buf_rec.T_QMESSLOGID   := p_worklogid;
    v_buf_rec.T_REQSYSDATE   := sysdate;

    v_buf_rec.T_ID := InsertReportLE( v_buf_rec );

    BEGIN
      select GUID, 
             RequestTime, 
             BrokerageContractNumber, 
             to_date(ReportStartDate, 'dd.mm.yyyy'), 
             to_date(ReportEndDate, 'dd.mm.yyyy'),
             ClientCode
        into  v_buf_rec.T_GUIDREQ, 
              v_RequestTime, 
              v_buf_rec.T_CONTRNUMBER, 
              v_buf_rec.T_STARTDATE, 
              v_buf_rec.T_ENDDATE, 
              v_buf_rec.T_CLIENTCODE
        from json_table (p_messbody, '$.GetLegalEntityReportReq' columns 
                                        ( GUID                    varchar2(2000) path '$.GUID',
                                          RequestTime             varchar2(100) path '$.RequestTime',
                                          BrokerageContractNumber varchar2(100) path '$.BrokerageContractNumber',
                                          ReportStartDate         varchar2(100) path '$.ReportStartDate',
                                          ReportEndDate           varchar2(100) path '$.ReportEndDate',
                                          ClientCode              varchar2(100) path '$.ClientCode.ObjectId'
                                           )
                          );
    EXCEPTION 
      WHEN OTHERS THEN
        o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
        o_MSGText := SUBSTR('Непредвиденная ошибка при разборе входящего JSON-сообщения: '||sqlerrm, 1, 300);
        o_MSGText := FixErrorRunReportLE( o_MSGText, v_buf_rec.T_ID );
        return;
    END;
    
    v_buf_rec.T_REQREGDATE := CAST(it_xml.char_to_timestamp_iso8601(v_RequestTime) AS DATE);
    
    UpdateReportLE( v_buf_rec );
    
    if v_buf_rec.T_CLIENTCODE is null then
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText:= 'Не удалось разобрать xml по причине: не удалось получить тег GetLegalEntityReportReq.ClientCode.ObjectId';
      o_MSGText := FixErrorRunReportLE( o_MSGText, v_buf_rec.T_ID );
      return;
    end if;
    
    v_buf_rec.T_PARTYID := GetPartyIDByCFT(v_buf_rec.T_CLIENTCODE);
    if v_buf_rec.T_PARTYID = -1 then 
      o_MSGCode := ERROR_CLIENT_NOTFOUND;
      o_MSGText:= 'Не найден клиент в СОФР по коду ClientCode = '||v_buf_rec.T_CLIENTCODE;
      o_MSGText := FixErrorRunReportLE( o_MSGText, v_buf_rec.T_ID );
      
      SendGetReportResp(p_buf_rec => v_buf_rec, 
                        p_MsgID => vMsgID_resp, 
                        p_IsSuccess => 'false', 
                        p_ErrorCode => 2, 
                        p_ErrorDesc => 'СОФР не смог сформировать отчет: клиент не найден');
      return;
    end if;
    
    UpdateReportLE( v_buf_rec );

    vContract_rec := GetDlContrIDByContractNumber(v_buf_rec.T_CONTRNUMBER, v_buf_rec.T_STARTDATE, v_buf_rec.T_ENDDATE);
    if vContract_rec.DlContrID <= 0 then
      o_MSGCode := ERROR_CONTRACT_NOTFOUND;
      o_MSGText:= 'Не удалось определить действующий договор по клиенту в СОФР с кодом клиента ClientCode = '||v_buf_rec.T_CLIENTCODE||' и номером договора BrokerageContractNumber= '||v_buf_rec.T_CONTRNUMBER;
      o_MSGText := FixErrorRunReportLE( o_MSGText, v_buf_rec.T_ID );
      
      SendGetReportResp(p_buf_rec => v_buf_rec, 
                        p_MsgID => vMsgID_resp, 
                        p_IsSuccess => 'false', 
                        p_ErrorCode => 3, 
                        p_ErrorDesc => 'СОФР не смог сформировать отчет: договор не найден');
      return;
    else 
       v_buf_rec.T_DLCONTRID := vContract_rec.DlContrID;
    end if;
    
    if vContract_rec.PartyID <> v_buf_rec.T_PARTYID then
      o_MSGCode := ERROR_CLIENT_NOTMATCH;
      o_MSGText:= 'Клиент по договору '||v_buf_rec.T_CONTRNUMBER||' не совпадает с переданным. Передан ID='||v_buf_rec.T_PARTYID||', клиент по договору ID='||vContract_rec.PartyID;
      o_MSGText := FixErrorRunReportLE( o_MSGText, v_buf_rec.T_ID );
      
      SendGetReportResp(p_buf_rec => v_buf_rec, 
                        p_MsgID => vMsgID_resp, 
                        p_IsSuccess => 'false', 
                        p_ErrorCode => 99, 
                        p_ErrorDesc => 'СОФР не смог сформировать отчет:  другие ошибки');
      return;
    end if;
    
    UpdateReportLE( v_buf_rec );
     
    v_SignerParty := IT_BrokerReport.GetDefaultSigner(v_buf_rec.t_ErrorText);
    IF v_SignerParty = -1 THEN
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText := v_buf_rec.t_ErrorText;
      o_MSGText := FixErrorRunReportLE( o_MSGText, v_buf_rec.T_ID );
      
      SendGetReportResp(p_buf_rec => v_buf_rec, 
                        p_MsgID => vMsgID_resp, 
                        p_IsSuccess => 'false', 
                        p_ErrorCode => 99, 
                        p_ErrorDesc => 'СОФР не смог сформировать отчет:  другие ошибки');
      return;
    ELSE
      v_buf_rec.T_FACSIMILE := pm_common.GetNoteTextStr(v_SignerParty, Rsb_Secur.OBJTYPE_PARTY, 109, TRUNC(SYSDATE));
      IF v_buf_rec.t_Facsimile IS NULL OR v_buf_rec.t_Facsimile = CHR(0) OR v_buf_rec.t_Facsimile = CHR(1) THEN
        o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
        o_MSGText := 'Не удалось получить почтовый логин сотрудника (ID субъекта = '||v_SignerParty||') из соответствующего примечания';
        o_MSGText := FixErrorRunReportLE( o_MSGText, v_buf_rec.T_ID );
        
        SendGetReportResp(p_buf_rec => v_buf_rec, 
                          p_MsgID => vMsgID_resp, 
                          p_IsSuccess => 'false', 
                          p_ErrorCode => 99, 
                          p_ErrorDesc => 'СОФР не смог сформировать отчет:  другие ошибки');
        return;
      ELSE
        v_buf_rec.t_Facsimile := SUBSTR(v_buf_rec.t_Facsimile, 1, INSTR(v_buf_rec.t_Facsimile, '@') - 1); 
        UpdateReportLE( v_buf_rec );
      END IF;
    END IF;
    
    RsbSessionData.SetOurBank(1);
    
    --Подготовка отчетных данных
    BEGIN
      IT_BrokerReport.CreateAllData(p_DlContrID => v_buf_rec.t_DlContrID, 
                                    p_BegDate => v_buf_rec.t_StartDate, 
                                    p_EndDate => v_buf_rec.t_EndDate, 
                                    p_ByExchange => 1, 
                                    p_ByOutExchange => 1, 
                                    p_IsEDP => 0); 
    EXCEPTION
       WHEN OTHERS THEN
        o_MSGCode := ERROR_ERROR_RUN_REPORT;
        o_MSGText := SUBSTR('Непредвиденная ошибка при подготовке отчетных данных: '||sqlerrm, 1, 300);
        o_MSGText := FixErrorRunReportLE( o_MSGText, v_buf_rec.T_ID );

        SendGetReportResp(p_buf_rec => v_buf_rec, 
                          p_MsgID => vMsgID_resp, 
                          p_IsSuccess => 'false', 
                          p_ErrorCode => 99, 
                          p_ErrorDesc => 'СОФР не смог сформировать отчет:  другие ошибки');
        return;
    END;

    v_buf_rec.t_ErrorText := IT_BrokerReport.BrokerReportRun(p_buf_rec => v_buf_rec, 
                                                             p_isOTC => 0,
                                                             o_json_resp => v_json_stock);
    
    if v_buf_rec.t_ErrorText <> 'OK' then
      o_MSGCode := ERROR_ERROR_RUN_REPORT;
      o_MSGText := SUBSTR('Непредвиденная ошибка при формировании исходящего JSON-сообщения по бирже: '||v_buf_rec.t_ErrorText , 1, 300);
      o_MSGText := FixErrorRunReportLE( o_MSGText, v_buf_rec.T_ID );
      
      SendGetReportResp(p_buf_rec => v_buf_rec, 
                        p_MsgID => vMsgID_resp, 
                        p_IsSuccess => 'false', 
                        p_ErrorCode => 99, 
                        p_ErrorDesc => 'СОФР не смог сформировать отчет:  другие ошибки');
      return;
    end if;
    
    v_buf_rec.t_ErrorText := IT_BrokerReport.BrokerReportRun(p_buf_rec => v_buf_rec, 
                                                             p_isOTC => 1,
                                                             o_json_resp => v_json_otc);
    
    if v_buf_rec.t_ErrorText <> 'OK' then
      o_MSGCode := ERROR_ERROR_RUN_REPORT;
      o_MSGText := SUBSTR('Непредвиденная ошибка при формировании исходящего JSON-сообщения по внебирже: '||v_buf_rec.t_ErrorText , 1, 300);
      o_MSGText := FixErrorRunReportLE( o_MSGText, v_buf_rec.T_ID );
      
      SendGetReportResp(p_buf_rec => v_buf_rec, 
                        p_MsgID => vMsgID_resp, 
                        p_IsSuccess => 'false', 
                        p_ErrorCode => 99, 
                        p_ErrorDesc => 'СОФР не смог сформировать отчет:  другие ошибки');
      return;
    end if;
    
    SELECT  /*JSON_OBJECT('GenerateLegalEntityReportReq' IS*/
              JSON_OBJECT('BrokerageContractNumber' IS '№'||v_buf_rec.t_ContrNumber||' от '||TO_CHAR(vContract_rec.ContractDate, 'DD.MM.YYYY')
                                ,'ExecutorName' IS RSB_BRKREP_RSHB_NEW.GetSignerName(v_SignerParty)
                                ,'ClientName' IS IT_BrokerReport.GetClientName(v_buf_rec.t_PartyID)
                                ,'BrokerageReportName' IS 'Отчет брокера за период с '||TO_CHAR(v_buf_rec.t_StartDate, 'DD.MM.YYYY')||' по '||TO_CHAR(v_buf_rec.t_EndDate, 'DD.MM.YYYY')||', дата формирования '||TO_CHAR(SYSDATE, 'DD.MM.YYYY')
                                ,'GUID' value o_msgid
                                ,'RequestTime' value v_RequestTime
                                ,'GUIDReq' value v_buf_rec.t_GUIDReq
                                ,'ClientCode' IS GetEKK(v_buf_rec.T_DLCONTRID)
                                ,'StockPortfolio' IS v_json_stock FORMAT JSON
                                ,'OTCPortfolio' IS v_json_otc FORMAT JSON
                                ABSENT ON NULL
                                RETURNING CLOB)
            /*RETURNING CLOB)*/
      INTO v_json_resp
      FROM dual;

    v_buf_rec.t_repname := replace(v_buf_rec.t_ContrNumber,'/','-')||'_'||
                           to_char(v_buf_rec.t_StartDate, 'DDMMYYYY')||'_'||
                           to_char(v_buf_rec.t_EndDate, 'DDMMYYYY');
    
    v_buf_rec.t_jsonsysdate := sysdate;
    v_buf_rec.t_ErrorText := chr(1); /* OK */
    
    UpdateReportLE( v_buf_rec );

    v_templateparams := null;
    v_templateparams := it_ips_dfactory.add_templateparams(p_templateparams => v_templateparams
                                                          ,p_paramName      => 'df_facsimile'
                                                          ,p_paramType      => 'FILE'
                                                          ,p_paramValue     => v_buf_rec.t_Facsimile||'.png');
    v_templateparams := it_ips_dfactory.add_templateparams(p_templateparams => v_templateparams
                                                          ,p_paramName      => 'df_facsimile1'
                                                          ,p_paramType      => 'FILE'
                                                          ,p_paramValue     => v_buf_rec.t_Facsimile||'.png');


    v_MessMETA := it_ips_dfactory.add_KafkaHeader_Xmessmeta( p_List_dllvalues_dbt => 4193
                                                            ,p_traceid => v_buf_rec.T_GUIDREQ 
                                                            ,p_requestid => vMsgID
                                                            ,p_templateparams => v_templateparams
                                                            ,p_outputfilename => v_buf_rec.t_repname);
    
    it_kafka.load_msg_S3(p_msgid => vMsgID
                         ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                         ,p_ServiceName => 'IPS_DFACTORY.GenerateLegalEntityReport' -- Имя сервиса 
                         ,p_Receiver => it_ips_dfactory.C_C_SYSTEM_NAME
                         ,p_CORRmsgid => v_buf_rec.T_GUIDREQ
                         ,p_MESSBODY => v_json_resp  -- JSON c данными   
                         ,p_MessMETA => v_MessMETA
                         ,p_isquery => 1
                         ,o_ErrorCode => v_MSGCode_pdf
                         ,o_ErrorDesc => v_MSGText_pdf);

  IF RSB_Common.GetRegFlagValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ИНТЕГРАЦИЯ КАФКА\ВЫГРУЗКА ОТЧЕТА БРОКЕРА В XLSX') = 'X' THEN                      
    -- Выгрузка XLSX
    v_MessMETA_XLSX := it_ips_dfactory.add_KafkaHeader_Xmessmeta( p_List_dllvalues_dbt => 4195
                                                            ,p_traceid => v_buf_rec.T_GUIDREQ 
                                                            ,p_requestid => vMsgID_XLSX
                                                            ,p_templateparams => v_templateparams
                                                            ,p_outputfilename => v_buf_rec.t_repname);
    it_kafka.load_msg_S3(
      p_msgid         => vMsgID_XLSX,
      p_message_type  => it_q_message.C_C_MSG_TYPE_R,
      p_ServiceName   => 'IPS_DFACTORY.GenerateLegalEntityReport',
      p_Receiver      => it_ips_dfactory.C_C_SYSTEM_NAME,
      p_CORRmsgid     => v_buf_rec.T_GUIDREQ,
      p_MESSBODY      => v_json_resp,        
      p_MessMETA      => v_MessMETA_XLSX,
      p_isquery       => 1,
      o_ErrorCode     => v_MSGCode_xlsx,
      o_ErrorDesc     => v_MSGText_xlsx);                   
    END IF;                      
 
    IF RSB_Common.GetRegFlagValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ИНТЕГРАЦИЯ КАФКА\ВЫГРУЗКА ОТЧЕТА БРОКЕРА В XLSX') = 'X' THEN
        IF NVL(v_MSGCode_xlsx,0) <> 0 THEN
          o_MSGCode := v_MSGCode_xlsx;
          o_MSGText := v_MSGText_xlsx;
        ELSIF NVL(v_MSGCode_pdf,0) <> 0 THEN
          o_MSGCode := v_MSGCode_pdf;
          o_MSGText := v_MSGText_pdf;
        ELSE
          o_MSGCode := 0;
          o_MSGText := 'Успешно';
        END IF;
      ELSE
        o_MSGCode := NVL(v_MSGCode_pdf,0);
        o_MSGText := v_MSGText_pdf;
        IF o_MSGCode = 0 THEN
          o_MSGText := 'Успешно';
        END IF;
    END IF;                 
                 
    if o_MSGCode <> 0 then 
      v_buf_rec.t_ErrorText := o_MSGText;
      v_buf_rec.t_status := 'N';

      UpdateReportLE( v_buf_rec );
    end if;
  END;

/**  
  * Cтатус формирования отчета фабрикой документов
  * @since RSHB 115
  * @qtest NO
  */
  procedure SendFileNotification (p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype) is 
                                     
    vMsgID      itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
    
    vTurnOn     char(1);
    
    x_header    CLOB;
    v_json_req  JSON_OBJECT_T;
    v_trace_id  varchar2(200);
    v_isSuccess boolean;
    v_json_arr  JSON_ARRAY_T;
    v_error_obj JSON_OBJECT_T;
    
    v_buf_rec     duserlebrokrepreq_dbt%rowtype;
    v_error_text  duserlebrokrepreq_dbt.t_errortext%type;
    
    v_t         varchar2(5);
    v_ErrorCode varchar2(20);
    v_ErrorDesc varchar2(300);
    
  begin

    o_MSGCode := 0;
    o_MSGText := 'Успешно';
    o_msgid   := vMsgID;
    
    vTurnOn :=  nvl(RSB_Common.GetRegFlagValue(C_NAME_REGVAL_NDBOLE_BROKERREPORT_ONOFF),'0');
    if vTurnOn <> 'X' then
      o_MSGCode := -1;
      o_MSGText := 'Настройка '||C_NAME_REGVAL_NDBOLE_BROKERREPORT_ONOFF||' отключена';
      return;
    end if;
    
    if p_messbody is null then
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText:= 'Не передан входящий параметр p_messbody';
      return;
    end if;
    
    if p_messmeta is null then
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText:= 'Не передан входящий параметр p_messmeta';
      return;
    end if;
        
    SELECT extractValue(p_messmeta, 'KAFKA/Header')
      into x_header
    from dual;

    v_json_req := JSON_OBJECT_T(x_header);
    v_trace_id:= v_json_req.get_string('x-trace-id');
    
    if v_trace_id is null then
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText:= 'Не найден параметр x-trace-id в p_messmeta';
      return;
    end if;
    
    BEGIN
      select * 
        into v_buf_rec
        from duserlebrokrepreq_dbt
       where t_guidreq = v_trace_id;
    EXCEPTION
      when no_data_found then 
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText:= 'Не найдена запись с GUID = '||v_trace_id;
      return;
    END;

    BEGIN
      v_json_req := JSON_OBJECT_T(p_messbody);
      v_isSuccess := v_json_req.get_Boolean('isSuccess');
      v_json_arr := v_json_req.get_Array('errors');
    EXCEPTION 
      WHEN OTHERS THEN
        o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
        o_MSGText := SUBSTR('Непредвиденная ошибка при разборе входящего JSON-сообщения: '||sqlerrm, 1, 300);
        return;
    END;
    
    if v_isSuccess then 
      update duserlebrokrepreq_dbt
         set t_status = 'Y', 
             t_readytime = systimestamp
       where t_id = v_buf_rec.t_id;
       v_t := 'true';
       v_ErrorCode := 0;
       v_ErrorDesc := 'OK';
    else 
      v_error_text := '';
      FOR j IN 0 .. v_json_arr.get_size - 1 LOOP
        IF v_json_arr.get(j).is_object THEN
          v_error_obj := TREAT(v_json_arr.get(j) AS JSON_OBJECT_T);
          v_error_text := substr(v_error_text
                                 ||' '||v_error_obj.get_string('errorCode')
                                 ||'-'||v_error_obj.get_string('errorMessage'),1,300);
        END IF;
        v_t := 'false';
        v_ErrorCode := 1;
        v_ErrorDesc := 'ips не смог сформировать отчет';
      END LOOP;
      
      update duserlebrokrepreq_dbt
         set t_ErrorText = v_error_text,
             t_status = 'N', 
             t_readytime = systimestamp
       where t_id = v_buf_rec.t_id;
    end if;
    
    SendGetReportResp(p_buf_rec => v_buf_rec, 
                      p_MsgID => vMsgID, 
                      p_header => x_header,
                      p_IsSuccess => v_t, 
                      p_ErrorCode => v_ErrorCode, 
                      p_ErrorDesc => v_ErrorDesc);

  end;
  
  /**  
  * BOSS-5842 Установить статус обработки запроса
  * @since RSHB 120
  * @qtest NO
  */
  procedure BrokerageAgreementTermsReqSetState(p_requestID integer
                                              ,p_state     integer
                                              ,p_errorCode integer
                                              ,p_errorText varchar2) is
    pragma autonomous_transaction;
  begin
    update dUserLEAgrDataReq_dbt
       set t_state = p_state, t_errorCode = p_errorCode, t_errortext = SubStr(p_errorText, 1, 300)
     where t_ID = p_requestID;
    
    if p_errorCode != 0 then
      it_log.log_error('SendBrokerageAgreementTerms (id '||to_char(p_requestID)||')', to_char(p_errorCode)||': '||p_errorText);
    end if;
    
    commit;
  end;
  
  /**  
  * BOSS-5842 Сохранить запрос в буферную таблицу
  * @since RSHB 120
  * @qtest NO
  */
  procedure BrokerateAgreementTermsReqSave(p_reqGUID varchar2
                                          ,p_requestID varchar2
                                          ,p_processID varchar2
                                          ,p_reqDate date
                                          ,p_reqJson clob
                                          ,p_clientCFTID varchar2
                                          ,p_state integer
                                          ,p_partyID integer
                                          ,p_dlContrID integer
                                          ,p_errorCode integer
                                          ,p_errorText varchar2) is
    pragma autonomous_transaction;
  begin
    insert into dUserLEAgrDataReq_dbt
                (t_reqGUID, t_requestID, t_processID,
                 t_reqDate, t_reqJson, 
                 t_clientCFTID, t_state,
                 t_partyID, t_dlContrID,
                 t_errorCode, t_errorText)
         values (p_reqGUID, p_requestID, p_processID,
                 p_reqDate, p_reqJson, 
                 p_clientCFTID, p_state,
                 p_partyID, p_dlContrID,
                 p_errorCode, p_errorText);

    commit;
  end;
  
  /**  
  * BOSS-5842 Запрос из ДБО "Свой бизнес" на открытие/изменение ДБО СОФР
  * @since RSHB 120
  * @qtest NO
  */
  procedure SendBrokerageAgreementTerms( p_worklogid integer
                                      ,p_messbody  clob
                                      ,p_messmeta  xmltype
                                      ,o_msgid     out varchar2
                                      ,o_MSGCode   out integer
                                      ,o_MSGText   out varchar2
                                      ,o_messbody  out clob
                                      ,o_messmeta  out xmltype) is
    v_enable      varchar2(1);
    v_msgID       itt_q_message_log.msgid%type;
    v_json_req    JSON_OBJECT_T;
    v_header      CLOB;
    v_requestID   varchar2(100);
    v_processID   varchar2(100);
    v_requestTime date;
    v_clientCFTID varchar2(20) := chr(1);
  begin
    v_enable :=  nvl(RSB_Common.GetRegFlagValue(C_NAME_REGVAL_NDBOLE_BROKERAGRTERMS_ONOFF),'0');
    if v_enable != 'X' then
      o_MSGCode := -1;
      o_MSGText := 'Настройка '||C_NAME_REGVAL_NDBOLE_BROKERAGRTERMS_ONOFF||' отключена';
      return;
    end if;
    
    o_MSGCode := 0;
    o_MSGText := 'Успешно';
    
    begin
      select GUID,
             ProcessID,
             cast(it_xml.char_to_timestamp_iso8601(RequestTime) as date),
             ClientCFTID
        into v_msgID,
             v_processID,
             v_requestTime,
             v_clientCFTID
        from json_table (p_messbody, '$.sendBrokerageAgreementTermsReq'
                                     columns (GUID         varchar2(100) path '$.GUID',
                                              ProcessID    varchar2(100) path '$.processId',
                                              RequestTime  varchar2(30)  path '$.RequestTime',
                                              ClientCFTID  varchar2(30)  path '$.data.clientInfo.clientId.objectId'));
      o_msgid := v_msgID;
    exception
      when others then
        o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
        o_MSGText := substr('Ошибка разбора входящего JSON-сообщения: '||sqlerrm, 1, 300);
    end;
    
    if p_messmeta is null then
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText:= 'Не передан входящий параметр p_messmeta';
      return;
    end if;
        
    SELECT extractValue(p_messmeta, 'KAFKA/Header')
      into v_header
    from dual;

    v_json_req := JSON_OBJECT_T(v_header);
    v_requestID  := v_json_req.get_string('x-request-id');
    
    if v_requestID is null then
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText:= 'Не найден параметр x-request-id в p_messmeta';
      return;
    end if;
    
    BrokerateAgreementTermsReqSave(p_reqGUID => v_msgID
                                  ,p_requestID => v_requestID
                                  ,p_processID => v_processID
                                  ,p_reqDate => v_requestTime
                                  ,p_reqJson => p_messbody
                                  ,p_clientCFTID => v_clientCFTID
                                  ,p_state => case when o_MSGCode = 0 then BROKERAGRTERMS_REQ_STATE_NEW else BROKERAGRTERMS_REQ_STATE_SENDANSWER end
                                  ,p_partyID => 0
                                  ,p_dlContrID => 0
                                  ,p_errorCode => o_MSGCode
                                  ,p_errorText => case when o_MSGCode = 0 then chr(1) else o_MSGText end);
                 
    exception
      when others then
        o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
        o_MSGText := substr(sqlerrm, 1, 300);
  end;

  /**
  * BOSS-5842 Ответ в ДБО "Свой бизнес" о статусе открытия/измнения ДБО СОФР
  * @since RSHB 120
  * @qtest NO
  */
  procedure SendBrokerageAgreementTermsResp(p_requestID integer
                                           ,o_errorCode out number
                                           ,o_errorDesc out varchar2) is
    v_MessMeta XMLType;
    v_json clob := null;
    v_MsgID itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
    v_addParams IT_IPS_DFactory.tt_addparams := null;
    v_req dUserLEAgrDataReq_dbt%rowtype;
  begin
    o_errorCode := 0;
    o_errorDesc := '';
    
    select req.*
      into v_req
      from dUserLEAgrDataReq_dbt req
     where req.t_id = p_requestID;
    
    if v_req.t_errorCode != 0 then
      select json_arrayagg(json_object('code' is to_char(v_req.t_errorCode),
                                       'message' is v_req.t_errorText))
      into v_json
      from dual;
    end if;

    select json_object('sendBrokerageAgreementTermsResp' is
                 json_object('guid' is v_MsgID,
                             'requestTime' is it_xml.timestamp_to_char_iso8601(SYSDATE),
                             'errors' is v_json format json,
                             'data' is decode(v_req.t_dlContrID, 0, null, 
                                                     json_object('agreementInfo' is  
                                                                          (select json_object('agreementNumber' is sfc.t_number,
                                                                                              'agreementDate' is to_char(sfc.t_dateBegin, 'DD-MM-YYYY'))
                                                                           from dDlContr_dbt dlc, dSfContr_dbt sfc
                                                                          where dlc.t_dlContrID = v_req.t_dlContrID
                                                                            and sfc.t_ID = dlc.t_sfContrID)))
                       absent on null))
      into v_json
      from dual;
    
    v_addParams := it_ips_dfactory.add_addparams(p_addParams  => v_addParams
                                                ,p_paramName  => 'x-client-id'
                                                ,p_paramValue => v_req.t_clientCFTID);
    
    v_MessMeta := IT_IPS_DFactory.add_KafkaHeader_Xmessmeta( p_List_dllvalues_dbt => 4196
                                                            ,p_traceid => v_req.t_reqGUID
                                                            ,p_requestid => v_req.t_requestID
                                                            ,p_addparams => v_addParams
                                                            ,p_templateparams => NULL
                                                            ,p_outputfilename => NULL);

    it_kafka.load_msg(io_msgid => v_MsgID
                     ,p_message_type => it_q_message.C_C_MSG_TYPE_A
                     ,p_ServiceName => 'NDBOLE.SendBrokerageAgreementTerms'
                     ,p_Receiver => it_ndbole.C_C_SYSTEM_NAME
                     ,p_MESSBODY => v_json
                     ,p_MessMETA => v_MessMETA
                     ,p_CORRmsgid => v_req.t_reqGUID
                     ,o_ErrorCode => o_errorCode
                     ,o_ErrorDesc => o_errorDesc);
  exception
    when others then
      o_errorCode := abs(sqlcode);
      o_errorDesc := dbms_utility.format_error_stack || ' ' || dbms_utility.format_error_backtrace;
  end;
  
  /**  
  * BOSS-5842 Информирование сопровождения о возникших ошибках
  * @since RSHB 120
  * @qtest NO
  */
  procedure SupportInforming(p_requestID integer
                            ,o_errorCode out number
                            ,o_errorDesc out varchar2) is
    v_req dUserLEAgrDataReq_dbt%rowtype;
  begin
    o_errorCode := 0;
    o_errorDesc := '';
    
    select req.*
      into v_req
      from dUserLEAgrDataReq_dbt req
     where req.t_id = p_requestID;

    if v_req.t_errorCode != 0 then
      it_event.RegisterError(p_SystemId    => C_C_SYSTEM_NAME
                            ,p_ServiceName => 'SendBrokerageAgreementTerms'
                            ,p_ErrorCode   => v_req.t_errorCode
                            ,p_ErrorDesc   => 'Сервис SendBrokerageAgreementTerms: ' || v_req.t_errorText
                            ,p_LevelInfo   => 8);
    end if;
    
    exception
      when others then
        o_errorCode := abs(sqlcode);
        o_errorDesc := dbms_utility.format_error_stack || ' ' || dbms_utility.format_error_backtrace;
  end;
  
  /**  
  * BOSS-5842 Отправка запроса в Фабрику документов на формирование уведомления 4.4
  * @since RSHB 120
  * @qtest NO
  */
  procedure GenerateDocument(p_requestID integer
                            ,o_errorCode out number
                            ,o_errorDesc out varchar2) is
    v_MessMeta XMLType;
    v_MsgID itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
    v_reqID itt_q_message_log.msgid%type;
    
    v_json clob;
    v_clientCFTID dUserLEAgrDataReq_dbt.t_clientCFTID%type;
  begin
    o_errorCode := 0;
    o_errorDesc := '';
    
    select json_arrayagg(json_object('BrokAccountNumber' is acc.t_account, 
                                     'SectionName' is (case when sf.t_servKind = 15 then 'Срочный рынок'
                                                            when sf.t_servKind = 1 and sf.t_servKindSub = 8 then 'Фондовый рынок'
                                                            when sf.t_servKind = 1 and sf.t_servKindSub = 9 then 'Внебиржевой рынок'
                                                            when sf.t_servKind = 21 and sf.t_servKindSub = 8 then 'Валютный рынок'
                                                            else '' end) || '( ' || (select t_CCY from dFinInstr_dbt fin where fin.t_fiid = dlacc.t_fiid) || ' )',
                                     'SectionCode' is '"' || decode(mp.t_mpCode, chr(1), '', mp.t_mpCode) || '"' format json,
                                     'SectionRegDate' is '"' || decode(mp.t_mpRegDate, to_date('01.01.0001', 'DD.MM.YYYY'), '', to_char(mp.t_mpRegDate, 'DD.MM.YYYY')) || '"' format json))
      into v_json
      from dUserLEAgrDataReq_dbt req
      join dDlContrMP_dbt mp
        on mp.t_dlContrID = req.t_dlContrID
       and mp.t_mpCloseDate = to_date('01.01.0001', 'DD.MM.YYYY')
      join dSfContr_dbt sf 
        on sf.t_ID = mp.t_sfContrID
      join dDlContrAcc_dbt dlacc 
        on dlacc.t_dlContrID = mp.t_dlContrID
       and dlacc.t_mpID = mp.t_id
      join dAccount_dbt acc
        on acc.t_accountID = dlacc.t_accountID
     where req.t_id = p_requestID
     order by sf.t_servkind, sf.t_servkindsub;
    
    select json_object('GUID' is req.t_reqGUID,
                       'RequestTime' is it_xml.timestamp_to_char_iso8601(SYSDATE),
                       'BrokerageAgreementConfirmNotification' 
                                              is json_object('SuccessfulNotice' 
                                                                  is json_object('FullName' is party.t_name,
                                                                                 'ContrNumber' is sfc.t_number,
                                                                                 'ContrDate' is to_char(sfc.t_dateBegin, 'DD.MM.YYYY'),
                                                                                 'ClientCode' is '"' || nvl((select t_code
                                                                                                              from ddlobjcode_dbt 
                                                                                                             where t_ObjectType = OBJECTTYPE_CONTRACT
                                                                                                               and t_CodeKind = DLCK_EKK
                                                                                                               and t_objectid = req.t_dlContrID 
                                                                                                               and t_bankclosedate = to_date('01010001','ddmmyyyy')), '') || '"' format json,
                                                                                 'DepoContractNumber' is '"' || trim(GetNoteKindFromContract(req.t_dlContrID, CONTRACT_NOTEKIND_DEPO_CONTRACTNUMBER)) || '"' format json,
                                                                                 'DepoContractDate' is '"' || GetNoteKindFromContract(req.t_dlContrID, CONTRACT_NOTEKIND_DEPO_CONTRACTDATE) || '"' format json,
                                                                                 'DepoAccountNumber' is '"' || GetNoteKindFromContract(req.t_dlContrID, CONTRACT_NOTEKIND_DEPO_ACCOUNT) || '"' format json,
                                                                                 'DepoTradingAccountNumber' is '"' || GetNoteKindFromContract(req.t_dlContrID, CONTRACT_NOTEKIND_DEPO_TRADEACCOUNT) || '"' format json,
                                                                                 'Partyid' is to_char(req.t_partyID),
                                                                                 'AccontList' is v_json format json))),
           req.t_reqGUID,
           req.t_clientCFTID
      into v_json,
           v_reqID,
           v_clientCFTID
      from dUserLEAgrDataReq_dbt req, dParty_dbt party, dDlContr_dbt dlc, dSfContr_dbt sfc
     where req.t_ID = p_requestID
       and req.t_partyID = party.t_partyID
       and dlc.t_dlContrID = req.t_dlContrID
       and dlc.t_sfContrID = sfc.t_ID;
    
    v_MessMeta := IT_IPS_DFactory.add_KafkaHeader_Xmessmeta( p_List_dllvalues_dbt => 4198
                                                            ,p_traceid => v_reqID
                                                            ,p_requestid => v_MsgID
                                                            ,p_templateparams => NULL
                                                            ,p_outputfilename => v_clientCFTID || ' Уведомление о приеме на брокерское и депозитарное обслуживание');
    IT_Kafka.load_msg_S3(p_msgid => v_MsgID
                        ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                        ,p_ServiceName => 'IPS_DFACTORY.GenerateDocument' 
                        ,p_Receiver => IT_IPS_DFactory.C_C_SYSTEM_NAME
                        ,p_CORRmsgid => NULL
                        ,p_MESSBODY => v_json
                        ,p_MessMETA => v_MessMETA
                        ,p_isquery => 0
                        ,o_ErrorCode => o_errorCode
                        ,o_ErrorDesc => o_errorDesc);
    
  exception
    when others then
      o_errorCode := abs(sqlcode);
      o_errorDesc := sqlerrm;
  end;
  
  /**  
  * BOSS-5842 Отправка в ДБО ЮЛ сообщения о готовности уведомления 4.4
  * @since RSHB 120
  * @qtest NO
  */
  procedure SendLEBrokerageAgreementInfo(p_req dUserLEAgrDataReq_dbt%rowtype
                                        ,p_bucketName varchar2
                                        ,p_outPath varchar2
                                        ,p_outputFilename varchar2
                                        ,o_errorCode out number
                                        ,o_errorDesc out varchar2) is
    v_MessMeta XMLType;
    v_json clob := null;
    v_MsgID itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
    v_addParams IT_IPS_DFactory.tt_addparams := null;
  begin
    select json_object('SendLEBrokerageAgreementInfoReq' is
                 json_object('GUID' is v_MsgID,
                             'RequestTime' is it_xml.timestamp_to_char_iso8601(SYSDATE),
                             'ProcessId' is p_req.t_processID,
                             'ClientId' is json_object('ObjectId' is p_req.t_clientCFTID,
                                                       'SystemId' is 'CFT',
                                                       'SystemNodeId' is '')
                       absent on null))
      into v_json
      from dual;
      
    v_addParams := it_ips_dfactory.add_addparams(p_addParams  => v_addParams
                                                ,p_paramName  => 'x-client-id'
                                                ,p_paramValue => p_req.t_clientCFTID);
    v_addParams := it_ips_dfactory.add_addparams(p_addParams  => v_addParams
                                                ,p_paramName  => 'x-bucket-name'
                                                ,p_paramValue => p_bucketName);
    v_addParams := it_ips_dfactory.add_addparams(p_addParams  => v_addParams
                                                ,p_paramName  => 'x-out-path'
                                                ,p_paramValue => p_outPath);
    
    v_MessMeta := IT_IPS_DFactory.add_KafkaHeader_Xmessmeta( p_List_dllvalues_dbt => 4197
                                                            ,p_traceid => p_req.t_reqGUID
                                                            ,p_requestid => v_MsgID
                                                            ,p_addparams => v_addParams
                                                            ,p_templateparams => NULL
                                                            ,p_outputfilename => p_outputFilename);
                                                            
    it_kafka.load_msg(io_msgid => v_MsgID
                     ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                     ,p_ServiceName => 'NDBOLE.SendLEBrokerageAgreementInfo'
                     ,p_Receiver => it_ndbole.C_C_SYSTEM_NAME
                     ,p_MESSBODY => v_json
                     ,p_MessMETA => v_MessMETA
                     ,p_CORRmsgid => p_req.t_reqGUID
                     ,o_ErrorCode => o_errorCode
                     ,o_ErrorDesc => o_errorDesc);
  end;
  
  /**  
  * BOSS-5842 Ответ на запрос в Фабрику документов на формирование уведомления 4.4
  * @since RSHB 120
  * @qtest NO
  */
  procedure GenerateDocumentResp(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype) is
    vMsgID      itt_q_message_log.msgid%type := it_q_message.get_sys_guid;

    x_header    clob;
    v_json      JSON_OBJECT_T;
    v_trace_id  varchar2(200);
    v_bucketName varchar2(100);
    v_outPath   varchar2(300);
    v_outputFileName varchar2(300);
    v_isSuccess boolean;
    v_json_arr  JSON_ARRAY_T;
    v_errorObj  JSON_OBJECT_T;
    
    v_req       dUserLEAgrDataReq_dbt%rowtype;
    v_ErrorCode varchar2(20);
    v_ErrorText varchar2(300);
  begin
    begin
      o_MSGCode := 0;
      o_MSGText := 'Успешно';
      o_msgid   := vMsgID;

      if p_messbody is null then
        o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
        o_MSGText:= 'Не передан входящий параметр p_messbody';
        return;
      elsif p_messmeta is null then
        o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
        o_MSGText:= 'Не передан входящий параметр p_messmeta';
        return;
      end if;

      select extractValue(p_messmeta, 'KAFKA/Header')
        into x_header
        from dual;

      v_json := JSON_OBJECT_T(x_header);
      v_trace_id := v_json.get_string('x-trace-id');
      v_bucketName := v_json.get_string('x-bucket-name');
      v_outPath := v_json.get_string('x-out-path');
      v_outputFilename := v_json.get_string('x-output-file-name');

      if v_trace_id is null then
        o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
        o_MSGText:= 'Не найден параметр x-trace-id в p_messmeta';
        return;
      end if;

      begin
        select * 
          into v_req
          from dUserLEAgrDataReq_dbt
         where t_reqGUID = v_trace_id;
      exception
        when no_data_found then 
        o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
        o_MSGText:= 'Не найдена запись с GUID = '||v_trace_id;
        return;
      end;

      begin
        v_json := JSON_OBJECT_T(p_messbody);
        v_isSuccess := v_json.get_Boolean('isSuccess');
        v_json_arr := v_json.get_Array('errors');
       exception 
        when others then
          o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
          o_MSGText := substr('Непредвиденная ошибка при разборе входящего JSON-сообщения: '||sqlerrm, 1, 300);
          return;
      end;

      if v_isSuccess then 
         v_ErrorCode := 0;
         v_ErrorText := 'Сформировано уведомление';
      else 
        v_ErrorCode := 1;
        v_ErrorText := 'Ошибка IPS формирования уведомления: ';

        for j in 0 .. v_json_arr.get_size - 1 loop
          if v_json_arr.get(j).is_object then
            v_errorObj := treat(v_json_arr.get(j) as JSON_OBJECT_T);
            v_ErrorText := substr(v_ErrorText
                                  ||' '||v_errorObj.get_string('errorCode')
                                  ||'-'||v_errorObj.get_string('errorMessage') , 1 ,300);
          end if;
        end loop;
      end if;

      BrokerageAgreementTermsReqSetState(p_requestID => v_req.t_id
                                        ,p_state     => v_req.t_state
                                        ,p_errorCode => v_ErrorCode
                                        ,p_errorText => v_ErrorText);

      v_req.t_errorText := v_errorText;

      SendLEBrokerageAgreementInfo(p_req => v_req,
                                   p_bucketName => v_bucketName,
                                   p_outPath => v_outPath,
                                   p_outputFilename => v_outputFilename,
                                   o_errorCode => o_MSGCode,
                                   o_errorDesc => o_MSGText);

    exception
      when others then
        o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
        o_MSGText := substr(sqlerrm, 1, 300);
    end;
    
    if o_MSGCode != 0 then
      BrokerageAgreementTermsReqSetState(p_requestID => v_req.t_id
                                        ,p_state     => v_req.t_state
                                        ,p_errorCode => o_MSGCode
                                        ,p_errorText => o_MSGText);
      
      it_event.RegisterError(p_SystemId    => C_C_SYSTEM_NAME
                            ,p_ServiceName => 'SendBrokerageAgreementTerms'
                            ,p_ErrorCode   => o_MSGCode
                            ,p_ErrorDesc   => 'Сервис SendBrokerageAgreementTerms: ' || o_MSGText
                            ,p_LevelInfo   => 8);
    end if;
  end;

END it_ndbole;
/