CREATE OR REPLACE package body it_broker is

/******************************************************************************
 NAME: CLIENT_INFO
 PURPOSE: Формирование json сообщения для передачи в Свои Инвестии через kafka

 REVISIONS:
 Ver Date Author Description
 --------- ---------- --------------- ------------------------------------
 1.0 17.05.2024 Shishkin-ev 1. Created this package.
******************************************************************************/
 

/*коды субьекта*/
PARTY_CODEKIND_ABS    CONSTANT NUMBER(10) := 101;     --код ЦФТ
PARTY_CODEKIND_QUICK  CONSTANT NUMBER(10) := 105;   --код квика

/*типы объектов**/
OBJECTTYPE_SUBJECT     CONSTANT NUMBER(10) := 3;       --субъект
OBJECTTYPE_CONTRACT    CONSTANT NUMBER(10) := 207;    --договор
OBJECTTYPE_SUBCONTRACT CONSTANT NUMBER(10) := 659;   --субдоговор

OBJCATEG_TYPE_CONNECT CONSTANT NUMBER(10) := 170; --тип подключения (СКПЭП/ДВУХФАКТОР...И Т.Д.)
OBJCATEG_TYPE_RISK CONSTANT NUMBER(10) := 103; --уровень риска

/*торговые площадки*/
SERV_SUBKIND_STOCK CONSTANT NUMBER(10)     := 8; --биржевой рынок
SERV_SUBKIND_STOCK_OVER CONSTANT NUMBER(10):= 9; --внебиржевой рынок

SERV_KIND_FOND CONSTANT NUMBER(10)    := 1; --Фондовый диллинг
SERV_KIND_FORTS CONSTANT NUMBER(10)   := 15; --Срочные контракты
SERV_KIND_MMVB CONSTANT NUMBER(10)    := 21; --Валютный рынок
SERV_KIND_CBO CONSTANT NUMBER(10)     := 8; --СВО
SERV_KIND_ACCBILLS CONSTANT NUMBER(10):= 14; --учетные векселя

TRADING_PLATFORM_SPB CONSTANT NUMBER(10) := 151337;   --СПБ
TRADING_PLATFORM_MMVB CONSTANT NUMBER(10) := 2;       --ММВБ


/*примечания по договору Депо*/
CONTRACT_NOTEKIND_DEPO_ACCOUNT CONSTANT NUMBER(10) := 101;           -- Счет Депо Владельца
CONTRACT_NOTEKIND_DEPO_CONTRACTNUMBER CONSTANT NUMBER(10) := 102;   -- Номер договора Депо
CONTRACT_NOTEKIND_DEPO_OPENDATE CONSTANT NUMBER(10):= 103;      -- Дата открытия договора Депо
CONTRACT_NOTEKIND_DEPO_TRADEACOOUNT CONSTANT NUMBER(10) := 104;      -- Торговый счет Депо
CONTRACT_NOTEKIND_DEPO_CLOSEDATE CONSTANT NUMBER(10):= 110;      -- Дата закрытия договора Депо

CONTRACT_NOTEKIND_DEPO_TCA_MMVB CONSTANT NUMBER(10):= 5;      -- Счета клиента ТКС на ММВБ
CONTRACT_NOTEKIND_DEPO_TCA_SPB CONSTANT NUMBER(10):= 10;      -- Счета клиента ТКС на СПБ

CONTRACT_NOTEKIND_TARIF CONSTANT NUMBER(10):= 146;      -- тариф
CONTRACT_NOTEKIND_DATESTART_TARIF CONSTANT NUMBER(10):= 147;      -- дата начала действия тарифа


CONTRACT_NOTEKIND_DATE_MODIFICATION CONSTANT NUMBER(10):= 170; -- Дата последней модификации договора


/*типы контактов субъекта*/
CONTACT_KIND_PHONE CONSTANT NUMBER(10):= 1; --телефон
CONTACT_KIND_EMAIL CONSTANT NUMBER(10):= 5; --email

/*
sql запрос для получения данных(строка) из примечания
@since RSHB 105
@qtest NO
@param p_objecttype тип объекта 
@param p_notekind номер примечания 
@return sql запрос 
*/

FUNCTION GetNoteTextstring(p_objecttype in number, p_notekind in number) return varchar2   is
 v_notetexts varchar2(3000);
BEGIN v_notetexts := '
    SELECT trim(chr(0) from RSB_STRUCT.getString(C.T_TEXT))  FROM dnotetext_dbt C 
           WHERE C.T_ID =  
            ( SELECT MAX(B.T_ID) FROM dnotetext_dbt B 
              WHERE B.T_OBJECTTYPE = '||p_objecttype||' 
               AND B.T_NOTEKIND in ('||p_notekind||') 
               AND B.T_DOCUMENTID = LPAD( TO_CHAR(ddl.t_dlcontrid), 34, ''0'') 
               AND B.T_DATE <= to_date(''01.01.9999'',''dd.mm.yyyy''))';
               
    return v_notetexts;   
    
end;

/*
sql запрос для получения данных(дата) из примечания
@since RSHB 105
@qtest NO
@param p_objecttype тип объекта 
@param p_notekind номер примечания 
@return sql запрос
*/

FUNCTION GetNoteTextdate(p_objecttype in number, p_notekind in number) return varchar2   is
 v_notetextd varchar2(3000);
BEGIN
 v_notetextd :='
   select coalesce((SELECT trim(chr(0) from to_char( RSB_STRUCT.getdate(C.T_TEXT),''dd.mm.yyyy''))  FROM dnotetext_dbt C 
           WHERE C.T_ID =  
            ( SELECT MAX(B.T_ID) FROM dnotetext_dbt B 
              WHERE B.T_OBJECTTYPE = '||p_objecttype||' 
               AND B.T_NOTEKIND = '||p_notekind||' 
               AND B.T_DOCUMENTID = LPAD( TO_CHAR(ddl.t_dlcontrid), 34, ''0'') 
               AND B.T_DATE <= to_date(''01.01.9999'',''dd.mm.yyyy''))) , ''01.01.0001'') from dual';
               
    return v_notetextd;   
    
end;

/*
sql запрос для получения для получения данных из категории
@since RSHB 105
@qtest NO
@param p_objecttype тип объекта 
@param p_groupid номер категории 
@return sql запрос
*/

FUNCTION GetValueAttr_sql(p_objecttype in number, p_groupid in number) return varchar2   is
 AttrValue_sql varchar2(1000);
BEGIN AttrValue_sql :='
                  select attr.t_name FROM dobjatcor_dbt atcor, DOBJATTR_DBT attr
                         WHERE attr.t_objecttype = atcor.t_objecttype
                           AND atcor.t_objecttype = '||p_objecttype||'
                           AND attr.t_groupid = atcor.t_groupid
                           AND atcor.t_groupid = '||p_groupid||'
                           AND ATTR.T_ATTRID = atcor.T_ATTRID
                           AND atcor.t_object = LPAD (ddl.t_dlcontrid, 34, ''0'')
                           AND t_validtodate = to_date(''31.12.9999'',''dd.mm.yyyy'')'; 
          
    return AttrValue_sql;   

EXCEPTION
    WHEN others THEN RETURN '';
END;    

/*
Функция получения sql запроса типа авторизации клиента
@since RSHB 105
@qtest NO
@param p_objecttype тип объекта 
@param p_groupid номер категории 
@return sql запрос
*/

FUNCTION GetAuthorizationInfo_SQL(p_objecttype in number, p_notekind in number) return varchar2   is
 AuthorizationInfo varchar2(1000);
BEGIN
 AuthorizationInfo :='
  select JSON_OBJECT (''AuthorizationInfo'' IS
         json_arrayagg( JSON_OBJECT (''AuthorizationType'' is attr.t_name)
         format json RETURNING CLOB)RETURNING CLOB) 
               FROM dobjatcor_dbt atcor, DOBJATTR_DBT attr
         WHERE attr.t_objecttype = atcor.t_objecttype
           AND atcor.t_objecttype = '||p_objecttype||'
           AND attr.t_groupid = atcor.t_groupid
           AND atcor.t_groupid = '||p_notekind||'
           AND ATTR.T_ATTRID = atcor.T_ATTRID
           AND atcor.t_object = LPAD (ddl.t_dlcontrid, 34, ''0'')
           AND t_validtodate = to_date(''31.12.9999'',''dd.mm.yyyy'')'; 
          
    return AuthorizationInfo;   

EXCEPTION
    WHEN others THEN RETURN '';
END;    


/*
Функция получения информации по клиенту физику(ФИО,дата рождения, пол)
@since RSHB 105
@qtest NO
@param p_clientid id клиента 
@param Kind вид получаемой информации 
@return в зависимости от Kind, либо ФИО в json формате, либо дату рождения, либо пол
*/

FUNCTION GetPersonInfo(p_clientid in number, Kind IN VARCHAR2) return varchar2   is
  PersonInfo varchar2(1000);
  NO_PersonInfo EXCEPTION;
BEGIN
 
    IF KIND = 'FIO' THEN
      BEGIN
        select 
             JSON_OBJECT(
                         'LastName' is pers.t_name1,
                         'FirstName' is pers.t_name2,
                         'MiddleName' is pers.t_name3
                         RETURNING CLOB)  into PersonInfo
             from dpersn_dbt pers where pers.t_personid = p_clientid;
         EXCEPTION
           WHEN NO_DATA_FOUND THEN 
                     select 
                    JSON_OBJECT(
                                          'LastName' is '',
                                          'FirstName' is '',
                                          'MiddleName' is ''
                                          RETURNING CLOB)  into PersonInfo
                      from dual;    
      END;
 
  END IF;
  
  IF KIND =  'BORN' THEN
    BEGIN
        select to_char(pers.t_born,'dd.mm.yyyy') into PersonInfo from dpersn_dbt pers where pers.t_personid = p_clientid;
           EXCEPTION
           WHEN NO_DATA_FOUND THEN 
                     select to_char(to_date('01.01.0001','dd.mm.yyyy'), 'dd.mm.yyyy') into PersonInfo from dual; 
      END;
  END IF;

  IF KIND =  'ISMAIL' THEN

    select decode(pers.t_ismale, chr(88),1,0) into PersonInfo from dpersn_dbt pers where pers.t_personid = p_clientid;

  END IF;
      
  return PersonInfo;

EXCEPTION

    WHEN NO_DATA_FOUND THEN RETURN '';

    WHEN OTHERS THEN RAISE NO_PersonInfo;

END;    

/*
Функция получения информации о документах клиента
@since RSHB 105
@qtest NO
@param p_clientid id клиента 
@return документы клиента в json формате
*/

FUNCTION GetDocument(p_clientid in number) return varchar2   is
  Document varchar2(5000);
  NO_Document EXCEPTION;
BEGIN

  SELECT
      JSON_OBJECT ('IdentityDocument' is
      json_arrayagg(
      JSON_OBJECT('DocumentType' is  (select dp.t_name from dpaprkind_dbt dp where dp.t_paperkind = r.t_paperkind), 
                  'DocumentSeries' is r.t_paperseries,
                  'DocumentNumber' is r.t_papernumber,
                  'IssuedDate' is to_char(r.T_PAPERISSUEDDATE,'dd.mm.yyyy'),
                  'Issuer' is  decode(r.T_PAPERISSUER, chr(1), null, r.T_PAPERISSUER),
                  'IssuerCode' is r.T_PAPERISSUERCODE ,
                  'MainDocumentStatus' is decode(r.T_ISMAIN, chr(88), 1,0) ,
                  'InvalidDocumentStatus' is decode(r.T_ISNOTVALID,chr(88),1,0) ,
                  'ValidDateTo' is to_char(r.T_VALIDTODATE,'dd.mm.yyyy')  RETURNING CLOB                                
                             )  format json RETURNING CLOB
                  )RETURNING CLOB)
          as json_doc into Document
  FROM DPERSNIDC_DBT r
  WHERE r.t_personid = p_Clientid;
       
  IF INSTR(Document,'DocumentType') = 0 THEN 
      SELECT
      JSON_OBJECT ('IdentityDocument' is
      json_arrayagg(
      JSON_OBJECT('DocumentType' is  '', 
                  'DocumentSeries' is '',
                  'DocumentNumber' is '',
                  'IssuedDate' is to_char(to_date('01.01.0001','dd.mm.yyyy'),'dd.mm.yyyy'),
                  'Issuer' is '' ,
                  'IssuerCode' is '' ,
                  'MainDocumentStatus' is '' ,
                  'InvalidDocumentStatus' is '' ,
                  'ValidDateTo' is to_char(to_date('01.01.0001','dd.mm.yyyy'),'dd.mm.yyyy')  RETURNING CLOB                                
                             )  format json RETURNING CLOB
                  )RETURNING CLOB)
          as json_doc into Document from dual;
  END IF;
  
  return Document;

EXCEPTION

    WHEN OTHERS THEN RAISE NO_Document;

END;    

/*
Функция получения адресса клиента
@since RSHB 105
@qtest NO
@param p_clientid id клиента 
@return адресс клиента в json формате
*/

FUNCTION GetAdress(p_clientid in number) return varchar2   is
  adress varchar2(2000);
  NO_ADRESS EXCEPTION;
BEGIN

  SELECT
       JSON_OBJECT ('ClientAddress' is
       json_arrayagg(
       JSON_OBJECT('ClientAddressType' is  (select adrtype.t_name from dadrtype_dbt adrtype where adrtype.t_type = adr.t_type), 
                   'ClientAddressDetails' is adr.t_adress
                 RETURNING CLOB )  format json RETURNING CLOB
                  )RETURNING CLOB )
          as json_doc into adress
     FROM DADRESS_DBT adr 
    WHERE adr.T_PARTYID = p_Clientid;
    
    return adress;
    
EXCEPTION

    WHEN NO_DATA_FOUND THEN RETURN '';

    WHEN OTHERS THEN RAISE NO_ADRESS;


END;    

/*
Функция получения id клиента в разных системах
@since RSHB 105
@qtest NO
@param p_clientid id клиента 
@return id  клиента в json формате
*/
  
FUNCTION GetClientIdList(p_clientid in number) return varchar2   is
  ClientIdList varchar2(2000);
  NO_ClientIdList EXCEPTION;
BEGIN
  
 
  SELECT JSON_OBJECT ('ClientId' IS          
         json_arrayagg(
         JSON_OBJECT('ObjectId' IS code,
                     'SystemId' IS SYSTEM,
                     'SystemNodeId' IS '1'
                     RETURNING CLOB)  format json RETURNING CLOB
                      ) RETURNING CLOB
                     )
               AS json_doc into ClientIdList
          FROM (
                SELECT TO_CHAR(d.t_partyid) AS code, 'SOFR' AS SYSTEM
                FROM dparty_dbt d  WHERE  t_partyid = p_clientid
         UNION ALL
                SELECT  obj.t_code, 'CFT' FROM DOBJCODE_DBT obj WHERE  obj.t_objectid = p_clientid AND obj.t_objecttype = OBJECTTYPE_SUBJECT AND obj.t_codekind IN (PARTY_CODEKIND_ABS) and obj.t_state = 0
         UNION ALL
                SELECT  obj.t_code, 'QUIK' FROM DOBJCODE_DBT obj WHERE  obj.t_objectid = p_clientid AND obj.t_objecttype = OBJECTTYPE_SUBJECT AND obj.t_codekind IN (PARTY_CODEKIND_QUICK) and obj.t_state = 0
               );
       
    return ClientIdList;
    
EXCEPTION

    WHEN NO_DATA_FOUND THEN RETURN '';

    WHEN OTHERS THEN RAISE NO_ClientIdList;

END;    

/*
Функция получения телефонов клиента 
@since RSHB 105
@qtest NO
@param p_clientid id клиента 
@return телефон клиента в json формате
*/

FUNCTION GetPhoneList(p_clientid in number) return varchar2   is
  PhoneList varchar2(5000);
BEGIN
  SELECT JSON_OBJECT ('Phone' IS          
         json_arrayagg(
         JSON_OBJECT( 'PhoneType' is  ( select t.t_name from DCONTACTTYPE_DBT t where t.t_kind = CONTACT_KIND_PHONE and t.t_type = cont.t_contacttype), 
                      'PhoneNumber' is cont.t_value,
                      'PhoneStatus' is decode(cont.t_ismain,chr(88),1,0)
                    RETURNING CLOB )  format json RETURNING CLOB
                      )RETURNING CLOB) into PhoneList
  FROM DCONTACT_DBT cont
  WHERE t_contactkind = CONTACT_KIND_PHONE and T_PARTYID = p_Clientid;
              
  return PhoneList;
    
EXCEPTION
  WHEN NO_DATA_FOUND THEN RETURN '';
END;

/*
Функция получения email адресса клиента 
@since RSHB 105
@qtest NO
@param p_clientid id клиента 
@return email адресс клиента в json формате
*/

FUNCTION GetEmailList(p_clientid in number) return varchar2   is
  EmailList varchar2(1000);
BEGIN
     SELECT JSON_OBJECT ('Email' IS 
            json_arrayagg(
            JSON_OBJECT( 'EmailType' is  (select t.t_name from DCONTACTTYPE_DBT t where t.t_kind = CONTACT_KIND_EMAIL and  t.t_type = D.t_contacttype),  
                         'EmailAddress' is d.t_value,
                         'EmailStatus' is decode(d.t_ismain,chr(88),1,0)
                       RETURNING CLOB)  format json RETURNING CLOB
                        )RETURNING CLOB) into EmailList
        FROM DCONTACT_DBT d
       WHERE d.t_contactkind = CONTACT_KIND_EMAIL and d.T_PARTYID = p_Clientid;
    return EmailList;
 
EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN '';
END;

/*
Функция получения признака квалифицированного инвестора 
@since RSHB 105
@qtest NO
@param p_clientid id клиента 
@return признак квалифицированного инвестора в json формате
*/
  
FUNCTION GetInvestorQualificationInfoList(p_clientid in number) return varchar2   is
  InvestorQualificationInfoList varchar2(1000);
BEGIN

     SELECT JSON_OBJECT ('InvestorQualificationInfo' IS 
            json_arrayagg(
            JSON_OBJECT( 'RegDate' is to_char(q.t_regdate,'dd.mm.yyyy'),
                                    'SysDate' is to_char(q.t_sysdate,'dd.mm.yyyy'),
                                    'SysTime' is to_char(q.t_systime,'dd.mm.yyyy hh24:mi:ss'),
                                    'ChangeDate' is to_char(q.t_changedate,'dd.mm.yyyy'),
                                   'InvestorQualificationStatus' is q.t_state,    
                                   'FinancialInstrumentType' is ''                                                                
                         RETURNING CLOB)  format json RETURNING CLOB
                        )RETURNING CLOB) into InvestorQualificationInfoList
        FROM DSCQINV_DBT Q WHERE  Q.T_PARTYID = p_Clientid;
       
    IF INSTR(InvestorQualificationInfoList,'RegDate') = 0 THEN 
             SELECT JSON_OBJECT ('InvestorQualificationInfo' IS 
                         json_arrayagg(
                         JSON_OBJECT( 'RegDate' is to_char(to_date('01.01.0001','dd.mm.yyyy'), 'dd.mm.yyyy'),
                                                 'SysDate' is to_char(to_date('01.01.0001','dd.mm.yyyy'), 'dd.mm.yyyy'),
                                                 'SysTime' is to_char(to_date('01.01.0001 01.01.01','dd.mm.yyyy hh24:mi:ss'),'dd.mm.yyyy hh24:mi:ss'),
                                                 'ChangeDate' is to_char(to_date('01.01.0001','dd.mm.yyyy'), 'dd.mm.yyyy'),
                                                 'InvestorQualificationStatus' is '',    
                                                 'FinancialInstrumentType' is ''                                                                
                        RETURNING CLOB)  format json RETURNING CLOB
                        )RETURNING CLOB) into InvestorQualificationInfoList from dual;
    END IF;                    
    
    return InvestorQualificationInfoList;
    
EXCEPTION
    WHEN OTHERS THEN RETURN '';

END;

/*
Функция получения типа клиента фл/юл/ип 
@since RSHB 105
@qtest NO
@param p_clientid id клиента 
@return тип клиента в json формате
*/

FUNCTION GetClientInfo(p_clientid in number) return varchar2   is
  ClientInfo varchar2(1000);
BEGIN

  SELECT 
     JSON_OBJECT(
                 'ClientType' is  'Физическое лицо' ,
                 'EmployerStatus' is (case when pers.t_isemployer = chr(88) then 1 else 0 end)
                 RETURNING CLOB)  into ClientInfo
    from dparty_dbt party, dpersn_dbt pers 
    where pers.t_personid = party.t_partyid and party.t_partyid = p_clientid;

  return ClientInfo;
    
EXCEPTION
    WHEN NO_DATA_FOUND THEN 
         SELECT 
         JSON_OBJECT(
                 'ClientType' is 'Юридическое лицо',
                 'EmployerStatus' is '0'
                 RETURNING CLOB)  into ClientInfo from dual;

       return ClientInfo;
    
END;

/*
Функция sql запроса ТКС клиента 
@since RSHB 105
@qtest NO
@return ТКС в json формате
*/
  
FUNCTION get_sql_tca return varchar2   is
 sql_tca varchar2(5000);
 sql_atcor varchar2(5000);
 sql_note_tca varchar2(5000);
 sql_result_tca varchar2(10000);

BEGIN
 
 sql_atcor := 'select t_attrid from dobjatcor_dbt t where t.t_objecttype= '||OBJECTTYPE_SUBCONTRACT||' and t.t_groupid=7 and t.t_object = LPAD (sf1.t_id, 10, ''0'')';
 
 sql_tca :='select case when  ('||sql_atcor||') = 2 and rmp.t_marketid = '||TRADING_PLATFORM_MMVB||' and sf1.t_servkind = '||SERV_KIND_FOND||' then (select t_depoacc from DDL_LIMITPRM_DBT where  t_id = 4)
                        when  ('||sql_atcor||') = 2 and rmp.t_marketid = '||TRADING_PLATFORM_MMVB||' and sf1.t_servkind = '||SERV_KIND_MMVB||' then (select t_depoacc from DDL_LIMITPRM_DBT where  t_id = 2)
                        when  ('||sql_atcor||') = 2 and rmp.t_marketid = '||TRADING_PLATFORM_SPB||'  then (select t_depoacc from DDL_LIMITPRM_DBT where  t_id = 5)
                        when   rmp.t_marketid = '||TRADING_PLATFORM_MMVB||' and sf1.t_servkind = '||SERV_KIND_FOND||' then (select t_depoacc from DDL_LIMITPRM_DBT where  t_id = 6)
                        when   rmp.t_marketid = '||TRADING_PLATFORM_MMVB||' and sf1.t_servkind = '||SERV_KIND_MMVB||' then (select t_depoacc from DDL_LIMITPRM_DBT where  t_id = 8)
                        when   rmp.t_marketid = '||TRADING_PLATFORM_SPB||' then (select t_depoacc from DDL_LIMITPRM_DBT where  t_id = 7)
            end from dual';

 sql_note_tca :='SELECT trim(chr(0) from RSB_STRUCT.getString(C.T_TEXT))  FROM dnotetext_dbt C 
                 WHERE C.T_ID =  
                           ( SELECT MAX(B.T_ID) FROM dnotetext_dbt B 
                             WHERE B.T_OBJECTTYPE = '||OBJECTTYPE_SUBCONTRACT||' 
                             AND B.T_NOTEKIND = CASE WHEN rmp.t_marketid = '||TRADING_PLATFORM_MMVB||' THEN '||CONTRACT_NOTEKIND_DEPO_TCA_MMVB||'
                                      WHEN rmp.t_marketid = '||TRADING_PLATFORM_SPB||'  THEN '||CONTRACT_NOTEKIND_DEPO_TCA_SPB||' end
                             AND B.T_DOCUMENTID = LPAD( TO_CHAR(rmp.t_sfcontrid), 10, ''0'')
                             AND B.T_DATE <= to_date(''01.01.9999'',''dd.mm.yyyy''))';

 sql_result_tca := 'select case when ( select ('||sql_note_tca||') from dual) is null then ('||sql_tca||') end from dual';    
            

 return sql_result_tca;

EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN '';
END;

/*
Функция получения счетов клиента 
@since RSHB 105
@qtest NO
@return SQL запрос
*/

FUNCTION get_sql_accounts return varchar2   is
 list_accounts varchar2(5000);

BEGIN
 list_accounts :='
  SELECT JSON_OBJECT(''TradeAccount'' is 
         json_arrayagg(
         JSON_OBJECT(
                    ''TradeAccountNumber'' is ac.t_account,
                    ''Currency'' is (select fiid.t_ccy from dfininstr_dbt fiid where fiid.t_fiid =a.t_code_currency),
                    ''OpeningDate'' is to_char(a.t_open_date,''dd.mm.yyyy''),
                    ''ClosingDate'' is to_char(a.t_close_date,''dd.mm.yyyy'')
                    RETURNING CLOB) format json RETURNING CLOB                                                                
                    )format json RETURNING CLOB)               
 FROM ddlcontrmp_dbt mp,                            
             dsfcontr_dbt sfcontr                         
          LEFT JOIN dmcaccdoc_dbt ac                 
          ON     ac.t_clientcontrid = sfcontr.t_id        
             AND ac.t_owner = sfcontr.t_partyid           
             AND ac.t_iscommon = CHR (88)            
             AND ac.t_catid IN (70)                  
       LEFT JOIN daccount_dbt a                      
       ON     ac.t_chapter = a.t_chapter             
          AND ac.t_currency = a.t_code_currency      
          AND ac.t_account = a.t_account             
 WHERE     sfcontr.t_id = mp.t_sfcontrid                  
       AND sfcontr.T_ID = sf1.t_id                        
       AND (SELECT t_fi_kind                         
              FROM dfininstr_dbt                     
             WHERE t_fiid = a.t_code_currency) = 1';

 return list_accounts;

EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN '';
END;

/*
Функция получения торговых площадок клиента 
@since RSHB 105
@qtest NO
@return SQL запрос
*/

FUNCTION get_sql_trading_platform return varchar2   is
 sql_trading_platform varchar2(10000);
BEGIN
 
 sql_trading_platform :='
  SELECT JSON_OBJECT( ''TradingPlatform'' is 
         json_arrayagg(
         JSON_OBJECT(
                     ''ServiceKind'' is (case when sf1.t_servkind = '||SERV_KIND_FOND||' then ''Фондовый дилинг''
                                              when sf1.t_servkind = '||SERV_KIND_FORTS||' then ''Срочные контракты''
                                              when sf1.t_servkind = '||SERV_KIND_MMVB||' then ''Валютный рынок''
                                              when sf1.t_servkind = '||SERV_KIND_CBO||' then ''СВО''
                                              when sf1.t_servkind = '||SERV_KIND_ACCBILLS||' then ''Учтенные векселя''   end ),
                     ''ServKindSub'' is (case when sf1.t_servkindsub = '||SERV_SUBKIND_STOCK||' then ''Биржевой рынок''
                                               when sf1.t_servkindsub = '||SERV_SUBKIND_STOCK_OVER||' then ''Внебиржевой рынок'' end),
                     ''Exchange'' is (case when rmp.t_marketid = '||TRADING_PLATFORM_MMVB||' then (select t_shortname from dparty_dbt where t_partyid = '||TRADING_PLATFORM_MMVB||')
                                           when rmp.t_marketid = '||TRADING_PLATFORM_SPB||'  then (select t_shortname from dparty_dbt where t_partyid = '||TRADING_PLATFORM_SPB||') end),
                     ''ClientCode'' is decode(rmp.t_mpcode, chr(1), null, rmp.t_mpcode),
                     ''TCA'' is ('||get_sql_tca()||'),                                                            
                     ''TradeAccountList'' is ('||get_sql_accounts()||')
                       RETURNING CLOB)  format json RETURNING CLOB                                                          
                      )RETURNING CLOB)               
  from DSFCONTR_DBT sf1,ddlcontrmp_dbt rmp
  where  rmp.t_sfcontrid = sf1.t_id and rmp.t_dlcontrid = ddl.t_dlcontrid and ((sf1.t_dateclose = to_date(''01.01.0001'',''dd.mm.yyyy'')) or (sfc.t_dateclose = trunc(sysdate)) or (sfc.t_dateclose = RSBSESSIONDATA.CURDATE))';
       
return sql_trading_platform;

EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN '';
END;

/*
Функция получения информации по договору клиента 
@since RSHB 105
@qtest NO
@param p_clientid id клиента 
@return SQL запрос
*/
  
FUNCTION get_sql_contract(p_clientid in number) return varchar2   is
  sql_list_contr varchar2(20000);
  depo_account varchar2(3000);
 
BEGIN

 depo_account := ' 
        SELECT case when  instr( JSON_OBJECT(''DepoAccountInfo'' is ( json_arrayagg(  JSON_OBJECT (''DepoAccount'' is (  (trim(chr(0) from RSB_STRUCT.getString(C.T_TEXT)) ))  )) )) , ''null'')=0    
             then  JSON_OBJECT(''DepoAccountInfo'' is (json_arrayagg( JSON_OBJECT (''DepoAccount'' is ( (trim(chr(0) from RSB_STRUCT.getString(C.T_TEXT)) ))  )format json) ) format json)
            else   JSON_OBJECT(''DepoAccountInfo'' is (json_arrayagg( JSON_OBJECT (''DepoAccount'' is ''''  )) ) ) end    
       FROM dnotetext_dbt C
           WHERE ((C.T_ID =  
            ( SELECT MAX(B.T_ID) FROM dnotetext_dbt B 
              WHERE B.T_OBJECTTYPE = '||OBJECTTYPE_CONTRACT||' 
               AND B.T_NOTEKIND in ('||CONTRACT_NOTEKIND_DEPO_ACCOUNT||')
               AND B.T_DOCUMENTID = LPAD( TO_CHAR(ddl.t_dlcontrid), 34, ''0'') 
               AND B.T_DATE <= to_date(''01.01.9999'',''dd.mm.yyyy''))
  ) or
  (C.T_ID =  
            ( SELECT MAX(B.T_ID) FROM dnotetext_dbt B 
              WHERE B.T_OBJECTTYPE =  '||OBJECTTYPE_CONTRACT||'  
               AND B.T_NOTEKIND in ('||CONTRACT_NOTEKIND_DEPO_TRADEACOOUNT||')
               AND B.T_DOCUMENTID = LPAD( TO_CHAR(ddl.t_dlcontrid), 34, ''0'') 
               AND B.T_DATE <= to_date(''01.01.9999'',''dd.mm.yyyy''))
  ))  ';

 sql_list_contr := '
     SELECT JSON_OBJECT(''Agreement'' is 
            json_arrayagg(
            JSON_OBJECT(''AgreementNumber'' is sfc.t_number, 
                        ''OpeningDate'' is to_char(sfc.t_datebegin,''dd.mm.yyyy''),
                        ''ModificationDate'' is ('||GetNoteTextdate(OBJECTTYPE_CONTRACT,CONTRACT_NOTEKIND_DATE_MODIFICATION)||'),
                        ''ClosingDate'' is to_char(sfc.t_dateclose,''dd.mm.yyyy''), 
                        ''ClientInfo'' is ('''||GetClientInfo(p_clientid)||''') format json,
                        ''AuthorizationInfoList'' is ('||GetAuthorizationInfo_SQL(OBJECTTYPE_CONTRACT,OBJCATEG_TYPE_CONNECT)||'),
                        ''EKK'' is (select RSB_SECUR.SC_GetDlObjCodeOnDate('||OBJECTTYPE_CONTRACT||',1,ddl.t_dlcontrid, trunc(sysdate)-1) from dual),        
                        ''TradingPlatformList'' is ('||get_sql_trading_platform()||' ),
                        ''AgreementType'' is decode(ddl.t_iis, chr(88),1,0),
                        ''Tariff'' is (select sfplan.t_name from DSFCONTRPLAN_DBT contrplan, DSFPLAN_DBT sfplan where contrplan.t_sfplanid = sfplan.t_sfplanid and contrplan.t_sfcontrid = sfc.t_id  and contrplan.t_end = to_date(''01.01.0001'',''dd.mm.yyyy'')),
                        ''TariffStartDate '' is (select to_char(contrplan.t_begin,''dd.mm.yyyy'') from DSFCONTRPLAN_DBT contrplan, DSFPLAN_DBT sfplan where contrplan.t_sfplanid = sfplan.t_sfplanid and contrplan.t_sfcontrid = sfc.t_id  and contrplan.t_end = to_date(''01.01.0001'',''dd.mm.yyyy'')),
                        ''RiskLevel'' is ('||GetValueAttr_sql(OBJECTTYPE_CONTRACT,OBJCATEG_TYPE_RISK)||'),
                        ''DepoNumber'' is ('||GetNoteTextstring(OBJECTTYPE_CONTRACT,CONTRACT_NOTEKIND_DEPO_CONTRACTNUMBER)||'),
                        ''DepoAccountInfoList'' is ('||depo_account||'),
                        ''DepoOpeningDate'' is ('||GetNoteTextdate(OBJECTTYPE_CONTRACT,CONTRACT_NOTEKIND_DEPO_OPENDATE)||') ,
                        ''DepoClosingDate'' is ('||GetNoteTextdate(OBJECTTYPE_CONTRACT,CONTRACT_NOTEKIND_DEPO_CLOSEDATE)||')                                                                   
                       RETURNING CLOB)   RETURNING CLOB
                        )RETURNING CLOB) as json_doc
       FROM DSFCONTR_DBT sfc,  ddlcontr_dbt ddl
       where DDL.T_SFCONTRID = sfc.t_id and
            sfc.t_partyid = '||p_clientid||' and sfc.t_servkind = 0 
            and ((sfc.t_dateclose = to_date(''01.01.0001'',''dd.mm.yyyy'')) or (sfc.t_dateclose = trunc(sysdate))  or (sfc.t_dateclose = RSBSESSIONDATA.CURDATE))';
  
    return sql_list_contr;

EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN '';
END;

/*
Сбор данных по клиенту 
@since RSHB 105
@qtest NO
@param p_Clientid  идентификатор клиента(dparty_dbt.t_partyid)
@param o_ErrorCode возвращаемый код ошибки
@param o_ErrorDesc возвращаемый текст ошибки
*/

PROCEDURE CLIENT_INFO( XML_CDC in varchar2                    
                     ,o_ErrorCode    out number 
                     ,o_ErrorDesc    out varchar2) 

IS
 v_GUID varchar(32);
 ErrorDesc varchar2(3000);
 ErrorCode  integer;
 sql_statement varchar2(30000);
 sql_list clob;
 MSGCode integer;
 MSGText varchar2(1000);
 RegVal integer;
 json_doc clob; 
 XML_TAB varchar(100);
 XML_PK varchar(100);
 Clientid number(10);
 NO_PersonInfo EXCEPTION;
 NO_Document EXCEPTION;   
 NO_ADRESS EXCEPTION;     
 NO_ClientIdList EXCEPTION;
 NO_RegVal EXCEPTION;     
 NO_LOAD EXCEPTION;       
 NO_DATA EXCEPTION;         


BEGIN

 /*рубильник, по умолчанию считаем, что сервис включен*/
 begin
    RegVal:=nvl(RSB_Common.GetRegStrValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ИНТЕГРАЦИЯ КАФКА\ИНТЕГР_СЕРВИС_СВОИ ИНВЕСТИЦИИ'),1);
    IF RegVal = 0 THEN
        RAISE NO_RegVal;

    END IF;
 end;
 
 BEGIN
   IF XML_CDC IS NOT NULL THEN
        SELECT EXTRACTVALUE(xmltype(XML_CDC),  '/XML/@tab') INTO  XML_TAB FROM DUAL;
        
        SELECT EXTRACTVALUE(xmltype(XML_CDC),  '/XML/@pk') INTO  XML_PK FROM DUAL;
        
        IF XML_TAB = 'DACCOUNT_DBT' THEN SELECT T_CLIENT INTO Clientid FROM DACCOUNT_DBT WHERE T_ACCOUNTID = XML_PK;  END IF;
 
        IF XML_TAB = 'DPARTY_DBT' THEN CLIENTID := XML_PK;  END IF;
 
        IF XML_TAB = 'DPERSN_DBT' THEN CLIENTID := XML_PK;  END IF;
        
        IF XML_TAB = 'DPERSNIDC_DBT' THEN CLIENTID := XML_PK;  END IF;
 
        IF XML_TAB = 'DADRESS_DBT' THEN CLIENTID := XML_PK;  END IF;
  
        IF XML_TAB = 'DCONTACT_DBT' THEN SELECT DC.T_PARTYID INTO Clientid FROM DCONTACT_DBT DC WHERE DC.T_RECID = XML_PK;  END IF;

        IF XML_TAB = 'DDLCONTR_DBT' THEN SELECT SF.T_PARTYID  INTO Clientid FROM DSFCONTR_DBT SF WHERE SF.T_ID IN(SELECT DD.T_SFCONTRID FROM DDLCONTR_DBT DD WHERE DD.T_DLCONTRID =XML_PK);  END IF;

        IF XML_TAB = 'DSCQINV_DBT' THEN CLIENTID := XML_PK;  END IF;
 
        IF XML_TAB = 'DSFCONTR_DBT' THEN SELECT SF.T_PARTYID  INTO Clientid FROM DSFCONTR_DBT SF WHERE SF.T_ID  = XML_PK;  END IF;

        IF XML_TAB = 'DSFCONTR_DBT' THEN SELECT SF.T_PARTYID INTO Clientid FROM DSFCONTR_DBT SF WHERE SF.T_ID IN (SELECT DD.T_SFCONTRID FROM DDLCONTRMP_DBT DD WHERE DD.T_ID = XML_PK);  END IF;

        IF XML_TAB = 'DSFCONTR_DBT' THEN SELECT SF.T_PARTYID INTO Clientid FROM DSFCONTR_DBT SF WHERE SF.T_ID IN (SELECT DD.T_SFCONTRID FROM DSFCONTRPLAN_DBT DD WHERE DD.T_ID = XML_PK);  END IF;
        
 END IF;
 END;
 
 /*Получение уникального текстового GUID*/                               
 SELECT CAST(SYS_GUID() AS VARCHAR2(32)) into v_GUID FROM dual;

 /*список корневых полей json*/                      
 sql_list := '
             ''GUID'' is ('''||v_GUID||'''), 
             ''RequestTime'' is (SELECT IT_XML.TIMESTAMP_TO_CHAR_ISO8601(SYSDATE) FROM DUAL),
             ''FullName'' is ('''||GetPersonInfo(Clientid,'FIO')||''') format json ,
             ''Birthday'' is ('''||GetPersonInfo(Clientid,'BORN')||'''),
             ''Gender'' is ('''||GetPersonInfo(Clientid,'ISMAIL')||''') format json,
             ''IdentityDocumentList'' is ('''||GetDocument(Clientid)||''') format json,
             ''ClientAddressList'' is ('''||GetAdress(Clientid)||''')format json,
             ''ClientIdList'' is ('''||GetClientIdList(Clientid)||''') format json,
             ''PhoneList'' is ('''||GetPhoneList(Clientid)||''') format json,
             ''EmailList'' is ('''||GetEmailList(Clientid)||''') format json,
             ''InvestorQualificationInfoList'' is  ('''||GetInvestorQualificationInfoList(Clientid)||''') format json,
             ''AgreementList'' is ('||get_sql_contract(Clientid)||') ';       
                               
 /*основной запрос, json_query для удобства отладки, для работы не нужен*/     
 sql_statement := 
                 'SELECT 
                  --json_query (  
                     replace( 
                      replace(
                        JSON_OBJECT(''CreateUpdateClientInfoReq'' is JSON_OBJECT('||sql_list||' RETURNING CLOB) RETURNING CLOB)
                      ,''null'',''""'')
                     ,''\u0001'',''""'')             
                  --, ''$'' RETURNING CLOB PRETTY )
                    as json_doc
                 FROM dparty_dbt party, dpartyown_dbt part
                 WHERE party.t_partyid= part.t_partyid AND party.t_partyid ='||Clientid||' and part.t_partykind = 1';

 
   BEGIN
      EXECUTE IMMEDIATE sql_statement INTO json_doc;     
      o_ErrorCode := 0;  
   EXCEPTION
       WHEN NO_DATA_FOUND THEN RAISE NO_DATA;
       WHEN OTHERS THEN o_ErrorCode := SQLCODE; o_ErrorDesc := SQLERRM;
   END;


  BEGIN 
     IF o_ErrorCode = 0 THEN
         it_kafka.load_msg(v_GUID,'R','SINV.CreateUpdateClientInfo','SINV', json_doc, ErrorCode, ErrorDesc,NULL,MSGCode, MSGText); 
         IF ErrorCode <> 0 THEN o_ErrorCode := ErrorCode; o_ErrorDesc  := ErrorDesc; END IF;
      END IF;
  END;   
  
  COMMIT;

 EXCEPTION
  WHEN NO_PersonInfo    THEN o_ErrorDesc := 'Ошибка получения данных по клиенту при выгрузке в Свои Инвестиции PersonInfo'; o_ErrorCode :=-1;  
                                                it_event.RegisterError(p_SystemId => 'SINV',p_ServiceName => 'CreateUpdateClientInfo',p_ErrorCode => o_ErrorCode,p_ErrorDesc => o_ErrorDesc,p_LevelInfo => 8);
    WHEN NO_Document  THEN o_ErrorDesc := 'Ошибка получения данных по клиенту при выгрузке в Свои Инвестиции Document';  o_ErrorCode :=-1; 
                                                 it_event.RegisterError(p_SystemId => 'SINV',p_ServiceName => 'CreateUpdateClientInfo',p_ErrorCode => o_ErrorCode,p_ErrorDesc => o_ErrorDesc,p_LevelInfo => 8);
    WHEN NO_ADRESS     THEN o_ErrorDesc := 'Ошибка получения данных по клиенту при выгрузке в Свои Инвестиции adress';  o_ErrorCode :=-1;  
                                                 it_event.RegisterError(p_SystemId => 'SINV',p_ServiceName => 'CreateUpdateClientInfo',p_ErrorCode => o_ErrorCode,p_ErrorDesc => o_ErrorDesc,p_LevelInfo => 8);
    WHEN NO_ClientIdList THEN o_ErrorDesc := 'Ошибка получения данных по клиенту при выгрузке в Свои Инвестиции ClientIdList'; o_ErrorCode :=-1; 
                                                  it_event.RegisterError(p_SystemId => 'SINV',p_ServiceName => 'CreateUpdateClientInfo',p_ErrorCode => o_ErrorCode,p_ErrorDesc => o_ErrorDesc,p_LevelInfo => 8);
    WHEN NO_RegVal       THEN o_ErrorDesc := 'Сервис передачи информации о клиенте в Свои Инвестиции выключен'; o_ErrorCode :=-1;  
                                                  it_event.RegisterError(p_SystemId => 'SINV',p_ServiceName => 'CreateUpdateClientInfo',p_ErrorCode => o_ErrorCode,p_ErrorDesc => o_ErrorDesc,p_LevelInfo => 8);
    WHEN NO_LOAD         THEN o_ErrorDesc := 'Ошибка передачи json сообщения в очередь QManager. Код: '||ErrorDesc||'. '||ErrorDesc;   o_ErrorCode :=-1;    
                                                   it_event.RegisterError(p_SystemId => 'SINV',p_ServiceName => 'CreateUpdateClientInfo',p_ErrorCode => o_ErrorCode,p_ErrorDesc => o_ErrorDesc,p_LevelInfo => 8);                                                                                             
    WHEN NO_DATA         THEN o_ErrorDesc := 'Не найден клиент по заданному partyid = '||Clientid;  o_ErrorCode :=-1;    
                                                   it_event.RegisterError(p_SystemId => 'SINV',p_ServiceName => 'CreateUpdateClientInfo',p_ErrorCode => o_ErrorCode,p_ErrorDesc => o_ErrorDesc,p_LevelInfo => 8);  
                                                               
 END;  
                                                                                                          
END IT_BROKER;
/