create or replace package body rudata_read as

  function get_seccode_mmvb (
    p_isin sofr_info_instruments.isin%type
  ) return sofr_info_instruments.seccode%type deterministic is
    l_seccode sofr_info_instruments.seccode%type;
  begin
    select max(g.seccode) keep (dense_rank last order by to_number(g.id))
      into l_seccode
      from sofr_info_instruments g
     where g.isin = p_isin
       and g.exchange like 'Московская Биржа / МБ%';
  
    return l_seccode;
  exception
    when no_data_found then
      return null;
  end get_seccode_mmvb;
  
  function get_seccode_spb (
    p_isin sofr_info_instruments.isin%type
  ) return sofr_info_instruments.seccode%type deterministic is
    l_seccode sofr_info_instruments.seccode%type;
  begin
    select max(g.seccode) keep (dense_rank last order by to_number(g.id))
      into l_seccode
      from sofr_info_instruments g
      join davoiriss_dbt a on a.t_isin = g.isin
      join dfininstr_dbt f on f.t_fiid = a.t_fiid
      join dparty_dbt p on p.t_partyid = f.t_issuer
     where g.isin = p_isin
       and g.exchange like '%СПБ%'
       and g.exch = case when p.t_nrcountry = 'RUS'
                         then 206
                         else 207
                     end;
  
    return l_seccode;
  exception
    when no_data_found then
      return null;
  end get_seccode_spb;
  
  
  function prepare_temp_table(
    p_FlAll in number, 
    p_Mode in number, 
    p_ISIN_LSIN in varchar2
  ) return varchar2 is
    vguid       varchar2(50);
    vpack_size  number := 1000;
    vfl_all     number := p_FlAll;
    vMode       number := p_Mode;
    vISIN_LSIN  varchar2(255) := p_ISIN_LSIN;
      
    vsql varchar2(32000);
  begin
    vguid := sys_guid();
    
    it_log.log('RUDATA. Заполнение таблицы rudata_securities, guid='||vguid,it_log.C_MSG_TYPE__DEBUG);
    
    vsql := 
    'INSERT INTO rudata_securities 
    ( T_CCY,
      T_AVOIRKIND, T_FI_CODE, T_FI_KIND, T_NAME, T_DEFINITION, 
      T_SETTLEMENT_CODE, T_FACEVALUE, T_DRAWINGDATE, ISINCODE, REGCODE, 
      T_QTY, T_NKDROUND_KIND, T_INCOMETYPE, T_NKDBASE_KIND, T_INCIRCULATIONDATE,
      T_TYPE, ISSUERINN, ISSUERUID, T_ISSUED, T_ENDPLACEMENTDATE,
      T_FIID, FINTOOLID, T_LSIN, T_ISSUERID, CFI, 
      T_ATTR_CODE, T_ISDRAWING, NRDCODE, T_BEGPLACEMENTDATE, FINTOOLTYPE, 
      SECURITYTYPE, COUPONTYPE, COUPON_TYPE, IS_SUBORDINATED, BASE_DR_FI,
      INDEXNOM, HAVEDEFAULT, SECURITYKIND, DRQTY, SHQTY, DRAWINGDATE_FM,
      SESSION_GUID, NUMBER_PACK)
    SELECT 
      NVL( nvl(E.FACEFTNAME_NRD, E.FACEFTNAME), ''RUB'' ) as T_CCY,
      CASE 
          WHEN E.FINTOOLTYPE = ''Облигация'' AND E.SECURITYTYPE = ''Гос'' AND INSTR(E.NICKNAME, ''ОФЗ'') = 1 AND ISSUERNAME = ''Минфин РФ'' THEN 24 
          WHEN E.FINTOOLTYPE = ''Облигация'' AND E.SECURITYTYPE = ''Гос'' AND INSTR(E.NICKNAME, ''КОБР'') = 1 AND ISSUERNAME = ''ЦБ РФ - Банк России'' THEN 25 
          WHEN E.FINTOOLTYPE = ''Облигация'' AND E.SECURITYTYPE = ''Гос'' AND INSTR(E.NICKNAME, ''ОФЗ'') <> 1 AND ( ISSUERNAME = ''Минфин Беларусь'' OR ISSUERNAME = ''Нацбанк Беларуси'' )  THEN 50 
          WHEN E.FINTOOLTYPE = ''Облигация'' AND E.SECURITYTYPE = ''Гос'' AND INSTR(E.NICKNAME, ''ОФЗ'') <> 1 THEN 21 
          WHEN E.FINTOOLTYPE = ''Облигация'' AND E.SECURITYTYPE = ''ЕвроГос'' AND E.BORROWERUID = 7080 THEN 27
          WHEN E.FINTOOLTYPE = ''Облигация'' AND E.SECURITYTYPE = ''ЕвроГос'' AND E.BORROWERUID != 7080 THEN 28 
          WHEN E.FINTOOLTYPE = ''Облигация'' AND E.SECURITYTYPE = ''Корп'' THEN 42 
          WHEN E.FINTOOLTYPE = ''Облигация'' AND E.SECURITYTYPE = ''Муни'' THEN 38 
          WHEN E.FINTOOLTYPE = ''Облигация'' AND E.SECURITYTYPE = ''ЕвроМуни'' THEN 39 
          WHEN E.FINTOOLTYPE = ''Облигация'' AND E.SECURITYTYPE = ''ЕвроКорп'' THEN 43 
          WHEN E.FINTOOLTYPE = ''Облигация'' AND E.COUNTRY <> ''RU'' THEN 50 
          WHEN E.FINTOOLTYPE in (''Акция'', ''Выпуск акции'') AND E.SECURITYTYPE = ''Обыкн-ая'' THEN 1 
          WHEN E.FINTOOLTYPE in (''Акция'', ''Выпуск акции'') AND E.SECURITYTYPE = ''Привилег-ая'' THEN 2 
          WHEN E.FINTOOLTYPE = ''Депозитарная расписка'' AND E.SECURITYTYPE = ''GDR'' THEN 46 /*глобальная*/ 
          WHEN E.FINTOOLTYPE = ''Депозитарная расписка'' AND E.SECURITYTYPE = ''ADR'' THEN 45 /*американская*/ 
          WHEN E.FINTOOLTYPE = ''Депозитарная расписка'' AND E.SECURITYTYPE = ''RDR'' THEN 47 /*российская*/ 
          WHEN E.FINTOOLTYPE = ''Депозитарная расписка'' AND E.SECURITYTYPE = ''SDR'' THEN 49 /*европейская*/ 
          WHEN E.FINTOOLTYPE = ''Депозитарная расписка'' THEN 10  /*все остальные ДР*/ 
          WHEN E.FINTOOLTYPE = ''Фонд'' THEN 16 
          WHEN E.FINTOOLTYPE = ''Ипотечный сертификат'' THEN 35 
          ELSE 0 
      END AS T_AVOIRKIND, 
      E.FINTOOLID AS T_FI_CODE, 
      2 AS T_FI_KIND, 
      NVL( E.NICKNAME, '' '' ) AS T_NAME, 
      NVL( E.FULLNAME, '' '' ) AS T_DEFINITION, 
      CASE 
        WHEN INSTR( UPPER(E.FULLNAME), ''ДОКУМЕНТАРН'' ) > 0 THEN 1 
        ELSE 0 
      END AS T_SETTLEMENT_CODE,  
      NVL( E.FACEVALUE, 0 ) AS T_FACEVALUE, 
      NVL( E.ENDMTYDATE, TO_DATE(''01010001'', ''ddmmyyyy'') ) AS T_DRAWINGDATE, 
      E.ISINCODE, 
      E.REGCODE, 
      NVL( E.SUMISSUEVOL, 0 ) AS T_QTY, 
      1 AS T_NKDROUND_KIND, 
      CASE 
        WHEN E.COUPONTYPE = ''Дисконт'' THEN 2 
        ELSE 1 
      END AS T_INCOMETYPE, 
      CASE E.BASIS
        WHEN ''act/365'' THEN 0 
        WHEN ''30/360'' THEN 1 
        WHEN ''30E/360'' THEN 9 
        WHEN ''act/act'' THEN 4 
        WHEN ''act/act ISDA'' THEN 4 
        WHEN ''act/360'' THEN 2 
        WHEN ''act/364'' THEN 8 
        WHEN ''30/365'' THEN 3 
        WHEN ''30/act'' THEN 5 
        ELSE 1 
      END AS T_NKDBASE_KIND, 
      NVL( E.BEGDISTDATE, TO_DATE(''01010001'', ''ddmmyyyy'') ) AS T_INCIRCULATIONDATE, 
      CASE E.SECURITYTYPE 
        WHEN ''ETF'' THEN 3 
        WHEN ''Закрытый'' THEN 2 
        WHEN ''Открытый'' THEN 1 
        WHEN ''Интервальный'' THEN 0 
        ELSE 0 
      END AS T_TYPE, 
      E.ISSUERINN, 
      E.ISSUERUID, 
      NVL(E.REGDATE, TO_DATE(''01010001'', ''ddmmyyyy'')) AS T_ISSUED, 
      NVL(E.ENDDISTDATE, TO_DATE(''01010001'', ''ddmmyyyy'')) AS T_ENDPLACEMENTDATE, 
      B.T_FIID,
      E.FINTOOLID, 
      A.T_LSIN, 
      B.T_ISSUER AS T_ISSUERID, 
      E.CFI, 
      E.SEC_TYPE_BR_CODE AS T_ATTR_CODE, 
      CASE 
        WHEN E.STATUS = ''Погашен'' THEN 1 
        ELSE 0 
      END AS T_ISDRAWING, 
      E.NRDCODE,
      NVL(E.BEGDISTDATE, TO_DATE(''01010001'', ''ddmmyyyy'')) AS T_BEGPLACEMENTDATE, 
      E.FINTOOLTYPE, 
      E.SECURITYTYPE, 
      CASE 
        WHEN E.COUPONTYPE = ''Плавающий'' THEN CHR(88) 
        ELSE CHR(0) 
      END AS COUPONTYPE, 
      E.COUPONTYPE AS COUPON_TYPE,
      E.ISSUBORDINATED AS IS_SUBORDINATED,
      E.ISINCODEBASE_NRD AS BASE_DR_FI,
      instr(E.fullname, ''с индексируемым номиналом'') as indexnom,
      NVL( E.HAVEDEFAULT, 0) as HAVEDEFAULT,
      E.SecurityKind,
      E.DRQTY, 
      E.SHQTY,
      b.t_drawingdate drawingdate_fm,
      :vguid,
      floor(rownum/:vpack_size)';
    
    if vfl_all = 0 then /* все ценные бумаги */
      vsql := vsql|| 
      'FROM 
        sofr_info_fintoolreferencedata E, 
        davoiriss_dbt A, 
        dfininstr_dbt B 
      WHERE (E.SecurityKind <> ''Дробная часть'' or E.SecurityKind is null) 
        AND B.T_FIID = A.T_FIID 
        AND B.T_FI_KIND = 2 
        AND E.ISINCODE = A.T_ISIN 
        AND A.T_ISIN NOT IN( CHR(1), CHR(0)) 
        AND A.T_ISIN IS NOT NULL 
        AND E.ISINCODE IS NOT NULL  
        AND E.ISINCODE NOT IN( CHR(1), CHR(0))';
      execute immediate vsql using vguid, vpack_size;
    else /* загрузка по ISIN/LSIN */
      if vMode = 0 then /* обновление ценной бумаги */
        vsql := vsql|| 
        'FROM 
          sofr_info_fintoolreferencedata E,
          davoiriss_dbt A, 
          dfininstr_dbt B 
        WHERE (E.SecurityKind <> ''Дробная часть'' or E.SecurityKind is null) 
          AND ( (E.ISINCODE IS NOT NULL AND E.ISINCODE = :vISIN_LSIN)
                  OR 
                (E.REGCODE IS NOT NULL AND E.REGCODE = :vISIN_LSIN) 
              )
          AND B.T_FIID = A.T_FIID 
          AND B.T_FI_KIND = 2
          AND E.ISINCODE = A.T_ISIN';
        execute immediate vsql using vguid, vpack_size, vISIN_LSIN, vISIN_LSIN;
      else /* вставка */
        vsql := vsql|| 
        'FROM 
          sofr_info_fintoolreferencedata E
          left join davoiriss_dbt A on E.ISINCODE = A.T_ISIN 
          left join dfininstr_dbt B on B.T_FIID = A.T_FIID AND B.T_FI_KIND = 2
        WHERE (E.SecurityKind <> ''Дробная часть'' or E.SecurityKind is null) 
          AND ( (E.ISINCODE IS NOT NULL AND E.ISINCODE = :vISIN_LSIN)
                  OR 
                (E.REGCODE IS NOT NULL AND E.REGCODE = :vISIN_LSIN) 
              )';
        execute immediate vsql using vguid, vpack_size, vISIN_LSIN, vISIN_LSIN;
      end if;
    end if;
      
    commit;
     
    it_log.log('RUDATA. Заполнение таблицы rudata_securities - конец',it_log.C_MSG_TYPE__DEBUG);
      
    update rudata_securities r
       set T_FACEVALUEFI = (SELECT NVL(MIN(C.T_FIID),-1) FROM dfininstr_dbt C 
                             WHERE C.T_CCY = r.T_CCY 
                               AND C.T_FI_KIND = 1 
                               AND ROWNUM = 1)
    where r.session_guid = vguid;
    it_log.log('RUDATA. Заполнение таблицы rudata_securities - T_FACEVALUEFI',it_log.C_MSG_TYPE__DEBUG);
    
    update rudata_securities r 
       set T_ISSUER = NVL( (SELECT C.T_OBJECTID FROM dobjcode_dbt C 
                             WHERE C.T_OBJECTTYPE = 3 
                               AND C.T_CODEKIND = 16 
                               AND C.T_STATE = 0 
                               AND ROWNUM = 1 
                               AND C.T_OBJECTID = r.T_ISSUERID 
                               AND EXISTS( SELECT 1 FROM dpartyown_dbt D 
                                            WHERE D.T_PARTYID = C.T_OBJECTID AND D.T_PARTYKIND = 5 ))
                            , 7)
    where r.session_guid = vguid;
    it_log.log('RUDATA. Заполнение таблицы rudata_securities - T_ISSUER',it_log.C_MSG_TYPE__DEBUG);
    
    update rudata_securities r 
       set T_ISSUER = NVL( (SELECT C.T_OBJECTID FROM dobjcode_dbt C 
                             WHERE C.T_OBJECTTYPE = 3 
                               AND C.T_CODEKIND = 16 
                               AND C.T_STATE = 0 
                               AND ROWNUM = 1 
                               AND SUBSTR(C.T_CODE, 1, CASE 
                                                        WHEN INSTR(C.T_CODE, '/') > 0 THEN INSTR(C.T_CODE, '/') - 1 
                                                        ELSE LENGTH(C.T_CODE) 
                                                       END ) =  r.ISSUERINN
                               AND EXISTS( SELECT 1 FROM dpartyown_dbt D 
                                            WHERE D.T_PARTYID = C.T_OBJECTID AND D.T_PARTYKIND = 5 ))
                            , 7)
    where r.session_guid = vguid and T_ISSUER = 7;
    it_log.log('RUDATA. Заполнение таблицы rudata_securities - T_ISSUER 2',it_log.C_MSG_TYPE__DEBUG);

    update rudata_securities r 
       set NOTRESIDENT = (SELECT CASE 
                                   WHEN C.T_NOTRESIDENT = CHR(88) THEN 1 
                                   ELSE 0 
                                 END 
                            FROM dparty_dbt C 
                           WHERE C.T_PARTYID = r.T_ISSUERID )
    where r.session_guid = vguid;
    it_log.log('RUDATA. Заполнение таблицы rudata_securities - NOTRESIDENT',it_log.C_MSG_TYPE__DEBUG);

    update rudata_securities r 
       set (OGRN, TIN, ISSUER_NRD, SWIFT, LEI_CODE, ISSUER_INTERFAX, STATE_REG_NUMBER, KPP) = 
       (SELECT I.OGRN, I.TIN, I.ISSUER_NRD, I.SWIFT, I.LEI_CODE, I.FININSTID, I.STATE_REG_NUMBER, I.KPP 
          FROM sofr_info_emitents I 
         WHERE I.FININSTID = to_char(r.ISSUERUID))
     where r.session_guid = vguid;
     
    it_log.log('RUDATA. Заполнение таблицы rudata_securities - параметры эмитента',it_log.C_MSG_TYPE__DEBUG);
    
    commit;
    
    return vguid;

  end;
  
  -- Обновление эмитентов по данным из SOFR_INFO_EMITENTS
  procedure update_emitents as
    pragma autonomous_transaction;
    v_SystemId    varchar2(200) := 'RUDATA';
    v_ServiceName varchar2(200) := 'Обновление эмитента ';
    v_msg         varchar2(4000);
    v_Set         varchar2(4000);
    v_ErrorDesc   varchar2(2000);
    v_ErrorCode   integer;
    tmp           integer;
    procedure add_logMsg(p_msg itt_log.msg%type) is
    begin
      it_log.log_handle(p_object => 'loadRuData.mac', p_msg => p_msg, p_msg_type => it_log.C_MSG_TYPE__DEBUG);
    end;
  
    procedure add_logErr(p_msg itt_log.msg%type,p_PartyID varchar2, p_id_emitent  varchar2, p_period integer  ) is
    begin
      if p_msg is not null
      then
        if p_period = 0 then
            add_logMsg(p_msg);
        else
          it_event.AddErrorITLogMonitoring(p_SystemId => v_SystemId
                                ,p_ServiceName => v_ServiceName||'#' || p_PartyID || '#' || p_id_emitent 
                                ,p_ErrorCode => 100
                                ,p_ErrorDesc => p_msg
                                ,p_LevelInfo => 3
                                ,p_MsgBODY => 'Ошибка обновления по SOFR_INFO_EMITENTS :' || p_msg
                                ,p_period => p_period);
        end if;
      end if;
    end;
  
    function GetMap(p_Rang       integer
                   ,p_inn        varchar2
                   ,p_fininstid  varchar2
                   ,p_tin        varchar2
                   ,p_issuer_nrd varchar2
                   ,p_lei_code   varchar2
                   ,p_swift      varchar2) return varchar2 is
    begin
      return ' (' || case trunc(p_Rang / 1000) when 0 then 'INN=' || p_inn when 1 then 'FININSTID=' || p_fininstid when 2 then 'TIN=' || p_tin when 3 then 'ISSUER_NRD=' || p_issuer_nrd when 4 then 'LEI_CODE=' || p_lei_code when 5 then 'SWIFT=' || p_swift else 'ОШИБКА' end || ') ';
    end;
  
    function Get_errorMSG(p_PartyID     varchar2
                         ,p_id_emitent  varchar2
                         ,p_shortname   varchar2
                         ,shortname_rus varchar2
                         ,p_Map         varchar2
                         ,p_cnt_party   integer
                         ,p_cnt_em      integer) return varchar2 as
    begin
      return 'PartyID= ' || p_PartyID || ' ID_EMITENT= ' || p_id_emitent || ' ' || case when p_cnt_em > 1 then shortname_rus else p_shortname end || p_Map || ' : В выборке SOFR_INFO_EMITENTS ' || p_cnt_party || ' стр. в DPARTY_DBT ' || p_cnt_em || ' стр. ';
    end;
   
   function GetCHRSelect (p_chr char ) return char as
   begin
    return case when p_chr = chr(0) then ' ' else p_chr end;
   end;   
  begin
    for party in (with prt as
                     (select /*+ materialize */
                     distinct first_value(decode(oc.t_CodeKind, 16, nvl(oc.t_code, chr(1)))) ignore nulls over(partition by t.t_partyid) INN
                             ,first_value(decode(oc.t_CodeKind, 106, nvl(oc.t_code, chr(1)))) ignore nulls over(partition by t.t_partyid) FININSTID
                             ,first_value(decode(oc.t_CodeKind, 62, nvl(oc.t_code, chr(1)))) ignore nulls over(partition by t.t_partyid) TIN
                             ,first_value(decode(oc.t_CodeKind, 8, nvl(oc.t_code, chr(1)))) ignore nulls over(partition by t.t_partyid) ISSUER_NRD
                             ,first_value(decode(oc.t_CodeKind, 69, nvl(oc.t_code, chr(1)))) ignore nulls over(partition by t.t_partyid) LEI_CODE
                             ,first_value(decode(oc.t_CodeKind, 6, nvl(oc.t_code, chr(1)))) ignore nulls over(partition by t.t_partyid) SWIFT
                             ,nvl(t.t_notresident,chr(0)) t_notresident
                             ,nvl(t.t_isresidenceunknown,chr(0)) t_isresidenceunknown
                             ,t.t_ShortName
                             ,t.t_name
                             ,nvl(t.t_NRCountry, chr(1)) t_NRCountry
                             ,nvl(pn.t_name, chr(1)) p_FULLNAME_ENG
                             ,nvl(pns.t_name, chr(1)) p_ShortName
                             ,t.t_partyid
                             ,case when (select count(*) from dpartyown_dbt bnk where bnk.t_PartyKind in (2, 7, 15, 16, 17, 21) -- 2- Банк, 7 - Контрагент по сделкам, 15 - Орган гос.власти, 16 - Орган гос.власти субъекта, 17 - Орган местной власти, 21 - Контрагент по договору
                                                                                    and  bnk.t_PartyID = t.t_partyid and rownum < 2) > 0 
                                        then chr(88) else chr(0) end not_update_name 
                       from dparty_dbt t
                       join dpartyown_dbt partyown
                         on (partyown.t_PartyID = t.t_PartyID and partyown.t_PartyKind = 5 and partyown.t_SubKind = 0)
                       left join dobjcode_dbt oc
                         on (t_objecttype = 3 and t_objectid = t.t_partyid and t_CodeKind in (16, 106, 62, 8, 69, 6))
                       left join DPARTYNAME_VIEW pn
                         on (pn.t_partyid = t.t_partyid and pn.t_nametypeid = 3)
                       left join DPARTYNAME_VIEW pns
                         on (pns.t_partyid = t.t_partyid and pns.t_nametypeid = 2)
                      where t.t_partyid > 10
                        and t.t_locked != chr(88)
                         )
                    select *
                      from (select party.*
                                  ,count(*) over(partition by id_emitent) cnt_em
                                  ,count(*) over(partition by t_partyid) cnt_party
                              from (select party.*
                                          ,min(rang) over(partition by party.t_partyid) as min_rang
                                      from (select /*+ cardinality(em 100000) cardinality(party 1000000)*/
                                             party.*
                                            ,substr(coalesce(em.SHORTNAME_RUS_NRD, em.shortname_rus, chr(1)), 1, 60) SHORTNAME_RUS
                                            ,substr(coalesce(em.fullname_rus_nrd, em.fullname_rus, chr(1)), 1, 320) fullname_rus
                                            ,nvl(em.FULLNAME_ENG_NRD, chr(1)) FULLNAME_ENG_NRD
                                            ,nvl(em.Country, chr(1)) Country
                                            ,em.id_emitent
                                            ,case
                                               when em.inn = party.inn then
                                                0
                                               when em.fininstid = party.fininstid then
                                                1000
                                               when em.tin = party.tin then
                                                2000
                                               when em.issuer_nrd = party.issuer_nrd then
                                                3000
                                               when em.lei_code = party.lei_code then
                                                4000
                                               when em.fswift = party.swift then
                                                5000
                                               else
                                                null
                                             end + decode(em.fininstid, party.fininstid, 0, 1) --
                                             +decode(em.tin, party.tin, 0, 2) --
                                             +decode(em.issuer_nrd, party.issuer_nrd, 0, 4) --
                                             +decode(em.lei_code, party.lei_code, 0, 8) --
                                             +decode(em.fswift, party.swift, 0, 16) --
                                             +nvl2(em.state_reg_date, 0, 32) -- 
                                             +nvl2(em.inn, 0, 64) + nvl2(em.tin, 0, 128) -- 
                                             +nvl2(em.issuer_nrd, 0, 256) as rang
                                              from (select case
                                                             when length(trim(em.swift)) = 8 then
                                                              trim(em.swift) || 'XXX'
                                                             else
                                                              trim(em.swift)
                                                           end as fswift
                                                          ,em.*
                                                      from SOFR_INFO_EMITENTS em) em
                                              join prt party
                                                on (em.inn = party.inn or em.fininstid = party.fininstid or em.tin = party.tin or em.issuer_nrd = party.issuer_nrd or
                                                   em.lei_code = party.lei_code or em.fswift = party.swift)) party) party
                             where rang = min_rang)
                     where cnt_em > 1
                        or cnt_party > 1
                        or T_NRCOUNTRY != COUNTRY
                        or ( t_isresidenceunknown != decode( COUNTRY,chr(1),chr(88), chr(0)))
                        or ( t_notresident != decode( COUNTRY,'RUS', chr(0),chr(88)))
                        or (T_SHORTNAME != SHORTNAME_RUS and not_update_name = chr(0))
                        or (t_name != fullname_rus and fullname_rus != chr(1) and not_update_name = chr(0) )
                        or (p_FULLNAME_ENG != FULLNAME_ENG_NRD and FULLNAME_ENG_NRD != chr(1) and not_update_name = chr(0) )
                        or (p_SHORTNAME != SHORTNAME_RUS and p_SHORTNAME != chr(1) and not_update_name = chr(0) )
                     order by case
                                when cnt_em > 1
                                     or cnt_party > 1 then
                                 0
                                else
                                 1
                              end
                             ,case
                                when cnt_em > 1 then
                                 id_emitent
                                else
                                 to_char(t_partyid)
                              end
                             ,to_char(t_partyid))
    loop
      if party.cnt_em > 1
         or party.cnt_party > 1
      then
        add_logErr(Get_errorMSG(party.t_partyid
                               ,party.id_emitent
                               ,party.T_SHORTNAME
                               ,party.SHORTNAME_RUS
                               ,GetMap(party.rang, party.inn, party.fininstid, party.tin, party.issuer_nrd, party.lei_code, party.swift)
                               ,party.cnt_party
                               ,party.cnt_em), party.t_partyid, party.id_emitent,60*60*4 -- раз в 4ч
                               );
        continue;
      end if;
      v_msg := null;
      v_SET := null;
      begin
        select p.t_partyid into tmp from dparty_dbt p where p.t_partyid = party.t_partyid for update nowait;
      exception
        when others then
          add_logErr('PartyID= ' || party.t_partyid || ' ' || party.t_shortname ||
                     GetMap(party.rang, party.inn, party.fininstid, party.tin, party.issuer_nrd, party.lei_code, party.swift) || ': Блокировка другим процессом в системе ', party.t_partyid, party.id_emitent,0);
          rollback;
          continue;
      end;
      -- COUNTRY
      if party.T_NRCOUNTRY != party.COUNTRY and party.COUNTRY != chr(1)
      then
        v_SET := v_SET || ', p.T_NRCOUNTRY = $$COUNTRY';
        v_msg := v_msg || chr(10) || ' ' || 'T_NRCOUNTRY [' || party.T_NRCOUNTRY || ']=>[' || party.COUNTRY || ']';
      end if;
      -- T_ISRESIDENCEUNKNOWN
      if  party.t_isresidenceunknown != case when party.COUNTRY = chr(1) then chr(88) else chr(0) end  
      then
        v_SET := v_SET || ', t_isresidenceunknown =  '||case when party.COUNTRY = chr(1) then 'chr(88) ' else 'chr(0) ' end;
        v_msg := v_msg || chr(10) || ' ' || 'T_ISRESIDENCEUNKNOWN [' || GetCHRSelect(party.t_isresidenceunknown) || ']=>[' || GetCHRSelect(case when party.COUNTRY = chr(1) then chr(88) else chr(0) end) || ']';
      end if;
      -- T_NOTRESIDENT
      if party.t_notresident != case when party.COUNTRY = 'RUS' then  chr(0) else chr(88) end 
      then
        v_SET := v_SET || ', t_notresident = '||case when party.COUNTRY = 'RUS' then 'chr(0) ' else 'chr(88) ' end;
        v_msg := v_msg || chr(10) || ' ' || 'T_NOTRESIDENT [' || GetCHRSelect(party.t_notresident) || ']=>[' || GetCHRSelect(case when party.COUNTRY = 'RUS' then chr(0) else chr(88)  end)|| ']';
      end if;        
      -- SHORTNAME_RUS
      if party.T_SHORTNAME != party.SHORTNAME_RUS and party.SHORTNAME_RUS != chr(1) and party.not_update_name = chr(0) 
      then
        v_SET := v_SET || ', p.t_shortname = $$SHORTNAME_RUS';
        v_msg := v_msg || chr(10) || ' ' || 'T_SHORTNAME [' || party.T_SHORTNAME || ']=>[' || party.SHORTNAME_RUS || ']';
      end if;
      -- fullname_rus
      if party.t_name != party.fullname_rus
         and party.fullname_rus != chr(1)  and party.not_update_name = chr(0) 
      then
        v_SET := v_SET || ', p.t_name = $$FULLNAME_RUS';
        v_msg := v_msg || chr(10) || ' ' || 'T_NAME [' || party.t_name || ']=>[' || party.fullname_rus || ']';
      end if;
      -- FULLNAME_ENG_NRD
      if party.p_FULLNAME_ENG != party.FULLNAME_ENG_NRD
         and party.FULLNAME_ENG_NRD != chr(1)  and party.not_update_name = chr(0) 
      then
        begin
          select pn.t_partynameid into tmp from dPartyName_dbt pn
           where pn.t_partynameid = (select min(t_partynameid) from dPartyName_dbt where t_partyid = party.t_partyid and t_nametypeid = 3) for update nowait;
        exception
          when no_data_found then
            null;
          when others then
            add_logErr('PartyID= ' || party.t_partyid || ' ' || party.t_shortname ||
                       GetMap(party.rang, party.inn, party.fininstid, party.tin, party.issuer_nrd, party.lei_code, party.swift) || ': Блокировка другим процессом в системе ', party.t_partyid, party.id_emitent,0);
            rollback;
            continue;
        end;
        update dPartyName_dbt pn
           set pn.T_NAME = party.FULLNAME_ENG_NRD
         where pn.t_partynameid = (select min(t_partynameid) from dPartyName_dbt where t_partyid = party.t_partyid and t_nametypeid = 3) ;
        if sql%rowcount = 0
        then
          insert into dPartyName_dbt
            (t_Partyid
            ,t_Name
            ,t_Nametypeid)
          values
            (party.t_partyid
            ,party.FULLNAME_ENG_NRD
            ,3);
        end if;
        v_msg := v_msg || chr(10) || ' ' || 'FULLNAME_ENG [' || party.p_FULLNAME_ENG || ']=>[' || party.FULLNAME_ENG_NRD || ']';
      end if;
      -- SHORTNAME
      if party.p_SHORTNAME != party.SHORTNAME_RUS and party.p_SHORTNAME != chr(1) and party.not_update_name = chr(0) 
      then
        begin
          select pn.t_partynameid into tmp from dPartyName_dbt pn
           where pn.t_partynameid = (select min(t_partynameid) from dPartyName_dbt where t_partyid = party.t_partyid and t_nametypeid = 2) for update nowait;
        exception
          when no_data_found then
            null;
          when others then
            add_logErr('PartyID= ' || party.t_partyid || ' ' || party.t_shortname ||
                       GetMap(party.rang, party.inn, party.fininstid, party.tin, party.issuer_nrd, party.lei_code, party.swift) || ': Блокировка другим процессом в системе ', party.t_partyid, party.id_emitent,0);
            rollback;
            continue;
        end;
        update dPartyName_dbt pn
           set pn.T_NAME = party.SHORTNAME_RUS
         where pn.t_partynameid = (select min(t_partynameid) from dPartyName_dbt where t_partyid = party.t_partyid and t_nametypeid = 2) ;
        if sql%rowcount = 0
        then
          insert into dPartyName_dbt
            (t_Partyid
            ,t_Name
            ,t_Nametypeid)
          values
            (party.t_partyid
            ,party.SHORTNAME_RUS
            ,2);
        end if;
        v_msg := v_msg || chr(10) || ' ' || 'SHORTNAME(2) [' || party.p_SHORTNAME || ']=>[' || party.SHORTNAME_RUS || ']';
      end if;
      if v_SET is not null
      then
        v_SET := replace(replace(replace(v_SET, '$$COUNTRY', it_parallel_exec.Str_to_sql(party.COUNTRY)), '$$SHORTNAME_RUS', it_parallel_exec.Str_to_sql(party.SHORTNAME_RUS))
                        ,'$$FULLNAME_RUS'
                        ,it_parallel_exec.Str_to_sql(party.fullname_rus));
        v_SET := 'update dparty_dbt p set ' || ltrim(trim(v_SET), ',') || ' where  p.t_partyid = :partyid';
        execute immediate v_SET
          using party.t_partyid;
      end if;
      if v_msg is not null
      then
        add_logMsg('PartyID= ' || party.t_partyid || ' ' || party.t_shortname ||
                   GetMap(party.rang, party.inn, party.fininstid, party.tin, party.issuer_nrd, party.lei_code, party.swift) || ':   Изменение ' || v_msg);
        v_msg := null;
      end if;
      commit;
    end loop;
  exception
    when others then
      rollback;
      v_ErrorCode := abs(sqlcode);
      v_ErrorDesc := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
      it_event.AddErrorITLogMonitoring(p_SystemId => v_SystemId
                            ,p_ServiceName => v_ServiceName
                            ,p_ErrorCode => v_ErrorCode
                            ,p_ErrorDesc => v_ErrorDesc
                            ,p_LevelInfo => 8
                            ,p_backtrace => sys.dbms_utility.format_error_backtrace
                            ,p_MsgBODY => 'Ошибка обновления эмитентов по SOFR_INFO_EMITENTS :' || v_ErrorDesc);
  end;


-- Обновление ценных бумаг по данным из SOFR_INFO_FINTOOLREFERENCEDATA
procedure update_fininstr as
  pragma autonomous_transaction;
  v_SystemId    varchar2(200) := 'RUDATA';
  v_ServiceName varchar2(200) := 'Обновление ценной бумаги ';
  c_objecttype      constant dObjAtcor_dbt.t_Objecttype%type := 12;
  c_5_check_date    constant date := trunc(sysdate);
  c_5_update_date   constant date := date '2025-01-01';
  c_5_groupid       constant dObjAtcor_dbt.t_Groupid%type := 5;
  c_5_attrid_OTHERS constant integer := 28;
  v_msg           varchar2(4000);
  v_errmsg        varchar2(4000);
  v_last_fr_count integer := -1;
  v_ErrorDesc     varchar2(2000);
  v_ErrorCode     integer;
  function GetPrefMsg(p_FIID     dfininstr_dbt.t_fiid%type
                     ,p_isincode varchar2
                     ,p_regcode  varchar2
                     ,p_fi_name  varchar2) return varchar2 is
  begin
    return 'FIID= ' || p_FIID || case when p_isincode is not null then ' ISIN=[' || p_isincode || '] ' else ' LSIN=[' || p_regcode || '] ' end || p_fi_name || ' : ';
  end;

  procedure add_logMsg(p_FIID     dfininstr_dbt.t_fiid%type
                      ,p_isincode varchar2
                      ,p_regcode  varchar2
                      ,p_fi_name  varchar2
                      ,p_msg      itt_log.msg%type) is
  begin
    it_log.log_handle(p_object => 'loadRuData.mac', p_msg => GetPrefMsg(p_FIID, p_isincode, p_regcode, p_fi_name) || p_msg, p_msg_type => it_log.C_MSG_TYPE__MSG);
  end;

  procedure add_logErr(p_FIID   dfininstr_dbt.t_fiid%type
                      ,p_msg    itt_log.msg%type
                      ,p_period integer default 60 * 60 * 4 -- раз в 4ч
                       ) is
  begin
    if p_FIID > 0
       and p_msg is not null
    then
      it_event.AddErrorITLogMonitoring(p_SystemId => v_SystemId
                                      ,p_ServiceName => v_ServiceName || '#' || p_FIID
                                      ,p_ErrorCode => 100
                                      ,p_ErrorDesc => p_msg
                                      ,p_LevelInfo => 3
                                      ,p_MsgBODY => 'Ошибка обновления по SOFR_INFO_FINTOOLREFERENCEDATA :' || p_msg
                                      ,p_period => p_period);
    end if;
  end;

begin
  for fi in (with fr as
                (select /*+ materialize */
                 fr.*
                ,count(distinct SEC_TYPE_BR_CODE) over(partition by isincode, regcode, AVOIRKIND) fr_count
                  from (select distinct fr.SEC_TYPE_BR_CODE
                                       ,fr.isincode
                                       ,fr.regcode
                                       ,case
                                          when FINTOOLTYPE = 'Облигация'
                                               and SECURITYTYPE = 'Гос'
                                               and INSTR(NICKNAME, 'ОФЗ') = 1
                                               and ISSUERNAME = 'Минфин РФ' then
                                           24
                                          when FINTOOLTYPE = 'Облигация'
                                               and SECURITYTYPE = 'Гос'
                                               and INSTR(NICKNAME, 'КОБР') = 1
                                               and ISSUERNAME = 'ЦБ РФ - Банк России' then
                                           25
                                          when FINTOOLTYPE = 'Облигация'
                                               and SECURITYTYPE = 'Гос'
                                               and INSTR(NICKNAME, 'ОФЗ') <> 1
                                               and (ISSUERNAME = 'Минфин Беларусь' or ISSUERNAME = 'Нацбанк Беларуси') then
                                           50
                                          when FINTOOLTYPE = 'Облигация'
                                               and SECURITYTYPE = 'Гос'
                                               and INSTR(NICKNAME, 'ОФЗ') <> 1 then
                                           21
                                          when FINTOOLTYPE = 'Облигация'
                                               and BORROWERUID = 7080
                                               and SECURITYTYPE = 'ЕвроГос' then
                                           27
                                          when FINTOOLTYPE = 'Облигация'
                                               and SECURITYTYPE = 'ЕвроГос'
                                               and BORROWERUID != 7080 then
                                           28
                                          when FINTOOLTYPE = 'Облигация'
                                               and SECURITYTYPE = 'Корп' then
                                           42
                                          when FINTOOLTYPE = 'Облигация'
                                               and SECURITYTYPE = 'Муни' then
                                           38
                                          when FINTOOLTYPE = 'Облигация'
                                               and SECURITYTYPE = 'ЕвроМуни' then
                                           39
                                          when FINTOOLTYPE = 'Облигация'
                                               and SECURITYTYPE = 'ЕвроКорп' then
                                           43
                                          when FINTOOLTYPE = 'Облигация'
                                               and COUNTRY <> 'RU' then
                                           50
                                          when (FINTOOLTYPE = 'Акция' or FINTOOLTYPE = 'Выпуск акции')
                                               and SECURITYTYPE = 'Обыкн-ая' then
                                           1
                                          when (FINTOOLTYPE = 'Акция' or FINTOOLTYPE = 'Выпуск акции')
                                               and SECURITYTYPE = 'Привилег-ая' then
                                           2
                                          when FINTOOLTYPE = 'Депозитарная расписка'
                                               and SECURITYTYPE = 'GDR' then
                                           46
                                          when FINTOOLTYPE = 'Депозитарная расписка'
                                               and SECURITYTYPE = 'ADR' then
                                           45
                                          when FINTOOLTYPE = 'Депозитарная расписка'
                                               and SECURITYTYPE = 'RDR' then
                                           47
                                          when FINTOOLTYPE = 'Депозитарная расписка'
                                               and SECURITYTYPE = 'SDR' then
                                           49
                                          when FINTOOLTYPE = 'Депозитарная расписка' then
                                           10
                                          when FINTOOLTYPE = 'Фонд' then
                                           16
                                          when FINTOOLTYPE = 'Ипотечный сертификат' then
                                           35
                                          else
                                           0
                                        end as AVOIRKIND --Вид ценной бумаги
                          from SOFR_INFO_FINTOOLREFERENCEDATA fr
                         where trim(fr.SEC_TYPE_BR_CODE) is not null
                           and (trim(fr.isincode) is not null or trim(fr.regcode) is not null)
                           and fr.fintooltype is not null) fr)
               select *
                 from (select fi.*
                             ,count(*) over(partition by fi.t_fiid) fi_count
                         from (select /*+ index(ob DOBJATCOR_DBT_IDX0) */
                                atr.t_name atr724
                               ,fr.SEC_TYPE_BR_CODE
                               ,nvl(fr.fr_count, 1) fr_count
                               ,atrfr.t_attrid fr_attrid
                               ,ob.t_attrid fi_attrid
                               ,fi.t_name fi_name
                               ,nvl(ob.t_validfromdate, c_5_update_date) t_validfromdate
                               ,ob.t_validtodate
                               ,fr.isincode
                               ,fr.regcode
                               ,fi.t_fiid
                               ,case
                                  when av.t_isin = fr.isincode
                                       and av.t_lsin = fr.regcode then
                                   0
                                  when av.t_isin = fr.isincode
                                       and fr.regcode is null
                                       and fr.AVOIRKIND = ak.t_avoirkind then
                                   1
                                  when av.t_isin = fr.isincode
                                       and fr.regcode is null then
                                   2
                                  when av.t_lsin = fr.regcode
                                       and fr.isincode is null then
                                   3
                                  else
                                   null
                                end rang
                               ,min(case
                                      when av.t_isin = fr.isincode
                                           and av.t_lsin = fr.regcode then
                                       0
                                      when av.t_isin = fr.isincode
                                           and fr.regcode is null
                                           and fr.AVOIRKIND = ak.t_avoirkind then
                                       1
                                      when av.t_isin = fr.isincode
                                           and fr.regcode is null then
                                       2
                                      when av.t_lsin = fr.regcode
                                           and fr.isincode is null then
                                       3
                                      else
                                       null
                                    end) over(partition by fi.t_fiid) min_rang
                                 from dfininstr_dbt fi
                                 join davoiriss_dbt av
                                   on av.t_fiid = fi.t_fiid
                                  and av.t_isin != chr(1)
                                 join davrkinds_dbt ak
                                   on fi.t_fi_kind = ak.t_fi_kind
                                  and ak.t_isemissive = chr(88)
                                  and fi.t_avoirkind = ak.t_avoirkind
                                 left join fr
                                   on (av.t_isin = fr.isincode or av.t_lsin = fr.regcode)
                                 left join dObjAtcor_dbt ob
                                   on ob.t_objecttype = c_objecttype
                                  and ob.t_groupid = c_5_groupid
                                  and ob.t_object = lpad(fi.t_fiid, 10, '0')
                                  and ob.t_validfromdate <= c_5_check_date
                                  and ob.t_validtodate >= c_5_check_date
                                 left join dobjattr_dbt atr
                                   on atr.t_objecttype = c_objecttype
                                  and atr.t_groupid = c_5_groupid
                                  and atr.t_attrid = ob.t_attrid
                                 left join dobjattr_dbt atrfr
                                   on atrfr.t_objecttype = c_objecttype
                                  and atrfr.t_groupid = c_5_groupid
                                  and atrfr.t_name = fr.SEC_TYPE_BR_CODE
                                where fi.t_isclosed = chr(0)
                                  and fi.t_fi_kind = 2) fi
                        where rang = min_rang)
                where rang is null
                   or fr_count > 1
                   or fi_count > 1
                   or (nvl(fi_attrid, -1) != c_5_attrid_OTHERS and decode(fr_attrid, fi_attrid, 1, 0) = 0)
                   or (fi_attrid = c_5_attrid_OTHERS and nvl(fr_attrid,c_5_attrid_OTHERS) != c_5_attrid_OTHERS)
                order by case
                           when fr_count > 1
                                or fi_count > 1 then
                            0
                           else
                            1
                         end
                        ,t_fiid)
  loop
    v_msg := chr(10) || case when fi.rang is not null then 'R'||fi.rang else 'нет в RUDATA' end || ':[' || fi.atr724 || ']=>[' || nvl(fi.sec_type_br_code, 'OTHERS') || ']';
    if fi.fr_count > 1
       or fi.fi_count > 1
    then
      if v_last_fr_count != fi.t_fiid
      then
        add_logErr(v_last_fr_count, v_errmsg);
        v_errmsg        := GetPrefMsg(fi.t_fiid, fi.isincode, fi.regcode, fi.fi_name) || 'В выборке в DFININSTR_DBT ' || fi.fi_count || 'стр. в SOFR_INFO_FINTOOLREFERENCEDATA ' ||
                           fi.fr_count || ' стр.';
        v_last_fr_count := fi.t_fiid;
      end if;
      v_errmsg := v_errmsg || v_msg;
      continue;
    end if;
    add_logErr(v_last_fr_count, v_errmsg);
    v_errmsg        := null;
    v_last_fr_count := -1;
    begin
      categ_utils.save_categ(p_object_type => c_objecttype
                            ,p_group_id => c_5_groupid
                            ,p_object => lpad(fi.t_fiid, 10, '0')
                            ,p_attr_id => nvl(fi.fr_attrid, c_5_attrid_OTHERS)
                            ,p_date => case
                                         when fi.t_validfromdate > c_5_update_date then
                                          fi.t_validfromdate
                                         else
                                          c_5_update_date
                                       end);
    exception
      when others then
        add_logErr(fi.t_fiid, GetPrefMsg(fi.t_fiid, fi.isincode, fi.regcode, fi.fi_name) || ' Ошибка ! ' || sqlerrm || chr(10) || v_msg);
        continue;
    end;
    add_logMsg(fi.t_fiid, fi.isincode, fi.regcode, fi.fi_name, v_msg);
    commit;
  end loop;
  add_logErr(v_last_fr_count, v_errmsg);
exception
  when others then
    rollback;
    v_ErrorCode := abs(sqlcode);
    v_ErrorDesc := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
    it_event.AddErrorITLogMonitoring(p_SystemId => v_SystemId
                                    ,p_ServiceName => v_ServiceName
                                    ,p_ErrorCode => v_ErrorCode
                                    ,p_ErrorDesc => v_ErrorDesc
                                    ,p_LevelInfo => 8
                                    ,p_backtrace => sys.dbms_utility.format_error_backtrace
                                    ,p_MsgBODY => 'Ошибка ценных бумаг по SOFR_INFO_FINTOOLREFERENCEDATA :' || v_ErrorDesc);
end;


end rudata_read;
/
