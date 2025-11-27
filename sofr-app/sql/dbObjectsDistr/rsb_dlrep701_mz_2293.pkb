CREATE OR REPLACE PACKAGE BODY rsb_dlrep701_mz_2293
IS
 
  FUNCTION GetContractorID( p_IsExchange IN NUMBER, p_PARTYID IN NUMBER, p_MARKETID IN NUMBER, p_BROKERID IN NUMBER ) RETURN NUMBER
  IS
  BEGIN
     return (case when p_IsExchange = 1 and p_PARTYID > 0 then p_PARTYID
                  when p_IsExchange = 1 then p_MARKETID
                  when p_MARKETID > 0 and p_BROKERID > 0 then p_BROKERID
                  else p_PARTYID end);
  END GetContractorID;

  PROCEDURE GetContragentSWIFTInfo( p_ContractorID IN NUMBER, p_BANKCONTRAGENT OUT VARCHAR2, p_IsSWIFTCode OUT CHAR )
  IS
  BEGIN
     p_BANKCONTRAGENT := chr(1);
     p_IsSWIFTCode := chr(0);

     begin
      select nvl(trim(chr(0) from cast(rsb_struct.getString(rsi_rsb_kernel.GetNote(cnst.OBJTYPE_PARTY,
                                                                                   LPAD(p_ContractorID, 10, '0'),
                                                                                   PARTY_NOTE_KIND_SWIFT_CODE_FOR_701,
                                                                                   to_date('31.12.9999','DD.MM.YYYY')
                                                                                  )
                                                           ) as varchar2(1023)
                                      )
                 ),chr(1)) into p_BANKCONTRAGENT
        from dual;
     EXCEPTION WHEN NO_DATA_FOUND THEN p_BANKCONTRAGENT := chr(1);
     end;

     if( p_BANKCONTRAGENT = chr(1) )then
       begin
        SELECT t_Code into p_BANKCONTRAGENT
          FROM dobjcode_dbt
         WHERE t_ObjectID = p_ContractorID
           AND t_ObjectType = cnst.OBJTYPE_PARTY
           AND t_CodeKind = cnst.PTCK_SWIFT
           AND ROWNUM = 1;
       EXCEPTION WHEN NO_DATA_FOUND THEN p_BANKCONTRAGENT := chr(1);
       end;
     end if;

     if( p_BANKCONTRAGENT <> chr(1) )then
        p_IsSWIFTCode := chr(88);
     else
       begin
        select t_Name||NVL((select ' '||T_CodeNum3 from DCOUNTRY_DBT where t_Codelat3 = T_NRCOUNTRY and rownum = 1),chr(1)) into p_BANKCONTRAGENT
              from dparty_dbt
             where t_partyid = p_ContractorID;
       EXCEPTION WHEN NO_DATA_FOUND THEN p_BANKCONTRAGENT := chr(1);
       end;
     end if;

  END GetContragentSWIFTInfo;

  PROCEDURE GetBankContragentInfo( p_ContractorID IN NUMBER, p_ISRESIDENT IN CHAR, p_IsREPO IN NUMBER, p_Date IN DATE, p_BANKCONTRAGENT OUT VARCHAR2, p_IsSWIFTCode OUT CHAR, p_TypeContr OUT NUMBER )
  IS
     v_partyKind VARCHAR2(1024) := chr(1);
     v_Category NUMBER := 0;
  BEGIN
     p_BANKCONTRAGENT := chr(1);
     begin
       SELECT LISTAGG(t_partykind, ',') WITHIN GROUP (ORDER BY t_partykind) into v_partykind
         FROM dpartyown_dbt
        WHERE t_partyid = p_ContractorID;
     EXCEPTION WHEN NO_DATA_FOUND THEN v_partykind := chr(1);
     end;
     v_partykind := ','||v_partykind||',';
     begin
       SELECT Attr.t_NumInList into v_Category
         FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
        WHERE AtCor.t_ObjectType = cnst.OBJTYPE_PARTY
          AND AtCor.t_GroupID    = 1 /*Субъекты эк.деят-ти по 302-П*/
          AND AtCor.t_Object     = LPAD(p_ContractorID, 10, '0')
          AND Attr.t_AttrID      = AtCor.t_AttrID
          AND Attr.t_ObjectType  = AtCor.t_ObjectType
          AND Attr.t_GroupID     = AtCor.t_GroupID;
     EXCEPTION WHEN NO_DATA_FOUND THEN v_Category := 0;
     end;
     if instr(v_partykind,',2,') != 0 then
        if instr(v_partykind,',29,') != 0 then
           p_BANKCONTRAGENT := 'CBRF';
           p_TypeContr := 1;
        elsif instr(v_partykind,',8,') != 0 then /*Принадлежность <Пенсионный фонд>*/
           p_BANKCONTRAGENT := 'PF';
           p_TypeContr := 5;
        elsif ((instr(v_partykind,',78,') != 0) or (instr(v_partykind,',79,') != 0) or (instr(v_partykind,',80,') != 0)) then /*Принадлежность <НПФ> или <Фонд пенсионных резервов НПФ> или <Фонд имущ. обесп. УД/СС НПФ>*/
           p_BANKCONTRAGENT := 'NPF';
           p_TypeContr := 11;
        elsif v_Category = 2 then /*Категория <:302-П> = <Фин.органы субъектов РФ>*/
           p_BANKCONTRAGENT := 'RB';
           p_TypeContr := 7;
        elsif v_Category = 3 then /*Категория <:302-П> = <Гос.внебюдж. фонды>*/
           p_BANKCONTRAGENT := 'NB';
           p_TypeContr := 8;
        elsif ((instr(v_partykind,',17,') != 0) or (instr(v_partykind,',26,') != 0)) then /*Принадлежность <Орган местной власти> или <Местный орган исполнит. власти>*/
           p_BANKCONTRAGENT := 'LB';
           p_TypeContr := 9;
        elsif ((v_Category >= 5) and (v_Category <= 10)) then /*Категория <:302-П> = <Фин.орг.в фед.собственности> или <Ком.орг.в фед.собственности> или <Неком.орг.в фед.собственности> или <Финорг гос.соб (кроме фед.)> или <Коморг гос.соб.(кроме фед.)> или <Некоморг гос.соб (кроме фед.)>*/
           p_BANKCONTRAGENT := 'GC';
           p_TypeContr := 10;
        elsif p_ISRESIDENT = 'R' then
           begin
            SELECT t_Code into p_BANKCONTRAGENT
              FROM ( SELECT t_Code
                       FROM dobjcode_dbt
                      WHERE t_ObjectID = p_ContractorID
                        AND t_ObjectType = cnst.OBJTYPE_PARTY
                        AND t_CodeKind = cnst.PTCK_DEAL
                        AND t_BankDate <= p_Date
                        AND (   t_BankCloseDate = TO_DATE ('01.01.0001', 'dd.mm.yyyy')
                             OR t_BankCloseDate > p_Date )
                   ORDER BY t_BankDate DESC )
             WHERE ROWNUM = 1;
             p_TypeContr := 2;

           EXCEPTION WHEN NO_DATA_FOUND THEN
             begin
              SELECT t_Code into p_BANKCONTRAGENT
                FROM ( SELECT t_Code
                         FROM dobjcode_dbt
                        WHERE t_ObjectID = p_ContractorID
                          AND t_ObjectType = cnst.OBJTYPE_PARTY
                          AND t_CodeKind = cnst.PTCK_BANKREGNUM
                          AND t_BankDate <= p_Date
                          AND (   t_BankCloseDate = TO_DATE ('01.01.0001', 'dd.mm.yyyy')
                               OR t_BankCloseDate > p_Date )
                     ORDER BY t_BankDate DESC )
               WHERE ROWNUM = 1;
               p_TypeContr := 2;
               EXCEPTION WHEN NO_DATA_FOUND THEN p_BANKCONTRAGENT := chr(1);
               end;
             end;
         
        elsif instr(v_partykind,',56,') != 0 then /*Принадлежность <Внешэкономбанк>*/
           p_BANKCONTRAGENT := '964';
           p_TypeContr := 2;
        elsif p_ISRESIDENT != 'R' then
           GetContragentSWIFTInfo(p_ContractorID, p_BANKCONTRAGENT, p_IsSWIFTCode);
           p_TypeContr := 3;
        end if;
     elsif p_IsREPO = 1 then
        if instr(v_partykind,',27,') != 0 then
           p_BANKCONTRAGENT := 'FB';
           p_TypeContr := 6;
        elsif instr(v_partykind,',52,') != 0 then
           GetContragentSWIFTInfo(p_ContractorID, p_BANKCONTRAGENT, p_IsSWIFTCode);
           p_TypeContr := 3;
        end if;
     end if;

  END GetBankContragentInfo;
  
  /*Контрагент - ЮЛ и его принадлежность не "Банк", "Биржа", "Центральный банк", "Международный банк развития", "РЦ Биржи", "Центральный контрагент", "Клиринговая организация" или "Московская биржа" */
  FUNCTION IsNeedGr17_18(p_ContractorID IN NUMBER) RETURN NUMBER RESULT_CACHE
  IS
   v_legalform NUMBER := 0;
   v_partyKind VARCHAR2(1024) := chr(1);
  BEGIN
    begin
       SELECT LISTAGG(t_partykind, ',') WITHIN GROUP (ORDER BY t_partykind) into v_partykind
         FROM dpartyown_dbt
        WHERE t_partyid = p_ContractorID;
        EXCEPTION WHEN NO_DATA_FOUND THEN RETURN 1;
     end;
     begin 
       SELECT t_LegalForm INTO v_legalform FROM DPARTY_DBT WHERE T_PARTYID = p_ContractorID;
       EXCEPTION WHEN NO_DATA_FOUND THEN RETURN 1;
     end;
     v_partykind := ',' || v_partykind || ',';
     if ( (v_legalform = 1 /*ЮЛ*/) and (instr(v_partykind,',2,') = 0) and (instr(v_partykind,',3,') = 0) and (instr(v_partykind,'29,') = 0) and (instr(v_partykind,'32,') = 0) and (instr(v_partykind,'46,') = 0) and (instr(v_partykind,'58,') = 0) and (instr(v_partykind,'63,') = 0) and (instr(v_partykind,'82,') = 0)) then
       RETURN 1;
     end if;
     
     RETURN 0;

  END IsNeedGr17_18;

  FUNCTION GetOTHERInfo(p_ContractorID IN NUMBER, p_IsClient IN CHAR, p_PartyKind IN VARCHAR2, p_COST IN NUMBER, p_COST_FIID IN NUMBER, p_OnDate IN DATE, p_FIID_840 IN NUMBER, p_TYPEADDINFO IN VARCHAR2 DEFAULT chr(1), p_IsOTC IN NUMBER DEFAULT 0) RETURN VARCHAR2
  IS
     v_party DPARTY_DBT%ROWTYPE;
     v_IsCategory NUMBER;
     v_IsCategory_32 NUMBER;
     v_CodeNum3 VARCHAR2(3) := chr(1);
     v_OTHER VARCHAR2(1024) := chr(1);
     v_isEconomSociety NUMBER;
  BEGIN

     begin
        SELECT * into v_party from dparty_dbt WHERE T_PARTYID = p_ContractorID;
     end;

     if p_IsClient != 'X' then

        if p_TYPEADDINFO = 'F3' or p_TYPEADDINFO = 'F5' then
           if( instr(p_PartyKind,',2,') != 0 ) then
              v_OTHER := 'A';
           end if;
        elsif( instr(p_PartyKind,',2,') != 0 ) then
           v_OTHER := chr(1);
        else
           begin
             select NVL( ( select 1
                             from dobjatcor_dbt att
                            where att.t_ObjectType = cnst.OBJTYPE_PARTY
                              and att.t_GroupID = 32
                              and att.t_AttrID = 1
                              and att.t_Object = to_char(p_ContractorID, 'FM0999999999')
                              AND ROWNUM = 1
                         ), 0 ) into v_IsCategory_32
             from dual;
           EXCEPTION WHEN NO_DATA_FOUND THEN v_IsCategory_32 := 0;
           end;

           begin
             select NVL( ( select 1
                             from dobjatcor_dbt att
                            where att.t_ObjectType = cnst.OBJTYPE_PARTY
                              and att.t_GroupID = 4
                              and att.t_AttrID = 3
                              and att.t_Object = to_char(p_ContractorID, 'FM0999999999')
                              AND ROWNUM = 1
                         ), 0 ) into v_isEconomSociety
             from dual;
           EXCEPTION WHEN NO_DATA_FOUND THEN v_isEconomSociety := 0;
           end;

           if instr(p_PartyKind,'69,') != 0 or instr(p_PartyKind,'52,') != 0 then
              v_OTHER := 'F';
              if( v_party.T_NotResident = 'X' )then
                 if( NVL(RSI_RSB_FIInstr.ConvSum(p_COST, p_COST_FIID, p_FIID_840, p_OnDate, 1),0) >= 1000000 )then
                    begin
                       select T_CodeNum3 into v_CodeNum3 from DCOUNTRY_DBT where t_Codelat3 = v_party.T_NRCOUNTRY and rownum = 1;
                    EXCEPTION WHEN NO_DATA_FOUND THEN v_CodeNum3 := '';
                    end;
                    v_OTHER := v_OTHER||v_CodeNum3;
                 end if;
              elsif( v_IsCategory_32 = 1 or instr(p_PartyKind,'34,') != 0 or instr(p_PartyKind,'52,') != 0 )then
                 if (v_isEconomSociety = 1 and instr(p_PartyKind,'52,') != 0 and RSI_RSBPARTY.GetPartyCode(p_ContractorID, 27 /*ОГРН*/) != CHR(1)) then
                    v_OTHER := v_OTHER||'996';
                 else
                 v_OTHER := v_OTHER||'998';
              end if;
              end if;
           elsif v_party.T_LegalForm = 1 and instr(p_PartyKind,'69,') = 0 and instr(p_PartyKind,'52,') = 0 then
              v_OTHER := 'K';
              if( v_party.T_NotResident = 'X' and NVL(RSI_RSB_FIInstr.ConvSum(p_COST, p_COST_FIID, p_FIID_840, p_OnDate, 1),0) >= 1000000 )then
                 begin
                    select T_CodeNum3 into v_CodeNum3 from DCOUNTRY_DBT where t_Codelat3 = v_party.T_NRCOUNTRY and rownum = 1;
                 EXCEPTION WHEN NO_DATA_FOUND THEN v_CodeNum3 := '';
                 end;
                 v_OTHER := v_OTHER||v_CodeNum3;
              end if;
           elsif v_IsCategory_32 = 1 or instr(p_PartyKind,'34,') != 0 then
              v_OTHER := '998';
           elsif v_party.T_LegalForm = 2 then
             if PM_COMMON.GetIsEmployer(p_ContractorID) = 'X' then
                v_OTHER := 'E';
             else
                begin
                  select NVL( ( select 1
                                  from dobjatcor_dbt att
                                 where att.t_ObjectType = cnst.OBJTYPE_PARTY
                                   and att.t_GroupID = PM_COMMON.PARTY_ATTR_TYPE
                                   and att.t_AttrID in ( PM_COMMON.PARTY_AT_NOTARY,
                                                         PM_COMMON.PARTY_AT_LAWYER )
                                   and att.t_Object = to_char(p_ContractorID, 'FM0999999999')
                                   AND ROWNUM = 1
                              ), 0 ) into v_IsCategory
                  from dual;
                EXCEPTION WHEN NO_DATA_FOUND THEN v_IsCategory := 0;
                end;
                if v_IsCategory = 1 then
                  v_OTHER := 'E';
                else
                  v_OTHER := 'I';
                  if( v_party.T_NotResident = 'X' and NVL(RSI_RSB_FIInstr.ConvSum(p_COST, p_COST_FIID, p_FIID_840, p_OnDate, 1),0) >= 1000000 )then
                     begin
                        select T_CodeNum3 into v_CodeNum3 from DCOUNTRY_DBT where t_Codelat3 = v_party.T_NRCOUNTRY and rownum = 1;
                     EXCEPTION WHEN NO_DATA_FOUND THEN v_CodeNum3 := '';
                     end;
                     v_OTHER := v_OTHER||v_CodeNum3;
                  end if;
                end if;
             end if;
           end if;
        end if;
     else
        if instr(p_PartyKind,',2,') != 0 then
           if p_IsOTC = 1 then
              v_OTHER := 'A';
           else
              v_OTHER := 'M';
           end if;
        elsif v_party.T_LegalForm = 2 and PM_COMMON.GetIsEmployer(p_ContractorID) != 'X' then
           begin
             select NVL( ( select 1
                             from dobjatcor_dbt att
                            where att.t_ObjectType = cnst.OBJTYPE_PARTY
                              and att.t_GroupID = PM_COMMON.PARTY_ATTR_TYPE
                              and att.t_AttrID in ( PM_COMMON.PARTY_AT_NOTARY,
                                                    PM_COMMON.PARTY_AT_LAWYER )
                              and att.t_Object = to_char(p_ContractorID, 'FM0999999999')
                              AND ROWNUM = 1
                         ), 0 ) into v_IsCategory
             from dual;
           EXCEPTION WHEN NO_DATA_FOUND THEN v_IsCategory := 0;
           end;
           if v_IsCategory != 1 then
             v_OTHER := 'I';
             if( v_party.T_NotResident = 'X' and NVL(RSI_RSB_FIInstr.ConvSum(p_COST, p_COST_FIID, p_FIID_840, p_OnDate, 1),0) >= 1000000 )then
                begin
                   select T_CodeNum3 into v_CodeNum3 from DCOUNTRY_DBT where t_Codelat3 = v_party.T_NRCOUNTRY and rownum = 1;
                EXCEPTION WHEN NO_DATA_FOUND THEN v_CodeNum3 := '';
                end;
                v_OTHER := v_OTHER||v_CodeNum3;
             end if;
           end if;
        elsif v_party.T_LegalForm = 1 and instr(p_PartyKind,'69,') = 0 and instr(p_PartyKind,'52,') = 0 then
           v_OTHER := 'K';
           if( v_party.T_NotResident = 'X' and NVL(RSI_RSB_FIInstr.ConvSum(p_COST, p_COST_FIID, p_FIID_840, p_OnDate, 1),0) >= 1000000 )then
              begin
                 select T_CodeNum3 into v_CodeNum3 from DCOUNTRY_DBT where t_Codelat3 = v_party.T_NRCOUNTRY and rownum = 1;
              EXCEPTION WHEN NO_DATA_FOUND THEN v_CodeNum3 := '';
              end;
              v_OTHER := v_OTHER||v_CodeNum3;
           end if;
        end if;
     end if;

     return v_OTHER;
  END GetOTHERInfo;

  PROCEDURE GetFloatRate(p_DEALID IN NUMBER, p_TYPE IN NUMBER, p_FIID_BA IN NUMBER, p_FloatRate OUT VARCHAR2, p_RateCCY OUT VARCHAR2, p_RateName OUT VARCHAR2, p_RateSpread OUT FLOAT)
  IS
     v_RateDuration VARCHAR2(3):= chr(1);
  BEGIN
     p_RateCCY    := chr(1);
     p_RateName   := chr(1);
     p_RateSpread := 0;
     begin
        SELECT fin.t_Duration||nvl((select case when fin.t_Name = 'CBR_KeyRate' then 'W'
                                                when ALG.T_INUMBERALG = 1 then 'D'
                                                when ALG.T_INUMBERALG = 2 then 'W'
                                                when ALG.T_INUMBERALG = 3 then 'M'
                                                else 'Y' end
                                    from dnamealg_dbt alg where ALG.T_ITYPEALG = 2350 and ALG.T_INUMBERALG = fin.t_TypeDuration ),''),
             NVL((select f.t_CCY from dfininstr_dbt f where f.t_fiid = fin.t_FaceValueFI and f.t_fiid != p_FIID_BA),''),
             NVL(fin.t_name,''), NVL(NFI.T_SPREAD,0)
        into v_RateDuration, p_RateCCY, p_RateName, p_RateSpread
          from ddvnfi_dbt nfi, dfininstr_dbt fin
         WHERE nfi.T_DEALID = p_DEALID
           and nfi.T_TYPE = p_TYPE
           and fin.t_FIID = nfi.T_RateID;
     EXCEPTION WHEN NO_DATA_FOUND THEN
      p_FloatRate := chr(1);
      return;
     end;

     if( p_RateCCY = chr(1) and p_RateName NOT IN ('UIIR',
                                                   'RUONIA_OIS_COMPOUND',
                                                   'RUB_KEY_RATE_CMP',
                                                   'OINOK',
                                                   'CDOR',
                                                   'KZIBID',
                                                   'CZKONIA',
                                                   'HONIX',
                                                   'SAIBOR',
                                                   'EFFR',
                                                   'RUONIA_AVG',
                                                   'BANKRATE',
                                                   'EURONIA',
                                                   'RIBOR12H',
                                                   'HIBOR',
                                                   'NFEA FX SWAP',
                                                   'KIBID',
                                                   'BUBOR',
                                                   'SWAP_DKK',
                                                   'SHIBOR',
                                                   'OIBOR',
                                                   'VNIBOR',
                                                   'MIACR',
                                                   'KazPrime',
                                                   'CITA',
                                                   'LIBID',
                                                   'KIEVIBOR',
                                                   'AMERIBOR',
                                                   'SIBOR',
                                                   'NOWA',
                                                   'CZEONIA',
                                                   'KIBOR',
                                                   'AONIA',
                                                   'CPI',
                                                   'KIMEAN',
                                                   'HONIA',
                                                   'PRIBOR',
                                                   'RUBID',
                                                   'MIBID',
                                                   'RUSFAR',
                                                   'TONAR',
                                                   'SARON',
                                                   'KIEIBOR',
                                                   'SOFR',
                                                   'MIFOR',
                                                   'RONIA',
                                                   'Tom/Next_DKK',
                                                   'RuRepo',
                                                   'TIBOR',
                                                   'CDI',
                                                   'UONIA',
                                                   'RIBOR18H',
                                                   'NIBOR',
                                                   'TWINA',
                                                   'EONIA',
                                                   'ESTR',
                                                   'LIBOR',
                                                   'TONIA',
                                                   'HUFONIA',
                                                   'CIBOR',
                                                   'RIGIBOR',
                                                   'SITIBOR',
                                                   'LPR',
                                                   'WIBOR',
                                                   'MIACRIG',
                                                   'MOEXRepo',
                                                   'SONIA',
                                                   'ROISFIX',
                                                   'TRYIBOR',
                                                   'KIEVPRIME',
                                                   'POLONIA',
                                                   'CBR_KeyRate',
                                                   'CORRA',
                                                   'EURIBOR',
                                                   'RUONIA',
                                                   'OISUSD_NCC',
                                                   'KAZIBOR',
                                                   'BBSW',
                                                   'TRYIBID',
                                                   'BKBM',
                                                   'MIBOR',
                                                   'FEDOIS',
                                                   'MOSPRIME',
                                                   'STIBOR',
                                                   'EXCHANGE_RATE',
                                                   'RUB_KEY_RATE_AVG')
        ) then 
      p_FloatRate := chr(1);
      return;
     end if;

     if  (instr(UPPER(p_RateName), 'RUONIA') != 0) then
       p_RateName := 'RUONIA';
     end if;

     p_FloatRate := ltrim(v_RateDuration)||'-'||(case when p_RateCCY <> chr(1) then p_RateCCY||'_' else '' end)||p_RateName||ltrim(to_char(p_RateSpread,'S9999990D9999'));

  END GetFloatRate;
  
    PROCEDURE GetPrcFloatRate(p_DEALID IN NUMBER, p_TYPE IN NUMBER, p_FIID IN NUMBER, p_FloatRate OUT VARCHAR2, p_RateCCY OUT VARCHAR2, p_RateName OUT VARCHAR2, p_RateSpread OUT FLOAT)
  IS
     v_RateDuration VARCHAR2(3):= chr(1);
  BEGIN
     p_RateName   := chr(1);
     p_RateSpread := 0;
     begin
        SELECT fin.t_Duration||nvl((select case when fin.t_Name = 'CBR_KeyRate' then 'W'
                                                when ALG.T_INUMBERALG = 1 then 'D'
                                                when ALG.T_INUMBERALG = 2 then 'W'
                                                when ALG.T_INUMBERALG = 3 then 'M'
                                                else 'Y' end
                                    from dnamealg_dbt alg where ALG.T_ITYPEALG = 2350 and ALG.T_INUMBERALG = fin.t_TypeDuration ),''),
             NVL(fin.t_name,''), NVL(NFI.T_SPREAD,0)
        into v_RateDuration, p_RateName, p_RateSpread
          from ddvnfi_dbt nfi, dfininstr_dbt fin
         WHERE nfi.T_DEALID = p_DEALID
           and nfi.T_TYPE = p_TYPE
           and fin.t_FIID = p_FIID;
     EXCEPTION WHEN NO_DATA_FOUND THEN
      p_FloatRate := chr(1);
      return;
     end;

     p_FloatRate := ltrim(v_RateDuration)||'-'||p_RateName||ltrim(to_char(p_RateSpread,'S9999990D9999'));

  END GetPrcFloatRate;

  FUNCTION GetTypeFinActive(p_FIID IN NUMBER, p_IsParent IN NUMBER DEFAULT 0) RETURN VARCHAR2
  IS
     v_TypeFinActive VARCHAR2(2) := chr(1);
  BEGIN
     begin
        select case when RSB_FIInstr.FI_AvrKindsGetRoot( fin.t_FI_KIND, fin.t_AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_BOND then 'F3'
                    when RSB_FIInstr.FI_AvrKindsGetRoot( fin.t_FI_KIND, fin.t_AvoirKind ) in(RSI_RSB_FIInstr.AVOIRKIND_SHARE,RSI_RSB_FIInstr.AVOIRKIND_INVESTMENT_SHARE) then 'F5'
                    when p_IsParent = 0 and RSB_FIInstr.FI_AvrKindsGetRoot( fin.t_FI_KIND, fin.t_AvoirKind ) = RSI_RSB_FIInstr.AVOIRKIND_DEPOSITORY_RECEIPT
                         then GetTypeFinActive(fin.t_ParentFI,1)
                    else chr(1) end into v_TypeFinActive
          from dfininstr_dbt fin
         where fin.t_FIID = p_FIID
           and fin.t_FI_KIND = RSI_RSB_FIInstr.FIKIND_AVOIRISS;
     EXCEPTION WHEN NO_DATA_FOUND THEN v_TypeFinActive := chr(1);
     end;
     return v_TypeFinActive;
  END GetTypeFinActive;

  FUNCTION GetFIIDForIndex(p_FIID IN NUMBER) RETURN NUMBER
  IS
     v_FaceValueFI NUMBER := -1;
  BEGIN
     begin
        select (case when fin.t_FI_KIND = RSI_RSB_FIInstr.FIKIND_INDEX THEN fin.t_facevaluefi ELSE fin.t_FIID END) INTO v_FaceValueFI
          from dfininstr_dbt fin
         where fin.t_FIID = p_FIID;
     EXCEPTION WHEN NO_DATA_FOUND THEN v_FaceValueFI := p_FIID;
     end;
     return v_FaceValueFI;
  END GetFIIDForIndex;

  PROCEDURE PRegNum(p_ContractorID IN NUMBER, p_regNum OUT VARCHAR2)
  IS
  BEGIN
    BEGIN
      SELECT '-ЦК' into p_regNum
      FROM DPARTYOWN_DBT partyOwn
      WHERE partyOwn.T_PARTYID = p_ContractorID
            AND partyOwn.T_PARTYKIND = 58;--Центральный контрагент
      EXCEPTION WHEN NO_DATA_FOUND THEN
        BEGIN
            SELECT '-К' into p_regNum
            FROM DPARTYOWN_DBT partyOwn
            WHERE partyOwn.T_PARTYID = p_ContractorID
                  AND partyOwn.T_PARTYKIND in (62, 42);--Некоммерческая организация
            EXCEPTION WHEN NO_DATA_FOUND THEN p_regNum := chr(1);
        END;
    END;
  END;
  PROCEDURE BICContr(p_ContractorID IN NUMBER, p_BIC OUT VARCHAR2)
  IS
  BEGIN
    SELECT ObjCode.T_CODE into p_BIC
    FROM DOBJCODE_DBT ObjCode
    WHERE ObjCode.T_OBJECTID = p_ContractorID
          AND ObjCode.T_OBJECTTYPE = PM_COMMON.OBJTYPE_PARTY
          AND ObjCode.T_CODEKIND   = PM_COMMON.PTCK_BIC;
    EXCEPTION WHEN NO_DATA_FOUND THEN p_BIC := chr(1);
  END;


  --получить время для "промежуточного" режима отчёта по которое сделки считаются за текущий день
  FUNCTION GetTimeForIntermediate RETURN DATE
  IS
  BEGIN
     return TO_DATE('15:30:00','HH24:MI:SS');
  END GetTimeForIntermediate;

  --проверяем что время меньше либо равно промежуточный по часам и минутам
  FUNCTION TimeIsIntermediate(p_Time IN DATE) RETURN NUMBER
  IS
     v_InterDate DATE := GetTimeForIntermediate();
  BEGIN
     return CASE WHEN TO_CHAR(p_Time,'HH24MI') <= TO_CHAR(v_InterDate,'HH24MI') THEN 1 ELSE 0 END;
  END TimeIsIntermediate;

  PROCEDURE CreateData_REPO(DepartmentID IN NUMBER, OnDate IN DATE, CodeType IN NUMBER, IsIntermediate NUMBER DEFAULT 0)
  IS
     v_DeltaSum NUMBER;
     v_COST_PAYFIID_1 NUMBER;
     v_COST_PAYFIID_2 NUMBER;
     TYPE rep701_t IS TABLE OF DDLREP701_TMP%ROWTYPE;
     g_rep701_ins rep701_t := rep701_t();
     rep701 DDLREP701_TMP%rowtype;
     v_owner_BIC NUMBER;
     v_contrkind VARCHAR2(1024) := chr(1);
  BEGIN
     FOR one_rec IN (with tick as(select TICK.T_DEALID, tick.T_BOFFICEKIND, TICK.T_DEALDATE, TICK.T_PARTYID, TICK.T_CLIENTID, TICK.T_MARKETID, TICK.T_BROKERID,
                                         RSB_SECUR.IsBuy(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) T_IsBuy,
                                         RSB_SECUR.IsExchange(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) T_IsExchange,
                                         RSB_SECUR.IsBroker(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) T_IsBroker,
                                         NVL((SELECT Attr.t_NumInList
                                                FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
                                               WHERE AtCor.t_ObjectType = Rsb_Secur.OBJTYPE_SECDEAL
                                                 AND AtCor.t_GroupID    = 50
                                                 AND AtCor.t_Object     = LPAD(TICK.T_DEALID, 34, '0')
                                                 AND Attr.t_AttrID      = AtCor.t_AttrID
                                                 AND Attr.t_ObjectType  = AtCor.t_ObjectType
                                                 AND Attr.t_GroupID     = AtCor.t_GroupID),0) T_TransCategory,
                                         
                                         -- Golovkin 17.02.2021 ID : 523162 (Без-)адресная сделка
                                         NVL((SELECT Attr.t_NumInList
                                                FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
                                               WHERE AtCor.t_ObjectType = Rsb_Secur.OBJTYPE_SECDEAL
                                                 AND AtCor.t_GroupID    = 105
                                                 AND AtCor.t_Object     = LPAD(TICK.T_DEALID, 34, '0')
                                                 AND Attr.t_AttrID      = AtCor.t_AttrID
                                                 AND Attr.t_ObjectType  = AtCor.t_ObjectType
                                                 AND Attr.t_GroupID     = AtCor.t_GroupID),0) T_AdressCategory,
                                                 
                                         case when TICK.T_MARKETID > 0 or NVL((SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = TICK.T_PARTYID AND T_PARTYKIND = 3),0) > 0
                                              then NVL(( SELECT DECODE(Rsb_Secur.SC_GetObjCodeOnDate(cnst.OBJTYPE_PARTY, cnst.PTCK_CONTR, T_PARTYID),'ММВБ','MMVB', 'ПАО СПБ','SPBEX', t_shortname) 
                                                 FROM DPARTY_DBT
                                                WHERE T_PARTYID = case when TICK.T_MARKETID > 0 then TICK.T_MARKETID
                                                                       when NVL((SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = TICK.T_PARTYID AND T_PARTYKIND = 3),0) > 0 then TICK.T_PARTYID
                                                                       else -1 end),'DO')
                                              when NVL((SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = TICK.T_PARTYID AND T_PARTYKIND IN (27, 58)),0) > 0 --Chesnokov D.S. Сделки с ФК всегда MMVB
                                              then 'MMVB'
                                              else NVL((SELECT case when Attr.t_NumInList = 1 then 'MMVB'
                                                                    when Attr.t_NumInList = 2 then 'RTRS'
                                                                    when Attr.t_NumInList = 3 then 'BBLG'
                                                                    when Attr.t_NumInList = 4 then 'PHONE' end
                                                          FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
                                                         WHERE AtCor.t_ObjectType = Rsb_Secur.OBJTYPE_SECDEAL
                                                           AND AtCor.t_GroupID    = 50
                                                           AND AtCor.t_Object     = LPAD(TICK.T_DEALID, 34, '0')
                                                           AND Attr.t_AttrID      = AtCor.t_AttrID
                                                           AND Attr.t_ObjectType  = AtCor.t_ObjectType
                                                           AND Attr.t_GroupID     = AtCor.t_GroupID),'DO') end T_MarketContractor
                                    from ddl_tick_dbt tick
                                   where TICK.T_BOFFICEKIND = rsb_secur.DL_SECURITYDOC
                                     and TICK.T_DEALSTATUS > CASE WHEN IsIntermediate = 1 THEN -1 ELSE 0 END
                                     AND 1 = CASE WHEN IsIntermediate = 1 THEN TimeIsIntermediate(TICK.T_DEALTIME) ELSE 1 END
                                     and TICK.T_DEALDATE = OnDate
                                     and TICK.T_DEPARTMENT = case when DepartmentID != -1 then DepartmentID else TICK.T_DEPARTMENT end
                                     and ( (CodeType = DL_EXTCODETYPE_OWN    and TICK.T_CLIENTID = -1) or
                                           (CodeType = DL_EXTCODETYPE_CLIENT and TICK.T_CLIENTID != -1)
                                         )
                                     and RSB_SECUR.IsRepo(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) = 1
                                 )
                     select TICK.T_DEALID, tick.T_BOFFICEKIND, TICK.T_DEALDATE, TICK.T_PARTYID, TICK.T_CLIENTID, TICK.T_MARKETID, TICK.T_BROKERID, TICK.T_IsBuy, TICK.T_IsExchange, TICK.T_MarketContractor, TICK.T_IsBroker, TICK.T_TransCategory,
                            LEG_1.T_IncomeRate, LEG_1.T_CFI, LEG_1.T_PAYFIID, LEG_1.T_TOTALCOST T_COST_CFI_1, LEG_2.T_TOTALCOST T_COST_CFI_2,
                            NVL( (select case when rq_cur.T_FACTDATE != to_date('01.01.0001','DD.MM.YYYY') then rq_cur.T_FACTDATE else rq_cur.T_PLANDATE end
                                    from ddlrq_dbt rq_cur
                                   where rq_cur.t_docid = tick.t_dealid
                                     AND rq_cur.t_dockind = tick.T_BOFFICEKIND
                                     AND rq_cur.t_subkind = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY
                                     AND rq_cur.t_type = RSI_DLRQ.DLRQ_TYPE_PAYMENT
                                     AND rq_cur.t_DealPart = 1),to_date('01.01.0001','DD.MM.YYYY')) T_PayDate_1,
                            NVL( (select case when rq_cur.T_FACTDATE != to_date('01.01.0001','DD.MM.YYYY') then rq_cur.T_FACTDATE else rq_cur.T_PLANDATE end
                                    from ddlrq_dbt rq_cur
                                   where rq_cur.t_docid = tick.t_dealid
                                     AND rq_cur.t_dockind = tick.T_BOFFICEKIND
                                     AND rq_cur.t_subkind = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY
                                     AND rq_cur.t_type = RSI_DLRQ.DLRQ_TYPE_PAYMENT
                                     AND rq_cur.t_DealPart = 2),to_date('01.01.0001','DD.MM.YYYY')) T_PayDate_2,
                            GetContractorID(TICK.T_IsExchange, TICK.T_PARTYID, TICK.T_MARKETID, TICK.T_BROKERID) T_ContractorID,
                            NVL((SELECT DECODE(T_NotResident,'X','N','R') from dparty_dbt WHERE T_PARTYID = GetContractorID(TICK.T_IsExchange, TICK.T_PARTYID, TICK.T_MARKETID, TICK.T_BROKERID)),chr(0)) T_ISRESIDENT,
                            NVL(','||(SELECT LISTAGG(t_partykind, ',') WITHIN GROUP (ORDER BY t_partykind)
                                   FROM dpartyown_dbt
                                  WHERE t_partyid = GetContractorID(TICK.T_IsExchange, TICK.T_PARTYID, TICK.T_MARKETID, TICK.T_BROKERID))||',',chr(1)) T_partykind,
                            NVL((select country.T_CodeNum3 from DCOUNTRY_DBT country, DPARTY_DBT party
                                  where country.t_Codelat3 = party.T_NRCOUNTRY
                                    and party.T_PARTYID = GetContractorID(TICK.T_IsExchange, TICK.T_PARTYID, TICK.T_MARKETID, TICK.T_BROKERID)
                                    and party.T_NotResident = 'X' --заполняем только для нерезидентов
                                    and rownum = 1 ),chr(1)) T_OKCMCODE,
                            case when Exists(SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = TICK.T_PARTYID AND T_PARTYKIND in(3,58)) then 1 else 2 end T_IsExchOrCentrContr,
                            NVL((select 1 from DPARTYOWN_DBT
                                  where T_PARTYID = GetContractorID(TICK.T_IsExchange, TICK.T_PARTYID, TICK.T_MARKETID, TICK.T_BROKERID)
                                    and T_PARTYKIND in(2,52) and rownum =1),0) T_ContractorIsBank,
                            NVL((select 1 from DPARTYOWN_DBT
                                  where T_PARTYID = TICK.T_CLIENTID
                                    and T_PARTYKIND = 2),0) T_ClientIsBank,
                            tick.t_AdressCategory -- Golovkin 17.02.2021 ID : 523162 (Без-)адресная сделка
                     from tick, ddl_leg_dbt leg_1, ddl_leg_dbt leg_2
                     where LEG_1.T_DEALID = TICK.T_DEALID
                       and LEG_1.T_LEGKIND = 0
                       and LEG_1.T_LEGID = 0
                       and LEG_2.T_DEALID = TICK.T_DEALID
                       and LEG_2.T_LEGKIND = 2
                       and LEG_2.T_LEGID = 0
                       and NVL((select 1 from DPARTYOWN_DBT
                                 where T_PARTYID = GetContractorID(TICK.T_IsExchange, TICK.T_PARTYID, TICK.T_MARKETID, TICK.T_BROKERID)
                                   and T_PARTYKIND in(2,27,52)
                                   and rownum = 1),0) > 0
                    )
     LOOP

       IF( NVL(one_rec.T_COST_CFI_1, 0) >= 1000 AND NVL(one_rec.T_COST_CFI_2, 0) >= 1000 )THEN
       
          v_COST_PAYFIID_1 := round(NVL(RSI_RSB_FIInstr.ConvSum(one_rec.T_COST_CFI_1, one_rec.T_CFI, one_rec.T_PAYFIID, OnDate, 1),0)/1000,3);
          v_COST_PAYFIID_2 := round(NVL(RSI_RSB_FIInstr.ConvSum(one_rec.T_COST_CFI_2, one_rec.T_CFI, one_rec.T_PAYFIID, OnDate, 1),0)/1000,3);

          /*Если для строки валюта требования и валюта обязательства одинаковы и разница между суммой требования и суммой обязательства,
            приведенных к виду три знака после запятой равны, то для продаж и прямых РЕПО единица валюты прибавляется к требованиям, для покупок и обратных РЕПО - к обязательствам.
           */
          if (greatest(v_COST_PAYFIID_1,v_COST_PAYFIID_2) - least(v_COST_PAYFIID_1,v_COST_PAYFIID_2)) = 0 then
              v_COST_PAYFIID_1 := v_COST_PAYFIID_1 + 0.001;
          end if;

          rep701.T_DEALID           := one_rec.T_DEALID;
          rep701.T_DEALDATE         := one_rec.T_PayDate_1;
          rep701.T_CALCDATE         := one_rec.T_PayDate_2;
          rep701.T_REQFIID          := one_rec.T_PAYFIID;
          rep701.T_REQSUM           := case when one_rec.T_IsBuy = 1 then v_COST_PAYFIID_2 else v_COST_PAYFIID_1 end;
          rep701.T_COMFIID          := one_rec.T_PAYFIID;
          rep701.T_COMSUM           := case when one_rec.T_IsBuy = 1 then v_COST_PAYFIID_1 else v_COST_PAYFIID_2 end;
          rep701.T_ISCLIENT         := case when one_rec.T_CLIENTID > 0 then 'X' else chr(0) end;
          rep701.T_ISRESIDENT       := one_rec.T_ISRESIDENT;
          rep701.T_OKCMCODE         := one_rec.T_OKCMCODE;

          rep701.T_ISMARKET         := chr(0);--пока не заполняем, т.к. РЕПО специально сортировать по биржевым/внебиржевым не просили

          rep701.T_DealPart := 1;

          rep701.T_MARKETCONTRAGENT := one_rec.T_MarketContractor;
          rep701.T_ISEXCHORCENTRCONTR := case when one_rec.T_MarketID > 0 then 1 else one_rec.T_IsExchOrCentrContr end;
          GetBankContragentInfo(one_rec.T_ContractorID,one_rec.T_ISRESIDENT,1,OnDate,rep701.T_BANKCONTRAGENT, rep701.T_ISSWIFTCODE, rep701.T_TYPECONTR);
          PRegNum (one_rec.T_ContractorID, rep701.T_PREGNUM);
          rep701.T_BIC := RSI_RSBPARTY.GetPartyCodeOnDate (one_rec.T_ContractorID, PM_COMMON.PTCK_BIC, OnDate, v_owner_BIC);
          rep701.T_OTHER := chr(1);
          if rep701.T_ISCLIENT = 'X' and one_rec.T_ContractorIsBank = 1 then
             rep701.T_OTHER := 'K';
          end if;

          rep701.T_TYPEADDINFO      := 'REPO';

          rep701.T_DEALADDINFO      := chr(1);
          rep701.T_RATEADDINFO      := 0;

          rep701.T_ISITOG           := chr(0);
          rep701.T_IS_UNION         := chr(0);

          rep701.T_DlKIND           := 1;--РЕПО

          rep701.T_RATEADDINFO      := one_rec.T_IncomeRate;

          rep701.T_DEALSIDE         := case when one_rec.T_IsBuy = 1 then 'B' else 'S' end;

          rep701.T_METHODADDINFO    := case when (instr(one_rec.T_PartyKind,',29,') != 0 or instr(one_rec.T_PartyKind,',27,') != 0) then 'DE'
                                            when  one_rec.T_MarketContractor = 'MMVB' then 'IE'
                                            when one_rec.t_AdressCategory = 'AC' then 'DE' -- Golovkin 17.02.2021 ID : 523162 (Без-)адресная сделка
                                            when  one_rec.T_IsExchange = 1 then 'IE'
                                            when (one_rec.T_IsExchange != 1 and one_rec.T_TransCategory = 4 and one_rec.T_IsBroker != 1) then 'DV'
                                            when (one_rec.T_IsExchange != 1 and one_rec.T_TransCategory = 4 and one_rec.T_IsBroker = 1) then 'IV'
                                            when (one_rec.T_IsExchange != 1 and (one_rec.T_TransCategory = 2 or one_rec.T_TransCategory = 3)) then 'DE'
                                            else 'DE' end;
                                            
                                            
           begin
              SELECT LISTAGG(t_partykind, ',') WITHIN GROUP (ORDER BY t_partykind) into v_contrkind
                FROM dpartyown_dbt
               WHERE t_partyid = one_rec.T_ContractorID;
              EXCEPTION WHEN NO_DATA_FOUND THEN v_contrkind := chr(1);
          end;
          
          v_contrkind := ',' || v_contrkind || ',';
           if ((instr(v_contrkind,',2,') != 0) or (instr(v_contrkind,',52,') != 0) or (instr(v_contrkind,',27,') != 0)) then
              rep701.T_ISMONEYMARKET := 'X';
           else
              rep701.T_ISMONEYMARKET := chr(0);
           end if;
          
          rep701.t_ContractorID := one_rec.t_ContractorID;
          if (rep701.T_IsMoneyMarket != 'X' and RSB_DLREP701_MZ_2293.IsNeedGr17_18(rep701.T_ContractorID) = 1)  then 
             rep701.T_ADDGRFLAG := 1;
          else 
             rep701.T_ADDGRFLAG := 0;
          end if;

          g_rep701_ins.extend;
          g_rep701_ins(g_rep701_ins.LAST) := rep701;
          

          if rep701.T_ISCLIENT = 'X' and one_rec.T_ClientIsBank = 1 then

             rep701.T_DealPart := 2;

             rep701.T_MARKETCONTRAGENT := 'MD';
             rep701.T_ISEXCHORCENTRCONTR := 2;
             rep701.T_OTHER    := 'K';
             rep701.T_COMFIID  := one_rec.T_PAYFIID;
             rep701.T_COMSUM   := case when one_rec.T_IsBuy = 1 then v_COST_PAYFIID_2 else v_COST_PAYFIID_1 end;
             rep701.T_REQFIID  := one_rec.T_PAYFIID;
             rep701.T_REQSUM   := case when one_rec.T_IsBuy = 1 then v_COST_PAYFIID_1 else v_COST_PAYFIID_2 end;
             rep701.T_DEALSIDE := case when one_rec.T_IsBuy = 1 then 'S' else 'B' end;

             g_rep701_ins.extend;
             g_rep701_ins(g_rep701_ins.LAST) := rep701;

          end if;
 
          
       END IF;

     END LOOP;

     IF g_rep701_ins IS NOT EMPTY THEN
         FORALL i IN g_rep701_ins.FIRST .. g_rep701_ins.LAST
              INSERT INTO DDLREP701_TMP
                   VALUES g_rep701_ins(i);
         g_rep701_ins.delete;
     END IF;

  END CreateData_REPO;

  PROCEDURE CreateData_NotREPO(DepartmentID IN NUMBER, OnDate IN DATE, FIID_840 IN NUMBER, CodeType IN NUMBER, IsIntermediate NUMBER DEFAULT 0)
  IS
     v_COST_PAYFIID NUMBER;
     TYPE rep701_t IS TABLE OF DDLREP701_TMP%ROWTYPE;
     g_rep701_ins rep701_t := rep701_t();
     rep701 DDLREP701_TMP%rowtype;
     v_owner_BIC NUMBER;
  BEGIN
     FOR one_rec IN (with tick as(select TICK.T_DEALID, tick.T_BOFFICEKIND, TICK.T_DEALDATE, TICK.T_PARTYID, TICK.T_CLIENTID, TICK.T_MARKETID, TICK.T_BROKERID, TICK.T_ChangeDate,
                                         RSB_SECUR.IsBuy(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) T_IsBuy,
                                         RSB_SECUR.IsExchange(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind)),1) T_IsExchange,
                                         RSB_SECUR.IsOutExchange(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind)),1) T_IsOutExchange,
                                         RSB_SECUR.IsOTC(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) T_IsOTC,
                                         RSB_SECUR.IsBroker(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) T_IsBroker,
                                         NVL((SELECT Attr.t_NumInList
                                                FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
                                               WHERE AtCor.t_ObjectType = Rsb_Secur.OBJTYPE_SECDEAL
                                                 AND AtCor.t_GroupID    = 50
                                                 AND AtCor.t_Object     = LPAD(TICK.T_DEALID, 34, '0')
                                                 AND Attr.t_AttrID      = AtCor.t_AttrID
                                                 AND Attr.t_ObjectType  = AtCor.t_ObjectType
                                                 AND Attr.t_GroupID     = AtCor.t_GroupID),0) T_TransCategory,
                                         case when TICK.T_MARKETID > 0 or NVL((SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = TICK.T_PARTYID AND T_PARTYKIND = 3),0) > 0
                                              then NVL(( SELECT DECODE(Rsb_Secur.SC_GetObjCodeOnDate(cnst.OBJTYPE_PARTY, cnst.PTCK_CONTR, T_PARTYID),'ММВБ','MMVB', 'ПАО СПБ','SPBEX', t_shortname) 
                                                 FROM DPARTY_DBT
                                                WHERE T_PARTYID = case when TICK.T_MARKETID > 0 then TICK.T_MARKETID
                                                                       when NVL((SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = TICK.T_PARTYID AND T_PARTYKIND = 3),0) > 0 then TICK.T_PARTYID
                                                                       else -1 end),'DO')
                                              when NVL((SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = TICK.T_PARTYID AND T_PARTYKIND IN (27, 58)),0) > 0
                                              then 'MMVB'
                                              else NVL((SELECT case when Attr.t_NumInList = 1 then 'MMVB'
                                                                    when Attr.t_NumInList = 2 then 'RTRS'
                                                                    when Attr.t_NumInList = 3 then 'BBLG'
                                                                    when Attr.t_NumInList = 4 then 'PHONE' end
                                                          FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
                                                         WHERE AtCor.t_ObjectType = Rsb_Secur.OBJTYPE_SECDEAL
                                                           AND AtCor.t_GroupID    = 50
                                                           AND AtCor.t_Object     = LPAD(TICK.T_DEALID, 34, '0')
                                                           AND Attr.t_AttrID      = AtCor.t_AttrID
                                                           AND Attr.t_ObjectType  = AtCor.t_ObjectType
                                                           AND Attr.t_GroupID     = AtCor.t_GroupID),'DO') end T_MarketContractor
                                    from ddl_tick_dbt tick
                                   where TICK.T_BOFFICEKIND = rsb_secur.DL_SECURITYDOC
                                     and TICK.T_DEALSTATUS > CASE WHEN IsIntermediate = 1 THEN -1 ELSE 0 END
                                     AND 1 = CASE WHEN IsIntermediate = 1 THEN TimeIsIntermediate(TICK.T_DEALTIME) ELSE 1 END
                                     and TICK.T_DEALDATE = OnDate
                                     and TICK.T_DEPARTMENT = case when DepartmentID != -1 then DepartmentID else TICK.T_DEPARTMENT end
                                     and ( (CodeType = DL_EXTCODETYPE_OWN    and TICK.T_CLIENTID = -1) or
                                           (CodeType = DL_EXTCODETYPE_CLIENT and TICK.T_CLIENTID != -1)
                                         )
                                     and RSB_SECUR.IsRepo(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) = 0
                                     and TICK.T_REQUESTID = 0
                                 )
                     select TICK.T_DEALID, TICK.T_BOFFICEKIND, TICK.T_DEALDATE, TICK.T_PARTYID, TICK.T_CLIENTID, TICK.T_MARKETID, TICK.T_BROKERID, TICK.T_ChangeDate, TICK.T_IsBuy, TICK.T_IsExchange,
                            TICK.T_IsOutExchange, TICK.T_IsOTC, TICK.T_MarketContractor, TICK.T_IsBroker, TICK.T_TransCategory,
                            LEG.T_CFI, LEG.T_PAYFIID, LEG.T_TOTALCOST T_COST_CFI, LEG.T_PFI,
                            NVL( (select case when rq_cur.T_FACTDATE != to_date('01.01.0001','DD.MM.YYYY') then rq_cur.T_FACTDATE else rq_cur.T_PLANDATE end
                                    from ddlrq_dbt rq_cur
                                   where rq_cur.t_docid = tick.t_dealid
                                     AND rq_cur.t_dockind = tick.T_BOFFICEKIND
                                     AND rq_cur.t_subkind = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY
                                     AND rq_cur.t_type = RSI_DLRQ.DLRQ_TYPE_PAYMENT
                                     AND rq_cur.t_DealPart = 1),to_date('01.01.0001','DD.MM.YYYY')) T_PayDate,
                             NVL( (select case when rq_avr.T_FACTDATE != to_date('01.01.0001','DD.MM.YYYY') then rq_avr.T_FACTDATE else rq_avr.T_PLANDATE end
                                     from ddlrq_dbt rq_avr
                                    where rq_avr.t_docid = tick.t_dealid
                                      AND rq_avr.t_dockind = tick.T_BOFFICEKIND
                                      AND rq_avr.t_subkind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                                      AND rq_avr.t_type = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                                      AND rq_avr.t_DealPart = 1),to_date('01.01.0001','DD.MM.YYYY')) T_SUPLDATE,
                            GetContractorID(TICK.T_IsExchange, TICK.T_PARTYID, TICK.T_MARKETID, TICK.T_BROKERID) T_ContractorID,
                            NVL((SELECT DECODE(T_NotResident,'X','N','R') from dparty_dbt WHERE T_PARTYID = GetContractorID(TICK.T_IsExchange, TICK.T_PARTYID, TICK.T_MARKETID, TICK.T_BROKERID)),chr(0)) T_ISRESIDENT,
                            NVL((select country.T_CodeNum3 from DCOUNTRY_DBT country, DPARTY_DBT party
                                  where country.t_Codelat3 = party.T_NRCOUNTRY
                                    and party.T_PARTYID = GetContractorID(TICK.T_IsExchange, TICK.T_PARTYID, TICK.T_MARKETID, TICK.T_BROKERID)
                                    and party.T_NotResident = 'X' --заполняем только для нерезидентов
                                    and rownum = 1 ),chr(1)) T_OKCMCODE,
                            case when Exists(SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = TICK.T_PARTYID AND T_PARTYKIND in(3,58)) then 1 else 2 end T_IsExchOrCentrContr,
                            NVL(','||(SELECT LISTAGG(t_partykind, ',') WITHIN GROUP (ORDER BY t_partykind)
                                   FROM dpartyown_dbt
                                       WHERE t_partyid = GetContractorID(TICK.T_IsExchange, TICK.T_PARTYID, TICK.T_MARKETID, TICK.T_BROKERID))||',',chr(1)) T_partykind,
                            NVL(','||(SELECT LISTAGG(t_partykind, ',') WITHIN GROUP (ORDER BY t_partykind)
                                        FROM dpartyown_dbt
                                       WHERE t_partyid = TICK.T_CLIENTID)||',',chr(1)) T_clientkind
                     from tick, ddl_leg_dbt leg
                     where LEG.T_DEALID = TICK.T_DEALID
                       and LEG.T_LEGKIND = 0
                       and LEG.T_LEGID = 0
                       and LEG.T_CFI != LEG.T_PAYFIID)
     LOOP
       IF IsIntermediate = 1 AND one_rec.T_IsOTC = 1 THEN
         CONTINUE;
       END IF;
       -- если по сделке были изменения условий, то возьмем первоначальные параметры
       IF( one_rec.T_CHANGEDATE != to_date('01.01.0001','DD.MM.YYYY') ) THEN
         FOR beg_data IN ( SELECT t_OldCFI1, t_OldTotalCost1, t_OldPFI1, t_OldPayFIID1,
                                  t_OldMaturity1, t_OldExpiry1, t_OldMaturityIsPrincipal1
                             FROM dsptkchng_dbt
                            WHERE t_DealID = one_rec.T_DEALID
                              AND t_OldInstance = 0 )
         LOOP
           one_rec.T_CFI      := beg_data.t_OldCFI1;
           one_rec.T_PAYFIID  := beg_data.t_OldPayFIID1;
           one_rec.T_COST_CFI := beg_data.t_OldTotalCost1;
           one_rec.T_PFI      := beg_data.t_OldPFI1;
           one_rec.T_PayDate  := CASE WHEN beg_data.t_OldMaturityIsPrincipal1 = 'X' THEN beg_data.t_OldExpiry1 ELSE beg_data.t_OldMaturity1 END;
           one_rec.T_SUPLDATE := CASE WHEN beg_data.t_OldMaturityIsPrincipal1 = 'X' THEN beg_data.t_OldMaturity1 ELSE beg_data.t_OldExpiry1 END;
           EXIT;
         END LOOP;
       END IF;

       v_COST_PAYFIID            := NVL(RSI_RSB_FIInstr.ConvSum(one_rec.T_COST_CFI, one_rec.T_CFI, one_rec.T_PAYFIID, OnDate, 1),0);

       rep701.T_DEALID           := one_rec.T_DEALID;
       rep701.T_DEALDATE         := one_rec.T_DEALDATE;
       rep701.T_CALCDATE         := greatest(one_rec.T_PayDate,one_rec.T_SUPLDATE);
       rep701.T_REQFIID          := case when one_rec.T_IsBuy = 1 then one_rec.T_CFI else one_rec.T_PAYFIID end;
       rep701.T_REQSUM           := case when one_rec.T_IsBuy = 1 then one_rec.T_COST_CFI else v_COST_PAYFIID end;
       rep701.T_COMFIID          := case when one_rec.T_IsBuy != 1 then one_rec.T_CFI else one_rec.T_PAYFIID end;
       rep701.T_COMSUM           := case when one_rec.T_IsBuy != 1 then one_rec.T_COST_CFI else v_COST_PAYFIID end;
       rep701.T_ISCLIENT         := case when one_rec.T_CLIENTID > 0 then 'X' else chr(0) end;
       rep701.T_ISRESIDENT       := one_rec.T_ISRESIDENT;
       rep701.T_OKCMCODE         := one_rec.T_OKCMCODE;
       rep701.T_ISMONEYMARKET := chr(0);

       rep701.T_ISMARKET         := chr(0);--пока не заполняем, т.к. покупки\продажи БОЦБ специально сортировать по биржевым/внебиржевым не просили

       rep701.T_DealPart := 1;

       rep701.T_ISBUY            := case when one_rec.T_IsBuy = 1 then 'X' else chr(0) end;

       rep701.T_MARKETCONTRAGENT := case when one_rec.T_IsOTC = 1 and rep701.T_ISCLIENT = 'X' then 'MS' 
                                         when one_rec.T_IsOTC = 1 and rep701.T_ISCLIENT != 'X' then 'CMOEX' else one_rec.T_MarketContractor end;
       rep701.T_ISEXCHORCENTRCONTR := case when one_rec.T_MarketID > 0 then 1 else one_rec.T_IsExchOrCentrContr end;

       if( one_rec.T_IsOutExchange = 1 )then
          GetBankContragentInfo(one_rec.T_ContractorID,one_rec.T_ISRESIDENT,0,OnDate,rep701.T_BANKCONTRAGENT,rep701.T_ISSWIFTCODE, rep701.T_TYPECONTR);
          PRegNum (one_rec.T_ContractorID, rep701.T_PREGNUM);
          rep701.T_BIC := RSI_RSBPARTY.GetPartyCodeOnDate (one_rec.T_ContractorID, PM_COMMON.PTCK_BIC, OnDate, v_owner_BIC);
       else
          rep701.T_BANKCONTRAGENT   := chr(1);
          rep701.T_TYPECONTR        := 0;
          rep701.T_PREGNUM          := chr(1);
          rep701.T_BIC              := chr(1);
          rep701.T_ISSWIFTCODE      := chr(0);
       end if;

       rep701.T_IS_UNION         := chr(0);
       if one_rec.T_IsOutExchange = 1 and instr(one_rec.T_partykind,',2,') = 0 and instr(one_rec.T_partykind,',3,') = 0 and instr(one_rec.T_partykind,',63,') = 0 and
          NVL(RSI_RSB_FIInstr.ConvSum(rep701.T_REQSUM, rep701.T_REQFIID, FIID_840, one_rec.T_DEALDATE, 1),0) < 1000000 and
          NVL(RSI_RSB_FIInstr.ConvSum(rep701.T_COMSUM, rep701.T_COMFIID, FIID_840, one_rec.T_DEALDATE, 1),0) < 1000000 then
          rep701.T_IS_UNION := 'X';
       elsif( rep701.T_ISCLIENT = 'X' and one_rec.T_IsOutExchange = 0 )then
          rep701.T_IS_UNION := 'X';
       end if;

       /*для определения T_OTHER нужен уже заполненный T_TYPEADDINFO, поэтому заполняем T_TYPEADDINFO раньше T_OTHER*/
       rep701.T_TYPEADDINFO      := GetTypeFinActive(one_rec.T_PFI);
       if one_rec.T_IsOTC = 1 then
          rep701.T_OTHER         := chr(0);
       elsif rep701.T_ISCLIENT = 'X' and rep701.T_IS_UNION != 'X' then
          rep701.T_OTHER         := 'C';
       else
          rep701.T_OTHER         := GetOTHERInfo(one_rec.T_ContractorID,rep701.T_ISCLIENT,one_rec.T_partykind,one_rec.T_COST_CFI,one_rec.T_CFI,one_rec.T_DEALDATE,FIID_840,rep701.T_TYPEADDINFO);
       end if;

       rep701.T_DEALADDINFO      := chr(1);
       rep701.T_RATEADDINFO      := 0;
       rep701.T_ISITOG           := chr(0);

       rep701.T_DlKIND           := 2;--сделки БО ЦБ кроме РЕПО

       rep701.T_DEALSIDE         := case when rep701.T_ISBUY = 'X' then 'B' else 'S' end;

       rep701.T_METHODADDINFO    := case when (instr(one_rec.T_PartyKind,',29,') != 0 or instr(one_rec.T_PartyKind,',27,') != 0) then 'DE'
                                         when  one_rec.T_IsExchange = 1 or one_rec.T_IsOTC = 1 then 'IE'
                                         when (one_rec.T_IsExchange != 1 and one_rec.T_TransCategory = 4 and one_rec.T_IsBroker != 1) then 'DV'
                                         when (one_rec.T_IsExchange != 1 and one_rec.T_TransCategory = 4 and one_rec.T_IsBroker = 1) then 'IV'
                                         when (one_rec.T_IsExchange != 1 and (one_rec.T_TransCategory = 2 or one_rec.T_TransCategory = 3)) then 'DE'
                                         else 'DE' end;
          
       rep701.T_CONTRACTORID := one_rec.T_ContractorID;
       
         if (rep701.T_IsMoneyMarket != 'X' and RSB_DLREP701_MZ_2293.IsNeedGr17_18(rep701.T_ContractorID) = 1)  then 
             rep701.T_ADDGRFLAG := 1;
          else 
             rep701.T_ADDGRFLAG := 0;
          end if;

       g_rep701_ins.extend;
       g_rep701_ins(g_rep701_ins.LAST) := rep701;

       if rep701.T_ISCLIENT = 'X' and rep701.T_IS_UNION != 'X' then

           rep701.T_DealPart := 2;

           if one_rec.T_IsOTC = 1 then
              rep701.T_BANKCONTRAGENT   := chr(1);
              rep701.T_TYPECONTR        := 0;
              rep701.T_PREGNUM          := chr(1);
              rep701.T_BIC              := chr(1);
              rep701.T_ISSWIFTCODE      := chr(0);
              
              rep701.T_MARKETCONTRAGENT := 'DO';
              rep701.T_METHODADDINFO    := 'DV';
           else
              rep701.T_MARKETCONTRAGENT := 'MD';
           end if;
           
           rep701.T_ISEXCHORCENTRCONTR := 2;
           rep701.T_OTHER            := GetOTHERInfo(one_rec.T_CLIENTID,rep701.T_ISCLIENT,one_rec.T_clientkind,one_rec.T_COST_CFI,one_rec.T_CFI,one_rec.T_DEALDATE,FIID_840,chr(1),one_rec.T_IsOTC);
           rep701.T_REQFIID          := case when one_rec.T_IsBuy = 1 then one_rec.T_PAYFIID else one_rec.T_CFI end;
           rep701.T_REQSUM           := case when one_rec.T_IsBuy = 1 then v_COST_PAYFIID else one_rec.T_COST_CFI end;
           rep701.T_COMFIID          := case when one_rec.T_IsBuy != 1 then one_rec.T_PAYFIID else one_rec.T_CFI end;
           rep701.T_COMSUM           := case when one_rec.T_IsBuy != 1 then v_COST_PAYFIID else one_rec.T_COST_CFI end;
           rep701.T_DEALSIDE         := case when rep701.T_ISBUY = 'X' then 'S' else 'B' end;

           g_rep701_ins.extend;
           g_rep701_ins(g_rep701_ins.LAST) := rep701;

       end if;

     END LOOP;

     IF g_rep701_ins IS NOT EMPTY THEN
         FORALL i IN g_rep701_ins.FIRST .. g_rep701_ins.LAST
              INSERT INTO DDLREP701_TMP
                   VALUES g_rep701_ins(i);
         g_rep701_ins.delete;
     END IF;

  END CreateData_NotREPO;

  PROCEDURE CreateData_DV_NDEAL(DepartmentID IN NUMBER, OnDate IN DATE, FIID_840 IN NUMBER, MarketKind IN NUMBER, CodeType IN NUMBER, IsIntermediate NUMBER DEFAULT 0)
  IS
     v_REQFIID NUMBER := -1;
     v_REQSUM  NUMBER := 0;
     v_COMFIID NUMBER := -1;
     v_COMSUM  NUMBER := 0;
     v_COST_PAYFIID NUMBER;
     TYPE rep701_t IS TABLE OF DDLREP701_TMP%ROWTYPE;
     g_rep701_ins rep701_t := rep701_t();
     rep701 DDLREP701_TMP%rowtype;
     v_owner_BIC NUMBER;
  BEGIN
     FOR one_rec IN ( select NDEAL.T_ID, NDEAL.T_DOCKIND, NDEAL.T_SECTOR, NDEAL.T_DVKIND, NDEAL.T_TYPE, NDEAL.T_SWAPTYPE, NDEAL.T_DATE, NDEAL.T_CLIENT, NDEAL.T_Contractor, NDEAL.T_OptionType,
                             NDEAL.T_Bonus, NDEAL.T_BonusFIID, NDEAL.T_BeginDate, NDEAL.T_AGENT, NDEAL.T_COMMENT, NDEAL.T_KIND, NDEAL.T_PERIODCLS,
                             NFI.T_ExecType, NFI.T_ExecDate, NFI.T_TYPE T_NFI_TYPE, NFI2.T_TYPE T_NFI_TYPE2, NFI.T_Rate, NFI.T_AccFIID,
                             NFI.T_PAYDATE, NFI.T_SUPLDATE, case when ndeal.T_Forvard = 'X' then NFI_FRW.T_FIID else NFI.T_FIID end t_FIID, 
                             NFI.T_PRICEFIID, NFI2.T_PRICEFIID T_PRICEFIID2, NFI2.T_FIID T_FIID2, NDEAL.t_MarketKind,
                             NFI.T_AMOUNT * (case when ndeal.T_Forvard = 'X' then NFI_FRW.T_AMOUNT else 1 end) T_AMOUNT,
                             case when ndeal.T_Forvard = 'X' or (NDEAL.T_DVKIND in(1,2) and NFI.t_ExecType = 0) then NFI.T_AMOUNT * NFI.T_PRICE else NFI.T_COST end T_COST,
                             case when ndeal.T_Forvard = 'X' or (NDEAL.T_DVKIND in(1,2) and NFI2.t_ExecType = 0) then NFI2.T_AMOUNT * NFI2.T_PRICE else NFI2.T_COST end T_COST2,
                             NVL((select 1 from DDVNFI_dbt t where t.T_DEALID = NDEAL.T_ID and t.t_FIID = case when ndeal.T_Forvard = 'X' then NFI_FRW.T_FIID else NFI.T_FIID end
                                                               and t.T_TYPE = case when NFI.T_TYPE = 0 then 2 else 0 end),0) T_IsSinglePcSw,
                             NVL((select T.T_SZNAMEALG from dnamealg_dbt t where T.T_ITYPEALG = 7004 and T.T_INUMBERALG = NDEAL.T_TYPE),chr(1)) T_TYPE_NAME,
                             NVL((select 1 from dfininstr_dbt fin where FIN.T_FIID = case when ndeal.T_Forvard = 'X' then NFI_FRW.T_FIID else NFI.T_FIID end and FIN.T_FI_KIND = 1),0) T_BA_IsCurr,
                             nvl((select 1 from dfininstr_dbt fin where FIN.T_FIID = case when ndeal.T_Forvard = 'X' then NFI_FRW.T_FIID else NFI.T_FIID end and (FIN.T_FI_KIND = 3 AND FIN.T_SETTLEMENT_CODE = 9 )),0) T_IsPrcIndex,
                             case when NDEAL.T_MarketKind <=0 then 0 else 1 end T_IsMarket,
                             NVL((SELECT DECODE(T_NotResident,'X','N','R') from dparty_dbt WHERE T_PARTYID = NDEAL.T_Contractor),chr(0)) T_ISRESIDENT,
                             NVL((SELECT Attr.t_NumInList
                                    FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
                                   WHERE ((AtCor.t_ObjectType = Rsb_Secur.OBJTYPE_OUTOPER_DV AND AtCor.t_GroupID = 3) or (AtCor.t_ObjectType = 148 AND AtCor.t_GroupID = 5))
                                     AND AtCor.t_Object     = LPAD(NDEAL.T_ID, 34, '0')
                                     AND Attr.t_AttrID      = AtCor.t_AttrID
                                     AND Attr.t_ObjectType  = AtCor.t_ObjectType
                                     AND Attr.t_GroupID     = AtCor.t_GroupID),0) T_TransCategory,
                             NVL((select country.T_CodeNum3 from DCOUNTRY_DBT country, DPARTY_DBT party
                                  where country.t_Codelat3 = party.T_NRCOUNTRY
                                    and party.T_PARTYID = NDEAL.T_Contractor
                                    and party.T_NotResident = 'X' --заполняем только для нерезидентов
                                    and rownum = 1 ),chr(1)) T_OKCMCODE,
                             case when Exists(SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = NDEAL.T_Contractor AND T_PARTYKIND in(3,58)) then 1 else 2 end T_IsExchOrCentrContr,
                             case when NVL((SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = NDEAL.T_Contractor AND T_PARTYKIND = 3),0) = 1
                                  then NVL(( SELECT DECODE(Rsb_Secur.SC_GetObjCodeOnDate(cnst.OBJTYPE_PARTY, cnst.PTCK_CONTR, T_PARTYID),'ММВБ','MMVB','ПАО СПБ','SPBEX','АО СПВБ','SPVB', t_shortname) 
                                               FROM DPARTY_DBT
                                              WHERE T_PARTYID = NDEAL.T_Contractor),chr(1))
                                   when NDEAL.T_SECTOR = 'X' and NDEAL.T_MARKETID > 0
                                  then NVL(( SELECT DECODE(Rsb_Secur.SC_GetObjCodeOnDate(cnst.OBJTYPE_PARTY, cnst.PTCK_CONTR, T_PARTYID),'ММВБ','MMVB','АО СПВБ','SPVB','ПАО СПБ','SPBEX', t_shortname) 
                                               FROM DPARTY_DBT
                                              WHERE T_PARTYID = NDEAL.T_MARKETID),chr(1)) 
                                  when NVL((SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = NDEAL.T_Contractor AND T_PARTYKIND = 58),0) = 1
                                  then 'MMVB'
                                  else NVL((SELECT case when Attr.t_NumInList = 1 then 'MMVB'
                                                        when Attr.t_NumInList = 2 then 'RTRS'
                                                        when Attr.t_NumInList = 3 then 'BBLG'
                                                        when Attr.t_NumInList = 4 then 'PHONE' end
                                              FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
                                             WHERE ((AtCor.t_ObjectType = Rsb_Secur.OBJTYPE_OUTOPER_DV AND AtCor.t_GroupID = 3) or (AtCor.t_ObjectType = 148 AND AtCor.t_GroupID = 5))
                                               AND AtCor.t_Object     = LPAD(NDEAL.T_ID, 34, '0')
                                               AND Attr.t_AttrID      = AtCor.t_AttrID
                                               AND Attr.t_ObjectType  = AtCor.t_ObjectType
                                               AND Attr.t_GroupID     = AtCor.t_GroupID),'DO') end T_MarketShortName,
                             NVL(','||(SELECT LISTAGG(t_partykind, ',') WITHIN GROUP (ORDER BY t_partykind)
                                        FROM dpartyown_dbt
                                       WHERE t_partyid = NDEAL.T_Contractor)||',',chr(1)) T_partykind,
                             NVL(','||(SELECT LISTAGG(t_partykind, ',') WITHIN GROUP (ORDER BY t_partykind)
                                         FROM dpartyown_dbt
                                        WHERE t_partyid = NDEAL.T_CLIENT)||',',chr(1)) T_clientkind,
                             trim(chr(0) from cast(rsb_struct.getString(rsi_rsb_kernel.GetNote(decode(NDEAL.T_DOCKIND,rsb_secur.DL_DVFXDEAL,RSB_Derivatives.OBJTYPE_FXOPER_DV,RSB_Derivatives.OBJTYPE_OUTOPER_DV),
                                                                   LPAD(NDEAL.T_ID, 34, '0'), decode(NDEAL.T_DOCKIND,rsb_secur.DL_DVFXDEAL,2,12), to_date('31.12.9999','DD.MM.YYYY'))) as varchar2(1023))) T_MarketFromNote,                                
                            (case when (NDEAL.T_DOCKIND != rsb_secur.DL_DVFXDEAL and NDEAL.t_MarketKind = Rsb_Secur.DV_MARKETKIND_SPFIMARKET) then 
                                (select t_PartyID from dpartyname_dbt where replace(upper(t_Name), '"', '') = replace(upper(nvl(trim(chr(0) from cast(rsb_struct.getString(rsi_rsb_kernel.GetNote(RSB_Derivatives.OBJTYPE_OUTOPER_DV, LPAD(NDEAL.T_ID, 34, '0'),
                                                                                   14, to_date('31.12.9999','DD.MM.YYYY'))) as varchar2(1023))),chr(1))), '"', '')  and rownum=1)
                             else 0 end) t_RelPartyRef
                        from ddvndeal_dbt ndeal JOIN ddvnfi_dbt NFI on NFI.T_DEALID = NDEAL.T_ID AND NFI.T_TYPE in(0,2) 
                                                            LEFT OUTER JOIN ddvnfi_dbt nfi_frw ON NFI_FRW.T_DEALID = NDEAL.T_ID and NFI_FRW.T_TYPE = 1 
                                                            LEFT OUTER JOIN ddvnfi_dbt NFI2 ON NFI2.T_DEALID = NDEAL.T_ID and NFI2.T_TYPE in(0,2) and NFI2.T_TYPE != NFI.T_TYPE
                       where NDEAL.T_DATE = OnDate
                         
                         and rsi_rsb_fiinstr.FI_GetRealFIKind(
                         case when ndeal.T_Forvard = 'X' then NFI_FRW.T_FIID else NFI.T_FIID end) != rsi_rsb_fiinstr.FIKIND_METAL 
                         and NDEAL.T_STATE > CASE WHEN IsIntermediate = 1 THEN -1 ELSE 0 END
                         AND 1 = CASE WHEN IsIntermediate = 1 THEN TimeIsIntermediate(NDEAL.T_TIME) ELSE 1 END
                         and NDEAL.T_DEPARTMENT = case when DepartmentID != -1 then DepartmentID else NDEAL.T_DEPARTMENT end
                         and ( ((MarketKind = DV_MARKETKIND_DERIV or MarketKind = DV_MARKETKIND_CURRENCY) and NDEAL.T_SECTOR = 'X' and NDEAL.T_MARKETKIND = MarketKind) or
                               (MarketKind = DV_MARKETKIND_OTHER and NOT (NDEAL.T_SECTOR = 'X' and (NDEAL.T_MARKETKIND = DV_MARKETKIND_DERIV or NDEAL.T_MARKETKIND = DV_MARKETKIND_CURRENCY)))
                             )
                         and ( (CodeType = DL_EXTCODETYPE_OWN    and NDEAL.T_CLIENT = -1) or
                               (CodeType = DL_EXTCODETYPE_CLIENT and NDEAL.T_CLIENT != -1) or
                               (CodeType = 0 and MarketKind = DV_MARKETKIND_OTHER)
                             )
                         AND (
                               (NDEAL.T_DVKIND = 4 and ((NVL((select 1 from DDVNFI_dbt t
                                                               where t.T_DEALID = NDEAL.T_ID and t.t_FIID = NFI.T_FIID
                                                                 and t.T_TYPE = case when NFI.T_TYPE = 0 then 2 else 0 end),0) = 1
                                                         and
                                                         (
                                                          (NFI.T_TYPE = 0 and ndeal.T_TYPE = RSB_Derivatives.ALG_DV_FIX_FLOAT) or
                                                          (NFI.T_TYPE = 2 and ndeal.T_TYPE = RSB_Derivatives.ALG_DV_FLOAT_FIX)
                                                         )
                                                        ) or
                                                        ndeal.T_TYPE = RSB_Derivatives.ALG_DV_FLOAT_FLOAT or
                                                        NVL((select 1 from DDVNFI_dbt t
                                                               where t.T_DEALID = NDEAL.T_ID and t.t_FIID = NFI.T_FIID
                                                                 and t.T_TYPE = case when NFI.T_TYPE = 0 then 2 else 0 end),0) = 0
                                                       )
                                                   --and NFI.T_AMOUNT >= 1000 --согласно ТЗ, для процентных деривативов такого ограничения нет, иначе может попасть только половина сделки
                               )
                             or
                               (NDEAL.T_DVKIND in(3,7) and nvl((select 1 from dfininstr_dbt fin where FIN.T_FIID = NFI.T_FIID and FIN.T_FI_KIND = 1),0) = 1)
                             or
                               (NDEAL.T_DVKIND = 1 and nvl((select 1 from dfininstr_dbt fin where FIN.T_FIID = NFI.T_FIID and (FIN.T_FI_KIND = 1 OR (FIN.T_FI_KIND = 3 AND FIN.T_SETTLEMENT_CODE in (1,3) ))),0) = 1)
                             or
                               (NDEAL.T_DVKIND = 2 and nvl((select 1 from dfininstr_dbt fin where FIN.T_FIID = case when ndeal.T_Forvard = 'X' then NFI_FRW.T_FIID else NFI.T_FIID end and (FIN.T_FI_KIND = 1 OR (FIN.T_FI_KIND = 3 AND FIN.T_SETTLEMENT_CODE in (1,3,9) ))),0) = 1)
                             or
                               (NDEAL.T_DVKIND = 5 and nvl((select 1 from dfininstr_dbt fin where FIN.T_FIID = NFI.T_FIID
                                and (FIN.T_FI_KIND = 1 or (FIN.T_FI_KIND = 2 and fin.t_avoirkind != 14 and NFI.T_PriceFIID != NFI.T_AccFIID and NFI.T_AccFIID != -1))),0) = 1 )
                             or
                               (NDEAL.T_DVKIND = 8 and NVL((SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = NDEAL.T_Contractor AND T_PARTYKIND = 2),0) = 1 and NFI.T_FIID != NFI.T_PriceFIID)
                             or
                               (NDEAL.T_DVKIND = 6 and NVL((SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = NDEAL.T_Contractor AND T_PARTYKIND = 2),0) = 1 and NDEAL.T_PeriodCls != 0
                                and nvl((select 1 from dfininstr_dbt fin where FIN.T_FIID = NFI.T_FIID and FIN.T_FI_KIND = 1),0) = 1)
                             )
                    )
     LOOP

       rep701.T_DEALID           := one_rec.T_ID;
       rep701.T_DEALDATE         := case when one_rec.T_DVKIND = 4 then one_rec.T_BeginDate else one_rec.T_DATE end;

       if( one_rec.T_DVKIND = 1 or one_rec.T_DVKIND = 2 or one_rec.T_DVKIND = 4 )then
          rep701.T_CALCDATE      := case when one_rec.T_ExecDate = to_date('01.01.0001','DD.MM.YYYY') then to_date('31.12.2999','DD.MM.YYYY') else one_rec.T_ExecDate end;
       else
          rep701.T_CALCDATE      := greatest(one_rec.T_PayDate,one_rec.T_SUPLDATE);
       end if;

       rep701.T_ISMARKET         := case when one_rec.T_IsMarket = 1 then 'X' else chr(0) end;
       rep701.T_ISMONEYMARKET    := chr(0);

       rep701.T_DealPart := 1;

       if( one_rec.T_DVKIND = 4 )then
          rep701.T_ISSINGLEPCSW := case when one_rec.T_IsSinglePcSw = 1 then chr(88) else chr(0) end;
          if( one_rec.T_TYPE = RSB_Derivatives.ALG_DV_FIX_FLOAT and one_rec.T_IsSinglePcSw = 1 )then
             v_REQFIID          := one_rec.T_FIID;
             v_REQSUM           := one_rec.T_AMOUNT/1000;
             v_COMFIID          := -1;
             v_COMSUM           := 0;
             rep701.T_DEALSIDE  := 'S';
          elsif( one_rec.T_TYPE = RSB_Derivatives.ALG_DV_FLOAT_FIX and one_rec.T_IsSinglePcSw = 1 )then
             v_REQFIID          := -1;
             v_REQSUM           := 0;
             v_COMFIID          := one_rec.T_FIID;
             v_COMSUM           := one_rec.T_AMOUNT/1000;
             rep701.T_DEALSIDE  := 'B';
          elsif( one_rec.T_IsSinglePcSw = 0 or one_rec.T_TYPE = RSB_Derivatives.ALG_DV_FLOAT_FLOAT )then
             --DEF-93450 Разновалютный процентный СВОП рынка СПФИ с типом обмена ставок fix/float
             if (one_rec.T_TYPE = RSB_Derivatives.ALG_DV_FIX_FLOAT and one_rec.t_MarketKind = Rsb_Secur.DV_MARKETKIND_SPFIMARKET and (one_rec.t_FIID2 is not null and one_rec.t_FIID != one_rec.t_FIID2)) then 
              rep701.T_DEALSIDE := chr(0);
              if(one_rec.T_NFI_TYPE = 0) then
                   v_REQFIID       := one_rec.T_FIID;
                   v_REQSUM      := one_rec.T_AMOUNT/1000;
                   v_COMFIID      := -1;
                   v_COMSUM     := 0;
                   rep701.T_DealPart := 1;
                else
                   v_REQFIID       := -1;
                   v_REQSUM      := 0;
                   v_COMFIID      := one_rec.T_FIID;
                   v_COMSUM     := one_rec.T_AMOUNT/1000;
                   rep701.T_DealPart := 2;
                end if;   
             else
                if(one_rec.T_NFI_TYPE = 2) then
                   v_REQFIID       := one_rec.T_FIID;
                   v_REQSUM      := one_rec.T_AMOUNT/1000;
                   v_COMFIID      := -1;
                   v_COMSUM     := 0;
                   rep701.T_DealPart := 1;
                   rep701.T_DEALSIDE := 'S';
                else
                   v_REQFIID       := -1;
                   v_REQSUM      := 0;
                   v_COMFIID      := one_rec.T_FIID;
                   v_COMSUM     := one_rec.T_AMOUNT/1000;
                   rep701.T_DealPart := 2;
                   rep701.T_DEALSIDE := 'B';
                end if;   
             end if;           
              
          end if;
       elsif( one_rec.T_DVKIND = 5 and one_rec.T_BA_IsCurr = 0 )then
          v_COST_PAYFIID := NVL(RSI_RSB_FIInstr.ConvSum(one_rec.T_COST, one_rec.T_PRICEFIID, one_rec.T_AccFIID, OnDate, 1),0);
          v_REQFIID := case when one_rec.T_TYPE = 1 then one_rec.T_AccFIID else one_rec.T_PRICEFIID end;
          v_REQSUM  := case when one_rec.T_TYPE = 1 then v_COST_PAYFIID else one_rec.T_COST end;
          v_COMFIID := case when one_rec.T_TYPE = 2 then one_rec.T_AccFIID else one_rec.T_PRICEFIID end;
          v_COMSUM  := case when one_rec.T_TYPE = 2 then v_COST_PAYFIID else one_rec.T_COST end;
       elsif( one_rec.T_DVKIND = 2 )then
          v_REQFIID := case when (one_rec.T_TYPE = 1 and one_rec.T_OptionType = 2) or (one_rec.T_TYPE = 2 and one_rec.T_OptionType = 1) then GetFIIDForIndex(one_rec.T_FIID) else GetFIIDForIndex(one_rec.T_PRICEFIID) end;
          v_REQSUM  := case when (one_rec.T_TYPE = 1 and one_rec.T_OptionType = 2) or (one_rec.T_TYPE = 2 and one_rec.T_OptionType = 1) then one_rec.T_AMOUNT else one_rec.T_COST end;
          v_COMFIID := case when (one_rec.T_TYPE = 2 and one_rec.T_OptionType = 2) or (one_rec.T_TYPE = 1 and one_rec.T_OptionType = 1) then GetFIIDForIndex(one_rec.T_FIID) else GetFIIDForIndex(one_rec.T_PRICEFIID) end;
          v_COMSUM  := case when (one_rec.T_TYPE = 2 and one_rec.T_OptionType = 2) or (one_rec.T_TYPE = 1 and one_rec.T_OptionType = 1) then one_rec.T_AMOUNT else one_rec.T_COST end;

          if ( one_rec.T_IsPrcIndex = 1) then
            if (one_rec.T_TYPE = 1) then -- покупка
               v_REQFIID := -1;
               v_REQSUM  := 0;
               v_COMSUM  := v_COMSUM/1000;
            else
               v_COMFIID := -1;
               v_COMSUM  := 0;
               v_REQSUM  := v_REQSUM/1000;
            end if;
          end if;
       elsif( one_rec.T_DVKIND = 1 )then
          v_REQFIID := case when one_rec.T_TYPE = 1 or (one_rec.T_TYPE = 5 and one_rec.T_NFI_TYPE = 0) or (one_rec.T_TYPE = 6 and one_rec.T_NFI_TYPE = 2) then GetFIIDForIndex(one_rec.T_FIID) else GetFIIDForIndex(one_rec.T_PRICEFIID) end;
          v_REQSUM  := case when one_rec.T_TYPE = 1 or (one_rec.T_TYPE = 5 and one_rec.T_NFI_TYPE = 0) or (one_rec.T_TYPE = 6 and one_rec.T_NFI_TYPE = 2) then one_rec.T_AMOUNT else one_rec.T_COST end;
          v_COMFIID := case when one_rec.T_TYPE = 2 or (one_rec.T_TYPE = 6 and one_rec.T_NFI_TYPE = 0) or (one_rec.T_TYPE = 5 and one_rec.T_NFI_TYPE = 2) then GetFIIDForIndex(one_rec.T_FIID) else GetFIIDForIndex(one_rec.T_PRICEFIID) end;
          v_COMSUM  := case when one_rec.T_TYPE = 2 or (one_rec.T_TYPE = 6 and one_rec.T_NFI_TYPE = 0) or (one_rec.T_TYPE = 5 and one_rec.T_NFI_TYPE = 2) then one_rec.T_AMOUNT else one_rec.T_COST end;
       else
          v_REQFIID := case when one_rec.T_TYPE = 1 or (one_rec.T_TYPE = 5 and one_rec.T_NFI_TYPE = 0) or (one_rec.T_TYPE = 6 and one_rec.T_NFI_TYPE = 2) then one_rec.T_FIID else one_rec.T_PRICEFIID end;
          v_REQSUM  := case when one_rec.T_TYPE = 1 or (one_rec.T_TYPE = 5 and one_rec.T_NFI_TYPE = 0) or (one_rec.T_TYPE = 6 and one_rec.T_NFI_TYPE = 2) then one_rec.T_AMOUNT else one_rec.T_COST end;
          v_COMFIID := case when one_rec.T_TYPE = 2 or (one_rec.T_TYPE = 6 and one_rec.T_NFI_TYPE = 0) or (one_rec.T_TYPE = 5 and one_rec.T_NFI_TYPE = 2) then one_rec.T_FIID else one_rec.T_PRICEFIID end;
          v_COMSUM  := case when one_rec.T_TYPE = 2 or (one_rec.T_TYPE = 6 and one_rec.T_NFI_TYPE = 0) or (one_rec.T_TYPE = 5 and one_rec.T_NFI_TYPE = 2) then one_rec.T_AMOUNT else one_rec.T_COST end;
          
          --BIQ-19605 
          if( one_rec.T_KIND IN (12715, 22710) and (one_rec.T_PRICEFIID = one_rec.T_PRICEFIID2 and one_rec.T_COST = one_rec.T_COST2)) then
             if  (one_rec.T_TYPE = RSB_DERIVATIVES.ALG_DV_BS) then
               if (v_REQFIID = one_rec.T_PRICEFIID and v_REQSUM = one_rec.T_COST) then
                 v_REQSUM  := v_REQSUM + 1;
               end if;
             else
               if (v_COMFIID = one_rec.T_PRICEFIID and v_COMSUM = one_rec.T_COST) then
                v_COMSUM := v_COMSUM + 1;
               end if;
             end if;
          end if;

       end if;
       

       rep701.T_REQFIID          := v_REQFIID;
       rep701.T_REQSUM           := v_REQSUM;
       rep701.T_COMFIID          := v_COMFIID;
       rep701.T_COMSUM           := v_COMSUM;

       rep701.T_ISCLIENT         := case when one_rec.T_CLIENT > 0 then 'X' else chr(0) end;
       rep701.T_ISRESIDENT       := one_rec.T_ISRESIDENT;
       rep701.T_OKCMCODE         := one_rec.T_OKCMCODE;

       if( instr(one_rec.T_partykind,',2,') != 0 and one_rec.T_MarketFromNote IS NOT NULL )then
          rep701.T_MARKETCONTRAGENT  := one_rec.T_MarketFromNote;
          rep701.T_ISEXCHORCENTRCONTR := 2;
       else
          rep701.T_MARKETCONTRAGENT  := one_rec.T_MarketShortName;
          rep701.T_ISEXCHORCENTRCONTR := one_rec.T_IsExchOrCentrContr;
       end if;
       
       if (OnDate >=  BIQ16634_BEGDATE) then
          if (rep701.T_MARKETCONTRAGENT = 'MMVB' AND ((instr(UPPER(one_rec.T_COMMENT), 'RFSM') != 0)
                                                   OR (instr(UPPER(one_rec.T_COMMENT), 'OTCF') != 0)
                                                   OR (instr(UPPER(one_rec.T_COMMENT), 'CNGD') != 0)
                                                   OR (instr(UPPER(one_rec.T_COMMENT), 'SNGD') != 0))) then
              rep701.T_MARKETCONTRAGENT := 'CMOEX';
              
              one_rec.T_Contractor   := CONTRACTOR_NKO_ID;
              one_rec.T_IsResident   := CONTRACTOR_NKO_ISRESIDENT;
          end if;
       end if;
       
       if ((instr(UPPER(one_rec.T_COMMENT), 'CPCL') != 0)
          OR (instr(UPPER(one_rec.T_COMMENT), 'NTPRO DEAL') != 0) ) then
            rep701.T_MARKETCONTRAGENT := 'NTPro';
       end if;
       
       IF ((IsIntermediate = 1) AND ((rep701.T_ISCLIENT != 'X') AND (rep701.T_MARKETCONTRAGENT = 'MMVB')  OR (rep701.T_MARKETCONTRAGENT = 'SPVB')) ) THEN
          CONTINUE;
       END IF;
      
       if (one_rec.t_MarketKind = Rsb_Secur.DV_MARKETKIND_SPFIMARKET) then
          rep701.T_MARKETCONTRAGENT := 'PFIMOEX';
          
          if ((one_rec.t_RelPartyRef > 0)) then
             one_rec.T_Contractor   := one_rec.t_RelPartyRef;
             begin
               select nvl(decode(T_NotResident,'X','N','R'), chr(0)) into one_rec.T_IsResident from dparty_dbt WHERE T_PARTYID = one_rec.t_RelPartyRef;
             exception
               WHEN NO_DATA_FOUND THEN
               one_rec.T_IsResident := chr(0);
            end;
         elsif (one_rec.T_Contractor != CONTRACTOR_NKO_ID ) then
            one_rec.T_Contractor := 0;
            one_rec.T_IsResident := chr(0);
         end if;
         
       end if;
  
       if( one_rec.T_IsMarket = 1  and not ( one_rec.t_MarketKind = Rsb_Secur.DV_MARKETKIND_SPFIMARKET) and rep701.T_MARKETCONTRAGENT not in ('CMOEX', 'NTPro')) then
          rep701.T_BANKCONTRAGENT := chr(1);
          rep701.T_TYPECONTR      := 0;
          rep701.T_PREGNUM        := chr(1);
          rep701.T_BIC            := chr(1);
       else
          GetBankContragentInfo(one_rec.T_Contractor,one_rec.T_ISRESIDENT,0,OnDate,rep701.T_BANKCONTRAGENT,rep701.T_ISSWIFTCODE, rep701.T_TYPECONTR);
          PRegNum (one_rec.T_Contractor, rep701.T_PREGNUM);
          rep701.T_BIC := RSI_RSBPARTY.GetPartyCodeOnDate (one_rec.T_Contractor, PM_COMMON.PTCK_BIC, OnDate, v_owner_BIC);
       end if;

       rep701.T_OTHER            := chr(1);
       rep701.T_DEALADDINFO      := chr(1);
       rep701.T_IS_UNION         := chr(0);
       rep701.T_TYPEADDINFO      := chr(1);
       rep701.T_RATEADDINFO      := 0;
       rep701.T_ISBUY            := chr(0);

       if( one_rec.T_DVKIND = 4 )then
          -- %% свопы не объединяем
          rep701.T_IS_UNION      := chr(0);
          rep701.T_TYPEADDINFO   := chr(1);
          if( rep701.T_ISCLIENT = 'X' AND one_rec.T_SECTOR = 'X')then
             rep701.T_OTHER      := 'C';
          end if;

          if(one_rec.T_TYPE = RSB_Derivatives.ALG_DV_FIX_FIX) or
            (one_rec.T_TYPE = RSB_Derivatives.ALG_DV_FIX_FLOAT and (one_rec.T_IsSinglePcSw = 1 or one_rec.T_NFI_TYPE = 0)) or
            (one_rec.T_TYPE = RSB_Derivatives.ALG_DV_FLOAT_FIX and (one_rec.T_IsSinglePcSw = 1 or one_rec.T_NFI_TYPE = 2))
          then
             rep701.T_RATEADDINFO := round(one_rec.T_Rate,4);
          end if;

          rep701.T_TYPEADDINFO    := case when one_rec.T_SWAPTYPE = 2 then 'OIS' when one_rec.T_IsSinglePcSw = 1 then 'IRS' else 'CS' end||' '|| case when LOWER(one_rec.T_TYPE_NAME) = 'float/fix' then 'fix/float' else LOWER(one_rec.T_TYPE_NAME) end;

          if( (one_rec.T_TYPE = RSB_Derivatives.ALG_DV_FLOAT_FIX and (one_rec.T_IsSinglePcSw = 1 or one_rec.T_NFI_TYPE = 0)) or
              (one_rec.T_TYPE = RSB_Derivatives.ALG_DV_FIX_FLOAT and (one_rec.T_IsSinglePcSw = 1 or one_rec.T_NFI_TYPE = 2)) or
              one_rec.T_TYPE = RSB_Derivatives.ALG_DV_FLOAT_FLOAT
            )then
             GetFloatRate(one_rec.T_ID,case when one_rec.T_TYPE = RSB_Derivatives.ALG_DV_FIX_FLOAT then 2
                                            when one_rec.T_TYPE = RSB_Derivatives.ALG_DV_FLOAT_FIX then 0
                                            when one_rec.T_TYPE = RSB_Derivatives.ALG_DV_FLOAT_FLOAT then one_rec.T_NFI_TYPE end, one_rec.T_FIID,
                          rep701.T_DEALADDINFO, rep701.T_RATECCY, rep701.T_RATENAME, rep701.T_RATESPREAD);
          end if;     

          if ( rep701.T_MARKETCONTRAGENT = 'MMVB' AND one_rec.T_ISRESIDENT = 'R') then
            rep701.T_MARKETCONTRAGENT := 'PFIMOEX';
          end if;       

          if ( rep701.T_MARKETCONTRAGENT NOT IN ('EUREX','CME','NYSE','PFIMOEX')) then
             rep701.T_OTHER := case when rep701.T_TYPECONTR = 0 OR rep701.T_TYPECONTR = '' OR rep701.T_TYPECONTR IS NULL
                                     then  'M'
                                    else  chr(1)
                               end;
          end if;

          if(rep701.T_TYPEADDINFO IN ('CS fix/fix','CS fix/float') AND rep701.T_DEALADDINFO = chr(1)) then
             rep701.T_RATEADDINFO := round(one_rec.T_Rate,4);
          end if;
         
          if (one_rec.T_SWAPTYPE = 2 and rep701.T_DEALSIDE = 'S' ) then
             rep701.T_DEALSIDE := 'B';
          elsif (one_rec.T_SWAPTYPE = 2 and rep701.T_DEALSIDE = 'B' ) then
             rep701.T_DEALSIDE := 'S';
          end if;

          rep701.T_DLKIND       := 8;-- %% своп
       elsif( one_rec.T_DVKIND = 3 or one_rec.T_DVKIND = 6 )then
          --свопы короткие и длинные не объединяем
          rep701.T_IS_UNION     := chr(0);
          rep701.T_DEALADDINFO  := 'S';
          if( one_rec.T_NFI_TYPE = 0 )then
             rep701.T_DealPart  := 1;
          else
             rep701.T_DealPart  := 2;
          end if;
          if( rep701.T_ISCLIENT = 'X' AND one_rec.T_SECTOR = 'X')then
             rep701.T_OTHER     := 'C';
          end if;

          rep701.T_DLKIND       := 7;--сделки своп: короткий и длинный

          rep701.T_ISBUY    := case when one_rec.T_TYPE = 1 or (one_rec.T_TYPE = 5 and one_rec.T_NFI_TYPE = 0) or (one_rec.T_TYPE = 6 and one_rec.T_NFI_TYPE = 2) then 'X' else chr(0) end;
          rep701.T_DEALSIDE := case when rep701.T_ISBUY = 'X' then 'B' else 'S' end;
       else
          rep701.T_ISBUY    := case when one_rec.T_TYPE = 1 or (one_rec.T_TYPE = 5 and one_rec.T_NFI_TYPE = 0) or (one_rec.T_TYPE = 6 and one_rec.T_NFI_TYPE = 2) then 'X' else chr(0) end;
          rep701.T_DEALSIDE := case when rep701.T_ISBUY = 'X' then 'B' else 'S' end;
        
          if (rep701.T_ISCLIENT != 'X' and (rep701.T_MARKETCONTRAGENT = 'MMVB' OR rep701.T_MARKETCONTRAGENT = 'SPVB') and OnDate < RSBSESSIONDATA.curdate) then --собственные биржевые 
            if(  (instr(one_rec.T_partykind,',3,') != 0) or (one_rec.T_Contractor = CONTRACTOR_NKO_ID) ) then -- контрагент - биржа
             rep701.T_IS_UNION := 'X';
            end if;
          end if;
          
          if( one_rec.T_DVKIND = 2 )then
             if ( one_rec.T_IsPrcIndex = 0) then
               if( one_rec.T_OptionType = 1 )then
                  rep701.T_TYPEADDINFO := 'P';
               else
                  rep701.T_TYPEADDINFO := 'C';
               end if;
             else
               if( one_rec.T_OptionType = 1 )then
                  rep701.T_TYPEADDINFO := 'Floor';
               else
                  rep701.T_TYPEADDINFO := 'Cap';
               end if;

               GetPrcFloatRate(one_rec.T_ID, one_rec.T_NFI_TYPE, one_rec.T_FIID, rep701.T_DEALADDINFO, rep701.T_RATECCY, rep701.T_RATENAME, rep701.T_RATESPREAD);
             end if;
             if( one_rec.T_Bonus != 0 ) then
                rep701.T_RATEADDINFO   := round(NVL(RSI_RSB_FIInstr.ConvSum(one_rec.T_Bonus, one_rec.T_BonusFIID, FIID_840, one_rec.T_DATE, 1),1)/1000,3);
             else
                rep701.T_RATEADDINFO   := 0.001;
             end if;
             if( one_rec.T_TYPE = 1 )then
                rep701.T_RATEADDINFO   := -rep701.T_RATEADDINFO;
             end if;
          elsif( one_rec.T_DVKIND = 1 or one_rec.T_DVKIND = 5 )then
             rep701.T_TYPEADDINFO := GetTypeFinActive(one_rec.T_FIID);
          end if;
          if( one_rec.T_DVKIND = 2 and one_rec.T_IsPrcIndex = 0)then
             rep701.T_DLKIND := 5;
          elsif  ( one_rec.T_DVKIND = 2 and one_rec.T_IsPrcIndex = 1)then
             rep701.T_DLKIND := 8;
          else
             rep701.T_DLKIND := 3;
          end if;
          /*для определения T_OTHER нужен уже заполненный T_TYPEADDINFO, поэтому заполняем T_TYPEADDINFO раньше T_OTHER*/
          if rep701.T_ISCLIENT = 'X' and rep701.T_IS_UNION != 'X' then
             rep701.T_OTHER         := 'C';
          elsif( one_rec.T_IsMarket = 0 )then
             if( one_rec.T_DvKind = 8 )then
                rep701.T_OTHER  := 'B';
             else
                rep701.T_OTHER  := GetOTHERInfo(one_rec.T_Contractor,rep701.T_ISCLIENT,one_rec.T_partykind,one_rec.T_COST,one_rec.T_PRICEFIID,one_rec.T_DATE,FIID_840,rep701.T_TYPEADDINFO);
             end if;
          end if;

       end if;

       if( (one_rec.T_DVKIND != 4 and one_rec.T_DVKIND != 7 and one_rec.T_DVKIND != 8) and one_rec.T_ExecType = 0 and not (one_rec.T_DVKIND = 2 and one_rec.T_IsPrcIndex = 1)) then
          rep701.T_DEALADDINFO := 'O';
       end if;

       rep701.T_METHODADDINFO := case when (instr(one_rec.T_PartyKind,',29,') != 0 or instr(one_rec.T_PartyKind,',27,') != 0) then 'DE'
                                      when  one_rec.T_IsMarket = 1 then 'IE'
                                          when (one_rec.T_IsMarket = 0 and one_rec.T_TransCategory = 4 and one_rec.T_AGENT = -1) then 'DV'
                                          when (one_rec.T_IsMarket = 0 and one_rec.T_TransCategory = 4 and one_rec.T_AGENT != -1) then 'IV'
                                          when (one_rec.T_IsMarket = 0 and (one_rec.T_TransCategory = 2 or one_rec.T_TransCategory = 3)) then 'DE' 
                                          else 'DE' end;
          
       rep701.T_CONTRACTORID := one_rec.T_Contractor;

       rep701.T_ISITOG := chr(0);
       
       if (rep701.T_IsMoneyMarket != 'X' and RSB_DLREP701_MZ_2293.IsNeedGr17_18(rep701.T_ContractorID) = 1)  then 
          --Графы не заполняются по валютным сделкам: форвард, фьючерс, опцион, валютный своп
          --Графы заполняются только для внебиржевых поставочных сделок "Покупка/продажа безналичная" со сроком не более Т+2, по которым объем требований или обязательств составляет не менее 1млн единиц валюты. 
          if (one_rec.T_DVKIND not in (1, 2, 3, 7)) then
             rep701.T_ADDGRFLAG := 1;
          elsif ( one_rec.T_DVKIND = 7 and one_rec.t_PeriodCls <= 2 ) then
             rep701.T_ADDGRFLAG := 2;
          else
             rep701.T_ADDGRFLAG := 0;
          end if;
       else 
           rep701.T_ADDGRFLAG := 0;
       end if;

       g_rep701_ins.extend;
       g_rep701_ins(g_rep701_ins.LAST) := rep701;

       if( rep701.T_ISCLIENT = 'X' and rep701.T_IS_UNION != 'X' )then

          if( one_rec.T_DVKIND = 3 or one_rec.T_DVKIND = 6 or one_rec.T_DVKIND = 4 )then
             if( one_rec.T_NFI_TYPE = 0 )then
                rep701.T_DealPart := 3;
             else
                rep701.T_DealPart := 4;
             end if;
          else
             rep701.T_DealPart := 2;
          end if;

          rep701.T_MARKETCONTRAGENT := 'MD';
          rep701.T_ISEXCHORCENTRCONTR := 2;
          rep701.T_OTHER            := GetOTHERInfo(one_rec.T_CLIENT,rep701.T_ISCLIENT,one_rec.T_clientkind,one_rec.T_COST,one_rec.T_PRICEFIID,one_rec.T_DATE,FIID_840);
          rep701.T_COMFIID          := v_REQFIID;
          rep701.T_COMSUM           := v_REQSUM;
          rep701.T_REQFIID          := v_COMFIID;
          rep701.T_REQSUM           := v_COMSUM;
          rep701.T_DEALSIDE         := case when rep701.T_DEALSIDE = 'B' then 'S' else 'B' end;

          g_rep701_ins.extend;
          g_rep701_ins(g_rep701_ins.LAST) := rep701;

       end if;

     END LOOP;

     IF g_rep701_ins IS NOT EMPTY THEN
         FORALL i IN g_rep701_ins.FIRST .. g_rep701_ins.LAST
              INSERT INTO DDLREP701_TMP
                   VALUES g_rep701_ins(i);
         g_rep701_ins.delete;
     END IF;

  END CreateData_DV_NDEAL;

  PROCEDURE CreateData_DV_DEAL(DepartmentID IN NUMBER, OnDate IN DATE, FIID_840 IN NUMBER, CodeType IN NUMBER, IsIntermediate NUMBER DEFAULT 0)
  IS
     v_REQFIID NUMBER := -1;
     v_REQSUM  NUMBER := 0;
     v_COMFIID NUMBER := -1;
     v_COMSUM  NUMBER := 0;
     TYPE rep701_t IS TABLE OF DDLREP701_TMP%ROWTYPE;
     g_rep701_ins rep701_t := rep701_t();
     rep701 DDLREP701_TMP%rowtype;
  BEGIN
     FOR one_rec IN ( select DEAL.T_CLIENT, DEAL.T_PositionCost, DEAL.T_DATE, DEAL.T_TYPE, DEAL.T_ID, DEAL.T_FIID, DEAL.T_PositionBonus,
                             Fideriv.T_TickFIID, Fideriv.T_LastCirculationDate, Fideriv.T_OptionType, FIN.T_DRAWINGDATE, FIN.T_SETTLEMENT_CODE, FIN.T_AVOIRKIND, FIN.T_ISSUER, FIN.T_FACEVALUE * DEAL.T_AMOUNT T_AMOUNT_BA,
                             case when Fideriv.t_PriceMode = 0/*DERIVATIVE_MODE_TICKFI*/ then case when Fideriv.t_TickFIID >= 0 then Fideriv.t_TickFIID else FIn.t_ParentFI end else FIn.t_ParentFI end t_PRICEFIID,
                             FIn.t_ParentFI t_SumPriceFIID,
                             case when nvl((select 1 from dfininstr_dbt f1 where f1.T_FIID = FIN.T_FACEVALUEFI and f1.T_FI_KIND = 1 ),0) = 1
                                      then FIN.T_FACEVALUEFI
                                  when nvl((select 1 from dfininstr_dbt f1 where f1.T_FIID = FIN.T_FACEVALUEFI and f1.T_FI_KIND = 3 ),0) = 1
                                      then  nvl((select F1.T_FACEVALUEFI from dfininstr_dbt f1 where f1.T_FIID = FIN.T_FACEVALUEFI),-1)
                                  when nvl((select 1 from dfininstr_dbt f1 where f1.T_FIID = FIN.T_FACEVALUEFI and f1.T_FI_KIND = 4 ),0) = 1
                                      then
                                           case when nvl((select 1 from dfininstr_dbt f2 where f2.T_FIID = nvl((select F1.T_FACEVALUEFI from dfininstr_dbt f1 where f1.T_FIID = FIN.T_FACEVALUEFI),-1)  and f2.T_FI_KIND = 1 ),0) = 1
                                                    then nvl((select F1.T_FACEVALUEFI from dfininstr_dbt f1 where f1.T_FIID = FIN.T_FACEVALUEFI),-1)
                                                    else nvl((select  F2.T_FACEVALUEFI from dfininstr_dbt f2 where f2.T_FIID = nvl((select F1.T_FACEVALUEFI from dfininstr_dbt f1 where f1.T_FIID = FIN.T_FACEVALUEFI),-1)),-1) end
                                  end T_FIID_BA,
                             NVL((SELECT DECODE(T_NotResident,'X','N','R') from dparty_dbt WHERE T_PARTYID = FIN.T_ISSUER),chr(0)) T_ISRESIDENT,
                             NVL((select country.T_CodeNum3 from DCOUNTRY_DBT country, DPARTY_DBT party
                                  where country.t_Codelat3 = party.T_NRCOUNTRY
                                    and party.T_PARTYID = FIN.T_ISSUER
                                    and party.T_NotResident = 'X' --заполняем только для нерезидентов
                                    and rownum = 1 ),chr(1)) T_OKCMCODE,
                             NVL(( SELECT DECODE(Rsb_Secur.SC_GetObjCodeOnDate(cnst.OBJTYPE_PARTY, cnst.PTCK_CONTR, T_PARTYID),'ММВБ','FORTS','ФБ СПБ','SPBEX','АО СПВБ','SPVB', t_shortname) --! После ввода Санк-Петербургской биржи добавить код SPVB сюда
                                     FROM DPARTY_DBT
                                    WHERE T_PARTYID = FIN.T_ISSUER),chr(1)) T_ContractorShortName,
                            FIN.T_ISSUER t_ContractorID,
                             cast(case when nvl((select 1 from dfininstr_dbt f1 where f1.T_FIID = FIN.T_FACEVALUEFI and f1.T_FI_KIND = 4 ),0) = 1
                                  then nvl((select to_char(F1.T_DRAWINGDATE,'MMYYYY') from dfininstr_dbt f1 where f1.T_FIID = FIN.T_FACEVALUEFI),chr(1)) end as VARCHAR2(6)) T_FutDrwDate,
                             NVL(','||(SELECT LISTAGG(t_partykind, ',') WITHIN GROUP (ORDER BY t_partykind)
                                         FROM dpartyown_dbt
                                       WHERE t_partyid = FIN.T_ISSUER)||',',chr(1)) T_partykind,
                             NVL(','||(SELECT LISTAGG(t_partykind, ',') WITHIN GROUP (ORDER BY t_partykind)
                                         FROM dpartyown_dbt
                                        WHERE t_partyid = DEAL.T_CLIENT)||',',chr(1)) T_clientkind,
                             NVL( (SELECT Attr.t_NumInList
                                     FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
                                    WHERE AtCor.t_ObjectType = 18 /*Производный инструмент*/
                                      AND AtCor.t_GroupID    = 2 /*Однодневный фьючерс*/
                                      AND AtCor.t_Object     = LPAD(FIN.T_FIID, 10, '0')
                                      AND Attr.t_AttrID      = AtCor.t_AttrID
                                      AND Attr.t_ObjectType  = AtCor.t_ObjectType
                                      AND Attr.t_GroupID     = AtCor.t_GroupID), 0) OneDay, 
                            case when Exists(SELECT 1 FROM DPARTYOWN_DBT WHERE T_PARTYID = FIN.T_ISSUER AND T_PARTYKIND in(3,58)) then 1 else 2 end T_IsExchOrCentrContr
                        from ddvdeal_dbt deal, dfininstr_dbt fin, dfideriv_dbt Fideriv
                       where DEAL.T_TYPE in('B','S')
                         and DEAL.T_DATE = OnDate
                         and DEAL.T_STATE > CASE WHEN IsIntermediate = 1 THEN -1 ELSE 0 END
                         --AND 1 = CASE WHEN IsIntermediate = 1 THEN TimeIsIntermediate(DEAL.T_TIME) ELSE 1 END
                         and DEAL.T_DEPARTMENT = case when DepartmentID != -1 then DepartmentID else DEAL.T_DEPARTMENT end
                         and ( (CodeType = DL_EXTCODETYPE_OWN    and DEAL.T_CLIENT = -1) or
                               (CodeType = DL_EXTCODETYPE_CLIENT and DEAL.T_CLIENT != -1)
                             )
                         and FIN.T_FIID = deal.T_FIID
                         and Fideriv.T_FIID = deal.T_FIID
                         and (nvl((select 1 from dfininstr_dbt f1 where f1.T_FIID = FIN.T_FACEVALUEFI and (f1.T_FI_KIND = 1 or (f1.T_FI_KIND = 3 and f1.T_SETTLEMENT_CODE in(1,3))) ),0) = 1
                              or nvl((select 1 from dfininstr_dbt f3
                                       where f3.T_FIID =  nvl((select f2.T_FACEVALUEFI from dfininstr_dbt f2 where f2.T_FIID = FIN.T_FACEVALUEFI and f2.T_FI_KIND = 4),-1)
                                         and (f3.T_FI_KIND = 1 or (f3.T_FI_KIND = 3 and f3.T_SETTLEMENT_CODE in(1,3))) ), 0) = 1
                             )
                      )
     LOOP

       rep701.T_DEALID           := one_rec.T_ID;
       rep701.T_DEALDATE         := one_rec.T_DATE;
       IF (one_rec.OneDay = 0) THEN
         rep701.T_CALCDATE       := one_rec.T_LastCirculationDate;
       ELSE
         rep701.T_CALCDATE       := to_date('31.12.2999','DD.MM.YYYY');
       END IF;

       rep701.T_FIID             := one_rec.T_FIID;

       --всё то что проверяется в when, проверяется, является ли сделка покупкой
       --для фьючерсов (T_AVOIRKIND = 1) доп условий не нужно, а для опционов (T_AVOIRKIND = 2) проверяется дополнительно, является ли опцион call (T_OptionType = 2)
       v_REQFIID := case when one_rec.T_TYPE = 'B' and (one_rec.T_AVOIRKIND = 1 or (one_rec.T_AVOIRKIND = 2 and one_rec.T_OptionType = 2)) then one_rec.T_FIID_BA else one_rec.T_PRICEFIID end;
       v_REQSUM  := case when one_rec.T_TYPE = 'B' and (one_rec.T_AVOIRKIND = 1 or (one_rec.T_AVOIRKIND = 2 and one_rec.T_OptionType = 2)) then one_rec.T_AMOUNT_BA else one_rec.T_PositionCost end;
       v_COMFIID := case when one_rec.T_TYPE = 'B' and (one_rec.T_AVOIRKIND = 1 or (one_rec.T_AVOIRKIND = 2 and one_rec.T_OptionType = 2)) then one_rec.T_PRICEFIID else one_rec.T_FIID_BA end;
       v_COMSUM  := case when one_rec.T_TYPE = 'B' and (one_rec.T_AVOIRKIND = 1 or (one_rec.T_AVOIRKIND = 2 and one_rec.T_OptionType = 2)) then one_rec.T_PositionCost else one_rec.T_AMOUNT_BA end;

       rep701.T_REQFIID          := v_REQFIID;
       rep701.T_REQSUM           := v_REQSUM;
       rep701.T_COMFIID          := v_COMFIID;
       rep701.T_COMSUM           := v_COMSUM;

       rep701.T_ISCLIENT         := case when one_rec.T_CLIENT > 0 then 'X' else chr(0) end;
       rep701.T_ISRESIDENT       := one_rec.T_ISRESIDENT;
       rep701.T_OKCMCODE         := one_rec.T_OKCMCODE;

       rep701.T_MARKETCONTRAGENT := one_rec.T_ContractorShortName;
       rep701.T_ISEXCHORCENTRCONTR := one_rec.T_IsExchOrCentrContr;
       rep701.T_BANKCONTRAGENT   := chr(1);

       rep701.T_OTHER            := case when one_rec.T_CLIENT > 0 then 'C' else chr(1) end;--данную строчку закомментить, если попросят объединять
       --rep701.T_OTHER          := chr(1);--данную строчку вернуть, если попросят объединять

       rep701.T_DEALADDINFO      := chr(1);
       rep701.T_TYPEADDINFO      := chr(1);
       rep701.T_RATEADDINFO      := 0;

       rep701.T_ISMARKET         := 'X';
       rep701.T_ISMONEYMARKET := chr(0);
       rep701.T_DealPart := 1;

       rep701.T_IS_UNION         := chr(0);--попросили не объединять
       --rep701.T_IS_UNION         := case when one_rec.T_CLIENT > 0 then 'X' else chr(0) end;--по собственным сделкам позже будем проверять, надо объединять или нет

       rep701.T_ISBUY := case when one_rec.T_TYPE = 'B' then 'X' else chr(0) end;

       if( one_rec.T_SETTLEMENT_CODE = 0 )then
          rep701.T_DEALADDINFO   := 'O';
       end if;

       if( one_rec.T_AVOIRKIND = 2 )then
          if( one_rec.T_TYPE = 'S' )then
             rep701.T_TYPEADDINFO := 'P';
          else
             rep701.T_TYPEADDINFO := 'C';
          end if;
          if( one_rec.T_FutDrwDate != chr(1) ) then
             rep701.T_TYPEADDINFO := rep701.T_TYPEADDINFO||one_rec.T_FutDrwDate;
          end if;
          if( one_rec.T_PositionBonus != 0 ) then
             rep701.T_RATEADDINFO   := round(NVL(RSI_RSB_FIInstr.ConvSum(one_rec.T_PositionBonus, one_rec.T_TickFIID, FIID_840, one_rec.T_DATE, 1),1)/1000,3);
          else
             rep701.T_RATEADDINFO   := 0.001;
          end if;
          if( one_rec.T_TYPE = 'B' )then
             rep701.T_RATEADDINFO   := -rep701.T_RATEADDINFO;
          end if;
          rep701.T_DLKIND := DLKIND_DVOPTION;
       else
          rep701.T_DLKIND := DLKIND_DVFUTURES;
       end if;

       rep701.T_DEALSIDE := one_rec.T_TYPE;

       rep701.T_METHODADDINFO := case when (instr(one_rec.T_PartyKind,',29,') != 0 or instr(one_rec.T_PartyKind,',27,') != 0) then 'DE' else 'IE' end;
          
       rep701.T_CONTRACTORID := one_rec.T_ContractorID;

       rep701.T_ISITOG := chr(0);

       g_rep701_ins.extend;
       g_rep701_ins(g_rep701_ins.LAST) := rep701;

       if rep701.T_ISCLIENT = 'X' then--этот блок закомментить, если попросят объединять

           rep701.T_DealPart := 2;

           rep701.T_MARKETCONTRAGENT := 'MD';
           rep701.T_ISEXCHORCENTRCONTR := 2;
           rep701.T_OTHER            := GetOTHERInfo(one_rec.T_CLIENT,rep701.T_ISCLIENT,one_rec.T_clientkind,one_rec.T_PositionCost,one_rec.T_PRICEFIID,one_rec.T_DATE,FIID_840);

           rep701.T_COMFIID          := v_REQFIID;
           rep701.T_COMSUM           := v_REQSUM;
           rep701.T_REQFIID          := v_COMFIID;
           rep701.T_REQSUM           := v_COMSUM;

           rep701.T_DEALSIDE         := case when rep701.T_DEALSIDE = 'B' then 'S' else 'B' end;

           g_rep701_ins.extend;
           g_rep701_ins(g_rep701_ins.LAST) := rep701;

       end if;

     END LOOP;

     IF g_rep701_ins IS NOT EMPTY THEN
         FORALL i IN g_rep701_ins.FIRST .. g_rep701_ins.LAST
              INSERT INTO DDLREP701_TMP
                   VALUES g_rep701_ins(i);
         g_rep701_ins.delete;
     END IF;

  END CreateData_DV_DEAL;

  PROCEDURE CreateData_MBK(DepartmentID IN NUMBER, OnDate IN DATE, FIID_840 IN NUMBER, IsIntermediate NUMBER DEFAULT 0)
  IS
     v_DeltaSum NUMBER;
     TYPE rep701_t IS TABLE OF DDLREP701_TMP%ROWTYPE;
     g_rep701_ins rep701_t := rep701_t();
     rep701 DDLREP701_TMP%rowtype;
     v_owner_BIC NUMBER;
  BEGIN
     FOR one_rec IN ( select TICK.T_DEALDATE, TICK.T_DEALID, TICK.T_DEALCODE, TICK.T_TYPEDOC, LEG.T_LEGKIND, TICK.T_FLAGTYPEDEAL, TICK.T_CLIENTID, TICK.T_PARTYID, TICK.T_BROKERID,
                             LEG.T_PFI, LEG.T_TablePercent, LEG.T_TypePercent, LEG.T_Caption, LEG.T_CORRECT,
                             LEG.T_COST/1000 T_COST, LEG.T_PRINCIPAL/1000 T_PRINCIPAL,
                             LEG.T_MATURITY, leg.t_Start, LEG.T_DURATION, LEG.T_PRICE /greatest(1,power(10,leg.t_Point)) T_RATE,
                             LEG.T_SROK, LEG.T_PERIODTYPE, LEG.T_PERIODNUMBER,
                             MMark_UTL.IsBUY(tick.t_DealGroup) T_IsBUY,
                             NVL((SELECT DECODE(T_NotResident,'X','N','R') from dparty_dbt WHERE T_PARTYID = TICK.T_PARTYID),chr(0)) T_ISRESIDENT,
                             NVL((select country.T_CodeNum3 from DCOUNTRY_DBT country, DPARTY_DBT party
                                  where country.t_Codelat3 = party.T_NRCOUNTRY
                                    and party.T_PARTYID = TICK.T_PARTYID
                                    and party.T_NotResident = 'X' --заполняем только для нерезидентов
                                    and rownum = 1 ),chr(1)) T_OKCMCODE,
                             case when LEG.T_TablePercent = 'X' and LEG.T_TypePercent = 1 then
                             NVL((SELECT fin.t_name
                                    from dfininstr_dbt fin
                                   WHERE fin.t_FIID = LEG.T_Caption),chr(1)) else chr(1) end T_RATENAME,
                             case when LEG.T_TablePercent = 'X' and LEG.T_TypePercent = 1 then
                             NVL((SELECT fin.t_Duration||nvl((select case when ALG.T_INUMBERALG = 1 then 'D'
                                                                          when ALG.T_INUMBERALG = 2 then 'W'
                                                                          when ALG.T_INUMBERALG = 3 then 'M'
                                                                          else 'Y' end
                                                                from dnamealg_dbt alg where ALG.T_ITYPEALG = 2350 and ALG.T_INUMBERALG = fin.t_TypeDuration ),'')||'_'||
                                         fin.t_name||ltrim(to_char(LEG.T_CORRECT,'S9999990D9999'))
                                    from dfininstr_dbt fin
                                   WHERE fin.t_FIID = LEG.T_Caption),chr(1)) else chr(1) end T_DEALADDINFO,
                             case when Exists(select 1 from DPARTYOWN_DBT
                                    where T_PARTYID = TICK.T_PARTYID
                                      and T_PARTYKIND in(2,52) and rownum =1)
                                   and Exists(select 1 from dnotetext_dbt where t_objecttype=103 and t_documentid=TICK.T_DEALID
                                             and OnDate between t_Date and t_ValidToDate and t_notekind=15)
                                then MMarkCommon.GetNoteTextString(103, TICK.T_DEALID, 15, OnDate)
                             when instr(UPPER(TICK.T_Comment),'BLOOMBERG') != 0 then 'BBLG' else
                             nvl((select case when ALG.T_INUMBERALG = 0 then 'DO'
                                                when ALG.T_INUMBERALG = 1 then 'RTRS'
                                                when ALG.T_INUMBERALG = 3 then 'PHONE'
                                                when ALG.T_INUMBERALG = 4 then 'MMVB'
                                                when ALG.T_INUMBERALG = 5 then 'BBLG'
                                                else 'RTRS' end
                                      from dnamealg_dbt alg where ALG.T_ITYPEALG = 2118 and ALG.T_INUMBERALG = TICK.T_LinkChannel ),'RTRS') end T_MARKETCONTRAGENT,
                             NVL(','||(SELECT LISTAGG(t_partykind, ',') WITHIN GROUP (ORDER BY t_partykind)
                                         FROM dpartyown_dbt
                                        WHERE t_partyid = TICK.T_PARTYID)||',',chr(1)) T_partykind
                        from ddl_tick_dbt tick, ddl_leg_dbt leg, dfininstr_dbt fin
                       where TICK.T_BOFFICEKIND = 102
                         and TICK.T_DEALDATE = OnDate
                         and TICK.T_DEALSTATUS > CASE WHEN IsIntermediate = 1 THEN -1 ELSE 0 END
                         AND 1 = CASE WHEN IsIntermediate = 1 THEN TimeIsIntermediate(TICK.T_DEALTIME) ELSE 1 END
                         and TICK.T_DEPARTMENT = case when DepartmentID != -1 then DepartmentID else TICK.T_DEPARTMENT end
                         and TICK.T_FLAGTYPEDEAL != 3 and not(TICK.T_FLAGTYPEDEAL = 2 and TICK.T_TYPEDOC = 'L')
                         and not (TICK.T_AutoPlacement = 'X' and TICK.T_FLAGTYPEDEAL = 1)
                         and LEG.T_DEALID = TICK.T_DEALID
                         and leg.t_LegKind = 0
                         and LEG.T_LEGID = 1
                         and LEG.T_MATURITY != leg.t_Start
                         and LEG.T_PRINCIPAL >= 1000
                         and FIN.T_FIID = LEG.T_PFI
                         and FIN.T_FI_KIND = 1
                         and TICK.T_DEALTYPE != 12335
                      )
     LOOP

       rep701.T_DEALID           := one_rec.T_DEALID;
       rep701.T_DEALPART      := 1;
       rep701.T_DEALDATE         := one_rec.t_Start;
       rep701.T_CALCDATE         := one_rec.T_MATURITY;

       rep701.T_REQFIID          := one_rec.T_PFI;
       rep701.T_REQSUM           := case when one_rec.T_IsBUY > 0 then one_rec.T_PRINCIPAL else one_rec.T_PRINCIPAL + one_rec.T_COST end;
       rep701.T_COMFIID          := one_rec.T_PFI;
       rep701.T_COMSUM           := case when one_rec.T_IsBUY > 0 then one_rec.T_PRINCIPAL + one_rec.T_COST else one_rec.T_PRINCIPAL end;

       rep701.T_ISCLIENT         := chr(0);--оставляю так пока, из постановки непонятно, что считать посреднической сделкой (возможно, сделку с контрагентом с принадлежностью банк)

       rep701.T_ISRESIDENT       := one_rec.T_ISRESIDENT;
       rep701.T_OKCMCODE         := one_rec.T_OKCMCODE;

       rep701.T_MARKETCONTRAGENT := one_rec.T_MARKETCONTRAGENT;
       rep701.T_ISEXCHORCENTRCONTR := 2;
       rep701.T_ISMONEYMARKET := 'X';

       GetBankContragentInfo(one_rec.T_PARTYID,one_rec.T_ISRESIDENT,1,OnDate,rep701.T_BANKCONTRAGENT,rep701.T_ISSWIFTCODE, rep701.T_TYPECONTR);
       PRegNum (one_rec.T_PARTYID, rep701.T_PREGNUM);
       rep701.T_BIC := RSI_RSBPARTY.GetPartyCodeOnDate (one_rec.T_PARTYID, PM_COMMON.PTCK_BIC, OnDate, v_owner_BIC);

       if( one_rec.T_SROK = 'X' )then
          rep701.T_OTHER := 'L';
       else
          rep701.T_OTHER := chr(1);
       end if;

       if( one_rec.T_TYPEDOC = 'L' or one_rec.T_TYPEDOC = 'D' )then
          rep701.T_TYPEADDINFO   := 'DEPO';
       end if;

       if( one_rec.T_TablePercent = 'X' and one_rec.T_TypePercent = 1 )then
          rep701.T_RATEADDINFO   := 0;
          rep701.T_DEALADDINFO   := one_rec.T_DEALADDINFO;
          rep701.T_RATENAME      := one_rec.T_RATENAME;
          rep701.T_RATESPREAD    := one_rec.T_CORRECT;
       else
          rep701.T_RATEADDINFO   := one_rec.T_RATE;
          rep701.T_DEALADDINFO   := chr(1);
          rep701.T_RATENAME      := chr(1);
          rep701.T_RATESPREAD    := 0;
       end if;

       rep701.T_IS_UNION         := chr(0);--про объединение ничего не пишут, значит, не объединяем

       rep701.T_ISBUY := case when one_rec.T_IsBUY > 0 then 'X' else chr(0) end;
      
       rep701.T_DEALSIDE := case when rep701.T_ISBUY = 'X' then 'B' else 'S' end;

       rep701.T_DLKIND := 9;

       rep701.T_ISITOG := chr(0);

       rep701.T_ISMARKET := chr(0);

       rep701.T_METHODADDINFO    := case when (instr(one_rec.T_PartyKind,',29,') != 0 or instr(one_rec.T_PartyKind,',27,') != 0) then 'DE'
                                         when  one_rec.T_MarketContragent = 'PHONE' then 'DV'
                                         when (one_rec.T_MarketContragent = 'RTRS' or one_rec.T_MarketContragent = 'BBLG' or one_rec.T_MarketContragent = 'DO') then 'DE'
                                         else 'DE' end;
                                         
       rep701.T_CONTRACTORID := one_rec.T_PartyID;

       g_rep701_ins.extend;
       g_rep701_ins(g_rep701_ins.LAST) := rep701;

     END LOOP;

     IF g_rep701_ins IS NOT EMPTY THEN
         FORALL i IN g_rep701_ins.FIRST .. g_rep701_ins.LAST
              INSERT INTO DDLREP701_TMP
                   VALUES g_rep701_ins(i);
         g_rep701_ins.delete;
     END IF;

  END CreateData_MBK;

  PROCEDURE CreateData_SC(DepartmentID IN NUMBER, OnDate IN DATE, FIID_840 IN NUMBER, CodeType IN NUMBER, IsIntermediate NUMBER DEFAULT 0)
  IS
  BEGIN
      CreateData_REPO(DepartmentID,OnDate,CodeType, IsIntermediate);
      CreateData_NotREPO(DepartmentID,OnDate,FIID_840,CodeType, IsIntermediate);
  END CreateData_SC;

  PROCEDURE CreateData_DV(DepartmentID IN NUMBER, OnDate IN DATE, FIID_840 IN NUMBER, MarketKind IN NUMBER, CodeType IN NUMBER, IsIntermediate NUMBER DEFAULT 0)
  IS
  BEGIN
      CreateData_DV_NDEAL(DepartmentID,OnDate,FIID_840,MarketKind,CodeType, IsIntermediate);
      IF MarketKind = DV_MARKETKIND_DERIV THEN -- Биржевые операции с опционами и фьючерсами отбираются, если в панели установлен признак "биржевой срочный рынок"
         CreateData_DV_DEAL(DepartmentID,OnDate,FIID_840,CodeType, IsIntermediate);
      END IF;
  END CreateData_DV;

  PROCEDURE CorrectData(OnDate IN DATE, p_FIID_840 IN NUMBER, IsIntermediate NUMBER DEFAULT 0)
  IS
    v_ActDateStr VARCHAR2 (10):= Rsb_Common.GetRegStrValue('РСХБ\ДАТА АКТ. Ф. 0409701 (6406-У)', 0);
    v_ActDate DATE  := TO_DATE('01.07.2025', 'DD.MM.YYYY');
    TYPE dlrep701_t IS TABLE OF DDLREP701_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
    v_dlrep701 dlrep701_t;
  BEGIN
  
   IF v_ActDateStr <> CHR(1) THEN
       v_ActDate := TO_DATE(v_ActDateStr, 'DD.MM.YYYY');
   END IF;

     --попросили не объединять
    -- помечаем к объединению собственные биржевые фьючерсы\опционы
    
   if (IsIntermediate = 0 and OnDate < RSBSESSIONDATA.curdate) then
    update  DDLREP701_TMP tmp
       set TMP.T_IS_UNION = 'X'
     where TMP.T_IS_UNION != 'X'
       and TMP.T_ISCLIENT != 'X'
       and TMP.T_ISITOG != 'X'
       and TMP.T_DLKIND in(4,6)
       and  NVL((select count(1) from DDLREP701_TMP sum_tmp
                  where sum_TMP.T_IS_UNION != 'X'
                    and sum_TMP.T_ISCLIENT != 'X'
                    and sum_TMP.T_ISITOG != 'X'
                    and sum_TMP.T_DLKIND = TMP.T_DLKIND
                    and sum_tmp.T_DEALDATE = tmp.T_DEALDATE
                    and sum_tmp.T_CALCDATE          =   tmp.T_CALCDATE
                    and sum_tmp.T_REQFIID           =   tmp.T_REQFIID
                    and sum_tmp.T_COMFIID           =   tmp.T_COMFIID
                    and sum_tmp.T_ISRESIDENT        =   tmp.T_ISRESIDENT
                    and sum_tmp.T_BANKCONTRAGENT    =   tmp.T_BANKCONTRAGENT
                    and sum_tmp.T_MARKETCONTRAGENT  =   tmp.T_MARKETCONTRAGENT
                    and sum_tmp.T_OTHER             =   tmp.T_OTHER
                    and sum_tmp.T_DEALADDINFO       =   tmp.T_DEALADDINFO
                    and sum_tmp.T_TYPEADDINFO       =   tmp.T_TYPEADDINFO
                    and sum_tmp.T_DEALSIDE          =   tmp.T_DEALSIDE
                    and sum_tmp.T_METHODADDINFO     =   tmp.T_METHODADDINFO
                    and sum_tmp.T_ISBUY             =   tmp.T_ISBUY
                    and sum_tmp.T_FIID              =   tmp.T_FIID),0) >= 10;
    end if;
   
    -- отсеиваем те, что не объединяются
    delete from DDLREP701_TMP tmp
     where TMP.T_IS_UNION != 'X'
       and T_DLKIND not in(1,DLKIND_DVFUTURES,DLKIND_DVOPTION,8,9)
       and ((TMP.T_COMSUM < 1000 and TMP.T_COMFIID != -1) or (TMP.T_REQSUM < 1000 and TMP.T_REQFIID != -1));

    update DDLREP701_TMP tmp
       set TMP.T_COMSUM = CASE WHEN (TMP.T_COMSUM / 1000) < 1 THEN 1 ELSE TMP.T_COMSUM / 1000 END,
           TMP.T_REQSUM = CASE WHEN (TMP.T_REQSUM / 1000) < 1 THEN 1 ELSE TMP.T_REQSUM / 1000 END 
     where TMP.T_IS_UNION != 'X'
       and T_DLKIND not in(1,8, 9); --с суммами РЕПО , %% свопов и МБК разобрались, когда отбирали сделки

    -- объединяем собственные, подлежащие объединению
    SELECT
    /*T_DEALDATE         */  q.T_DEALDATE,
    /*T_CALCDATE         */  q.T_CALCDATE,
    /*T_REQFIID          */  q.T_REQFIID,
    /*T_REQSUM           */  q.T_REQSUM,
    /*T_COMFIID          */  q.T_COMFIID,
    /*T_COMSUM           */  q.T_COMSUM,
    /*T_ISRESIDENT       */  q.T_ISRESIDENT,
    /*T_BANKCONTRAGENT   */  q.T_BANKCONTRAGENT,
    /*T_MARKETCONTRAGENT */  q.T_MARKETCONTRAGENT,
    /*T_OTHER            */  q.T_OTHER,
    /*T_DEALADDINFO      */  q.T_DEALADDINFO,
    /*T_RATEADDINFO      */  0,
    /*T_TYPEADDINFO      */  q.T_TYPEADDINFO,
    /*T_ISCLIENT         */  chr(0),
    /*T_ISITOG           */  'X',
    /*T_DLKIND           */  q.T_DLKIND,
    /*T_IS_UNION         */  'X',
    /*T_DEALID           */  0,
    /*T_DEALPART         */  0,
    /*T_ISBUY            */  q.T_ISBUY,
    /*T_FIID             */  -1,
    /*T_ISMARKET         */  q.T_ISMARKET,
    /*T_AUTOKEY          */  0,
    /*T_DEALSIDE         */  q.T_DEALSIDE,
    /*T_METHODADDINFO    */  q.T_METHODADDINFO,
    /*T_ISSWIFTCODE      */  q.T_ISSWIFTCODE,
    /*T_OKCMCODE         */  q.T_OKCMCODE,
    /*T_ISSINGLEPCSW     */  q.T_ISSINGLEPCSW,
    /*T_ISEXCHORCENTRCONTR*/ q.T_ISEXCHORCENTRCONTR,
    /*T_RATECCY          */  q.T_RATECCY,
    /*T_RATENAME         */  q.T_RATENAME,
    /*T_RATESPREAD       */  q.T_RATESPREAD,
    /*T_PREGNUM          */  q.T_PREGNUM,
    /*T_TYPECONTR        */  q.T_TYPECONTR,
    /*T_BIC              */  q.T_BIC,
    /*T_LEI_CONTRACTOR*/ q.T_LEI_CONTRACTOR,
    /*T_INN_CONTRACTOR*/ q.T_INN_CONTRACTOR,
    /*T_CONTRACTORID*/ q.T_CONTRACTORID,
    /*T_ISMONEYMARKET*/ q.T_ISMONEYMARKET,
    /*T_CONTRACTORNAME*/   q.T_CONTRACTORNAME,
    /*T_ADDGRFLAG*/         q.T_ADDGRFLAG
    BULK COLLECT INTO v_dlrep701
    FROM (select NVL(sum(TMP.T_REQSUM),0) T_REQSUM, NVL(sum(TMP.T_COMSUM),0) T_COMSUM, TMP.T_ISBUY, TMP.T_ISMARKET,
                 TMP.T_DLKIND, TMP.T_DEALDATE, TMP.T_CALCDATE, TMP.T_REQFIID, TMP.T_COMFIID,TMP.T_ISRESIDENT, TMP.T_BANKCONTRAGENT, TMP.T_MARKETCONTRAGENT, TMP.T_OTHER,
                 TMP.T_DEALADDINFO, TMP.T_TYPEADDINFO, TMP.T_DEALSIDE, TMP.T_METHODADDINFO, TMP.T_ISSWIFTCODE, TMP.T_OKCMCODE, TMP.T_ISSINGLEPCSW, TMP.T_ISEXCHORCENTRCONTR,
                 TMP.T_RATECCY, TMP.T_RATENAME, TMP.T_RATESPREAD, TMP.T_TYPECONTR, TMP.T_PREGNUM, TMP.T_BIC, TMP.T_LEI_CONTRACTOR, TMP.T_INN_CONTRACTOR, TMP.T_CONTRACTORID, TMP.T_ISMONEYMARKET, TMP.T_CONTRACTORNAME, TMP.T_ADDGRFLAG
            from DDLREP701_TMP tmp
           where TMP.T_IS_UNION = 'X'
             and TMP.T_ISCLIENT != 'X'
             and TMP.T_ISITOG != 'X'
             and TMP.T_DLKIND not in(DLKIND_DVFUTURES,DLKIND_DVOPTION)
           group by TMP.T_ISMARKET, TMP.T_DLKIND, TMP.T_ISBUY, TMP.T_DEALDATE, TMP.T_CALCDATE, TMP.T_REQFIID, TMP.T_COMFIID, TMP.T_ISRESIDENT, TMP.T_BANKCONTRAGENT,
                    TMP.T_MARKETCONTRAGENT, TMP.T_OTHER, TMP.T_DEALADDINFO, TMP.T_TYPEADDINFO, TMP.T_DEALSIDE, TMP.T_METHODADDINFO, TMP.T_ISSWIFTCODE, TMP.T_OKCMCODE,
                    TMP.T_ISSINGLEPCSW, TMP.T_ISEXCHORCENTRCONTR, TMP.T_RATECCY, TMP.T_RATENAME, TMP.T_RATESPREAD, TMP.T_TYPECONTR, TMP.T_PREGNUM, TMP.T_BIC, TMP.T_LEI_CONTRACTOR, TMP.T_INN_CONTRACTOR, TMP.T_CONTRACTORID, TMP.T_ISMONEYMARKET, TMP.T_CONTRACTORNAME, TMP.T_ADDGRFLAG
           having (NVL(sum(TMP.T_REQSUM),0) >= 1000 and NVL(sum(TMP.T_COMSUM),0) >= 1000)
         ) q;

    IF v_dlrep701.COUNT > 0 THEN
       FORALL indx IN v_dlrep701.FIRST .. v_dlrep701.LAST
          INSERT INTO DDLREP701_TMP
               VALUES v_dlrep701 (indx);
    END IF;

    SELECT
    /*T_DEALDATE         */  q.T_DEALDATE,
    /*T_CALCDATE         */  q.T_CALCDATE,
    /*T_REQFIID          */  q.T_REQFIID,
    /*T_REQSUM           */  q.T_REQSUM,
    /*T_COMFIID          */  q.T_COMFIID,
    /*T_COMSUM           */  q.T_COMSUM,
    /*T_ISRESIDENT       */  q.T_ISRESIDENT,
    /*T_BANKCONTRAGENT   */  q.T_BANKCONTRAGENT,
    /*T_MARKETCONTRAGENT */  q.T_MARKETCONTRAGENT,
    /*T_OTHER            */  q.T_OTHER,
    /*T_DEALADDINFO      */  q.T_DEALADDINFO,
    /*T_RATEADDINFO      */  q.T_RATEADDINFO,
    /*T_TYPEADDINFO      */  q.T_TYPEADDINFO,
    /*T_ISCLIENT         */  chr(0),
    /*T_ISITOG           */  'X',
    /*T_DLKIND           */  q.T_DLKIND,
    /*T_IS_UNION         */  'X',
    /*T_DEALID           */  0,
    /*T_DEALPART         */  0,
    /*T_ISBUY            */  q.T_ISBUY,
    /*T_FIID             */  q.T_FIID,
    /*T_ISMARKET         */  q.T_ISMARKET,
    /*T_AUTOKEY          */  0,
    /*T_DEALSIDE         */  q.T_DEALSIDE,
    /*T_METHODADDINFO    */  q.T_METHODADDINFO,
    /*T_ISSWIFTCODE      */  q.T_ISSWIFTCODE,
    /*T_OKCMCODE         */  q.T_OKCMCODE,
    /*T_ISSINGLEPCSW     */  q.T_ISSINGLEPCSW,
    /*T_ISEXCHORCENTRCONTR*/ q.T_ISEXCHORCENTRCONTR,
    /*T_RATECCY          */  q.T_RATECCY,
    /*T_RATENAME         */  q.T_RATENAME,
    /*T_RATESPREAD       */  q.T_RATESPREAD,
    /*T_PREGNUM          */  q.T_PREGNUM,
    /*T_TYPECONTR        */  q.T_TYPECONTR,
    /*T_BIC              */  q.T_BIC,
    /*T_LEI_CONTRACTOR*/ q.T_LEI_CONTRACTOR,
    /*T_INN_CONTRACTOR*/ q.T_INN_CONTRACTOR,
    /*T_CONTRACTORID*/ q.T_CONTRACTORID,
    /*T_ISMONEYMARKET*/ q.T_ISMONEYMARKET,
    /*T_CONTRACTORNAME*/   q.T_CONTRACTORNAME,
    /*T_ADDGRFLAG*/         q.T_ADDGRFLAG
    BULK COLLECT INTO v_dlrep701
    FROM (select NVL(sum(TMP.T_REQSUM),0) T_REQSUM, NVL(sum(TMP.T_COMSUM),0) T_COMSUM, NVL(sum(TMP.T_RATEADDINFO),0) T_RATEADDINFO, TMP.T_ISBUY, TMP.T_FIID, TMP.T_ISMARKET,
                 TMP.T_DLKIND, TMP.T_DEALDATE, TMP.T_CALCDATE, TMP.T_REQFIID, TMP.T_COMFIID,TMP.T_ISRESIDENT, TMP.T_BANKCONTRAGENT, TMP.T_MARKETCONTRAGENT, TMP.T_OTHER,
                 TMP.T_DEALADDINFO, TMP.T_TYPEADDINFO, TMP.T_DEALSIDE, TMP.T_METHODADDINFO, TMP.T_ISSWIFTCODE, TMP.T_OKCMCODE, TMP.T_ISSINGLEPCSW, TMP.T_ISEXCHORCENTRCONTR,
                 TMP.T_RATECCY, TMP.T_RATENAME, TMP.T_RATESPREAD, TMP.T_TYPECONTR, TMP.T_PREGNUM, TMP.T_BIC, TMP.T_LEI_CONTRACTOR, TMP.T_INN_CONTRACTOR, TMP.T_CONTRACTORID, TMP.T_ISMONEYMARKET, TMP.T_CONTRACTORNAME, TMP.T_ADDGRFLAG
            from DDLREP701_TMP tmp
           where TMP.T_IS_UNION = 'X'
             and TMP.T_ISCLIENT != 'X'
             and TMP.T_ISITOG != 'X'
             and TMP.T_DLKIND in(DLKIND_DVFUTURES,DLKIND_DVOPTION)
           group by TMP.T_ISMARKET, TMP.T_DLKIND, TMP.T_FIID, TMP.T_ISBUY, TMP.T_DEALDATE, TMP.T_CALCDATE, TMP.T_REQFIID, TMP.T_COMFIID, TMP.T_ISRESIDENT, TMP.T_BANKCONTRAGENT,
                    TMP.T_MARKETCONTRAGENT, TMP.T_OTHER, TMP.T_DEALADDINFO, TMP.T_TYPEADDINFO, TMP.T_DEALSIDE, TMP.T_METHODADDINFO, TMP.T_ISSWIFTCODE, TMP.T_OKCMCODE,
                    TMP.T_ISSINGLEPCSW, TMP.T_ISEXCHORCENTRCONTR, TMP.T_RATECCY, TMP.T_RATENAME, TMP.T_RATESPREAD, TMP.T_TYPECONTR, TMP.T_PREGNUM, TMP.T_BIC, TMP.T_LEI_CONTRACTOR, TMP.T_INN_CONTRACTOR, TMP.T_CONTRACTORID, TMP.T_ISMONEYMARKET, TMP.T_CONTRACTORNAME, TMP.T_ADDGRFLAG
           having (NVL(sum(TMP.T_REQSUM),0) >= 1000 and NVL(sum(TMP.T_COMSUM),0) >= 1000)
         ) q;

    IF v_dlrep701.COUNT > 0 THEN
       FORALL indx IN v_dlrep701.FIRST .. v_dlrep701.LAST
          INSERT INTO DDLREP701_TMP
               VALUES v_dlrep701 (indx);
    END IF;

    -- выкидываем клиентские, которые при объединении не проходят по суммам вторых!!! строк
    DELETE FROM DDLREP701_TMP TMP
          WHERE TMP.T_IS_UNION = 'X'
            and TMP.T_ISCLIENT = 'X'
            and TMP.T_ISITOG != 'X'
            and( (select NVL(sum(T.T_REQSUM),0)
                    from DDLREP701_TMP t
                   where T.T_IS_UNION = TMP.T_IS_UNION
                     and T.T_ISCLIENT = TMP.T_ISCLIENT
                     and T.T_DEALDATE =          TMP.T_DEALDATE
                     and T.T_CALCDATE =          TMP.T_CALCDATE
                     and T.T_REQFIID =           TMP.T_REQFIID
                     and T.T_COMFIID =           TMP.T_COMFIID
                     and T.T_ISRESIDENT =        TMP.T_ISRESIDENT
                     and T.T_BANKCONTRAGENT =    TMP.T_BANKCONTRAGENT
                     and T.T_OTHER = TMP.T_OTHER
                     and T.T_DEALADDINFO = TMP.T_DEALADDINFO
                     and T.T_TYPEADDINFO = TMP.T_TYPEADDINFO
                     and t.T_DEALSIDE = TMP.T_DEALSIDE
                     and t.T_METHODADDINFO = TMP.T_METHODADDINFO
                     and T.T_DLKIND = TMP.T_DLKIND
                     and T.T_IS_UNION = TMP.T_IS_UNION
                     and T.T_ISCLIENT = TMP.T_ISCLIENT
                     and T.T_ISBUY = TMP.T_ISBUY
                     and T.T_ISMARKET = TMP.T_ISMARKET
                     and T.T_ISITOG  = TMP.T_ISITOG) < 1000 or
                 (select NVL(sum(T.T_COMSUM),0)
                    from DDLREP701_TMP t
                   where T.T_IS_UNION = TMP.T_IS_UNION
                     and T.T_ISCLIENT = TMP.T_ISCLIENT
                     and T.T_DEALDATE =          TMP.T_DEALDATE
                     and T.T_CALCDATE =          TMP.T_CALCDATE
                     and T.T_REQFIID =           TMP.T_REQFIID
                     and T.T_COMFIID =           TMP.T_COMFIID
                     and T.T_ISRESIDENT =        TMP.T_ISRESIDENT
                     and T.T_BANKCONTRAGENT =    TMP.T_BANKCONTRAGENT
                     and T.T_OTHER = TMP.T_OTHER
                     and T.T_DEALADDINFO = TMP.T_DEALADDINFO
                     and T.T_TYPEADDINFO = TMP.T_TYPEADDINFO
                     and t.T_DEALSIDE = TMP.T_DEALSIDE
                     and t.T_METHODADDINFO = TMP.T_METHODADDINFO
                     and T.T_IS_UNION = TMP.T_IS_UNION
                     and T.T_ISCLIENT = TMP.T_ISCLIENT
                     and T.T_ISBUY = TMP.T_ISBUY
                     and T.T_ISMARKET = TMP.T_ISMARKET
                     and T.T_ISITOG  = TMP.T_ISITOG) < 1000
               );

    --формируем  вторые строки для клиентских, подлежащих объединению
    SELECT
    /*T_DEALDATE         */  q.T_DEALDATE,
    /*T_CALCDATE         */  q.T_CALCDATE,
    /*T_REQFIID          */  q.T_REQFIID,
    /*T_REQSUM           */  q.T_REQSUM,
    /*T_COMFIID          */  q.T_COMFIID,
    /*T_COMSUM           */  q.T_COMSUM,
    /*T_ISRESIDENT       */  q.T_ISRESIDENT,
    /*T_BANKCONTRAGENT   */  q.T_BANKCONTRAGENT,
    /*T_MARKETCONTRAGENT */  'MD',
    /*T_OTHER            */  q.T_OTHER,
    /*T_DEALADDINFO      */  q.T_DEALADDINFO,
    /*T_RATEADDINFO      */  q.T_RATEADDINFO,
    /*T_TYPEADDINFO      */  q.T_TYPEADDINFO,
    /*T_ISCLIENT         */  'X',
    /*T_ISITOG           */  'X',
    /*T_DLKIND           */  q.T_DLKIND,
    /*T_IS_UNION         */  'X',
    /*T_DEALID           */  0,
    /*T_DEALPART         */  2,
    /*T_ISBUY            */  q.T_ISBUY,
    /*T_FIID             */  -1,
    /*T_ISMARKET         */  q.T_ISMARKET,
    /*T_AUTOKEY          */  0,
    /*T_DEALSIDE         */  q.T_DEALSIDE,
    /*T_METHODADDINFO    */  q.T_METHODADDINFO,
    /*T_ISSWIFTCODE      */  q.T_ISSWIFTCODE,
    /*T_OKCMCODE         */  q.T_OKCMCODE,
    /*T_ISSINGLEPCSW     */  q.T_ISSINGLEPCSW,
    /*T_ISEXCHORCENTRCONTR*/ q.T_ISEXCHORCENTRCONTR,
    /*T_RATECCY          */  q.T_RATECCY,
    /*T_RATENAME         */  q.T_RATENAME,
    /*T_RATESPREAD       */  q.T_RATESPREAD,
    /*T_PREGNUM          */  q.T_PREGNUM,
    /*T_TYPECONTR        */  q.T_TYPECONTR,
    /*T_BIC              */  q.T_BIC,
    /*T_LEI_CONTRACTOR*/ q.T_LEI_CONTRACTOR,
    /*T_INN_CONTRACTOR*/ q.T_INN_CONTRACTOR,
    /*T_CONTRACTORID*/ q.T_CONTRACTORID,
    /*T_ISMONEYMARKET*/ q.T_ISMONEYMARKET,
    /*T_CONTRACTORNAME*/   q.T_CONTRACTORNAME,
    /*T_ADDGRFLAG*/         q.T_ADDGRFLAG
    BULK COLLECT INTO v_dlrep701
    FROM (select NVL(sum(TMP.T_REQSUM),0) T_REQSUM, NVL(sum(TMP.T_COMSUM),0) T_COMSUM, NVL(sum(TMP.T_RATEADDINFO),0) T_RATEADDINFO, TMP.T_ISBUY, TMP.T_ISMARKET,
                 TMP.T_DLKIND, TMP.T_DEALDATE, TMP.T_CALCDATE, TMP.T_REQFIID, TMP.T_COMFIID,TMP.T_ISRESIDENT, TMP.T_BANKCONTRAGENT, TMP.T_OTHER, TMP.T_DEALADDINFO,
                 TMP.T_TYPEADDINFO, TMP.T_DEALSIDE, TMP.T_METHODADDINFO, TMP.T_ISSWIFTCODE, TMP.T_OKCMCODE, TMP.T_ISSINGLEPCSW, TMP.T_ISEXCHORCENTRCONTR,
                 TMP.T_RATECCY, TMP.T_RATENAME, TMP.T_RATESPREAD, TMP.T_TYPECONTR, TMP.T_PREGNUM, TMP.T_BIC, TMP.T_LEI_CONTRACTOR, TMP.T_INN_CONTRACTOR, TMP.T_CONTRACTORID, TMP.T_ISMONEYMARKET, TMP.T_CONTRACTORNAME, TMP.T_ADDGRFLAG
            from DDLREP701_TMP tmp
           where TMP.T_IS_UNION = 'X'
             and TMP.T_ISITOG != 'X'
             and TMP.T_ISCLIENT = 'X'
           group by TMP.T_ISMARKET, TMP.T_DLKIND, TMP.T_ISBUY, TMP.T_DEALDATE, TMP.T_CALCDATE, TMP.T_REQFIID, TMP.T_COMFIID, TMP.T_ISRESIDENT, TMP.T_BANKCONTRAGENT,
                    TMP.T_OTHER, TMP.T_DEALADDINFO, TMP.T_TYPEADDINFO, TMP.T_DEALSIDE, TMP.T_METHODADDINFO, TMP.T_ISSWIFTCODE, TMP.T_OKCMCODE, TMP.T_ISSINGLEPCSW,
                    TMP.T_ISEXCHORCENTRCONTR, TMP.T_RATECCY, TMP.T_RATENAME, TMP.T_RATESPREAD, TMP.T_TYPECONTR, TMP.T_PREGNUM, TMP.T_BIC, TMP.T_LEI_CONTRACTOR, TMP.T_INN_CONTRACTOR, TMP.T_CONTRACTORID, TMP.T_ISMONEYMARKET, TMP.T_CONTRACTORNAME, TMP.T_ADDGRFLAG
         ) q;

    IF v_dlrep701.COUNT > 0 THEN
       FORALL indx IN v_dlrep701.FIRST .. v_dlrep701.LAST
          INSERT INTO DDLREP701_TMP
               VALUES v_dlrep701 (indx);
    END IF;

    update DDLREP701_TMP tmp
       set TMP.T_DealID = TMP.t_autokey
     where TMP.T_IS_UNION = 'X'
       and TMP.T_ISITOG = 'X'
       and TMP.T_ISCLIENT = 'X';

    --формируем первые строки для клиентских, подлежащих объединению
    SELECT
    /*T_DEALDATE         */  q.T_DEALDATE,
    /*T_CALCDATE         */  q.T_CALCDATE,
    /*T_REQFIID          */  q.T_COMFIID,
    /*T_REQSUM           */  q.T_COMSUM,
    /*T_COMFIID          */  q.T_REQFIID,
    /*T_COMSUM           */  q.T_REQSUM,
    /*T_ISRESIDENT       */  q.T_ISRESIDENT,
    /*T_BANKCONTRAGENT   */  q.T_BANKCONTRAGENT,
    /*T_MARKETCONTRAGENT */  q.T_MARKETCONTRAGENT,
    /*T_OTHER            */  q.T_OTHER,
    /*T_DEALADDINFO      */  q.T_DEALADDINFO,
    /*T_RATEADDINFO      */  q.T_RATEADDINFO,
    /*T_TYPEADDINFO      */  q.T_TYPEADDINFO,
    /*T_ISCLIENT         */  'X',
    /*T_ISITOG           */  'X',
    /*T_DLKIND           */  q.T_DLKIND,
    /*T_IS_UNION         */  'X',
    /*T_DEALID           */  0,
    /*T_DEALPART         */  1,
    /*T_ISBUY            */  q.T_ISBUY,
    /*T_FIID             */  -1,
    /*T_ISMARKET         */  q.T_ISMARKET,
    /*T_AUTOKEY          */  0,
    /*T_DEALSIDE         */  q.T_DEALSIDE,
    /*T_METHODADDINFO    */  q.T_METHODADDINFO,
    /*T_ISSWIFTCODE      */  q.T_ISSWIFTCODE,
    /*T_OKCMCODE         */  q.T_OKCMCODE,
    /*T_ISSINGLEPCSW     */  q.T_ISSINGLEPCSW,
    /*T_ISEXCHORCENTRCONTR*/ q.T_ISEXCHORCENTRCONTR,
    /*T_RATECCY          */  q.T_RATECCY,
    /*T_RATENAME         */  q.T_RATENAME,
    /*T_RATESPREAD       */  q.T_RATESPREAD,
    /*T_PREGNUM          */  q.T_PREGNUM,
    /*T_TYPECONTR        */  q.T_TYPECONTR,
    /*T_BIK              */  q.T_BIC,
    /*T_LEI_CONTRACTOR*/ q.T_LEI_CONTRACTOR,
    /*T_INN_CONTRACTOR*/ q.T_INN_CONTRACTOR,
    /*T_CONTRACTORID*/ q.T_CONTRACTORID,
    /*T_ISMONEYMARKET*/ q.T_ISMONEYMARKET,
    /*T_CONTRACTORNAME*/   q.T_CONTRACTORNAME,
    /*T_ADDGRFLAG*/         q.T_ADDGRFLAG
    BULK COLLECT INTO v_dlrep701
    FROM (select NVL(sum(TMP.T_REQSUM),0) T_REQSUM, NVL(sum(TMP.T_COMSUM),0) T_COMSUM, NVL(sum(TMP.T_RATEADDINFO),0) T_RATEADDINFO, TMP.T_ISBUY, TMP.T_ISMARKET,
                 TMP.T_DLKIND, TMP.T_DEALDATE, TMP.T_CALCDATE, TMP.T_REQFIID, TMP.T_COMFIID,TMP.T_ISRESIDENT, TMP.T_BANKCONTRAGENT,TMP.T_MARKETCONTRAGENT, TMP.T_OTHER,
                 TMP.T_DEALADDINFO, TMP.T_TYPEADDINFO, TMP.T_DEALSIDE, TMP.T_METHODADDINFO, TMP.T_ISSWIFTCODE, TMP.T_OKCMCODE, TMP.T_ISSINGLEPCSW, TMP.T_ISEXCHORCENTRCONTR,
                 TMP.T_RATECCY, TMP.T_RATENAME, TMP.T_RATESPREAD, TMP.T_TYPECONTR, TMP.T_PREGNUM, TMP.T_BIC, TMP.T_LEI_CONTRACTOR, TMP.T_INN_CONTRACTOR, TMP.T_CONTRACTORID, TMP.T_ISMONEYMARKET, TMP.T_CONTRACTORNAME, TMP.T_ADDGRFLAG
            from DDLREP701_TMP tmp
           where TMP.T_IS_UNION = 'X'
             and TMP.T_ISITOG != 'X'
             and TMP.T_ISCLIENT = 'X'
           group by TMP.T_ISMARKET, TMP.T_DLKIND, TMP.T_ISBUY, TMP.T_DEALDATE, TMP.T_CALCDATE, TMP.T_REQFIID, TMP.T_COMFIID, TMP.T_ISRESIDENT, TMP.T_BANKCONTRAGENT,
                    TMP.T_MARKETCONTRAGENT, TMP.T_OTHER, TMP.T_DEALADDINFO, TMP.T_TYPEADDINFO, TMP.T_DEALSIDE, TMP.T_METHODADDINFO, TMP.T_ISSWIFTCODE, TMP.T_OKCMCODE,
                    TMP.T_ISSINGLEPCSW, TMP.T_ISEXCHORCENTRCONTR, TMP.T_RATECCY, TMP.T_RATENAME, TMP.T_RATESPREAD, TMP.T_TYPECONTR, TMP.T_PREGNUM, TMP.T_BIC, TMP.T_LEI_CONTRACTOR, TMP.T_INN_CONTRACTOR, TMP.T_CONTRACTORID, TMP.T_ISMONEYMARKET, TMP.T_CONTRACTORNAME, TMP.T_ADDGRFLAG
         ) q;

    IF v_dlrep701.COUNT > 0 THEN
       FORALL indx IN v_dlrep701.FIRST .. v_dlrep701.LAST
          INSERT INTO DDLREP701_TMP
               VALUES v_dlrep701 (indx);
    END IF;

    -- удаляем те, по которым образовались объединенные
    delete from DDLREP701_TMP tmp
     where TMP.T_IS_UNION = 'X'
       and TMP.T_ISITOG != 'X';

    -- привязываем к первым строкам клиентских вторые
    update DDLREP701_TMP tmp
       set TMP.T_DealID = NVL((select t.t_autokey
                                 from DDLREP701_TMP t
                                where t.T_DEALDATE         = TMP.T_DEALDATE
                                  and t.T_CALCDATE         = TMP.T_CALCDATE
                                  and t.T_REQFIID          = TMP.T_COMFIID
                                  and t.T_COMFIID          = TMP.T_REQFIID
                                  and t.T_ISRESIDENT       = TMP.T_ISRESIDENT
                                  and t.T_BANKCONTRAGENT   = TMP.T_BANKCONTRAGENT
                                  and t.T_DEALPART         = 2
                                  and t.T_OTHER            = TMP.T_OTHER
                                  and t.T_DEALADDINFO      = TMP.T_DEALADDINFO
                                  and t.T_RATEADDINFO      = TMP.T_RATEADDINFO
                                  and t.T_TYPEADDINFO      = TMP.T_TYPEADDINFO
                                  and t.T_ISCLIENT         = TMP.T_ISCLIENT
                                  and t.T_ISITOG           = TMP.T_ISITOG
                                  and t.T_DLKIND           = TMP.T_DLKIND
                                  and t.T_IS_UNION         = TMP.T_IS_UNION
                                  and t.T_ISBUY            = TMP.T_ISBUY
                                  and t.T_ISMARKET         = TMP.T_ISMARKET
                                  and t.T_DEALSIDE         = TMP.T_DEALSIDE
                                  and t.T_METHODADDINFO    = TMP.T_METHODADDINFO
                             ),0),
           TMP.T_OTHER = 'C'
     where TMP.T_IS_UNION = 'X'
       and TMP.T_ISITOG = 'X'
       and TMP.T_ISCLIENT = 'X'
       and TMP.T_DEALPART = 1;
              
     update DDLREP701_TMP tmp
       set TMP.T_COMSUM = TMP.T_COMSUM / 1000,
           TMP.T_REQSUM = TMP.T_REQSUM / 1000
     where TMP.T_ISITOG = 'X';
     
     if (OnDate >= v_ActDate) then
     
       update DDLREP701_TMP tmp
            set TMP.T_INN_CONTRACTOR  = DECODE(TMP.T_ISRESIDENT, 'R', Rsb_Secur.SC_GetObjCodeOnDate(cnst.OBJTYPE_PARTY, cnst.PTCK_INN, TMP.T_ContractorID), chr(1)),
                 TMP.T_LEI_CONTRACTOR   = DECODE(TMP.T_ISRESIDENT, 'N', Rsb_Secur.SC_GetObjCodeOnDate(cnst.OBJTYPE_PARTY, 69 /*LEI*/, TMP.T_ContractorID), chr(1)),
                 TMP.T_CONTRACTORNAME = DECODE(TMP.T_BANKCONTRAGENT, chr(1), NVL((SELECT NVL(UPPER(t_name), chr(1)) FROM DPARTY_DBT WHERE t_PartyID = TMP.t_ContractorID), chr(1)))
       where TMP.T_AddGrFlag = 1;
                 
       update DDLREP701_TMP tmp
            set TMP.T_INN_CONTRACTOR  = DECODE(TMP.T_ISRESIDENT, 'R', Rsb_Secur.SC_GetObjCodeOnDate(cnst.OBJTYPE_PARTY, cnst.PTCK_INN, TMP.T_ContractorID), chr(1)),
                 TMP.T_LEI_CONTRACTOR   = DECODE(TMP.T_ISRESIDENT, 'N', Rsb_Secur.SC_GetObjCodeOnDate(cnst.OBJTYPE_PARTY, 69 /*LEI*/, TMP.T_ContractorID), chr(1)),
                 TMP.T_CONTRACTORNAME = DECODE(TMP.T_BANKCONTRAGENT, chr(1), NVL((SELECT NVL(UPPER(t_name), chr(1)) FROM DPARTY_DBT WHERE t_PartyID = TMP.t_ContractorID), chr(1)))
       where TMP.T_AddGrFlag = 2
           and (T_COMSUM >= 1000 or T_REQSUM >= 1000);
               
        update DDLREP701_TMP tmp
             set TMP.T_INN_CONTRACTOR = substr(ltrim(TMP.T_INN_CONTRACTOR), 1, instr(ltrim(TMP.T_INN_CONTRACTOR)||'/', '/')-1)
       where TMP.T_INN_CONTRACTOR != chr(1);

      end if;

  END CorrectData;

END rsb_dlrep701_mz_2293;
/
