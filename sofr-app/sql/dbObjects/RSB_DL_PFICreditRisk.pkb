CREATE OR REPLACE PACKAGE BODY RSB_DL_PFICreditRisk
IS

  FUNCTION GetAccRestOnDate( pDocID IN NUMBER, pDocKind IN NUMBER, pRestDate IN DATE, pCatCodeStr IN VARCHAR2, pNotEmpty IN NUMBER, pOnFI IN NUMBER, pToFI OUT NUMBER, pToAcc OUT VARCHAR2 ) RETURN NUMBER
  IS
     v_AccRest NUMBER := 0;
     v_FI daccount_dbt.t_Code_Currency%TYPE := -1;
     v_Acc daccount_dbt.t_Account%TYPE := chr(1);
  BEGIN
     select AccRest, T_CODE_CURRENCY, T_ACCOUNT 
       into v_AccRest, v_FI, v_Acc
       from (select rsb_account.restac(ACC.T_ACCOUNT, ACC.T_CODE_CURRENCY, pRestDate, ACC.T_CHAPTER, null) AccRest, ACC.T_CODE_CURRENCY, ACC.T_ACCOUNT
               from dmcaccdoc_dbt mc, dmccateg_dbt cat, daccount_dbt acc
              where instr(pCatCodeStr,CAT.T_CODE) > 0
                and CAT.T_LEVELTYPE = 1
                and MC.T_CATID = CAT.T_ID
                and MC.T_DOCID = pDocID
                and MC.T_DOCKIND = pDocKind
                and (MC.T_DISABLINGDATE = to_date('01.01.0001','dd.mm.yyyy') or MC.T_DISABLINGDATE > pRestDate)
                and (MC.T_ACTIVATEDATE <= pRestDate)
                and MC.T_CURRENCY = case when pOnFI <> -1 then pOnFI else MC.T_CURRENCY end
                and ACC.T_ACCOUNT = MC.T_ACCOUNT
                and ACC.T_CHAPTER = MC.T_CHAPTER
                and ACC.T_CODE_CURRENCY = MC.T_CURRENCY
           order by MC.T_ID desc
            )
      where ((pNotEmpty = 1 and AccRest <> 0) or (pNotEmpty <> 1))
        and rownum = 1;
      
     pToFI  := v_FI; 
     pToAcc := v_Acc;
     RETURN v_AccRest;
  EXCEPTION WHEN OTHERS THEN
     pToFI  := -1; 
     pToAcc := chr(1);
     RETURN 0;
  END GetAccRestOnDate;

  FUNCTION GetAccRestOnDatePos( pFIID IN NUMBER, pRestDate IN DATE, pCatCodeStr IN VARCHAR2, pNotEmpty IN NUMBER, pOnFI IN NUMBER, pToFI OUT NUMBER, pToAcc OUT VARCHAR2 ) RETURN NUMBER
  IS
     v_AccRest NUMBER := 0;
     v_FI daccount_dbt.t_Code_Currency%TYPE := -1;
     v_Acc daccount_dbt.t_Account%TYPE := chr(1);
  BEGIN
     select AccRest, T_CODE_CURRENCY, T_ACCOUNT 
       into v_AccRest, v_FI, v_Acc
       from (select rsb_account.restac(ACC.T_ACCOUNT, ACC.T_CODE_CURRENCY, pRestDate, ACC.T_CHAPTER, null) AccRest, ACC.T_CODE_CURRENCY, ACC.T_ACCOUNT
               from dmcaccdoc_dbt mc, dmccateg_dbt cat, daccount_dbt acc
              where instr(pCatCodeStr,CAT.T_CODE) > 0
                and CAT.T_LEVELTYPE = 1
                and MC.T_CATID = CAT.T_ID
                and MC.T_DOCID =0
                and MC.T_FIID = pFIID
                and (MC.T_DISABLINGDATE = to_date('01.01.0001','dd.mm.yyyy') or MC.T_DISABLINGDATE > pRestDate)
                and (MC.T_ACTIVATEDATE <= pRestDate)
                and MC.T_CURRENCY = case when pOnFI <> -1 then pOnFI else MC.T_CURRENCY end
                and ACC.T_ACCOUNT = MC.T_ACCOUNT
                and ACC.T_CHAPTER = MC.T_CHAPTER
                and ACC.T_CODE_CURRENCY = MC.T_CURRENCY
           order by MC.T_ID desc
            )
      where ((pNotEmpty = 1 and AccRest <> 0) or (pNotEmpty <> 1))
        and rownum = 1;
      
     pToFI  := v_FI; 
     pToAcc := v_Acc;
     RETURN v_AccRest;
  EXCEPTION WHEN OTHERS THEN
     pToFI  := -1; 
     pToAcc := chr(1);
     RETURN 0;
  END GetAccRestOnDatePos;
  
  FUNCTION GetFI_Code( pFIID NUMBER ) RETURN VARCHAR2
  IS
     v_FI_Code dfininstr_dbt.t_FI_Code%TYPE := chr(1);
  BEGIN
     if( pFIID = RSI_RSB_FIInstr.NATCUR ) then
        return '810';
     else
        begin      
          select fininstr.t_FI_Code into v_FI_Code 
            from dfininstr_dbt fininstr 
           where fininstr.t_FIID = pFIID;
          return v_FI_Code;
        exception when NO_DATA_FOUND then 
          return chr(1);
        end;
     end if;
  END GetFI_Code; 
  
  FUNCTION GetFIIDbyCode( pFI_Code VARCHAR2 ) RETURN NUMBER
  IS
     v_FIID dfininstr_dbt.t_FIID%TYPE := -1;
  BEGIN
     if( pFI_Code = chr(1) ) then
        return -1;
     elsif( substr(pFI_Code,1,3) = '810' ) then
        return RSI_RSB_FIInstr.NATCUR;
     else
        begin      
          select fininstr.t_FIID into v_FIID 
            from dfininstr_dbt fininstr 
           where fininstr.t_FI_Code = substr(pFI_Code,1,length(fininstr.t_FI_Code)) --У строковых значений примечаний проблемы с типизацией
             and fininstr.t_FI_Kind = 1;
          return v_FIID;
        exception when NO_DATA_FOUND then 
          return -1;
        end;
     end if;
  END GetFIIDbyCode;
  
  FUNCTION GetContractorID( p_IsExchange IN NUMBER, p_PARTYID IN NUMBER, p_MARKETID IN NUMBER, p_BROKERID IN NUMBER ) RETURN NUMBER
  IS
  BEGIN
     return (case when p_IsExchange = 1 and p_PARTYID > 0 then p_PARTYID
                  when p_IsExchange = 1 then p_MARKETID
                  when p_MARKETID > 0 and p_BROKERID > 0 then p_BROKERID
                  else p_PARTYID end);
  END GetContractorID;  
  
  FUNCTION GetRatingRepOnDate( pObjType IN NUMBER, pObjID IN NUMBER, pNoteKind IN NUMBER, pParentId IN NUMBER, pRatingDate IN DATE) RETURN VARCHAR2
  IS
     v_RatingValue dobjattr_dbt.t_Name%TYPE := chr(1);
  BEGIN
     select attr.t_Name into v_RatingValue
       from dobjatcor_dbt atCor, dobjattr_dbt attr 
      where atCor.t_ObjectType = pObjType
        and atCor.t_GroupID    = pNoteKind
        and atCor.t_Object     = LPAD(pObjID, 10, '0')
        and pRatingDate between atCor.t_ValidFromDate and atCor.t_ValidToDate
        and attr.t_AttrID      = atCor.t_AttrID
        and attr.t_ObjectType  = atCor.t_ObjectType
        and attr.t_GroupID     = atCor.t_GroupID
        and attr.t_ParentID    = pParentId
        and (atCor.t_SysDate, atCor.t_SysTime) = (select max(atCor2.t_SysDate), max(atCor2.t_SysTime) 
                                                    from dobjatcor_dbt atCor2, dobjattr_dbt attr2
                                                   where atCor2.t_ObjectType = pObjType
                                                     and atCor2.t_GroupID    = pNoteKind
                                                     and atCor2.t_Object     = LPAD(pObjID, 10, '0')
                                                     and pRatingDate between atCor2.t_ValidFromDate and atCor2.t_ValidToDate
                                                     and attr2.t_AttrID      = atCor2.t_AttrID
                                                     and attr2.t_ObjectType  = atCor2.t_ObjectType
                                                     and attr2.t_GroupID     = atCor2.t_GroupID
                                                     and attr2.t_CodeList    = attr.t_CodeList);
     RETURN v_RatingValue;
  EXCEPTION WHEN OTHERS THEN

     RETURN chr(1);
  END GetRatingRepOnDate;

  FUNCTION GetRatingRep( pObjType IN NUMBER, pObjID IN NUMBER, pNoteKind IN NUMBER, pParentId IN NUMBER, pRatingDate IN DATE) RETURN VARCHAR2
  IS
     v_RatingValue dobjattr_dbt.t_Name%TYPE := chr(1);
     v_IsBank NUMBER := 0;
  BEGIN

    IF pObjType = 3 AND (pParentID = STANDARTPOORS_3453Y_RATING_PARENTID OR pParentID = MOODYS_3453Y_RATING_PARENTID OR pParentID = FITCHRATINGS_3453Y_RATING_PARENTID) THEN
      IF pRatingDate < TO_DATE('19.01.2015','DD.MM.YYYY') THEN
        v_RatingValue := CHR(1);
      ELSIF pRatingDate >= TO_DATE('19.01.2015','DD.MM.YYYY') AND pRatingDate < TO_DATE('25.02.2022','DD.MM.YYYY') THEN
        IF pObjID <> 618 THEN --ВЭБ.РФ не считаем КО
          SELECT NVL((SELECT 1 FROM dpartyown_dbt WHERE t_PartyID = pObjID AND t_PartyKind = 2 AND ROWNUM = 1), 0) INTO v_IsBank FROM DUAL;
        END IF;

        IF v_IsBank = 1 THEN
          v_RatingValue := GetRatingRepOnDate(pObjType, pObjID, pNoteKind, pParentId, TO_DATE('01.03.2014','DD.MM.YYYY'));
        ELSE
          v_RatingValue := GetRatingRepOnDate(pObjType, pObjID, pNoteKind, pParentId, TO_DATE('01.12.2014','DD.MM.YYYY'));
        END IF;
      ELSE
        v_RatingValue := GetRatingRepOnDate(pObjType, pObjID, pNoteKind, pParentId, TO_DATE('01.02.2022','DD.MM.YYYY'));
      END IF;
    ELSE
      v_RatingValue := GetRatingRepOnDate(pObjType, pObjID, pNoteKind, pParentId, pRatingDate);
    END IF;

    RETURN v_RatingValue;
  END;
  
  PROCEDURE CreateData_DV_NDEAL( OnDate IN DATE )
  IS
     v_toFIID NUMBER := -1;
     v_toFIID2 NUMBER := -1;
     v_toAcc daccount_dbt.t_Account%TYPE := chr(1);
     v_CSARecievedRest NUMBER := 0;
     v_CSATransferredRest NUMBER := 0;
     v_PointIndex NUMBER := 0;
     v_PointCount NUMBER := 0;
     TYPE pfiCreditRisk_t IS TABLE OF DDL_PFICREDITRISK_TMP%ROWTYPE;
     g_pfiCreditRisk_ins pfiCreditRisk_t := pfiCreditRisk_t();
     rep_creditRisk DDL_PFICREDITRISK_TMP%ROWTYPE;      
  BEGIN
     FOR rec IN ( select ndeal.t_ID, ndeal.t_DocKind, ndeal.t_Code, ndeal.t_ExtCode, ndeal.t_DVKind, ndeal.t_Type, ndeal.t_Contractor, ndeal.t_Sector, ndeal.t_MarketKind, ndeal.t_SwapType, ndeal.t_Forvard, 
                         to_char(ndeal.t_Date,'dd.mm.yyyy') t_Date, ndeal.t_OptionType, nFI.t_Price, nFI.t_Point, nFI.t_PriceFIID, nFI.t_ExecType, nFI.t_Type t_Part, fin.t_FIID, fin.t_FI_Kind, 
                         fin.t_FI_Code, fin.t_AvoirKind, fin.t_Name t_BAName, fin.t_Settlement_Code, nFI.t_Rate, nFI.t_RatePoint, nFI.t_StdFIID, 
                         NVL((select t_Name from dfininstr_dbt where t_FIID = nFI.t_RateID),chr(1)) t_RateName,
                         case when ndeal.t_DVKind = 4 then NVL(nFI2_prcswap.t_FIID, 0) else 0 end t_FI2_FIID,
                         case when ndeal.t_DVKind = 4 then NVL(nFI2_prcswap.t_Rate, 0) else 0 end t_FI2_Rate,
                         case when ndeal.t_DVKind = 4 then NVL(nFI2_prcswap.t_RatePoint, 0) else 0 end t_FI2_RatePoint,
                         case when ndeal.t_DVKind = 4 then NVL((select t_Name from dfininstr_dbt where t_FIID = nFI2_prcswap.t_RateID),chr(1)) else chr(1) end t_FI2_RateName,
                         NVL(nFI_frw.t_ExecDate, to_date('01.01.0001','DD.MM.YYYY')) t_DrawingDate,
                         NVL(csa.t_CSAID, 0) t_CSAID, NVL(csa.t_PartyMinPaySum, 0) t_PartyMinPaySum, NVL(csa.t_PartyMarginLimSum, 0) t_PartyMarginLimSum, NVL(csa.t_PartyMinPayCurr, -1) t_PartyMinPayCurr, 
                         NVL((select NVL((select t_Code from dllvalues_dbt where t_List = 4016 and t_Element = genagr.t_AuthorForm),'') || 
                                     ' Договор № ' || genagr.t_Code || ' от ' || to_char(genagr.t_Date_GenAgr,'DD.MM.YYYY')
                                from ddl_genagr_dbt genagr
                               where genagr.t_GenAgrID = ndeal.t_GenAgrID),chr(1)) t_GenAgrInfo,  
                         NVL((select case when genagr.t_Can_LiquidNetting = 'X' then 'Да' else 'Нет' end
                                from ddl_genagr_dbt genagr
                               where genagr.t_GenAgrID = ndeal.t_GenAgrID),chr(1)) t_GenAgrLiquidNetting,  
                         case when exists (select 1 
                                             from dpmpaym_dbt paym 
                                            where paym.t_Netting = 'X' 
                                              and paym.t_DocKind = ndeal.t_DocKind 
                                              and paym.t_DocumentID = ndeal.t_ID) 
                                   then 'Да' else 'Нет' end t_PaymNetting,                           
                         NVL((select party.t_ShortName 
                                from dparty_dbt party 
                               where party.t_PartyID = ndeal.t_Contractor), chr(1)) t_ContractorName,
                         NVL((select 'Банк' from dpartyown_dbt where t_PartyID = ndeal.t_Contractor and t_PartyKind = 2), 
                             (select decode(t_LegalForm, 1, 'ЮЛ', 'ФЛ') from dparty_dbt where t_PartyID = ndeal.t_Contractor)) t_ContractorKind,
                         NVL((select party.t_NRCountry 
                                from dparty_dbt party 
                               where party.t_PartyID = ndeal.t_Contractor), chr(1)) t_ContractorCountry,
                         NVL((select country.t_CountryID 
                                from dparty_dbt party, dcountry_dbt country 
                               where party.t_PartyID = ndeal.t_Contractor
                                 and country.t_CodeLat3 = party.t_NRCountry
                                 and rownum = 1), -1) t_CountryID,
                         NVL((select party.t_ShortName 
                                from dparty_dbt party 
                               where party.t_PartyID = fin.t_Issuer), chr(1)) t_IssuerShortName,
                         case when fin.t_FI_Kind = 7 then 
                                   NVL((select attr.t_Name 
                                          from dobjatcor_dbt atCor, dobjattr_dbt attr
                                         where atCor.t_ObjectType = 22 --Артикул
                                           and atCor.t_GroupID    = DL_CATEGORY_ARTICLEGROUP
                                           and atCor.t_Object     = LPAD(fin.t_FIID, 10, '0')
                                           and attr.t_AttrID      = atCor.t_AttrID
                                           and attr.t_ObjectType  = atCor.t_ObjectType
                                           and attr.t_GroupID     = atCor.t_GroupID),chr(1)) 
                              else chr(1) end t_ArticleGroupValue,
                         case when fin.t_FI_Kind = 2 then 
                                   NVL((select decode(NVL(t_ISIN,chr(1)), chr(1), t_LSIN, t_ISIN) 
                                          from davoiriss_dbt 
                                         where t_FIID = fin.t_FIID),chr(1))
                              else chr(1) end t_AvoirISIN,
                         case when (    (ndeal.t_DVKind in (3, 6) and nFI.t_Type = 2 and exists (select 1 
                                                                                                   from dpmpaym_dbt paym 
                                                                                                  where paym.t_DocKind = ndeal.t_DocKind  
                                                                                                    and paym.t_DocumentID = ndeal.t_ID  
                                                                                                    and paym.t_Purpose IN (Rsb_Payment.BAi, Rsb_Payment.CAi) --Платежи по 1ч
                                                                                                    and paym.t_ValueDate < OnDate
                                                                                                    and paym.t_PaymStatus <> Rsb_Payment.PM_REJECTED)
                                        )
                                     or (ndeal.t_DVKind = 4 and nFI.t_ExecType = RSB_Derivatives.DVSETTLEMET_STATE and exists (select 1 
                                                                                                                                 from dv_dvnpmgr pmgr
                                                                                                                                where pmgr.t_DealID = ndeal.t_ID 
                                                                                                                                  and pmgr.t_Type <> 3 
                                                                                                                                  and pmgr.t_PayDate < OnDate)
                                        )
                                    )
                                    then 1 else 0 end t_OneNominalExch, 
                         case when ndeal.t_DVKind in (3, 6) then greatest(nFI.t_PayDate, nFI.t_SuplDate) else nFI.t_ExecDate end t_EndDate,
                         case when ndeal.t_DVKind = 2 then NVL((select t_SzNameAlg from dnamealg_dbt where t_ITypeALG = 3424 and t_INumberAlg = ndeal.t_OptionStyle),chr(1)) else chr(1) end t_OptionStyleName,
                         case when ndeal.t_DVKind = 2 then NVL((select TO_NUMBER(lnk.t_AttrID) 
                                                                  from dobjlink_dbt lnk
                                                                 where LPAD(ndeal.t_ID, 34, 0) = lnk.t_ObjectID
                                                                   and lnk.t_ObjectType = Rsb_Derivatives.OBJTYPE_OUTOPER_DV
                                                                   and lnk.t_GroupID = 2
                                                                   and lnk.t_AttrType = 12
                                                                   and rownum = 1
                                                               ),-1)
                              else -1 end t_OptionLinkObj,
                         case when ndeal.t_DVKind = 4 then NVL((select sum(pmgr.t_Amount) 
                                                                 from dv_dvnpmgr pmgr
                                                                where pmgr.t_DealID = ndeal.t_ID 
                                                                  and pmgr.t_Side = 2 
                                                                  and pmgr.t_Type = 3 
                                                                  and (pmgr.t_Status = 1 or (pmgr.t_Status = 2 and pmgr.t_PayDate >= OnDate))
                                                               ),0) 
                              else 0 end t_ReqPmgrSum,  
                         case when ndeal.t_DVKind = 4 then NVL((select sum(pmgr.t_Amount) 
                                                                 from dv_dvnpmgr pmgr
                                                                where pmgr.t_DealID = ndeal.t_ID 
                                                                  and pmgr.t_Side = 1 
                                                                  and pmgr.t_Type = 3 
                                                                  and (pmgr.t_Status = 1 or (pmgr.t_Status = 2 and pmgr.t_PayDate >= OnDate))
                                                               ),0) 
                              else 0 end t_OblPmgrSum,                                                    
                         case when ndeal.t_DVKind = 4 then NVL((select t_FixDate 
                                                                  from (select RSI_RsbCalendar.GetDateAfterWorkDay(pmgr.t_PayDate, (case when pmgr.t_Side = 1 then nFI.t_FixDays else nFI2_prcswap.t_FixDays end), (case when pmgr.t_Side = 1 then nFI.t_CalKindID else nFI2_prcswap.t_CalKindID end)) t_FixDate
                                                                          from dv_dvnpmgr pmgr
                                                                         where pmgr.t_DealID = ndeal.t_ID 
                                                                           and pmgr.t_Type = 3
                                                                           and (   ndeal.t_Type = RSB_Derivatives.ALG_DV_FLOAT_FLOAT
                                                                                or (pmgr.t_Side = 1 and ndeal.t_Type = RSB_Derivatives.ALG_DV_FLOAT_FIX)
                                                                                or (pmgr.t_Side = 2 and ndeal.t_Type = RSB_Derivatives.ALG_DV_FIX_FLOAT)
                                                                               )
                                                                           and RSI_RsbCalendar.GetDateAfterWorkDay(pmgr.t_PayDate, (case when pmgr.t_Side = 1 then nFI.t_FixDays else nFI2_prcswap.t_FixDays end), (case when pmgr.t_Side = 1 then nFI.t_CalKindID else nFI2_prcswap.t_CalKindID end)) 
                                                                               between trunc(OnDate,'MM') /*Первый день месяца*/ and last_day(OnDate) /*Последний день месяца*/
                                                                      order by abs(OnDate - RSI_RsbCalendar.GetDateAfterWorkDay(pmgr.t_PayDate, (case when pmgr.t_Side = 1 then nFI.t_FixDays else nFI2_prcswap.t_FixDays end), (case when pmgr.t_Side = 1 then nFI.t_CalKindID else nFI2_prcswap.t_CalKindID end))) desc
                                                                       )
                                                                 where rownum = 1
                                                               ), 
                                                               case when exists(select 1 
                                                                                  from dv_dvnpmgr pmgr
                                                                                 where pmgr.t_DealID = ndeal.t_ID
                                                                                   and pmgr.t_Type = 3
                                                                                   and (   ndeal.t_Type = RSB_Derivatives.ALG_DV_FLOAT_FLOAT
                                                                                        or (pmgr.t_Side = 1 and ndeal.t_Type = RSB_Derivatives.ALG_DV_FLOAT_FIX)
                                                                                        or (pmgr.t_Side = 2 and ndeal.t_Type = RSB_Derivatives.ALG_DV_FIX_FLOAT)
                                                                                       )
                                                                                   and RSI_RsbCalendar.GetDateAfterWorkDay(pmgr.t_PayDate, (case when pmgr.t_Side = 1 then nFI.t_FixDays else nFI2_prcswap.t_FixDays end), (case when pmgr.t_Side = 1 then nFI.t_CalKindID else nFI2_prcswap.t_CalKindID end)) > last_day(OnDate)) 
                                                                         then last_day(OnDate)
                                                                    else to_date('01.01.0001','dd.mm.yyyy') 
                                                                end 
                                                              ) 
                              else to_date('01.01.0001','dd.mm.yyyy') end t_DateOfInterestRate,
                         NVL((select attr.t_Name 
                                from dobjatcor_dbt atCor, dobjattr_dbt attr
                               where atCor.t_ObjectType = 152 --Соглашение CSA
                                 and atCor.t_GroupID    = DVCSA_CATEGORY_MARGINKIND
                                 and atCor.t_Object     = LPAD(ndeal.t_CSAID, 34, '0')
                                 and attr.t_AttrID      = atCor.t_AttrID
                                 and attr.t_ObjectType  = atCor.t_ObjectType
                                 and attr.t_GroupID     = atCor.t_GroupID),'') t_MarginKind,
                         NVL((select attr.t_Name 
                                from dobjatcor_dbt atCor, dobjattr_dbt attr
                               where atCor.t_ObjectType = 152 --Соглашение CSA
                                 and atCor.t_GroupID    = DVCSA_CATEGORY_MARGINPAYER
                                 and atCor.t_Object     = LPAD(ndeal.t_CSAID, 34, '0')
                                 and attr.t_AttrID      = atCor.t_AttrID
                                 and attr.t_ObjectType  = atCor.t_ObjectType
                                 and attr.t_GroupID     = atCor.t_GroupID),'') t_MarginPayer,
                         NVL((select cast(RSB_Struct.getString(note.t_text) as varchar2(50))
                                from dnotetext_dbt note
                               where note.t_ObjectType = 152 --Соглашение CSA
                                 and note.t_DocumentID = LPAD(ndeal.t_CSAID, 34, '0')
                                 and note.t_NoteKind = DVCSA_NOTE_MARGINPERIOD
                                 and OnDate-1 between note.t_Date and note.t_ValidToDate),-1) t_MarginPeriod,
                         NVL((select cast(RSB_Struct.getString(note.t_text) as varchar2(50))
                                from dnotetext_dbt note
                               where note.t_ObjectType = 152 --Соглашение CSA
                                 and note.t_DocumentID = LPAD(ndeal.t_CSAID, 34, '0')
                                 and note.t_NoteKind = DVCSA_NOTE_MARGINFACTNUMDAYS
                                 and OnDate-1 between note.t_Date and note.t_ValidToDate),-1) t_MarginFactNumDays,
                         NVL((select RSB_Struct.getMoney(note.t_text)
                                from dnotetext_dbt note
                               where note.t_ObjectType = 145 --Внебиржевая операция с ПИ
                                 and note.t_DocumentID = LPAD(ndeal.t_ID, 34, '0')
                                 and note.t_NoteKind = DVNDEAL_NOTE_NETCOLLATERALSUM
                                 and OnDate-1 between note.t_Date and note.t_ValidToDate),0) t_NetCollateralSum,
                         NVL((select cast(RSB_Struct.getString(note.t_text) as varchar2(15))
                                from dnotetext_dbt note
                               where note.t_ObjectType = 145 --Внебиржевая операция с ПИ
                                 and note.t_DocumentID = LPAD(ndeal.t_ID, 34, '0')
                                 and note.t_NoteKind = DVNDEAL_NOTE_NETCOLLATERALCURR
                                 and OnDate-1 between note.t_Date and note.t_ValidToDate),chr(1)) t_NetCollateralCurr                                     
                        from ddvndeal_dbt ndeal, ddvnfi_dbt nFI_frw, ddvnfi_dbt nFI, ddvnfi_dbt nFI2_prcswap, dfininstr_dbt fin, ddvcsa_dbt csa, doproper_dbt oper
                       where ndeal.t_Date   < OnDate
                         and ndeal.t_State  > 0
                         and ndeal.t_IsPFI  = CHR(88)
                         and ndeal.t_Client = -1
                         and RSB_Derivatives.DV_IsExistOperStep(ndeal.t_ID, ndeal.t_DocKind, 'Ф', 'X', OnDate-1) > 0 
                         and nFI.t_DealID   = ndeal.t_ID   
                         and nFI_frw.t_DealID(+) = ndeal.t_ID 
                         and nFI_frw.t_Type(+) = 1
                         and nFI2_prcswap.t_DealID(+) = ndeal.t_ID 
                         and nFI2_prcswap.t_Type(+) = 2
                         and csa.t_CSAID(+) = ndeal.t_CSAID
                         and csa.t_BegDate(+) < OnDate
                         and (csa.t_EndDate(+) = to_date('01.01.0001','dd.mm.yyyy') or csa.t_EndDate(+) >= OnDate)
                         and  oper.t_dockind = ndeal.t_dockind and oper.t_documentid = lpad (ndeal.t_id, 34, '0')
                         and (oper.t_end_date = to_date ('01010001','ddmmyyyy') or oper.t_end_date >= OnDate)
                         and not exists (select 1 
                                           from doproper_dbt opr 
                                          where To_Number(opr.t_DocumentID) = ndeal.t_CSAID 
                                            and opr.t_DocKind = RSB_Secur.DV_CSA 
                                            and opr.t_End_Date <> to_date('01.01.0001','dd.mm.yyyy') 
                                            and opr.t_End_Date < OnDate)
                         and fin.t_FIID = case when nFI.t_StdFIID > 0 then nFI.t_StdFIID when ndeal.t_Forvard = 'X' then nFI_frw.t_FIID else nFI.t_FIID end
                         and (    (nFI.t_Type = 0 and ndeal.t_DVKind <> 4 --Процентные свопы отберем дальше, исходя из платежей по 2ч
                                   and not exists (select 1 
                                                     from dpmpaym_dbt paym 
                                                    where paym.t_DocKind = ndeal.t_DocKind  
                                                      and paym.t_DocumentID = ndeal.t_ID  
                                                      and paym.t_Purpose IN (Rsb_Payment.BAi, Rsb_Payment.CAi) --Платежи по 1ч
                                                      and paym.t_ValueDate < OnDate
                                                      and paym.t_PaymStatus <> Rsb_Payment.PM_REJECTED)
                                  ) 
                               or ( ((ndeal.t_DVKind in (3, 6) and nFI.t_Type = 2) or (ndeal.t_DVKind = 4 and nFI.t_Type = 0))
                                    and not exists (select 1 
                                                      from dpmpaym_dbt paym 
                                                     where paym.t_DocKind = ndeal.t_DocKind  
                                                       and paym.t_DocumentID = ndeal.t_ID  
                                                       and paym.t_Purpose IN (Rsb_Payment.BRi, Rsb_Payment.CRi) --Платежи по 2ч
                                                       and paym.t_ValueDate < OnDate
                                                       and paym.t_PaymStatus <> Rsb_Payment.PM_REJECTED)
                                  ) 
                             ) 
                )
     LOOP

       rep_creditRisk.t_DealID   := rec.t_ID;
       rep_creditRisk.t_DocKind  := rec.t_DocKind;
       rep_creditRisk.t_DealCode := case when rec.t_Code <> chr(1) then rec.t_Code else rec.t_ExtCode end;
       rep_creditRisk.t_Part     := rec.t_Part;    
             
       rep_creditRisk.t_PFICategory     := chr(1); 
       rep_creditRisk.t_PFISubCategory  := chr(1);
       rep_creditRisk.t_GoodsType       := chr(1);
       rep_creditRisk.t_DocKindName     := chr(1);
       rep_creditRisk.t_BAKind          := chr(1);
       rep_creditRisk.t_SecurFlag       := chr(1);
       rep_creditRisk.t_SecurIssuer     := chr(1);
       rep_creditRisk.t_NumNominalExch  := 0;
       rep_creditRisk.t_PayRateType     := chr(1);
       rep_creditRisk.t_ReceiveRateType := chr(1);
       rep_creditRisk.t_OptionType      := chr(1);
       rep_creditRisk.t_OptionPrice     := 0;
       rep_creditRisk.t_OptionPricePoint:= 0;
       rep_creditRisk.t_OptionCost      := 0;
       rep_creditRisk.t_OptionCostPoint := 0;
       rep_creditRisk.t_OptionStyle     := chr(1);       
       rep_creditRisk.t_PayRateType     := chr(1);
       rep_creditRisk.t_PayFixRate      := 0;
       rep_creditRisk.t_PayFixRatePoint := 0;
       rep_creditRisk.t_ReceiveRateType := chr(1);
       rep_creditRisk.t_ReceiveFixRate  := 0;
       rep_creditRisk.t_ReceiveFixRatePoint := 0;
       rep_creditRisk.t_ReqPmgrSum      := 0;
       rep_creditRisk.t_ReqPmgrCurr     := -1;
       rep_creditRisk.t_ReqPmgrSumRub   := 0;                                      
       rep_creditRisk.t_OblPmgrSum      := 0;
       rep_creditRisk.t_OblPmgrCurr     := -1;                                       
       rep_creditRisk.t_OblPmgrSumRub   := 0;                                       
       rep_creditRisk.t_DateOfInterestRate := to_date('01.01.0001','DD.MM.YYYY');
       rep_creditRisk.t_FloatRateAcc    := chr(1);

       rep_creditRisk.t_ContractorName  := rec.t_ContractorName;
       rep_creditRisk.t_ContractorKind  := rec.t_ContractorKind;
       rep_creditRisk.t_Country         := rec.t_ContractorCountry;
       rep_creditRisk.t_GenAgrInfo      := rec.t_GenAgrInfo;
       rep_creditRisk.t_IsLiquidNetting := rec.t_GenAgrLiquidNetting;
       rep_creditRisk.t_IsNetting       := rec.t_PaymNetting;
       rep_creditRisk.t_PeriodCls       := case when rec.t_DVKind = 5 then 'срочная' else 'ПФИ' end;
       
       rep_creditRisk.t_StartDate       := rec.t_Date;
       rep_creditRisk.t_EndDate         := rec.t_EndDate; 
       rep_creditRisk.t_StartDateBA     := to_date('01.01.0001','DD.MM.YYYY');
       rep_creditRisk.t_EndDateBA       := rec.t_DrawingDate;
       
       rep_creditRisk.t_PFICategory := case when    (rec.t_DVKind = 4 and rec.t_ExecType = RSB_Derivatives.DVSETTLEMET_CALC)  
                                                 or((rec.t_DVKind = 1 or rec.t_DVKind = 2 or rec.t_DVKind = 5) and (   (rec.t_FI_Kind = 2 and (   (RSB_FIInstr.FI_AvrKindsGetRoot(rec.t_FI_KIND, rec.t_AvoirKind) = RSI_RSB_FIInstr.AVOIRKIND_BOND) 
                                                                                                                                               or (RSB_FIInstr.FI_AvrKindsGetRoot(rec.t_FI_KIND, rec.t_AvoirKind) = RSI_RSB_FIInstr.AVOIRISSKIND_BASKET))) 
                                                                                                                    or (rec.t_FI_Kind = 3 and rec.t_Settlement_Code = 9 ))) --Кредитный
                                                 then 'Процентные' 
                                            when   ((rec.t_DVKind = 1 or rec.t_DVKind = 2 or rec.t_DVKind = 5) and rec.t_FI_Kind = 3 and rec.t_Settlement_Code = 1 ) --Валютный
                                                 or((rec.t_DVKind = 1 or rec.t_DVKind = 2 or rec.t_DVKind = 5) and rec.t_FI_Kind = 1)
                                                 or (rec.t_DVKind = 3 and rec.t_FI_Kind = 1)
                                                 or (rec.t_DVKind = 6 and rec.t_FI_Kind = 1)
                                                 or (rec.t_DVKind = 4 and rec.t_ExecType = RSB_Derivatives.DVSETTLEMET_STATE)
                                                 then 'Валютные' 
                                            when   ((rec.t_DVKind = 1 or rec.t_DVKind = 2 or rec.t_DVKind = 5) and (   (rec.t_FI_Kind = 2 and RSB_FIInstr.FI_AvrKindsGetRoot(rec.t_FI_KIND, rec.t_AvoirKind) = RSI_RSB_FIInstr.AVOIRKIND_SHARE) 
                                                                                                                    or (rec.t_FI_Kind = 3 and rec.t_Settlement_Code = 2 ))) --Фондовый
                                                 then 'Фондовые'
                                            when   ((rec.t_DVKind = 1 or rec.t_DVKind = 2 or rec.t_DVKind = 5) and (rec.t_FI_Kind = 6 or rec.t_FI_Kind = 7)
                                                 or((rec.t_DVKind = 3 or rec.t_DVKind = 6) and rec.t_FI_Kind = 6))
                                                 then 'Товарные'
                                       end;
       
       if( rep_creditRisk.t_PFICategory = 'Товарные' ) then
          rep_creditRisk.t_PFISubCategory := case when rec.t_FI_Kind = 6 then 'Металл' 
                                                  when rec.t_FI_Kind = 7 then rec.t_ArticleGroupValue 
                                             end;
       elsif( rep_creditRisk.t_PFICategory = 'Фондовые' ) then
          rep_creditRisk.t_PFISubCategory := case when rec.t_FI_Kind = 2 and RSB_FIInstr.FI_AvrKindsGetRoot(rec.t_FI_Kind, rec.t_AvoirKind) = RSI_RSB_FIInstr.AVOIRKIND_SHARE 
                                                     then 'Фондовые по эмитенту' 
                                                  when rec.t_FI_Kind = 3 
                                                     then 'Фондовые по индексу ценных бумаг' 
                                             end;
       end if;  
       
       if( rep_creditRisk.t_PFICategory = 'Товарные' ) then 
          rep_creditRisk.t_GoodsType := rec.t_BAName;
       end if; 
       
       rep_creditRisk.t_PFIVolType := case when rec.t_DVKind = 4 and rec.t_FIID = rec.t_FI2_FIID and rec.t_ExecType = RSB_Derivatives.DVSETTLEMET_CALC and rec.t_Type = RSB_Derivatives.ALG_DV_FLOAT_FLOAT
                                              then 'Базисный' else chr(1) end;
       
       rep_creditRisk.t_DocKindName := case when rec.t_DVKind = 1 
                                                 then 'Форвард'
                                            when rec.t_DVKind = 2 
                                                 then (case when rec.t_Type = RSB_Derivatives.ALG_DV_BUY then 'Покупка' else 'Продажа' end) || ' опциона ' ||                                       
                                                      (case when (rec.t_FI_Kind = 3 and rec.t_Forvard <> 'X') 
                                                                then (case when rec.t_OptionType = 1 then 'floor' else 'cap' end) 
                                                            else (case when rec.t_OptionType = 1 then 'Put' else 'Call' end) 
                                                        end) 
                                            when rec.t_DVKind = 3 or rec.t_DVKind = 6 
                                                 then 'СВОП'                        
                                            when rec.t_DVKind = 4 
                                                 then (case when instr(rec.t_Code, 'FRA:') = 1 and instr(rec.t_ExtCode, 'FRA:') = 1 
                                                               then 'Процентный форвард'
                                                            else (case when rec.t_ExecType = RSB_Derivatives.DVSETTLEMET_CALC 
                                                                          then 'Процентный СВОП' 
                                                                       else 'Валютно-процентный СВОП' 
                                                                  end)
                                                       end)
                                            when rec.t_DVKind = 5 
                                                 then 'Покупка/продажа Т+3'
                                       end;

       rep_creditRisk.t_BAKind := case when rec.t_StdFIID > 0
                                          then rec.t_FI_Code
                                       when (rec.t_DVKind = 4 and not (instr(rec.t_Code, 'FRA:') = 1 and instr(rec.t_ExtCode, 'FRA:') = 1)) or (rec.t_DVKind = 2 and rec.t_FI_Kind = 3 and rec.t_Forvard <> 'X')
                                          then 'Процентная ставка' 
                                       when rec.t_FI_Kind = 1 
                                          then 'Ин. валюта' 
                                       when rec.t_FI_Kind = 6
                                          then 'Драг. металл' 
                                       when rec.t_FI_Kind = 2
                                          then rec.t_AvoirISIN
                                       else rec.t_BAName 
                                  end;
            
       rep_creditRisk.t_SecurFlag := case when rep_creditRisk.t_PFICategory = 'Процентные' and rec.t_FI_Kind <> 3 and rec.t_DVKind <> 4
                                               then 'Долговые' 
                                          when rep_creditRisk.t_PFICategory = 'Фондовые' and rec.t_FI_Kind <> 3
                                               then 'Долевые'
                                          when (rep_creditRisk.t_PFICategory = 'Процентные' or rep_creditRisk.t_PFICategory = 'Фондовые') and rec.t_FI_Kind = 3 and (rec.t_DVKind <> 2 or rec.t_Forvard = 'X')
                                               then rec.t_BAName
                                     end;
       
       if( rep_creditRisk.t_SecurFlag = 'Долговые' or rep_creditRisk.t_SecurFlag = 'Долевые' ) then 
          rep_creditRisk.t_SecurIssuer := rec.t_IssuerShortName;   
       end if; 
       
       if( rec.t_DVKind = 3 or rec.t_DVKind = 6 or (rec.t_DVKind = 4 and rec.t_ExecType = RSB_Derivatives.DVSETTLEMET_STATE) ) then
          if( rec.t_OneNominalExch = 1 ) then
             rep_creditRisk.t_NumNominalExch := 1; 
          else 
             rep_creditRisk.t_NumNominalExch := 2; 
          end if; 
       end if;
       
       rep_creditRisk.t_ExecType   := case when rec.t_ExecType = RSB_Derivatives.DVSETTLEMET_STATE then 'Поставочная' else 'Беспоставочная' end;
       rep_creditRisk.t_IsExchange := case when rec.t_Sector = 'X' then 'Биржевая' else 'Внебиржевая' end;
       
       rep_creditRisk.t_BuySale    := case when rec.t_DVKind = 1 or rec.t_DVKind = 5 
                                                then (case when rec.t_Type = RSB_Derivatives.ALG_DV_BUY then 'Покупка' else 'Продажа' end)
                                           when rec.t_DVKind = 2 
                                                then (case when (rec.t_Type = RSB_Derivatives.ALG_DV_BUY and rec.t_OptionType = 1) or (rec.t_Type = RSB_Derivatives.ALG_DV_SALE and rec.t_OptionType = 2) 
                                                                 then 'Продажа' else 'Покупка' end)
                                           when rec.t_DVKind = 3 or rec.t_DVKind = 6 
                                                then (case when (rec.t_Part = 0 and rec.t_Type = RSB_Derivatives.ALG_DV_BS) or (rec.t_Part = 2 and rec.t_Type = RSB_Derivatives.ALG_DV_SB) 
                                                                 then 'Покупка' else 'Продажа' end)
                                           when rec.t_DVKind = 4
                                                then (case when    rec.t_Type = RSB_Derivatives.ALG_DV_FIX_FLOAT 
                                                                or ((rec.t_Type = RSB_Derivatives.ALG_DV_FIX_FIX or rec.t_Type = RSB_Derivatives.ALG_DV_FLOAT_FLOAT) 
                                                                     and rec.t_FIID = RSI_RSB_FIInstr.NATCUR and rec.t_FI2_FIID <> RSI_RSB_FIInstr.NATCUR) 
                                                                then 'Покупка'
                                                           when rec.t_Type = RSB_Derivatives.ALG_DV_FLOAT_FIX 
                                                                or ((rec.t_Type = RSB_Derivatives.ALG_DV_FIX_FIX or rec.t_Type = RSB_Derivatives.ALG_DV_FLOAT_FLOAT) 
                                                                     and rec.t_FIID <> RSI_RSB_FIInstr.NATCUR and rec.t_FI2_FIID = RSI_RSB_FIInstr.NATCUR)
                                                                then 'Продажа'
                                                           else 'Покупка/продажа'
                                                      end)
                                      end; 
            
       if(   ((rec.t_DVKind = 1 or rec.t_DVKind = 2 or rec.t_DVKind = 5) and rec.t_Type = RSB_Derivatives.ALG_DV_BUY)
          or ((rec.t_DVKind = 3 or rec.t_DVKind = 6) and ((rec.t_Part = 0 and rec.t_Type = RSB_Derivatives.ALG_DV_BS) or (rec.t_Part = 2 and rec.t_Type = RSB_Derivatives.ALG_DV_SB)))
          or (rec.t_DVKind = 4 and (    rec.t_Type = RSB_Derivatives.ALG_DV_FIX_FLOAT 
                                     or ((rec.t_Type = RSB_Derivatives.ALG_DV_FIX_FIX or rec.t_Type = RSB_Derivatives.ALG_DV_FLOAT_FLOAT) 
                                          and rec.t_FIID = RSI_RSB_FIInstr.NATCUR and rec.t_FI2_FIID <> RSI_RSB_FIInstr.NATCUR) )
             )
         ) then
          rep_creditRisk.t_BankPosition := 'Покупатель';
       elsif(   ((rec.t_DVKind = 1 or rec.t_DVKind = 2 or rec.t_DVKind = 5) and rec.t_Type = RSB_Derivatives.ALG_DV_SALE)
             or ((rec.t_DVKind = 3 or rec.t_DVKind = 6) and ((rec.t_Part = 0 and rec.t_Type = RSB_Derivatives.ALG_DV_SB) or (rec.t_Part = 2 and rec.t_Type = RSB_Derivatives.ALG_DV_BS)))
             or (rec.t_DVKind = 4 and (    rec.t_Type = RSB_Derivatives.ALG_DV_FLOAT_FIX
                                        or ((rec.t_Type = RSB_Derivatives.ALG_DV_FIX_FIX or rec.t_Type = RSB_Derivatives.ALG_DV_FLOAT_FLOAT) 
                                             and rec.t_FIID <> RSI_RSB_FIInstr.NATCUR and rec.t_FI2_FIID = RSI_RSB_FIInstr.NATCUR) )
                )
            ) then
          rep_creditRisk.t_BankPosition := 'Продавец';
       else
          rep_creditRisk.t_BankPosition := 'Покупатель/продавец';
       end if;
       
       if( rec.t_DVKind = 4 ) then --Для процентных свопов сначала пытаемся найти счет без остатка, а если не нашли, то уже берем любой нулевой
          rep_creditRisk.t_ReqSum := GetAccRestOnDate( rec.t_ID, rec.t_DocKind, OnDate-1, DVNDEAL_PLUS_CAT_NOMINAL, 1, -1, rep_creditRisk.t_ReqCurr, rep_creditRisk.t_ReqAcc );
       end if;  
       if( (rec.t_DVKind = 4 and rep_creditRisk.t_ReqAcc = chr(1)) or rec.t_DVKind <> 4 ) then
          rep_creditRisk.t_ReqSum := GetAccRestOnDate( rec.t_ID, rec.t_DocKind, OnDate-1, DVNDEAL_PLUS_CAT_NOMINAL, 0,
                                                       (case when rec.t_DVKind = 3 or rec.t_DVKind = 6 then (case when rep_creditRisk.t_BuySale = 'Покупка' then rec.t_FIID else rec.t_PriceFIID end) else -1 end),
                                                       rep_creditRisk.t_ReqCurr, rep_creditRisk.t_ReqAcc 
                                                     );
       end if;
       if( rep_creditRisk.t_ReqAcc <> chr(1) and rep_creditRisk.t_ReqCurr <> RSI_RSB_FIInstr.NATCUR ) then
          rep_creditRisk.t_ReqSumRub := RSB_FIInstr.ConvSum( rep_creditRisk.t_ReqSum, rep_creditRisk.t_ReqCurr, RSI_RSB_FIInstr.NATCUR, OnDate-1 ); 
       else  
          rep_creditRisk.t_ReqSumRub := rep_creditRisk.t_ReqSum;
       end if;
       
       if( rec.t_DVKind = 4 ) then
          rep_creditRisk.t_OblSum := GetAccRestOnDate( rec.t_ID, rec.t_DocKind, OnDate-1, DVNDEAL_MINUS_CAT_NOMINAL, 1, -1, rep_creditRisk.t_OblCurr, rep_creditRisk.t_OblAcc );
       end if;
       if( (rec.t_DVKind = 4 and rep_creditRisk.t_OblAcc = chr(1)) or rec.t_DVKind <> 4 ) then       
          rep_creditRisk.t_OblSum := GetAccRestOnDate( rec.t_ID, rec.t_DocKind, OnDate-1, DVNDEAL_MINUS_CAT_NOMINAL, 0, 
                                                       (case when rec.t_DVKind = 3 or rec.t_DVKind = 6 then (case when rep_creditRisk.t_BuySale = 'Продажа' then rec.t_FIID else rec.t_PriceFIID end) else -1 end), 
                                                       rep_creditRisk.t_OblCurr, rep_creditRisk.t_OblAcc 
                                                     );           
       end if;               
       if( rep_creditRisk.t_OblAcc <> chr(1) and rep_creditRisk.t_OblCurr <> RSI_RSB_FIInstr.NATCUR ) then
          rep_creditRisk.t_OblSumRub := RSB_FIInstr.ConvSum( rep_creditRisk.t_OblSum, rep_creditRisk.t_OblCurr, RSI_RSB_FIInstr.NATCUR, OnDate-1 ); 
       else 
          rep_creditRisk.t_OblSumRub := rep_creditRisk.t_OblSum;
       end if;
       
       if( rep_creditRisk.t_BuySale = 'Покупка' ) then
          rep_creditRisk.t_PFINominal := rep_creditRisk.t_ReqSum;
          rep_creditRisk.t_NominalCurrency := GetFI_Code( rep_creditRisk.t_ReqCurr ); 
       else
          rep_creditRisk.t_PFINominal := rep_creditRisk.t_OblSum;
          rep_creditRisk.t_NominalCurrency := GetFI_Code( rep_creditRisk.t_OblCurr ); 
       end if;
       
       if( (rec.t_DVKind = 3 or rec.t_DVKind = 6) and rec.t_Part = 0 ) then
          rep_creditRisk.t_FairValueAcc := chr(1);
          rep_creditRisk.t_FairValueActive := 0;
          rep_creditRisk.t_FairValuePassive := 0;   
       elsif( rec.t_DVKind = 2 and rec.t_OptionLinkObj > -1 ) then 
          rep_creditRisk.t_FairValueActive := GetAccRestOnDate( rec.t_OptionLinkObj, Rsb_Secur.DLDOC_ISSUE, OnDate-1, '+ПФИ ОЭБ', 1, -1, v_toFIID, rep_creditRisk.t_FairValueAcc );
          if( rep_creditRisk.t_FairValueAcc <> chr(1) ) then
             rep_creditRisk.t_FairValuePassive := 0; 
          else
             rep_creditRisk.t_FairValuePassive := GetAccRestOnDate( rec.t_OptionLinkObj, Rsb_Secur.DLDOC_ISSUE, OnDate-1, '-ПФИ ОЭБ', 1, -1, v_toFIID, rep_creditRisk.t_FairValueAcc );   
          end if;    
       else
          rep_creditRisk.t_FairValueActive := GetAccRestOnDate( rec.t_ID, rec.t_DocKind, OnDate-1, '+ПФИ', 1, -1, v_toFIID, rep_creditRisk.t_FairValueAcc );
          if( rep_creditRisk.t_FairValueAcc <> chr(1) ) then
             rep_creditRisk.t_FairValuePassive := 0; --Остаток должен быть только на одном счёте
          else
             rep_creditRisk.t_FairValuePassive := GetAccRestOnDate( rec.t_ID, rec.t_DocKind, OnDate-1, '-ПФИ', 1, -1, v_toFIID, rep_creditRisk.t_FairValueAcc );   
          end if;
       end if;
            
       if( rec.t_DVKind = 4 ) then
          rep_creditRisk.t_PayRateType     := rec.t_RateName;
          rep_creditRisk.t_PayFixRate      := rec.t_Rate;
          rep_creditRisk.t_PayFixRatePoint := rec.t_RatePoint;
          rep_creditRisk.t_ReceiveRateType := rec.t_FI2_RateName;
          rep_creditRisk.t_ReceiveFixRate  := rec.t_FI2_Rate;
          rep_creditRisk.t_ReceiveFixRatePoint := rec.t_FI2_RatePoint;
          
          rep_creditRisk.t_ReqPmgrSum      := rec.t_ReqPmgrSum;
          rep_creditRisk.t_ReqPmgrCurr     := rec.t_FI2_FIID;
          rep_creditRisk.t_ReqPmgrSumRub   := case when rep_creditRisk.t_ReqPmgrCurr > -1 and rep_creditRisk.t_ReqPmgrCurr <> RSI_RSB_FIInstr.NATCUR 
                                                        then RSB_FIInstr.ConvSum( rep_creditRisk.t_ReqPmgrSum, rec.t_FI2_FIID, RSI_RSB_FIInstr.NATCUR, OnDate-1 ) 
                                                   else rep_creditRisk.t_ReqPmgrSum end;                                       
          rep_creditRisk.t_OblPmgrSum      := rec.t_OblPmgrSum; 
          rep_creditRisk.t_OblPmgrCurr     := rec.t_FIID;                                      
          rep_creditRisk.t_OblPmgrSumRub   := case when rep_creditRisk.t_OblPmgrCurr > -1 and rep_creditRisk.t_OblPmgrCurr <> RSI_RSB_FIInstr.NATCUR 
                                                        then RSB_FIInstr.ConvSum( rep_creditRisk.t_OblPmgrSum, rec.t_FIID, RSI_RSB_FIInstr.NATCUR, OnDate-1 ) 
                                                   else rep_creditRisk.t_OblPmgrSum end;                                        
          rep_creditRisk.t_DateOfInterestRate := rec.t_DateOfInterestRate;
          if( rec.t_Type <> RSB_Derivatives.ALG_DV_FIX_FIX ) then
             rep_creditRisk.t_FloatRateAcc    := case when (rec.t_Type = RSB_Derivatives.ALG_DV_FLOAT_FLOAT or rep_creditRisk.t_BuySale = 'Покупка/продажа' or rep_creditRisk.t_BuySale = chr(1)) 
                                                            and (rep_creditRisk.t_ReqAcc <> chr(1) or rep_creditRisk.t_OblAcc <> chr(1))
                                                         then (case when rep_creditRisk.t_ReqAcc <> chr(1) then substr(rep_creditRisk.t_ReqAcc,1,5) else '' end) || 
                                                              (case when rep_creditRisk.t_ReqAcc <> chr(1) and rep_creditRisk.t_OblAcc <> chr(1) then ', ' else '' end) ||
                                                              (case when rep_creditRisk.t_OblAcc <> chr(1) then substr(rep_creditRisk.t_OblAcc,1,5) else '' end)
                                                      when rep_creditRisk.t_BuySale = 'Покупка' and rep_creditRisk.t_ReqAcc <> chr(1) then substr(rep_creditRisk.t_ReqAcc,1,5)
                                                      when rep_creditRisk.t_BuySale = 'Продажа' and rep_creditRisk.t_OblAcc <> chr(1) then substr(rep_creditRisk.t_OblAcc,1,5)
                                                      else chr(1)
                                                 end;       
          end if;    
       end if;    
       
       if( rec.t_DVKind = 2 ) then
          rep_creditRisk.t_OptionType       := case when (rec.t_FI_Kind = 3 and rec.t_Forvard <> 'X') 
                                                      then (case when rec.t_OptionType = 1 then 'floor' else 'cap' end)
                                                    else (case when rec.t_OptionType = 1 then 'Put' else 'Call' end) 
                                               end;   
          rep_creditRisk.t_OptionPrice      := case when (rec.t_FI_Kind = 1 and rec.t_Forvard <> 'X') then RSB_FIInstr.ConvSum(1, rec.t_FIID, RSI_RSB_FIInstr.NATCUR, OnDate-1) else -1 end;
          v_PointIndex := InStr(to_char(rep_creditRisk.t_OptionPrice),'.');
          v_PointCount := case when v_PointIndex > 0 then Length(SubStr(to_char(rep_creditRisk.t_OptionPrice), v_PointIndex + 1)) else 0 end; --Считаем кол-во десятичных знаков
          rep_creditRisk.t_OptionPricePoint := case when v_PointCount > 2 then v_PointCount else 2 end; 
          rep_creditRisk.t_OptionCost       := case when (rec.t_FI_Kind = 3 and rec.t_Forvard <> 'X') then -1 else rec.t_Price end;
          rep_creditRisk.t_OptionCostPoint  := rec.t_Point;
          rep_creditRisk.t_OptionStyle      := rec.t_OptionStyleName;   
       end if; 
       
       if( rec.t_CSAID > 0 ) then
          rep_creditRisk.t_Margining           := 'Да';
          rep_creditRisk.t_MarginKind          := rec.t_MarginKind || '-' || rec.t_MarginPayer;
          rep_creditRisk.t_MarginFrequency     := rec.t_MarginPeriod; 
          rep_creditRisk.t_PartyMarginLimSum   := rec.t_PartyMarginLimSum;
          rep_creditRisk.t_PartyMarginLimCurr  := GetFI_Code( rec.t_PartyMinPayCurr );
          rep_creditRisk.t_PartyMarginLimSumRub:= case when rec.t_PartyMinPayCurr > -1 and rec.t_PartyMinPayCurr <> RSI_RSB_FIInstr.NATCUR 
                                                            then RSB_FIInstr.ConvSum( rec.t_PartyMarginLimSum, rec.t_PartyMinPayCurr, RSI_RSB_FIInstr.NATCUR, OnDate-1 ) 
                                                       else rec.t_PartyMarginLimSum end;
          rep_creditRisk.t_PartyMinPaySum      := rec.t_PartyMinPaySum;
          rep_creditRisk.t_PartyMinPayCurr     := GetFI_Code( rec.t_PartyMinPayCurr );
          rep_creditRisk.t_PartyMinPaySumRub   := case when rec.t_PartyMinPayCurr > -1 and rec.t_PartyMinPayCurr <> RSI_RSB_FIInstr.NATCUR 
                                                            then RSB_FIInstr.ConvSum( rec.t_PartyMinPaySum, rec.t_PartyMinPayCurr, RSI_RSB_FIInstr.NATCUR, OnDate-1 )
                                                       else rec.t_PartyMinPaySum end; 

          v_CSARecievedRest := GetAccRestOnDate( rec.t_CSAID, RSB_Secur.DV_CSA, OnDate-1, '-МС', 0, -1, v_toFIID, rep_creditRisk.t_ReceivedSecurAcc );
          v_CSATransferredRest := GetAccRestOnDate( rec.t_CSAID, RSB_Secur.DV_CSA, OnDate-1, '+МС', 0, -1, v_toFIID2, rep_creditRisk.t_TransferredSecurAcc );
          if( v_CSARecievedRest <> 0 ) then 
             rep_creditRisk.t_SecurityAcc      := rep_creditRisk.t_ReceivedSecurAcc;
             rep_creditRisk.t_SecuritySum      := v_CSARecievedRest;              
             rep_creditRisk.t_SecurityCurr     := GetFI_Code(v_toFIID); 
             rep_creditRisk.t_SecuritySumRub   := case when v_toFIID > -1 and v_toFIID <> RSI_RSB_FIInstr.NATCUR 
                                                            then RSB_FIInstr.ConvSum( rep_creditRisk.t_SecuritySum, v_toFIID, RSI_RSB_FIInstr.NATCUR, OnDate-1 )
                                                       else rep_creditRisk.t_SecuritySum end;
          else
             rep_creditRisk.t_SecurityAcc      := rep_creditRisk.t_TransferredSecurAcc;
             rep_creditRisk.t_SecuritySum      := v_CSATransferredRest;              
             rep_creditRisk.t_SecurityCurr     := GetFI_Code(v_toFIID2); 
             rep_creditRisk.t_SecuritySumRub   := case when v_toFIID2 > -1 and v_toFIID2 <> RSI_RSB_FIInstr.NATCUR 
                                                            then RSB_FIInstr.ConvSum( rep_creditRisk.t_SecuritySum, v_toFIID2, RSI_RSB_FIInstr.NATCUR, OnDate-1 )
                                                       else rep_creditRisk.t_SecuritySum end;
          end if;
          
          rep_creditRisk.t_SecuritySum2        := rec.t_NetCollateralSum;
          rep_creditRisk.t_SecurityCurr2       := rec.t_NetCollateralCurr; 
          v_toFIID := GetFIIDbyCode( rec.t_NetCollateralCurr );    
          rep_creditRisk.t_SecuritySumRub2     := case when v_toFIID > -1 and v_toFIID <> RSI_RSB_FIInstr.NATCUR 
                                                            then RSB_FIInstr.ConvSum( rep_creditRisk.t_SecuritySum2, v_toFIID, RSI_RSB_FIInstr.NATCUR, OnDate-1 )
                                                       else rep_creditRisk.t_SecuritySum2 end;                                                   
          
          rep_creditRisk.t_SecurityType        := 'Денежные средства';
          rep_creditRisk.t_FactNumDays         := rec.t_MarginFactNumDays;  
       else
          rep_creditRisk.t_Margining           := 'Нет';
          rep_creditRisk.t_MarginKind          := chr(1);
          rep_creditRisk.t_MarginFrequency     := 0;
          rep_creditRisk.t_PartyMarginLimSum   := 0;
          rep_creditRisk.t_PartyMarginLimCurr  := chr(1);
          rep_creditRisk.t_PartyMarginLimSumRub:= 0;
          rep_creditRisk.t_PartyMinPaySum      := 0;
          rep_creditRisk.t_PartyMinPayCurr     := chr(1);
          rep_creditRisk.t_PartyMinPaySumRub   := 0;
          rep_creditRisk.t_SecurityAcc         := chr(1);
          rep_creditRisk.t_SecuritySum         := 0;
          rep_creditRisk.t_SecurityCurr        := chr(1);
          rep_creditRisk.t_SecuritySumRub      := 0;
          rep_creditRisk.t_ReceivedSecurAcc    := chr(1);
          rep_creditRisk.t_TransferredSecurAcc := chr(1);
          rep_creditRisk.t_SecuritySum2        := 0;
          rep_creditRisk.t_SecurityCurr2       := chr(1);     
          rep_creditRisk.t_SecuritySumRub2     := 0;
          rep_creditRisk.t_SecurityType        := chr(1);
          rep_creditRisk.t_FactNumDays         := 0;         
      end if;
       
       rep_creditRisk.t_StandartPoorsRub     := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, STANDARTPOORS_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_StandartPoorsRub2    := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, STANDARTPOORS_3453Y_RATING_PARENTID, OnDate-1);  
       rep_creditRisk.t_MoodysRub            := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, MOODYS_RATING_PARENTID, OnDate-1);         
       rep_creditRisk.t_MoodysRub2           := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, MOODYS_3453Y_RATING_PARENTID, OnDate-1);         
       rep_creditRisk.t_FitchRatingsRub      := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, FITCHRATINGS_RATING_PARENTID, OnDate-1);    
       rep_creditRisk.t_FitchRatingsRub2     := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, FITCHRATINGS_3453Y_RATING_PARENTID, OnDate-1);     
       rep_creditRisk.t_StandartPoorsCurr    := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_FOREIGN, STANDARTPOORS_RATING_PARENTID, OnDate-1); 
       rep_creditRisk.t_StandartPoorsCurr2   := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_FOREIGN, STANDARTPOORS_3453Y_RATING_PARENTID, OnDate-1); 
       rep_creditRisk.t_MoodysCurr           := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_FOREIGN, MOODYS_RATING_PARENTID, OnDate-1);        
       rep_creditRisk.t_MoodysCurr2          := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_FOREIGN, MOODYS_3453Y_RATING_PARENTID, OnDate-1);        
       rep_creditRisk.t_FitchRatingsCurr     := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_FOREIGN, FITCHRATINGS_RATING_PARENTID, OnDate-1);   
       rep_creditRisk.t_FitchRatingsCurr2    := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_FOREIGN, FITCHRATINGS_3453Y_RATING_PARENTID, OnDate-1);  
       rep_creditRisk.t_StandartPoorsCountry := GetRatingRep(5, rec.t_CountryID, COUNTRY_RATING_NATIONAL, STANDARTPOORS_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_MoodysCountry        := GetRatingRep(5, rec.t_CountryID, COUNTRY_RATING_NATIONAL, MOODYS_RATING_PARENTID, OnDate-1);     
       rep_creditRisk.t_FitchRatingsCountry  := GetRatingRep(5, rec.t_CountryID, COUNTRY_RATING_NATIONAL, FITCHRATINGS_RATING_PARENTID, OnDate-1);   

       rep_creditRisk.t_AKRARating   := GetRatingRep(Rsb_Secur.OBJTYPE_PARTY, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, AKRA_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_ExpertRating := GetRatingRep(Rsb_Secur.OBJTYPE_PARTY, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, EXPRA_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_NRCRating    := GetRatingRep(Rsb_Secur.OBJTYPE_PARTY, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, NCR_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_NRARating    := GetRatingRep(Rsb_Secur.OBJTYPE_PARTY, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, NRA_RATING_PARENTID, OnDate-1);         
       
       g_pfiCreditRisk_ins.extend;
       g_pfiCreditRisk_ins(g_pfiCreditRisk_ins.LAST) := rep_creditRisk;      
     END LOOP;

     IF g_pfiCreditRisk_ins IS NOT EMPTY THEN
         FORALL i IN g_pfiCreditRisk_ins.FIRST .. g_pfiCreditRisk_ins.LAST
             INSERT INTO DDL_PFICREDITRISK_TMP
                 VALUES g_pfiCreditRisk_ins(i);
        g_pfiCreditRisk_ins.delete;
     END IF;

  END CreateData_DV_NDEAL;
  
  PROCEDURE CreateData_DV_POS( OnDate IN DATE )
  IS
     v_toFIID NUMBER := -1;
     v_toFIID2 NUMBER := -1;
     v_toAcc daccount_dbt.t_Account%TYPE := chr(1);
     v_Temp NUMBER := 0;
     v_PointIndex NUMBER := 0;
     v_PointCount NUMBER := 0;
     TYPE pfiCreditRisk_t IS TABLE OF DDL_PFICREDITRISK_TMP%ROWTYPE;
     g_pfiCreditRisk_ins pfiCreditRisk_t := pfiCreditRisk_t();
     rep_creditRisk DDL_PFICREDITRISK_TMP%ROWTYPE;   
  BEGIN
     FOR rec IN ( select pos.t_ID, pos.t_FIID, turn.t_FairValue, turn.t_Date t_FairValueDate,
                         case when turn.t_LongPosition - turn.t_ShortPosition > 0 then 1 else 0 end t_Buy,
                         NVL(Rsb_Secur.SC_GetObjCodeOnDate(Rsb_Secur.OBJTYPE_FININSTR, 11, pos.t_FIID, OnDate-1), fin.t_FI_Code) t_ExtCode,
                         NVL((select distinct listagg(to_char(t_Date,'dd.mm.yyyy'), ', ') within group (order by t_Date) over()  
                                from (select t.t_Date
                                        from ddvfiturn_dbt t                             
                                       where t.t_Department  = pos.t_Department  
                                         and t.t_FIID        = pos.t_FIID       
                                         and t.t_Broker      = pos.t_Broker      
                                         and t.t_ClientContr = pos.t_ClientContr 
                                         and t.t_GenAgrID    = pos.t_GenAgrID
                                         and (t.t_Buy <> 0 or t.t_Sale <> 0)               
                                         and t.t_Date < OnDate 
                                     )
                             ), to_date('01.01.0001','dd.mm.yyyy')) t_Date, 
                         case when fin.t_AvoirKind = 2 then 1 else 0 end t_IsOption,
                         case when pos.t_Broker > -1 then pos.t_Broker else fin.t_Issuer end t_Contractor,
                         fideriv.t_OptionType, fideriv.t_Strike, fideriv.t_StrikeFIID, fideriv.t_StrikePoint,
                         case when finBA.t_FI_Kind = 4 and finBA.t_AvoirKind = 1 then 1 else 0 end t_OnFutures,
                         case when finBA.t_FI_Kind = 4 and finBA.t_AvoirKind = 1 then finBA.t_FI_Code else chr(1) end t_Futures_Code,
                         NVL((select t_SzNameAlg 
                                from dnamealg_dbt 
                               where t_ITypeALG = 3421 
                                 and t_INumberAlg = Rsb_Secur.Get_BA_Kind(finBA.t_FI_Kind, finBA.t_AvoirKind, RSB_FIInstr.FI_AvrKindsGetRoot(finBA.t_FI_Kind, finBA.t_AvoirKind))
                             ),chr(1)) t_BAKind_Name,
                         case when finBA.t_FI_Kind = 4 and finBA.t_AvoirKind = 1 then finBA_Futures.t_FI_Kind else finBA.t_FI_Kind end t_FI_Kind,
                         case when finBA.t_FI_Kind = 4 and finBA.t_AvoirKind = 1 then finBA_Futures.t_AvoirKind else finBA.t_AvoirKind end t_AvoirKind,
                         case when finBA.t_FI_Kind = 4 and finBA.t_AvoirKind = 1 then finBA_Futures.t_FI_Code else finBA.t_FI_Code end t_FI_Code,
                         case when finBA.t_FI_Kind = 4 and finBA.t_AvoirKind = 1 then finBA_Futures.t_Settlement_Code else finBA.t_Settlement_Code end t_Settlement_Code,
                         case when finBA.t_FI_Kind = 4 and finBA.t_AvoirKind = 1 then finBA_Futures.t_FIID else finBA.t_FIID end t_BAFIID,
                         case when finBA.t_FI_Kind = 4 and finBA.t_AvoirKind = 1 then finBA_Futures.t_Name else finBA.t_Name end t_BAName,
                         case when finBA.t_FI_Kind = 4 and finBA.t_AvoirKind = 1 then finBA.t_DrawingDate else fin.t_DrawingDate end t_DrawingDate,
                         NVL(fideriv_Futures.t_InCirculationDate, to_date('01.01.0001','DD.MM.YYYY')) t_Futures_InCirculationDate, 
                         NVL(fideriv_Futures.t_LastCirculationDate, to_date('01.01.0001','DD.MM.YYYY')) t_Futures_LastCirculationDate,
                         NVL((select NVL((select t_Code from dllvalues_dbt where t_List = 4016 and t_Element = genagr.t_AuthorForm),'') || 
                                     ' Договор № ' || genagr.t_Code || ' от ' || to_char(genagr.t_Date_GenAgr,'DD.MM.YYYY')
                                from ddl_genagr_dbt genagr
                               where genagr.t_GenAgrID = pos.t_GenAgrID),chr(1)) t_GenAgrInfo,  
                         NVL((select case when genagr.t_Can_LiquidNetting = 'X' then 'Да' else 'Нет' end
                                from ddl_genagr_dbt genagr
                               where genagr.t_GenAgrID = pos.t_GenAgrID),chr(1)) t_GenAgrLiquidNetting,  
                         NVL((select party.t_ShortName 
                                from dparty_dbt party 
                               where party.t_PartyID = (case when pos.t_Broker > -1 then pos.t_Broker else fin.t_Issuer end)), chr(1)) t_ContractorName, 
                         NVL((select 'Банк' from dpartyown_dbt where t_PartyKind = 2 and t_PartyID = (case when pos.t_Broker > -1 then pos.t_Broker else fin.t_Issuer end)), 
                             (select decode(t_LegalForm, 1, 'ЮЛ', 'ФЛ') from dparty_dbt where t_PartyID = (case when pos.t_Broker > -1 then pos.t_Broker else fin.t_Issuer end))) t_ContractorKind,
                         NVL((select party.t_NRCountry 
                                from dparty_dbt party 
                               where party.t_PartyID = (case when pos.t_Broker > -1 then pos.t_Broker else fin.t_Issuer end)), chr(1)) t_ContractorCountry,
                         NVL((select country.t_CountryID 
                                from dparty_dbt party, dcountry_dbt country 
                               where party.t_PartyID = (case when pos.t_Broker > -1 then pos.t_Broker else fin.t_Issuer end)
                                 and country.t_CodeLat3 = party.t_NRCountry
                                 and rownum = 1), -1) t_CountryID,
                         NVL((select party.t_ShortName 
                                from dparty_dbt party 
                               where party.t_PartyID = (case when finBA.t_FI_Kind = 4 and finBA.t_AvoirKind = 1 then finBA_Futures.t_Issuer else finBA.t_Issuer end)), chr(1)) t_IssuerShortName,
                         NVL((select attr.t_Name 
                                from dobjatcor_dbt atCor, dobjattr_dbt attr
                               where atCor.t_ObjectType = 22 --Артикул
                                 and atCor.t_GroupID    = DL_CATEGORY_ARTICLEGROUP
                                 and atCor.t_Object     = LPAD((case when finBA.t_FI_Kind = 4 and finBA.t_AvoirKind = 1 then finBA_Futures.t_FIID else finBA.t_FIID end), 10, '0')
                                 and attr.t_AttrID      = atCor.t_AttrID
                                 and attr.t_ObjectType  = atCor.t_ObjectType
                                 and attr.t_GroupID     = atCor.t_GroupID),chr(1)) t_ArticleGroupValue,
                         case when finBA.t_FI_Kind = 2 then 
                                  NVL((select decode(NVL(t_ISIN,chr(1)), chr(1), t_LSIN, t_ISIN) 
                                         from davoiriss_dbt 
                                        where t_FIID = finBA.t_FIID),chr(1))
                              else chr(1) end t_AvoirISIN,
                         case when fin.t_AvoirKind = 2 then NVL((select t_SzNameAlg from dnamealg_dbt where t_ITypeALG = 3424 and t_INumberAlg = fideriv.t_OptionStyle),chr(1)) else chr(1) end t_OptionStyleName                                                                          
                    from ddvfipos_dbt pos, ddvfiturn_dbt turn, dfininstr_dbt fin, dfideriv_dbt fideriv, dfininstr_dbt finBA, dfideriv_dbt fideriv_Futures, dfininstr_dbt finBA_Futures                                               
                   where (pos.t_CloseDate is null or pos.t_CloseDate = to_date('01.01.0001','DD.MM.YYYY') or pos.t_CloseDate >= OnDate )
                     and pos.t_Client = -1  
                     and turn.t_Department  = pos.t_Department  
                     and turn.t_FIID        = pos.t_FIID       
                     and turn.t_Broker      = pos.t_Broker      
                     and turn.t_ClientContr = pos.t_ClientContr 
                     and turn.t_GenAgrID    = pos.t_GenAgrID
                     and turn.t_Date = (select max(t.t_Date)                              
                                          from ddvfiturn_dbt t                             
                                         where t.t_Department  = pos.t_Department  
                                           and t.t_FIID        = pos.t_FIID       
                                           and t.t_Broker      = pos.t_Broker      
                                           and t.t_ClientContr = pos.t_ClientContr 
                                           and t.t_GenAgrID    = pos.t_GenAgrID               
                                           and t.t_Date < OnDate                              
                                       )   
                     and turn.t_LongPosition - turn.t_ShortPosition != 0                                                                                                                        
                     and fin.t_FIID = pos.t_FIID 
                     and fideriv.T_FIID = pos.T_FIID
                     and finBA.t_FIID = fin.t_FaceValueFI
                     and fideriv_Futures.T_FIID(+) = finBA.t_FIID
                     and finBA_Futures.t_FIID(+) = finBA.t_FaceValueFI
                )
     LOOP      
       rep_creditRisk.t_DealID   := rec.t_ID;
       rep_creditRisk.t_DocKind  := 193; --Позиция по ПИ
       rep_creditRisk.t_DealCode := rec.t_ExtCode;
       rep_creditRisk.t_Part     := 0;    
     
       rep_creditRisk.t_IsNetting       := 'Нет';
       rep_creditRisk.t_PFICategory     := chr(1); 
       rep_creditRisk.t_PFISubCategory  := chr(1);
       rep_creditRisk.t_GoodsType       := chr(1);
       rep_creditRisk.t_DocKindName     := chr(1);
       rep_creditRisk.t_BAKind          := chr(1);
       rep_creditRisk.t_SecurFlag       := chr(1);
       rep_creditRisk.t_SecurIssuer     := chr(1);
       rep_creditRisk.t_NumNominalExch  := 0;
       rep_creditRisk.t_PayRateType     := chr(1);
       rep_creditRisk.t_ReceiveRateType := chr(1);
       rep_creditRisk.t_OptionType      := chr(1);
       rep_creditRisk.t_OptionPrice     := 0;
       rep_creditRisk.t_OptionPricePoint:= 0;
       rep_creditRisk.t_OptionCost      := 0;
       rep_creditRisk.t_OptionCostPoint := 0;
       rep_creditRisk.t_OptionStyle     := chr(1);   
       rep_creditRisk.t_StartDateBA     := to_date('01.01.0001','DD.MM.YYYY');
       rep_creditRisk.t_EndDateBA       := to_date('01.01.0001','DD.MM.YYYY');
       rep_creditRisk.t_PayRateType     := chr(1);
       rep_creditRisk.t_PayFixRate      := 0;
       rep_creditRisk.t_PayFixRatePoint := 0;
       rep_creditRisk.t_ReceiveRateType := chr(1);
       rep_creditRisk.t_ReceiveFixRate  := 0;
       rep_creditRisk.t_ReceiveFixRatePoint := 0;
       rep_creditRisk.t_ReqPmgrSum      := 0;
       rep_creditRisk.t_ReqPmgrCurr     := -1;
       rep_creditRisk.t_ReqPmgrSumRub   := 0;                                      
       rep_creditRisk.t_OblPmgrSum      := 0;  
       rep_creditRisk.t_OblPmgrCurr     := -1;                                     
       rep_creditRisk.t_OblPmgrSumRub   := 0;                                       
       rep_creditRisk.t_DateOfInterestRate := to_date('01.01.0001','DD.MM.YYYY');
       rep_creditRisk.t_FloatRateAcc    := chr(1);
       
       rep_creditRisk.t_ContractorName  := rec.t_ContractorName;
       rep_creditRisk.t_ContractorKind  := rec.t_ContractorKind;
       rep_creditRisk.t_Country         := rec.t_ContractorCountry;
       rep_creditRisk.t_GenAgrInfo      := rec.t_GenAgrInfo;
       rep_creditRisk.t_IsLiquidNetting := rec.t_GenAgrLiquidNetting;
       rep_creditRisk.t_PeriodCls       := 'ПФИ';
       
       rep_creditRisk.t_StartDate       := rec.t_Date;
       rep_creditRisk.t_EndDate         := rec.t_DrawingDate;
      
       rep_creditRisk.t_PFICategory := case when (   (rec.t_FI_Kind = 2 and (   (RSB_FIInstr.FI_AvrKindsGetRoot(rec.t_FI_Kind, rec.t_AvoirKind) = RSI_RSB_FIInstr.AVOIRKIND_BOND) 
                                                                             or (RSB_FIInstr.FI_AvrKindsGetRoot(rec.t_FI_Kind, rec.t_AvoirKind) = RSI_RSB_FIInstr.AVOIRISSKIND_BASKET))) 
                                                  or (rec.t_FI_Kind = 3 and rec.t_Settlement_Code = 9)) 
                                                 then 'Процентные' 
                                            when ((rec.t_FI_Kind = 3 and rec.t_Settlement_Code = 1) or rec.t_FI_Kind = 1) 
                                                 then 'Валютные' 
                                            when (   (rec.t_FI_Kind = 2 and RSB_FIInstr.FI_AvrKindsGetRoot(rec.t_FI_Kind, rec.t_AvoirKind) = RSI_RSB_FIInstr.AVOIRKIND_SHARE) 
                                                  or (rec.t_FI_Kind = 3 and rec.t_Settlement_Code = 2)) 
                                                 then 'Фондовые'
                                            when (rec.t_FI_Kind = 6 or rec.t_FI_Kind = 7)
                                                 then 'Товарные'
                                       end;
       
       if( rep_creditRisk.t_PFICategory = 'Товарные' ) then
          rep_creditRisk.t_PFISubCategory := case when rec.t_FI_Kind = 6
                                                     then 'Металл' 
                                                  when rec.t_FI_Kind = 7  
                                                     then rec.t_ArticleGroupValue
                                             end;
       elsif( rep_creditRisk.t_PFICategory = 'Фондовые' ) then
          rep_creditRisk.t_PFISubCategory := case when rec.t_FI_Kind = 2 and RSB_FIInstr.FI_AvrKindsGetRoot(rec.t_FI_Kind, rec.t_AvoirKind) = RSI_RSB_FIInstr.AVOIRKIND_SHARE 
                                                     then 'Фондовые по эмитенту' 
                                                  when rec.t_FI_Kind = 3 
                                                     then 'Фондовые по индексу ценных бумаг' 
                                             end;
       end if; 
       
       rep_creditRisk.t_DocKindName := case when rec.t_IsOption = 1 then (case when rec.t_Buy = 1 then 'Положительный' else 'Отрицательный' end) || ' исходящий остаток в позиции по опциону ' || 
                                                                         (case when rec.t_OptionType = 1 then 'Put' else 'Call' end)
                                            else 'Позиция по фьючерсу' end;
                                     
       rep_creditRisk.t_BAKind := rec.t_BAKind_Name; 
       rep_creditRisk.t_BAKind := case when rec.t_OnFutures = 1
                                          then rec.t_Futures_Code
                                       when rec.t_FI_Kind = 1 
                                          then 'Ин. валюта' 
                                       when rec.t_FI_Kind = 6
                                          then 'Драг. металл' 
                                       when rec.t_FI_Kind = 2
                                          then rec.t_AvoirISIN
                                       else rec.t_BAName 
                                  end;
       
       if( rep_creditRisk.t_PFICategory = 'Товарные' ) then 
          rep_creditRisk.t_GoodsType := rec.t_BAName;
       end if; 
       
       rep_creditRisk.t_PFIVolType := chr(1);
       
       rep_creditRisk.t_SecurFlag := case when rep_creditRisk.t_PFICategory = 'Процентные' and rec.t_FI_Kind <> 3
                                               then 'Долговые' 
                                          when rep_creditRisk.t_PFICategory = 'Фондовые' and rec.t_FI_Kind <> 3
                                               then 'Долевые'
                                          when (rep_creditRisk.t_PFICategory = 'Процентные' or rep_creditRisk.t_PFICategory = 'Фондовые') and rec.t_FI_Kind = 3
                                               then rec.t_BAName
                                     end;
       
       if( rep_creditRisk.t_SecurFlag = 'Долговые' or rep_creditRisk.t_SecurFlag = 'Долевые' ) then 
          rep_creditRisk.t_SecurIssuer := rec.t_IssuerShortName;   
       end if;   
       
       rep_creditRisk.t_ExecType   := case when rec.t_Settlement_Code = RSB_Derivatives.DVSETTLEMET_STATE then 'Поставочная' else 'Беспоставочная' end;
       rep_creditRisk.t_IsExchange := 'Биржевая';
       
       rep_creditRisk.t_BuySale    := case when rec.t_IsOption = 1 
                                                then (case when (rec.t_Buy = 1 and rec.t_OptionType = 1) or (rec.t_Buy = 0 and rec.t_OptionType = 2) then 'Продажа' else 'Покупка' end)
                                           else (case when rec.t_Buy = 1 then 'Покупка' else 'Продажа' end)
                                      end; 
       
       rep_creditRisk.t_BankPosition := case when rec.t_Buy = 1 then 'Покупатель' else 'Продавец' end;
/*       
       rep_creditRisk.t_ReqSum := GetAccRestOnDate( rec.t_ID, rep_creditRisk.t_DocKind, OnDate-1, DVPOS_PLUS_CAT_NOMINAL, 
                                                    0, -1, rep_creditRisk.t_ReqCurr, rep_creditRisk.t_ReqAcc );
*/                                                    
/*KD*/                                                    
       rep_creditRisk.t_ReqSum := GetAccRestOnDatePos( rec.t_fiID, OnDate-1, DVPOS_PLUS_CAT_NOMINAL, 
                                                    1, -1, rep_creditRisk.t_ReqCurr, rep_creditRisk.t_ReqAcc );

                                  
       if( rep_creditRisk.t_ReqAcc <> chr(1) and rep_creditRisk.t_ReqCurr <> RSI_RSB_FIInstr.NATCUR ) then
          rep_creditRisk.t_ReqSumRub := RSB_FIInstr.ConvSum( rep_creditRisk.t_ReqSum, rep_creditRisk.t_ReqCurr, RSI_RSB_FIInstr.NATCUR, OnDate-1 ); 
       else  
          rep_creditRisk.t_ReqSumRub := rep_creditRisk.t_ReqSum;
       end if;
/*       
       rep_creditRisk.t_OblSum := GetAccRestOnDate( rec.t_ID, rep_creditRisk.t_DocKind, OnDate-1, DVPOS_MINUS_CAT_NOMINAL, 
                                                    0, -1, rep_creditRisk.t_OblCurr, rep_creditRisk.t_OblAcc );
*/      
/*KD*/                                              
       rep_creditRisk.t_OblSum := GetAccRestOnDatePos( rec.t_FIID, OnDate-1, DVPOS_MINUS_CAT_NOMINAL, 
                                                    1, -1, rep_creditRisk.t_OblCurr, rep_creditRisk.t_OblAcc );
                                                    
                                                    
                                                    
       if( rep_creditRisk.t_OblAcc <> chr(1) and rep_creditRisk.t_OblCurr <> RSI_RSB_FIInstr.NATCUR ) then
          rep_creditRisk.t_OblSumRub := RSB_FIInstr.ConvSum( rep_creditRisk.t_OblSum, rep_creditRisk.t_OblCurr, RSI_RSB_FIInstr.NATCUR, OnDate-1 ); 
       else 
          rep_creditRisk.t_OblSumRub := rep_creditRisk.t_OblSum;
       end if;
       
       if( rep_creditRisk.t_BuySale = 'Покупка' ) then
          rep_creditRisk.t_PFINominal := rep_creditRisk.t_ReqSum;
          rep_creditRisk.t_NominalCurrency := GetFI_Code( rep_creditRisk.t_ReqCurr ); 
       else
          rep_creditRisk.t_PFINominal := rep_creditRisk.t_OblSum;
          rep_creditRisk.t_NominalCurrency := GetFI_Code( rep_creditRisk.t_OblCurr ); 
       end if;
 /*      
       rep_creditRisk.t_FairValueActive := GetAccRestOnDate( rec.t_ID, rep_creditRisk.t_DocKind, OnDate-1, '+ПФИ, +ПФИ ФО', 1, -1, v_toFIID, rep_creditRisk.t_FairValueAcc );
       if( rep_creditRisk.t_FairValueAcc <> chr(1) ) then
          rep_creditRisk.t_FairValuePassive := 0; --Остаток должен быть только на одном счёте
       else
          rep_creditRisk.t_FairValuePassive := GetAccRestOnDate( rec.t_ID, rep_creditRisk.t_DocKind, OnDate-1, '-ПФИ, -ПФИ ФО', 1, -1, v_toFIID, rep_creditRisk.t_FairValueAcc );   
       end if;
*/      
/*KD*/ 
/*     
       rep_creditRisk.t_FairValueActive := GetAccRestOnDatePos( rec.t_FIID, OnDate-1, '+ПФИ, +ПФИ ФО, +ПФИ1', 1, -1, v_toFIID, rep_creditRisk.t_FairValueAcc );
       if( rep_creditRisk.t_FairValueAcc <> chr(1) ) then
          rep_creditRisk.t_FairValuePassive := 0; --Остаток должен быть только на одном счёте
       else
          rep_creditRisk.t_FairValuePassive := GetAccRestOnDatePos( rec.t_FIID,  OnDate-1, '-ПФИ, -ПФИ ФО, -ПФИ1', 1, -1, v_toFIID, rep_creditRisk.t_FairValueAcc );   
       end if;
*/
       
       if( rec.t_FairValue > 0 ) then 
          v_Temp := GetAccRestOnDatePos( rec.t_FIID, OnDate-1/*rec.t_FairValueDate*/, '+ПФИ ФО, +ПФИ1', 0/*1*/, -1, v_toFIID, rep_creditRisk.t_FairValueAcc );
          rep_creditRisk.t_FairValueActive  := rec.t_FairValue;
          rep_creditRisk.t_FairValuePassive := 0 ;
       elsif( rec.t_FairValue < 0 ) then 
          v_Temp := GetAccRestOnDatePos( rec.t_FIID, OnDate-1/*rec.t_FairValueDate*/, '-ПФИ ФО, -ПФИ1', 0/*1*/, -1, v_toFIID, rep_creditRisk.t_FairValueAcc );  
          rep_creditRisk.t_FairValueActive  := 0 ;
          rep_creditRisk.t_FairValuePassive := rec.t_FairValue; 
       else   
          rep_creditRisk.t_FairValueAcc     := chr(1);  
          rep_creditRisk.t_FairValueActive  := 0 ;
          rep_creditRisk.t_FairValuePassive := 0; 
       end if;
                                           
       if( rec.t_IsOption = 1 ) then
          rep_creditRisk.t_OptionType       := case when rec.t_OptionType = 1 then 'Put' else 'Call' end;  
          rep_creditRisk.t_OptionPrice      := case when (rec.t_FI_Kind = 1 and rec.t_OnFutures <> 1) then RSB_FIInstr.ConvSum(1, rec.t_BAFIID, RSI_RSB_FIInstr.NATCUR, OnDate-1) else -1 end;
          v_PointIndex := InStr(to_char(rep_creditRisk.t_OptionPrice),'.');
          v_PointCount := case when v_PointIndex > 0 then Length(SubStr(to_char(rep_creditRisk.t_OptionPrice), v_PointIndex + 1)) else 0 end; --Считаем кол-во десятичных знаков;
          rep_creditRisk.t_OptionPricePoint := case when v_PointCount > 2 then v_PointCount else 2 end; 
          rep_creditRisk.t_OptionCost       := rec.t_Strike;
          rep_creditRisk.t_OptionCostPoint  := rec.t_StrikePoint;
          rep_creditRisk.t_OptionStyle      := rec.t_OptionStyleName;   
       end if; 
       
       if( rec.t_OnFutures = 1 ) then
          if( rep_creditRisk.t_PFICategory = 'Процентные' or rep_creditRisk.t_PFICategory = 'Валютные' or rep_creditRisk.t_PFICategory = 'Фондовые' ) then
             rep_creditRisk.t_StartDateBA := rec.t_Futures_InCirculationDate;
          end if;
          rep_creditRisk.t_EndDateBA   := rec.t_Futures_LastCirculationDate;
       end if;
       
       rep_creditRisk.t_Margining           := 'Нет';
       rep_creditRisk.t_MarginKind          := chr(1);
       rep_creditRisk.t_MarginFrequency     := 0;
       rep_creditRisk.t_PartyMarginLimSum   := 0;
       rep_creditRisk.t_PartyMarginLimCurr  := chr(1);
       rep_creditRisk.t_PartyMarginLimSumRub:= 0;
       rep_creditRisk.t_PartyMinPaySum      := 0;
       rep_creditRisk.t_PartyMinPayCurr     := chr(1);
       rep_creditRisk.t_PartyMinPaySumRub   := 0;
       rep_creditRisk.t_SecurityAcc         := chr(1);
       rep_creditRisk.t_SecuritySum         := 0;
       rep_creditRisk.t_SecurityCurr        := chr(1);
       rep_creditRisk.t_SecuritySumRub      := 0;
       rep_creditRisk.t_ReceivedSecurAcc    := chr(1);
       rep_creditRisk.t_TransferredSecurAcc := chr(1);
       rep_creditRisk.t_SecuritySum2        := 0;
       rep_creditRisk.t_SecurityCurr2       := chr(1);     
       rep_creditRisk.t_SecuritySumRub2     := 0;
       rep_creditRisk.t_SecurityType        := chr(1);
       rep_creditRisk.t_FactNumDays         := 0;  
       
       rep_creditRisk.t_StandartPoorsRub     := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, STANDARTPOORS_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_StandartPoorsRub2    := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, STANDARTPOORS_3453Y_RATING_PARENTID, OnDate-1);  
       rep_creditRisk.t_MoodysRub            := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, MOODYS_RATING_PARENTID, OnDate-1);         
       rep_creditRisk.t_MoodysRub2           := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, MOODYS_3453Y_RATING_PARENTID, OnDate-1);         
       rep_creditRisk.t_FitchRatingsRub      := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, FITCHRATINGS_RATING_PARENTID, OnDate-1);    
       rep_creditRisk.t_FitchRatingsRub2     := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, FITCHRATINGS_3453Y_RATING_PARENTID, OnDate-1);     
       rep_creditRisk.t_StandartPoorsCurr    := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_FOREIGN, STANDARTPOORS_RATING_PARENTID, OnDate-1); 
       rep_creditRisk.t_StandartPoorsCurr2   := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_FOREIGN, STANDARTPOORS_3453Y_RATING_PARENTID, OnDate-1); 
       rep_creditRisk.t_MoodysCurr           := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_FOREIGN, MOODYS_RATING_PARENTID, OnDate-1);        
       rep_creditRisk.t_MoodysCurr2          := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_FOREIGN, MOODYS_3453Y_RATING_PARENTID, OnDate-1);        
       rep_creditRisk.t_FitchRatingsCurr     := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_FOREIGN, FITCHRATINGS_RATING_PARENTID, OnDate-1);   
       rep_creditRisk.t_FitchRatingsCurr2    := GetRatingRep(3, rec.t_Contractor, CONTRACTOR_RATING_FOREIGN, FITCHRATINGS_3453Y_RATING_PARENTID, OnDate-1);  
       rep_creditRisk.t_StandartPoorsCountry := GetRatingRep(5, rec.t_CountryID, COUNTRY_RATING_NATIONAL, STANDARTPOORS_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_MoodysCountry        := GetRatingRep(5, rec.t_CountryID, COUNTRY_RATING_NATIONAL, MOODYS_RATING_PARENTID, OnDate-1);     
       rep_creditRisk.t_FitchRatingsCountry  := GetRatingRep(5, rec.t_CountryID, COUNTRY_RATING_NATIONAL, FITCHRATINGS_RATING_PARENTID, OnDate-1);                            
       rep_creditRisk.t_AKRARating           := GetRatingRep(Rsb_Secur.OBJTYPE_PARTY, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, AKRA_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_ExpertRating         := GetRatingRep(Rsb_Secur.OBJTYPE_PARTY, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, EXPRA_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_NRCRating            := GetRatingRep(Rsb_Secur.OBJTYPE_PARTY, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, NCR_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_NRARating            := GetRatingRep(Rsb_Secur.OBJTYPE_PARTY, rec.t_Contractor, CONTRACTOR_RATING_NATIONAL, NRA_RATING_PARENTID, OnDate-1);                                                             
       
       g_pfiCreditRisk_ins.extend;
       g_pfiCreditRisk_ins(g_pfiCreditRisk_ins.LAST) := rep_creditRisk;              
     END LOOP;

     IF g_pfiCreditRisk_ins IS NOT EMPTY THEN
         FORALL i IN g_pfiCreditRisk_ins.FIRST .. g_pfiCreditRisk_ins.LAST
             INSERT INTO DDL_PFICREDITRISK_TMP
                 VALUES g_pfiCreditRisk_ins(i);
        g_pfiCreditRisk_ins.delete;
     END IF;

  END CreateData_DV_POS;

  PROCEDURE CreateData_SC( OnDate IN DATE )
  IS
     v_toFIID NUMBER := -1;
     v_toFIID2 NUMBER := -1;
     v_toAcc daccount_dbt.t_Account%TYPE := chr(1);
     v_UsingCSAID ddvcsa_dbt.t_CSAID%TYPE := 0;
     v_CSAPartyMinPaySum ddvcsa_dbt.t_PartyMinPaySum%TYPE;
     v_CSAPartyMarginLimSum ddvcsa_dbt.t_PartyMarginLimSum%TYPE;
     v_CSAPartyCurr ddvcsa_dbt.t_PartyMinPayCurr%TYPE;
     v_CSAMarginKind dobjattr_dbt.t_Name%TYPE;
     v_CSAMarginPayer dobjattr_dbt.t_Name%TYPE;
     v_CSAMarginPeriod ddl_pficreditrisk_tmp.t_MarginFrequency%TYPE;
     v_CSAMarginFactNumDays ddl_pficreditrisk_tmp.t_FactNumDays%TYPE;
     v_CSARecievedRest NUMBER := 0;
     v_CSATransferredRest NUMBER := 0;
     TYPE pfiCreditRisk_t IS TABLE OF DDL_PFICREDITRISK_TMP%ROWTYPE;
     g_pfiCreditRisk_ins pfiCreditRisk_t := pfiCreditRisk_t();
     rep_creditRisk DDL_PFICREDITRISK_TMP%ROWTYPE;      
  BEGIN
     FOR rec IN (with tick as( select tick.t_DealID, tick.t_BofficeKind, tick.t_DealCode, tick.t_DealCodeTS, tick.t_PartyID, tick.t_MarketID, tick.t_BrokerID, tick.t_PFI, tick.t_GenAgrID, 
                                      tick.t_IsNetting, tick.t_DealDate,
                                      RSB_Secur.IsBuy(RSB_Secur.get_OperationGroup(RSB_Secur.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) t_IsBuy,
                                      RSB_Secur.IsBroker(RSB_Secur.get_OperationGroup(RSB_Secur.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) t_IsBroker,
                                      RSB_Secur.IsExchange(RSB_Secur.get_OperationGroup(RSB_Secur.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) t_IsExchange,
                                      NVL((select min(rq.t_FactDate)
                                             from ddlrq_dbt rq
                                            where rq.t_DocKind = tick.t_BofficeKind
                                              and rq.t_DocID = tick.t_DealID
                                              and rq.t_Type in (Rsi_dlrq.DLRQ_TYPE_PAYMENT, Rsi_dlrq.DLRQ_TYPE_DELIVERY)
                                              and rq.t_FactDate <> to_date('01.01.0001','DD.MM.YYYY')
                                           ), to_date('01.01.0001','DD.MM.YYYY')) t_MinFactDate
                                 from ddl_tick_dbt tick
                                where tick.t_DealDate < OnDate
                                  and tick.t_DealStatus > 0
                                  and tick.t_IsPFI = CHR(88)
                                  and tick.t_ClientID = -1
                                  and tick.t_BofficeKind = RSB_Secur.OBJTYPE_SECDEAL
                              )
                  select tick.t_DealID, tick.t_BofficeKind, tick.t_DealCode, tick.t_DealCodeTS, tick.t_IsBuy, tick.t_IsExchange, tick.t_IsNetting, to_char(tick.t_DealDate,'dd.mm.yyyy') t_Date, 
                         fin.t_FIID, fin.t_FI_Kind, fin.t_AvoirKind, fin.t_DrawingDate, GetContractorID(tick.t_IsExchange, tick.t_PartyID, tick.t_MarketID, tick.t_BrokerID) t_ContractorID,
                         NVL((select t_SzNameAlg 
                                from dnamealg_dbt 
                               where t_ITypeALG = 3421 
                                 and t_INumberAlg = Rsb_Secur.Get_BA_Kind(fin.t_FI_Kind, fin.t_AvoirKind, RSB_FIInstr.FI_AvrKindsGetRoot(fin.t_FI_Kind, fin.t_AvoirKind)) 
                             ),chr(1)) t_BAKind_Name,
                         NVL((select party.t_ShortName 
                                from dparty_dbt party 
                               where party.t_PartyID = GetContractorID(tick.t_IsExchange, tick.t_PartyID, tick.t_MarketID, tick.t_BrokerID)),chr(1)) t_ContractorName,
                         NVL((select 'Банк' from dpartyown_dbt where t_PartyKind = 2 and t_PartyID = GetContractorID(tick.t_IsExchange, tick.t_PartyID, tick.t_MarketID, tick.t_BrokerID)), 
                             (select decode(t_LegalForm, 1, 'ЮЛ', 'ФЛ') from dparty_dbt where t_PartyID = GetContractorID(tick.t_IsExchange, tick.t_PartyID, tick.t_MarketID, tick.t_BrokerID))) t_ContractorKind,
                         NVL((select party.t_NRCountry 
                                from dparty_dbt party 
                               where party.t_PartyID = GetContractorID(tick.t_IsExchange, tick.t_PartyID, tick.t_MarketID, tick.t_BrokerID)), chr(1)) t_ContractorCountry,
                         NVL((select country.t_CountryID 
                                from dparty_dbt party, dcountry_dbt country 
                               where party.t_PartyID = GetContractorID(tick.t_IsExchange, tick.t_PartyID, tick.t_MarketID, tick.t_BrokerID)
                                 and country.t_CodeLat3 = party.t_NRCountry
                                 and rownum = 1), -1) t_CountryID,
                         NVL((select NVL((select t_Code from dllvalues_dbt where t_List = 4016 /*Авторы формы ГС*/ and t_Element = genagr.t_AuthorForm),'') || 
                                     ' Договор № ' || genagr.t_Code || ' от ' || to_char(genagr.t_Date_GenAgr,'DD.MM.YYYY')
                                from ddl_genagr_dbt genagr
                               where genagr.t_GenAgrID = tick.t_GenAgrID),chr(1)) t_GenAgrInfo,  
                         NVL((select case when genagr.t_Can_LiquidNetting = 'X' then 'Да' else 'Нет' end
                                from ddl_genagr_dbt genagr
                               where genagr.t_GenAgrID = tick.t_GenAgrID),chr(1)) t_GenAgrLiquidNetting,
                         NVL((select party.t_ShortName 
                                from dparty_dbt party 
                               where party.t_PartyID = fin.t_Issuer), chr(1)) t_IssuerShortName,
                         NVL((select decode(NVL(t_ISIN,chr(1)), chr(1), t_LSIN, t_ISIN) 
                                from davoiriss_dbt 
                               where t_FIID = fin.t_FIID),chr(1)) t_AvoirISIN,
                         (select greatest (max(leg.t_Maturity), max(leg.t_Expiry)) from ddl_leg_dbt leg where leg.t_DealID = tick.t_DealID) t_EndDate,
                         NVL((select RSB_Struct.getString(note.t_text)
                                from dnotetext_dbt note
                               where note.t_ObjectType = 101 --Сделка с ценными бумагами
                                 and note.t_DocumentID = LPAD(tick.t_DealID, 34, '0')
                                 and note.t_NoteKind = SC_NOTE_CSACODE
                                 and OnDate-1 between note.t_Date and note.t_ValidToDate),chr(1)) t_CSACode,
                         NVL((select RSB_Struct.getMoney(note.t_text)
                                from dnotetext_dbt note
                               where note.t_ObjectType = 101 --Сделка с ценными бумагами
                                 and note.t_DocumentID = LPAD(tick.t_DealID, 34, '0')
                                 and note.t_NoteKind = SC_NOTE_NETCOLLATERALSUM
                                 and OnDate-1 between note.t_Date and note.t_ValidToDate),0) t_NetCollateralSum,
                         NVL((select cast(RSB_Struct.getString(note.t_text) as varchar2(15))
                                from dnotetext_dbt note
                               where note.t_ObjectType = 101 --Сделка с ценными бумагами
                                 and note.t_DocumentID = LPAD(tick.t_DealID, 34, '0')
                                 and note.t_NoteKind = SC_NOTE_NETCOLLATERALCURR
                                 and OnDate-1 between note.t_Date and note.t_ValidToDate),chr(1)) t_NetCollateralCurr
                    from tick, dfininstr_dbt fin
                   where fin.t_FIID = tick.t_PFI
                     and (tick.t_MinFactDate = to_date('01.01.0001','DD.MM.YYYY') or tick.t_MinFactDate >= OnDate)                    
                )                    
     LOOP
       rep_creditRisk.t_DealID          := rec.t_DealID;
       rep_creditRisk.t_DocKind         := rec.t_BofficeKind;
       rep_creditRisk.t_DealCode        := case when rec.t_DealCode <> chr(1) then rec.t_DealCode else rec.t_DealCodeTS end;
       rep_creditRisk.t_Part            := 0;  
       
       rep_creditRisk.t_ContractorName  := rec.t_ContractorName;
       rep_creditRisk.t_ContractorKind  := rec.t_ContractorKind;
       rep_creditRisk.t_Country         := rec.t_ContractorCountry;
       rep_creditRisk.t_GenAgrInfo      := rec.t_GenAgrInfo;
       rep_creditRisk.t_IsLiquidNetting := rec.t_GenAgrLiquidNetting;
       rep_creditRisk.t_IsNetting       := case when rec.t_IsNetting = 'X' then 'Да' else 'Нет' end;
       rep_creditRisk.t_PeriodCls       := 'срочная';
       
       rep_creditRisk.t_StartDate       := rec.t_Date;
       rep_creditRisk.t_EndDate         := rec.t_EndDate; 
       rep_creditRisk.t_StartDateBA     := rec.t_EndDate; --Логика не ясна
       rep_creditRisk.t_EndDateBA       := rec.t_DrawingDate; 
       
       rep_creditRisk.t_PFICategory     := chr(1); 
       rep_creditRisk.t_PFISubCategory  := chr(1);
       
       rep_creditRisk.t_PFICategory := case when ( RSB_FIInstr.FI_AvrKindsGetRoot(rec.t_FI_Kind, rec.t_AvoirKind) = RSI_RSB_FIInstr.AVOIRKIND_BOND )
                                                 then 'Процентные' 
                                            when ( RSB_FIInstr.FI_AvrKindsGetRoot(rec.t_FI_Kind, rec.t_AvoirKind) = RSI_RSB_FIInstr.AVOIRKIND_SHARE )
                                                 then 'Фондовые'
                                       end;
                                      
       if( rep_creditRisk.t_PFICategory = 'Фондовые' ) then
          rep_creditRisk.t_PFISubCategory := 'Фондовые по эмитенту'; 
       end if;                                        
       
       rep_creditRisk.t_GoodsType   := chr(1);
       rep_creditRisk.t_PFIVolType  := chr(1); 
       rep_creditRisk.t_DocKindName := 'Форвард'; 
       rep_creditRisk.t_BAKind      := rec.t_AvoirISIN; 
       
       rep_creditRisk.t_SecurFlag := case when rep_creditRisk.t_PFICategory = 'Процентные'
                                               then 'Долговые' 
                                          when rep_creditRisk.t_PFICategory = 'Фондовые'
                                               then 'Долевые'
                                     end;
       
       if( rep_creditRisk.t_SecurFlag = 'Долговые' or rep_creditRisk.t_SecurFlag = 'Долевые' ) then 
          rep_creditRisk.t_SecurIssuer := rec.t_IssuerShortName;   
       end if; 
       
       rep_creditRisk.t_ExecType    := 'Поставочная';
       rep_creditRisk.t_IsExchange  := case when rec.t_IsExchange = 1 then 'Биржевая' else 'Внебиржевая' end;
       
       rep_creditRisk.t_BuySale      := case when rec.t_IsBuy = 1 then 'Покупка' else 'Продажа' end;
       rep_creditRisk.t_BankPosition := case when rec.t_IsBuy = 1 then 'Покупатель' else 'Продавец' end;
       
       rep_creditRisk.t_ReqSum := GetAccRestOnDate( rec.t_DealID, rec.t_BofficeKind, OnDate-1, SC_PLUS_CAT_NOMINAL, 0, -1, rep_creditRisk.t_ReqCurr, rep_creditRisk.t_ReqAcc );
       if( rep_creditRisk.t_ReqAcc <> chr(1) and rep_creditRisk.t_ReqCurr <> RSI_RSB_FIInstr.NATCUR ) then
          rep_creditRisk.t_ReqSumRub := RSB_FIInstr.ConvSum( rep_creditRisk.t_ReqSum, rep_creditRisk.t_ReqCurr, RSI_RSB_FIInstr.NATCUR, OnDate-1 ); 
       else  
          rep_creditRisk.t_ReqSumRub := rep_creditRisk.t_ReqSum;
       end if;
       
       rep_creditRisk.t_OblSum := GetAccRestOnDate( rec.t_DealID, rec.t_BofficeKind, OnDate-1, SC_MINUS_CAT_NOMINAL, 0, -1, rep_creditRisk.t_OblCurr, rep_creditRisk.t_OblAcc );
       if( rep_creditRisk.t_OblAcc <> chr(1) and rep_creditRisk.t_OblCurr <> RSI_RSB_FIInstr.NATCUR ) then
          rep_creditRisk.t_OblSumRub := RSB_FIInstr.ConvSum( rep_creditRisk.t_OblSum, rep_creditRisk.t_OblCurr, RSI_RSB_FIInstr.NATCUR, OnDate-1 ); 
       else 
          rep_creditRisk.t_OblSumRub := rep_creditRisk.t_OblSum;
       end if;
       
       if( rep_creditRisk.t_BuySale = 'Покупка' ) then
          rep_creditRisk.t_PFINominal := rep_creditRisk.t_ReqSum;
          rep_creditRisk.t_NominalCurrency := GetFI_Code( rep_creditRisk.t_ReqCurr ); 
       else
          rep_creditRisk.t_PFINominal := rep_creditRisk.t_OblSum;
          rep_creditRisk.t_NominalCurrency := GetFI_Code( rep_creditRisk.t_OblCurr ); 
       end if;
       
       rep_creditRisk.t_FairValueActive := GetAccRestOnDate( rec.t_DealID, rec.t_BofficeKind, OnDate-1, '+ПФИ', 1, -1, v_toFIID, rep_creditRisk.t_FairValueAcc );
       if( rep_creditRisk.t_FairValueAcc <> chr(1) ) then
          rep_creditRisk.t_FairValuePassive := 0; --Остаток должен быть только на одном счёте
       else
          rep_creditRisk.t_FairValuePassive := GetAccRestOnDate( rec.t_DealID, rec.t_BofficeKind, OnDate-1, '-ПФИ', 1, -1, v_toFIID, rep_creditRisk.t_FairValueAcc );     
       end if;
       
       rep_creditRisk.t_NumNominalExch      := 0;
       rep_creditRisk.t_PayRateType         := chr(1);
       rep_creditRisk.t_ReceiveRateType     := chr(1);
       rep_creditRisk.t_OptionType          := chr(1);
       rep_creditRisk.t_OptionPrice         := 0;
       rep_creditRisk.t_OptionPricePoint    := 0;
       rep_creditRisk.t_OptionCost          := 0;
       rep_creditRisk.t_OptionCostPoint     := 0;
       rep_creditRisk.t_OptionStyle         := chr(1);   
       rep_creditRisk.t_PayRateType         := chr(1);
       rep_creditRisk.t_PayFixRate          := 0;
       rep_creditRisk.t_PayFixRatePoint     := 0;
       rep_creditRisk.t_ReceiveRateType     := chr(1);
       rep_creditRisk.t_ReceiveFixRate      := 0;
       rep_creditRisk.t_ReceiveFixRatePoint := 0; 
       rep_creditRisk.t_ReqPmgrSum          := 0;
       rep_creditRisk.t_ReqPmgrCurr         := -1;
       rep_creditRisk.t_ReqPmgrSumRub       := 0;                                      
       rep_creditRisk.t_OblPmgrSum          := 0; 
       rep_creditRisk.t_OblPmgrCurr         := -1;                                      
       rep_creditRisk.t_OblPmgrSumRub       := 0;                                       
       rep_creditRisk.t_DateOfInterestRate  := to_date('01.01.0001','DD.MM.YYYY');
       rep_creditRisk.t_FloatRateAcc        := chr(1);
 
       v_UsingCSAID := 0;         
       if( rec.t_CSACode <> chr(1) ) then  
          begin 
          --Берем первую попавшуюся запись, т.к. код договора CSA не обязан быть уникальным (можно дополнительно проверять контрагента csa.t_Party = rec.t_ContractorID, но и этого может быть недостаточно)
             select csa.t_CSAID, csa.t_PartyMinPaySum, csa.t_PartyMarginLimSum, csa.t_PartyMinPayCurr, 
                    NVL((select attr.t_Name 
                           from dobjatcor_dbt atCor, dobjattr_dbt attr
                          where atCor.t_ObjectType = 152 --Соглашение CSA
                            and atCor.t_GroupID    = DVCSA_CATEGORY_MARGINKIND
                            and atCor.t_Object     = LPAD(csa.t_CSAID, 34, '0')
                            and attr.t_AttrID      = atCor.t_AttrID
                            and attr.t_ObjectType  = atCor.t_ObjectType
                            and attr.t_GroupID     = atCor.t_GroupID),'') t_MarginKind,
                    NVL((select attr.t_Name 
                           from dobjatcor_dbt atCor, dobjattr_dbt attr
                          where atCor.t_ObjectType = 152 --Соглашение CSA
                            and atCor.t_GroupID    = DVCSA_CATEGORY_MARGINPAYER
                            and atCor.t_Object     = LPAD(csa.t_CSAID, 34, '0')
                            and attr.t_AttrID      = atCor.t_AttrID
                            and attr.t_ObjectType  = atCor.t_ObjectType
                            and attr.t_GroupID     = atCor.t_GroupID),'') t_MarginPayer,
                    NVL((select cast(RSB_Struct.getString(note.t_text) as varchar2(50))
                           from dnotetext_dbt note
                          where note.t_ObjectType = 152 --Соглашение CSA
                            and note.t_DocumentID = LPAD(csa.t_CSAID, 34, '0')
                            and note.t_NoteKind = DVCSA_NOTE_MARGINPERIOD
                            and OnDate-1 between note.t_Date and note.t_ValidToDate),-1) t_MarginPeriod,
                    NVL((select cast(RSB_Struct.getString(note.t_text) as varchar2(50))
                           from dnotetext_dbt note
                          where note.t_ObjectType = 152 --Соглашение CSA
                            and note.t_DocumentID = LPAD(csa.t_CSAID, 34, '0')
                            and note.t_NoteKind = DVCSA_NOTE_MARGINFACTNUMDAYS
                            and OnDate-1 between note.t_Date and note.t_ValidToDate),-1) t_MarginFactNumDays 
               into v_UsingCSAID, v_CSAPartyMinPaySum, v_CSAPartyMarginLimSum, v_CSAPartyCurr, v_CSAMarginKind, v_CSAMarginPayer, v_CSAMarginPeriod, v_CSAMarginFactNumDays
               from ddvcsa_dbt csa, doproper_dbt opr
              where csa.t_Code = substr(rec.t_CSACode,1,length(csa.t_Code))
                and csa.t_BegDate < OnDate
                and (csa.t_EndDate = to_date('01.01.0001','DD.MM.YYYY') or csa.t_EndDate >= OnDate)
                and To_Number(opr.t_DocumentID) = csa.t_CSAID 
                and opr.t_DocKind = RSB_Secur.DV_CSA 
                and (opr.t_End_Date = to_date('01.01.0001','DD.MM.YYYY') or opr.t_End_Date >= OnDate)
                and rownum = 1; 
          exception
             when OTHERS then v_UsingCSAID := 0;
          end; 
       end if;    
       
       if( v_UsingCSAID > 0 ) then
          rep_creditRisk.t_Margining           := 'Да';
          rep_creditRisk.t_MarginKind          := v_CSAMarginKind || '-' || v_CSAMarginPayer;
          rep_creditRisk.t_MarginFrequency     := v_CSAMarginPeriod; 
          rep_creditRisk.t_PartyMarginLimSum   := v_CSAPartyMarginLimSum;
          rep_creditRisk.t_PartyMarginLimCurr  := GetFI_Code( v_CSAPartyCurr );
          rep_creditRisk.t_PartyMarginLimSumRub:= case when v_CSAPartyCurr <> RSI_RSB_FIInstr.NATCUR 
                                                            then RSB_FIInstr.ConvSum( v_CSAPartyMarginLimSum, v_CSAPartyCurr, RSI_RSB_FIInstr.NATCUR, OnDate-1 ) 
                                                       else v_CSAPartyMarginLimSum end;
          rep_creditRisk.t_PartyMinPaySum      := v_CSAPartyMinPaySum;
          rep_creditRisk.t_PartyMinPayCurr     := GetFI_Code( v_CSAPartyCurr );
          rep_creditRisk.t_PartyMinPaySumRub   := case when v_CSAPartyCurr <> RSI_RSB_FIInstr.NATCUR 
                                                            then RSB_FIInstr.ConvSum( v_CSAPartyMinPaySum, v_CSAPartyCurr, RSI_RSB_FIInstr.NATCUR, OnDate-1 )
                                                       else v_CSAPartyMinPaySum end; 

          v_CSARecievedRest := GetAccRestOnDate( v_UsingCSAID, RSB_Secur.DV_CSA, OnDate-1, '-МС', 0, -1, v_toFIID, rep_creditRisk.t_ReceivedSecurAcc );
          v_CSATransferredRest := GetAccRestOnDate( v_UsingCSAID, RSB_Secur.DV_CSA, OnDate-1, '+МС', 0, -1, v_toFIID2, rep_creditRisk.t_TransferredSecurAcc );
          if( v_CSARecievedRest <> 0 ) then 
             rep_creditRisk.t_SecurityAcc      := rep_creditRisk.t_ReceivedSecurAcc;
             rep_creditRisk.t_SecuritySum      := v_CSARecievedRest;              
             rep_creditRisk.t_SecurityCurr     := GetFI_Code(v_toFIID); 
             rep_creditRisk.t_SecuritySumRub   := case when v_toFIID > -1 and v_toFIID <> RSI_RSB_FIInstr.NATCUR 
                                                            then RSB_FIInstr.ConvSum( rep_creditRisk.t_SecuritySum, v_toFIID, RSI_RSB_FIInstr.NATCUR, OnDate-1 )
                                                       else rep_creditRisk.t_SecuritySum end;
          else
             rep_creditRisk.t_SecurityAcc      := rep_creditRisk.t_TransferredSecurAcc;
             rep_creditRisk.t_SecuritySum      := v_CSATransferredRest;              
             rep_creditRisk.t_SecurityCurr     := GetFI_Code(v_toFIID2); 
             rep_creditRisk.t_SecuritySumRub   := case when v_toFIID2 > -1 and v_toFIID2 <> RSI_RSB_FIInstr.NATCUR 
                                                            then RSB_FIInstr.ConvSum( rep_creditRisk.t_SecuritySum, v_toFIID2, RSI_RSB_FIInstr.NATCUR, OnDate-1 )
                                                       else rep_creditRisk.t_SecuritySum end;
          end if;
          
          rep_creditRisk.t_SecuritySum2        := rec.t_NetCollateralSum;
          rep_creditRisk.t_SecurityCurr2       := rec.t_NetCollateralCurr; 
          v_toFIID := GetFIIDbyCode( rec.t_NetCollateralCurr );    
          rep_creditRisk.t_SecuritySumRub2     := case when v_toFIID > -1 and v_toFIID <> RSI_RSB_FIInstr.NATCUR 
                                                            then RSB_FIInstr.ConvSum( rep_creditRisk.t_SecuritySum2, v_toFIID, RSI_RSB_FIInstr.NATCUR, OnDate-1 )
                                                       else rep_creditRisk.t_SecuritySum2 end;   
          
          rep_creditRisk.t_SecurityType        := 'Денежные средства';
          rep_creditRisk.t_FactNumDays         := v_CSAMarginFactNumDays;   
       else
          rep_creditRisk.t_Margining           := 'Нет';
          rep_creditRisk.t_MarginKind          := chr(1);
          rep_creditRisk.t_MarginFrequency     := 0;
          rep_creditRisk.t_PartyMarginLimSum   := 0;
          rep_creditRisk.t_PartyMarginLimCurr  := chr(1);
          rep_creditRisk.t_PartyMarginLimSumRub:= 0;
          rep_creditRisk.t_PartyMinPaySum      := 0;
          rep_creditRisk.t_PartyMinPayCurr     := chr(1);
          rep_creditRisk.t_PartyMinPaySumRub   := 0;
          rep_creditRisk.t_SecurityAcc         := chr(1);
          rep_creditRisk.t_SecuritySum         := 0;
          rep_creditRisk.t_SecurityCurr        := chr(1);
          rep_creditRisk.t_SecuritySumRub      := 0;
          rep_creditRisk.t_ReceivedSecurAcc    := chr(1);
          rep_creditRisk.t_TransferredSecurAcc := chr(1);
          rep_creditRisk.t_SecuritySum2        := 0;
          rep_creditRisk.t_SecurityCurr2       := chr(1);     
          rep_creditRisk.t_SecuritySumRub2     := 0;
          rep_creditRisk.t_SecurityType        := chr(1);
          rep_creditRisk.t_FactNumDays         := 0;           
       end if; 
       
       rep_creditRisk.t_StandartPoorsRub     := GetRatingRep(3, rec.t_ContractorID, CONTRACTOR_RATING_NATIONAL, STANDARTPOORS_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_StandartPoorsRub2    := GetRatingRep(3, rec.t_ContractorID, CONTRACTOR_RATING_NATIONAL, STANDARTPOORS_3453Y_RATING_PARENTID, OnDate-1);  
       rep_creditRisk.t_MoodysRub            := GetRatingRep(3, rec.t_ContractorID, CONTRACTOR_RATING_NATIONAL, MOODYS_RATING_PARENTID, OnDate-1);         
       rep_creditRisk.t_MoodysRub2           := GetRatingRep(3, rec.t_ContractorID, CONTRACTOR_RATING_NATIONAL, MOODYS_3453Y_RATING_PARENTID, OnDate-1);         
       rep_creditRisk.t_FitchRatingsRub      := GetRatingRep(3, rec.t_ContractorID, CONTRACTOR_RATING_NATIONAL, FITCHRATINGS_RATING_PARENTID, OnDate-1);    
       rep_creditRisk.t_FitchRatingsRub2     := GetRatingRep(3, rec.t_ContractorID, CONTRACTOR_RATING_NATIONAL, FITCHRATINGS_3453Y_RATING_PARENTID, OnDate-1);     
       rep_creditRisk.t_StandartPoorsCurr    := GetRatingRep(3, rec.t_ContractorID, CONTRACTOR_RATING_FOREIGN, STANDARTPOORS_RATING_PARENTID, OnDate-1); 
       rep_creditRisk.t_StandartPoorsCurr2   := GetRatingRep(3, rec.t_ContractorID, CONTRACTOR_RATING_FOREIGN, STANDARTPOORS_3453Y_RATING_PARENTID, OnDate-1); 
       rep_creditRisk.t_MoodysCurr           := GetRatingRep(3, rec.t_ContractorID, CONTRACTOR_RATING_FOREIGN, MOODYS_RATING_PARENTID, OnDate-1);        
       rep_creditRisk.t_MoodysCurr2          := GetRatingRep(3, rec.t_ContractorID, CONTRACTOR_RATING_FOREIGN, MOODYS_3453Y_RATING_PARENTID, OnDate-1);        
       rep_creditRisk.t_FitchRatingsCurr     := GetRatingRep(3, rec.t_ContractorID, CONTRACTOR_RATING_FOREIGN, FITCHRATINGS_RATING_PARENTID, OnDate-1);   
       rep_creditRisk.t_FitchRatingsCurr2    := GetRatingRep(3, rec.t_ContractorID, CONTRACTOR_RATING_FOREIGN, FITCHRATINGS_3453Y_RATING_PARENTID, OnDate-1);  
       rep_creditRisk.t_StandartPoorsCountry := GetRatingRep(5, rec.t_CountryID, COUNTRY_RATING_NATIONAL, STANDARTPOORS_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_MoodysCountry        := GetRatingRep(5, rec.t_CountryID, COUNTRY_RATING_NATIONAL, MOODYS_RATING_PARENTID, OnDate-1);     
       rep_creditRisk.t_FitchRatingsCountry  := GetRatingRep(5, rec.t_CountryID, COUNTRY_RATING_NATIONAL, FITCHRATINGS_RATING_PARENTID, OnDate-1);             
       rep_creditRisk.t_AKRARating           := GetRatingRep(Rsb_Secur.OBJTYPE_PARTY, rec.t_ContractorID, CONTRACTOR_RATING_NATIONAL, AKRA_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_ExpertRating         := GetRatingRep(Rsb_Secur.OBJTYPE_PARTY, rec.t_ContractorID, CONTRACTOR_RATING_NATIONAL, EXPRA_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_NRCRating            := GetRatingRep(Rsb_Secur.OBJTYPE_PARTY, rec.t_ContractorID, CONTRACTOR_RATING_NATIONAL, NCR_RATING_PARENTID, OnDate-1);
       rep_creditRisk.t_NRARating            := GetRatingRep(Rsb_Secur.OBJTYPE_PARTY, rec.t_ContractorID, CONTRACTOR_RATING_NATIONAL, NRA_RATING_PARENTID, OnDate-1);                     
       
       g_pfiCreditRisk_ins.extend;
       g_pfiCreditRisk_ins(g_pfiCreditRisk_ins.LAST) := rep_creditRisk;      
     END LOOP;

     IF g_pfiCreditRisk_ins IS NOT EMPTY THEN
         FORALL i IN g_pfiCreditRisk_ins.FIRST .. g_pfiCreditRisk_ins.LAST
             INSERT INTO DDL_PFICREDITRISK_TMP
                 VALUES g_pfiCreditRisk_ins(i);
        g_pfiCreditRisk_ins.delete;
     END IF;

  END CreateData_SC;
  
END RSB_DL_PFICreditRisk;
/
