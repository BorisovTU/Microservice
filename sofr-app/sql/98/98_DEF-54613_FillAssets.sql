CREATE OR REPLACE procedure Usr_FillAssets as
   v_repdate date := sysdate-1;
   v_count number;
   v_MMVB_Code varchar2(35);
   v_SPB_Code varchar2(35);
   v_mp_mmvb number;
   v_mp_spb number;
   v_fiid number;
   v_rate number;
   v_nkd number;
   v_fiid_table number;
   
   TYPE avoir_amount_temp_rec IS RECORD (
      isin        varchar2(25),
      facevaluefi number,
      restcb      number,
      fiid        number,
      fiid_rate   number,
      fi_kind    number
   );
   
   TYPE avoir_amount_t IS TABLE OF avoir_amount_temp_rec
   INDEX BY PLS_INTEGER;
   v_avoir_amount avoir_amount_t;
   
   avoir_amount_rec SP_ACTIVE_LIST%ROWTYPE;

   function GetRateFin(p_fiid in number, p_daterate in date, p_mp_mmvb in number, p_mp_spb in number, p_ToFI out number)
   return number is
      v_cnt number;

      v_rate_mmvb number;
      v_rate_spb number;
      v_rateid number;
      v_ratedate date;
      v_days number;
      v_true_mp number := -1;
      t_RD       DRATEDEF_DBT%ROWTYPE;
      v_ToFI number(10);
      v_rate number;

      c_type_value_mmvb  constant number(10) := 17; -- объем торгов ММВБ
      c_type_value_spb constant number(10) := 35;  -- объем торгов СПБ
      c_type_price constant number(10) := 1; -- рыночная цена
   begin
      select count(*) into v_cnt
        from dratedef_dbt where t_otherfi = p_fiid and t_type = 1;
      v_days := p_daterate-to_date('01.01.0001','DD.MM.YYYY');
            
      if v_cnt > 1 then
         v_rate_mmvb :=  RSI_RSB_FIInstr.FI_GetRate(p_fiid, -1, c_type_value_mmvb, p_daterate, v_days, 0, v_rateid, v_ratedate);
         v_rate_spb :=  RSI_RSB_FIInstr.FI_GetRate(p_fiid, -1, c_type_value_spb, p_daterate, v_days, 0, v_rateid, v_ratedate);
         if v_rate_mmvb >= v_rate_spb then
            v_true_mp := p_mp_mmvb;
         else 
            v_true_mp := p_mp_spb;
         end if;
      else 
         v_true_mp := -1;
      end if;
      
      v_rate :=  RSI_RSB_FIInstr.FI_GetRate(p_fiid, -1, c_type_price, p_daterate, v_days, 0, v_rateid, v_ratedate, 
                                             false, null, null, null, null, v_true_mp);
      if v_rate > 0 then 
         select * into t_RD
            from dratedef_dbt
           where t_RateID = v_rateid;
          
          if t_RD.t_OtherFI = p_fiid then
             v_ToFI := t_RD.t_FIID;
          else
             v_ToFI := t_RD.t_OtherFI;
          end if;
      else 
        v_ToFI := 0;
      end if;
      
      p_ToFI := v_ToFI;
      return v_rate;
   -- а если exception, значит будет exception      
   end;

   function GetPartyidByCode(p_code in varchar2) return number is
        res number;
    begin
        select t_objectid into res
         from dobjcode_dbt
        where t_objecttype = 3 and t_codekind = 1 and t_code = p_code and t_state= 0;
        return res;
    exception
        when others then return -1;    
    end;

   function GetFiCode(p_fiid in number) return varchar2 is
      res varchar2(3);
   begin
      select t_ccy into res 
        from dfininstr_dbt
       where t_fiid = p_fiid;
      
      return res;
   exception   
      when no_data_found then return '---';
   end;
  
/* обрезаем количество до 10 символов, так как количество ценных бумаг - это строка 10 символов */
  function CutNumber(p_amount in number) return number is
    v_str varchar2(200);
    v_length number(10);
    v_waste number;
    c_dimension constant number := 10;
    res number;
  begin
    res := p_amount;
    v_str := to_char(p_amount);
    v_length := length(v_str);
    
    v_waste := v_length - c_dimension;
    if v_waste > 0 then
        v_str := substr(v_str,1,c_dimension);
        res:= to_number(v_str);
    end if;
    return res;
  end;  
  

begin
   execute immediate 'truncate table SP_ACTIVE_LIST';
   v_count := 0;
   v_MMVB_Code := trim(rsb_common.GetRegStrValue('SECUR\MICEX_CODE', 0));
   v_SPB_Code  := trim(rsb_common.GetRegStrValue('SECUR\SPBEX_CODE', 0));
    
   v_mp_mmvb := GetPartyidByCode(v_MMVB_Code);
   v_mp_spb :=  GetPartyidByCode(v_SPB_Code);
  
   for datax in (
         SELECT sp.CFTId, p.t_partyid, p.t_name, sf.t_number, sf.t_datebegin,
                nvl(sp.OperationDate-1,trunc(v_repdate)) OperationDate, dl.t_dlcontrid
           FROM SP_ACTIVE_LIST_IN sp,
                dparty_dbt        p,
                dobjcode_dbt      obj,
                dsfcontr_dbt      sf,
                ddlcontr_dbt      dl
          WHERE obj.t_objecttype = 3 AND obj.t_codekind = 101 AND obj.t_state = 0 AND obj.t_objectid = p.t_partyid
            AND sp.CFTId = obj.t_code
            AND sf.t_partyid = p.t_partyid 
            AND (sf.t_dateclose > nvl(sp.OperationDate-1,trunc(v_repdate)) or sf.t_dateclose = to_date('01010001','ddmmyyyy'))
            AND sf.t_id = dl.t_sfcontrid
         ) loop

      -- брокерские счета
      -- для ЕДП не повторяются
      insert into SP_ACTIVE_LIST(CFTId, KindProduct, OperationDate, Account, KindActiv, Currency, AmountCur, equivalent, Amount)
         SELECT datax.CFTId, t_account, datax.OperationDate, t_account, 'Денежные средства', t_ccy, restacc, restacc*rate, restacc*rate
           FROM ( SELECT acc.t_account, fi.t_ccy, 
                         RSI_RSB_ACCOUNT.restall( acc.t_account, acc.t_chapter, acc.t_code_currency, datax.OperationDate, acc.t_code_currency) restacc,
                         RSI_RSB_FIInstr.ConvSum(1, acc.t_code_currency, RSI_RSB_FIInstr.NATCUR, datax.OperationDate) rate
                    FROM daccount_dbt  acc,
                         dfininstr_dbt fi
                   WHERE (acc.t_account, acc.t_chapter, acc.t_code_currency) in (
                           SELECT settacc.t_account, settacc.t_chapter, settacc.t_fiid
                             FROM dsfssi_dbt       ssi, 
                                  dsettacc_dbt     settacc, 
                                  ddlcontrmp_dbt   mp,
                                  dsfcontr_dbt     sf_mp
                            WHERE ssi.t_objecttype = 659 AND ssi.t_objectid = lpad(mp.t_sfcontrid, 10, chr(48))
                              AND settacc.t_settaccid = ssi.t_setaccid 
                              AND mp.t_sfcontrid = sf_mp.t_id 
                              AND ( sf_mp.t_dateclose > datax.OperationDate or sf_mp.t_dateclose = to_date('01010001','ddmmyyyy'))
                              AND mp.t_dlcontrid = datax.t_dlcontrid 
                           )
                     AND (acc.t_close_date > datax.OperationDate or acc.t_close_date = to_date('01010001','ddmmyyyy'))
                     AND fi.t_fiid = acc.t_code_currency ) ;

      -- бумажные счета
      SELECT a.t_isin, a.t_facevaluefi, a.restcb, a.t_fiid,  a.t_facevaluefi, a.t_fi_kind
       BULK COLLECT INTO v_avoir_amount
        FROM ( SELECT -1 * rsb_account.restac(accd.t_Account, accd.t_Currency, datax.OperationDate, accd.t_Chapter, NULL) AS restcb, 
                      a.t_isin, f.t_name, f.t_fiid, f.t_facevaluefi, f.t_fi_kind
                 FROM dmcaccdoc_dbt  accd,
                      daccount_dbt acc,
                      dmccateg_dbt   cat,
                      davoiriss_dbt  a,
                      dfininstr_dbt  f,
                      ddlcontrmp_dbt mp,
                      dsfcontr_dbt   sf_mp
                WHERE accd.t_Chapter = 22 AND accd.t_owner = datax.t_partyid 
                  AND accd.t_ClientContrID = mp.t_sfcontrid AND accd.t_iscommon = chr(88) 
                  AND acc.t_account = accd.t_account and acc.t_code_currency = accd.t_currency and acc.t_chapter = accd.t_chapter 
                  AND (acc.t_close_date > datax.OperationDate or acc.t_close_date = to_date('01010001','ddmmyyyy'))
                  AND cat.t_Id = accd.t_CatID AND cat.t_LevelType = 1 AND cat.t_Code in ( 'ЦБ Клиента, ВУ','НЦБ клиента, ВУ')
                  AND a.T_FIID = accd.T_CURRENCY  
                  AND f.t_fi_kind = 2 AND a.t_fiid = f.t_fiid 
                  AND mp.t_sfcontrid = sf_mp.t_id AND sf_mp.t_servkind = 1 
                  AND ( sf_mp.t_dateclose > datax.OperationDate or sf_mp.t_dateclose = to_date('01010001','ddmmyyyy'))
                  AND mp.t_dlcontrid = datax.t_dlcontrid 
               ) a ; 
    
      if v_avoir_amount.count > 0 then
         FOR i IN v_avoir_amount.FIRST .. v_avoir_amount.LAST LOOP
            avoir_amount_rec.CFTId         := datax.CFTId;
            avoir_amount_rec.KindActiv     := 'Ценные бумаги';
            avoir_amount_rec.KindProduct   := v_avoir_amount(i).isin; 
            avoir_amount_rec.Account       := null; 
            avoir_amount_rec.OperationDate := datax.OperationDate;
            avoir_amount_rec.Currency      := null; 
            avoir_amount_rec.Count         := CutNumber(v_avoir_amount(i).restcb);
            avoir_amount_rec.AmountCur     := 0;
            avoir_amount_rec.equivalent    := 0;
            avoir_amount_rec.Amount        := 0;
            
            v_fiid := v_avoir_amount(i).fiid;
            v_rate := GetRateFin(v_fiid, datax.OperationDate, v_mp_mmvb, v_mp_spb, v_avoir_amount(i).fiid_rate);

            if RSI_RSB_FIInstr.FI_IsAvrKindBond( v_avoir_amount(i).fi_kind ) then 
                -- облигация, валюта номинала
                v_fiid_table := v_avoir_amount(i).facevaluefi;
            else 
                -- валюта котировки
               v_fiid_table := v_avoir_amount(i).fiid_rate; 
            end if;

            v_nkd  := RSI_RSB_FIInstr.CalcNKD( v_fiid, datax.OperationDate, 1, 0);            
            avoir_amount_rec.Currency := GetFiCode(v_fiid_table);
            
            if avoir_amount_rec.Count > 0 then
               avoir_amount_rec.equivalent := RSI_RSB_FIInstr.ConvSum(v_rate, v_avoir_amount(i).fiid_rate, RSI_RSB_FIInstr.NATCUR, datax.OperationDate);
            end if;
            if v_rate > 0 then 
               avoir_amount_rec.AmountCur := (v_rate+v_nkd)* avoir_amount_rec.Count ;
               avoir_amount_rec.Amount := round(RSI_RSB_FIInstr.ConvSum(avoir_amount_rec.AmountCur, v_avoir_amount(i).fiid_rate, RSI_RSB_FIInstr.NATCUR, datax.OperationDate),2);
               avoir_amount_rec.AmountCur := round(RSI_RSB_FIInstr.ConvSum(avoir_amount_rec.AmountCur, v_avoir_amount(i).fiid_rate, v_fiid_table, datax.OperationDate),2);
            end if;
            
            insert into SP_ACTIVE_LIST values avoir_amount_rec;
         END LOOP;
      end if;

      v_count := v_count+1;
      if mod(v_count,1000) = 0 then
         commit;
      end if;
      
   end loop;
  
   commit;
   
end;
/
