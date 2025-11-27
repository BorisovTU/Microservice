create or replace package body rshb_brkrep is

  CourseTypeCCFut number := rsb_common.GetRegIntValue('ПРОИЗВОДНЫЕ ИНСТРУМЕНТЫ\ВИД КУРСА ОЦЕНКИ СС ФЬЮЧЕРСА');
  CourseTypeCCOpt number := rsb_common.GetRegIntValue('ПРОИЗВОДНЫЕ ИНСТРУМЕНТЫ\ВИД КУРСА ОЦЕНКИ СС ОПЦИОНА');

  -- Вставка в лог
  procedure InsertLog(mes in varchar2) is
    pragma autonomous_transaction;
  begin
    insert into dlog_u_dbt values (0, substr(mes, 1, 500));
    commit;
  end;

  -- Чистка данных
  procedure ClearData is
  begin
    -- Дистрибутивные структуры по ФР
    DELETE FROM DBRKREPDEAL_u_TMP;
    DELETE FROM DBRKREPINACC_u_TMP;
    DELETE FROM DBRKREPPOOL_u_TMP;
  
    --DELETE FROM DBRKREPDEAL_TMP_dbt;
    --DELETE FROM DBRKREPINACC_TMP_dbt;
    --DELETE FROM DBRKREPPOOL_TMP_dbt;
  
    -- Пользовательские структуры по СР
    delete from dbrkrep_contr_u_tmp;
    delete from dbrkrep_contr_acc_u_tmp;
    delete from dbrkrepdeal_u_fm1_tmp;
    delete from dbrkrepdeal_u_fm2_1_tmp;
    delete from dbrkrepdeal_u_fm2_2_tmp;
    delete from dbrkrepdeal_u_fm3_tmp;
  end;

  -- Получить вариационную маржу
  procedure GetMargin(p_DealID in number,
                      p_Date   in date,
                      p_Margin out number) is
  begin
    select nvl(max(dvdlturn.t_margin), 0)
      into p_Margin
      from ddvdlturn_dbt dvdlturn
      join (select max(dvdlturn.t_id) t_id
              from ddvdlturn_dbt dvdlturn
             where dvdlturn.t_dealid = p_DealID
               and dvdlturn.t_date <= p_Date) t
        on t.t_id = dvdlturn.t_id;
  end;

  -- Получить сумму комиссий биржи и брокера
  procedure GetComiss(p_ClientID in number,
                      p_DealID   in number,
                      p_MarkID   in number,
                      p_ComMark  out number,
                      p_ComClir  out number,
                      p_ComBr    out number) is
    p_ComMark2 number; /*CHVA*/
  begin
    -- Комиссия биржи и клиринговая комиссия
    select nvl(sum(case
                     when lower(sfcomiss.t_name) like '%клир%' then
                      t_sum
                     else
                      0
                   end),
               0) t_sumclir,
           nvl(sum(case
                     when lower(sfcomiss.t_name) not like '%клир%' then
                      t_sum
                     else
                      0
                   end),
               0) t_summark
      into p_ComClir, p_ComMark
      from ddvdlcom_dbt dvdlcom
      join dsfcomiss_dbt sfcomiss
        on sfcomiss.t_comissid = dvdlcom.t_comissid
       and sfcomiss.t_receiverid = p_MarkID
     where dvdlcom.t_dealid = p_DealID;
    -- Комиссия брокера
    --select nvl(sum(rsi_rsb_fiinstr.ConvSum(sfbasobj.t_commsum, sfbasobj.t_fiid, 0, sfdefcom.t_datefee, 1)), 0) t_sumbr
    --into p_ComBr
    --from dsfbasobj_dbt sfbasobj
    --join dsfdefcom_dbt sfdefcom on sfdefcom.t_id = sfbasobj.t_defcommid
    --join dsfsi_dbt sfsi_p on sfsi_p.t_objecttype = 663/*Удерж. периодическая комиссия*/ and
    -- sfsi_p.t_objectid = lpad(sfbasobj.t_defcommid,10,'0') and
    -- sfsi_p.t_debetcredit = 0 and
    -- sfsi_p.t_partyid = p_ClientID
    --join dsfsi_dbt sfsi_r on sfsi_r.t_objecttype = 663/*Удерж. периодическая комиссия*/ and
    -- sfsi_r.t_objectid = lpad(sfbasobj.t_defcommid,10,'0') and
    -- sfsi_r.t_debetcredit = 1
    --join ddp_dep_dbt dp_dep on dp_dep.t_partyid = sfsi_r.t_partyid
    --where sfbasobj.t_baseobjecttype = 140/*Операция с ПИ*/ and
    -- sfbasobj.t_baseobjectid = p_DealID;
  
    /*CHVA включаем комиссию за исполнение*/
  
    select nvl(sum(t_sum), 0) t_summark2
      into p_ComMark2
      from ddvfi_com_dbt fi_com, ddvdeal_dbt deal, dsfcomiss_dbt sfcomiss
     where sfcomiss.t_comissid = fi_com.t_comissid
       and sfcomiss.t_receiverid = p_MarkID
       and sfcomiss.t_number = 68
       and sfcomiss.t_feetype = 1
       and fi_com.t_client = p_ClientID
       and deal.t_id = p_DealID
       and fi_com.t_date = deal.t_date;
    p_ComMark := p_ComMark + p_ComMark2;
    /*CHVA*/
  
    select nvl(sum(t_sum), 0) t_sumbr
      into p_ComBr
      from ddvdlcom_dbt dvdlcom
      join dsfcomiss_dbt sfcomiss
        on sfcomiss.t_comissid = dvdlcom.t_comissid
      join ddp_dep_dbt dp_dep
        on dp_dep.t_partyid = sfcomiss.t_receiverid
     where dvdlcom.t_dealid = p_DealID;
  end;

  -- Получить данные по позиции
  procedure GetPosData(p_ContrID     in number,
                       p_BegDate     in date,
                       p_EndDate     in date,
                       p_fiid        in number,
                       p_department  in number,
                       p_broker      in number,
                       p_genagrid    in number,
                       p_indate      in date,
                       p_outdate     in date,
                       p_avoirkind   in number,
                       p_tickfiid    in number,
                       p_strikefiid  in number,
                       p_inRest      out number,
                       p_turnPlus    out number,
                       p_turnMinus   out number,
                       p_outRest     out number,
                       p_inMargin    out number,
                       p_outMargin   out number,
                       p_inGuaranty  out number,
                       p_outGuaranty out number,
                       p_Pin         out number,
                       p_Pout        out number) is
    CourseKind number;
    PFI        number;
  begin
    -- Входящие остатки
    p_inRest     := 0;
    p_inMargin   := 0;
    p_inGuaranty := 0;
    -- Были обороты до даты начала периода отчета
    if (p_indate != to_date('01.01.0001', 'dd.mm.yyyy')) then
      -- Остаток бумаг берем из предыдущего оборота
      begin
        select t_longposition - t_shortposition t_rest
          into p_inRest
          from ddvfiturn_dbt
         where t_fiid = p_fiid
           and t_department = p_department
           and t_broker = p_broker
           and t_clientcontr = p_ContrID
           and t_genagrid = p_genagrid
           and t_date = p_indate;
      exception
        when no_data_found then
          null;
      end;
      -- Вариационную маржу и гарантийное обеспечение из оборотов по всем предыдущим оборотам
      select nvl(sum(t_guaranty), 0)
        into p_inGuaranty
        from ddvfiturn_dbt
       where t_fiid = p_fiid
         and t_department = p_department
         and t_broker = p_broker
         and t_clientcontr = p_ContrID
         and t_genagrid = p_genagrid
         and t_date in (select max(t_date)
                          from ddvfiturn_dbt
                         where t_fiid = p_fiid
                           and t_department = p_department
                           and t_broker = p_broker
                           and t_clientcontr = p_ContrID
                           and t_genagrid = p_genagrid
                           and t_date < p_BegDate);
      -- Маржу берём из t_marginDay
      select p_inMargin + nvl(sum(t_marginDay), 0)
        into p_inMargin
        from ddvfiturn_dbt
       where t_fiid = p_fiid
         and t_department = p_department
         and t_broker = p_broker
         and t_clientcontr = p_ContrID
         and t_genagrid = p_genagrid
         and t_date = p_BegDate;
    end if;
    -- Оборотов не было
    if (p_outdate = to_date('01.01.0001', 'dd.mm.yyyy')) then
      p_turnPlus    := 0;
      p_turnMinus   := 0;
      p_outRest     := p_inRest;
      p_outMargin   := p_inMargin;
      p_outGuaranty := p_inGuaranty;
      -- Были обороты
    else
      select nvl(sum(t_buy), 0) t_buy,
             nvl(sum(t_sale), 0) + nvl(sum(T_LONGEXECUTION), 0) t_sale, /*PNV 511820*/
             p_inRest + nvl(sum(t_buy), 0) - nvl(sum(t_sale), 0) -
             nvl(sum(T_LONGEXECUTION), 0), /*PNV 511820*/
             p_inMargin + nvl(sum(t_margin), 0),
             nvl(sum(t_guaranty), 0)
        into p_turnPlus, p_turnMinus, p_outRest, p_outMargin, p_outGuaranty
        from ddvfiturn_dbt
       where t_fiid = p_fiid
         and t_department = p_department
         and t_broker = p_broker
         and t_clientcontr = p_ContrID
         and t_genagrid = p_genagrid
         and t_date between p_BegDate and p_EndDate;
    end if;
    -- Расчетная цена
    if (p_avoirkind = rsb_derivatives.DV_DERIVATIVE_FUTURES) then
      CourseKind := CourseTypeCCFut;
      PFI        := p_TickFIID;
    elsif (p_avoirkind = rsb_derivatives.DV_DERIVATIVE_OPTION) then
      CourseKind := CourseTypeCCOpt;
      PFI        := p_StrikeFIID;
    else
      p_Pin  := 0;
      p_Pout := 0;
      return;
    end if;
    if (PFI = rsi_rsb_fiinstr.NATCUR) then
      p_Pin  := nvl(rsi_rsb_fiinstr.ConvSumType(1,
                                                p_fiid,
                                                PFI,
                                                CourseKind,
                                                p_BegDate - 1,
                                                0),
                    0);
      p_Pout := nvl(rsi_rsb_fiinstr.ConvSumType(1,
                                                p_fiid,
                                                PFI,
                                                CourseKind,
                                                p_EndDate,
                                                0),
                    0);
    else
      p_Pin := nvl(rsi_rsb_fiinstr.ConvSumType(1,
                                               p_fiid,
                                               PFI,
                                               CourseKind,
                                               p_BegDate - 1,
                                               0),
                   0);
      --p_Pin := nvl(rsi_rsb_fiinstr.ConvSum(p_Pin, PFI, rsi_rsb_fiinstr.NATCUR, p_BegDate - 1, 1), 0);
      p_Pout := nvl(rsi_rsb_fiinstr.ConvSumType(1,
                                                p_fiid,
                                                PFI,
                                                CourseKind,
                                                p_EndDate,
                                                0),
                    0);
      --p_Pout := nvl(rsi_rsb_fiinstr.ConvSum(p_Pout, PFI, rsi_rsb_fiinstr.NATCUR, p_EndDate, 1), 0);
    end if;
  end;

  -- Формирование списка счетов
  procedure CreateAccounts(p_ClientID in number,
                           p_ContrID  in number,
                           p_BegDate  in date,
                           p_EndDate  in date) is
  begin
    /** Golovkin 26.01.2022 ID : 538062 Не подтягиваются счета до перехода на ЕДП */
    insert into dbrkrep_contr_acc_u_tmp
      select p_ClientID,
             sfmp.t_id,
             acc.t_currency,
             acc.t_account,
             acc.t_chapter,
             acc.t_departmentid,
             rsb_account.restall(acc.t_account,
                                 acc.t_chapter,
                                 acc.t_currency,
                                 p_BegDate - 1),
             rsb_account.restall(acc.t_account,
                                 acc.t_chapter,
                                 acc.t_currency,
                                 p_EndDate),
             rsb_account.kreditac(acc.t_Account,
                                  acc.t_Chapter,
                                  acc.t_currency,
                                  p_BegDate,
                                  p_EndDate,
                                  null),
             rsb_account.debetac(acc.t_Account,
                                 acc.t_Chapter,
                                 acc.t_currency,
                                 p_BegDate,
                                 p_EndDate,
                                 null)
          FROM dsfcontr_dbt sfmp
               INNER JOIN dmcaccdoc_dbt acc
                   ON (    (acc.t_dockind = 3001 AND acc.t_docid = sfmp.t_id)
                        OR (acc.t_dockind = 0 AND acc.t_clientcontrid = sfmp.t_id ANd acc.t_owner = sfmp.T_PARTYID) )
                      AND acc.T_ACTIVATEDATE <= p_EndDate
                      AND (acc.t_disablingdate >= p_BegDate or acc.t_disablingdate = to_date('01.01.0001','dd.mm.yyyy'))
               INNER JOIN dmccateg_dbt categ 
                  ON     categ.t_id = acc.t_catid
                     AND categ.t_LevelType = 1
                     AND categ.t_Code IN ('ДС клиента, ц/б', 'Брокерский счет ДБО')               
         WHERE     sfmp.t_partyid = p_ClientID
               AND (p_ContrID = 0 or sfmp.t_id = p_ContrID)
               AND sfmp.t_ServKind in (rsi_npto.PTSK_STOCKDL /*1 фондовый дилинг*/, rsi_npto.PTSK_DV /*15 Срочные контракты (ФИССИКО)*/)
         group by sfmp.t_id,
                  acc.t_currency,
                  acc.t_account,
                  acc.t_chapter,
                  acc.t_departmentid;
  end;

  -- Вставка данных по договору
  procedure InsertContr(p_ClientID in number,
                        p_ContrID  in number,
                        p_BegDate  in date,
                        p_EndDate  in date) is
  begin
    -- Общие данные
    insert into dbrkrep_contr_u_tmp
      select p_ClientID,
             p_ContrID,
             sfcontr.t_servkind,
             -- bpv nvl(sfcontr_dl.t_name, case when t_legalform = 1 then party.t_ShortName else party.t_name end),
             case
               when t_legalform = 1 then
                sfcontr_dl.t_name
               else
                party.t_name
             end,
             -- ks 24.04.2019
             /*
             NVL((select objcode.t_Code
             from dobjcode_dbt objcode
             where objcode.t_ObjectType = cnst.OBJTYPE_PARTY and
             objcode.t_CodeKind = cnst.PTCK_MICEX\*Код на ММВБ*\ and
             objcode.t_ObjectID = party.t_PartyID and
             objcode.t_State = 0), CHR(1)) ClientCode,
             */
             NVL((select objcode.t_Code
                   from ddlobjcode_dbt objcode
                  where objcode.t_ObjectType = 207 /*Договор брокерского обслуживания*/
                    and objcode.t_CodeKind = 1 /*Единый краткий код*/
                    and objcode.t_ObjectID = dlcontr.t_dlcontrid /*and
                  objcode.t_State = 0*/
                 ),
                 CHR(1)) ClientCode,
             NVL(sfcontr_dl.t_DateConc,
                 nvl(sfcontr.t_DateConc, to_date('01.01.0001', 'dd.mm.yyyy'))) ContrDate,
             NVL(sfcontr_dl.t_Number, nvl(sfcontr.t_Number, CHR(1))) ContrNumber,
             case
               when party.t_legalform = 1 then
                chr(0)
               when nvl(persn.t_isemployer, chr(0)) = chr(88) then
                chr(0)
               else
                chr(88)
             end IsIndividual,
             -- (select nvl(max(rtrim(UTL_RAW.CAST_TO_VARCHAR2(notetext.t_text),chr(0))),chr(1)) note104
             -- from ddlcontrmp_dbt dlcontrmp
             -- join dnotetext_dbt notetext on notetext.t_objecttype = 207/*Договор брокерского обслуживания*/ and
             -- notetext.t_documentid = lpad(dlcontrmp.t_dlcontrid,34,'0') and
             -- notetext.t_notekind = case when nvl(sfcontr.t_servkindsub,0) = 9 then 101/*Счет Депо Владельца*/
             -- else 104/*Торговый счет Депо*/ end and
             -- notetext.t_date <= p_EndDate and
             -- notetext.t_validtodate > p_EndDate
             -- where dlcontrmp.t_sfcontrid = p_ContrID),
             (select /*сначала пытаемся вывести М- счет, если его нет, т работаем по старому*/
               case
                 when nvl(max(rtrim(UTL_RAW.CAST_TO_VARCHAR2(notetext3.t_text),
                                    chr(0))),
                          chr(1)) = chr(1) or sfcontr.t_servkindsub = 8 then
                  nvl(max(rtrim(UTL_RAW.CAST_TO_VARCHAR2(notetext.t_text),
                                chr(0))),
                      chr(1)) || case
                    when max(notetext2.t_notekind) is not null then
                     '/' || max(rtrim(UTL_RAW.CAST_TO_VARCHAR2(notetext2.t_text),
                                      chr(0)))
                    else
                     ''
                  end
                 else
                  nvl(max(rtrim(UTL_RAW.CAST_TO_VARCHAR2(notetext3.t_text),
                                chr(0))),
                      chr(1)) || case
                    when max(notetext4.t_notekind) is not null then
                     '/' || max(rtrim(UTL_RAW.CAST_TO_VARCHAR2(notetext4.t_text),
                                      chr(0)))
                    else
                     ''
                  end
               end note104
                from ddlcontrmp_dbt dlcontrmp
                left join dsfcontr_dbt sfcnt
                    on sfcnt.t_PartyID = p_ClientID
                    and sfcnt.t_ID = p_ContrID
                left join dnotetext_dbt notetext
                  on notetext.t_objecttype = 207 /*Договор брокерского обслуживания*/
                 and notetext.t_documentid =
                     lpad(dlcontrmp.t_dlcontrid, 34, '0')
                 and notetext.t_notekind = case
                       when nvl(sfcnt.t_servkindsub, 0) = 9 then
                        101 /*Счет Депо Владельца*/
                       else
                        104 /*Торговый счет Депо*/
                     end
                 and notetext.t_date <= p_EndDate
                 and notetext.t_validtodate > p_EndDate
                left join dnotetext_dbt notetext2
                  on nvl(sfcnt.t_servkind, 0) = 1
                 and notetext2.t_objecttype = 207 /*Договор брокерского обслуживания*/
                 and notetext2.t_documentid =
                     lpad(dlcontrmp.t_dlcontrid, 34, '0')
                 and notetext2.t_notekind = case
                       when nvl(sfcnt.t_servkindsub, 0) = 8 /*биржа*/
                        then
                        106 /*Раздел Торгового счета Депо*/
                       when nvl(sfcnt.t_servkindsub, 0) = 9 /*внебиржа*/
                        then
                        105 /*Раздел счета Депо Владельца*/
                       else
                        null
                     end
                 and notetext2.t_date <= p_EndDate
                 and notetext2.t_validtodate > p_EndDate
                left join dnotetext_dbt notetext3
                  on nvl(sfcnt.t_servkind, 0) = 1
                 and notetext3.t_objecttype = 207 /*Договор брокерского обслуживания*/
                 and notetext3.t_documentid =
                     lpad(dlcontrmp.t_dlcontrid, 34, '0')
                 and notetext3.t_notekind = 107
                 and -- счет по 3-х стороннему договору
                     notetext3.t_date <= p_EndDate
                 and notetext3.t_validtodate > p_EndDate
                left join dnotetext_dbt notetext4
                  on nvl(sfcnt.t_servkind, 0) = 1
                 and notetext4.t_objecttype = 207 /*Договор брокерского обслуживания*/
                 and notetext4.t_documentid =
                     lpad(dlcontrmp.t_dlcontrid, 34, '0')
                 and notetext4.t_notekind = 108
                 and -- раздел по 3-х стороннему договору
                     notetext4.t_date <= p_EndDate
                 and notetext4.t_validtodate > p_EndDate
               where dlcontrmp.t_sfcontrid = p_ContrID),
             case
               when nvl(sfcontr.t_servkindsub, 0) = 9 then
                'N'
               else
                chr(0)
             end,
             case
               when sfcontr.t_PartyID = 114800 then
                NVL(sfcontr.t_Number, CHR(1))
               else
                NVL(sfcontr_dl.t_Number, CHR(1))
             end FullContrNumber -- Golovkin
        from dparty_dbt party
        left join dsfcontr_dbt sfcontr
          on sfcontr.t_PartyID = party.t_PartyID
         and sfcontr.t_ID = p_ContrID
        left join dpersn_dbt persn
          on persn.t_personid = party.t_partyid
        left join ddlcontrmp_dbt dlcontrmp
          on dlcontrmp.t_sfcontrid = sfcontr.t_id
        left join ddlcontr_dbt dlcontr
          on dlcontr.t_dlcontrid = dlcontrmp.t_dlcontrid
        left join dsfcontr_dbt sfcontr_dl
          on sfcontr_dl.t_id = dlcontr.t_sfcontrid
       where party.t_PartyID = p_ClientID;
    -- insert into DBRKREP_CONTR_U_TMP_dbt select * from DBRKREP_CONTR_U_TMP where t_clientid = p_ClientID;
  end;

  --Получить цену поручения
  FUNCTION GetReqPrice(p_DealID IN NUMBER) RETURN FLOAT AS
  
    v_Price FLOAT;
  BEGIN
    /*
      SELECT (CASE WHEN req.t_PriceType = 2 THEN RSI_RSB_FIInstr.ConvSum( (req.t_Price * RSI_RSB_FIInstr.FI_GetNominalOnDate(req.t_FIID, req.t_Date) / 100.0), fin.t_FaceValueFI, req.t_PriceFIID, req.t_Date)
                   ELSE req.t_Price END
             )
    */
    SELECT DISTINCT req.t_Price --PNV 534684
      INTO v_Price
      FROM ddvdeal_dbt   tk,
           dspground_dbt ground,
           dspgrdoc_dbt  dealdoc,
           dspgrdoc_dbt  reqdoc,
           ddl_req_dbt   req,
           dfininstr_dbt fin
     WHERE tk.t_ID = p_DealID
       AND dealdoc.t_sourcedocid = tk.t_ID
       AND dealdoc.t_sourcedockind = 192
       AND ground.t_spgroundid = dealdoc.t_spgroundid
       AND ground.t_spgroundid = reqdoc.t_spgroundid
       AND dealdoc.t_sourcedocid != reqdoc.t_sourcedocid
       AND dealdoc.t_sourcedockind != reqdoc.t_sourcedockind
       AND reqdoc.t_sourcedocid = req.t_id
       AND reqdoc.t_sourcedockind = req.t_kind
       AND fin.t_FIID = req.t_FIID;
  
    RETURN v_Price;
  
  EXCEPTION
    WHEN OTHERS /*NO_DATA_FOUND*/
     THEN
      RETURN NULL;
    
  END GetReqPrice;

  -- Договор доступен для отбора
  -- пока непонятно как использовать
  function IsValidClientContr(p_ClientID      in number,
                              p_ContrID       in number,
                              p_IsFiMovement  in number,
                              p_IsNotZeroRest in number) return boolean is
    cnt integer;
    res boolean := false;
  begin
    if ((p_IsFiMovement != 0) or (p_IsNotZeroRest != 0)) then
      if (p_IsFiMovement != 0) then
        select count(1)
          into cnt
          from dbrkrep_contr_acc_u_tmp
         where t_contrid = p_ContrID
           and t_clientid = p_ClientID
           and (t_creditac != 0 or t_debetac != 0);
        if (cnt = 0) then
          res := true;
        end if;
      end if;
      if (p_IsNotZeroRest != 0) then
        select count(1)
          into cnt
          from dbrkrep_contr_acc_u_tmp
         where t_contrid = p_ContrID
           and t_clientid = p_ClientID
           and (t_restin != 0 or t_restout != 0);
        if (cnt > 0) then
          res := true;
        end if;
      end if;
    else
      return res;
    end if;
    return false;
  end;

  -- Формирование данных по договору фондового рынка
  procedure CreateDealData_MM(p_ClientID      in number,
                              p_ContrID       in number,
                              p_BegDate       in date,
                              p_EndDate       in date,
                              p_ByExchange    in number,
                              p_ByOutExchange in number,
                              p_IsFiMovement  in number,
                              p_IsNotZeroRest in number) is
  begin
    --dbms_output.put_line('!!1='||to_char(sysdate, 'mi:ss'));
    RSB_BRKREP_u.SetUsingContr(p_ClientID, p_ContrID, p_BegDate, p_EndDate);
    --dbms_output.put_line('!!2='||to_char(sysdate, 'mi:ss'));
    RSB_BRKREP_u.CreateDealData(p_ClientID,
                                p_ContrID,
                                0 /*сразу по всем*/,
                                p_BegDate,
                                p_EndDate,
                                p_ByExchange,
                                p_ByOutExchange);
    --dbms_output.put_line('!!3='||to_char(sysdate, 'mi:ss'));
    RSB_BRKREP_u.CreateCompData(p_ClientID,
                                p_ContrID,
                                5,
                                p_BegDate,
                                p_EndDate,
                                p_ByExchange,
                                p_ByOutExchange);
    RSB_BRKREP_u.CreateInAccData(p_ClientID,
                                 p_ContrID,
                                 p_BegDate,
                                 p_EndDate);
    RSB_BRKREP_u.CorrectInAccData(p_ClientID,
                                  p_ContrID,
                                  p_BegDate,
                                  p_EndDate);
    --dbms_output.put_line('!!4='||to_char(sysdate, 'mi:ss'));
    RSB_BRKREP_u.CreatePoolData(p_ClientID,
                                p_ContrID,
                                p_BegDate,
                                p_EndDate);
    --dbms_output.put_line('!!5='||to_char(sysdate, 'mi:ss'));
  
    -- insert into DBRKREPDEAL_TMP_dbt select * from DBRKREPDEAL_TMP where t_clientid = p_ClientID;
    -- insert into DBRKREPINACC_TMP_dbt select * from DBRKREPINACC_TMP where t_clientid = p_ClientID;
    -- insert into DBRKREPPOOL_TMP_dbt select * from DBRKREPPOOL_TMP where t_clientid = p_ClientID;
    -- insert into DBRKREPDEAL_U_TMP_dbt select * from dbrkrepdeal_u_tmp where t_clientid = p_ClientID;
    --dbms_output.put_line('!!6='||to_char(sysdate, 'mi:ss'));
    -- Добавлеяем договоры для печати
    for c in (select distinct t_contrid
                from dbrkrepdeal_u_tmp brkrepdeal
                join dsfcontr_dbt sfcontr
                  on sfcontr.t_id = brkrepdeal.t_contrid
                 and -- Golovkin 31.05.2019 ID : 487446
                     sfcontr.t_servkind = rsi_npto.PTSK_STOCKDL
               where brkrepdeal.t_clientid = p_ClientID
                    --and t_contrid = p_ContrID -- Golovkin 31.05.2019 ID : 487446
                 and t_part != 0
                    -- and brkrepdeal.t_a05_d >= p_BegDate -- Заключенные в этот период сделки
                    -- and brkrepdeal.t_a05_d <= p_EndDate
                 and ((brkrepdeal.t_a05_d >= p_BegDate -- Заключенные в этот период сделки
                     and brkrepdeal.t_a05_d <= p_EndDate) or
                     (brkrepdeal.t_a05_d <= p_EndDate and
                     brkrepdeal.t_a06 > p_EndDate)) /*CHVA 513805*/
              /*CHVA не учитываются договора, по которым были заключены сделки но не исполнены на дату отчета, 
              при этом у них 0 движение по счетам 306 и остатки на них, также и по счетам внутреннего учета (t_a56 = 0 и t_a57 =0) */
              union
              select distinct brkrep_contr_acc_u.t_contrid t_contrid
                from dbrkrep_contr_acc_u_tmp brkrep_contr_acc_u
                join dsfcontr_dbt sfcontr
                  on sfcontr.t_id = brkrep_contr_acc_u.t_contrid
                 and sfcontr.t_servkind = rsi_npto.PTSK_STOCKDL
               where brkrep_contr_acc_u.t_clientid = p_ClientID
                 and (brkrep_contr_acc_u.t_creditac != 0 or
                     brkrep_contr_acc_u.t_debetac != 0)
              /*
               union
               select distinct brkreppool_u.t_contrid t_contrid
               from dbrkreppool_u_tmp brkreppool_u
               join dsfcontr_dbt sfcontr on sfcontr.t_id = brkreppool_u.t_contrid and
               sfcontr.t_servkind = rsi_npto.PTSK_STOCKDL
               where brkreppool_u.t_clientid = p_ClientID and
               (brkreppool_u.t_a75 != 0 or brkreppool_u.t_a78 != 0)
              */
              union
              select distinct brkrepinacc_u.t_contrid t_contrid
                from dbrkrepinacc_u_tmp brkrepinacc_u
                join dsfcontr_dbt sfcontr
                  on sfcontr.t_id = brkrepinacc_u.t_contrid
                 and sfcontr.t_servkind = rsi_npto.PTSK_STOCKDL
               where brkrepinacc_u.t_clientid = p_ClientID
                 and (brkrepinacc_u.t_a56 != 0 or brkrepinacc_u.t_a57 != 0)
              union /*CHVA включено формирование отчетов по договорам у которых указана категория формировать нулевой отчет*/
              select sfcontr.t_id t_contrid
                from dsfcontr_dbt sfcontr, ddlcontrmp_dbt contrmp
               where contrmp.t_sfcontrid = sfcontr.t_id
                 and sfcontr.t_servkind = 1
                 and sfcontr.t_servkindsub = 8
                 and sfcontr.t_datebegin < p_BegDate
                 and (sfcontr.t_dateclose =
                     to_date('01.01.0001', 'dd.mm.yyyy') OR
                     sfcontr.t_dateclose > p_EndDate)
                 and contrmp.t_dlcontrid IN
                     (select contr.t_dlcontrid
                        from dobjatcor_dbt att, ddlcontr_dbt contr
                       where lpad(contr.t_dlcontrid, 34, '0') = att.t_object
                         and att.t_objecttype = 207
                         and att.t_groupid = 150
                         and att.t_attrid = 1)
                 and sfcontr.t_partyid = p_ClientID
                 and p_ContrID = 0
              /*CHVA*/
              union
              select p_ContrID t_contrid
                from dual
               where p_ContrID > 0) loop
      InsertContr(p_ClientID, c.t_ContrID, p_BegDate, p_EndDate);
      --InsertLog(p_ClientID||' '||c.t_ContrID||' '||p_BegDate||' '||p_EndDate);
    end loop;
  
    --dbms_output.put_line('!!7='||to_char(sysdate, 'mi:ss'));
  end;

  -- Формирование данных по договору срочного рынка
  procedure CreateDealData_FM(p_ClientID      in number,
                              p_ContrID       in number,
                              p_BegDate       in date,
                              p_EndDate       in date,
                              p_ByExchange    in number,
                              p_ByOutExchange in number) is
    type type_brkrepdeal_u_fm1 is table of dbrkrepdeal_u_fm1_tmp%rowtype;
    coll_brkrepdeal_u_fm1 type_brkrepdeal_u_fm1 := type_brkrepdeal_u_fm1();
    type type_brkrepdeal_u_fm2_1 is table of dbrkrepdeal_u_fm2_1_tmp%rowtype;
    coll_brkrepdeal_u_fm2_1 type_brkrepdeal_u_fm2_1 := type_brkrepdeal_u_fm2_1();
    type type_brkrepdeal_u_fm2_2 is table of dbrkrepdeal_u_fm2_2_tmp%rowtype;
    coll_brkrepdeal_u_fm2_2 type_brkrepdeal_u_fm2_2 := type_brkrepdeal_u_fm2_2();
    type type_brkrepdeal_u_fm3 is table of dbrkrepdeal_u_fm3_tmp%rowtype;
    coll_brkrepdeal_u_fm3 type_brkrepdeal_u_fm3 := type_brkrepdeal_u_fm3();
    --i number;
  begin
  
    -- Только для биржевых
    if (p_ByExchange = 0) then
      return;
    end if;
  
    -- I. ИНФОРМАЦИЯ О СДЕЛКАХ
    for c in (select sfcontr.t_id t_sfcontrid,
                     dvdeal.t_id t_dealid,
                     case
                       when (nvl(oproper.t_end_date, to_date('01.01.0001', 'dd.mm.yyyy')) = to_date('01.01.0001', 'dd.mm.yyyy') or
                             nvl(oproper.t_end_date, to_date('01.01.0001', 'dd.mm.yyyy')) > p_EndDate) then
                        0
                       else
                        1
                     end t_dealisfinish,
                     dvdeal.t_extcode t_code,
                     --dvdeal.t_date t_date,
                     nvl(SpGround.t_registrdate, dvdeal.t_date) t_date,
                     dvdeal.t_time t_time,
                     case
                       when fininstr.t_avoirkind =
                            rsb_derivatives.DV_DERIVATIVE_OPTION then
                        case
                          when fideriv.t_optiontype is null or
                               fideriv.t_optiontype not in (1, 2) then
                           nvl(avrkinds.t_name, chr(1))
                          when fideriv.t_optiontype = 1 then
                           nvl(avrkinds.t_name, chr(1)) || ' - ' || 'Put'
                          else
                           nvl(avrkinds.t_name, chr(1)) || ' - ' || 'Call'
                        end
                       else
                        nvl(avrkinds.t_name, chr(1))
                     end t_contrname,
                     nvl(fininstr.t_name, chr(1)) t_contr,
                     case
                       when oprkoper.t_systypes is null then
                        chr(1)
                       when oprkoper.t_systypes like '%B%' then
                        'Покупка'
                       else
                        'Продажа'
                     end t_name,
                     case
                       when party.t_shortname is null then
                        chr(1)
                       when party.t_shortname = 'ММВБ' then
                        'Биржа Фортс'
                       else
                        party.t_shortname
                     end t_market,
                     nvl(oproper.t_end_date,
                         to_date('01.01.0001', 'dd.mm.yyyy')) t_paydate,
                     case
                       when fininstr.t_avoirkind =
                            rsb_derivatives.DV_DERIVATIVE_FUTURES /*1*/
                        then
                        dvdeal.t_price /*dvdeal.t_cost*/
                       else
                        dvdeal.t_bonus
                     end t_cost,
                     case
                       when fininstr.t_avoirkind =
                            rsb_derivatives.DV_DERIVATIVE_OPTION /*2*/
                        then
                        dvdeal.t_price /*dvdeal.t_cost*/
                       else
                        0
                     end t_costopt,
                     case
                       when fininstr.t_avoirkind =
                            rsb_derivatives.DV_DERIVATIVE_OPTION /*2*/
                        then
                        dvdeal.t_bonus
                       else
                        0
                     end t_bonusopt,
                     dvdeal.t_amount t_amount,
                     case
                       when fininstr.t_avoirkind =
                            rsb_derivatives.DV_DERIVATIVE_FUTURES /*1*/
                        then
                        dvdeal.t_positioncost
                       else
                        dvdeal.t_positionbonus
                     end t_positioncost,
                     case
                       when regexp_like(oprkoper.t_systypes, '[B]') then
                        -1
                       else
                        1
                     end t_bonussign,
                     nvl(fininstr.t_issuer, -1) t_issuer,
                     case when RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_OPER_DV, LPAD(dvdeal.t_id, 34, '0'), 116, dvdeal.t_Date) = 1 then 'X' else CHR(0) end AS t_MarginCall
                from ddvdeal_dbt dvdeal
                join dsfcontr_dbt sfcontr
                  on sfcontr.t_ServKind = rsi_npto.PTSK_DV /*15 Срочные контракты (ФИССИКО)*/
                 and sfcontr.t_partyid = p_ClientID
                 and (sfcontr.t_DateClose =
                     to_date('01.01.0001', 'dd.mm.yyyy') or
                     sfcontr.t_DateClose >= p_BegDate)
                 and (p_ContrID = 0 or sfcontr.t_id = p_ContrID)
                 and dvdeal.t_clientcontr = sfcontr.t_id
                join doprkoper_dbt oprkoper
                  on oprkoper.t_kind_operation = dvdeal.t_kind
                 and
                    -- Фьючерс/Опцион
                     regexp_like(oprkoper.t_systypes, '[U|O]')
                 and
                    -- Покупка/Продажа
                     regexp_like(oprkoper.t_systypes, '[B|S]')
                left join dfininstr_dbt fininstr
                  on fininstr.t_fiid = dvdeal.t_fiid
                left join davrkinds_dbt avrkinds
                  on avrkinds.t_fi_kind = fininstr.t_fi_kind
                 and avrkinds.t_avoirkind = fininstr.t_avoirkind
                left join dfideriv_dbt fideriv
                  on fideriv.t_fiid = dvdeal.t_fiid
                left join dparty_dbt party
                  on party.t_partyid = fininstr.t_issuer
                left join doproper_dbt oproper
                  on oproper.t_kind_operation = dvdeal.t_kind
                 and oproper.t_documentid = lpad(dvdeal.t_id, 34, '0')
                left join Dspgrdoc_Dbt SpGrDoc
                  on SpGrDoc.T_SOURCEDOCKIND = 192
                 and SpGrDoc.T_SOURCEDOCID = DVDeal.T_ID
                left join DSpGround_Dbt SpGround
                  on SpGround.T_KIND = 251
                 and SpGround.T_SPGROUNDID = SpGrDoc.T_SPGROUNDID
               where dvdeal.t_state > 0
                 and dvdeal.t_date <= p_EndDate
                 and (nvl(oproper.t_end_date, to_date('01.01.0001', 'dd.mm.yyyy')) = to_date('01.01.0001', 'dd.mm.yyyy') or
                      nvl(oproper.t_end_date, to_date('01.01.0001', 'dd.mm.yyyy')) >= p_BegDate)) loop
      -- Добавляем значение в коллекцию
      coll_brkrepdeal_u_fm1.extend();
      -- Заполняем запись
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_clientid := p_ClientID;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_contrid := c.t_sfcontrid;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_dealid := c.t_dealid;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_dealisfinish := c.t_dealisfinish;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_code := c.t_code;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_date := c.t_date;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_time := c.t_time;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_contrname := c.t_contrname;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_contr := c.t_contr;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_name := c.t_name;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_market := c.t_market;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_paydate := c.t_paydate;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_cost := c.t_cost;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_costopt := c.t_costopt;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_bonusopt := c.t_bonusopt;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_amount := c.t_amount;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_positioncost := c.t_positioncost;
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_bonussign := c.t_bonussign;
      -- Вариационная маржа
      GetMargin(c.t_dealid,
                p_EndDate,
                coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_margin);
      -- Комиссия
      GetComiss(p_ClientID,
                c.t_dealid,
                c.t_issuer,
                coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_commark,
                coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_comclir,
                coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_combr);
    
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_reqprice := GetReqPrice(c.t_dealid);
      coll_brkrepdeal_u_fm1(coll_brkrepdeal_u_fm1.last).t_MarginCall := c.t_MarginCall;
    end loop;
    -- Сохраняем коллекцию
    forall i in coll_brkrepdeal_u_fm1.first .. coll_brkrepdeal_u_fm1.last
      insert into dbrkrepdeal_u_fm1_tmp values coll_brkrepdeal_u_fm1 (i);
    -- Чистим коллекцию
    coll_brkrepdeal_u_fm1.delete;
    -- Commit
    commit;
  
    -- II. ИНФОРМАЦИЯ ОБ ИНЫХ ОПЕРАЦИЯХ (часть 1)
    for c in (select sfcontr.t_id t_sfcontrid,
                     dvdeal.t_id t_dealid,
                     case
                       when (nvl(oproper.t_end_date, to_date('01.01.0001', 'dd.mm.yyyy')) = to_date('01.01.0001', 'dd.mm.yyyy') or
                             nvl(oproper.t_end_date, to_date('01.01.0001', 'dd.mm.yyyy')) > p_EndDate) then
                        0
                       else
                        1
                     end t_dealisfinish,
                     dvdeal.t_extcode t_code,
                     dvdeal.t_date t_date,
                     dvdeal.t_time t_time,
                     case
                       when fininstr.t_avoirkind =
                            rsb_derivatives.DV_DERIVATIVE_OPTION then
                        case
                          when fideriv.t_optiontype is null or
                               fideriv.t_optiontype not in (1, 2) then
                           nvl(avrkinds.t_name, chr(1))
                          when fideriv.t_optiontype = 1 then
                           nvl(avrkinds.t_name, chr(1)) || ' - ' || 'Put'
                          else
                           nvl(avrkinds.t_name, chr(1)) || ' - ' || 'Call'
                        end
                       else
                        nvl(avrkinds.t_name, chr(1))
                     end t_contrname,
                     nvl(fininstr.t_name, chr(1)) t_contr,
                     nvl(oprkoper.t_name, chr(1)) t_name,
                     nvl(party.t_shortname, chr(1)) t_market,
                     nvl(oproper.t_end_date,
                         to_date('01.01.0001', 'dd.mm.yyyy')) t_paydate,
                     dvdeal.t_price t_cost, --dvdeal.t_cost t_cost,
                     dvdeal.t_amount t_amount,
                     dvdeal.t_price * dvdeal.t_amount t_positioncost, --dvdeal.t_positioncost t_positioncost,
                     nvl(fininstr.t_issuer, -1) t_issuer
                from ddvdeal_dbt dvdeal
                join dsfcontr_dbt sfcontr
                  on sfcontr.t_ServKind = rsi_npto.PTSK_DV /*15 Срочные контракты (ФИССИКО)*/
                 and sfcontr.t_partyid = p_ClientID
                 and (sfcontr.t_DateClose =
                     to_date('01.01.0001', 'dd.mm.yyyy') or
                     sfcontr.t_DateClose >= p_BegDate)
                 and (p_ContrID = 0 or sfcontr.t_id = p_ContrID)
                 and dvdeal.t_clientcontr = sfcontr.t_id
                join doprkoper_dbt oprkoper
                  on oprkoper.t_kind_operation = dvdeal.t_kind
                 and
                    -- Фьючерс/Опцион
                     regexp_like(oprkoper.t_systypes, '[U|O]')
                 and
                    -- Исполнение
                     regexp_like(oprkoper.t_systypes, '[E|X]') /*CHVA*/
              -- KS 22.05.2019 введена категория на сделке Является экспирацией если стоит значение ДА то это исполнение сделки
              /* join dobjatcor_dbt objatcor on objatcor.t_objecttype = 140 and
              objatcor.t_groupid = 101 and
              objatcor.t_object = lpad(dvdeal.t_id,34,'0')*/
                left join dfininstr_dbt fininstr
                  on fininstr.t_fiid = dvdeal.t_fiid
                left join davrkinds_dbt avrkinds
                  on avrkinds.t_fi_kind = fininstr.t_fi_kind
                 and avrkinds.t_avoirkind = fininstr.t_avoirkind
                left join dfideriv_dbt fideriv
                  on fideriv.t_fiid = dvdeal.t_fiid
                left join dparty_dbt party
                  on party.t_partyid = fininstr.t_issuer
                left join doproper_dbt oproper
                  on oproper.t_kind_operation = dvdeal.t_kind
                 and oproper.t_documentid = lpad(dvdeal.t_id, 34, '0')
               where dvdeal.t_state > 0
                 and dvdeal.t_date <= p_EndDate
                 and (nvl(oproper.t_end_date, to_date('01.01.0001', 'dd.mm.yyyy')) = to_date('01.01.0001', 'dd.mm.yyyy') or
                      nvl(oproper.t_end_date, to_date('01.01.0001', 'dd.mm.yyyy')) >= p_BegDate)) loop
      -- Добавляем значение в коллекцию
      coll_brkrepdeal_u_fm2_1.extend();
      -- Заполняем запись
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_clientid := p_ClientID;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_contrid := c.t_sfcontrid;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_dealid := c.t_dealid;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_dealisfinish := c.t_dealisfinish;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_code := c.t_code;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_date := c.t_date;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_time := c.t_time;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_contrname := c.t_contrname;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_contr := c.t_contr;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_name := c.t_name;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_market := c.t_market;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_paydate := c.t_paydate;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_cost := c.t_cost;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_amount := c.t_amount;
      coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_positioncost := c.t_positioncost;
      -- Комиссия
      GetComiss(p_ClientID,
                c.t_dealid,
                c.t_issuer,
                coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_commark,
                coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_comclir,
                coll_brkrepdeal_u_fm2_1(coll_brkrepdeal_u_fm2_1.last).t_combr);
    end loop;
    -- Сохраняем коллекцию
    forall i in coll_brkrepdeal_u_fm2_1.first .. coll_brkrepdeal_u_fm2_1.last
      insert into dbrkrepdeal_u_fm2_1_tmp
      values coll_brkrepdeal_u_fm2_1
        (i);
    -- Чистим коллекцию
    coll_brkrepdeal_u_fm2_1.delete;
    -- Commit
    commit;
  
    -- II. ИНФОРМАЦИЯ ОБ ИНЫХ ОПЕРАЦИЯХ (часть 2)
    for c in (select t_sfcontrid,
                     t_code,
                     t_operdate,
                     t_opername,
                     t_sum_payer,
                     t_sum_receiver
                from (select op.t_sfcontrid t_sfcontrid,
                             op.t_code t_code,
                             op.t_operdate t_operdate,
                             op.t_opername t_opername,
                             sum(case
                                   when acctrn_deb.t_sum_payer is null then
                                    0
                                   else
                                    rsb_fiinstr.convsum(acctrn_deb.t_sum_payer,
                                                        acctrn_deb.t_fiid_payer,
                                                        0,
                                                        acctrn_deb.t_date_carry,
                                                        1)
                                 end) t_sum_payer,
                             sum(case
                                   when acctrn_cred.t_sum_receiver is null then
                                    0
                                   else
                                    rsb_fiinstr.convsum(acctrn_cred.t_sum_receiver,
                                                        acctrn_cred.t_fiid_receiver,
                                                        0,
                                                        acctrn_cred.t_date_carry,
                                                        1)
                                 end) t_sum_receiver
                        from (select sfcontr.t_id t_sfcontrid,
                                     nptxop.t_code,
                                     nptxop.t_operdate,
                                     case
                                       when lower(namealg.t_sznamealg) =
                                            'зачисление' then
                                        'Зачисление ДС'
                                       else
                                        'Списание ДС'
                                     end t_opername,
                                     nptxop.t_dockind t_dockind,
                                     nptxop.t_id t_id,
                                     nptxop.t_account t_account,
                                     nptxop.t_currency t_currency,
                                     nptxop.t_department t_department
                                from dnptxop_dbt nptxop
                                join dsfcontr_dbt sfcontr
                                  on sfcontr.t_ServKind = rsi_npto.PTSK_DV /*15 Срочные контракты (ФИССИКО)*/
                                 and sfcontr.t_partyid = p_ClientID
                                 and (sfcontr.t_DateClose =
                                     to_date('01.01.0001', 'dd.mm.yyyy') or
                                     sfcontr.t_DateClose >= p_BegDate)
                                 and (p_ContrID = 0 or
                                     sfcontr.t_id = p_ContrID)
                                 and nptxop.t_contract = sfcontr.t_id
                                join dnamealg_dbt namealg
                                  on namealg.t_itypealg = 7334
                                 and namealg.t_inumberalg =
                                     nptxop.t_subkind_operation
                                 and lower(namealg.t_sznamealg) in
                                     ('зачисление',
                                      'списание')
                               where nptxop.t_dockind = rsb_secur.DL_WRTMONEY /*4607 - Операция зачисления/списания денежных средств*/
                                 and nptxop.t_status >= 1
                                 and nptxop.t_operdate between p_BegDate and
                                     p_EndDate) op
                        join doproper_dbt oproper
                          on oproper.t_dockind = op.t_dockind
                         and oproper.t_documentid = lpad(op.t_id, 34, '0')
                        join doprdocs_dbt oprdocs
                          on oprdocs.t_id_operation = oproper.t_id_operation
                         and oprdocs.t_dockind = 1 /*Проводка*/
                        left join dacctrn_dbt acctrn_deb
                          on acctrn_deb.t_acctrnid = oprdocs.t_acctrnid
                         and acctrn_deb.t_state = 1
                         and acctrn_deb.t_chapter = 1
                         and acctrn_deb.t_account_payer = op.t_account
                         and acctrn_deb.t_fiid_payer = op.t_currency
                         and acctrn_deb.t_department = op.t_department
                        left join dacctrn_dbt acctrn_cred
                          on acctrn_cred.t_acctrnid = oprdocs.t_acctrnid
                         and acctrn_cred.t_state = 1
                         and acctrn_cred.t_chapter = 1
                         and acctrn_cred.t_account_receiver = op.t_account
                         and acctrn_cred.t_fiid_receiver = op.t_currency
                         and acctrn_cred.t_department = op.t_department
                       group by op.t_sfcontrid,
                                op.t_code,
                                op.t_operdate,
                                op.t_opername)
               where t_sum_payer != 0
                  or t_sum_receiver != 0) loop
      -- Добавляем значение в коллекцию
      coll_brkrepdeal_u_fm2_2.extend();
      -- Заполняем запись
      coll_brkrepdeal_u_fm2_2(coll_brkrepdeal_u_fm2_2.last).t_clientid := p_ClientID;
      coll_brkrepdeal_u_fm2_2(coll_brkrepdeal_u_fm2_2.last).t_contrid := c.t_sfcontrid;
      coll_brkrepdeal_u_fm2_2(coll_brkrepdeal_u_fm2_2.last).t_dealisfinish := 1;
      coll_brkrepdeal_u_fm2_2(coll_brkrepdeal_u_fm2_2.last).t_code := c.t_code;
      coll_brkrepdeal_u_fm2_2(coll_brkrepdeal_u_fm2_2.last).t_operdate := c.t_operdate;
      coll_brkrepdeal_u_fm2_2(coll_brkrepdeal_u_fm2_2.last).t_opername := c.t_opername;
      coll_brkrepdeal_u_fm2_2(coll_brkrepdeal_u_fm2_2.last).t_sum_payer := c.t_sum_payer;
      coll_brkrepdeal_u_fm2_2(coll_brkrepdeal_u_fm2_2.last).t_sum_receiver := c.t_sum_receiver;
    end loop;
    -- Сохраняем коллекцию
    forall i in coll_brkrepdeal_u_fm2_2.first .. coll_brkrepdeal_u_fm2_2.last
      insert into dbrkrepdeal_u_fm2_2_tmp
      values coll_brkrepdeal_u_fm2_2
        (i);
    -- Чистим коллекцию
    coll_brkrepdeal_u_fm2_2.delete;
    -- Commit
    commit;
  
    -- III. ДВИЖЕНИЕ СТАНДАРТНЫХ КОНТРАКТОВ
    for c in (select dvfiturn.t_sfcontrid,
                     dvfipos.t_id t_fiposid,
                     case
                       when fininstr.t_avoirkind =
                            rsb_derivatives.DV_DERIVATIVE_OPTION then
                        case
                          when fideriv.t_optiontype is null or
                               fideriv.t_optiontype not in (1, 2) then
                           nvl(avrkinds.t_name, chr(1))
                          when fideriv.t_optiontype = 1 then
                           nvl(avrkinds.t_name, chr(1)) || ' - ' || 'Put'
                          else
                           nvl(avrkinds.t_name, chr(1)) || ' - ' || 'Call'
                        end
                       else
                        nvl(avrkinds.t_name, chr(1))
                     end t_fi_name,
                     nvl(fininstr.t_name, chr(1)) t_fi_code,
                     dvfiturn.t_fiid t_fiid,
                     dvfiturn.t_department t_department,
                     dvfiturn.t_broker t_broker,
                     dvfiturn.t_genagrid t_genagrid,
                     dvfiturn.t_indate t_indate,
                     dvfiturn.t_outdate t_outdate,
                     nvl(avrkinds.t_avoirkind, 0) t_avoirkind,
                     nvl(fideriv.t_tickfiid, -1) t_tickfiid,
                     nvl(fideriv.t_strikefiid, -1) t_strikefiid
                from (select sfcontr.t_id t_sfcontrid,
                             dvfiturn.t_fiid,
                             dvfiturn.t_department,
                             dvfiturn.t_broker,
                             dvfiturn.t_clientcontr,
                             dvfiturn.t_genagrid,
                             max(case
                                   when dvfiturn.t_date < p_BegDate then
                                    dvfiturn.t_date
                                   else
                                    to_date('01.01.0001', 'dd.mm.yyyy')
                                 end) t_indate,
                             max(case
                                   when dvfiturn.t_date between p_BegDate and
                                        p_EndDate then
                                    dvfiturn.t_date
                                   else
                                    to_date('01.01.0001', 'dd.mm.yyyy')
                                 end) t_outdate
                        from ddvfiturn_dbt dvfiturn
                        join dsfcontr_dbt sfcontr
                          on sfcontr.t_ServKind = rsi_npto.PTSK_DV /*15 Срочные контракты (ФИССИКО)*/
                         and sfcontr.t_partyid = p_ClientID
                         and (sfcontr.t_DateClose =
                             to_date('01.01.0001', 'dd.mm.yyyy') or
                             sfcontr.t_DateClose >= p_BegDate)
                         and (p_ContrID = 0 or sfcontr.t_id = p_ContrID)
                         and dvfiturn.t_clientcontr = sfcontr.t_id
                      --where dvfiturn.t_date BETWEEN p_BegDate AND p_EndDate --Chesnokov 497880, 499781
                       where dvfiturn.t_date <= p_EndDate
                       group by sfcontr.t_id,
                                dvfiturn.t_fiid,
                                dvfiturn.t_department,
                                dvfiturn.t_broker,
                                dvfiturn.t_clientcontr,
                                dvfiturn.t_genagrid) dvfiturn
                join ddvfipos_dbt dvfipos
                  on dvfipos.t_fiid = dvfiturn.t_fiid
                 and dvfipos.t_department = dvfiturn.t_department
                 and dvfipos.t_broker = dvfiturn.t_broker
                 and dvfipos.t_clientcontr = dvfiturn.t_clientcontr
                 and dvfipos.t_genagrid = dvfiturn.t_genagrid
                 and (dvfipos.t_state != 2 or
                     dvfipos.t_closedate >= p_BegDate) --CHVA 502883
                left join dfininstr_dbt fininstr
                  on fininstr.t_fiid = dvfiturn.t_fiid
                left join davrkinds_dbt avrkinds
                  on avrkinds.t_fi_kind = fininstr.t_fi_kind
                 and avrkinds.t_avoirkind = fininstr.t_avoirkind
                left join dfideriv_dbt fideriv
                  on fideriv.t_fiid = dvfiturn.t_fiid) loop
      -- Добавляем значение в коллекцию
      coll_brkrepdeal_u_fm3.extend();
      -- Заполняем запись
      coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_clientid := p_ClientID;
      coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_contrid := c.t_sfcontrid;
      coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_dealisfinish := 1;
      coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_fi_name := c.t_fi_name;
      coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_fi_code := c.t_fi_code;
      -- Данные по позиции
      GetPosData(c.t_sfcontrid,
                 p_BegDate,
                 p_EndDate,
                 c.t_fiid,
                 c.t_department,
                 c.t_broker,
                 c.t_genagrid,
                 c.t_indate,
                 c.t_outdate,
                 c.t_avoirkind,
                 c.t_tickfiid,
                 c.t_strikefiid,
                 coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_inRest,
                 coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_turnPlus,
                 coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_turnMinus,
                 coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_outRest,
                 coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_inMargin,
                 coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_outMargin,
                 coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_inGuaranty,
                 coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_outGuaranty,
                 coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_Pin,
                 coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_Pout);
      --insertlog(c.t_sfcontrid||' '||c.t_fiid||' '||c.t_department||' '||c.t_broker||' '||c.t_genagrid||' '||c.t_indate||' '||c.t_outdate||' '||c.t_avoirkind||' '||c.t_tickfiid||' '||c.t_strikefiid);
    --insertlog(coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_inRest
    --||' '||coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_turnPlus
    --||' '||coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_turnMinus
    --||' '||coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_outRest
    --||' '||coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_inMargin
    --||' '||coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_outMargin
    --||' '||coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_inGuaranty
    --||' '||coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_outGuaranty
    --||' '||coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_Pin
    --||' '||coll_brkrepdeal_u_fm3(coll_brkrepdeal_u_fm3.last).t_Pout);
    end loop;
    -- Сохраняем коллекцию
    -- KS 25.04.2019 выводим только ненулевые строки
    forall i in coll_brkrepdeal_u_fm3.first .. coll_brkrepdeal_u_fm3.last
    --i := coll_brkrepdeal_u_fm3.first;
    --loop
    -- if ((coll_brkrepdeal_u_fm3(i).t_inRest!=0) or
    -- (coll_brkrepdeal_u_fm3(i).t_turnPlus!=0) or
    -- (coll_brkrepdeal_u_fm3(i).t_turnMinus!=0) or
    -- (coll_brkrepdeal_u_fm3(i).t_outRest!=0)) then
      insert into dbrkrepdeal_u_fm3_tmp values coll_brkrepdeal_u_fm3 (i);
    -- end if;
    -- exit when i = coll_brkrepdeal_u_fm3.last;
    -- i := i + 1;
    --end loop;
    -- Чистим коллекцию
    coll_brkrepdeal_u_fm3.delete;
    -- Commit
    commit;
  
    -- Данные по договорам
    for c in (select distinct t_contrid
                from dbrkrepdeal_u_fm1_tmp
               where t_clientid = p_ClientID
              union
              select distinct t_contrid
                from dbrkrepdeal_u_fm2_1_tmp
               where t_clientid = p_ClientID
              union
              select distinct t_contrid
                from dbrkrepdeal_u_fm2_2_tmp
               where t_clientid = p_ClientID
              union
              select distinct t_contrid
                from dbrkrepdeal_u_fm3_tmp
               where t_clientid = p_ClientID
              union all
              select distinct brkrep_contr_acc_u.t_contrid t_contrid
                from dbrkrep_contr_acc_u_tmp brkrep_contr_acc_u
                join dsfcontr_dbt sfcontr
                  on sfcontr.t_id = brkrep_contr_acc_u.t_contrid
                 and sfcontr.t_servkind = rsi_npto.PTSK_DV
               where brkrep_contr_acc_u.t_clientid = p_ClientID
                 and (brkrep_contr_acc_u.t_creditac != 0 or
                     brkrep_contr_acc_u.t_debetac != 0)
              union all
              select p_ContrID t_contrid
                from dual
               where p_ContrID > 0) loop
      InsertContr(p_ClientID, c.t_contrid, p_BegDate, p_EndDate);
      --InsertLog('2='||p_ClientID||' '||c.t_ContrID||' '||p_BegDate||' '||p_EndDate);
    end loop;
  end;

  -- Основая процедура сбора данных для отчета брокера
  function CreateData(p_ClientID      in number,
                      p_ContrID       in number,
                      p_BegDate       in date,
                      p_EndDate       in date,
                      p_ByExchange    in number,
                      p_ByOutExchange in number,
                      p_IsFiMovement  in number,
                      p_IsNotZeroRest in number,
                      p_ByStock       in number,
                      p_ByDv          in number,
                      errmes          in out varchar2) return number is
    objcode dobjcode_dbt.t_code%type;
  begin
    --dbms_output.put_line('!1='||to_char(sysdate, 'mi:ss'));
    -- Отбираем счета
    CreateAccounts(p_ClientID, p_ContrID, p_BegDate, p_EndDate);
    --dbms_output.put_line('!2='||to_char(sysdate, 'mi:ss'));
    -- Отбираем данные по договорам фондового дилинга
    /*CHVA*/
    if (p_ByStock = 1) then
      CreateDealData_MM(p_ClientID,
                        p_ContrID,
                        p_BegDate,
                        p_EndDate,
                        p_ByExchange,
                        p_ByOutExchange,
                        p_IsFiMovement,
                        p_IsNotZeroRest);
    end if;
    --dbms_output.put_line('!3='||to_char(sysdate, 'mi:ss'));
    -- Отбираем данные по срочным контрактам
    if (p_ByDv = 1) then
      CreateDealData_FM(p_ClientID,
                        p_ContrID,
                        p_BegDate,
                        p_EndDate,
                        p_ByExchange,
                        p_ByOutExchange);
    end if;
    /*CHVA*/
    --dbms_output.put_line('!4='||to_char(sysdate, 'mi:ss'));
    -- Управляющая компания
    begin
      select objcode.t_Code
        into objcode
        from dobjcode_dbt objcode
       where objcode.t_ObjectType = cnst.OBJTYPE_PARTY
         and objcode.t_CodeKind = 102 /*Код Диасофт*/
         and objcode.t_ObjectID = p_ClientID
         and objcode.t_State = 0;
    exception
      when others then
        objcode := chr(1);
    end;
    if (objcode = '2010005009145') then
      -- Остатки ДС
      for c in (select brkrep_contr_acc_u.t_contrid t_contrid
                  from dbrkrep_contr_acc_u_tmp brkrep_contr_acc_u
                 where brkrep_contr_acc_u.t_clientid = p_ClientID
                   and (brkrep_contr_acc_u.t_restin != 0 or
                       brkrep_contr_acc_u.t_restout != 0)
                    or exists (select 1
                          from dobjatcor_dbt
                         where t_objecttype = 207
                           and t_groupid = 150
                           and t_object =
                               (select lpad(t_dlcontrid, 34, '0')
                                  from ddlcontr_dbt
                                 where t_dlcontrid in
                                       (select t_dlcontrid
                                          from ddlcontrmp_dbt
                                         where t_sfcontrid =
                                               brkrep_contr_acc_u.t_contrid)))
                   and not exists
                 (select 1
                          from dbrkrep_contr_u_tmp brkrep_contr_u
                         where brkrep_contr_u.t_contrid =
                               brkrep_contr_acc_u.t_contrid
                           and brkrep_contr_u.t_clientid = p_ClientID)) loop
        InsertContr(p_ClientID, c.t_contrid, p_BegDate, p_EndDate);
      end loop;
      -- Остатки ЦБ
      /*
       for c in (select brkreppool_u.t_contrid
       from dbrkreppool_u_tmp brkreppool_u
       where brkreppool_u.t_clientid = p_ClientID and
       (brkreppool_u.t_a75 != 0 or brkreppool_u.t_a78 != 0) and
       not exists (select 1 from dbrkrep_contr_u_tmp brkrep_contr_u
       where brkrep_contr_u.t_contrid = brkreppool_u.t_contrid and
       brkrep_contr_u.t_clientid = p_ClientID))
       loop
       InsertContr(p_ClientID, c.t_contrid, p_BegDate, p_EndDate);
       end loop;
      */
      for c in (select brkrepinacc_u.t_contrid
                  from dbrkrepinacc_u_tmp brkrepinacc_u
                 where brkrepinacc_u.t_clientid = p_ClientID
                   and (brkrepinacc_u.t_a55 != 0 or brkrepinacc_u.t_a58 != 0)
                   and not exists
                 (select 1
                          from dbrkrep_contr_u_tmp brkrep_contr_u
                         where brkrep_contr_u.t_contrid =
                               brkrepinacc_u.t_contrid
                           and brkrep_contr_u.t_clientid = p_ClientID)) loop
        InsertContr(p_ClientID, c.t_contrid, p_BegDate, p_EndDate);
      end loop;
    end if;
  
    -- Успешное завершение
    return 1;
  exception
    when others then
      errmes := substr('Исключение: ' || sqlerrm, 1, 200);
      InsertLog(errmes);
      return 0;
  end;

end rshb_brkrep;
/
