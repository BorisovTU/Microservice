/*
$Name: dps_com.inc
$Module: Депозитарий
$Description: общий макрос комиссий
*/


import "dlcnst.inc", Календарь, DPInter, FIInter, cb_sql, CTInter, CurrInter, SfInter, deposerv;
import RsbDataSet;

var MacroError :integer = 0; 

const SF_BASETYPE_SUM     = 1;      /* Сумма*/
const SF_BASETYPE_QUONT   = 2;      /* Количество*/

const SYS_ANL_AINPACCOUNTING = 2; /* Аналитика "Учет Актива в Пассиве" */

/*Блокировки ценных бумг*/
const BlockNO               = OBJTYPE_DEPOACC_BLOCK_STANDART  ;  /* Нет блокировки */
const BlockZalog            = OBJTYPE_DEPOACC_BLOCK_PLEDGE ;     /* Ц/б, блокированные в залоге */
const BlockZaklad           = OBJTYPE_DEPOACC_BLOCK_INDEPOSIT ;  /* Ц/б, блокированные в закладе */
const BlockCheck            = OBJTYPE_DEPOACC_BLOCK_INCHECK ;    /* На проверке */
const BlockOnReregistration = OBJTYPE_DEPOACC_BLOCK_INREBILLING ;/* На переоформлении */
const BlockRoute            = OBJTYPE_DEPOACC_BLOCK_ROUTE ;      /* Маршрут */
const BlockSent             = OBJTYPE_DEPOACC_BLOCK_TRANSMITTED ;/* Отправлены */

const юридическое_лицо= 1;
const физическое_лицо = 2;

/*Виды отчетов*/
const DepoRepAmount     = 3;     /*номер вида отчета Выписка со счета Депо*/
const DepoRepMove       = 20;    /*номер вида отчета Выписка о состоянии счета Депо*/

private const SP_DEPOPER_INFOPER = 830;

/* повторное предоставление отчётов */
macro DopReportNum(RepType, DepoacntID, ДатаНачала, ДатаКонца, AddBasObj)
   var  splogopr = TBFile("splogopr", "R", 2);
   var  spinfopr = TBFile("spinfopr", "R", 1);
   file spgr("spground");
   file spground("spground") key 6;
   var stat;
   var ReportsNum = 0;
   var query, queryinf;
   var IsDontDub;

   queryinf = " t_docType = " + RepType;

   ClearRecord(spinfopr);
   spinfopr.AddFilter(queryinf);
   while( spinfopr.next() )

      query = " t_recId = " + spinfopr.rec.SPInfDocID
            + " and t_State = " + 3 /*SC_SPDRAFT_STATUS_CLOSE*/
            + " and t_OperState != " + DP_INVSTATUSORDER_REJECT
            + " and t_carryDate >= " + GetSQLDate(ДатаНачала)
            + " and t_carryDate <= " + GetSQLDate(ДатаКонца);

      ClearRecord(splogopr);
      splogopr.AddFilter(query);

      stat = splogopr.next();
      while(stat)
         if(spinfopr.rec.DepoAcc == DepoacntID)
            spgr.SPgroundID = splogopr.rec.Draft;
            /* кроме операций об исполнении поручений, сформированных автоматически */
            if( (GetEQ(spgr)) and 
                (spgr.SourceDocKind == SP_DEPOPER_INFOPER) and
                (spgr.SourceDocID == splogopr.rec.AutoKey) )
               spground.SourceDocKind = SP_DEPOPER_INFOPER;
               spground.SourceDocID   = splogopr.rec.AutoKey;
               spground.Direction     = EXITING;
               spground.Department    = {OperDprt};
               if((GetGE(spground)) and 
                  ((spground.SourceDocKind == SP_DEPOPER_INFOPER) and 
                   (spground.SourceDocID   == splogopr.rec.AutoKey) and
                   (spground.Direction     == EXITING) and
                   (spground.Department    == {OperDprt}) ) )
                  IsDontDub = false; 
                  ReportsNum = ReportsNum + spground.Copies;
                  if((spground.Copies) and (splogopr.rec.IsDubl != SET_CHAR))
                     ReportsNum = ReportsNum - 1; /*если не дубликат, то отчет один бесплатно*/
                     IsDontDub = true;
                  end;

                  if( valType(AddBasObj) != V_UNDEF )
                     if( (IsDontDub) and (spground.Copies-1 > 0) )
                        stat = ExecMacro2(AddBasObj, spground.Copies-1, splogopr.rec.AutoKey);
                     elif( not IsDontDub )
                        stat = ExecMacro2(AddBasObj, spground.Copies, splogopr.rec.AutoKey);
                     end;
                  end;
               end;
            end;
         end;
         if( stat )
           stat = Next(splogopr);
         end;
      end;
   end;

   return ReportsNum;
end;

/* государственная ли ц\б ? */
macro ГосЦБ(FIID)
   file avoiriss("avoiriss");
   avoiriss.FIID = FIID;
   if(GetEQ(avoiriss))
      if(avoiriss.State == SET_CHAR)
         return 1; /*государственная*/
      end;
   end;
   return 0; /*не государственная*/
end;

/*Подсчет кол-ва дней обслуживания счета депо*/
macro CalcDepoacntPay(BeginDate, EndDate, AcntOpenDate)
   if(EndDate < (AcntOpenDate+1))
     return 0; /*дата конца периода начисления меньше даты открытия счета*/
   end;    
   if(BeginDate < (AcntOpenDate+1))
     BeginDate = (AcntOpenDate+1);
   end;
   return FloatMonths(BeginDate, EndDate);  /*((EndDate-BeginDate)+1)/30.;*/
end;

/*номинал ц.б. платежа*/
macro PaymNominal(pmpaym)
   var Nomin = 0, tNom=0, NomFaceValFi=0;
   if(GetNominalValuePaym(pmpaym, tNom, NomFaceValFi) == 0)
      if(NomFaceValFi)
         if(ConvSum (Nomin, tNom, pmpaym.ValueDate, NomFaceValFi, 0) != 0)
            MsgBox("Не найден курс валюты номинала ", NomFaceValFi, " к национальной валюте на ", pmpaym.ValueDate);
            Nomin = 0;
         end;
      else 
         Nomin = tNom;
      end;
   end;
   return Nomin;
end;


/*конвертация сумм в разных валютах */
MACRO SmartConvSum( sumTo:money, sumFrom:money, sinceDate, fiidFrom, fiidTo, fiidTrans )
  var NewSum, TransSum;

  if( fiidFrom == fiidTo )  
    NewSum = sumFrom;
  elif( ConvSum( NewSum, sumFrom, sinceDate, fiidFrom, fiidTo ) != 0 )
    if( ConvSum( TransSum, sumFrom, sinceDate, fiidFrom, fiidTrans ) == 0 )
      if( ConvSum( NewSum, TransSum, sinceDate, fiidTrans, fiidTo ) != 0 )
         MsgBox( "Не могу сконвертировать сумму из ", ПолучитьКодФинИн(fiidTrans)," в ", ПолучитьКодФинИн(fiidTo) );
         return false;
      end;
    else
       MsgBox( "Не могу сконвертировать сумму из ", ПолучитьКодФинИн(fiidFrom)," в ", ПолучитьКодФинИн(fiidTrans) );
       return false;
    end;
  end;
  SetParm( 0, NewSum );
  return true;
END;

macro IsNostroAcc( ActiveAccountID, IsNostro )
  FILE depoacnt(depoacnt);
  FILE corschem(corschem) key 3;
  var cmd, account;

  SetParm( 2, false);

  cmd = DL_RSDCommand("select * from daccount_dbt where t_AccountID = ?");
  cmd.AddParam(ActiveAccountID);

  account = cmd.Execute();

  if(account.moveNext())
    depoacnt.Autokey = account.DepoRoot;
    if( GetEQ(depoacnt) )
      corschem.Account = depoacnt.BriefCode;
      corschem.FIID = -1;
      corschem.Department = {OperDprt};
      if( GetEQ(corschem) )
        if( corschem.IsNostro == "X" )
          SetParm( 2, true);
        end;
        return true;
      else
        return true;
      end;
    else
      msgbox( "Не найден счет ДЕПО с AutoKey = ", account.DepoRoot);
      return false;
    end;
  else
    msgbox( "Не найден счет c AccountID = ", ActiveAccountID );
    return false;
  end;
end;


macro CheckActiveAccount( ic, cDate, IsCashless, IsRightPlace:@bool )
  var cmd, DataSet;
  var stat = TRUE, IsNostro;
  
  IsRightPlace = false;

  cmd = DL_RSDCommand("select rsb_depo.icaccountid(?, 1, ?) as LAccountID from dual");
  cmd.AddParam(ic.InvCardID);
  cmd.AddParam(cDate);

  DataSet = cmd.Execute();
  if(DataSet.MoveNext())
    stat = IsNostroAcc( int(DataSet.LAccountID), IsNostro );
    if( ((IsCashless) AND (IsNostro)) OR (( NOT IsCashless) AND ( NOT IsNostro)) )
      IsRightPlace = true;
    end;
  end;

  return stat;
  
end;

macro InvCardRURRestforAcc( valRest, KindAcc, AccountID, FIID, cDate, IsCashless, AinPEnabled )

   var stat, mstat;
   var IKdebet,   /* Обороты по карточке по дебету */
       IKkredit,  /* Обороты по карточке по кредиту */
       IKrest,    /* Остаток на карточке на начало периода */
       IKLrest;   /* Остаток на карточке на окончание периода */
   var IsRightPlace;
   var Nominal, NomFIID;
   var Gfirst_date, Glast_date;
   var ic, cmd;

   if( cDate > {curdate} )
     cDate == {curdate};
   end;

   Gfirst_date = cDate;
   Glast_date  = cDate;

   valRest = $0;

   cmd = DL_RSDCommand(  " select ic.*, vficert.t_Issuer, vficert.t_FIID, vficert.t_FaceValueFI, "
                       + "        rsb_depo.icnominal(ic.t_InvCardID, ?) as FaceValue "
                       + "   from dinvcard_dbt ic, dv_ficert_dbt vficert "
                       + "  where vficert.t_FiCertID = ic.t_FiCertID "
                       + "    and vficert.t_FIID = ? "
                       + "    and vficert.t_Department = ? "
                       + "    and (   ( ic.t_LAccountID = ? or ic.t_HAccountID = ? )"
                       + "          or ( Exists ( select 1 from dinvchng_dbt ch where ch.t_InvCardID = ic.t_InvCardID and (ch.t_LAccountID = ? or ch.t_HAccountID = ?)) )"
                       + "        ) "
                      );
   cmd.AddParam(cDate);
   cmd.AddParam(FIID);
   cmd.AddParam({OperDprt});
   cmd.AddParam(AccountID);
   cmd.AddParam(AccountID);
   cmd.AddParam(AccountID);
   cmd.AddParam(AccountID);

   ic = cmd.Execute();

   stat = ic.MoveNext();
   while(stat)
      IKrest  = 0; 
      IKdebet = 0; 
      IKkredit= 0; 
      IKLrest = 0;

      InvCardRestAndOverturnByAcc(ic.InvCardID, AccountID, Gfirst_date, Glast_date, @IKrest, @IKdebet, @IKkredit, @IKLrest);

      if( IKLrest )
        IsRightPlace = true;

        if( AinPEnabled )
          stat = CheckActiveAccount( ic, cDate, IsCashless, @IsRightPlace );
        end;

        if( (stat) AND (IsRightPlace))
          Nominal = ic.FaceValue;
          NomFIID = ic.FaceValueFI;

          if( Nominal != $0 )
            if( NomFIID != NATCUR )
              if( NOT SmartConvSum( Nominal, Nominal, cDate, NomFIID, NATCUR, NATCUR ) )
                 stat = FALSE;
              end;
            end;
            if( stat )
              valRest = valRest + Nominal * IKLrest;
            end;
          end;
        end;
      end;
      
      if( stat )
        stat = ic.MoveNext();
      end;
   end;

   SetParm(0, valRest);
   return TRUE;
end;


/*
Получение значения настройки "Учет актива в пассиве"
*/
macro GetAinPAccState( AinPEnabled )
  var err;
  var AinPEnabledTmp;

  GetRegistryValue( "DEPO\\AINPACCOUNTING", V_BOOL, AinPEnabledTmp, err );
  if( NOT err )
    SetParm( 0, AinPEnabledTmp );
  else
    SetParm( 0, FALSE );
    msgbox("Ошибка поиска настройки \"Учет актива в пассиве\"");
  end;

  if( NOT err )
    return true;
  else
    return false;
  end;
end;


macro DefineRealRest( account, cDate, IsCashless, cRest )
  var stat = TRUE, IsNostro;

  file accvanl(accvanl) key 1;
  file accsub(accsub);

  var Select, query, DataSet;

  query =   " select NVL(SUM(rsb_account.restsa(accsub.t_AccAnaliticsID, accsub.t_SubAccountID, ? /*1*/, 1)), 0) as cRest"
          + "   from daccvanl_dbt vanl, daccsub_dbt accsub "
          + "  where vanl.t_FIID = ? /*2*/ "
          + "    and vanl.t_Chapter = 5 "
          + "    and vanl.t_Account = ? /*3*/"
          + "    and vanl.t_AnaliticsID = " + SYS_ANL_AINPACCOUNTING
          + "    and accsub.t_AccAnaliticsID = vanl.t_AccAnaliticsID "
          + "    and accsub.t_SpecialType = 0"
          + "    and Exists(SELECT ac.t_Account "
          + "                 FROM daccount_dbt ac, dcorschem_dbt cr, ddepoacnt_dbt acnt "
          + "                WHERE ac.t_Account = accsub.t_SubAccountCode"
          + "                  AND ac.t_Code_Currency = vanl.t_FIID "
          + "                  AND ac.t_Chapter = vanl.t_Chapter "
          + "                  AND acnt.t_AutoKey = ac.t_DepoRoot "
          + "                  AND cr.t_Account = acnt.t_BriefCode "
          + "                  AND cr.t_FIID = -1 "
          + "                  AND cr.t_IsNostro = 'X' "
          + "                  AND cr.t_Department = ? /*5*/)";


  Select = RSDCommand(query);


  Select.AddParam("", RSDBP_IN, cDate );   /*1*/
  Select.AddParam("", RSDBP_IN, account.Code_Currency );     /*2*/
  Select.AddParam("", RSDBP_IN, account.Account ); /*3*/
  Select.AddParam("", RSDBP_IN, {OperDprt} ); /*5*/

  Select.Execute();
  DataSet = TRsbDataSet(Select);

  cRest = $0;
  if(DataSet.MoveNext())
    cRest = DataSet.cRest;
  end;

  SetParm(3, cRest);

  return stat;
end;


macro TranferToRUR(cRest, cDate, fininstr, avoiriss, valRest )
  var stat = true;

  if( FI_IsQuoted( avoiriss.FIID, cDate ) ) /* Для котируемой определяем рыночную стоимость */
     if( SmartConvSum( valRest, cRest, cDate, fininstr.FIID, NATCUR, fininstr.FaceValueFI ) )
        SetParm(4, valRest);
     else
        stat = FALSE; 
     end;
  else                           /* Для НЕ котируемой определяем номинальную стоимость */
     valRest = money(cRest * fininstr.FaceValue);   /*по номиналу ценной бумаги*/
     if(fininstr.FaceValueFI != NATCUR) /*Не рублевая бумажка попалась*/
        if( NOT SmartConvSum( valRest, valRest, cDate, fininstr.FaceValueFI, NATCUR, NATCUR ) )
           stat = FALSE;
        end;
     end;
     if( stat )
        SetParm(4, valRest);
     end;
  end;

  return stat;
end;


macro CalculateRURCost( cRest, account, cDate, valRest, IsCashless )
  var stat = TRUE, AinPEnabled;

  record fininstr("fininstr");
  record avoiriss("avoiriss");

  if( ПолучитьФинИн( account.Code_Currency, fininstr, avoiriss ) == 0 )
     stat = GetAinPAccState( AinPEnabled );
     if( stat )
       if( Index(account.Type_Account, "I") ) /* Если режим хранения "Закрытое" */
         if( InvCardRURRestforAcc( valRest, account.Kind_Account, account.AccountID, account.Code_Currency, cDate, IsCashless, AinPEnabled ) )
            SetParm(3, valRest);
         else
            stat = FALSE; 
         end;
       else
         if( AinPEnabled ) /* Если включен актив в пассиве, определим реальный остаток, подлежащий обложению */
           stat = DefineRealRest( account, cDate, IsCashless, cRest );
         end;
         if(stat)
           stat = TranferToRUR(cRest, cDate, fininstr, avoiriss, valRest );
           if(stat)
             SetParm(3, valRest);
           end;
         end;
       end;
     end;
  else
     MsgBox("Не найдена ценная бумага счета ", account.Account);
     stat = FALSE;
  end;

  return stat;
end;

private class FIIDSmList( _FIID, _Sum )
   var FIID, Sum;

    FIID = _FIID;
    Sum  = _Sum;
end;

/* добавить очередную сумму по ц/б в список */
private macro AddToFIIDSumList( FIID, Sum, list_our : TArray )
   var i = 0, Exists = false;
   var listElem = null;
   while( i < list_our.size )
     if( list_our[i].FIID == FIID )
        list_our[i].Sum = list_our[i].Sum + Sum;
        Exists = true;
     end;
     i = i + 1;
   end;
   if( not Exists )
     listElem = FIIDSmList( FIID, Sum );
     list_our[list_our.Size] = listElem;
   end;
end;

/*подсчет СРЕДНЕЙ ПО ПЕРИОДУ стоимости ц.б. в RUR на счете депо*/
macro CalcStoringAvoir(DepoacntID, BeginDate, EndDate, AvoirFiltr, IsCashless, AddBasObj)
   var cDate, cRest=$0, tRetSum=$0, RetSum=$0, valRest=$0, stat = true;
   var query = ""; 
   var list_our = TArray(), iCount = 0;
   var account, Select;
   
   list_our.Size = 0;

   Select = RSDCommand( "select t_Code_Currency, t_Account, t_Type_Account, t_Kind_Account from daccount_dbt where t_DepoRoot = ?" );

   Select.AddParam("", RSDBP_IN, DepoacntID );

   Select.Execute();

   account = TRsbDataSet(Select);

   while(account.MoveNext())
      tRetSum = $0;
      if( ExecMacro2(AvoirFiltr, account.Code_Currency) )
         cDate = BeginDate;
         while( ( stat ) AND ( cDate <= EndDate ) ) /*цикл по дням периода*/
            cRest = RestAC(account.account, account.Code_Currency, cDate, cDate, 5); /*остатки*/
            if(cRest) /*Есть остаток - нужно конвертить в нац. валюту (!!!)*/
              stat = CalculateRURCost( cRest, account, cDate, valRest, IsCashless );
              if(stat)
                tRetSum = tRetSum + valRest;
              end;
            end;
            if( stat )
               cDate = cDate + 1;
            end;
         end;
         if( stat )
            tRetSum = tRetSum / (EndDate-BeginDate+1); /*СРЕДНЯЯ по периоду стоимость*/
         end;
         if( stat )
            RetSum = RetSum + tRetSum;
            /* запоминаем сумму по каждому ФИ */
            AddToFIIDSumList( account.Code_Currency, tRetSum, list_our );
         end;
      end;
   end;
   iCount = 0;
   stat = true;
   if( valType(AddBasObj) != V_UNDEF )
      while( (stat) and (iCount < list_our.Size) )
         stat = ExecMacro2(AddBasObj, list_our[iCount].Sum, list_our[iCount].FIID);
         iCount = iCount + 1;
      end;
   end;

   return RetSum;
end;


macro CalcPaymentAvoir( depoac_buf, pmpaym_buf, RetSum )
  var stat;

  var  accsubdc, Select, query;
  file accvanl(accvanl);
  file account("account");
  
  RetSum = $0;

  query = "select t_AccAnaliticsID from daccsubdc_dbt where t_PaymentID = ?";

  Select = RSDCommand( query );

  Select.AddParam("", RSDBP_IN, pmpaym_buf.PaymentID );

  Select.Execute();

  accsubdc = TRsbDataSet(Select);

  stat = accsubdc.MoveNext();
  if(stat) /* Если записи есть - было позиционирование */
    while( stat )
      accvanl.AccAnaliticsID = accsubdc.AccAnaliticsID;
      if(GetEQ(accvanl))
        account.Account = accvanl.Account;
        account.Chapter = accvanl.Chapter;
        account.Code_Currency = accvanl.FIID;
        if( GetEQ(account) )
          if(account.DepoRoot == depoac_buf.Root) /* Если пассивный л/с принадлежит обрабатываемому счету депо, берем эту субсумму */
            RetSum = RetSum + accsubdc.Sum; 
          end;
        else
          msgbox("Не найден лицевой счет ", accvanl.Account);
          return false;
        end;
      else
        msgbox("Не найдена аналитика субсчета");
        return false;
      end;

      stat = accsubdc.MoveNext();
      if(NOT stat)
        stat = false;
      end;
    end;
  else
    msgbox("Не найдена техническая проводка для платежа с  PaymentID = ", pmpaym_buf.PaymentID);
    return false;
  end;

  SetParm(2, RetSum);

  return true;
end;


macro CalcPaymentAvoirIndoor( depoac_buf, pmpaym_buf, RetSum )
  var stat, Nominal, NomFIID;
  var DataSet, cmd, query;
  
  RetSum = $0;

  query =   " select acc.t_DepoRoot, vficert.t_FaceValueFI,  "
          + "        rsb_depo.icnominal(ic.t_InvCardID, ?) as FaceValue "
          + "   from dspdrmove_dbt smove, dspdraft_dbt draft, dinvlist_dbt il, dinvchng_dbt ch, doproper_dbt opr, daccount_dbt acc, dv_ficert_dbt vficert, dinvcard_dbt ic "
          + "  where smove.t_PaymentID = ? "
          + "    and draft.t_AutoKey = smove.t_DraftID "
          + "    and opr.t_DocKind = draft.t_Kind "
          + "    and opr.t_DocumentID = TO_CHAR(draft.t_AutoKey) "
          + "    and il.t_PaymentID = smove.t_PaymentID "
          + "    and ic.t_InvCardID = il.t_InvCardID "
          + "    and vficert.t_FiCertID = ic.t_FiCertID "
          + "    and ch.t_InvCardID = ic.t_InvCardID "
          + "    and ch.t_ID_Operation = opr.t_ID_Operation "
          + "    and acc.t_AccountID = ch.t_HAccountID";

  cmd = DL_RSDCommand(query);
  cmd.AddParam(pmpaym_buf.ValueDate);
  cmd.AddParam(pmpaym_buf.PaymentID);

  DataSet = cmd.Execute();

  stat = DataSet.moveNext();
  if(stat) /* Если записи есть - было позиционирование */
    while( stat )
      
      if(DataSet.DepoRoot == depoac_buf.Root) /* Если пассивный л/с принадлежит обрабатываемому счету депо, берем эту субсумму */
        Nominal = DataSet.FaceValue;
        NomFIID = DataSet.FaceValueFI;
        
        if( Nominal != $0 )
          if( NomFIID != NATCUR )
            if( NOT SmartConvSum( Nominal, Nominal, pmpaym_buf.ValueDate, NomFIID, NATCUR, NATCUR ) )
               return FALSE;
            end;
          end;
          if( stat )
            RetSum = RetSum + Nominal;
          end;
        end;
      end;

      stat = DataSet.moveNext();
    end;
  else
    msgbox("Не найдена запись об изменении сертификата для платежа с PaymentID = ", pmpaym_buf.PaymentID);
    return false;
  end;

  SetParm(2, RetSum);

  return true;
end;

macro GetSpdraftByPayment(PaymentID)
   var query;
   var Select;
   var DataSet;
   
   query = "select t_DraftID from dspdrmove_dbt where t_PaymentID = ?";
   Select = RSDCommand( query );

   Select.AddParam("PaymentID", RSDBP_IN, PaymentID ); 

   Select.Execute();

   DataSet = TRsbDataSet(Select);

   if( DataSet.MoveNext() )
      return DataSet.DraftID;
   end;

   return 0;
end;

