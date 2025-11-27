/************************************************************************/
/*         Автоматизированная банковская система RS-Bank v5.0           */
/*                 Copyright (c) R-Style Software Lab 1998              */
/*                                                                      */
/*  Имя файла        : dlcarry.inc                                      */
/*                                                                      */
/*  Описание         : Библиотека функций для проводок          */
/*                                                                      */
/*  Программист      : Антонов А.А.                                     */
/*                                                                      */
/*  Создан           : 04.12.98                                         */
/*                                                                      */
/************************************************************************/
IMPORT  CTInter, OprInter, FIInter, InsCarryDoc, globals, mccatacf, dlmisc, PaymInter;

// главы счетов
CONST
    CHPT_BALANCE    = 1,        // балансовые счета
    CHPT_OFFBALANCE = 3,        // внебалансовые счета
    CHPT_FUTURES    = 4;        // срочные сделки


RECORD Doc(acctrn);

/************************************************************************/
/* Проводка документа                           */
/************************************************************************/
MACRO DocCarry(WORub, Chapter, FIID_deb, DbAccount, CrAccount, Amount_deb, ValDate, CarryResult, Ground, PaymentID, Method, accTrn, Shifr_Oper ,
               Number_pack, FIID_cred, Amount_cred, SumEquivalentCarry,NumberOper)
 //TEG 16.01.19 добавлена обработка параметра номера пачки/*PNV*/
 //TEG 02.05.19 Добавлена обработка параметра Номер пользователя
  var tr, PaymentObj = null;
  var stat = 0;

  if (ValType(ValDate) == V_UNDEF)     ValDate = {curdate}; end;
  if (ValType(CarryResult) == V_UNDEF) CarryResult = 1;     end;
  if (ValType(Ground) == V_UNDEF)      Ground = "";         end;

  /*PNV*/
  if( Amount_deb != null )
     if( Chapter != 5 )
       Amount_deb = money(round(Amount_deb, 2));
     end;
     if( Amount_deb < $0 )  
        return 1;
     end;
  else
    Amount_deb = $0;
  end;
  /*PNV*/

  /*PNV*/
  if (FIID_cred == null)
      FIID_cred = FIID_deb;
  end;
  if (Amount_cred == null)
      Amount_cred = Amount_deb;
  end; 
  /*PNV*/

  if(Amount_deb == $0)
     return 0;
  end;

  if (ValType(Method) == V_UNDEF) Method = PMMET_ID; end;

  if( (PaymentID != NULL) AND (PaymentID > 0) )
     PaymentObj = RSBPayment( PaymentID );
     tr = PaymentObj.MakeTransaction();
  else
     /*проводку формируем без платежа*/
     tr = RsbAccTransaction();
  end;

  if(tr == NULL)
    return 1;
  end;

  tr.Chapter         = Chapter;
  tr.Date_Carry      = ValDate;

  if( PaymentObj != null )
    tr.Number_Pack   = PaymentObj.NumberPack;
    tr.Numb_Document = PaymentObj.Number;
    //TEG 02.05.19 Ролевая модель
    tr.Oper = PaymentObj.Oper; 
  end;

  if (ValType(Number_pack) != V_UNDEF)
    tr.Number_pack=Number_pack;
  end;
  //TEG 02.05.19 Ролевая модель
  if (ValType(NumberOper) != V_UNDEF)
    tr.Oper=NumberOper;
  end;
  tr.ResultCarry     = CarryResult;
  tr.Ground          = Ground;
  tr.Department      = {OperDprt};
  tr.FIIDPayer       = FIID_deb;/*PNV*/
  tr.FIIDReceiver    = FIID_cred;/*PNV*/
  tr.SumPayer        = MoneyL(Amount_deb);/*PNV*/
  tr.SumReceiver     = MoneyL(Amount_cred);/*PNV*/
  tr.AccountPayer    = DbAccount;
  tr.AccountReceiver = CrAccount;

  /*PNV*/
  if ((SumEquivalentCarry != null) AND (SumEquivalentCarry != 0))
    tr.SumEquivalentCarry = SumEquivalentCarry;
    /*чтобы не было ядерной проводки урегулирования*/    tr.TypeDocument = "Ц";/*Исключать из процедуры переоценки*/
  end;
  /*PNV*/

  if(Chapter == 5)
    tr.SumEquivalentCarry = 0;
  end;

  if (ValType(Shifr_Oper) != V_UNDEF) 
    tr.Shifr_Oper = Shifr_Oper;
  end;

  if( not tr.Carry() )
     return 1;
  end;      

  if( NOT stat )
    SetParm( 11, tr );
  end;

  return stat;
END;

/*Мультивалютная проводка*/
MACRO DL_MultyCarry (FIID_From, Acc_From, Amount_From, 
                     FIID_To, Acc_To, Amount_To, Глава,
                     СчетДоходов, СчетРасходов, ДатаВыполнения, ДатаВалютирования,
                     Ground, PaymentID,
                     Shifr_Oper, Number_pack, NumberOper)
 //TEG 16.01.19 добавлена обработка параметра номера пачки 02.05.19 добавлена обработка параметра номер пользователя

 var tr, PaymentObj = null;

 if( Amount_From != null )
    Amount_From = money( round( Amount_From, 2 ) );
    if( Amount_From < $0 )
       return false;
    end;
 else
   Amount_From = $0;
 end;

 if( Amount_To != null )
    Amount_To = money( round( Amount_To, 2 ) );
    if( Amount_To < $0 )
       return false;
    end;
 else
   Amount_To = $0;
 end;

 if((Amount_From == $0) AND (Amount_To == $0))
    return true;
 end;

  if( (PaymentID != NULL) AND (PaymentID > 0) )
     PaymentObj = RSBPayment( PaymentID );
     tr = PaymentObj.MakeTransaction();
  else
     /*проводку формируем без платежа*/
     tr = RsbAccTransaction();
  end;

  if(tr == NULL)
    return 1;
  end;

  tr.Chapter         = Глава;
  tr.Date_Carry      = ДатаВыполнения;

  if( PaymentObj != null )
    tr.Number_Pack   = PaymentObj.NumberPack;
    tr.Numb_Document = PaymentObj.Number;
    tr.oper  = PaymentObj.oper;
  end;
//TEG если передали пачку то присвоим
  if (ValType(Number_pack) != V_UNDEF)
    tr.Number_pack=Number_pack;
  end;
//TEG 02.05.19 ролевая модель
   if (ValType(NumberOper) != V_UNDEF)
    tr.Oper=NumberOper;
   end;

  tr.ResultCarry     = 1;
  tr.Ground          = Ground;
  tr.Department      = {OperDprt};
  tr.FIIDPayer       = FIID_From;
  tr.FIIDReceiver    = FIID_To;
  tr.SumPayer        = MoneyL(Amount_From);
  tr.SumReceiver     = MoneyL(Amount_To);
  tr.AccountPayer    = Acc_From;
  tr.AccountReceiver = Acc_To;

  if (ValType(Shifr_Oper) != V_UNDEF)
    tr.Shifr_Oper = Shifr_Oper;
  end;

  if( not tr.Carry() )
     return 1;
  end;      
END;

/*Подготовить буфера к проводкам*/
MACRO DL_PrepareCarryBuffer (Buffer)
    SetBuff( Doc, Buffer );
END;



/**********************************************************
* Класс формирования проводок
* (должен быть) основан на RsbAccTransaction
**********************************************************/
CLASS /* [inspired by] (RsbAccTransaction) */ DL_AccTrans

private const
    fiid_na = -1;

private var
    //Exchange Rate Category Plus/Minus
    ERCP = TArray(22),
    ERCM = TArray(22),
    //Роли ФИ счетов доходов/расходов
    ERCProle = TArray(22),
    ERCMrole = TArray(22);

    ERCP[CHPT_BALANCE]      = "+БМаржаП,ср-ва в ин.вал.";
    ERCM[CHPT_BALANCE]      = "-БМаржаП,ср-ва в ин.вал.";
    ERCP[CHPT_OFFBALANCE]   = "ВнебалСчетКорресп";
    ERCM[CHPT_OFFBALANCE]   = "ВнебалСчетКорресп";
    ERCP[CHPT_FUTURES]      = "+Форвард, маржа";
    ERCM[CHPT_FUTURES]      = "-Форвард, маржа";

    ERCProle[CHPT_BALANCE]      = null;
    ERCMrole[CHPT_BALANCE]      = null;
    ERCProle[CHPT_OFFBALANCE]   = FIROLE_CORACC_PASSIVE;
    ERCMrole[CHPT_OFFBALANCE]   = FIROLE_CORACC_ACTIVE;
    ERCProle[CHPT_FUTURES]      = null;
    ERCMrole[CHPT_FUTURES]      = null;

var 
                                                            // ___параметры поиска счетов плательщика/получателя
    CatPayer        = "",       CatReceiver     = "",       // категории плательщика/получателя
    FDPayer         = null,     FDReceiver      = null,     // ПД плательщика/получателя
    FiRolePayer     = null,     FiRoleReceiver  = null,     // роль ФИ плательщика/получателя
    DatePayer       = null,     DateReceiver    = null,     // дата актуализации счётов
    IsOpenPayer     = MC_OPENACC_CREATE,                    // режим открытия счетов плательщика
    IsOpenReceiver  = MC_OPENACC_CREATE,                    // режим открытия счетов получателя

                                                            // ___параметры поиска счетов курсовых разниц
    ERFiRolePlus    = null,     ERFiRoleMinus   = null,     // роль ФИ счетов переоценки
    ERCatPlus       = "",       ERCatMinus      = "",       // категории счётов доходов/расходов

                                                            // ___параметры RsbAccTransaction
    AccountPayer    = "",       AccountReceiver = "",
    FIIDPayer       = fiid_na,  FIIDReceiver    = fiid_na,
    SumPayer        = $0,       SumReceiver     = $0,
    Chapter         = CHPT_BALANCE,
    Date_carry      = {curdate},
    Date_value      = null,
    Department      = null,
    ERAccountPlus   = "",       ERAccountMinus  = "",
    Ground          = "",
    Sum             = $0,
    FIID            = fiid_na,
    ResultCarry     = null,
    Shifr_oper      = "",
    PrimaryDoc      = NULL,

                                                            // ___доп. параметры
    Date_conv       = null,                                 // дата курса для конвертации
    PaymentID       = 0;                                    // ID платежа

    // Получить счёт по категории
    PRIVATE MACRO CalcAccount(Cat, FD, Account:@string, IsOpen, Cur, FIRole, Date)
    var Acc = "";

        DL_ByDefault(Date, date_carry);

        if (FD)
            Acc = MC_FindAndOpenAccountEx(Cat, FD, Date, 0, IsOpen, null, Cur, null, null, null, FIRole);

            if (Acc == "")
                return null;
            else
                Account = Acc;
                return Account;
            end;
        else
            return null;
        end;
    END; // CalcAccount()

    // Пересчитать счёт плательщика (дебет)
    MACRO CalcPayerAccount()
        DL_ByDefault(FDPayer, PrimaryDoc);

        return CalcAccount(
            CatPayer, FDPayer, @AccountPayer, IsOpenPayer, FIIDPayer, FIRolePayer, DatePayer);
    END; // CalcPayerAccount()

    // Получить счёт плательщика (дебет)
    MACRO GetPayerAccount()
        if (AccountPayer != "")
            return true;
        end;

        return CalcPayerAccount();
    END; // GetPayerAccount()

    // Пересчитать счёт получателя (кредит)
    MACRO CalcReceiverAccount()
        DL_ByDefault(FDReceiver, PrimaryDoc);

        return CalcAccount(
            CatReceiver, FDReceiver, @AccountReceiver, IsOpenReceiver, FIIDReceiver, FIRoleReceiver, DateReceiver);
    END; // CalcReceiverAccount()

    // Получить счёт получателя (кредит)
    MACRO GetReceiverAccount()
        if (AccountReceiver != "")
            return true;
        end;

        return CalcReceiverAccount();
    END; // GetReceiverAccount()

    // Конвертация
    PRIVATE MACRO DoConvert(sum_from, sum_to:@money, cur_from, cur_to)
    var s = $0, stat = 0;
        if ((cur_from == fiid_na) or (cur_to == fiid_na))
            return false;
        end;

        DL_ByDefault(date_conv, date_carry);

        stat = ConvSum(s, sum_from, date_conv, cur_from, cur_to, 0);
        if (stat)
            return false;
        end;

        sum_to = s;

        return true;
    END; // DoConvert()

    // Конвертировать сумму получателя в сумму плательщика
    MACRO ConvertToPayer()
        return DoConvert(SumReceiver, @SumPayer, FIIDReceiver, FIIDPayer);
    END; // ConvertToPayer()

    // Конвертировать сумму плательщика в сумму получателя
    MACRO ConvertToReceiver()
        return DoConvert(SumPayer, @SumReceiver, FIIDPayer, FIIDReceiver);
    END; // ConvertToPayer()

    // реализация одновалютной проводки
    PRIVATE MACRO AccTransBO()
    var stat;

        stat = DocCarry(
            null,               // WORub
            Chapter,            // Chapter
            FIID,               // FIID
            AccountPayer,       // DbAccount
            AccountReceiver,    // CrAccount
            Sum,                // Amount
            date_carry,         // ValDate
            ResultCarry,        // CarryResult
            Ground,             // Ground
            PaymentID,          // PaymentID
            null,               // Method
            null);              // TmpID

        return (stat == 0);
    END; // AccTransBO()

    // одновалютная проводка
    PRIVATE MACRO DoAccTrans()
    var stat = 0;

        if (Sum == $0)
            if   (SumPayer != $0)
                Sum = SumPayer;
            elif (SumReceiver != $0)
                Sum = SumReceiver;
            else
                return false; // no sum
            end;
        end;

        return AccTransBO();

    END; // DoAccTrans()

    // определить категории доходов/расходов
    MACRO CalcERCatByChapter()
        if (ERCatPlus == "")
            ERCatPlus = ERCP[Chapter];
            if(ERCProle[Chapter] != null)
               ERFiRolePlus = ERCProle[Chapter];
            end;
        end;

        if (ERCatMinus == "")
            ERCatMinus = ERCM[Chapter];
            if(ERCMrole[Chapter] != null)
               ERFiRoleMinus = ERCMrole[Chapter];
            end;
        end;
    END; // CalcERCatByChapter()

    // получить счёт курсовых разниц
    PRIVATE MACRO CalcERAccount(account:@string, categ, firole, reg_path)
        if (account != "")
            // счёт задан явно
        else
            if (not CalcAccount(categ, PrimaryDoc, @account, MC_OPENACC_CREATE, null, firole))
                return false;
            end;
        end;

        return true;
    END; // CalcERAccount()

    // реализация мультивалютной проводки
    PRIVATE MACRO MultyAccTransBO()
    var d_value = date_value;

        DL_ByDefault(d_value, date_carry);

        return DL_MultyCarry (
            FIIDPayer,          // FIID_From
            AccountPayer,       // Acc_From
            SumPayer,           // Amount_From
            FIIDReceiver,       // FIID_To
            AccountReceiver,    // Acc_To
            SumReceiver,        // Amount_To
            Chapter,            // Глава
            ERAccountPlus,      // СчетДоходов
            ERAccountMinus,     // СчетРасходов
            date_carry,         // ДатаВыполнения
            d_value,            // ДатаВалютирования
            Ground,             // Ground
            PaymentID);         // PaymentID

    END; // MultyAccTransBO()

    // мультивалютная проводка
    PRIVATE MACRO DoMultyAccTrans()
        if ((FIIDPayer == fiid_na) or (FIIDReceiver == fiid_na))
            return false;
        end;

        CalcERCatByChapter();

        if (not CalcERAccount(@ERAccountPlus, ERCatPlus, ERFiRolePlus))
            return false;
        end;

        if (not CalcERAccount(@ERAccountMinus, ERCatMinus, ERFiRoleMinus))
            return false;
        end;

        if ((SumPayer == $0) and (SumReceiver != $0))
            if (not ConvertToPayer())
                return false;
            end;
            Sum = SumPayer; // for consistency
        end;
        
        if ((SumReceiver == $0) and (SumPayer != $0))
            if (not ConvertToReceiver())
                return false;
            end;
        end;

        return MultyAccTransBO();

    END; // DoMultyAccTrans()

    // проводка
    MACRO Carry(status_after)
        if ((Sum == $0) and (SumPayer == $0) and (SumReceiver == $0))
            return true;
        end;

        if (Sum != $0)
            SumPayer = Sum;
        else
            Sum = SumPayer;
        end;

        if (FIID != fiid_na)
            FIIDPayer = FIID;
        else
            FIID = FIIDPayer;
        end;

        if ((FIIDReceiver == fiid_na) and (FIID != fiid_na))
            FIIDReceiver = FIID;
        end;

        if (not GetPayerAccount())
            msgbox("Ошибка при поиске/открытии счета плательщика");
            return false;
        end;

        if (not GetReceiverAccount())
            msgbox("Ошибка при поиске/открытии счета получателя");
            return false;
        end;

        if (FIIDPayer == FIIDReceiver)
            return DoAccTrans();
        else
            return DoMultyAccTrans();
        end;

    END; // Carry()
    
END; // DL_AccTrans

