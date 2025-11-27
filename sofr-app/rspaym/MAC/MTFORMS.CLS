Import "mtcomcls.cls", "mtfldfrm.cls";

ARRAY MT103SerializersPal, MT100SerializersPal, MT200SerializersPal,
      MT202SerializersPal, MT205SerializersPal,MT900SerializersPal,
      MT910SerializersPal,MT9XXForwardRowSerializers,MT9XXRowSerializers,
      MT9XXSerializers, MTn92Serializers, MTn95Serializers, MTn96Serializers, MTn99Serializers;


MT103SerializersPal(0) = "TMTField20SerializerString,TMTField20SerializerStruct";
/* TMTSection13CSerializerMenuAv необходимо для работы графических сериалайзеров, потому что только он содержит имя поля */
MT103SerializersPal(1) = "TSwiftFormSerializerString,TMTSection13CSerializerMenuAv,TSwiftFormSerializerStruct";
MT103SerializersPal(2) = "TMTField23SerializerString,TMTField23SerializerStruct";
/* TMTSection23ESerializerMenuAv необходимо для работы графических сериалайзеров, потому что только он содержит имя поля */
MT103SerializersPal(3) = "TSwiftFormSerializerString,TMTSection23ESerializerMenuAv";
MT103SerializersPal(4) = "TMTField26SerializerString,TMTField26SerializerStruct";
MT103SerializersPal(5) = "TMTField32SerializerString,TMTField32SerializerStruct";
MT103SerializersPal(6) = "TMTField33SerializerString,TMTField33SerializerStruct";
MT103SerializersPal(7) = "TMTField36SerializerString,TMTField36SerializerStruct";
MT103SerializersPal(8) = "TMTField50SerializerString,TMTField50SerializerStruct";
MT103SerializersPal(9) = "TMTFieldBankInstSerializerString,TMTFieldBankInstSerializerStruct";
MT103SerializersPal(10) = "TMTFieldBankInstSerializerString,TMTFieldBankInstSerializerStruct";
MT103SerializersPal(11) = "TMTFieldBankInstSerializerString,TMTFieldBankInstSerializerStruct";
MT103SerializersPal(12) = "TMTFieldBankInstSerializerString,TMTFieldBankInstSerializerStruct";
MT103SerializersPal(13) = "TMTFieldBankInstSerializerString,TMTFieldBankInstSerializerStruct";
MT103SerializersPal(14) = "TMTFieldBankInstSerializerString,TMTFieldBankInstSerializerStruct";
MT103SerializersPal(15) = "TMTFieldBankInstSerializerString,TMTFieldBankInstSerializerStruct";
MT103SerializersPal(16) = "TMTField59SerializerString,TMTField59SerializerStruct";
MT103SerializersPal(17) = "TMTField70SerializerString,TMTField70SerializerStruct";
MT103SerializersPal(18) = "TMTField71ASerializerString,TMTField71ASerializerStruct";
/* TMTSection71FSerializerMenuAv необходимо для работы графических сериалайзеров, потому что только он содержит имя поля */
MT103SerializersPal(19) = "TSwiftFormSerializerString,TMTField71FSerializerStruct,TMTSection71FSerializerMenuAv";
MT103SerializersPal(20) = "TMTField71GSerializerString,TMTField71GSerializerStruct";
MT103SerializersPal(21) = "TMTField72SerializerString,TMTField72SerializerStruct";
MT103SerializersPal(22) = "TMTField77BSerializerString";


MT100SerializersPal(0) = "TMTField20SerializerStruct,TMTField20SerializerString";
MT100SerializersPal(1) = "TMTField32SerializerStruct,TMTField32SerializerString";
MT100SerializersPal(2) = "TMTField50SerializerStruct,TMTField50SerializerString";
MT100SerializersPal(3) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT100SerializersPal(4) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT100SerializersPal(5) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT100SerializersPal(6) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT100SerializersPal(7) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT100SerializersPal(8) = "TMTField59SerializerStruct,TMTField59SerializerString";
MT100SerializersPal(9) = "TMTField70SerializerStruct,TMTField70SerializerString";
MT100SerializersPal(10)= "TMTField71ASerializerStruct,TMTField71ASerializerString";
MT100SerializersPal(11)= "TMTField72SerializerStruct,TMTField72SerializerString";

MT200SerializersPal(0) = "TMTField20SerializerStruct,TMTField20SerializerString";
MT200SerializersPal(1) = "TMTField32SerializerStruct,TMTField32SerializerString";
MT200SerializersPal(2) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT200SerializersPal(3) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT200SerializersPal(4) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT200SerializersPal(5) = "TMTField72SerializerStruct,TMTField72SerializerString";

MT202SerializersPal(asize(MT202SerializersPal)) = "TMTField20SerializerString,TMTField20SerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTField21SerializerString,TMTField21SerializerStruct";
/*MT202SerializersPal(asize(MT202SerializersPal)) = "TSwiftFormSerializerString";*/
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTField32SerializerString,TMTField32SerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTFieldBankInstSerializerString,TMTFieldBankInstSerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTFieldBankInstSerializerString,TMTFieldBankInstSerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTFieldBankInstSerializerString,TMTFieldBankInstSerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTFieldBankInstSerializerString,TMTFieldBankInstSerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTFieldBankInstSerializerString,TMTFieldBankInstSerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTFieldBankInstSerializerString,TMTFieldBankInstSerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTField72SerializerString,TMTField72SerializerStruct";
/*для MT202COV */
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTField50SerializerString,TMTField50SerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTFieldBankInstCOVSerializerString,TMTFieldBankInstSerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTFieldBankInstCOVSerializerString,TMTFieldBankInstSerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTFieldBankInstCOVSerializerString,TMTFieldBankInstSerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTField59SerializerString,TMTField59SerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTField70SerializerString,TMTField70SerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTField72COVSerializerString,TMTField72SerializerStruct";
MT202SerializersPal(asize(MT202SerializersPal)) = "TMTField33SerializerString,TMTField33SerializerStruct";

MT205SerializersPal(0) = "TMTField20SerializerStruct,TMTField20SerializerString";
MT205SerializersPal(1) = "TMTField21SerializerStruct,TMTField21SerializerString";
MT205SerializersPal(2) = "TMTField32SerializerStruct,TMTField32SerializerString";
MT205SerializersPal(3) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT205SerializersPal(4) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT205SerializersPal(5) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT205SerializersPal(6) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT205SerializersPal(7) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT205SerializersPal(8) = "TMTField72SerializerStruct,TMTField72SerializerString";

MT900SerializersPal(0) = "TMTField20SerializerStruct,TMTField20SerializerString";
MT900SerializersPal(1) = "TMTField21SerializerStruct,TMTField21SerializerString";
MT900SerializersPal(2) = "TMTField25SerializerStruct,TMTField25SerializerString";
MT900SerializersPal(3) = "TMTField13SerializerStruct,TMTField13SerializerString";
MT900SerializersPal(4) = "TMTField32SerializerStruct,TMTField32SerializerString";
MT900SerializersPal(5) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT900SerializersPal(6) = "TMTField72SerializerStruct,TMTField72SerializerString";

MT910SerializersPal(0) = "TMTField20SerializerStruct,TMTField20SerializerString";
MT910SerializersPal(1) = "TMTField21SerializerStruct,TMTField21SerializerString";
MT910SerializersPal(2) = "TMTField25SerializerStruct,TMTField25SerializerString";
MT910SerializersPal(3) = "TMTField13SerializerStruct,TMTField13SerializerString";
MT910SerializersPal(4) = "TMTField32SerializerStruct,TMTField32SerializerString";
MT910SerializersPal(5) = "TMTField50SerializerStruct,TMTField50SerializerString";
MT910SerializersPal(6) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT910SerializersPal(7) = "TMTFieldBankInstSerializerStruct,TMTFieldBankInstSerializerString";
MT910SerializersPal(8) = "TMTField72SerializerStruct,TMTField72SerializerString";


MT9XXForwardRowSerializers(ASize(MT9XXForwardRowSerializers)) = "TMTField65SerializerString";

MT9XXRowSerializers(ASize(MT9XXRowSerializers)) = "TMTField61SerializerString";
MT9XXRowSerializers(ASize(MT9XXRowSerializers)) = "TMTField86SerializerString";

MT9XXSerializers(ASize(MT9XXSerializers)) = "TMTField20SerializerString,TMTField20SerializerStruct";
MT9XXSerializers(ASize(MT9XXSerializers)) = "TMTField21SerializerString,TMTField21SerializerStruct";
MT9XXSerializers(ASize(MT9XXSerializers)) = "TMTField25SerializerString,TMTField25SerializerStruct";
MT9XXSerializers(ASize(MT9XXSerializers)) = "TMTField28SerializerString,TMTField28SerializerStruct";
MT9XXSerializers(ASize(MT9XXSerializers)) = "TMTField60SerializerString,TMTField60SerializerStruct";
MT9XXSerializers(ASize(MT9XXSerializers)) = "TSwiftFormSerializerString,TSwiftFormVIPSerializerStruct";
MT9XXSerializers(ASize(MT9XXSerializers)) = "TMTField62SerializerString,TMTField62SerializerStruct";
MT9XXSerializers(ASize(MT9XXSerializers)) = "TMTField64SerializerString,TMTField64SerializerStruct";
MT9XXSerializers(ASize(MT9XXSerializers)) = "TSwiftFormSerializerString";
MT9XXSerializers(ASize(MT9XXSerializers)) = "TMTField86SerializerString";

MTn92Serializers(ASize(MTn92Serializers)) = "TMTField20SerializerString";
MTn92Serializers(ASize(MTn92Serializers)) = "TMTField21SerializerString";
MTn92Serializers(ASize(MTn92Serializers)) = "TMTField11SerializerString";
MTn92Serializers(ASize(MTn92Serializers)) = "TMTField79SerializerString";
MTn92Serializers(ASize(MTn92Serializers)) = "TMTFieldsCopySerializerString";

MTn95Serializers(ASize(MTn95Serializers)) = "TMTField20SerializerString";
MTn95Serializers(ASize(MTn95Serializers)) = "TMTField21SerializerString";
MTn95Serializers(ASize(MTn95Serializers)) = "TMTField75SerializerString";
MTn95Serializers(ASize(MTn95Serializers)) = "TMTField77SerializerString";
MTn95Serializers(ASize(MTn95Serializers)) = "TMTField11SerializerString";
MTn95Serializers(ASize(MTn95Serializers)) = "TMTField79SerializerString";
MTn95Serializers(ASize(MTn95Serializers)) = "TMTFieldsCopySerializerString";

MTn96Serializers(ASize(MTn96Serializers)) = "TMTField20SerializerString";
MTn96Serializers(ASize(MTn96Serializers)) = "TMTField21SerializerString";
MTn96Serializers(ASize(MTn96Serializers)) = "TMTField76SerializerString";
MTn96Serializers(ASize(MTn96Serializers)) = "TMTField77SerializerString";
MTn96Serializers(ASize(MTn96Serializers)) = "TMTField11SerializerString";
MTn96Serializers(ASize(MTn96Serializers)) = "TMTField79SerializerString";
MTn96Serializers(ASize(MTn96Serializers)) = "TMTFieldsCopySerializerString";

MTn99Serializers(ASize(MTn99Serializers)) = "TMTField20SerializerString";
MTn99Serializers(ASize(MTn99Serializers)) = "TMTField21SerializerString";
MTn99Serializers(ASize(MTn99Serializers)) = "TMTField79SerializerString";



Class (TMTForm) TMTForm103(_Owner, Min, Max, _Serializer)
   InitTMTForm(_Owner, Min, Max, "MT103", _Serializer);

   TMTField20(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTRepeatableSection13C(This, NULL);
   TMTField23B(This, NULL, FIELD_MANDATORY);
   TMTRepeatableSection23E(This, NULL);
   TMTField26T(This, NULL, FIELD_OPTIONAL);
   TMTField32(This, NULL, FIELD_MANDATORY, "A", "A");
   TMTField33(This, NULL, FIELD_OPTIONAL, "B", "B");
   TMTField36(This, NULL, FIELD_OPTIONAL, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField50(This, NULL, FIELD_MANDATORY, "A", "AFK");
   TMTField51(This, NULL, FIELD_OPTIONAL, "A", "A");
   TMTField52(This, NULL, FIELD_OPTIONAL, "A", "AD");
   TMTField53(This, NULL, FIELD_OPTIONAL, "A", "ABD");
   TMTField54(This, NULL, FIELD_OPTIONAL, "A", "ABD");
   TMTField55(This, NULL, FIELD_OPTIONAL, "A", "ABD");
   TMTField56(This, NULL, FIELD_OPTIONAL, "A", "ACD");
   TMTField57(This, NULL, FIELD_OPTIONAL, "A", "ABCD");
   TMTField59(This, NULL, FIELD_MANDATORY, "A", "AF" + NO_FIELD_OPTION);
   TMTField70(This, NULL, FIELD_OPTIONAL, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField71A(This, NULL, FIELD_MANDATORY);
   TMTRepeatableSection71F(This, NULL);
   TMTField71G(This, NULL, FIELD_OPTIONAL);
   TMTField72(This, NULL, FIELD_OPTIONAL);
   TMTField77B(This, NULL, FIELD_OPTIONAL);

   Macro IsValid()
      Var Fld32A, Fld33B, Fld36, Fld23B, Fld23E, Sec23E,
          Flds = TArray(), Vals = TArray(), i = 0;
      If (NOT IsValid())
         Return FALSE;
      End;

      Fld32A = GetField("32");
      Fld33B = GetField("33");
      Fld36  = GetField("36");
/*----------------------------------- C1 -------------------------------------*/
      If (Fld33B.IsEmpty())
         If (NOT Fld36.IsEmpty())
            ShowMsg("Поле 36 недопустимо, если отсутствует поле 33В");
            Return FALSE;
         End;
      Else
         If (Fld33B.GetSubField("Code_Currency").Value == Fld32A.GetSubField("Code_Currency").Value)
            If (NOT Fld36.IsEmpty())
               ShowMsg("Поле 36 недопустимо, если совпадает код валюты в поле 32А и 33В");
               Return FALSE;
            End;
         Else
            If (Fld36.IsEmpty())
               ShowMsg("Поле 36 обязательно, если не совпадает код валюты в поле 32А и 33В");
               Return FALSE;
            End;
         End;
      End;

/*----------------------------------- C2 -------------------------------------*/

/*----------------------------------- C3 -------------------------------------*/
      Sec23E = GetField("RepeatableSection23E");
      Fld23B = GetField("23");
      Flds.Value(0) = "23EInstructionCode";
      Vals.Value(0) = "CHQB";

      If (Fld23B.GetSubField("BankOperationCode").Value == "SPRI")
         i = 0;
         While (i < Sec23E.ArrayValue.Size)
            If (Index("SDVA,TELB,PHOB,INTC", Sec23E.ArrayValue.Value(i).GetSubField("23EInstructionCode").Value) < 1)
               ShowMsg("Если поле 23В равно SPRI, то поле 23Е может содержать только SDVA, TELB, PHOB и INTC");
               Return FALSE;
            End;
            i = i + 1;
         End;
      Elif (Fld23B.GetSubField("BankOperationCode").Value == "SSTD")
         If (NOT Sec23E.IsEmpty())
            ShowMsg("Если поле 23В равно SSTD, то поле 23Е - недопустимо");
            Return FALSE;
         End;
      Elif (Fld23B.GetSubField("BankOperationCode").Value == "SPAY")
         If (NOT Sec23E.IsEmpty())
            ShowMsg("Если поле 23В равно SPAY, то поле 23Е - недопустимо");
            Return FALSE;
         End;
      End;

      If (Index("SPRI,SSTD,SPAY", Fld23B.GetSubField("BankOperationCode").Value) > 0)
         If ((GetField("53").Option == "D") AND (NOT GetField("53").IsEmpty()))
            ShowMsg("Если 23В равно SPTI,SSTD или SPAY, то в поле 53 не может быть использована опция D");
            Return FALSE;
         Elif ((GetField("53").Option == "B") AND (NOT GetField("53").IsEmpty()))
            If (Trim(GetField("53").GetSubField("PartyIdentifier")) == "")
               ShowMsg("Если 23В равно SPTI,SSTD или SPAY, то в поле 53 с опцией В должен юыть заполнен PartyIdentifier");
               Return FALSE;
            End;
         End;
         If ((GetField("54").Option == "A") AND (NOT GetField("54").IsEmpty()))
            ShowMsg("Если 23В равно SPTI,SSTD или SPAY, то в поле 54 может быть использована только опция A");
            Return FALSE;
         End;
         If ((GetField("55").Option == "A") AND (NOT GetField("55").IsEmpty()))
            ShowMsg("Если 23В равно SPTI,SSTD или SPAY, то в поле 55 может быть использована только опция A");
            Return FALSE;
         End;
         If (NOT GetField("57").IsEmpty())
            If (NOT ((GetField("57").Option == "A") OR
                     (GetField("57").Option == "C") OR
                     ((GetField("57").Option == "D") AND ((Trim(GetField("57").GetSubField("PartyIdentifier").Value) != "")))))
               ShowMsg("Если 23В равно SPTI,SSTD или SPAY, то в поле 57 может быть использована только опция A, C или D(PartyIdentifier заполнен обязательно)");
               Return FALSE;
            End;
         End;
         If (Trim(GetField("59").GetSubField("Account")) == "")
            ShowMsg("Если 23В равно SPTI,SSTD или SPAY, то в поле 59 должен быть заполнен счет");
            Return FALSE;
         End;
         If (Index("SSTD,SPAY", Fld23B.GetSubField("BankOperationCode").Value) > 0)
             If (NOT ((GetField("56").Option == "A") OR
                     ((GetField("56").Option == "C") AND (TRUE))))
               ShowMsg("Если 23В равно SSTD или SPAY, то в поле 56 возможны опции А и С");
               Return FALSE;
             End;
         Else
            If (NOT GetField("56").IsEmpty())
               ShowMsg("Если 23В равно SPRI, то поле 56 - недопустимо");
               Return FALSE;
            End;
         End;
      End;

      If (NOT GetField("55").IsEmpty())
         If (GetField("53").IsEmpty())
            ShowMsg("Если присутствует поле 55, то должно также быть и поле 53");
            Return FALSE;
         End;
         If (GetField("54").IsEmpty())
            ShowMsg("Если присутствует поле 55, то должно также быть и поле 54");
            Return FALSE;
         End;
      End;

      If (NOT GetField("56").IsEmpty())
         If (GetField("57").IsEmpty())
            ShowMsg("Если присутствует поле 56, то должно также быть и поле 57");
            Return FALSE;
         End;
      End;

      Flds.Size = Vals.Size = 0;
      Flds.Value(0) = "23EInstructionCode";
      Vals.Value(0) = "CHQB";
      If (ValType(Sec23E.GetFieldByContent(Flds, Vals)) != V_UNDEF)
         If (Trim(GetField("59").GetSubField("Account").Value) != "")
            ShowMsg("Если присутствует поле 23Е с кодом CHQB, то в поле 59 недопустимо пле СЧЕТ");
            Return FALSE;
         End;
      End;

      i = GetField("71", "A").GetSubField("Code").Value;
      If (i == "OUR")
         If (NOT GetField("RepeatableSection71F").IsEmpty())
            ShowMsg("Если 71А равно OUR, то поля 71F - недопустимы");
            Return FALSE;
         End;
      Elif (i == "SHA")
         If (NOT GetField("71", "G").IsEmpty())
            ShowMsg("Если 71А равно SHA, то поле 71G - недопустимо");
            Return FALSE;
         End;
      Elif (i == "BEN")
         If (NOT GetField("71", "G").IsEmpty())
            ShowMsg("Если 71А равно BEN, то поле 71G - недопустимо");
            Return FALSE;
         End;
         If (GetField("RepeatableSection71F").IsEmpty())
            ShowMsg("Если 71А равно BEN, то должно присутствовать хотя бы одно поле 71F");
            Return FALSE;
         End;
      End;

      If ((NOT GetField("RepeatableSection71F").IsEmpty()) OR
          (NOT GetField("71", "G").IsEmpty()))
         If (GetField("33").IsEmpty())
            ShowMsg("Если присутствует поле 71F или 71G, то поле 33В - обязательно");
            Return FALSE;
         End;
      End;

      If (GetField("56").IsEmpty())
         Flds.Size = Vals.Size = 0;
         Flds.Value(0) = "23EInstructionCode";
         Vals.Value(0) = "TELI";
         If (ValType(Sec23E.GetFieldByContent(Flds, Vals)) != V_UNDEF)
            ShowMsg("Если отсутствует поле 56, то в полях 23Е недопустимы коды TELI и PHOI");
            Return FALSE;
         End;
         Vals.Value(0) = "PHOI";
         If (ValType(Sec23E.GetFieldByContent(Flds, Vals)) != V_UNDEF)
            ShowMsg("Если отсутствует поле 56, то в полях 23Е недопустимы коды TELI и PHOI");
            Return FALSE;
         End;
      End;

      If (GetField("57").IsEmpty())
         Flds.Size = Vals.Size = 0;
         Flds.Value(0) = "23EInstructionCode";
         Vals.Value(0) = "TELE";
         If (ValType(Sec23E.GetFieldByContent(Flds, Vals)) != V_UNDEF)
            ShowMsg("Если отсутствует поле 57, то в полях 23Е недопустимы коды TELE и PHON");
            Return FALSE;
         End;
         Vals.Value(0) = "PHON";
         If (ValType(Sec23E.GetFieldByContent(Flds, Vals)) != V_UNDEF)
            ShowMsg("Если отсутствует поле 57, то в полях 23Е недопустимы коды TELE и PHON");
            Return FALSE;
         End;
      End;

      If (NOT GetField("71", "G").IsEmpty())
         If (GetField("32").GetSubField("Code_Currency").Value !=
             GetField("71", "G").GetSubField("Code_Currency").Value)
            ShowMsg("Коды валют в полях 32A и 71G должны быть одинаковы");
            Return FALSE;
         End;
      End;
      Return TRUE;
   End;

   Macro UpdateSerializers()
      Var Sec, MenuRow;
      Sec = GetField("RepeatableSection13C");
      MenuRow = 0;
      While (MenuRow < Sec.ArrayValue.Size)
        if (valtype(Sec.ArrayValue.Value(MenuRow)) != V_UNDEF)
          Sec.ArrayValue.Value(MenuRow).UnRegisterSerializers();
          Sec.ArrayValue.Value(MenuRow).RegisterSerializer("TMTField13SerializerString");
        end;
        MenuRow = MenuRow + 1;
      End;
      Sec = GetField("RepeatableSection23E");
      MenuRow = 0;
      While (MenuRow < Sec.ArrayValue.Size)
        if (valtype(Sec.ArrayValue.Value(MenuRow)) != V_UNDEF)
         Sec.ArrayValue.Value(MenuRow).UnRegisterSerializers();
         Sec.ArrayValue.Value(MenuRow).RegisterSerializer("TMTField23SerializerString");
        end;
        MenuRow = MenuRow + 1;
      End;
      Sec = GetField("RepeatableSection71F");
      MenuRow = 0;
      While (MenuRow < Sec.ArrayValue.Size)
         if (valtype(Sec.ArrayValue.Value(MenuRow)) != V_UNDEF)
          Sec.ArrayValue.Value(MenuRow).UnRegisterSerializers();
          Sec.ArrayValue.Value(MenuRow).RegisterSerializer("TMTField71FSerializerString");
         end;
         MenuRow = MenuRow + 1;
      End;
   End;
End;

Class (TMTForm103) TMTForm103Bnk(_Owner, Min, Max, _Serializer)
   Macro UpdateSerializers()
      Var Sec, MenuRow;
      Sec = GetField("RepeatableSection13C");
      MenuRow = 0;
      While (MenuRow < Sec.ArrayValue.Size)
         Sec.ArrayValue.Value(MenuRow).UnRegisterSerializers();
         Sec.ArrayValue.Value(MenuRow).RegisterSerializer("TMTField13SerializerString,TMTField13SerializerFrmScr,TMTField13SerializerDialog");
         MenuRow = MenuRow + 1;
      End;
      Sec = GetField("RepeatableSection23E");
      MenuRow = 0;
      While (MenuRow < Sec.ArrayValue.Size)
         Sec.ArrayValue.Value(MenuRow).UnRegisterSerializers();
         Sec.ArrayValue.Value(MenuRow).RegisterSerializer("TMTField23SerializerString,TMTField23SerializerFrmScr,TMTField23ESerializerDialog");
         MenuRow = MenuRow + 1;
      End;
      Sec = GetField("RepeatableSection71F");
      MenuRow = 0;
      While (MenuRow < Sec.ArrayValue.Size)
         Sec.ArrayValue.Value(MenuRow).UnRegisterSerializers();
         Sec.ArrayValue.Value(MenuRow).RegisterSerializer("TMTField71FSerializerString,TMTField71FSerializerFrmScr,TMTField71FSerializerDialog");
         MenuRow = MenuRow + 1;
      End;
   End;
   InitTMTForm103(_Owner, Min, Max, _Serializer);
End;

Class (TMTForm) TMTForm100(_Owner, Min, Max, _Serializer)
   InitTMTForm(_Owner, Min, Max, "MT100", _Serializer);

   TMTField20(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField32(This, NULL, FIELD_MANDATORY, "A", "A");
   TMTField50X(This, NULL, FIELD_MANDATORY);
   TMTField52(This, NULL, FIELD_OPTIONAL, "A", "AD");
   TMTField53(This, NULL, FIELD_OPTIONAL, "A", "ABD");
   TMTField54(This, NULL, FIELD_OPTIONAL, "A", "ABD");
   TMTField56(This, NULL, FIELD_OPTIONAL, "A", "AD");
   TMTField57(This, NULL, FIELD_OPTIONAL, "A", "ABD");
   TMTField59X(This, NULL, FIELD_MANDATORY);
   TMTField70(This, NULL, FIELD_OPTIONAL, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField71A(This, NULL, FIELD_OPTIONAL);
   TMTField72(This, NULL, FIELD_OPTIONAL);

   Macro IsValid()
      Var Fld56, Fld57;

      If (NOT IsValid())
         Return FALSE;
      End;

      Fld56 = GetField("56");
      Fld57 = GetField("57");
/*----------------------------------- C1 -------------------------------------*/
      If ((NOT Fld56.IsEmpty()) AND Fld57.IsEmpty())
         ShowMsg("Если присутстует поле 56, то должно пристутствовать и поле 57");
         Return FALSE;
      End;
/*----------------------------------- C2 -------------------------------------*/
      Return TRUE;
   End;
End;

Class (TMTForm) TMTForm200(_Owner, Min, Max, _Serializer)
   InitTMTForm(_Owner, Min, Max, "MT200", _Serializer);

   TMTField20(This, NULL, FIELD_MANDATORY,NO_FIELD_OPTION, NO_FIELD_OPTION);
   /*TMTRepeatableSection13C(This, NULL);*/
   TMTField32(This, NULL, FIELD_MANDATORY,"A", "A");
   TMTField53(This, NULL, FIELD_OPTIONAL, "B", "B");
   TMTField56(This, NULL, FIELD_OPTIONAL, "A", "AD");
   TMTField57(This, NULL, FIELD_MANDATORY,"A", "ABD");
   TMTField72(This, NULL, FIELD_OPTIONAL);

   Macro IsValid()
      Return true;
   End;
End;

Class (TMTForm) TMTForm202(_Owner, Min, Max, _Serializer)
   var IsCov = false;
   InitTMTForm(_Owner, Min, Max, "MT202", _Serializer);

   TMTField20(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField21(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   /*TMTRepeatableSection13C(This, NULL);*/
   TMTField32(This, NULL, FIELD_MANDATORY, "A", "A");
   TMTField52(This, NULL, FIELD_OPTIONAL, "A", "AD");
   TMTField53(This, NULL, FIELD_OPTIONAL, "A", "ABD");
   TMTField54(This, NULL, FIELD_OPTIONAL, "A", "ABD");
   TMTField56(This, NULL, FIELD_OPTIONAL, "A", "AD");
   TMTField57(This, NULL, FIELD_OPTIONAL, "A", "ABD");
   TMTField58(This, NULL, FIELD_MANDATORY, "A", "AD");
   TMTField72(This, NULL, FIELD_OPTIONAL);

   /*
    поля формы MT202COV. К тем из них, которые повторяют номера основных полей,
    можно обращаться только по индексу
    */
   TMTField50(This, NULL, FIELD_OPTIONAL,"A", "AFK");
   TMTField52(This, NULL, FIELD_OPTIONAL, "A", "AD");
   TMTField56(This, NULL, FIELD_OPTIONAL, "A", "ACD");
   TMTField57(This, NULL, FIELD_OPTIONAL, "A", "ABCD");
   TMTField59(This, NULL, FIELD_OPTIONAL, NO_FIELD_OPTION, "A"+ NO_FIELD_OPTION);
   TMTField70(This, NULL, FIELD_OPTIONAL, NO_FIELD_OPTION,NO_FIELD_OPTION);
   TMTField72(This, NULL, FIELD_OPTIONAL);
   TMTField33(This, NULL, FIELD_OPTIONAL, "B","B");

   MACRO SetIsCov (newVal)
     IsCov = newVal
   END;

   Macro IsValid()
      Var Fld56, Fld57;

      If (NOT IsValid())
         Return FALSE;
      End;

      Fld56 = GetField("56");
      Fld57 = GetField("57");
/*----------------------------------- C1 -------------------------------------*/
      If ((NOT Fld56.IsEmpty()) AND Fld56.IsEmpty())
         ShowMsg("Если присутстует поле 56, то должно пристутствовать и поле 57");
         Return FALSE;
      End;
/*----------------------------------- C2 -------------------------------------*/
    if (IsCov and (GetField("59").IsEmpty() OR (GetField("50").IsEmpty())))
        ShowMsg("Для МТ202COV поля 50 и 59 являются обязательными");
        Return FALSE;
    end;
    Return TRUE;
   End;
End;

Class (TMTForm) TMTForm205(_Owner, Min, Max, _Serializer)
   InitTMTForm(_Owner, Min, Max, "MT205", _Serializer);

   TMTField20(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField21(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField32(This, NULL, FIELD_MANDATORY, "A", "A");
   TMTField52(This, NULL, FIELD_MANDATORY, "A", "AD");
   TMTField53(This, NULL, FIELD_OPTIONAL, "A", "ABD");
   TMTField56(This, NULL, FIELD_OPTIONAL, "A", "AD");
   TMTField57(This, NULL, FIELD_OPTIONAL, "A", "ABD");
   TMTField58(This, NULL, FIELD_MANDATORY, "A", "AD");
   TMTField72(This, NULL, FIELD_OPTIONAL);

   Macro IsValid()
      Var Fld56, Fld57;

      If (NOT IsValid())
         Return FALSE;
      End;

      Fld56 = GetField("56");
      Fld57 = GetField("57");
/*----------------------------------- C1 -------------------------------------*/
      If ((NOT Fld56.IsEmpty()) AND Fld56.IsEmpty())
         ShowMsg("Если присутстует поле 56, то должно пристутствовать и поле 57");
         Return FALSE;
      End;
/*----------------------------------- C2 -------------------------------------*/
      Return TRUE;
   End;
End;


Class (TMTForm) TMTForm900(_Owner, Min, Max, _Serializer)
   InitTMTForm(_Owner, Min, Max, "MT900", _Serializer);

   TMTField20(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField21(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField25(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField13(This, NULL, FIELD_OPTIONAL, "D", "D");
   TMTField32(This, NULL, FIELD_MANDATORY, "A", "A");
   TMTField52(This, NULL, FIELD_OPTIONAL, "A", "AD");
   TMTField72(This, NULL, FIELD_OPTIONAL);

End;

Class (TMTForm) TMTForm910(_Owner, Min, Max, _Serializer)
   InitTMTForm(_Owner, Min, Max, "MT910", _Serializer);

   TMTField20(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField21(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField25(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField13(This, NULL, FIELD_OPTIONAL, "D", "D");
   TMTField32(This, NULL, FIELD_MANDATORY, "A", "A");
   TMTField50(This, NULL, FIELD_OPTIONAL, "A", "AFK");
   TMTField52(This, NULL, FIELD_OPTIONAL, "A", "AD");
   TMTField56(This, NULL, FIELD_OPTIONAL, "A", "AD");
   TMTField72(This, NULL, FIELD_OPTIONAL);

   Macro IsValid()
      Var Fld50, Fld52;

      If (NOT IsValid())
         Return FALSE;
      End;

      Fld50 = not GetField("50").IsEmpty();
      Fld52 = not GetField("52").IsEmpty();
/*----------------------------------- C1 -------------------------------------*/
      If ((Fld50 AND Fld52) OR            /* Оба заполнены */
          ((not Fld50) AND (not Fld52)))   /* Оба пустые */
         ShowMsg("В сообщении должно присутствовать поле 50a или поле 52a,но не оба поля вместе");
         Return FALSE;
      End;

      Return true;
   End;
End;

Class (TMTRepeatableSec) TMTRepeatableSectionMT9XXRow(_Owner, _Serializer)
   Macro AddSection()
      Var MenuRow;
      Var Sec = TMT9XXRow(This, "TSwiftFormSerializerString");
      MenuRow = 0;
      While (MenuRow < Sec.ArrayValue.Size)
         Sec.ArrayValue.Value(MenuRow).UnRegisterSerializers();
         Sec.ArrayValue.Value(MenuRow).RegisterSerializer(MT9XXRowSerializers(MenuRow));
         MenuRow = MenuRow + 1;
      End;
   End;

   InitTMTRepeatableSec(_Owner, 0, NULL, "RepeatableSectionMT9XXRow", _Serializer);
End;

Class (TMTRepeatableSec) TMTRepeatableSectionMT9XXForwardRow(_Owner, _Serializer)
   Macro AddSection()
      Var MenuRow;
      Var Sec = TMT9XXForwardRow(This, "TSwiftFormSerializerString");
      MenuRow = 0;
      While (MenuRow < Sec.ArrayValue.Size)
         Sec.ArrayValue.Value(MenuRow).UnRegisterSerializers();
         Sec.ArrayValue.Value(MenuRow).RegisterSerializer(MT9XXForwardRowSerializers(MenuRow));
         MenuRow = MenuRow + 1;
      End;
   End;

   InitTMTRepeatableSec(_Owner, 0, NULL, "RepeatableSectionMT9XXForwardRow", _Serializer);
End;

Class (TMTForm) TMTForm9XX(_Owner, Min, Max, _Serializer)
   InitTMTForm(_Owner, Min, Max, "MT9XX", _Serializer);

   TMTField20(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField21(This, NULL, FIELD_OPTIONAL, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField25(This, NULL, FIELD_MANDATORY);
   TMTField28(This, NULL, FIELD_MANDATORY);
   TMTField60(This, NULL, FIELD_MANDATORY);
   TMTRepeatableSectionMT9XXRow(This, NULL);
   TMTField62(This, NULL, FIELD_MANDATORY);
   TMTField64(This, NULL, FIELD_OPTIONAL);
   TMTRepeatableSectionMT9XXForwardRow(This, NULL);
   TMTField86(This, NULL, FIELD_OPTIONAL);

   Macro IsValid()
      Return TRUE;
   End;

   Macro UpdateSerializers()
      Var Sec, MenuRow, Counter = 0;
      Sec = GetField("RepeatableSectionMT9XXRow");
      While (Counter < Sec.ArrayValue.Size)
         Sec.ArrayValue.Value(Counter).UnRegisterSerializers();
         Sec.ArrayValue.Value(Counter).RegisterSerializer("TSwiftFormSerializerString");
         MenuRow = 0;
         While (MenuRow < Sec.ArrayValue.Value(Counter).ArrayValue.Size)
            Sec.ArrayValue.Value(Counter).ArrayValue.Value(MenuRow).UnRegisterSerializers();
            Sec.ArrayValue.Value(Counter).ArrayValue.Value(MenuRow).RegisterSerializer(MT9XXRowSerializers(MenuRow));
            MenuRow = MenuRow + 1;
         End;
         Counter = Counter + 1;
      End;

      Sec = GetField("RepeatableSectionMT9XXForwardRow");
      Counter = 0;
      While (Counter < Sec.ArrayValue.Size)
         Sec.ArrayValue.Value(Counter).UnRegisterSerializers();
         Sec.ArrayValue.Value(Counter).RegisterSerializer("TSwiftFormSerializerString");
         MenuRow = 0;
         While (MenuRow < Sec.ArrayValue.Value(Counter).ArrayValue.Size)
            Sec.ArrayValue.Value(Counter).ArrayValue.Value(MenuRow).UnRegisterSerializers();
            Sec.ArrayValue.Value(Counter).ArrayValue.Value(MenuRow).RegisterSerializer(MT9XXForwardRowSerializers(MenuRow));
            MenuRow = MenuRow + 1;
         End;
         Counter = Counter + 1;
      End;
   End;
End;

Class (TMTForm) TMTFormN92(_Owner, Min, Max,_Serializer)
   InitTMTForm(_Owner, Min, Max, "MTn92", _Serializer);
   TMTField20(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField21(This, NULL, FIELD_OPTIONAL, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField11(This, NULL, FIELD_OPTIONAL, "R", "RS");
   TMTField79(This, NULL, FIELD_OPTIONAL);
   TMTFieldsCopy(This, NULL, FIELD_OPTIONAL);

   Macro IsValid()
      var f79, ffc;
      f79 = GetField("79") ;
      ffc = GetField("FIELDSCOPY") ;
      if (not f79.IsEmpty() AND not ffc.IsEmpty());
        /* С1 */
        ShowMsg("В сообщении должно присутствовать поле либо 79 либо копия по крайней мере обязательных полей исходного сообщения, но не то и другое вместе");
        Return FALSE;
      end;
      Return IsValid();
   End;
End;

Class (TMTForm) TMTFormN95(_Owner,  Min, Max, _Serializer)
   InitTMTForm(_Owner, Min, Max,  "MTn95", _Serializer);
   TMTField20(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField21(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField75(This, NULL, FIELD_MANDATORY);
   TMTField77(This, NULL, FIELD_OPTIONAL);
   TMTField11(This, NULL, FIELD_OPTIONAL, "S", "RS");
   TMTField79(This, NULL, FIELD_OPTIONAL);
   TMTFieldsCopy(This, NULL, FIELD_OPTIONAL);


   Macro IsValid()
      var f79, ffc;

      f79 = GetField("79") ;
      ffc = GetField("FIELDSCOPY") ;
      if (not f79.IsEmpty() AND not ffc.IsEmpty());
        /* С1 */
        ShowMsg("В сообщении должно присутствовать поле либо 79 либо копия по крайней мере обязательных полей исходного сообщения, но не то и другое вместе");
        Return FALSE;
      end;
      Return IsValid();
   End;
End;


Class (TMTForm) TMTFormN96(_Owner, Min, Max,_Serializer)
   InitTMTForm(_Owner, Min, Max, "MTn96", _Serializer);
   TMTField20(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField21(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField76(This, NULL, FIELD_MANDATORY);
   TMTField77(This, NULL, FIELD_OPTIONAL);
   TMTField11(This, NULL, FIELD_OPTIONAL, "R", "RS");
   TMTField79(This, NULL, FIELD_OPTIONAL);
   TMTFieldsCopy(This, NULL, FIELD_OPTIONAL);

   Macro IsValid()
      var f79, ffc;
      f79 = GetField("79") ;
      ffc = GetField("FIELDSCOPY") ;
      if (not f79.IsEmpty() AND not ffc.IsEmpty());
        /* С1 */
        ShowMsg("В сообщении должно присутствовать поле либо 79 либо копия по крайней мере обязательных полей исходного сообщения, но не то и другое вместе");
        Return FALSE;
      end;
      Return IsValid();
   End;
End;

Class (TMTForm) TMTFormN99(_Owner, Min, Max,_Serializer)
   InitTMTForm(_Owner, Min, Max, "MTn99", _Serializer);
   TMTField20(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField21(This, NULL, FIELD_OPTIONAL, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField79(This, NULL, FIELD_MANDATORY);
End;
