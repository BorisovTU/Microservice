Import "mtcomcls.cls";

/*------------------------------ SufFields -----------------------------------*/
Class (TMTContainable) TMTDate(_Owner, _Value : DATE)
   InitTMTContainable(_Owner, "Date", _Value);
End;

Class (TMTContainable) TMTEntryDate(_Owner, _Value : DATE)
   InitTMTContainable(_Owner, "EntryDate", _Value);
End;

Class (TMTContainable) TMTValueDate(_Owner, _Value : DATE)
   InitTMTContainable(_Owner, "ValueDate", _Value);
End;

Class (TMTContainable) TMTDateTime(_Owner, _Value : String)
   InitTMTContainable(_Owner, "DateTime", _Value);
End;

Class (TMTContainable) TMTCodeCurrency(_Owner, _Value : String)
   InitTMTContainable(_Owner, "Code_Currency", _Value);
End;

Class (TMTContainable) TMTAmount(_Owner, _Value : double)
   InitTMTContainable(_Owner, "Amount", _Value);
End;

Class (TMTContainable) TMTReference(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "Reference", _Value);
End;

Class (TMTContainable) TMTRelatedReference(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "RelatedReference", _Value);
End;

Class (TMTContainable) TMTName(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "Name", _Value);
End;

Class (TMTContainable) TMTAddress(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "Address", _Value);
End;

Class (TMTContainable) TMTCity(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "City", _Value);
End;

Class (TMTContainable) TMTCountry(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "Country", _Value);
End;

Class (TMTContainable) TMTLocation(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "Location", _Value);
End;

Class (TMTContainable) TMTAccount(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "Account", _Value);
End;

Class (TMTContainable) TMTBIC(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "BIC", _Value);
End;

Class (TMTContainable) TMTBIC_TRIC(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "BIC_TRIC", _Value);
End;

Class (TMTContainable) TMTTimeCode(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "TimeCode", _Value);
End;

Class (TMTContainable) TMTTimeIndication(_Owner, _Value : TIME)
   InitTMTContainable(_Owner, "TimeIndication", _Value);
End;

Class (TMTContainable) TMTTimeSign(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "TimeSign", _Value);
End;

Class (TMTContainable) TMTTimeOffset(_Owner, _Value : TIME)
   InitTMTContainable(_Owner, "TimeOffset", _Value);
End;

Class (TMTContainable) TMTBankOperationCode(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "BankOperationCode", _Value);
End;

Class (TMTContainable) TMT23EInstructionCode(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "23EInstructionCode", _Value);
End;

Class (TMTContainable) TMT23EAdditionalInfo(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "23EAdditionalInfo", _Value);
End;

Class (TMTContainable) TMTTransactionTypeCode(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "TransactionTypeCode", _Value);
End;

Class (TMTContainable) TMTRate(_Owner, _Value : DOUBLE)
   InitTMTContainable(_Owner, "Rate", _Value);
End;

Class (TMTContainable) TMTPartyIdentifier(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "PartyIdentifier", _Value);
End;

Class (TMTContainable) TMTTextRow(_Owner, _Value : STRING, _Name)
   InitTMTContainable(_Owner, _Name, _Value);
End;

Class (TMTContainable) TMT71ACode(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "Code", _Value);
End;

Class (TMTContainable) TMTStatementNumber(_Owner, _Value : INTEGER)
   InitTMTContainable(_Owner, "StatementNumber", _Value);
End;

Class (TMTContainable) TMTSequenceNumber(_Owner, _Value : INTEGER)
   InitTMTContainable(_Owner, "SequenceNumber", _Value);
End;

Class (TMTContainable) TMTDCMark(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "DCMark", _Value);
End;

Class (TMTContainable) TMTDCRowMark(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "DCRowMark", _Value);
End;

Class (TMTContainable) TMTFundsCode(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "FundsCode", _Value);
End;

Class (TMTContainable) TMTTransactionTypeIdentificationCode(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "TransactionTypeIdentificationCode", _Value);
End;

Class (TMTContainable) TMTSupplementaryDetails(_Owner, _Value : STRING)
   InitTMTContainable(_Owner, "SupplementaryDetails", _Value);
End;

Class (TMTContainable) TMTDummy(_Owner)
   InitTMTContainable(_Owner, "Dummy", "");
End;

/*-------------------------------- Fields ------------------------------------*/

Class (TMTField) TMTMSField(_Owner, Min, Max, _Name, _Serializer, FT, FO, AO)
   Macro Validate()
      Var Account = GetSubField("Account");
      Var BIC = GetSubField("BIC");
      Var Location = GetSubField("Location");
      Var PI = GetSubField("PartyIdentifier");
      Var NameFld = GetSubField("Name");

      If (StrLen(AvailableOptions) == 1)
         Option = AvailableOptions;
         Return TRUE;
      End;

      If ((ValType(BIC) != V_UNDEF) AND (Trim(BIC.Value) != ""))
         Option = "A";
         Return TRUE;
      Elif ((ValType(Location) != V_UNDEF) AND (Trim(Location.Value) != ""))
         Option = "B";
         Return TRUE;
      Elif ((ValType(NameFld) != V_UNDEF) AND
            (   (Trim(GetSubField("Name").Value) != "")
            OR (Trim(GetSubField("Address").Value) != "")
            OR (Trim(GetSubField("City").Value) != "")
            OR (Trim(GetSubField("Country").Value) != "")))
         If (ValType(Account) == V_UNDEF)
            Option = "D";
            Return TRUE;
         Elif (Index(AvailableOptions, "K") != 0)
            Option = "K";
            Return TRUE;
         Else
            Option = NO_FIELD_OPTION;
            Return TRUE;
         End;
      Elif ((ValType(PI) != V_UNDEF) AND (Trim(PI.Value) != ""))
         If ((SubStr(Trim(PI.Value), 1, 2) == "//") AND
             (Index(AvailableOptions, "C") > 0))
            Option = "C";
         Else
            Option = "D";
         End;
         Return TRUE;
      Else
         Option = NO_FIELD_OPTION;
         Return TRUE;
      End;
   End;

   Macro IsValid()
      Var Account = GetSubField("Account");
      Var BIC = GetSubField("BIC");
      Var Location = GetSubField("Location");
      Var PI = GetSubField("PartyIdentifier");
      Var NameFld = GetSubField("Name");
      Var AccSubField = NULL, CSInd, CSIndNext, TmpStr, CheckInn= false, TmpFld = null;

      If (IsEmpty())
         Return IsValid();
      End;

      If ((ValType(BIC) != V_UNDEF) AND (BIC.Value == FAKE_BIC))
         ShowMsg("В поле " + This.Name + " не заполнен BIC (заполнен фиктивным значением)");
         Return FALSE;
      End;

      If ((ValType(BIC) != V_UNDEF) AND (NOT BIC.IsEmpty()))
        if(IsBank)
         SerialBIC.BIC_Key = BIC.Value;
         If (NOT GetEQ(SerialBIC))
            ShowMsg("БИК " + BIC.Value + " отсутствует в справочнике банков");
            Return FALSE;
         Elif (IsBank AND SerialBIC.Lock != "")
            ShowMsg("БИК " + BIC.Value + " заблокирован в справочнике банков");
            Return FALSE;
         End;
        end;
      End;

      AccSubField = GetSubField("Account");
      If (ValType(AccSubField) == V_UNDEF)
         AccSubField = GetSubField("PartyIdentifier");
      End;
      TmpFld = Owner.GetField("32","A");
      if (not	 TmpFld.IsEmpty())
        TmpFld = TmpFld.GetSubfield("CodeCurrency");
        if (valtype (TmpFld) == V_GENOBJ)
          if((TmpFld.GetSubfield("CodeCurrency").Value == "RUB") OR
             (TmpFld.GetSubfield("CodeCurrency").Value == "RUR") )
            CheckInn = true;
          end;
        end;
      end;
      If ((Name == "59") AND CheckInn)
         If ((SubStr(NameFld.Value, 1, 3) == "INN") OR
             (SubStr(NameFld.Value, 1, 3) == "ИНН") )
            TmpStr = trim(SubStr(NameFld.Value, 4));
            CSInd = Index (TmpStr, ".");
/* AB 30.08.10  отрезаем КПП, если есть */
            if (CSInd)
              TMpStr = SubStr (TmpStr, 1, CSInd - 1);
            end;
            If (IsDigitStr(TmpStr) AND
                (TmpStr!="0") AND (StrLen(TmpStr) != 10) AND (StrLen(TmpStr) != 12))
               ShowMsg("ИНН должен состоять из 10 или 12 цифр");
               Return FALSE;
            End;
         End;
      End;

      If (ValType(AccSubField) != V_UNDEF)
         If (SubStr(AccSubField.Value, 1, 2) == "//")
            If (Option == "A")
               CSInd = GetValueInArrayIndex(StrUpr(SubStr(AccSubField.Value, 3, 2)), OptionA_NCSC);
               If (CSInd != -1)
                  AccSubField.Value = SubStr(AccSubField.Value, 1, 2) +
                                      StrUpr(SubStr(AccSubField.Value, 3, 2)) +
                                      SubStr(AccSubField.Value, 5);
                  If (NOT CheckMask(SubStr(AccSubField.Value, 5), OptionA_NCSC_Format(CSInd)))
                     CSIndNext = GetValueInArrayIndex(StrUpr(SubStr(AccSubField.Value, 3, 2)), OptionA_NCSC, CSInd + 1);
                     If (CSIndNext != -1)
                        If (NOT CheckMask(SubStr(AccSubField.Value, 5), OptionA_NCSC_Format(CSIndNext)))
                           ShowMsg("Поле Party Identifier в поле " + This.Name + " должно подпадать под такую маску " + OptionA_NCSC_Format(CSInd) + "|или " + OptionA_NCSC_Format(CSIndNext));
                           Return FALSE;
                        End;
                     Else
                        ShowMsg("Поле Party Identifier в поле " + This.Name + " должно подпадать под такую маску " + OptionA_NCSC_Format(CSInd));
                        Return FALSE;
                     End;
                  End;
               End;
            Elif ((Option == "D") OR (Option == "C"))
               CSInd = GetValueInArrayIndex(StrUpr(SubStr(AccSubField.Value, 3, 2)), OptionD_NCSC);
               If (CSInd != -1)
                  AccSubField.Value = SubStr(AccSubField.Value, 1, 2) +
                                      StrUpr(SubStr(AccSubField.Value, 3, 2)) +
                                      SubStr(AccSubField.Value, 5);
                  If (NOT CheckMask(SubStr(AccSubField.Value, 5), OptionD_NCSC_Format(CSInd)))
                     CSIndNext = GetValueInArrayIndex(StrUpr(SubStr(AccSubField.Value, 3, 2)), OptionD_NCSC, CSInd + 1);
                     If (CSIndNext != -1)
                        If (NOT CheckMask(SubStr(AccSubField.Value, 5), OptionD_NCSC_Format(CSIndNext)))
                           ShowMsg("Поле Party Identifier в поле " + This.Name + " должно подпадать под такую маску " + OptionD_NCSC_Format(CSInd) + "|или " + OptionD_NCSC_Format(CSIndNext));
                           Return FALSE;
                        End;
                     Else
                        ShowMsg("Поле Party Identifier в поле " + This.Name + " должно подпадать под такую маску " + OptionD_NCSC_Format(CSInd));
                        Return FALSE;
                     End;
                  End;
               End;
            End;
/*25.05.2015  voikin
            If (StrUpr(SubStr(AccSubField.Value, 3, 2)) == RussiaCountryCode)
               If (StrLen(AccSubField.Value) == 34)
                  If (SubStr(AccSubField.Value, 11, 3) != SubStr(AccSubField.Value, 32, 3))
                     ShowMsg("Три последних символа БИКа должны совпадать|с тремя последними символами коррсчета");
                     Return FALSE;
                  End;
               End;
            End;
*/
         Else
            If (StrLen(AccSubField.Value) == 1)
               ShowMsg("Не заполнен счет в поле " + This.Name);
               Return FALSE;
            End;
         End;
      End;

      If (Option == "A")
         If (BIC.IsEmpty())
            ShowMsg("При опции А не может быть пустой БИК в поле " + This.Name);
            Return FALSE;
         End;
      Elif (Option == "B")
         If (Location.IsEmpty() AND ((ValType(PI) != V_UNDEF) AND PI.IsEmpty()))
            ShowMsg("При опции B не может быть пустой Location и PartyIdentidier в поле " + This.Name);
            Return FALSE;
         End;
      Elif (Option == "C")
         If ((ValType(PI) != V_UNDEF) AND PI.IsEmpty())
            ShowMsg("При опции C не может быть пустой Party Identifier в поле " + This.Name);
            Return FALSE;
         End;
      Elif (Option == "D")
         If (((ValType(NameFld) != V_UNDEF) AND NameFld.IsEmpty()) OR
             (Trim(GetSubField("Name").Value) == DUMMY_NAME))
            ShowMsg("При опции D не может быть пустой Name в поле " + This.Name);
            Return FALSE;
         End;
      Elif ((Option == "K") OR (Option == NO_FIELD_OPTION))
         If ((ValType(NameFld) != V_UNDEF) AND
             ((Trim(GetSubField("Name").Value) == "") OR (Trim(GetSubField("Name").Value) == DUMMY_NAME)) AND
             (Trim(GetSubField("Address").Value) == "") AND
             (Trim(GetSubField("City").Value) == "") AND
             (Trim(GetSubField("Country").Value) == ""))
            ShowMsg("При опции " + Option + " не может быть пустой Name в поле " + This.Name);
            Return FALSE;
         End;
      End;
      Return IsValid();
   End;
   MACRO Clear ()
      /* AB 07.09.2009 Опцию тоже надо чистить */
      this.Option = NO_FIELD_OPTION;
      return Clear();
   END;
   InitTMTField(_Owner, Min, Max, _Name, _Serializer, FT, FO, AO);
End;

Class (TMTField) TMTField13(_Owner, _Serializer, FT, FO, AO)
   InitTMTField(_Owner, 4, 5, "13", _Serializer, FT, FO, AO);
   TMTTimeCode(This, "", NULL);
   TMTDate(This, Date(0, 0, 0), NULL);
   TMTTimeIndication(This, Time(0, 0, 0), NULL);
   TMTTimeSign(This, "", NULL);
   TMTTimeOffset(This, Time(0, 0, 0), NULL);

   Macro IsValid()
      Var HH;
      If (IsEmpty())
         Return IsValid();
      End;
      If ((Option == "C")and(NOT IsValueInArray(GetSubField("TimeCode").Value, Field13TimeCodes)))
         ShowMsg("Неверное значение поля TimeCode");
         Return FALSE;
      End;
      If (NOT IsValueInArray(GetSubField("TimeSign").Value, Field13TimeSigns))
         ShowMsg("Неверное значение поля TimeSign");
         Return FALSE;
      End;

      TimeSplit(GetSubField("TimeOffset").Value, HH);
      If (HH > 13)
         ShowMsg("Часы смещения должны быть в пределах 0..13");
         Return FALSE;
      End;
      Return IsValid();
   End;
End;

Class (TMTField) TMTField15(_Owner, _Serializer, FT, FO, AO)
   InitTMTField(_Owner, 1, 1, "15", _Serializer, FT, FO, AO);
   TMTDummy(This);
End;

Class (TMTField) TMTField19(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 1, 1, "19", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTAmount(This, double(0), NULL);
End;

Class (TMTField) TMTField20(_Owner, _Serializer, FT, FO, AO)
   InitTMTField(_Owner, 1, 1, "20", _Serializer, FT, FO, AO);
   TMTReference(This, "", NULL);
End;

Class (TMTField) TMTField21(_Owner, _Serializer, FT, FO, AO)
   InitTMTField(_Owner, 1, 1, "21", _Serializer, FT, FO, AO);
   TMTReference(This, "", NULL);

   Macro IsValid()
      Var Ref = Trim(GetSubField("Reference").Value);
      If (IsEmpty())
         Return IsValid();
      End;
      If (StrLen(Ref) < 2)
         ShowMsg("Длина поля РЕФЕРЕНС должна быть больше 1 символа");
         Return FALSE;
      End;

      If (SubStr(Ref, 1, 1) == "/")
         ShowMsg("Поля РЕФЕРЕНС не может начинаться с символа '/'");
         Return FALSE;
      End;

      If (Index(Ref, "//") > 0)
         ShowMsg("Поле РЕФЕРЕНС не может содержать последовательности символов '//'");
         Return FALSE;
      End;
      Return IsValid();
   End;
End;

Class (TMTField) TMTField23(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 1, 1, "23", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTBankOperationCode(This, "", NULL);

   Macro IsValid()
      If (IsEmpty())
         Return IsValid();
      End;
      If (NOT IsValueInArray(GetSubField("BankOperationCode").Value, Field23Values))
         ShowMsg("Неверное значение поля BankOperationCode");
         Return FALSE;
      End;
      Return IsValid();
   End;
End;

Class (TMTField) TMTField23B(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 1, 1, "23", _Serializer, FT, "B", "B");
   TMTBankOperationCode(This, "", NULL);

   Macro IsValid()
      If (IsEmpty())
         Return IsValid();
      End;
      If (NOT IsValueInArray(GetSubField("BankOperationCode").Value, Field23BValues))
         ShowMsg("Неверное значение поля BankOperationCode");
         Return FALSE;
      End;
      Return IsValid();
   End;
End;

Class (TMTField) TMTField23E(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 1, 2, "23", _Serializer, FT, "E", "E");
   TMT23EInstructionCode(This, "", NULL);
   TMT23EAdditionalInfo(This, "", NULL);

   Macro IsValid()
      If (IsEmpty())
         Return IsValid();
      End;
      If (NOT IsValueInArray(GetSubField("23EInstructionCode").Value, Field23EValues))
         ShowMsg("Неверное значение поля 23EInstructionCode");
         Return FALSE;
      End;

      If ((NOT GetSubField("23EAdditionalInfo").IsEmpty()) AND
          (GetSubField("23EInstructionCode").Value != "PHON") AND
          (GetSubField("23EInstructionCode").Value != "PHOB") AND
          (GetSubField("23EInstructionCode").Value != "PHOI") AND
          (GetSubField("23EInstructionCode").Value != "TELE") AND
          (GetSubField("23EInstructionCode").Value != "TELB") AND
          (GetSubField("23EInstructionCode").Value != "TELI") AND
          (GetSubField("23EInstructionCode").Value != "HOLD") AND
          (GetSubField("23EInstructionCode").Value != "REPA"))
         ShowMsg("Дополнительная информация может быть только с кодами|PHON, PHOB, PHOI, TELE, TELB, TELI, HOLD, REPA");
         Return FALSE;
      End;

      If ((NOT GetSubField("23EAdditionalInfo").IsEmpty()) AND
          (SubStr(GetSubField("23EAdditionalInfo").Value, 1, 1) != "/"))
         ShowMsg("Поле 23EAdditionalInfo должно начинаться с символа '/'");
         Return FALSE;
      End;
      Return IsValid();
   End;
End;

Class (TMTField) TMTField25(_Owner, _Serializer, FT, FO, AO)
   InitTMTField(_Owner, 1, 1, "25", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTAccount(This, "", NULL);

   Macro IsValid()
     If (IsEmpty())
       ShowMsg("Поле 25 должно быть заполнено");
     End;
     Return IsValid();
   End;
End;

Class (TMTField) TMTField26T(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 1, 1, "26", _Serializer, FT, "T", "T");
   TMTTransactionTypeCode(This, "", NULL);

   Macro IsValid()
      If (IsEmpty())
         Return IsValid();
      End;
      If ((asize (Field26TTransactionTypeCodes) > 0 ) AND /*AB 19.08.09  если есть массив */
         NOT IsValueInArray(GetSubField("TransactionTypeCode").Value, Field26TTransactionTypeCodes))
         ShowMsg("Неверное значение поля TransactionTypeCode");
         Return FALSE;
      End;

      Return IsValid();
   End;
End;

Class (TMTField) TMTField28(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 1, 2, "28", _Serializer, FT, "C", "C");
   TMTStatementNumber(This, "", NULL);
   TMTSequenceNumber(This, "", NULL);
End;

Class (TMTField) TMTField32(_Owner, _Serializer, FT, FO, AO)
   InitTMTField(_Owner, 3, 3, "32", _Serializer, FT, FO, AO);
   TMTValueDate(This, Date(0, 0, 0), NULL);
   TMTCodeCurrency(This, "", NULL);
   TMTAmount(This, double(0), NULL);

   Macro IsValid()
      If (IsEmpty())
         Return IsValid();
      End;
      If (GetSubField("ValueDate").Value == Date(0, 0, 0))
         ShowMsg("Дата валютирования не может быть нулевой");
         Return FALSE;
      End;

      If (GetSubField("Amount").Value == double(0.0))
         ShowMsg("Сумма не может быть нулевой");
         Return FALSE;
      End;

      Return IsValid();
   End;
End;

Class (TMTField) TMTField32B(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 2, 2, "32", _Serializer, FT, "B", "B");
   TMTCodeCurrency(This, "", NULL);
   TMTAmount(This, double(0), NULL);

   Macro IsValid()
      If (IsEmpty())
         Return IsValid();
      End;
      If (GetSubField("Amount").Value == double(0.0))
         ShowMsg("Сумма не может быть нулевой");
         Return FALSE;
      End;

      Return IsValid();
   End;
End;

Class (TMTField) TMTField33(_Owner, _Serializer, FT, FO, AO)
   InitTMTField(_Owner, 2, 2, "33", _Serializer, FT, FO, AO);
   TMTCodeCurrency(This, "", NULL);
   TMTAmount(This, double(0), NULL);

   Macro IsValid()
      If (IsEmpty())
         Return IsValid();
      End;

      If (GetSubField("Amount").Value == double(0.0))
         ShowMsg("Сумма не может быть нулевой");
         Return FALSE;
      End;

      Return IsValid();
   End;

   MACRO Clear ()
      var ValTmp;
      this.Option = NO_FIELD_OPTION;
      if (not this.IsEmpty())
       GetSubField("Amount").Value = double(0.0);
       ValTmp = GetSubField("CodeCurrency");
       if (valtype(ValTmp) != V_UNDEF)
        ValTmp.Value = "";
       end;
       ValTmp = null;
      end;
      return Clear();
   END;
End;

Class (TMTField) TMTField36(_Owner, _Serializer, FT, FO, AO)
   InitTMTField(_Owner, 1, 1, "36", _Serializer, FT, FO, AO);
   TMTRate(This, Double(0), NULL);
End;

Class (TMTMSField) TMTField50(_Owner, _Serializer, FT, FO, AO)
   InitTMTMSField(_Owner, 1, 6, "50", _Serializer, FT, FO, AO);
   TMTAccount(This, "", NULL);
   TMTBIC(This, "", NULL);
   TMTName(This, "", NULL);
   TMTAddress(This, "", NULL);
   TMTCity(This, "", NULL);
   TMTCountry(This, "", NULL);
End;

Class (TMTMSField) TMTField50X(_Owner, _Serializer, FT)
   InitTMTMSField(_Owner, 1, 4, "50", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTName(This, "", NULL);
   TMTAddress(This, "", NULL);
   TMTCity(This, "", NULL);
   TMTCountry(This, "", NULL);
End;

Class (TMTMSField) TMTFieldBankInst(_Owner, Min, Max, _Name, _Serializer, FT, FO, AO)
   InitTMTMSField(_Owner, Min, Max, _Name, _Serializer, FT, FO, AO);
   TMTPartyIdentifier(This, "", NULL);
   TMTBIC(This, "", NULL);
   TMTBIC_TRIC(This, "", NULL);
End;

Class (TMTFieldBankInst) TMTField51(_Owner, _Serializer, FT, FO, AO)
   InitTMTFieldBankInst(_Owner, 1, 3, "51", _Serializer, FT, FO, AO);
End;

Class (TMTFieldBankInst) TMTField52(_Owner, _Serializer, FT, FO, AO)
   InitTMTFieldBankInst(_Owner, 1, 8, "52", _Serializer, FT, FO, AO);
   TMTName(This, "", NULL);
   TMTAddress(This, "", NULL);
   TMTCity(This, "", NULL);
   TMTCountry(This, "", NULL);
   TMTLocation(This, "", NULL);
End;

Class (TMTFieldBankInst) TMTField53(_Owner, _Serializer, FT, FO, AO)
   InitTMTFieldBankInst(_Owner, 1, 8, "53", _Serializer, FT, FO, AO);
   TMTName(This, "", NULL);
   TMTAddress(This, "", NULL);
   TMTCity(This, "", NULL);
   TMTCountry(This, "", NULL);
   TMTLocation(This, "", NULL);
End;

Class (TMTFieldBankInst) TMTField54(_Owner, _Serializer, FT, FO, AO)
   InitTMTFieldBankInst(_Owner, 1, 8, "54", _Serializer, FT, FO, AO);
   TMTName(This, "", NULL);
   TMTAddress(This, "", NULL);
   TMTCity(This, "", NULL);
   TMTCountry(This, "", NULL);
   TMTLocation(This, "", NULL);
End;

Class (TMTFieldBankInst) TMTField55(_Owner, _Serializer, FT, FO, AO)
   InitTMTFieldBankInst(_Owner, 1, 8, "55", _Serializer, FT, FO, AO);
   TMTName(This, "", NULL);
   TMTAddress(This, "", NULL);
   TMTCity(This, "", NULL);
   TMTCountry(This, "", NULL);
   TMTLocation(This, "", NULL);
End;

Class (TMTFieldBankInst) TMTField56(_Owner, _Serializer, FT, FO, AO)
   InitTMTFieldBankInst(_Owner, 1, 7, "56", _Serializer, FT, FO, AO);
   TMTName(This, "", NULL);
   TMTAddress(This, "", NULL);
   TMTCity(This, "", NULL);
   TMTCountry(This, "", NULL);
End;

Class (TMTFieldBankInst) TMTField57(_Owner, _Serializer, FT, FO, AO)
   InitTMTFieldBankInst(_Owner, 1, 8, "57", _Serializer, FT, FO, AO);
   TMTName(This, "", NULL);
   TMTAddress(This, "", NULL);
   TMTCity(This, "", NULL);
   TMTCountry(This, "", NULL);
   TMTLocation(This, "", NULL);
End;

Class (TMTFieldBankInst) TMTField58(_Owner, _Serializer, FT, FO, AO)
   InitTMTFieldBankInst(_Owner, 1, 7, "58", _Serializer, FT, FO, AO);
   TMTName(This, "", NULL);
   TMTAddress(This, "", NULL);
   TMTCity(This, "", NULL);
   TMTCountry(This, "", NULL);
End;

Class (TMTMSField) TMTField59(_Owner, _Serializer, FT, FO, AO)
   InitTMTMSField(_Owner, 1, 6, "59", _Serializer, FT, FO, AO);
   TMTAccount(This, "", NULL);
   TMTBIC(This, "", NULL);
   TMTName(This, "", NULL);
   TMTAddress(This, "", NULL);
   TMTCity(This, "", NULL);
   TMTCountry(This, "", NULL);
End;

Class (TMTMSField) TMTField59X(_Owner, _Serializer, FT)
   InitTMTMSField(_Owner, 1, 5, "59", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTAccount(This, "", NULL);
   TMTName(This, "", NULL);
   TMTAddress(This, "", NULL);
   TMTCity(This, "", NULL);
   TMTCountry(This, "", NULL);
End;

Class (TMTField) TMTField60(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 0, 10, "60", _Serializer, FT, "F", "FM");
   TMTDCMark(This, "", NULL);
   TMTDate(This, Date(0, 0, 0), NULL);
   TMTCodeCurrency(This, "", NULL);
   TMTAmount(This, double(0), NULL);
End;

Class (TMTField) TMTField61(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 0, 10, "61", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTDate(This, Date(0, 0, 0), NULL);
   TMTEntryDate(This, Date(0, 0, 0), NULL);
   TMTDCMark(This, "", NULL);
   TMTFundsCode(This, "", NULL);
   TMTAmount(This, double(0), NULL);
   TMTTransactionTypeIdentificationCode(This, "", NULL);
   TMTReference(This, "", NULL);
   TMTRelatedReference(This, "", NULL);
   TMTSupplementaryDetails(This, "", NULL);
End;

Class (TMTField) TMTField62(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 0, 10, "62", _Serializer, FT, "F", "FM");
   TMTDCMark(This, "", NULL);
   TMTDate(This, Date(0, 0, 0), NULL);
   TMTCodeCurrency(This, "", NULL);
   TMTAmount(This, double(0), NULL);
End;

Class (TMTField) TMTField64(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 0, 10, "64", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTDCMark(This, "", NULL);
   TMTDate(This, Date(0, 0, 0), NULL);
   TMTCodeCurrency(This, "", NULL);
   TMTAmount(This, double(0), NULL);
End;

Class (TMTField) TMTField65(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 0, 10, "65", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTDCMark(This, "", NULL);
   TMTDate(This, Date(0, 0, 0), NULL);
   TMTCodeCurrency(This, "", NULL);
   TMTAmount(This, double(0), NULL);
End;

Class (TMTField) TMTField70(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 0, 4, "70", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTTextRow(This, "", "Row0");
   TMTTextRow(This, "", "Row1");
   TMTTextRow(This, "", "Row2");
   TMTTextRow(This, "", "Row3");

   Macro IsValid()
      Return IsValid();
   End;
End;

Class (TMTField) TMTField71A(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 1, 1, "71", _Serializer, FT, "A", "A");
   TMT71ACode(This, "", NULL);
   Macro IsValid()
      Var Field71AValues;
      If (IsEmpty())
         Return IsValid();
      End;

      If (Owner.Name == "MT100")
         Field71AValues = Field71AValues100;
      Else
         Field71AValues = Field71AValues103;
      End;

      If (NOT IsValueInArray(GetSubField("Code").Value, Field71AValues))
         ShowMsg("Недопустимое значение поля 71A");
         Return FALSE;
      End;
      Return IsValid();
   End;
End;

Class (TMTField) TMTField71F(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 2, 2, "71", _Serializer, FT, "F", "F");
   TMTCodeCurrency(This, "", NULL);
   TMTAmount(This, double(0), NULL);

   Macro IsValid()
      If (IsEmpty())
         Return IsValid();
      End;
/*  AB 22.08.09 иногда может
      If (GetSubField("Amount").Value == double(0.0))
         ShowMsg("Сумма не может быть нулевой");
         Return FALSE;
      End;
*/
      Return IsValid();
   End;
End;

Class (TMTField) TMTField71G(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 2, 2, "71", _Serializer, FT, "G", "G");
   TMTCodeCurrency(This, "", NULL);
   TMTAmount(This, double(0), NULL);

   Macro IsValid()
      If (IsEmpty())
         Return IsValid();
      End;

      If (GetSubField("Amount").Value == double(0.0))
         ShowMsg("Сумма не может быть нулевой");
         Return FALSE;
      End;

      Return IsValid();
   End;
End;

Class (TMTField) TMTField72(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 0, 6, "72", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTTextRow(This, "", "Row0");
   TMTTextRow(This, "", "Row1");
   TMTTextRow(This, "", "Row2");
   TMTTextRow(This, "", "Row3");
   TMTTextRow(This, "", "Row4");
   TMTTextRow(This, "", "Row5");

   Macro IsValid()
      Var F72Code = "", i = 0, CurStr;
      Var Field72Values;

      If (Owner.Name == "MT103")
         Field72Values = Field72Values103;
      Elif (Owner.Name == "MT100")
         Field72Values = Field72Values100;
      Elif (Owner.Name == "MT200")
         Field72Values = Field72Values200;
      Elif (Owner.Name == "MT202")
         Field72Values = Field72Values202;
      elif (Owner.Name == "MT910")
         return true;
      elif (Owner.Name == "MT900")
         return true;
      End;

      If (IsEmpty())
         Return IsValid();
      End;

      If (Trim(GetSubField("Row0").Value) == "")
         MsgBox("Не заполнена первая строка");
         Return FALSE;
      End;

      While (i < 6)
         CurStr = GetSubField("Row" + i).Value;
         If (Trim(CurStr) != "")
            If (i != 0)
               If (SubStr(CurStr, 1, 2) == "//")
               Elif (SubStr(CurStr, 1, 1) == "/")
                  CurStr = SubStr(CurStr, 2);
                  If (Index(CurStr, "/") < 1)
                     ShowMsg("Строка " + String (i + 1) + " в поле 72 должна начинаться с символа '/' и ключевого слова или '//'");
                     Return FALSE;
                  End;
                  /*F72Code = SubStr(CurStr, 1, Index(CurStr, "/") - 1);
                  If (NOT IsValueInArray(F72Code, Field72Values))
                     ShowMsg("Недопустимое значение ключевого слова " + F72Code + " в строке " + String (i + 1) + " в поле 72");
                     Return FALSE;
                  End;
                  */
               Else
                  ShowMsg("Строка " + String (i + 1) + " в поле 72 должна начинаться с символа '/' и ключевого слова или '//'");
                  Return FALSE;
               End;
            Else
               If (SubStr(CurStr, 1, 1) == "/")
                  CurStr = SubStr(CurStr, 2);
                  If (Index(CurStr, "/") < 1)
                     ShowMsg("Строка " + String (i + 1) + " в поле 72 должна начинаться с символа '/' и ключевого слова");
                     Return FALSE;
                  End;
                  /*F72Code = SubStr(CurStr, 1, Index(CurStr, "/") - 1);
                  If (NOT IsValueInArray(F72Code, Field72Values))
                     ShowMsg("Недопустимое значение ключевого слова " + F72Code + " в строке " + String (i + 1) + " в поле 72");
                     Return FALSE;
                  End;
                  */
               Else
                  ShowMsg("Строка " + String (i + 1) + " в поле 72 должна начинаться с символа '/' и ключевого слова");
                  Return FALSE;
               End;
            End;
         End;
         i = i + 1;
      End;
      Return IsValid();
   End;
End;

Class (TMTField) TMTField77B(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 0, 6, "77", _Serializer, FT, "B", "B");
   TMTTextRow(This, "", "Row0");
   TMTTextRow(This, "", "Row1");
   TMTTextRow(This, "", "Row2");
End;

Class (TMTField) TMTField86(_Owner, _Serializer, FT)
   InitTMTField(_Owner, 0, 6, "86", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTTextRow(This, "", "Row0");
   TMTTextRow(This, "", "Row1");
   TMTTextRow(This, "", "Row2");
   TMTTextRow(This, "", "Row3");
   TMTTextRow(This, "", "Row4");
   TMTTextRow(This, "", "Row5");
End;

Class (TMTRepeatableSec) TMTRepeatableSection13C(_Owner, _Serializer)
   Macro AddSection()
      TMTField13(This, "TMTField13SerializerString", FIELD_OPTIONAL, "C", "C");
   End;

   Macro IsValid()
      If (NOT IsValid())
         Return FALSE;
      End;
      Return TRUE;
   End;

   InitTMTRepeatableSec(_Owner, NULL, NULL, "RepeatableSection13C", _Serializer);
End;

Class (TMTRepeatableSec) TMTRepeatableSection23E(_Owner, _Serializer)
   Macro AddSection()
      TMTField23E(This, "TMTField23SerializerString", FIELD_OPTIONAL);
   End;

   Macro IsValid()
      Var i = 0, j = 0;
      Var SubFlds = TArray(), Values1 = TArray(), Values2 = TArray(),
          Val1 = TArray(), Val2 = TArray();

      If (NOT IsValid())
         Return FALSE;
      End;
      While (i < ArrayValue.Size - 1)
         j = i + 1;
         While (j < ArrayValue.Size)
            If ((ArrayValue.Value(i).GetSubField("23EInstructionCode").Value ==
                ArrayValue.Value(j).GetSubField("23EInstructionCode").Value) AND
                (NOT ArrayValue.Value(i).GetSubField("23EInstructionCode").IsEmpty()))
               ShowMsg("Одно кодовое в поле 23Е слово не может повторяться дважды");
               Return FALSE;
            End;
            j = j + 1;
         End;
         i = i + 1;
      End;

      SubFlds.Value(0) = "23EInstructionCode";
      Val1.Value(0) = "SDVA"; Val2.Value(0) = "HOLD";
      Val1.Value(1) = "SDVA"; Val2.Value(1) = "CHQB";
      Val1.Value(2) = "INTC"; Val2.Value(2) = "BONL";
      Val1.Value(3) = "INTC"; Val2.Value(3) = "HOLD";
      Val1.Value(4) = "INTC"; Val2.Value(4) = "CHQB";
      Val1.Value(5) = "REPA"; Val2.Value(5) = "HOLD";
      Val1.Value(6) = "REPA"; Val2.Value(6) = "CHQB";
      Val1.Value(7) = "REPA"; Val2.Value(7) = "BONL";
      Val1.Value(8) = "REPA"; Val2.Value(8) = "CORT";
      Val1.Value(9) = "CORT"; Val2.Value(9) = "BONL";
      Val1.Value(10) = "CORT"; Val2.Value(10) = "HOLD";
      Val1.Value(11) = "CORT"; Val2.Value(11) = "CHQB";
      Val1.Value(12) = "HOLD"; Val2.Value(12) = "CHQB";
      Val1.Value(13) = "PHOB"; Val2.Value(13) = "TELB";
      Val1.Value(14) = "PHON"; Val2.Value(14) = "TELE";
      Val1.Value(15) = "PHOI"; Val2.Value(15) = "TELI";

      i = 0;
      While (i < Val1.Size)
         Values1.Value(0) = Val1.Value(i); Values2.Value(0) = Val2.Value(i);
         If ((ValType(GetFieldByContent(SubFlds, Values1)) != V_UNDEF) AND
             (ValType(GetFieldByContent(SubFlds, Values2)) != V_UNDEF))
            ShowMsg("Не могут одновременно встречаться кодовые слова " + Values1.Value(0) + " и " + Values2.Value(0));
            Return FALSE;
         End;
         i = i + 1;
      End;

      Return TRUE;
   End;

   InitTMTRepeatableSec(_Owner, 0, 14, "RepeatableSection23E", _Serializer);
End;

Class (TMTRepeatableSec) TMTRepeatableSection71F(_Owner, _Serializer)
   Macro AddSection()
      TMTField71F(This, "TMTField71FSerializerString", FIELD_OPTIONAL);
   End;

   InitTMTRepeatableSec(_Owner, NULL, NULL, "RepeatableSection71F", _Serializer);
End;

Class (TMTForm) TMT102B(_Owner, _Serializer)
   InitTMTForm(_Owner, 14, NULL, "MT102B", _Serializer);
   TMTField21(This, NULL, FIELD_MANDATORY, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField32B(This, NULL, FIELD_MANDATORY);
   TMTField50X(This, NULL, FIELD_OPTIONAL);
   TMTField52(This, NULL, FIELD_OPTIONAL, "A", "ABC");
   TMTField57(This, NULL, FIELD_OPTIONAL, "A", "AC");
   TMTField59X(This, NULL, FIELD_MANDATORY);
   TMTField70(This, NULL, FIELD_OPTIONAL, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTField26T(This, NULL, FIELD_OPTIONAL);
   TMTField77B(This, NULL, FIELD_OPTIONAL);
   TMTField33(This, NULL, FIELD_OPTIONAL, "B", "B");
   TMTField71A(This, NULL, FIELD_OPTIONAL);
   TMTField71F(This, NULL, FIELD_OPTIONAL);
   TMTField71G(This, NULL, FIELD_OPTIONAL);
   TMTField36(This, NULL, FIELD_OPTIONAL, NO_FIELD_OPTION, NO_FIELD_OPTION);
End;

Class (TMTForm) TMT9XXRow(_Owner, _Serializer)
   InitTMTForm(_Owner, 2, NULL, "MT9XXRow", _Serializer);
   TMTField61(This, NULL, FIELD_MANDATORY);
   TMTField86(This, NULL, FIELD_OPTIONAL);
End;

Class (TMTForm) TMT9XXForwardRow(_Owner, _Serializer)
   InitTMTForm(_Owner, 0, NULL, "MT9XXForwardRow", _Serializer);
   TMTField65(This, NULL, FIELD_OPTIONAL);
End;


Class (TMTField) TMTField75(_Owner, _Serializer, FT)
   var i = 0, maxcnt = 6;
   InitTMTField(_Owner, 0, maxcnt, "75", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   while ( i< maxcnt)
     TMTTextRow(This, "", "Row" + i);
     i = i + 1;
   end;
End;

Class (TMTField) TMTField76(_Owner, _Serializer, FT)
   var i = 0, maxcnt = 6;
   InitTMTField(_Owner, 0, maxcnt, "76", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   while ( i< maxcnt)
     TMTTextRow(This, "", "Row" + i);
     i = i + 1;
   end;
End;


Class (TMTField) TMTField77(_Owner, _Serializer, FT)
   var i = 0, maxcnt = 20;
   InitTMTField(_Owner, 0, maxcnt, "77", _Serializer, FT, "A", "A");
   while ( i< maxcnt)
     TMTTextRow(This, "", "Row" + i);
     i = i + 1;
   end;
End;


Class (TMTField) TMTField11(_Owner, _Serializer, FT, FO, AO)
   var i = 0, maxcnt = 3;
   InitTMTField(_Owner, 2, maxcnt, "11", _Serializer, FT, FO, AO);
   while ( i< maxcnt)
     TMTTextRow(This, "", "Row" + i);
     i = i + 1;
   end;

   MACRO IsValid ()
     var r0 = "", r1 = "", r2 = "", mask = "";

     if (this.IsEmpty ())
      return IsValid();
     end;

     if (not getsubfield("Row0").IsEmpty())
       r0 = getsubfield("Row0").value;
     end;
     if (not getsubfield("Row1").IsEmpty())
       r1 = getsubfield("Row1").value;
     end;
     mask = "3!n";
     If (NOT CheckMask(r0, mask))
       ShowMsg("Первая строка в поле " + This.Name + " должна соответствовать маске " + mask);
       return false;
     end;
     mask = "6!n";
     If (NOT CheckMask(r1, mask))
       ShowMsg("Вторая строка в поле " + This.Name + " должна соответствовать маске " + mask);
       return false;
     end;
     if (not getsubfield("Row2").IsEmpty())
       r2 = getsubfield("Row2").value;
       mask = "4!n6!n";
/*TODO: CheckMask в такой маске проверяет только первую часть - 4!n*/
       If (NOT CheckMask(r2, mask))
         ShowMsg("Третья строка в поле " + This.Name + " должна соответствовать маске " + mask);
         return false;
       end;
     end;
     return IsValid();
   END;
End;


Class (TMTField) TMTField79(_Owner, _Serializer, FT)
   var i = 0, maxcnt = 35;
   InitTMTField(_Owner, 0, maxcnt, "79", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   while ( i< maxcnt)
     TMTTextRow(This, "", "Row" + i);
     i = i + 1;
   end;
End;

Class (TMTField) TMTFieldsCopy(_Owner, _Serializer, FT)
   var i = 0, maxcnt = 1;
   InitTMTField(_Owner, 0, maxcnt, "FIELDSCOPY", _Serializer, FT, NO_FIELD_OPTION, NO_FIELD_OPTION);
   TMTTextRow(This, "", "Row0");
End;

