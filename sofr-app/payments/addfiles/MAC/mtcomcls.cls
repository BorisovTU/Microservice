/*
  TMTContainer
   ├─TMTField
   └─TMTForm
      └─TMTRepeatableSec
  TMTContainable
*/
Import mtconsts, "mtserial.cls", "empty.cls";

Class TMTErrorStream()
   Macro ShowMsg(_Msg)
   End;
End;

/*
 AB 08.09.09 пустышка для улучшения работы GetField
*/
Class TMTEmptyField ()
  var Name = "", Option = "";
  MACRO IsEmpty ()
    return true;
  END;
end;

Class TMTContainer(_Owner, Min, Max, N, _Serializer)
   Var Owner = SafeGetWeakRef(_Owner), MinEntry = Min, MaxEntry = Max;
   Var ArrayValue = TArray(), /*StringValue = "",*/ Name = N;
   Var Serializers = TArray();
   Var ErrorStream, LastError = "";

   Macro Clear()
      Var Count = 0;
      While (Count < ArrayValue.Size)
         ArrayValue.Value(Count).Clear();
         Count = Count + 1;
      End;
   End;

   Macro IsAlphaStr(Str)
      If (StrLen(Str) == 0)
         Return FALSE;
      End;
      If (StrLen(Str) == 1)
         If (Index(AlphaStr, StrLwr(Str)) > 0)
            Return TRUE;
         End;
         Return FALSE;
      End;
      Return IsAlphaStr(SubStr(Str, 2));
   End;

   Macro IsDigitStr(Str)
      If (StrLen(Str) == 0)
         Return FALSE;
      End;
      If (StrLen(Str) == 1)
         If (Index(DigitStr, Str) > 0)
            Return TRUE;
         End;
         Return FALSE;
      End;
      Return IsDigitStr(SubStr(Str, 2));
   End;

   Macro CheckMask(Str, Mask)
      Var i = 0, MaskToken, StrToken, Min, Max, Chr, CurSym;

      While (StrLen(Str) > 0)
         If (StrLen(Mask) == 0)
            If (StrLen(Str) > 0)
               Return FALSE;
            End;
         End;

         If (IsDigitStr(SubStr(Mask, 1, 1)))
            Min = Int(SubStr(Mask, 1, 2));
            Max = Int(SubStr(Mask, 4, 2));
            Chr = SubStr(Mask, 6, 1);
            Mask = SubStr(Mask, 7);
            CurSym = i = Min;

            If (StrLen(Str) < Min)
               Return FALSE;
            End;

            If (Chr == "n")
               If (NOT IsDigitStr(SubStr(Str, 1, Min)))
                  Return FALSE;
               End;
            Elif (Chr == "x")
               If (NOT IsAlphaStr(SubStr(Str, 1, Min)))
                  Return FALSE;
               End;
            End;

            While (i < Max)
               If (Chr == "n")
                  If (NOT IsDigitStr(SubStr(Str, i + 1, 1)))
                     i = Max;
                  End;
               Elif (Chr == "x")
                  If (NOT IsAlphaStr(SubStr(Str, i + 1, 1)))
                     i = Max;
                  End;
               End;
               CurSym = CurSym + 1;
               i = i + 1;
            End;
            Str = SubStr(Str, CurSym + 1);
         Else
            If (SubStr(Str, 1, 1) != SubStr(Mask, 1, 1))
               Return FALSE;
            End;
            Mask = SubStr(Mask, 2);
            Str  = SubStr(Str,  2);
         End;
      End;

      If (StrLen(Mask) > 0)
         Return FALSE;
      End;

      Return TRUE;
   End;

   Macro ShowMsg(_Msg)
      LastError = _Msg;
      If (ValType(Owner) != V_UNDEF)
         Owner.ShowMsg(_Msg);
      Else
         ErrorStream.ShowMsg(_Msg);
      End;
   End;

   Macro IsEmpty()
      Var Count = 0;
      While (Count < ArrayValue.Size)
         If (NOT ArrayValue.Value(Count).IsEmpty())
            Return False;
         End;
         Count = Count + 1;
      End;
      Return True;
   End;

   Macro UnRegisterSerializers()
      Serializers.Size = 0;
   End;

   Macro RegisterSerializer(Ser)
      Var Pos = 0;
      If (ValType(Ser) == V_STRING)
         Pos = Index(Ser, ",");
         While (Pos > 0)
            Serializers.Value(Serializers.Size) = GenObject(SubStr(Ser, 1, Pos - 1), SafeGetWeakRef(This));
            Ser =  SubStr(Ser, Pos + 1);
            Pos = Index(Ser, ",");
         End;
         Serializers.Value(Serializers.Size) = GenObject(Ser, SafeGetWeakRef(This));
      Elif (ValType(Ser) == V_GENOBJ)
         Serializers.Value(Serializers.Size) = Ser;
         Serializers.Value(Serializers.Size - 1).Owner = SafeGetWeakRef(This);
      Elif (ValType(Ser) == V_UNDEF)
         ;
      Else
         ShowMsg("Не могу зарегистрировать такой сериалайзер - " + GenClassName(Ser));
      End;
   End;

   Macro SerializeUp(Storage, _Param)
      Var i = 0, RetVal;
      While (i < Serializers.Size)
         If (Serializers.Value(i).IsMyStorage(Storage, _Param))
            RetVal = Serializers.Value(i).SerializeUp(Storage, _Param);
            SetParm(1, Storage);
            Return RetVal;
         End;
         i = i + 1;
      End;
      SetParm(1, Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
     Var i = 0, RetVal;
     While(i < Serializers.Size)
        If (Serializers.Value(i).IsMyStorage(_Storage, _Param))
           RetVal = Serializers.Value(i).SerializeDown(_Storage, _Param);
           SetParm(1, _Storage);
           Return RetVal;
        End;
        i = i + 1;
     End;
     SetParm(1, _Storage);
     Return True;
   End;

   Macro ShiftArrayRight(_Array, _Position, _Count)
      Var Counter = 0, Max;
      If (ValType(_Array) == V_UNDEF)
         _Array = ArrayValue;
      End;

      Max = _Array.Size - _Position + 1;

      While (Counter < Max)
         _Array.Value(_Array.Size + _Count - Counter - 1) = _Array.Value(_Array.Size - Counter - 1);
         Counter = Counter + 1;
      End;

      Return _Array;
   End;

   Macro ShiftArrayLeft(_Position, _Count)
      Var i = 0, _Array;
      _Array = ArrayValue;

      If (_Position - _Count < 0)
        ShowMsg("Нельзя сдвигать массив больше, чем на " + _Position + " элементов влево");
      End;
      if (_Position  == _Array.Size)
       i = _Count;
      end;
      While (i < _Count)
          _Array.Value(_Position - _Count + i) = _Array.Value(_Position + i);
          i = i + 1;
      End;

      _Array.Size = _Array.Size - _Count;

      Return _Array;
   End;

   Macro Pack()
      Var i = 0;
      While (i < ArrayValue.Size)
         If ((ValType(ArrayValue.Value(i)) == V_UNDEF) OR ArrayValue.Value(i).IsEmpty())
            ShiftArrayLeft(i + 1, 1);
         Else
            i = i + 1;
         End;
      End;
   End;

   If (ValType(Owner) != V_UNDEF)
      Owner.ArrayValue.Value(Owner.ArrayValue.Size) = This;
   End;

   If (ValType(MinEntry) == V_UNDEF)
      MinEntry = 0;
   End;

   If (ValType(MaxEntry) == V_UNDEF)
      MaxEntry = 65535;
   End;

   RegisterSerializer(_Serializer);
End;

Class TMTContainable(_Owner, _Name, _Value)
   Var Owner = SafeGetWeakRef(_Owner), Name = _Name, Value = _Value;
   Owner.ArrayValue.Value(Owner.ArrayValue.Size) = This;

   Macro Clear()
      Value = GetEmptyValue(ValType(Value));
   End;

   Macro IsEmpty()
      Return (String(GetEmptyValue(ValType(Value))) == String(Value));
   End;

   Macro SetTo(_Val)
      If (This.IsEmpty())
         Value = _Val;
      End;
   End;
End;

Class (TMTContainer) TMTField(OF, Min, Max, FN, Serializer, FT, FO, AO)
   Var Option = FO, AvailableOptions = AO, Type = FT, Scr;
   var EmptyFld = TMTEmptyField ();

   Macro Copy(_Src)
      Var i = 0, j = 1, Val;
      While (GetParm(j, Val))
         If (NOT Val.IsEmpty())
            If ((Int(_Src.Name) <= 50) OR (Int(_Src.Name) >= 59))
               This.Option = _Src.Option;
            Else
               If (Index(This.AvailableOptions, _Src.Option) > 0)
                  This.Option = _Src.Option;
               Else
                  This.GetSubField("PartyIdentifier").Value = _Src.GetSubField("PartyIdentifier").Value;
                  This.Option = "D";
                  If (_Src.Option == "B")
                     This.GetSubField("Name").Value = _Src.GetSubField("Location").Value;
                  Else
                     This.GetSubField("Name").Value = "Name";
                  End;
                  Return;
               End;
            End;
            While (i < _Src.ArrayValue.Size)
               Val = This.GetSubField(_Src.ArrayValue.Value(i).Name);
               If (ValType(Val) != V_UNDEF)
                  Val.Value = _Src.ArrayValue.Value(i).Value;
               End;
               i = i + 1;
            End;
            Return;
         End;
         j = j + 1;
      End;
   End;

   Macro IsValueInArray(_Value, _Array)
      Var i = 0;

      While (i < ASize(_Array))
         If (ValType(_Array(i)) == ValType(_Value))
            If (_Array(i) == _Value)
               Return TRUE;
            End;
         End;
         i = i + 1;
      End;
      Return False;
   End;

   Macro GetValueInArrayIndex(_Value, _Array, _CurInd)
      Var i = 0;

      If (ValType(_CurInd) == V_INTEGER)
         i = _CurInd;
      End;

      While (i < ASize(_Array))
         If (ValType(_Array(i)) == ValType(_Value))
            If (_Array(i) == _Value)
               Return i;
            End;
         End;
         i = i + 1;
      End;
      Return -1;
   End;

   Macro IsValid()
      If (ArrayValue.Size < MinEntry)
         ShowMsg("MIN ERROR " + Name);
         Return False;
      End;

      If (ArrayValue.Size > MaxEntry)
         ShowMsg("MAX ERROR " + Name);
         Return False;
      End;

      If ((Type == FIELD_MANDATORY) AND (IsEmpty()))
         ShowMsg("Не заполнено обязательное поле " + Name);
         Return FALSE;
      End;

      If ((Index(AvailableOptions, Option) < 1) AND (NOT IsEmpty))
         ShowMsg("Недопустимая опция " + Option + " в поле " + Name);
         Return FALSE;
      End;

      Return True;
   End;

   Macro Validate()
      Return TRUE;
   End;

   Macro GetPrintableOption()
      If (Option == NO_FIELD_OPTION)
         Return NO_FIELD_OPTION_PRINTABLE;
      End;
      Return Option;
   End;

   Macro GetFieldSubScribe()
      Var Str = FieldSubScribe(This.Name), Pos;
      Pos = Index(Str, "@" + This.Option + "@");
      If (Pos < 1)
         Return Str;
      End;
      Str = SubStr(Str, Pos + 3);
      Pos = Index(Str, "@");
      If (Pos < 1)
         Return Str;
      Else
         Return SubStr(Str, 1, Pos - 1);
      End;
   End;

   Macro Clear()
      Var Count = 0;
      While (Count < ArrayValue.Size)
         ArrayValue.Value(Count).Value = GetEmptyValue(ValType(ArrayValue.Value(Count).Value));
         Count = Count + 1;
      End;
   End;

   Macro GetSubField(_Name)
      Var i = 0;
      While (i < ArrayValue.Size)
         If (StrUpr(_Name) == StrUpr(ArrayValue.Value(i).Name))
            Return ArrayValue.Value(i);
         End;
         i = i + 1;
      End;
      Return NULL;
   End;

   Macro GetSubFieldByContent(_Content)
      Var i = 0;
      While (i < ArrayValue.Size)
         If (ValType(ArrayValue.Value(i).Value) == ValType(_Content))
            If (String(ArrayValue.Value(i).Value) == String(_Content))
               Return ArrayValue.Value(i);
            End;
         End;
         i = i + 1;
      End;
      Return NULL;
   End;

   Macro GetSubFieldBySubContent(_Content)
      Var i = 0;
      While (i < ArrayValue.Size)
         If (ValType(ArrayValue.Value(i).Value) == ValType(_Content))
            If (Index(String(ArrayValue.Value(i).Value), String(_Content)))
               Return ArrayValue.Value(i);
            End;
         End;
         i = i + 1;
      End;
      Return NULL;
   End;

   InitTMTContainer(OF, Min, Max, FN, Serializer);

   If (ValType(Option) == V_UNDEF)
      ShowMsg("Не определена опция поля");
   End;

   If (ValType(AvailableOptions) == V_UNDEF)
      ShowMsg("Не определены допустимые опции");
   End;

   If (ValType(Type) == V_UNDEF)
      Type = FIELD_MANDATORY;
   Elif (ValType(Type) != V_STRING)
      ShowMsg("Неверный тип параметра - ТИП ПОЛЯ");
   Else
      Type = SubStr(FT, 1, 1);
   End;
End;

Class (TMTContainer) TMTForm(OF, Min, Max, FN, Serializer)
   var EmptyFld = TMTEmptyField ();

   Macro IsValid()
      Var i = 0;
      While (i < ArrayValue.Size)
         If (Not ArrayValue.Value(i).IsValid())
            Return False;
         End;
         i = i + 1;
      End;
      Return True;
   End;

   Macro UpdateSerializers()
   End;

   Macro Validate()
      Var i = 0;
      While (i < ArrayValue.Size)
         If (NOT ArrayValue.Value(i).Validate())
            Return FALSE;
         End;
         i = i + 1;
      End;
      Return TRUE;
   End;

   Macro GetField(_Fld, _Opt)
      Var i = 0;

      If (ValType(_Fld) == V_INTEGER)
        If ((_Fld < ArrayValue.Size) AND (_Fld > 0))
           Return ArrayValue.Value(_Fld);
        Else
           ShowMsg("Неверный индекс поля");
        End;
      End;
      While (i < ArrayValue.Size)

        If (ArrayValue.Value(i).Name == _Fld)
          If ((ValType(_Opt) == V_UNDEF) OR
              (ValType(_Opt) == V_STRING) AND (ArrayValue.Value(i).Option == _Opt))
             Return ArrayValue.Value(i);
          End;
        End;
        i = i + 1;
      End;
      /* AB 08.09.09 так не нужно проверять результат на V_UNDEF  */
      return EmptyFld;
   End;

   Macro GetFieldID(_Fld)
      Var i = 0;

      While (i < ArrayValue.Size)
         If (ArrayValue.Value(i).Name == _Fld)
            Return i;
         End;
         i = i + 1;
      End;
      Return -1;
   End;

   Macro IsFieldPresent(FldName, FldOpt)
      Var i = 0;
/*      While ()

      End;*/
      Return False;
   End;

   InitTMTContainer(OF, Min, Max, FN, Serializer);
End;

Class (TMTForm) TMTRepeatableSec(OF, Min, Max, FN, Serializer)
   Macro AddSection()
   End;

   Macro GetFieldByContent(SubFlds, Values)
      Var i = 0, j = 0, ThisIt = TRUE;
      While (j < ArrayValue.Size)
         i = 0;
         ThisIt = TRUE;
         While ((i < SubFlds.Size) AND ThisIt)
            If (ArrayValue.Value(j).GetSubField(SubFlds.Value(i)).Value != Values.Value(i))
               ThisIt = FALSE;
            End;
            i = i + 1;
         End;
         If (ThisIt)
            Return ArrayValue.Value(j);
         End;
         j = j + 1;
      End;
      Return NULL;
   End;

   InitTMTForm(OF, Min, Max, FN, Serializer);
End;

