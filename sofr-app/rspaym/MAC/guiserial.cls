/*****************************************************************************
 сериалайзеры редактирования форм в ВПС
***************************************************************************/
var serial_counter;
Class (TSwiftFormSerializer) TSwiftFormToGUIFrm(_Owner)
   Macro IsMyStorage(_Storage, _Param)
      If (Not IsMyStorage(_Storage, _Param))
         Return False;
      End;
      If (not Index (StrUpr(GenClassName(Storage)), "TGUIFORM"))
         Return false;
      End;
      Return true;
   End;
   InitTSwiftFormSerializer(_Owner);
End;

Class (TSerializer) TMTFieldSerializerGUIFrm(_Owner)

   MACRO IsMyStorage(_Storage, _Param)
      If (Not IsMyStorage(_Storage, _Param))
         Return False;
      End;
      If (not Index (StrUpr(GenClassName(_Storage)),"TGUIFORM"))
         Return False;
      End;
      Return True;
   END;

  macro ChSubStr(Str,SStr,CStr)
    var TmpStr="",
    Pos=0;

    Pos = Index(Str,SStr);
    while(Pos)
         TmpStr = TmpStr+Trim(SubStr(Str, 1, Pos - 1))+CStr;
         Str = SubStr(Str, Pos + StrLen(SStr));
         Pos = Index(Str,SStr);
    end;
    return Trim(TmpStr+Str);
  end;


   MACRO SerializeUp (_Storage)
     If (Owner.IsEmpty())
         Return True;
     End;
     return true;
   END;

   MACRO SerializeDown(_Storage);
      var tempstr, cntr, Opt ="", retval = True;
      if (Index (Owner.Name, "RepeatableSection"))
        return retval;
      end;
      cntr = _Storage.ControlByName("F" + Owner.Name);
      if ((Valtype (cntr) == V_INTEGER) AND
          (Valtype (cntr = _Storage.ControlByName("F" + Owner.Name + Owner.Option)) == V_INTEGER))
         Owner.Clear();
         return retval;
      end;
      if (strupr(GenClassName (cntr)) == "TEDIT" )
       tempstr = cntr.GetText();
      elif(strupr(GenClassName (cntr)) == "TEDIT2SEC")
       opt = cntr.GetText(0);
       if (opt == "")
        opt = NO_FIELD_OPTION;
       end;
       Owner.Option = opt;
       tempstr = cntr.GetText(1);
      elif(strupr(GenClassName (cntr)) == "TEDIT3SEC")
       opt = cntr.GetText(0);
       if (opt == "")
        opt = NO_FIELD_OPTION;
       end;
       Owner.Option = opt;
       tempstr = cntr.GetText(1) + FIELD_STRING_SPLITTER + cntr.GetText(2);
      end;
/*AddPtk ("Name = " + Owner.Name + " tempstr = " + tempstr);*/
      if (tempstr == "")
       Owner.Clear ();
      else
       retval  = Owner.SerializeUp(tempstr, WITHOUT_FIELD);
      end;
      SetParm(1, _Storage);
      Return retval;
   END;
   InitTSerializer(_Owner);
END;

Class (TMTFieldSerializerGUIFrm) TMTField20SerializerGUIFrm(_Owner)

   Macro SerializeUp(_Storage);
      If (Owner.IsEmpty())
         Return True;
      End;
      _Storage.ControlByName("F20").SetText(Owner.GetSubField("Reference").Value);
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTField21SerializerGUIFrm(_Owner)

   Macro SerializeUp(_Storage);
      If (Owner.IsEmpty())
         Return True;
      End;

      _Storage.ControlByName("F21").SetText(Owner.GetSubField("Reference").Value);
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTField25SerializerGUIFrm(_Owner)

   Macro SerializeUp(_Storage);
      If (Owner.IsEmpty())
         Return True;
      End;

      _Storage.ControlByName("F25").SetText(Owner.GetSubField("Account").Value);
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTField13SerializerGUIFrm(_Owner)
   Macro SerializeUp(_Storage);
      var tempstr;
      If (Owner.IsEmpty())
         Return True;
      End;
      /**/
      tempstr= "";
      Owner.SerializeDown(tempstr, WITHOUT_FIELD);
      _Storage.AddFld13D("F13D", tempstr);
      SetParm(1, _Storage);
      Return True;
   End;

   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTField13CSerializerGUIFrm(_Owner)

   Macro SerializeUp(_Storage);
      var tempstr, i=0, baseControl;
      If (Owner.IsEmpty())
         Return True;
      End;
      Owner.Pack();
      baseControl = _Storage.ControlByName ("F21");
      if (Valtype(baseControl) == V_INTEGER)
        baseControl = _Storage.ControlByName ("F20");
      end;
      while (i< Owner.ArrayValue.Size)
       tempstr= "";
       Owner.ArrayValue.Value(i).SerializeDown(tempstr, WITHOUT_FIELD);
       _Storage.AddRepeatableField ("F13C",tempstr, 60, 240,
                                    130, 19, baseControl, 0);
       i = i + 1;
      end;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage);
      var tempstr, i, cntr, retval = true;
      /*If (Owner.IsEmpty())
         Return retval;
      End;
      */
      cntr = _Storage.ControlByName ("ScrollF13C");
      Owner.ArrayValue.Size = 0;
      if (ValType (cntr) == V_INTEGER)
       return retval;
      end;
      i = 0;
      while  (i < cntr.Controls.Size)
        Owner.AddSection ();
        i = i + 1;
      end;
      i = 0;
      while (i <  cntr.Controls.Size )
       tempstr= cntr.Controls.Value(i).GetText();
       retval = retval AND Owner.ArrayValue.Value(i).SerializeUp(tempstr, WITHOUT_FIELD);
       i = i + 1;
      end;
      SetParm(1, _Storage);
      Return retval;
   End;
   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTField23BSerializerGUIFrm(_Owner)

   Macro SerializeUp(_Storage);
      var fld;
      If (Owner.IsEmpty())
         Return True;
      End;
      _Storage.ControlByName("F23B").SetText(Owner.GetSubField("BankOperationCode").Value);
      SetParm(1, _Storage);
      Return True;
   End;

   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTField23ESerializerGUIFrm(_Owner)

   Macro SerializeUp(_Storage);
      var i = 0, tempstr;
      If (Owner.IsEmpty())
         Return True;
      End;
       Owner.Pack();
       while (i< Owner.ArrayValue.Size)
       tempstr= "";
       Owner.ArrayValue.Value(i).SerializeDown(tempstr, WITHOUT_FIELD);
       _Storage.AddRepeatableField ("F23E", tempstr, 60, 250,
                                    230, 35, _Storage.ControlByName ("F23B"), 0);
       i = i + 1;
      end;

      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage);
      var tempstr, cntr, i, retval = true ;
      /*If (Owner.IsEmpty())
         Return retval;
      End;
      */
      cntr = _Storage.ControlByName ("ScrollF23E");
      Owner.ArrayValue.Size = 0;
      if (ValType (cntr) == V_INTEGER)
       return retval;
      end;
      i = 0;
      while  (i < cntr.Controls.Size)
        Owner.AddSection ();
        i = i + 1;
      end;
      i = 0;
      while (i <  cntr.Controls.Size)
       tempstr= cntr.Controls.Value(i).GetText();
       retval = retval AND Owner.ArrayValue.Value(i).SerializeUp(tempstr, WITHOUT_FIELD);
       i = i + 1;
      end;
      SetParm(1, _Storage);
      Return retval;
   End;
   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTField26SerializerGUIFrm(_Owner)
   Macro SerializeUp(_Storage);
      If (Owner.IsEmpty())
         Return True;
      End;
      _Storage.AddFld26T("F26T", Owner.GetSubField("TransactionTypeCode").Value);
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTField32SerializerGUIFrm(_Owner)
   Macro SerializeUp(_Storage);
      var tempstr;
      If (Owner.IsEmpty())
         Return True;
      End;
      tempstr = "";
      Owner.SerializeDown(tempstr, WITHOUT_FIELD);
      _Storage.ControlByName("F32").SetText(Owner.Option,tempstr);
      SetParm(1, _Storage);
      Return True;
   End;

   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTField33SerializerGUIFrm(_Owner)
   Macro SerializeUp(_Storage);
      var tempstr;
      If (Owner.IsEmpty())
         Return True;
      End;
      tempstr = "";
      Owner.SerializeDown(tempstr, WITHOUT_FIELD);
      _Storage.AddFld33B("F33B", tempstr);
      SetParm(1, _Storage);
      Return True;
   End;

   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTField36SerializerGUIFrm(_Owner)
   Macro SerializeUp(_Storage);
      If (Owner.IsEmpty())
         Return True;
      End;
      _Storage.AddFld36("F36", Owner.GetSubField("Rate").Value);
      SetParm(1, _Storage);
      Return True;
   End;

   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTFieldMSSerializerGUIFrm(_Owner)

   MACRO SerializeUp(_Storage, _Param);
      var tempstr, acc, opt;

      If (Owner.IsEmpty())
         Return True;
      End;

      If ((Int(Owner.Name) < 59) AND (Int(Owner.Name) > 50 ))
         acc = Owner.GetSubField("PartyIdentifier").Value;
         if (Owner.Option == "A")
            _Storage.AddFld5X("F"+ Owner.Name, Owner.Option, acc,
                              Owner.GetSubField("BIC").Value);
         Elif (Owner.Option == "B")
            _Storage.AddFld5X("F"+ Owner.Name, Owner.Option, acc,
                              Owner.GetSubField("Location").Value);

         Elif (Owner.Option == "C")
            _Storage.AddFld5X("F"+ Owner.Name, Owner.Option, acc, "");

         Elif (Owner.Option == "D")
            tempstr = OutSubFields("Name,Address,City,Country");
            _Storage.AddFld5X("F"+ Owner.Name, Owner.Option, acc, tempstr);
         end;
      elif ((Int(Owner.Name) == 50) OR (Int(Owner.Name) == 59))
         if (ValType(_Storage.ControlByName("F"+ Owner.Name)) == V_INTEGER)
           _Storage.AddFld5X("F"+ Owner.Name, "","","");
         end;
         acc = Owner.GetSubField("Account");
         If (ValType(acc) == V_UNDEF)
          acc = "";
         else
          acc = acc.Value;
         End;
         If ((Owner.Option == "K") OR (Owner.Option == NO_FIELD_OPTION) OR (Owner.Option == "F"))
            if (Owner.Option == NO_FIELD_OPTION)
             opt = "";
            else
             opt = Owner.Option;
            end;
            tempstr = OutSubFields("Name,Address,City,Country");
            _Storage.ControlByName("F"+ Owner.Name).SetText(opt,acc,tempstr);
         else
            _Storage.ControlByName("F"+ Owner.Name).SetText(Owner.Option, acc,
                                                            Owner.GetSubField("BIC").Value);
         end;
      end;
      SetParm(1, _Storage);
      Return True;
   END;
   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldMSSerializerGUIFrm) TMTField50SerializerGUIFrm(_Owner)
   InitTMTFieldMSSerializerGUIFrm(_Owner);
End;

Class (TMTFieldMSSerializerGUIFrm) TMTField59SerializerGUIFrm(_Owner)
   InitTMTFieldMSSerializerGUIFrm(_Owner);
End;

Class (TMTFieldMSSerializerGUIFrm) TMTFieldBankInstSerializerGUIFrm(_Owner)
   InitTMTFieldMSSerializerGUIFrm(_Owner);
End;

Class (TMTFieldMSSerializerGUIFrm) TMTField70SerializerGUIFrm(_Owner)
   Macro SerializeUp(_Storage);
      var tempstr, i = 0;

      If (Owner.IsEmpty())
         Return True;
      End;
      tempstr ="";
      while  (i < 4)
        if (tempstr != "")
         tempstr = tempstr + RN + Owner.GetSubField("Row" + i).Value;
        else
         tempstr = tempstr + Owner.GetSubField("Row" + i).Value;
        end;
        i = i + 1;
      end;
      _Storage.AddFld70("F70", tempstr);
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldMSSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTField71ASerializerGUIFrm(_Owner)
   Macro SerializeUp(_Storage);
      var cntr;
      If (Owner.IsEmpty())
         Return True;
      End;
      cntr = _Storage.ControlByName("F71A");
      if(valtype (cntr) == V_GENOBJ)
       _Storage.ControlByName("F71A").SetText(Owner.GetSubField("Code").Value);
      else
       _Storage.AddFld71A("F71A",Owner.GetSubField("Code").Value);
      end;
      SetParm(1, _Storage);
      Return True;
   End;

   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTField71FSerializerGUIFrm(_Owner)
   Macro SerializeUp(_Storage);
      var i = 0, tmp;
      If (Owner.IsEmpty())
         Return True;
      End;
      Owner.Pack();
      while (i< Owner.ArrayValue.Size)
       if (valtype (Owner.ArrayValue.Value(i)) != V_UNDEF)
        tmp = _Storage.AddRepeatableField ("F71F", Owner.ArrayValue.Value(i).GetSubField("Code_Currency").Value+
                                          ChSubstr(Owner.ArrayValue.Value(i).GetSubField("Amount").Value, ".", ","),
                                          60, 240, 130, 18,_Storage.ControlByName ("F71A"), 3, true);
       end;
       i = i + 1;
      end;
      SetParm(1, _Storage);

      Return True;
   End;

   Macro SerializeDown(_Storage);
      var tempstr, cntr, i, retval = true;
      /*If (Owner.IsEmpty())
         Return retval;
      End;
      */
      cntr = _Storage.ControlByName ("ScrollF71F");
      Owner.ArrayValue.Size = 0;
      if (ValType (cntr) == V_INTEGER)
       return retval;
      end;
      i = 0;
      while  (i < cntr.Controls.Size)
        Owner.AddSection ();
        i = i + 1;
      end;
      i =  0;
      while (i < cntr.Controls.Size )
       tempstr= cntr.Controls.Value(i).GetText();
       retval = retval AND Owner.ArrayValue.Value(i).SerializeUp(tempstr, WITHOUT_FIELD);
       i = i + 1;
      end;
      SetParm(1, _Storage);
      Return retval;
   End;
   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldSerializerGUIFrm) TMTField71GSerializerGUIFrm(_Owner)
   Macro SerializeUp(_Storage);
      var tempstr;
      If (Owner.IsEmpty())
         Return True;
      End;
      _Storage.AddFld71G ("F71G",  Owner.GetSubField("Code_Currency").Value +
                          this.ChSubstr(Owner.GetSubField("Amount").Value, ".", ","));
      SetParm(1, _Storage);
      Return True;
   End;

   InitTMTFieldSerializerGUIFrm(_Owner);
End;

Class (TMTFieldMSSerializerGUIFrm) TMTField72SerializerGUIFrm(_Owner)
   Macro SerializeUp(_Storage);
      var tempstr, i = 0;

      If (Owner.IsEmpty())
         Return True;
      End;
      tempstr ="";
      while  (i < 6)
        if (tempstr != "")
         tempstr = tempstr + RN + Owner.GetSubField("Row" + i).Value;
        else
         tempstr = tempstr + Owner.GetSubField("Row" + i).Value;
        end;
        i = i + 1;
      end;
      _Storage.AddFld72("F72", tempstr);
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldMSSerializerGUIFrm(_Owner);
End;


Class (TMTFieldMSSerializerGUIFrm) TMTField77BSerializerGUIFrm(_Owner)
   Macro SerializeUp(_Storage);
      var tempstr, i = 0;

      If (Owner.IsEmpty())
         Return True;
      End;
      tempstr ="";
      while  (i < 3)
        if (tempstr != "")
         tempstr = tempstr + RN + Owner.GetSubField("Row" + i).Value;
        else
         tempstr = tempstr + Owner.GetSubField("Row" + i).Value;
        end;
        i = i + 1;
      end;
      _Storage.AddFld77B("F77B", tempstr);
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldMSSerializerGUIFrm(_Owner);
End;


MACRO AddSerializersToArray (arr)
 var arr_size, tmp, idx;
 serial_counter = 0;
 if (ValType (arr) != V_UNDEF)
  arr_size = asize(arr);
  while (serial_counter< arr_size)
    tmp = Index (arr(serial_counter), "TSwiftFormToArrayOfScrRow");
    if (not tmp)
      if (index (arr(serial_counter), "TMTField26") > 0)
        tmp =  "TMTField26Serializer";
      elif (index (arr(serial_counter), "TMTField23") > 0)
        tmp =  "TMTField23BSerializer";
      else
        tmp = Substr(arr(serial_counter), 1, Index(arr(serial_counter), "Doc") - 1);
        if(not StrLen(tmp)or(index(tmp,",")))
          tmp = Substr(arr(serial_counter), 1, Index(arr(serial_counter), "Struct") - 1);
        end;
        if(not StrLen(tmp)or(index(tmp,",")))
          tmp = Substr(arr(serial_counter), 1, Index(arr(serial_counter), "String") - 1);
        end;
      end;
      if(tmp == "TSwiftFormSerializer")
        arr(serial_counter) = arr(serial_counter)+ "," + "TSwiftFormToGUIFrm";
      elif(Index(tmp, "COV"))
        arr(serial_counter) = arr(serial_counter)+ "," + StrSubst(tmp,"COV","") + "GuiFrm";
      elif(StrLen(tmp))
        arr(serial_counter) = arr(serial_counter)+ "," + tmp + "GuiFrm";
      end;
    else
      tmp = Substr (arr(serial_counter), Index (arr(serial_counter),"TMTSection"));
      idx = Index (tmp , ",") - 1;
      if (idx < 1)
       idx = strlen (tmp  + 1);
      end;
      tmp = Substr (tmp, 1, idx);
      arr(serial_counter) = arr(serial_counter)  + "," + "TMTField" + Substr(tmp, strlen("TMTSection") + 1,Index (tmp, "Serializer") - 1) + "GuiFrm";
    end;
    /*AddPtk ("n  = " + serial_counter + " val " + arr(serial_counter));*/
    serial_counter = serial_counter  + 1;
  end;
 end;
END;
AddSerializersToArray (MT103SerializersPal);
AddSerializersToArray (MT100SerializersPal);
AddSerializersToArray (MT200SerializersPal);
AddSerializersToArray (MT202SerializersPal);
/*AddSerializersToArray (MT900SerializersPal);*/
AddSerializersToArray (MT910SerializersPal);
