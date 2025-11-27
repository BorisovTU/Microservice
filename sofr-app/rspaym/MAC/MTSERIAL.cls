Import mtconsts;

Const SERIAL_MESSAGE = "MESSAGE";
Const NOT_FIELD_RETVAL = 0;
Const WRONG_FIELD_RETVAL = 1;
Const IMPORTED_FIELD_RETVAL = 2;
Const REPEATABLE_END = 3;

Const WITHOUT_FIELD = 1;
Const WITH_FIELD = 2;
Const TO_TEMPLATE = 3;
Const WITH_FIELD_RUR4  = 4;
Const WITH_FIELD_RUR5  = 5;
Const WITH_FIELD_CLEAR = 6;

Const WITH_FIELD_RUR4_CYR  = 7;
Const WITH_FIELD_RUR5_CYR  = 8;
Const WITH_FIELD_CLEAR_CYR = 9;

Const TRANS_SET_RUR4  = 4;
Const TRANS_SET_RUR5  = 5;
Const TRANS_SET_CLEAR = 0;

Const TRANS_DIRECTION_TO_LAT = 0;
Const TRANS_DIRECTION_TO_CYR = 1;

Const ASIS_DELIM_RUR4 = "YX";
Const ASIS_DELIM_RUR5 = "'";

const LAT_UPPER_ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const LAT_UPPER_ALPHA_CLEAR = strfor(32)+ "()+,-./:?\n\r0123456789" + LAT_UPPER_ALPHA;
/* заполнение по умолчанию строк, по которым будет выполняться транслитерация CLEAR */
CLEAR_RUS_SW   = "\" А Б В Г Д Е Ё Ж З И Й К Л М Н О П Р С Т У Ф Х Ц Ч Ш Щ Ъ Ы Ь Э Ю Я ";
CLEAR_LAT_SW   =  "' A B V G D E E J Z I Y K L M N O P R S T U F H TSCHSHCH' Y ' E YUYA";
/* заполнение по умолчанию строк, по которым будет выполняться транслитерация RUR6
   версия стандарта от 03.06.2010
   здесь приведено как эатлон.
*/
RUR6_RUS_SW   = "!\"$%&'*;=_АБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯЁ№#@^`~";
RUR6_LAT_SW   = "bmspdjfvrzABVGDEJZIiKLMNOPRSTUFHCcQqxYXeuaonnffjf";
//RUR6_RUS_NONE_SW  = strfor(32)+ "()+,-./0123456789:?<>[\]{|}\n\r";
//RUR6_LAT_NONE_SW  = strfor(32)+ "()+,-./0123456789:?)((/)(/)\n\r";
RUR6_RUS_NONE_SW  = strfor(32)+ "\n()\r+,-./0123456789:?<>[\]{|}";
RUR6_LAT_NONE_SW  = strfor(32)+ "\n()\r+,-./0123456789:?)((/)(/)";
SW_RUS  = "";
SW_LAT    = "";
NONE_SW_RUS = "";
NONE_SW_LAT   = "";
TempDirPath = GetInit("TempDir") + "\\";
/* флаг использования новой функции транслитерации RUR6, для совместимости со старыми макросами */
NewRUR6Mode = true;

SerialCurrency   = null;
SerialISOCurrency= null;
SerialBIC        = null;

MACRO DebugOutString (str)
  var dt,d,m,y;

  if (not ExistFile (TempDirPath + "TranslitDebug.On"))
    return;
  end;
  dt = date();
  datesplit(dt,d,m,y);
  setoutput (TempDirPath  + string({oper}) + "_" + string (d:2:o) + string (m:2:o) + string (y:4:o) + ".txt", true);
  println (string (time()) + " " + str);
  setoutput (null,true);
END;

macro MyDateSplit(_DT, _DD, _MM, _YY)
/* 02.06.2015 voikin */
  var DT, DD, MM, YY;

  if  (ValType(_DT) == V_DATE)
    DT = _DT;
  elif(ValType(_DT) == V_DTTM)
    DtTmSplit(_DT, DT);
  else
    return false;
  end;

  DateSplit(DT, DD, MM, YY);

  SetParm(1,DD);
  SetParm(2,MM);
  SetParm(3,YY);

end;

ReplaceMacro("DateSplit","MyDateSplit");


macro IsKeyWordInField(Field, kword)
  var s ="";
  Field.SerializeDown(s, WITHOUT_FIELD);
  return index(s, kword);
end;


MACRO LoadTransStringsFromTable (tbl)

 SW_RUS  = RUR6_RUS_SW;
 SW_LAT  = RUR6_LAT_SW;
 NONE_SW_RUS = RUR6_RUS_NONE_SW;
 NONE_SW_LAT = RUR6_LAT_NONE_SW;

 return true;
 /*open (tbl, null, null, true);
 NewRUR6Mode = (getrecordsize(tbl)== 8);
 if (not NewRUR6Mode)
   return;
 end;

 keynum(tbl, 2);

 while (next(tbl))
   if (tbl.IsKbdSw)
    if ((tbl.Rus!="") AND (tbl.Lat != ""))
     SW_RUS = SW_RUS  + tbl.Rus;
     SW_LAT   = SW_LAT  + tbl.Lat;
    end;
   else
    if ((tbl.Rus!="") AND (tbl.Lat != ""))
     NONE_SW_RUS = NONE_SW_RUS + tbl.Rus;
     NONE_SW_LAT = NONE_SW_LAT + tbl.Lat;
    end;
   end;
 end;
 close(tbl);

 DebugOutString("=============== after load from table ===================");
 DebugOutString("RUR6_RUS_SW="+ SW_RUS);
 DebugOutString("RUR6_LAT_SW="+ SW_LAT);
 DebugOutString("RUR6_RUS_NONE_SW="+ NONE_SW_RUS);
 DebugOutString("RUR6_LAT_NONE_SW="+ NONE_SW_LAT);
 /*длины строк *_from и *_to должны совпадать иначе нет взаимного соответствия Rus <=> Lat*/
 if ((strlen(SW_RUS) == 0) OR
     (strlen(SW_LAT) != strlen(SW_RUS)) OR
     (strlen(NONE_SW_RUS) == 0) OR
     (strlen(NONE_SW_RUS) != strlen(NONE_SW_LAT)))
   RunError ("Ошибка при чтении таблицы транслитерации " + filename(tbl));
 end;
 */
END;

/* формирование строк соответствия для транслитерации RUR6 */
LoadTransStringsFromTable (SerialFtr5);

/* новая функция транслитерации RUR6
   str - входящая строка
   dir - направление транслитерации
         0 - Rus->Lat
         1 - Lat->Rus
*/
MACRO TransStringRur6_old (str, dir)
  var TmpStr = str, RetStr ="",  curChr = "", NewStr = "",KbdSwitchFlag = false, idx, idx1;

  If (ValType(dir) == V_UNDEF)
     Return TmpStr;
  End;
  /* кириллица */
  if   (dir == TRANS_DIRECTION_TO_CYR)
    While (StrLen(TmpStr) > 0)
      curChr  = SubStr(TmpStr, 1, 1);
      TmpStr  = Substr(TmpStr, 2);
      if (curChr == ASIS_DELIM_RUR5)
        KbdSwitchFlag = not KbdSwitchFlag;
        NewStr ="";
      else
         idx = index(NONE_SW_LAT, curChr);
         if (idx)
           /* обратное преобразованием может быть "много к одному".
              эта проверка на случай если символ есть и в SW_LAT и в NONE_SW_LAT
              тогда приоритет за SW_LAT.
           */
           idx1 = index(SW_LAT, curChr);
           if (idx1)
             NewStr = Substr (SW_RUS, idx1,1);
           else
             NewStr = Substr (NONE_SW_RUS, idx,1);
           end;
         else
           if (KbdSwitchFlag)
             NewStr = curChr;
           else
             idx = index(SW_LAT, curChr);
             if (idx)
               NewStr = Substr (SW_RUS, idx, 1);
             else
               NewStr = "";
             end;
           end;
         end;
      end;
      retstr = retstr + NewStr;
    end;
  /* латынь */
  elif (dir == TRANS_DIRECTION_TO_LAT)
    TmpStr = WinToDos(StrUpr(DosToWin(Str)));
    While (StrLen(TmpStr) > 0)
      curChr  = SubStr(TmpStr, 1, 1);
      TmpStr  = Substr(TmpStr, 2);
      /*не надо транслитерить */
      if (index(LAT_UPPER_ALPHA, curChr))
         if (KbdSwitchFlag)
           NewStr = curChr;
         else
           NewStr = ASIS_DELIM_RUR5 + curChr;
           KbdSwitchFlag = true;
         end;
      /* надо транслитерить и символ "не буква"*/
      elif (idx = index(NONE_SW_RUS, curChr))
        NewStr = substr(NONE_SW_LAT, idx, 1);
      /* надо транслитерить и символ "буква"*/
      elif (idx = index(SW_RUS, curChr))
        NewStr = substr(SW_LAT, idx, 1);
        if (KbdSwitchFlag)
          NewStr = ASIS_DELIM_RUR5 + NewStr;
          KbdSwitchFlag = false;
        end;
      else
        NewStr = "";
      end;
      retstr = retstr + NewStr;
    end;
    /* строка кончилась, а клавиатуру обратно не переключили */
    if (KbdSwitchFlag and strlen(retstr))
      retstr = retstr + "'";
    end;
  end;
  return retStr;
END;

/* новая функция транслитерации RUR6
   str - входящая строка
   dir - направление транслитерации
         0 - Rus->Lat
         1 - Lat->Rus
   _RusInQuotes - признак взятия транслитерированной кириллицы в кавычки
*/
MACRO TransStringRur6(str, dir, _RusInQuotes)
  var TmpStr = str, RetStr ="",  curChr = "", NewStr = "",KbdSwitchFlag = false, idx, idx1;
  var RusInQuotes;

  If (ValType(dir) == V_UNDEF)
     Return TmpStr;
  End;

  If (ValType(_RusInQuotes) == V_UNDEF)
     RusInQuotes = false;
  else
     RusInQuotes = _RusInQuotes;
  End;

  /*По умолчанию клавиатура в режиме RUS*/
  if(RusInQuotes)/*если брать в кавычки транслитерированную кириллицу*/
    KbdSwitchFlag = true;/*будем считать, что клавиатура изначально в режиме LAT*/
  end;

  /* кириллица *//*По умолчанию клавиатура в режиме LAT*/
  if   (dir == TRANS_DIRECTION_TO_CYR)/*LAT->RUS*/
    While (StrLen(TmpStr) > 0)

      curChr  = SubStr(TmpStr, 1, 1);
      TmpStr  = Substr(TmpStr, 2);
      if (curChr == ASIS_DELIM_RUR5)/*встретили '*/
        KbdSwitchFlag = not KbdSwitchFlag;/*переключаем клавиатуру*/
        NewStr ="";                       /*саму ' не печатаем*/
      else                          /*не кавычка*/
         idx = index(NONE_SW_LAT, curChr);
         if (idx)
           /* обратное преобразованием может быть "много к одному".
              эта проверка на случай если символ есть и в SW_LAT и в NONE_SW_LAT
              тогда приоритет за SW_LAT.
           */
           idx1 = index(SW_LAT, curChr);
           if (idx1)
             NewStr = Substr (SW_RUS, idx1,1);
           else
             NewStr = Substr (NONE_SW_RUS, idx,1);
           end;
         else
           if (KbdSwitchFlag)/*клавиатура в режиме RUS*/
             NewStr = curChr;/*символ не транслитерируем*/
           else              /*клавиатура в режиме LAT*/
             idx = index(SW_LAT, curChr);
             if (idx)                    /*нашли символ в латинской таблице*/
               NewStr = Substr (SW_RUS, idx, 1);/*заменяем на символ из киреллической таблицы*/
             else                        /*не нашли символ в латинской таблице*/
               NewStr = "";                     /*символ не печатаем*/
             end;
           end;
         end;
      end;
      retstr = retstr + NewStr;
    end;
  /* латынь */
  elif (dir == TRANS_DIRECTION_TO_LAT)/*RUS->LAT*/
    TmpStr = WinToDos(StrUpr(DosToWin(Str)));
    While (StrLen(TmpStr) > 0)
      curChr  = SubStr(TmpStr, 1, 1);
      TmpStr  = Substr(TmpStr, 2);
      /*не надо транслитерить */
      if (index(LAT_UPPER_ALPHA, curChr)or(curChr == "/"))/*встретили латинский символ*/
         if (KbdSwitchFlag)/*клавиатура в режиме LAT*/
           NewStr = curChr;/*символ не транслитерируем*/
         else              /*клавиатура в режиме RUS*/
           NewStr = ASIS_DELIM_RUR5 + curChr;/*добавляем кавычку перед символом*/
           KbdSwitchFlag = true;/*переводим клавиатуру в режим LAT*/
         end;
      /* надо транслитерить и символ "не буква"*/
      elif (idx = index(NONE_SW_RUS, curChr))
        NewStr = substr(NONE_SW_LAT, idx, 1);
/*
        if (not KbdSwitchFlag and RusInQuotes and (curChr == "/"))/*если клавиатура в режиме RUS и нужно брать в кавычки транслитерированный кириллический текст*/
          NewStr = ASIS_DELIM_RUR5 + NewStr;/*добавляем кавычку перед символом*/
          KbdSwitchFlag = true;        /*переводим клавиатуру в режим LAT*/
        end;
*/
      /* надо транслитерить и символ "буква"*/
      elif (idx = index(SW_RUS, curChr))/*нашли символ в киреллической таблице*/
        NewStr = substr(SW_LAT, idx, 1);/*заменим символ из латинской таблицы*/
        if (KbdSwitchFlag)              /*если клавиатура в режиме LAT*/
          NewStr = ASIS_DELIM_RUR5 + NewStr;/*добавляем кавычку перед символом*/
          KbdSwitchFlag = false;        /*переводим клавиатуру в режим RUS*/
        end;
      else
        NewStr = "";
      end;
      retstr = retstr + NewStr;
    end;

    /* строка кончилась, а клавиатуру обратно не переключили */
    if (strlen(retstr) and ((KbdSwitchFlag and not RusInQuotes)or(not KbdSwitchFlag and RusInQuotes)))
      retstr = retstr + "'";
    end;
  end;
  return retStr;
END;

/* новая функция транслитерации CLEAR
   str - входящая строка
   dir - направление транслитерации
         0 - Rus->Lat
         1 - Lat->Rus
*/
MACRO TransStringCLEAR (str, dir)
  var TmpStr = str, RetStr ="",  curChr = "", NewStr = "",idx;

  If (ValType(dir) == V_UNDEF)
     Return TmpStr;
  End;
  if (dir == TRANS_DIRECTION_TO_LAT)
    TmpStr = WinToDos(StrUpr(DosToWin(Str)));
    While (StrLen(TmpStr) > 0)
      curChr  = SubStr(TmpStr, 1, 1);
      TmpStr  = Substr(TmpStr, 2);
      /*не надо транслитерить */
      if (index(LAT_UPPER_ALPHA_CLEAR, curChr))
         NewStr = curChr;
      /* надо транслитерить и символ "буква"*/
      elif (idx = index(CLEAR_RUS_SW, curChr + " "))
        NewStr = trim(substr(CLEAR_LAT_SW, idx, 2));
      else
        NewStr = "";
      end;
      retstr = retstr + NewStr;
    end;
  else
    retStr = TmpStr;
  end;
  return retStr;
END;

Class TMTMenuRow(_Owner, _Name)
   Var Name = _Name, Owner = SafeGetWeakRef(_Owner);
End;

Class TMTMenu()
   Var Rows = TArray();

   Macro AddRow(Row)
      Rows.Value(Rows.Size) = Row;
      Return Rows.Size - 1;
   End;
End;

Class TMTErrorStreamMsgBox()
   Macro ShowMsg(_Msg)
      MsgBox(_Msg);
   End;
End;

Class TMTErrorStreamPtk()
   Macro ShowMsg(_Msg)
      AddPtk(_Msg);
   End;
End;

/*----------------------------------------------------------------------------*/
Macro IntTo0Str(Str, Len)
   Var Val = Str, Exp = 1;
   While (Len > 0)
      Exp = Exp * 10;
      Len = Len - 1;
   End;
   Return SubStr(String(Exp + Val), 2);
End;

Class TSerializer(_Owner, _Param)
   Var Str;
   Var Owner = SafeGetWeakRef(_Owner), Param = _Param;
   Var Storage;

   Macro OutSubFields(Flds)
      Var i = 0, Pos = Index(Flds, ","), SubFld, TmpSubFld;
      Str = "";
      While (Pos != 0)
         SubFld = SubStr(Flds, 1, Pos - 1);
         Flds = SubStr(Flds, Pos + 1);
         TmpSubFld = Owner.GetSubField(SubFld);
         If ((ValType (TmpSubFld) == V_GENOBJ ) AND NOT TmpSubFld.IsEmpty())
            Str = Str + FIELD_STRING_SPLITTER + TmpSubFld.Value;
         End;
         Pos = Index(Flds, ",");
      End;
      TmpSubFld = Owner.GetSubField(Flds);
      If ( (ValType (TmpSubFld) == V_GENOBJ ) AND
           NOT TmpSubFld.IsEmpty())
         Str = Str + FIELD_STRING_SPLITTER + TmpSubFld.Value;
      End;
      Str = Trim(Str);
      Return Str;
   End;

   Macro SplitMultyStr(Str, Ar)
      Var i = 0, Pos = Index(Str, FIELD_STRING_SPLITTER), SubFld;
      ASize(Ar, 0);
      While (Pos != 0)
         SubFld = SubStr(Str, 1, Pos - 1);
         Str = SubStr(Str, Pos + StrLen(FIELD_STRING_SPLITTER));
         If (Trim(SubFld) != "")
            Ar(i) = SubFld;
            i = i + 1;
         End;
         Pos = Index(Str, FIELD_STRING_SPLITTER);
      End;
      If (Trim(Str) != "")
         Ar(i) = Str;
      End;
   End;

   Macro IsMyStorage(_Storage, _Param)
      If (ValType(_Storage) != V_UNDEF)
         Storage = _Storage;
      End;
      If (ValType(Storage) == V_UNDEF)
         Return False;
      End;
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Return False;
   End;

   Macro SerializeUp(_Storage, _Param)
      Return False;
   End;
End;

/*----------------------------------------------------------------------------*/

Class TScrRow(Num, Opt, Val, Scr, Obj)
   Var FldNumber = Num, FldOption = Opt, FldValue = Val, FldObject = Obj,
       FldSubscr = Scr;
End;

Class (TSerializer) TSwiftFormSerializer(_Owner)
   Macro SerializeDown(_Storage, _Param)
      Var i = 0;
      While (i < Owner.ArrayValue.Size)
         If (Not Owner.ArrayValue.Value(i).SerializeDown(_Storage))
            SetParm(1, _Storage);
            Return False;
         End;
         i = i + 1;
      End;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeUp(_Storage, _Param)
      Var i = 0;
      While (i < Owner.ArrayValue.Size)
         If (Not Owner.ArrayValue.Value(i).SerializeUp(_Storage))
            SetParm(1, _Storage);
            Return False;
         End;
         i = i + 1;
      End;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTSerializer(_Owner);
End;

Class (TSwiftFormSerializer) TSwiftFormToArrayOfScrRow(_Owner)
   Macro IsMyStorage(_Storage, _Param)
      If (Not IsMyStorage(_Storage, _Param))
         Return False;
      End;
      If (StrUpr(GenClassName(Storage)) == "TARRAYASBTRTABLE")
         Return True;
      End;
      Return False;
   End;
   InitTSwiftFormSerializer(_Owner);
End;

Class (TSwiftFormSerializer) TSwiftFormSerializerMenu(_Owner)
   Macro IsMyStorage(_Storage, _Param)
      If (Not IsMyStorage(_Storage, _Param))
         Return False;
      End;
      If (StrUpr(GenClassName(Storage)) == "TMTMENU")
         Return True;
      End;
      Return False;
   End;
   InitTSwiftFormSerializer(_Owner);
End;

Macro MySubStr(str, pos, len)
   Return Substr (str, pos, len);
End;

Macro MyGenPropId(Obj, Prop)
   GenGetProp(Obj, Prop);
   Return 1;
OnError()
   Return -1;
End;

ReplaceMacro("GenPropID","MyGenPropID");


Class (TSwiftFormSerializer) TSwiftFormSerializerString(_Owner)
   Macro IsMyStorage(_Storage, _Param)
      If (Not IsMyStorage(_Storage, _Param))
         Return False;
      End;
      If ((ValType(_Storage) == V_STRING)/* AND (_Param == SERIAL_MESSAGE)*/)
         Return TRUE;
      End;
      Return False;
   End;
   InitTSwiftFormSerializer(_Owner);

   Macro SerializeDown(_Storage, _Param)
      Var i = 0, Opt, OldStorageLen = StrLen(_Storage);
      While (i < Owner.ArrayValue.Size)
         Str = "";
         If (Not Owner.ArrayValue.Value(i).IsEmpty())
            If (Not Owner.ArrayValue.Value(i).SerializeDown(Str, _Param))
               SetParm(1, _Storage);
               Return False;
            Else
               If (Str != "")
                  If ((SubStr(Owner.ArrayValue.Value(i).Name, 1, 10) != "Repeatable") AND
                      (SubStr(Owner.ArrayValue.Value(i).Name, 1, 2) != "MT"))
                     _Storage = _Storage + Str;
                  Else
                     _Storage = _Storage + FIELD_STRING_SPLITTER + Str;
                  End;
               End;
            End;
         End;
         i = i + 1;
      End;
      If (OldStorageLen == 0)
         _Storage = SubStr(_Storage, 3);
      End;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeUp(InpStr, _Param)
      Var StartPos, EndPos, SecPos, Field, FieldNum, ColumnPos, FieldFound,
          FieldI = 0, FieldOption, FieldFull, InpStrFull, RetVal;
      If ((FieldI == Owner.ArrayValue.Size) AND (SubStr(Owner.Name, 1, 10) == "Repeatable"))
         Owner.AddSection();
      End;
      While ((StartPos = Index(InpStr, FIELD_DELIM)) != 0)
         If (StartPos != 1)
   /* ---------------- Wrong field starting ---------------- */
            Owner.ShowMsg("Неверная позиция "+ StartPos +" разделителя" + InpStr);
            Return FALSE;
         End;
         InpStrFull = InpStr;
         InpStr = SubStr(InpStr, 4);
         EndPos = Index(InpStr, FIELD_DELIM);
         If (EndPos != 0)
           Field = MySubStr(InpStr, 1, EndPos - 1);
           InpStr = SubStr(InpStr, EndPos);
         Else
           Field = InpStr;
           InpStr = "";
         End;
         Field    = trim (Field);
         FieldNum = SubStr(Field, 1, 2);
         ColumnPos = Index(Field, ":");

         If (ColumnPos == 3)
           FieldOption = NO_FIELD_OPTION;
         Elif (ColumnPos == 4)
           FieldOption = SubStr(Field, 3, 1);
         Else
   /* ---------------- Wrong field column position ---------------- */
           Owner.ShowMsg("Неверная позиция разделителя номера поля. Строка = " + Field + " Позиция = " +ColumnPos);
           SetParm(1, ""); /* Иначе зацикливание */
           Return FALSE;
         End;
         FieldFull = ":" + Field;
         Field = SubStr(Field, ColumnPos + 1);
         FieldFound = FALSE;

         While ((FieldI < Owner.ArrayValue.Size) AND (NOT FieldFound))
          /*  Owner.ShowMsg(Owner.ArrayValue.Value(FieldI).Name + " "   + FieldNum);
            Owner.ShowMsg(Owner.ArrayValue.Value(FieldI).Option + " " + FieldOption);
           */
            If ((Owner.ArrayValue.Value(FieldI).Name == FieldNum) AND
                (MyGenPropId(Owner.ArrayValue.Value(FieldI), "AvailableOptions") != -1) AND
                (Index(Owner.ArrayValue.Value(FieldI).AvailableOptions, FieldOption) > 0))
               Owner.ArrayValue.Value(FieldI).Option = FieldOption;
               If (NOT Owner.ArrayValue.Value(FieldI).SerializeUp(Field, _Param))
   /* ---------------- Wrong field format ---------------- */
                  Owner.ArrayValue.Value(FieldI).Clear();
                  Owner.ShowMsg("Неверный формат поля " + FieldNum + FieldOption);
                  If (ValType(Owner.Owner) != V_UNDEF)
                     Return WRONG_FIELD_RETVAL;
                  Else
                     Return FALSE;
                  End;
               End;
               FieldI = FieldI + 1;
               FieldFound = TRUE;
            Elif ((SubStr(Owner.ArrayValue.Value(FieldI).Name, 1, 10) == "Repeatable") OR
                  ((SubStr(Owner.ArrayValue.Value(FieldI).Name, 1, 2) == "MT") AND
                   (ValType(Owner.Owner) != V_UNDEF)))
               RetVal = Owner.ArrayValue.Value(FieldI).SerializeUp(InpStrFull, _Param);
               If (RetVal == WRONG_FIELD_RETVAL)
   /* ---------------- Wrong field format ---------------- */
                  If (ValType(Owner.Owner) != V_UNDEF)
                     Return WRONG_FIELD_RETVAL;
                  Elif (InpStrFull == "")
                     Return REPEATABLE_END;
                  Else
                     Owner.ShowMsg("Присутствует неверное поле " + FieldNum + FieldOption);
                     Return FALSE;
                  End;
               Elif (RetVal == REPEATABLE_END)
                  FieldI = Owner.ArrayValue.Size;
/*                  FieldFound = FALSE;*/
               Elif (RetVal == NOT_FIELD_RETVAL)
                  FieldI = FieldI + 1;
                  InpStr = InpStrFull;
                  FieldFound = TRUE;
               Elif (RetVal == IMPORTED_FIELD_RETVAL)
                  FieldFound = TRUE;
                  InpStr = InpStrFull;
/*                  FieldI = FieldI + 1;*/
               Else
                  Owner.ShowMsg("Вернули неизвестное значение " + RetVal);
                  Return FALSE;
               End;
            Elif (Owner.ArrayValue.Value(FieldI).Type == FIELD_MANDATORY)
/* ---------------- Mandatory field not present ---------------- */
               If ((ValType(Owner.Owner) == V_UNDEF)/* OR
                   (AND Owner.IsEmpty())*/)
                  Owner.ShowMsg("Пропущено обязательное поле " + Owner.ArrayValue.Value(FieldI).Name);
                  Return FALSE;
               Elif (Owner.IsEmpty())
                  Return REPEATABLE_END;
               Else
                  Return NOT_FIELD_RETVAL;
               End;
            /* копия полей */
            elif (Owner.ArrayValue.Value(FieldI).Name == "FIELDSCOPY")
               Owner.ArrayValue.Value(FieldI).SerializeUp(trim(InpStrFull), _Param);
               If (ValType(Owner.Owner) != V_UNDEF)
                 SetParm(1, InpStr);
               End;
               return true;
            Else
               FieldI = FieldI + 1;
            End;
         End;
         If (NOT FieldFound)
   /* ---------------- Illegal field present ---------------- */
            If ((SubStr(Owner.Name, 1, 10) == "Repeatable") OR
                ((SubStr(Owner.Name, 1, 2) == "MT") AND (ValType(Owner.Owner) != V_UNDEF)))
               SetParm(1, InpStrFull);
               Return NOT_FIELD_RETVAL;
            Else
               Owner.ShowMsg("Присутствует неверное поле " + FieldNum + FieldOption);
               Return FALSE;
            End;
         End;

         If ((FieldI == Owner.ArrayValue.Size) AND (SubStr(Owner.Name, 1, 10) == "Repeatable"))
            Owner.AddSection();
         End;
      End;
      While (FieldI < Owner.ArrayValue.Size)
         If (MyGenPropID(Owner.ArrayValue.Value(FieldI), "Type") != -1)
            If (Owner.ArrayValue.Value(FieldI).Type == FIELD_MANDATORY)
   /* ---------------- Mandatory field not present ---------------- */
               Owner.ShowMsg("Отсутствует обязательное поле " + Owner.ArrayValue.Value(FieldI).Name);
               Return FALSE;
            End;
         Elif (Owner.MinEntry > Owner.ArrayValue.Size)
            Owner.ShowMsg("Отсутствует обязательная секция " + Owner.ArrayValue.Value(FieldI).Name);
            Return FALSE;
         End;
         FieldI = FieldI + 1;
      End;
      If (ValType(Owner.Owner) != V_UNDEF)
         SetParm(1, InpStr);
      End;
      Return TRUE;
   End;
End;
/*----------------------------------------------------------------------------*/
Class (TSerializer) TMTFieldSerializerString(_Owner)
   Var TransSet = NULL;
   Var TransDirection = NULL;
   Var TransSetTable = NULL;
   Var Translated = NULL;

   Var ASIS_DELIM, DOUBLE_ASIS_DELIM, DOUBLE_ASIS_RN_DELIM,
       DOUBLE_ASIS_RN_2SLASH_DELIM, ASIS_RN_SLASH_DELIM;

   Macro SetTransSetAndDirection(TSet, TDir)
      TransSet = TSet;
      If (TSet == TRANS_SET_RUR4)
         ASIS_DELIM = ASIS_DELIM_RUR4;
      Elif (TSet == TRANS_SET_RUR5)
         ASIS_DELIM = ASIS_DELIM_RUR5;
      Elif (TSet == TRANS_SET_CLEAR)
         ASIS_DELIM = "";
      End;

      If (StrLen(ASIS_DELIM) != 0)
         DOUBLE_ASIS_DELIM = ASIS_DELIM + ASIS_DELIM;
         DOUBLE_ASIS_RN_DELIM = ASIS_DELIM + FIELD_STRING_SPLITTER + ASIS_DELIM;
         DOUBLE_ASIS_RN_2SLASH_DELIM = ASIS_DELIM + FIELD_STRING_SPLITTER + "//" + ASIS_DELIM;
         ASIS_RN_SLASH_DELIM = ASIS_DELIM + FIELD_STRING_SPLITTER + "/";
      Else
         DOUBLE_ASIS_DELIM = "";
         DOUBLE_ASIS_RN_DELIM = "";
         DOUBLE_ASIS_RN_2SLASH_DELIM = "";
         ASIS_RN_SLASH_DELIM = "";
      End;

      TransDirection = TDir;
      If ((TDir < 0) OR (TDir > 1))
         TransDirection = NULL;
      End;
      If (ValType(TransDirection) != V_UNDEF)
         Translated = 1 - TransDirection;
      Else
         Translated = -1;
      End;
   End;

   Macro ChSubStr(_Str, _Pos, _Len ,_NewSubStr)
      Return SubStr(_Str, 1, _Pos - 1) + _NewSubStr + SubStr(_Str, _Pos + _Len);
   End;

   Macro CutAsisDelims(_Str)
      Var DelimPos, EndPos, KeyWord;
      If (StrLen(ASIS_DELIM) == 0)
         Return _Str;
      End;
      While (TRUE)
         If ((DelimPos = Index(_Str, DOUBLE_ASIS_DELIM)) > 0)
            _Str = ChSubStr(_Str, DelimPos, StrLen(DOUBLE_ASIS_DELIM), "");
         Elif ((DelimPos = Index(_Str, DOUBLE_ASIS_RN_DELIM)) > 0)
            _Str = ChSubStr(_Str, DelimPos, StrLen(DOUBLE_ASIS_RN_DELIM), FIELD_STRING_SPLITTER);
         Elif ((DelimPos = Index(_Str, DOUBLE_ASIS_RN_2SLASH_DELIM)) > 0)
            _Str = ChSubStr(_Str, DelimPos, StrLen(DOUBLE_ASIS_RN_2SLASH_DELIM), FIELD_STRING_SPLITTER + "//");
         Elif ((DelimPos = Index(_Str, ASIS_RN_SLASH_DELIM)) > 0)
            EndPos = Index(SubStr(_Str, DelimPos + StrLen(ASIS_RN_SLASH_DELIM)), "/" + ASIS_DELIM);
            If (EndPos > 0)
               KeyWord = SubStr(_Str, DelimPos + StrLen(ASIS_RN_SLASH_DELIM) - 1, EndPos + 1);
               _Str = ChSubStr(_Str, DelimPos, StrLen(ASIS_RN_SLASH_DELIM) + StrLen(KeyWord) + StrLen(ASIS_DELIM) - 1, FIELD_STRING_SPLITTER + KeyWord);
            Else
               _Str = ChSubStr(_Str, DelimPos, StrLen(ASIS_RN_SLASH_DELIM), ASIS_DELIM + " " + FIELD_STRING_SPLITTER + "/");
            End;
         Else
            While ((DelimPos = Index(_Str, " " + FIELD_STRING_SPLITTER)) > 0)
               _Str = ChSubStr(_Str, DelimPos, 3, FIELD_STRING_SPLITTER);
            End;
            Return _Str;
         End;
      End;
   End;

   Macro SplitStrByRN(Ar, S)
      Var i = 0, Ind;
      Ar.Size = 0;
      While ((Ind = Index(S, FIELD_STRING_SPLITTER)) > 0)
         Ar.Value(i) = SubStr(S, 1, Ind - 1);
         S = SubStr(S, Ind + StrLen(FIELD_STRING_SPLITTER));
         i = i + 1;
      End;
      Ar.Value(i) = S;
   End;

   Macro SplitSubFields(_S, _Rest)
      Macro NormStr(S)
         If ((SubStr(S, 1, 1) == ":") OR (SubStr(S, 1, 1) == "-"))
            S = " " + S;
         End;
         Return S;
      End;

      Macro Min(p1, p2)
         If (p1 > p2)
            Return p2;
         End;
         Return p1;
      End;

      Var Counter = 0, Len, StrEnd, FirstSym, i;
      Var SubFldsAr = TArray();

      SplitStrByRN(SubFldsAr, _S);
      While (Counter < SubFldsAr.Size)
         If ((Len = StrLen(SubFldsAr.Value(Counter))) > 35)
            StrEnd = SubStr(SubFldsAr.Value(Counter), 36);
            SubFldsAr.Value(Counter) = SubStr(SubFldsAr.Value(Counter), 1, 35);
            FirstSym = SubStr(StrEnd, 1, 1);
            If (ValType(SubFldsAr.Value(Counter + 1)) == V_UNDEF)
               If (Owner.Name == "72")
                  SubFldsAr.Value(Counter + 1) = "//" + StrEnd;
               Else
                  SubFldsAr.Value(Counter + 1) = NormStr(StrEnd);
               End;
            Elif (SubStr(SubFldsAr.Value(Counter + 1), 1, 2) == "//")
               SubFldsAr.Value(Counter + 1) = "//" + StrEnd + SubStr(SubFldsAr.Value(Counter + 1), 3);
            Elif (SubStr(SubFldsAr.Value(Counter + 1), 1, 1) == "/")
               Owner.ShiftArrayRight(SubFldsAr, Counter + 1, 1);
               If (Owner.Name == "72")
                  SubFldsAr.Value(Counter + 1) = "//" + StrEnd;
               Else
                  SubFldsAr.Value(Counter + 1) = NormStr(StrEnd);
               End;
            Else
               SubFldsAr.Value(Counter + 1) = NormStr(StrEnd) + SubFldsAr.Value(Counter + 1);
            End;
         End;
         Counter = Counter + 1;
      End;

      SetParm(2, "");
      If (Owner.Name == "70")
         If (SubFldsAr.Size > 4);
            Counter = 5; _Rest = SubFldsAr.Value(4);
            While (Counter < SubFldsAr.Size)
               _Rest = _Rest + " " + SubFldsAr.Value(Counter);
               Counter = Counter + 1;
            End;
            SetParm(2, _Rest);
         End;
         SubFldsAr.Size = Min(4, SubFldsAr.Size);
      Elif (Owner.Name == "72")
         SubFldsAr.Size = Min(6, SubFldsAr.Size);
      Elif (Owner.Name == "77")
        SubFldsAr.Size = Min(3, SubFldsAr.Size);
      Else
         If (Owner.Option == "B")
            If (SubStr(SubFldsAr.Value(0), 1, 1) == "/")
               SubFldsAr.Size = Min(2, SubFldsAr.Size);
            Else
               SubFldsAr.Size = Min(1, SubFldsAr.Size);
            End;
         Elif ((Owner.Option == "D") OR
               (Owner.Option == "F") OR
               (Owner.Option == "K") OR
               (Owner.Option == NO_FIELD_OPTION))
            If (SubStr(SubFldsAr.Value(0), 1, 1) == "/")
               SubFldsAr.Size = Min(5, SubFldsAr.Size);
            Else
               SubFldsAr.Size = Min(4, SubFldsAr.Size);
            End;
         End;
      End;

      Counter = 1; _S = SubFldsAr.Value(0);
      While (Counter < SubFldsAr.Size)
         _S = _S + FIELD_STRING_SPLITTER + SubFldsAr.Value(Counter);
         Counter = Counter + 1;
      End;

      SetParm(1, _S);
   End;

   Macro TransString(_Str)
      Var NewStr = "", DelimPos, StrToAdd;
      If (ValType(TransDirection) == V_UNDEF)
         Return _Str;
      End;
      If (SubStr(_Str, 1, 1) == "/")
         _Str = RN + _Str;
      End;
      /*для RUR6 пытаемся использовать новую функцию */
      if ((TransSet==TRANS_SET_RUR5 ) AND NewRUR6Mode)
          DebugOutString ("RUR6 направление транслитерации = " + string(TransDirection));
          DebugOutString ("RUR6 вход = " + _Str);
          NewStr = TransStringRur6 (_Str, TransDirection);
          DebugOutString ("RUR6 выход = " + NewStr);
          If (SubStr(NewStr, 1, 2) == RN)
            NewStr = SubStr(NewStr, 3);
          End;
          return NewStr;
      /* для Clear тоже */
      elif ((TransDirection == TRANS_DIRECTION_TO_LAT)AND(TransSet==TRANS_SET_CLEAR))
         DebugOutString ("CLEAR направление транслитерации = " + string(TransDirection));
         DebugOutString ("CLEAR вход = " + _Str);
         NewStr = TransStringClear (_Str, TransDirection);
         DebugOutString ("CLEAR выход = " + NewStr);
         If (SubStr(NewStr, 1, 2) == RN)
           NewStr = SubStr(NewStr, 3);
         End;
         return NewStr;
      end;
   End;

/* А.Берило Для корректной транслитерации строки содержащей > 1 ключ. слова
 правил М.Эсаулов */
   MACRO TransKeyWords (Str)
     var CodeWordEndPos, tmpStr = RN + Str, retval = "", idx, KValueEndPos, RNpos, begOffset= 4;
     while (strlen(tmpStr))
       CodeWordEndPos = Index(SubStr(tmpStr, begOffset), "/");
       If (CodeWordEndPos < 1) /*нет окончания ключевого слова*/
             Return Retval + (TransString(tmpStr));
       Else
          idx = Substr (tmpStr, CodeWordEndPos + begOffset);
          KValueEndPos = index (idx, RN + "/");
          if (KValueEndPos)
           RetVal = Retval +  RN + SubStr(tmpStr, 3, CodeWordEndPos + 1) +
                    TransString(SubStr(tmpStr, CodeWordEndPos + begOffset, KValueEndPos - 1));
           tmpStr = Substr (tmpStr, CodeWordEndPos + 3 + KValueEndPos);
          else
           RetVal = Retval +  RN + SubStr(tmpStr, 3, CodeWordEndPos + 1) +
                    TransString(SubStr(tmpStr, CodeWordEndPos + begOffset));
           tmpStr = "";

          end;
       End;
     end;
     return retval;
   END;

   /* транслитерация 77-го поля */
   MACRO TransFld77 (Str)
     var CodeWordEndPos, tmpStr = Str, retval = "", idx, KValueEndPos, RNpos, begOffset= 2;
     while (strlen(tmpStr))
       CodeWordEndPos = Index(SubStr(tmpStr, begOffset), "/");
       If (CodeWordEndPos < 1) /*нет окончания ключевого слова*/
             Return Retval + (TransString(tmpStr));
       Else
          idx = Substr (tmpStr, CodeWordEndPos + begOffset);
          KValueEndPos = index (idx, "/");
          if (KValueEndPos)
           RetVal = Retval +  SubStr(tmpStr, 1, CodeWordEndPos + 1) +
                    TransString(SubStr(tmpStr, CodeWordEndPos + begOffset, KValueEndPos - 1));
           tmpStr = Substr (tmpStr, CodeWordEndPos + 1 + KValueEndPos);
          else
           RetVal = Retval +  SubStr(tmpStr, 1, CodeWordEndPos + 1) +
                    TransString(SubStr(tmpStr, CodeWordEndPos + begOffset));
           tmpStr = "";

          end;
       End;
     end;
     return retval;
   END;

   Macro TransSubField(_SubFld)

      Var SubFld = Owner.GetSubField(_SubFld), CodeWordEndPos;
      If (ValType(SubFld) == V_UNDEF)
         Return;
      End;
      If (SubStr(SubFld.Value, 1, 1) != "/") /*если  поле начинается не со слэша*/
         If (SubFld.Name != "BIC")           /* и это не БИК, то транслитерим*/
            SubFld.Value = TransString(SubFld.Value);
         End;
      Elif (SubStr(SubFld.Value, 1, 2) == "//")  /*если с двух слэшей*/
         SubFld.Value = "//" + TransString(SubStr(SubFld.Value, 3)); /*транслетирим что за слэшами*/
      Else /*начинается только с одного слэша*/
         CodeWordEndPos = Index(SubStr(SubFld.Value, 2), "/");
         If (CodeWordEndPos < 1) /*не нашли окончания кодового слова*/
           If ((SubFld.Name != "BIC") and (SubFld.Name != "Account")) /*поле не БИК и не Счет*/
              SubFld.Value = TransString(SubFld.Value); /*транслитерим*/
           End;
         Else /*значит кодовое слово*/
            SubFld.Value = TransKeyWords (SubFld.Value) /*транслитерим как кодовое слово*/
         End;
      End;
   End;

   Macro TransSubFields(_SubFlds)

      Var _SubFld, CommaPos, TmpTransStr = "", SubFldObj, nonTransSubF ="";

      While ((CommaPos = Index(_SubFlds, ",")) > 0)
         _SubFld  = SubStr(_SubFlds, 1, CommaPos - 1);
         _SubFlds = SubStr(_SubFlds, CommaPos + 1);
         SubFldObj = Owner.GetSubField(_SubFld);
         If ( (valtype(SubFldObj) == V_GENOBJ) AND
                not SubFldObj.IsEmpty())
              /* поле транслитерируемое */
              if (not index ("ACCOUNT,BIC,PARTYIDENTIFIER,", strupr(_SubFld)+ ","))
                If (TmpTransStr != "")
                   TmpTransStr = TmpTransStr + RN + SubFldObj.Value;
                Else
                   TmpTransStr = SubFldObj.Value;
                End;
              /* поле не транслитерируемое */
              else
                nonTransSubF = nonTransSubF + SubFldObj.Value + RN;
              end;
         End;
      End;

      /*убираем завершающий RN*/
      nonTransSubF = trim(nonTransSubF);

      if ((TransSet==TRANS_SET_RUR5) AND (Owner.Name == "77"))
        TmpTransStr = TransFld77(TmpTransStr);
      elif(index(TmpTransStr, "/") == 1) /*поле начинается с кодового слова*/
       TmpTransStr = TransKeyWords (TmpTransStr);
      else
       TmpTransStr = TransString(TmpTransStr);
      end;
      if (nonTransSubF !="")
        TmpTransStr = nonTransSubF+ RN + TmpTransStr;
      end;
      Owner.SerializeUp(TmpTransStr, WITHOUT_FIELD);
   End;

   Macro IsMyStorage(_Storage, _Param)
      If (Not IsMyStorage(_Storage, _Param))
         Return False;
      End;
      If ((ValType(Storage) == V_STRING) AND (ValType(_Param) != V_UNDEF))
         Return TRUE;
      End;
      Return FALSE;
   End;

   Macro SerializeDown(_S, _P)

      If ((_P == WITH_FIELD_RUR4) OR (_P == WITH_FIELD_RUR5) OR (_P == WITH_FIELD_CLEAR) OR
          (_P == WITH_FIELD_RUR4_CYR) OR (_P == WITH_FIELD_RUR5_CYR) OR (_P == WITH_FIELD_CLEAR_CYR))
         _P = WITH_FIELD;
      End;
      If ((_P == WITH_FIELD) AND (Trim(_S) != ""))
         _S = FIELD_DELIM + Owner.Name + Owner.GetPrintableOption + ":" + _S;
      Elif (_P == TO_TEMPLATE)
         If (Owner.IsTemplateble)
            _S = FIELD_DELIM + Owner.Name + Owner.GetPrintableOption + ":" + _S;
         End;
      Elif (_P == WITHOUT_FIELD)
      Else
         _S = "";
      End;

      SetParm(1, _S);
   End;

   InitTSerializer(_Owner);
End;

Class (TMTFieldSerializerString) TMTField13SerializerString(_Owner)
   Macro SerializeDown(_Storage, _Param)
      Var HH, MM, DD, MN, YY;
      If (Owner.Option == "C")
         Str = "/" + Owner.GetSubField("TimeCode").Value + "/";
      else
        DateSplit(Owner.GetSubField("Date").Value, DD, MN, YY);
        YY = String(10000 + YY);
        DD = String(100 + DD);
        MN = String(100 + MN);
        Str = SubStr(YY, 4, 2) + SubStr(MN, 2, 2) + SubStr(DD, 2, 2);
      End;
      TimeSplit(Owner.GetSubField("TimeIndication").Value, HH, MM);
      Str = Str + IntTo0Str(HH, 2) + IntTo0Str(MM, 2)
                + Owner.GetSubField("TimeSign").Value;
      TimeSplit(Owner.GetSubField("TimeOffset").Value, HH, MM);
      Str = Str + IntTo0Str(HH, 2) + IntTo0Str(MM, 2);
      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeUp(_Storage, _Param)
      Var DD, MN, YY;
      If (Owner.Option == "C")
         If ((SubStr(_Storage, 1, 1) == "/") AND (SubStr(_Storage, 9, 1) == "/"))
            Owner.GetSubField("TimeCode").Value = SubStr(_Storage, 2, 7);
         Else
            Owner.ShowMsg("Неверный формат 13-го поля");
            Return FALSE;
         End;
         Owner.GetSubField("TimeIndication").Value = Time(Int(SubStr(_Storage, 10, 2)),
                                                          Int(SubStr(_Storage, 12, 2)),
                                                          0);
         Owner.GetSubField("TimeSign").Value = SubStr(_Storage, 14, 1);
         Owner.GetSubField("TimeOffset").Value = Time(Int(SubStr(_Storage, 15, 2)),
                                                      Int(SubStr(_Storage, 17, 2)),
                                                      0);
      elif(Owner.Option == "D")
         DD = Int(SubStr(_Storage, 5, 2));
         MN = Int(SubStr(_Storage, 3, 2));
         YY = Int(SubStr(_Storage, 1, 2));
         If (YY < 80)
            YY = 2000 + YY;
         Else
            YY = 1900 + YY;
         End;
         If ((DD == 0) AND (MN == 0) AND (YY == 2000))
            YY = 0;
         End;
         Owner.GetSubField("Date").Value = Date(DD, MN, YY);
         Owner.GetSubField("TimeIndication").Value = Time(Int(SubStr(_Storage, 7, 2)),
                                                          Int(SubStr(_Storage, 9, 2)),
                                                          0);
         Owner.GetSubField("TimeSign").Value = SubStr(_Storage, 11, 1);
         Owner.GetSubField("TimeOffset").Value = Time(Int(SubStr(_Storage, 12, 2)),
                                                      Int(SubStr(_Storage, 14, 2)),
                                                      0);
      End;
      Return TRUE;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField19SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var DD, MM;
      DD = _Storage;
      MM = Index(DD, ",");
      If (MM != 0)
         DD = SubStr(DD, 1, MM - 1) + "." + SubStr(DD, MM + 1);
      End;
      Owner.GetSubField("Amount").Value = Double(DD);
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var  S = String(Owner.GetSubField("Amount").Value:0:2), Pos;
      Pos = Index(S, ".");
      If (Pos != 0)
         S = SubStr(S, 1, Pos - 1) + "," + SubStr(S, Pos + 1);
      End;
      Str =S;

      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField20SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var StrD, StrNum;
      File gf("genref.dbt","swift.def") write;

      If (IsBank)
         If (NOT Open(gf, PALATADBDIR_MT + "\\genref.dbt"))
            Owner.ShowMsg("Не могу открыть файл " + PALATADBDIR_MT + "\\genref.dbt");
            Return FALSE;
         End;
      End;

      Owner.GetSubField("Reference").Value = _Storage;
      SetParm(1, _Storage);
      If (Owner.GetSubField("Reference").Value == "?")
         gf.Form=Int(SubStr(Owner.Owner.Name, 3, 3));
         gf.Type="S";
         gf.Num=2000000000;

         If (GetLE(gf) AND (gf.Type=="S") AND (gf.Form==Int(SubStr(Owner.Owner.Name, 3, 3))))
            gf.Num=gf.Num+1;
         Else
            gf.Type="S";
            gf.Form=Int(SubStr(Owner.Owner.Name, 3, 3));
            gf.Num=1;
         End;

         If (NOT Insert(gf))
            Return FALSE;
         Else
            strNum=String(gf.Num);
            If (Strlen(strNum)>=3)
               strNum=Substr(strNum,Strlen(strNum)-2);
            Else
               strNum=MkStr("0", 3 - StrLen(strNum)) + strNum;
            End;

            strd=trim(String({curdate}));
            If (Strlen(strd)<10)
               strd="0"+strd;
            End;
            strd=SubStr(strd,9,2)+SubStr(strd,4,2)+SubStr(strd,1,2);

            Owner.GetSubField("Reference").Value = String("S")+
                                                   strd+
                                                   String(Int(SubStr(Owner.Owner.Name, 3, 3)))+
                                                   "D"+strNum;
         end;
      End;
      Return TRUE;
   End;

   Macro SerializeDown(_Storage, _Param)
      Str = Trim(Owner.GetSubField("Reference").Value);

      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField21SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var StrD, StrNum;

      Owner.GetSubField("Reference").Value = _Storage;
      SetParm(1, _Storage);
      Return TRUE;
   End;

   Macro SerializeDown(_Storage, _Param)
      Str = Trim(Owner.GetSubField("Reference").Value);
      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField23SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      If (Owner.Option == "B")
         Owner.GetSubField("BankOperationCode").Value = _Storage;
      Elif (Owner.Option == NO_FIELD_OPTION)
         Owner.GetSubField("BankOperationCode").Value = _Storage;
      Elif (Owner.Option == "E")
         Owner.GetSubField("23EInstructionCode").Value = SubStr(_Storage, 1, 4);
         If ((SubStr(_Storage, 5, 1) == "/") OR (Trim(SubStr(_Storage, 5)) == ""))
            Owner.GetSubField("23EAdditionalInfo").Value = SubStr(_Storage, 5);
         Else
            Return False;
         End;
      End;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      If (Owner.Option == "B")
         Str = Owner.GetSubField("BankOperationCode").Value;
      Elif (Owner.Option == "E")
         Str = Owner.GetSubField("23EInstructionCode").Value
             + Owner.GetSubField("23EAdditionalInfo").Value;
      End;
      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField25SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var StrD, StrNum;

      Owner.GetSubField("Account").Value = _Storage;
      SetParm(1, _Storage);
      Return TRUE;
   End;

   Macro SerializeDown(_Storage, _Param)
      Str = Owner.GetSubField("Account").Value;
      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField26SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Owner.GetSubField("TransactionTypeCode").Value = _Storage;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Str = Owner.GetSubField("TransactionTypeCode").Value;
      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField28SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var SlashPos;
      If (Owner.Option == "C")
         SlashPos = Index(_Storage, "/");
         If (SlashPos > 0)
            Owner.GetSubField("StatementNumber").Value = Int(SubStr(_Storage, 1, SlashPos - 1));
            Owner.GetSubField("SequenceNumber").Value = Int(SubStr(_Storage, SlashPos + 1));
         Else
            Owner.GetSubField("StatementNumber").Value = Int(_Storage);
            Owner.GetSubField("SequenceNumber").Value = 0;
         End;
      End;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Str = Owner.GetSubField("StatementNumber").Value;
      If (NOT Owner.GetSubField("SequenceNumber").IsEmpty())
         Str = Str + "/" + Owner.GetSubField("SequenceNumber").Value;
      End;
      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField32SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var DD, MM, YY;
      If ((Owner.Option == "A") or (Owner.Option == "D"))
         DD = Int(SubStr(_Storage, 5, 2));
         MM = Int(SubStr(_Storage, 3, 2));
         YY = Int(SubStr(_Storage, 1, 2));
         If (YY < 80)
            YY = 2000 + YY;
         Else
            YY = 1900 + YY;
         End;
         If ((DD == 0) AND (MM == 0) AND (YY == 2000))
            YY = 0;
         End;
         Owner.GetSubField("ValueDate").Value = Date(DD, MM, YY);
         Owner.GetSubField("Code_Currency").Value = SubStr(_Storage, 7, 3);
         If (Owner.GetSubField("Code_Currency").Value == "RUR")
            Owner.GetSubField("Code_Currency").Value = RussiaCurrencyCode;
         End;
         DD = SubStr(_Storage, 10);
         MM = Index(DD, ",");
         If (MM != 0)
            DD = SubStr(DD, 1, MM - 1) + "." + SubStr(DD, MM + 1);
         End;
         Owner.GetSubField("Amount").Value = Double(DD);
      End;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var  DD, MM, YY, S = String(Owner.GetSubField("Amount").Value:0:2), Pos;
      Str = "";

      If ((Owner.Option == "A") or (Owner.Option == "D"))
         Pos = Index(S, ".");
         If (Pos != 0)
            S = SubStr(S, 1, Pos - 1) + "," + SubStr(S, Pos + 1);
         End;
         DateSplit(Owner.GetSubField("ValueDate").Value, DD, MM, YY);
         YY = String(10000 + YY);
         DD = String(100 + DD);
         MM = String(100 + MM);
         Str = SubStr(YY, 4, 2) + SubStr(MM, 2, 2) + SubStr(DD, 2, 2) +
               Owner.GetSubField("Code_Currency").Value + S;
      End;
      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField32BSerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var DD, MM, YY;
      If (Owner.Option == "B")
         Owner.GetSubField("Code_Currency").Value = SubStr(_Storage, 1, 3);
         If (Owner.GetSubField("Code_Currency").Value == "RUR")
            Owner.GetSubField("Code_Currency").Value = RussiaCurrencyCode;
         End;
         DD = SubStr(_Storage, 4);
         MM = Index(DD, ",");
         If (MM != 0)
            DD = SubStr(DD, 1, MM - 1) + "." + SubStr(DD, MM + 1);
         End;
         Owner.GetSubField("Amount").Value = Double(DD);
      End;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var  DD, MM, YY, S = String(Owner.GetSubField("Amount").Value:0:2), Pos;
      Str = "";

      If (Owner.Option == "B")
         Pos = Index(S, ".");
         If (Pos != 0)
            S = SubStr(S, 1, Pos - 1) + "," + SubStr(S, Pos + 1);
         End;
         Str = Owner.GetSubField("Code_Currency").Value + S;
      End;
      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField33SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var DD, MM;
      If (Owner.Option == "B")
         Owner.GetSubField("Code_Currency").Value = SubStr(_Storage, 1, 3);
         If (Owner.GetSubField("Code_Currency").Value == "RUR")
            Owner.GetSubField("Code_Currency").Value = RussiaCurrencyCode;
         End;
         DD = SubStr(_Storage, 4);
         MM = Index(DD, ",");
         If (MM != 0)
            DD = SubStr(DD, 1, MM - 1) + "." + SubStr(DD, MM + 1);
         End;
         Owner.GetSubField("Amount").Value = Double(DD);
      End;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var S, Pos;
      Str = "";

      If (Owner.Option == "B")
         S = String(Owner.GetSubField("Amount").Value:0:2);
         Pos = Index(S, ".");
         If (Pos != 0)
            S = SubStr(S, 1, Pos - 1) + "," + SubStr(S, Pos + 1);
         End;
         Str = Owner.GetSubField("Code_Currency").Value + S;
      End;
      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField36SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var DD, MM;
      DD = _Storage;
      MM = Index(DD, ",");
      If (MM != 0)
         DD = SubStr(DD, 1, MM - 1) + "." + SubStr(DD, MM + 1);
      End;
      Owner.GetSubField("Rate").Value = Double(DD);
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var Pos;
      Str = String(Owner.GetSubField("Rate").Value);

      Pos = Index(Str, ".");
      If (Pos != 0)
         Str = SubStr(Str, 1, Pos - 1) + "," + SubStr(Str, Pos + 1);
      End;
      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTFieldMSSerializerString(_Owner)
   Var SubFieldsStr;
   Macro GetSTR(Str)
      If (ValType(Str) == V_UNDEF)
         Return "";
      End;
      Return Str;
   End;
   /* AB 17.11.09
    является ли строка строкой символов
    функция добавлена из за неясности назначения
    IsAlphaStr в mtcomcls
   */
   MACRO IsAlphaStr ()
      var i=1, len = strlen(str), ccode;
      while(i <= len)
       ccode = CodeFor(substr(str,i,1));
       if((ccode < 65) OR
         ((ccode>90) AND (ccode<97)) OR
         (ccode > 122))
        return false;
       end;
       i=i+1;
      end;
      return true;
   END;
   Macro SerializeUp(_Storage, _Param)
      ARRAY Ar;
      Var PI = Owner.GetSubField("PartyIdentifier");
      Var Account = Owner.GetSubField("Account");
      Var CurStr = 0;

      SplitMultyStr(_Storage, Ar);
      If ((SubStr(Ar(CurStr), 1, 1) == "/") OR
          /*AB 17.11.09 в Party Identifier 57-го поля может быть национальный идентиывикатор с 2-я // */
          ((SubStr(Ar(CurStr), 1, 2) == "//") AND IsAlphaStr (SubStr(Ar(CurStr), 3, 2))) OR
          (Owner.Option == "F"))
         If (ValType(PI) != V_UNDEF)
            PI.Value = Ar(CurStr);
         Elif (ValType(Account) != V_UNDEF)
            Account.Value = Ar(CurStr);
         Elif (Owner.Option != NO_FIELD_OPTION)
            Owner.ShowMsg("Неверная строка в поле " + Owner.Name + " - " + Owner.Option);
            Return FALSE;
         Else
            CurStr = -1;
         End;
         CurStr = CurStr + 1;
      End;

      If (Owner.Option == "A")
         Owner.GetSubField("BIC").Value = GetSTR(Ar(CurStr));
         Return TRUE;
      Elif (Owner.Option == "B")
         Owner.GetSubField("Location").Value = GetSTR(Ar(CurStr));
         Return TRUE;
      Elif (Owner.Option == "C")
         Return TRUE;
      Elif ((Owner.Option == "D") OR (Owner.Option == NO_FIELD_OPTION) OR
            (Owner.Option == "K") OR (Owner.Option == "F"))
         Owner.GetSubField("Name").Value = GetSTR(Ar(CurStr));
         Owner.GetSubField("Address").Value = GetSTR(Ar(CurStr + 1));
         Owner.GetSubField("City").Value = GetSTR(Ar(CurStr + 2));
         Owner.GetSubField("Country").Value = GetSTR(Ar(CurStr + 3));
         Return TRUE;
      Else
         Owner.ShowMsg("Неверная опция поля " + Owner.Name + " - " + Owner.Option);
         Return FALSE;
      End;

      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var Fir = "PartyIdentifier";
      Var F72Field, F72SubField, BNF, SubFldName;
      Var SaveAr = TArray(), CurStr, SaveStr = "", F70Rest = "", F70ToF72 = "";
      If (ValType(Owner.GetSubField("Account")) != V_UNDEF)
         Fir = "Account";
      End;
      If (((Int(Owner.Name) < 60) AND (Int(Owner.Name) >= 50)) OR (Int(Owner.Name) == 87))
         If ((Owner.Option == "K") OR (Owner.Option == NO_FIELD_OPTION)  OR (Owner.Option == "F"))
            SubFieldsStr = "Account";
         Elif (Owner.Option == "A")
            SubFieldsStr = Fir + ",BIC";
         Elif (Owner.Option == "B")
            SubFieldsStr = Fir;
         Elif (Owner.Option == "C")
            SubFieldsStr = Fir;
         Elif (Owner.Option == "D")
            SubFieldsStr = Fir;
         End;
         SubFieldsStr = SubFieldsStr + ",Name,Address,City,Country,Location";
      else
        if(not StrLen(SubFieldsStr))/*voikin 10.10.2014*/
          SubFieldsStr = "Row0,Row1,Row2,Row3,Row4,Row5,Row6";
        end;
      End;

      If (Owner.Name != "70")
         /*addptk (Owner.Name + " param = " + _Param );*/
         if (_Param == WITH_FIELD_RUR5)
            SetTransSetAndDirection(TRANS_SET_RUR5, TRANS_DIRECTION_TO_LAT);
            _Param = WITH_FIELD;
         Elif (_Param == WITH_FIELD_CLEAR)
            SetTransSetAndDirection(TRANS_SET_CLEAR, TRANS_DIRECTION_TO_LAT);
            _Param = WITH_FIELD;
         Elif (_Param == WITH_FIELD_RUR5_CYR)
            SetTransSetAndDirection(TRANS_SET_RUR5, TRANS_DIRECTION_TO_CYR);
            _Param = WITH_FIELD;
         Elif (_Param == WITH_FIELD_CLEAR_CYR)
            SetTransSetAndDirection(TRANS_SET_CLEAR, TRANS_DIRECTION_TO_CYR);
            _Param = WITH_FIELD;
         Else
            SetTransSetAndDirection(-1, -1);
         End;
         if (TransSet != -1)
          TransSubFields(SubFieldsStr);
         end;
      Else
         if (_Param == WITH_FIELD_RUR5)
            SetTransSetAndDirection(TRANS_SET_RUR5, TRANS_DIRECTION_TO_LAT);
            BNF = "/NZP/";
            _Param = WITH_FIELD;
         Elif (_Param == WITH_FIELD_CLEAR)
            SetTransSetAndDirection(TRANS_SET_CLEAR, TRANS_DIRECTION_TO_LAT);
            BNF = "/BNF/";
         Elif (_Param == WITH_FIELD_RUR5_CYR)
            SetTransSetAndDirection(TRANS_SET_RUR5, TRANS_DIRECTION_TO_CYR);
            BNF = "/NZP/";
            _Param = WITH_FIELD;
         Elif (_Param == WITH_FIELD_CLEAR_CYR)
            SetTransSetAndDirection(TRANS_SET_CLEAR, TRANS_DIRECTION_TO_CYR);
            BNF = "/BNF/";
            _Param = WITH_FIELD;
         Else
            SetTransSetAndDirection(-1, -1);
         End;
         If (ValType(TransDirection) != V_UNDEF)
            SaveAr.Value(0) = Owner.GetSubField("Row0").Value;
            SaveAr.Value(1) = Owner.GetSubField("Row1").Value;
            SaveAr.Value(2) = Owner.GetSubField("Row2").Value;
            SaveAr.Value(3) = Owner.GetSubField("Row3").Value;
            SaveStr = Owner.GetSubField("Row0").Value +
                      Owner.GetSubField("Row1").Value +
                      Owner.GetSubField("Row2").Value +
                      Owner.GetSubField("Row3").Value;
            CurStr = SaveStr;
            Owner.GetSubField("Row0").Value = SaveStr;
            Owner.GetSubField("Row1").Value = "";
            Owner.GetSubField("Row2").Value = "";
            Owner.GetSubField("Row3").Value = "";
            TransSubFields("Row0,Row1,Row2,Row3");
            Str = OutSubFields(SubFieldsStr);
            Str = CutAsisDelims(Str);
            SplitSubFields(Str, F70ToF72);
            While (StrLen(F70ToF72) > 0)
               F70Rest = SubStr(SaveStr, StrLen(SaveStr) - StrLen(F70Rest));
               CurStr = Owner.GetSubField("Row0").Value = SubStr(SaveStr, 1, StrLen(SaveStr) - StrLen(F70Rest));
               Owner.GetSubField("Row1").Value = "";
               Owner.GetSubField("Row2").Value = "";
               Owner.GetSubField("Row3").Value = "";
               TransSubFields("Row0,Row1,Row2,Row3");
               Str = "";
               Str = OutSubFields(SubFieldsStr);
               Str = CutAsisDelims(Str);
               SplitSubFields(Str, F70ToF72);
            End;
            If (StrLen(F70Rest) > 0)
              F72Field = Owner.Owner.GetField("72");
              If (ValType(F72Field) != V_UNDEF)
                F72SubField = F72Field.GetSubFieldBySubContent(BNF);
                If (ValType(F72SubField) != V_UNDEF)
                  SubFldName = Int(SubStr(F72SubField.Name, 4)) + 1;
                  F72Field.GetSubField("Row" + String(SubFldName - 1)).Value =
                     BNF +
                     F70Rest +
                     substr(F72Field.GetSubField("Row" + String(SubFldName - 1)).Value,6);
               Else /* \NZP\ нет будем создавать  */
                     If (F72Field.GetSubField("Row0").IsEmpty())
                        F72Field.GetSubField("Row0").Value = BNF + F70Rest;
                     Else
                        SubFldName = 5;
                        While (SubFldName > 0)
                           F72Field.GetSubField("Row" + String(SubFldName)).Value = F72Field.GetSubField("Row" + String(SubFldName - 1)).Value;
                           SubFldName = SubFldName - 1;
                        End;
                        F72Field.GetSubField("Row0").Value = BNF + F70Rest;
                     End;
                End;
              End;
            End;
            Str = OutSubFields(SubFieldsStr);
            Str = CutAsisDelims(Str);
            SplitSubFields(Str);
            SerializeDown(Str, _Param);
            _Storage = _Storage + Str;
            SetParm(1, _Storage);
            Owner.GetSubField("Row0").Value = SaveAr.Value(0);
            Owner.GetSubField("Row1").Value = SaveAr.Value(1);
            Owner.GetSubField("Row2").Value = SaveAr.Value(2);
            Owner.GetSubField("Row3").Value = SaveAr.Value(3);
            Return True;
         End;
      End;
      Str = OutSubFields(SubFieldsStr);

      If ((ValType(TransDirection) != V_UNDEF) AND (TransDirection == TRANS_DIRECTION_TO_LAT))
         Str = CutAsisDelims(Str);
         SplitSubFields(Str);
      End;

      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldMSSerializerString) TMTField50SerializerString(_Owner)
   InitTMTFieldMSSerializerString(_Owner);
End;

Class (TMTFieldMSSerializerString) TMTField59SerializerString(_Owner)
   InitTMTFieldMSSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTFieldVIPROWSerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var DD, MM, YY;
      Owner.GetSubField("DCMark").Value = SubStr(_Storage, 1, 1);
      _Storage = SubStr(_Storage, 2);

      DD = Int(SubStr(_Storage, 5, 2));
      MM = Int(SubStr(_Storage, 3, 2));
      YY = Int(SubStr(_Storage, 1, 2));
      If (YY < 80)
         YY = 2000 + YY;
      Else
         YY = 1900 + YY;
      End;
      If ((DD == 0) AND (MM == 0) AND (YY == 2000))
         YY = 0;
      End;
      Owner.GetSubField("Date").Value = Date(DD, MM, YY);
      Owner.GetSubField("Code_Currency").Value = SubStr(_Storage, 7, 3);
      If (Owner.GetSubField("Code_Currency").Value == "RUR")
         Owner.GetSubField("Code_Currency").Value = RussiaCurrencyCode;
      End;
      DD = SubStr(_Storage, 10);
      MM = Index(DD, ",");
      If (MM != 0)
         DD = SubStr(DD, 1, MM - 1) + "." + SubStr(DD, MM + 1);
      End;
      Owner.GetSubField("Amount").Value = Double(DD);
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var  DD, MM, YY, S = String(Owner.GetSubField("Amount").Value:0:2), Pos;
      Str = "";

      Pos = Index(S, ".");
      If (Pos != 0)
         S = SubStr(S, 1, Pos - 1) + "," + SubStr(S, Pos + 1);
      End;
      DateSplit(Owner.GetSubField("Date").Value, DD, MM, YY);
      YY = String(10000 + YY);
      DD = String(100 + DD);
      MM = String(100 + MM);
      Str = Owner.GetSubField("DCMark").Value +
            SubStr(YY, 4, 2) + SubStr(MM, 2, 2) + SubStr(DD, 2, 2) +
            Owner.GetSubField("Code_Currency").Value + S;
      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField61SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var DD, MM, YY;
      DD = Int(SubStr(_Storage, 5, 2));
      MM = Int(SubStr(_Storage, 3, 2));
      YY = Int(SubStr(_Storage, 1, 2));
      If (YY < 80)
         YY = 2000 + YY;
      Else
         YY = 1900 + YY;
      End;
      If ((DD == 0) AND (MM == 0) AND (YY == 2000))
         YY = 0;
      End;
      Owner.GetSubField("Date").Value = Date(DD, MM, YY);
      _Storage = SubStr(_Storage, 7);

      If (Owner.IsDigitStr(SubStr(_Storage, 1, 1)))
         DD = Int(SubStr(_Storage, 3, 2));
         MM = Int(SubStr(_Storage, 1, 2));
         Owner.GetSubField("EntryDate").Value = Date(DD, MM, YY);
         _Storage = SubStr(_Storage, 5);
      End;
      Owner.GetSubField("DCMark").Value = SubStr(_Storage, 1, 1);
      _Storage = SubStr(_Storage, 2);
      If (Owner.GetSubField("DCMark").Value == "R")
         Owner.GetSubField("DCMark").Value = Owner.GetSubField("DCMark").Value +
                                             SubStr(_Storage, 1, 1);
         _Storage = SubStr(_Storage, 2);
      End;

      If (NOT Owner.IsDigitStr(SubStr(_Storage, 1, 1)))
         Owner.GetSubField("FundsCode").Value = SubStr(_Storage, 1, 1);
         _Storage = SubStr(_Storage, 2);
      End;

      YY = Index(_Storage, FIELD_STRING_SPLITTER);
      If (YY > 0)
         Owner.GetSubField("SupplementaryDetails").Value = SubStr(_Storage, YY + 2);
         _Storage = SubStr(_Storage, 1, YY - 1);
      End;

      YY = StrBrk(_Storage, "SNF");
      If (YY > 0)
         DD = SubStr(_Storage, 1, YY - 1);
         MM = Index(DD, ",");

         If (MM != 0)
            DD = SubStr(DD, 1, MM - 1) + "." + SubStr(DD, MM + 1);
         End;
         Owner.GetSubField("Amount").Value = Double(DD);
         Owner.GetSubField("TransactionTypeIdentificationCode").Value = SubStr(_Storage, YY, 4);
         _Storage = SubStr(_Storage, YY + 4);
         If (SubStr(_Storage, 1, 1) == ".")
           _Storage = SubStr(_Storage, 2);
         End;
         YY = Index(_Storage, "//");
         If (YY > 0)
            Owner.GetSubField("Reference").Value = Trim(SubStr(_Storage, 1, YY - 1));
            Owner.GetSubField("RelatedReference").Value = Trim(SubStr(_Storage, YY + 2));
         Else
            Owner.GetSubField("Reference").Value = Trim(_Storage);
         End;
         _Storage = "";
      End;

      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var  DD, MM, YY, S = String(Owner.GetSubField("Amount").Value:0:2), Pos;
      Str = "";

      Pos = Index(S, ".");
      If (Pos != 0)
         S = SubStr(S, 1, Pos - 1) + "," + SubStr(S, Pos + 1);
      End;
      DateSplit(Owner.GetSubField("Date").Value, DD, MM, YY);
      YY = String(10000 + YY);
      DD = String(100 + DD);
      MM = String(100 + MM);
      Str = SubStr(YY, 4, 2) + SubStr(MM, 2, 2) + SubStr(DD, 2, 2);
      If (not Owner.GetSubField("EntryDate").IsEmpty())
         DateSplit(Owner.GetSubField("EntryDate").Value, DD, MM, YY);
         DD = String(100 + DD);
         MM = String(100 + MM);
         If (DD != "00")
            Str = Str + SubStr(MM, 2, 2) + SubStr(DD, 2, 2);
         End;
      End;
      Str = Str + Owner.GetSubField("DCMark").Value;
      Str = Str + Owner.GetSubField("FundsCode").Value;
      Str = Str + S;
      Str = Str + Owner.GetSubField("TransactionTypeIdentificationCode").Value;
      Str = Str + Owner.GetSubField("Reference").Value;
      DateSplit(Owner.GetSubField("EntryDate").Value, DD, MM, YY);
      If (Owner.GetSubField("RelatedReference").Value != "")
         Str = Str + "//" + Owner.GetSubField("RelatedReference").Value;
      End;
      If (Owner.GetSubField("SupplementaryDetails").Value != "")
         Str = Str + FIELD_STRING_SPLITTER + Owner.GetSubField("SupplementaryDetails").Value;
      End;
      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldVIPROWSerializerString) TMTField60SerializerString(_Owner)
   InitTMTFieldVIPROWSerializerString(_Owner);
End;

Class (TMTFieldVIPROWSerializerString) TMTField62SerializerString(_Owner)
   InitTMTFieldVIPROWSerializerString(_Owner);
End;

Class (TMTFieldVIPROWSerializerString) TMTField64SerializerString(_Owner)
   InitTMTFieldVIPROWSerializerString(_Owner);
End;

Class (TMTFieldVIPROWSerializerString) TMTField65SerializerString(_Owner)
   InitTMTFieldVIPROWSerializerString(_Owner);
End;

Class (TMTFieldMSSerializerString) TMTFieldBankInstSerializerString(_Owner)
   InitTMTFieldMSSerializerString(_Owner);
End;

/* для сериализации полей покрытия 52-57 в МТ202COV*/
Class (TMTFieldBankInstSerializerString) TMTFieldBankInstCOVSerializerString(_Owner)
  InitTMTFieldBankInstSerializerString (_Owner);
END;


Class (TMTFieldMSSerializerString) TMTField70SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      ARRAY Ar;
      Var i = 0;
      SplitMultyStr(_Storage, Ar);
      While ((i < ASize(Ar)) AND (i < 4))
         Owner.GetSubField("Row" + String(i)).Value = Ar(i);
         i = i + 1;
      End;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      SubFieldsStr = "Row0,Row1,Row2,Row3";
      SerializeDown(_Storage, _Param);
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldMSSerializerString(_Owner);
End;

Class (TMTFieldMSSerializerString) TMTField77BSerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      ARRAY Ar;
      Var i = 0;
      SplitMultyStr(_Storage, Ar);
      While ((i < ASize(Ar)) AND (i < 3))
         Owner.GetSubField("Row" + String(i)).Value = Ar(i);
         i = i + 1;
      End;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      SubFieldsStr = "Row0,Row1,Row2";
      SerializeDown(_Storage, _Param);
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldMSSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField71ASerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Owner.GetSubField("Code").Value = _Storage;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Str = Owner.GetSubField("Code").Value;
      SerializeDown(Str, _Param);
      _Storage = _Storage + Str;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldSerializerString) TMTField71FSerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var DD, MM;
      Owner.GetSubField("Code_Currency").Value = SubStr(_Storage, 1, 3);
      If (Owner.GetSubField("Code_Currency").Value == "RUR")
         Owner.GetSubField("Code_Currency").Value = RussiaCurrencyCode;
      End;
      DD = SubStr(_Storage, 4);
      MM = Index(DD, ",");
      If (MM != 0)
         DD = SubStr(DD, 1, MM - 1) + "." + SubStr(DD, MM + 1);
      End;
      Owner.GetSubField("Amount").Value = Double(DD);
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var  S = String(Owner.GetSubField("Amount").Value:0:2), Pos;

      Pos = Index(S, ".");
      If (Pos != 0)
         S = SubStr(S, 1, Pos - 1) + "," + SubStr(S, Pos + 1);
      End;
      S = Owner.GetSubField("Code_Currency").Value + S;
      SerializeDown(S, _Param);
      _Storage = _Storage + S;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldMSSerializerString) TMTField71BSerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      ARRAY Ar;
      Var i = 0, maxval =6;
      SplitMultyStr(_Storage, Ar);
      if(ASize(Ar) < maxval)
        maxval = ASize(Ar);
      end;
      While (i < maxval)
         Owner.GetSubField("Row" + String(i)).Value = Ar(i);
         i = i + 1;
      End;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      SubFieldsStr = "Row0,Row1,Row2,Row3,Row4,Row5";
      SerializeDown(_Storage, _Param);
      SetParm(1, _Storage);
      Return True;
      Return True;
   End;
   InitTMTFieldMSSerializerString(_Owner);
End;



Class (TMTFieldSerializerString) TMTField71GSerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var DD, MM;
      Owner.GetSubField("Code_Currency").Value = SubStr(_Storage, 1, 3);
      If (Owner.GetSubField("Code_Currency").Value == "RUR")
         Owner.GetSubField("Code_Currency").Value = RussiaCurrencyCode;
      End;
      DD = SubStr(_Storage, 4);
      MM = Index(DD, ",");
      If (MM != 0)
         DD = SubStr(DD, 1, MM - 1) + "." + SubStr(DD, MM + 1);
      End;
      Owner.GetSubField("Amount").Value = double(DD);
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var  S = String(Owner.GetSubField("Amount").Value:0:2), Pos;

      Pos = Index(S, ".");
      If (Pos != 0)
         S = SubStr(S, 1, Pos - 1) + "," + SubStr(S, Pos + 1);
      End;
      S = Owner.GetSubField("Code_Currency").Value + S;
      SerializeDown(S, _Param);
      _Storage = _Storage + S;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
End;

Class (TMTFieldMSSerializerString) TMTField72SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      ARRAY Ar;
      Var i = 0;
      SplitMultyStr(_Storage, Ar);
      While ((i < ASize(Ar)) AND (i < 6))
         Owner.GetSubField("Row" + String(i)).Value = Ar(i);
         i = i + 1;
      End;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      SubFieldsStr = "Row0,Row1,Row2,Row3,Row4,Row5";
      SerializeDown(_Storage, _Param);
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldMSSerializerString(_Owner);
End;

Class (TMTField72SerializerString) TMTField72COVSerializerString(_Owner)
  InitTMTField72SerializerString(_Owner);
END;


Class (TMTFieldMSSerializerString) TMTField86SerializerString(_Owner)
   Macro SerializeUp(_Storage, _Param)
      ARRAY Ar;
      Var i = 0;
      SplitMultyStr(_Storage, Ar);
      While ((i < ASize(Ar)) AND (i < 6))
         Owner.GetSubField("Row" + String(i)).Value = Ar(i);
         i = i + 1;
      End;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      SubFieldsStr = "Row0,Row1,Row2,Row3,Row4,Row5";
      SerializeDown(_Storage, _Param);
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldMSSerializerString(_Owner);
End;

CLASS (TMTFieldSerializerString) TMTField75SerializerString (_Owner)

  Macro SerializeUp(_Storage, _Param)
      ARRAY Ar;
      Var i = 0;
      SplitMultyStr(_Storage, Ar);
      While ((i < ASize(Ar)) AND (i < Owner.MaxEntry))
         Owner.GetSubField("Row" + String(i)).Value = Ar(i);
         i = i + 1;
      End;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var i = 0, brk = false, subf, str="";

      While ((i < Owner.MaxEntry) and not brk)
         subf = Owner.GetSubField("Row" + String(i));
         brk = subf.isempty();
         if (not brk )
           str = str + subf.Value + FIELD_STRING_SPLITTER;
         end;
         i = i + 1;
      End;
      SerializeDown(Str, _Param);
      if (_Param == WITH_FIELD)
       _Storage = _Storage + FIELD_STRING_SPLITTER+ trim(Str);
      else
       _Storage = _Storage + trim(Str);
      end;
      SetParm(1, _Storage);
      Return True;
/*
      SubFieldsStr = "Row0,Row1,Row2,Row3,Row4,Row5";
      SerializeDown(_Storage, _Param);
      SetParm(1, _Storage);
      Return True;
*/
   End;
   InitTMTFieldSerializerString(_Owner);
END;

CLASS (TMTField75SerializerString) TMTField76SerializerString (_Owner)
   InitTMTField75SerializerString(_Owner);
END;

CLASS (TMTField75SerializerString) TMTField77SerializerString (_Owner)
   InitTMTField75SerializerString(_Owner);
END;

CLASS (TMTField75SerializerString) TMTField11SerializerString (_Owner)
   InitTMTField75SerializerString(_Owner);
END;

CLASS (TMTField75SerializerString) TMTField79SerializerString (_Owner)
   InitTMTField75SerializerString(_Owner);
END;

CLASS (TMTFieldSerializerString) TMTFieldsCopySerializerString (_Owner)

   Macro SerializeUp(_Storage, _Param)
      ARRAY Ar;
      Var i = 0;
      Owner.GetSubField("Row0").Value = _Storage;
      SetParm(1, _Storage);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var i = 0, subf, str="";

      subf = Owner.GetSubField("Row0");
      if (not subf.isempty() )
         if (_Param == WITH_FIELD)
           _Storage = _Storage + RN + subf.Value;
         else
           _Storage = _Storage + subf.Value;
         end;
      end;
      SetParm(1, _Storage);
      Return True;
   End;
   InitTMTFieldSerializerString(_Owner);
END;

/*----------------------------------------------------------------------------*/

Class (TSerializer) TMTFieldSerializerMenuAv(_Owner)
   Macro IsMyStorage(_Storage, _Param)
      If (Not IsMyStorage(_Storage, _Param))
         Return False;
      End;
      If (StrUpr(GenClassName(Storage)) != "TMTMENU")
         Return False;
      End;
      Return True;
   End;

   Macro SerializeDown(_Storage)
      If (Owner.IsEmpty())
         _Storage.Rows.Value(_Storage.Rows.Size) = TMTMenuRow(Owner, Owner.Name + " - " + Owner.AvailableOptions);
      End;
      Return True;
   End;

   InitTSerializer(_Owner);
End;

Class (TSerializer) TMTSection13CSerializerMenuAv(_Owner)
   Macro IsMyStorage(_Storage, _Param)
      If (Not IsMyStorage(_Storage, _Param))
         Return False;
      End;
      If (StrUpr(GenClassName(Storage)) != "TMTMENU")
         Return False;
      End;
      Return True;
   End;

   Macro SerializeDown(_Storage)
      Var Fld13;
      If (Owner.MaxEntry <= Owner.ArrayValue.Size)
         Return True;
      End;
      Fld13 = GenObject("TMTField13", Owner, "TMTField13SerializerDoc", FIELD_OPTIONAL, "C", "C");
      Fld13.RegisterSerializer("TMTField13SerializerFrmScr");
      Fld13.RegisterSerializer("TMTField13SerializerString");
      Fld13.RegisterSerializer("TMTField13SerializerDialog");
      _Storage.Rows.Value(_Storage.Rows.Size) = TMTMenuRow(Fld13, Fld13.Name + " - " + Fld13.AvailableOptions);
      Return True;
   End;

   InitTSerializer(_Owner);
End;

Class (TSerializer) TMTSection23ESerializerMenuAv(_Owner)
   Macro IsMyStorage(_Storage, _Param)
      If (Not IsMyStorage(_Storage, _Param))
         Return False;
      End;
      If (StrUpr(GenClassName(_Storage)) != "TMTMENU")
         Return False;
      End;
      Return True;
   End;

   Macro SerializeDown(_Storage)
      Var Fld23E;
      If (Owner.MaxEntry <= Owner.ArrayValue.Size)
         Return True;
      End;
      Fld23E = GenObject("TMTField23E", Owner, "TMTField23SerializerDoc", FIELD_OPTIONAL);
      Fld23E.RegisterSerializer("TMTField23SerializerFrmScr");
      Fld23E.RegisterSerializer("TMTField23SerializerString");
      Fld23E.RegisterSerializer("TMTField23ESerializerDialog");
      _Storage.Rows.Value(_Storage.Rows.Size) = TMTMenuRow(Fld23E, Fld23E.Name + " - " + Fld23E.AvailableOptions);
      Return True;
   End;

   InitTSerializer(_Owner);
End;

Class (TSerializer) TMTSection71FSerializerMenuAv(_Owner)
   Macro IsMyStorage(_Storage, _Param)
      If (Not IsMyStorage(_Storage, _Param))
         Return False;
      End;
      If (StrUpr(GenClassName(_Storage)) != "TMTMENU")
         Return False;
      End;
      Return True;
   End;

   Macro SerializeDown(_Storage)
      Var Fld71F;
      If (Owner.MaxEntry <= Owner.ArrayValue.Size)
         Return True;
      End;
      Fld71F = GenObject("TMTField71F", Owner, "TMTField71FSerializerDoc", FIELD_OPTIONAL);
      Fld71F.RegisterSerializer("TMTField71FSerializerFrmScr");
      Fld71F.RegisterSerializer("TMTField71FSerializerString");
      Fld71F.RegisterSerializer("TMTField71FSerializerDialog");
      _Storage.Rows.Value(_Storage.Rows.Size) = TMTMenuRow(Fld71F, Fld71F.Name + " - " + Fld71F.AvailableOptions);
      Return True;
   End;

   InitTSerializer(_Owner);
End;

/*----------------------------------------------------------------------------*/
Class (TSwiftFormSerializer) TSwiftFormSerializerStruct(_Owner)
   Macro IsMyStorage(_Storage, _Param)
      var idx;
      If (Not IsMyStorage(_Storage, _Param))
         Return False;
      End;
//      idx = FldIndex(_Storage, "Reference");
      idx = GenPropID(_Storage, "Reference");
//      If (((ValType(idx) == V_INTEGER) AND (idx != -1)) OR ((valtype(_Storage) == V_GENOBJ) and (ValType(GenGetProp(_Storage, "Reference")) == V_STRING)))
      If (((ValType(idx) == V_INTEGER) AND (idx != -1)) OR ((valtype(_Storage) == V_GENOBJ) and (ValType(MyGenPropId(_Storage, "Reference")) == V_STRING)))
         Return TRUE;
      End;
      Return False;
   End;
   InitTSwiftFormSerializer(_Owner);
End;

Class (TSwiftFormSerializer) TSwiftFormVIPSerializerStruct(_Owner)
   Macro IsMyStorage(_Storage, _Param)
      var idx;
      If (Not IsMyStorage(_Storage, _Param))
         Return False;
      End;
//      idx = FldIndex(_Storage, "Reference");
      idx = GenPropID(_Storage, "Reference");

//      If (((ValType(idx) == V_INTEGER) AND (idx != -1)) OR ((valtype(_Storage) == V_GENOBJ) and (ValType(GenGetProp(_Storage, "Reference")) == V_STRING)))
      If (((ValType(idx) == V_INTEGER) AND (idx != -1)) OR ((valtype(_Storage) == V_GENOBJ) and (ValType(MyGenPropId(_Storage, "Reference")) == V_STRING)))
         Return TRUE;
      End;
      Return FALSE;
   End;

   Macro SerializeUp(_Storage, _Param)
      Var SerStr = FIELD_STRING_SPLITTER + _Storage.Text;
      If (NOT Owner.SerializeUp(SerStr, WITH_FIELD))
         Return FALSE;
      End;
      Return TRUE;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var SerStr = "";
      If (Owner.SerializeDown(SerStr, WITH_FIELD))
         _Storage.Text = SerStr;
      Else
         Return FALSE;
      End;
      Return TRUE;
   End;

   InitTSwiftFormSerializer(_Owner);
End;

Class (TSerializer) TMTFieldSerializerStruct(_Owner)
   Macro IsMyStorage(_Storage, _Param)
      var idx;
      If (Not IsMyStorage(_Storage, _Param))
         Return False;
      End;
      idx = FldIndex(_Storage, "Reference");
/*!!!*/
/*
      if((valtype(_Storage) == V_GENOBJ))
      end;
*/
      idx = GenPropID(_Storage, "Reference");

//      If (((ValType(idx) == V_INTEGER) AND (idx != -1)) OR ((valtype(_Storage) == V_GENOBJ) and (ValType(GenGetProp(_Storage, "Reference")) == V_STRING)))
      If (((ValType(idx) == V_INTEGER) AND (idx != -1)) OR ((valtype(_Storage) == V_GENOBJ) and (ValType(MyGenPropId(_Storage, "Reference")) == V_STRING)))
         Return TRUE;
      End;
      Return FALSE;
   End;

   InitTSerializer(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField13SerializerStruct(_Owner)
   Macro SerializeDown(_Storage, _Param)
      Return True;
   End;

   Macro SerializeUp(_Storage, _Param)
      Return FALSE;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField20SerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Owner.GetSubField("Reference").Value = _Storage.Reference;
      Return TRUE;
   End;

   Macro SerializeDown(_Storage, _Param)
      _Storage.Reference = Trim(Owner.GetSubField("Reference").Value);
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField21SerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Owner.GetSubField("Reference").Value = _Storage.F21;
      Return TRUE;
   End;

   Macro SerializeDown(_Storage, _Param)
      _Storage.F21 = Trim(Owner.GetSubField("Reference").Value);
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField23SerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField25SerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Owner.GetSubField("Account").Value = _Storage.F25;
      Return TRUE;
   End;

   Macro SerializeDown(_Storage, _Param)
      _Storage.F25 = Owner.GetSubField("Account").Value;
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField26SerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField28SerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var SerStr = _Storage.F28;
      Owner.SerializeUp(SerStr, WITHOUT_FIELD);
      If (NOT Owner.IsEmpty())
         Owner.Option = _Storage.F28Type;
      End;
      Return TRUE;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var SerStr = "";
      If (NOT Owner.IsEmpty())
         Owner.SerializeDown(SerStr, WITHOUT_FIELD);
         _Storage.F28 = SerStr;
         _Storage.F28Type = Owner.Option;
      End;
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField32SerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Str = _Storage.F32VDCCA;
      Owner.Option = _Storage.F32Type;
      Owner.SerializeUp(Str, WITHOUT_FIELD);
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Str = "";

      Owner.SerializeDown(Str, WITHOUT_FIELD);
      _Storage.F32Type = Owner.GetPrintableOption();
      _Storage.F32VDCCA = Str;
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField33SerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField36SerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTFieldMSSerializerStruct(_Owner)
   Var SubFieldsStr;
   Macro GetSTR(Str)
      If (ValType(Str) == V_UNDEF)
         Return "";
      End;
      Return Str;
   End;

   Macro SerializeUp(_Storage, _Param)
      Var Fir = "F" + Owner.Name + "PI", NA = "F" + Owner.Name + "NA";

      Str = "";
      If (FldIndex(_Storage, Fir) == -1)
         Fir = "F" + Owner.Name + "Account";
         If (FldIndex(_Storage, Fir) != -1)
            Str = Trim(_Storage(FldIndex(_Storage, Fir)));
         End;
      Else
         Str = Trim(_Storage(FldIndex(_Storage, Fir)));
      End;

      If (FldIndex(_Storage, NA) != -1)
         If (Str == "")
            Str = _Storage(FldIndex(_Storage, NA));
         Else
            Str = Str + FIELD_STRING_SPLITTER + _Storage(FldIndex(_Storage, NA));
         End;
      End;

      If (FldIndex(_Storage, "F" + Owner.Name + "Type") == -1)
         Owner.Option = NO_FIELD_OPTION;
      Else
         Owner.Option = _Storage(FldIndex(_Storage, "F" + Owner.Name + "Type"));
      End;

      If (Trim(Str) != "")
         Return Owner.SerializeUp(Str, WITHOUT_FIELD);
      End;
      Return TRUE;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var Fir = "PI", Fir1 = "PartyIdentifier";
      If (ValType(Owner.GetSubField("Account")) != V_UNDEF)
         Fir = "Account";
         Fir1 = "Account";
      End;
      If (ValType(Owner.GetSubField("BIC")) != V_UNDEF)
         SubFieldsStr = "BIC";
      Else
         SubFieldsStr = "";
      End;
      If (ValType(Owner.GetSubField("Name")) != V_UNDEF)
         If (SubFieldsStr == "")
            SubFieldsStr = "Name,Address,City,Country";
         Else
            SubFieldsStr = SubFieldsStr + ",Name,Address,City,Country";
         End;
      End;
      If ((FldIndex(_Storage, "F" + Owner.Name + "Type") != -1) AND (NOT Owner.IsEmpty()))
         _Storage(FldIndex(_Storage, "F" + Owner.Name + "Type")) = Owner.GetPrintableOption();
      End;
      If (FldIndex(_Storage, "F" + Owner.Name + Fir) != -1)
         _Storage(FldIndex(_Storage, "F" + Owner.Name + Fir)) = Owner.GetSubField(Fir1).Value;
      End;
      Str = OutSubFields(SubFieldsStr);
      _Storage(FldIndex(_Storage, "F" + Owner.Name + "NA")) = Str;
      Return True;
   End;

   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldMSSerializerStruct) TMTField50SerializerStruct(_Owner)
   InitTMTFieldMSSerializerStruct(_Owner);
End;

Class (TMTFieldMSSerializerStruct) TMTField59SerializerStruct(_Owner)
   InitTMTFieldMSSerializerStruct(_Owner);
End;

Class (TMTFieldMSSerializerStruct) TMTFieldBankInstSerializerStruct(_Owner)
   InitTMTFieldMSSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField60SerializerStruct(_Owner)

   Macro SerializeUp(_Storage, _Param)
      Var SerStr = "";

      if (valtype (_Storage) == V_GENOBJ)
        SerStr = _Storage.F60;
      else
        SerStr = _Storage.F60a;
      end;
      Owner.SerializeUp(SerStr, WITHOUT_FIELD);
      If (NOT Owner.IsEmpty())
         Owner.Option = _Storage.F60aType;
      End;
      Return TRUE;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var SerStr = "";
      If (NOT Owner.IsEmpty())
         Owner.SerializeDown(SerStr, WITHOUT_FIELD);
         if (valtype (_Storage) == V_GENOBJ)
           _Storage.F60 = SerStr;
         else
           _Storage.F60a = SerStr;
         end;
         _Storage.F60aType = Owner.Option;
      End;
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField62SerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var SerStr = _Storage.F62;
      Owner.SerializeUp(SerStr, WITHOUT_FIELD);
      If (NOT Owner.IsEmpty())
         Owner.Option = _Storage.F62aType;
      End;
      Return TRUE;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var SerStr = "";
      If (NOT Owner.IsEmpty())
         Owner.SerializeDown(SerStr, WITHOUT_FIELD);
         _Storage.F62 = SerStr;
         _Storage.F62aType = Owner.Option;
      End;
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField64SerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Var SerStr = _Storage.F64;
      Owner.SerializeUp(SerStr, WITHOUT_FIELD);
      Return TRUE;
   End;

   Macro SerializeDown(_Storage, _Param)
      Var SerStr = "";
      Owner.SerializeDown(SerStr, WITHOUT_FIELD);
      _Storage.F64 = SerStr;
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldMSSerializerStruct) TMTField70SerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Str = _Storage.F70;
      Return Owner.SerializeUp(Str, WITHOUT_FIELD);
   End;

   Macro SerializeDown(_Storage, _Param)
      SubFieldsStr = "Row0,Row1,Row2,Row3";
      Str = OutSubFields(SubFieldsStr);
      _Storage.F70 = Str;
      Return True;
   End;
   InitTMTFieldMSSerializerStruct(_Owner);
End;

Class (TMTFieldMSSerializerStruct) TMTField77BSerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      SubFieldsStr = "Row0,Row1,Row2";
      Str = OutSubFields(SubFieldsStr);
      _Storage.F77 = Str;
      Return True;
   End;
   InitTMTFieldMSSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField71ASerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      if (fldindex (_Storage,"F71Type") != -1)
       Owner.Option = _Storage.F71Type;
       Owner.GetSubField("Code").Value = _Storage.F71Code;
      else
       Owner.Option = "A";
       Owner.GetSubField("Code").Value = _Storage.F71A;
      end;
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      if (fldindex (_Storage,"F71Type") != -1)
       _Storage.F71Type = Owner.GetPrintableOption();
       _Storage.F71Code = Owner.GetSubField("Code").Value;
      else
       _Storage.F71A = Owner.GetSubField("Code").Value;
      end;

      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField71FSerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldSerializerStruct) TMTField71GSerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Return True;
   End;

   Macro SerializeDown(_Storage, _Param)
      Return True;
   End;
   InitTMTFieldSerializerStruct(_Owner);
End;

Class (TMTFieldMSSerializerStruct) TMTField72SerializerStruct(_Owner)
   Macro SerializeUp(_Storage, _Param)
      Str = _Storage.F72;
      If (Trim(Str) != "")
         Return Owner.SerializeUp(Str, WITHOUT_FIELD);
      End;
      Return TRUE;
   End;

   Macro SerializeDown(_Storage, _Param)
      SubFieldsStr = "Row0,Row1,Row2,Row3,Row4,Row5";
      _Storage.F72 = OutSubFields(SubFieldsStr);
      Return True;
   End;
   InitTMTFieldMSSerializerStruct(_Owner);
End;
